"""Bounded search over maintenance waves and predictive pod placements."""

from collections.abc import Iterator
from datetime import datetime
from itertools import combinations
from uuid import uuid4

from .batch_models import BatchPlan, BatchWave, CapacityProjection, NodePriority, PolicySnapshot
from .constraints import broken_affinities, placement_blocks
from .models import Cluster, Placement, Pod, PodPhase, Problem
from .policy import MaintenancePolicy, SafetyError


def node_capacity(cluster: Cluster, node: str) -> CapacityProjection:
    pods = [pod for pod in cluster.pods.values() if pod.node == node]
    cpu = sum(pod.requests.cpu_millis for pod in pods)
    memory = sum(pod.requests.memory_mib for pod in pods)
    capacity = cluster.nodes[node].allocatable
    return CapacityProjection(
        node=node, cpu_millis_used=cpu, cpu_millis_free=capacity.cpu_millis - cpu,
        memory_mib_used=memory, memory_mib_free=capacity.memory_mib - memory,
    )


def priority(cluster: Cluster, node: str, policy: MaintenancePolicy) -> NodePriority:
    host = cluster.nodes[node]
    pods = [pod for pod in cluster.pods.values() if pod.node == node]
    capacity = node_capacity(cluster, node)
    pressure = min(1.0, max(
        capacity.cpu_millis_used / max(1, host.allocatable.cpu_millis),
        capacity.memory_mib_used / max(1, host.allocatable.memory_mib),
    ))
    health = (100 - host.health_score) / 100 if host.ready else 1.0
    critical = float(any(
        cluster.workloads[pod.workload].critical
        or cluster.workloads[pod.workload].namespace in policy.settings.protected_namespaces
        for pod in pods
    ))
    # A pod with no availability headroom makes the node operationally risky.
    tight = 0
    for pod in pods:
        try:
            policy.validate_availability(cluster, pod, evicting=True)
        except SafetyError:
            tight += 1
    operational = tight / len(pods) if pods else 0.0
    risk = 100 * (0.2 * health + 0.25 * pressure + 0.3 * critical + 0.25 * operational)
    return NodePriority(
        node=node, priority_score=round(100 * (0.65 * health + 0.35 * (1 - risk / 100)), 2),
        risk_score=round(risk, 2), health_deficit=round(health, 4),
        utilization=round(pressure, 4), criticality=critical, operational_risk=round(operational, 4),
    )


class WaveSearch:
    def __init__(self, policy: MaintenancePolicy, settings: PolicySnapshot) -> None:
        self.policy = policy
        self.settings = settings.settings
        self.steps = 0
        self.problems: dict[tuple[str, str], Problem] = {}

    def visit(self) -> None:
        if self.steps >= self.settings.search_limit:
            raise SafetyError("search_limit", "Scheduling search limit reached before a safe full schedule was found")
        self.steps += 1

    def report(self, error: SafetyError) -> None:
        problem = error.problem
        if len(self.problems) < 48:
            self.problems[(problem.code, problem.message)] = problem

    def schedule(
        self, cluster: Cluster, remaining: tuple[str, ...], seconds: int,
    ) -> list[BatchWave] | None:
        if not remaining:
            return []
        unavailable = sum(not node.ready or not node.schedulable for node in cluster.nodes.values())
        width = min(self.settings.parallel_limit, self.settings.max_unavailable_nodes - unavailable, len(remaining))
        if width <= 0:
            self.report(SafetyError("unavailable_node_limit", "Existing unavailable nodes leave no maintenance slots"))
            return None
        for size in range(width, 0, -1):
            for targets in combinations(remaining, size):
                self.visit()
                try:
                    for projected, wave in self.wave_options(cluster, targets, seconds):
                        tail = self.schedule(
                            projected, tuple(node for node in remaining if node not in targets),
                            seconds - wave.estimated_seconds,
                        )
                        if tail is not None:
                            return [wave, *tail]
                except SafetyError as error:
                    if error.problem.code == "search_limit":
                        raise
                    self.report(error)
        return None

    def wave_options(
        self, cluster: Cluster, targets: tuple[str, ...], seconds: int,
    ) -> Iterator[tuple[Cluster, BatchWave]]:
        snapshot = cluster.model_copy(deep=True)
        moving = [pod for pod in snapshot.pods.values() if pod.node in targets]
        for node in targets:
            self.policy.validate_target(snapshot, node, (pod for pod in moving if pod.node == node))
            snapshot.nodes[node].schedulable = False
        for pod in moving:
            del snapshot.pods[pod.name]
        for pod in moving:
            self.policy.validate_availability(snapshot, pod, evicting=True)
            if pod.grace_seconds > self.policy.settings.termination_timeout_seconds:
                raise SafetyError("termination_timeout", f"Pod {pod.name} exceeds its grace-period timeout")
            if pod.startup_seconds > self.policy.settings.readiness_timeout_seconds:
                raise SafetyError("readiness_timeout", f"Pod {pod.name} exceeds its readiness timeout")
        duration = max((pod.grace_seconds for pod in moving), default=0) + max(
            (pod.startup_seconds for pod in moving), default=0,
        )
        if duration > seconds:
            raise SafetyError("operation_deadline", f"Wave {', '.join(targets)} needs {duration}s with {seconds}s remaining")
        choices = {
            pod.name: self.destinations(snapshot, pod) for pod in moving
        }
        moving.sort(key=lambda pod: (
            len(choices[pod.name]), -pod.requests.cpu_millis, -pod.requests.memory_mib, pod.name,
        ))
        for placements in self.place(snapshot, moving):
            broken = broken_affinities(snapshot)
            if broken:
                self.report(SafetyError("pod_affinity", f"Required co-location is missing for {', '.join(broken)}"))
                continue
            projected = snapshot.model_copy(deep=True)
            for node in targets:
                projected.nodes[node].schedulable = True
            yield projected, BatchWave(
                nodes=list(targets), placements=placements, estimated_seconds=duration,
                capacity=[node_capacity(projected, node) for node in sorted(projected.nodes)],
            )

    def destinations(self, cluster: Cluster, pod: Pod) -> list[tuple[int, int, int, str]]:
        choices = []
        rejected = []
        capacity_blocked = False
        for name, node in sorted(cluster.nodes.items()):
            if not node.ready or not node.schedulable:
                continue
            # Defer required co-location until all pod destinations are assigned.
            blocks = [
                reason for reason in placement_blocks(cluster, pod, node, cluster.pods)
                if reason != "pod_affinity"
            ]
            capacity = node_capacity(cluster, name)
            cpu = capacity.cpu_millis_free - pod.requests.cpu_millis
            memory = capacity.memory_mib_free - pod.requests.memory_mib
            capacity_blocked |= not blocks and (cpu < 0 or memory < 0)
            if cpu < 0:
                blocks.append(f"cpu_shortage={-cpu}m")
            if memory < 0:
                blocks.append(f"memory_shortage={-memory}MiB")
            if blocks:
                rejected.append(f"{name} ({', '.join(blocks)})")
                continue
            preference_cost = sum(
                taint.effect == "PreferNoSchedule"
                and not any(toleration.accepts(taint) for toleration in pod.tolerations)
                for taint in node.taints
            )
            choices.append((preference_cost, cpu, memory, name))
        if not choices:
            details = "; ".join(rejected) or "no Ready, schedulable destinations"
            code = "insufficient_capacity" if capacity_blocked or not rejected else "placement_constraints"
            raise SafetyError(code, f"No destination fits {pod.name}: {details}")
        return sorted(choices)

    def place(self, cluster: Cluster, pods: list[Pod]) -> Iterator[list[Placement]]:
        # Use a stack so large waves are limited by the search budget, not Python recursion.
        choices: list[Iterator[tuple[int, int, int, str]]] = []
        assigned: list[Placement] = []
        try:
            while True:
                index = len(assigned)
                if index == len(pods):
                    yield assigned.copy()
                else:
                    pod = pods[index]
                    if len(choices) == index:
                        try:
                            choices.append(iter(self.destinations(cluster, pod)))
                        except SafetyError as error:
                            self.report(error)
                            choices.append(iter(()))
                    option = next(choices[index], None)
                    if option is not None:
                        self.visit()
                        node = option[3]
                        cluster.pods[pod.name] = pod.model_copy(
                            update={"node": node, "phase": PodPhase.READY},
                        )
                        assigned.append(Placement(pod=pod.name, destination=node))
                        continue
                    choices.pop()
                if not assigned:
                    return
                del cluster.pods[assigned.pop().pod]
        finally:
            for placement in assigned:
                del cluster.pods[placement.pod]


def build_plan(
    cluster: Cluster, nodes: list[str], policy: MaintenancePolicy, settings: PolicySnapshot,
    now: datetime,
) -> BatchPlan:
    plan = BatchPlan(
        id=uuid4().hex, nodes=nodes, cluster_revision=cluster.revision,
        policy_revision=settings.revision, safe=False,
        priorities=sorted(
            (priority(cluster, node, policy) for node in nodes if node in cluster.nodes),
            key=lambda item: (-item.priority_score, item.node),
        ),
    )
    search = WaveSearch(policy, settings)
    try:
        for node in nodes:
            policy.validate_target(cluster, node, (pod for pod in cluster.pods.values() if pod.node == node))
        budget = settings.settings.time_budget(now)
        if budget is None:
            raise SafetyError("maintenance_window_closed", "No configured UTC maintenance window is open")
        waves = search.schedule(cluster, tuple(item.node for item in plan.priorities), budget)
        if waves is not None:
            plan.safe = True
            plan.waves = waves
            plan.estimated_seconds = sum(wave.estimated_seconds for wave in waves)
    except SafetyError as error:
        if error.problem.code == "search_limit":
            plan.problems.append(error.problem)
        else:
            search.report(error)
    plan.search_steps = search.steps
    if plan.safe:
        plan.alternatives_rejected = list(search.problems.values())
    else:
        plan.problems.extend(search.problems.values())
    return plan
