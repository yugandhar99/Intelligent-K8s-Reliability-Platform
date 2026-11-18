from collections.abc import Iterable

from .models import Cluster, Pod, PodPhase, PolicySettings, Problem, Resources


class SafetyError(Exception):
    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.problem = Problem(code=code, message=message)


class MaintenancePolicy:
    def __init__(self, settings: PolicySettings) -> None:
        self.settings = settings

    def validate_target(self, cluster: Cluster, node: str, pods: Iterable[Pod]) -> None:
        target = cluster.nodes.get(node)
        if target is None:
            raise SafetyError("node_not_found", f"Unknown node {node}")
        if not target.ready or not target.schedulable:
            raise SafetyError("node_unavailable", f"Node {node} must be Ready and schedulable")
        for pod in pods:
            workload = cluster.workloads[pod.workload]
            if workload.critical or workload.namespace in self.settings.protected_namespaces:
                raise SafetyError("protected_workload", f"Pod {pod.name} belongs to protected workload {pod.workload}")

    def validate_availability(self, cluster: Cluster, pod: Pod, *, evicting: bool) -> None:
        workload = cluster.workloads[pod.workload]
        ready = [
            candidate for candidate in cluster.pods.values()
            if candidate.phase == PodPhase.READY
            and cluster.nodes[candidate.node].ready
            and (not evicting or candidate.name != pod.name)
        ]
        if sum(candidate.workload == pod.workload for candidate in ready) < workload.min_available:
            raise SafetyError("application_availability", f"Workload {pod.workload} requires {workload.min_available} ready pods")
        for budget in cluster.budgets:
            if workload.namespace != budget.namespace or workload.name not in budget.workloads:
                continue
            count = sum(
                cluster.workloads[candidate.workload].namespace == budget.namespace
                and cluster.workloads[candidate.workload].name in budget.workloads
                for candidate in ready
            )
            if count < budget.min_available:
                raise SafetyError("disruption_budget", f"Budget {budget.name} requires {budget.min_available} ready pods")

    def destination(self, cluster: Cluster, pod: Pod) -> str:
        candidates: list[tuple[int, int, str]] = []
        for name, node in cluster.nodes.items():
            if name == pod.node or not node.ready or not node.schedulable:
                continue
            occupants = [other for other in cluster.pods.values() if other.node == name]
            free_cpu = node.allocatable.cpu_millis - sum(other.requests.cpu_millis for other in occupants)
            free_memory = node.allocatable.memory_mib - sum(other.requests.memory_mib for other in occupants)
            if free_cpu < 0 or free_memory < 0:
                continue
            if pod.requests.fits(Resources(cpu_millis=free_cpu, memory_mib=free_memory)):
                candidates.append((free_cpu - pod.requests.cpu_millis, free_memory - pod.requests.memory_mib, name))
        if not candidates:
            raise SafetyError("insufficient_capacity", f"No Ready, schedulable destination fits CPU and memory requests for {pod.name}")
        return min(candidates)[2]
