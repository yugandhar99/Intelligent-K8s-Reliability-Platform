"""Execute a prevalidated batch against an isolated simulated snapshot."""

from collections.abc import Callable
from dataclasses import dataclass

from .batch_models import BatchPlan, BatchPolicy, BatchWave
from .models import Cluster, PodPhase
from .policy import MaintenancePolicy, SafetyError
from .simulator import EventDetails


BatchRecorder = Callable[[str, str, EventDetails], None]


@dataclass
class BatchRuntime:
    cluster: Cluster
    policy: MaintenancePolicy
    settings: BatchPolicy
    budget: int
    record: BatchRecorder
    elapsed: int = 0

    def advance(self, seconds: int) -> None:
        elapsed = self.elapsed + seconds
        if elapsed > self.budget:
            self.cluster.simulated_seconds += self.budget - self.elapsed
            self.elapsed = self.budget
            raise SafetyError("operation_deadline", "Simulation reached the deadline or maintenance window end")
        self.elapsed = elapsed
        self.cluster.simulated_seconds += seconds

    def retry(self, action: str, node: str, subject: str, failures: int) -> None:
        for attempt in range(1, failures + 1):
            if attempt > self.settings.retry_limit:
                raise SafetyError("retry_exhausted", f"{action} for {subject} exceeded its transient retry limit")
            delay = min(
                self.settings.backoff_max_seconds,
                self.settings.backoff_base_seconds * 2 ** (attempt - 1),
            )
            self.record("api.retry_scheduled", node, {
                "action": action, "subject": subject, "attempt": attempt, "backoff_seconds": delay,
            })
            self.advance(delay)

    def run(self, plan: BatchPlan) -> None:
        for index, wave in enumerate(plan.waves, start=1):
            self.record("wave.started", "batch", {"wave": index, "nodes": ", ".join(wave.nodes)})
            self.run_wave(wave)
            self.record("wave.completed", "batch", {"wave": index})

    def run_wave(self, wave: BatchWave) -> None:
        for name in wave.nodes:
            node = self.cluster.nodes[name]
            self.retry("cordon", name, name, node.transient_cordon_failures)
            node.schedulable = False
            self.record("node.cordoned", name, {})
        for placement in wave.placements:
            pod = self.cluster.pods[placement.pod]
            self.retry("eviction", pod.node, pod.name, pod.transient_eviction_failures)

        # All evictions are authorized as a group before changing any pod's phase.
        for placement in wave.placements:
            pod = self.cluster.pods[placement.pod]
            pod.phase = PodPhase.TERMINATING
            self.record("pod.termination_started", pod.node, {"pod": pod.name})

        terminated_at = 0
        for placement in sorted(
            wave.placements,
            key=lambda item: (
                self.policy.settings.termination_timeout_seconds
                if self.cluster.pods[item.pod].termination_stalls
                else self.cluster.pods[item.pod].grace_seconds,
                item.pod,
            ),
        ):
            pod = self.cluster.pods[placement.pod]
            duration = self.policy.settings.termination_timeout_seconds if pod.termination_stalls else pod.grace_seconds
            self.advance(duration - terminated_at)
            terminated_at = duration
            if pod.termination_stalls:
                raise SafetyError("termination_timeout", f"Pod {pod.name} stalled during termination")
            self.record("pod.evicted", pod.node, {"pod": pod.name})

        for placement in wave.placements:
            pod = self.cluster.pods[placement.pod]
            pod.node = placement.destination
            pod.phase = PodPhase.STARTING
            self.record("pod.replacement_scheduled", pod.node, {"pod": pod.name})

        ready_at = 0
        for placement in sorted(
            wave.placements,
            key=lambda item: (
                self.policy.settings.readiness_timeout_seconds
                if self.cluster.pods[item.pod].readiness_fails
                else self.cluster.pods[item.pod].startup_seconds,
                item.pod,
            ),
        ):
            pod = self.cluster.pods[placement.pod]
            duration = self.policy.settings.readiness_timeout_seconds if pod.readiness_fails else pod.startup_seconds
            self.advance(duration - ready_at)
            ready_at = duration
            if pod.readiness_fails or not self.cluster.nodes[pod.node].ready:
                raise SafetyError("readiness_timeout", f"Replacement for {pod.name} did not become Ready")
            pod.phase = PodPhase.READY
            self.record("pod.ready", pod.node, {"pod": pod.name})
        for placement in wave.placements:
            self.policy.validate_availability(self.cluster, self.cluster.pods[placement.pod], evicting=False)

        for name in wave.nodes:
            node = self.cluster.nodes[name]
            if node.fails_during_maintenance:
                node.ready = False
                self.record("node.failed", name, {})
                raise SafetyError("node_failed", f"Node {name} became unready before scheduling was restored")
            node.schedulable = True
            self.record("node.uncordoned", name, {})
