from collections.abc import Callable

from .models import Cluster, Placement, PodPhase, State
from .policy import MaintenancePolicy, SafetyError


StageChange = Callable[[State], None]
EventDetails = dict[str, str | int | bool]
EventRecorder = Callable[[str, int, EventDetails], None]


def simulate_maintenance(
    cluster: Cluster,
    node: str,
    policy: MaintenancePolicy,
    *,
    executing: bool = False,
    change_state: StageChange = lambda state: None,
    record: EventRecorder = lambda event, seconds, details: None,
) -> list[Placement]:
    """Apply the same scheduling and safety decisions to planning and execution."""
    pods = sorted(
        (pod for pod in cluster.pods.values() if pod.node == node),
        key=lambda pod: (-pod.requests.cpu_millis, -pod.requests.memory_mib, pod.name),
    )
    policy.validate_target(cluster, node, pods)
    cluster.nodes[node].schedulable = False
    change_state(State.CORDONED)
    record("node.cordoned", cluster.simulated_seconds, {})
    change_state(State.DRAINING)
    placements: list[Placement] = []
    for pod in pods:
        policy.validate_availability(cluster, pod, evicting=True)
        destination = policy.destination(cluster, pod)
        settings = policy.settings
        pod.phase = PodPhase.TERMINATING
        record("pod.termination_started", cluster.simulated_seconds, {"pod": pod.name})
        if pod.grace_seconds > settings.termination_timeout_seconds or (executing and pod.termination_stalls):
            cluster.simulated_seconds += settings.termination_timeout_seconds
            raise SafetyError("termination_timeout", f"Pod {pod.name} did not terminate within the configured timeout")
        cluster.simulated_seconds += pod.grace_seconds
        record("pod.evicted", cluster.simulated_seconds, {"pod": pod.name})
        pod.node = destination
        pod.phase = PodPhase.STARTING
        record("pod.replacement_scheduled", cluster.simulated_seconds, {"pod": pod.name, "destination": destination})
        if pod.startup_seconds > settings.readiness_timeout_seconds or (executing and pod.readiness_fails):
            cluster.simulated_seconds += settings.readiness_timeout_seconds
            raise SafetyError("readiness_timeout", f"Replacement for {pod.name} did not become Ready within the configured timeout")
        cluster.simulated_seconds += pod.startup_seconds
        pod.phase = PodPhase.READY
        policy.validate_availability(cluster, pod, evicting=False)
        record("pod.ready", cluster.simulated_seconds, {"pod": pod.name, "destination": destination})
        placements.append(Placement(pod=pod.name, destination=destination))

    change_state(State.VALIDATING_READINESS)
    for pod in pods:
        if pod.phase != PodPhase.READY or not cluster.nodes[pod.node].ready:
            raise SafetyError("replacement_unready", f"Replacement for {pod.name} is not Ready")
        policy.validate_availability(cluster, pod, evicting=False)
    change_state(State.UNCORDONING)
    cluster.nodes[node].schedulable = True
    record("node.uncordoned", cluster.simulated_seconds, {})
    return placements
