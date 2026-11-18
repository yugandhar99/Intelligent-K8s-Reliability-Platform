"""Normalize untrusted telemetry into typed observations and explainable findings."""

from datetime import datetime
from hashlib import sha256
import json
import re
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field

Signal = Literal["crash", "oom", "pending", "scheduling", "cpu_fit", "memory_fit",
                 "node_unready", "pressure", "pdb", "eviction_failed", "capacity"]
Source = Literal["event", "snapshot", "engine", "demo", "forecast"]

FAMILIES = {
    "crash": "workload_health", "oom": "workload_health",
    "pending": "placement", "scheduling": "placement", "cpu_fit": "placement", "memory_fit": "placement",
    "node_unready": "node_health", "pressure": "node_health",
    "pdb": "maintenance", "eviction_failed": "maintenance", "capacity": "capacity",
}
RUNBOOKS = {
    "inspect_crash": "Inspect container exits, restart counters and the most recent rollout.",
    "inspect_memory": "Compare requests and limits with measured memory and investigate OOM terminations.",
    "inspect_scheduling": "Check scheduler decisions, placement constraints and eligible worker capacity.",
    "inspect_node": "Inspect node readiness, pressure conditions and kubelet health before maintenance.",
    "inspect_budget": "Review the disruption budget and ready replicas. Retry only after an eviction is permitted.",
    "inspect_capacity": "Review CPU and memory request growth and provision headroom before maintenance.",
}
RULES = {
    "crash": ("Container restart loop", "inspect_crash", 45),
    "oom": ("Memory exhaustion or an enforced memory limit", "inspect_memory", 70),
    "pending": ("Pod has no viable placement yet", "inspect_scheduling", 35),
    "scheduling": ("Scheduler rejected placement", "inspect_scheduling", 65),
    "cpu_fit": ("The pod requests more CPU than any schedulable node can offer", "inspect_capacity", 76),
    "memory_fit": ("The pod requests more memory than any schedulable node can offer", "inspect_capacity", 76),
    "node_unready": ("Node readiness degraded", "inspect_node", 72),
    "pressure": ("Kubelet reports node resource pressure", "inspect_node", 68),
    "pdb": ("Disruption budget reports no available disruption", "inspect_budget", 64),
    "eviction_failed": ("An eviction was rejected during maintenance", "inspect_budget", 66),
    "capacity": ("Schedulable request headroom is declining or already low", "inspect_capacity", 55),
}
EVENT_SIGNALS = {
    "BackOff": "crash", "CrashLoopBackOff": "crash", "OOMKilled": "oom",
    "FailedScheduling": "scheduling", "NodeNotReady": "node_unready",
    "NodeHasMemoryPressure": "pressure", "NodeHasDiskPressure": "pressure",
    "NodeHasPIDPressure": "pressure", "EvictionBlocked": "eviction_failed",
    "FailedEviction": "eviction_failed",
}
REF = re.compile(r"^[a-zA-Z0-9][a-zA-Z0-9._-]{0,252}$")


class Observation(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    id: str
    source: Source
    signal: Signal
    subject: str
    namespace: str = ""
    node: str | None = None
    timestamp: float
    count: int = Field(default=1, ge=1, le=1_000_000)
    current_basis_points: int | None = Field(default=None, ge=0)
    projected_basis_points: int | None = Field(default=None, ge=0)

    @property
    def group(self) -> str:
        return digest(FAMILIES[self.signal], self.namespace, self.subject)


def digest(*values) -> str:
    return sha256(json.dumps(values, separators=(",", ":"), sort_keys=True).encode()).hexdigest()


def ref(value: object) -> str:
    if isinstance(value, str) and REF.fullmatch(value):
        return value
    return "unknown" if not value else f"redacted-{digest(str(value))[:16]}"


def timestamp(value: object, fallback: float) -> float:
    if isinstance(value, str):
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
            if parsed.tzinfo is not None:
                return min(fallback, parsed.timestamp())
        except (ValueError, OverflowError, OSError):
            pass
    return fallback


def observation(source: Source, signal: Signal, identity: str, subject: str, now: float, *,
                namespace: str = "", node: str | None = None, count: int = 1,
                current: int | None = None, projected: int | None = None) -> Observation:
    return Observation(id=digest(source, signal, identity), source=source, signal=signal, subject=ref(subject),
                       namespace=ref(namespace) if namespace else "", node=ref(node) if node else None,
                       timestamp=now, count=min(max(1, count), 1_000_000),
                       current_basis_points=current, projected_basis_points=projected)


def kube_observations(events: list[dict], snapshot: dict, now: float) -> list[Observation]:
    from .kube_planning import Inventory, quantities, ready, requests, worker

    inventory = Inventory(snapshot)
    pods = {pod["metadata"]["uid"]: pod for pod in inventory.pods}
    workers = {name for name, node in inventory.nodes.items() if worker(node)}

    def pod_scope(pod: dict) -> tuple[str, str, str | None]:
        meta = pod["metadata"]
        deployment = inventory.deployment(pod)
        subject = deployment["metadata"]["name"] if deployment else meta["name"]
        return subject, meta["namespace"], pod["spec"].get("nodeName")

    inventory_capacity = [
        quantities(node["status"]["allocatable"]) for node in inventory.nodes.values()
        if ready(node) and not node["spec"].get("unschedulable") and node["status"].get("allocatable")
    ]
    findings = []
    for event in events:
        target = event.get("involvedObject") or event.get("regarding") or {}
        reason = event.get("reason")
        signal = EVENT_SIGNALS.get(reason) if isinstance(reason, str) else None
        kind = target.get("kind")
        pod = pods.get(target.get("uid"))
        event_uid = event.get("metadata", {}).get("uid")
        if not signal or not event_uid:
            continue
        if kind == "Pod" and signal in {"crash", "oom", "scheduling"}:
            subject, namespace, node = pod_scope(pod) if pod else (target.get("name"), target.get("namespace"), None)
        elif kind == "Node" and signal in {"node_unready", "pressure", "eviction_failed"}:
            subject, namespace, node = target.get("name"), "", target.get("name")
        elif kind == "ServiceAccount" and signal == "eviction_failed":
            # Stage 4 emits this Event against its service account. The engine history supplies the node.
            continue
        else:
            continue
        series = event.get("series") or {}
        at = timestamp(series.get("lastObservedTime") or event.get("lastTimestamp") or event.get("eventTime")
                       or event.get("metadata", {}).get("creationTimestamp"), now)
        if at < now - 3600:
            continue
        count = series.get("count", event.get("count", 1))
        findings.append(observation("event", signal, event_uid, subject, at, namespace=namespace,
                                    node=node, count=count if type(count) is int else 1))

    for pod in pods.values():
        meta, status = pod["metadata"], pod.get("status", {})
        if meta.get("deletionTimestamp") or status.get("phase") in {"Succeeded", "Failed"}:
            continue
        subject, namespace, node = pod_scope(pod)
        def record(signal: Signal, suffix: str, count: int = 1) -> None:
            findings.append(observation("snapshot", signal, f"{meta['uid']}:{suffix}", subject, now,
                                        namespace=namespace, node=node, count=count))

        for container in status.get("containerStatuses", []) + status.get("initContainerStatuses", []):
            state, previous = container.get("state", {}), container.get("lastState", {})
            count = container.get("restartCount", 0)
            if state.get("waiting", {}).get("reason") == "CrashLoopBackOff":
                record("crash", container["name"], count)
            if any(info.get("terminated", {}).get("reason") == "OOMKilled" for info in (state, previous)):
                record("oom", container["name"], count)
        if status.get("phase") == "Pending" and any(
            condition["type"] == "PodScheduled" and condition["status"] == "False"
            and condition.get("reason") == "Unschedulable" for condition in status.get("conditions", [])
        ):
            record("pending", "unschedulable")
            if inventory_capacity:
                for index, signal in enumerate(("cpu_fit", "memory_fit")):
                    if pod["spec"].get("containers") and requests(pod)[index] > max(
                        capacity[index] for capacity in inventory_capacity
                    ):
                        record(signal, "exceeds_node_allocatable")

    for name in workers:
        node = inventory.nodes[name]
        for condition in node.get("status", {}).get("conditions", []):
            kind, status = condition["type"], condition["status"]
            signal = None
            if kind == "Ready" and status in {"False", "Unknown"}:
                signal = "node_unready"
            elif kind in {"MemoryPressure", "DiskPressure", "PIDPressure"} and status == "True":
                signal = "pressure"
            if signal:
                identity = digest(node["metadata"]["uid"], kind, condition.get("lastTransitionTime"))
                findings.append(observation("snapshot", signal, identity, name, now, node=name))

    for pdb in inventory.pdbs:
        meta, status = pdb["metadata"], pdb.get("status", {})
        expected = status.get("expectedPods", 0)
        if (expected > 0 and status.get("currentHealthy", 0) >= expected and status.get("disruptionsAllowed") == 0
                and status.get("observedGeneration") == meta.get("generation")):
            findings.append(observation("snapshot", "pdb", f"{meta['uid']}:{meta.get('generation')}",
                                        meta["name"], now, namespace=meta["namespace"]))
    return findings


def explain(observations: list[Observation], lead: Signal | None = None) -> dict:
    signals = {item.signal for item in observations}
    strongest = lead or max(signals, key=lambda signal: (RULES[signal][2], signal))
    reason, _, confidence = RULES[strongest]
    repeated = (any(item.count >= 2 for item in observations if item.signal == "eviction_failed")
                or sum(item.signal == "eviction_failed" for item in observations) >= 2)
    independent = len(signals) > 1 or {"event", "snapshot"} <= {item.source for item in observations}
    confidence = min(90, confidence + (12 if independent else 0) + (8 if repeated else 0))
    return {
        "hypothesis": reason, "confidence": confidence, "severity": "high" if repeated or confidence >= 72 else "warning",
        "signals": sorted(signals), "runbooks": sorted({RULES[signal][1] for signal in signals}),
        "supporting_evidence": [item.id for item in observations if item.signal == strongest or independent],
        "uncertainty": "Signals corroborate the symptom. The initiating change still needs verification."
        if independent else "One type of evidence supports this hypothesis. Check for other contributing factors.",
    }


def capacity_forecast(samples: list[tuple[float, int, int]]) -> dict:
    """Rates use basis points of total allocatable requests per worker pool."""
    if not samples:
        return {"samples": 0, "latest_sample_at": None, "warnings": [], "resources": {}}
    resources, warnings = {}, []
    for index, name in enumerate(("cpu", "memory"), 1):
        latest = samples[-1][index]
        projected = latest
        if len(samples) >= 3:
            first, middle, last = samples[0], samples[-2], samples[-1]
            elapsed = last[0] - first[0]
            if elapsed >= 120 and first[index] < middle[index] < latest and latest - first[index] >= 500:
                projected = min(20_000, round(latest + 1800 * (latest - first[index]) / elapsed))
        resources[name] = {"current_percent": latest / 100, "projected_30m_percent": projected / 100}
        if max(latest, projected) >= 8500:
            warnings.append(name)
    return {"samples": len(samples), "latest_sample_at": samples[-1][0], "warnings": warnings, "resources": resources}


def demo_observations(now: float) -> list[Observation]:
    """Original signals with stable deduplication keys for repeatable demonstrations."""
    return [
        observation("demo", "crash", "catalog-sidecar", "catalog-demo", now, namespace="demo-lab", count=6),
        observation("demo", "oom", "catalog-limit", "catalog-demo", now, namespace="demo-lab", count=2),
        observation("demo", "pending", "reporting-pending", "reporting-demo", now, namespace="demo-lab"),
        observation("demo", "scheduling", "reporting-fit", "reporting-demo", now, namespace="demo-lab", count=3),
        observation("demo", "pdb", "broker-pdb", "broker-demo", now, namespace="demo-lab"),
        observation("demo", "pressure", "juniper-memory", "willow", now, node="willow"),
        observation("demo", "eviction_failed", "cedar-evictions", "cedar", now, node="cedar", count=3),
    ]
