"""Scrape durable state without contacting Kubernetes from the scrape thread."""

from bisect import bisect_left
from collections import defaultdict
from math import inf
from threading import Event, Thread
from time import time

from prometheus_client.core import GaugeMetricFamily, HistogramMetricFamily

from .telemetry import WALL_BUCKETS, logger, span

STATES = ("queued", "running", "retrying", "paused", "succeeded", "failed", "rejected", "cancelled")


class ClusterMonitor:
    def __init__(self, adapter, telemetry, interval):
        self.adapter, self.telemetry, self.interval = adapter, telemetry, interval
        self.snapshot = None
        self.available = False
        self.stop = Event()
        self.thread = Thread(target=self.run, name="observability-cluster", daemon=True)

    def run(self):
        from .kube_adapter import KubeUnavailable, LabSafetyError

        with self.telemetry.bind():
            while not self.stop.is_set():
                try:
                    with span("kubernetes.inventory.poll"):
                        inventory = self.adapter.discover()
                        usage = self.adapter.node_usage()
                    self.snapshot = (time(), live_measurements(inventory, usage))
                    self.available = True
                except (KubeUnavailable, LabSafetyError) as error:
                    self.available = False
                    logger.warning("telemetry.cluster_poll_failed", extra={"fields": {"error": type(error).__name__}})
                self.stop.wait(self.interval)

    def close(self):
        self.stop.set()
        self.thread.join()


def live_measurements(snapshot, usage):
    from .kube_planning import quantities, ready, requests, worker

    nodes = {node["metadata"]["name"]: node for node in snapshot["nodes"]}
    result = []
    for role in ("worker", "control_plane"):
        selected = {name: node for name, node in nodes.items() if worker(node) == (role == "worker")}
        flags = [ready(node) for node in selected.values()]
        result.extend([
            ("cluster_nodes", "Nodes by role and readiness", ("role", "state"), (role, "ready"), sum(flags)),
            ("cluster_nodes", "Nodes by role and readiness", ("role", "state"), (role, "unready"), len(flags) - sum(flags)),
        ])
    result += [
        ("cluster_pressure_nodes", "Nodes reporting pressure", ("resource",), (kind,), sum(
            any(item["type"] == condition and item["status"] == "True" for item in node["status"].get("conditions", []))
            for node in nodes.values()
        )) for kind, condition in (("memory", "MemoryPressure"), ("disk", "DiskPressure"), ("pid", "PIDPressure"))
    ]
    alloc = [quantities(node["status"]["allocatable"]) for node in nodes.values() if worker(node)]
    demand = [requests(pod) for pod in snapshot["pods"] if pod["spec"].get("nodeName") in nodes
              and worker(nodes[pod["spec"]["nodeName"]]) and pod.get("status", {}).get("phase") not in {"Failed", "Succeeded"}]
    worker_names = {name for name, node in nodes.items() if worker(node)}
    usage_names = worker_names & usage.keys() if usage is not None else set()
    result.append(("runtime_metrics_available", "All workers have metrics-server samples", (), (),
                   int(bool(worker_names) and usage_names == worker_names)))
    for index, resource in enumerate(("cpu", "memory")):
        capacity = sum(values[index] for values in alloc)
        if capacity:
            result.append(("cluster_resource_ratio", "Worker resource fraction", ("resource", "source"),
                           (resource, "requests"), float(sum(values[index] for values in demand) / capacity)))
            if worker_names and usage_names == worker_names:
                result.append(("cluster_resource_ratio", "Worker resource fraction", ("resource", "source"),
                               (resource, "usage"), float(sum(quantities(usage[name])[index] for name in worker_names) / capacity)))
    deployments = [dep for dep in snapshot["deployments"] if dep["metadata"]["namespace"] == "reliability-lab"]
    return result + workload_metrics(
        sum(dep.get("status", {}).get("readyReplicas", 0) for dep in deployments),
        sum(dep["spec"].get("replicas", 1) for dep in deployments),
        [(pdb.get("status", {}).get("currentHealthy", 0), pdb.get("status", {}).get("desiredHealthy", 0),
          pdb.get("status", {}).get("disruptionsAllowed", 0))
         for pdb in snapshot["pdbs"] if pdb["metadata"]["namespace"] == "reliability-lab"
         and pdb.get("status", {}).get("expectedPods", 0)
         and pdb["status"].get("observedGeneration") == pdb["metadata"].get("generation")],
    )


def workload_metrics(ready_count, desired, budgets):
    return [
        ("application_replicas", "Application replicas by state", ("state",), ("ready",), ready_count),
        ("application_replicas", "Application replicas by state", ("state",), ("desired",), desired),
        ("pdb_violations", "Budgets with healthy replicas below their minimum", (), (),
         sum(healthy < minimum for healthy, minimum, _ in budgets)),
        ("pdb_zero_slack", "Budgets permitting zero disruptions", (), (), sum(slack == 0 for _, _, slack in budgets)),
    ]


def simulation_measurements(cluster):
    nodes = list(cluster.nodes.values())
    ready_count = sum(node.ready for node in nodes)
    result = [
        ("cluster_nodes", "Nodes by role and readiness", ("role", "state"), ("worker", "ready"), ready_count),
        ("cluster_nodes", "Nodes by role and readiness", ("role", "state"), ("worker", "unready"), len(nodes) - ready_count),
    ]
    for name in ("cpu", "memory"):
        field = "cpu_millis" if name == "cpu" else "memory_mib"
        capacity = sum(getattr(node.allocatable, field) for node in nodes)
        if capacity:
            requested = sum(getattr(pod.requests, field) for pod in cluster.pods.values())
            result.append(("cluster_resource_ratio", "Worker resource fraction", ("resource", "source"),
                           (name, "requests"), requested / capacity))
    ready_pods = [pod for pod in cluster.pods.values() if pod.phase == "ready"]
    budgets = []
    for budget in cluster.budgets:
        count = sum(pod.workload in budget.workloads for pod in ready_pods)
        budgets.append((count, budget.min_available, max(0, count - budget.min_available)))
    return result + workload_metrics(len(ready_pods), len(cluster.pods), budgets)


def histogram(name, description, labels, samples):
    family = HistogramMetricFamily(f"reliability_{name}_seconds", description, labels=labels)
    limits = (*WALL_BUCKETS, inf)
    for label_values, durations in samples.items():
        counts = [0] * len(limits)
        total = 0.0
        for duration in durations:
            value = max(0.0, duration)
            counts[bisect_left(limits, value)] += 1
            total += value
        running = 0
        buckets = []
        for limit, count in zip(limits, counts):
            running += count
            buckets.append(("+Inf" if limit == inf else str(float(limit)), running))
        family.add_metric(label_values, buckets, total)
    return family


class DurableCollector:
    def __init__(self, store, mode, pool, cluster_monitor):
        self.store, self.mode, self.pool, self.monitor = store, mode, pool, cluster_monitor

    def collect(self):
        gauges = {}

        def emit(name, description, labels, values, value, mode=None):
            if name not in gauges:
                gauges[name] = GaugeMetricFamily(f"reliability_{name}", description, labels=("mode", *labels))
            gauges[name].add_metric((mode or self.mode, *values), value)

        now = time()
        with self.store.transaction() as db:
            if self.mode == "simulation":
                for sample in simulation_measurements(self.store.snapshot(db)):
                    emit(*sample)
            emit("workers_healthy", "Workers with a current heartbeat", (), (),
                 db.execute("SELECT count(*) FROM job_workers WHERE expires>?", (now,)).fetchone()[0])
            stamp, = db.execute("SELECT COALESCE(MAX(heartbeat), 0) FROM job_workers").fetchone()
            emit("worker_latest_heartbeat_timestamp_seconds", "Newest recorded heartbeat", (), (), stamp)
            emit("leases_overdue", "Worker-held leases past expiry", (), (),
                 db.execute("SELECT count(*) FROM jobs WHERE worker IS NOT NULL AND expiry<=?", (now,)).fetchone()[0])
            counts = {(kind, state): count for kind, state, count in db.execute(
                "SELECT kind,json_extract(payload,'$.state'),COUNT(*) FROM jobs GROUP BY 1,2"
            )}
            workflows = ("single", "batch", "kubernetes") if self.mode == "kubernetes" else ("single", "batch")
            durations = defaultdict(list)
            active = 0
            for kind in workflows:
                mode = "kubernetes" if kind == "kubernetes" else "simulation"
                for state in STATES:
                    if state in {"succeeded", "failed", "rejected", "cancelled"}:
                        durations[(mode, kind, state)]
                    emit("jobs", "Durable jobs by current state", ("workflow", "state"),
                         (kind, state), counts.get((kind, state), 0), mode)
                for state, started, finished in db.execute("""SELECT json_extract(jobs.payload,'$.state'),
                    MIN(job_history.timestamp),MAX(CASE WHEN job_history.event IN
                    ('job.succeeded','job.failed','job.rejected','job.cancelled') THEN job_history.timestamp END)
                    FROM jobs JOIN job_history ON job_history.job_id=jobs.id WHERE jobs.kind=? GROUP BY jobs.id""",
                    (kind,)):
                    if finished is not None:
                        durations[(mode, kind, state)].append(finished - started)
                    elif state in {"queued", "running", "retrying"}:
                        active = max(active, now - started)
            emit("oldest_active_job_seconds", "Age of oldest queued, running or retrying job", (), (), active)
            incident_times = {(self.mode, "detection"): [], (self.mode, "resolution"): []}
            for opened, detected, resolved in db.execute("""SELECT i.opened,a.timestamp,i.resolved FROM ai_incidents i
                JOIN ai_incident_audit a ON a.incident_id=i.id AND a.event='incident.opened' WHERE i.pool=?""",
                (self.pool,)):
                incident_times[(self.mode, "detection")].append(detected - opened)
                if resolved is not None:
                    incident_times[(self.mode, "resolution")].append(resolved - opened)
            for state in ("open", "resolved"):
                count, = db.execute("SELECT COUNT(*) FROM ai_incidents WHERE pool=? AND (resolved IS NULL)=?",
                                    (self.pool, int(state == "open"))).fetchone()
                emit("incidents", "Incidents by state", ("state",), (state,), count)
            approvals = {(self.mode,): [stamp - created for stamp, created in db.execute(
                """SELECT a.timestamp,r.created FROM ai_remediations a JOIN ai_recommendations r ON r.id=a.id
                JOIN ai_incidents i ON i.id=r.incident_id WHERE i.pool=?""", (self.pool,)
            )]}
            daily = {
                name: (input_tokens, output_tokens, charge)
                for name, input_tokens, output_tokens, charge in db.execute("""SELECT provider,
                    COALESCE(SUM(input_tokens),0),COALESCE(SUM(output_tokens),0),SUM(charge)
                    FROM ai_usage WHERE day=? GROUP BY provider""", (int(now // 86400),))
            }
            for name in ("mock", "openai", "anthropic", "gemini"):
                inp, out, charge = daily.get(name, (0, 0, 0))
                emit("ai_tokens_today", "Reported UTC-day AI tokens", ("provider", "direction"), (name, "input"), inp)
                emit("ai_tokens_today", "Reported UTC-day AI tokens", ("provider", "direction"), (name, "output"), out)
                emit("ai_cost_today_dollars", "UTC-day accounted USD including unsettled reservations",
                     ("provider",), (name,), charge / 1_000_000)
        if self.monitor:
            emit("cluster_poll_success", "Latest live cluster poll succeeded", (), (), int(self.monitor.available))
            if snapshot := self.monitor.snapshot:
                stamp, measurements = snapshot
                emit("cluster_last_success_timestamp_seconds", "Time of last successful live cluster poll", (), (), stamp)
                for sample in measurements:
                    emit(*sample)
        yield from gauges.values()
        yield histogram("job_duration", "Durable job wall time from admission through terminal state",
                        ("mode", "workflow", "outcome"), durations)
        yield histogram("incident_duration", "Incident observation-to-detection or observation-to-resolution time",
                        ("mode", "phase"), incident_times)
        yield histogram("aiops_approval_duration", "Time from recommendation to approved job admission",
                        ("mode",), approvals)
