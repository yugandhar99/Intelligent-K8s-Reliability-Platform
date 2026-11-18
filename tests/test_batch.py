from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime
from pathlib import Path
from threading import Barrier

from fastapi.testclient import TestClient
import pytest

from app.batch_models import BatchPolicy, MaintenanceWindow, PolicySnapshot
from app.batch_runtime import BatchRuntime
from app.batch_service import BatchService
from app.main import create_app
from app.models import (
    Cluster, DisruptionBudget, Node, Pod, PodAffinity, PodPhase, PolicySettings,
    Resources, State, Taint, Toleration,
)
from app.policy import MaintenancePolicy
from app.random_scenarios import random_cluster
from app.scenarios import synthetic_cluster
from app.scheduler import build_plan, node_capacity
from app.store import Store


PLAN = "/maintenance/batches/plans"
EXECUTE = "/maintenance/batches/execute"
OPERATIONS = "/maintenance/batches/operations"
POLICY = "/maintenance/policy"
NOW = datetime(2026, 1, 1, 12, tzinfo=UTC)


@pytest.fixture
def batch_api(tmp_path: Path):
    path = tmp_path / "cluster.sqlite3"
    with TestClient(create_app(path)) as client:
        yield client, Store(path)


@pytest.fixture
def stage_one_policy() -> MaintenancePolicy:
    return MaintenancePolicy(PolicySettings(
        protected_namespaces={"kube-system", "platform-security"},
        termination_timeout_seconds=30, readiness_timeout_seconds=60,
    ))


def snapshot(store: Store) -> Cluster:
    with store.transaction() as db:
        return Store.snapshot(db)


def replace_cluster(store: Store, cluster: Cluster) -> None:
    with store.transaction(write=True) as db:
        db.execute("UPDATE cluster SET payload = ? WHERE id = 1", (cluster.model_dump_json(),))


def update_policy(client: TestClient, **updates) -> dict:
    current = client.get(POLICY).json()["settings"]
    response = client.put(POLICY, json={**current, **updates})
    assert response.status_code == 200, response.text
    return response.json()


def create_plan(client: TestClient, nodes: list[str]) -> dict:
    response = client.post(PLAN, json={"nodes": nodes})
    assert response.status_code == 201, response.text
    return response.json()


def execute(client: TestClient, plan_id: str) -> dict:
    response = client.post(EXECUTE, json={"plan_id": plan_id, "dry_run": False})
    assert response.status_code == 200, response.text
    return response.json()


def two_workloads() -> Cluster:
    """Create two safely drainable targets with distinct primary workloads."""
    cluster = synthetic_cluster()
    cluster.nodes["ash"] = Node(name="ash", allocatable=Resources(cpu_millis=2600, memory_mib=4000))
    cluster.pods["checkout-1"].node = "maple"
    cluster.pods["catalog-2"].node = "willow"
    return cluster


def test_parallel_wave_projects_resources_and_has_concurrent_virtual_timing(batch_api):
    client, store = batch_api
    replace_cluster(store, two_workloads())
    before = snapshot(store)
    plan = create_plan(client, ["cedar", "maple"])
    assert plan["safe"] and len(plan["waves"]) == 1
    wave = plan["waves"][0]
    assert set(wave["nodes"]) == {"cedar", "maple"}
    assert wave["estimated_seconds"] == 13
    assert {p["pod"] for p in wave["placements"]} == {"catalog-1", "checkout-1"}
    assert snapshot(store) == before
    operation = execute(client, plan["id"])
    assert operation["state"] == "succeeded"
    assert operation["simulated_elapsed_seconds"] == 13
    assert client.get(f"{OPERATIONS}/{operation['id']}").json() == operation
    after = snapshot(store)
    assert after.simulated_seconds == 13 and after.revision == 1
    assert all(after.nodes[node].schedulable for node in wave["nodes"])
    assert wave["capacity"] == [node_capacity(after, node).model_dump() for node in sorted(after.nodes)]
    starts = [event["simulated_seconds"] for event in operation["audit"] if event["event"] == "pod.termination_started"]
    assert starts == [0, 0]


@pytest.mark.parametrize("limit_name", ["parallel_limit", "max_unavailable_nodes"])
def test_parallel_limits_make_serial_waves(batch_api, limit_name):
    client, store = batch_api
    replace_cluster(store, two_workloads())
    update_policy(client, **{limit_name: 1})
    plan = create_plan(client, ["cedar", "maple"])
    assert plan["safe"] and len(plan["waves"]) == 2
    assert all(len(wave["nodes"]) == 1 for wave in plan["waves"])


def test_existing_unavailable_nodes_consume_slots(batch_api):
    client, store = batch_api
    cluster = two_workloads()
    cluster.nodes["ash"].schedulable = False
    replace_cluster(store, cluster)
    update_policy(client, max_unavailable_nodes=1)
    plan = create_plan(client, ["cedar", "maple"])
    assert not plan["safe"] and plan["problems"][0]["code"] == "unavailable_node_limit"


def test_concurrent_disruption_budget_forces_serial_waves(batch_api):
    client, store = batch_api
    cluster = two_workloads()
    cluster.budgets.append(DisruptionBudget(
        name="store-aggregate", namespace="shop", workloads={"catalog", "checkout"}, min_available=3,
    ))
    replace_cluster(store, cluster)
    plan = create_plan(client, ["cedar", "maple"])
    assert plan["safe"]
    assert [len(wave["nodes"]) for wave in plan["waves"]] == [1, 1]
    assert any(
        reason["code"] == "disruption_budget" and "store-aggregate" in reason["message"]
        for reason in plan["alternatives_rejected"]
    )
    assert execute(client, plan["id"])["state"] == "succeeded"


@pytest.mark.parametrize("unavailable", ["starting", "terminating", "node_unready"])
def test_unavailable_replicas_do_not_satisfy_batch_availability(batch_api, unavailable):
    client, store = batch_api
    cluster = two_workloads()
    if unavailable == "node_unready":
        cluster.nodes["willow"].ready = False
    else:
        cluster.pods["catalog-2"].phase = PodPhase(unavailable)
    replace_cluster(store, cluster)
    plan = create_plan(client, ["cedar"])
    assert not plan["safe"]
    assert plan["problems"][0]["code"] == "application_availability"
    assert "would have 0" in plan["problems"][0]["message"]


def test_strict_disruption_budget_explains_required_and_projected_counts(batch_api):
    client, store = batch_api
    cluster = two_workloads()
    cluster.budgets[0].min_available = 2
    replace_cluster(store, cluster)
    plan = create_plan(client, ["cedar"])
    assert not plan["safe"]
    assert plan["problems"][0]["code"] == "disruption_budget"
    assert "catalog-availability would have 1 ready pods and requires 2" in plan["problems"][0]["message"]


@pytest.mark.parametrize(("capacity", "shortage"), [
    (Resources(cpu_millis=300, memory_mib=8000), "cpu_shortage="),
    (Resources(cpu_millis=8000, memory_mib=300), "memory_shortage="),
])
def test_predictive_capacity_exhaustion_reports_bottleneck(batch_api, capacity, shortage):
    client, store = batch_api
    cluster = two_workloads()
    for node in cluster.nodes.values():
        if node.name != "cedar":
            node.allocatable = capacity
    replace_cluster(store, cluster)
    plan = create_plan(client, ["cedar"])
    assert not plan["safe"]
    assert plan["problems"][0]["code"] == "insufficient_capacity"
    assert shortage in plan["problems"][0]["message"]


@pytest.mark.parametrize(("field", "rule", "blocked"), [
    ("node_selector", {"pool": "blue"}, "node_selector"),
    ("node_affinity", [{"pool": "blue"}, {"pool": "green"}], "node_affinity"),
    ("pod_affinity", [PodAffinity(workloads={"shop/checkout"})], "pod_affinity"),
    ("pod_anti_affinity", [PodAffinity(workloads={"shop/checkout"})], "placement_constraints"),
])
def test_required_constraints_drive_destination(batch_api, field, rule, blocked):
    client, store = batch_api
    cluster = two_workloads()
    target = cluster.pods["catalog-1"]
    setattr(target, field, rule)
    cluster.nodes["maple"].labels["pool"] = "blue"
    if field == "pod_anti_affinity":
        # Only Ash has no Checkout peer. Restrict to it explicitly.
        cluster.nodes["ash"].labels["pool"] = "ash"
        target.node_selector = {"pool": "ash"}
    if field == "pod_affinity":
        # Maple has a Checkout peer and Willow can no longer host Catalog.
        cluster.nodes["willow"].allocatable = Resources(cpu_millis=1100, memory_mib=3000)
    replace_cluster(store, cluster)
    plan = create_plan(client, ["cedar"])
    assert plan["safe"]
    expected = "ash" if field == "pod_anti_affinity" else "maple"
    assert plan["waves"][0]["placements"] == [{"pod": target.name, "destination": expected}]

    if field in {"node_selector", "node_affinity"}:
        cluster.nodes["maple"].labels.clear()
    elif field == "pod_affinity":
        target.pod_affinity[0].workloads = {"shop/missing"}
    else:
        cluster.pods["checkout-1"].node = "ash"
    replace_cluster(store, cluster)
    rejected = create_plan(client, ["cedar"])
    assert not rejected["safe"]
    assert any(blocked in reason["code"] or blocked in reason["message"] for reason in rejected["problems"])


def test_reciprocal_anti_affinity_of_existing_pod_is_enforced(batch_api):
    client, store = batch_api
    cluster = two_workloads()
    cluster.pods["checkout-1"].pod_anti_affinity = [PodAffinity(workloads={"shop/catalog"})]
    cluster.pods["catalog-1"].node_selector = {"pool": "single"}
    cluster.nodes["maple"].labels["pool"] = "single"
    replace_cluster(store, cluster)
    plan = create_plan(client, ["cedar"])
    assert not plan["safe"]
    assert "existing_pod_anti_affinity" in plan["problems"][0]["message"]


def test_projected_affinity_can_bind_two_relocated_pods(batch_api):
    client, store = batch_api
    cluster = two_workloads()
    cluster.pods["catalog-1"].pod_affinity = [PodAffinity(workloads={"shop/checkout"})]
    cluster.pods["checkout-1"].pod_affinity = [PodAffinity(workloads={"shop/catalog"})]
    replace_cluster(store, cluster)
    plan = create_plan(client, ["cedar", "maple"])
    assert plan["safe"] and len(plan["waves"]) == 1
    assert len({placement["destination"] for placement in plan["waves"][0]["placements"]}) == 1


@pytest.mark.parametrize("effect", ["NoSchedule", "NoExecute"])
def test_hard_taints_require_matching_toleration(batch_api, effect):
    client, store = batch_api
    cluster = two_workloads()
    cluster.nodes["ash"].labels["pool"] = "tainted"
    cluster.nodes["ash"].taints = [Taint(key="dedicated", value="ops", effect=effect)]
    pod = cluster.pods["catalog-1"]
    pod.node_selector = {"pool": "tainted"}
    pod.tolerations = [Toleration(key="dedicated", value="wrong", effect=effect)]
    replace_cluster(store, cluster)
    plan = create_plan(client, ["cedar"])
    assert not plan["safe"] and "untolerated_taint" in plan["problems"][0]["message"]
    pod.tolerations = [Toleration(key="dedicated", operator="Exists", effect=effect)]
    replace_cluster(store, cluster)
    allowed = create_plan(client, ["cedar"])
    assert allowed["safe"]
    assert allowed["waves"][0]["placements"][0]["destination"] == "ash"


def test_soft_taints_prefer_an_untainted_destination(batch_api):
    client, store = batch_api
    cluster = two_workloads()
    cluster.nodes["willow"].taints = [Taint(key="prefer", effect="PreferNoSchedule")]
    replace_cluster(store, cluster)
    plan = create_plan(client, ["cedar"])
    assert plan["safe"]
    assert plan["waves"][0]["placements"][0]["destination"] != "willow"


def test_health_priority_and_criticality_explanation(batch_api):
    client, store = batch_api
    cluster = two_workloads()
    cluster.nodes["maple"].health_score = 25
    replace_cluster(store, cluster)
    update_policy(client, parallel_limit=1)
    plan = create_plan(client, ["cedar", "maple"])
    assert plan["priorities"][0]["node"] == "maple"
    assert plan["priorities"][0]["health_deficit"] == 0.75
    assert plan["priorities"][0]["risk_score"] > plan["priorities"][1]["risk_score"]
    assert plan["waves"][0]["nodes"] == ["maple"]
    protected = create_plan(client, ["willow"])
    assert protected["priorities"][0]["criticality"] == 1
    assert protected["problems"][0]["code"] == "protected_workload"


@pytest.mark.parametrize(("field", "code"), [
    ("fails_during_maintenance", "node_failed"),
    ("readiness_fails", "readiness_timeout"),
    ("termination_stalls", "termination_timeout"),
])
def test_late_failure_rolls_back_prior_successful_wave(batch_api, field, code):
    client, store = batch_api
    cluster = two_workloads()
    cluster.nodes["cedar"].health_score = 30
    subject = cluster.nodes["maple"] if field == "fails_during_maintenance" else cluster.pods["checkout-1"]
    setattr(subject, field, True)
    replace_cluster(store, cluster)
    update_policy(client, parallel_limit=1)
    plan = create_plan(client, ["cedar", "maple"])
    assert plan["waves"][0]["nodes"] == ["cedar"]
    result = execute(client, plan["id"])
    assert result["state"] == "failed" and result["problem"]["code"] == code
    assert any(event["event"] == "wave.completed" for event in result["audit"])
    assert result["audit"][-2]["event"] == "simulation.rolled_back"
    assert snapshot(store) == cluster


def test_bounded_exponential_retry_and_success(batch_api):
    client, store = batch_api
    cluster = two_workloads()
    cluster.nodes["cedar"].transient_cordon_failures = 3
    cluster.pods["catalog-1"].transient_eviction_failures = 2
    replace_cluster(store, cluster)
    update_policy(client, backoff_max_seconds=5)
    result = execute(client, create_plan(client, ["cedar"])["id"])
    assert result["state"] == "succeeded"
    retries = [event["details"] for event in result["audit"] if event["event"] == "api.retry_scheduled"]
    assert [event["backoff_seconds"] for event in retries] == [2, 4, 5, 2, 4]
    assert [event["action"] for event in retries] == ["cordon"] * 3 + ["eviction"] * 2
    assert result["simulated_elapsed_seconds"] == 30


def test_retry_exhaustion_restores_scheduling(batch_api):
    client, store = batch_api
    cluster = two_workloads()
    cluster.pods["catalog-1"].transient_eviction_failures = 3
    replace_cluster(store, cluster)
    update_policy(client, retry_limit=2)
    result = execute(client, create_plan(client, ["cedar"])["id"])
    assert result["state"] == "failed"
    assert result["problem"]["code"] == "retry_exhausted"
    assert snapshot(store) == cluster
    assert result["simulated_elapsed_seconds"] == 6


def test_insufficient_predicted_deadline_refuses_to_cordon(batch_api):
    client, store = batch_api
    update_policy(client, operation_deadline_seconds=12)
    cluster = snapshot(store)
    plan = create_plan(client, ["cedar"])
    assert not plan["safe"] and plan["problems"][0]["code"] == "operation_deadline"
    result = execute(client, plan["id"])
    assert result["state"] == "rejected"
    assert all(event["event"] != "node.cordoned" for event in result["audit"])
    assert snapshot(store) == cluster


def test_retries_can_exhaust_runtime_deadline(batch_api):
    client, store = batch_api
    cluster = two_workloads()
    cluster.pods["catalog-1"].transient_eviction_failures = 2
    replace_cluster(store, cluster)
    update_policy(client, operation_deadline_seconds=15)
    plan = create_plan(client, ["cedar"])
    assert plan["safe"] and plan["estimated_seconds"] == 13
    result = execute(client, plan["id"])
    assert result["state"] == "failed" and result["problem"]["code"] == "operation_deadline"
    assert result["simulated_elapsed_seconds"] == 15
    assert snapshot(store) == cluster


def test_maintenance_window_closes_between_plan_and_execution(tmp_path, stage_one_policy):
    clock = [datetime(2026, 1, 1, 23, 55, tzinfo=UTC)]
    store = Store(tmp_path / "window.sqlite3")
    store.initialize()
    settings = BatchPolicy(windows=[MaintenanceWindow(start="23:00", end="00:30")])
    service = BatchService(store, stage_one_policy, settings, clock=lambda: clock[0])
    service.initialize()
    assert settings.time_budget(datetime(2026, 1, 2, 0, 29, 50, tzinfo=UTC)) == 10
    plan = service.create_plan(["cedar"])
    assert plan.safe
    clock[0] = datetime(2026, 1, 2, 0, 30, tzinfo=UTC)
    result = service.execute(plan.id)
    assert result.state == State.REJECTED
    assert result.problem.code == "maintenance_window_closed"
    assert result.replan_reasons == ["scheduling_inputs_changed"]
    assert snapshot(store).revision == 0


@pytest.mark.parametrize("change", ["capacity", "placement"])
def test_batch_replans_after_cluster_changes(batch_api, change):
    client, store = batch_api
    cluster = two_workloads()
    replace_cluster(store, cluster)
    original = create_plan(client, ["cedar"])
    old_destination = original["waves"][0]["placements"][0]["destination"]
    if change == "capacity":
        cluster.nodes[old_destination].allocatable = Resources(cpu_millis=649, memory_mib=3000)
    else:
        cluster.pods["checkout-1"].node = "cedar"
    cluster.revision += 1
    replace_cluster(store, cluster)
    result = execute(client, original["id"])
    assert result["state"] == "succeeded"
    assert result["replanned"] and result["replan_reasons"] == ["stale_cluster_revision"]
    assert result["effective_plan"]["waves"] != original["waves"]
    assert snapshot(store).revision == 2


def test_batch_replans_policy_change_and_can_reject_new_limits(batch_api):
    client, store = batch_api
    replace_cluster(store, two_workloads())
    original = create_plan(client, ["cedar", "maple"])
    updated = update_policy(client, parallel_limit=1, operation_deadline_seconds=13)
    assert updated["revision"] == 1
    result = execute(client, original["id"])
    assert result["state"] == "rejected" and result["problem"]["code"] == "operation_deadline"
    assert result["replan_reasons"] == ["stale_policy_revision"]
    assert snapshot(store).revision == 0


@pytest.mark.parametrize("mode", ["same_plan", "overlap", "disjoint"])
def test_parallel_requests_are_idempotent_conflict_aware_or_replanned(batch_api, mode):
    client, store = batch_api
    replace_cluster(store, two_workloads())
    first = create_plan(client, ["cedar"])
    second = first if mode == "same_plan" else create_plan(
        client, ["cedar", "maple"] if mode == "overlap" else ["maple"],
    )
    barrier = Barrier(2)

    def submit(plan_id: str) -> dict:
        with TestClient(create_app(store.path)) as worker:
            barrier.wait(timeout=5)
            return execute(worker, plan_id)

    with ThreadPoolExecutor(max_workers=2) as executor:
        results = list(executor.map(submit, [first["id"], second["id"]]))
    if mode == "same_plan":
        assert results[0] == results[1] and results[0]["state"] == "succeeded"
    elif mode == "overlap":
        assert {result["state"] for result in results} == {"succeeded", "rejected"}
        rejected = next(result for result in results if result["state"] == "rejected")
        assert rejected["problem"]["code"] == "conflicting_operation"
    else:
        assert all(result["state"] == "succeeded" for result in results)
        assert sum(result["replanned"] for result in results) == 1
    assert snapshot(store).revision == (2 if mode == "disjoint" else 1)


def test_stage_one_mutation_conflicts_with_old_overlapping_batch(batch_api):
    client, store = batch_api
    replace_cluster(store, two_workloads())
    batch = create_plan(client, ["cedar"])
    single = client.post("/maintenance/plans", json={"node": "cedar"}).json()
    result = client.post("/maintenance/execute", json={"plan_id": single["id"], "dry_run": False}).json()
    assert result["state"] == "succeeded"
    assert execute(client, batch["id"])["problem"]["code"] == "conflicting_operation"


def test_policy_and_dry_run_persist_without_mutating_cluster(batch_api):
    client, store = batch_api
    before = snapshot(store)
    configured = update_policy(client, parallel_limit=1)
    assert update_policy(client, parallel_limit=1) == configured
    plan = create_plan(client, ["cedar"])
    response = client.post(EXECUTE, json={"plan_id": plan["id"]})
    assert response.status_code == 200
    assert response.json()["waves"] == plan["waves"] and response.json()["id"] != plan["id"]
    assert snapshot(store) == before
    with TestClient(create_app(store.path)) as restarted:
        assert restarted.get(POLICY).json() == configured


@pytest.mark.parametrize("body", [
    {"nodes": []}, {"nodes": ["cedar", "cedar"]}, {"nodes": [""]},
])
def test_batch_rejects_invalid_requests(batch_api, body):
    client, _ = batch_api
    assert client.post(PLAN, json=body).status_code == 422


def test_policy_validation_and_missing_records(batch_api):
    client, _ = batch_api
    for invalid in (
        {"parallel_limit": 0},
        {"backoff_base_seconds": 8, "backoff_max_seconds": 2},
        {"windows": [{"start": "12:00", "end": "12:00"}]},
        {"windows": [{"start": "12:00Z", "end": "13:00Z"}]},
    ):
        assert client.put(POLICY, json=invalid).status_code == 422
    assert client.post(EXECUTE, json={"plan_id": "unknown"}).status_code == 404
    assert client.get(f"{OPERATIONS}/unknown").status_code == 404
    assert create_plan(client, ["ghost"])["problems"][0]["code"] == "node_not_found"


def test_search_limit_fails_safely(batch_api):
    client, store = batch_api
    original = snapshot(store)
    update_policy(client, search_limit=1)
    plan = create_plan(client, ["cedar"])
    assert not plan["safe"] and plan["search_steps"] == 1
    assert plan["problems"][0]["code"] == "search_limit"
    assert snapshot(store) == original


@pytest.mark.parametrize("seed", list(range(8)))
def test_seeded_batches_respect_resource_and_availability_invariants(seed, stage_one_policy):
    cluster = random_cluster(seed)
    assert cluster == random_cluster(seed)
    settings = BatchPolicy()
    plan = build_plan(
        cluster, ["target-0", "target-1"], stage_one_policy, PolicySnapshot(settings=settings), NOW,
    )
    assert plan.safe
    expected_pods = set(cluster.pods)
    wave_nodes: set[str] = set()
    runtime = BatchRuntime(cluster, stage_one_policy, settings, 180, lambda *_: None)
    for wave in plan.waves:
        assert len(wave.nodes) <= settings.parallel_limit
        assert not wave_nodes.intersection(wave.nodes)
        wave_nodes.update(wave.nodes)
        assert all(placement.destination not in wave.nodes for placement in wave.placements)
        runtime.run_wave(wave)
        for node in cluster.nodes:
            capacity = node_capacity(cluster, node)
            assert capacity.cpu_millis_free >= 0 and capacity.memory_mib_free >= 0
        assert set(cluster.pods) == expected_pods
        assert all(pod.phase == PodPhase.READY for pod in cluster.pods.values())
    assert wave_nodes == {"target-0", "target-1"}
    assert runtime.elapsed == plan.estimated_seconds


@pytest.mark.parametrize("kind", ["placement_pressure", "backtracking_pressure"])
def test_seeded_packing_cases_have_safe_alternatives_to_greedy(kind, stage_one_policy):
    from app.policy import SafetyError
    from app.simulator import simulate_maintenance

    cluster = random_cluster(23, kind=kind)
    with pytest.raises(SafetyError, match="CPU and memory requests"):
        simulate_maintenance(cluster.model_copy(deep=True), "target-0", stage_one_policy)
    settings = BatchPolicy()
    plan = build_plan(cluster, ["target-0"], stage_one_policy, PolicySnapshot(settings=settings), NOW)
    assert plan.safe
    if kind == "backtracking_pressure":
        assert plan.search_steps > 1 + len(plan.waves[0].placements)
        assert any(reason.code == "insufficient_capacity" for reason in plan.alternatives_rejected)
    runtime = BatchRuntime(cluster, stage_one_policy, settings, 180, lambda *_: None)
    runtime.run(plan)
    for node in cluster.nodes:
        capacity = node_capacity(cluster, node)
        assert capacity.cpu_millis_free >= 0 and capacity.memory_mib_free >= 0


def test_legacy_stage_one_snapshot_loads_without_reseed(tmp_path):
    import json

    path = tmp_path / "legacy.sqlite3"
    store = Store(path)
    store.initialize()
    cluster = synthetic_cluster().model_dump(mode="json")
    cluster["revision"] = 3
    cluster["pods"]["catalog-1"]["node"] = "willow"
    for node in cluster["nodes"].values():
        for field in ("labels", "taints", "health_score", "transient_cordon_failures",
                      "fails_during_maintenance", "last_maintenance_revision"):
            del node[field]
    for pod in cluster["pods"].values():
        for field in ("node_selector", "node_affinity", "pod_affinity", "pod_anti_affinity",
                      "tolerations", "transient_eviction_failures"):
            del pod[field]
    with store.transaction(write=True) as db:
        db.execute("UPDATE cluster SET payload = ? WHERE id = 1", (json.dumps(cluster),))
    with TestClient(create_app(path)) as client:
        assert create_plan(client, ["cedar"])["cluster_revision"] == 3
        assert client.get(POLICY).json()["revision"] == 0
        assert client.get("/cluster").json()["pods"]["catalog-1"]["node"] == "willow"


@pytest.mark.parametrize(("field", "code"), [
    ("grace_seconds", "termination_timeout"),
    ("startup_seconds", "readiness_timeout"),
])
def test_known_grace_and_readiness_timeouts_are_preflight_blockers(batch_api, field, code):
    client, store = batch_api
    cluster = two_workloads()
    setattr(cluster.pods["catalog-1"], field, 61)
    replace_cluster(store, cluster)
    plan = create_plan(client, ["cedar"])
    assert not plan["safe"] and plan["problems"][0]["code"] == code
    assert snapshot(store) == cluster


def test_runtime_rollback_preserves_preexisting_cordon(batch_api):
    client, store = batch_api
    cluster = two_workloads()
    cluster.nodes["ash"].schedulable = False
    cluster.nodes["cedar"].fails_during_maintenance = True
    replace_cluster(store, cluster)
    result = execute(client, create_plan(client, ["cedar"])["id"])
    assert result["problem"]["code"] == "node_failed"
    assert snapshot(store) == cluster
    assert not snapshot(store).nodes["ash"].schedulable
