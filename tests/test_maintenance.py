from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from threading import Barrier

from fastapi.testclient import TestClient
import pytest

from app.main import create_app
from app.models import Cluster, DisruptionBudget, PodPhase, Resources
from app.store import Store


@pytest.fixture
def api(tmp_path_factory: pytest.TempPathFactory):
    # Keep test databases in the project, alongside the ignored local demo database.
    path = tmp_path_factory.mktemp("cluster") / "cluster.sqlite3"
    with TestClient(create_app(path)) as client:
        yield client, path


def snapshot(path: Path) -> Cluster:
    with Store(path).transaction() as connection:
        return Store.snapshot(connection)


def save(path: Path, cluster: Cluster) -> None:
    with Store(path).transaction(write=True) as connection:
        connection.execute("UPDATE cluster SET payload = ? WHERE id = 1", (cluster.model_dump_json(),))


def plan(client: TestClient, node: str = "cedar") -> dict:
    response = client.post("/maintenance/plans", json={"node": node})
    assert response.status_code == 201
    return response.json()


def execute(client: TestClient, plan_id: str) -> dict:
    response = client.post("/maintenance/execute", json={"plan_id": plan_id, "dry_run": False})
    assert response.status_code == 200
    return response.json()


def audit(client: TestClient, operation: dict) -> list[dict]:
    response = client.get(f"/maintenance/operations/{operation['id']}/audit")
    assert response.status_code == 200
    return response.json()


def test_full_workflow_and_persistence(api):
    client, path = api
    assert client.get("/health").json() == {"status": "ok", "mode": "simulation"}
    before = client.get("/cluster").json()
    proposal = plan(client)
    assert proposal["safe"]
    assert proposal["placements"] == [
        {"pod": "checkout-1", "destination": "willow"},
        {"pod": "catalog-1", "destination": "maple"},
    ]
    preview = client.post("/maintenance/execute", json={"plan_id": proposal["id"]})
    assert preview.status_code == 200
    assert preview.json()["placements"] == proposal["placements"]
    assert client.get("/cluster").json() == before

    operation = execute(client, proposal["id"])
    assert operation["state"] == "succeeded"
    assert execute(client, proposal["id"]) == operation
    after = client.get("/cluster").json()
    assert after["revision"] == 1
    assert after["simulated_seconds"] == 26
    assert after["nodes"]["cedar"]["schedulable"]
    assert all(pod["phase"] == "ready" and pod["node"] != "cedar" for pod in after["pods"].values())
    events = audit(client, operation)
    assert [event["details"]["state"] for event in events
            if event["event"] == "operation.state_changed"] == [
        "validating", "cordoned", "draining", "validating_readiness", "uncordoning", "succeeded",
    ]
    assert len(events) == 17
    eviction = [event for event in events if event["event"] == "pod.evicted"]
    readiness = [event for event in events if event["event"] == "pod.ready"]
    assert [event["simulated_seconds"] for event in eviction] == [5, 18]
    assert [event["simulated_seconds"] for event in readiness] == [13, 26]
    with TestClient(create_app(path)) as reopened:
        assert reopened.get(f"/maintenance/operations/{operation['id']}").json() == operation
        assert reopened.get("/cluster").json() == after
        assert audit(reopened, operation) == events


@pytest.mark.parametrize("capacity", [
    Resources(cpu_millis=1200, memory_mib=4096),
    Resources(cpu_millis=4000, memory_mib=1500),
])
def test_detects_cpu_and_memory_shortages(api, capacity):
    client, path = api
    cluster = snapshot(path)
    for name in ("maple", "willow"):
        cluster.nodes[name].allocatable = capacity
    save(path, cluster)
    assert plan(client)["problems"][0]["code"] == "insufficient_capacity"
    assert snapshot(path) == cluster


def test_protected_namespace_and_critical_workload(api):
    client, path = api
    assert plan(client, "willow")["problems"][0]["code"] == "protected_workload"
    cluster = snapshot(path)
    cluster.workloads["shop/checkout"].critical = True
    save(path, cluster)
    proposal = plan(client)
    result = execute(client, proposal["id"])
    assert result["state"] == "rejected"
    assert result["problem"]["code"] == "protected_workload"
    assert snapshot(path) == cluster
    assert all(event["event"] != "node.cordoned" for event in audit(client, result))


@pytest.mark.parametrize("policy_code", ["disruption_budget", "application_availability"])
def test_enforces_budget_and_application_floor_independently(api, policy_code):
    client, path = api
    cluster = snapshot(path)
    if policy_code == "disruption_budget":
        cluster.budgets.append(DisruptionBudget(
            name="shop-wide", namespace="shop",
            workloads={"checkout", "catalog"}, min_available=4,
        ))
    else:
        cluster.workloads["shop/checkout"].min_available = 2
    save(path, cluster)
    assert plan(client)["problems"][0]["code"] == policy_code


@pytest.mark.parametrize("phase", [PodPhase.STARTING, PodPhase.TERMINATING])
def test_unready_replicas_do_not_count_toward_availability(api, phase):
    client, path = api
    cluster = snapshot(path)
    cluster.pods["checkout-2"].phase = phase
    save(path, cluster)
    assert plan(client)["problems"][0]["code"] == "application_availability"


@pytest.mark.parametrize("failure, code", [
    ("termination_stalls", "termination_timeout"),
    ("readiness_fails", "readiness_timeout"),
])
def test_runtime_failure_rolls_back_even_after_a_replacement_is_ready(api, failure, code):
    client, path = api
    cluster = snapshot(path)
    setattr(cluster.pods["catalog-1"], failure, True)
    save(path, cluster)
    proposal = plan(client)
    assert proposal["safe"]
    operation = execute(client, proposal["id"])
    assert operation["state"] == "failed"
    assert operation["problem"]["code"] == code
    assert snapshot(path) == cluster
    events = audit(client, operation)
    assert any(event["event"] == "pod.ready" for event in events)
    assert events[-2]["event"] == "simulation.rolled_back"
    assert events[-1]["details"]["state"] == "failed"


def test_planning_detects_known_timeout(api):
    client, path = api
    cluster = snapshot(path)
    cluster.pods["checkout-1"].startup_seconds = 61
    save(path, cluster)
    assert plan(client)["problems"][0]["code"] == "readiness_timeout"


def test_rejects_unready_nodes_and_unavailable_destinations(api):
    client, path = api
    cluster = snapshot(path)
    cluster.nodes["maple"].ready = False
    cluster.nodes["willow"].schedulable = False
    save(path, cluster)
    assert plan(client, "maple")["problems"][0]["code"] == "node_unavailable"
    assert plan(client)["problems"][0]["code"] == "insufficient_capacity"


@pytest.mark.parametrize("same_plan", [True, False])
def test_concurrent_execution_serializes_writers(api, same_plan):
    client, path = api
    first = plan(client)["id"]
    second = first if same_plan else plan(client, "maple")["id"]
    barrier = Barrier(2)

    def submit(plan_id: str) -> dict:
        # Each worker opens its own application and SQLite connections.
        with TestClient(create_app(path)) as worker:
            barrier.wait(timeout=5)
            return execute(worker, plan_id)

    with ThreadPoolExecutor(max_workers=2) as executor:
        results = list(executor.map(submit, (first, second)))
    assert snapshot(path).revision == 1
    if same_plan:
        assert results[0] == results[1]
        assert results[0]["state"] == "succeeded"
    else:
        assert {result["state"] for result in results} == {"succeeded", "rejected"}
        stale = next(result for result in results if result["state"] == "rejected")
        assert stale["problem"]["code"] == "stale_plan"


def test_missing_records_and_input_validation(api):
    client, _ = api
    assert plan(client, "unknown")["problems"][0]["code"] == "node_not_found"
    assert client.get("/maintenance/operations/unknown").status_code == 404
    assert client.get("/maintenance/operations/unknown/audit").status_code == 404
    assert client.post("/maintenance/execute", json={"plan_id": "unknown"}).status_code == 404
    assert client.post("/maintenance/execute", json={"plan_id": "unknown", "dry_run": "false"}).status_code == 422
    assert client.post("/maintenance/plans", json={"node": ""}).status_code == 422
