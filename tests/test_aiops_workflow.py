from concurrent.futures import ThreadPoolExecutor
import json
import tomllib
from pathlib import Path
from threading import Barrier
from time import monotonic, sleep

from fastapi.testclient import TestClient
import pytest

from app.aiops import ApprovalRefused, IncidentService
from app.aiops_api import Operators
from app.aiops_providers import AIService, MockProvider
from app.aiops_signals import observation
from app.batch_models import BatchPolicy
from app.batch_service import BatchService
from app.jobs import JobService
from app.main import create_app
from app.models import PolicySettings
from app.policy import MaintenancePolicy
from app.service import MaintenanceService
from app.store import MaintenanceConflict, Store

TOKEN_A = "operator-alexa-stage5-verification-token"
TOKEN_B = "operator-samuel-stage5-verification-token"
AUTH = {"Authorization": f"Bearer {TOKEN_A}"}


@pytest.fixture
def incidents(tmp_path):
    settings = tomllib.loads(Path("app/config.toml").read_text())
    store = Store(tmp_path / "incidents.sqlite3")
    policy = MaintenancePolicy(PolicySettings.model_validate(settings["maintenance"]))
    single = MaintenanceService(store, policy)
    batch = BatchService(store, policy, BatchPolicy.model_validate(settings["batch"]))
    clock = [100_000.0]
    jobs = JobService(single, batch, clock=lambda: clock[0])
    ai = AIService(store, MockProvider(), 0, lambda: clock[0])
    store.initialize()
    batch.initialize()
    jobs.initialize()
    service = IncidentService(jobs, ai, lambda: clock[0])
    service.initialize()
    return service, clock


def actionable(service):
    service.demo()
    incident = next(item for item in service.incidents(50, "open") if item["subject"] == "cedar")
    recommendation = service.recommend(incident["id"])
    assert recommendation["action"]["type"] == "drain_worker"
    return incident, recommendation


def add(service, *evidence):
    with service.store.transaction(write=True) as db:
        return service.ingest(db, list(evidence))


def test_demo_correlates_anomalies_and_deduplicates_polls(incidents):
    service, _ = incidents
    assert service.demo()["opened"] == 5
    first = service.incidents(50, "open")
    assert service.demo()["updated"] == 0
    assert service.incidents(50, "open") == first
    assert {signal for incident in first for signal in incident["signals"]} == {
        "crash", "oom", "scheduling", "pending", "pressure", "pdb", "eviction_failed",
    }
    assert sum(item["evidence_count"] for item in first) == 7


def test_occurrence_growth_updates_one_incident_and_history(incidents):
    service, clock = incidents
    first = observation("event", "crash", "same-series", "shop", clock[0], count=4)
    result = add(service, first)
    incident, = result["incidents"]
    assert add(service, first.model_copy(update={"count": 3}))["updated"] == 0
    assert add(service, first.model_copy(update={"count": 6}))["updated"] == 1
    details = service.incident(incident)
    assert details["revision"] == 2 and details["evidence_count"] == 1
    assert details["evidence"][0]["count"] == 6
    audit = service.history(incident, 0, 5)
    assert [entry["event"] for entry in audit] == ["incident.opened", "evidence.recorded", "evidence.recorded"]
    assert service.history(incident, audit[1]["sequence"], 1) == audit[2:]


def test_resolved_episode_stays_closed_until_new_evidence(incidents):
    service, clock = incidents
    event = observation("event", "oom", "old", "api", clock[0])
    previous, = add(service, event)["incidents"]
    assert service.resolve(previous, "alexa", "recovered")["status"] == "resolved"
    assert add(service, event)["opened"] == 0
    clock[0] += 50
    new, = add(service, observation("event", "oom", "new", "api", clock[0]))["incidents"]
    assert new != previous
    assert [row["id"] for row in service.incidents(50, "resolved")] == [previous]
    assert [row["id"] for row in service.incidents(50, "open")] == [new]


def test_recommendation_cites_real_evidence_and_uses_known_runbooks(incidents):
    service, _ = incidents
    service.demo()
    crash = next(item for item in service.incidents(50, "open") if "crash" in item["signals"])
    rec = service.recommend(crash["id"])
    assert rec["provider"] == "mock" and rec["confidence"] == 82
    assert set(rec["evidence"]) == {item["id"] for item in crash["evidence"]}
    assert rec["summary"] and rec["uncertainty"]
    assert {entry["id"] for entry in rec["runbooks"]} == {"inspect_crash", "inspect_memory"}
    assert rec["action"] is None
    assert service.recommend(crash["id"]) == rec
    assert service.ai.usage()["providers"][0]["attempts"] == 1


def test_expired_recommendation_cannot_queue_job_and_is_audited(incidents):
    service, clock = incidents
    incident, rec = actionable(service)
    clock[0] = rec["expires_at"]
    with pytest.raises(ApprovalRefused, match="expired"):
        service.approve(rec["id"], "alexa", rec["action"]["nonce"])
    assert not service.jobs.list_jobs(100).jobs
    assert service.history(incident["id"], 0, 50)[-1]["actor"] == "alexa"
    renewed = service.recommend(incident["id"])
    assert renewed["id"] != rec["id"] and renewed["action"]["nonce"] != rec["action"]["nonce"]
    assert service.ai.usage()["providers"][0]["attempts"] == 1


def test_cluster_drift_refuses_a_previously_safe_approval(incidents):
    service, _ = incidents
    incident, rec = actionable(service)
    with service.store.transaction(write=True) as db:
        cluster = service.store.snapshot(db)
        cluster.revision += 1
        db.execute("UPDATE cluster SET payload=? WHERE id=1", (cluster.model_dump_json(),))
    with pytest.raises(ApprovalRefused, match="Cluster state changed"):
        service.approve(rec["id"], "alexa", rec["action"]["nonce"])
    assert service.history(incident["id"], 0, 25)[-1]["event"] == "approval.refused"
    assert not service.jobs.list_jobs(100).jobs


def test_evidence_coinciding_with_signoff_invalidates_incident_revision(incidents):
    service, clock = incidents
    _, rec = actionable(service)
    add(service, observation("demo", "pdb", "another-block", "cedar", clock[0], node="cedar"))
    with pytest.raises(ApprovalRefused, match="Incident changed"):
        service.approve(rec["id"], "alexa", rec["action"]["nonce"])


def test_forged_node_event_cannot_nominate_executable_target(incidents):
    service, clock = incidents
    incident, = add(service, observation("event", "eviction_failed", "untrusted", "cedar", clock[0],
                                        node="cedar", count=100))["incidents"]
    rec = service.recommend(incident)
    assert rec["action"] is None
    with pytest.raises(ApprovalRefused, match="allowlisted"):
        service.approve(rec["id"], "alexa", "0" * 32)
    assert not service.jobs.list_jobs(100).jobs


def test_preflight_blocks_unsafe_target_even_when_demo_evidence_is_trusted(incidents):
    service, clock = incidents
    node = "willow"
    incident, = add(service, observation("demo", "eviction_failed", "link", node, clock[0], node=node))["incidents"]
    rec = service.recommend(incident)
    assert rec["action"] is None and rec["risk"]["blocked"]
    assert rec["risk"]["components"]["failed_preflight"] == 60


def test_concurrent_replay_consumes_one_approval_and_queues_one_job(incidents):
    service, _ = incidents
    _, rec = actionable(service)
    gate = Barrier(2)

    def approve(actor):
        gate.wait(timeout=2)
        try:
            return service.approve(rec["id"], actor, rec["action"]["nonce"])
        except ApprovalRefused as error:
            assert "consumed" in str(error)
            return None

    with ThreadPoolExecutor(max_workers=2) as pool:
        accepted, = [item for item in pool.map(approve, ("alexa", "samuel")) if item]
    job, = service.jobs.list_jobs(100).jobs
    assert accepted["job"]["id"] == job.id
    assert service.remediation(rec["id"])["approved_by"] == accepted["approved_by"]


def test_safe_remediation_uses_job_engine_and_records_post_incident_outcome(incidents):
    service, clock = incidents
    incident, rec = actionable(service)
    approval = service.approve(rec["id"], "alexa", rec["action"]["nonce"])
    assert service.jobs.single.cluster().revision == 0
    with pytest.raises(MaintenanceConflict, match="active"):
        service.resolve(incident["id"], "alexa", "mitigated")
    for _ in range(2):
        assert service.jobs.run(service.jobs.claim("unit-worker"))
    assert service.remediation(rec["id"])["job"]["state"] == "succeeded"
    assert service.jobs.single.cluster().revision == 1
    with pytest.raises(MaintenanceConflict, match="Resolve"):
        service.report(incident["id"])
    clock[0] += 123
    service.resolve(incident["id"], "samuel", "mitigated")
    report = service.report(incident["id"])
    assert report["timeline"][-1]["event"] == "incident.resolved"
    assert report["duration_seconds"] == 123 and report["recommendation_count"] == 1
    assert report["remediations"] == [{"recommendation": rec["id"], "job": approval["job"]["id"],
                                       "approved_by": "alexa", "outcome": "succeeded"}]


def test_persistent_history_survives_service_restart(incidents):
    service, clock = incidents
    incident, rec = actionable(service)
    replacement = IncidentService(service.jobs, service.ai, lambda: clock[0])
    replacement.initialize()
    assert replacement.recommendations(incident["id"]) == [rec]
    assert replacement.history(incident["id"], 0, 50) == service.history(incident["id"], 0, 50)


def test_capacity_history_produces_predictive_warning_and_risk_components(incidents):
    service, clock = incidents
    with service.store.transaction(write=True) as db:
        for offset, percent in ((3, 6200), (2, 6800), (1, 7000)):
            db.execute("INSERT INTO ai_capacity VALUES (?,?,?,?)",
                       (service.pool, int(clock[0] // 60) - offset, percent, 2000))
        forecast = service.forecast(db)
    assert forecast["warnings"] == ["cpu"]
    risk = service.assess("cedar")
    assert risk["score"] == 15 and risk["components"]["low_pool_headroom"] == 15
    assert risk["forecast"]["resources"]["cpu"]["projected_30m_percent"] > 85


@pytest.mark.parametrize("settings", [
    "[]", '{"operator":"short"}', '{"invalid identity":"'+TOKEN_A+'"}',
    json.dumps({"alex": TOKEN_A, "sam": TOKEN_A}), '{"operator":5}',
])
def test_operator_configuration_rejects_unsafe_token_mappings(settings):
    with pytest.raises(ValueError):
        Operators(settings)


@pytest.fixture
def api(tmp_path, monkeypatch):
    monkeypatch.setenv("MAINTENANCE_MODE", "simulation")
    monkeypatch.setenv("AIOPS_OPERATOR_TOKENS", json.dumps({"alexa": TOKEN_A, "samuel": TOKEN_B}))
    with TestClient(create_app(tmp_path / "api.sqlite3")) as client:
        yield client


def api_recommendation(api):
    assert api.post("/aiops/demo").status_code == 200
    incident = next(item for item in api.get("/aiops/incidents").json() if item["subject"] == "cedar")
    response = api.post(f"/aiops/incidents/{incident['id']}/recommendations")
    assert response.status_code == 200
    return incident, response.json()


@pytest.mark.parametrize(("headers", "overrides", "status"), [
    ({}, {}, 401),
    ({"Authorization": "Bearer "+TOKEN_A+"extra"}, {}, 401),
    (AUTH, {"approved": False}, 422),
    (AUTH, {"approved": 1}, 422),
    (AUTH, {"node": "willow"}, 422),
    (AUTH, {"action": "delete_namespace"}, 422),
    (AUTH, {"actor": "samuel"}, 422),
    (AUTH, {"nonce": "X" * 32}, 409),
])
def test_approval_endpoint_rejects_unauthenticated_or_tampered_requests(api, headers, overrides, status):
    _, rec = api_recommendation(api)
    response = api.post(f"/aiops/recommendations/{rec['id']}/approve",
                        headers=headers, json={"nonce": rec["action"]["nonce"], "approved": True, **overrides})
    assert response.status_code == status
    assert api.get("/maintenance/jobs").json()["jobs"] == []


def test_api_binds_approval_identity_to_token_and_provides_status_and_report(api):
    incident, rec = api_recommendation(api)
    body = {"nonce": rec["action"]["nonce"], "approved": True}
    path = f"/aiops/recommendations/{rec['id']}/approve"
    approval = api.post(path, headers=AUTH, json=body)
    assert approval.status_code == 202 and approval.json()["approved_by"] == "alexa"
    assert api.post(path, headers={"Authorization": f"Bearer {TOKEN_B}"}, json=body).status_code == 409
    deadline = monotonic() + 5
    while monotonic() < deadline:
        result = api.get(f"/aiops/remediations/{rec['id']}").json()
        if result["job"]["state"] == "succeeded":
            break
        sleep(0.05)
    assert result["job"]["state"] == "succeeded"
    assert [event["event"] for event in result["history"]][-1] == "job.succeeded"
    url = f"/aiops/incidents/{incident['id']}"
    assert api.post(f"{url}/resolve", json={"disposition": "recovered"}).status_code == 401
    assert api.post(f"{url}/resolve", headers=AUTH, json={"disposition": "recovered"}).status_code == 200
    assert api.get(f"{url}/report").json()["remediations"][0]["outcome"] == "succeeded"
    assert api.get(f"{url}/recommendations").json()[0]["id"] == rec["id"]
    assert api.get("/aiops/usage").json()["providers"][0]["accounted_microusd"] == 0


def test_missing_recommendation_and_incident_are_404(api):
    assert api.get("/aiops/incidents/missing").status_code == 404
    assert api.get("/aiops/remediations/missing").status_code == 404
    assert api.get("/aiops/incidents/missing/report").status_code == 404


def test_paid_provider_generation_requires_operator_auth_even_at_zero_budget(tmp_path, monkeypatch):
    from app.aiops_providers import HTTPProvider

    monkeypatch.setenv("MAINTENANCE_MODE", "simulation")
    monkeypatch.setenv("AIOPS_OPERATOR_TOKENS", json.dumps({"alexa": TOKEN_A}))
    monkeypatch.setenv("AIOPS_DAILY_LIMIT_MICROUSD", "0")
    monkeypatch.setattr("app.main.configured_provider", lambda: HTTPProvider("openai", "fixture", "sample-model", 1, 1))
    with TestClient(create_app(tmp_path / "metered.sqlite3")) as client:
        incident = client.post("/aiops/demo").json()["incidents"][0]
        path = f"/aiops/incidents/{incident}/recommendations"
        assert client.post(path).status_code == 401
        rec = client.post(path, headers=AUTH).json()
        assert rec["provider"] == "mock" and rec["fallback"] == "budget_exhausted"


def test_periodic_collector_retries_after_transient_failure_and_stops():
    from threading import Event
    from app.aiops_collector import EventCollector
    from app.kube_adapter import KubeUnavailable

    finished = Event()

    class Lab:
        attempts = 0

        def collect(self):
            self.attempts += 1
            if self.attempts == 1:
                raise KubeUnavailable("transient")
            finished.set()

    lab = Lab()
    collector = EventCollector(lab, 0.01)
    collector.thread.start()
    try:
        assert finished.wait(2)
    finally:
        collector.close()
    assert lab.attempts >= 2 and not collector.thread.is_alive()
