from io import StringIO
import json
import logging
from time import monotonic, sleep, time

from fastapi.testclient import TestClient
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from prometheus_client.parser import text_string_to_metric_families
import pytest

from app.main import JsonFormatter, create_app
from app.telemetry_state import live_measurements


def values(payload, metric):
    return [(sample.labels, sample.value) for family in text_string_to_metric_families(payload)
            for sample in family.samples if sample.name == f"reliability_{metric}"]


@pytest.fixture
def monitored(tmp_path, monkeypatch):
    monkeypatch.setenv("MAINTENANCE_MODE", "simulation")
    monkeypatch.setenv("AIOPS_OPERATOR_TOKENS", json.dumps({"private-operator": "x" * 32}))
    monkeypatch.setenv("OBS_SLO_API", "0.97")
    monkeypatch.delenv("OBS_OTLP_TRACES_ENDPOINT", raising=False)
    app = create_app(tmp_path / "metrics.sqlite3")
    exporter = InMemorySpanExporter()
    app.state.telemetry.provider.add_span_processor(SimpleSpanProcessor(exporter))
    stream = StringIO()
    logger = logging.getLogger("maintenance")
    handler = logging.StreamHandler(stream)
    handler.setFormatter(JsonFormatter())
    logger.addHandler(handler)
    old_level = logger.level
    logger.setLevel(logging.INFO)
    try:
        with TestClient(app) as api:
            yield api, exporter, stream, tmp_path
    finally:
        logger.removeHandler(handler)
        logger.setLevel(old_level)


def test_http_metrics_use_templates_and_exclude_scrapes(monitored):
    api, _, _, _ = monitored
    assert api.get("/cluster").status_code == 200
    for suffix in ("random-job", "another-job"):
        assert api.get(f"/maintenance/jobs/{suffix}").status_code == 404
    assert api.get("/unknown-private-path").status_code == 404
    output = api.get("/metrics")
    assert "text/plain" in output.headers["Content-Type"]
    requests = values(output.text, "http_requests_total")
    assert ({"mode": "simulation", "route": "/maintenance/jobs/{job_id}", "method": "GET", "status_class": "4xx"}, 2) in requests
    assert {labels["route"] for labels, _ in requests} == {"/cluster", "/maintenance/jobs/{job_id}", "unmatched"}
    assert any(labels.get("le") == "+Inf" for labels, _ in values(output.text, "http_request_duration_seconds_bucket"))
    assert values(output.text, "slo_target_ratio")[0] == ({"mode": "simulation", "slo": "api"}, 0.97)


def test_durable_metrics_and_simulated_evictions(monitored):
    api, exporter, _, _ = monitored
    plan = api.post("/maintenance/plans", json={"node": "cedar"}).json()
    job = api.post("/maintenance/jobs", json={"kind": "single", "plan_id": plan["id"]}).json()
    deadline = monotonic() + 5
    while monotonic() < deadline:
        if api.get(f"/maintenance/jobs/{job['id']}").json()["state"] == "succeeded":
            break
        sleep(0.05)
    metrics = api.get("/metrics").text
    assert ({"mode": "simulation", "workflow": "single", "state": "succeeded"}, 1) in values(metrics, "jobs")
    assert ({"mode": "simulation", "workflow": "single", "outcome": "succeeded"}, 1) in values(metrics, "job_duration_seconds_count")
    assert ({"mode": "simulation", "outcome": "ok"}, 2) in values(metrics, "eviction_attempts_total")
    assert values(metrics, "workers_healthy") == [({"mode": "simulation"}, 1)]
    assert any(span.name == "maintenance.worker.step" and span.attributes["operation_id"] == job["id"]
               for span in exporter.get_finished_spans())


def test_provider_and_api_spans_correlate_with_json_logs(monitored):
    api, exporter, logs, _ = monitored
    incident = api.post("/aiops/demo").json()["incidents"][0]
    traceparent = "00-99aabbccddeeff001122334455667788-1234567890123456-01"
    response = api.post(f"/aiops/incidents/{incident}/recommendations", headers={"traceparent": traceparent})
    assert response.status_code == 200
    spans = exporter.get_finished_spans()
    provider = next(item for item in spans if item.name == "aiops.provider")
    recommendation = next(item for item in spans if item.name == "aiops.recommend")
    assert provider.parent.span_id == recommendation.context.span_id
    assert recommendation.attributes["incident_id"] == incident
    assert format(provider.context.trace_id, "032x") == traceparent.split("-")[1]
    records = [json.loads(line) for line in logs.getvalue().splitlines()]
    record = next(item for item in records if item["event"] == "recommendation.generated")
    assert record["trace_id"] == traceparent.split("-")[1] and record["incident_id"] == incident
    assert ({"mode": "simulation", "provider": "mock", "outcome": "ok"}, 1) in values(
        api.get("/metrics").text, "ai_attempt_duration_seconds_count")


def test_credentials_and_unbounded_ids_are_absent_from_metrics_spans_and_logs(monitored):
    api, exporter, logs, _ = monitored
    secret = "raw-AUTH-credential-and-private-path-1701"
    api.post(f"/aiops/incidents/{secret}/recommendations?token={secret}", headers={
        "Authorization": f"Bearer {secret}", "tracestate": f"test={secret}",
    })
    path = f"/aiops/recommendations/{secret}/approve"
    api.post(path, headers={"Authorization": f"Bearer {secret}"}, json={"approved": True, "nonce": "0" * 32})
    payload = api.get("/metrics").text
    assert secret not in payload and "private-operator" not in payload
    assert secret not in logs.getvalue()
    assert secret not in " ".join(item.to_json() for item in exporter.get_finished_spans())
    assert ({"mode": "simulation", "reason": "auth", "workflow": "aiops"}, 1) in values(
        payload, "approval_rejections_total")


@pytest.mark.parametrize(("ready", "healthy", "expected", "present"), [(True, 2, 0, False), (False, 1, 1, True)])
def test_live_measurements_separate_usage_requests_and_budget_violations(ready, healthy, expected, present):
    node = "k8s-reliability-lab-worker"
    inventory = {"nodes": [{
        "metadata": {"name": node}, "status": {"conditions": [{"type": "Ready", "status": str(ready)}],
                                              "allocatable": {"cpu": "2", "memory": "8Gi", "pods": "100"}},
    }], "pods": [], "deployments": [], "pdbs": [{
        "metadata": {"namespace": "reliability-lab", "generation": 1},
        "status": {"observedGeneration": 1, "expectedPods": 2, "currentHealthy": healthy,
                   "desiredHealthy": 2, "disruptionsAllowed": 0},
    }]}
    rows = {(key, tags): value for key, _, _, tags, value in live_measurements(
        inventory, {node: {"cpu": "1", "memory": "4Gi"}} if present else None)}
    assert rows["cluster_nodes", ("worker", "unready")] == int(not ready)
    assert rows["pdb_violations", ()] == expected
    assert rows["pdb_zero_slack", ()] == 1
    assert rows["cluster_resource_ratio", ("cpu", "requests")] == 0
    assert rows["runtime_metrics_available", ()] == int(present)
    if present:
        assert rows["cluster_resource_ratio", ("cpu", "usage")] == 0.5
    else:
        assert ("cluster_resource_ratio", ("cpu", "usage")) not in rows


@pytest.mark.parametrize(("name", "input_value"), [
    ("OBS_SLO_API", "0"), ("OBS_SLO_MAINTENANCE", "1"), ("OBS_SLO_RECOVERY", "nan"),
    ("OBS_RECOVERY_DEADLINE_SECONDS", "-1"),
])
def test_slo_config_rejects_invalid_targets(tmp_path, monkeypatch, name, input_value):
    monkeypatch.setenv(name, input_value)
    with pytest.raises(ValueError, match="objectives"):
        create_app(tmp_path / "slo.sqlite3")


def test_unhandled_server_error_increments_5xx_without_exporting_message(monitored):
    api, exporter, logs, _ = monitored

    @api.app.get("/controlled-error")
    def controlled_error():
        raise RuntimeError("sensitive internal error from test fixture")

    with pytest.raises(RuntimeError, match="sensitive internal"):
        api.get("/controlled-error")
    assert ({"mode": "simulation", "route": "/controlled-error", "method": "GET", "status_class": "5xx"}, 1) in values(
        api.get("/metrics").text, "http_requests_total")
    operation = next(item for item in exporter.get_finished_spans() if item.name == "GET /controlled-error")
    assert operation.status.status_code.name == "ERROR" and not operation.events
    assert "sensitive internal" not in logs.getvalue()
