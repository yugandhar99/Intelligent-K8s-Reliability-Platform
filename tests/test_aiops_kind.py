"""Exercise real Event collection and approved remediation on the pinned Kind lab."""

import json
import os
from time import monotonic, sleep

from fastapi.testclient import TestClient
import pytest

from app.jobs import TERMINAL
from app.main import create_app
from test_kubernetes_live import assert_restored, lab, select

pytestmark = pytest.mark.skipif(os.getenv("RUN_KIND_TESTS") != "1", reason="requires explicit Kind lab opt-in")


def until(read, ready, timeout=60):
    end = monotonic() + timeout
    while monotonic() < end:
        result = read()
        if ready(result):
            return result
        sleep(0.3)
    pytest.fail(f"Observed state did not reach target: {result}")


def test_kind_detects_real_incidents_and_routes_approved_retry_through_live_safety(lab, tmp_path, monkeypatch):
    kube, admin = lab
    monkeypatch.setenv("MAINTENANCE_MODE", "kubernetes")
    monkeypatch.setenv("LAB_KUBECONFIG", "data/kind-live.yaml")
    monkeypatch.setenv("LAB_IDENTITY", "data/kind-identity.json")
    monkeypatch.setenv("LAB_APPROVAL_TOKEN", "stage4-test-approval-token-only-for-kind")
    monkeypatch.setenv("AIOPS_OPERATOR_TOKENS", json.dumps({"kind-operator": "stage5-test-operator-token-for-local-kind"}))
    monkeypatch.setenv("AIOPS_PROVIDER", "mock")
    monkeypatch.setenv("AIOPS_COLLECTION_SECONDS", "0")
    auth = {"Authorization": "Bearer stage5-test-operator-token-for-local-kind"}
    original = "kubernetes/aiops-demo.yaml"
    with TestClient(create_app(tmp_path / "aiops-kind.sqlite3")) as api:
        def scan():
            response = api.post("/aiops/collect")
            assert response.status_code == 200, response.text
            return api.get("/aiops/incidents?limit=100").json()

        def recommend(incident):
            response = api.post(f"/aiops/incidents/{incident['id']}/recommendations")
            assert response.status_code == 200, response.text
            return response.json()

        def job_result(job_id):
            return until(lambda: api.get(f"/maintenance/jobs/{job_id}").json(), lambda job: job["state"] in TERMINAL)

        assert api.post("/aiops/demo").status_code == 409
        started = monotonic()
        admin("apply", "-f", original)
        try:
            found = until(scan, lambda rows: any(
                row["subject"] == "aiops-crash-demo" and any(
                    item["source"] == "event" and item["signal"] == "crash" for item in row["evidence"]
                ) for row in rows
            ) and any(row["subject"] == "aiops-pending-demo" and "scheduling" in row["signals"]
                      and {"pending", "cpu_fit"} <= set(row["signals"]) for row in rows))
            for subject, expected in (("aiops-crash-demo", "inspect_crash"), ("aiops-pending-demo", "inspect_scheduling")):
                case = next(item for item in found if item["subject"] == subject)
                rec = recommend(case)
                assert rec["provider"] == "mock" and rec["action"] is None
                assert expected in {entry["id"] for entry in rec["runbooks"]}
            print(f"Kind CrashLoopBackOff and FailedScheduling correlated in {monotonic() - started:.2f}s")
        finally:
            admin("delete", "-f", original, "--ignore-not-found=true", "--wait=true", "--timeout=40s")
            until(lambda: kube.discover()["pods"],
                  lambda pods: not any(pod["metadata"]["name"].startswith(("aiops-crash-demo-", "aiops-pending-demo-"))
                                       for pod in pods))

        admin("scale", "deployment/blocked", "--replicas=3")
        try:
            admin("rollout", "status", "deployment/blocked", "--timeout=60s")
            target = select(kube, "blocked")["node"]
            plan = api.post("/kubernetes/maintenance/plans", json={"node": target}).json()
            assert any(item["pdb"] == "blocked" and item["disruptions_allowed"] == 0 for item in plan["pods"])
            approved = api.post("/kubernetes/maintenance/execute",
                                headers={"Authorization": "Bearer stage4-test-approval-token-only-for-kind"},
                                json={"plan_id": plan["id"], "approver": "fixture", "approved": True})
            assert approved.status_code == 202, approved.text
            failed = job_result(approved.json()["id"])
            assert failed["state"] == "failed" and failed["problem"]["code"] == "eviction_blocked"
            found = scan()
            assert any(row["subject"] == "blocked" and "pdb" in row["signals"] for row in found)
            incident = next(row for row in found if row["subject"] == target and "eviction_failed" in row["signals"])
            assert set(incident["signals"]) == {"eviction_failed", "pdb"}
            blocked = recommend(incident)
            assert blocked["risk"]["blocked"] and blocked["action"] is None
            rejected = api.post(f"/aiops/recommendations/{blocked['id']}/approve",
                                headers=auth, json={"nonce": "0" * 32, "approved": True})
            assert rejected.status_code == 409

            admin("patch", "pdb", "blocked", "--type=merge", "-p", '{"spec":{"minAvailable":2}}')
            allowed = until(lambda: recommend(incident), lambda result: result["action"] is not None)
            admin("patch", "pdb", "blocked", "--type=merge", "-p", '{"spec":{"minAvailable":3}}')
            stale = api.post(f"/aiops/recommendations/{allowed['id']}/approve", headers=auth,
                             json={"nonce": allowed["action"]["nonce"], "approved": True})
            assert stale.status_code == 409, stale.text
            assert_restored(kube, target)
            admin("patch", "pdb", "blocked", "--type=merge", "-p", '{"spec":{"minAvailable":2}}')
            allowed = until(lambda: recommend(incident), lambda result: result["action"] is not None)
            assert allowed["action"]["node"] == target and allowed["action"]["mode"] == "kubernetes"
            assert not allowed["risk"]["blocked"]
            started = monotonic()
            response = api.post(f"/aiops/recommendations/{allowed['id']}/approve", headers=auth,
                                json={"nonce": allowed["action"]["nonce"], "approved": True})
            assert response.status_code == 202, response.text
            assert response.json()["approved_by"] == "kind-operator"
            result = job_result(response.json()["job"]["id"])
            print(f"Kind AIOps approved retry {target}: {result['state']} in {monotonic() - started:.2f}s")
            assert result["state"] == "succeeded", result
            assert_restored(kube, target)
            status = api.get(f"/aiops/remediations/{allowed['id']}").json()
            assert {"live.approved", "live.node_cordoned", "live.eviction_accepted", "live.replacement_ready",
                    "live.scheduling_restored", "job.succeeded"} <= {row["event"] for row in status["history"]}
            assert all(row["details"].get("approver") == "kind-operator" for row in status["history"]
                       if row["event"] == "live.approved")
            url = f"/aiops/incidents/{incident['id']}"
            assert api.post(f"{url}/resolve", json={"disposition": "mitigated"}, headers=auth).status_code == 200
            assert api.get(f"{url}/report").json()["remediations"][0]["outcome"] == "succeeded"
        finally:
            admin("patch", "pdb", "blocked", "--type=merge", "-p", '{"spec":{"minAvailable":3}}')
            admin("scale", "deployment/blocked", "--replicas=0")
            until(lambda: kube.discover()["pods"],
                  lambda pods: not any(pod["metadata"]["name"].startswith("blocked-") for pod in pods))
