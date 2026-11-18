"""Opt-in integration tests using the dedicated, pre-provisioned Kind lab."""

from base64 import b64decode
from concurrent.futures import ThreadPoolExecutor
from hashlib import sha256
import json
import os
from pathlib import Path
import subprocess
import tomllib
from time import monotonic, sleep, time

from fastapi.testclient import TestClient
import pytest
import yaml

from app.batch_models import BatchPolicy
from app.batch_service import BatchService
from app.jobs import JobService, TERMINAL
from app.kube_adapter import LAB, NAMESPACE, OWNER, WORKERS, KubernetesAdapter
from app.kube_jobs import LiveMaintenance
from app.kube_planning import Inventory
from app.main import create_app
from app.models import PolicySettings
from app.policy import MaintenancePolicy
from app.service import MaintenanceService
from app.store import MaintenanceConflict, Store

pytestmark = pytest.mark.skipif(os.getenv("RUN_KIND_TESTS") != "1", reason="requires explicit Kind lab opt-in")
TOKEN = "dedicated-integration-test-approval-token"


@pytest.fixture
def lab():
    path = Path("data")
    identity = path / "kind-identity.json"
    credentials = path / "kind-live.yaml"
    kube = KubernetesAdapter(credentials, identity)
    [cluster] = yaml.safe_load((path / "kind-admin.yaml").read_text())["clusters"]
    assert cluster["name"] == f"kind-{LAB}"
    endpoint = cluster["cluster"]
    assert endpoint["server"] == kube.pin["server"]
    assert sha256(b64decode(endpoint["certificate-authority-data"])).hexdigest() == kube.pin["ca_sha256"]

    def admin(*args):
        # The same TLS pin and namespace UID gate the fixture's lab-only mutations.
        kube.validate_identity()
        return subprocess.check_output([
            "kubectl", "--kubeconfig", str(path / "kind-admin.yaml"), "--context", f"kind-{LAB}",
            "--request-timeout=4s", "-n", NAMESPACE, *args,
        ], text=True, timeout=70)

    try:
        for name in ("web", "api"):
            admin("rollout", "status", f"deployment/{name}", "--timeout=60s")
        yield kube, admin
    finally:
        kube.close()


def select(kube, workload=None):
    snapshot = kube.discover()
    inventory = Inventory(snapshot)
    plans = [inventory.plan(node) for node in sorted(WORKERS)]
    return next(plan for plan in plans if not plan["problems"] and plan["pods"] and (
        workload is None or any(item["pdb"] == workload for item in plan["pods"])
    ))


def assert_restored(kube, target):
    node = kube.node(target)
    assert not node["spec"].get("unschedulable", False)
    assert OWNER not in node["metadata"].get("annotations", {})


def services(kube, db, clock=time):
    store = Store(db)
    config = tomllib.loads(Path("app/config.toml").read_text())
    policy = MaintenancePolicy(PolicySettings.model_validate(config["maintenance"]))
    single = MaintenanceService(store, policy)
    batch = BatchService(store, policy, BatchPolicy.model_validate(config["batch"]))
    jobs = JobService(single, batch, clock=clock)
    store.initialize()
    batch.initialize()
    jobs.initialize()
    jobs.live = LiveMaintenance(jobs, kube)
    jobs.live.initialize()
    return jobs


def drive(jobs, job_id, limit=60):
    deadline = monotonic() + limit
    while monotonic() < deadline:
        job = jobs.status(job_id)
        if job.state in TERMINAL:
            return job
        if lease := jobs.claim("recovery-worker"):
            jobs.run(lease)
        sleep(0.15)
    pytest.fail(f"Job did not finish: {jobs.status(job_id)}")


def test_successful_api_drain_monitors_readiness_and_http(lab, tmp_path, monkeypatch):
    kube, admin = lab
    monkeypatch.setenv("MAINTENANCE_MODE", "kubernetes")
    monkeypatch.setenv("LAB_KUBECONFIG", "data/kind-live.yaml")
    monkeypatch.setenv("LAB_IDENTITY", "data/kind-identity.json")
    monkeypatch.setenv("LAB_APPROVAL_TOKEN", TOKEN)
    target = select(kube)["node"]
    with TestClient(create_app(tmp_path / "http.sqlite3")) as api:
        assert api.get("/health").json()["mode"] == "kubernetes"
        inventory = api.get("/kubernetes/cluster").json()
        assert {node["metadata"]["name"] for node in inventory["nodes"]} == WORKERS | {f"{LAB}-control-plane"}
        assert inventory["namespaces"] and inventory["pods"] and inventory["deployments"] and inventory["pdbs"]
        plan = api.post("/kubernetes/maintenance/plans", json={"node": target}).json()
        assert plan["pods"] and not plan["problems"]
        assert_restored(kube, target)
        request = {"plan_id": plan["id"], "approver": "kind-test", "approved": True}
        assert api.post("/kubernetes/maintenance/execute", json=request).status_code == 403
        headers = {"Authorization": f"Bearer {TOKEN}"}
        assert api.post("/kubernetes/maintenance/execute", json={**request, "approved": 1},
                        headers=headers).status_code == 422
        assert api.post("/maintenance/jobs", json={"kind": "kubernetes", "plan_id": plan["id"]}).status_code == 409
        started = monotonic()
        response = api.post("/kubernetes/maintenance/execute", json=request, headers=headers)
        assert response.status_code == 202, response.text
        job_id = response.json()["id"]
        counts = {"web": [], "api": []}
        successes = dict.fromkeys(counts, 0)
        observed_cordon = False
        while monotonic() - started < 60:
            snapshot = kube.discover()
            sampled = Inventory(snapshot)
            observed_cordon |= sampled.nodes[target]["spec"].get("unschedulable", False)
            for name in counts:
                deployment = next(dep for dep in sampled.deployments.values() if dep["metadata"]["name"] == name)
                counts[name].append(len(sampled.ready_pods(deployment["metadata"]["uid"])))
                try:
                    body = admin("get", "--raw", f"/api/v1/namespaces/{NAMESPACE}/services/http:{name}:80/proxy/")
                    successes[name] += "Welcome to nginx" in body
                except subprocess.SubprocessError:
                    pass
            job = api.get(f"/maintenance/jobs/{job_id}").json()
            if job["state"] in TERMINAL:
                break
            sleep(0.2)
        history = api.get(f"/maintenance/jobs/{job_id}/history").json()
        elapsed = monotonic() - started
        if job["state"] == "succeeded":
            completed = next(event["timestamp"] for event in history if event["event"] == "job.succeeded")
            elapsed = completed - history[0]["timestamp"]
        print(f"drain {target}: {len(plan['pods'])} pods in {elapsed:.2f}s; "
              f"minimum Ready replicas { {name: min(values) for name, values in counts.items()} }; "
              f"HTTP success/attempts { {name: (successes[name], len(values)) for name, values in counts.items()} }")
        assert job["state"] == "succeeded", (job, history)
        assert observed_cordon
        assert all(min(values) >= 2 and successes[name] == len(values) for name, values in counts.items())
        assert sum(event["event"] == "live.replacement_ready" for event in history) == len(plan["pods"])
        assert_restored(kube, target)
        assert not Inventory(kube.discover()).residents(target)
        events = json.loads(admin("get", "events", "-o", "json"))["items"]
        assert {"NodeCordoned", "DrainVerified", "SchedulingRestored"} <= {
            event["reason"] for event in events if job_id in event.get("message", "")
        }


def test_restrictive_pdb_blocks_eviction(lab, tmp_path):
    kube, admin = lab
    admin("scale", "deployment/blocked", "--replicas=3")
    try:
        admin("rollout", "status", "deployment/blocked", "--timeout=60s")
        target = select(kube, "blocked")["node"]
        jobs = services(kube, tmp_path / "blocked.sqlite3")
        plan = jobs.live.plan(target)
        blocked = next(item for item in plan["pods"] if item["pdb"] == "blocked")
        assert blocked["disruptions_allowed"] == 0
        start = monotonic()
        job = jobs.live.approve(plan["id"], "kind-test")
        result = drive(jobs, job.id)
        print(f"restrictive PDB: {monotonic() - start:.2f}s, {result.state}, {result.problem}")
        assert result.state == "failed" and result.problem.code == "eviction_blocked"
        assert any(pod["metadata"]["uid"] == blocked["uid"] and not pod["metadata"].get("deletionTimestamp")
                   for pod in kube.discover()["pods"])
        assert_restored(kube, target)
    finally:
        admin("scale", "deployment/blocked", "--replicas=0")
        for _ in range(50):
            if not json.loads(admin("get", "pods", "-l", "app=blocked", "-o", "json"))["items"]:
                break
            sleep(0.2)
        else:
            pytest.fail("Blocked demo pods did not finish terminating")


def test_capacity_and_concurrent_approval(lab, tmp_path):
    kube, admin = lab
    target = select(kube)["node"]
    others = sorted(WORKERS - {target})
    cordoned = []
    try:
        for node in others:
            assert_restored(kube, node)
            admin("cordon", node)
            cordoned.append(node)
        assert any("Insufficient" in problem for problem in Inventory(kube.discover()).plan(target)["problems"])
    finally:
        for node in cordoned:
            admin("uncordon", node)

    jobs = services(kube, tmp_path / "concurrent.sqlite3")
    first, second = jobs.live.plan(target), jobs.live.plan(target)
    def approve(plan):
        try:
            return jobs.live.approve(plan["id"], "kind-test")
        except MaintenanceConflict as error:
            assert "active" in str(error)
            return None

    with ThreadPoolExecutor(max_workers=2) as pool:
        accepted, = [job for job in pool.map(approve, (first, second)) if job]
    assert jobs.live.approve(accepted.plan_id, "kind-test").id == accepted.id
    lease = jobs.claim("first")
    assert lease and jobs.claim("second") is None
    assert jobs.run(lease)
    assert drive(jobs, accepted.id).state == "succeeded"
    assert_restored(kube, target)


def test_stale_ownership_does_not_uncordon(lab, tmp_path):
    kube, admin = lab
    target = select(kube)["node"]
    jobs = services(kube, tmp_path / "ownership.sqlite3")
    plan = jobs.live.plan(target)
    job = jobs.live.approve(plan["id"], "kind-test")
    assert jobs.run(jobs.claim("first"))
    assert jobs.run(jobs.claim("first"))
    admin("annotate", "node", target, f"{OWNER}=external-test-owner", "--overwrite")
    try:
        result = drive(jobs, job.id)
        node = kube.node(target)
        assert result.state == "failed" and result.problem.code == "ownership_or_identity_changed"
        assert node["spec"]["unschedulable"] and node["metadata"]["annotations"][OWNER] == "external-test-owner"
    finally:
        admin("annotate", "node", target, f"{OWNER}-")
        admin("uncordon", target)


def test_interruption_after_eviction_recovers_from_api_state(lab, tmp_path, monkeypatch):
    kube, _ = lab
    target = select(kube)["node"]
    offset = 0
    clock = lambda: time() + offset
    db = tmp_path / "interrupted.sqlite3"
    jobs = services(kube, db, clock)
    job = jobs.live.approve(jobs.live.plan(target)["id"], "kind-test")
    for _ in range(3):
        assert jobs.run(jobs.claim("interrupted-worker"))
    lease = jobs.claim("interrupted-worker")
    original = kube.evict

    def interrupted(pod):
        original(pod)
        raise BaseException("worker died before committing accepted eviction")

    with monkeypatch.context() as patch:
        patch.setattr(kube, "evict", interrupted)
        with pytest.raises(BaseException, match="worker died"):
            jobs.run(lease)
    offset = 3
    recovered = services(kube, db, clock)
    assert not recovered.run(lease)
    recovery_started = monotonic()
    result = drive(recovered, job.id)
    print(f"interrupted eviction: {monotonic() - recovery_started:.2f}s after restart, {result.state}")
    assert result.state == "succeeded"
    history = recovered.history(job.id)
    assert any(event.event == "job.recovered" for event in history)
    first_intent = next(event.details["pod_uid"] for event in history if event.event == "live.eviction_intent")
    assert all(event.details["pod_uid"] != first_intent for event in history if event.event == "live.eviction_accepted")
    assert_restored(kube, target)
