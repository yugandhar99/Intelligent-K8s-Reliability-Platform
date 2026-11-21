"""Generate real simulator activity and optionally request an approved lab drain."""

import argparse
from concurrent.futures import ThreadPoolExecutor
import json
import os
from pathlib import Path
from time import monotonic, sleep, time
from urllib.request import Request, urlopen
from urllib.error import HTTPError

SIM = "http://127.0.0.1:8000"
LIVE = "http://127.0.0.1:8001"


def api(base, path, method="GET", body=None, *, token=None, expected=200):
    headers = {"Content-Type": "application/json"}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    try:
        with urlopen(Request(base + path, data=json.dumps(body).encode() if body is not None else None,
                             headers=headers, method=method), timeout=12) as response:
            status, payload = response.status, response.read()
    except HTTPError as error:
        status, payload = error.code, error.read()
    if status != expected:
        raise RuntimeError(f"{method} {path} returned HTTP {status}, expected {expected}")
    return json.loads(payload)


def await_job(base, job):
    deadline = monotonic() + 90
    while monotonic() < deadline:
        details = api(base, f"/maintenance/jobs/{job}")
        if details["state"] in {"succeeded", "failed", "rejected", "cancelled"}:
            return details
        sleep(0.3)
    raise RuntimeError("Job did not reach a terminal state")


def activity():
    api(SIM, "/aiops/demo", "POST")
    for incident in api(SIM, "/aiops/incidents"):
        rec = api(SIM, f"/aiops/incidents/{incident['id']}/recommendations", "POST")
        token = os.environ["OBS_OPERATOR_TOKEN"]
        if action := rec["action"]:
            path = f"/aiops/recommendations/{rec['id']}/approve"
            approved = {"nonce": action["nonce"], "approved": True}
            api(SIM, path, "POST", approved, expected=401)
            job = api(SIM, path, "POST", approved, token=token, expected=202)["job"]["id"]
            print("Simulation approved job", await_job(SIM, job)["state"], flush=True)
            api(SIM, path, "POST", approved, token=token, expected=409)
        # Keep open and resolved examples in Grafana.
        if incident["subject"] in {"cedar", "catalog-demo"}:
            api(SIM, f"/aiops/incidents/{incident['id']}/resolve", "POST",
                {"disposition": "mitigated"}, token=token)


def expired_lease(database: Path):
    from app.jobs import Job
    from app.models import Plan
    from app.store import Store
    from uuid import uuid4

    # Fault injection is scoped to an explicitly supplied simulation demo database.
    store = Store(database)
    with store.transaction(write=True) as db:
        cluster = store.snapshot(db)
        plan = Plan(id=uuid4().hex, node="cedar", cluster_revision=cluster.revision, safe=True, placements=[], problems=[])
        job = Job(id=uuid4().hex, kind="single", plan_id=plan.id, nodes=[plan.node], state="running")
        db.execute("INSERT INTO plans VALUES (?,?)", (plan.id, plan.model_dump_json()))
        db.execute("INSERT INTO jobs(id,kind,plan_id,payload,worker,expiry,fence) VALUES (?,?,?,?,?,?,?)",
                   (job.id, job.kind, job.plan_id, job.model_dump_json(), "demo-interrupted-worker", time() - 0.1, 1))
        db.execute("INSERT INTO job_owner VALUES (1,?)", (job.id,))
        db.execute("INSERT INTO job_history(job_id,timestamp,event,details) VALUES (?,?,?,?)",
                   (job.id, time() - 1, "job.queued", "{}"))
    result = await_job(SIM, job.id)
    recovered = [row["event"] for row in api(SIM, f"/maintenance/jobs/{job.id}/history")]
    if "job.recovered" not in recovered:
        raise RuntimeError("Expected recovery evidence from the running worker")
    print("Simulation expired lease reconciled", result["state"], flush=True)


def live_drain(base=LIVE):
    cluster = api(base, "/kubernetes/cluster")
    workers = [node["metadata"]["name"] for node in cluster["nodes"]
               if node["metadata"]["name"].startswith("k8s-reliability-lab-worker")]
    for node in workers:
        plan = api(base, "/kubernetes/maintenance/plans", "POST", {"node": node}, expected=201)
        pods = plan["pods"]
        if pods and not plan["problems"] and all(pod["disruptions_allowed"] > 0 for pod in pods):
            job = api(base, "/kubernetes/maintenance/execute", "POST",
                      {"plan_id": plan["id"], "approver": "observability-demo", "approved": True},
                      token=os.environ["OBS_LAB_TOKEN"], expected=202)["id"]
            start = monotonic()
            result = await_job(base, job)
            if result["state"] != "succeeded":
                raise RuntimeError("The approved Kind demonstration drain did not succeed")
            print(f"Kind approved drain: {len(pods)} pods, completed in {monotonic() - start:.2f}s", flush=True)
            return
    raise RuntimeError("No eligible demo worker contains evictable pods")


def traffic(base, seconds):
    end = monotonic() + seconds
    while monotonic() < end:
        api(base, "/health")
        api(base, "/aiops/incidents")
        api(base, "/maintenance/jobs")
        sleep(1.2)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("command", choices=["simulation", "traffic", "live-drain"])
    parser.add_argument("--database", type=Path)
    parser.add_argument("--seconds", type=int, default=180)
    parser.add_argument("--approve-live", action="store_true")
    args = parser.parse_args()
    if args.command == "simulation":
        activity()
        if args.database:
            expired_lease(args.database)
    elif args.command == "traffic":
        with ThreadPoolExecutor(max_workers=2) as pool:
            for future in (pool.submit(traffic, SIM, args.seconds), pool.submit(traffic, LIVE, args.seconds)):
                future.result()
    elif args.approve_live:
        live_drain()
    else:
        parser.error("Pass --approve-live after reviewing the dedicated lab operation")


if __name__ == "__main__":
    main()
