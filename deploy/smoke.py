"""Exercise readiness, a durable simulation job, metrics, and restart persistence."""

import argparse
import json
from time import monotonic, sleep
from urllib.request import Request, urlopen


def probe(base: str, *, existing_job: str = "", api_slo: str = "") -> dict:
    start = monotonic()

    def get(path: str, payload=None):
        request = Request(
            base + path,
            data=json.dumps(payload).encode() if payload is not None else None,
            headers={"Content-Type": "application/json"},
        )
        with urlopen(request, timeout=5) as response:
            return response.read()

    assert json.loads(get("/ready")) == {"status": "ready", "mode": "simulation"}
    if existing_job:
        job_id = existing_job
    else:
        plan = json.loads(get("/maintenance/plans", {"node": "cedar"}))
        assert plan["safe"], plan
        job = json.loads(
            get("/maintenance/jobs", {"kind": "single", "plan_id": plan["id"]})
        )
        job_id = job["id"]

    for _ in range(60):
        job = json.loads(get(f"/maintenance/jobs/{job_id}"))
        if job["state"] in {"succeeded", "failed", "rejected", "cancelled"}:
            break
        sleep(0.2)
    assert job["state"] == "succeeded", job
    metrics = get("/metrics").decode()
    assert (
        "reliability_jobs{" in metrics
        and "reliability_worker_heartbeats_total{" in metrics
    )
    if api_slo:
        assert (
            f'reliability_slo_target_ratio{{mode="simulation",slo="api"}} {api_slo}'
            in metrics
        )
    return {
        "job": job_id,
        "state": job["state"],
        "seconds": round(monotonic() - start, 2),
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--base", default="http://127.0.0.1:8000")
    parser.add_argument("--existing-job", default="")
    parser.add_argument("--api-slo", default="")
    args = parser.parse_args()
    print(
        json.dumps(
            probe(args.base, existing_job=args.existing_job, api_slo=args.api_slo)
        )
    )
