"""Bounded loopback API benchmark for an isolated simulation database."""

import argparse
import asyncio
import json
from collections import Counter
from math import ceil
from time import perf_counter
from urllib.parse import urlsplit

import httpx


async def burst(client, count, method, path, expected, payload=None):
    gate = asyncio.Event()

    async def call():
        await gate.wait()
        started = perf_counter()
        try:
            response = await client.request(method, path, json=payload)
            status = str(response.status_code)
        except httpx.RequestError:
            response, status = None, "transport_error"
        return perf_counter() - started, status, response

    tasks = [asyncio.create_task(call()) for _ in range(count)]
    await asyncio.sleep(0)
    started = perf_counter()
    gate.set()
    rows = await asyncio.gather(*tasks)
    elapsed = perf_counter() - started
    latencies = sorted(duration * 1000 for duration, _, _ in rows)
    statuses = Counter(status for _, status, _ in rows)
    result = {
        "concurrent_requests": count,
        "elapsed_seconds": round(elapsed, 4),
        "requests_per_second": round(count / elapsed, 2),
        "successful_requests_per_second": round(statuses[str(expected)] / elapsed, 2),
        "p50_ms": round(latencies[ceil(count * 0.50) - 1], 2),
        "p95_ms": round(latencies[ceil(count * 0.95) - 1], 2),
        "p99_ms": round(latencies[ceil(count * 0.99) - 1], 2),
        "error_rate": (count - statuses[str(expected)]) / count,
        "responses": dict(statuses),
    }
    return result, [response for _, _, response in rows if response is not None]


async def run(base, count, submissions):
    url = urlsplit(base)
    if (
        url.scheme != "http"
        or url.hostname != "127.0.0.1"
        or url.path
        or url.query
        or url.fragment
        or url.username
        or url.password
    ):
        raise ValueError("Use an explicit HTTP loopback API base URL")
    async with httpx.AsyncClient(
        base_url=base,
        timeout=8,
        trust_env=False,
        limits=httpx.Limits(max_connections=count, max_keepalive_connections=count),
    ) as client:
        ready = (await client.get("/ready")).raise_for_status().json()
        if ready != {"status": "ready", "mode": "simulation"}:
            raise RuntimeError(
                "Use a healthy simulation instance with an isolated database"
            )
        for _ in range(10):
            (await client.get("/cluster")).raise_for_status()

        reads, _ = await burst(client, count, "GET", "/cluster", 200)
        result = {"cluster_reads": reads}
        if submissions:
            plan = (
                (await client.post("/maintenance/plans", json={"node": "cedar"}))
                .raise_for_status()
                .json()
            )
            if not plan["safe"]:
                raise RuntimeError("The Cedar simulation plan is unsafe")
            payload = {"kind": "single", "plan_id": plan["id"]}
            writes, responses = await burst(
                client, submissions, "POST", "/maintenance/jobs", 202, payload
            )
            ids = {
                response.json()["id"]
                for response in responses
                if response.status_code == 202
            }
            rows = (
                (await client.get("/maintenance/jobs", params={"limit": 100}))
                .raise_for_status()
                .json()["jobs"]
            )
            linked = [job for job in rows if job["plan_id"] == plan["id"]]
            writes.update(unique_job_ids=len(ids), durable_rows=len(linked))
            if len(ids) == len(linked) == 1:
                job_id = ids.pop()
                for _ in range(80):
                    job = (
                        (await client.get(f"/maintenance/jobs/{job_id}"))
                        .raise_for_status()
                        .json()
                    )
                    if job["state"] in {"succeeded", "failed", "rejected", "cancelled"}:
                        break
                    await asyncio.sleep(0.1)
                writes["job_outcome"] = job["state"]
            result["duplicate_submissions"] = writes

    print(json.dumps(result, indent=2), flush=True)
    if reads["error_rate"] or (
        submissions
        and (writes["error_rate"] or writes.get("job_outcome") != "succeeded")
    ):
        raise SystemExit(1)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base", default="http://127.0.0.1:18000")
    parser.add_argument(
        "--requests", type=int, choices=range(1, 101), default=100, metavar="1..100"
    )
    parser.add_argument(
        "--submit", type=int, choices=range(21), default=0, metavar="0..20"
    )
    options = parser.parse_args()
    if options.submit > options.requests:
        parser.error("--submit cannot exceed --requests")
    asyncio.run(run(options.base, options.requests, options.submit))
