# Final performance and reliability validation

Measured on November 18, 2026, against the local Docker Compose simulator and dedicated four-node Kind lab. The load test reused the validated application image on loopback. The full regression suite ran from the v1.0.0 source using the pinned Python 3.12 dependency lock.

## Bounded HTTP bursts

| Endpoint | Concurrent requests | Elapsed | Requests/s | P50 | P95 | P99 | Error rate |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `GET /cluster` | 100 | 350.8 ms | 285.05 | 266.10 ms | 320.96 ms | 327.31 ms | 0/100 (0%) |
| `POST /maintenance/jobs` | 20 | 219.2 ms | 91.26 | 81.83 ms | 182.87 ms | 208.40 ms | 0/20 (0%) |

The 100 reads all returned HTTP 200. The write burst sent the same valid plan ID 20 times and received 20 HTTP 202 responses. All returned one job ID. A subsequent query found a single durable row for that plan, and the job finished successfully.

The client warms the API with ten sequential reads and releases each measured burst behind a shared gate. Latencies use a monotonic clock from request initiation through the response body. Percentiles use nearest rank. Throughput divides the burst count by the wall time from release through the last completion. Transport errors and unexpected HTTP status codes count as failures. Job submission latency measures admission. Completion is verified separately. The measurements represent one local burst per endpoint with no sustained load phase.

### Reproduce

Use the [Compose setup](DEVSECOPS.md) with an isolated simulator database. Set `PLATFORM_PORT=18000` when starting the platform container. Run the benchmark from a host environment with the locked Python dependencies.

```bash
python -m benchmarks.load_api --base http://127.0.0.1:18000 --requests 100 --submit 20
```

The optional submission burst creates and executes one simulation plan. Use the dedicated validation instance for repeat runs.

## Kind availability and recovery

The integration test drained `k8s-reliability-lab-worker2`, replaced four pods, and restored scheduling in **11.82 seconds**. Each of the two workloads had three desired replicas and at least two Ready replicas in every sample. Requests through the Kubernetes API proxy to the demo Services succeeded **12/12** times for `web` and **12/12** times for `api`. This is sampled availability.

| Check | Observed result |
| --- | --- |
| Competing live approvals and worker claims | The duplicate approval was rejected, and only the owning worker could continue. |
| Stale cordon ownership | The engine stopped when another actor owned the cordon and preserved that scheduling state. |
| Restrictive disruption budget | Kubernetes returned HTTP 429 and left the targeted pod in place. The test completed in 7.29 seconds. |
| Worker interruption after eviction | A replacement worker reconciled the real cluster state and completed in 8.80 seconds after restart without repeating the completed eviction. |
| Authenticated AIOps remediation | An approved retry used the existing safe worker drain path and completed in 8.86 seconds. |

This run passed **265 tests in 94.28 seconds**, including six dedicated Kind tests. The suite also exercises SQLite fencing, checkpoint recovery, backup and restore, single-writer admission, and provider failure handling. The local CLI checks validated Docker Compose configuration, the runtime image's non-root setting and health check, Helm lint/render/package, Terraform format and validation, and GitHub Actions syntax. GitHub-hosted CI and CodeQL execution remain unverified.

Re-run the availability and recovery tests using the [Kind setup](KUBERNETES.md).

```bash
RUN_KIND_TESTS=1 python -m pytest -q -s
```

The Kind sample represents two small demo Services on one local host. A production availability or capacity claim requires a longer workload-specific test and additional monitoring.
