# Intelligent Kubernetes Maintenance & Reliability Platform

This standalone platform plans and rehearses Kubernetes node maintenance. Simulation remains the default and seeds an original cluster of three nodes, three workloads, and five pods. Stage 4 adds opt-in discovery, dry-run forecasts, and approved maintenance against a dedicated four-node Kind lab. Stage 5 adds event correlation, capacity warnings, and evidence-based recommendations with authenticated remediation approval. Stage 6 adds Prometheus metrics, OpenTelemetry traces, Grafana dashboards, SLOs, and local alerting. The simulator, batch scheduling, and durable job APIs are still available.

## Run locally

Use Python 3.11 or newer with pip and venv available.

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements.txt
python -m pytest
python -m uvicorn app.main:app --host 127.0.0.1 --port 8000
```

Open the API documentation at `http://127.0.0.1:8000/docs`. The default database is `data/cluster.sqlite3`. Set `MAINTENANCE_DB=data/rehearsal.sqlite3` before launching Uvicorn to use a separate simulation. A new database seeds the original topology automatically. Edit [app/config.toml](app/config.toml) and restart the API to change the protected namespaces and simulated timeouts.

## Run the Kubernetes lab

Use [the Kubernetes lab guide](docs/KUBERNETES.md) to create `k8s-reliability-lab`, install the demo workloads and restricted credentials, review a live dry-run plan, and approve a worker drain. The guide includes the measured integration test results. Live mode patches scheduling and evicts eligible pods through Kubernetes. Reboot and OS patching are simulated.

## Explore AIOps

Start the API in simulation mode and send `POST /aiops/demo` to seed original incident scenarios. Use `GET /aiops/incidents` to review correlated signals and request a recommendation for any open incident. The default mock provider runs locally with zero API charges. See the [AIOps guide](docs/AIOPS.md) for the approval workflow, optional provider configuration, and Kind validation.

## Monitor the platform

The [observability guide](docs/OBSERVABILITY.md) starts Prometheus, Grafana, Alertmanager, and Jaeger with Docker Compose. It documents the metric labels and SLO calculations, shows how to reproduce a controlled Kind readiness failure, and links to four screenshots from the running Grafana stack.

## Rehearse single-node maintenance

Run these requests from another terminal with Uvicorn running. Use a fresh database for the sample Cedar topology. The shell examples use `curl` and Python to extract IDs from the responses.

```bash
api=http://127.0.0.1:8000
curl -fsS "$api/health"
curl -fsS "$api/cluster"

plan=$(curl -fsS "$api/maintenance/plans" \
  -H 'Content-Type: application/json' -d '{"node":"cedar"}')
printf '%s\n' "$plan" | python -m json.tool
plan_id=$(printf '%s' "$plan" | python -c 'import json,sys; print(json.load(sys.stdin)["id"])')

curl -fsS "$api/maintenance/execute" -H 'Content-Type: application/json' \
  -d "{\"plan_id\":\"$plan_id\"}"

operation=$(curl -fsS "$api/maintenance/execute" -H 'Content-Type: application/json' \
  -d "{\"plan_id\":\"$plan_id\",\"dry_run\":false}")
printf '%s\n' "$operation" | python -m json.tool
operation_id=$(printf '%s' "$operation" | python -c 'import json,sys; print(json.load(sys.stdin)["id"])')

curl -fsS "$api/maintenance/operations/$operation_id"
curl -fsS "$api/maintenance/operations/$operation_id/audit"
curl -fsS "$api/maintenance/plans" \
  -H 'Content-Type: application/json' -d '{"node":"willow"}'
```

| Endpoint | Behavior |
| --- | --- |
| `GET /health` | Checks SQLite access and reports simulation mode |
| `GET /cluster` | Returns the persisted topology, cluster revision, and virtual clock |
| `POST /maintenance/plans` | Saves a dry-run plan with projected placements or the first blocking policy finding |
| `POST /maintenance/execute` | Defaults to a fresh dry-run plan for the same node. An explicit JSON boolean `false` executes the submitted plan in simulation |
| `GET /maintenance/operations/{operation_id}` | Returns the terminal state and any failure or rejection reason |
| `GET /maintenance/operations/{operation_id}/audit` | Returns ordered state changes and simulator events |

Explicit execution returns HTTP 200 with an operation state of `succeeded`, `rejected`, or `failed`. Missing plan and operation IDs return HTTP 404, malformed requests return HTTP 422, and a SQLite operational error returns HTTP 503.

## Rehearse batch maintenance

Use a fresh simulation database to reproduce the sample topology. The initial batch plans Cedar and Maple in separate waves because draining both together would leave Catalog with no Ready replica.

```bash
api=http://127.0.0.1:8000
curl -fsS "$api/maintenance/policy"

batch=$(curl -fsS "$api/maintenance/batches/plans" \
  -H 'Content-Type: application/json' -d '{"nodes":["cedar","maple"]}')
printf '%s\n' "$batch" | python -m json.tool
batch_id=$(printf '%s' "$batch" | python -c 'import json,sys; print(json.load(sys.stdin)["id"])')

curl -fsS "$api/maintenance/batches/execute" -H 'Content-Type: application/json' \
  -d "{\"plan_id\":\"$batch_id\"}"

operation=$(curl -fsS "$api/maintenance/batches/execute" -H 'Content-Type: application/json' \
  -d "{\"plan_id\":\"$batch_id\",\"dry_run\":false}")
operation_id=$(printf '%s' "$operation" | python -c 'import json,sys; print(json.load(sys.stdin)["id"])')
curl -fsS "$api/maintenance/batches/operations/$operation_id" | python -m json.tool
```

| Endpoint | Behavior |
| --- | --- |
| `GET /maintenance/policy` | Reads the current batch policy and revision |
| `PUT /maintenance/policy` | Replaces batch policy settings with the request body and advances the revision if they changed |
| `POST /maintenance/batches/plans` | Saves a dry-run plan for a list of distinct nodes |
| `POST /maintenance/batches/execute` | Defaults to a fresh dry-run plan. An explicit JSON boolean `false` executes the batch in simulation |
| `GET /maintenance/batches/operations/{operation_id}` | Reads the terminal state, effective plan, replan reasons, and ordered audit events |

Use the `settings` object from GET as the body of PUT when changing individual fields. Fields omitted from PUT take schema defaults. The `[batch]` table in [app/config.toml](app/config.toml) seeds a database on first start. Subsequent changes made through the API persist across restarts. Policies control parallel wave width, total unavailable nodes, search steps, deadlines, and retry limits and delays. An empty `windows` list permits maintenance at any time. A window such as `{"start":"23:00","end":"01:00"}` allows maintenance starting at 23:00 UTC and ending at 01:00 UTC the next day. A batch must fit the operation deadline and the time remaining in the current window.

### Stage 2 scheduling and recovery

- [Batch scheduler](app/scheduler.py) ranks targets using node health deficit, resource utilization, workload criticality, and disruption headroom. A bounded search tries wider waves first and backtracks over placements and wave order. Each wave includes predicted CPU and memory usage and time. Unsafe plans include specific blocking reasons. Safe plans include blockers encountered while trying alternatives.
- [Placement rules](app/constraints.py) evaluate exact-match node selectors, alternative required node-affinity terms, pod affinity and reciprocal anti-affinity by workload and topology label, and taints and tolerations. `PreferNoSchedule` influences placement ordering. The scheduler validates required co-location after projecting all relocations in a wave.
- [Batch service](app/batch_service.py) holds the shared SQLite write lock while revalidating and executing. It automatically replans on changed revisions or scheduling decisions. A plan whose target was maintained after planning is rejected as an operation conflict. Reusing a plan ID returns its original operation.
- [Batch runtime](app/batch_runtime.py) cordons each wave, simulates overlapping termination and startup, waits for readiness, and restores scheduling before the next wave. Injected transient cordon and eviction failures use capped exponential backoff. Expected failures or deadlines discard the entire tentative snapshot and persist the failure audit, including any earlier waves that completed inside the simulation.

## Stage 1 architecture and safety semantics

- [Models and scenario](app/models.py) define resource requests in millicores and MiB, pod phases, workload availability floors, and namespace-scoped Pod Disruption Budgets. The [scenario generator](app/scenarios.py) produces a deterministic fixture.
- [Maintenance policy](app/policy.py) rejects protected workloads and namespaces, unavailable target nodes, and evictions that would violate a workload or matching budget. Ready replicas count toward availability only when their node is Ready. Destinations must be Ready, schedulable, and able to fit resource requests alongside existing pods.
- [Simulator](app/simulator.py) cordons the target and drains one pod at a time. It advances a virtual clock through termination and replacement startup, waits for the replacement to become Ready before the next eviction, checks final readiness, and uncordons the node.
- [Service and state machine](app/service.py) revalidate the plan under a SQLite write lock, including its revision and placements. The successful path is `pending → validating → cordoned → draining → validating_readiness → uncordoning → succeeded`. Unsafe plans finish as `rejected`. Simulated runtime failures finish as `failed` and leave the stored topology and clock at their pre-operation values.
- [SQLite storage](app/store.py) atomically publishes the successful snapshot together with the operation and its audit trail. Writers serialize across application instances using the same database. A successful operation invalidates other plans at that revision. Repeating an explicit execution with the same plan ID returns its existing operation.
- [FastAPI](app/main.py) serves the local interface and emits JSON operational logs with plan or operation correlation fields. The durable audit endpoint includes wall-clock timestamps and simulated event times.

## Stage 1 measured results

- **Automated tests** — 15 passed in **1.99 seconds** on Python 3.14.4 and Pytest 9.2.0. Scenarios cover CPU and memory shortages, critical workloads and protected namespaces, independent application and disruption-budget checks, unready replicas, termination and readiness failures, persistence, and concurrent operations. The installed Starlette release emitted one deprecation warning for its HTTPX-based test client.
- **Local HTTP demonstration** — Uvicorn bound to loopback. Health returned HTTP 200 and planning returned HTTP 201. The default dry run returned HTTP 200 and preserved revision 0. Explicit execution returned HTTP 200 in **16.38 ms** for this single local request. Checkout moved to Willow and Catalog to Maple. Cedar was uncordoned, the cluster advanced to revision 1 after **26 simulated seconds**, and the audit endpoint returned **17 events**. Planning Willow reported `protected_workload`.

## Stage 1 limitations

- The simulator uses integer minimum-ready thresholds and workload-name selectors for budgets. Kubernetes percentage budgets, selector expressions, taints, affinity, topology spread, storage constraints, and controller reconciliation are outside this model.
- Scheduling is deterministic and greedy. It may reject a maintenance plan that a different eviction order or placement could satisfy. Plans report the first blocking policy finding.
- Readiness, graceful termination, and injected failures use synthetic inputs and virtual time. Rollback is an atomic simulator snapshot behavior.
- Execution is synchronous, with a single SQLite writer and terminal operation status visible after commit. Plan and audit retention is manual. The local API has no authentication.
- Verification used Python 3.14.4. The project declares Python 3.11+ and uses standard library features available in 3.11.


## Stage 2 measured results

- **Automated tests** — 71 passed in **4.35 seconds** on Python 3.14.4 and Pytest 9.2.0, including all 15 original Stage 1 tests. The suite covers concurrent requests, parallel waves, capacity and availability limits, placement constraints, replanning, retries, deadlines, rollback, and legacy database loading. Starlette emitted one deprecation warning for its HTTPX-based test client.
- **Local HTTP demonstration** — A batch targeting Maple and Cedar preserved revision 0 during dry run. The plan explained why Catalog availability required two waves. Updating the parallel limit advanced the policy to revision 1. Executing the original plan detected the stale policy, replanned, and succeeded in **26 simulated seconds** with **24 audit events**. Both nodes were schedulable at cluster revision 1.

### Scheduler benchmark

Run `python -m benchmarks.compare_schedulers` from the project root. Both algorithms receive the same freshly generated topology and target for each seed. The timing includes in-process plan computation and snapshot copies and excludes HTTP and SQLite.

| Scenario family | Cases | Greedy safe | Stage 2 safe | Greedy median | Stage 2 median |
| --- | ---: | ---: | ---: | ---: | ---: |
| Mixed resources and placements | 100 | 100 | 100 | 0.1855 ms | 0.3580 ms |
| Complementary CPU and memory shapes | 100 | 0 | 100 | 0.1202 ms | 0.2644 ms |
| Fragmentation requiring backtracking | 100 | 0 | 100 | 0.3818 ms | 0.7808 ms |

The two pressure families were constructed to expose greedy placement failures. The results measure these synthetic inputs and do not estimate performance or success rates on production clusters.

### Stage 2 limitations

- A wave budgets disruption for all pods on its targets at once. The planner can split nodes across waves. A single node whose pods are only safe to evict sequentially can still be handled by the Stage 1 endpoint.
- Placement implements a subset of Kubernetes semantics. Node affinity uses exact label matches, pod affinity uses explicit workload IDs, and the simulator has no storage topology, selector expressions, preemption, or eviction toleration timers.
- Parallelism is modeled inside a wave. Separate API operations are serialized by SQLite, and operation status becomes visible at commit. The UTC window is checked at execution start and the remaining window and deadline are consumed on the virtual clock.
- Batch policy applies to batch endpoints. The original single-node endpoints retain their Stage 1 policy and stale-plan behavior. The local API has no authentication.

## Stage 3 durable maintenance

Create a single-node or batch plan through its existing endpoint, then submit its ID to the durable queue. Jobs advance in the background and survive application restarts. The existing synchronous execution and dry-run endpoints remain available.

```bash
api=http://127.0.0.1:8000
batch=$(curl -fsS "$api/maintenance/batches/plans" \
  -H 'Content-Type: application/json' -d '{"nodes":["cedar","maple"]}')
plan_id=$(printf '%s' "$batch" | python -c 'import json,sys; print(json.load(sys.stdin)["id"])')
job=$(curl -fsS "$api/maintenance/jobs" -H 'Content-Type: application/json' \
  -d "{\"kind\":\"batch\",\"plan_id\":\"$plan_id\"}")
job_id=$(printf '%s' "$job" | python -c 'import json,sys; print(json.load(sys.stdin)["id"])')
curl -fsS "$api/maintenance/jobs/$job_id" | python -m json.tool
curl -fsS "$api/maintenance/jobs/$job_id/history" | python -m json.tool
```

The same plan and kind return the same job. A batch checkpoints one wave at a time, and a single-node job checkpoints the complete maintenance operation. Pause and cancel take effect at transaction boundaries. Completed waves remain visible after cancellation or a later failure.

See the [recovery guide](docs/RECOVERY.md) for endpoints, failure scenarios, backup and restore commands, and measured results.
