# Intelligent Kubernetes Maintenance & Reliability Platform

Stage 1 is a standalone, simulation-only platform for planning and rehearsing Kubernetes node maintenance. It starts with an original synthetic cluster of three nodes, three workloads, and five pods. The application uses FastAPI, Pydantic, and SQLite and has no Kubernetes client or credentials integration.

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

## Rehearse maintenance

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

## Architecture and safety semantics

- [Models and scenario](app/models.py) define resource requests in millicores and MiB, pod phases, workload availability floors, and namespace-scoped Pod Disruption Budgets. The [scenario generator](app/scenarios.py) produces a deterministic fixture.
- [Maintenance policy](app/policy.py) rejects protected workloads and namespaces, unavailable target nodes, and evictions that would violate a workload or matching budget. Ready replicas count toward availability only when their node is Ready. Destinations must be Ready, schedulable, and able to fit resource requests alongside existing pods.
- [Simulator](app/simulator.py) cordons the target and drains one pod at a time. It advances a virtual clock through termination and replacement startup, waits for the replacement to become Ready before the next eviction, checks final readiness, and uncordons the node.
- [Service and state machine](app/service.py) revalidate the plan under a SQLite write lock, including its revision and placements. The successful path is `pending → validating → cordoned → draining → validating_readiness → uncordoning → succeeded`. Unsafe plans finish as `rejected`. Simulated runtime failures finish as `failed` and leave the stored topology and clock at their pre-operation values.
- [SQLite storage](app/store.py) atomically publishes the successful snapshot together with the operation and its audit trail. Writers serialize across application instances using the same database. A successful operation invalidates other plans at that revision. Repeating an explicit execution with the same plan ID returns its existing operation.
- [FastAPI](app/main.py) serves the local interface and emits JSON operational logs with plan or operation correlation fields. The durable audit endpoint includes wall-clock timestamps and simulated event times.

## Measured results

- **Automated tests** — 15 passed in **1.99 seconds** on Python 3.14.4 and Pytest 9.2.0. Scenarios cover CPU and memory shortages, critical workloads and protected namespaces, independent application and disruption-budget checks, unready replicas, termination and readiness failures, persistence, and concurrent operations. The installed Starlette release emitted one deprecation warning for its HTTPX-based test client.
- **Local HTTP demonstration** — Uvicorn bound to loopback. Health returned HTTP 200 and planning returned HTTP 201. The default dry run returned HTTP 200 and preserved revision 0. Explicit execution returned HTTP 200 in **16.38 ms** for this single local request. Checkout moved to Willow and Catalog to Maple. Cedar was uncordoned, the cluster advanced to revision 1 after **26 simulated seconds**, and the audit endpoint returned **17 events**. Planning Willow reported `protected_workload`.

## Stage 1 limitations

- The simulator uses integer minimum-ready thresholds and workload-name selectors for budgets. Kubernetes percentage budgets, selector expressions, taints, affinity, topology spread, storage constraints, and controller reconciliation are outside this model.
- Scheduling is deterministic and greedy. It may reject a maintenance plan that a different eviction order or placement could satisfy. Plans report the first blocking policy finding.
- Readiness, graceful termination, and injected failures use synthetic inputs and virtual time. Rollback is an atomic simulator snapshot behavior.
- Execution is synchronous, with a single SQLite writer and terminal operation status visible after commit. Plan and audit retention is manual. The local API has no authentication.
- Verification used Python 3.14.4. The project declares Python 3.11+ and uses standard library features available in 3.11.
