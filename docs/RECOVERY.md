# Durable maintenance and disaster recovery

Stage 3 runs only against the local synthetic cluster. Existing Stage 1 and Stage 2 databases gain the job tables on application startup. The existing single-node and batch execution routes retain their synchronous transaction semantics.

## Architecture

- **Queue and ownership.** SQLite stores jobs keyed by kind and plan ID. A singleton ownership row serializes durable maintenance across processes and blocks conflicting synchronous writes while a job is active. Plans and reads remain available. Reusing a plan with the other execution mode receives HTTP 409. If an earlier job maintained a target after planning, a queued batch rejects the stale target, and a single-node job retains Stage 1's stricter revision check.
- **Fencing and heartbeats.** A claim persists the worker ID, a lease expiry, and an increasing fence value. The next write transaction verifies all three values and cluster ownership. Expired leases and operator controls invalidate old claims. A write transaction retains SQLite's lock through commit, so a worker cannot take over midway through another worker's transaction. A worker heartbeat can extend a live claim. The default heartbeat and polling interval is 100 ms and the lease duration is two seconds.
- **Checkpoints.** Preparation persists the validated schedule and a hash of the authoritative SQLite cluster snapshot. Each wave holds the cluster write lock and atomically commits its placements, Ready pods, scheduling restoration, revision, checkpoint, and ordered audit records. A single-node job uses the original simulator and commits the complete node operation as one step. Batch plans apply the Stage 2 availability, placement, and node-disruption checks. A wave also checks the current window and remaining simulated deadline before it runs.
- **Reconciliation.** On startup the worker automatically scans for expired leases before claiming work. An interrupted transaction rolls back, leaving the last committed wave intact. The worker verifies the snapshot hash and restarts from the next checkpoint. Unexpected snapshot drift fails the operation with `cluster_drift` and leaves the observed scheduling state untouched. A policy change during an active batch stops subsequent waves with `policy_changed`.
- **Retries and audit.** If a transaction can still write after an injected or recoverable step failure, it rolls back to a savepoint and records the attempt and next eligible time in SQLite. Delays grow exponentially to the configured cap. The retry limit applies per checkpoint, and a successful checkpoint clears the current retry state. The audit retains retry events. Failures that prevent SQLite from writing at all leave the prior durable state intact until storage returns.
- **Lifecycle.** FastAPI starts a background worker on startup and joins it on shutdown. The worker finishes its current transaction, expires any remaining lease, and marks its heartbeat inactive.

## API

| Method and path | Result |
| --- | --- |
| `POST /maintenance/jobs` | Submits a `kind` of `single` or `batch` and a `plan_id`. Returns HTTP 202 with the job. |
| `GET /maintenance/jobs` | Lists durable jobs newest first. Pass `limit` and the returned `next_cursor` as `before` to paginate. |
| `GET /maintenance/jobs/{id}` | Reads state, progress, simulated time, retry count, and any reported problem. |
| `GET /maintenance/jobs/{id}/history` | Reads ordered audit entries. Pass `after` with the last sequence to continue and `limit` to adjust page size. |
| `POST /maintenance/jobs/{id}/pause` | Pauses at the next transaction boundary. An already claimed job keeps cluster ownership. |
| `POST /maintenance/jobs/{id}/resume` | Requeues a paused job at the same checkpoint. A scheduled retry keeps its original eligibility time. |
| `POST /maintenance/jobs/{id}/cancel` | Fences the worker and releases ownership. Committed waves stay in place. |
| `POST /maintenance/recovery` | Reconciles expired claims immediately and returns the count. Live claims keep their leases. |
| `GET /maintenance/workers` | Shows recent worker IDs, heartbeat and expiry timestamps, and current health. |

Timestamps in these responses use Unix seconds. Poll job status until it reaches `succeeded`, `rejected`, `failed`, or `cancelled`. After `cluster_drift`, inspect the snapshot and job history before creating a new plan. If a job is paused after claiming ownership, resume or cancel it to let queued jobs proceed. Missing records return HTTP 404 and conflicting controls return HTTP 409.

## Backup and restore

Run from the project root with the application's dependencies available. The backup uses SQLite's online backup API, so it can capture committed state while a worker is running. The destination directory must already exist.

```bash
python -m app.disaster_recovery backup data/cluster.sqlite3 data/backup.sqlite3
python -m app.disaster_recovery restore data/backup.sqlite3 data/restored.sqlite3
MAINTENANCE_DB=data/restored.sqlite3 python -m uvicorn app.main:app --host 127.0.0.1 --port 8000
```

The CLI checks SQLite integrity, foreign keys, and cluster deserialization before publishing the copy. It requires a new destination with no SQLite sidecar files. Restore expires copied leases so a new worker can reconcile immediately. Paused jobs remain paused. Stop the application using the original database before switching the simulation to the restored database. Backups represent a point in time. Plans, audit entries, and progress committed after that point exist only in the original database.

## Failure drills and test evidence

The full suite passed **117 tests in 4.94 seconds** on Python 3.14.4, including all 71 Stage 1 and Stage 2 tests. Run `PYTHONPATH=.deps:. python3 -m pytest -q` with the installed dependencies. Fault hooks provide isolated injection before a step, during eviction, after the SQLite cluster write, and after commit. Tests inject SQLite operational errors, network exceptions, and terminal failures. No fault switches are exposed over HTTP.

The process tests use an isolated SQLite file and send SIGKILL to a Python worker during eviction, after the SQLite write and before commit, and immediately after a committed wave. A distinct process starts the FastAPI lifespan against the same file. Tests verify recovery from the correct checkpoint, unique committed eviction events, preserved workload and disruption budgets, and restored scheduling. Additional tests cover competing workers, heartbeat renewal and expiry, stale fences, both submission modes, retries across restart, pause and cancellation, snapshot drift, maintenance windows, backup integrity, and graceful shutdown.

A loopback Uvicorn run returned HTTP 201 for the batch plan and HTTP 202 for the job. Duplicate submission returned the same ID. The Cedar/Maple batch reached two checkpoints and cluster revision 2, with both targets schedulable and 25 history events. The client observed completion after 556.8 ms, including HTTP and polling. After a verified backup and restore, a second Uvicorn process returned the same job and history, and the recovery endpoint reported zero stale leases.

## Benchmarks

Run `PYTHONPATH=.deps:. python3 -m benchmarks.recovery`. The benchmark uses 30 fresh SQLite databases and the Cedar/Maple two-wave topology. Timings omit HTTP, worker polling, and the wait for natural lease expiry. The recovery sample explicitly releases the simulated lost worker's lease. SQLite commits and backup verification are included.

| Operation | Median | p95 |
| --- | ---: | ---: |
| Enqueue through first wave | 2.36 ms | 6.41 ms |
| Reconcile and finish second wave | 0.75 ms | 1.77 ms |
| Verified backup | 0.46 ms | 7.02 ms |
| Verified restore | 0.60 ms | 1.61 ms |

## Limitations

- Simulation actions and their audit share one SQLite transaction. External Kubernetes calls would require their own idempotency and ownership protocol. The simulator models readiness and termination using virtual time.
- Checkpoint granularity is one wave or one complete single-node operation. Uncommitted simulator work can be recomputed. The system preserves committed waves, and cancellation does not reverse their pod placements.
- SQLite serializes writers. There is one active durable job per database. A paused owner or a retrying owner can delay unrelated work. Persistent storage outages prevent progress and may delay shutdown until the SQLite lock timeout.
- Reconciliation verifies the local simulator snapshot and fails closed on unexplained drift. Determining the source of manual edits or corruption requires operator review.
- The API is unauthenticated and intended for loopback access. Backup publication uses local filesystem hard links and directory sync. Job, audit, worker, and backup retention are manual.
