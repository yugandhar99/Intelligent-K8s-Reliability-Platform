from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, time
from pathlib import Path
import signal
import sqlite3
import subprocess
import sys
from time import monotonic, sleep, time as wall_time

from fastapi.testclient import TestClient
import pytest

from app.batch_models import BatchPolicy, MaintenanceWindow
from app.batch_service import BatchService
from app.disaster_recovery import copy_database
from app.jobs import JobService
from app.main import create_app
from app.models import PodPhase, PolicySettings, State
from app.policy import MaintenancePolicy, SafetyError
from app.service import MaintenanceService, RecordNotFound
from app.store import MaintenanceConflict, Store


def services(path, **options):
    store = Store(path)
    store.initialize()
    policy = MaintenancePolicy(PolicySettings(
        protected_namespaces={"kube-system", "platform-security"},
        termination_timeout_seconds=30, readiness_timeout_seconds=60,
    ))
    single = MaintenanceService(store, policy)
    batch = BatchService(store, policy, BatchPolicy())
    batch.initialize()
    jobs = JobService(single, batch, **options)
    jobs.initialize()
    return jobs


@pytest.fixture
def env(tmp_path):
    clock = [100.0]
    jobs = services(tmp_path / "cluster.sqlite3", clock=lambda: clock[0])
    return jobs, clock


def submit(jobs, kind="batch", nodes=("cedar", "maple")):
    plan = jobs.batch.create_plan(list(nodes)) if kind == "batch" else jobs.single.create_plan(nodes[0])
    return jobs.submit(kind, plan.id)


def step(jobs):
    lease = jobs.claim("worker")
    assert lease and jobs.run(lease)
    return jobs.status(lease.job_id)


def drain(jobs, job_id):
    for _ in range(40):
        job = jobs.status(job_id)
        if job.state in {"succeeded", "failed", "rejected", "cancelled"}:
            return job
        step(jobs)
    pytest.fail("Job did not reach a terminal state")


def events(jobs, job_id, name):
    return [event for event in jobs.history(job_id) if event.event == name]


def edit(jobs, change):
    with jobs.store.transaction(write=True) as db:
        cluster = jobs.store.snapshot(db)
        change(cluster)
        db.execute("UPDATE cluster SET payload = ? WHERE id = 1", (cluster.model_dump_json(),))


@pytest.mark.parametrize("kind,count", [("single", 1), ("batch", 2)])
def test_commit_and_audit_for_each_kind(env, kind, count):
    jobs, _ = env
    job = submit(jobs, kind)
    result = drain(jobs, job.id)
    assert (result.state, result.checkpoint, result.total_steps) == ("succeeded", count, count)
    assert [event.details["revision"] for event in events(jobs, job.id, "job.checkpoint")] == list(range(1, count + 1))
    cluster = jobs.single.cluster()
    assert cluster.revision == count
    assert all(cluster.nodes[node].schedulable for node in job.nodes)
    assert all(pod.phase == PodPhase.READY for pod in cluster.pods.values())
    for pod in cluster.pods.values():
        jobs.single.policy.validate_availability(cluster, pod, evicting=False)


@pytest.mark.parametrize("kind", ["single", "batch"])
def test_concurrent_submit_is_idempotent(env, kind):
    jobs, _ = env
    initial = submit(jobs, kind)
    with ThreadPoolExecutor(max_workers=8) as pool:
        ids = list(pool.map(lambda _: jobs.submit(kind, initial.plan_id).id, range(16)))
    assert set(ids) == {initial.id}
    assert len(events(jobs, initial.id, "job.queued")) == 1


@pytest.mark.parametrize("kind", ["single", "batch"])
@pytest.mark.parametrize("durable_first", [False, True])
def test_same_plan_cannot_execute_in_both_modes(env, kind, durable_first):
    jobs, _ = env
    service = jobs.single if kind == "single" else jobs.batch
    plan = service.create_plan("cedar" if kind == "single" else ["cedar"])
    if durable_first:
        job = jobs.submit(kind, plan.id)
        with pytest.raises(MaintenanceConflict):
            service.execute(plan.id)
        assert drain(jobs, job.id).state == "succeeded"
    else:
        assert service.execute(plan.id).state == State.SUCCEEDED
        with pytest.raises(MaintenanceConflict):
            jobs.submit(kind, plan.id)


@pytest.mark.parametrize("kind", ["single", "batch"])
def test_distinct_stale_plans_cannot_repeat_maintenance(env, kind):
    jobs, _ = env
    first = submit(jobs, kind)
    second = submit(jobs, kind)
    assert drain(jobs, first.id).state == "succeeded"
    result = drain(jobs, second.id)
    assert result.state == "rejected"
    assert result.problem.code in {"stale_plan", "conflicting_operation"}
    assert not events(jobs, second.id, "pod.evicted")


@pytest.mark.parametrize("command", ["pause", "cancel"])
@pytest.mark.parametrize("completed", [0, 1])
def test_control_stops_at_transaction_boundary(env, command, completed):
    jobs, _ = env
    initial = submit(jobs)
    legacy = jobs.single.create_plan("maple")
    step(jobs)
    if completed:
        step(jobs)
    original = jobs.single.cluster()
    lease = jobs.claim("old")
    controlled = jobs.control(initial.id, command)
    assert controlled.checkpoint == completed
    assert not jobs.run(lease)
    assert jobs.single.cluster() == original
    if command == "pause":
        assert jobs.claim("idle") is None
        with pytest.raises(MaintenanceConflict):
            jobs.single.execute(legacy.id)
        assert jobs.control(initial.id, "resume").state == "queued"
        assert drain(jobs, initial.id).state == "succeeded"
    else:
        with pytest.raises(MaintenanceConflict):
            jobs.control(initial.id, "resume")
        assert jobs.claim("idle") is None
        assert jobs.control(initial.id, "cancel").state == "cancelled"
        assert all(original.nodes[node].schedulable for node in initial.nodes)


def test_pause_before_claim_does_not_block_next_job(env):
    jobs, _ = env
    paused = submit(jobs)
    jobs.control(paused.id, "pause")
    other = submit(jobs, "single")
    assert drain(jobs, other.id).state == "succeeded"
    assert jobs.status(paused.id).state == "paused"


def test_lease_expiry_and_fence(env):
    jobs, clock = env
    job = submit(jobs)
    old = jobs.claim("old")
    assert jobs.claim("new") is None
    clock[0] += 3
    replacement = jobs.claim("new")
    assert replacement.fence > old.fence
    assert not jobs.run(old)
    assert jobs.run(replacement)
    assert not jobs.run(replacement)
    assert len(events(jobs, job.id, "job.recovered")) == 1


def test_only_one_worker_claims_job(env):
    jobs, _ = env
    submit(jobs)
    with ThreadPoolExecutor(max_workers=8) as pool:
        claims = list(pool.map(lambda i: jobs.claim(str(i)), range(8)))
    assert sum(claim is not None for claim in claims) == 1


def test_heartbeat_expiry_and_release(env):
    jobs, clock = env
    jobs.heartbeat("worker")
    assert jobs.workers()[0].healthy
    clock[0] += 1
    jobs.heartbeat("worker")
    clock[0] += 1.5
    assert jobs.workers()[0].healthy
    clock[0] += 1
    assert not jobs.workers()[0].healthy
    jobs.heartbeat("worker")
    jobs.release("worker")
    assert not jobs.workers()[0].healthy


@pytest.mark.parametrize("recover_first", [False, True])
def test_reconciliation_detects_unowned_scheduling_change(env, recover_first):
    jobs, clock = env
    job = submit(jobs)
    step(jobs)
    step(jobs)
    lease = jobs.claim("old")
    edit(jobs, lambda cluster: setattr(cluster.nodes["maple"], "schedulable", False))
    if recover_first:
        clock[0] += 3
        assert jobs.recover() == 1
    else:
        assert jobs.run(lease)
    result = jobs.status(job.id)
    assert (result.state, result.problem.code, result.checkpoint) == ("failed", "cluster_drift", 1)
    assert not jobs.single.cluster().nodes["maple"].schedulable
    assert jobs.recover() == 0


@pytest.mark.parametrize("fault_type,code", [
    (ConnectionError, "network_interruption"), (sqlite3.OperationalError, "database_error"),
])
@pytest.mark.parametrize("point", ["before_step", "during_eviction", "after_cluster_write"])
def test_transient_failures_persist_backoff_and_rollback(env, fault_type, code, point):
    jobs, clock = env
    job = submit(jobs)
    step(jobs)
    original = jobs.single.cluster()

    def fail(current, _):
        if current == point:
            raise fault_type("synthetic interruption")

    jobs.fault = fail
    first = step(jobs)
    assert (first.state, first.retries, first.next_attempt, first.problem.code) == ("retrying", 1, 102, code)
    assert jobs.claim("other") is None
    assert jobs.single.cluster() == original
    assert not events(jobs, job.id, "pod.evicted")

    restarted = services(jobs.store.path, clock=lambda: clock[0], fault=fail)
    clock[0] = 102
    second = step(restarted)
    assert (second.retries, second.next_attempt) == (2, 106)
    clock[0] = 106
    restarted.fault = None
    assert drain(restarted, job.id).state == "succeeded"
    assert [event.details["delay"] for event in events(restarted, job.id, "job.retry_scheduled")] == [2, 4]


def test_retry_limit_releases_owner(env):
    jobs, _ = env
    jobs.batch.update_policy(BatchPolicy(retry_limit=0))
    job = submit(jobs)
    step(jobs)

    def fail(point, _):
        if point == "after_cluster_write":
            raise ConnectionError("synthetic network outage")

    jobs.fault = fail
    result = step(jobs)
    assert (result.state, result.problem.code) == ("failed", "retry_exhausted")
    assert jobs.single.cluster().revision == 0
    assert jobs.single.execute(jobs.single.create_plan("cedar").id).state == State.SUCCEEDED


def test_partial_failure_keeps_only_finished_wave(env):
    jobs, _ = env
    job = submit(jobs)
    step(jobs)
    step(jobs)
    original = jobs.single.cluster()

    def fail(point, _):
        if point == "during_eviction":
            raise SafetyError("partial_failure", "Synthetic partial wave failure")

    jobs.fault = fail
    result = step(jobs)
    assert (result.state, result.checkpoint, result.problem.code) == ("failed", 1, "partial_failure")
    assert jobs.single.cluster() == original
    assert len(events(jobs, job.id, "job.checkpoint")) == 1


@pytest.mark.parametrize("kind", ["single", "batch"])
def test_protected_target_rejected(env, kind):
    jobs, _ = env
    job = submit(jobs, kind, ("willow",))
    result = drain(jobs, job.id)
    assert (result.state, result.problem.code, result.checkpoint) == ("rejected", "protected_workload", 0)
    assert not events(jobs, job.id, "node.cordoned")


def test_policy_change_after_checkpoint_stops_before_next_eviction(env):
    jobs, _ = env
    job = submit(jobs)
    step(jobs)
    step(jobs)
    jobs.batch.update_policy(BatchPolicy(parallel_limit=1))
    result = step(jobs)
    assert (result.state, result.problem.code, result.checkpoint) == ("failed", "policy_changed", 1)


def test_window_closes_between_waves(env):
    jobs, _ = env
    now = [datetime(2026, 1, 1, 12, tzinfo=UTC)]
    jobs.batch.clock = lambda: now[0]
    jobs.batch.update_policy(BatchPolicy(windows=[MaintenanceWindow(start=time(12), end=time(13))]))
    job = submit(jobs)
    step(jobs)
    step(jobs)
    now[0] = now[0].replace(hour=13)
    result = step(jobs)
    assert (result.state, result.problem.code, result.checkpoint) == ("failed", "operation_deadline", 1)


def test_backup_and_restore_recover_interrupted_job(env, tmp_path):
    jobs, _ = env
    job = submit(jobs)
    step(jobs)
    step(jobs)
    jobs.heartbeat("lost")
    assert jobs.claim("lost")
    backup = tmp_path / "backup.sqlite3"
    restored = tmp_path / "restore.sqlite3"
    copy_database(jobs.store.path, backup)
    copy_database(backup, restored, restore=True)
    new = services(restored, clock=jobs.clock)
    assert not new.workers()[0].healthy
    assert new.recover() == 1
    assert drain(new, job.id).checkpoint == 2
    assert jobs.status(job.id).state == "running"
    assert jobs.single.cluster().revision == 1


@pytest.mark.parametrize("source_kind", ["missing", "garbage", "empty", "invalid_fk"])
def test_invalid_backup_fails_without_publishing(env, tmp_path, source_kind):
    jobs, _ = env
    source = tmp_path / "bad.sqlite3"
    if source_kind == "garbage":
        source.write_bytes(b"corrupt synthetic file")
    if source_kind == "empty":
        source.touch()
    if source_kind == "invalid_fk":
        source = jobs.store.path
        with sqlite3.connect(source) as db:
            db.execute("INSERT INTO job_owner VALUES (1, 'missing')")
    dest = tmp_path / "out.sqlite3"
    with pytest.raises((sqlite3.Error, ValueError)):
        copy_database(source, dest)
    assert not dest.exists()


def test_backup_never_overwrites_destination(env, tmp_path):
    jobs, _ = env
    dest = tmp_path / "existing.sqlite3"
    dest.write_bytes(b"keep this file")
    with pytest.raises(FileExistsError):
        copy_database(jobs.store.path, dest)
    assert dest.read_bytes() == b"keep this file"


def test_pagination_and_missing_records(env):
    jobs, _ = env
    older = submit(jobs)
    newer = submit(jobs)
    page = jobs.list_jobs(1)
    assert page.jobs == [newer]
    last = jobs.list_jobs(1, page.next_cursor)
    assert last.jobs == [older] and last.next_cursor is None
    step(jobs)
    event, = jobs.history(older.id, limit=1)
    assert event.event == "job.queued"
    assert all(item.sequence > event.sequence for item in jobs.history(older.id, after=event.sequence))
    with pytest.raises(RecordNotFound):
        jobs.history("missing")


def eventually(read, condition, timeout=8):
    end = monotonic() + timeout
    while monotonic() < end:
        value = read()
        if condition(value):
            return value
        sleep(0.02)
    pytest.fail("Timed out waiting for simulation state")


def test_api_queue_history_recovery_and_graceful_shutdown(tmp_path):
    path = tmp_path / "api.sqlite3"
    with TestClient(create_app(path)) as client:
        plan = client.post("/maintenance/batches/plans", json={"nodes": ["cedar", "maple"]}).json()
        response = client.post("/maintenance/jobs", json={"kind": "batch", "plan_id": plan["id"]})
        assert response.status_code == 202
        job_id = response.json()["id"]
        assert client.post("/maintenance/jobs", json={"kind": "batch", "plan_id": plan["id"]}).json()["id"] == job_id
        result = eventually(lambda: client.get(f"/maintenance/jobs/{job_id}").json(),
                            lambda job: job["state"] == "succeeded")
        assert result["checkpoint"] == 2
        history = client.get(f"/maintenance/jobs/{job_id}/history").json()
        assert history[0]["event"] == "job.queued" and history[-1]["event"] == "job.succeeded"
        assert client.get("/maintenance/jobs").json()["jobs"][0]["id"] == job_id
        assert client.post("/maintenance/recovery").json() == {"reconciled_leases": 0}
        assert any(worker["healthy"] for worker in client.get("/maintenance/workers").json())
        assert client.post(f"/maintenance/jobs/{job_id}/cancel").status_code == 409
        assert client.get("/maintenance/jobs/missing").status_code == 404
        assert client.get("/maintenance/jobs?limit=0").status_code == 422
    jobs = services(path)
    assert not any(worker.healthy for worker in jobs.workers())


# The child holds a real SQLite transaction open at the selected fault boundary.
CHILD = """
from pathlib import Path
import sys
from time import sleep
from app.jobs import JobService
from app.batch_service import BatchService
from app.batch_models import BatchPolicy
from app.service import MaintenanceService
from app.policy import MaintenancePolicy
from app.models import PolicySettings
from app.store import Store

path, marker, fault_point = sys.argv[1:]
store = Store(Path(path))
policy = MaintenancePolicy(PolicySettings(
    protected_namespaces={"kube-system", "platform-security"},
    termination_timeout_seconds=30, readiness_timeout_seconds=60,
))
def fault(point, job):
    checkpoint = 0 if fault_point != "after_commit" else 1
    if point == fault_point and job.checkpoint == checkpoint:
        Path(marker).touch()
        sleep(60)

jobs = JobService(MaintenanceService(store, policy), BatchService(store, policy, BatchPolicy()),
                  fault=fault, lease_seconds=0.30)
jobs.heartbeat("child")
while lease := jobs.claim("child"):
    jobs.run(lease)
"""


RESTART = """
import asyncio
from pathlib import Path
import sys
from app.main import create_app

async def main():
    app = create_app(Path(sys.argv[1]))
    async with app.router.lifespan_context(app):
        await asyncio.to_thread(sys.stdin.read)

asyncio.run(main())
"""


@pytest.mark.parametrize("point,committed", [
    ("during_eviction", 0), ("after_cluster_write", 0), ("after_commit", 1),
])
def test_sigkill_and_application_restart(tmp_path, point, committed):
    jobs = services(tmp_path / "crash.sqlite3", clock=wall_time)
    job = submit(jobs)
    marker = tmp_path / "fault-ready"
    process = subprocess.Popen([sys.executable, "-c", CHILD, str(jobs.store.path), str(marker), point])
    try:
        eventually(marker.exists, bool)
        process.kill()
        assert process.wait(timeout=5) == -signal.SIGKILL
    finally:
        if process.poll() is None:
            process.kill()
            process.wait(timeout=5)

    snapshot = jobs.single.cluster()
    assert jobs.status(job.id).checkpoint == committed
    assert snapshot.revision == committed
    assert all(snapshot.nodes[node].schedulable for node in job.nodes)
    with subprocess.Popen([sys.executable, "-c", RESTART, str(jobs.store.path)], stdin=subprocess.PIPE) as restarted:
        try:
            result = eventually(lambda: jobs.status(job.id), lambda status: status.state == "succeeded")
            assert result.checkpoint == 2
            assert jobs.single.cluster().revision == 2
        finally:
            restarted.communicate(timeout=5)
        assert restarted.returncode == 0
    evictions = [(event.details["pod"], event.details["node"]) for event in events(jobs, job.id, "pod.evicted")]
    assert evictions and len(evictions) == len(set(evictions))
    for pod in jobs.single.cluster().pods.values():
        jobs.single.policy.validate_availability(jobs.single.cluster(), pod, evicting=False)
    assert bool(events(jobs, job.id, "job.recovered")) == (committed == 0)


def test_heartbeat_renews_live_lease_and_cannot_revive_expired_lease(env):
    jobs, clock = env
    submit(jobs)
    lease = jobs.claim("old")
    clock[0] += 1
    jobs.heartbeat("old")
    clock[0] += 1.5
    assert jobs.claim("other") is None
    clock[0] += 1
    jobs.heartbeat("old")
    assert not jobs.run(lease)
    new = jobs.claim("other")
    assert new.fence > lease.fence


def test_maintenance_preserves_an_unrelated_cordon(env):
    jobs, _ = env
    edit(jobs, lambda cluster: setattr(cluster.nodes["willow"], "schedulable", False))
    job = submit(jobs, nodes=("cedar",))
    assert drain(jobs, job.id).state == "succeeded"
    result = jobs.single.cluster()
    assert result.nodes["cedar"].schedulable
    assert not result.nodes["willow"].schedulable


def test_restore_refuses_stale_sidecar(env, tmp_path):
    jobs, _ = env
    dest = tmp_path / "restore.sqlite3"
    Path(f"{dest}-wal").touch()
    with pytest.raises(FileExistsError):
        copy_database(jobs.store.path, dest, restore=True)
    assert not dest.exists()
