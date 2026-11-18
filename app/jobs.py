"""Durable maintenance jobs and fenced worker leases."""

from collections.abc import Callable
from hashlib import sha256
import json
import sqlite3
from time import time
from typing import TYPE_CHECKING, Literal
from uuid import uuid4

from pydantic import BaseModel, Field

from .batch_models import BatchPlan, PolicySnapshot
from .batch_runtime import BatchRuntime
from .batch_service import BatchService
from .models import Cluster, Operation, Plan, PolicySettings, Problem, State
from .policy import SafetyError
from .scheduler import build_plan
from .service import MaintenanceService, RecordNotFound, load_record
from .store import MaintenanceConflict


if TYPE_CHECKING:
    from .kube_jobs import LiveMaintenance


Kind = Literal["single", "batch", "kubernetes"]
Command = Literal["pause", "resume", "cancel"]
FaultHook = Callable[[str, "Job"], None]
TerminalState = Literal["succeeded", "failed", "rejected", "cancelled"]
TERMINAL = ("succeeded", "failed", "rejected", "cancelled")


class Job(BaseModel):
    id: str
    kind: Kind
    plan_id: str
    nodes: list[str]
    state: Literal["queued", "running", "retrying", "paused"] | TerminalState = "queued"
    checkpoint: int = 0
    total_steps: int | None = None
    simulated_seconds: int = 0
    retries: int = 0
    next_attempt: float = 0
    problem: Problem | None = None


class Checkpoint(BaseModel):
    snapshot_hash: str
    policy: PolicySnapshot
    single_policy: PolicySettings
    base_seconds: int
    batch: BatchPlan | None = None


class Lease(BaseModel):
    job_id: str
    worker_id: str
    fence: int


class WorkerHealth(BaseModel):
    id: str
    heartbeat: float
    expires: float
    healthy: bool


class JobPage(BaseModel):
    jobs: list[Job]
    next_cursor: int | None


class HistoryEvent(BaseModel):
    sequence: int
    timestamp: float
    event: str
    details: dict[str, str | int | float | bool] = Field(default_factory=dict)


class JobService:
    def __init__(
        self, single: MaintenanceService, batch: BatchService, *,
        clock: Callable[[], float] = time, lease_seconds: float = 2,
        fault: FaultHook | None = None,
    ) -> None:
        self.single = single
        self.batch = batch
        self.store = single.store
        self.clock = clock
        self.lease_seconds = lease_seconds
        self.fault = fault
        self.live: LiveMaintenance | None = None

    def initialize(self) -> None:
        with self.store.transaction(write=True) as db:
            db.execute("""CREATE TABLE IF NOT EXISTS jobs (
                id TEXT PRIMARY KEY, kind TEXT NOT NULL, plan_id TEXT NOT NULL,
                payload TEXT NOT NULL, prepared TEXT, worker TEXT,
                expiry REAL NOT NULL DEFAULT 0, fence INTEGER NOT NULL DEFAULT 0,
                UNIQUE(kind, plan_id)
            )""")
            db.execute("""CREATE TABLE IF NOT EXISTS job_owner (
                singleton INTEGER PRIMARY KEY CHECK(singleton = 1),
                job_id TEXT NOT NULL UNIQUE REFERENCES jobs(id)
            )""")
            db.execute("""CREATE TABLE IF NOT EXISTS job_history (
                sequence INTEGER PRIMARY KEY, job_id TEXT NOT NULL REFERENCES jobs(id),
                timestamp REAL NOT NULL, event TEXT NOT NULL, details TEXT NOT NULL
            )""")
            db.execute("CREATE INDEX IF NOT EXISTS job_history_lookup ON job_history(job_id, sequence)")
            db.execute("""CREATE TABLE IF NOT EXISTS job_workers (
                id TEXT PRIMARY KEY, heartbeat REAL NOT NULL, expires REAL NOT NULL
            )""")

    def event(self, db: sqlite3.Connection, job: Job, event: str, **details: str | int | float | bool) -> None:
        db.execute("INSERT INTO job_history(job_id, timestamp, event, details) VALUES (?, ?, ?, ?)",
                   (job.id, self.clock(), event, json.dumps(details)))

    @staticmethod
    def save(db: sqlite3.Connection, job: Job) -> None:
        db.execute("UPDATE jobs SET payload = ? WHERE id = ?", (job.model_dump_json(), job.id))

    @staticmethod
    def get(db: sqlite3.Connection, job_id: str) -> Job:
        return load_record(db, "SELECT payload FROM jobs WHERE id = ?", job_id, Job)

    def plan(self, db: sqlite3.Connection, kind: Kind, plan_id: str) -> BatchPlan | Plan:
        if kind == "batch":
            return load_record(db, "SELECT payload FROM batch_plans WHERE id = ?", plan_id, BatchPlan)
        return load_record(db, "SELECT payload FROM plans WHERE id = ?", plan_id, Plan)

    def submit(self, kind: Kind, plan_id: str) -> Job:
        if kind == "kubernetes":
            raise MaintenanceConflict("Kubernetes execution requires the live approval endpoint")
        with self.store.transaction(write=True) as db:
            return self.submit_in(db, kind, plan_id)

    def submit_in(self, db: sqlite3.Connection, kind: Literal["single", "batch"], plan_id: str) -> Job:
        row = db.execute("SELECT payload FROM jobs WHERE kind = ? AND plan_id = ?", (kind, plan_id)).fetchone()
        if row:
            return Job.model_validate_json(row[0])
        table = "batch_operations" if kind == "batch" else "operations"
        if db.execute(f"SELECT 1 FROM {table} WHERE plan_id = ?", (plan_id,)).fetchone():
            raise MaintenanceConflict("This plan already has a synchronous operation")
        plan = self.plan(db, kind, plan_id)
        nodes = plan.nodes if isinstance(plan, BatchPlan) else [plan.node]
        job = Job(id=uuid4().hex, kind=kind, plan_id=plan_id, nodes=nodes)
        db.execute("INSERT INTO jobs(id, kind, plan_id, payload) VALUES (?, ?, ?, ?)",
                   (job.id, kind, plan_id, job.model_dump_json()))
        self.event(db, job, "job.queued")
        return job

    def status(self, job_id: str) -> Job:
        with self.store.transaction() as db:
            return self.get(db, job_id)

    def list_jobs(self, limit: int, before: int | None = None) -> JobPage:
        with self.store.transaction() as db:
            rows = db.execute(
                "SELECT rowid, payload FROM jobs WHERE (? IS NULL OR rowid < ?) ORDER BY rowid DESC LIMIT ?",
                (before, before, limit + 1),
            ).fetchall()
            return JobPage(
                jobs=[Job.model_validate_json(row[1]) for row in rows[:limit]],
                next_cursor=rows[limit - 1][0] if len(rows) > limit else None,
            )

    def history(self, job_id: str, after: int = 0, limit: int = 100) -> list[HistoryEvent]:
        with self.store.transaction() as db:
            self.get(db, job_id)
            return [
                HistoryEvent(sequence=sequence, timestamp=timestamp, event=event, details=json.loads(details))
                for sequence, timestamp, event, details in db.execute(
                    """SELECT sequence, timestamp, event, details FROM job_history
                       WHERE job_id = ? AND sequence > ? ORDER BY sequence LIMIT ?""",
                    (job_id, after, limit),
                )
            ]

    def control(self, job_id: str, command: Command) -> Job:
        with self.store.transaction(write=True) as db:
            job = self.get(db, job_id)
            if job.kind == "kubernetes":
                raise MaintenanceConflict("Live jobs reconcile and release owned cordons automatically")
            target = {"pause": "paused", "resume": "queued", "cancel": "cancelled"}[command]
            if job.state == target:
                return job
            if job.state in TERMINAL:
                raise MaintenanceConflict(f"Job is already {job.state}")
            if command == "resume" and job.state != "paused":
                raise MaintenanceConflict("Only paused jobs can be resumed")
            job.state = target
            self.save(db, job)
            db.execute("UPDATE jobs SET worker = NULL, expiry = 0, fence = fence + 1 WHERE id = ?", (job.id,))
            if command == "cancel":
                db.execute("DELETE FROM job_owner WHERE job_id = ?", (job.id,))
            self.event(db, job, f"job.{target}", checkpoint=job.checkpoint)
            return job

    def reconcile(self, db: sqlite3.Connection, job: Job, prepared: Checkpoint) -> bool:
        if self.snapshot_hash(db) != prepared.snapshot_hash:
            self.finish(db, job, "failed", Problem(
                code="cluster_drift", message="Cluster differs from the committed checkpoint; inspect before replanning",
            ))
            return False
        return True

    @staticmethod
    def snapshot_hash(db: sqlite3.Connection) -> str:
        payload, = db.execute("SELECT payload FROM cluster WHERE id = 1").fetchone()
        return sha256(payload.encode()).hexdigest()

    def heartbeat(self, worker_id: str) -> None:
        with self.store.transaction(write=True) as db:
            now = self.clock()
            db.execute("""INSERT INTO job_workers VALUES (?, ?, ?)
                          ON CONFLICT(id) DO UPDATE SET heartbeat = excluded.heartbeat, expires = excluded.expires""",
                       (worker_id, now, now + self.lease_seconds))

            db.execute("UPDATE jobs SET expiry = ? WHERE worker = ? AND expiry > ?",
                       (now + self.lease_seconds, worker_id, now))

    def workers(self) -> list[WorkerHealth]:
        with self.store.transaction() as db:
            now = self.clock()
            return [WorkerHealth(id=worker, heartbeat=heartbeat, expires=expiry, healthy=expiry > now)
                    for worker, heartbeat, expiry in db.execute(
                        "SELECT id, heartbeat, expires FROM job_workers ORDER BY heartbeat DESC LIMIT 100")]

    def release(self, worker_id: str) -> None:
        with self.store.transaction(write=True) as db:
            db.execute("UPDATE job_workers SET expires = 0 WHERE id = ?", (worker_id,))
            db.execute("UPDATE jobs SET expiry = 0 WHERE worker = ?", (worker_id,))

    def recover(self) -> int:
        with self.store.transaction(write=True) as db:
            return self.recover_expired(db, self.clock())

    def recover_expired(self, db: sqlite3.Connection, now: float) -> int:
        rows = db.execute("""SELECT payload, prepared FROM jobs
                             WHERE worker IS NOT NULL AND expiry <= ?""", (now,)).fetchall()
        for payload, prepared in rows:
            job = Job.model_validate_json(payload)
            db.execute("UPDATE jobs SET worker = NULL, expiry = 0, fence = fence + 1 WHERE id = ?", (job.id,))
            if prepared and not self.reconcile(db, job, Checkpoint.model_validate_json(prepared)):
                continue
            job.state = "queued"
            self.save(db, job)
            self.event(db, job, "job.recovered", checkpoint=job.checkpoint)
        return len(rows)

    def claim(self, worker_id: str) -> Lease | None:
        with self.store.transaction(write=True) as db:
            now = self.clock()
            self.recover_expired(db, now)
            row = db.execute("""SELECT jobs.payload, jobs.fence FROM job_owner JOIN jobs
                                ON job_owner.job_id = jobs.id""").fetchone()
            if row is None:
                row = db.execute("""SELECT payload, fence FROM jobs
                                    WHERE json_extract(payload, '$.state') IN ('queued', 'retrying')
                                    AND (kind != 'kubernetes' OR ?)
                                    ORDER BY rowid LIMIT 1""", (self.live is not None,)).fetchone()
            if row is None:
                return None
            job = Job.model_validate_json(row[0])
            if job.kind == "kubernetes" and self.live is None:
                return None
            if job.state not in {"queued", "retrying"} or job.next_attempt > now:
                return None
            fence = row[1] + 1
            db.execute("INSERT OR IGNORE INTO job_owner VALUES (1, ?)", (job.id,))
            db.execute("UPDATE jobs SET worker = ?, expiry = ?, fence = ? WHERE id = ?",
                       (worker_id, now + self.lease_seconds, fence, job.id))
            job.state = "running"
            self.save(db, job)
            self.event(db, job, "job.claimed", worker=worker_id, fence=fence)
            return Lease(job_id=job.id, worker_id=worker_id, fence=fence)

    def run(self, lease: Lease) -> bool:
        with self.store.transaction(write=True) as db:
            row = db.execute("""SELECT jobs.payload, jobs.prepared FROM jobs JOIN job_owner ON job_owner.job_id = jobs.id
                                WHERE jobs.id = ? AND worker = ? AND fence = ? AND expiry > ?""",
                             (lease.job_id, lease.worker_id, lease.fence, self.clock())).fetchone()
            if row is None:
                return False
            job = Job.model_validate_json(row[0])
            if job.kind == "kubernetes":
                if self.live is None:
                    return False
                self.live.step(db, job)
                db.execute("UPDATE jobs SET worker = NULL, expiry = 0 WHERE id = ?", (job.id,))
                return True
            prepared = Checkpoint.model_validate_json(row[1]) if row[1] else None
            if prepared and not self.reconcile(db, job, prepared):
                return True

            settings = prepared.policy if prepared else self.batch.read_policy(db)
            db.execute("SAVEPOINT job_step")
            try:
                self.inject("before_step", job)
                if prepared is None:
                    self.prepare(db, job, settings)
                else:
                    self.apply(db, job, prepared)
            except (sqlite3.OperationalError, ConnectionError) as error:
                db.execute("ROLLBACK TO job_step")
                job = Job.model_validate_json(row[0])
                self.retry(db, job, settings, error)
            except SafetyError as error:
                db.execute("ROLLBACK TO job_step")
                job = Job.model_validate_json(row[0])
                self.finish(db, job, "failed" if prepared else "rejected", error.problem)
            db.execute("RELEASE job_step")
            db.execute("UPDATE jobs SET worker = NULL, expiry = 0 WHERE id = ?", (job.id,))
        self.inject("after_commit", job)
        return True

    def inject(self, point: str, job: Job) -> None:
        if self.fault:
            self.fault(point, job)

    def prepare(self, db: sqlite3.Connection, job: Job, settings: PolicySnapshot) -> None:
        original = self.plan(db, job.kind, job.plan_id)
        cluster = self.store.snapshot(db)
        prepared = Checkpoint(
            snapshot_hash=self.snapshot_hash(db), policy=settings, single_policy=self.single.policy.settings,
            base_seconds=cluster.simulated_seconds,
        )
        if isinstance(original, BatchPlan):
            for name in original.nodes:
                if name in cluster.nodes and cluster.nodes[name].last_maintenance_revision > original.cluster_revision:
                    raise SafetyError("conflicting_operation", f"{name} was maintained after planning")
            current = build_plan(cluster, original.nodes, self.batch.policy, settings, self.batch.clock())
            if not current.safe:
                problem = current.problems[0]
                raise SafetyError(problem.code, problem.message)
            prepared.batch = current
            job.total_steps = len(current.waves)
        else:
            job.total_steps = 1
        db.execute("UPDATE jobs SET prepared = ? WHERE id = ?", (prepared.model_dump_json(), job.id))
        self.ready(db, job)
        self.event(db, job, "job.prepared", steps=job.total_steps)

    def apply(self, db: sqlite3.Connection, job: Job, prepared: Checkpoint) -> None:
        if prepared.single_policy != self.single.policy.settings or (
            prepared.batch is not None and prepared.policy != self.batch.read_policy(db)
        ):
            raise SafetyError("policy_changed", "Policy changed during maintenance; create a fresh plan")
        cluster = self.store.snapshot(db)
        if prepared.batch is None:
            original = self.plan(db, "single", job.plan_id)
            operation = Operation(id=job.id, plan_id=job.plan_id, node=job.nodes[0])
            events = self.single._execute_plan(original, cluster, operation)
            if operation.state == State.SUCCEEDED:
                for event in events:
                    self.event(db, job, event.event, node=event.node, simulated_seconds=event.simulated_seconds,
                               **event.details)
                    if event.event == "pod.evicted":
                        self.inject("during_eviction", job)
            else:
                self.finish(db, job, "rejected" if operation.state == State.REJECTED else "failed",
                            operation.problem)
                return
            targets = job.nodes
        else:
            targets = self.apply_wave(db, job, cluster, prepared.batch, prepared.policy)
        cluster.revision += 1
        for name in targets:
            cluster.nodes[name].last_maintenance_revision = cluster.revision
        db.execute("UPDATE cluster SET payload = ? WHERE id = 1", (cluster.model_dump_json(),))
        self.inject("after_cluster_write", job)
        prepared.snapshot_hash = self.snapshot_hash(db)
        db.execute("UPDATE jobs SET prepared = ? WHERE id = ?", (prepared.model_dump_json(), job.id))
        job.checkpoint += 1
        job.simulated_seconds = cluster.simulated_seconds - prepared.base_seconds
        self.event(db, job, "job.checkpoint", checkpoint=job.checkpoint, revision=cluster.revision)
        if job.checkpoint == job.total_steps:
            self.finish(db, job, "succeeded")
        else:
            self.ready(db, job)

    def apply_wave(
        self, db: sqlite3.Connection, job: Job, cluster: Cluster, plan: BatchPlan, settings: PolicySnapshot,
    ) -> list[str]:
        policy = settings.settings
        budget = policy.time_budget(self.batch.clock())
        remaining = min(budget or 0, policy.operation_deadline_seconds - job.simulated_seconds)
        if remaining <= 0:
            raise SafetyError("operation_deadline", "No maintenance window or deadline budget remains")
        wave = plan.waves[job.checkpoint]

        def record(event: str, node: str, details: dict) -> None:
            self.event(db, job, event, node=node, simulated_seconds=cluster.simulated_seconds, **details)
            if event == "pod.evicted":
                self.inject("during_eviction", job)

        if job.checkpoint == 0:
            self.event(db, job, "job.started", simulated_seconds=cluster.simulated_seconds)
        BatchRuntime(cluster, self.batch.policy, policy, remaining, record).run_wave(wave)
        return wave.nodes

    def ready(self, db: sqlite3.Connection, job: Job) -> None:
        job.state = "queued"
        job.retries = 0
        job.next_attempt = 0
        job.problem = None
        self.save(db, job)

    def finish(self, db: sqlite3.Connection, job: Job, state: TerminalState, problem: Problem | None = None) -> None:
        job.state = state
        job.problem = problem
        job.next_attempt = 0
        self.save(db, job)
        db.execute("DELETE FROM job_owner WHERE job_id = ?", (job.id,))
        db.execute("UPDATE jobs SET worker = NULL, expiry = 0 WHERE id = ?", (job.id,))
        self.event(db, job, f"job.{state}", code=problem.code if problem else "", checkpoint=job.checkpoint)

    def retry(self, db: sqlite3.Connection, job: Job, settings: PolicySnapshot, error: Exception) -> None:
        policy = settings.settings
        job.retries += 1
        code = "database_error" if isinstance(error, sqlite3.OperationalError) else "network_interruption"
        if job.retries > policy.retry_limit:
            self.finish(db, job, "failed", Problem(code="retry_exhausted", message=f"Retry limit reached after {code}"))
            return
        delay = min(policy.backoff_max_seconds, policy.backoff_base_seconds * 2 ** (job.retries - 1))
        job.state = "retrying"
        job.next_attempt = self.clock() + delay
        job.problem = Problem(code=code, message="Simulation step rolled back and will be retried")
        self.save(db, job)
        self.event(db, job, "job.retry_scheduled", attempt=job.retries, delay=delay, code=code)
