from collections.abc import Callable
from datetime import UTC, datetime
import sqlite3
from uuid import uuid4

from .batch_models import BatchOperation, BatchPlan, BatchPolicy, PolicySnapshot
from .batch_runtime import BatchRuntime
from .models import AuditEvent, Cluster, Problem, State
from .policy import MaintenancePolicy, SafetyError
from .scheduler import build_plan
from .service import load_record, logger
from .simulator import EventDetails
from .store import Store
from .telemetry import simulated_eviction_event, sync_operation


def utc_now() -> datetime:
    return datetime.now(UTC)


class BatchService:
    def __init__(
        self, store: Store, policy: MaintenancePolicy, defaults: BatchPolicy,
        clock: Callable[[], datetime] = utc_now,
    ) -> None:
        self.store = store
        self.policy = policy
        self.defaults = defaults
        self.clock = clock

    def initialize(self) -> None:
        with self.store.transaction(write=True) as db:
            db.execute("CREATE TABLE IF NOT EXISTS batch_policy (id INTEGER PRIMARY KEY CHECK (id = 1), payload TEXT NOT NULL)")
            db.execute("CREATE TABLE IF NOT EXISTS batch_plans (id TEXT PRIMARY KEY, payload TEXT NOT NULL)")
            db.execute("""CREATE TABLE IF NOT EXISTS batch_operations (
                id TEXT PRIMARY KEY, plan_id TEXT NOT NULL UNIQUE REFERENCES batch_plans(id),
                payload TEXT NOT NULL
            )""")
            defaults = PolicySnapshot(settings=self.defaults)
            db.execute("INSERT OR IGNORE INTO batch_policy VALUES (1, ?)", (defaults.model_dump_json(),))

    @staticmethod
    def read_policy(db: sqlite3.Connection) -> PolicySnapshot:
        row = db.execute("SELECT payload FROM batch_policy WHERE id = 1").fetchone()
        return PolicySnapshot.model_validate_json(row[0])

    def get_policy(self) -> PolicySnapshot:
        with self.store.transaction() as db:
            return self.read_policy(db)

    def update_policy(self, settings: BatchPolicy) -> PolicySnapshot:
        with self.store.transaction(write=True) as db:
            previous = self.read_policy(db)
            if settings == previous.settings:
                return previous
            updated = PolicySnapshot(revision=previous.revision + 1, settings=settings)
            db.execute("UPDATE batch_policy SET payload = ? WHERE id = 1", (updated.model_dump_json(),))
        logger.info("batch.policy_updated", extra={"fields": {"policy_revision": updated.revision}})
        return updated

    def create_plan(self, nodes: list[str]) -> BatchPlan:
        with self.store.transaction(write=True) as db:
            plan = self.plan_for(db, nodes)
        logger.info("batch.plan_created", extra={"fields": {
            "plan_id": plan.id, "nodes": nodes, "safe": plan.safe,
            "cluster_revision": plan.cluster_revision, "policy_revision": plan.policy_revision,
        }})
        return plan

    def plan_for(self, db: sqlite3.Connection, nodes: list[str]) -> BatchPlan:
        plan = build_plan(self.store.snapshot(db), nodes, self.policy, self.read_policy(db), self.clock())
        db.execute("INSERT INTO batch_plans VALUES (?, ?)", (plan.id, plan.model_dump_json()))
        return plan

    def preview(self, plan_id: str) -> BatchPlan:
        with self.store.transaction(write=True) as db:
            original = load_record(db, "SELECT payload FROM batch_plans WHERE id = ?", plan_id, BatchPlan)
            return self.plan_for(db, original.nodes)

    @sync_operation("batch")
    def execute(self, plan_id: str) -> BatchOperation:
        with self.store.transaction(write=True) as db:
            previous = db.execute(
                "SELECT payload FROM batch_operations WHERE plan_id = ?", (plan_id,),
            ).fetchone()
            if previous is not None:
                return BatchOperation.model_validate_json(previous[0])
            self.store.check_synchronous_write(db, "batch", plan_id)
            original = load_record(db, "SELECT payload FROM batch_plans WHERE id = ?", plan_id, BatchPlan)
            cluster = self.store.snapshot(db)
            result = self.run(original, cluster, self.read_policy(db))
            if result.state == State.SUCCEEDED:
                cluster.revision += 1
                for target in original.nodes:
                    cluster.nodes[target].last_maintenance_revision = cluster.revision
                db.execute("UPDATE cluster SET payload = ? WHERE id = 1", (cluster.model_dump_json(),))
            db.execute("INSERT INTO batch_operations VALUES (?, ?, ?)", (result.id, plan_id, result.model_dump_json()))

        for event in result.audit:
            logger.info(event.event, extra={"fields": event.model_dump(mode="json")})
        return result

    def status(self, operation_id: str) -> BatchOperation:
        with self.store.transaction() as db:
            return load_record(db, "SELECT payload FROM batch_operations WHERE id = ?", operation_id, BatchOperation)

    def run(self, original: BatchPlan, cluster: Cluster, settings: PolicySnapshot) -> BatchOperation:
        result = BatchOperation(id=uuid4().hex, plan_id=original.id, effective_plan=original)

        def record(event: str, node: str, details: EventDetails) -> None:
            result.audit.append(AuditEvent(
                timestamp=self.clock().isoformat(), event=event, operation_id=result.id,
                node=node, simulated_seconds=cluster.simulated_seconds, details=details,
            ))

        def transition(state: State, problem: Problem | None = None) -> None:
            result.state = state
            result.problem = problem
            details: EventDetails = {"state": state.value}
            if problem:
                details["code"] = problem.code
            record("batch.state_changed", "batch", details)

        transition(State.VALIDATING)
        conflicts = [
            name for name in original.nodes
            if name in cluster.nodes and cluster.nodes[name].last_maintenance_revision > original.cluster_revision
        ]
        if conflicts:
            transition(State.REJECTED, Problem(
                code="conflicting_operation",
                message=f"Maintenance already completed after planning for {', '.join(conflicts)}",
            ))
            return result

        now = self.clock()
        current = build_plan(cluster, original.nodes, self.policy, settings, now)
        if current.cluster_revision != original.cluster_revision:
            result.replan_reasons.append("stale_cluster_revision")
        if current.policy_revision != original.policy_revision:
            result.replan_reasons.append("stale_policy_revision")
        if not result.replan_reasons and (
            current.waves != original.waves
            or current.priorities != original.priorities
            or current.problems != original.problems
            or current.alternatives_rejected != original.alternatives_rejected
            or current.safe != original.safe
        ):
            result.replan_reasons.append("scheduling_inputs_changed")
        if result.replan_reasons:
            result.replanned = True
            result.effective_plan = current
            record("batch.replanned", "batch", {
                "reasons": ", ".join(result.replan_reasons), "effective_plan_id": current.id,
            })
        if not current.safe:
            transition(State.REJECTED, current.problems[0])
            return result

        budget = settings.settings.time_budget(now)
        if budget is None:
            raise RuntimeError("Safe batch plan requires an open maintenance window")
        runtime = BatchRuntime(cluster, self.policy, settings.settings, budget, record)
        transition(State.DRAINING)
        try:
            runtime.run(current)
        except SafetyError as error:
            record("simulation.rolled_back", "batch", {"code": error.problem.code})
            transition(State.FAILED, error.problem)
        else:
            transition(State.SUCCEEDED)
        result.simulated_elapsed_seconds = runtime.elapsed
        return result
