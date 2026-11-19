from datetime import UTC, datetime
import logging
import sqlite3
from typing import TypeVar
from uuid import uuid4

from pydantic import BaseModel

from .models import AuditEvent, Cluster, Operation, Plan, State
from .policy import MaintenancePolicy, SafetyError
from .simulator import EventDetails, simulate_maintenance
from .store import Store
from .telemetry import simulated_eviction_event, sync_operation


logger = logging.getLogger("maintenance")
Record = TypeVar("Record", bound=BaseModel)

TRANSITIONS = {
    State.PENDING: {State.VALIDATING},
    State.VALIDATING: {State.CORDONED, State.REJECTED},
    State.CORDONED: {State.DRAINING, State.FAILED},
    State.DRAINING: {State.VALIDATING_READINESS, State.FAILED},
    State.VALIDATING_READINESS: {State.UNCORDONING, State.FAILED},
    State.UNCORDONING: {State.SUCCEEDED, State.FAILED},
}


class RecordNotFound(Exception):
    pass


def load_record(
    connection: sqlite3.Connection, query: str, key: str, model: type[Record]
) -> Record:
    row = connection.execute(query, (key,)).fetchone()
    if row is None:
        raise RecordNotFound(f"{model.__name__} {key} was not found")
    return model.model_validate_json(row[0])


class MaintenanceService:
    def __init__(self, store: Store, policy: MaintenancePolicy) -> None:
        self.store = store
        self.policy = policy

    def create_plan(self, node: str) -> Plan:
        with self.store.transaction(write=True) as connection:
            cluster = self.store.snapshot(connection)
            plan = self.plan_for(cluster, node)
            connection.execute("INSERT INTO plans VALUES (?, ?)", (plan.id, plan.model_dump_json()))
        logger.info("maintenance.plan_created", extra={"fields": {
            "plan_id": plan.id, "node": node, "safe": plan.safe,
            "cluster_revision": plan.cluster_revision,
            "problem_codes": [problem.code for problem in plan.problems],
        }})
        return plan

    def plan_for(self, cluster: Cluster, node: str) -> Plan:
        plan = Plan(id=uuid4().hex, node=node, cluster_revision=cluster.revision,
                    safe=True, placements=[], problems=[])
        try:
            plan.placements = simulate_maintenance(cluster, node, self.policy)
        except SafetyError as error:
            plan.safe = False
            plan.problems.append(error.problem)
        return plan

    def preview(self, plan_id: str) -> Plan:
        with self.store.transaction() as connection:
            plan = load_record(connection, "SELECT payload FROM plans WHERE id = ?", plan_id, Plan)
        return self.create_plan(plan.node)

    @sync_operation("single")
    def execute(self, plan_id: str) -> Operation:
        with self.store.transaction(write=True) as connection:
            previous = connection.execute(
                "SELECT payload FROM operations WHERE plan_id = ?", (plan_id,)
            ).fetchone()
            if previous is not None:
                return Operation.model_validate_json(previous[0])

            self.store.check_synchronous_write(connection, "single", plan_id)
            plan = load_record(connection, "SELECT payload FROM plans WHERE id = ?", plan_id, Plan)
            cluster = self.store.snapshot(connection)
            operation = Operation(id=uuid4().hex, plan_id=plan.id, node=plan.node)
            events = self._execute_plan(plan, cluster, operation)
            if operation.state == State.SUCCEEDED:
                cluster.revision += 1
                cluster.nodes[plan.node].last_maintenance_revision = cluster.revision
                connection.execute("UPDATE cluster SET payload = ? WHERE id = 1", (cluster.model_dump_json(),))
            connection.execute("INSERT INTO operations VALUES (?, ?, ?)", (operation.id, plan.id, operation.model_dump_json()))
            connection.executemany(
                "INSERT INTO audit (operation_id, payload) VALUES (?, ?)",
                [(operation.id, event.model_dump_json()) for event in events],
            )
        for event in events:
            simulated_eviction_event(event.event, event.details)
            logger.info(event.event, extra={"fields": event.model_dump(mode="json")})
        return operation

    def _execute_plan(self, plan: Plan, cluster: Cluster, operation: Operation) -> list[AuditEvent]:
        events: list[AuditEvent] = []

        def record(event: str, seconds: int, details: EventDetails) -> None:
            events.append(AuditEvent(
                timestamp=datetime.now(UTC).isoformat(), event=event,
                operation_id=operation.id, node=operation.node,
                simulated_seconds=seconds, details=details,
            ))

        def transition(state: State) -> None:
            if state not in TRANSITIONS.get(operation.state, set()):
                raise RuntimeError(f"Invalid transition from {operation.state} to {state}")
            operation.state = state
            record("operation.state_changed", cluster.simulated_seconds, {"state": state.value})

        record("operation.created", cluster.simulated_seconds, {"plan_id": plan.id})
        transition(State.VALIDATING)
        try:
            if plan.cluster_revision != cluster.revision:
                raise SafetyError("stale_plan", "The cluster revision changed. Create a fresh plan.")
            if not plan.safe:
                problem = plan.problems[0]
                raise SafetyError(problem.code, problem.message)
            placements = simulate_maintenance(cluster.model_copy(deep=True), plan.node, self.policy)
            if placements != plan.placements:
                raise SafetyError("plan_changed", "Safety revalidation changed the placements. Create a fresh plan.")
        except SafetyError as error:
            operation.problem = error.problem
            record("operation.rejected", cluster.simulated_seconds, {"code": error.problem.code})
            transition(State.REJECTED)
            return events

        try:
            simulate_maintenance(cluster, plan.node, self.policy, executing=True,
                                 change_state=transition, record=record)
        except SafetyError as error:
            operation.problem = error.problem
            record("simulation.rolled_back", cluster.simulated_seconds, {"code": error.problem.code})
            transition(State.FAILED)
        else:
            transition(State.SUCCEEDED)
        return events

    def status(self, operation_id: str) -> Operation:
        with self.store.transaction() as connection:
            return load_record(connection, "SELECT payload FROM operations WHERE id = ?", operation_id, Operation)

    def audit(self, operation_id: str) -> list[AuditEvent]:
        with self.store.transaction() as connection:
            load_record(connection, "SELECT payload FROM operations WHERE id = ?", operation_id, Operation)
            return [
                AuditEvent.model_validate_json(row[0])
                for row in connection.execute(
                    "SELECT payload FROM audit WHERE operation_id = ? ORDER BY sequence", (operation_id,)
                )
            ]

    def cluster(self) -> Cluster:
        with self.store.transaction() as connection:
            return self.store.snapshot(connection)
