from enum import StrEnum
from typing import Annotated

from pydantic import BaseModel, Field, StrictBool


NonNegative = Annotated[int, Field(ge=0)]


class Resources(BaseModel):
    cpu_millis: NonNegative
    memory_mib: NonNegative

    def fits(self, capacity: "Resources") -> bool:
        return (
            self.cpu_millis <= capacity.cpu_millis
            and self.memory_mib <= capacity.memory_mib
        )


class Node(BaseModel):
    name: str
    allocatable: Resources
    schedulable: bool = True
    ready: bool = True


class Workload(BaseModel):
    namespace: str
    name: str
    min_available: NonNegative
    critical: bool = False


class PodPhase(StrEnum):
    READY = "ready"
    STARTING = "starting"
    TERMINATING = "terminating"


class Pod(BaseModel):
    name: str
    workload: str
    node: str
    requests: Resources
    phase: PodPhase = PodPhase.READY
    grace_seconds: NonNegative = 5
    startup_seconds: NonNegative = 8
    termination_stalls: bool = False
    readiness_fails: bool = False


class DisruptionBudget(BaseModel):
    name: str
    namespace: str
    workloads: set[str]
    min_available: NonNegative


class Cluster(BaseModel):
    nodes: dict[str, Node]
    workloads: dict[str, Workload]
    pods: dict[str, Pod]
    budgets: list[DisruptionBudget]
    revision: NonNegative = 0
    simulated_seconds: NonNegative = 0


class PolicySettings(BaseModel):
    protected_namespaces: set[str]
    termination_timeout_seconds: NonNegative
    readiness_timeout_seconds: NonNegative


class Placement(BaseModel):
    pod: str
    destination: str


class Problem(BaseModel):
    code: str
    message: str


class Plan(BaseModel):
    id: str
    node: str
    cluster_revision: int
    safe: bool
    placements: list[Placement]
    problems: list[Problem]


class PlanRequest(BaseModel):
    node: Annotated[str, Field(min_length=1, max_length=200)]


class ExecuteRequest(BaseModel):
    plan_id: Annotated[str, Field(min_length=1, max_length=200)]
    dry_run: StrictBool = True


class State(StrEnum):
    PENDING = "pending"
    VALIDATING = "validating"
    CORDONED = "cordoned"
    DRAINING = "draining"
    VALIDATING_READINESS = "validating_readiness"
    UNCORDONING = "uncordoning"
    SUCCEEDED = "succeeded"
    REJECTED = "rejected"
    FAILED = "failed"


class Operation(BaseModel):
    id: str
    plan_id: str
    node: str
    state: State = State.PENDING
    problem: Problem | None = None


class AuditEvent(BaseModel):
    timestamp: str
    event: str
    operation_id: str
    node: str
    simulated_seconds: int
    details: dict[str, str | int | bool]
