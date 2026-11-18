from enum import StrEnum
from typing import Annotated, Literal

from pydantic import BaseModel, Field, StrictBool, model_validator


NonNegative = Annotated[int, Field(ge=0)]


class Resources(BaseModel):
    cpu_millis: NonNegative
    memory_mib: NonNegative

    def fits(self, capacity: "Resources") -> bool:
        return (
            self.cpu_millis <= capacity.cpu_millis
            and self.memory_mib <= capacity.memory_mib
        )


class Taint(BaseModel):
    key: str
    value: str = ""
    effect: Literal["NoSchedule", "NoExecute", "PreferNoSchedule"] = "NoSchedule"


class Toleration(BaseModel):
    key: str
    value: str = ""
    effect: Literal["NoSchedule", "NoExecute", "PreferNoSchedule"] | None = None
    operator: Literal["Equal", "Exists"] = "Equal"

    def accepts(self, taint: Taint) -> bool:
        return (
            (self.key == taint.key or (self.operator == "Exists" and not self.key))
            and (self.effect is None or self.effect == taint.effect)
            and (self.operator == "Exists" or self.value == taint.value)
        )


class PodAffinity(BaseModel):
    topology_key: str = "kubernetes.io/hostname"
    workloads: Annotated[set[str], Field(min_length=1)]


class Node(BaseModel):
    name: str
    allocatable: Resources
    schedulable: bool = True
    ready: bool = True
    labels: dict[str, str] = Field(default_factory=dict)
    taints: list[Taint] = Field(default_factory=list)
    health_score: Annotated[int, Field(ge=0, le=100)] = 100
    transient_cordon_failures: NonNegative = 0
    fails_during_maintenance: bool = False
    last_maintenance_revision: NonNegative = 0

    @model_validator(mode="after")
    def add_hostname(self) -> "Node":
        self.labels.setdefault("kubernetes.io/hostname", self.name)
        return self


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
    node_selector: dict[str, str] = Field(default_factory=dict)
    node_affinity: list[dict[str, str]] = Field(default_factory=list)
    pod_affinity: list[PodAffinity] = Field(default_factory=list)
    pod_anti_affinity: list[PodAffinity] = Field(default_factory=list)
    tolerations: list[Toleration] = Field(default_factory=list)
    transient_eviction_failures: NonNegative = 0


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
