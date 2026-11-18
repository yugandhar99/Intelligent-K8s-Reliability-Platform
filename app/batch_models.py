from datetime import datetime, time, timedelta
from typing import Annotated

from pydantic import BaseModel, ConfigDict, Field, model_validator

from .models import AuditEvent, Placement, Problem, State


class MaintenanceWindow(BaseModel):
    model_config = ConfigDict(extra="forbid")

    start: time
    end: time

    @model_validator(mode="after")
    def validate_times(self) -> "MaintenanceWindow":
        if self.start.tzinfo or self.end.tzinfo or self.start == self.end:
            raise ValueError("use distinct UTC start and end times without offsets")
        return self

    def remaining(self, now: datetime) -> int | None:
        for offset in (0, -1):
            date = now.date() + timedelta(days=offset)
            start = datetime.combine(date, self.start, now.tzinfo)
            end_date = date + timedelta(days=int(self.end < self.start))
            end = datetime.combine(end_date, self.end, now.tzinfo)
            if start <= now < end:
                return int((end - now).total_seconds())
        return None


class BatchPolicy(BaseModel):
    model_config = ConfigDict(extra="forbid")

    max_unavailable_nodes: Annotated[int, Field(ge=1, le=32)] = 2
    parallel_limit: Annotated[int, Field(ge=1, le=8)] = 2
    operation_deadline_seconds: Annotated[int, Field(ge=1, le=86400)] = 180
    retry_limit: Annotated[int, Field(ge=0, le=10)] = 3
    backoff_base_seconds: Annotated[int, Field(ge=1, le=60)] = 2
    backoff_max_seconds: Annotated[int, Field(ge=1, le=600)] = 16
    search_limit: Annotated[int, Field(ge=1, le=100000)] = 10000
    windows: Annotated[list[MaintenanceWindow], Field(max_length=24)] = Field(default_factory=list)

    @model_validator(mode="after")
    def validate_backoff(self) -> "BatchPolicy":
        if self.backoff_max_seconds < self.backoff_base_seconds:
            raise ValueError("backoff_max_seconds must be at least backoff_base_seconds")
        return self

    def time_budget(self, now: datetime) -> int | None:
        if not self.windows:
            return self.operation_deadline_seconds
        open_windows = [remaining for window in self.windows if (remaining := window.remaining(now)) is not None]
        return min(self.operation_deadline_seconds, max(open_windows)) if open_windows else None


class PolicySnapshot(BaseModel):
    revision: int = 0
    settings: BatchPolicy


class BatchRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    nodes: Annotated[list[Annotated[str, Field(min_length=1, max_length=200)]], Field(min_length=1, max_length=32)]

    @model_validator(mode="after")
    def unique_targets(self) -> "BatchRequest":
        if len(set(self.nodes)) != len(self.nodes):
            raise ValueError("node names must be unique")
        return self


class NodePriority(BaseModel):
    node: str
    priority_score: float
    risk_score: float
    health_deficit: float
    utilization: float
    criticality: float
    operational_risk: float


class CapacityProjection(BaseModel):
    node: str
    cpu_millis_used: int
    cpu_millis_free: int
    memory_mib_used: int
    memory_mib_free: int


class BatchWave(BaseModel):
    nodes: list[str]
    placements: list[Placement]
    estimated_seconds: int
    capacity: list[CapacityProjection]


class BatchPlan(BaseModel):
    id: str
    nodes: list[str]
    cluster_revision: int
    policy_revision: int
    safe: bool
    waves: list[BatchWave] = Field(default_factory=list)
    priorities: list[NodePriority] = Field(default_factory=list)
    problems: list[Problem] = Field(default_factory=list)
    estimated_seconds: int = 0
    search_steps: int = 0
    alternatives_rejected: list[Problem] = Field(default_factory=list)


class BatchOperation(BaseModel):
    id: str
    plan_id: str
    state: State = State.PENDING
    effective_plan: BatchPlan
    replanned: bool = False
    replan_reasons: list[str] = Field(default_factory=list)
    simulated_elapsed_seconds: int = 0
    problem: Problem | None = None
    audit: list[AuditEvent] = Field(default_factory=list)
