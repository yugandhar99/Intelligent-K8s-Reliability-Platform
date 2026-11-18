from typing import Annotated

from fastapi import APIRouter, Query
from pydantic import BaseModel, ConfigDict, Field

from .jobs import Command, HistoryEvent, Job, JobPage, JobService, Kind, WorkerHealth


class JobRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    kind: Kind
    plan_id: Annotated[str, Field(min_length=1, max_length=200)]


def job_router(service: JobService) -> APIRouter:
    router = APIRouter(prefix="/maintenance", tags=["durable maintenance"])

    @router.post("/jobs", status_code=202)
    def submit(request: JobRequest) -> Job:
        return service.submit(request.kind, request.plan_id)

    @router.get("/jobs")
    def jobs(limit: Annotated[int, Query(ge=1, le=100)] = 50,
             before: Annotated[int | None, Query(ge=1)] = None) -> JobPage:
        return service.list_jobs(limit, before)

    @router.get("/jobs/{job_id}")
    def job(job_id: str) -> Job:
        return service.status(job_id)

    @router.get("/jobs/{job_id}/history")
    def history(job_id: str, after: Annotated[int, Query(ge=0)] = 0,
                limit: Annotated[int, Query(ge=1, le=100)] = 100) -> list[HistoryEvent]:
        return service.history(job_id, after, limit)

    @router.post("/jobs/{job_id}/{command}")
    def control(job_id: str, command: Command) -> Job:
        return service.control(job_id, command)

    @router.post("/recovery")
    def recover() -> dict[str, int]:
        return {"reconciled_leases": service.recover()}

    @router.get("/workers")
    def workers() -> list[WorkerHealth]:
        return service.workers()

    return router
