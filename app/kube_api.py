"""Opt-in discovery, dry-run planning, and explicit approval for the lab."""

from hmac import compare_digest
from typing import Annotated

from fastapi import APIRouter, Header, HTTPException
from pydantic import BaseModel, ConfigDict, Field, StrictBool, field_validator

from .jobs import Job
from .kube_jobs import LiveMaintenance


class DryRun(BaseModel):
    model_config = ConfigDict(extra="forbid")
    node: Annotated[str, Field(min_length=1, max_length=253)]


class Approval(BaseModel):
    model_config = ConfigDict(extra="forbid")
    plan_id: Annotated[str, Field(pattern=r"^[0-9a-f]{32}$")]
    approver: Annotated[str, Field(min_length=1, max_length=100, pattern=r"\S")]
    approved: StrictBool

    @field_validator("approved")
    @classmethod
    def explicit_approval(cls, value: bool) -> bool:
        if not value:
            raise ValueError("Explicit approval is required")
        return value


def live_router(service: LiveMaintenance, approval_token: str) -> APIRouter:
    router = APIRouter(prefix="/kubernetes", tags=["local Kubernetes lab"])

    @router.get("/cluster")
    def cluster() -> dict:
        return service.adapter.discover()

    @router.post("/maintenance/plans", status_code=201)
    def plan(request: DryRun) -> dict:
        return service.plan(request.node)

    @router.post("/maintenance/execute", status_code=202)
    def execute(request: Approval, authorization: Annotated[str | None, Header()] = None) -> Job:
        expected = f"Bearer {approval_token}".encode()
        if not authorization or not compare_digest(authorization.encode(), expected):
            raise HTTPException(403, "Live maintenance requires the operator approval token")
        service.adapter.validate_identity()
        return service.approve(request.plan_id, request.approver)

    return router
