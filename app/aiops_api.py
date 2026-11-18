"""AIOps endpoints and local per-operator authentication."""

from hmac import compare_digest
import json
import re
from typing import Annotated, Literal

from fastapi import APIRouter, Depends, Header, HTTPException, Query
from pydantic import BaseModel, ConfigDict, Field, StrictBool, field_validator

from .aiops import IncidentService


class Operators:
    def __init__(self, config: str) -> None:
        identities = json.loads(config)
        if not isinstance(identities, dict) or any(
            not re.fullmatch(r"[a-zA-Z0-9._-]{1,64}", name)
            or not isinstance(token, str) or not re.fullmatch(r"[a-zA-Z0-9._~+-]{32,128}", token)
            for name, token in identities.items()
        ) or len(set(identities.values())) != len(identities):
            raise ValueError("Configure a distinct bearer secret for each operator")
        self.identities = {name: f"Bearer {token}".encode() for name, token in identities.items()}

    def authenticate(self, authorization: Annotated[str | None, Header()] = None) -> str:
        supplied = (authorization or "").encode()
        identity = None
        for name, token in self.identities.items():
            if compare_digest(supplied, token):
                identity = name
        if identity is None:
            raise HTTPException(401, "An authenticated AIOps operator is required", headers={"WWW-Authenticate": "Bearer"})
        return identity


class SignOff(BaseModel):
    model_config = ConfigDict(extra="forbid")
    nonce: Annotated[str, Field(pattern=r"^[A-Za-z0-9_-]{32}$")]
    approved: StrictBool

    @field_validator("approved")
    @classmethod
    def require_approval(cls, value: bool) -> bool:
        if not value:
            raise ValueError("Explicit approval is required")
        return value


class Resolution(BaseModel):
    model_config = ConfigDict(extra="forbid")
    disposition: Literal["mitigated", "recovered", "false_positive", "accepted_risk"]


def aiops_router(service: IncidentService, operators: Operators) -> APIRouter:
    router = APIRouter(prefix="/aiops", tags=["AIOps"])
    actor_dep = Depends(operators.authenticate)

    @router.post("/collect")
    def collect() -> dict:
        return service.collect()

    @router.post("/demo")
    def demo() -> dict:
        return service.demo()

    @router.get("/incidents")
    def incidents(limit: Annotated[int, Query(ge=1, le=100)] = 50,
                  status: Literal["all", "open", "resolved"] = "open") -> list[dict]:
        return service.incidents(limit, status)

    @router.get("/incidents/{incident}")
    def incident_detail(incident: str) -> dict:
        return service.incident(incident)

    @router.get("/incidents/{incident}/history")
    def history(incident: str, after: Annotated[int, Query(ge=0)] = 0,
                limit: Annotated[int, Query(ge=1, le=100)] = 100) -> list[dict]:
        return service.history(incident, after, limit)

    @router.get("/incidents/{incident}/recommendations")
    def recommendations(incident: str) -> list[dict]:
        return service.recommendations(incident)

    @router.post("/incidents/{incident}/recommendations")
    def recommend(incident: str, authorization: Annotated[str | None, Header()] = None) -> dict:
        if service.ai.provider.name != "mock":
            operators.authenticate(authorization)
        return service.recommend(incident)

    @router.post("/recommendations/{recommendation}/approve", status_code=202)
    def approve(recommendation: str, request: SignOff, actor: str = actor_dep) -> dict:
        return service.approve(recommendation, actor, request.nonce)

    @router.get("/remediations/{recommendation}")
    def remediation(recommendation: str) -> dict:
        return service.remediation(recommendation)

    @router.post("/incidents/{incident}/resolve")
    def resolve(incident: str, request: Resolution, actor: str = actor_dep) -> dict:
        return service.resolve(incident, actor, request.disposition)

    @router.get("/incidents/{incident}/report")
    def report(incident: str) -> dict:
        return service.report(incident)

    @router.get("/risk")
    def risk(node: Annotated[str, Query(min_length=1, max_length=253)]) -> dict:
        return service.assess(node)

    @router.get("/usage")
    def usage() -> dict:
        return service.ai.usage()

    return router
