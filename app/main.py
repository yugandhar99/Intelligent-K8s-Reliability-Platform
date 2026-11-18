from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
import json
import logging
import os
from pathlib import Path
import sqlite3
import tomllib

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from .batch_api import batch_router
from .batch_models import BatchPolicy
from .batch_service import BatchService
from .models import AuditEvent, Cluster, ExecuteRequest, Operation, Plan, PlanRequest, PolicySettings
from .policy import MaintenancePolicy
from .service import MaintenanceService, RecordNotFound, logger
from .store import Store


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        fields = getattr(record, "fields", {})
        return json.dumps({
            "timestamp": self.formatTime(record),
            "level": record.levelname,
            "event": record.getMessage(),
            **fields,
        })


def create_app(db_path: Path | None = None) -> FastAPI:
    with Path(__file__).with_name("config.toml").open("rb") as config:
        configuration = tomllib.load(config)
    settings = PolicySettings.model_validate(configuration["maintenance"])
    defaults = BatchPolicy.model_validate(configuration["batch"])
    store = Store(db_path or Path(os.getenv("MAINTENANCE_DB", "data/cluster.sqlite3")))
    policy = MaintenancePolicy(settings)
    service = MaintenanceService(store, policy)
    batch_service = BatchService(store, policy, defaults)

    @asynccontextmanager
    async def lifespan(_: FastAPI) -> AsyncIterator[None]:
        if not logger.handlers:
            handler = logging.StreamHandler()
            handler.setFormatter(JsonFormatter())
            logger.addHandler(handler)
            logger.setLevel(logging.INFO)
            logger.propagate = False
        store.initialize()
        batch_service.initialize()
        yield

    app = FastAPI(title="Intelligent Kubernetes Maintenance & Reliability Platform",
                  version="0.2.0", lifespan=lifespan)
    app.include_router(batch_router(batch_service))

    @app.exception_handler(RecordNotFound)
    async def missing_record(_: Request, error: RecordNotFound) -> JSONResponse:
        return JSONResponse(status_code=404, content={"detail": str(error)})

    @app.exception_handler(sqlite3.OperationalError)
    async def storage_error(_: Request, error: sqlite3.OperationalError) -> JSONResponse:
        logger.error("storage.unavailable", extra={"fields": {
            "sqlite_error": getattr(error, "sqlite_errorname", "UNKNOWN"),
        }})
        return JSONResponse(status_code=503, content={"detail": "Simulation storage unavailable"})

    @app.get("/health")
    def health() -> dict[str, str]:
        service.cluster()
        return {"status": "ok", "mode": "simulation"}

    @app.get("/cluster")
    def cluster() -> Cluster:
        return service.cluster()

    @app.post("/maintenance/plans", status_code=201)
    def plan(request: PlanRequest) -> Plan:
        return service.create_plan(request.node)

    @app.post("/maintenance/execute")
    def execute(request: ExecuteRequest) -> Plan | Operation:
        if request.dry_run:
            return service.preview(request.plan_id)
        return service.execute(request.plan_id)

    @app.get("/maintenance/operations/{operation_id}")
    def status(operation_id: str) -> Operation:
        return service.status(operation_id)

    @app.get("/maintenance/operations/{operation_id}/audit")
    def audit(operation_id: str) -> list[AuditEvent]:
        return service.audit(operation_id)

    return app


app = create_app()
