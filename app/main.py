import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
import json
import logging
import os
from pathlib import Path
from time import time
import sqlite3
import tomllib

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from .aiops import IncidentService
from .aiops_api import Operators, aiops_router
from .aiops_providers import AIService, configured_provider
from .batch_api import batch_router
from .batch_models import BatchPolicy
from .batch_service import BatchService
from .job_api import job_router
from .job_worker import JobWorker
from .jobs import JobService
from .models import AuditEvent, Cluster, ExecuteRequest, Operation, Plan, PlanRequest, PolicySettings
from .policy import MaintenancePolicy
from .service import MaintenanceService, RecordNotFound, logger
from .store import MaintenanceConflict, Store


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
    mode = os.getenv("MAINTENANCE_MODE", "simulation")
    default_db = "data/live.sqlite3" if mode == "kubernetes" else "data/cluster.sqlite3"
    store = Store(db_path or Path(os.getenv("MAINTENANCE_DB", default_db)))
    policy = MaintenancePolicy(settings)
    service = MaintenanceService(store, policy)
    batch_service = BatchService(store, policy, defaults)

    jobs = JobService(service, batch_service)
    if mode not in {"simulation", "kubernetes"}:
        raise ValueError("MAINTENANCE_MODE must be simulation or kubernetes")
    live = None
    collector = None
    if mode == "kubernetes":
        from .kube_adapter import KubeUnavailable, KubernetesAdapter, LabSafetyError
        from .kube_api import live_router
        from .kube_jobs import LiveMaintenance

        approval_token = os.environ["LAB_APPROVAL_TOKEN"]
        if len(approval_token) < 32:
            raise ValueError("LAB_APPROVAL_TOKEN requires at least 32 characters")
        adapter = KubernetesAdapter(Path(os.environ["LAB_KUBECONFIG"]), Path(os.environ["LAB_IDENTITY"]))
        live = jobs.live = LiveMaintenance(jobs, adapter)

    operators = Operators(os.getenv("AIOPS_OPERATOR_TOKENS", "{}"))
    ai = AIService(store, configured_provider(), int(os.getenv("AIOPS_DAILY_LIMIT_MICROUSD", "0")), time)
    aiops = IncidentService(jobs, ai)
    if live:
        from .aiops_collector import EventCollector

        interval = int(os.getenv("AIOPS_COLLECTION_SECONDS", "30"))
        if interval < 0:
            raise ValueError("AIOPS_COLLECTION_SECONDS must be nonnegative")
        if interval:
            collector = EventCollector(aiops, interval)

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
        jobs.initialize()
        if live:
            live.initialize()
        aiops.initialize()
        worker = JobWorker(jobs)
        worker.start()
        if collector:
            collector.thread.start()
        try:
            yield
        finally:
            if collector:
                await asyncio.to_thread(collector.close)
            await asyncio.to_thread(worker.close)
            if live:
                live.adapter.close()

    app = FastAPI(title="Intelligent Kubernetes Maintenance & Reliability Platform",
                  version="0.5.0", lifespan=lifespan)
    app.include_router(batch_router(batch_service))
    app.include_router(job_router(jobs))
    app.include_router(aiops_router(aiops, operators))

    if live:
        app.include_router(live_router(live, approval_token))

        @app.exception_handler(LabSafetyError)
        async def unsafe_lab(_: Request, error: LabSafetyError) -> JSONResponse:
            return JSONResponse(status_code=409, content={"detail": str(error)})

        @app.exception_handler(KubeUnavailable)
        async def lab_unavailable(_: Request, error: KubeUnavailable) -> JSONResponse:
            return JSONResponse(status_code=503, content={"detail": str(error)})

    @app.exception_handler(MaintenanceConflict)
    async def operation_conflict(_: Request, error: MaintenanceConflict) -> JSONResponse:
        return JSONResponse(status_code=409, content={"detail": str(error)})

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
        if live:
            live.adapter.validate_identity()
        return {"status": "ok", "mode": mode}

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
