from fastapi import APIRouter

from .batch_models import BatchOperation, BatchPlan, BatchPolicy, BatchRequest, PolicySnapshot
from .batch_service import BatchService
from .models import ExecuteRequest


def batch_router(service: BatchService) -> APIRouter:
    router = APIRouter(prefix="/maintenance", tags=["batch maintenance"])

    @router.get("/policy")
    def policy() -> PolicySnapshot:
        return service.get_policy()

    @router.put("/policy")
    def update_policy(settings: BatchPolicy) -> PolicySnapshot:
        return service.update_policy(settings)

    @router.post("/batches/plans", status_code=201)
    def plan(request: BatchRequest) -> BatchPlan:
        return service.create_plan(request.nodes)

    @router.post("/batches/execute")
    def execute(request: ExecuteRequest) -> BatchPlan | BatchOperation:
        if request.dry_run:
            return service.preview(request.plan_id)
        return service.execute(request.plan_id)

    @router.get("/batches/operations/{operation_id}")
    def status(operation_id: str) -> BatchOperation:
        return service.status(operation_id)

    return router
