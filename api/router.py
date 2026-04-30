from fastapi import APIRouter
from api.v1 import cost, health, user, admin
from api.v1 import ml_cost
from api.v1 import audit, compute, query, ai, access
from api.v1 import lakeflow, storage, lineage
from api.v1 import governance, cluster_health, marketplace, platform_ops
from api.v1 import executive
from api.v1 import alerts
from api.v1 import cloud_cost

api_router = APIRouter()
api_router.include_router(user.router,          prefix="/v1")
api_router.include_router(admin.router,         prefix="/v1")
api_router.include_router(executive.router,     prefix="/v1")
api_router.include_router(health.router,        prefix="/v1")
api_router.include_router(cost.router,          prefix="/v1")
api_router.include_router(ml_cost.router,       prefix="/v1")
api_router.include_router(audit.router,         prefix="/v1")
api_router.include_router(compute.router,       prefix="/v1")
api_router.include_router(query.router,         prefix="/v1")
api_router.include_router(ai.router,            prefix="/v1")
api_router.include_router(access.router,        prefix="/v1")
api_router.include_router(lakeflow.router,      prefix="/v1")
api_router.include_router(storage.router,       prefix="/v1")
api_router.include_router(lineage.router,       prefix="/v1")
api_router.include_router(governance.router,    prefix="/v1")
api_router.include_router(cluster_health.router, prefix="/v1")
api_router.include_router(marketplace.router,   prefix="/v1")
api_router.include_router(platform_ops.router,  prefix="/v1")
api_router.include_router(alerts.router,        prefix="/v1")
api_router.include_router(cloud_cost.router,    prefix="/v1")
