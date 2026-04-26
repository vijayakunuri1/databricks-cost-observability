import asyncio
import logging
import threading
from datetime import date, timedelta
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from databricks.sdk import WorkspaceClient, AccountClient

from core.dependencies import get_workspace_client, get_account_client
from services.cost_service import CostService

_log = logging.getLogger("security")

router = APIRouter(prefix="/cost", tags=["Cost"])

# Thread-safe singleton for CostService (Fix #5)
_cost_svc_instance: Optional[CostService] = None
_cost_svc_lock = threading.Lock()


def _default_start() -> str:
    return (date.today() - timedelta(days=30)).isoformat()


def _default_end() -> str:
    return date.today().isoformat()


def _account_client_dep() -> Optional[AccountClient]:
    try:
        return get_account_client()
    except Exception:
        return None


def _svc(
    client: WorkspaceClient = Depends(get_workspace_client),
    account_client: Optional[AccountClient] = Depends(_account_client_dep),
) -> CostService:
    global _cost_svc_instance
    with _cost_svc_lock:
        if _cost_svc_instance is None:
            _cost_svc_instance = CostService(client, account_client)
    return _cost_svc_instance


async def _handle_async(fn):
    try:
        return await asyncio.to_thread(fn)
    except ValueError as e:
        _log.warning("VALIDATION_ERROR handler=%s error=%s", fn.__qualname__ if hasattr(fn, '__qualname__') else 'cost', e)
        raise HTTPException(status_code=422, detail="Invalid input parameters")
    except RuntimeError as e:
        _log.warning("RUNTIME_ERROR handler=%s error=%s", fn.__qualname__ if hasattr(fn, '__qualname__') else 'cost', e)
        raise HTTPException(status_code=400, detail="Request could not be processed")
    except Exception as e:
        _log.exception("UNHANDLED_ERROR handler=%s", fn.__qualname__ if hasattr(fn, '__qualname__') else 'cost')
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/anomalies", summary="Running services in dev/test environments with anomaly scoring")
async def get_anomalies(svc: CostService = Depends(_svc)):
    return await _handle_async(svc.get_anomalies)


@router.get("/workspace-access", summary="Account-level workspace → group/user/SP assignments across all workspaces")
async def get_workspace_access(svc: CostService = Depends(_svc)):
    return await _handle_async(svc.get_workspace_access)


@router.get("/workspace-groups", summary="AD groups with access to the workspace and their members/privileges")
async def get_workspace_groups(svc: CostService = Depends(_svc)):
    return await _handle_async(svc.get_workspace_groups)


@router.get("/workspace-users", summary="List of users with access to the workspace")
async def get_workspace_users(svc: CostService = Depends(_svc)):
    return await _handle_async(svc.get_workspace_users)


@router.get("/filters", summary="Available filter values (workspaces, SKUs, tag keys)")
async def get_filters(svc: CostService = Depends(_svc)):
    return await _handle_async(svc.get_filters)


@router.get("/spike-drilldown", summary="Break down a single day's cost by workspace, product, and resource")
async def get_spike_drilldown(
    date: str = Query(..., description="YYYY-MM-DD", min_length=10, max_length=10, pattern=r"^\d{4}-\d{2}-\d{2}$"),
    svc: CostService = Depends(_svc),
):
    return await _handle_async(lambda: svc.get_spike_drilldown(date))


@router.get("/resource-groups", summary="Distinct resource groups, optionally filtered by subscription(s)")
async def get_resource_groups(
    subscription: Optional[str] = Query(default=None, max_length=500),
    svc: CostService = Depends(_svc),
):
    subs = [s.strip() for s in subscription.split(",") if s.strip()] if subscription else []
    return await _handle_async(lambda: svc.get_resource_groups(subs))


@router.get("/tag-values", summary="Distinct values for a given compute tag key")
async def get_tag_values(
    tag_key: str = Query(..., min_length=1, max_length=128),
    svc: CostService = Depends(_svc),
):
    return await _handle_async(lambda: svc.get_tag_values(tag_key))


@router.get("/summary", summary="KPI summary — total DBUs, estimated cost, workspace/product counts")
async def get_summary(
    start_date: Optional[str] = Query(default=None, min_length=10, max_length=10, pattern=r"^\d{4}-\d{2}-\d{2}$"),
    end_date: Optional[str] = Query(default=None, min_length=10, max_length=10, pattern=r"^\d{4}-\d{2}-\d{2}$"),
    workspace_id: Optional[str] = Query(default=None, max_length=500),
    subscription: Optional[str] = Query(default=None, max_length=500),
    resource_group: Optional[str] = Query(default=None, max_length=90),
    tag_key: Optional[str] = Query(default=None, max_length=128),
    tag_value: Optional[str] = Query(default=None, max_length=256),
    svc: CostService = Depends(_svc),
):
    sd = start_date or _default_start()
    ed = end_date or _default_end()
    return await _handle_async(lambda: svc.get_summary(sd, ed, workspace_id, subscription, resource_group, tag_key, tag_value))


@router.get("/daily-trend", summary="Daily DBU consumption grouped by product category")
async def get_daily_trend(
    start_date: Optional[str] = Query(default=None, min_length=10, max_length=10, pattern=r"^\d{4}-\d{2}-\d{2}$"),
    end_date: Optional[str] = Query(default=None, min_length=10, max_length=10, pattern=r"^\d{4}-\d{2}-\d{2}$"),
    workspace_id: Optional[str] = Query(default=None, max_length=500),
    subscription: Optional[str] = Query(default=None, max_length=500),
    resource_group: Optional[str] = Query(default=None, max_length=90),
    tag_key: Optional[str] = Query(default=None, max_length=128),
    tag_value: Optional[str] = Query(default=None, max_length=256),
    svc: CostService = Depends(_svc),
):
    sd = start_date or _default_start()
    ed = end_date or _default_end()
    return await _handle_async(lambda: svc.get_daily_trend(sd, ed, workspace_id, subscription, resource_group, tag_key, tag_value))


@router.get("/by-product", summary="DBU and estimated cost broken down by SKU")
async def get_by_product(
    start_date: Optional[str] = Query(default=None, min_length=10, max_length=10, pattern=r"^\d{4}-\d{2}-\d{2}$"),
    end_date: Optional[str] = Query(default=None, min_length=10, max_length=10, pattern=r"^\d{4}-\d{2}-\d{2}$"),
    workspace_id: Optional[str] = Query(default=None, max_length=500),
    subscription: Optional[str] = Query(default=None, max_length=500),
    resource_group: Optional[str] = Query(default=None, max_length=90),
    tag_key: Optional[str] = Query(default=None, max_length=128),
    tag_value: Optional[str] = Query(default=None, max_length=256),
    svc: CostService = Depends(_svc),
):
    sd = start_date or _default_start()
    ed = end_date or _default_end()
    return await _handle_async(lambda: svc.get_by_product(sd, ed, workspace_id, subscription, resource_group, tag_key, tag_value))


@router.get("/by-workspace", summary="DBU and estimated cost broken down by workspace")
async def get_by_workspace(
    start_date: Optional[str] = Query(default=None, min_length=10, max_length=10, pattern=r"^\d{4}-\d{2}-\d{2}$"),
    end_date: Optional[str] = Query(default=None, min_length=10, max_length=10, pattern=r"^\d{4}-\d{2}-\d{2}$"),
    workspace_id: Optional[str] = Query(default=None, max_length=500),
    subscription: Optional[str] = Query(default=None, max_length=500),
    resource_group: Optional[str] = Query(default=None, max_length=90),
    tag_key: Optional[str] = Query(default=None, max_length=128),
    tag_value: Optional[str] = Query(default=None, max_length=256),
    svc: CostService = Depends(_svc),
):
    sd = start_date or _default_start()
    ed = end_date or _default_end()
    return await _handle_async(lambda: svc.get_by_workspace(sd, ed, workspace_id, subscription, resource_group, tag_key, tag_value))


@router.get("/jobs-compute-cost", summary="Cost per job (last 30 days) linked to billing usage")
async def get_jobs_compute_cost(svc: CostService = Depends(_svc)):
    """Job IDs from system.billing.usage joined to system.lakeflow.jobs for cost attribution."""
    return await _handle_async(svc.get_jobs_compute_cost)


@router.get("/by-tag-env", summary="DBU by tag value broken down by environment (dev/test/uat/prod)")
async def get_by_tag_env(
    tag_key: str = Query(..., min_length=1, max_length=128),
    start_date: Optional[str] = Query(default=None, min_length=10, max_length=10, pattern=r"^\d{4}-\d{2}-\d{2}$"),
    end_date: Optional[str] = Query(default=None, min_length=10, max_length=10, pattern=r"^\d{4}-\d{2}-\d{2}$"),
    workspace_id: Optional[str] = Query(default=None, max_length=500),
    subscription: Optional[str] = Query(default=None, max_length=500),
    resource_group: Optional[str] = Query(default=None, max_length=90),
    svc: CostService = Depends(_svc),
):
    sd = start_date or _default_start()
    ed = end_date or _default_end()
    return await _handle_async(lambda: svc.get_by_tag_env(tag_key, sd, ed, workspace_id, subscription, resource_group))


@router.get("/by-tag", summary="DBU and estimated cost broken down by compute tag value")
async def get_by_tag(
    tag_key: str = Query(..., min_length=1, max_length=128),
    start_date: Optional[str] = Query(default=None, min_length=10, max_length=10, pattern=r"^\d{4}-\d{2}-\d{2}$"),
    end_date: Optional[str] = Query(default=None, min_length=10, max_length=10, pattern=r"^\d{4}-\d{2}-\d{2}$"),
    workspace_id: Optional[str] = Query(default=None, max_length=500),
    subscription: Optional[str] = Query(default=None, max_length=500),
    resource_group: Optional[str] = Query(default=None, max_length=90),
    svc: CostService = Depends(_svc),
):
    sd = start_date or _default_start()
    ed = end_date or _default_end()
    return await _handle_async(lambda: svc.get_by_tag(tag_key, sd, ed, workspace_id, subscription, resource_group))
