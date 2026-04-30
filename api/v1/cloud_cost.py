import asyncio
import logging
import threading
from datetime import date, timedelta
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from databricks.sdk import WorkspaceClient

from core.dependencies import get_workspace_client
from services.cloud_cost_service import CloudCostService

_log = logging.getLogger(__name__)

router = APIRouter(prefix="/cloud-cost", tags=["Cloud Cost"])

_svc_instance: Optional[CloudCostService] = None
_svc_lock = threading.Lock()


def _default_start() -> str:
    return (date.today() - timedelta(days=30)).isoformat()


def _default_end() -> str:
    return date.today().isoformat()


def _svc(client: WorkspaceClient = Depends(get_workspace_client)) -> CloudCostService:
    global _svc_instance
    with _svc_lock:
        if _svc_instance is None:
            _svc_instance = CloudCostService(client)
    return _svc_instance


async def _handle(fn):
    try:
        return await asyncio.to_thread(fn)
    except ValueError as e:
        _log.warning("VALIDATION_ERROR cloud_cost error=%s", e)
        raise HTTPException(status_code=422, detail="Invalid input parameters")
    except Exception as exc:
        _log.exception("UNHANDLED_ERROR cloud_cost")
        raise HTTPException(status_code=500, detail="Internal server error")


def _parse_accounts(account: Optional[str]) -> list[str]:
    if not account:
        return []
    return [a.strip() for a in account.split(",") if a.strip()]


@router.get("/providers", summary="Distinct cloud providers with ingested data")
async def get_providers(svc: CloudCostService = Depends(_svc)):
    return await _handle(svc.get_providers)


@router.get("/accounts", summary="Distinct accounts (subscription/account/project) per provider")
async def get_accounts(
    provider: Optional[str] = Query(default=None, max_length=10),
    svc: CloudCostService = Depends(_svc),
):
    return await _handle(lambda: svc.get_accounts(provider))


@router.get("/summary", summary="KPI summary — total cost, MoM, account/service/region counts")
async def get_summary(
    start_date: Optional[str] = Query(default=None, min_length=10, max_length=10, pattern=r"^\d{4}-\d{2}-\d{2}$"),
    end_date: Optional[str] = Query(default=None, min_length=10, max_length=10, pattern=r"^\d{4}-\d{2}-\d{2}$"),
    provider: Optional[str] = Query(default=None, max_length=10),
    account: Optional[str] = Query(default=None, max_length=1000),
    svc: CloudCostService = Depends(_svc),
):
    sd = start_date or _default_start()
    ed = end_date or _default_end()
    return await _handle(lambda: svc.get_summary(sd, ed, provider, _parse_accounts(account)))


@router.get("/daily-trend", summary="Daily cost grouped by cloud provider")
async def get_daily_trend(
    start_date: Optional[str] = Query(default=None, min_length=10, max_length=10, pattern=r"^\d{4}-\d{2}-\d{2}$"),
    end_date: Optional[str] = Query(default=None, min_length=10, max_length=10, pattern=r"^\d{4}-\d{2}-\d{2}$"),
    provider: Optional[str] = Query(default=None, max_length=10),
    account: Optional[str] = Query(default=None, max_length=1000),
    svc: CloudCostService = Depends(_svc),
):
    sd = start_date or _default_start()
    ed = end_date or _default_end()
    return await _handle(lambda: svc.get_daily_trend(sd, ed, provider, _parse_accounts(account)))


@router.get("/by-service", summary="Cost broken down by cloud service (top N)")
async def get_by_service(
    start_date: Optional[str] = Query(default=None, min_length=10, max_length=10, pattern=r"^\d{4}-\d{2}-\d{2}$"),
    end_date: Optional[str] = Query(default=None, min_length=10, max_length=10, pattern=r"^\d{4}-\d{2}-\d{2}$"),
    provider: Optional[str] = Query(default=None, max_length=10),
    account: Optional[str] = Query(default=None, max_length=1000),
    top_n: int = Query(default=20, ge=1, le=100),
    svc: CloudCostService = Depends(_svc),
):
    sd = start_date or _default_start()
    ed = end_date or _default_end()
    return await _handle(lambda: svc.get_by_service(sd, ed, provider, _parse_accounts(account), top_n))


@router.get("/by-account", summary="Cost broken down by subscription/account/project")
async def get_by_account(
    start_date: Optional[str] = Query(default=None, min_length=10, max_length=10, pattern=r"^\d{4}-\d{2}-\d{2}$"),
    end_date: Optional[str] = Query(default=None, min_length=10, max_length=10, pattern=r"^\d{4}-\d{2}-\d{2}$"),
    provider: Optional[str] = Query(default=None, max_length=10),
    account: Optional[str] = Query(default=None, max_length=1000),
    svc: CloudCostService = Depends(_svc),
):
    sd = start_date or _default_start()
    ed = end_date or _default_end()
    return await _handle(lambda: svc.get_by_account(sd, ed, provider, _parse_accounts(account)))


@router.get("/by-region", summary="Cost broken down by region")
async def get_by_region(
    start_date: Optional[str] = Query(default=None, min_length=10, max_length=10, pattern=r"^\d{4}-\d{2}-\d{2}$"),
    end_date: Optional[str] = Query(default=None, min_length=10, max_length=10, pattern=r"^\d{4}-\d{2}-\d{2}$"),
    provider: Optional[str] = Query(default=None, max_length=10),
    account: Optional[str] = Query(default=None, max_length=1000),
    svc: CloudCostService = Depends(_svc),
):
    sd = start_date or _default_start()
    ed = end_date or _default_end()
    return await _handle(lambda: svc.get_by_region(sd, ed, provider, _parse_accounts(account)))


@router.get("/by-sku", summary="Cost broken down by SKU / meter (top N) — includes usage quantity, unit price, pricing model")
async def get_by_sku(
    start_date: Optional[str] = Query(default=None, min_length=10, max_length=10, pattern=r"^\d{4}-\d{2}-\d{2}$"),
    end_date: Optional[str] = Query(default=None, min_length=10, max_length=10, pattern=r"^\d{4}-\d{2}-\d{2}$"),
    provider: Optional[str] = Query(default=None, max_length=10),
    account: Optional[str] = Query(default=None, max_length=1000),
    top_n: int = Query(default=30, ge=1, le=200),
    svc: CloudCostService = Depends(_svc),
):
    sd = start_date or _default_start()
    ed = end_date or _default_end()
    return await _handle(lambda: svc.get_by_sku(sd, ed, provider, _parse_accounts(account), top_n))


@router.get("/by-category", summary="Cost broken down by service category (Compute/Storage/Network…)")
async def get_by_category(
    start_date: Optional[str] = Query(default=None, min_length=10, max_length=10, pattern=r"^\d{4}-\d{2}-\d{2}$"),
    end_date: Optional[str] = Query(default=None, min_length=10, max_length=10, pattern=r"^\d{4}-\d{2}-\d{2}$"),
    provider: Optional[str] = Query(default=None, max_length=10),
    account: Optional[str] = Query(default=None, max_length=1000),
    svc: CloudCostService = Depends(_svc),
):
    sd = start_date or _default_start()
    ed = end_date or _default_end()
    return await _handle(lambda: svc.get_by_category(sd, ed, provider, _parse_accounts(account)))
