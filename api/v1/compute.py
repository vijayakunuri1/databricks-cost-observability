import asyncio
import logging
import threading
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException
from databricks.sdk import WorkspaceClient, AccountClient

from core.dependencies import get_workspace_client
from services.compute_service import ComputeService

_log = logging.getLogger("security")

router = APIRouter(prefix="/platform", tags=["Platform Insights"])

_compute_svc_instance: Optional[ComputeService] = None
_compute_svc_lock = threading.Lock()


def _account_client_dep() -> Optional[AccountClient]:
    try:
        from core.dependencies import get_account_client
        return get_account_client()
    except Exception:
        return None


def _compute_svc(
    client: WorkspaceClient = Depends(get_workspace_client),
    account_client: Optional[AccountClient] = Depends(_account_client_dep),
) -> ComputeService:
    global _compute_svc_instance
    with _compute_svc_lock:
        if _compute_svc_instance is None:
            _compute_svc_instance = ComputeService(client, account_client)
    return _compute_svc_instance


async def _handle_async(fn):
    try:
        return await asyncio.to_thread(fn)
    except ValueError as exc:
        _log.warning("VALIDATION_ERROR handler=compute error=%s", exc)
        raise HTTPException(status_code=422, detail="Invalid input parameters")
    except RuntimeError as exc:
        _log.warning("RUNTIME_ERROR handler=compute error=%s", exc)
        raise HTTPException(status_code=400, detail="Request could not be processed")
    except Exception as exc:
        _log.exception("UNHANDLED_ERROR handler=compute")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get(
    "/compute-insights",
    summary="Compute right-sizing — cluster/warehouse configs, utilisation, and optimisation flags",
)
async def get_compute_insights(svc: ComputeService = Depends(_compute_svc)):
    """Analyse system.compute tables to surface over-provisioned clusters,
    warehouses without auto-stop, DBR version sprawl, and actual
    utilisation vs configured capacity.

    Results are cached for 30 minutes.
    """
    return await _handle_async(svc.get_compute_insights)
