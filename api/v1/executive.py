import asyncio
import logging
import threading
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException
from databricks.sdk import WorkspaceClient

from core.dependencies import get_workspace_client
from services.executive_service import ExecutiveService

_log = logging.getLogger("security")

router = APIRouter(prefix="/executive", tags=["Executive"])

_svc_instance: Optional[ExecutiveService] = None
_svc_lock = threading.Lock()


def _svc(client: WorkspaceClient = Depends(get_workspace_client)) -> ExecutiveService:
    global _svc_instance
    with _svc_lock:
        if _svc_instance is None:
            _svc_instance = ExecutiveService(client)
    return _svc_instance


async def _handle_async(fn):
    try:
        return await asyncio.to_thread(fn)
    except ValueError as exc:
        _log.warning("VALIDATION_ERROR handler=executive error=%s", exc)
        raise HTTPException(status_code=422, detail="Invalid input parameters")
    except RuntimeError as exc:
        err = str(exc)
        _log.error("RUNTIME_ERROR handler=executive error=%s", err)
        if "TABLE_OR_VIEW_NOT_FOUND" in err or "SCHEMA_NOT_FOUND" in err or "UNRESOLVED_COLUMN" in err:
            raise HTTPException(status_code=404, detail="Required data source not available")
        raise HTTPException(status_code=400, detail="Request could not be processed")
    except Exception as exc:
        _log.exception("UNHANDLED_ERROR handler=executive error=%s", str(exc))
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get(
    "/summary",
    summary="Executive scorecard — spend KPIs, MoM trend, forecast, savings opportunities, platform health",
)
async def get_executive_summary(svc: ExecutiveService = Depends(_svc)):
    """Aggregate cross-domain executive scorecard from system.billing, system.lakeflow,
    system.access.audit, and system.compute. Covers spend KPIs with MoM delta,
    projected month-end cost, quantified savings opportunities, job health, user
    adoption, and governance signals. Cached 20 minutes."""
    return await _handle_async(svc.get_executive_summary)
