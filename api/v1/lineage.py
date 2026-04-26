import asyncio
import logging
import threading
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException
from databricks.sdk import WorkspaceClient

from core.dependencies import get_workspace_client
from services.lineage_service import LineageService

_log = logging.getLogger("security")
router = APIRouter(prefix="/platform", tags=["Platform Insights"])

_svc_instance: Optional[LineageService] = None
_svc_lock = threading.Lock()


def _svc(client: WorkspaceClient = Depends(get_workspace_client)) -> LineageService:
    global _svc_instance
    with _svc_lock:
        if _svc_instance is None:
            _svc_instance = LineageService(client)
    return _svc_instance


async def _handle_async(fn):
    try:
        return await asyncio.to_thread(fn)
    except ValueError as exc:
        _log.warning("VALIDATION_ERROR handler=lineage error=%s", exc)
        raise HTTPException(status_code=422, detail="Invalid input parameters")
    except RuntimeError as exc:
        _log.warning("RUNTIME_ERROR handler=lineage error=%s", exc)
        raise HTTPException(status_code=400, detail="Request could not be processed")
    except Exception as exc:
        _log.exception("UNHANDLED_ERROR handler=lineage")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/lineage-insights", summary="Data Lineage, Model Serving, MLflow experiments")
async def get_lineage_insights(svc: LineageService = Depends(_svc)):
    """Query system.access lineage, system.serving, and system.mlflow.
    Cached 30 minutes."""
    return await _handle_async(svc.get_lineage_insights)
