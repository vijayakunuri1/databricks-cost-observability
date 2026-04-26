import asyncio
import logging
import threading
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException
from databricks.sdk import WorkspaceClient

from core.dependencies import get_workspace_client
from services.storage_service import StorageService

_log = logging.getLogger("security")
router = APIRouter(prefix="/platform", tags=["Platform Insights"])

_svc_instance: Optional[StorageService] = None
_svc_lock = threading.Lock()


def _svc(client: WorkspaceClient = Depends(get_workspace_client)) -> StorageService:
    global _svc_instance
    with _svc_lock:
        if _svc_instance is None:
            _svc_instance = StorageService(client)
    return _svc_instance


async def _handle_async(fn):
    try:
        return await asyncio.to_thread(fn)
    except ValueError as exc:
        _log.warning("VALIDATION_ERROR handler=storage error=%s", exc)
        raise HTTPException(status_code=422, detail="Invalid input parameters")
    except RuntimeError as exc:
        _log.warning("RUNTIME_ERROR handler=storage error=%s", exc)
        raise HTTPException(status_code=400, detail="Request could not be processed")
    except Exception as exc:
        _log.exception("UNHANDLED_ERROR handler=storage")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/storage-insights", summary="Storage — table inventory, optimization ops, schema growth")
async def get_storage_insights(svc: StorageService = Depends(_svc)):
    """Analyse system.storage and information_schema for data lake health.
    Cached 30 minutes."""
    return await _handle_async(svc.get_storage_insights)
