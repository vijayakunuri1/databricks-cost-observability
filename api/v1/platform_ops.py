import asyncio
import logging
import threading
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException
from databricks.sdk import WorkspaceClient

from core.dependencies import get_workspace_client
from services.platform_ops_service import PlatformOpsService

_log = logging.getLogger("security")
router = APIRouter(prefix="/platform", tags=["Platform Operations"])

_svc_instance: Optional[PlatformOpsService] = None
_svc_lock = threading.Lock()


def _svc(client: WorkspaceClient = Depends(get_workspace_client)) -> PlatformOpsService:
    global _svc_instance
    with _svc_lock:
        if _svc_instance is None:
            _svc_instance = PlatformOpsService(client)
    return _svc_instance


async def _handle(fn):
    try:
        return await asyncio.to_thread(fn)
    except ValueError as exc:
        _log.warning("VALIDATION_ERROR handler=platform_ops error=%s", exc)
        raise HTTPException(status_code=422, detail="Invalid input parameters")
    except RuntimeError as exc:
        _log.warning("RUNTIME_ERROR handler=platform_ops error=%s", exc)
        raise HTTPException(status_code=400, detail="Request could not be processed")
    except Exception:
        _log.exception("UNHANDLED_ERROR handler=platform_ops")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/platform-ops", summary="Networking, dashboards, and alerts")
async def get_platform_ops(svc: PlatformOpsService = Depends(_svc)):
    """Query system.networking, system.lakeview, and SDK alerts. Cached 30 minutes."""
    return await _handle(svc.get_platform_ops)
