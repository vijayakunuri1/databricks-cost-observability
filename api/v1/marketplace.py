import asyncio
import logging
import threading
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException
from databricks.sdk import WorkspaceClient

from core.dependencies import get_workspace_client
from services.marketplace_service import MarketplaceService

_log = logging.getLogger("security")
router = APIRouter(prefix="/platform", tags=["Marketplace"])

_svc_instance: Optional[MarketplaceService] = None
_svc_lock = threading.Lock()


def _svc(client: WorkspaceClient = Depends(get_workspace_client)) -> MarketplaceService:
    global _svc_instance
    with _svc_lock:
        if _svc_instance is None:
            _svc_instance = MarketplaceService(client)
    return _svc_instance


async def _handle(fn):
    try:
        return await asyncio.to_thread(fn)
    except ValueError as exc:
        _log.warning("VALIDATION_ERROR handler=marketplace error=%s", exc)
        raise HTTPException(status_code=422, detail="Invalid input parameters")
    except RuntimeError as exc:
        _log.warning("RUNTIME_ERROR handler=marketplace error=%s", exc)
        raise HTTPException(status_code=400, detail="Request could not be processed")
    except Exception:
        _log.exception("UNHANDLED_ERROR handler=marketplace")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/marketplace-insights", summary="Marketplace listings and consumer adoption")
async def get_marketplace_insights(svc: MarketplaceService = Depends(_svc)):
    """Query system.marketplace tables. Cached 60 minutes."""
    return await _handle(svc.get_marketplace_insights)
