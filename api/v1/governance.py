import asyncio
import logging
import threading
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException
from databricks.sdk import WorkspaceClient

from core.dependencies import get_workspace_client
from services.governance_service import GovernanceService

_log = logging.getLogger("security")
router = APIRouter(prefix="/platform", tags=["Platform Governance"])

_svc_instance: Optional[GovernanceService] = None
_svc_lock = threading.Lock()


def _svc(client: WorkspaceClient = Depends(get_workspace_client)) -> GovernanceService:
    global _svc_instance
    with _svc_lock:
        if _svc_instance is None:
            _svc_instance = GovernanceService(client)
    return _svc_instance


async def _handle(fn):
    try:
        return await asyncio.to_thread(fn)
    except ValueError as exc:
        _log.warning("VALIDATION_ERROR handler=governance error=%s", exc)
        raise HTTPException(status_code=422, detail="Invalid input parameters")
    except RuntimeError as exc:
        _log.warning("RUNTIME_ERROR handler=governance error=%s", exc)
        raise HTTPException(status_code=400, detail="Request could not be processed")
    except Exception:
        _log.exception("UNHANDLED_ERROR handler=governance")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/governance-insights", summary="IAM events, service principals, groups, tokens")
async def get_governance_insights(svc: GovernanceService = Depends(_svc)):
    """Query system.access.audit for IAM lifecycle events. Cached 30 minutes."""
    return await _handle(svc.get_governance_insights)
