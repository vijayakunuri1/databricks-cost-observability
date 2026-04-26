import asyncio
import logging
import threading
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException
from databricks.sdk import WorkspaceClient

from core.dependencies import get_workspace_client
from services.audit_service import AuditService

_log = logging.getLogger("security")

router = APIRouter(prefix="/platform", tags=["Platform Insights"])

_audit_svc_instance: Optional[AuditService] = None
_audit_svc_lock = threading.Lock()


def _audit_svc(
    client: WorkspaceClient = Depends(get_workspace_client),
) -> AuditService:
    global _audit_svc_instance
    with _audit_svc_lock:
        if _audit_svc_instance is None:
            _audit_svc_instance = AuditService(client)
    return _audit_svc_instance


async def _handle_async(fn):
    try:
        return await asyncio.to_thread(fn)
    except ValueError as exc:
        _log.warning("VALIDATION_ERROR handler=audit error=%s", exc)
        raise HTTPException(status_code=422, detail="Invalid input parameters")
    except RuntimeError as exc:
        error_detail = str(exc)
        _log.error("RUNTIME_ERROR handler=audit error=%s", error_detail)
        if "TABLE_OR_VIEW_NOT_FOUND" in error_detail or "SCHEMA_NOT_FOUND" in error_detail or "UNRESOLVED_COLUMN" in error_detail:
            raise HTTPException(status_code=404, detail="Required data source not available")
        raise HTTPException(status_code=400, detail="Request could not be processed")
    except Exception as exc:
        _log.exception("UNHANDLED_ERROR handler=audit error=%s", str(exc))
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get(
    "/user-adoption",
    summary="User adoption analytics — active/inactive users, service usage, audit trends",
)
async def get_user_adoption(svc: AuditService = Depends(_audit_svc)):
    """Analyse system.access.audit to provide executive-level visibility into
    platform adoption: who is active, who is inactive (licence waste), and
    which services are most used.

    Results are cached for 30 minutes.
    """
    return await _handle_async(svc.get_user_adoption)
