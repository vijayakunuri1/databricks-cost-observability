import asyncio
import logging
import threading
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException
from databricks.sdk import WorkspaceClient

from core.dependencies import get_workspace_client
from services.lakeflow_service import LakeflowService

_log = logging.getLogger("security")
router = APIRouter(prefix="/platform", tags=["Platform Insights"])

_svc_instance: Optional[LakeflowService] = None
_svc_lock = threading.Lock()


def _svc(client: WorkspaceClient = Depends(get_workspace_client)) -> LakeflowService:
    global _svc_instance
    with _svc_lock:
        if _svc_instance is None:
            _svc_instance = LakeflowService(client)
    return _svc_instance


async def _handle_async(fn):
    try:
        return await asyncio.to_thread(fn)
    except ValueError as exc:
        _log.warning("VALIDATION_ERROR handler=lakeflow error=%s", exc)
        raise HTTPException(status_code=422, detail="Invalid input parameters")
    except RuntimeError as exc:
        _log.warning("RUNTIME_ERROR handler=lakeflow error=%s", exc)
        raise HTTPException(status_code=400, detail="Request could not be processed")
    except Exception as exc:
        _log.exception("UNHANDLED_ERROR handler=lakeflow")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/job-insights", summary="Job SLA — success/failure rates, durations, pipeline health")
async def get_job_insights(svc: LakeflowService = Depends(_svc)):
    """Analyse system.lakeflow for job run success rates, failures,
    duration trends, and DLT pipeline health. Cached 30 minutes."""
    return await _handle_async(svc.get_job_insights)
