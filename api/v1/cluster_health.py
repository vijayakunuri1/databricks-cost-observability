import asyncio
import logging
import threading
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException
from databricks.sdk import WorkspaceClient

from core.dependencies import get_workspace_client
from services.cluster_health_service import ClusterHealthService

_log = logging.getLogger("security")
router = APIRouter(prefix="/platform", tags=["Cluster Health"])

_svc_instance: Optional[ClusterHealthService] = None
_svc_lock = threading.Lock()


def _svc(client: WorkspaceClient = Depends(get_workspace_client)) -> ClusterHealthService:
    global _svc_instance
    with _svc_lock:
        if _svc_instance is None:
            _svc_instance = ClusterHealthService(client)
    return _svc_instance


async def _handle(fn):
    try:
        return await asyncio.to_thread(fn)
    except ValueError as exc:
        _log.warning("VALIDATION_ERROR handler=cluster_health error=%s", exc)
        raise HTTPException(status_code=422, detail="Invalid input parameters")
    except RuntimeError as exc:
        _log.warning("RUNTIME_ERROR handler=cluster_health error=%s", exc)
        raise HTTPException(status_code=400, detail="Request could not be processed")
    except Exception:
        _log.exception("UNHANDLED_ERROR handler=cluster_health")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/cluster-health", summary="Cluster events timeline and node utilisation")
async def get_cluster_health(svc: ClusterHealthService = Depends(_svc)):
    """Query system.compute.cluster_events and node_timeline. Cached 20 minutes."""
    return await _handle(svc.get_cluster_health)
