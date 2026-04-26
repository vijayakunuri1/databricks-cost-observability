import asyncio
import logging
import threading
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException
from databricks.sdk import WorkspaceClient, AccountClient

from core.dependencies import get_workspace_client
from services.cost_service import CostService
from services.ml_service import MLAnomalyService

_log = logging.getLogger("security")

router = APIRouter(prefix="/cost", tags=["ML Anomalies"])

# Thread-safe singletons so trained model and cache persist across requests.
_ml_svc_instance: Optional[MLAnomalyService] = None
_ml_svc_lock = threading.Lock()
_cost_svc_instance: Optional[CostService] = None
_cost_svc_lock = threading.Lock()


def _account_client_dep() -> Optional[AccountClient]:
    try:
        from core.dependencies import get_account_client
        return get_account_client()
    except Exception:
        return None


def _svc(
    client: WorkspaceClient = Depends(get_workspace_client),
    account_client: Optional[AccountClient] = Depends(_account_client_dep),
) -> CostService:
    global _cost_svc_instance
    with _cost_svc_lock:
        if _cost_svc_instance is None:
            _cost_svc_instance = CostService(client, account_client)
    return _cost_svc_instance


def _ml_svc(
    client: WorkspaceClient = Depends(get_workspace_client),
    account_client: Optional[AccountClient] = Depends(_account_client_dep),
) -> MLAnomalyService:
    global _ml_svc_instance
    with _ml_svc_lock:
        if _ml_svc_instance is None:
            _ml_svc_instance = MLAnomalyService(client, account_client)
    return _ml_svc_instance


async def _handle_async(fn):
    try:
        return await asyncio.to_thread(fn)
    except ValueError as exc:
        _log.warning("VALIDATION_ERROR handler=ml_cost error=%s", exc)
        raise HTTPException(status_code=422, detail="Invalid input parameters")
    except RuntimeError as exc:
        _log.warning("RUNTIME_ERROR handler=ml_cost error=%s", exc)
        raise HTTPException(status_code=400, detail="Request could not be processed")
    except Exception as exc:
        _log.exception("UNHANDLED_ERROR handler=ml_cost")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get(
    "/ml-anomalies",
    summary=(
        "ML anomaly and waste detection — "
        "baseline Z-score, Isolation Forest, cost drift, idle resources"
    ),
)
async def get_ml_anomalies(
    svc:    CostService       = Depends(_svc),
    ml_svc: MLAnomalyService  = Depends(_ml_svc),
):
    """Train lightweight ML models on 90 days of billing history and score
    every currently-running resource for anomalous cost or waste behaviour.

    Results are cached for 30 minutes; subsequent calls within that window
    skip re-training and only re-score the live resource list.
    """
    def _run():
        running = svc.get_anomalies()
        return ml_svc.get_ml_anomalies(running)
    return await _handle_async(_run)
