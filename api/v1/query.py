import asyncio
import logging
import threading
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException
from databricks.sdk import WorkspaceClient, AccountClient

from core.dependencies import get_workspace_client
from services.query_service import QueryService

_log = logging.getLogger("security")

router = APIRouter(prefix="/platform", tags=["Platform Insights"])

_query_svc_instance: Optional[QueryService] = None
_query_svc_lock = threading.Lock()


def _account_client_dep() -> Optional[AccountClient]:
    try:
        from core.dependencies import get_account_client
        return get_account_client()
    except Exception:
        return None


def _query_svc(
    client: WorkspaceClient = Depends(get_workspace_client),
    account_client: Optional[AccountClient] = Depends(_account_client_dep),
) -> QueryService:
    global _query_svc_instance
    with _query_svc_lock:
        if _query_svc_instance is None:
            _query_svc_instance = QueryService(client, account_client)
    return _query_svc_instance


async def _handle_async(fn):
    try:
        return await asyncio.to_thread(fn)
    except ValueError as exc:
        _log.warning("VALIDATION_ERROR handler=query error=%s", exc)
        raise HTTPException(status_code=422, detail="Invalid input parameters")
    except RuntimeError as exc:
        _log.warning("RUNTIME_ERROR handler=query error=%s", exc)
        raise HTTPException(status_code=400, detail="Request could not be processed")
    except Exception as exc:
        _log.exception("UNHANDLED_ERROR handler=query")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get(
    "/query-insights",
    summary="Query cost attribution — top users, expensive queries, warehouse utilisation",
)
async def get_query_insights(svc: QueryService = Depends(_query_svc)):
    """Analyse system.query.history to surface top cost-driving users,
    expensive queries, slow queries, and warehouse utilisation patterns.

    Results are cached for 30 minutes.
    """
    return await _handle_async(svc.get_query_insights)
