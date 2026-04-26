import asyncio
import logging
import threading
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException
from databricks.sdk import WorkspaceClient

from core.dependencies import get_workspace_client
from services.ai_service import AIService

_log = logging.getLogger("security")

router = APIRouter(prefix="/platform", tags=["Platform Insights"])

_ai_svc_instance: Optional[AIService] = None
_ai_svc_lock = threading.Lock()


def _ai_svc(
    client: WorkspaceClient = Depends(get_workspace_client),
) -> AIService:
    global _ai_svc_instance
    with _ai_svc_lock:
        if _ai_svc_instance is None:
            _ai_svc_instance = AIService(client)
    return _ai_svc_instance


async def _handle_async(fn):
    try:
        return await asyncio.to_thread(fn)
    except ValueError as exc:
        _log.warning("VALIDATION_ERROR handler=ai error=%s", exc)
        raise HTTPException(status_code=422, detail="Invalid input parameters")
    except RuntimeError as exc:
        _log.warning("RUNTIME_ERROR handler=ai error=%s", exc)
        raise HTTPException(status_code=400, detail="Request could not be processed")
    except Exception as exc:
        _log.exception("UNHANDLED_ERROR handler=ai")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get(
    "/ai-insights",
    summary="AI/LLM usage — token consumption, model serving cost, gateway analytics",
)
async def get_ai_insights(svc: AIService = Depends(_ai_svc)):
    """Analyse system.ai_gateway and AI-related billing to surface
    token usage, model serving costs, and per-user AI consumption.

    Results are cached for 30 minutes.
    """
    return await _handle_async(svc.get_ai_insights)
