import asyncio
import logging
import threading
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Request
from databricks.sdk import WorkspaceClient, AccountClient

from core.config import get_settings
from core.dependencies import get_workspace_client, require_admin
from core.validators import validate_sql_identifier
from services.access_service import AccessService

_log = logging.getLogger("security")

router = APIRouter(prefix="/platform", tags=["Platform Insights"])

_access_svc_instance: Optional[AccessService] = None
_access_svc_lock = threading.Lock()


def _account_client_dep() -> Optional[AccountClient]:
    try:
        from core.dependencies import get_account_client
        return get_account_client()
    except Exception:
        return None


def _access_svc(
    client: WorkspaceClient = Depends(get_workspace_client),
    account_client: Optional[AccountClient] = Depends(_account_client_dep),
) -> AccessService:
    global _access_svc_instance
    with _access_svc_lock:
        if _access_svc_instance is None:
            _access_svc_instance = AccessService(client, account_client)
    return _access_svc_instance


async def _handle_async(fn):
    try:
        return await asyncio.to_thread(fn)
    except ValueError as exc:
        _log.warning("VALIDATION_ERROR handler=access error=%s", exc)
        raise HTTPException(status_code=422, detail="Invalid input parameters")
    except RuntimeError as exc:
        _log.warning("RUNTIME_ERROR handler=access error=%s", exc)
        raise HTTPException(status_code=400, detail="Request could not be processed")
    except Exception as exc:
        _log.exception("UNHANDLED_ERROR handler=access")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/access-debug", summary="Debug: grants per catalog")
async def access_debug(request: Request, svc: AccessService = Depends(_access_svc)):
    """Debug: SHOW GRANTS ON CATALOG for all catalogs. Admin only, disabled in production."""
    if get_settings().is_production:
        raise HTTPException(status_code=404, detail="Not found")
    require_admin(request)
    result = {}
    # All catalogs from information_schema
    try:
        rows = svc._execute("SELECT DISTINCT catalog_name FROM system.information_schema.catalogs ORDER BY catalog_name")
        catalogs = [({k.lower(): v for k, v in r.items()}).get("catalog_name", "") for r in rows]
        catalogs = [c for c in catalogs if c and not c.startswith("__")]
        result["catalogs"] = catalogs
    except Exception as e:
        _log.warning("ACCESS_DEBUG catalog listing failed: %s", e)
        result["catalogs_error"] = "Failed to list catalogs"
        catalogs = []
    # SHOW GRANTS ON CATALOG for each
    grants_per_cat = {}
    for cat in catalogs:
        try:
            safe_cat = validate_sql_identifier(cat)
            rows = svc._execute(f"SHOW GRANTS ON CATALOG `{safe_cat}`")
            principals = {}
            for r in rows:
                low = {k.lower(): v for k, v in r.items()}
                p = (low.get("principal") or low.get("grantee") or "").strip()
                a = (low.get("actiontype") or low.get("action_type") or "").strip()
                if p:
                    principals.setdefault(p, []).append(a)
            grants_per_cat[cat] = {"grant_count": len(rows), "principals": principals}
        except Exception as e:
            _log.warning("ACCESS_DEBUG grants check failed for catalog %s: %s", cat, e)
            grants_per_cat[cat] = {"error": "Failed to retrieve grants"}
    result["grants_per_catalog"] = grants_per_cat
    return result


@router.get(
    "/access-governance",
    summary="Access governance — user/group grants across catalogs, schemas, tables",
)
async def get_access_governance(force: bool = False, svc: AccessService = Depends(_access_svc)):
    """Build a comprehensive access map showing every principal's
    Unity Catalog grants, group memberships, and security policies.

    Results are cached for 30 minutes. Pass ?force=true to bust the cache.
    """
    if force:
        svc.clear_cache()
    return await _handle_async(svc.get_access_governance)
