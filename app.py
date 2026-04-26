from contextlib import asynccontextmanager
import asyncio
import logging
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import Response

from core.config import get_settings
from core.security import (
    AuditLogMiddleware,
    HTTPSRedirectMiddleware,
    SecurityHeadersMiddleware,
)
from core.dependencies import resolve_user_identity
from api.router import api_router

_log = logging.getLogger("security")



def _forbidden_html(user: str = "") -> str:
    import html as _html
    user_line = (
        f'<p style="margin-top:12px;font-size:0.8rem;color:#4b5563">'
        f'Signed in as <code style="color:#9ca3af">{_html.escape(user)}</code></p>'
    ) if user else ""
    return f"""<!doctype html>
<html><head><title>Access Denied</title>
<style>body{{font-family:sans-serif;background:#0f1117;color:#e5e7eb;
  display:flex;align-items:center;justify-content:center;height:100vh;margin:0}}
  .box{{text-align:center}}.icon{{font-size:3rem;margin-bottom:12px}}
  h1{{color:#f87171;margin:0 0 8px}}p{{color:#6b7280;font-size:0.9rem}}
</style>
</head><body><div class="box">
  <div class="icon">&#128274;</div>
  <h1>Access Denied</h1>
  <p>You are not authorised to use this application.<br>
     Contact the workspace administrator to request access.</p>
  {user_line}
</div></body></html>"""



class CacheControlMiddleware(BaseHTTPMiddleware):
    """Add Cache-Control headers to API JSON responses.

    Browsers/proxies cache successful GET responses for 10 minutes so that
    an executive who reopens the dashboard within that window gets instant
    results from their browser cache rather than a fresh round-trip.
    """
    async def dispatch(self, request: Request, call_next) -> Response:
        response = await call_next(request)
        path = request.url.path
        # Never cache user/admin endpoints — they change immediately when permissions are updated
        _no_cache = path.startswith("/api/v1/user/") or path.startswith("/api/v1/admin/")
        if (
            request.method == "GET"
            and path.startswith("/api/v1/")
            and response.status_code == 200
            and not _no_cache
        ):
            response.headers["Cache-Control"] = "private, max-age=600"
        elif _no_cache:
            response.headers["Cache-Control"] = "no-store"
        return response


class AllowlistMiddleware(BaseHTTPMiddleware):
    """Restrict access to users listed in ALLOWED_USERS.

    Databricks Apps injects the authenticated user's email in one of several
    possible headers (X-Forwarded-User, X-Databricks-User-Email, etc.).
    When ALLOWED_USERS is non-empty, only those emails (case-insensitive) are
    permitted.  The /api/v1/health (root only) is always allowed through.

    When ALLOWED_USERS is empty, all requests with a valid identity header
    are permitted (workspace-authenticated users only).
    """

    # Paths that bypass all auth checks (exact match)
    _PUBLIC_PATHS = {"/api/v1/health", "/api/v1/health/"}

    async def dispatch(self, request: Request, call_next):
        # Health-check endpoint is always public
        if request.url.path in self._PUBLIC_PATHS:
            return await call_next(request)

        user = resolve_user_identity(request)
        ip = request.headers.get("x-forwarded-for", request.client.host if request.client else "unknown").split(",")[0].strip()
        
        settings = get_settings()
        
        # In development mode, allow unauthenticated requests to localhost
        if settings.environment == "development" and request.client and request.client.host in ["127.0.0.1", "localhost"]:
            # Provide a default user for local development
            if not user:
                user = "local-dev@localhost"
                _log.debug("DEVELOPMENT_MODE allowing localhost request without auth")

        # Require a valid identity header on every protected request
        if not user:
            _log.warning(
                "AUTH_NO_IDENTITY ip=%s path=%s method=%s",
                ip, request.url.path, request.method,
            )
            return HTMLResponse(_forbidden_html(), status_code=403)  # no identity to show

        # Skip allowlist check for development localhost
        is_dev_localhost = (
            settings.environment == "development" 
            and request.client 
            and request.client.host in ["127.0.0.1", "localhost"]
        )
        
        if not is_dev_localhost:
            allowed = settings.allowed_users_set
            # If an allowlist is configured, enforce it
            if allowed and user not in allowed:
                _log.warning(
                    "AUTH_NOT_ALLOWED user=%s ip=%s path=%s",
                    user, ip, request.url.path,
                )
                return HTMLResponse(_forbidden_html(user), status_code=403)

        return await call_next(request)


async def _background_warmup() -> None:
    """Prime all service caches in the background after server startup.

    Runs 5 s after boot so the server is fully ready before we issue SQL.
    All service get_* calls are fired concurrently; individual failures are
    swallowed so a broken system table never prevents the app from starting.
    """
    await asyncio.sleep(5)
    _log.info("WARMUP_START priming all service caches")
    try:
        from core.dependencies import get_workspace_client, get_account_client
        client = get_workspace_client()

        # Lazily import to avoid circular imports at module level
        from services.audit_service import AuditService
        from services.ai_service import AIService
        from services.cluster_health_service import ClusterHealthService
        from services.compute_service import ComputeService
        from services.executive_service import ExecutiveService
        from services.governance_service import GovernanceService
        from services.lakeflow_service import LakeflowService
        from services.lineage_service import LineageService
        from services.platform_ops_service import PlatformOpsService
        from services.storage_service import StorageService

        import api.v1.audit as _m_audit
        import api.v1.ai as _m_ai
        import api.v1.cluster_health as _m_ch
        import api.v1.compute as _m_cmp
        import api.v1.executive as _m_exec
        import api.v1.governance as _m_gov
        import api.v1.lakeflow as _m_lf
        import api.v1.lineage as _m_lin
        import api.v1.platform_ops as _m_ops
        import api.v1.storage as _m_stor

        def _ensure(module, inst_attr, lock_attr, cls, *args):
            """Create service instance and register it with the API module if not set."""
            with getattr(module, lock_attr):
                if getattr(module, inst_attr) is None:
                    setattr(module, inst_attr, cls(*args))
            return getattr(module, inst_attr)

        try:
            account_client = get_account_client()
        except Exception:
            account_client = None

        audit_svc = _ensure(_m_audit, "_audit_svc_instance", "_audit_svc_lock", AuditService, client)
        ai_svc    = _ensure(_m_ai,    "_ai_svc_instance",    "_ai_svc_lock",    AIService,    client)
        ch_svc    = _ensure(_m_ch,    "_svc_instance",       "_svc_lock",       ClusterHealthService, client)
        cmp_svc   = _ensure(_m_cmp,   "_compute_svc_instance", "_compute_svc_lock", ComputeService, client, account_client)
        exec_svc  = _ensure(_m_exec,  "_svc_instance",       "_svc_lock",       ExecutiveService, client)
        gov_svc   = _ensure(_m_gov,   "_svc_instance",       "_svc_lock",       GovernanceService, client)
        lf_svc    = _ensure(_m_lf,    "_svc_instance",       "_svc_lock",       LakeflowService,   client)
        lin_svc   = _ensure(_m_lin,   "_svc_instance",       "_svc_lock",       LineageService,    client)
        ops_svc   = _ensure(_m_ops,   "_svc_instance",       "_svc_lock",       PlatformOpsService, client)
        stor_svc  = _ensure(_m_stor,  "_svc_instance",       "_svc_lock",       StorageService,    client)

        # Fire all cache-fills concurrently; return_exceptions=True so one failure
        # doesn't cancel the rest.
        results = await asyncio.gather(
            asyncio.to_thread(audit_svc.get_user_adoption),
            asyncio.to_thread(ai_svc.get_ai_insights),
            asyncio.to_thread(ch_svc.get_cluster_health),
            asyncio.to_thread(cmp_svc.get_compute_insights),
            asyncio.to_thread(exec_svc.get_executive_summary),
            asyncio.to_thread(gov_svc.get_governance_insights),
            asyncio.to_thread(lf_svc.get_job_insights),
            asyncio.to_thread(lin_svc.get_lineage_insights),
            asyncio.to_thread(ops_svc.get_platform_ops),
            asyncio.to_thread(stor_svc.get_storage_insights),
            return_exceptions=True,
        )
        failed = sum(1 for r in results if isinstance(r, Exception))
        if failed:
            _log.warning("WARMUP_PARTIAL %d/%d services failed to pre-cache", failed, len(results))
        else:
            _log.info("WARMUP_DONE all %d service caches primed", len(results))
    except Exception as exc:
        _log.warning("WARMUP_FAILED %s", exc)


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Configure structured logging on startup
    settings = get_settings()
    log_level = getattr(logging, settings.log_level.upper(), logging.INFO)
    logging.basicConfig(
        level=log_level,
        format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
    )
    # Ensure the security logger is always at least INFO
    logging.getLogger("security").setLevel(min(log_level, logging.INFO))
    _log.info("APP_START environment=%s version=%s", settings.environment, settings.app_version)
    # Prime all service caches in the background — first user gets instant data
    asyncio.create_task(_background_warmup())
    yield
    _log.info("APP_STOP")


def create_app() -> FastAPI:
    settings = get_settings()
    app = FastAPI(
        title="Databricks Cost Observability",
        description="DBU and cost monitoring via Databricks system tables",
        version=settings.app_version,
        docs_url="/docs" if not settings.is_production else None,
        redoc_url=None,
        lifespan=lifespan,
    )

    # Middleware stack (outermost first → executed first on request)
    # 1. HTTPS redirect (outermost — runs before anything else)
    app.add_middleware(HTTPSRedirectMiddleware)
    # 2. Security response headers
    app.add_middleware(SecurityHeadersMiddleware)
    # 3. Audit log + rate limiting
    app.add_middleware(AuditLogMiddleware)
    # 4. Allowlist auth check
    app.add_middleware(AllowlistMiddleware)
    # 5. GZip compression — compresses JSON responses ≥1 KB (typically 80-90% smaller)
    app.add_middleware(GZipMiddleware, minimum_size=1000)
    # 6. Cache-Control headers for API responses (10-min browser cache)
    app.add_middleware(CacheControlMiddleware)
    # 7. CORS
    app.add_middleware(
        CORSMiddleware,
        allow_origins=[
            f"https://{settings.databricks_host}" if settings.databricks_host else "http://localhost:8000",
        ],
        allow_credentials=False,
        allow_methods=["GET", "POST", "PUT", "DELETE", "OPTIONS"],
        allow_headers=["Content-Type", "Authorization"],
    )

    app.include_router(api_router, prefix="/api")
    app.mount("/", StaticFiles(directory="static", html=True), name="static")
    return app


app = create_app()

if __name__ == "__main__":
    import uvicorn
    settings = get_settings()
    uvicorn.run(
        app,
        host="127.0.0.1",
        port=8000,
        log_level="info"
    )
