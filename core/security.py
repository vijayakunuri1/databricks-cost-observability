"""Security utilities — structured logging, HTTPS enforcement, rate limiting, bot detection."""

import logging
import re
import time
import uuid
from collections import defaultdict
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import RedirectResponse, Response

from core.config import get_settings

# ── Structured security logger ────────────────────────────────────────────────

_log = logging.getLogger("security")


# ── Helpers ───────────────────────────────────────────────────────────────────

def _client_ip(request: Request) -> str:
    """Best-effort real client IP behind Databricks Apps reverse proxy."""
    forwarded = request.headers.get("x-forwarded-for", "")
    if forwarded:
        return forwarded.split(",")[0].strip()
    return request.client.host if request.client else "unknown"


def _resolve_user(request: Request) -> str:
    """Extract user identity for logging. Sanitised to prevent log injection."""
    for h in ("x-forwarded-user", "x-databricks-user-email",
              "x-auth-request-user", "remote-user"):
        val = (request.headers.get(h) or "").strip()
        if val:
            # Strip control characters and truncate for safe logging
            cleaned = re.sub(r"[\x00-\x1f\x7f]", "", val)[:128].lower()
            return cleaned
    return ""


# ── Tiered Rate Limiter ──────────────────────────────────────────────────────
#
# Four endpoint tiers with different limits.  Each tier is tracked independently
# per IP using sliding-window counters.  A separate burst tracker catches
# short spikes across all endpoints.  Repeated violations lead to temporary
# IP bans.
#
# Tier      | Window | Limit  | Burst (5s)
# ──────────|────────|────────|──────────
# auth      |  60s   |   20   |   5
# heavy     |  60s   |   30   |   5
# api       |  60s   |  120   |  20
# static    |  60s   |  200   |  40


class _SlidingWindowCounter:
    """Thread-unsafe (but async-safe) sliding-window counter per key."""

    __slots__ = ("_window", "_limit", "_data")

    def __init__(self, window: float, limit: int):
        self._window = window
        self._limit = limit
        self._data: dict[str, list[float]] = defaultdict(list)

    def is_exceeded(self, key: str) -> bool:
        now = time.monotonic()
        bucket = self._data[key]
        self._data[key] = bucket = [t for t in bucket if now - t < self._window]
        if len(bucket) >= self._limit:
            return True
        bucket.append(now)
        return False

    @property
    def limit(self) -> int:
        return self._limit

    @property
    def window(self) -> float:
        return self._window


# Tier definitions: (window_seconds, max_requests)
_TIER_AUTH   = _SlidingWindowCounter(60, 20)     # /api/v1/health/whoami, auth probes
_TIER_HEAVY  = _SlidingWindowCounter(60, 30)     # AI insights, ML anomalies, access-governance
_TIER_API    = _SlidingWindowCounter(60, 120)    # Normal API endpoints
_TIER_STATIC = _SlidingWindowCounter(60, 200)    # Static files

# Burst limiters (short window)
_BURST_AUTH   = _SlidingWindowCounter(5, 5)
_BURST_HEAVY  = _SlidingWindowCounter(5, 5)
_BURST_API    = _SlidingWindowCounter(5, 20)
_BURST_STATIC = _SlidingWindowCounter(5, 40)

# Auth-failure progressive backoff: ip → list of 403 timestamps (last 10 min)
_auth_failures: dict[str, list[float]] = defaultdict(list)
_AUTH_FAIL_WINDOW = 600  # 10 minutes
_AUTH_FAIL_BAN_THRESHOLD = 10  # 10 failures in window → temp ban

# Temporary IP bans: ip → ban_expiry (monotonic)
_ip_bans: dict[str, float] = {}
_BAN_DURATION = 300  # 5-minute ban after threshold exceeded

# Rate-limit violation accumulator for escalation: ip → violation timestamps
_violations: dict[str, list[float]] = defaultdict(list)
_VIOLATION_WINDOW = 300   # 5 minutes
_VIOLATION_BAN_THRESHOLD = 5  # 5 rate-limit hits → temp ban

# Patterns for heavy endpoints (computationally expensive or email-sending)
_HEAVY_PATHS = re.compile(
    r"^/api/v1/(platform/ai-insights"
    r"|platform/access-governance"
    r"|platform/compute-insights"
    r"|platform/query-insights"
    r"|platform/user-adoption"
    r"|cost/ml-anomalies"
    r"|cost/spike-drilldown"
    r"|alerts/test-email"
    r"|alerts/send-now)"
)
_AUTH_PATHS = re.compile(r"^/api/v1/(health/whoami|admin/)")

# Known bot / scanner user-agent patterns
_BOT_UA_RE = re.compile(
    r"(?i)(python-requests|httpx|curl/|wget/|scrapy|bot|spider|crawl"
    r"|headless|phantom|selenium|puppeteer|playwright|httrack|nikto|sqlmap"
    r"|nmap|masscan|nuclei|gobuster|dirbuster|ffuf|burp)",
)


def _classify_path(path: str) -> tuple[_SlidingWindowCounter, _SlidingWindowCounter, str]:
    """Return (tier_counter, burst_counter, tier_name) for a given path."""
    if _AUTH_PATHS.match(path):
        return _TIER_AUTH, _BURST_AUTH, "auth"
    if _HEAVY_PATHS.match(path):
        return _TIER_HEAVY, _BURST_HEAVY, "heavy"
    if path.startswith("/api/"):
        return _TIER_API, _BURST_API, "api"
    return _TIER_STATIC, _BURST_STATIC, "static"


def _is_ip_banned(ip: str) -> bool:
    """Check and clear expired bans."""
    expiry = _ip_bans.get(ip)
    if expiry is None:
        return False
    if time.monotonic() >= expiry:
        del _ip_bans[ip]
        return False
    return True


def _ban_ip(ip: str, reason: str) -> None:
    _ip_bans[ip] = time.monotonic() + _BAN_DURATION
    _log.warning("IP_BANNED ip=%s duration=%ds reason=%s", ip, _BAN_DURATION, reason)


def _record_auth_failure(ip: str) -> None:
    """Track auth failures; ban IP if threshold exceeded."""
    now = time.monotonic()
    window = _auth_failures[ip]
    _auth_failures[ip] = window = [t for t in window if now - t < _AUTH_FAIL_WINDOW]
    window.append(now)
    if len(window) >= _AUTH_FAIL_BAN_THRESHOLD:
        _ban_ip(ip, "auth_failure_flood")
        _auth_failures[ip] = []  # reset after ban


def _record_violation(ip: str, tier: str) -> None:
    """Track rate-limit violations; ban IP if excessive."""
    now = time.monotonic()
    window = _violations[ip]
    _violations[ip] = window = [t for t in window if now - t < _VIOLATION_WINDOW]
    window.append(now)
    if len(window) >= _VIOLATION_BAN_THRESHOLD:
        _ban_ip(ip, f"rate_limit_violations_{tier}")
        _violations[ip] = []


def _is_suspicious_request(request: Request) -> str | None:
    """Return a reason string if the request looks like an automated bot, else None."""
    ua = request.headers.get("user-agent", "")

    # Missing user-agent entirely is suspicious
    if not ua:
        return "missing_user_agent"

    # Known bot/scanner signatures
    if _BOT_UA_RE.search(ua):
        return "bot_user_agent"

    # API calls with no Accept header (browsers always send one)
    accept = request.headers.get("accept", "")
    path = request.url.path
    if path.startswith("/api/") and not accept:
        return "missing_accept_header"

    return None


def _rate_limit_response(ip: str, tier: str, counter: _SlidingWindowCounter) -> Response:
    """Build a 429 response with Retry-After header."""
    return Response(
        content=f'{{"detail":"Rate limit exceeded ({tier}: {counter.limit}/{int(counter.window)}s)"}}',
        status_code=429,
        media_type="application/json",
        headers={"Retry-After": str(int(counter.window))},
    )


# ── HTTPS Redirect Middleware ─────────────────────────────────────────────────

class HTTPSRedirectMiddleware(BaseHTTPMiddleware):
    """Redirect HTTP → HTTPS in production.

    Databricks Apps terminates TLS at the proxy and forwards requests over
    HTTP internally.  The original scheme is available via X-Forwarded-Proto.
    In production mode, any request that arrived over plain HTTP is redirected.
    In non-production, this middleware is a no-op.
    """

    async def dispatch(self, request: Request, call_next):
        settings = get_settings()
        if settings.is_production:
            proto = (request.headers.get("x-forwarded-proto") or "").lower()
            if proto == "http":
                url = request.url.replace(scheme="https")
                return RedirectResponse(url, status_code=301)
        return await call_next(request)


# ── Security Headers Middleware ───────────────────────────────────────────────

class SecurityHeadersMiddleware(BaseHTTPMiddleware):
    """Inject standard security headers on every response."""

    # Content-Security-Policy applied to all responses.
    # 'unsafe-inline' is required because the SPA uses inline <script>/<style>.
    # frame-ancestors, object-src, and base-uri are the high-value controls.
    _CSP = (
        "default-src 'self'; "
        "script-src 'self' 'unsafe-inline'; "
        "style-src 'self' 'unsafe-inline'; "
        "img-src 'self' data: blob:; "
        "font-src 'self' data:; "
        "connect-src 'self'; "
        "frame-ancestors 'none'; "
        "object-src 'none'; "
        "base-uri 'self'"
    )

    async def dispatch(self, request: Request, call_next):
        # Attach a per-request trace ID so log lines can be correlated
        request_id = str(uuid.uuid4())
        response: Response = await call_next(request)
        response.headers["X-Request-ID"] = request_id
        response.headers["X-Content-Type-Options"] = "nosniff"
        response.headers["X-Frame-Options"] = "DENY"
        response.headers["Referrer-Policy"] = "strict-origin-when-cross-origin"
        response.headers["Permissions-Policy"] = "camera=(), microphone=(), geolocation=()"
        response.headers["Content-Security-Policy"] = self._CSP
        # Discourage search-engine and bot indexing
        response.headers["X-Robots-Tag"] = "noindex, nofollow"
        if get_settings().is_production:
            response.headers["Strict-Transport-Security"] = (
                "max-age=63072000; includeSubDomains; preload"
            )
            # Prevent aggressive caching of API responses
            if request.url.path.startswith("/api/"):
                response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate"
        return response


# ── Audit / Traffic Logging Middleware ────────────────────────────────────────

class AuditLogMiddleware(BaseHTTPMiddleware):
    """Log every request with user, IP, path, status, and duration.

    Enforces:
    - Tiered rate limiting (auth / heavy / api / static)
    - Burst protection (5-second window)
    - Auth-failure progressive backoff → temp IP ban
    - Repeated-violation escalation → temp IP ban
    - Bot/scanner detection and blocking
    """

    async def dispatch(self, request: Request, call_next):
        ip = _client_ip(request)
        user = _resolve_user(request)
        path = request.url.path
        method = request.method
        start = time.monotonic()

        # ── IP ban check ──────────────────────────────────────────────
        if _is_ip_banned(ip):
            _log.warning("BANNED_REQUEST ip=%s user=%s path=%s", ip, user or "-", path)
            return Response(
                content='{"detail":"Temporarily blocked"}',
                status_code=403,
                media_type="application/json",
                headers={"Retry-After": str(_BAN_DURATION)},
            )

        # ── Bot detection (production only) ───────────────────────────
        if get_settings().is_production and path.startswith("/api/"):
            bot_reason = _is_suspicious_request(request)
            if bot_reason:
                _log.warning(
                    "BOT_DETECTED ip=%s user=%s path=%s reason=%s ua=%s",
                    ip, user or "-", path, bot_reason,
                    (request.headers.get("user-agent", ""))[:120],
                )
                _record_violation(ip, f"bot_{bot_reason}")
                return Response(
                    content='{"detail":"Request blocked"}',
                    status_code=403,
                    media_type="application/json",
                )

        # ── Tiered rate limiting ──────────────────────────────────────
        tier, burst, tier_name = _classify_path(path)

        # Check burst first (short window)
        if burst.is_exceeded(ip):
            _log.warning(
                "BURST_LIMIT ip=%s user=%s tier=%s path=%s",
                ip, user or "-", tier_name, path,
            )
            _record_violation(ip, f"burst_{tier_name}")
            return _rate_limit_response(ip, f"burst_{tier_name}", burst)

        # Check sustained rate
        if tier.is_exceeded(ip):
            _log.warning(
                "RATE_LIMIT ip=%s user=%s tier=%s path=%s",
                ip, user or "-", tier_name, path,
            )
            _record_violation(ip, tier_name)
            return _rate_limit_response(ip, tier_name, tier)

        # ── Process request ───────────────────────────────────────────
        response: Response = await call_next(request)

        duration_ms = round((time.monotonic() - start) * 1000)
        status = response.status_code

        # ── Track auth failures for progressive backoff ───────────────
        if status == 403:
            _record_auth_failure(ip)

        # ── Structured log line ───────────────────────────────────────
        request_id = response.headers.get("X-Request-ID", "-")
        log_data = (
            f"method={method} path={path} status={status} "
            f"duration_ms={duration_ms} ip={ip} user={user or '-'} "
            f"tier={tier_name} request_id={request_id}"
        )

        if status == 403:
            _log.warning("AUTH_DENIED %s", log_data)
        elif status == 429:
            _log.warning("RATE_LIMITED %s", log_data)
        elif status >= 500:
            _log.error("SERVER_ERROR %s", log_data)
        elif status >= 400:
            _log.warning("CLIENT_ERROR %s", log_data)
        else:
            _log.info("REQUEST %s", log_data)

        return response
