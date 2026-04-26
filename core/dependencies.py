import re
from functools import lru_cache
from databricks.sdk import WorkspaceClient, AccountClient
from fastapi import Request, HTTPException
from core.config import get_settings
from core.validators import validate_user_identity
import logging

_log = logging.getLogger("security")

# Headers that Databricks Apps may use to forward the authenticated user email
_USER_HEADERS = [
    "x-forwarded-user",
    "x-databricks-user-email",
    "x-auth-request-user",
    "remote-user",
]

# Matches a proper email: something@domain.tld
_EMAIL_RE = re.compile(r'^[^@\s]+@[^@\s]+\.[^@\s]+$')
# Matches a Databricks numeric workspace user ID: digits@digits
_NUMERIC_ID_RE = re.compile(r'^\d+@\d+$')


@lru_cache(maxsize=512)
def _lookup_email_for_numeric_id(numeric_id: str) -> str:
    """Resolve a Databricks numeric user ID (userId@workspaceId) to an email.

    Resolution order:
    1. USER_ID_MAP env var (manual override — most reliable)
    2. Databricks SCIM Users API
    3. Return numeric_id as-is (fallback)
    """
    # Check manual map first (no API call required)
    manual = get_settings().user_id_map_dict.get(numeric_id.lower().strip())
    if manual:
        _log.info("RESOLVED_USER_MAP numeric_id=%s email=%s", numeric_id, manual)
        return manual

    user_id = numeric_id.split('@')[0]
    try:
        client = get_workspace_client()
        user = client.users.get(id=user_id)
        email = (getattr(user, 'user_name', None) or numeric_id).lower().strip()
        _log.info("RESOLVED_USER_API numeric_id=%s email=%s", numeric_id, email)
        return email
    except Exception as exc:
        _log.warning("Could not resolve numeric user ID %s: %s", numeric_id, exc)
        return numeric_id


def resolve_user_identity(request: Request) -> str:
    """Return the best available user identity from request headers.

    Resolution order:
    1. Any header value that already looks like a real email (user@domain.tld)
    2. Databricks numeric ID (userId@workspaceId) resolved to email via Users API
    3. First non-empty header value as-is (fallback)
    """
    candidates = []
    for h in _USER_HEADERS:
        val = re.sub(r"[\x00-\x1f\x7f]", "", (request.headers.get(h) or "").strip())[:128].lower()
        if val:
            candidates.append(val)

    for v in candidates:
        if _EMAIL_RE.match(v):
            return v

    for v in candidates:
        if _NUMERIC_ID_RE.match(v):
            return _lookup_email_for_numeric_id(v)

    return candidates[0] if candidates else ""


def get_current_user(request: Request) -> str:
    """Extract authenticated user from proxy-injected headers."""
    for header in _USER_HEADERS:
        val = (request.headers.get(header) or "").strip()
        if val:
            try:
                return validate_user_identity(val)
            except ValueError:
                continue
    raise HTTPException(status_code=403, detail="Access denied")


def require_admin(request: Request) -> str:
    """Require the caller to be listed in ADMIN_USERS.

    Returns the verified admin email. Raises 403 otherwise.
    Shared guard used by admin and alert management endpoints.
    """
    user = resolve_user_identity(request)
    if not user or user not in get_settings().admin_users_set:
        _log.warning(
            "ADMIN_ACCESS_DENIED user=%s path=%s method=%s",
            user or "-", request.url.path, request.method,
        )
        raise HTTPException(status_code=403, detail="Admin access required")
    return user


@lru_cache(maxsize=1)
def get_account_client() -> AccountClient:
    settings = get_settings()
    account_id = settings.databricks_account_id
    kwargs = dict(
        host="https://accounts.azuredatabricks.net",
        account_id=account_id,
    )
    if settings.databricks_client_id and settings.databricks_client_secret:
        return AccountClient(**kwargs,
                             client_id=settings.databricks_client_id,
                             client_secret=settings.databricks_client_secret)
    if settings.databricks_token:
        return AccountClient(**kwargs, token=settings.databricks_token)
    return AccountClient(**kwargs)


class MockWorkspaceClient:
    """Stub WorkspaceClient for development mode (no Databricks credentials)"""
    def __init__(self):
        self.workspace_id = "dev-local"
        _log.warning("MockWorkspaceClient: Running in development mode without Databricks credentials")


@lru_cache(maxsize=1)
def get_workspace_client() -> WorkspaceClient:
    """
    WorkspaceClient auto-detects credentials injected by Databricks Apps
    (DATABRICKS_HOST, DATABRICKS_CLIENT_ID, DATABRICKS_CLIENT_SECRET).
    Falls back to ~/.databrickscfg for local dev.
    Returns MockWorkspaceClient if no credentials available (development mode).
    """
    settings = get_settings()
    if settings.databricks_client_id and settings.databricks_client_secret:
        return WorkspaceClient(
            host=settings.databricks_host,
            client_id=settings.databricks_client_id,
            client_secret=settings.databricks_client_secret,
        )
    if settings.databricks_token:
        return WorkspaceClient(
            host=settings.databricks_host,
            token=settings.databricks_token,
        )
    
    # Try to create WorkspaceClient with auto-detection (e.g., from ~/.databrickscfg)
    try:
        return WorkspaceClient()
    except Exception as e:
        # No credentials available - return mock client for development mode
        _log.warning("No Databricks credentials found, using mock client for development: %s", str(e))
        return MockWorkspaceClient()  # type: ignore

