"""User config endpoint — returns per-user tab/workspace/tag restrictions."""

from fastapi import APIRouter, Request
from core.config import get_settings
from core.dependencies import resolve_user_identity

router = APIRouter(prefix="/user", tags=["User"])

_FULL_ACCESS  = {"allowed_tabs": None,  "workspaces": None, "tags": None, "is_admin": False}
_NO_ACCESS    = {"allowed_tabs": [],    "workspaces": None, "tags": None, "is_admin": False}


def _format_config(cfg: dict, *, is_admin: bool) -> dict:
    """Normalise a raw config dict into the API response shape."""
    raw_tags = cfg.get("tags") or []
    tags = []
    for t in raw_tags:
        if isinstance(t, dict):
            # Support both {key: ..., value: ...} and {product: my-team} formats
            if "key" in t:
                tags.append({"key": str(t["key"]), "value": str(t.get("value", ""))})
            else:
                for k, v in t.items():
                    tags.append({"key": str(k), "value": str(v)})
                    break
    return {
        "allowed_tabs": cfg.get("tabs"),
        "workspaces": cfg.get("workspaces"),
        "tags": tags,
        "is_admin": is_admin,
    }


@router.get("/config")
def get_user_config(request: Request):
    """Returns the current user's tab/workspace/tag restrictions and admin flag.

    Resolution order:
    1. Admin users → full access + is_admin: true
    2. Delta table (app_user_permissions) → per-user config
    3. USER_CONFIGS env var → legacy fallback
    4. No config found → full access, is_admin: false
    """
    settings = get_settings()
    user = resolve_user_identity(request)

    # Admins always get full access and the admin flag
    if user and user in settings.admin_users_set:
        return {**_FULL_ACCESS, "is_admin": True}

    # Try Delta table first
    try:
        from services.user_permissions_service import get_service
        svc = get_service()
        if svc is not None:
            cfg = svc.get_for_user(user)
            if cfg is not None:
                return _format_config(cfg, is_admin=False)
    except Exception:
        pass

    # Fall back to env-var config
    env_cfgs = settings.user_configs_dict
    if env_cfgs and user:
        env_cfg = env_cfgs.get(user)
        if env_cfg:
            return _format_config(env_cfg, is_admin=False)

    return _FULL_ACCESS
