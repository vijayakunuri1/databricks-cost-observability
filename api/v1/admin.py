"""Admin API — user permission management (admin-only)."""

import logging
from typing import Optional

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel, field_validator

from core.dependencies import require_admin
from core.validators import (
    validate_email_address,
    validate_tab_list,
    validate_workspace_list,
)

router = APIRouter(prefix="/admin", tags=["Admin"])
_log = logging.getLogger("security")

_NOTES_MAX_LEN = 500


class UserConfigPayload(BaseModel):
    email: str
    tabs: Optional[list[str]] = None
    workspaces: Optional[list[str]] = None
    tags: Optional[list[dict]] = None
    notes: Optional[str] = None

    @field_validator("email")
    @classmethod
    def _chk_email(cls, v: str) -> str:
        try:
            return validate_email_address(v)
        except ValueError:
            raise ValueError("Invalid email address format")

    @field_validator("notes")
    @classmethod
    def _chk_notes(cls, v: Optional[str]) -> Optional[str]:
        if v is None:
            return None
        stripped = v.strip()
        if len(stripped) > _NOTES_MAX_LEN:
            raise ValueError(f"Notes must be {_NOTES_MAX_LEN} characters or fewer")
        return stripped

    @field_validator("tabs")
    @classmethod
    def _chk_tabs(cls, v: Optional[list[str]]) -> Optional[list[str]]:
        if v is None:
            return None
        try:
            return validate_tab_list(v)
        except ValueError as exc:
            raise ValueError(str(exc))

    @field_validator("workspaces")
    @classmethod
    def _chk_workspaces(cls, v: Optional[list[str]]) -> Optional[list[str]]:
        if v is None:
            return None
        try:
            return validate_workspace_list(v)
        except ValueError as exc:
            raise ValueError(str(exc))

    @field_validator("tags")
    @classmethod
    def _chk_tags(cls, v: Optional[list[dict]]) -> Optional[list[dict]]:
        if v is None:
            return None
        if len(v) > 50:
            raise ValueError("Too many tag filter entries")
        return v


@router.get("/users")
def list_users(request: Request):
    """List all configured user permissions (admin only)."""
    require_admin(request)
    from services.user_permissions_service import get_service
    svc = get_service()
    if svc is None:
        raise HTTPException(status_code=503, detail="Permissions store unavailable")
    return svc.get_all()


@router.put("/users")
def upsert_user(payload: UserConfigPayload, request: Request):
    """Create or update a user's permission config (admin only)."""
    admin = require_admin(request)
    from services.user_permissions_service import get_service
    svc = get_service()
    if svc is None:
        raise HTTPException(status_code=503, detail="Permissions store unavailable")
    config = {
        "tabs":       payload.tabs,
        "workspaces": payload.workspaces,
        "tags":       payload.tags or [],
        "notes":      payload.notes or "",
    }
    try:
        svc.upsert(payload.email, config, admin)
    except Exception as exc:
        _log.error("ADMIN_UPSERT_FAILED admin=%s target=%s error=%s", admin, payload.email, exc)
        raise HTTPException(status_code=500, detail="Operation failed")
    _log.info("ADMIN_UPSERT_USER admin=%s target=%s", admin, payload.email)
    return {"ok": True}


@router.delete("/users/{email:path}")
def delete_user(email: str, request: Request):
    """Remove a user's permission config (admin only)."""
    admin = require_admin(request)
    try:
        safe_email = validate_email_address(email)
    except ValueError:
        raise HTTPException(status_code=422, detail="Invalid email address")
    from services.user_permissions_service import get_service
    svc = get_service()
    if svc is None:
        raise HTTPException(status_code=503, detail="Permissions store unavailable")
    try:
        svc.delete(safe_email)
    except Exception as exc:
        _log.error("ADMIN_DELETE_FAILED admin=%s target=%s error=%s", admin, safe_email, exc)
        raise HTTPException(status_code=500, detail="Operation failed")
    _log.info("ADMIN_DELETE_USER admin=%s target=%s", admin, safe_email)
    return {"ok": True}
