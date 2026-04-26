from fastapi import APIRouter, Request
from core.dependencies import resolve_user_identity

router = APIRouter(prefix="/health", tags=["Health"])


@router.get("/")
def health_check():
    return {"status": "ok"}


@router.get("/whoami", include_in_schema=True)
def whoami(request: Request):
    """Returns the resolved user identity (numeric IDs resolved to email)."""
    user = resolve_user_identity(request)
    return {"user": user or None}
