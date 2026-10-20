from fastapi import APIRouter, HTTPException, Response
from starlette.concurrency import run_in_threadpool
from pydantic import BaseModel, Field

from fastink.auth import permission
from fastink.auth.apikey import create_api_key, list_api_keys, revoke_api_key
from fastink.auth.principal import Principal
from fastink.auth.scopes import require_scope
from fastink.auth.session import DEFAULT_SCOPES
from fastink.common.config import get_config
from fastink.common.logger import logger

router = APIRouter(tags=["apikeys"])

KNOWN_SCOPES = set(DEFAULT_SCOPES)


async def _ensure_admin(principal: Principal) -> None:
    """Re-check the permission: an admin scope in a stale session is not enough."""

    try:
        allowed = await run_in_threadpool(
            permission.check_user_permission,
            username=principal.username,
            permission="admin",
        )
    except Exception:
        allowed = False
    if not allowed:
        raise HTTPException(status_code=403, detail="Admin permission required")


class ApiKeyCreate(BaseModel):
    name: str
    scopes: list[str] = []
    expires_in_days: int | None = Field(default=None, ge=1, le=3650)


@router.post("/apikeys", status_code=201)
async def create(
    body: ApiKeyCreate,
    response: Response,
    principal: Principal = require_scope("admin", user_only=True),
):
    await _ensure_admin(principal)
    name = body.name.strip()
    if not name:
        raise HTTPException(status_code=400, detail="name is required")
    if len(name) > 64:
        raise HTTPException(status_code=400, detail="name must be <= 64 characters")
    unknown = [scope for scope in body.scopes if scope not in KNOWN_SCOPES]
    if unknown:
        raise HTTPException(
            status_code=400, detail=f"unknown scopes: {', '.join(unknown)}"
        )
    try:
        max_days = max(
            0, int(get_config("security", "apikey_max_ttl_days", fallback=0) or 0)
        )
    except (ValueError, TypeError):
        logger.error("invalid security.apikey_max_ttl_days; using 0")
        max_days = 0
    max_days = min(max_days, 3650)
    days = body.expires_in_days
    if days:
        days = min(days, 3650)
    if max_days > 0:
        days = min(days or max_days, max_days)
    expires_in = days * 86400 if days else None

    try:
        token, record = await create_api_key(
            name, body.scopes, principal.username, expires_in=expires_in
        )
    except ValueError as error:
        raise HTTPException(status_code=400, detail=str(error))
    except RuntimeError:
        raise HTTPException(status_code=503, detail="API key store unavailable")
    response.headers["Cache-Control"] = "no-store"
    return {"api_key": token, **record}


@router.get("/apikeys")
async def list_keys(principal: Principal = require_scope("admin", user_only=True)):
    await _ensure_admin(principal)
    try:
        return {"api_keys": await list_api_keys()}
    except RuntimeError:
        raise HTTPException(status_code=503, detail="API key store unavailable")


@router.delete("/apikeys/{key_id}", status_code=204)
async def revoke(key_id: str, principal: Principal = require_scope("admin", user_only=True)):
    await _ensure_admin(principal)
    try:
        if not await revoke_api_key(key_id):
            raise HTTPException(status_code=404, detail="API key not found")
    except RuntimeError:
        raise HTTPException(status_code=503, detail="API key store unavailable")
