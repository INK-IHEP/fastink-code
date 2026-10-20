from fastapi import APIRouter, HTTPException, Request, Response
from pydantic import BaseModel
from starlette.concurrency import run_in_threadpool

from fastink.auth import permission as permission_mod
from fastink.auth import ratelimit
from fastink.auth import user as user_mod
from fastink.auth.backends.errors import (
    AccountExpiredError,
    PasswordExpiredError,
    UserNotFoundError,
)
from fastink.auth.backends.registry import get_auth_backend
from fastink.auth.principal import Principal, get_current_principal
from fastink.auth.scopes import require_scope
from fastink.auth.session import (
    DEFAULT_ABSOLUTE_TTL,
    DEFAULT_IDLE_TTL,
    DEFAULT_SCOPES,
    create_session,
    revoke_session,
)
from fastink.common.config import get_config
from fastink.common.logger import logger

router = APIRouter(tags=["auth"])

# Only backends whose create_token/validate_user actually verify credentials
# may mint v3 sessions (the AuthBackend protocol allows no-op implementations).
ALLOWED_LOGIN_BACKENDS = {"password", "krb5"}


class LoginRequest(BaseModel):
    username: str
    password: str


class UnlockRequest(BaseModel):
    username: str
    ip: str | None = None


@router.post("/auth/login")
async def login(body: LoginRequest, request: Request):
    try:
        body.username = ratelimit.normalize_login_username(body.username)
    except ValueError:
        raise HTTPException(status_code=401, detail="Invalid username or password")
    ip = ratelimit.client_ip(request)
    retry_after = await run_in_threadpool(ratelimit.reserve, ip, body.username)
    if retry_after:
        raise HTTPException(
            status_code=429,
            detail="Too many failed attempts, try again later",
            headers={"Retry-After": str(retry_after)},
        )

    try:
        auth_type = get_config("auth", "type", fallback="krb5")
        if auth_type not in ALLOWED_LOGIN_BACKENDS:
            raise HTTPException(status_code=501, detail="Login method not supported")
        backend = get_auth_backend(auth_type)
    except HTTPException:
        await run_in_threadpool(ratelimit.refund, ip, body.username)
        raise
    except Exception as error:
        await run_in_threadpool(ratelimit.refund, ip, body.username)
        logger.error("v3 login backend unavailable (%s)", error)
        raise HTTPException(
            status_code=503, detail="Authentication service unavailable"
        )

    try:
        if auth_type == "password":
            valid = await run_in_threadpool(
                backend.validate_user, body.username, body.password
            )
            if not valid:
                raise ValueError("invalid credentials")
        else:
            result = await run_in_threadpool(
                backend.create_token, body.username, body.password
            )
            if not result:
                raise ValueError("backend did not produce credentials")
    except (ValueError, UserNotFoundError, PasswordExpiredError, AccountExpiredError) as error:
        logger.warning(
            "v3 login failed for %s (method=%s): %s",
            body.username,
            auth_type,
            error,
        )
        raise HTTPException(status_code=401, detail="Invalid username or password")
    except Exception as error:
        await run_in_threadpool(ratelimit.refund, ip, body.username)
        logger.error(
            "v3 login backend error for %s (method=%s): %s",
            body.username,
            auth_type,
            error,
        )
        raise HTTPException(
            status_code=503, detail="Authentication service unavailable"
        )

    if auth_type == "password":
        try:
            if not await run_in_threadpool(
                user_mod.get_user, username=body.username
            ):
                await run_in_threadpool(
                    user_mod.add_user, body.username, None,
                    password=body.password,
                )
        except Exception as error:
            await run_in_threadpool(ratelimit.refund, ip, body.username)
            logger.error(
                "v3 login provisioning failed for %s: %s", body.username, error
            )
            raise HTTPException(
                status_code=503, detail="Authentication service unavailable"
            )

    absolute_ttl, idle_ttl = DEFAULT_ABSOLUTE_TTL, DEFAULT_IDLE_TTL

    scopes = list(DEFAULT_SCOPES)
    try:
        if await run_in_threadpool(
            permission_mod.check_user_permission,
            username=body.username,
            permission="admin",
        ):
            scopes.append("admin")
    except Exception as error:
        logger.debug("Admin scope check failed for %s: %s", body.username, error)

    try:
        token, expires_in = await create_session(
            body.username,
            scopes,
            auth_type,
            absolute_ttl=absolute_ttl,
            idle_ttl=idle_ttl,
        )
    except RuntimeError:
        await run_in_threadpool(ratelimit.refund, ip, body.username)
        raise HTTPException(
            status_code=503, detail="Authentication service unavailable"
        )
    await run_in_threadpool(ratelimit.succeed, ip, body.username)
    return {
        "access_token": token,
        "token_type": "Bearer",
        "expires_in": expires_in,
        "scope": " ".join(scopes),
    }


@router.post("/auth/unlock")
async def unlock(
    body: UnlockRequest,
    principal: Principal = require_scope("admin", user_only=True),
):
    try:
        body.username = ratelimit.normalize_login_username(body.username)
    except ValueError as error:
        raise HTTPException(status_code=400, detail=str(error))
    if body.ip:
        body.ip = body.ip.strip()
    try:
        allowed = await run_in_threadpool(
            permission_mod.check_user_permission,
            username=principal.username,
            permission="admin",
        )
    except Exception as error:
        logger.exception("admin permission check failed: %s", error)
        raise HTTPException(
            status_code=503, detail="Permission check unavailable"
        )
    if not allowed:
        raise HTTPException(status_code=403, detail="Admin permission required")
    unlocked = await run_in_threadpool(ratelimit.unlock, body.username, body.ip)
    if not unlocked:
        raise HTTPException(
            status_code=503, detail="Rate limit store unavailable"
        )
    return {"username": body.username, "unlocked": True}


@router.post("/auth/logout", status_code=204)
async def logout(request: Request):
    authorization = request.headers.get("Authorization", "")
    if authorization.startswith("Bearer "):
        token = authorization[7:].strip()
        if token.startswith("ink_"):
            raise HTTPException(
                status_code=400,
                detail="API keys cannot be logged out; revoke via /api/v3/apikeys",
            )
        try:
            await revoke_session(token)
        except RuntimeError:
            raise HTTPException(
                status_code=503, detail="Authentication service unavailable"
            )
    return Response(status_code=204)


@router.post("/auth/validate")
async def validate_token(request: Request):
    principal = get_current_principal(request)
    return {"valid": True, "username": principal.username}


@router.get("/auth/userinfo")
async def userinfo(request: Request):
    principal = get_current_principal(request)
    return {
        "sub": principal.subject,
        "preferred_username": principal.username,
        "groups": principal.groups or [],
        "scopes": principal.scopes or [],
    }


@router.get("/auth/permissions")
async def permissions(request: Request):
    principal = get_current_principal(request)
    return {
        "username": principal.username,
        "scopes": principal.scopes or [],
        "auth_method": principal.auth_method,
    }
