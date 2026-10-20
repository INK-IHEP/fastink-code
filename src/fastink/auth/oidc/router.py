import base64
import json
import secrets
from urllib.parse import urlencode

from fastapi import APIRouter, Cookie, Form, Header, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse

from fastink.auth import ratelimit
from fastink.common import config
from fastink.common.logger import logger
from .flows import (
    approve_device_code,
    authorize,
    create_device_code,
    exchange_code,
    exchange_device_code,
    userinfo,
    validate_device_user_code,
    validate_authorization_request,
)
from .login import login_page, verify_login
from .sso import complete_sso_code
from . import store


router = APIRouter()


def _error(error: Exception) -> JSONResponse:
    status_code = 503 if isinstance(error, (RuntimeError, TimeoutError)) else 400
    return JSONResponse({"error": str(error).split(":", 1)[0]}, status_code=status_code)


_AUTH_PARAMS = ("client_id", "redirect_uri", "response_type", "scope", "state", "code_challenge", "code_challenge_method", "nonce")


def _params(**kwargs: str | None) -> dict[str, str | None]:
    return {name: kwargs.get(name) for name in _AUTH_PARAMS}


def _validate(params: dict[str, str | None]) -> None:
    validate_authorization_request(
        client_id=params["client_id"],
        redirect_uri=params["redirect_uri"],
        response_type=params["response_type"],
        code_challenge=params["code_challenge"],
        code_challenge_method=params["code_challenge_method"],
        scope=params.get("scope"),
    )


@router.get("/authorize", response_class=HTMLResponse)
def authorization_endpoint(
    client_id: str,
    redirect_uri: str,
    response_type: str,
    scope: str = "openid",
    state: str | None = None,
    code_challenge: str | None = None,
    code_challenge_method: str | None = None,
    nonce: str | None = None,
):
    params = _params(
        client_id=client_id, redirect_uri=redirect_uri, response_type=response_type,
        scope=scope, state=state, code_challenge=code_challenge,
        code_challenge_method=code_challenge_method, nonce=nonce,
    )
    try:
        _validate(params)
        return login_page(params)
    except (RuntimeError, ValueError, TimeoutError) as exc:
        return _error(exc)


@router.post("/authorize", response_class=HTMLResponse)
def authorization_login(
    request: Request,
    username: str = Form(...), password: str = Form(...), client_id: str = Form(...),
    redirect_uri: str = Form(...), response_type: str = Form(...), scope: str = Form("openid"),
    state: str | None = Form(None), code_challenge: str | None = Form(None),
    code_challenge_method: str | None = Form(None), nonce: str | None = Form(None),
):
    params = _params(
        client_id=client_id, redirect_uri=redirect_uri, response_type=response_type,
        scope=scope, state=state, code_challenge=code_challenge,
        code_challenge_method=code_challenge_method, nonce=nonce,
    )
    try:
        username = ratelimit.normalize_login_username(username)
    except ValueError:
        return login_page(params, "Invalid username or password")
    ip = ratelimit.client_ip(request)
    try:
        _validate(params)
        retry_after = ratelimit.reserve(ip, username)
        if retry_after:
            return HTMLResponse(
                login_page(params, "Too many failed attempts, try again later"),
                status_code=429,
                headers={"Retry-After": str(retry_after)},
            )
        try:
            valid = verify_login(username, password)
        except (RuntimeError, ValueError, TimeoutError):
            ratelimit.refund(ip, username)
            raise
        if not valid:
            return login_page(params, "Invalid username or password")
        ratelimit.succeed(ip, username)
        location = authorize(username=username, **params)
        return RedirectResponse(location, status_code=302)
    except (RuntimeError, ValueError, TimeoutError) as exc:
        return _error(exc)
    except HTTPException:
        ratelimit.refund(ip, username)
        raise
    except Exception as exc:
        ratelimit.refund(ip, username)
        logger.exception("authorize login backend error for %s: %s", username, exc)
        return _error(RuntimeError("Authentication service unavailable"))


@router.get("/authorize/sso")
def sso_authorization_endpoint(
    client_id: str, redirect_uri: str, response_type: str, scope: str = "openid",
    state: str | None = None, code_challenge: str | None = None,
    code_challenge_method: str | None = None, nonce: str | None = None,
):
    params = _params(
        client_id=client_id, redirect_uri=redirect_uri, response_type=response_type,
        scope=scope, state=state, code_challenge=code_challenge,
        code_challenge_method=code_challenge_method, nonce=nonce,
    )
    try:
        _validate(params)
        settings = config.get_config("auth", "sso", fallback={}) or {}
        authorize_url = settings.get("authorize_url") or ""
        app_key = settings.get("app_key") or ""
        redirect_uri = settings.get("redirect_uri") or ""
        if not authorize_url or not app_key or not redirect_uri:
            raise ValueError("invalid_request: SSO not configured")
        pending_id = secrets.token_urlsafe(32)
        state_params = {name: value for name, value in params.items() if value is not None}
        state_params["pending_id"] = pending_id
        store.store_sso_pending(pending_id, params)
        state = base64.urlsafe_b64encode(json.dumps(state_params).encode()).rstrip(b"=").decode()
        location = str(authorize_url)
        query = {
            "client_id": str(app_key),
            "redirect_uri": str(redirect_uri),
            "response_type": "code",
            "state": state,
        }
        response = RedirectResponse(f"{location}?{urlencode(query)}", status_code=302)
        response.set_cookie("oidc_sso_pending", pending_id, httponly=True, samesite="lax", secure=True)
        return response
    except (KeyError, RuntimeError, ValueError) as exc:
        return _error(exc)


@router.get("/authorize/sso/callback")
def sso_callback(
    code: str, state: str, oidc_sso_pending: str | None = Cookie(None)
):
    try:
        if not oidc_sso_pending:
            raise ValueError("invalid_request")
        decoded = base64.urlsafe_b64decode(state + "===")
        state_params = json.loads(decoded)
        pending_id = state_params.pop("pending_id", None)
        pending = store.consume_sso_pending(oidc_sso_pending)
        if pending is None or pending_id != oidc_sso_pending:
            raise ValueError("invalid_request")
        params = pending
        identity = complete_sso_code(code)
        location = authorize(username=identity["local_username"], **params)
        response = RedirectResponse(location, status_code=302)
        response.delete_cookie("oidc_sso_pending")
        return response
    except (KeyError, RuntimeError, ValueError, json.JSONDecodeError) as exc:
        return _error(exc)


@router.post("/token")
async def token_endpoint(
    grant_type: str = Form(...),
    code: str | None = Form(None),
    client_id: str = Form(...),
    redirect_uri: str = Form(""),
    code_verifier: str | None = Form(None),
    device_code: str | None = Form(None),
):
    try:
        if grant_type == "authorization_code" and code and code_verifier:
            response = JSONResponse(await exchange_code(
                code=code,
                client_id=client_id,
                redirect_uri=redirect_uri,
                code_verifier=code_verifier,
            ))
            response.headers["Cache-Control"] = "no-store"
            response.headers["Pragma"] = "no-cache"
            return response
        if grant_type == "urn:ietf:params:oauth:grant-type:device_code" and device_code:
            response = JSONResponse(await exchange_device_code(device_code, client_id))
            response.headers["Cache-Control"] = "no-store"
            response.headers["Pragma"] = "no-cache"
            return response
        raise ValueError("invalid_grant")
    except (RuntimeError, ValueError, TimeoutError) as exc:
        return _error(exc)


def _bearer(authorization: str | None) -> str:
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="invalid_token")
    return authorization[7:]


@router.get("/userinfo")
@router.post("/userinfo")
async def userinfo_endpoint(authorization: str | None = Header(None)):
    try:
        return await userinfo(_bearer(authorization))
    except HTTPException:
        raise
    except Exception as exc:
        return _error(exc)


@router.post("/device_authorization")
def device_authorization_endpoint(
    client_id: str = Form(...), scope: str = Form("openid")
):
    try:
        return create_device_code(client_id, scope)
    except (RuntimeError, ValueError, TimeoutError) as exc:
        return _error(exc)


def _device_notice(record: dict) -> str:
    scope = record.get("scope") or "openid"
    return f"Application {record.get('client_id', '')} requests scope: {scope}"


@router.get("/device", response_class=HTMLResponse)
def device_login_page(user_code: str):
    try:
        record = validate_device_user_code(user_code)
        return login_page(
            {"user_code": user_code},
            action="/device",
            notice=_device_notice(record),
        )
    except (RuntimeError, ValueError, TimeoutError) as exc:
        return _error(exc)


@router.get("/device/approve")
def removed_device_approval_endpoint():
    return JSONResponse({"error": "not_found"}, status_code=404)


@router.post("/device", response_class=HTMLResponse)
def device_login(
    request: Request,
    username: str = Form(...), password: str = Form(...), user_code: str = Form(...)
):
    ip = ratelimit.client_ip(request)
    try:
        record = validate_device_user_code(user_code)
        params = {"user_code": user_code}
        try:
            username = ratelimit.normalize_login_username(username)
        except ValueError:
            return login_page(
                params,
                "Invalid username or password",
                action="/device",
                notice=_device_notice(record),
            )
        retry_after = ratelimit.reserve(ip, username)
        if retry_after:
            return HTMLResponse(
                login_page(
                    params,
                    "Too many failed attempts, try again later",
                    action="/device",
                    notice=_device_notice(record),
                ),
                status_code=429,
                headers={"Retry-After": str(retry_after)},
            )
        try:
            valid = verify_login(username, password)
        except (RuntimeError, ValueError, TimeoutError):
            ratelimit.refund(ip, username)
            raise
        if not valid:
            return login_page(
                params,
                "Invalid username or password",
                action="/device",
                notice=_device_notice(record),
            )
        ratelimit.succeed(ip, username)
        approve_device_code(user_code, username)
        return HTMLResponse("<p>Device approved — return to your terminal</p>")
    except (RuntimeError, ValueError, TimeoutError) as exc:
        return _error(exc)
    except HTTPException:
        ratelimit.refund(ip, username)
        raise
    except Exception as exc:
        ratelimit.refund(ip, username)
        logger.exception("device login backend error for %s: %s", username, exc)
        return _error(RuntimeError("Authentication service unavailable"))
