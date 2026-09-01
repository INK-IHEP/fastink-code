import base64
import json
import secrets
import time
from urllib.parse import urlencode

from fastapi import APIRouter, Cookie, Form, Header, HTTPException
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse

from fastink.common import config
from .discovery import discovery_document
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
from .keys import jwks


router = APIRouter()


def _error(error: Exception) -> JSONResponse:
    status_code = 503 if isinstance(error, RuntimeError) else 400
    return JSONResponse({"error": str(error).split(":", 1)[0]}, status_code=status_code)


@router.get("/.well-known/openid-configuration")
def openid_configuration():
    return discovery_document()


@router.get("/jwks")
def json_web_key_set():
    try:
        return jwks()
    except (RuntimeError, ValueError) as exc:
        return _error(exc)


_AUTH_PARAMS = ("client_id", "redirect_uri", "response_type", "scope", "state", "code_challenge", "code_challenge_method", "nonce")
_sso_pending: dict[str, tuple[dict[str, str | None], float]] = {}


def _params(**kwargs: str | None) -> dict[str, str | None]:
    return {name: kwargs.get(name) for name in _AUTH_PARAMS}


def _validate(params: dict[str, str | None]) -> None:
    validate_authorization_request(
        client_id=params["client_id"],
        redirect_uri=params["redirect_uri"],
        response_type=params["response_type"],
        code_challenge=params["code_challenge"],
        code_challenge_method=params["code_challenge_method"],
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
    except (RuntimeError, ValueError) as exc:
        return _error(exc)


@router.post("/authorize", response_class=HTMLResponse)
def authorization_login(
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
        _validate(params)
        if not verify_login(username, password):
            return login_page(params, "Invalid username or password")
        location = authorize(username=username, **params)
        return RedirectResponse(location, status_code=302)
    except (RuntimeError, ValueError) as exc:
        return _error(exc)


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
        _sso_pending[pending_id] = (params, time.time() + 600)
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
        pending = _sso_pending.pop(oidc_sso_pending, None)
        if pending is None or pending[1] <= time.time() or pending_id != oidc_sso_pending:
            raise ValueError("invalid_request")
        params = pending[0]
        identity = complete_sso_code(code)
        location = authorize(username=identity["local_username"], **params)
        response = RedirectResponse(location, status_code=302)
        response.delete_cookie("oidc_sso_pending")
        return response
    except (KeyError, RuntimeError, ValueError, json.JSONDecodeError) as exc:
        return _error(exc)


@router.post("/token")
def token_endpoint(
    grant_type: str = Form(...),
    code: str | None = Form(None),
    client_id: str = Form(...),
    redirect_uri: str = Form(""),
    code_verifier: str | None = Form(None),
    device_code: str | None = Form(None),
):
    try:
        if grant_type == "authorization_code" and code and code_verifier:
            response = JSONResponse(exchange_code(
                code=code,
                client_id=client_id,
                redirect_uri=redirect_uri,
                code_verifier=code_verifier,
            ))
            response.headers["Cache-Control"] = "no-store"
            response.headers["Pragma"] = "no-cache"
            return response
        if grant_type == "urn:ietf:params:oauth:grant-type:device_code" and device_code:
            response = JSONResponse(exchange_device_code(device_code, client_id))
            response.headers["Cache-Control"] = "no-store"
            response.headers["Pragma"] = "no-cache"
            return response
        raise ValueError("invalid_grant")
    except (RuntimeError, ValueError) as exc:
        return _error(exc)


def _bearer(authorization: str | None) -> str:
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="invalid_token")
    return authorization[7:]


@router.get("/userinfo")
@router.post("/userinfo")
def userinfo_endpoint(authorization: str | None = Header(None)):
    try:
        return userinfo(_bearer(authorization))
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
    except (RuntimeError, ValueError) as exc:
        return _error(exc)


@router.get("/device", response_class=HTMLResponse)
def device_login_page(user_code: str):
    try:
        validate_device_user_code(user_code)
        return login_page({"user_code": user_code}, action="/device")
    except (RuntimeError, ValueError) as exc:
        return _error(exc)


@router.get("/device/approve")
def removed_device_approval_endpoint():
    return JSONResponse({"error": "not_found"}, status_code=404)


@router.post("/device", response_class=HTMLResponse)
def device_login(
    username: str = Form(...), password: str = Form(...), user_code: str = Form(...)
):
    try:
        validate_device_user_code(user_code)
        if not verify_login(username, password):
            return login_page({"user_code": user_code}, "Invalid username or password", action="/device")
        approve_device_code(user_code, username)
        return HTMLResponse("<p>Device approved — return to your terminal</p>")
    except (RuntimeError, ValueError) as exc:
        return _error(exc)
