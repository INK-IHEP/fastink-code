import base64
import hashlib
import secrets
from typing import Any
from urllib.parse import urlencode

from fastink.auth import permission
from fastink.auth.backends.krb5 import get_krb5
from fastink.auth.session import create_session
from fastink.common.logger import logger
from starlette.concurrency import run_in_threadpool

from . import oidc_settings, store


def _settings() -> tuple[str, str]:
    settings = oidc_settings()
    issuer = str(settings.get("issuer", "")).rstrip("/")
    audience = str(settings.get("audience", ""))
    if not issuer or not audience:
        raise RuntimeError("OIDC not configured: issuer and audience are required")
    return issuer, audience


def _clients() -> list[dict[str, Any]]:
    settings = oidc_settings()
    clients = settings.get("clients")
    if clients:
        return clients
    return [{
        "client_id": _settings()[1],
        "redirect_uris": settings.get("allowed_redirect_uris", []),
        "grants": ["authorization_code", "urn:ietf:params:oauth:grant-type:device_code"],
        "scopes": ["openid", "profile"],
    }]


def _find_client(client_id: str) -> dict[str, Any]:
    for client in _clients():
        if client.get("client_id") == client_id:
            return client
    raise ValueError("unauthorized_client")


def _check_client(client_id: str, grant_type: str | None = None) -> dict[str, Any]:
    client = _find_client(client_id)
    if grant_type and grant_type not in client.get("grants", []):
        raise ValueError("unauthorized_client")
    return client


def _granted_scopes(client: dict[str, Any], requested: str | None) -> list[str]:
    allowed = [str(scope) for scope in (client.get("scopes") or [])]
    wanted = [scope for scope in (requested or "").split() if scope] or ["openid"]
    unknown = [scope for scope in wanted if scope not in allowed]
    if unknown:
        raise ValueError("invalid_scope")
    return wanted


async def _effective_scopes(
    client: dict[str, Any], requested: str | None, username: str
) -> list[str]:
    scopes = _granted_scopes(client, requested)
    if "admin" in scopes:
        try:
            is_admin = await run_in_threadpool(
                permission.check_user_permission,
                username=username,
                permission="admin",
            )
        except Exception:
            is_admin = False
        if not is_admin:
            scopes = [scope for scope in scopes if scope != "admin"]
    return scopes


def _check_redirect_uri(redirect_uri: str, client: dict[str, Any] | None = None) -> None:
    allowed = []
    if client is not None:
        allowed = client.get("redirect_uris", [])
    if not allowed:
        allowed = oidc_settings().get("allowed_redirect_uris") or []
    if not isinstance(allowed, (list, tuple, set)):
        raise ValueError("invalid_request: redirect_uri allowlist misconfigured")
    if redirect_uri not in allowed:
        raise ValueError("invalid_request: redirect_uri is not registered")


def validate_authorization_request(
    *,
    client_id: str,
    redirect_uri: str,
    response_type: str,
    code_challenge: str | None,
    code_challenge_method: str | None,
    scope: str | None = None,
) -> None:
    client = _check_client(client_id, "authorization_code")
    _check_redirect_uri(redirect_uri, client)
    if response_type != "code":
        raise ValueError("unsupported_response_type")
    if not code_challenge or code_challenge_method != "S256":
        raise ValueError("invalid_request: S256 PKCE is required")
    if scope:
        _granted_scopes(client, scope)


def authorize(
    *,
    client_id: str,
    redirect_uri: str,
    response_type: str,
    scope: str,
    state: str | None,
    code_challenge: str | None,
    code_challenge_method: str | None,
    username: str = "anonymous",
    nonce: str | None = None,
) -> str:
    validate_authorization_request(
        client_id=client_id,
        redirect_uri=redirect_uri,
        response_type=response_type,
        code_challenge=code_challenge,
        code_challenge_method=code_challenge_method,
        scope=scope,
    )
    code = secrets.token_urlsafe(32)
    store.store_code(code, {
        "client_id": client_id,
        "redirect_uri": redirect_uri,
        "username": username,
        "challenge": code_challenge,
        "scope": scope,
    })
    if username != "anonymous":
        try:
            get_krb5(username=username)
        except Exception as e:
            logger.warning("Post-login credential ensure failed for %s: %s", username, e)
    query = {"code": code}
    if state:
        query["state"] = state
    return f"{redirect_uri}?{urlencode(query)}"


def _consume_code(code: str, client_id: str, redirect_uri: str, verifier: str) -> dict:
    result = store.get_code(code)
    if result is None:
        raise ValueError("invalid_grant")
    stored, raw = result
    if stored["client_id"] != client_id or stored["redirect_uri"] != redirect_uri:
        raise ValueError("invalid_grant")
    challenge = hashlib.sha256(verifier.encode()).digest()
    expected = base64.urlsafe_b64encode(challenge).rstrip(b"=").decode()
    if not secrets.compare_digest(expected, stored["challenge"]):
        raise ValueError("invalid_grant")
    if not store.delete_code_if_matches(code, raw):
        raise ValueError("invalid_grant")
    return stored


async def exchange_code(
    *, code: str, client_id: str, redirect_uri: str, code_verifier: str
) -> dict[str, Any]:
    stored = await run_in_threadpool(
        _consume_code, code, client_id, redirect_uri, code_verifier
    )
    client = _check_client(client_id, "authorization_code")
    scopes = await _effective_scopes(client, stored.get("scope"), stored["username"])
    token, expires_in = await create_session(stored["username"], scopes, "oauth")
    return {
        "access_token": token,
        "token_type": "Bearer",
        "expires_in": expires_in,
        "scope": " ".join(scopes),
    }


async def userinfo(token: str) -> dict[str, Any]:
    from fastink.auth.session import validate_session

    principal = await validate_session(token)
    if principal is None:
        raise ValueError("invalid_token")
    return {
        "sub": principal.subject,
        "preferred_username": principal.username,
    }


def create_device_code(client_id: str, scope: str) -> dict[str, Any]:
    client = _check_client(client_id, "urn:ietf:params:oauth:grant-type:device_code")
    granted = _granted_scopes(client, scope)
    device_code = secrets.token_urlsafe(32)
    user_code = secrets.token_urlsafe(6).upper()
    store.store_device_code(device_code, {
        "client_id": client_id,
        "user_code": user_code,
        "scope": " ".join(granted),
        "interval": 5,
    })
    issuer, _ = _settings()
    return {
        "device_code": device_code,
        "user_code": user_code,
        "verification_uri": f"{issuer}/device",
        "verification_uri_complete": f"{issuer}/device?user_code={user_code}",
        "expires_in": 600,
        "interval": 5,
    }


async def exchange_device_code(device_code: str, client_id: str) -> dict[str, Any]:
    stored = await run_in_threadpool(store.poll_device_code, device_code)
    if stored == "slow_down":
        raise ValueError("slow_down")
    if stored is None:
        raise ValueError("invalid_grant")
    if stored["client_id"] != client_id:
        raise ValueError("invalid_grant")
    if not stored.get("approved"):
        raise ValueError("authorization_pending")
    consumed = await run_in_threadpool(store.consume_device_code, device_code)
    if consumed is None or not consumed.get("approved"):
        raise ValueError("invalid_grant")
    client = _check_client(client_id, "urn:ietf:params:oauth:grant-type:device_code")
    scopes = await _effective_scopes(
        client, consumed.get("scope"), consumed["username"]
    )
    token, expires_in = await create_session(consumed["username"], scopes, "device")
    return {
        "access_token": token,
        "token_type": "Bearer",
        "expires_in": expires_in,
        "scope": " ".join(scopes),
    }


def validate_device_user_code(user_code: str) -> dict[str, Any]:
    record = store.get_device_code_by_user_code(user_code)
    if record is None:
        raise ValueError("invalid_request")
    return record


def approve_device_code(user_code: str, username: str) -> None:
    if not store.approve_device(user_code, username):
        raise ValueError("invalid_request")
