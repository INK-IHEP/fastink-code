import base64
import hashlib
import secrets
import time
from typing import Any
from urllib.parse import urlencode

import jwt

from fastink.auth.backends.krb5 import get_krb5
from fastink.common.logger import logger

from . import oidc_settings, store
from .keys import private_key, public_jwk


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
) -> None:
    client = _check_client(client_id, "authorization_code")
    _check_redirect_uri(redirect_uri, client)
    if response_type != "code":
        raise ValueError("unsupported_response_type")
    if not code_challenge or code_challenge_method != "S256":
        raise ValueError("invalid_request: S256 PKCE is required")


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
    )
    code = secrets.token_urlsafe(32)
    store.store_code(code, {
        "client_id": client_id,
        "redirect_uri": redirect_uri,
        "username": username,
        "challenge": code_challenge,
        "nonce": nonce,
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


def _jwt_claims(username: str, token_type: str) -> dict[str, Any]:
    issuer, audience = _settings()
    now = int(time.time())
    return {
        "iss": issuer,
        "sub": username,
        "aud": audience,
        "iat": now,
        "exp": now + 3600,
        "preferred_username": username,
        "typ": token_type,
    }


def _sign(claims: dict[str, Any]) -> str:
    return jwt.encode(
        claims,
        private_key(),
        algorithm="RS256",
        headers={"kid": public_jwk()["kid"]},
    )


def _token_response(username: str, nonce: str | None = None) -> dict[str, Any]:
    access_claims = _jwt_claims(username, "at+jwt")
    id_claims = _jwt_claims(username, "JWT")
    if nonce:
        id_claims["nonce"] = nonce
    return {
        "access_token": _sign(access_claims),
        "token_type": "Bearer",
        "expires_in": 3600,
        "id_token": _sign(id_claims),
        "scope": "openid",
    }


def exchange_code(
    *, code: str, client_id: str, redirect_uri: str, code_verifier: str
) -> dict[str, Any]:
    stored = _consume_code(code, client_id, redirect_uri, code_verifier)
    return _token_response(stored["username"], stored.get("nonce"))


def userinfo(token: str) -> dict[str, Any]:
    issuer, audience = _settings()
    claims = jwt.decode(
        token,
        private_key().public_key(),
        algorithms=["RS256"],
        audience=audience,
        issuer=issuer,
    )
    if claims.get("typ") != "at+jwt":
        raise ValueError("invalid_token")
    return {
        "sub": claims["sub"],
        "preferred_username": claims["preferred_username"],
    }


def create_device_code(client_id: str, scope: str) -> dict[str, Any]:
    _check_client(client_id, "urn:ietf:params:oauth:grant-type:device_code")
    device_code = secrets.token_urlsafe(32)
    user_code = secrets.token_urlsafe(6).upper()
    store.store_device_code(device_code, {
        "client_id": client_id,
        "user_code": user_code,
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


def exchange_device_code(device_code: str, client_id: str) -> dict[str, Any]:
    stored = store.get_device_code(device_code)
    if stored is None:
        raise ValueError("invalid_grant")
    if stored["client_id"] != client_id:
        raise ValueError("invalid_grant")
    if not stored.get("approved"):
        raise ValueError("authorization_pending")
    consumed = store.consume_device_code(device_code)
    if consumed is None or not consumed.get("approved"):
        raise ValueError("invalid_grant")
    return _token_response(consumed["username"])


def validate_device_user_code(user_code: str) -> None:
    if store.get_device_code_by_user_code(user_code) is None:
        raise ValueError("invalid_request")


def approve_device_code(user_code: str, username: str) -> None:
    if not store.approve_device(user_code, username):
        raise ValueError("invalid_request")


def complete_sso_login(code: str) -> dict[str, Any]:
    from .sso import complete_sso_login as complete

    return complete(code)
