import base64
import hashlib
import secrets
import time
from dataclasses import dataclass
from threading import Lock
from typing import Any
from urllib.parse import urlencode

import jwt

from . import oidc_settings
from .keys import private_key, public_jwk


@dataclass
class _Code:
    client_id: str
    redirect_uri: str
    username: str
    challenge: str
    expires_at: float
    user_code: str = ""
    approved: bool = False
    nonce: str | None = None


# ponytail: in-memory codes are single-instance only; use Redis for multi-instance deployments.
_codes: dict[str, _Code] = {}
_device_codes: dict[str, _Code] = {}
_store_lock = Lock()


def _settings() -> tuple[str, str]:
    settings = oidc_settings()
    issuer = str(settings.get("issuer", "")).rstrip("/")
    audience = str(settings.get("audience", ""))
    if not issuer or not audience:
        raise RuntimeError("OIDC not configured: issuer and audience are required")
    return issuer, audience


def _check_client(client_id: str) -> None:
    _, audience = _settings()
    if client_id != audience:
        raise ValueError("unauthorized_client")


def _check_redirect_uri(redirect_uri: str) -> None:
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
    _check_client(client_id)
    _check_redirect_uri(redirect_uri)
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
    with _store_lock:
        _codes[code] = _Code(
            client_id,
            redirect_uri,
            username,
            code_challenge,
            time.time() + 300,
            nonce=nonce,
        )
    query = {"code": code}
    if state:
        query["state"] = state
    return f"{redirect_uri}?{urlencode(query)}"


def _consume_code(code: str, client_id: str, redirect_uri: str, verifier: str) -> _Code:
    with _store_lock:
        stored = _codes.get(code)
        if stored is None or stored.expires_at <= time.time():
            _codes.pop(code, None)
            raise ValueError("invalid_grant")
        if stored.client_id != client_id or stored.redirect_uri != redirect_uri:
            raise ValueError("invalid_grant")
        challenge = hashlib.sha256(verifier.encode()).digest()
        expected = base64.urlsafe_b64encode(challenge).rstrip(b"=").decode()
        if not secrets.compare_digest(expected, stored.challenge):
            raise ValueError("invalid_grant")
        del _codes[code]
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
    return _token_response(stored.username, stored.nonce)


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
    _check_client(client_id)
    device_code = secrets.token_urlsafe(32)
    user_code = secrets.token_urlsafe(6).upper()
    with _store_lock:
        _device_codes[device_code] = _Code(
            client_id, "", "anonymous", "", time.time() + 600, user_code
        )
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
    with _store_lock:
        stored = _device_codes.get(device_code)
        if stored is None or stored.expires_at <= time.time():
            _device_codes.pop(device_code, None)
            raise ValueError("invalid_grant")
        if stored.client_id != client_id:
            raise ValueError("invalid_grant")
        if not stored.approved:
            raise ValueError("authorization_pending")
        del _device_codes[device_code]
    return _token_response(stored.username)


def validate_device_user_code(user_code: str) -> None:
    with _store_lock:
        if not any(
            stored.user_code == user_code and stored.expires_at > time.time()
            for stored in _device_codes.values()
        ):
            raise ValueError("invalid_request")


def approve_device_code(user_code: str, username: str) -> None:
    with _store_lock:
        for stored in _device_codes.values():
            if stored.user_code == user_code and stored.expires_at > time.time():
                stored.username = username
                stored.approved = True
                return
    raise ValueError("invalid_request")


def complete_sso_login(code: str) -> dict[str, Any]:
    from .sso import complete_sso_login as complete

    return complete(code)
