import json
from dataclasses import asdict

import jwt
import pytest
from cryptography.hazmat.primitives.asymmetric import rsa
from fastapi import Depends, FastAPI, Request
from fastapi.testclient import TestClient

from fastink.auth.oidc.bearer import validate_bearer
from fastink.auth.oidc.principal import Principal, get_current_principal
from fastink.routers.headers import UserValidationMiddleware
from fastink.routers.status import InkStatus


def _set_auth_config(monkeypatch, mode):
    from fastink.common import config

    settings = {
        "issuer": "https://issuer.example",
        "audience": "fastink",
        "algorithms": ["RS256"],
        "jwks_ttl": 300,
        "leeway": 0,
    }

    def fake_get_config(section=None, option=None, fallback=None, **_kwargs):
        if section == "auth" and option == "mode":
            return mode
        if section == "auth" and option == "oidc":
            return settings
        return fallback

    monkeypatch.setattr(config, "get_config", fake_get_config)


def _build_app():
    app = FastAPI()
    app.add_middleware(UserValidationMiddleware, skip_routers=[])

    @app.get("/api/protected")
    async def protected(request: Request):
        principal = getattr(request.state, "principal", None)
        return {
            "status": InkStatus.SUCCESS,
            "principal": asdict(principal) if principal else None,
        }

    @app.get("/api/dependency")
    async def dependency(principal: Principal = Depends(get_current_principal)):
        return {"status": InkStatus.SUCCESS, "username": principal.username}

    return TestClient(app)


INK_HEADERS = {"Ink-Username": "alice", "Ink-Token": "ink-token"}


def test_legacy_uses_ink_headers_without_consulting_authorization(monkeypatch):
    _set_auth_config(monkeypatch, "legacy")
    monkeypatch.setattr(
        "fastink.routers.headers.validate_token", lambda username, token: True
    )
    consulted = False

    def fail_if_consulted(token):
        nonlocal consulted
        consulted = True
        raise AssertionError("legacy mode must not inspect bearer tokens")

    monkeypatch.setattr(
        "fastink.routers.headers.validate_bearer",
        fail_if_consulted,
        raising=False,
    )

    response = _build_app().get(
        "/api/protected",
        headers={**INK_HEADERS, "Authorization": "Bearer should-not-be-read"},
    )

    assert response.status_code == 200
    assert response.json()["status"] == InkStatus.SUCCESS
    assert consulted is False


def test_dual_accepts_valid_bearer_and_sets_request_principal(monkeypatch):
    _set_auth_config(monkeypatch, "dual")
    principal = Principal(
        username="alice",
        issuer="https://issuer.example",
        subject="subject-1",
        groups=["users"],
        scopes=["openid"],
    )
    monkeypatch.setattr("fastink.routers.headers.validate_bearer", lambda token: principal)

    response = _build_app().get(
        "/api/protected", headers={"Authorization": "Bearer valid-token"}
    )

    assert response.status_code == 200
    assert response.json()["status"] == InkStatus.SUCCESS
    assert response.json()["principal"] == asdict(principal)

    dependency_response = _build_app().get(
        "/api/dependency", headers={"Authorization": "Bearer valid-token"}
    )
    assert dependency_response.json() == {"status": InkStatus.SUCCESS, "username": "alice"}


def test_dual_rejects_invalid_bearer_without_falling_back_to_ink(monkeypatch):
    _set_auth_config(monkeypatch, "dual")
    ink_calls = []
    monkeypatch.setattr("fastink.routers.headers.validate_bearer", lambda token: None)
    monkeypatch.setattr(
        "fastink.routers.headers.validate_token",
        lambda username, token: ink_calls.append((username, token)) or True,
    )

    response = _build_app().get(
        "/api/protected",
        headers={**INK_HEADERS, "Authorization": "Bearer invalid-token"},
    )

    assert response.status_code == 200
    assert response.json()["status"] == InkStatus.USER_INVALID
    assert ink_calls == []


def test_dual_without_authorization_falls_back_to_ink(monkeypatch):
    _set_auth_config(monkeypatch, "dual")
    ink_calls = []
    monkeypatch.setattr(
        "fastink.routers.headers.validate_token",
        lambda username, token: ink_calls.append((username, token)) or True,
    )

    response = _build_app().get("/api/protected", headers=INK_HEADERS)

    assert response.status_code == 200
    assert response.json()["status"] == InkStatus.SUCCESS
    assert ink_calls == [("alice", "ink-token")]


def test_oidc_rejects_ink_headers_without_bearer(monkeypatch):
    _set_auth_config(monkeypatch, "oidc")
    ink_calls = []
    monkeypatch.setattr(
        "fastink.routers.headers.validate_token",
        lambda username, token: ink_calls.append((username, token)) or True,
    )

    response = _build_app().get("/api/protected", headers=INK_HEADERS)

    assert response.status_code == 200
    assert response.json()["status"] == InkStatus.USER_INVALID
    assert ink_calls == []


def test_get_current_principal_requires_middleware_state():
    from starlette.requests import Request

    request = Request({"type": "http", "headers": [], "state": {}})

    with pytest.raises(Exception) as error:
        get_current_principal(request)

    assert getattr(error.value, "status_code", None) == 401


def test_validate_bearer_verifies_signature_and_claims(monkeypatch):
    from fastink.auth.oidc import bearer

    private_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    public_jwk = jwt.algorithms.RSAAlgorithm.to_jwk(private_key.public_key())
    monkeypatch.setattr(
        bearer,
        "oidc_settings",
        lambda: {
            "issuer": "https://issuer.example",
            "audience": "fastink",
            "algorithms": ["RS256"],
            "leeway": 0,
        },
    )
    monkeypatch.setattr(
        bearer,
        "jwks_for_issuer",
        lambda issuer: {"keys": [{**json.loads(public_jwk), "kid": "test-key"}]},
    )
    token = jwt.encode(
        {
            "iss": "https://issuer.example",
            "sub": "subject-1",
            "preferred_username": "alice",
            "aud": "fastink",
            "exp": 4102444800,
            "typ": "at+jwt",
            "groups": ["users"],
            "scope": "openid profile",
        },
        private_key,
        algorithm="RS256",
        headers={"kid": "test-key"},
    )

    principal = validate_bearer(token)

    assert principal == Principal(
        username="alice",
        issuer="https://issuer.example",
        subject="subject-1",
        groups=["users"],
        scopes=["openid", "profile"],
    )


def test_validate_bearer_rejects_id_token(monkeypatch):
    from fastink.auth.oidc import bearer

    private_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    public_jwk = jwt.algorithms.RSAAlgorithm.to_jwk(private_key.public_key())
    monkeypatch.setattr(bearer, "oidc_settings", lambda: {"issuer": "https://issuer.example", "audience": "fastink", "algorithms": ["RS256"]})
    monkeypatch.setattr(bearer, "jwks_for_issuer", lambda issuer: {"keys": [{**json.loads(public_jwk), "kid": "test-key"}]})
    token = jwt.encode({"iss": "https://issuer.example", "sub": "subject-1", "aud": "fastink", "exp": 4102444800, "typ": "JWT"}, private_key, algorithm="RS256", headers={"kid": "test-key"})
    assert validate_bearer(token) is None
