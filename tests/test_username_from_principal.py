"""get_username/get_token must surface the OIDC principal in dual mode.

Bearer-authenticated requests carry the identity in request.state.principal
(UserValidationMiddleware sets it), but the business-layer helpers
get_username()/get_token() only read Ink-* headers. That breaks every v2
router in dual mode: username resolves to None and downstream code fails
with R02 (getpwnam(None)). These tests pin the expected fix: helpers prefer
the principal, falling back to the legacy headers.
"""

from fastapi import Depends, FastAPI, Request
from fastapi.testclient import TestClient

from fastink.auth.oidc.principal import Principal, get_current_principal
from fastink.routers.headers import (
    get_token,
    get_username,
    UserValidationMiddleware,
)
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

    @app.get("/api/username")
    async def username_endpoint(
        username: str = Depends(get_username),
        token: str = Depends(get_token),
    ):
        return {
            "status": InkStatus.SUCCESS,
            "username": username,
            "token": token,
        }

    return TestClient(app)


INK_HEADERS = {"Ink-Username": "alice", "Ink-Token": "ink-token"}
PRINCIPAL = Principal(
    username="alice",
    issuer="https://issuer.example",
    subject="subject-1",
)


def test_dual_bearer_principal_drives_get_username(monkeypatch):
    """dual + valid Bearer: get_username/get_token return principal identity."""
    _set_auth_config(monkeypatch, "dual")
    monkeypatch.setattr(
        "fastink.routers.headers.validate_bearer", lambda token: PRINCIPAL
    )

    response = _build_app().get(
        "/api/username", headers={"Authorization": "Bearer valid-token"}
    )

    assert response.status_code == 200
    assert response.json()["status"] == InkStatus.SUCCESS
    assert response.json()["username"] == "alice"


def test_dual_bearer_principal_get_token_returns_bearer(monkeypatch):
    """dual + valid Bearer: get_token returns the bearer token text."""
    _set_auth_config(monkeypatch, "dual")
    monkeypatch.setattr(
        "fastink.routers.headers.validate_bearer", lambda token: PRINCIPAL
    )

    response = _build_app().get(
        "/api/username", headers={"Authorization": "Bearer bearer-xyz"}
    )

    assert response.status_code == 200
    assert response.json()["token"] == "bearer-xyz"


def test_legacy_headers_still_drive_helpers(monkeypatch):
    """legacy mode: get_username/get_token keep reading Ink-* headers."""
    _set_auth_config(monkeypatch, "legacy")
    monkeypatch.setattr(
        "fastink.routers.headers.validate_token", lambda username, token: True
    )

    response = _build_app().get("/api/username", headers=INK_HEADERS)

    assert response.status_code == 200
    assert response.json()["username"] == "alice"
    assert response.json()["token"] == "ink-token"
