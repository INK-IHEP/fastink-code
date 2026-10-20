from unittest.mock import patch

from fastapi import FastAPI, Request
from fastapi.testclient import TestClient

from fastink.auth.principal import Principal
from fastink.routers.v3.middleware import BearerAuthMiddleware


def _build_app(public_paths=None):
    app = FastAPI()
    app.add_middleware(BearerAuthMiddleware, public_paths=public_paths or [])

    @app.get("/api/v3/test")
    async def protected(request: Request):
        principal = getattr(request.state, "principal", None)
        return {"username": principal.username if principal else None}

    @app.get("/api/v3/health")
    async def health():
        return {"status": "ok"}

    @app.get("/api/v2/legacy")
    async def legacy():
        return {"status": "legacy"}

    return TestClient(app)


def test_missing_bearer_returns_401():
    resp = _build_app().get("/api/v3/test")
    assert resp.status_code == 401
    assert resp.json()["detail"] == "Missing or invalid Authorization header"
    assert resp.headers["www-authenticate"] == "Bearer"


def test_invalid_session_returns_401():
    async def invalid(token):
        return None

    with patch("fastink.routers.v3.middleware.validate_session", invalid):
        resp = _build_app().get(
            "/api/v3/test", headers={"Authorization": "Bearer invalid"}
        )
    assert resp.status_code == 401
    assert resp.json()["detail"] == "Invalid or expired token"


def test_valid_session_sets_principal():
    principal = Principal(
        username="alice", issuer="session", subject="alice", auth_method="password"
    )

    async def valid(token):
        return principal

    with patch("fastink.routers.v3.middleware.validate_session", valid):
        resp = _build_app().get(
            "/api/v3/test", headers={"Authorization": "Bearer valid-token"}
        )
    assert resp.status_code == 200
    assert resp.json()["username"] == "alice"


def test_session_store_failure_returns_503():
    async def broken(token):
        raise RuntimeError("session store unavailable: down")

    with patch("fastink.routers.v3.middleware.validate_session", broken):
        resp = _build_app().get(
            "/api/v3/test", headers={"Authorization": "Bearer any"}
        )
    assert resp.status_code == 503
    assert resp.json()["detail"] == "Authentication service unavailable"


def test_non_v3_path_passes_through():
    resp = _build_app().get("/api/v2/legacy")
    assert resp.status_code == 200
    assert resp.json()["status"] == "legacy"


def test_public_path_skips_auth():
    resp = _build_app(public_paths=["/api/v3/health"]).get("/api/v3/health")
    assert resp.status_code == 200
    assert resp.json()["status"] == "ok"


def test_non_bearer_authorization_returns_401():
    resp = _build_app().get("/api/v3/test", headers={"Authorization": "Basic abc123"})
    assert resp.status_code == 401
