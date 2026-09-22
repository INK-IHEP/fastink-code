import pytest
from fastapi import FastAPI, Request
from fastapi.testclient import TestClient

from fastink.auth.oidc.principal import Principal
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
    client = _build_app()
    resp = client.get("/api/v3/test")
    assert resp.status_code == 401
    assert resp.json()["detail"] == "Missing or invalid Authorization header"


def test_invalid_bearer_returns_401(monkeypatch):
    monkeypatch.setattr(
        "fastink.routers.v3.middleware.validate_bearer", lambda token: None
    )
    client = _build_app()
    resp = client.get("/api/v3/test", headers={"Authorization": "Bearer invalid"})
    assert resp.status_code == 401
    assert resp.json()["detail"] == "Invalid or expired token"


def test_valid_bearer_sets_principal(monkeypatch):
    principal = Principal(
        username="alice", issuer="iss", subject="sub", auth_method="oidc"
    )
    monkeypatch.setattr(
        "fastink.routers.v3.middleware.validate_bearer", lambda token: principal
    )
    client = _build_app()
    resp = client.get("/api/v3/test", headers={"Authorization": "Bearer valid"})
    assert resp.status_code == 200
    assert resp.json()["username"] == "alice"


def test_non_v3_path_passes_through():
    client = _build_app()
    resp = client.get("/api/v2/legacy")
    assert resp.status_code == 200
    assert resp.json()["status"] == "legacy"


def test_public_path_skips_auth():
    client = _build_app(public_paths=["/api/v3/health"])
    resp = client.get("/api/v3/health")
    assert resp.status_code == 200
    assert resp.json()["status"] == "ok"


def test_non_bearer_authorization_returns_401():
    client = _build_app()
    resp = client.get("/api/v3/test", headers={"Authorization": "Basic abc123"})
    assert resp.status_code == 401
