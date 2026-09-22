import pytest
from fastapi import Depends, FastAPI, HTTPException, Request
from fastapi.testclient import TestClient

from fastink.auth.oidc.principal import Principal
from fastink.auth.scopes import require_scope


def test_scope_sufficient():
    app = FastAPI()

    @app.get("/test")
    async def endpoint(principal: Principal = require_scope("jobs:read")):
        return {"username": principal.username}

    principal = Principal(
        username="alice", issuer="iss", subject="sub",
        auth_method="oidc", scopes=["openid", "jobs:read"],
    )

    from unittest.mock import patch
    with patch("fastink.auth.scopes.get_current_principal", return_value=principal):
        client = TestClient(app)
        resp = client.get("/test")
        assert resp.status_code == 200
        assert resp.json()["username"] == "alice"


def test_scope_missing_returns_403():
    app = FastAPI()

    @app.get("/test")
    async def endpoint(principal: Principal = require_scope("jobs:read")):
        return {"username": principal.username}

    principal = Principal(
        username="alice", issuer="iss", subject="sub",
        auth_method="oidc", scopes=["openid"],
    )

    from unittest.mock import patch
    with patch("fastink.auth.scopes.get_current_principal", return_value=principal):
        client = TestClient(app)
        resp = client.get("/test")
        assert resp.status_code == 403
        assert "jobs:read" in resp.json()["detail"]


def test_scope_none_scopes_returns_403():
    app = FastAPI()

    @app.get("/test")
    async def endpoint(principal: Principal = require_scope("jobs:read")):
        return {"username": principal.username}

    principal = Principal(
        username="alice", issuer="iss", subject="sub",
        auth_method="oidc", scopes=None,
    )

    from unittest.mock import patch
    with patch("fastink.auth.scopes.get_current_principal", return_value=principal):
        client = TestClient(app)
        resp = client.get("/test")
        assert resp.status_code == 403


def test_multiple_scopes_all_required():
    app = FastAPI()

    @app.get("/test")
    async def endpoint(principal: Principal = require_scope("jobs:read", "jobs:submit")):
        return {"ok": True}

    from unittest.mock import patch

    principal = Principal(
        username="alice", issuer="iss", subject="sub",
        auth_method="oidc", scopes=["jobs:read"],
    )
    with patch("fastink.auth.scopes.get_current_principal", return_value=principal):
        client = TestClient(app)
        resp = client.get("/test")
        assert resp.status_code == 403
        assert "jobs:submit" in resp.json()["detail"]

    principal_all = Principal(
        username="alice", issuer="iss", subject="sub",
        auth_method="oidc", scopes=["jobs:read", "jobs:submit"],
    )
    with patch("fastink.auth.scopes.get_current_principal", return_value=principal_all):
        client = TestClient(app)
        resp = client.get("/test")
        assert resp.status_code == 200
