from fastapi import FastAPI
from fastapi.testclient import TestClient

from fastink.auth.principal import Principal
from fastink.routers.v3 import auth as auth_router


def test_login_rate_limited(monkeypatch):
    app = FastAPI()
    app.include_router(auth_router.router, prefix="/api/v3")
    monkeypatch.setattr(auth_router.ratelimit, "reserve", lambda ip, user: 30)

    response = TestClient(app).post(
        "/api/v3/auth/login", json={"username": "alice", "password": "x"}
    )

    assert response.status_code == 429
    assert response.headers["retry-after"] == "30"


def test_login_reserves_attempt(monkeypatch):
    calls = []

    class Backend:
        def create_token(self, username, password):
            raise ValueError("invalid")

        def validate_user(self, username, password):
            return False

    app = FastAPI()
    app.include_router(auth_router.router, prefix="/api/v3")
    monkeypatch.setattr(
        auth_router.ratelimit,
        "reserve",
        lambda ip, user: calls.append((ip, user)),
    )
    monkeypatch.setattr(auth_router, "get_auth_backend", lambda name: Backend())

    response = TestClient(app).post(
        "/api/v3/auth/login", json={"username": " alice ", "password": "x"}
    )

    assert response.status_code == 401
    assert calls == [("testclient", "alice")]


def test_admin_can_unlock(monkeypatch):
    calls = []
    admin = Principal(
        username="admin",
        issuer="session",
        subject="admin",
        auth_method="password",
        scopes=["admin"],
    )
    monkeypatch.setattr(
        "fastink.auth.scopes.get_current_principal", lambda request: admin
    )
    monkeypatch.setattr(
        auth_router.permission_mod, "check_user_permission", lambda **kw: True
    )
    monkeypatch.setattr(
        auth_router.ratelimit,
        "unlock",
        lambda username, ip=None: calls.append((username, ip)) or True,
    )

    app = FastAPI()
    app.include_router(auth_router.router, prefix="/api/v3")
    response = TestClient(app).post(
        "/api/v3/auth/unlock", json={"username": "alice"}
    )

    assert response.status_code == 200
    assert calls == [("alice", None)]
