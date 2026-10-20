from fastapi import FastAPI
from fastapi.testclient import TestClient

from fastink.routers.status import InkStatus
from fastink.routers.v2 import auth_manager


def _app():
    app = FastAPI()
    app.include_router(auth_manager.router, prefix="/api/v2/auth")
    return app


def _call(monkeypatch, add_error):
    calls = []

    monkeypatch.setattr(auth_manager.user, "get_user", lambda **kwargs: None)
    monkeypatch.setattr(auth_manager, "get_config", lambda *args, **kwargs: "password")

    def add_user(**kwargs):
        raise add_error

    monkeypatch.setattr(auth_manager.user, "add_user", add_user)
    monkeypatch.setattr(auth_manager.ratelimit, "reserve", lambda ip, user: None)
    monkeypatch.setattr(
        auth_manager.ratelimit, "refund", lambda ip, user: calls.append(user)
    )

    response = TestClient(_app()).post(
        "/api/v2/auth/create_and_get_token",
        json={"username": " new-user ", "password": "wrong"},
    )
    return response, calls


def test_v2_bad_password_on_new_user_keeps_reservation(monkeypatch):
    response, calls = _call(monkeypatch, ValueError("bad credentials"))
    assert response.status_code == 200
    assert response.json()["status"] == InkStatus.USER_CREATION_FAILURE
    assert calls == []


def test_v2_provisioning_service_error_refunds(monkeypatch):
    response, calls = _call(monkeypatch, RuntimeError("db down"))
    assert response.status_code == 200
    assert response.json()["status"] == InkStatus.USER_CREATION_FAILURE
    assert calls == ["new-user"]
