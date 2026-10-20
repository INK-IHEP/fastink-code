import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from fastink.routers.v2 import admin_manager


def _client():
    app = FastAPI()
    app.include_router(admin_manager.router, prefix="/api/v2/admin")
    return TestClient(app)


def test_delete_user_revokes_sessions(monkeypatch):
    calls = []
    monkeypatch.setattr(admin_manager, "_authorize", lambda *a, **k: None)
    monkeypatch.setattr(admin_manager.user, "delete_user", lambda **kw: True)

    async def revoke(subject):
        calls.append(subject)
        return 1

    monkeypatch.setattr(admin_manager, "revoke_user_sessions", revoke)
    response = _client().post("/api/v2/admin/delete_user", json={"username": "alice"})
    assert response.json()["status"] == "200"
    assert calls == ["alice"]


def test_update_user_revokes_old_and_new_sessions(monkeypatch):
    calls = []
    monkeypatch.setattr(admin_manager, "_authorize", lambda *a, **k: None)
    monkeypatch.setattr(admin_manager.user, "update_user", lambda **kw: True)

    async def revoke(subject):
        calls.append(subject)
        return 1

    monkeypatch.setattr(admin_manager, "revoke_user_sessions", revoke)
    response = _client().post(
        "/api/v2/admin/update_user",
        json={"username": "alice", "new_username": "bob"},
    )
    assert response.json()["status"] == "200"
    assert calls == ["alice", "bob"]


def test_delete_user_warns_when_revocation_fails(monkeypatch):
    monkeypatch.setattr(admin_manager, "_authorize", lambda *a, **k: None)
    monkeypatch.setattr(admin_manager.user, "delete_user", lambda **kw: True)

    async def revoke(subject):
        raise RuntimeError("store down")

    monkeypatch.setattr(admin_manager, "revoke_user_sessions", revoke)
    response = _client().post("/api/v2/admin/delete_user", json={"username": "alice"})
    body = response.json()
    assert body["status"] == "200"
    assert body["data"]["sessions_revoked"] is False


def test_cli_resolves_username_from_uid(monkeypatch):
    from fastink.commands import user as user_cmd

    monkeypatch.setattr(
        user_cmd.user, "get_user", lambda **kw: {"username": "alice"}
    )
    assert user_cmd._resolve_username(None, 1234) == "alice"
    assert user_cmd._resolve_username("bob", None) == "bob"
    assert user_cmd._resolve_username(None, None) is None
