import pytest
from fastapi.testclient import TestClient

from fastink.auth.backends.errors import (
    AccountExpiredError,
    PasswordExpiredError,
    UserNotFoundError,
)
from fastink.main import app
from fastink.routers.status import InkStatus

client = TestClient(app)


class _FakeBackend:
    def __init__(self, create_error=None, get_error=None):
        self.create_error = create_error
        self.get_error = get_error

    def create_token(self, username, password=None):
        if self.create_error is not None:
            raise self.create_error
        return {"method": "krb5"}

    def get_token(self, username):
        if self.get_error is not None:
            raise self.get_error
        return "token"


def _patch(monkeypatch, backend):
    monkeypatch.setattr(
        "fastink.auth.user.get_user", lambda username: {"id": 1}
    )
    monkeypatch.setattr(
        "fastink.routers.v2.auth_manager.get_auth_backend", lambda: backend
    )


def _post(monkeypatch, endpoint, error):
    _patch(monkeypatch, _FakeBackend(create_error=error, get_error=error))
    return client.post(
        endpoint,
        json={"username": "alice", "password": "secret"},
    )


def _post_create_ok_get_error(monkeypatch, error):
    """create_and_get_token where create_token succeeds and only get_token
    raises — proves the get_token path maps typed errors too."""
    _patch(monkeypatch, _FakeBackend(create_error=None, get_error=error))
    return client.post(
        "/api/v2/auth/create_and_get_token",
        json={"username": "alice", "password": "secret"},
    )


def _get(monkeypatch, error):
    _patch(monkeypatch, _FakeBackend(get_error=error))
    return client.get("/api/v2/auth/get_token", params={"username": "alice"})


def _assert_envelope(resp, status_code):
    """Standard body contract: HTTP 200 + {status, msg, data=None}."""
    assert resp.status_code == 200
    body = resp.json()
    assert body["status"] == status_code
    assert isinstance(body["msg"], str) and body["msg"]
    assert body["data"] is None


def test_create_token_user_not_found(monkeypatch):
    resp = _post(monkeypatch, "/api/v2/auth/create_token", UserNotFoundError())
    _assert_envelope(resp, InkStatus.USER_NOT_FOUND)


def test_create_token_account_expired(monkeypatch):
    resp = _post(monkeypatch, "/api/v2/auth/create_token", AccountExpiredError())
    _assert_envelope(resp, InkStatus.ACCOUNT_EXPIRED)


def test_create_token_password_expired(monkeypatch):
    resp = _post(monkeypatch, "/api/v2/auth/create_token", PasswordExpiredError())
    _assert_envelope(resp, InkStatus.PASSWORD_EXPIRED)


def test_get_token_user_not_found(monkeypatch):
    resp = _get(monkeypatch, UserNotFoundError())
    _assert_envelope(resp, InkStatus.USER_NOT_FOUND)


def test_get_token_account_expired(monkeypatch):
    resp = _get(monkeypatch, AccountExpiredError())
    _assert_envelope(resp, InkStatus.ACCOUNT_EXPIRED)


def test_get_token_password_expired(monkeypatch):
    resp = _get(monkeypatch, PasswordExpiredError())
    _assert_envelope(resp, InkStatus.PASSWORD_EXPIRED)


def test_create_and_get_token_user_not_found(monkeypatch):
    resp = _post(
        monkeypatch, "/api/v2/auth/create_and_get_token", UserNotFoundError()
    )
    _assert_envelope(resp, InkStatus.USER_NOT_FOUND)


def test_create_and_get_token_account_expired(monkeypatch):
    resp = _post(
        monkeypatch, "/api/v2/auth/create_and_get_token", AccountExpiredError()
    )
    _assert_envelope(resp, InkStatus.ACCOUNT_EXPIRED)


def test_create_and_get_token_password_expired(monkeypatch):
    resp = _post(
        monkeypatch, "/api/v2/auth/create_and_get_token", PasswordExpiredError()
    )
    _assert_envelope(resp, InkStatus.PASSWORD_EXPIRED)


def test_create_and_get_token_get_token_user_not_found(monkeypatch):
    resp = _post_create_ok_get_error(monkeypatch, UserNotFoundError())
    _assert_envelope(resp, InkStatus.USER_NOT_FOUND)


def test_create_and_get_token_get_token_account_expired(monkeypatch):
    resp = _post_create_ok_get_error(monkeypatch, AccountExpiredError())
    _assert_envelope(resp, InkStatus.ACCOUNT_EXPIRED)


def test_create_and_get_token_get_token_password_expired(monkeypatch):
    resp = _post_create_ok_get_error(monkeypatch, PasswordExpiredError())
    _assert_envelope(resp, InkStatus.PASSWORD_EXPIRED)


def test_create_token_generic_error_still_maps_to_a04(monkeypatch):
    resp = _post(monkeypatch, "/api/v2/auth/create_token", RuntimeError("boom"))
    _assert_envelope(resp, InkStatus.TOKEN_CREATION_FAILURE)


def test_get_token_generic_error_still_maps_to_a02(monkeypatch):
    resp = _get(monkeypatch, RuntimeError("boom"))
    _assert_envelope(resp, InkStatus.TOKEN_INVALID)


def test_create_token_not_implemented_still_maps_to_a04(monkeypatch):
    resp = _post(
        monkeypatch, "/api/v2/auth/create_token", NotImplementedError("nope")
    )
    _assert_envelope(resp, InkStatus.TOKEN_CREATION_FAILURE)
