import pytest

from fastink.auth.oidc import login


class _Backend:
    def __init__(self, error):
        self.error = error

    def create_token(self, username, password):
        raise self.error


def test_krb5_timeout_propagates(monkeypatch):
    monkeypatch.setattr(
        login, "get_auth_backend", lambda name: _Backend(TimeoutError("kdc"))
    )
    with pytest.raises(TimeoutError):
        login._krb5_login("alice", "x")


def test_krb5_runtime_error_propagates(monkeypatch):
    monkeypatch.setattr(
        login, "get_auth_backend", lambda name: _Backend(RuntimeError("kdc"))
    )
    with pytest.raises(RuntimeError):
        login._krb5_login("alice", "x")


def test_krb5_invalid_password_returns_false(monkeypatch):
    monkeypatch.setattr(
        login, "get_auth_backend", lambda name: _Backend(ValueError("bad"))
    )
    assert login._krb5_login("alice", "x") is False
