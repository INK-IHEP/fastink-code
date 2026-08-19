"""Unit tests for passwordless ticket refill in the krb5 backend.

Covers the SSO flow where get_krb5 hits one of its three dead-ends
(no ticket / expired ticket / renew failed) and falls back to the
hookable acquire_ccache_passwordless to mint a fresh ccache without a
password.
"""

from datetime import datetime, timedelta

import pytest

from fastink.auth.backends import krb5
from fastink.common import hooks


HOOK_NAME = "fastink.auth.backends.krb5.acquire_ccache_passwordless"


@pytest.fixture(autouse=True)
def clean_hook_registry():
    """Isolate each test from hook registrations."""
    saved = hooks._HOOKS_REGISTRY.copy()
    hooks._HOOKS_REGISTRY.clear()
    yield
    hooks._HOOKS_REGISTRY.clear()
    hooks._HOOKS_REGISTRY.update(saved)


@pytest.fixture
def stub_user(monkeypatch):
    """get_user returns a fixed record; account status is always valid."""
    monkeypatch.setattr(
        krb5, "get_user", lambda **kw: {"id": 1, "username": "alice"}
    )
    monkeypatch.setattr(
        krb5, "validate_account_status",
        lambda username=None: {"account_valid": True, "password_valid": True},
    )


@pytest.fixture
def capture_persist(monkeypatch):
    """Record calls to _persist_ccache_token instead of touching the DB."""
    calls = []
    monkeypatch.setattr(
        krb5, "_persist_ccache_token",
        lambda username, token: calls.append((username, token)),
    )
    return calls


def _register_mint(returns):
    @hooks.register_hook(HOOK_NAME)
    def _mint(username):
        return returns

    return _mint


class TestDefaultHook:
    def test_default_raises_passwordless_unavailable(self):
        with pytest.raises(krb5.PasswordlessTicketUnavailable):
            krb5.acquire_ccache_passwordless("alice")


class TestNoTicketDeadEnd:
    def test_refills_when_hook_registered(
        self, stub_user, capture_persist, monkeypatch
    ):
        _register_mint("minted-token")
        monkeypatch.setattr(
            krb5, "get_kerberos_token",
            lambda **kw: (_ for _ in ()).throw(Exception("no row")),
        )
        token = krb5.get_krb5(username="alice")
        assert token == "minted-token"
        assert capture_persist == [("alice", "minted-token")]

    def test_raises_original_when_no_hook(self, stub_user, monkeypatch):
        monkeypatch.setattr(
            krb5, "get_kerberos_token",
            lambda **kw: (_ for _ in ()).throw(Exception("no row")),
        )
        with pytest.raises(ValueError, match="Token not exists in database"):
            krb5.get_krb5(username="alice")


class TestExpiredDeadEnd:
    def test_refills_when_hook_registered(
        self, stub_user, capture_persist, monkeypatch
    ):
        _register_mint("fresh-token")
        monkeypatch.setattr(
            krb5, "get_kerberos_token",
            lambda **kw: {
                "token": "old",
                "expired_at": datetime.now() - timedelta(hours=1),
            },
        )
        token = krb5.get_krb5(username="alice")
        assert token == "fresh-token"
        assert capture_persist == [("alice", "fresh-token")]

    def test_raises_original_when_no_hook(self, stub_user, monkeypatch):
        monkeypatch.setattr(
            krb5, "get_kerberos_token",
            lambda **kw: {
                "token": "old",
                "expired_at": datetime.now() - timedelta(hours=1),
            },
        )
        with pytest.raises(ValueError, match="Token is expired"):
            krb5.get_krb5(username="alice")


class TestRenewFailedDeadEnd:
    def _expiring_ticket(self):
        # Not expired, but inside the expire_in window -> renew path.
        return {
            "token": "expiring",
            "expired_at": datetime.now() + timedelta(seconds=60),
        }

    def test_refills_when_renew_fails_and_hook_registered(
        self, stub_user, capture_persist, monkeypatch
    ):
        _register_mint("renew-fallback-token")
        monkeypatch.setattr(
            krb5, "get_kerberos_token", lambda **kw: self._expiring_ticket()
        )
        monkeypatch.setattr(
            krb5, "token_to_ccachefile", lambda token, ccachefile: None
        )
        monkeypatch.setattr(
            krb5, "_renew_tgt",
            lambda ccachefile: (_ for _ in ()).throw(ValueError("renew failed")),
        )
        token = krb5.get_krb5(username="alice", expire_in=3600)
        assert token == "renew-fallback-token"
        assert capture_persist == [("alice", "renew-fallback-token")]

    def test_raises_original_when_renew_fails_and_no_hook(
        self, stub_user, monkeypatch
    ):
        monkeypatch.setattr(
            krb5, "get_kerberos_token", lambda **kw: self._expiring_ticket()
        )
        monkeypatch.setattr(
            krb5, "token_to_ccachefile", lambda token, ccachefile: None
        )
        monkeypatch.setattr(
            krb5, "_renew_tgt",
            lambda ccachefile: (_ for _ in ()).throw(ValueError("renew failed")),
        )
        with pytest.raises(ValueError, match="renew failed"):
            krb5.get_krb5(username="alice", expire_in=3600)


class TestPrincipalResolution:
    def test_uses_db_username_when_called_by_email(
        self, capture_persist, monkeypatch
    ):
        """When get_krb5 is called by email, the mint hook must receive the
        resolved Kerberos principal, not None."""
        monkeypatch.setattr(
            krb5, "get_user",
            lambda **kw: {"id": 7, "username": "bob"},
        )
        monkeypatch.setattr(
            krb5, "validate_account_status",
            lambda username=None: {"account_valid": True, "password_valid": True},
        )
        seen = {}

        @hooks.register_hook(HOOK_NAME)
        def _mint(username):
            seen["username"] = username
            return "tok"

        monkeypatch.setattr(
            krb5, "get_kerberos_token",
            lambda **kw: (_ for _ in ()).throw(Exception("no row")),
        )
        krb5.get_krb5(email="bob@example.org")
        assert seen["username"] == "bob"
