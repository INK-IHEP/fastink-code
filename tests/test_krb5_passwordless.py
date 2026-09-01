"""Unit tests for passwordless ticket refill in the krb5 backend.

Covers the SSO flow where get_krb5 hits one of its three dead-ends
(no ticket / expired ticket / expiring soon) and falls back to the
hookable acquire_ccache_passwordless to mint a fresh ccache without a
password.
"""

import base64
from datetime import datetime, timedelta
import logging

import pytest

from fastink.auth.backends import krb5
from fastink.auth.backends.errors import (
    AccountExpiredError,
    PasswordExpiredError,
    UserNotFoundError,
)
from fastink.common import hooks
from tests.test_ccache import _cred, build_ccache


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


def _ccache_token(*, endtime: int, renew_until: int) -> str:
    data = build_ccache(
        4,
        creds=(
            _cred(
                4,
                (b"EXAMPLE.COM", b"krbtgt", b"EXAMPLE.COM"),
                endtime=endtime,
                renew_until=renew_until,
            ),
        ),
    )
    return base64.b64encode(data).decode()


class TestDefaultHook:
    def test_default_raises_passwordless_unavailable(self):
        with pytest.raises(krb5.PasswordlessTicketUnavailable):
            krb5.acquire_ccache_passwordless("alice")


class TestNoTicketDeadEnd:
    def test_refills_when_hook_registered(
        self, stub_user, capture_persist, monkeypatch, caplog
    ):
        _register_mint("minted-token")
        monkeypatch.setattr(
            krb5, "get_kerberos_token",
            lambda **kw: (_ for _ in ()).throw(Exception("no row")),
        )
        with caplog.at_level(logging.INFO, logger="ink"):
            token = krb5.get_krb5(username="alice")
        assert token == "minted-token"
        assert capture_persist == [("alice", "minted-token")]
        assert any(
            record.message
            == "No Kerberos token in DB for alice (no row); attempting passwordless refill."
            for record in caplog.records
        )
        assert any(
            record.message == "Passwordless ticket acquired for alice, persisting."
            for record in caplog.records
        )

    def test_raises_original_when_no_hook(self, stub_user, monkeypatch, caplog):
        monkeypatch.setattr(
            krb5, "get_kerberos_token",
            lambda **kw: (_ for _ in ()).throw(Exception("no row")),
        )
        with caplog.at_level(logging.INFO, logger="ink"):
            with pytest.raises(ValueError, match="Token not exists in database"):
                krb5.get_krb5(username="alice")
        assert any(
            record.message
            == "Passwordless refill unavailable for alice; raising original error: "
            "Token not exists in database: no row"
            for record in caplog.records
        )


class TestExpiredDeadEnd:
    def test_refills_when_hook_registered(
        self, stub_user, capture_persist, monkeypatch, caplog
    ):
        _register_mint("fresh-token")
        monkeypatch.setattr(
            krb5, "get_kerberos_token",
            lambda **kw: {
                "token": "old",
                "expired_at": datetime.now() - timedelta(hours=1),
            },
        )
        with caplog.at_level(logging.INFO, logger="ink"):
            token = krb5.get_krb5(username="alice")
        assert token == "fresh-token"
        assert capture_persist == [("alice", "fresh-token")]
        assert any(
            record.message.startswith(
                "DB token for alice expired or expiring soon (db_expired_at="
            )
            and ", remaining=" in record.message
            and ", threshold=21600s); re-issuing." in record.message
            for record in caplog.records
        )
        assert any(
            record.message == "Passwordless ticket acquired for alice, persisting."
            for record in caplog.records
        )

    def test_raises_original_when_no_hook(self, stub_user, monkeypatch, caplog):
        monkeypatch.setattr(
            krb5, "get_kerberos_token",
            lambda **kw: {
                "token": "old",
                "expired_at": datetime.now() - timedelta(hours=1),
            },
        )
        with caplog.at_level(logging.INFO, logger="ink"):
            with pytest.raises(ValueError, match="Token is expired"):
                krb5.get_krb5(username="alice")
        assert any(
            record.message.startswith(
                "DB token for alice expired or expiring soon (db_expired_at="
            )
            and ", remaining=" in record.message
            and ", threshold=21600s); re-issuing." in record.message
            for record in caplog.records
        )
        assert any(
            record.message
            == "Passwordless refill unavailable for alice; raising original error: "
            "Token is expired"
            for record in caplog.records
        )


class TestExpiringSoonReissue:
    def _expiring_ticket(self):
        return {
            "token": "expiring",
            "expired_at": datetime.now() + timedelta(seconds=60),
        }

    def test_reissues_when_expiring_soon_and_hook_registered(
        self, stub_user, capture_persist, monkeypatch, caplog
    ):
        _register_mint("minted-token")
        monkeypatch.setattr(
            krb5, "get_kerberos_token", lambda **kw: self._expiring_ticket()
        )
        with caplog.at_level(logging.INFO, logger="ink"):
            token = krb5.get_krb5(username="alice", expire_in=3600)
        assert token == "minted-token"
        assert capture_persist == [("alice", "minted-token")]
        assert any(
            record.message.startswith(
                "DB token for alice expired or expiring soon (db_expired_at="
            )
            and ", remaining=" in record.message
            and ", threshold=3600s); re-issuing." in record.message
            for record in caplog.records
        )
        assert any(
            record.message == "Passwordless ticket acquired for alice, persisting."
            for record in caplog.records
        )

    def test_raises_original_when_expiring_soon_and_no_hook(
        self, stub_user, monkeypatch, caplog
    ):
        monkeypatch.setattr(
            krb5, "get_kerberos_token", lambda **kw: self._expiring_ticket()
        )
        with caplog.at_level(logging.INFO, logger="ink"):
            with pytest.raises(ValueError, match="Token is expired"):
                krb5.get_krb5(username="alice", expire_in=3600)
        assert any(
            record.message.startswith(
                "DB token for alice expired or expiring soon (db_expired_at="
            )
            and ", remaining=" in record.message
            and ", threshold=3600s); re-issuing." in record.message
            for record in caplog.records
        )
        assert any(
            record.message
            == "Passwordless refill unavailable for alice; raising original error: "
            "Token is expired"
            for record in caplog.records
        )


class TestValidateKrb5Token:
    def test_validates_ccache_without_temp_file(self, monkeypatch):
        now = int(datetime.now().timestamp())
        token = _ccache_token(endtime=now + 3600, renew_until=now + 7200)

        def fail_if_called(*args, **kwargs):
            raise AssertionError("temporary ccache path was used")

        monkeypatch.setattr(krb5.tempfile, "mkstemp", fail_if_called)
        monkeypatch.setattr(
            krb5, "token_to_ccachefile", fail_if_called, raising=False
        )

        assert krb5.validate_krb5_token("alice", token) is True

    def test_rejects_ccache_for_different_username(self):
        now = int(datetime.now().timestamp())
        token = _ccache_token(endtime=now + 3600, renew_until=now + 7200)

        with pytest.raises(Exception, match="username not match"):
            krb5.validate_krb5_token("bob", token)

    def test_rejects_expired_but_renewable_ccache(self):
        """An expired TGT must be rejected even while still renewable
        (expired_at < now < renew_until)."""
        now = int(datetime.now().timestamp())
        token = _ccache_token(endtime=now - 3600, renew_until=now + 7200)

        with pytest.raises(Exception, match="Kerberos token expired"):
            krb5.validate_krb5_token("alice", token)


class TestPersistRecordsRealExpiry:
    def test_persists_ccache_endtime(self, monkeypatch):
        endtime = int(datetime.now().timestamp()) + 12345
        token = _ccache_token(endtime=endtime, renew_until=endtime + 3600)
        captured = {}
        monkeypatch.setattr(
            krb5, "get_user", lambda **kw: {"id": 1, "username": "alice"}
        )
        monkeypatch.setattr(
            krb5, "save_token", lambda **kwargs: captured.update(kwargs)
        )

        krb5._persist_ccache_token("alice", token)

        assert captured["expired_at"] == datetime.fromtimestamp(endtime)
        assert captured["expired_at"] != datetime.now() + timedelta(hours=25)


class TestAccountStatusLogging:
    def test_logs_expired_account_before_raising(self, stub_user, monkeypatch, caplog):
        monkeypatch.setattr(
            krb5,
            "validate_account_status",
            lambda username=None: {"account_valid": False, "password_valid": True},
        )
        with caplog.at_level(logging.INFO, logger="ink"):
            with pytest.raises(AccountExpiredError, match="Account is expired"):
                krb5.get_krb5(username="alice")
        assert any(
            record.message
            == "Account is expired for alice (validate_result={'account_valid': False, "
            "'password_valid': True})."
            for record in caplog.records
        )

    def test_logs_expired_password_before_raising(self, stub_user, monkeypatch, caplog):
        monkeypatch.setattr(
            krb5,
            "validate_account_status",
            lambda username=None: {"account_valid": True, "password_valid": False},
        )
        with caplog.at_level(logging.INFO, logger="ink"):
            with pytest.raises(PasswordExpiredError, match="Account password is expired"):
                krb5.get_krb5(username="alice")
        assert any(
            record.message
            == "Account password is expired for alice (validate_result={'account_valid': True, "
            "'password_valid': False})."
            for record in caplog.records
        )


class TestDirectToken:
    def test_logs_direct_db_token_return(self, stub_user, monkeypatch, caplog):
        expired_at = datetime.now() + timedelta(hours=2)
        monkeypatch.setattr(
            krb5,
            "get_kerberos_token",
            lambda **kw: {"token": "valid", "expired_at": expired_at},
        )
        with caplog.at_level(logging.INFO, logger="ink"):
            token = krb5.get_krb5(username="alice", expire_in=60)
        assert token == "valid"
        assert any(
            record.message.startswith(
                "Returning DB token directly for alice (db_expired_at="
            )
            and ", remaining=" in record.message
            and record.message.endswith("s).")
            for record in caplog.records
        )


class TestRefillFailureLogging:
    def test_logs_generic_passwordless_failure(self, caplog):
        original_error = ValueError("original error")

        @hooks.register_hook(HOOK_NAME)
        def _mint(_username):
            raise RuntimeError("mint failed")

        with caplog.at_level(logging.INFO, logger="ink"):
            with pytest.raises(RuntimeError, match="mint failed"):
                krb5._refill_passwordless_or_raise("alice", original_error)
        assert any(
            record.message
            == "Passwordless refill failed for alice (mint failed); "
            "original error was: original error"
            for record in caplog.records
        )


class TestPrincipalResolution:
    def test_uses_db_username_for_principal(
        self, capture_persist, monkeypatch
    ):
        """The mint hook and the account-status validator must both receive
        the resolved Kerberos principal from the DB record, so uid-only
        calls never validate a None username."""
        monkeypatch.setattr(
            krb5, "get_user",
            lambda **kw: {"id": 7, "username": "bob"},
        )
        validated = {}

        def fake_validate(username=None):
            validated["username"] = username
            return {"account_valid": True, "password_valid": True}

        monkeypatch.setattr(krb5, "validate_account_status", fake_validate)
        seen = {}

        @hooks.register_hook(HOOK_NAME)
        def _mint(username):
            seen["username"] = username
            return "tok"

        monkeypatch.setattr(
            krb5, "get_kerberos_token",
            lambda **kw: (_ for _ in ()).throw(Exception("no row")),
        )
        krb5.get_krb5(uid=7)
        assert seen["username"] == "bob"
        assert validated["username"] == "bob"

    def test_missing_user_raises_user_not_found_error(self, monkeypatch):
        """common.get_user raises NoResultFound for a missing user; get_krb5
        must convert that to a clean UserNotFoundError (not leak the
        SQLAlchemy exception)."""
        from sqlalchemy.exc import NoResultFound

        def raise_missing(**kw):
            raise NoResultFound("User not found")

        monkeypatch.setattr(krb5, "get_user", raise_missing)
        with pytest.raises(UserNotFoundError, match="User not found"):
            krb5.get_krb5(username="ghost")
