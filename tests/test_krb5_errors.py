import pytest
from sqlalchemy.exc import NoResultFound

from fastink.auth.backends import krb5
from fastink.auth.backends.errors import (
    AccountExpiredError,
    PasswordExpiredError,
    UserNotFoundError,
)


class _FakeChild:
    """Minimal pexpect.spawn stand-in returning preset expect() indices."""

    def __init__(self, indices):
        self._indices = list(indices)
        self._call = 0
        self.before = b""
        self.after = b""
        self.patterns_seen = []

    def expect(self, patterns, timeout=5):
        self.patterns_seen.append(patterns)
        idx = self._indices[self._call]
        self._call += 1
        return idx

    def sendline(self, value):
        pass

    def close(self):
        pass


def _patch_spawn(monkeypatch, indices):
    monkeypatch.setattr(krb5.pexpect, "spawn", lambda *a, **k: _FakeChild(indices))


# --- _generate_tgt: initial expect (pre-password) ---


def test_generate_tgt_user_not_found(monkeypatch):
    _patch_spawn(monkeypatch, [1])
    with pytest.raises(UserNotFoundError):
        krb5._generate_tgt("nobody", "secret", "/tmp/fake_ccache")


def test_generate_tgt_account_expired(monkeypatch):
    _patch_spawn(monkeypatch, [2])
    with pytest.raises(AccountExpiredError):
        krb5._generate_tgt("expired", "secret", "/tmp/fake_ccache")


def test_generate_tgt_preauth_failure_stays_valueerror(monkeypatch):
    _patch_spawn(monkeypatch, [3])
    with pytest.raises(ValueError):
        krb5._generate_tgt("preauth", "secret", "/tmp/fake_ccache")


def test_generate_tgt_timeout(monkeypatch):
    _patch_spawn(monkeypatch, [4])
    with pytest.raises(TimeoutError):
        krb5._generate_tgt("slow", "secret", "/tmp/fake_ccache")


# --- _generate_tgt: after password submitted ---


def test_generate_tgt_wrong_password(monkeypatch):
    _patch_spawn(monkeypatch, [0, 0])
    with pytest.raises(ValueError):
        krb5._generate_tgt("user", "bad", "/tmp/fake_ccache")


def test_generate_tgt_password_expired_after_password(monkeypatch):
    """Regression: the kinit 'Password has expired' message must NOT be
    treated as success (it previously fell through to EOF)."""
    _patch_spawn(monkeypatch, [0, 1])
    with pytest.raises(PasswordExpiredError):
        krb5._generate_tgt("user", "secret", "/tmp/fake_ccache")


def test_auth_index_pattern_order_locks_eof_race_fix(monkeypatch):
    """The password-expired pattern must come BEFORE pexpect.EOF — pexpect
    matches in list order, so after-EOF would revive the bug where
    password expiry is misread as success."""
    child = _FakeChild([0, 1])
    monkeypatch.setattr(krb5.pexpect, "spawn", lambda *a, **k: child)
    with pytest.raises(PasswordExpiredError):
        krb5._generate_tgt("user", "secret", "/tmp/fake_ccache")
    patterns = child.patterns_seen[1]  # second expect = post-password
    assert patterns[0] == "kinit: Password incorrect"
    assert patterns[1] == "kinit: Password has expired.*"
    assert patterns[2] is krb5.pexpect.EOF
    assert patterns[3] is krb5.pexpect.TIMEOUT


def test_generate_tgt_success(monkeypatch):
    _patch_spawn(monkeypatch, [0, 2])
    assert krb5._generate_tgt("user", "secret", "/tmp/fake_ccache") is True


def test_generate_tgt_verify_timeout(monkeypatch):
    _patch_spawn(monkeypatch, [0, 3])
    with pytest.raises(TimeoutError):
        krb5._generate_tgt("user", "secret", "/tmp/fake_ccache")


# --- create_krb5: exception type must propagate ---


def test_create_krb5_propagates_typed_error(monkeypatch):
    monkeypatch.setattr(krb5.tempfile, "mkstemp", lambda: (1, "/tmp/fake_ccache"))
    monkeypatch.setattr(krb5.os, "close", lambda fd: None)
    monkeypatch.setattr(krb5.os, "remove", lambda path: None)
    monkeypatch.setattr(
        krb5, "_generate_tgt", lambda u, p, c: (_ for _ in ()).throw(PasswordExpiredError())
    )

    with pytest.raises(PasswordExpiredError):
        krb5.create_krb5("user", "secret")


# --- get_krb5: three-way mapping ---


def test_get_krb5_user_not_found(monkeypatch):
    def _raise(username=None, uid=None):
        raise NoResultFound("no such user")

    monkeypatch.setattr(krb5, "get_user", _raise)
    with pytest.raises(UserNotFoundError):
        krb5.get_krb5(username="nobody")


def test_get_krb5_account_expired(monkeypatch):
    monkeypatch.setattr(
        krb5, "get_user", lambda **k: {"id": 1, "username": "expired"}
    )
    monkeypatch.setattr(
        krb5, "validate_account_status", lambda username: {"account_valid": False}
    )
    with pytest.raises(AccountExpiredError):
        krb5.get_krb5(username="expired")


def test_get_krb5_password_expired(monkeypatch):
    monkeypatch.setattr(
        krb5, "get_user", lambda **k: {"id": 1, "username": "pwexp"}
    )
    monkeypatch.setattr(
        krb5,
        "validate_account_status",
        lambda username: {"account_valid": True, "password_valid": False},
    )
    with pytest.raises(PasswordExpiredError):
        krb5.get_krb5(username="pwexp")
