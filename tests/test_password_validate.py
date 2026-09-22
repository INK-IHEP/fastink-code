from types import SimpleNamespace

import pytest
from passlib.hash import sha512_crypt

from fastink.auth.backends import password as password_backend
from fastink.auth.backends.password import _get_shadow_hash, _validate_user


def test_get_shadow_hash_returns_matching_hash():
    shadow_text = "alice:$6$alice-salt$hash:20000:0:99999:7:::\nbob:$6$bob-salt$other:20000:0:99999:7:::"

    assert _get_shadow_hash("alice", shadow_text) == "$6$alice-salt$hash"


def test_get_shadow_hash_returns_none_for_unknown_user():
    assert _get_shadow_hash("nobody", "alice:$6$alice-salt$hash:20000:0:99999:7:::") is None


@pytest.mark.parametrize("shadow_hash", ["!$6$locked$hash", "*"])
def test_get_shadow_hash_preserves_locked_hashes(shadow_hash):
    shadow_text = f"alice:{shadow_hash}:20000:0:99999:7:::"

    assert _get_shadow_hash("alice", shadow_text) == shadow_hash


@pytest.fixture
def valid_shadow_user(monkeypatch):
    shadow_hash = sha512_crypt.hash("known-password")
    monkeypatch.setattr(
        password_backend,
        "_read_shadow",
        lambda: f"alice:{shadow_hash}:20000:0:99999:7:::",
    )
    monkeypatch.setattr(
        password_backend.pwd,
        "getpwnam",
        lambda username: SimpleNamespace(pw_uid=1001),
    )


def test_validate_user_accepts_right_password(valid_shadow_user):
    assert _validate_user("alice", "known-password") is True


def test_validate_user_rejects_wrong_password(valid_shadow_user):
    assert _validate_user("alice", "wrong-password") is False


def test_validate_user_rejects_user_missing_from_shadow(monkeypatch):
    monkeypatch.setattr(
        password_backend,
        "_read_shadow",
        lambda: "bob:$6$bob-salt$hash:20000:0:99999:7:::",
    )
    monkeypatch.setattr(
        password_backend.pwd,
        "getpwnam",
        lambda username: SimpleNamespace(pw_uid=1001),
    )

    assert _validate_user("alice", "known-password") is False


@pytest.mark.parametrize("shadow_hash", ["*", "!$6$whatever$hash"])
def test_validate_user_rejects_locked_account(monkeypatch, shadow_hash):
    monkeypatch.setattr(
        password_backend,
        "_read_shadow",
        lambda: f"alice:{shadow_hash}:20000:0:99999:7:::",
    )
    monkeypatch.setattr(
        password_backend.pwd,
        "getpwnam",
        lambda username: SimpleNamespace(pw_uid=1001),
    )

    assert _validate_user("alice", "any") is False


def test_validate_user_rejects_uid_mismatch(valid_shadow_user):
    assert _validate_user("alice", "known-password", uid=1002) is False
