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


# --- _verify_password_hash: passlib formats + system crypt(3) fallback ---

from fastink.auth.backends.password import _verify_password_hash  # noqa: E402


def test_verify_known_format_right_and_wrong_password():
    shadow_hash = sha512_crypt.hash("s3cret")
    assert _verify_password_hash("s3cret", shadow_hash) is True
    assert _verify_password_hash("wrong", shadow_hash) is False


def test_verify_unknown_format_falls_back_to_system_crypt(monkeypatch):
    # passlib cannot identify "$y$..." (yescrypt); the crypt(3) fallback
    # must be consulted and its result compared against the shadow hash.
    monkeypatch.setattr(
        password_backend, "_system_crypt", lambda pw, h: h if pw == "good" else "nope"
    )
    assert _verify_password_hash("good", "$y$j9T$fakesalt$fakehash") is True
    assert _verify_password_hash("bad", "$y$j9T$fakesalt$fakehash") is False


def test_verify_unknown_format_crypt_unavailable(monkeypatch):
    monkeypatch.setattr(password_backend, "_system_crypt", lambda pw, h: None)
    assert _verify_password_hash("any", "$y$j9T$fakesalt$fakehash") is False


@pytest.mark.parametrize("locked", ["*", "!", "!$6$whatever$hash"])
def test_verify_locked_hashes_fail_closed(monkeypatch, locked):
    # Locked/disabled shadow entries must never verify, even through the
    # system-crypt fallback.
    monkeypatch.setattr(
        password_backend, "_system_crypt", lambda pw, h: None
    )
    assert _verify_password_hash("any", locked) is False


def _yescrypt_probe():
    """Return a (password, hash) pair if the runtime crypt(3) speaks yescrypt."""
    from fastink.auth.backends.password import _system_crypt

    password = "yescrypt-probe-pw"
    computed = _system_crypt(password, "$y$j9T$FH3xS.Tv5tGbUiHT")
    if computed and computed.startswith("$y$"):
        return password, computed
    return None


@pytest.mark.skipif(_yescrypt_probe() is None, reason="runtime crypt(3) lacks yescrypt")
def test_verify_real_yescrypt_roundtrip():
    password, shadow_hash = _yescrypt_probe()
    assert _verify_password_hash(password, shadow_hash) is True
    assert _verify_password_hash(password + "x", shadow_hash) is False


def test_missing_user_runs_dummy_verification(monkeypatch):
    """Missing users must pay the same hashing cost as a real verify
    (timing equalization against account enumeration)."""
    calls = []
    monkeypatch.setattr(
        password_backend,
        "_verify_password_hash",
        lambda pw, h: calls.append(h) or False,
    )
    monkeypatch.setattr(
        password_backend,
        "_read_shadow",
        lambda: "bob:$6$bobsalt$hash:20000:0:99999:7:::",
    )
    monkeypatch.setattr(
        password_backend.pwd,
        "getpwnam",
        lambda username: SimpleNamespace(pw_uid=1001),
    )

    assert _validate_user("alice", "any") is False
    assert calls == [password_backend._DUMMY_SHADOW_HASH]


def test_missing_user_in_passwd_runs_dummy_verification(monkeypatch):
    """The KeyError branch (user absent from passwd entirely) must also
    pay the dummy verification cost, not return instantly."""
    calls = []
    monkeypatch.setattr(
        password_backend,
        "_verify_password_hash",
        lambda pw, h: calls.append(h) or False,
    )

    def raise_keyerror(username):
        raise KeyError(username)

    monkeypatch.setattr(password_backend.pwd, "getpwnam", raise_keyerror)

    assert _validate_user("ghost", "any") is False
    assert calls == [password_backend._DUMMY_SHADOW_HASH]


def test_dummy_hash_uses_default_rounds():
    """The dummy must use the glibc-default cost (implicit rounds=5000),
    not an inflated rounds= parameter that would make missing-user
    lookups slower than typical real ones."""
    assert not password_backend._DUMMY_SHADOW_HASH.startswith("$6$rounds=")
    assert password_backend._DUMMY_SHADOW_HASH.startswith("$6$")
    assert sha512_crypt.identify(password_backend._DUMMY_SHADOW_HASH)
