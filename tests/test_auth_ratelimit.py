import uuid

import pytest
import redis

from fastink.auth import ratelimit


@pytest.fixture
def limiter(monkeypatch):
    try:
        ratelimit._redis().ping()
    except redis.exceptions.RedisError:
        pytest.skip("redis unavailable")
    monkeypatch.setattr(ratelimit, "_settings", lambda: (2, 3, 60, 120))
    return ratelimit


def _user():
    return f"rl-{uuid.uuid4().hex[:8]}"


def _cleanup(ip, username):
    client = ratelimit._redis()
    client.delete(
        ratelimit._user_key(username),
        ratelimit._ip_key(ip),
        ratelimit._user_lock(username),
        ratelimit._ip_lock(ip),
    )


def test_reserve_allows_then_locks_user(limiter):
    ip, user = "10.1.0.1", _user()
    try:
        assert limiter.reserve(ip, user) is None
        assert limiter.reserve(ip, user) is None
        retry = limiter.reserve(ip, user)
        assert retry is not None and 58 <= retry <= 120
    finally:
        _cleanup(ip, user)


def test_locks_ip_across_users(limiter):
    ip = "10.1.0.2"
    users = [_user(), _user(), _user(), _user()]
    try:
        assert limiter.reserve(ip, users[0]) is None
        assert limiter.reserve(ip, users[1]) is None
        assert limiter.reserve(ip, users[2]) is None
        retry = limiter.reserve(ip, users[3])
        assert retry is not None and 58 <= retry <= 120
    finally:
        for user in users:
            _cleanup(ip, user)


def test_per_ip_can_be_disabled(limiter, monkeypatch):
    monkeypatch.setattr(limiter, "_settings", lambda: (2, 0, 60, 120))
    ip = "10.1.0.6"
    users = [_user(), _user(), _user(), _user()]
    try:
        for user in users:
            assert limiter.reserve(ip, user) is None
    finally:
        for user in users:
            _cleanup(ip, user)


def test_success_clears_user_counter(limiter):
    ip, user = "10.1.0.3", _user()
    try:
        assert limiter.reserve(ip, user) is None
        assert limiter.reserve(ip, user) is None
        limiter.succeed(ip, user)
        assert limiter.reserve(ip, user) is None
    finally:
        _cleanup(ip, user)


def test_refund_undoes_attempt(limiter):
    ip, user = "10.1.0.4", _user()
    try:
        assert limiter.reserve(ip, user) is None
        limiter.refund(ip, user)
        assert limiter.reserve(ip, user) is None
    finally:
        _cleanup(ip, user)


def test_normalize_login_username_rejects_option_like_and_realm(monkeypatch):
    def config(section, option=None, **kwargs):
        if (section, option) == ("auth", "type"):
            return "krb5"
        if (section, option) == ("auth", "krb5_validate"):
            return {"realm": ""}
        return kwargs.get("fallback")

    monkeypatch.setattr(ratelimit, "get_config", config)
    for name in ("-k", "alice@IHEP.AC.CN", ""):
        with pytest.raises(ValueError):
            ratelimit.normalize_login_username(name)


def test_normalize_login_username_strips_configured_realm(monkeypatch):
    def config(section, option=None, **kwargs):
        if (section, option) == ("auth", "type"):
            return "krb5"
        if (section, option) == ("auth", "krb5_validate"):
            return {"realm": "IHEP.AC.CN"}
        return kwargs.get("fallback")

    monkeypatch.setattr(ratelimit, "get_config", config)
    assert ratelimit.normalize_login_username(" alice ") == "alice"
    assert ratelimit.normalize_login_username("alice@IHEP.AC.CN") == "alice"
    assert ratelimit.normalize_login_username("alice@OTHER") == "alice@OTHER"


def test_realm_qualified_username_shares_user_bucket(monkeypatch):
    def config(section, option=None, **kwargs):
        if (section, option) == ("auth", "krb5_validate"):
            return {"realm": "IHEP.AC.CN"}
        if (section, option) == ("security", "rate_limit"):
            return {}
        return kwargs.get("fallback")

    monkeypatch.setattr(ratelimit, "get_config", config)
    assert ratelimit._user_key("alice@IHEP.AC.CN") == ratelimit._user_key("alice")
    assert ratelimit._user_lock("alice@IHEP.AC.CN") == ratelimit._user_lock("alice")
    assert ratelimit._user_key("alice@OTHER.REALM") != ratelimit._user_key("alice")


def test_settings_non_mapping_uses_defaults(monkeypatch):
    monkeypatch.setattr(ratelimit, "get_config", lambda *args, **kwargs: 10)
    assert ratelimit._settings() == (5, 0, 300, 900)


def test_password_mode_does_not_merge_realm_buckets(monkeypatch):
    def config(section, option=None, **kwargs):
        if (section, option) == ("auth", "type"):
            return "password"
        if (section, option) == ("auth", "krb5_validate"):
            return {"realm": "IHEP.AC.CN"}
        return kwargs.get("fallback")

    monkeypatch.setattr(ratelimit, "get_config", config)
    assert ratelimit._user_key("alice@IHEP.AC.CN") != ratelimit._user_key("alice")


def test_settings_invalid_strings_use_defaults(monkeypatch):
    monkeypatch.setattr(
        ratelimit,
        "get_config",
        lambda *args, **kwargs: {
            "failures_per_user": "five",
            "failures_per_ip": "many",
            "window": "5m",
            "lockout": None,
        },
    )
    assert ratelimit._settings() == (5, 0, 300, 900)


def test_settings_clamp_invalid_bounds(monkeypatch):
    monkeypatch.setattr(
        ratelimit,
        "get_config",
        lambda *args, **kwargs: {
            "failures_per_user": 0,
            "failures_per_ip": -1,
            "window": 0,
            "lockout": -5,
        },
    )
    assert ratelimit._settings() == (1, 0, 1, 1)


def test_redis_failure_fails_open(monkeypatch):
    def broken():
        raise redis.exceptions.RedisError("redis down")

    monkeypatch.setattr(ratelimit, "_redis", broken)
    assert ratelimit.reserve("10.1.0.5", "alice") is None
