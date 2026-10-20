import base64
import json
import logging
import subprocess
import threading
import time
from dataclasses import dataclass, field
from types import SimpleNamespace

import pytest
import redis.exceptions

from fastink.auth.backends import krb5_validate_runtime as runtime
from fastink.auth.backends import krb5_validation
from fastink.auth.backends.ccache import splice_tgt_only
from tests.test_krb5_validation import (
    SERVICE_PRINCIPAL,
    _credential,
    _validated_cache,
    build_forged_ccache,
)


KRB5_VALIDATE_INFRA = runtime.Krb5ValidateInfra


@dataclass
class FakeRedis:
    values: dict[str, str] = field(default_factory=dict)
    expires_at: dict[str, float] = field(default_factory=dict)
    reads: list[str] = field(default_factory=list)
    writes: list[tuple[str, str, int | None, str | None]] = field(default_factory=list)
    fail_get: bool = False
    fail_set: bool = False

    def get(self, key: str) -> str | None:
        self.reads.append(key)
        if self.fail_get:
            raise redis.exceptions.RedisError("get failed")
        expires_at = self.expires_at.get(key)
        if expires_at is not None and expires_at <= time.time():
            self.values.pop(key, None)
            self.expires_at.pop(key, None)
        return self.values.get(key)

    def set(
        self,
        key: str,
        value: str,
        *,
        ex: int | None = None,
        px: int | None = None,
    ) -> None:
        if ex is not None and px is not None:
            raise redis.exceptions.ResponseError("multiple expiration arguments")
        if ex == 0 or px == 0:
            raise redis.exceptions.ResponseError("invalid expire time")
        expiration = px if px is not None else ex
        self.writes.append(("set", key, expiration, value))
        if self.fail_set:
            raise redis.exceptions.RedisError("set failed")
        self.values[key] = value
        if px is not None:
            self.expires_at[key] = time.time() + px / 1000
        elif ex is not None:
            self.expires_at[key] = time.time() + ex

    def delete(self, key: str) -> int:
        self.writes.append(("delete", key, None, None))
        deleted = int(key in self.values)
        self.values.pop(key, None)
        self.expires_at.pop(key, None)
        return deleted

    def ttl(self, key: str) -> int:
        if key not in self.values:
            return -2
        return max(0, int(self.expires_at[key] - time.time()))


def _settings(
    *,
    cache_ttl: int = 600,
    negative_backoff: int = 5,
    service_principal: str = SERVICE_PRINCIPAL,
):
    return runtime.Krb5ValidateSettings(
        service_principal,
        "IHEPKRB5",
        cache_ttl,
        negative_backoff,
        5,
        8,
    )


def _token(data: bytes | None = None) -> str:
    return base64.b64encode(data or build_forged_ccache()).decode()


def _keys(token: str, settings=None) -> tuple[str, str]:
    settings = settings or _settings()
    decoded = base64.b64decode(token, validate=True)
    spliced, _username = splice_tgt_only(decoded, settings.realm.encode())
    return krb5_validation._cache_keys(settings, spliced)


def _configure(monkeypatch, cache: FakeRedis, runner, settings=None):
    settings = settings or _settings()
    monkeypatch.setattr(krb5_validation, "load_krb5_validate_settings", lambda: settings)
    monkeypatch.setattr(krb5_validation, "run_kvno_validation", runner)
    monkeypatch.setattr(krb5_validation, "_redis", lambda: cache, raising=False)
    return settings


def _success_runner(endtime: int | None = None):
    def runner(cache, settings):
        return _validated_cache(cache, endtime=endtime)

    return runner


def _cache_entry(principal: str, valid_until: int | float) -> str:
    return json.dumps({"principal": principal, "valid_until": valid_until})


def test_valid_positive_hit_skips_kdc(monkeypatch):
    cache = FakeRedis()
    calls = []
    _configure(monkeypatch, cache, lambda *args: calls.append(args))
    token = _token()
    positive_key, _ = _keys(token)
    cache.values[positive_key] = _cache_entry("alice", int(time.time()) + 60)

    assert krb5_validation.validate_krb5_token_real("alice", token) is True
    assert calls == []


def test_wrong_username_is_rejected_before_redis_read(monkeypatch):
    cache = FakeRedis()
    _configure(monkeypatch, cache, lambda *args: pytest.fail("KDC must not run"))
    token = _token()
    positive_key, _ = _keys(token)
    original = _cache_entry("alice", int(time.time()) + 60)
    cache.values[positive_key] = original

    assert krb5_validation.validate_krb5_token_real("bob", token) is False
    assert cache.reads == []
    assert cache.values[positive_key] == original


def test_principal_mismatch_is_a_miss(monkeypatch):
    cache = FakeRedis()
    calls = []

    def runner(cache_bytes, settings):
        calls.append((cache_bytes, settings))
        return _validated_cache(cache_bytes, endtime=int(time.time()) + 60)

    _configure(monkeypatch, cache, runner)
    token = _token()
    positive_key, _ = _keys(token)
    cache.values[positive_key] = _cache_entry("bob", int(time.time()) + 60)

    assert krb5_validation.validate_krb5_token_real("alice", token) is True
    assert len(calls) == 1


def test_expired_positive_is_a_miss(monkeypatch):
    cache = FakeRedis()
    calls = []

    def runner(cache_bytes, settings):
        calls.append(cache_bytes)
        return _validated_cache(cache_bytes, endtime=int(time.time()) + 60)

    _configure(monkeypatch, cache, runner)
    token = _token()
    positive_key, _ = _keys(token)
    cache.values[positive_key] = _cache_entry("alice", int(time.time()) - 1)

    assert krb5_validation.validate_krb5_token_real("alice", token) is True
    assert len(calls) == 1


@pytest.mark.parametrize(
    "raw",
    [
        "not-json",
        json.dumps({"principal": "alice", "valid_until": "not-a-number"}),
        json.dumps({"principal": "alice", "valid_until": float("nan")}),
        json.dumps({"principal": "alice", "valid_until": float("inf")}),
    ],
    ids=["malformed", "non-numeric", "nan", "infinity"],
)
def test_invalid_positive_entry_is_a_miss(monkeypatch, raw):
    cache = FakeRedis()
    calls = []

    def reject(cache_bytes, settings):
        calls.append((cache_bytes, settings))
        raise runtime.Krb5KdcRejection(1)

    _configure(
        monkeypatch,
        cache,
        reject,
    )
    token = _token()
    positive_key, failure_key = _keys(token)
    cache.values[positive_key] = raw

    assert krb5_validation.validate_krb5_token_real("alice", token) is False
    assert len(calls) == 1
    assert cache.values[failure_key] == "1"


def test_urlsafe_base64_variant_is_rejected(monkeypatch):
    cache = FakeRedis()
    calls = []
    _configure(monkeypatch, cache, lambda *args: calls.append(args))
    original = build_forged_ccache(
        extra_credentials=_credential(ticket=b"\xff\xff\xff")
    )
    standard_token = base64.b64encode(original).decode()
    urlsafe_token = base64.urlsafe_b64encode(original).decode()
    assert standard_token != urlsafe_token
    assert "-" in urlsafe_token or "_" in urlsafe_token

    assert krb5_validation.validate_krb5_token_real("alice", urlsafe_token) is False
    assert calls == []
    assert cache.reads == []


def test_canonical_reencoding_of_identical_bytes_keeps_cache_key_stable():
    decoded = build_forged_ccache()
    canonical_token = base64.b64encode(decoded).decode()
    round_trip = base64.b64decode(canonical_token, validate=True)
    settings = _settings()
    spliced, _username = splice_tgt_only(decoded, settings.realm.encode())
    round_trip_spliced, _username = splice_tgt_only(
        round_trip, settings.realm.encode()
    )

    assert krb5_validation._cache_keys(settings, spliced) == krb5_validation._cache_keys(
        settings, round_trip_spliced
    )


def test_changed_decoded_byte_uses_a_different_key(monkeypatch):
    cache = FakeRedis()
    calls = []

    def runner(cache_bytes, settings):
        calls.append(cache_bytes)
        return _validated_cache(cache_bytes, endtime=int(time.time()) + 60)

    _configure(monkeypatch, cache, runner)
    original = build_forged_ccache()
    changed = original.replace(b"not-der", b"not-dec", 1)
    original_token = _token(original)
    changed_token = _token(changed)
    original_key, _ = _keys(original_token)
    changed_key, _ = _keys(changed_token)
    cache.values[original_key] = _cache_entry("alice", int(time.time()) + 60)

    assert krb5_validation.validate_krb5_token_real("alice", original_token) is True
    assert krb5_validation.validate_krb5_token_real("alice", changed_token) is True
    assert original_key != changed_key
    assert len(calls) == 1
    assert changed_key in cache.reads


def test_positive_cache_isolated_by_service_validation_context(monkeypatch):
    cache = FakeRedis()
    calls = []
    settings_a = _settings(service_principal=SERVICE_PRINCIPAL)
    settings_b = _settings(
        service_principal="xrootd/other.ihep.ac.cn@IHEPKRB5",
    )

    def runner(cache_bytes, settings):
        calls.append(settings.service_principal)
        raise ValueError("target context must miss cached result")

    token = _token()
    positive_key_a, _failure_key_a = _keys(token, settings_a)
    cache.values[positive_key_a] = _cache_entry("alice", int(time.time()) + 60)
    _configure(monkeypatch, cache, runner, settings_b)

    assert krb5_validation.validate_krb5_token_real("alice", token) is False
    assert calls == [settings_b.service_principal]
    positive_key_b, _failure_key_b = _keys(token, settings_b)
    assert positive_key_a != positive_key_b


def test_negative_cache_isolated_by_service_validation_context(monkeypatch):
    cache = FakeRedis()
    calls = []
    settings_a = _settings(service_principal=SERVICE_PRINCIPAL)
    settings_b = _settings(
        service_principal="xrootd/other.ihep.ac.cn@IHEPKRB5",
    )

    def reject(cache_bytes, settings):
        calls.append(settings.service_principal)
        raise runtime.Krb5KdcRejection(1)

    token = _token()
    _configure(monkeypatch, cache, reject, settings_a)
    _, failure_key_a = _keys(token, settings_a)
    assert krb5_validation.validate_krb5_token_real("alice", token) is False
    assert cache.values[failure_key_a] == "1"

    _configure(monkeypatch, cache, reject, settings_b)
    assert krb5_validation.validate_krb5_token_real("alice", token) is False
    assert calls == [settings_a.service_principal, settings_b.service_principal]
    _, failure_key_b = _keys(token, settings_b)
    assert failure_key_a != failure_key_b
    assert cache.values[failure_key_b] == "1"


def test_stripped_record_variants_share_positive_cache_entry(monkeypatch):
    cache = FakeRedis()
    calls = []

    def runner(cache_bytes, settings):
        calls.append(cache_bytes)
        return _validated_cache(cache_bytes, endtime=int(time.time()) + 60)

    _configure(monkeypatch, cache, runner)
    original = build_forged_ccache()
    variant = original + _credential(
        server_components=(b"other", b"host"),
        endtime=int(time.time()) + 60,
    )
    original_token = _token(original)
    variant_token = _token(variant)
    assert original_token != variant_token
    assert _keys(original_token) == _keys(variant_token)

    assert krb5_validation.validate_krb5_token_real("alice", original_token) is True
    assert krb5_validation.validate_krb5_token_real("alice", variant_token) is True
    assert len(calls) == 1


def test_stripped_record_variants_share_negative_cache_entry(monkeypatch):
    cache = FakeRedis()
    calls = []

    def reject(cache_bytes, settings):
        calls.append(cache_bytes)
        raise runtime.Krb5KdcRejection(1)

    _configure(monkeypatch, cache, reject)
    original = build_forged_ccache()
    variant = original + _credential(
        server_components=(b"other", b"host"),
        endtime=int(time.time()) + 60,
    )
    original_token = _token(original)
    variant_token = _token(variant)
    _, failure_key = _keys(original_token)
    assert _keys(original_token) == _keys(variant_token)

    assert krb5_validation.validate_krb5_token_real("alice", original_token) is False
    assert krb5_validation.validate_krb5_token_real("alice", variant_token) is False
    assert len(calls) == 1
    assert cache.values[failure_key] == "1"


def test_positive_valid_until_is_clamped_to_service_endtime(monkeypatch):
    cache = FakeRedis()
    monkeypatch.setattr(krb5_validation.time, "time", lambda: 1000.0)
    settings = _settings(cache_ttl=600)
    _configure(monkeypatch, cache, _success_runner(endtime=1010), settings)

    assert krb5_validation.validate_krb5_token_real("alice", _token()) is True
    positive_write = next(write for write in cache.writes if write[0] == "set")
    assert json.loads(positive_write[3])["valid_until"] == 1010
    assert positive_write[2] == 10000


def test_positive_valid_until_is_clamped_to_cache_ttl(monkeypatch):
    cache = FakeRedis()
    monkeypatch.setattr(krb5_validation.time, "time", lambda: 1000.0)
    settings = _settings(cache_ttl=10)
    _configure(monkeypatch, cache, _success_runner(endtime=1100), settings)

    assert krb5_validation.validate_krb5_token_real("alice", _token()) is True
    positive_write = next(write for write in cache.writes if write[0] == "set")
    assert json.loads(positive_write[3])["valid_until"] == 1010
    assert positive_write[2] == 10000


def test_validation_with_subsecond_remaining_uses_millisecond_ttl(monkeypatch):
    cache = FakeRedis()
    now = [1000.0]

    def clock():
        return now[0]

    monkeypatch.setattr(krb5_validation.time, "time", clock)

    calls = []

    def runner(*args):
        calls.append(args)
        if len(calls) > 1:
            raise ValueError("cache miss reached KDC")
        now[0] = 1000.4
        return 1001

    settings = _settings(cache_ttl=1)
    _configure(monkeypatch, cache, lambda *args: pytest.fail("runner must not run"), settings)
    monkeypatch.setattr(krb5_validation, "_validate_with_kdc", runner)
    token = _token()
    positive_key, _failure_key = _keys(token)

    assert krb5_validation.validate_krb5_token_real("alice", token) is True
    positive_write = next(write for write in cache.writes if write[1] == positive_key)
    assert 590 <= positive_write[2] <= 610

    now[0] = 1000.5
    assert krb5_validation.validate_krb5_token_real("alice", token) is True

    now[0] = 1001.0
    assert krb5_validation.validate_krb5_token_real("alice", token) is False
    assert len(calls) == 2


def test_validation_with_nonpositive_remaining_is_not_cached(monkeypatch):
    cache = FakeRedis()
    monkeypatch.setattr(krb5_validation.time, "time", lambda: 1001.0)
    settings = _settings(cache_ttl=1)
    calls = []

    def validate_with_kdc(*args):
        calls.append(args)
        return 1001

    _configure(
        monkeypatch,
        cache,
        lambda *args: pytest.fail("runner must not run"),
        settings,
    )
    monkeypatch.setattr(krb5_validation, "_validate_with_kdc", validate_with_kdc)

    assert krb5_validation.validate_krb5_token_real("alice", _token()) is False
    assert calls
    assert not any(write[0] == "set" for write in cache.writes)


def test_validation_expiry_during_redis_write_fails_closed(monkeypatch):
    cache = FakeRedis()
    now = [1000.0]
    monkeypatch.setattr(krb5_validation.time, "time", lambda: now[0])
    settings = _settings(cache_ttl=10)
    _configure(
        monkeypatch,
        cache,
        lambda *args: pytest.fail("runner must not run"),
        settings,
    )
    monkeypatch.setattr(krb5_validation, "_validate_with_kdc", lambda *args: 1010)

    original_set = cache.set

    def set_and_advance(*args, **kwargs):
        original_set(*args, **kwargs)
        now[0] = 1011.0

    monkeypatch.setattr(cache, "set", set_and_advance)
    token = _token()
    positive_key, _failure_key = _keys(token, settings)

    assert krb5_validation.validate_krb5_token_real("alice", token) is False
    assert cache.get(positive_key) is None


def test_success_deletes_stale_failure_backoff(monkeypatch):
    cache = FakeRedis()
    _configure(monkeypatch, cache, _success_runner(endtime=int(time.time()) + 60))
    token = _token()
    positive_key, failure_key = _keys(token)
    cache.values[failure_key] = "1"
    cache.expires_at[failure_key] = time.time() - 1

    assert krb5_validation.validate_krb5_token_real("alice", token) is True
    assert failure_key not in cache.values
    assert any(write == ("delete", failure_key, None, None) for write in cache.writes)
    assert positive_key in cache.values


def test_wrong_username_does_not_poison_following_correct_request(monkeypatch):
    cache = FakeRedis()
    calls = []

    def runner(cache_bytes, settings):
        calls.append(cache_bytes)
        return _validated_cache(cache_bytes, endtime=int(time.time()) + 60)

    _configure(monkeypatch, cache, runner)
    token = _token()

    assert krb5_validation.validate_krb5_token_real("bob", token) is False
    assert krb5_validation.validate_krb5_token_real("alice", token) is True
    assert len(calls) == 1


def test_kdc_rejection_writes_backoff_and_second_request_skips_kdc(monkeypatch):
    cache = FakeRedis()
    calls = []

    def reject(cache_bytes, settings):
        calls.append(cache_bytes)
        raise runtime.Krb5KdcRejection(1)

    settings = _settings(negative_backoff=5)
    _configure(monkeypatch, cache, reject, settings)
    token = _token()
    _, failure_key = _keys(token)

    assert krb5_validation.validate_krb5_token_real("alice", token) is False
    assert cache.values[failure_key] == "1"
    backoff_write = next(
        write for write in cache.writes if write[1] == failure_key
    )
    assert backoff_write[2] == 5
    assert krb5_validation.validate_krb5_token_real("alice", token) is False
    assert len(calls) == 1


def test_kdc_rejection_with_zero_backoff_writes_no_key(monkeypatch):
    cache = FakeRedis()
    settings = _settings(negative_backoff=0)
    _configure(monkeypatch, cache, lambda *args: (_ for _ in ()).throw(runtime.Krb5KdcRejection(1)), settings)

    assert krb5_validation.validate_krb5_token_real("alice", _token()) is False
    assert cache.writes == []


def test_redis_reuses_client_for_same_config_and_rebuilds_on_fingerprint_change(
    monkeypatch,
):
    config = {"host": "redis-a", "port": 6379, "password": "secret"}
    clients = []

    class Client:
        pass

    def make_client(**kwargs):
        clients.append((Client(), kwargs))
        return clients[-1][0]

    monkeypatch.setattr(krb5_validation, "get_config", lambda section: config)
    monkeypatch.setattr(krb5_validation, "Redis", make_client)

    first = krb5_validation._redis()
    assert krb5_validation._redis() is first
    assert len(clients) == 1
    assert clients[0][1]["socket_connect_timeout"] == 1.0
    assert clients[0][1]["socket_timeout"] == 1.0
    assert clients[0][1]["decode_responses"] is True

    config = {"host": "redis-b", "port": 6379, "password": "secret"}
    second = krb5_validation._redis()
    assert second is not first
    assert len(clients) == 2


def test_redis_client_publication_is_serialized_across_fingerprint_changes(
    monkeypatch,
):
    configs = [
        {"host": "redis-a", "port": 6379, "password": "secret-a"},
        {"host": "redis-b", "port": 6380, "password": "secret-b"},
    ]
    thread_config = threading.local()
    start = threading.Barrier(5)
    state_lock = threading.Lock()
    active = 0
    maximum_active = 0
    results = []
    errors = []

    class Client:
        def __init__(self, kwargs):
            self.host = kwargs["host"]
            self.port = kwargs["port"]
            self.password = kwargs["password"]

    def get_config(section):
        return thread_config.value

    def make_client(**kwargs):
        nonlocal active, maximum_active
        with state_lock:
            active += 1
            maximum_active = max(maximum_active, active)
        time.sleep(0.01)
        with state_lock:
            active -= 1
        return Client(kwargs)

    monkeypatch.setattr(krb5_validation, "get_config", get_config)
    monkeypatch.setattr(krb5_validation, "Redis", make_client)

    def worker(config):
        thread_config.value = config
        try:
            start.wait(timeout=5)
            client = krb5_validation._redis()
            results.append((client, config))
        except BaseException as error:
            errors.append(error)

    threads = [
        threading.Thread(target=worker, args=(configs[index % len(configs)],))
        for index in range(4)
    ]
    for thread in threads:
        thread.start()
    start.wait(timeout=5)
    for thread in threads:
        thread.join(timeout=5)

    assert not errors
    assert maximum_active == 1
    assert len(results) == 4
    for client, config in results:
        assert (client.host, client.port, client.password) == (
            config["host"],
            config["port"],
            config["password"],
        )
    assert krb5_validation._redis_client is not None
    assert (
        krb5_validation._redis_client.host,
        krb5_validation._redis_client.port,
        krb5_validation._redis_client.password,
    ) == krb5_validation._redis_fingerprint


def test_mixed_kvno_failure_does_not_write_public_backoff(monkeypatch):
    cache = FakeRedis()
    settings = _settings()
    _configure(monkeypatch, cache, runtime.run_kvno_validation, settings)
    monkeypatch.setattr(
        subprocess,
        "run",
        lambda argv, **kwargs: SimpleNamespace(
            returncode=1,
            stderr=b"Cannot contact any KDC: Ticket expired",
        ),
    )

    assert krb5_validation.validate_krb5_token_real("alice", _token()) is False
    assert cache.writes == []


@pytest.mark.parametrize(
    "error",
    [
        runtime.Krb5ValidateTimeout("timeout"),
        KRB5_VALIDATE_INFRA("infra"),
        runtime.Krb5ValidateBusy("busy"),
        runtime.Krb5ValidateConfigError("config"),
        ValueError("identity or malformed"),
    ],
    ids=["timeout", "infra", "busy", "config", "value-error"],
)
def test_non_rejection_failures_write_nothing(monkeypatch, error):
    cache = FakeRedis()

    def reject(cache_bytes, settings):
        raise error

    _configure(monkeypatch, cache, reject)
    assert krb5_validation.validate_krb5_token_real("alice", _token()) is False
    assert cache.writes == []


def test_runner_infrastructure_failure_writes_no_backoff(monkeypatch):
    cache = FakeRedis()
    _configure(
        monkeypatch,
        cache,
        lambda *args: (_ for _ in ()).throw(KRB5_VALIDATE_INFRA("infra")),
    )

    assert krb5_validation.validate_krb5_token_real("alice", _token()) is False
    assert cache.writes == []


@pytest.mark.parametrize(
    ("failure_name", "category"),
    [
        ("Krb5KdcRejection", "krb5 validation kdc rejection"),
        ("Krb5ValidateInfra", "krb5 validation infra failure"),
        ("Krb5ValidateTimeout", "krb5 validation subprocess timeout"),
        ("Krb5ValidateBusy", "krb5 validation semaphore busy"),
        ("Krb5ValidateConfigError", "krb5 validation config error"),
    ],
    ids=["kdc", "infra", "timeout", "busy", "config"],
)
def test_runtime_failures_emit_fixed_warning_category(
    monkeypatch, caplog, failure_name, category
):
    cache = FakeRedis()
    failure_type = getattr(krb5_validation, failure_name, KRB5_VALIDATE_INFRA)
    failure = failure_type(1)
    _configure(monkeypatch, cache, lambda *args: (_ for _ in ()).throw(failure))
    token = _token()

    with caplog.at_level(logging.WARNING, logger="ink"):
        assert krb5_validation.validate_krb5_token_real("alice", token) is False

    assert [record.getMessage() for record in caplog.records if record.levelno == logging.WARNING] == [
        category
    ]
    assert token not in caplog.text


def test_redis_failures_emit_fixed_warning_category(monkeypatch, caplog):
    cache = FakeRedis(fail_get=True)
    _configure(monkeypatch, cache, lambda *args: pytest.fail("KDC must not run"))
    token = _token()

    with caplog.at_level(logging.WARNING, logger="ink"):
        assert krb5_validation.validate_krb5_token_real("alice", token) is False

    assert [record.getMessage() for record in caplog.records if record.levelno == logging.WARNING] == [
        "krb5 validation redis error"
    ]
    assert token not in caplog.text


def test_redis_get_failure_fails_closed_without_kdc(monkeypatch):
    cache = FakeRedis(fail_get=True)
    calls = []
    _configure(monkeypatch, cache, lambda *args: calls.append(args))

    assert krb5_validation.validate_krb5_token_real("alice", _token()) is False
    assert calls == []


def test_positive_write_failure_fails_closed(monkeypatch):
    cache = FakeRedis(fail_set=True)
    calls = []

    def runner(cache_bytes, settings):
        calls.append(cache_bytes)
        return _validated_cache(cache_bytes, endtime=int(time.time()) + 60)

    _configure(monkeypatch, cache, runner)
    assert krb5_validation.validate_krb5_token_real("alice", _token()) is False
    assert len(calls) == 1


def test_positive_hit_wins_over_existing_failure_backoff(monkeypatch):
    cache = FakeRedis()
    _configure(monkeypatch, cache, lambda *args: pytest.fail("KDC must not run"))
    token = _token()
    positive_key, failure_key = _keys(token)
    cache.values[positive_key] = _cache_entry("alice", int(time.time()) + 60)
    cache.values[failure_key] = "1"

    assert krb5_validation.validate_krb5_token_real("alice", token) is True
    assert cache.values[failure_key] == "1"
    assert failure_key not in cache.reads
