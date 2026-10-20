import builtins
import importlib
import logging
import sys
import math
import threading

import pytest

from fastink.auth.backends import krb5_validate_runtime as runtime
from fastink.common import config


REALM = "IHEPKRB5"
VALID_PRINCIPAL = "xrootd/besproxy01.ihep.ac.cn@IHEPKRB5"


@pytest.fixture(autouse=True)
def reset_semaphore(monkeypatch):
    monkeypatch.setattr(runtime, "_kvno_semaphore", None)
    monkeypatch.setattr(runtime, "_kvno_semaphore_max", None)


def _config(monkeypatch, values=None):
    values = {
        "service_principal": VALID_PRINCIPAL,
        "realm": REALM,
        **(values or {}),
    }
    monkeypatch.setattr(
        config,
        "get_config",
        lambda section, option, fallback=None: values,
    )
    return values


def _invalid(monkeypatch, values=None):
    _config(monkeypatch, values)
    with pytest.raises(runtime.Krb5ValidateConfigError):
        runtime.load_krb5_validate_settings()


def test_approved_ihep_target_is_accepted_with_defaults(monkeypatch):
    _config(monkeypatch)

    settings = runtime.load_krb5_validate_settings()

    assert settings.service_principal == VALID_PRINCIPAL
    assert settings.realm == REALM
    assert settings.cache_ttl == 600
    assert settings.negative_backoff == 5
    assert settings.timeout == 5
    assert settings.max_concurrency == 8
    with pytest.raises(AttributeError):
        settings.realm = "OTHER"


def test_optional_numeric_values_are_loaded_without_coercion(monkeypatch):
    _config(
        monkeypatch,
        {
            "cache_ttl": 1200,
            "negative_backoff": 0,
            "timeout": 2.5,
            "max_concurrency": 3,
        },
    )

    settings = runtime.load_krb5_validate_settings()

    assert settings.cache_ttl == 1200
    assert settings.negative_backoff == 0
    assert settings.timeout == 2.5
    assert settings.max_concurrency == 3


@pytest.mark.parametrize("field", ["service_principal", "realm"])
@pytest.mark.parametrize("value", [None, "", 1, [], True])
def test_required_text_fields_must_be_nonempty_strings(monkeypatch, field, value):
    _invalid(monkeypatch, {field: value})


@pytest.mark.parametrize(
    "principal",
    [
        "xrootd/besproxy01.ihep.ac.cn",
        "xrootd/besproxy01.ihep.ac.cn@",
        "xrootd/besproxy01.ihep.ac.cn@IHEPKRB5@OTHER",
        "xrootd//besproxy01.ihep.ac.cn@IHEPKRB5",
        "/besproxy01.ihep.ac.cn@IHEPKRB5",
        "xrootd/besproxy01.ihep.ac.cn/ @IHEPKRB5",
        "xrootd/besproxy01.ihep.ac.cn@OTHER",
        "xrootd/besproxy01.ihep.ac.cn@ihepkrb5",
        "krbtgt/IHEPKRB5@IHEPKRB5",
        "KRBtgt/IHEPKRB5@IHEPKRB5",
        "-xrootd/besproxy01.ihep.ac.cn@IHEPKRB5",
        "xrootd/-besproxy01.ihep.ac.cn@IHEPKRB5",
        "xrootd/besproxy01.ihep.ac.cn\\@IHEPKRB5",
        "xrootd/besproxy01.ihep.ac.cn@IHEPKRB5/OTHER",
        "xrootd/besproxy01.ihep.ac.cn\x00@IHEPKRB5",
        "xrootd/besproxy01.ihep.ac.cn\n@IHEPKRB5",
        "xrootd/besproxy01.ihep.ac.cn\x1f@IHEPKRB5",
    ],
)
def test_service_principal_syntax_is_strict(monkeypatch, principal):
    _invalid(monkeypatch, {"service_principal": principal})


@pytest.mark.parametrize(
    "realm",
    ["IHEP\x00KRB5", "IHEP/KRB5", "IHEP@KRB5", "IHEP\\KRB5", "-IHEPKRB5"],
)
def test_configured_realm_must_be_one_safe_component(monkeypatch, realm):
    _invalid(monkeypatch, {"realm": realm})


@pytest.mark.parametrize("control", ["\u202e", "\u200b"])
def test_unicode_format_controls_are_rejected_from_realm(monkeypatch, control):
    realm = f"{control}{REALM}"
    _invalid(
        monkeypatch,
        {"realm": realm, "service_principal": f"xrootd/host@{realm}"},
    )


@pytest.mark.parametrize("control", ["\u202e", "\u200b"])
def test_unicode_format_controls_are_rejected_from_service_principal(
    monkeypatch, control
):
    _invalid(
        monkeypatch,
        {"service_principal": f"xrootd/{control}host@{REALM}"},
    )


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("cache_ttl", True),
        ("cache_ttl", False),
        ("cache_ttl", 0),
        ("cache_ttl", -1),
        ("cache_ttl", 1.5),
        ("cache_ttl", "600"),
        ("negative_backoff", True),
        ("negative_backoff", False),
        ("negative_backoff", -1),
        ("negative_backoff", 1.5),
        ("negative_backoff", "5"),
        ("timeout", True),
        ("timeout", False),
        ("timeout", 0),
        ("timeout", -1),
        ("timeout", math.inf),
        ("timeout", -math.inf),
        ("timeout", math.nan),
        ("timeout", "5"),
        ("max_concurrency", True),
        ("max_concurrency", False),
        ("max_concurrency", 0),
        ("max_concurrency", -1),
        ("max_concurrency", 1.5),
        ("max_concurrency", "8"),
    ],
)
def test_numeric_settings_are_strictly_validated(monkeypatch, field, value):
    _invalid(monkeypatch, {field: value})


def test_unknown_setting_is_rejected(monkeypatch):
    _invalid(monkeypatch, {"unexpected": "value"})


def test_non_mapping_config_is_rejected(monkeypatch):
    monkeypatch.setattr(
        config,
        "get_config",
        lambda section, option, fallback=None: [VALID_PRINCIPAL, REALM],
    )

    with pytest.raises(runtime.Krb5ValidateConfigError):
        runtime.load_krb5_validate_settings()


def test_config_failure_log_contains_no_principal_or_realm(caplog, monkeypatch):
    secret_principal = "secret-service/secret-host@SECRETREALM"
    _config(monkeypatch, {"service_principal": secret_principal, "realm": "OTHER"})

    with caplog.at_level(logging.ERROR, logger="ink"):
        with pytest.raises(runtime.Krb5ValidateConfigError):
            runtime.load_krb5_validate_settings()

    assert "Invalid auth.krb5_validate configuration" in caplog.text
    assert secret_principal not in caplog.text
    assert "SECRETREALM" not in caplog.text


def test_validation_does_not_import_subprocess_or_redis(monkeypatch):
    def fail_config_load(*args, **kwargs):
        raise AssertionError("config loaded during module import")

    monkeypatch.setattr(config, "get_config", fail_config_load)
    real_import = builtins.__import__

    def reject_runner_imports(name, *args, **kwargs):
        if name == "subprocess" or name == "redis" or name.startswith("redis."):
            raise AssertionError(f"unexpected runner import: {name}")
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", reject_runner_imports)
    spec = importlib.util.spec_from_file_location(
        "_krb5_validate_runtime_import_check", runtime.__file__
    )
    fresh = importlib.util.module_from_spec(spec)
    monkeypatch.setitem(sys.modules, spec.name, fresh)
    spec.loader.exec_module(fresh)

    _config(monkeypatch)
    assert fresh.load_krb5_validate_settings().service_principal == VALID_PRINCIPAL


def test_semaphore_is_process_global_and_bounded(monkeypatch):
    _config(monkeypatch, {"max_concurrency": 2})
    settings = runtime.load_krb5_validate_settings()

    semaphore = runtime.get_kvno_semaphore(settings)

    assert semaphore is runtime.get_kvno_semaphore(settings)
    assert isinstance(semaphore, threading.BoundedSemaphore)
    assert semaphore.acquire(blocking=False)
    assert semaphore.acquire(blocking=False)
    assert not semaphore.acquire(blocking=False)
    semaphore.release()
    semaphore.release()


def test_semaphore_rejects_changed_capacity_without_replacing_it(monkeypatch):
    _config(monkeypatch, {"max_concurrency": 2})
    first = runtime.get_kvno_semaphore(runtime.load_krb5_validate_settings())
    _config(monkeypatch, {"max_concurrency": 3})

    with pytest.raises(runtime.Krb5ValidateConfigError, match="restart"):
        runtime.get_kvno_semaphore(runtime.load_krb5_validate_settings())

    assert runtime.get_kvno_semaphore(
        runtime.Krb5ValidateSettings(VALID_PRINCIPAL, REALM, 600, 5, 5, 2)
    ) is first
