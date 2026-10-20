"""Validated runtime settings for the krb5 token-validation runner."""

from __future__ import annotations

import math
import os
import tempfile
import threading
import unicodedata
from dataclasses import dataclass
from typing import Any, NoReturn, Optional, Union

from fastink.common import config
from fastink.common.logger import logger


_DEFAULTS = {
    "cache_ttl": 600,
    "negative_backoff": 5,
    "timeout": 5,
    "max_concurrency": 8,
}
_MAX_KVNO_STDERR = 64 * 1024
KDC_REJECTION_STDERR_PATTERNS = (
    "Matching credential not found",
    "Ticket expired",
    "integrity check failed",
    "not found in Kerberos database while getting initial credentials",
    "Generic preauthentication failure",
)
INFRA_STDERR_PATTERNS = (
    "Cannot contact any KDC",
    "Cannot resolve",
    "No route to host",
    "Network is unreachable",
    "Connection refused",
    "timed out",
    "Configuration error",
    "Cannot open",
    "Permission denied",
    "Server not found in Kerberos database",
)
_ALLOWED_KEYS = frozenset({"service_principal", "realm", *_DEFAULTS})

_kvno_semaphore: Optional[threading.BoundedSemaphore] = None
_kvno_semaphore_max: Optional[int] = None
_kvno_semaphore_lock = threading.Lock()


class Krb5ValidateConfigError(ValueError):
    """Raised when krb5 validation settings cannot be used safely."""


class Krb5ValidateBusy(RuntimeError):
    """Raised when the local kvno concurrency limit is exhausted."""


class Krb5ValidateTimeout(RuntimeError):
    """Raised when kvno does not finish within the configured timeout."""


class Krb5ValidateInfra(RuntimeError):
    """Raised when kvno fails for an infrastructure or configuration reason."""


class Krb5KdcRejection(RuntimeError):
    """Raised when kvno reports that the KDC rejected the validation."""

    def __init__(self, returncode: int) -> None:
        super().__init__("kvno validation was rejected")
        self.returncode = returncode


@dataclass(frozen=True)
class Krb5ValidateSettings:
    service_principal: str
    realm: str
    cache_ttl: int
    negative_backoff: int
    timeout: Union[int, float]
    max_concurrency: int


def _invalid(field: str, reason: str) -> NoReturn:
    message = f"auth.krb5_validate.{field}: {reason}"
    logger.error("Invalid auth.krb5_validate configuration: %s", message)
    raise Krb5ValidateConfigError(message)


def _text(value: Any, field: str) -> str:
    if not isinstance(value, str) or not value:
        _invalid(field, "must be a non-empty string")
    if any(unicodedata.category(char) in {"Cc", "Cf"} for char in value):
        _invalid(field, "must not contain control characters")
    try:
        value.encode("utf-8")
    except UnicodeEncodeError:
        _invalid(field, "must be valid UTF-8 text")
    return value


def _realm(value: Any) -> str:
    realm = _text(value, "realm")
    if realm.startswith("-"):
        _invalid("realm", "must not start with '-'")
    if any(char in realm for char in "/@\\") or any(char.isspace() for char in realm):
        _invalid("realm", "must be one unescaped component")
    return realm


def _principal(value: Any, realm: str) -> str:
    principal = _text(value, "service_principal")
    if any(char.isspace() for char in principal):
        _invalid("service_principal", "must not contain whitespace")
    if "\\" in principal or principal.count("@") != 1:
        _invalid("service_principal", "must use one unescaped realm separator")

    service, principal_realm = principal.split("@")
    if not service or not principal_realm:
        _invalid("service_principal", "must have non-empty service and realm parts")
    if principal_realm.encode("utf-8") != realm.encode("utf-8"):
        _invalid("service_principal", "realm does not match configured realm")

    components = service.split("/")
    if any(not component for component in components):
        _invalid("service_principal", "must not contain empty components")
    if any(component.startswith("-") for component in components):
        _invalid("service_principal", "components must not start with '-'")
    if components[0].casefold() == "krbtgt":
        _invalid("service_principal", "krbtgt is not an allowed target")
    return principal


def _integer(value: Any, field: str, minimum: int) -> int:
    if type(value) is not int or value < minimum:
        _invalid(field, f"must be an integer >= {minimum}")
    return value


def _timeout(value: Any) -> Union[int, float]:
    if type(value) not in (int, float) or not math.isfinite(value) or value <= 0:
        _invalid("timeout", "must be a finite number > 0")
    return value


def _settings_from_mapping(values: Any) -> Krb5ValidateSettings:
    if not isinstance(values, dict):
        _invalid("settings", "must be a mapping")
    if set(values) - _ALLOWED_KEYS:
        _invalid("settings", "contains an unsupported option")

    realm = _realm(values.get("realm"))
    principal = _principal(values.get("service_principal"), realm)
    return Krb5ValidateSettings(
        service_principal=principal,
        realm=realm,
        cache_ttl=_integer(values.get("cache_ttl", _DEFAULTS["cache_ttl"]), "cache_ttl", 1),
        negative_backoff=_integer(
            values.get("negative_backoff", _DEFAULTS["negative_backoff"]),
            "negative_backoff",
            0,
        ),
        timeout=_timeout(values.get("timeout", _DEFAULTS["timeout"])),
        max_concurrency=_integer(
            values.get("max_concurrency", _DEFAULTS["max_concurrency"]),
            "max_concurrency",
            1,
        ),
    )


def load_krb5_validate_settings() -> Krb5ValidateSettings:
    """Load and strictly validate ``auth.krb5_validate`` settings."""
    try:
        values = config.get_config("auth", "krb5_validate", fallback={})
    except Exception as error:
        logger.error(
            "Invalid auth.krb5_validate configuration: unable to load settings (%s)",
            type(error).__name__,
        )
        raise Krb5ValidateConfigError("auth.krb5_validate: unable to load settings") from error
    return _settings_from_mapping(values)


def _validate_settings_object(settings: Krb5ValidateSettings) -> None:
    if not isinstance(settings, Krb5ValidateSettings):
        raise Krb5ValidateConfigError("settings must be Krb5ValidateSettings")
    realm = _realm(settings.realm)
    _principal(settings.service_principal, realm)
    _integer(settings.cache_ttl, "cache_ttl", 1)
    _integer(settings.negative_backoff, "negative_backoff", 0)
    _timeout(settings.timeout)
    _integer(settings.max_concurrency, "max_concurrency", 1)


def get_kvno_semaphore(
    settings: Optional[Krb5ValidateSettings] = None,
) -> threading.BoundedSemaphore:
    """Return the process-global kvno semaphore without replacing generations."""
    if settings is None:
        settings = load_krb5_validate_settings()

    global _kvno_semaphore, _kvno_semaphore_max
    with _kvno_semaphore_lock:
        if _kvno_semaphore is None:
            _validate_settings_object(settings)
            _kvno_semaphore = threading.BoundedSemaphore(settings.max_concurrency)
            _kvno_semaphore_max = settings.max_concurrency
        elif _kvno_semaphore_max != settings.max_concurrency:
            logger.error("krb5 validation max_concurrency changed; restart required")
            raise Krb5ValidateConfigError(
                "auth.krb5_validate.max_concurrency changed; restart required"
            )
        return _kvno_semaphore


def run_kvno_validation(
    ccache_bytes: bytes, settings: Krb5ValidateSettings
) -> bytes:
    """Validate a sanitized ccache with a bounded, hermetic kvno process."""
    if len(ccache_bytes) > 1024 * 1024:
        raise ValueError("ccache must be no larger than 1 MiB")

    import subprocess

    semaphore = get_kvno_semaphore(settings)
    if not semaphore.acquire(timeout=settings.timeout):
        raise Krb5ValidateBusy("kvno validation capacity is unavailable")

    try:
        with tempfile.TemporaryDirectory() as directory:
            os.chmod(directory, 0o700)
            cache_path = os.path.join(directory, "ccache")
            descriptor = os.open(
                cache_path,
                os.O_WRONLY | os.O_CREAT | os.O_EXCL,
                0o600,
            )
            with os.fdopen(descriptor, "wb") as cache_file:
                cache_file.write(ccache_bytes)
                cache_file.flush()
            os.chmod(cache_path, 0o600)

            cache_name = f"FILE:{cache_path}"
            try:
                result = subprocess.run(
                    ["/usr/bin/kvno", "-c", cache_name, settings.service_principal],
                    shell=False,
                    timeout=settings.timeout,
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.PIPE,
                    env={
                        "PATH": "/usr/bin:/bin",
                        "LC_ALL": "C",
                        "KRB5_CONFIG": "/etc/krb5.conf",
                        "KRB5CCNAME": cache_name,
                        "KRB5_CLIENT_KTNAME": "/dev/null",
                        "KRB5_KTNAME": "/dev/null",
                    },
                )
            except subprocess.TimeoutExpired as error:
                raise Krb5ValidateTimeout("kvno validation timed out") from error

            if result.returncode == 0:
                with open(cache_path, "rb") as cache_file:
                    return cache_file.read()

            if result.returncode < 0:
                raise Krb5ValidateInfra("kvno process terminated by signal")

            stderr_text = result.stderr[:_MAX_KVNO_STDERR].decode(errors="replace")
            if any(pattern in stderr_text for pattern in INFRA_STDERR_PATTERNS):
                raise Krb5ValidateInfra("kvno infrastructure failure")
            if any(pattern in stderr_text for pattern in KDC_REJECTION_STDERR_PATTERNS):
                raise Krb5KdcRejection(result.returncode)
            raise Krb5ValidateInfra("kvno failure was not classified")
    finally:
        semaphore.release()
