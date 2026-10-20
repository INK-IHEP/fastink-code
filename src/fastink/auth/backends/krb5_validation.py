from __future__ import annotations

import base64
import hashlib
import json
import math
import threading
import time
from typing import Final

import redis.exceptions
from redis import Redis

from fastink.auth.backends.ccache import service_ticket_endtime, splice_tgt_only
from fastink.auth.backends.krb5_validate_runtime import (
    Krb5KdcRejection,
    Krb5ValidateBusy,
    Krb5ValidateConfigError,
    Krb5ValidateInfra,
    Krb5ValidateTimeout,
    Krb5ValidateSettings,
    load_krb5_validate_settings,
    run_kvno_validation,
)
from fastink.common.config import get_config
from fastink.common.logger import logger


_MAX_ENCODED_TOKEN_LENGTH: Final = 2 * 1024 * 1024
_MAX_CCACHE_LENGTH: Final = 1024 * 1024
_POSITIVE_PREFIX: Final = "krb5val:v1:"
_FAILURE_PREFIX: Final = "krb5valfail:v1:"
_REDIS_TIMEOUT: Final = 1.0
_LOG_REDIS_ERROR: Final = "krb5 validation redis error"
_LOG_KDC_REJECTION: Final = "krb5 validation kdc rejection"
_LOG_INFRA_FAILURE: Final = "krb5 validation infra failure"
_LOG_SUBPROCESS_TIMEOUT: Final = "krb5 validation subprocess timeout"
_LOG_SEMAPHORE_BUSY: Final = "krb5 validation semaphore busy"
_LOG_CONFIG_ERROR: Final = "krb5 validation config error"

_redis_client: Redis | None = None
_redis_fingerprint: tuple[object, object, object] | None = None
_redis_lock = threading.Lock()


def _redis() -> Redis:
    global _redis_client, _redis_fingerprint
    with _redis_lock:
        cfg = get_config("redis")
        fingerprint = (
            cfg.get("host", "localhost"),
            cfg.get("port", 6379),
            cfg.get("password", None),
        )
        if _redis_client is not None and _redis_fingerprint == fingerprint:
            return _redis_client
        client = Redis(
            host=fingerprint[0],
            port=fingerprint[1],
            password=fingerprint[2],
            socket_connect_timeout=_REDIS_TIMEOUT,
            socket_timeout=_REDIS_TIMEOUT,
            decode_responses=True,
        )
        _redis_client = client
        _redis_fingerprint = fingerprint
        return client


def _prepare_validation(
    username: str, token: str
) -> tuple[Krb5ValidateSettings, bytes, bytes]:
    settings = load_krb5_validate_settings()
    if len(token) > _MAX_ENCODED_TOKEN_LENGTH:
        raise ValueError("encoded token exceeds authentication limit")

    decoded = base64.b64decode(token, validate=True)
    if len(decoded) > _MAX_CCACHE_LENGTH:
        raise ValueError("ccache exceeds authentication limit")

    username_bytes = username.encode("utf-8")
    spliced, cache_username = splice_tgt_only(decoded, settings.realm.encode("utf-8"))
    if cache_username != username_bytes:
        raise ValueError("username does not match ccache principal")
    return settings, spliced, username_bytes


def _cache_keys(settings: Krb5ValidateSettings, spliced: bytes) -> tuple[str, str]:
    context = hashlib.sha256(
        settings.realm.encode("utf-8")
        + b"\x00"
        + settings.service_principal.encode("utf-8")
    ).hexdigest()[:16]
    digest = hashlib.sha256(spliced).hexdigest()
    return (
        f"{_POSITIVE_PREFIX}{context}:{digest}",
        f"{_FAILURE_PREFIX}{context}:{digest}",
    )


def _positive_hit(raw: str | bytes | None, username: str, now: float) -> bool:
    if raw is None:
        return False
    try:
        entry = json.loads(raw)
    except (TypeError, ValueError, UnicodeDecodeError):
        return False
    if not isinstance(entry, dict):
        return False

    principal = entry.get("principal")
    valid_until = entry.get("valid_until")
    if principal != username or isinstance(valid_until, bool):
        return False
    if not isinstance(valid_until, (int, float)):
        return False
    try:
        return math.isfinite(valid_until) and valid_until > now
    except OverflowError:
        return False


def _validate_with_kdc(
    settings: Krb5ValidateSettings,
    spliced: bytes,
    username_bytes: bytes,
) -> int:
    service, service_realm = settings.service_principal.rsplit("@", 1)
    server_components = [component.encode("utf-8") for component in service.split("/")]
    returned = run_kvno_validation(spliced, settings)
    return service_ticket_endtime(
        returned,
        client=username_bytes,
        server_components=server_components,
        server_realm=service_realm.encode("utf-8"),
    )


def validate_krb5_token_real(username: str, token: str) -> bool:
    """Validate a ccache by obtaining the target service ticket from the KDC."""
    try:
        settings, spliced, username_bytes = _prepare_validation(username, token)
        positive_key, failure_key = _cache_keys(settings, spliced)
        client = _redis()
        try:
            if _positive_hit(client.get(positive_key), username, time.time()):
                return True
            if client.get(failure_key) is not None:
                return False

            validation_started_at = time.time()
            service_endtime = _validate_with_kdc(settings, spliced, username_bytes)
            valid_until = min(
                validation_started_at + settings.cache_ttl,
                service_endtime,
            )
            remaining = valid_until - time.time()
            if remaining <= 0:
                return False

            client.set(
                positive_key,
                json.dumps({"principal": username, "valid_until": valid_until}),
                px=max(1, int(remaining * 1000)),
            )
            client.delete(failure_key)
            return time.time() < valid_until
        except Krb5KdcRejection:
            logger.warning(_LOG_KDC_REJECTION)
            if settings.negative_backoff > 0:
                client.set(failure_key, "1", ex=settings.negative_backoff)
            return False
        except redis.exceptions.RedisError:
            logger.warning(_LOG_REDIS_ERROR)
            return False
    except redis.exceptions.RedisError:
        logger.warning(_LOG_REDIS_ERROR)
        return False
    except Krb5ValidateInfra:
        logger.warning(_LOG_INFRA_FAILURE)
        return False
    except Krb5ValidateTimeout:
        logger.warning(_LOG_SUBPROCESS_TIMEOUT)
        return False
    except Krb5ValidateBusy:
        logger.warning(_LOG_SEMAPHORE_BUSY)
        return False
    except Krb5ValidateConfigError:
        logger.warning(_LOG_CONFIG_ERROR)
        return False
    except ValueError:
        return False
    except Exception as error:
        logger.error("Kerberos token validation failed (%s)", type(error).__name__)
        return False
