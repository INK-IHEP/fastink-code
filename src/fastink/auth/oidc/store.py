import json
import time
from functools import wraps
from typing import Any, Optional

import redis.exceptions
from redis import Redis

from fastink.common.config import get_config

_PREFIX = "oidc:"
_CODE_TTL = 300
_DEVICE_TTL = 600
_SSO_TTL = 600


def _translate_redis_errors(fn):
    @wraps(fn)
    def wrapper(*args, **kwargs):
        try:
            return fn(*args, **kwargs)
        except redis.exceptions.RedisError as e:
            raise RuntimeError(f"OIDC store unavailable: {e}") from e
    return wrapper


def _redis() -> Redis:
    cfg = get_config("redis")
    return Redis(
        host=cfg.get("host", "localhost"),
        port=cfg.get("port", 6379),
        password=cfg.get("password", None),
        decode_responses=True,
    )


def _key(kind: str, identifier: str) -> str:
    return f"{_PREFIX}{kind}:{identifier}"


@_translate_redis_errors
def store_code(code: str, data: dict[str, Any], ttl: int = _CODE_TTL) -> None:
    data["expires_at"] = time.time() + ttl
    _redis().set(_key("code", code), json.dumps(data), ex=ttl)


@_translate_redis_errors
def get_code(code: str) -> Optional[tuple[dict[str, Any], str]]:
    r = _redis()
    raw = r.get(_key("code", code))
    if raw is None:
        return None
    data = json.loads(raw)
    if data.get("expires_at", 0) <= time.time():
        r.delete(_key("code", code))
        return None
    return data, raw


_DELETE_IF_MATCH_LUA = """
local current = redis.call('GET', KEYS[1])
if not current then return 0 end
if current ~= ARGV[1] then return 0 end
redis.call('DEL', KEYS[1])
return 1
"""


@_translate_redis_errors
def delete_code_if_matches(code: str, raw: str) -> bool:
    return bool(_redis().eval(_DELETE_IF_MATCH_LUA, 1, _key("code", code), raw))


@_translate_redis_errors
def store_device_code(device_code: str, data: dict[str, Any], ttl: int = _DEVICE_TTL) -> None:
    data["expires_at"] = time.time() + ttl
    r = _redis()
    r.set(_key("device", device_code), json.dumps(data), ex=ttl)
    r.set(_key("user_code", data["user_code"]), device_code, ex=ttl)


@_translate_redis_errors
def get_device_code(device_code: str) -> Optional[dict[str, Any]]:
    result = _redis().get(_key("device", device_code))
    if result is None:
        return None
    data = json.loads(result)
    if data.get("expires_at", 0) <= time.time():
        _redis().delete(_key("device", device_code))
        return None
    return data


_APPROVE_LUA = """
local raw = redis.call('GET', KEYS[1])
if not raw then return 0 end
local data = cjson.decode(raw)
if data.expires_at and data.expires_at <= tonumber(ARGV[1]) then return 0 end
data.username = ARGV[2]
data.approved = true
local ttl = redis.call('TTL', KEYS[1])
if ttl > 0 then
    redis.call('SET', KEYS[1], cjson.encode(data), 'EX', ttl)
end
return 1
"""


@_translate_redis_errors
def approve_device(user_code: str, username: str) -> bool:
    r = _redis()
    device_code = r.get(_key("user_code", user_code))
    if device_code is None:
        return False
    result = r.eval(
        _APPROVE_LUA, 1, _key("device", device_code), time.time(), username
    )
    return bool(result)


@_translate_redis_errors
def get_device_code_by_user_code(user_code: str) -> Optional[dict[str, Any]]:
    device_code = _redis().get(_key("user_code", user_code))
    if device_code is None:
        return None
    return get_device_code(device_code)


@_translate_redis_errors
def consume_device_code(device_code: str) -> Optional[dict[str, Any]]:
    r = _redis()
    key = _key("device", device_code)
    pipe = r.pipeline()
    pipe.get(key)
    pipe.delete(key)
    result, _ = pipe.execute()
    if result is None:
        return None
    data = json.loads(result)
    if data.get("expires_at", 0) <= time.time():
        return None
    return data


@_translate_redis_errors
def store_sso_pending(pending_id: str, params: dict[str, Any], ttl: int = _SSO_TTL) -> None:
    _redis().set(_key("sso", pending_id), json.dumps({
        "params": params,
        "expires_at": time.time() + ttl,
    }), ex=ttl)


@_translate_redis_errors
def consume_sso_pending(pending_id: str) -> Optional[dict[str, Any]]:
    r = _redis()
    key = _key("sso", pending_id)
    pipe = r.pipeline()
    pipe.get(key)
    pipe.delete(key)
    result, _ = pipe.execute()
    if result is None:
        return None
    data = json.loads(result)
    if data.get("expires_at", 0) <= time.time():
        return None
    return data.get("params")
