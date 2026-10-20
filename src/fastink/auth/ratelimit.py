import ipaddress

import redis

from fastink.common.config import get_config
from fastink.common.logger import logger

_PREFIX = "ink:rl:pw:"
_STORE_ERRORS = (redis.exceptions.RedisError, ValueError, KeyError, TypeError)
_client = None

_RESERVE_LUA = """
local locked_user = redis.call('GET', KEYS[3])
if locked_user then return redis.call('TTL', KEYS[3]) end
local locked_ip = redis.call('GET', KEYS[4])
if locked_ip then return redis.call('TTL', KEYS[4]) end

local user_count = redis.call('INCR', KEYS[1])
if user_count == 1 then redis.call('EXPIRE', KEYS[1], ARGV[1]) end
local ip_count = redis.call('INCR', KEYS[2])
if ip_count == 1 then redis.call('EXPIRE', KEYS[2], ARGV[1]) end

if user_count > tonumber(ARGV[2]) then
    redis.call('SET', KEYS[3], '1', 'EX', ARGV[4])
    return tonumber(ARGV[4])
end
if tonumber(ARGV[3]) > 0 and ip_count > tonumber(ARGV[3]) then
    redis.call('SET', KEYS[4], '1', 'EX', ARGV[4])
    return tonumber(ARGV[4])
end
return 0
"""

_SUCCEED_LUA = """
redis.call('DEL', KEYS[1], KEYS[2])
local ip_count = tonumber(redis.call('GET', KEYS[3]) or 0)
if ip_count > 0 then redis.call('DECR', KEYS[3]) end
return 1
"""

_REFUND_LUA = """
local user_count = tonumber(redis.call('GET', KEYS[1]) or 0)
if user_count > 0 then user_count = redis.call('DECR', KEYS[1]) end
if tonumber(user_count) <= tonumber(ARGV[1]) then
    redis.call('DEL', KEYS[3])
end
local ip_count = tonumber(redis.call('GET', KEYS[2]) or 0)
if ip_count > 0 then ip_count = redis.call('DECR', KEYS[2]) end
if tonumber(ip_count) <= tonumber(ARGV[2]) then
    redis.call('DEL', KEYS[4])
end
return 1
"""


def _redis():
    global _client
    if _client is None:
        cfg = get_config("redis")
        _client = redis.Redis(
            host=cfg.get("host", "localhost"),
            port=cfg.get("port", 6379),
            password=cfg.get("password", None),
            decode_responses=True,
            socket_connect_timeout=3,
            socket_timeout=3,
        )
    return _client


def _int_setting(cfg, name: str, default: int, minimum: int) -> int:
    try:
        return max(minimum, int(cfg.get(name, default)))
    except (ValueError, TypeError):
        logger.error(
            "invalid security.rate_limit.%s; using default %s", name, default
        )
        return default


def _settings() -> tuple[int, int, int, int]:
    cfg = get_config("security", "rate_limit", fallback={}) or {}
    if not isinstance(cfg, dict):
        logger.error("security.rate_limit must be a mapping; using defaults")
        cfg = {}
    return (
        _int_setting(cfg, "failures_per_user", 5, 1),
        _int_setting(cfg, "failures_per_ip", 0, 0),
        _int_setting(cfg, "window", 300, 1),
        _int_setting(cfg, "lockout", 900, 1),
    )


def normalize_login_username(username: str) -> str:
    """Validate a login name and normalize the krb5 realm suffix.

    Raises ValueError for names that kinit would treat as options or that
    cannot be mapped to a stable rate-limit bucket.
    """
    username = username.strip()
    if not username or username.startswith("-"):
        raise ValueError("invalid username")
    if str(get_config("auth", "type", fallback="krb5")) != "krb5":
        return username
    cfg = get_config("auth", "krb5_validate", fallback={}) or {}
    realm = str(cfg.get("realm") or "")
    if "@" in username:
        if not realm:
            raise ValueError(
                "realm-qualified username requires auth.krb5_validate.realm"
            )
        if username.endswith("@" + realm):
            return username[: -(len(realm) + 1)]
    return username


def _trusted_proxy(host: str) -> bool:
    try:
        address = ipaddress.ip_address(host)
    except ValueError:
        return False
    return address.is_loopback or address.is_private


def client_ip(request) -> str:
    """Trust X-Real-IP only when the direct peer is a private/loopback proxy."""
    host = request.client.host if request.client else ""
    forwarded = request.headers.get("x-real-ip")
    if forwarded and _trusted_proxy(host):
        return forwarded
    return host or "unknown"


def _normalize_user(username: str) -> str:
    username = username.strip()
    if str(get_config("auth", "type", fallback="krb5")) != "krb5":
        return username
    cfg = get_config("auth", "krb5_validate", fallback={}) or {}
    realm = str(cfg.get("realm") or "")
    if realm and username.endswith("@" + realm):
        return username[: -(len(realm) + 1)]
    return username


def _user_key(username: str) -> str:
    return f"{_PREFIX}user:{_normalize_user(username)}"


def _ip_key(ip: str) -> str:
    return f"{_PREFIX}ip:{ip}"


def _user_lock(username: str) -> str:
    return f"{_PREFIX}lock:user:{_normalize_user(username)}"


def _ip_lock(ip: str) -> str:
    return f"{_PREFIX}lock:ip:{ip}"


def reserve(ip: str, username: str) -> int | None:
    """Atomically count an attempt; return retry-after seconds if blocked."""
    try:
        per_user, per_ip, window, lockout = _settings()
        retry = _redis().eval(
            _RESERVE_LUA,
            4,
            _user_key(username),
            _ip_key(ip),
            _user_lock(username),
            _ip_lock(ip),
            window,
            per_user,
            per_ip,
            lockout,
        )
        value = int(retry or 0)
        return value if value > 0 else None
    except _STORE_ERRORS as error:
        # Availability over brute-force defense: sessions still fail closed.
        logger.warning("rate limit unavailable (%s); allowing login attempt", error)
        return None


def succeed(ip: str, username: str) -> None:
    try:
        _settings()
        _redis().eval(
            _SUCCEED_LUA,
            3,
            _user_key(username),
            _user_lock(username),
            _ip_key(ip),
        )
    except _STORE_ERRORS as error:
        logger.warning("rate limit reset unavailable: %s", error)


def unlock(username: str, ip: str | None = None) -> bool:
    """Clear a user's (and optionally an IP's) counter and lock."""
    try:
        keys = [_user_key(username), _user_lock(username)]
        if ip:
            keys.extend([_ip_key(ip), _ip_lock(ip)])
        _redis().delete(*keys)
        return True
    except _STORE_ERRORS as error:
        logger.warning("rate limit unlock unavailable: %s", error)
        return False


def refund(ip: str, username: str) -> None:
    """Undo an attempt that failed for a non-credential/service reason."""
    try:
        per_user, per_ip, _, _ = _settings()
        _redis().eval(
            _REFUND_LUA,
            4,
            _user_key(username),
            _ip_key(ip),
            _user_lock(username),
            _ip_lock(ip),
            per_user,
            per_ip,
        )
    except _STORE_ERRORS as error:
        logger.warning("rate limit refund unavailable: %s", error)
