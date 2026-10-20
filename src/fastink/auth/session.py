import asyncio
import hashlib
import json
import secrets
import time
from weakref import WeakKeyDictionary

import redis.exceptions
from redis.asyncio import Redis

from fastink.auth.principal import Principal
from fastink.common.config import get_config

_KEY_PREFIX = "ink:session:"
_INDEX_PREFIX = "ink:user_sessions:"

_CREATE_SESSION_LUA = """
local prefix = ARGV[4]
local members = redis.call('SMEMBERS', KEYS[2])
for _, d in ipairs(members) do
    if redis.call('EXISTS', prefix .. d) == 0 then
        redis.call('SREM', KEYS[2], d)
    end
end
redis.call('SET', KEYS[1], ARGV[1], 'EX', ARGV[2])
redis.call('SADD', KEYS[2], ARGV[3])
local ttl = redis.call('TTL', KEYS[2])
if ttl < tonumber(ARGV[5]) then
    redis.call('EXPIRE', KEYS[2], ARGV[5])
end
return 1
"""

_REFRESH_SESSION_LUA = """
if redis.call('EXISTS', KEYS[1]) == 0 then
    return 0
end
redis.call('SET', KEYS[1], ARGV[1], 'EX', ARGV[2])
return 1
"""

_REVOKE_USER_LUA = """
local members = redis.call('SMEMBERS', KEYS[1])
local prefix = ARGV[1]
for _, d in ipairs(members) do
    redis.call('DEL', prefix .. d)
end
redis.call('DEL', KEYS[1])
return #members
"""

DEFAULT_SCOPES = [
    "openid",
    "profile",
    "jobs:read",
    "jobs:submit",
    "jobs:cancel",
    "files:read",
    "files:write",
    "files:delete",
]

DEFAULT_ABSOLUTE_TTL = 7 * 86400
DEFAULT_IDLE_TTL = 12 * 3600


_STORE_ERRORS = (redis.exceptions.RedisError, ValueError, KeyError)


_clients: WeakKeyDictionary = WeakKeyDictionary()


def _client():
    loop = asyncio.get_running_loop()
    client = _clients.get(loop)
    if client is None:
        cfg = get_config("redis")
        client = Redis(
            host=cfg.get("host", "localhost"),
            port=cfg.get("port", 6379),
            password=cfg.get("password", None),
            decode_responses=True,
            socket_connect_timeout=3,
            socket_timeout=3,
        )
        _clients[loop] = client
    return client


def _now() -> int:
    return int(time.time())


def _digest(token: str) -> str:
    return hashlib.sha256(token.encode()).hexdigest()


def _session_key(digest: str) -> str:
    return f"{_KEY_PREFIX}{digest}"


def _index_key(subject: str) -> str:
    return f"{_INDEX_PREFIX}{subject}"


def _unavailable(error: Exception) -> RuntimeError:
    return RuntimeError(f"session store unavailable: {error}")


async def create_session(
    subject: str,
    scopes: list[str],
    auth_method: str,
    *,
    absolute_ttl: int = DEFAULT_ABSOLUTE_TTL,
    idle_ttl: int = DEFAULT_IDLE_TTL,
) -> tuple[str, int]:
    token = secrets.token_urlsafe(32)
    digest = _digest(token)
    now = _now()
    record = {
        "subject": subject,
        "scopes": list(scopes),
        "auth_method": auth_method,
        "created_at": now,
        "absolute_expires_at": now + absolute_ttl,
        "idle_expires_at": now + idle_ttl,
        "idle_ttl": idle_ttl,
    }
    try:
        client = _client()
        # Atomic: prune dead members, write session + index, extend (never
        # shorten) the index TTL. A concurrent revoke-all is serialized by
        # Redis, so a login is either revoked or indexed afterwards.
        await client.eval(
            _CREATE_SESSION_LUA,
            2,
            _session_key(digest),
            _index_key(subject),
            json.dumps(record),
            absolute_ttl,
            digest,
            _KEY_PREFIX,
            absolute_ttl + 60,
        )
    except _STORE_ERRORS as error:
        raise _unavailable(error) from error
    return token, idle_ttl


async def validate_session(token: str) -> Principal | None:
    if not token:
        return None
    digest = _digest(token)
    try:
        client = _client()
        raw = await client.get(_session_key(digest))
        if raw is None:
            return None
        record = json.loads(raw)
        now = _now()
        if (
            record.get("absolute_expires_at", 0) <= now
            or record.get("idle_expires_at", 0) <= now
        ):
            await client.delete(_session_key(digest))
            await client.srem(_index_key(record.get("subject", "")), digest)
            return None

        idle_ttl = int(record.get("idle_ttl") or 0)
        if idle_ttl and record["idle_expires_at"] - now < idle_ttl // 2:
            # Refresh only when half the idle window is spent: keeps one write
            # per long window instead of one per request.
            record["idle_expires_at"] = min(
                now + idle_ttl, record["absolute_expires_at"]
            )
            remaining = record["absolute_expires_at"] - now
            # Refresh only if the key still exists: a concurrent revoke must
            # never be resurrected by this write.
            await client.eval(
                _REFRESH_SESSION_LUA,
                1,
                _session_key(digest),
                json.dumps(record),
                max(1, remaining),
            )
    except _STORE_ERRORS as error:
        raise _unavailable(error) from error

    return Principal(
        username=record["subject"],
        issuer="session",
        subject=record["subject"],
        auth_method=record.get("auth_method") or "session",
        scopes=record.get("scopes") or [],
    )


async def revoke_session(token: str) -> bool:
    if not token:
        return False
    digest = _digest(token)
    try:
        client = _client()
        raw = await client.get(_session_key(digest))
        if raw is None:
            return False
        record = json.loads(raw)
        pipe = client.pipeline()
        pipe.delete(_session_key(digest))
        pipe.srem(_index_key(record.get("subject", "")), digest)
        await pipe.execute()
    except _STORE_ERRORS as error:
        raise _unavailable(error) from error
    return True


async def revoke_user_sessions(subject: str) -> int:
    try:
        client = _client()
        return int(
            await client.eval(_REVOKE_USER_LUA, 1, _index_key(subject), _KEY_PREFIX)
        )
    except _STORE_ERRORS as error:
        raise _unavailable(error) from error


def revoke_user_sessions_sync(subject: str) -> int:
    """Sync entry point for CLI/admin commands (must not run inside a loop)."""

    async def scenario():
        _clients.clear()
        try:
            return await revoke_user_sessions(subject)
        finally:
            await _client().aclose()
            _clients.clear()

    return asyncio.run(scenario())
