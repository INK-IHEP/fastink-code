import hashlib
import json
import secrets
import time

import redis.exceptions

from fastink.auth import session as session_mod
from fastink.auth.principal import Principal
from fastink.common.logger import logger

_APIKEY_PREFIX = "ink:apikey:"
_APIKEY_INDEX = "ink:apikeys"
_NAME_PREFIX = "ink:apikey:name:"
_DELETE_NAME_IF_MATCH_LUA = """
if redis.call('GET', KEYS[1]) == ARGV[1] then
    return redis.call('DEL', KEYS[1])
end
return 0
"""


_STORE_ERRORS = (redis.exceptions.RedisError, ValueError, KeyError)


def _digest(token: str) -> str:
    return hashlib.sha256(token.encode()).hexdigest()


def _key(digest: str) -> str:
    return f"{_APIKEY_PREFIX}{digest}"


def _unavailable(error: Exception) -> RuntimeError:
    return RuntimeError(f"apikey store unavailable: {error}")


async def create_api_key(
    name: str,
    scopes: list[str],
    created_by: str,
    expires_in: int | None = None,
) -> tuple[str, dict]:
    token = "ink_" + secrets.token_urlsafe(32)
    digest = _digest(token)
    now = int(time.time())
    record = {
        "name": name,
        "scopes": list(scopes),
        "created_by": created_by,
        "created_at": now,
        "expires_at": now + expires_in if expires_in else None,
    }
    try:
        client = session_mod._client()
        name_key = f"{_NAME_PREFIX}{name}"
        claimed = (
            await client.set(name_key, digest, nx=True, ex=expires_in)
            if expires_in
            else await client.set(name_key, digest, nx=True)
        )
        if not claimed:
            raise ValueError("name already exists")
        try:
            pipe = client.pipeline()
            if expires_in:
                pipe.set(_key(digest), json.dumps(record), ex=expires_in)
            else:
                pipe.set(_key(digest), json.dumps(record))
            pipe.sadd(_APIKEY_INDEX, digest)
            await pipe.execute()
        except Exception:
            await client.delete(name_key)
            raise
    except ValueError:
        raise
    except _STORE_ERRORS as error:
        raise _unavailable(error) from error
    return token, {"key_id": digest, **record}


async def validate_api_key(token: str) -> Principal | None:
    if not token.startswith("ink_"):
        return None
    digest = _digest(token)
    try:
        raw = await session_mod._client().get(_key(digest))
    except _STORE_ERRORS as error:
        raise _unavailable(error) from error
    if raw is None:
        return None
    record = json.loads(raw)
    name = str(record.get("name") or "")
    if not name:
        return None
    expires_at = record.get("expires_at")
    if expires_at and int(expires_at) <= int(time.time()):
        try:
            client = session_mod._client()
            pipe = client.pipeline()
            pipe.delete(_key(digest))
            pipe.srem(_APIKEY_INDEX, digest)
            await pipe.execute()
            await client.eval(
                _DELETE_NAME_IF_MATCH_LUA, 1, f"{_NAME_PREFIX}{name}", digest
            )
        except _STORE_ERRORS as error:
            raise _unavailable(error) from error
        return None
    return Principal(
        username=f"service:{name}",
        issuer="apikey",
        subject=name,
        auth_method="apikey",
        scopes=record.get("scopes") or [],
    )


async def list_api_keys() -> list[dict]:
    try:
        client = session_mod._client()
        digests = await client.smembers(_APIKEY_INDEX)
        keys = []
        for digest in digests:
            raw = await client.get(_key(digest))
            if raw is None:
                await client.srem(_APIKEY_INDEX, digest)
                continue
            record = json.loads(raw)
            expires_at = record.get("expires_at")
            if expires_at and int(expires_at) <= int(time.time()):
                await client.delete(_key(digest))
                await client.srem(_APIKEY_INDEX, digest)
                await client.eval(
                    _DELETE_NAME_IF_MATCH_LUA,
                    1,
                    f"{_NAME_PREFIX}{record.get('name', '')}",
                    digest,
                )
                continue
            keys.append({"key_id": digest, **record})
    except _STORE_ERRORS as error:
        raise _unavailable(error) from error
    return keys


async def revoke_api_key(key_id: str) -> bool:
    try:
        client = session_mod._client()
        raw = await client.get(_key(key_id))
        if raw is None:
            return False
        record = json.loads(raw)
        pipe = client.pipeline()
        pipe.delete(_key(key_id))
        pipe.srem(_APIKEY_INDEX, key_id)
        await pipe.execute()
        await client.eval(
            _DELETE_NAME_IF_MATCH_LUA,
            1,
            f"{_NAME_PREFIX}{record.get('name', '')}",
            key_id,
        )
    except _STORE_ERRORS as error:
        raise _unavailable(error) from error
    return True
