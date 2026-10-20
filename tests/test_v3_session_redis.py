import asyncio
import uuid

import pytest

from fastink.auth import session as session_mod


def run(coro):
    """Run one scenario on a dedicated event loop with a fresh Redis client.

    The production client is process-cached; reusing it across asyncio.run()
    loops raises "Event loop is closed".
    """

    async def wrapper():
        session_mod._clients.clear()
        try:
            return await coro
        finally:
            await session_mod._client().aclose()
            session_mod._clients.clear()

    return asyncio.run(wrapper())


@pytest.fixture(autouse=True)
def require_redis():
    async def ping():
        session_mod._clients.clear()
        try:
            return await session_mod._client().ping()
        finally:
            await session_mod._client().aclose()
            session_mod._clients.clear()

    try:
        asyncio.run(ping())
    except Exception:
        pytest.skip("Redis unavailable")


@pytest.fixture
def subject():
    name = f"session-test-{uuid.uuid4().hex[:8]}"
    yield name
    run(session_mod.revoke_user_sessions(name))


def test_index_ttl_survives_shorter_session(subject):
    async def scenario():
        await session_mod.create_session(
            subject, ["openid"], "password", absolute_ttl=30 * 86400
        )
        await session_mod.create_session(
            subject, ["openid"], "password", absolute_ttl=7 * 86400
        )
        return await session_mod._client().ttl(session_mod._index_key(subject))

    assert run(scenario()) > 29 * 86400


def test_create_revoke_interleaving_keeps_invariant(subject):
    async def scenario():
        for _ in range(20):
            create = asyncio.create_task(
                session_mod.create_session(
                    subject, ["openid"], "password", absolute_ttl=60
                )
            )
            revoke = asyncio.create_task(session_mod.revoke_user_sessions(subject))
            token, _ = await create
            await revoke
            if await session_mod.validate_session(token) is not None:
                indexed = await session_mod._client().sismember(
                    session_mod._index_key(subject), session_mod._digest(token)
                )
                assert indexed == 1

    run(scenario())


def test_revoke_user_sessions_clears_real_index(subject):
    async def scenario():
        token, _ = await session_mod.create_session(
            subject, ["openid"], "password", absolute_ttl=60
        )
        revoked = await session_mod.revoke_user_sessions(subject)
        exists = await session_mod._client().exists(session_mod._index_key(subject))
        still_valid = await session_mod.validate_session(token)
        return revoked, exists, still_valid

    revoked, exists, still_valid = run(scenario())
    assert revoked == 1
    assert exists == 0
    assert still_valid is None


def test_refresh_cannot_recreate_revoked_session(subject):
    async def scenario():
        token, _ = await session_mod.create_session(
            subject, ["openid"], "password", absolute_ttl=60, idle_ttl=60
        )
        client = session_mod._client()
        key = session_mod._session_key(session_mod._digest(token))
        raw = await client.get(key)
        await client.delete(key)
        result = await client.eval(
            session_mod._REFRESH_SESSION_LUA, 1, key, raw, 30
        )
        return result, await client.exists(key)

    result, exists = run(scenario())
    assert result == 0
    assert exists == 0
