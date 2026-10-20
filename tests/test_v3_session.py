import asyncio
import hashlib
import time

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from fastink.auth import session as session_mod
from fastink.auth.session import (
    create_session,
    revoke_session,
    revoke_user_sessions,
    validate_session,
)


class _Pipeline:
    def __init__(self, redis):
        self.redis = redis
        self.ops = []

    def delete(self, *keys):
        self.ops.append(("delete", keys))
        return self

    def srem(self, key, *values):
        self.ops.append(("srem", key, values))
        return self

    async def execute(self):
        for op in self.ops:
            name = op[0]
            if name == "delete":
                await self.redis.delete(*op[1])
            elif name == "srem":
                await self.redis.srem(op[1], *op[2])
        return [None] * len(self.ops)


class FakeRedis:
    def __init__(self):
        self.values = {}
        self.expires = {}
        self.sets = {}

    def _alive(self, key):
        exp = self.expires.get(key)
        if exp is not None and exp <= time.time():
            self.values.pop(key, None)
            self.expires.pop(key, None)
            return False
        return key in self.values

    def _expired(self, key):
        exp = self.expires.get(key)
        return exp is not None and exp <= time.time()

    async def get(self, key):
        return self.values.get(key) if self._alive(key) else None

    async def set(self, key, value, ex=None):
        self.values[key] = value
        if ex is not None:
            self.expires[key] = time.time() + ex
        else:
            self.expires.pop(key, None)

    async def delete(self, *keys):
        for key in keys:
            self.values.pop(key, None)
            self.expires.pop(key, None)
            self.sets.pop(key, None)

    async def sadd(self, key, *values):
        self.sets.setdefault(key, set()).update(values)

    async def srem(self, key, *values):
        bucket = self.sets.get(key, set())
        for value in values:
            bucket.discard(value)

    async def smembers(self, key):
        if self._expired(key):
            self.sets.pop(key, None)
            self.expires.pop(key, None)
            return set()
        return set(self.sets.get(key, set()))

    async def expire(self, key, ttl):
        new_expiry = time.time() + ttl
        if self.expires.get(key, 0) < new_expiry:
            self.expires[key] = new_expiry

    async def eval(self, script, numkeys, *args):
        if len(args) == 7:
            session_key, index_key, record_json, absolute_ttl, digest, prefix, index_ttl = args
            absolute_ttl, index_ttl = int(absolute_ttl), int(index_ttl)
            for member in list(self.sets.get(index_key, set())):
                if self._expired(prefix + member) or prefix + member not in self.values:
                    self.sets[index_key].discard(member)
            self.values[session_key] = record_json
            self.expires[session_key] = time.time() + absolute_ttl
            self.sets.setdefault(index_key, set()).add(digest)
            new_expiry = time.time() + index_ttl
            if self.expires.get(index_key, 0) < new_expiry:
                self.expires[index_key] = new_expiry
            return 1
        if len(args) == 3:
            key, record_json, remaining = args
            if self._expired(key) or key not in self.values:
                return 0
            self.values[key] = record_json
            self.expires[key] = time.time() + int(remaining)
            return 1
        if len(args) == 2:
            index_key, prefix = args
            members = set(self.sets.get(index_key, set()))
            for member in members:
                self.values.pop(prefix + member, None)
                self.expires.pop(prefix + member, None)
            self.sets.pop(index_key, None)
            self.expires.pop(index_key, None)
            return len(members)
        return 1

    def pipeline(self):
        return _Pipeline(self)


@pytest.fixture
def fake_redis(monkeypatch):
    fake = FakeRedis()
    monkeypatch.setattr(session_mod, "_client", lambda: fake)
    return fake


def test_create_and_validate_roundtrip(fake_redis):
    token, expires_in = asyncio.run(
        create_session("alice", ["openid", "jobs:read"], "password")
    )
    assert expires_in > 0
    principal = asyncio.run(validate_session(token))
    assert principal is not None
    assert principal.username == "alice"
    assert principal.scopes == ["openid", "jobs:read"]
    assert principal.auth_method == "password"
    assert principal.issuer == "session"


def test_redis_stores_only_digest(fake_redis):
    token, _ = asyncio.run(create_session("alice", ["openid"], "password"))
    assert all(token not in key for key in fake_redis.values)
    assert all(token not in value for value in fake_redis.values.values())


def test_revoke_session(fake_redis):
    token, _ = asyncio.run(create_session("alice", ["openid"], "password"))
    assert asyncio.run(revoke_session(token)) is True
    assert asyncio.run(validate_session(token)) is None
    assert asyncio.run(revoke_session(token)) is False


def test_revoke_user_sessions(fake_redis):
    t1, _ = asyncio.run(create_session("alice", ["openid"], "password"))
    t2, _ = asyncio.run(create_session("alice", ["openid"], "password"))
    t3, _ = asyncio.run(create_session("bob", ["openid"], "password"))
    assert asyncio.run(revoke_user_sessions("alice")) == 2
    assert asyncio.run(validate_session(t1)) is None
    assert asyncio.run(validate_session(t2)) is None
    assert asyncio.run(validate_session(t3)) is not None


def test_absolute_expiry(fake_redis):
    token, _ = asyncio.run(
        create_session("alice", ["openid"], "password", absolute_ttl=1)
    )
    time.sleep(1.1)
    assert asyncio.run(validate_session(token)) is None


def test_redis_failure_raises_runtime_error(monkeypatch):
    import redis.exceptions

    class Broken:
        async def get(self, key):
            raise redis.exceptions.ConnectionError("down")

        async def set(self, *a, **k):
            raise redis.exceptions.ConnectionError("down")

        async def delete(self, *a):
            raise redis.exceptions.ConnectionError("down")

        async def srem(self, *a):
            raise redis.exceptions.ConnectionError("down")

        async def eval(self, *a, **k):
            raise redis.exceptions.ConnectionError("down")

        def pipeline(self):
            raise redis.exceptions.ConnectionError("down")

    monkeypatch.setattr(session_mod, "_client", lambda: Broken())
    with pytest.raises(RuntimeError, match="session store unavailable"):
        asyncio.run(create_session("alice", ["openid"], "password"))
    with pytest.raises(RuntimeError, match="session store unavailable"):
        asyncio.run(validate_session("token"))


def test_login_returns_token(monkeypatch):
    from fastink.routers.v3 import auth as auth_router

    class Backend:
        def validate_user(self, username, password, uid=None):
            return username == "alice" and password == "secret"

    monkeypatch.setattr(auth_router, "get_config", lambda *a, **k: "password")
    monkeypatch.setattr(auth_router, "get_auth_backend", lambda *a, **k: Backend())
    monkeypatch.setattr(
        auth_router.user_mod, "get_user", lambda **kw: {"username": "alice"}
    )
    monkeypatch.setattr(
        auth_router, "create_session", lambda *a, **k: _async_value(("tok", 100))
    )

    app = FastAPI()
    app.include_router(auth_router.router, prefix="/api/v3")
    response = TestClient(app).post(
        "/api/v3/auth/login", json={"username": "alice", "password": "secret"}
    )
    assert response.status_code == 200
    body = response.json()
    assert body["access_token"] == "tok"
    assert body["token_type"] == "Bearer"
    assert body["expires_in"] == 100


def test_login_rejects_bad_credentials(monkeypatch):
    from fastink.routers.v3 import auth as auth_router

    class Backend:
        def validate_user(self, username, password, uid=None):
            return False

    monkeypatch.setattr(auth_router, "get_config", lambda *a, **k: "password")
    monkeypatch.setattr(auth_router, "get_auth_backend", lambda *a, **k: Backend())

    app = FastAPI()
    app.include_router(auth_router.router, prefix="/api/v3")
    response = TestClient(app).post(
        "/api/v3/auth/login", json={"username": "alice", "password": "wrong"}
    )
    assert response.status_code == 401
    assert response.json()["detail"] == "Invalid username or password"


async def _async_value(value):
    return value


def test_index_ttl_not_shortened_and_user_revocation_covers_all(fake_redis):
    cli_token, _ = asyncio.run(
        create_session("alice", ["openid"], "password", absolute_ttl=30 * 86400)
    )
    web_token, _ = asyncio.run(
        create_session("alice", ["openid"], "password", absolute_ttl=7 * 86400)
    )
    index_key = "ink:user_sessions:alice"
    assert fake_redis.expires[index_key] >= time.time() + 29 * 86400
    assert asyncio.run(revoke_user_sessions("alice")) == 2
    assert asyncio.run(validate_session(cli_token)) is None
    assert asyncio.run(validate_session(web_token)) is None


def test_login_returns_503_when_session_store_down(monkeypatch):
    from fastink.routers.v3 import auth as auth_router

    class Backend:
        def validate_user(self, username, password, uid=None):
            return True

    async def broken(*args, **kwargs):
        raise RuntimeError("session store unavailable")

    monkeypatch.setattr(auth_router, "get_config", lambda *a, **k: "password")
    monkeypatch.setattr(auth_router, "get_auth_backend", lambda *a, **k: Backend())
    monkeypatch.setattr(
        auth_router.user_mod, "get_user", lambda **kw: {"username": "alice"}
    )
    monkeypatch.setattr(auth_router, "create_session", broken)

    app = FastAPI()
    app.include_router(auth_router.router, prefix="/api/v3")
    response = TestClient(app).post(
        "/api/v3/auth/login", json={"username": "alice", "password": "x"}
    )
    assert response.status_code == 503
    assert response.json()["detail"] == "Authentication service unavailable"


def test_create_prunes_dead_index_members(fake_redis):
    first, _ = asyncio.run(create_session("alice", ["openid"], "password"))
    digest = "ink:session:" + hashlib.sha256(first.encode()).hexdigest()
    fake_redis.values.pop(digest, None)
    second, _ = asyncio.run(create_session("alice", ["openid"], "password"))
    index = fake_redis.sets["ink:user_sessions:alice"]
    assert len(index) == 1
    assert asyncio.run(validate_session(second)) is not None


def test_logout_returns_503_when_store_down(monkeypatch):
    from fastink.routers.v3 import auth as auth_router

    async def broken(token):
        raise RuntimeError("session store unavailable")

    monkeypatch.setattr(auth_router, "revoke_session", broken)
    app = FastAPI()
    app.include_router(auth_router.router, prefix="/api/v3")
    response = TestClient(app).post(
        "/api/v3/auth/logout", headers={"Authorization": "Bearer any"}
    )
    assert response.status_code == 503
    assert response.json()["detail"] == "Authentication service unavailable"


def test_idle_expiry_returns_none(fake_redis, monkeypatch):
    clock = [1_800_000_000]
    monkeypatch.setattr(session_mod, "_now", lambda: clock[0])
    token, _ = asyncio.run(
        create_session("alice", ["openid"], "password", absolute_ttl=100, idle_ttl=10)
    )
    clock[0] += 11
    assert asyncio.run(validate_session(token)) is None


def test_no_refresh_before_half_window(fake_redis, monkeypatch):
    clock = [1_800_000_000]
    monkeypatch.setattr(session_mod, "_now", lambda: clock[0])
    token, _ = asyncio.run(
        create_session("alice", ["openid"], "password", absolute_ttl=100, idle_ttl=10)
    )
    clock[0] += 4
    assert asyncio.run(validate_session(token)) is not None
    key = session_mod._session_key(session_mod._digest(token))
    record = __import__("json").loads(fake_redis.values[key])
    assert record["idle_expires_at"] == 1_800_000_010


def test_refresh_after_half_window(fake_redis, monkeypatch):
    clock = [1_800_000_000]
    monkeypatch.setattr(session_mod, "_now", lambda: clock[0])
    token, _ = asyncio.run(
        create_session("alice", ["openid"], "password", absolute_ttl=100, idle_ttl=10)
    )
    clock[0] += 6
    assert asyncio.run(validate_session(token)) is not None
    key = session_mod._session_key(session_mod._digest(token))
    record = __import__("json").loads(fake_redis.values[key])
    assert record["idle_expires_at"] == 1_800_000_016


def test_refresh_capped_at_absolute_expiry(fake_redis, monkeypatch):
    clock = [1_800_000_000]
    monkeypatch.setattr(session_mod, "_now", lambda: clock[0])
    token, _ = asyncio.run(
        create_session("alice", ["openid"], "password", absolute_ttl=7, idle_ttl=10)
    )
    clock[0] += 6
    assert asyncio.run(validate_session(token)) is not None
    key = session_mod._session_key(session_mod._digest(token))
    record = __import__("json").loads(fake_redis.values[key])
    assert record["idle_expires_at"] == 1_800_000_007


def test_login_krb5_dispatch_success(monkeypatch):
    from fastink.routers.v3 import auth as auth_router

    calls = []

    class Backend:
        def create_token(self, username, password):
            calls.append((username, password))
            return {"method": "krb5"}

    monkeypatch.setattr(auth_router, "get_config", lambda *a, **k: "krb5")
    monkeypatch.setattr(auth_router, "get_auth_backend", lambda *a, **k: Backend())
    monkeypatch.setattr(
        auth_router, "create_session", lambda *a, **k: _async_value(("tok", 100))
    )
    app = FastAPI()
    app.include_router(auth_router.router, prefix="/api/v3")
    response = TestClient(app).post(
        "/api/v3/auth/login", json={"username": "alice", "password": "secret"}
    )
    assert response.status_code == 200
    assert calls == [("alice", "secret")]


def test_login_krb5_failure_uniform_401(monkeypatch):
    from fastink.routers.v3 import auth as auth_router

    class Backend:
        def create_token(self, username, password):
            raise ValueError("kinit failed: client not found")

    monkeypatch.setattr(auth_router, "get_config", lambda *a, **k: "krb5")
    monkeypatch.setattr(auth_router, "get_auth_backend", lambda *a, **k: Backend())
    app = FastAPI()
    app.include_router(auth_router.router, prefix="/api/v3")
    response = TestClient(app).post(
        "/api/v3/auth/login", json={"username": "alice", "password": "x"}
    )
    assert response.status_code == 401
    assert response.json()["detail"] == "Invalid username or password"


def test_refresh_does_not_recreate_revoked_session(fake_redis):
    token, _ = asyncio.run(
        create_session("alice", ["openid"], "password", absolute_ttl=100, idle_ttl=10)
    )
    key = session_mod._session_key(session_mod._digest(token))
    record = fake_redis.values.pop(key)
    fake_redis.expires.pop(key, None)
    result = asyncio.run(
        fake_redis.eval(session_mod._REFRESH_SESSION_LUA, 1, key, record, 30)
    )
    assert result == 0
    assert key not in fake_redis.values


def test_login_unsupported_backend_returns_501(monkeypatch):
    from fastink.routers.v3 import auth as auth_router

    monkeypatch.setattr(auth_router, "get_config", lambda *a, **k: "apikey")
    monkeypatch.setattr(auth_router, "get_auth_backend", lambda *a, **k: object())
    app = FastAPI()
    app.include_router(auth_router.router, prefix="/api/v3")
    response = TestClient(app).post(
        "/api/v3/auth/login", json={"username": "alice", "password": "x"}
    )
    assert response.status_code == 501


def test_login_noop_backend_result_returns_401(monkeypatch):
    from fastink.routers.v3 import auth as auth_router

    class Backend:
        def create_token(self, username, password):
            return None

    monkeypatch.setattr(auth_router, "get_config", lambda *a, **k: "krb5")
    monkeypatch.setattr(auth_router, "get_auth_backend", lambda *a, **k: Backend())
    app = FastAPI()
    app.include_router(auth_router.router, prefix="/api/v3")
    response = TestClient(app).post(
        "/api/v3/auth/login", json={"username": "alice", "password": "x"}
    )
    assert response.status_code == 401


def test_oidc_shim_keeps_default_auth_method():
    from fastink.auth.oidc.principal import Principal as OidcPrincipal

    principal = OidcPrincipal(username="alice", issuer="iss", subject="sub")
    assert principal.auth_method == "oidc"


def test_missing_redis_config_raises_runtime_error(monkeypatch):
    def broken():
        raise ValueError("missing redis config")

    monkeypatch.setattr(session_mod, "_client", broken)
    with pytest.raises(RuntimeError, match="session store unavailable"):
        asyncio.run(create_session("alice", ["openid"], "password"))


def test_logout_success_returns_204(monkeypatch):
    from fastink.routers.v3 import auth as auth_router

    revoked = []

    async def revoke(token):
        revoked.append(token)
        return True

    monkeypatch.setattr(auth_router, "revoke_session", revoke)
    app = FastAPI()
    app.include_router(auth_router.router, prefix="/api/v3")
    response = TestClient(app).post(
        "/api/v3/auth/logout", headers={"Authorization": "Bearer tok-123"}
    )
    assert response.status_code == 204
    assert revoked == ["tok-123"]


def test_login_password_mode_provisions_missing_user(monkeypatch):
    from fastink.routers.v3 import auth as auth_router

    class Backend:
        def validate_user(self, username, password, uid=None):
            return True

    calls = []
    monkeypatch.setattr(auth_router, "get_config", lambda *a, **k: "password")
    monkeypatch.setattr(auth_router, "get_auth_backend", lambda *a, **k: Backend())
    monkeypatch.setattr(auth_router.user_mod, "get_user", lambda **kw: {})
    monkeypatch.setattr(
        auth_router.user_mod,
        "add_user",
        lambda username, uid=None, **kw: calls.append(username) or True,
    )
    monkeypatch.setattr(
        auth_router, "create_session", lambda *a, **k: _async_value(("tok", 100))
    )
    app = FastAPI()
    app.include_router(auth_router.router, prefix="/api/v3")
    response = TestClient(app).post(
        "/api/v3/auth/login", json={"username": "alice", "password": "x"}
    )
    assert response.status_code == 200
    assert calls == ["alice"]


def test_login_backend_timeout_returns_503(monkeypatch):
    from fastink.routers.v3 import auth as auth_router

    class Backend:
        def create_token(self, username, password):
            raise TimeoutError("kdc timeout")

    monkeypatch.setattr(auth_router, "get_config", lambda *a, **k: "krb5")
    monkeypatch.setattr(auth_router, "get_auth_backend", lambda *a, **k: Backend())
    app = FastAPI()
    app.include_router(auth_router.router, prefix="/api/v3")
    response = TestClient(app).post(
        "/api/v3/auth/login", json={"username": "alice", "password": "x"}
    )
    assert response.status_code == 503


def test_login_grants_admin_scope_for_admins(monkeypatch):
    from fastink.routers.v3 import auth as auth_router

    class Backend:
        def validate_user(self, username, password, uid=None):
            return True

    captured = {}

    async def fake_create(subject, scopes, method, **kwargs):
        captured["scopes"] = list(scopes)
        return "tok", 100

    monkeypatch.setattr(auth_router, "get_config", lambda *a, **k: "password")
    monkeypatch.setattr(auth_router, "get_auth_backend", lambda *a, **k: Backend())
    monkeypatch.setattr(
        auth_router.user_mod, "get_user", lambda **kw: {"username": "alice"}
    )
    monkeypatch.setattr(
        auth_router.permission_mod,
        "check_user_permission",
        lambda **kw: True,
    )
    monkeypatch.setattr(auth_router, "create_session", fake_create)
    app = FastAPI()
    app.include_router(auth_router.router, prefix="/api/v3")
    response = TestClient(app).post(
        "/api/v3/auth/login", json={"username": "alice", "password": "x"}
    )
    assert response.status_code == 200
    assert "admin" in captured["scopes"]
    assert "admin" in response.json()["scope"].split()


def test_logout_rejects_api_key():
    from fastink.routers.v3 import auth as auth_router_module

    app = FastAPI()
    app.include_router(auth_router_module.router, prefix="/api/v3")
    response = TestClient(app).post(
        "/api/v3/auth/logout", headers={"Authorization": "Bearer ink_example"}
    )
    assert response.status_code == 400
    assert "API keys cannot be logged out" in response.json()["detail"]
