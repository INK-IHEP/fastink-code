import asyncio
import uuid
from unittest.mock import patch

from fastapi import FastAPI, Request
from fastapi.testclient import TestClient

from fastink.auth.apikey import (
    create_api_key,
    list_api_keys,
    revoke_api_key,
    validate_api_key,
)
from fastink.auth.principal import Principal
from fastink.routers.v3 import apikeys as apikeys_router
from fastink.routers.v3.middleware import BearerAuthMiddleware


def _name():
    return f"svc-{uuid.uuid4().hex[:8]}"


def test_api_key_roundtrip():
    name = _name()
    token, record = asyncio.run(create_api_key(name, ["jobs:read"], "admin"))
    assert token.startswith("ink_")
    assert record["key_id"]

    principal = asyncio.run(validate_api_key(token))
    assert principal.username == f"service:{name}"
    assert principal.auth_method == "apikey"
    assert principal.scopes == ["jobs:read"]

    assert asyncio.run(revoke_api_key(record["key_id"])) is True
    assert asyncio.run(validate_api_key(token)) is None
    assert asyncio.run(revoke_api_key(record["key_id"])) is False


def test_api_key_expires():
    name = _name()
    with patch("fastink.auth.apikey.time.time", return_value=1000):
        token, record = asyncio.run(
            create_api_key(name, ["openid"], "admin", expires_in=10)
        )
    assert record["expires_at"] == 1010

    with patch("fastink.auth.apikey.time.time", return_value=1011):
        assert asyncio.run(validate_api_key(token)) is None


def test_api_key_list_skips_expired():
    name = _name()
    with patch("fastink.auth.apikey.time.time", return_value=1000):
        _, record = asyncio.run(
            create_api_key(name, ["openid"], "admin", expires_in=10)
        )

    with patch("fastink.auth.apikey.time.time", return_value=1011):
        keys = asyncio.run(list_api_keys())

    assert all(key["key_id"] != record["key_id"] for key in keys)


def test_api_key_list_contains_record():
    name = _name()
    _, record = asyncio.run(create_api_key(name, ["jobs:read"], "admin"))
    keys = asyncio.run(list_api_keys())
    assert any(key["key_id"] == record["key_id"] and key["name"] == name for key in keys)
    asyncio.run(revoke_api_key(record["key_id"]))


def test_api_key_authenticates_v3_request():
    name = _name()
    token, record = asyncio.run(create_api_key(name, ["openid"], "admin"))

    app = FastAPI()
    app.add_middleware(BearerAuthMiddleware, public_paths=[])

    @app.get("/api/v3/whoami")
    async def whoami(request: Request):
        return {"user": request.state.principal.username}

    response = TestClient(app).get(
        "/api/v3/whoami", headers={"Authorization": f"Bearer {token}"}
    )
    assert response.status_code == 200
    assert response.json()["user"] == f"service:{name}"
    asyncio.run(revoke_api_key(record["key_id"]))


def test_apikeys_router_requires_admin_scope(monkeypatch):
    admin = Principal(
        username="admin",
        issuer="session",
        subject="admin",
        auth_method="password",
        scopes=["admin"],
    )
    member = Principal(
        username="member",
        issuer="session",
        subject="member",
        auth_method="password",
        scopes=["openid"],
    )

    app = FastAPI()
    app.add_middleware(BearerAuthMiddleware, public_paths=[])
    app.include_router(apikeys_router.router, prefix="/api/v3")

    async def as_admin(token):
        return admin

    from unittest.mock import patch
    from fastink.routers.v3 import apikeys as apikeys_module

    monkeypatch.setattr(
        apikeys_module.permission, "check_user_permission", lambda **kw: True
    )

    with patch("fastink.routers.v3.middleware.validate_session", as_admin):
        created = TestClient(app).post(
            "/api/v3/apikeys",
            json={"name": _name(), "scopes": ["jobs:read"]},
            headers={"Authorization": "Bearer test-session"},
        )
    assert created.status_code == 201
    body = created.json()
    assert body["api_key"].startswith("ink_")
    assert body["key_id"]

    async def as_member(token):
        return member

    with patch("fastink.routers.v3.middleware.validate_session", as_member):
        forbidden = TestClient(app).get(
            "/api/v3/apikeys", headers={"Authorization": "Bearer test-session"}
        )
    assert forbidden.status_code == 403

    async def cleanup(token):
        return admin

    with patch("fastink.routers.v3.middleware.validate_session", cleanup):
        deleted = TestClient(app).delete(
            f"/api/v3/apikeys/{body['key_id']}",
            headers={"Authorization": "Bearer test-session"},
        )
    assert deleted.status_code == 204


def test_apikeys_router_rejects_unknown_scope(monkeypatch):
    admin = Principal(
        username="admin",
        issuer="session",
        subject="admin",
        auth_method="password",
        scopes=["admin"],
    )

    async def as_admin(token):
        return admin

    from unittest.mock import patch
    from fastink.routers.v3 import apikeys as apikeys_module

    monkeypatch.setattr(
        apikeys_module.permission, "check_user_permission", lambda **kw: True
    )

    app = FastAPI()
    app.add_middleware(BearerAuthMiddleware, public_paths=[])
    app.include_router(apikeys_router.router, prefix="/api/v3")
    with patch("fastink.routers.v3.middleware.validate_session", as_admin):
        response = TestClient(app).post(
            "/api/v3/apikeys",
            json={"name": "svc", "scopes": ["not-a-scope"]},
            headers={"Authorization": "Bearer test-session"},
        )
    assert response.status_code == 400


def test_api_key_cannot_access_user_bound_route():
    from fastink.auth.scopes import require_scope

    name = _name()
    token, record = asyncio.run(create_api_key(name, ["jobs:read"], "admin"))

    app = FastAPI()
    app.add_middleware(BearerAuthMiddleware, public_paths=[])

    @app.get("/api/v3/user-bound")
    async def user_bound(
        principal: Principal = require_scope("jobs:read", user_only=True),
    ):
        return {"ok": True}

    response = TestClient(app).get(
        "/api/v3/user-bound", headers={"Authorization": f"Bearer {token}"}
    )
    assert response.status_code == 403
    asyncio.run(revoke_api_key(record["key_id"]))


def test_api_key_stored_as_digest_only():
    from fastink.auth import session as session_mod

    name = _name()
    token, record = asyncio.run(create_api_key(name, ["openid"], "admin"))

    async def scan():
        client = session_mod._client()
        keys = [key async for key in client.scan_iter("ink:apikey:*")]
        values = [await client.get(key) for key in keys]
        return keys, values

    keys, values = asyncio.run(scan())
    assert keys
    assert all(token not in key for key in keys)
    assert all(token not in (value or "") for value in values)
    asyncio.run(revoke_api_key(record["key_id"]))


def test_apikeys_router_rechecks_admin_permission(monkeypatch):
    from unittest.mock import patch
    from fastink.routers.v3 import apikeys as apikeys_module

    admin = Principal(
        username="alice",
        issuer="session",
        subject="alice",
        auth_method="password",
        scopes=["admin"],
    )

    async def as_admin(token):
        return admin

    monkeypatch.setattr(
        apikeys_module.permission, "check_user_permission", lambda **kw: False
    )
    app = FastAPI()
    app.add_middleware(BearerAuthMiddleware, public_paths=[])
    app.include_router(apikeys_module.router, prefix="/api/v3")
    with patch("fastink.routers.v3.middleware.validate_session", as_admin):
        response = TestClient(app).get(
            "/api/v3/apikeys", headers={"Authorization": "Bearer test-session"}
        )
    assert response.status_code == 403
    assert response.json()["detail"] == "Admin permission required"


def test_apikeys_router_rejects_duplicate_and_long_names(monkeypatch):
    from unittest.mock import patch
    from fastink.routers.v3 import apikeys as apikeys_module

    admin = Principal(
        username="admin",
        issuer="session",
        subject="admin",
        auth_method="password",
        scopes=["admin"],
    )

    async def as_admin(token):
        return admin

    monkeypatch.setattr(
        apikeys_module.permission, "check_user_permission", lambda **kw: True
    )
    app = FastAPI()
    app.add_middleware(BearerAuthMiddleware, public_paths=[])
    app.include_router(apikeys_module.router, prefix="/api/v3")
    headers = {"Authorization": "Bearer test-session"}
    with patch("fastink.routers.v3.middleware.validate_session", as_admin):
        first = TestClient(app).post(
            "/api/v3/apikeys", json={"name": "dup-name", "scopes": []}, headers=headers
        )
        assert first.status_code == 201
        duplicate = TestClient(app).post(
            "/api/v3/apikeys", json={"name": "dup-name", "scopes": []}, headers=headers
        )
        long_name = TestClient(app).post(
            "/api/v3/apikeys", json={"name": "x" * 65, "scopes": []}, headers=headers
        )
        key_id = first.json()["key_id"]
        TestClient(app).delete(f"/api/v3/apikeys/{key_id}", headers=headers)
    assert duplicate.status_code == 400
    assert long_name.status_code == 400
