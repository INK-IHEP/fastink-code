"""Integration tests for IPWhitelistMiddleware and UserValidationMiddleware
routing semantics.

These build a throwaway FastAPI app with the middleware explicitly attached
(independent of the global ip_whitelist_access / security_access config
switches, which are off in the test environment) so we can assert:

  - ip_controlled_routers uses exact/prefix matching via _path_matches
  - a whitelisted IP passes, a non-whitelisted IP is rejected with the
    FastINK IP_BANNED envelope (HTTP 200, status=IP_BANNED)
  - endpoints NOT in ip_controlled_routers bypass the IP check entirely
"""

from fastapi import FastAPI
from fastapi.testclient import TestClient

from fastink.routers.headers import IPWhitelistMiddleware, UserValidationMiddleware
from fastink.routers.status import InkStatus


def _build_ip_app(ip_whitelist, controlled):
    app = FastAPI()
    app.add_middleware(
        IPWhitelistMiddleware,
        ip_whitelist=ip_whitelist,
        forbidden_routers=controlled,
    )

    @app.get("/api/v2/auth/get_token")
    async def get_token():
        return {"status": InkStatus.SUCCESS, "msg": "ok", "data": {"token": "t"}}

    @app.get("/api/v2/auth/create_user")
    async def create_user():
        return {"status": InkStatus.SUCCESS, "msg": "ok", "data": None}

    @app.get("/api/v2/auth/create_token")
    async def create_token():
        return {"status": InkStatus.SUCCESS, "msg": "ok", "data": None}

    return TestClient(app)


CONTROLLED = [
    "/api/v1/",
    "/api/v2/auth/get_token",
    "/api/v2/auth/create_user",
]
WHITELIST = ["127.0.0.1", "192.168.51.96"]


class TestIPControlledEndpoints:
    def test_whitelisted_ip_allowed(self):
        client = _build_ip_app(WHITELIST, CONTROLLED)
        resp = client.get("/api/v2/auth/get_token", headers={"X-Real-IP": "192.168.51.96"})
        assert resp.status_code == 200
        assert resp.json()["data"]["token"] == "t"

    def test_non_whitelisted_ip_rejected(self):
        client = _build_ip_app(WHITELIST, CONTROLLED)
        resp = client.get("/api/v2/auth/get_token", headers={"X-Real-IP": "8.8.8.8"})
        # FastINK convention: always HTTP 200, business error in status field
        assert resp.status_code == 200
        body = resp.json()
        assert body["status"] == InkStatus.IP_BANNED
        assert body["data"] is None

    def test_newly_controlled_create_user_rejected_off_whitelist(self):
        client = _build_ip_app(WHITELIST, CONTROLLED)
        resp = client.get("/api/v2/auth/create_user", headers={"X-Real-IP": "8.8.8.8"})
        assert resp.json()["status"] == InkStatus.IP_BANNED


class TestIPUncontrolledEndpoints:
    def test_uncontrolled_endpoint_bypasses_ip_check(self):
        # create_token is NOT in the controlled list -> any IP allowed
        client = _build_ip_app(WHITELIST, CONTROLLED)
        resp = client.get("/api/v2/auth/create_token", headers={"X-Real-IP": "8.8.8.8"})
        assert resp.status_code == 200
        assert resp.json()["status"] == InkStatus.SUCCESS


class TestExactMatchIsolation:
    def test_exact_pattern_does_not_leak_to_sibling(self):
        # A hypothetical sibling path that shares the prefix of an EXACT
        # controlled entry must NOT be IP-controlled.
        app = FastAPI()
        app.add_middleware(
            IPWhitelistMiddleware,
            ip_whitelist=WHITELIST,
            forbidden_routers=["/api/v2/auth/get_token"],
        )

        @app.get("/api/v2/auth/get_token_public")
        async def sibling():
            return {"status": InkStatus.SUCCESS, "msg": "ok", "data": None}

        client = TestClient(app)
        # off-whitelist IP still allowed because the sibling is not controlled
        resp = client.get("/api/v2/auth/get_token_public", headers={"X-Real-IP": "8.8.8.8"})
        assert resp.json()["status"] == InkStatus.SUCCESS


def _build_skip_app(skip_routers):
    app = FastAPI()
    app.add_middleware(UserValidationMiddleware, skip_routers=skip_routers)

    @app.get("/api/v2/auth/create_token")
    async def create_token():
        return {"status": InkStatus.SUCCESS, "msg": "ok", "data": None}

    @app.get("/api/v2/auth/get_permission")
    async def get_permission():
        return {"status": InkStatus.SUCCESS, "msg": "ok", "data": None}

    return TestClient(app)


class TestSkipRoutersExact:
    def test_skipped_exact_endpoint_needs_no_token(self):
        client = _build_skip_app(["/api/v2/auth/create_token"])
        resp = client.get("/api/v2/auth/create_token")
        assert resp.status_code == 200
        assert resp.json()["status"] == InkStatus.SUCCESS

    def test_non_skipped_endpoint_requires_token(self):
        # get_permission is NOT skipped -> missing headers -> TOKEN_INVALID
        client = _build_skip_app(["/api/v2/auth/create_token"])
        resp = client.get("/api/v2/auth/get_permission")
        assert resp.status_code == 200
        assert resp.json()["status"] == InkStatus.TOKEN_INVALID
