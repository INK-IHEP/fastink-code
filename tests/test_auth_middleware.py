"""Integration tests for IPWhitelistMiddleware and UserValidationMiddleware
routing semantics.

These build a throwaway FastAPI app with the middleware explicitly attached
(independent of the global ip_whitelist_access / security_access config
switches, which are off in the test environment) so we can assert:

  - ip_controlled_routers uses exact/prefix matching via _path_matches
  - a whitelisted IP passes, a non-whitelisted IP is rejected with the
    FastINK IP_BANNED envelope (HTTP 200, status=IP_BANNED)
  - endpoints NOT in ip_controlled_routers bypass the IP check entirely
  - endpoints in token_bypass_routers skip the IP check when the request
    carries a VALID Ink-Username/Ink-Token pair, and still require a
    whitelisted IP otherwise (missing or invalid credentials)
"""

import asyncio
import json
import threading

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from fastapi import Request
from fastapi.responses import JSONResponse

from fastink.routers.headers import IPWhitelistMiddleware, UserValidationMiddleware
from fastink.routers.status import InkStatus


def _build_ip_app(ip_whitelist, controlled, token_bypass=None):
    app = FastAPI()
    kwargs = {"ip_whitelist": ip_whitelist, "forbidden_routers": controlled}
    if token_bypass is not None:
        kwargs["token_bypass_routers"] = token_bypass
    app.add_middleware(IPWhitelistMiddleware, **kwargs)

    @app.get("/api/v2/auth/get_token")
    async def get_token():
        return {"status": InkStatus.SUCCESS, "msg": "ok", "data": {"token": "t"}}

    @app.get("/api/v2/auth/get_permission")
    async def get_permission():
        return {"status": InkStatus.SUCCESS, "msg": "ok", "data": None}

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
    "/api/v2/auth/get_permission",
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


BY_PASS = ["/api/v2/auth/get_permission"]
AUTH_HEADERS = {"Ink-Username": "alice", "Ink-Token": "valid-token"}
PERMISSION_URL = "/api/v2/auth/get_permission?username=alice"


class TestIPTokenBypass:
    """token_bypass_routers: valid credentials skip the IP check on the
    designated routers; anything else still requires a whitelisted IP.
    The queried username must match the validated header identity."""

    def test_valid_token_bypasses_ip_check(self, monkeypatch):
        calls = []

        def fake_validate(username, token):
            calls.append((username, token))
            return True

        monkeypatch.setattr("fastink.routers.headers.validate_token", fake_validate)
        client = _build_ip_app(WHITELIST, CONTROLLED, token_bypass=BY_PASS)
        resp = client.get(
            PERMISSION_URL,
            headers={"X-Real-IP": "8.8.8.8", **AUTH_HEADERS},
        )
        assert resp.status_code == 200
        assert resp.json()["status"] == InkStatus.SUCCESS
        # the bypass branch must actually have been exercised (the path IS
        # IP-controlled, so SUCCESS can only come from token validation)
        assert calls == [("alice", "valid-token")]

    def test_query_username_mismatch_still_ip_checked(self, monkeypatch):
        # a valid account must not enumerate another user's permissions
        monkeypatch.setattr(
            "fastink.routers.headers.validate_token", lambda username, token: True
        )
        client = _build_ip_app(WHITELIST, CONTROLLED, token_bypass=BY_PASS)
        resp = client.get(
            "/api/v2/auth/get_permission?username=bob",
            headers={"X-Real-IP": "8.8.8.8", **AUTH_HEADERS},
        )
        assert resp.json()["status"] == InkStatus.IP_BANNED

    def test_no_token_still_ip_checked(self, monkeypatch):
        monkeypatch.setattr(
            "fastink.routers.headers.validate_token", lambda username, token: True
        )
        client = _build_ip_app(WHITELIST, CONTROLLED, token_bypass=BY_PASS)
        resp = client.get(PERMISSION_URL, headers={"X-Real-IP": "8.8.8.8"})
        assert resp.json()["status"] == InkStatus.IP_BANNED

    def test_empty_headers_still_ip_checked(self, monkeypatch):
        # present-but-empty headers must not trigger validation
        monkeypatch.setattr(
            "fastink.routers.headers.validate_token", lambda username, token: True
        )
        client = _build_ip_app(WHITELIST, CONTROLLED, token_bypass=BY_PASS)
        resp = client.get(
            PERMISSION_URL,
            headers={"X-Real-IP": "8.8.8.8", "Ink-Username": "", "Ink-Token": ""},
        )
        assert resp.json()["status"] == InkStatus.IP_BANNED

    def test_invalid_token_still_ip_checked(self, monkeypatch):
        # header presence alone must NOT bypass — the token must validate
        monkeypatch.setattr(
            "fastink.routers.headers.validate_token", lambda username, token: False
        )
        client = _build_ip_app(WHITELIST, CONTROLLED, token_bypass=BY_PASS)
        resp = client.get(
            PERMISSION_URL,
            headers={"X-Real-IP": "8.8.8.8", **AUTH_HEADERS},
        )
        assert resp.json()["status"] == InkStatus.IP_BANNED

    def test_whitelisted_ip_without_token_passes_without_validation(self, monkeypatch):
        # the IP check short-circuits before token validation, so a
        # whitelisted client is not forced to pay the validation cost
        calls = []

        def fake_validate(username, token):
            calls.append((username, token))
            return False

        monkeypatch.setattr("fastink.routers.headers.validate_token", fake_validate)
        client = _build_ip_app(WHITELIST, CONTROLLED, token_bypass=BY_PASS)
        resp = client.get(
            PERMISSION_URL, headers={"X-Real-IP": "192.168.51.96"}
        )
        assert resp.json()["status"] == InkStatus.SUCCESS
        assert calls == []

    def test_controlled_without_bypass_still_ip_checked(self, monkeypatch):
        # get_token is IP-controlled but NOT in the bypass list -> a valid
        # token does not lift the IP restriction (it returns credentials)
        monkeypatch.setattr(
            "fastink.routers.headers.validate_token", lambda username, token: True
        )
        client = _build_ip_app(WHITELIST, CONTROLLED, token_bypass=BY_PASS)
        resp = client.get(
            "/api/v2/auth/get_token",
            headers={"X-Real-IP": "8.8.8.8", **AUTH_HEADERS},
        )
        assert resp.json()["status"] == InkStatus.IP_BANNED

    def test_bypass_router_not_controlled_warns(self, caplog):
        # middleware __init__ runs when the stack is first built (first
        # request), so fire one inside the caplog context
        import logging

        client = _build_ip_app(
            WHITELIST, CONTROLLED, token_bypass=["/api/v2/auth/not_controlled"]
        )
        with caplog.at_level(logging.WARNING):
            client.get("/api/v2/auth/get_token")
        assert any("not IP-controlled" in r.message for r in caplog.records)


def _make_request(path, headers=None, query_string=b"", client_ip="8.8.8.8"):
    return Request(
        {
            "type": "http",
            "asgi": {"version": "3.0", "spec_version": "2.0"},
            "http_version": "1.1",
            "method": "GET",
            "scheme": "http",
            "path": path,
            "raw_path": path.encode(),
            "query_string": query_string,
            "root_path": "",
            "headers": [
                (name.lower().encode(), value.encode())
                for name, value in (headers or {}).items()
            ],
            "client": (client_ip, 1234),
            "server": ("testserver", 80),
        }
    )


def _run_middleware(middleware, request):
    async def exercise():
        event_loop_thread = threading.get_ident()

        async def call_next(_request):
            return JSONResponse(
                {"status": InkStatus.SUCCESS, "msg": "ok", "data": None}
            )

        return await middleware.dispatch(request, call_next), event_loop_thread

    return asyncio.run(exercise())


@pytest.mark.parametrize(
    ("valid", "expected_status"),
    [
        (True, InkStatus.SUCCESS),
        (False, InkStatus.USER_INVALID),
    ],
)
def test_user_validation_runs_token_validation_in_threadpool(
    monkeypatch, valid, expected_status
):
    validator_threads = []

    def fake_validate(username, token):
        validator_threads.append(threading.get_ident())
        assert (username, token) == ("alice", "valid-token")
        return valid

    monkeypatch.setattr("fastink.routers.headers.get_auth_mode", lambda: "legacy")
    monkeypatch.setattr("fastink.routers.headers.validate_token", fake_validate)
    middleware = UserValidationMiddleware(lambda scope: None)

    response, event_loop_thread = _run_middleware(
        middleware,
        _make_request(
            "/api/v2/auth/get_permission",
            headers={"Ink-Username": "alice", "Ink-Token": "valid-token"},
        ),
    )

    assert validator_threads
    assert validator_threads[0] != event_loop_thread
    assert response.status_code == 200
    assert json.loads(response.body)["status"] == expected_status
    assert json.loads(response.body)["data"] is None


@pytest.mark.parametrize(
    ("valid", "expected_status"),
    [
        (True, InkStatus.SUCCESS),
        (False, InkStatus.IP_BANNED),
    ],
)
def test_ip_token_bypass_runs_token_validation_in_threadpool(
    monkeypatch, valid, expected_status
):
    validator_threads = []

    def fake_validate(username, token):
        validator_threads.append(threading.get_ident())
        assert (username, token) == ("alice", "valid-token")
        return valid

    monkeypatch.setattr("fastink.routers.headers.validate_token", fake_validate)
    middleware = IPWhitelistMiddleware(
        lambda scope: None,
        ip_whitelist=["127.0.0.1"],
        forbidden_routers=["/api/v2/auth/get_permission"],
        token_bypass_routers=["/api/v2/auth/get_permission"],
    )

    response, event_loop_thread = _run_middleware(
        middleware,
        _make_request(
            "/api/v2/auth/get_permission",
            headers={"Ink-Username": "alice", "Ink-Token": "valid-token"},
            query_string=b"username=alice",
        ),
    )

    assert validator_threads
    assert validator_threads[0] != event_loop_thread
    assert response.status_code == 200
    assert json.loads(response.body)["status"] == expected_status
    assert json.loads(response.body)["data"] is None


class TestTestClientIP:
    def test_testclient_passes_ip_check(self):
        client = _build_ip_app(WHITELIST, CONTROLLED)
        resp = client.get("/api/v2/auth/get_token", headers={"X-Real-IP": "testclient"})
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
