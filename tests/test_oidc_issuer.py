import base64
import hashlib
import json
import time
from urllib.parse import parse_qs, urlencode, urlparse

import jwt
import pytest
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from fastapi import FastAPI
from fastapi.testclient import TestClient

from fastink.auth.oidc.router import router as oidc_router


ISSUER = "https://oidc.test.example"
AUDIENCE = "test-client"


@pytest.fixture
def client(monkeypatch):
    private_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    signing_key = private_key.private_bytes(
        serialization.Encoding.PEM,
        serialization.PrivateFormat.PKCS8,
        serialization.NoEncryption(),
    ).decode()
    settings = {
        "issuer": ISSUER,
        "audience": AUDIENCE,
        "algorithms": ["RS256"],
        "signing_key": signing_key,
        "jwks_ttl": 300,
        "leeway": 0,
        "allowed_redirect_uris": ["https://client.example/callback"],
        "clients": [
            {
                "client_id": AUDIENCE,
                "redirect_uris": ["https://client.example/callback"],
                "grants": ["authorization_code"],
                "scopes": ["openid", "profile"],
            },
            {
                "client_id": "test-cli",
                "redirect_uris": [],
                "grants": ["urn:ietf:params:oauth:grant-type:device_code"],
                "scopes": ["openid", "profile"],
            },
            {
                "client_id": "test-admin",
                "redirect_uris": ["https://admin.example/callback"],
                "grants": [
                    "authorization_code",
                    "urn:ietf:params:oauth:grant-type:device_code",
                ],
                "scopes": ["openid", "admin"],
            },
        ],
    }
    sso_settings = {
        "app_key": "app-key",
        "app_secret": "app-secret",
        "authorize_url": "https://sso.example/authorize",
        "token_url": "https://sso.example/token",
        "umt_api": "https://sso.example/umt",
        "redirect_uri": f"{ISSUER}/authorize/sso/callback",
    }

    from fastink.common import config

    def fake_get_config(section=None, option=None, fallback=None, **_kwargs):
        if section == "auth" and option == "oidc":
            return settings
        if section == "auth" and option == "mode":
            return "oidc"
        if section == "auth" and option == "sso":
            return sso_settings
        return fallback

    monkeypatch.setattr(config, "get_config", fake_get_config)
    app = FastAPI()
    app.include_router(oidc_router)
    return TestClient(app, base_url=ISSUER)


def _pkce_challenge(code_verifier):
    digest = hashlib.sha256(code_verifier.encode()).digest()
    return base64.urlsafe_b64encode(digest).rstrip(b"=").decode()


def _oidc_params(code_verifier="correct-verifier"):
    return {
        "client_id": AUDIENCE,
        "redirect_uri": "https://client.example/callback",
        "response_type": "code",
        "scope": "openid profile",
        "state": "state-123",
        "code_challenge": _pkce_challenge(code_verifier),
        "code_challenge_method": "S256",
    }


def _authorize(client, monkeypatch, code_verifier="correct-verifier"):
    params = _oidc_params(code_verifier)
    monkeypatch.setattr("fastink.auth.oidc.router.verify_login", lambda username, password: True)
    response = client.get("/authorize", params=params, follow_redirects=False)
    assert response.status_code == 200
    response = client.post(
        "/authorize",
        data={**params, "username": "alice", "password": "x"},
        follow_redirects=False,
    )
    assert response.status_code == 302
    query = parse_qs(urlparse(response.headers["location"]).query)
    return query["code"][0]


def test_authorize_serves_login_page(client):
    response = client.get("/authorize", params=_oidc_params())

    assert response.status_code == 200
    assert '<form method="post" action="/authorize">' in response.text
    assert 'name="username"' in response.text
    assert 'name="password"' in response.text
    assert 'name="redirect_uri" value="https://client.example/callback"' in response.text
    assert "/authorize/sso?" in response.text


def test_authorize_username_query_param_is_ignored(client):
    response = client.get("/authorize", params={**_oidc_params(), "username": "alice"})

    assert response.status_code == 200
    assert "code=" not in response.text


def test_authorize_still_rejects_bad_oidc_params_first(client):
    params = _oidc_params()
    del params["code_challenge"]
    response = client.get("/authorize", params=params)

    assert response.status_code == 400
    assert response.json()["error"] == "invalid_request"


def test_login_invalid_password_shows_generic_error(client, monkeypatch):
    monkeypatch.setattr("fastink.auth.oidc.router.verify_login", lambda username, password: False)
    response = client.post(
        "/authorize",
        data={**_oidc_params(), "username": "unknown", "password": "bad"},
        follow_redirects=False,
    )

    assert response.status_code == 200
    assert "Invalid username or password" in response.text
    assert "code=" not in response.text
    assert "Location" not in response.headers


def test_sso_login_redirects_to_upstream(client):
    response = client.get("/authorize/sso", params=_oidc_params(), follow_redirects=False)

    assert response.status_code == 302
    location = urlparse(response.headers["location"])
    query = parse_qs(location.query)
    assert location.scheme + "://" + location.netloc + location.path == "https://sso.example/authorize"
    assert query["client_id"] == ["app-key"]
    state = json.loads(base64.urlsafe_b64decode(query["state"][0] + "=="))
    assert {key: state[key] for key in _oidc_params()} == _oidc_params()
    assert state["pending_id"]


def test_sso_login_rejects_empty_upstream_settings(client, monkeypatch):
    from fastink.common import config

    def fake_get_config(section=None, option=None, fallback=None, **_kwargs):
        if section == "auth" and option == "oidc":
            return {"issuer": ISSUER, "audience": AUDIENCE, "allowed_redirect_uris": ["https://client.example/callback"]}
        if section == "auth" and option == "sso":
            return {"authorize_url": "https://sso.example/authorize", "app_key": "", "redirect_uri": ""}
        return fallback

    monkeypatch.setattr(config, "get_config", fake_get_config)
    response = client.get("/authorize/sso", params=_oidc_params(), follow_redirects=False)

    assert response.status_code == 400
    assert response.json()["error"] == "invalid_request"


def test_sso_login_sets_secure_pending_cookie(client):
    response = client.get("/authorize/sso", params=_oidc_params(), follow_redirects=False)

    assert "oidc_sso_pending=" in response.headers["set-cookie"]
    assert "Secure" in response.headers["set-cookie"]


def test_sso_callback_issues_code(client, monkeypatch):
    monkeypatch.setattr(
        "fastink.auth.oidc.router.complete_sso_code",
        lambda code: {"subject": "umt-issuer", "local_username": "afsaccount"},
    )
    start = client.get("/authorize/sso", params=_oidc_params(), follow_redirects=False)
    state = parse_qs(urlparse(start.headers["location"]).query)["state"][0]
    response = client.get(
        "/authorize/sso/callback",
        params={"code": "upstream-code", "state": state},
        follow_redirects=False,
    )

    assert response.status_code == 302
    query = parse_qs(urlparse(response.headers["location"]).query)
    assert query["code"]
    token = client.post(
        "/token",
        data={
            "grant_type": "authorization_code",
            "code": query["code"][0],
            "client_id": AUDIENCE,
            "redirect_uri": "https://client.example/callback",
            "code_verifier": "correct-verifier",
        },
    ).json()
    info = client.get(
        "/userinfo", headers={"Authorization": f"Bearer {token['access_token']}"}
    )
    assert info.json()["preferred_username"] == "afsaccount"


def test_sso_callback_requires_pending_cookie(client, monkeypatch):
    monkeypatch.setattr("fastink.auth.oidc.router.complete_sso_code", lambda code: {"subject": "s", "local_username": "alice"})
    state = base64.urlsafe_b64encode(json.dumps({"pending_id": "unknown"}).encode()).decode().rstrip("=")
    response = client.get("/authorize/sso/callback", params={"code": "code", "state": state}, follow_redirects=False)
    assert response.status_code == 400
    assert response.json()["error"] == "invalid_request"




def test_authorize_requires_pkce_s256(client):
    params = {
        "client_id": AUDIENCE,
        "redirect_uri": "https://client.example/callback",
        "response_type": "code",
        "scope": "openid",
    }
    assert client.get("/authorize", params=params).status_code == 400

    params.update(
        code_challenge=_pkce_challenge("verifier"),
        code_challenge_method="S256",
    )
    response = client.get("/authorize", params=params, follow_redirects=False)
    assert response.status_code == 200


def test_authorize_rejects_unregistered_redirect_uri(client):
    params = {
        "client_id": AUDIENCE,
        "redirect_uri": "https://evil.example/callback",
        "response_type": "code",
        "scope": "openid",
        "code_challenge": _pkce_challenge("verifier"),
        "code_challenge_method": "S256",
    }
    response = client.get("/authorize", params=params)

    assert response.status_code == 400
    assert response.json()["error"] == "invalid_request"


def test_token_exchange_pkce_roundtrip(client, monkeypatch):
    verifier = "roundtrip-verifier"
    code = _authorize(client, monkeypatch, verifier)
    response = client.post(
        "/token",
        data={
            "grant_type": "authorization_code",
            "code": code,
            "client_id": AUDIENCE,
            "redirect_uri": "https://client.example/callback",
            "code_verifier": verifier,
        },
    )

    assert response.status_code == 200
    tokens = response.json()
    assert tokens["access_token"]
    assert tokens["token_type"] == "Bearer"
    assert set(tokens["scope"].split()) == {"openid", "profile"}
    assert "id_token" not in tokens

    info = client.get(
        "/userinfo", headers={"Authorization": f"Bearer {tokens['access_token']}"}
    )
    assert info.status_code == 200
    assert info.json()["preferred_username"] == "alice"

def test_token_rejects_wrong_verifier(client, monkeypatch):
    code = _authorize(client, monkeypatch, "correct-verifier")
    response = client.post(
        "/token",
        data={
            "grant_type": "authorization_code",
            "code": code,
            "client_id": AUDIENCE,
            "redirect_uri": "https://client.example/callback",
            "code_verifier": "wrong-verifier",
        },
    )
    assert response.status_code == 400
    assert response.json()["error"] == "invalid_grant"


def test_failed_exchange_does_not_burn_code(client, monkeypatch):
    code = _authorize(client, monkeypatch, "correct-verifier")
    bad = client.post(
        "/token",
        data={
            "grant_type": "authorization_code",
            "code": code,
            "client_id": AUDIENCE,
            "redirect_uri": "https://client.example/callback",
            "code_verifier": "wrong-verifier",
        },
    )
    assert bad.status_code == 400
    good = client.post(
        "/token",
        data={
            "grant_type": "authorization_code",
            "code": code,
            "client_id": AUDIENCE,
            "redirect_uri": "https://client.example/callback",
            "code_verifier": "correct-verifier",
        },
    )
    assert good.status_code == 200
    assert good.json()["access_token"]


def test_device_poll_before_approval_does_not_burn_code(client, monkeypatch):
    clock = [time.time()]
    monkeypatch.setattr("fastink.auth.oidc.store.time.time", lambda: clock[0])
    created = client.post("/device_authorization", data={"client_id": "test-cli"}).json()
    pending = client.post("/token", data={
        "grant_type": "urn:ietf:params:oauth:grant-type:device_code",
        "device_code": created["device_code"],
        "client_id": "test-cli",
    })
    assert pending.status_code == 400
    assert pending.json()["error"] == "authorization_pending"
    monkeypatch.setattr("fastink.auth.oidc.router.verify_login", lambda u, p: True)
    client.post("/device", data={"user_code": created["user_code"], "username": "alice", "password": "x"})
    clock[0] += 6
    token = client.post("/token", data={
        "grant_type": "urn:ietf:params:oauth:grant-type:device_code",
        "device_code": created["device_code"],
        "client_id": "test-cli",
    })
    assert token.status_code == 200
    assert token.json()["access_token"]


def test_authorize_rate_limited(client, monkeypatch):
    calls = []
    monkeypatch.setattr(
        "fastink.auth.oidc.router.ratelimit.reserve",
        lambda ip, user: calls.append(user) or 30,
    )
    response = client.post(
        "/authorize",
        data={
            "client_id": AUDIENCE,
            "redirect_uri": "https://client.example/callback",
            "response_type": "code",
            "scope": "openid",
            "code_challenge": "x" * 43,
            "code_challenge_method": "S256",
            "username": " alice ",
            "password": "x",
        },
    )
    assert response.status_code == 429
    assert calls == ["alice"]
    assert "Too many failed attempts" in response.text


def test_authorize_backend_error_returns_503_and_refunds(client, monkeypatch):
    calls = []
    monkeypatch.setattr(
        "fastink.auth.oidc.router.ratelimit.reserve", lambda ip, user: None
    )
    monkeypatch.setattr(
        "fastink.auth.oidc.router.ratelimit.refund",
        lambda ip, user: calls.append((ip, user)),
    )

    def boom(username, password):
        raise Exception("database down")

    monkeypatch.setattr("fastink.auth.oidc.router.verify_login", boom)
    response = client.post(
        "/authorize",
        data={
            "client_id": AUDIENCE,
            "redirect_uri": "https://client.example/callback",
            "response_type": "code",
            "scope": "openid",
            "code_challenge": "x" * 43,
            "code_challenge_method": "S256",
            "username": "alice",
            "password": "x",
        },
    )
    assert response.status_code == 503
    assert calls


def test_device_rate_limited(client, monkeypatch):
    created = client.post(
        "/device_authorization", data={"client_id": "test-cli"}
    ).json()
    monkeypatch.setattr(
        "fastink.auth.oidc.router.ratelimit.reserve", lambda ip, user: 30
    )
    response = client.post(
        "/device",
        data={
            "user_code": created["user_code"],
            "username": "alice",
            "password": "x",
        },
    )
    assert response.status_code == 429
    assert "Too many failed attempts" in response.text


def test_device_poll_too_fast_returns_slow_down(client):
    created = client.post("/device_authorization", data={"client_id": "test-cli"}).json()
    first = _poll_device(client, created["device_code"])
    assert first.status_code == 400
    assert first.json()["error"] == "authorization_pending"
    second = _poll_device(client, created["device_code"])
    assert second.status_code == 400
    assert second.json()["error"] == "slow_down"


def test_device_approval_requires_authenticated_login(client, monkeypatch):
    created = client.post("/device_authorization", data={"client_id": "test-cli"}).json()
    assert client.post("/device/approve", data={"user_code": created["user_code"], "username": "alice"}).status_code == 405
    assert client.get("/device", params={"user_code": created["user_code"]}).status_code == 200
    monkeypatch.setattr("fastink.auth.oidc.router.verify_login", lambda username, password: True)
    response = client.post("/device", data={"user_code": created["user_code"], "username": "alice", "password": "x"})
    assert response.status_code == 200
    token = client.post("/token", data={"grant_type": "urn:ietf:params:oauth:grant-type:device_code", "device_code": created["device_code"], "client_id": "test-cli"}).json()
    info = client.get(
        "/userinfo", headers={"Authorization": f"Bearer {token['access_token']}"}
    )
    assert info.json()["preferred_username"] == "alice"


def test_token_response_is_not_cacheable(client, monkeypatch):
    code = _authorize(client, monkeypatch)
    response = client.post("/token", data={"grant_type": "authorization_code", "code": code, "client_id": AUDIENCE, "redirect_uri": "https://client.example/callback", "code_verifier": "correct-verifier"})
    assert response.headers["cache-control"] == "no-store"
    assert response.headers["pragma"] == "no-cache"



def test_authorize_calls_ensure_credential(client, monkeypatch):
    calls = []
    monkeypatch.setattr(
        "fastink.auth.oidc.flows.get_krb5",
        lambda username: calls.append(username),
    )
    _authorize(client, monkeypatch)
    assert calls == ["alice"]


def test_authorize_credential_failure_does_not_block_login(client, monkeypatch):
    def fail(username):
        raise RuntimeError("torch unavailable")
    monkeypatch.setattr("fastink.auth.oidc.flows.get_krb5", fail)
    code = _authorize(client, monkeypatch)
    assert code  # login succeeded despite credential failure


def test_client_registration_rejects_unknown_client(client, monkeypatch):
    params = _oidc_params()
    params["client_id"] = "unknown-client"
    response = client.get("/authorize", params=params)
    assert response.status_code == 400
    assert response.json()["error"] == "unauthorized_client"


def test_device_code_rejected_for_web_only_client(client, monkeypatch):
    response = client.post(
        "/device_authorization",
        data={"client_id": AUDIENCE, "scope": "openid"},
    )
    assert response.status_code == 400
    assert "unauthorized" in response.json()["error"]


def test_client_empty_redirect_uris_falls_back_to_top_level(client, monkeypatch):
    from fastink.auth.oidc import flows
    settings = flows.oidc_settings()
    settings["clients"] = [
        {"client_id": AUDIENCE, "redirect_uris": [], "grants": ["authorization_code"]},
    ]
    monkeypatch.setattr(flows, "oidc_settings", lambda: settings)
    flows.validate_authorization_request(
        client_id=AUDIENCE,
        redirect_uri="https://client.example/callback",
        response_type="code",
        code_challenge="x",
        code_challenge_method="S256",
    )


def test_authorize_rejects_scope_outside_client_allowlist(client, monkeypatch):
    params = {**_oidc_params(), "scope": "openid admin"}
    response = client.get("/authorize", params=params)
    assert response.status_code == 400
    assert response.json()["error"] == "invalid_scope"


def test_token_scope_is_the_requested_subset(client, monkeypatch):
    params = {**_oidc_params(), "scope": "openid"}
    monkeypatch.setattr(
        "fastink.auth.oidc.router.verify_login", lambda username, password: True
    )
    response = client.post(
        "/authorize",
        data={**params, "username": "alice", "password": "x"},
        follow_redirects=False,
    )
    code = parse_qs(urlparse(response.headers["location"]).query)["code"][0]
    tokens = client.post(
        "/token",
        data={
            "grant_type": "authorization_code",
            "code": code,
            "client_id": AUDIENCE,
            "redirect_uri": params["redirect_uri"],
            "code_verifier": "correct-verifier",
        },
    ).json()
    assert tokens["scope"] == "openid"


def _exchange_code(client, code, client_id=AUDIENCE, verifier="correct-verifier"):
    return client.post(
        "/token",
        data={
            "grant_type": "authorization_code",
            "code": code,
            "client_id": client_id,
            "redirect_uri": "https://client.example/callback",
            "code_verifier": verifier,
        },
    )


def _approve_device(client, monkeypatch, scope="openid"):
    created = client.post(
        "/device_authorization", data={"client_id": "test-cli", "scope": scope}
    ).json()
    monkeypatch.setattr(
        "fastink.auth.oidc.router.verify_login", lambda username, password: True
    )
    client.post(
        "/device",
        data={"user_code": created["user_code"], "username": "alice", "password": "x"},
    )
    return created


def _poll_device(client, device_code, client_id="test-cli"):
    return client.post(
        "/token",
        data={
            "grant_type": "urn:ietf:params:oauth:grant-type:device_code",
            "device_code": device_code,
            "client_id": client_id,
        },
    )


def test_code_redeemed_by_wrong_client_then_correct_then_replay(client, monkeypatch):
    code = _authorize(client, monkeypatch)
    wrong = _exchange_code(client, code, client_id="test-cli")
    assert wrong.status_code == 400
    assert wrong.json()["error"] == "invalid_grant"

    good = _exchange_code(client, code)
    assert good.status_code == 200
    replay = _exchange_code(client, code)
    assert replay.status_code == 400
    assert replay.json()["error"] == "invalid_grant"


def test_device_code_redeemed_by_wrong_client_then_correct_then_replay(
    client, monkeypatch
):
    clock = [time.time()]
    monkeypatch.setattr("fastink.auth.oidc.store.time.time", lambda: clock[0])
    created = _approve_device(client, monkeypatch)

    wrong = _poll_device(client, created["device_code"], client_id=AUDIENCE)
    assert wrong.status_code == 400
    assert wrong.json()["error"] == "invalid_grant"

    clock[0] += 6
    good = _poll_device(client, created["device_code"])
    assert good.status_code == 200
    clock[0] += 6
    replay = _poll_device(client, created["device_code"])
    assert replay.status_code == 400
    assert replay.json()["error"] == "invalid_grant"


def test_device_authorization_rejects_scope_outside_client(client):
    response = client.post(
        "/device_authorization",
        data={"client_id": "test-cli", "scope": "openid admin"},
    )
    assert response.status_code == 400
    assert response.json()["error"] == "invalid_scope"


def _admin_code_params():
    return {
        "client_id": "test-admin",
        "redirect_uri": "https://admin.example/callback",
        "response_type": "code",
        "scope": "openid admin",
        "state": "s",
        "code_challenge": _pkce_challenge("correct-verifier"),
        "code_challenge_method": "S256",
    }


def _exchange_admin_code(client, monkeypatch, is_admin):
    import fastink.auth.permission as permission

    monkeypatch.setattr(
        permission, "check_user_permission", lambda **kw: is_admin
    )
    monkeypatch.setattr(
        "fastink.auth.oidc.router.verify_login", lambda username, password: True
    )
    params = _admin_code_params()
    response = client.post(
        "/authorize",
        data={**params, "username": "alice", "password": "x"},
        follow_redirects=False,
    )
    code = parse_qs(urlparse(response.headers["location"]).query)["code"][0]
    return client.post(
        "/token",
        data={
            "grant_type": "authorization_code",
            "code": code,
            "client_id": "test-admin",
            "redirect_uri": params["redirect_uri"],
            "code_verifier": "correct-verifier",
        },
    ).json()


def test_oauth_admin_scope_requires_user_permission(client, monkeypatch):
    import asyncio
    from fastink.auth.session import validate_session

    tokens = _exchange_admin_code(client, monkeypatch, is_admin=False)
    principal = asyncio.run(validate_session(tokens["access_token"]))
    assert principal.scopes == ["openid"]

    tokens = _exchange_admin_code(client, monkeypatch, is_admin=True)
    principal = asyncio.run(validate_session(tokens["access_token"]))
    assert set(principal.scopes) == {"openid", "admin"}


def test_device_admin_scope_requires_user_permission(client, monkeypatch):
    import asyncio
    import fastink.auth.permission as permission
    from fastink.auth.session import validate_session

    monkeypatch.setattr(permission, "check_user_permission", lambda **kw: False)
    created = client.post(
        "/device_authorization",
        data={"client_id": "test-admin", "scope": "openid admin"},
    ).json()
    monkeypatch.setattr(
        "fastink.auth.oidc.router.verify_login", lambda username, password: True
    )
    client.post(
        "/device",
        data={"user_code": created["user_code"], "username": "alice", "password": "x"},
    )
    tokens = _poll_device(client, created["device_code"], client_id="test-admin").json()
    principal = asyncio.run(validate_session(tokens["access_token"]))
    assert principal.scopes == ["openid"]


def test_empty_scope_defaults_to_openid(client, monkeypatch):
    params = {**_oidc_params(), "scope": ""}
    monkeypatch.setattr(
        "fastink.auth.oidc.router.verify_login", lambda username, password: True
    )
    response = client.post(
        "/authorize",
        data={**params, "username": "alice", "password": "x"},
        follow_redirects=False,
    )
    code = parse_qs(urlparse(response.headers["location"]).query)["code"][0]
    tokens = client.post(
        "/token",
        data={
            "grant_type": "authorization_code",
            "code": code,
            "client_id": AUDIENCE,
            "redirect_uri": params["redirect_uri"],
            "code_verifier": "correct-verifier",
        },
    ).json()
    assert tokens["scope"] == "openid"


def test_device_page_shows_client_and_scope(client):
    created = client.post(
        "/device_authorization",
        data={"client_id": "test-cli", "scope": "openid"},
    ).json()
    page = client.get("/device", params={"user_code": created["user_code"]}).text
    assert "test-cli" in page
    assert "openid" in page
