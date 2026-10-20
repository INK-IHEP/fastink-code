import asyncio
import base64
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import threading
import time
from types import SimpleNamespace

import pytest
from fastapi.responses import JSONResponse
from starlette.requests import Request

from fastink.auth.backends import krb5_validate_runtime as runtime
from fastink.auth.backends import krb5_validation
from fastink.auth.backends.ccache import parse_ccache_full
from fastink.auth.backends.registry import discover, get_auth_backend
from fastink.routers import headers
from fastink.routers.status import InkStatus
from tests.test_krb5_validation import (
    SERVICE_PRINCIPAL,
    _credential,
    build_forged_ccache,
)
from tests.test_krb5_validation_cache import FakeRedis


LIVE_USERNAME = os.getenv("FASTINK_LIVE_KRB5_USERNAME")
LIVE_TOKEN = os.getenv("FASTINK_LIVE_KRB5_TOKEN")
LIVE_KRB5_AVAILABLE = bool(
    os.getenv("FASTINK_LIVE_KDC") == "1"
    and LIVE_USERNAME
    and LIVE_TOKEN
    and shutil.which("kvno")
    and shutil.which("klist")
)


@pytest.fixture(autouse=True)
def reset_kvno_semaphore(monkeypatch):
    monkeypatch.setattr(runtime, "_kvno_semaphore", None)
    monkeypatch.setattr(runtime, "_kvno_semaphore_max", None)


def _settings(*, timeout=5, max_concurrency=8):
    return runtime.Krb5ValidateSettings(
        SERVICE_PRINCIPAL,
        "IHEPKRB5",
        600,
        5,
        timeout,
        max_concurrency,
    )


def _token(cache=None):
    return base64.b64encode(cache or build_forged_ccache()).decode()


def _service_target(settings):
    service, realm = settings.service_principal.rsplit("@", 1)
    return tuple(component.encode() for component in service.split("/")), realm.encode()


def _issue_target(cache, settings, *, endtime=None):
    parsed = parse_ccache_full(cache)
    server_components, server_realm = _service_target(settings)
    return cache + _credential(
        client_realm=parsed["default_principal"]["realm"],
        client_components=tuple(parsed["default_principal"]["components"]),
        server_realm=server_realm,
        server_components=server_components,
        endtime=endtime or int(time.time()) + 3600,
    )


def _configure(monkeypatch, cache, runner, settings=None):
    settings = settings or _settings()
    monkeypatch.setattr(krb5_validation, "load_krb5_validate_settings", lambda: settings)
    monkeypatch.setattr(krb5_validation, "run_kvno_validation", runner)
    monkeypatch.setattr(krb5_validation, "_redis", lambda: cache)
    discover()
    return settings


def _backend():
    discover()
    return get_auth_backend("krb5")


def _make_request(path, headers_map):
    return Request(
        {
            "type": "http",
            "asgi": {"version": "3.0", "spec_version": "2.0"},
            "http_version": "1.1",
            "method": "GET",
            "scheme": "http",
            "path": path,
            "raw_path": path.encode(),
            "query_string": b"",
            "root_path": "",
            "headers": [
                (name.lower().encode(), value.encode())
                for name, value in headers_map.items()
            ],
            "client": ("8.8.8.8", 1234),
            "server": ("testserver", 80),
        }
    )


def test_forged_token_rejected_through_registered_backend(monkeypatch):
    cache = FakeRedis()
    calls = []
    settings = _settings()

    def reject(cache_bytes, received_settings):
        calls.append((cache_bytes, received_settings))
        raise runtime.Krb5KdcRejection(1)

    _configure(monkeypatch, cache, reject, settings)

    assert _backend().validate_token(username="alice", token=_token()) is False
    assert len(calls) == 1
    assert calls[0][1] is settings


def test_registered_backend_requires_post_kvno_target_ticket(monkeypatch):
    cache = FakeRedis()
    settings = _configure(monkeypatch, cache, _issue_target)
    token = _token()

    assert _backend().validate_token(username="alice", token=token) is True

    missing_ticket_cache = FakeRedis()
    monkeypatch.setattr(krb5_validation, "_redis", lambda: missing_ticket_cache)
    monkeypatch.setattr(krb5_validation, "run_kvno_validation", lambda cache, _: cache)
    assert _backend().validate_token(username="alice", token=token) is False
    assert settings.service_principal == SERVICE_PRINCIPAL


def test_registered_backend_strips_preseeded_target_ticket(monkeypatch):
    settings = _settings()
    cache = FakeRedis()
    preseeded = build_forged_ccache(
        extra_credentials=_credential(
            endtime=int(time.time()) + 3600,
            ticket=b"forged-target-ticket",
        )
    )
    received = []

    def issue_target(cache_bytes, received_settings):
        received.append(cache_bytes)
        parsed = parse_ccache_full(cache_bytes)
        target, realm = _service_target(received_settings)
        assert all(
            credential["server"] != {"realm": realm, "components": list(target)}
            for credential in parsed["credentials"]
        )
        return _issue_target(cache_bytes, received_settings)

    _configure(monkeypatch, cache, issue_target, settings)

    assert _backend().validate_token(username="alice", token=_token(preseeded)) is True
    assert len(received) == 1


def test_identity_mismatch_does_not_poison_cache(monkeypatch):
    cache = FakeRedis(values={"sentinel": "unchanged"})
    settings = _configure(monkeypatch, cache, _issue_target)
    token = _token()
    before_values = dict(cache.values)
    before_writes = list(cache.writes)

    assert _backend().validate_token(username="mallory", token=token) is False
    assert cache.values == before_values
    assert cache.writes == before_writes
    assert cache.reads == []

    assert _backend().validate_token(username="alice", token=token) is True
    assert any(key.startswith("krb5val:v1:") for key in cache.values)
    assert settings.realm == "IHEPKRB5"


def test_user_validation_middleware_keeps_event_loop_responsive(monkeypatch):
    cache = FakeRedis()
    started = threading.Event()
    release = threading.Event()
    settings = _settings()

    def blocked_runner(cache_bytes, received_settings):
        started.set()
        assert release.wait(5)
        return _issue_target(cache_bytes, received_settings)

    _configure(monkeypatch, cache, blocked_runner, settings)
    monkeypatch.setattr(headers, "get_auth_mode", lambda: "legacy")
    monkeypatch.setattr(
        headers,
        "get_config",
        lambda section, option: "krb5" if option == "type" else None,
    )
    middleware = headers.UserValidationMiddleware(lambda scope: None)

    async def exercise():
        async def call_next(_request):
            return JSONResponse(
                {"status": InkStatus.SUCCESS, "msg": "ok", "data": None}
            )

        completed = False
        heartbeats = 0

        async def heartbeat():
            nonlocal heartbeats
            while not completed:
                heartbeats += 1
                await asyncio.sleep(0.01)

        request_task = asyncio.create_task(
            middleware.dispatch(
                _make_request(
                    "/api/v2/auth/get_permission",
                    {"Ink-Username": "alice", "Ink-Token": _token()},
                ),
                call_next,
            )
        )
        heartbeat_task = asyncio.create_task(heartbeat())
        try:
            deadline = asyncio.get_running_loop().time() + 5
            while not started.is_set():
                assert asyncio.get_running_loop().time() < deadline
                await asyncio.sleep(0.01)
            before = heartbeats
            await asyncio.sleep(0.05)
            assert heartbeats > before
            release.set()
            response = await asyncio.wait_for(request_task, 5)
            assert response.status_code == 200
            assert response.body
        finally:
            completed = True
            release.set()
            await asyncio.wait_for(heartbeat_task, 5)

    asyncio.run(exercise())


def test_cancellation_keeps_runtime_semaphore_until_runner_finishes(monkeypatch):
    settings = _settings(timeout=5, max_concurrency=1)
    cache = FakeRedis()
    _configure(monkeypatch, cache, runtime.run_kvno_validation, settings)
    semaphore = runtime.get_kvno_semaphore(settings)
    started = threading.Event()
    release = threading.Event()
    finished = threading.Event()

    def blocked_subprocess(_argv, **_kwargs):
        started.set()
        assert release.wait(5)
        finished.set()
        return SimpleNamespace(returncode=1)

    monkeypatch.setattr(subprocess, "run", blocked_subprocess)
    monkeypatch.setattr(headers, "get_auth_mode", lambda: "legacy")
    monkeypatch.setattr(
        headers,
        "get_config",
        lambda section, option: "krb5" if option == "type" else None,
    )
    middleware = headers.UserValidationMiddleware(lambda scope: None)

    async def exercise():
        async def call_next(_request):
            return JSONResponse(
                {"status": InkStatus.SUCCESS, "msg": "ok", "data": None}
            )

        task = asyncio.create_task(
            middleware.dispatch(
                _make_request(
                    "/api/v2/auth/get_permission",
                    {"Ink-Username": "alice", "Ink-Token": _token()},
                ),
                call_next,
            )
        )
        deadline = asyncio.get_running_loop().time() + 5
        while not started.is_set():
            assert asyncio.get_running_loop().time() < deadline
            await asyncio.sleep(0.01)

        task.cancel()
        await asyncio.sleep(0.05)
        assert semaphore._value == 0

        release.set()
        try:
            await asyncio.wait_for(task, 5)
        except asyncio.CancelledError:
            pass
        deadline = asyncio.get_running_loop().time() + 5
        while not finished.is_set() or semaphore._value != 1:
            assert asyncio.get_running_loop().time() < deadline
            await asyncio.sleep(0.01)
        assert semaphore._value == 1

    asyncio.run(exercise())


def test_runtime_tempdirs_removed_after_timeout_and_success(monkeypatch):
    settings = _settings(max_concurrency=1)
    created = []
    real_temporary_directory = tempfile.TemporaryDirectory

    def tracked_temporary_directory(*args, **kwargs):
        kwargs.setdefault("prefix", "fastink-krb5-validation-")
        directory = real_temporary_directory(*args, **kwargs)
        created.append(Path(directory.name))
        return directory

    monkeypatch.setattr(runtime.tempfile, "TemporaryDirectory", tracked_temporary_directory)
    outcome = "timeout"

    def fake_subprocess(argv, **kwargs):
        if outcome == "timeout":
            raise subprocess.TimeoutExpired(argv, kwargs["timeout"])
        return SimpleNamespace(returncode=0)

    monkeypatch.setattr(subprocess, "run", fake_subprocess)

    with pytest.raises(runtime.Krb5ValidateTimeout):
        runtime.run_kvno_validation(b"cache", settings)
    assert not created[-1].exists()

    outcome = "success"
    assert runtime.run_kvno_validation(b"cache", settings) == b"cache"
    assert not created[-1].exists()
    assert not list(Path(tempfile.gettempdir()).glob("fastink-krb5-validation-*"))


@pytest.mark.skipif(
    not LIVE_KRB5_AVAILABLE,
    reason="set FASTINK_LIVE_KDC=1 with FASTINK_LIVE_KRB5_USERNAME/TOKEN and kvno",
)
def test_live_valid_tgt_validates(monkeypatch):
    monkeypatch.setattr(krb5_validation, "_redis", lambda: FakeRedis())
    assert _backend().validate_token(username=LIVE_USERNAME, token=LIVE_TOKEN) is True


@pytest.mark.skipif(
    not LIVE_KRB5_AVAILABLE,
    reason="set FASTINK_LIVE_KDC=1 with FASTINK_LIVE_KRB5_USERNAME/TOKEN and kvno",
)
def test_live_forged_tgt_is_rejected(monkeypatch):
    monkeypatch.setattr(krb5_validation, "_redis", lambda: FakeRedis())
    forged = _token(build_forged_ccache(username=LIVE_USERNAME.encode()))
    assert _backend().validate_token(username=LIVE_USERNAME, token=forged) is False


@pytest.mark.skipif(
    not LIVE_KRB5_AVAILABLE,
    reason="set FASTINK_LIVE_KDC=1 with FASTINK_LIVE_KRB5_USERNAME/TOKEN and kvno",
)
def test_live_valid_tgt_fails_with_child_only_unreachable_kdc(monkeypatch, tmp_path):
    profile = tmp_path / "krb5-dead.conf"
    profile.write_text(
        "[libdefaults]\n"
        "  default_realm = IHEPKRB5\n"
        "  dns_lookup_kdc = false\n"
        "[realms]\n"
        "  IHEPKRB5 = {\n"
        "    kdc = 192.0.2.1\n"
        "  }\n"
    )
    real_run = subprocess.run

    def dead_kdc_run(argv, **kwargs):
        child_env = dict(kwargs["env"])
        child_env["KRB5_CONFIG"] = str(profile)
        kwargs["env"] = child_env
        return real_run(argv, **kwargs)

    monkeypatch.setattr(krb5_validation, "_redis", lambda: FakeRedis())
    monkeypatch.setattr(subprocess, "run", dead_kdc_run)
    assert _backend().validate_token(username=LIVE_USERNAME, token=LIVE_TOKEN) is False
