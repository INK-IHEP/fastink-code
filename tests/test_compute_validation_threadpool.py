import asyncio
import threading
from types import SimpleNamespace

import pytest
from fastapi import Request

from fastink.computing.job_prepare import PrepareJobRequest
from fastink.routers.status import InkStatus
from fastink.routers.v2 import compute_resources


ENDPOINTS = [
    "list_job_types",
    "prepare_job",
    "get_job_credentials",
    "resolve_job_proxy",
]


def _request(principal=None):
    request = Request(
        {
            "type": "http",
            "asgi": {"version": "3.0", "spec_version": "2.0"},
            "http_version": "1.1",
            "method": "GET",
            "scheme": "http",
            "path": "/api/v2/cr/test",
            "raw_path": b"/api/v2/cr/test",
            "query_string": b"",
            "root_path": "",
            "headers": [],
            "client": ("127.0.0.1", 1234),
            "server": ("testserver", 80),
        }
    )
    if principal is not None:
        request.state.principal = principal
    return request


def _call(endpoint_name, request, username, token):
    if endpoint_name == "list_job_types":
        return compute_resources.list_job_types(request, username, token)
    if endpoint_name == "prepare_job":
        return compute_resources.prepare_job(
            PrepareJobRequest(job_type="vscode", cluster_id="htcondor", overrides={}),
            request,
            username,
            token,
        )
    if endpoint_name == "get_job_credentials":
        return compute_resources.get_job_credentials(
            username, token, "/opencode/worker01/123456789/alice/"
        )
    return compute_resources.resolve_job_proxy(username, token, "openchamber")


def _patch_downstream(monkeypatch, calls):
    monkeypatch.setattr(
        compute_resources,
        "list_job_type_capabilities",
        lambda: calls.append("list") or {"vscode": {}},
    )
    monkeypatch.setattr(
        compute_resources,
        "prepare_job_request",
        lambda *args: calls.append("prepare") or {"job_type": args[0]},
    )
    monkeypatch.setattr(
        compute_resources, "change_username_to_uid", lambda _username: calls.append("uid") or 1001
    )

    class FakeApp:
        async def connect(self, **_kwargs):
            calls.append("connect")
            return SimpleNamespace(passwd="secret")

        def get_proxy_credentials(self, _result):
            calls.append("credentials")
            return "Basic secret"

        async def resolve_proxy_fallback(self, **_kwargs):
            calls.append("fallback")
            return SimpleNamespace(host="worker", port="1234", passwd="secret")

    class FakeScheduler:
        async def query_job(self, _job_type):
            calls.append("query")
            return []

    monkeypatch.setattr(compute_resources.computing_registry, "try_get", lambda _name: FakeApp())
    monkeypatch.setattr(compute_resources, "get_scheduler", lambda *_args: FakeScheduler())


def _assert_success(endpoint_name, response):
    if endpoint_name in {"list_job_types", "prepare_job"}:
        assert response["status"] == InkStatus.SUCCESS
    elif endpoint_name == "get_job_credentials":
        assert response.status_code == 204
    else:
        assert response.status_code == 200


@pytest.mark.parametrize("endpoint_name", ENDPOINTS)
def test_compute_validation_runs_in_threadpool(monkeypatch, endpoint_name):
    calls = []
    validator_threads = []
    monkeypatch.setattr(
        compute_resources.headers,
        "validate_token",
        lambda username, token: (
            calls.append((username, token)),
            validator_threads.append(threading.get_ident()),
            True,
        )[-1],
    )
    _patch_downstream(monkeypatch, calls)

    response = asyncio.run(
        _call(endpoint_name, _request(), "alice", "valid-token")
    )

    assert validator_threads[0] != threading.get_ident()
    _assert_success(endpoint_name, response)


@pytest.mark.parametrize("endpoint_name", ENDPOINTS)
def test_compute_validation_rejection_skips_downstream(monkeypatch, endpoint_name):
    calls = []
    validator_threads = []
    monkeypatch.setattr(
        compute_resources.headers,
        "validate_token",
        lambda username, token: (
            calls.append((username, token)),
            validator_threads.append(threading.get_ident()),
            False,
        )[-1],
    )
    _patch_downstream(monkeypatch, calls)

    response = asyncio.run(
        _call(endpoint_name, _request(), "alice", "invalid-token")
    )

    assert validator_threads[0] != threading.get_ident()
    assert calls == [("alice", "invalid-token")]
    if endpoint_name in {"list_job_types", "prepare_job"}:
        assert response["status"] == InkStatus.TOKEN_INVALID
    else:
        assert response.status_code == 401


@pytest.mark.parametrize("endpoint_name", ENDPOINTS)
def test_compute_missing_headers_reject_without_validation(monkeypatch, endpoint_name):
    calls = []
    monkeypatch.setattr(
        compute_resources.headers,
        "validate_token",
        lambda username, token: calls.append((username, token)) or True,
    )

    response = asyncio.run(_call(endpoint_name, _request(), None, None))

    assert calls == []
    if endpoint_name in {"list_job_types", "prepare_job"}:
        assert response["status"] == InkStatus.TOKEN_INVALID
    else:
        assert response.status_code == 401


@pytest.mark.parametrize("endpoint_name", ["list_job_types", "prepare_job"])
def test_compute_authenticated_principal_skips_validation(monkeypatch, endpoint_name):
    calls = []
    monkeypatch.setattr(
        compute_resources.headers,
        "validate_token",
        lambda _username, _token: calls.append(True) or False,
    )
    _patch_downstream(monkeypatch, calls)
    principal = SimpleNamespace(username="alice", auth_method="legacy")

    response = asyncio.run(
        _call(endpoint_name, _request(principal), "alice", None)
    )

    assert calls == (["list"] if endpoint_name == "list_job_types" else ["prepare"])
    _assert_success(endpoint_name, response)
