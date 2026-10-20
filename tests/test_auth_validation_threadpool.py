import asyncio
import threading

import pytest

from fastink.routers import headers
from fastink.routers.status import InkStatus
from fastink.routers.v2 import auth_manager


def _validator_spy(calls, thread_ids, result):
    def validate(username, token):
        calls.append((username, token))
        thread_ids.append(threading.get_ident())
        return result

    return validate


@pytest.mark.parametrize("result", [True, False])
def test_validate_token_offloads_validation_and_preserves_envelope(
    monkeypatch, result
):
    calls = []
    thread_ids = []
    monkeypatch.setattr(
        headers,
        "validate_token",
        _validator_spy(calls, thread_ids, result),
    )

    response = asyncio.run(
        auth_manager.validate_token(username="alice", token="valid-token")
    )

    assert calls == [("alice", "valid-token")]
    assert thread_ids[0] != threading.get_ident()
    assert response == {
        "status": InkStatus.SUCCESS if result else InkStatus.TOKEN_INVALID,
        "msg": "Token validation successful" if result else "Token validation failed",
        "data": None,
    }


@pytest.mark.parametrize("result", [True, False])
def test_auth_request_offloads_validation_and_preserves_response(
    monkeypatch, result
):
    calls = []
    thread_ids = []
    monkeypatch.setattr(
        headers,
        "validate_token",
        _validator_spy(calls, thread_ids, result),
    )

    response = asyncio.run(
        auth_manager.auth_request(username="alice", token="valid-token")
    )

    assert calls == [("alice", "valid-token")]
    assert thread_ids[0] != threading.get_ident()
    if result:
        assert response.status_code == 200
        assert response.headers["X-Auth-Request-User"] == "alice"
        assert response.body == (
            b'{"status":"200","msg":"Token validation successful","data":null}'
        )
    else:
        assert response == {
            "status": InkStatus.TOKEN_INVALID,
            "msg": "Token validation failed",
            "data": None,
        }


@pytest.mark.parametrize(
    "endpoint",
    [auth_manager.validate_token, auth_manager.auth_request],
)
def test_auth_endpoints_skip_validation_when_headers_are_missing(monkeypatch, endpoint):
    calls = []
    monkeypatch.setattr(
        headers,
        "validate_token",
        _validator_spy(calls, [], True),
    )

    response = asyncio.run(endpoint(username=None, token=None))

    assert calls == []
    assert response["status"] == InkStatus.TOKEN_INVALID
