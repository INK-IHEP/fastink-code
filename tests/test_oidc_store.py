import json
import time
from unittest.mock import MagicMock, patch

import pytest

from fastink.auth.oidc.store import (
    approve_device,
    consume_device_code,
    consume_sso_pending,
    get_device_code,
    store_code,
    store_device_code,
    store_sso_pending,
)


@pytest.fixture
def mock_redis():
    store = {}
    client = MagicMock()

    def setex(key, ttl, value):
        try:
            parsed = json.loads(value)
        except (json.JSONDecodeError, TypeError):
            parsed = value
        store[key] = (parsed, time.time() + ttl)

    def set_(key, value, ex=None):
        try:
            parsed = json.loads(value)
        except (json.JSONDecodeError, TypeError):
            parsed = value
        store[key] = (parsed, time.time() + (ex or 3600))

    def get(key):
        entry = store.get(key)
        if entry is None:
            return None
        data, expires = entry
        if expires <= time.time():
            del store[key]
            return None
        if isinstance(data, str):
            return data
        return json.dumps(data)

    def delete(key):
        store.pop(key, None)

    def eval_(script, numkeys, *args):
        key = args[0]
        if len(args) == 2:
            now = None
            raw = args[1]
            entry = store.get(key)
            if entry is None:
                return 0
            data, expires = entry
            current = json.dumps(data) if not isinstance(data, str) else data
            if current != raw:
                return 0
            del store[key]
            return 1
        now = float(args[1])
        username = args[2]
        entry = store.get(key)
        if entry is None:
            return 0
        data, expires = entry
        if expires <= now:
            return 0
        data["username"] = username
        data["approved"] = True
        return 1

    client.setex = setex
    client.set = set_
    client.get = get
    client.delete = delete
    client.ttl = lambda key: max(0, int(store[key][1] - time.time())) if key in store else -2
    client.eval = eval_
    client.pipeline = lambda: _Pipeline(store, get, delete)
    return client


class _Pipeline:
    def __init__(self, store, get_fn, delete_fn):
        self._store = store
        self._get = get_fn
        self._delete = delete_fn
        self._ops = []

    def get(self, key):
        self._ops.append(("get", key))
        return self

    def delete(self, key):
        self._ops.append(("delete", key))
        return self

    def execute(self):
        results = []
        for op, key in self._ops:
            if op == "get":
                results.append(self._get(key))
            elif op == "delete":
                self._delete(key)
                results.append(1)
        return results


@pytest.fixture(autouse=True)
def _patch_redis(mock_redis):
    with patch("fastink.auth.oidc.store._redis", return_value=mock_redis):
        yield


def test_store_and_get_device_code():
    store_device_code("dc123", {"user_code": "ABC-DEF", "expires_at": time.time() + 600})
    data = get_device_code("dc123")
    assert data is not None
    assert data["user_code"] == "ABC-DEF"


def test_approve_device():
    store_device_code("dc123", {"user_code": "ABC-DEF", "expires_at": time.time() + 600})
    assert approve_device("ABC-DEF", "alice") is True
    data = get_device_code("dc123")
    assert data["approved"] is True
    assert data["username"] == "alice"


def test_approve_device_nonexistent():
    assert approve_device("NOPE", "alice") is False


def test_consume_device_code():
    store_device_code("dc123", {"user_code": "ABC", "expires_at": time.time() + 600})
    data = consume_device_code("dc123")
    assert data is not None
    assert consume_device_code("dc123") is None


def test_store_and_consume_sso_pending():
    store_sso_pending("pid123", {"client_id": "web", "state": "s123"})
    params = consume_sso_pending("pid123")
    assert params is not None
    assert params["client_id"] == "web"
    assert consume_sso_pending("pid123") is None


def test_consume_expired_sso_pending_returns_none():
    store_sso_pending("pid123", {"client_id": "web"}, ttl=0)
    time.sleep(0.01)
    assert consume_sso_pending("pid123") is None


def test_get_code_does_not_delete():
    store_code("abc123", {"client_id": "web", "username": "alice"})
    from fastink.auth.oidc.store import get_code, delete_code_if_matches
    result = get_code("abc123")
    assert result is not None
    data, raw = result
    assert data["username"] == "alice"
    assert get_code("abc123") is not None
    assert delete_code_if_matches("abc123", raw) is True
    assert get_code("abc123") is None


def test_delete_code_if_matches_rejects_stale_value():
    store_code("abc123", {"client_id": "web", "username": "alice"})
    from fastink.auth.oidc.store import get_code, delete_code_if_matches
    _, raw = get_code("abc123")
    store_code("abc123", {"client_id": "web", "username": "mallory"})
    assert delete_code_if_matches("abc123", raw) is False
    result = get_code("abc123")
    assert result[0]["username"] == "mallory"


def test_redis_outage_raises_runtime_error():
    import redis.exceptions
    from unittest.mock import patch as _patch
    from fastink.auth.oidc import store as store_mod

    broken = MagicMock()
    broken.get.side_effect = redis.exceptions.ConnectionError("redis down")
    broken.set.side_effect = redis.exceptions.ConnectionError("redis down")
    broken.eval.side_effect = redis.exceptions.ConnectionError("redis down")
    broken.pipeline.side_effect = redis.exceptions.ConnectionError("redis down")

    with _patch.object(store_mod, "_redis", return_value=broken):
        with pytest.raises(RuntimeError, match="store unavailable"):
            store_mod.store_code("x", {"a": 1})
        with pytest.raises(RuntimeError, match="store unavailable"):
            store_mod.get_code("x")
