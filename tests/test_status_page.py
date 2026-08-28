"""Unit tests for the /status uptime page probe logic and API."""

import asyncio
import json
import time
from unittest import mock

from fastink.service.status_probe import (
    REDIS_KEY_PREFIX,
    META_KEY,
    _expand_path,
    _invalidate_token,
    _token_cache,
    judge_response,
)


class TestJudgeResponse:
    def test_three_part_success(self):
        assert judge_response(200, {"status": "200", "msg": "ok", "data": {}}) is True

    def test_three_part_success_enum_string(self):
        assert judge_response(200, {"status": "SUCCESS", "msg": "ok", "data": {}}) is True

    def test_three_part_business_error_is_down(self):
        # HTTP 200 but business error (FastINK convention: always HTTP 200)
        assert judge_response(200, {"status": "F03", "msg": "invalid path", "data": None}) is False

    def test_http_error_is_down(self):
        assert judge_response(500, {"status": "200"}) is False

    def test_health_style_response_without_envelope(self):
        # /health returns {"status": "ok"}? No -- it has a status key.
        # A raw JSON body without "status" key succeeds on HTTP 200.
        assert judge_response(200, {"version": "1.0"}) is True

    def test_health_endpoint_status_ok_string(self):
        # /health returns {"status": "ok"} which is an accepted success value.
        assert judge_response(200, {"status": "ok"}) is True

    def test_non_dict_body_is_down(self):
        assert judge_response(200, "not json object") is False


class TestExpandPath:
    def test_placeholders_expanded(self):
        with mock.patch("fastink.service.status_probe.get_config") as m:
            def fake(section, option=None, fallback=None):
                values = {
                    ("test", "username"): "probeuser",
                    ("status_page", "test_home"): "",
                    ("status_page", "probe_cluster"): "",
                }
                return values.get((section, option), fallback)
            m.side_effect = fake
            path = _expand_path("/api/v2/fs/list_path?path={test_home}&u={test_user}&c={cluster}")
        assert "{test_user}" not in path
        assert "probeuser" in path
        assert "/home/probeuser" in path
        assert "slurm" in path

    def test_no_placeholders_passthrough(self):
        with mock.patch("fastink.service.status_probe.get_config", return_value=""):
            assert _expand_path("/health") == "/health"


class TestTokenCache:
    def test_invalidate_clears_token(self):
        _token_cache["token"] = "sometoken"
        _invalidate_token()
        assert _token_cache["token"] is None


class TestStatusAPI:
    """Exercise the /api/v2/status/get_status aggregation logic with a fake redis."""

    def test_get_status_aggregates_points(self):
        from fastink.routers.v2 import status_manager

        now = int(time.time())
        points = [
            json.dumps({"t": now - 120, "ok": True, "ms": 10}),   # newest first (LPUSH order)
            json.dumps({"t": now - 180, "ok": False, "ms": 55}),
            json.dumps({"t": now - 240, "ok": None, "ms": 0}),
        ]

        class FakeRedis:
            async def hgetall(self, key):
                assert key == META_KEY
                return {
                    b"last_run": str(now).encode(),
                    b"probes": json.dumps([{"name": "health", "auth": "none"}]).encode(),
                }
            async def lrange(self, key, start, end):
                assert key == f"{REDIS_KEY_PREFIX}health"
                return [p.encode() for p in points]
            async def aclose(self):
                pass

        with mock.patch.object(status_manager, "redis_connect", return_value=FakeRedis()):
            resp = asyncio.run(status_manager.get_status(limit="1440"))

        assert resp["status"] == "200" or str(resp["status"]) in {"200", "SUCCESS"}
        data = resp["data"]
        assert data["prober_stale"] is False
        probe = data["probes"][0]
        assert probe["name"] == "health"
        # newest point (ok=True) determines current state
        assert probe["current"] == "up"
        # uptime counts only known points: 1 up of 2 known = 50%
        assert probe["uptime_percent"] == 50.0
        # points served oldest-first
        assert probe["points"][0]["ok"] is None
        assert probe["points"][-1]["ok"] is True

    def test_get_status_stale_prober(self):
        from fastink.routers.v2 import status_manager

        class FakeRedis:
            async def hgetall(self, key):
                return {b"last_run": b"100", b"probes": b"[]"}  # ancient
            async def lrange(self, key, start, end):
                return []
            async def aclose(self):
                pass

        with mock.patch.object(status_manager, "redis_connect", return_value=FakeRedis()):
            resp = asyncio.run(status_manager.get_status(limit="1440"))
        assert resp["data"]["prober_stale"] is True

    def test_get_status_redis_down_returns_error_envelope(self):
        from fastink.routers.v2 import status_manager

        def boom():
            raise ConnectionError("redis unreachable")

        with mock.patch.object(status_manager, "redis_connect", side_effect=boom):
            resp = asyncio.run(status_manager.get_status(limit="1440"))
        assert resp["data"] is None
        assert resp["status"] != "200"

    def test_status_page_returns_html(self):
        from fastink.routers.v2.status_manager import _STATUS_PAGE_HTML
        assert "<!DOCTYPE html>" in _STATUS_PAGE_HTML
        assert "/api/v2/status/get_status" in _STATUS_PAGE_HTML


class TestLimitSemantics:
    """limit truncates returned points but uptime is computed on full history."""

    def _fake_redis(self, now, n_points):
        points = [
            json.dumps({"t": now - i * 60, "ok": (i % 2 == 0), "ms": 5})
            for i in range(n_points)  # newest first; even offsets are up
        ]

        class FakeRedis:
            async def hgetall(self, key):
                return {
                    b"last_run": str(now).encode(),
                    b"probes": json.dumps([{"name": "health", "auth": "none"}]).encode(),
                }
            async def lrange(self, key, start, end):
                if end == -1:
                    return [p.encode() for p in points]
                return [p.encode() for p in points[: end + 1]]
            async def aclose(self):
                pass
        return FakeRedis()

    def test_limit_truncates_points_not_uptime(self):
        from fastink.routers.v2 import status_manager
        now = int(time.time())
        with mock.patch.object(status_manager, "redis_connect", return_value=self._fake_redis(now, 10)):
            resp = asyncio.run(status_manager.get_status(limit="4"))
        probe = resp["data"]["probes"][0]
        # only 4 points returned...
        assert len(probe["points"]) == 4
        # ...but uptime uses all 10 (5 of 10 up = 50%)
        assert probe["uptime_percent"] == 50.0
        # returned points are the newest ones (last of oldest-first list)
        assert probe["points"][-1]["t"] == now

    def test_limit_invalid_string_falls_back(self):
        from fastink.routers.v2 import status_manager
        now = int(time.time())
        with mock.patch.object(status_manager, "redis_connect", return_value=self._fake_redis(now, 3)):
            resp = asyncio.run(status_manager.get_status(limit="abc"))
        # falls back to 1440, envelope stays 200 (str Enum compares by value)
        assert resp["status"] == "200"
        assert len(resp["data"]["probes"][0]["points"]) == 3


class TestFsHealth:
    """Filesystem health checks for /api/v2/status/get_fs_health."""

    def test_check_empty_path_list_is_healthy(self):
        from fastink.routers.v2.status_manager import _check_fs_paths
        summary = _check_fs_paths([])
        assert summary["all_ok"] is True
        assert summary["paths"] == []

    def test_check_healthy_path(self, tmp_path):
        from fastink.routers.v2.status_manager import _check_fs_paths
        (tmp_path / "somefile").write_text("data")
        summary = _check_fs_paths([str(tmp_path)])
        assert summary["all_ok"] is True
        assert summary["paths"][0]["ok"] is True

    def test_check_missing_path(self, tmp_path):
        from fastink.routers.v2.status_manager import _check_fs_paths
        summary = _check_fs_paths([str(tmp_path / "nope")])
        assert summary["all_ok"] is False
        assert summary["paths"][0]["reason"] == "missing"

    def test_check_empty_dir_is_healthy(self, tmp_path):
        # Emptiness is intentionally not unhealthy (freshly provisioned mount).
        from fastink.routers.v2.status_manager import _check_fs_paths
        empty = tmp_path / "empty"
        empty.mkdir()
        summary = _check_fs_paths([str(empty)])
        assert summary["all_ok"] is True
        assert summary["paths"][0]["ok"] is True

    def test_check_file_not_dir(self, tmp_path):
        from fastink.routers.v2.status_manager import _check_fs_paths
        f = tmp_path / "afile"
        f.write_text("x")
        summary = _check_fs_paths([str(f)])
        assert summary["all_ok"] is False
        assert summary["paths"][0]["reason"] == "not a directory"

    def test_check_mixed_reports_all_failures(self, tmp_path):
        from fastink.routers.v2.status_manager import _check_fs_paths
        good = tmp_path / "good"
        good.mkdir()
        summary = _check_fs_paths([str(good), str(tmp_path / "missing")])
        assert summary["all_ok"] is False
        oks = {p["path"]: p["ok"] for p in summary["paths"]}
        assert oks[str(good)] is True
        assert oks[str(tmp_path / "missing")] is False

    def test_endpoint_all_healthy(self, tmp_path):
        from fastink.routers.v2 import status_manager
        from fastink.routers.status import InkStatus
        (tmp_path / "f").write_text("x")
        with mock.patch.object(status_manager, "get_config", return_value=[str(tmp_path)]):
            resp = asyncio.run(status_manager.get_fs_health())
        assert resp["status"] == InkStatus.SUCCESS
        # aggregate-only response: no internal paths leaked to anonymous callers
        assert resp["data"] == {"total": 1, "healthy": 1, "unhealthy": 0}
        assert "path" not in resp["data"]

    def test_endpoint_unhealthy_returns_error_envelope(self, tmp_path):
        from fastink.routers.v2 import status_manager
        from fastink.routers.status import InkStatus
        with mock.patch.object(status_manager, "get_config", return_value=[str(tmp_path / "gone")]):
            resp = asyncio.run(status_manager.get_fs_health())
        assert resp["status"] != InkStatus.SUCCESS
        assert resp["data"]["unhealthy"] == 1
        # failing path string must not be in the public response
        assert str(tmp_path / "gone") not in str(resp["data"])

    def test_endpoint_empty_config_is_healthy(self):
        from fastink.routers.v2 import status_manager
        from fastink.routers.status import InkStatus
        with mock.patch.object(status_manager, "get_config", return_value=[]):
            resp = asyncio.run(status_manager.get_fs_health())
        assert resp["status"] == InkStatus.SUCCESS
        assert resp["data"] == {"total": 0, "healthy": 0, "unhealthy": 0}

    def test_endpoint_non_list_config_is_error(self):
        from fastink.routers.v2 import status_manager
        from fastink.routers.status import InkStatus
        with mock.patch.object(status_manager, "get_config", return_value="/not/a/list"):
            resp = asyncio.run(status_manager.get_fs_health())
        assert resp["status"] == InkStatus.PARAM_ERROR
        assert resp["data"] is None

    def test_endpoint_none_config_is_error(self):
        # A misconfigured `null` must surface as an error, not be silently
        # coerced to an empty (healthy) list.
        from fastink.routers.v2 import status_manager
        from fastink.routers.status import InkStatus
        with mock.patch.object(status_manager, "get_config", return_value=None):
            resp = asyncio.run(status_manager.get_fs_health())
        assert resp["status"] == InkStatus.PARAM_ERROR
        assert resp["data"] is None

    def test_endpoint_relative_path_entry_is_error(self):
        # A relative or blank entry (config typo) must not silently check CWD.
        from fastink.routers.v2 import status_manager
        from fastink.routers.status import InkStatus
        with mock.patch.object(status_manager, "get_config", return_value=["relative/path", ""]):
            resp = asyncio.run(status_manager.get_fs_health())
        assert resp["status"] == InkStatus.PARAM_ERROR
        assert resp["data"] is None

    def test_endpoint_nul_byte_entry_is_error(self):
        # A NUL byte would raise ValueError deep in stat; reject at validation.
        from fastink.routers.v2 import status_manager
        from fastink.routers.status import InkStatus
        with mock.patch.object(status_manager, "get_config", return_value=["/mnt/\x00bad"]):
            resp = asyncio.run(status_manager.get_fs_health())
        assert resp["status"] == InkStatus.PARAM_ERROR
        assert resp["data"] is None

    def test_endpoint_unhealthy_uses_fs_status(self, tmp_path):
        # Unhealthy mounts return the filesystem status code, not INTERNAL_ERROR.
        from fastink.routers.v2 import status_manager
        from fastink.routers.status import InkStatus
        with mock.patch.object(status_manager, "get_config", return_value=[str(tmp_path / "gone")]):
            resp = asyncio.run(status_manager.get_fs_health())
        assert resp["status"] == InkStatus.FS_UNKNOWN_ERROR
        assert resp["data"]["unhealthy"] == 1
