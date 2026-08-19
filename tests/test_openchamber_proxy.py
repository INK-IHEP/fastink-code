import asyncio
from types import SimpleNamespace

from fastapi.testclient import TestClient

from fastink.computing.apps import _helpers as app_helpers
from fastink.computing.apps.openchamber import OpenChamberApp
from fastink.main import app


client = TestClient(app)


class _FakeWriter:
    def __init__(self):
        self.closed = False

    def close(self):
        self.closed = True

    async def wait_closed(self):
        return None


def test_openchamber_connect_url_contains_job_id(monkeypatch):
    async def fake_read_login_info(job_id, uid, cluster_id):
        assert (job_id, uid, cluster_id) == (12345, 1001, "htcondor")
        return "/jobs/openchamber-latest", (
            '{"HOST":"worker01","PORT":"61234","PASSWD":"secret"}'
        )

    monkeypatch.setattr(app_helpers, "read_login_info", fake_read_login_info)
    monkeypatch.setattr(app_helpers, "get_nginx_node", lambda: "https://ink.example")
    monkeypatch.setattr(
        "fastink.computing.apps.openchamber.get_config",
        lambda *args, **kwargs: 8446,
    )

    result = asyncio.run(
        OpenChamberApp().connect(job_id=12345, uid=1001, cluster_id="htcondor")
    )

    assert result.url == "https://ink.example:8446/?_ink_job_id=12345"


def test_openchamber_fallback_uses_latest_login_info(monkeypatch):
    async def fake_latest(**kwargs):
        assert kwargs == {
            "username": "alice",
            "uid": 1001,
            "job_type": "openchamber",
        }
        return "/jobs/openchamber-20260730-120000", (
            '{"HOST":"worker02","PORT":"62345","PASSWD":"new-secret"}'
        )

    writer = _FakeWriter()

    async def fake_open_connection(host, port):
        assert (host, port) == ("worker02", 62345)
        return object(), writer

    monkeypatch.setattr(app_helpers, "read_latest_job_login_info", fake_latest)
    monkeypatch.setattr("asyncio.open_connection", fake_open_connection)
    monkeypatch.setattr(app_helpers, "get_nginx_node", lambda: "https://ink.example")
    monkeypatch.setattr(
        "fastink.computing.apps.openchamber.get_config",
        lambda *args, **kwargs: 8446,
    )

    result = asyncio.run(
        OpenChamberApp().resolve_proxy_fallback(
            username="alice",
            uid=1001,
            cluster_id="htcondor",
        )
    )

    assert result.host == "worker02"
    assert result.port == "62345"
    assert result.passwd == "new-secret"
    assert writer.closed is True


def test_openchamber_fallback_rejects_unreachable_latest_job(monkeypatch):
    async def fake_latest(**_kwargs):
        return "/jobs/openchamber-20260730-120000", (
            '{"HOST":"worker02","PORT":"62345","PASSWD":"new-secret"}'
        )

    async def fake_open_connection(_host, _port):
        raise ConnectionRefusedError

    monkeypatch.setattr(app_helpers, "read_latest_job_login_info", fake_latest)
    monkeypatch.setattr("asyncio.open_connection", fake_open_connection)

    result = asyncio.run(
        OpenChamberApp().resolve_proxy_fallback(
            username="alice",
            uid=1001,
            cluster_id="htcondor",
        )
    )

    assert result is None


def test_latest_login_info_does_not_try_older_directory(monkeypatch):
    entries = [
        {"type": "directory", "path": "/jobs/openchamber-20260729-120000"},
        {"type": "directory", "path": "/jobs/openchamber-20260730-120000"},
        {"type": "directory", "path": "/jobs/openchamber-invalid"},
    ]

    async def fake_list_path(**kwargs):
        return entries

    async def fake_path_exist(**kwargs):
        assert kwargs["name"] == (
            "/jobs/openchamber-20260730-120000/app_login.info"
        )
        return False, None

    monkeypatch.setattr(app_helpers, "get_user_jobs_dir", lambda *_args: "/jobs")
    monkeypatch.setattr(app_helpers, "get_config", lambda *_args: "root://xrd")
    monkeypatch.setattr(app_helpers._storage_common, "list_path", fake_list_path)
    monkeypatch.setattr(app_helpers._storage_common, "path_exist", fake_path_exist)

    result = asyncio.run(
        app_helpers.read_latest_job_login_info(
            username="alice",
            uid=1001,
            job_type="openchamber",
        )
    )

    assert result is None


def test_latest_login_info_reads_newest_directory(monkeypatch):
    entries = [
        {"type": "directory", "path": "/jobs/openchamber-20260729-120000"},
        {"type": "file", "path": "/jobs/openchamber-20260731-120000"},
        {"type": "directory", "path": "/jobs/openchamber-20260730-120000"},
    ]

    async def fake_list_path(**kwargs):
        assert kwargs["dname"] == "/jobs"
        return entries

    async def fake_path_exist(**kwargs):
        assert kwargs["name"] == (
            "/jobs/openchamber-20260730-120000/app_login.info"
        )
        return True, "file"

    async def fake_read_file(uid, path):
        assert uid == 1001
        assert path == "/jobs/openchamber-20260730-120000/app_login.info"
        return '{"HOST":"worker02","PORT":"62345","PASSWD":"secret"}'

    monkeypatch.setattr(app_helpers, "get_user_jobs_dir", lambda *_args: "/jobs")
    monkeypatch.setattr(app_helpers, "get_config", lambda *_args: "root://xrd")
    monkeypatch.setattr(app_helpers._storage_common, "list_path", fake_list_path)
    monkeypatch.setattr(app_helpers._storage_common, "path_exist", fake_path_exist)
    monkeypatch.setattr(app_helpers, "read_file", fake_read_file)

    result = asyncio.run(
        app_helpers.read_latest_job_login_info(
            username="alice",
            uid=1001,
            job_type="openchamber",
        )
    )

    assert result == (
        "/jobs/openchamber-20260730-120000",
        '{"HOST":"worker02","PORT":"62345","PASSWD":"secret"}',
    )


def test_resolve_job_proxy_uses_app_fallback(monkeypatch):
    from fastink.routers.v2 import compute_resources

    class FakeScheduler:
        async def query_job(self, job_type):
            assert job_type == "openchamber"
            return []

    class FakeApp:
        async def resolve_proxy_fallback(self, **kwargs):
            assert kwargs == {
                "username": "alice",
                "uid": 1001,
                "cluster_id": "htcondor",
            }
            return SimpleNamespace(
                host="worker03",
                port="63456",
                passwd="fallback-secret",
            )

    monkeypatch.setattr(
        compute_resources.headers,
        "validate_token",
        lambda username, token: (username, token) == ("alice", "valid-token"),
    )
    monkeypatch.setattr(
        compute_resources,
        "change_username_to_uid",
        lambda username: 1001,
    )
    monkeypatch.setattr(
        compute_resources,
        "get_scheduler",
        lambda cluster_id, username: FakeScheduler(),
    )
    monkeypatch.setattr(
        compute_resources.computing_registry,
        "try_get",
        lambda job_type: FakeApp(),
    )

    response = client.get(
        "/api/v2/cr/resolve_job_proxy",
        headers={
            "Ink-Username": "alice",
            "Ink-Token": "valid-token",
        },
    )

    assert response.status_code == 200
    assert response.json() == {
        "host": "worker03",
        "port": "63456",
        "passwd": "fallback-secret",
    }


def test_resolve_job_proxy_prefers_running_job(monkeypatch):
    from fastink.routers.v2 import compute_resources

    class FakeScheduler:
        async def query_job(self, job_type):
            return [{
                "jobId": "45678",
                "jobStatus": "RUNNING",
                "connect_sign": "True",
            }]

    class FakeApp:
        async def connect(self, job_id, uid, cluster_id):
            assert (job_id, uid, cluster_id) == (45678, 1001, "htcondor")
            return SimpleNamespace(
                host="worker04",
                port="64567",
                passwd="running-secret",
            )

        async def resolve_proxy_fallback(self, **_kwargs):
            raise AssertionError("fallback must not run for a usable job")

    monkeypatch.setattr(
        compute_resources.headers,
        "validate_token",
        lambda username, token: (username, token) == ("alice", "valid-token"),
    )
    monkeypatch.setattr(
        compute_resources,
        "change_username_to_uid",
        lambda username: 1001,
    )
    monkeypatch.setattr(
        compute_resources,
        "get_scheduler",
        lambda cluster_id, username: FakeScheduler(),
    )
    monkeypatch.setattr(
        compute_resources.computing_registry,
        "try_get",
        lambda job_type: FakeApp(),
    )

    response = client.get(
        "/api/v2/cr/resolve_job_proxy",
        headers={
            "Ink-Username": "alice",
            "Ink-Token": "valid-token",
        },
    )

    assert response.status_code == 200
    assert response.json() == {
        "host": "worker04",
        "port": "64567",
        "passwd": "running-secret",
    }


def test_resolve_job_proxy_reports_fallback_storage_failure(monkeypatch):
    from fastink.routers.v2 import compute_resources

    class FakeScheduler:
        async def query_job(self, _job_type):
            return []

    class FakeApp:
        async def resolve_proxy_fallback(self, **_kwargs):
            raise RuntimeError("storage unavailable")

    monkeypatch.setattr(
        compute_resources.headers,
        "validate_token",
        lambda username, token: (username, token) == ("alice", "valid-token"),
    )
    monkeypatch.setattr(
        compute_resources,
        "change_username_to_uid",
        lambda username: 1001,
    )
    monkeypatch.setattr(
        compute_resources,
        "get_scheduler",
        lambda cluster_id, username: FakeScheduler(),
    )
    monkeypatch.setattr(
        compute_resources.computing_registry,
        "try_get",
        lambda job_type: FakeApp(),
    )

    response = client.get(
        "/api/v2/cr/resolve_job_proxy",
        headers={
            "Ink-Username": "alice",
            "Ink-Token": "valid-token",
        },
    )

    assert response.status_code == 500
