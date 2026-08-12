import base64
from types import SimpleNamespace

from fastapi.testclient import TestClient

from fastink.main import app


client = TestClient(app)


def test_job_credentials(monkeypatch):
    from fastink.routers.v2 import compute_resources

    class FakeOpenCodeApp:
        async def connect(self, job_id, uid, cluster_id):
            assert job_id == 6789
            assert uid == 1234
            assert cluster_id == "htcondor"
            return SimpleNamespace(passwd="job-password")

        def get_proxy_credentials(self, result):
            expected = base64.b64encode(f"opencode:{result.passwd}".encode()).decode()
            return f"Basic {expected}"

    monkeypatch.setattr(
        compute_resources.headers,
        "validate_token",
        lambda u, t: (u, t) == ("alice", "valid-token"),
    )
    monkeypatch.setattr(
        compute_resources, "change_username_to_uid", lambda username: 1234
    )
    monkeypatch.setattr(
        compute_resources.computing_registry,
        "try_get",
        lambda name: FakeOpenCodeApp(),
    )

    response = client.get(
        "/api/v2/cr/get_job_credentials",
        headers={
            "Ink-Username": "alice",
            "Ink-Token": "valid-token",
            "X-Original-URI": "/opencode/worker01/123456789/alice/",
        },
    )

    expected = base64.b64encode(b"opencode:job-password").decode()
    assert response.status_code == 204
    assert response.headers["X-Job-Credentials"] == f"Basic {expected}"


def test_job_credentials_rejects_bypass_header(monkeypatch):
    from fastink.routers.v2 import compute_resources

    monkeypatch.setattr(compute_resources.headers, "validate_token", lambda _u, _t: False)

    response = client.get(
        "/api/v2/cr/get_job_credentials",
        headers={
            "Ink-Username": "alice",
            "X-Internal-Auth": "bypass",
            "X-Original-URI": "/opencode/worker01/123456789/alice/",
        },
    )

    assert response.status_code == 401
