from unittest.mock import patch

from fastapi import FastAPI
from fastapi.testclient import TestClient
from sqlalchemy.exc import NoResultFound

from fastink.auth.principal import Principal
from fastink.routers.v3 import jobs as jobs_router
from fastink.routers.v3.middleware import BearerAuthMiddleware

SLURM_BODY = {
    "cluster_id": "slurm",
    "job_type": "batch",
    "mem": 1000,
    "partition": "cpu",
    "account": "test",
    "qos": "normal",
    "cpu": 1,
    "job_script": "#!/bin/bash\necho hi",
    "submit_mode": "sync",
}

HTC_BODY = {"cluster_id": "htcondor", "job_type": "batch", "mem": 1000}


def _principal(scopes=("jobs:read", "jobs:submit", "jobs:cancel")):
    return Principal(
        username="alice",
        issuer="session",
        subject="alice",
        auth_method="krb5",
        scopes=list(scopes),
    )


def _app():
    app = FastAPI()
    app.add_middleware(BearerAuthMiddleware, public_paths=[])
    app.include_router(jobs_router.router, prefix="/api/v3")
    return app


def _request(method, url, principal=None, **kwargs):
    async def fake_session(token):
        return principal

    with patch("fastink.routers.v3.middleware.validate_session", fake_session):
        return getattr(TestClient(_app()), method)(
            url, headers={"Authorization": "Bearer test"}, **kwargs
        )


class FakeScheduler:
    def __init__(self, submit_result=None, submit_error=None, cancel_result=None):
        self.submit_result = submit_result
        self.submit_error = submit_error
        self.cancel_result = cancel_result
        self.calls = []

    async def submit_job(self, job):
        self.calls.append(("submit", job))
        if self.submit_error:
            raise self.submit_error
        return self.submit_result

    async def cancel_job(self, *, job_id=None, submit_uuid=None):
        self.calls.append(("cancel", job_id, submit_uuid))
        return self.cancel_result

    async def query_job(self, job_type=None):
        self.calls.append(("query", job_type))
        return [{"jobId": "1", "jobType": "batch"}]


def _submit(body, fake):
    with patch.object(jobs_router, "get_scheduler", lambda *a, **k: fake):
        return _request("post", "/api/v3/jobs", _principal(), json=body)


def test_submit_sync_returns_job_id():
    fake = FakeScheduler(
        submit_result={"cluster": "slurm", "job_id": "123", "job_status": "SUBMITTED"}
    )
    response = _submit(SLURM_BODY, fake)
    assert response.status_code == 201
    assert response.json()["job_id"] == "123"
    assert response.json()["status"] == "SUBMITTED"


def test_submit_async_returns_submit_uuid():
    fake = FakeScheduler(
        submit_result={
            "cluster": "slurm",
            "submit_uuid": "u-1",
            "job_status": "SUBMITTING",
            "job_id": None,
        }
    )
    response = _submit(SLURM_BODY, fake)
    assert response.status_code == 202
    assert response.json()["submit_uuid"] == "u-1"
    assert response.json()["job_id"] is None


def test_submit_htc_async_returns_pending():
    fake = FakeScheduler(submit_result=None)
    response = _submit(HTC_BODY, fake)
    assert response.status_code == 202
    assert response.json()["status"] == "SUBMITTING"
    assert response.json()["job_id"] is None


def test_submit_htc_sync_returns_job_id():
    fake = FakeScheduler(submit_result=42)
    body = dict(HTC_BODY, submit_mode="sync")
    response = _submit(body, fake)
    assert response.status_code == 201
    assert response.json()["job_id"] == "42"


def test_submit_duplicate_returns_409():
    fake = FakeScheduler(
        submit_result={
            "cluster": "slurm",
            "job_status": "DUPLICATE",
            "error": "job_type 'batch' already running",
        }
    )
    response = _submit(SLURM_BODY, fake)
    assert response.status_code == 409
    assert "already running" in response.json()["detail"]


def test_submit_requires_scope():
    fake = FakeScheduler(submit_result=None)
    with patch.object(jobs_router, "get_scheduler", lambda *a, **k: fake):
        response = _request(
            "post", "/api/v3/jobs", _principal(scopes=["jobs:read"]), json=SLURM_BODY
        )
    assert response.status_code == 403


def test_submit_rejects_api_key():
    from fastink.auth.principal import Principal as P

    api_key = P(
        username="service:svc",
        issuer="apikey",
        subject="svc",
        auth_method="apikey",
        scopes=["jobs:submit"],
    )

    async def no_session(token):
        return None

    async def as_key(token):
        return api_key

    with patch("fastink.routers.v3.middleware.validate_session", no_session), patch(
        "fastink.routers.v3.middleware.validate_api_key", as_key
    ):
        response = TestClient(_app()).post(
            "/api/v3/jobs",
            json=SLURM_BODY,
            headers={"Authorization": "Bearer ink_test"},
        )
    assert response.status_code == 403


def test_submit_rejects_invalid_job_type():
    fake = FakeScheduler(submit_result=None)
    body = dict(SLURM_BODY, job_type="../escape")
    response = _submit(body, fake)
    assert response.status_code == 400
    assert fake.calls == []


def test_submit_rejects_script_path():
    fake = FakeScheduler(submit_result=None)
    body = dict(SLURM_BODY, script_path="/etc/shadow")
    response = _submit(body, fake)
    assert response.status_code == 400
    assert fake.calls == []


def test_cancel_by_job_id():
    fake = FakeScheduler(
        cancel_result={"cluster": "slurm", "job_id": "123", "job_status": "CANCELLED"}
    )
    with patch.object(jobs_router, "change_username_to_uid", lambda u: 21620), patch.object(
        jobs_router, "get_job_info", lambda *a, **k: (None, None, None, None)
    ), patch.object(jobs_router, "get_scheduler", lambda *a, **k: fake):
        response = _request(
            "post",
            "/api/v3/jobs/cancel",
            _principal(),
            json={"cluster_id": "slurm", "job_id": "123"},
        )
    assert response.status_code == 200
    assert response.json()["status"] == "CANCELLED"
    assert fake.calls == [("cancel", "123", None)]


def test_cancel_pending_by_submit_uuid():
    fake = FakeScheduler(cancel_result=None)
    with patch.object(jobs_router, "get_scheduler", lambda *a, **k: fake):
        response = _request(
            "post",
            "/api/v3/jobs/cancel",
            _principal(),
            json={"cluster_id": "slurm", "submit_uuid": "u-1"},
        )
    assert response.status_code == 200
    assert fake.calls == [("cancel", None, "u-1")]


def test_cancel_requires_identifier():
    response = _request(
        "post", "/api/v3/jobs/cancel", _principal(), json={"cluster_id": "slurm"}
    )
    assert response.status_code == 400


def test_cancel_htc_pending_is_rejected():
    response = _request(
        "post",
        "/api/v3/jobs/cancel",
        _principal(),
        json={"cluster_id": "htcondor", "submit_uuid": "u-1"},
    )
    assert response.status_code == 400


def test_submit_maps_adapter_errors():
    cases = [(ValueError, 400), (PermissionError, 403), (RuntimeError, 502)]
    for error, status in cases:
        fake = FakeScheduler(submit_error=error("boom"))
        response = _submit(SLURM_BODY, fake)
        assert response.status_code == status, error


def test_cancel_htc_by_job_id():
    fake = FakeScheduler(
        cancel_result={"cluster": "htcondor", "job_id": "123", "job_status": "CANCELLED"}
    )
    with patch.object(jobs_router, "change_username_to_uid", lambda u: 21620), patch.object(
        jobs_router, "get_job_info", lambda *a, **k: (None, None, None, None)
    ), patch.object(jobs_router, "get_scheduler", lambda *a, **k: fake):
        response = _request(
            "post",
            "/api/v3/jobs/cancel",
            _principal(),
            json={"cluster_id": "htcondor", "job_id": "123"},
        )
    assert response.status_code == 200
    assert fake.calls == [("cancel", "123", None)]


def test_cancel_rejects_both_identifiers():
    fake = FakeScheduler(cancel_result=None)
    with patch.object(jobs_router, "get_scheduler", lambda *a, **k: fake):
        response = _request(
            "post",
            "/api/v3/jobs/cancel",
            _principal(),
            json={"cluster_id": "slurm", "job_id": "123", "submit_uuid": "u-1"},
        )
    assert response.status_code == 400
    assert fake.calls == []


def test_cancel_rejects_invalid_job_id():
    response = _request(
        "post",
        "/api/v3/jobs/cancel",
        _principal(),
        json={"cluster_id": "slurm", "job_id": "123;rm"},
    )
    assert response.status_code == 400


def test_cancel_rejects_leading_dash():
    response = _request(
        "post",
        "/api/v3/jobs/cancel",
        _principal(),
        json={"cluster_id": "slurm", "job_id": "-a"},
    )
    assert response.status_code == 400


def test_cancel_rejects_unowned_job():
    fake = FakeScheduler()

    def not_owned(*args, **kwargs):
        raise NoResultFound("not yours")

    async def no_failed_record(*args, **kwargs):
        return None

    with patch.object(jobs_router, "change_username_to_uid", lambda u: 21620), patch.object(
        jobs_router, "get_job_info", not_owned
    ), patch.object(
        jobs_router, "get_failed_job", no_failed_record
    ), patch.object(jobs_router, "redis_connect", lambda: object()), patch.object(
        jobs_router, "get_scheduler", lambda *a, **k: fake
    ):
        response = _request(
            "post",
            "/api/v3/jobs/cancel",
            _principal(),
            json={"cluster_id": "slurm", "job_id": "999"},
        )

    assert response.status_code == 404
    assert fake.calls == []


def test_cancel_allows_owned_failed_submission():
    fake = FakeScheduler(cancel_result=None)

    def not_owned(*args, **kwargs):
        raise NoResultFound("failed submit")

    async def failed_record(*args, **kwargs):
        return {"failReason": "submit failed"}

    with patch.object(jobs_router, "change_username_to_uid", lambda u: 21620), patch.object(
        jobs_router, "get_job_info", not_owned
    ), patch.object(
        jobs_router, "get_failed_job", failed_record
    ), patch.object(jobs_router, "redis_connect", lambda: object()), patch.object(
        jobs_router, "get_scheduler", lambda *a, **k: fake
    ):
        response = _request(
            "post",
            "/api/v3/jobs/cancel",
            _principal(),
            json={"cluster_id": "slurm", "job_id": "1792464174000"},
        )

    assert response.status_code == 200
    assert fake.calls == [("cancel", "1792464174000", None)]


def test_cancel_requires_scope():
    response = _request(
        "post",
        "/api/v3/jobs/cancel",
        _principal(scopes=["jobs:read"]),
        json={"cluster_id": "slurm", "job_id": "123"},
    )
    assert response.status_code == 403


def test_cancel_rejects_api_key():
    api_key = Principal(
        username="service:svc",
        issuer="apikey",
        subject="svc",
        auth_method="apikey",
        scopes=["jobs:cancel"],
    )

    async def no_session(token):
        return None

    async def as_key(token):
        return api_key

    with patch("fastink.routers.v3.middleware.validate_session", no_session), patch(
        "fastink.routers.v3.middleware.validate_api_key", as_key
    ):
        response = TestClient(_app()).post(
            "/api/v3/jobs/cancel",
            json={"cluster_id": "slurm", "job_id": "123"},
            headers={"Authorization": "Bearer ink_test"},
        )
    assert response.status_code == 403


def test_list_jobs_calls_get_scheduler_with_cluster_and_user(monkeypatch):
    calls = []

    def fake_get(cluster_id, username):
        calls.append((cluster_id, username))
        return FakeScheduler()

    monkeypatch.setattr(jobs_router, "get_config", lambda *a, **k: ["slurm"])
    with patch.object(jobs_router, "get_scheduler", fake_get):
        response = _request("get", "/api/v3/jobs", _principal())
    assert response.status_code == 200
    assert calls == [("slurm", "alice")]


def test_output_dispatches_by_cluster(monkeypatch):
    calls = {}

    async def hpc_output(**kwargs):
        calls["hpc"] = kwargs
        return "slurm out", ""

    async def htc_output(**kwargs):
        calls["htc"] = kwargs
        return "htc out", "htc err"

    monkeypatch.setattr(jobs_router, "change_username_to_uid", lambda username: 21620)
    monkeypatch.setattr(jobs_router.hpc_check_job, "get_job_output", hpc_output)
    monkeypatch.setattr(jobs_router.htc_check_job, "get_job_output", htc_output)

    slurm = _request(
        "get",
        "/api/v3/jobs/123/output",
        _principal(),
        params={"cluster_id": "slurm"},
    )
    assert slurm.status_code == 200
    assert slurm.json()["output"] == "slurm out"
    assert calls["hpc"] == {"uid": 21620, "job_id": "123", "cluster_id": "slurm"}

    htc = _request(
        "get",
        "/api/v3/jobs/123/output",
        _principal(),
        params={"cluster_id": "htcondor"},
    )
    assert htc.status_code == 200
    assert htc.json()["error"] == "htc err"
    assert calls["htc"] == {"uid": 21620, "job_id": "123", "clusterid": "htcondor"}


def test_output_returns_failed_submission_reason(monkeypatch):
    async def failed_record(r, username, job_id):
        return {"failReason": "submission failed"}

    monkeypatch.setattr(jobs_router, "get_failed_job", failed_record)
    monkeypatch.setattr(jobs_router, "redis_connect", lambda: object())

    response = _request(
        "get",
        "/api/v3/jobs/1792464174000/output",
        _principal(),
        params={"cluster_id": "htcondor"},
    )

    assert response.status_code == 200
    assert response.json() == {
        "job_id": "1792464174000",
        "output": "",
        "error": "submission failed",
    }


def test_output_unknown_cluster(monkeypatch):
    monkeypatch.setattr(jobs_router, "change_username_to_uid", lambda username: 21620)
    response = _request(
        "get",
        "/api/v3/jobs/123/output",
        _principal(),
        params={"cluster_id": "unknown"},
    )
    assert response.status_code == 400


def test_output_requires_local_user(monkeypatch):
    def no_user(username):
        raise KeyError(username)

    monkeypatch.setattr(jobs_router, "change_username_to_uid", no_user)
    response = _request(
        "get",
        "/api/v3/jobs/123/output",
        _principal(),
        params={"cluster_id": "slurm"},
    )
    assert response.status_code == 400


def test_list_jobs_uses_get_scheduler(monkeypatch):
    fake = FakeScheduler()
    monkeypatch.setattr(jobs_router, "get_config", lambda *a, **k: ["slurm"])
    with patch.object(jobs_router, "get_scheduler", lambda *a, **k: fake):
        response = _request("get", "/api/v3/jobs", _principal())
    assert response.status_code == 200
    assert response.json()["total"] == 1
    assert fake.calls == [("query", None)]
