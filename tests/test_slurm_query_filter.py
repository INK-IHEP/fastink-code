import asyncio
import json


class FakeRedis:
    async def exists(self, _key):
        return False

    async def get(self, _key):
        return None

    async def hget(self, _key, _field):
        return None

    async def lrange(self, _key, _start, _end):
        return [
            json.dumps(
                {
                    "job_type": "jupyter",
                    "submit_uuid": "submit-jupyter",
                    "async_submit_time": "2026-08-25 10:00:00",
                }
            ),
            json.dumps(
                {
                    "job_type": "common",
                    "submit_uuid": "submit-common",
                    "async_submit_time": "2026-08-25 10:00:30",
                }
            ),
            json.dumps(
                {
                    "job_type": "batch",
                    "submit_uuid": "submit-batch",
                    "async_submit_time": "2026-08-25 10:01:00",
                }
            ),
        ]


def _scheduler(monkeypatch):
    from fastink.computing.adapter import hpcadapter

    scheduler = hpcadapter.HPC_Scheduler.__new__(hpcadapter.HPC_Scheduler)
    scheduler.UID = 1234
    scheduler.USERNAME = "alice"
    scheduler.CLUSTER_TYPE = "slurm"
    monkeypatch.setattr(
        scheduler,
        "_get_interactive_job_types",
        lambda: ["jupyter", "vscode"],
    )
    return hpcadapter, scheduler


def test_slurm_query_job_type_filter_helpers(monkeypatch):
    _, scheduler = _scheduler(monkeypatch)
    returnable = {"common", "jupyter", "vscode"}

    assert scheduler._requested_job_types(None) is None
    assert scheduler._requested_job_types("all") is None
    assert scheduler._requested_job_types(" jupyter, vscode ") == {
        "jupyter",
        "vscode",
    }

    assert scheduler._get_returnable_job_types() == returnable
    assert scheduler._should_return_slurm_job_type("jupyter", None, returnable)
    assert scheduler._should_return_slurm_job_type("common", None, returnable)
    assert scheduler._should_return_slurm_job_type("jupyter", {"jupyter"}, returnable)
    assert not scheduler._should_return_slurm_job_type("", None, returnable)
    assert not scheduler._should_return_slurm_job_type("batch", None, returnable)
    assert not scheduler._should_return_slurm_job_type("vscode", {"jupyter"}, returnable)


def test_slurm_query_skips_non_interactive_jobs_before_db_sync(monkeypatch):
    hpcadapter, scheduler = _scheduler(monkeypatch)
    redis = FakeRedis()
    db_lookup_job_ids = []
    inserted = []

    sacct_output = "\n".join(
        [
            "101|cpu|PENDING|00:00|1|node01|jupyter|2026-08-25T10:00:00|Unknown|Unknown|/work/jupyter|24:00:00|sbatch --output=/work/jupyter/%j.out --error=/work/jupyter/%j.err",
            "102|cpu|RUNNING|00:01|1|node02||2026-08-25T10:01:00|2026-08-25T10:02:00|Unknown|/work/terminal-empty|24:00:00|sbatch terminal.sh",
            "103|cpu|RUNNING|00:01|1|node03|batch|2026-08-25T10:03:00|2026-08-25T10:04:00|Unknown|/work/terminal-batch|24:00:00|sbatch batch.sh",
            "104|cpu|PENDING|00:00|1|node04|common|2026-08-25T10:05:00|Unknown|Unknown|/work/common|24:00:00|sbatch --output=/work/common/%j.out --error=/work/common/%j.err",
        ]
    )

    async def fake_sub_command(*_args, **_kwargs):
        return sacct_output.encode()

    async def fake_list_failed_jobs(_redis, _username):
        return [
            {"clusterId": "slurm", "jobType": "jupyter", "failId": "failed-jupyter"},
            {"clusterId": "slurm", "jobType": "common", "failId": "failed-common"},
            {"clusterId": "slurm", "jobType": "batch", "failId": "failed-batch"},
        ]

    def fake_failed_job_query_entry(_cluster_id, record):
        return {
            "clusterId": "slurm",
            "jobId": record["failId"],
            "jobType": record["jobType"],
            "jobStatus": "FAILED",
        }

    def fake_insert_job_info(*args, **_kwargs):
        inserted.append(args)

    def fake_get_job_info(_uid, job_id, _cluster_id):
        db_lookup_job_ids.append(job_id)
        return ("common" if job_id == "104" else "jupyter", "QUEUEING", 0, 0)

    monkeypatch.setattr(hpcadapter, "redis_connect", lambda: redis)
    monkeypatch.setattr(hpcadapter, "sub_command", fake_sub_command)
    monkeypatch.setattr(hpcadapter, "get_job_info", fake_get_job_info)
    monkeypatch.setattr(hpcadapter, "get_job_connect_info", lambda *_args: ("False",))
    monkeypatch.setattr(hpcadapter, "insert_job_info", fake_insert_job_info)
    monkeypatch.setattr(hpcadapter, "list_failed_jobs", fake_list_failed_jobs)
    monkeypatch.setattr(
        hpcadapter,
        "failed_job_query_entry",
        fake_failed_job_query_entry,
    )
    monkeypatch.setattr(hpcadapter.computing_registry, "iptables_jobtypes", lambda: [])
    monkeypatch.setattr(
        hpcadapter,
        "jobid_sort_key",
        lambda job: str(job.get("jobId") or job.get("submitUuid") or ""),
    )

    jobs = asyncio.run(scheduler.query_job())

    assert inserted == []
    assert db_lookup_job_ids == ["101", "104"]
    assert {job["jobType"] for job in jobs} == {"common", "jupyter"}
    assert {job.get("jobId") for job in jobs} == {
        "101",
        "104",
        "",
        "failed-common",
        "failed-jupyter",
    }
    assert all(job["jobType"] != "batch" for job in jobs)


def test_slurm_query_all_means_all_interactive_jobs(monkeypatch):
    hpcadapter, scheduler = _scheduler(monkeypatch)
    redis = FakeRedis()

    async def fake_sub_command(*_args, **_kwargs):
        return (
            "101|cpu|PENDING|00:00|1|node01|jupyter|2026-08-25T10:00:00|Unknown|Unknown|/work/jupyter|24:00:00|sbatch jupyter.sh\n"
            "102|cpu|PENDING|00:00|1|node02|batch|2026-08-25T10:00:00|Unknown|Unknown|/work/batch|24:00:00|sbatch batch.sh\n"
            "103|cpu|PENDING|00:00|1|node03|common|2026-08-25T10:00:00|Unknown|Unknown|/work/common|24:00:00|sbatch common.sh"
        ).encode()

    async def fake_list_failed_jobs(*_args):
        return []

    monkeypatch.setattr(hpcadapter, "redis_connect", lambda: redis)
    monkeypatch.setattr(hpcadapter, "sub_command", fake_sub_command)
    monkeypatch.setattr(
        hpcadapter,
        "get_job_info",
        lambda *_args: ("jupyter", "QUEUEING", 0, 0),
    )
    monkeypatch.setattr(hpcadapter, "get_job_connect_info", lambda *_args: ("False",))
    monkeypatch.setattr(hpcadapter, "list_failed_jobs", fake_list_failed_jobs)
    monkeypatch.setattr(hpcadapter.computing_registry, "iptables_jobtypes", lambda: [])
    monkeypatch.setattr(
        hpcadapter,
        "jobid_sort_key",
        lambda job: str(job.get("jobId") or job.get("submitUuid") or ""),
    )

    jobs = asyncio.run(scheduler.query_job("all"))

    assert {job["jobType"] for job in jobs} == {"common", "jupyter"}


def test_slurm_cancel_job_deletes_failed_submission_record(monkeypatch):
    hpcadapter, scheduler = _scheduler(monkeypatch)

    deleted_ids = []

    async def fake_delete_failed_job(_redis, username, fail_id):
        deleted_ids.append((username, fail_id))
        return True

    async def fail_sub_command(*_args, **_kwargs):
        raise AssertionError("failed pseudo ids must not call scancel")

    monkeypatch.setattr(hpcadapter, "redis_connect", lambda: object())
    monkeypatch.setattr(hpcadapter, "delete_failed_job", fake_delete_failed_job)
    monkeypatch.setattr(hpcadapter, "sub_command", fail_sub_command)

    result = asyncio.run(scheduler.cancel_job(job_id="1787624916305"))

    assert deleted_ids == [("alice", "1787624916305")]
    assert result == {
        "cluster": "slurm",
        "submit_uuid": None,
        "job_id": "1787624916305",
        "job_status": "DELETED",
        "hidden": True,
    }


def test_slurm_cancel_job_treats_expired_failed_record_as_deleted(monkeypatch):
    hpcadapter, scheduler = _scheduler(monkeypatch)

    async def fake_delete_failed_job(*_args):
        return False

    async def fail_sub_command(*_args, **_kwargs):
        raise AssertionError("expired failed pseudo ids must not call scancel")

    monkeypatch.setattr(hpcadapter, "redis_connect", lambda: object())
    monkeypatch.setattr(hpcadapter, "delete_failed_job", fake_delete_failed_job)
    monkeypatch.setattr(hpcadapter, "sub_command", fail_sub_command)

    result = asyncio.run(scheduler.cancel_job(job_id="1787624916305"))

    assert result["job_status"] == "DELETED"
    assert result["hidden"] is True
