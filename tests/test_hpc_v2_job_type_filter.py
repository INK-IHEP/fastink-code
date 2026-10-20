import asyncio
import importlib
import sys

from sqlalchemy.exc import NoResultFound


def _load_hpc_job_types(monkeypatch, tmp_path):
    config_path = tmp_path / "config.yml"
    config_path.write_text(
        """
common:
  log_path: /tmp/ink-test.log
  krb5_enabled: false
storage:
  xrd_host: root://localhost
  max_file_size: 1024
  fs_backend: nfs
jobtype:
  jupyter: {}
  vscode: {}
""".strip()
    )
    monkeypatch.setenv("INK_CONFIG_FILE", str(config_path))

    from fastink.common import config

    config._CONFIG_CACHE = None
    config._CONFIG_MTIME = None
    sys.modules.pop("fastink.computing.hpc.v2.hpc_job_types", None)
    return importlib.import_module("fastink.computing.hpc.v2.hpc_job_types")


def test_hpc_v2_job_type_helpers_allow_common_and_configured_types(
    monkeypatch,
    tmp_path,
):
    hpc_job_types = _load_hpc_job_types(monkeypatch, tmp_path)

    returnable = hpc_job_types.get_returnable_hpc_job_types()

    assert returnable == {"common", "jupyter", "vscode"}
    assert hpc_job_types.requested_hpc_job_types(None) is None
    assert hpc_job_types.requested_hpc_job_types("all") is None
    assert hpc_job_types.requested_hpc_job_types(" jupyter, vscode ") == {
        "jupyter",
        "vscode",
    }
    assert hpc_job_types.normalize_hpc_job_type("batch", "common") == "common"
    assert hpc_job_types.normalize_hpc_job_type("", "batch") == ""
    assert hpc_job_types.should_return_hpc_job_type("common", None, returnable)
    assert not hpc_job_types.should_return_hpc_job_type("batch", None, returnable)


def test_hpc_v2_query_filters_unmarked_jobs_before_db_sync(monkeypatch, tmp_path):
    _load_hpc_job_types(monkeypatch, tmp_path)
    from fastink.computing.hpc.v2 import hpc_query_jobs

    db_lookup_job_ids = []
    inserted = []
    commands = []

    sacct_output = "\n".join(
        [
            "JobID|Partition|JobName|User|State|Elapsed|NNodes|NodeList|WCKey|AdminComment|Start|Submit|WorkDir",
            "101|cpu|ink-jupyter|alice|PENDING|00:00|1|node01|jupyter||Unknown|2026-08-25T10:00:00|/work/jupyter",
            "102|cpu|terminal-empty|alice|RUNNING|00:01|1|node02|||2026-08-25T10:02:00|2026-08-25T10:01:00|/work/terminal-empty",
            "103|cpu|terminal-batch|alice|RUNNING|00:01|1|node03|batch||2026-08-25T10:04:00|2026-08-25T10:03:00|/work/terminal-batch",
            "104|cpu|legacy-common|alice|PENDING|00:00|1|node04||common|Unknown|2026-08-25T10:05:00|/work/common",
        ]
    )

    async def fake_sub_command(command, *_args, **_kwargs):
        commands.append(command)
        if "sacct -u" in command:
            return sacct_output.encode()
        if "scontrol show job" in command:
            return b"StdOut=/tmp/%j.out StdErr=/tmp/%j.err"
        raise AssertionError(f"unexpected command: {command}")

    def fake_get_job_info(_uid, job_id, _cluster_id):
        db_lookup_job_ids.append(job_id)
        if job_id == "104" and not inserted:
            raise NoResultFound()
        return ("jupyter", "QUEUEING", 0, 0)

    def fake_insert_job_info(*args):
        inserted.append(args)

    monkeypatch.setattr(hpc_query_jobs, "change_uid_to_username", lambda _uid: "alice")
    monkeypatch.setattr(hpc_query_jobs, "sub_command", fake_sub_command)
    monkeypatch.setattr(hpc_query_jobs, "get_job_info", fake_get_job_info)
    monkeypatch.setattr(hpc_query_jobs, "insert_job_info", fake_insert_job_info)
    monkeypatch.setattr(hpc_query_jobs, "get_job_connect_info", lambda *_args: ("False",))
    monkeypatch.setattr(hpc_query_jobs, "update_job_status", lambda *_args: None)

    jobs = asyncio.run(hpc_query_jobs.get_user_jobs(1234, "all", "slurm"))

    assert "WCKey" in commands[0]
    assert db_lookup_job_ids == ["101", "104", "104"]
    assert len(inserted) == 1
    assert inserted[0][4] == "common"
    assert {job["jobId"] for job in jobs} == {"101", "104"}
    assert {job["jobType"] for job in jobs} == {"common", "jupyter"}


def test_hpc_v2_system_jobs_counts_only_ink_job_types(monkeypatch, tmp_path):
    _load_hpc_job_types(monkeypatch, tmp_path)
    from fastink.computing.hpc.v2 import hpc_system_jobs

    output = "\n".join(
        [
            "JobID|Partition|JobName|User|State|Elapsed|NNodes|NodeList|WCKey|AdminComment|Start|Submit",
            "101|cpu|ink-jupyter|alice|PENDING|00:00|1|node01|jupyter||Unknown|2026-08-25T10:00:00",
            "102|cpu|terminal-empty|alice|RUNNING|00:01|1|node02|||2026-08-25T10:02:00|2026-08-25T10:01:00",
            "103|cpu|terminal-batch|alice|RUNNING|00:01|1|node03|batch||2026-08-25T10:04:00|2026-08-25T10:03:00",
            "104|cpu|legacy-common|alice|RUNNING|00:00|1|node04||common|Unknown|2026-08-25T10:05:00",
        ]
    )

    class FakeProcess:
        returncode = 0

        async def communicate(self):
            return output.encode(), b""

    async def fake_create_subprocess_exec(*_args, **_kwargs):
        return FakeProcess()

    monkeypatch.setattr(
        hpc_system_jobs.asyncio,
        "create_subprocess_exec",
        fake_create_subprocess_exec,
    )

    queueing, running = asyncio.run(hpc_system_jobs.get_hpc_system_jobs())

    assert queueing["jupyter"] == 1
    assert running["common"] == 1
    assert running["vscode"] == 0
    assert "batch" not in queueing
