import asyncio
import importlib
import sys


def _load_slurm_cron(monkeypatch, tmp_path):
    config_path = tmp_path / "config.yml"
    config_path.write_text(
        """
common:
  log_path: /tmp/ink-test.log
  krb5_enabled: false
crond:
  submit_workers:
    - slurm
jobtype:
  jupyter: {}
""".strip()
    )
    monkeypatch.setenv("INK_CONFIG_FILE", str(config_path))

    from fastink.common import config

    config._CONFIG_CACHE = None
    config._CONFIG_MTIME = None
    sys.modules.pop("fastink.computing.crond.slurm_cron", None)
    return importlib.import_module("fastink.computing.crond.slurm_cron")


def test_slurm_cron_job_type_filter_allows_common(monkeypatch, tmp_path):
    slurm_cron = _load_slurm_cron(monkeypatch, tmp_path)

    returnable = slurm_cron._get_returnable_slurm_job_types()

    assert returnable == {"common", "jupyter"}
    assert slurm_cron._should_track_slurm_job_type("jupyter", returnable)
    assert slurm_cron._should_track_slurm_job_type("common", returnable)
    assert not slurm_cron._should_track_slurm_job_type("", returnable)
    assert not slurm_cron._should_track_slurm_job_type("batch", returnable)


def test_slurm_update_job_state_skips_unmarked_jobs_before_db_insert(
    monkeypatch,
    tmp_path,
):
    slurm_cron = _load_slurm_cron(monkeypatch, tmp_path)
    inserted = []
    job_exists_checks = []

    sacct_output = "\n".join(
        [
            "|".join(
                [
                    "101", "alice", "cpu", "PENDING", "00:00", "1", "node01",
                    "jupyter", "2026-08-25T10:00:00", "Unknown", "Unknown",
                    "/work/jupyter", "24:00:00", "sbatch jupyter.sh",
                ]
            ),
            "|".join(
                [
                    "102", "alice", "cpu", "RUNNING", "00:01", "1", "node02",
                    "", "2026-08-25T10:01:00", "2026-08-25T10:02:00",
                    "Unknown", "/work/terminal-empty", "24:00:00",
                    "sbatch terminal.sh",
                ]
            ),
            "|".join(
                [
                    "103", "alice", "cpu", "RUNNING", "00:01", "1", "node03",
                    "batch", "2026-08-25T10:03:00", "2026-08-25T10:04:00",
                    "Unknown", "/work/terminal-batch", "24:00:00",
                    "sbatch batch.sh",
                ]
            ),
            "|".join(
                [
                    "104", "alice", "cpu", "PENDING", "00:00", "1", "node04",
                    "common", "2026-08-25T10:05:00", "Unknown", "Unknown",
                    "/work/common", "24:00:00", "sbatch common.sh",
                ]
            ),
        ]
    )

    async def fake_sub_command(*_args, **_kwargs):
        return sacct_output.encode()

    def fake_job_exists(job_id, cluster):
        job_exists_checks.append((job_id, cluster))
        return False

    def fake_insert_job_info(**kwargs):
        inserted.append(kwargs)

    monkeypatch.setattr(slurm_cron, "redis_connect", lambda: object())
    monkeypatch.setattr(slurm_cron, "sub_command", fake_sub_command)
    monkeypatch.setattr(slurm_cron, "get_active_cluster_jobs", lambda _cluster: [])
    monkeypatch.setattr(slurm_cron, "change_username_to_uid", lambda username: 1234)
    monkeypatch.setattr(slurm_cron, "job_exists", fake_job_exists)
    monkeypatch.setattr(slurm_cron, "insert_job_info", fake_insert_job_info)

    asyncio.run(slurm_cron.slurm_update_job_state("slurm"))

    assert job_exists_checks == [("101", "slurm"), ("104", "slurm")]
    assert [job["jobid"] for job in inserted] == ["101", "104"]
    assert {job["job_type"] for job in inserted} == {"common", "jupyter"}


def test_slurm_update_job_time_skips_jobs_missing_from_db(monkeypatch, tmp_path):
    slurm_cron = _load_slurm_cron(monkeypatch, tmp_path)
    start_checks = []
    end_checks = []

    async def fake_sub_command(*_args, **_kwargs):
        return (
            "101|alice|2026-08-25T10:00:00|2026-08-25T10:10:00\n"
            "999|alice|2026-08-25T11:00:00|2026-08-25T11:10:00"
        ).encode()

    monkeypatch.setattr(slurm_cron, "sub_command", fake_sub_command)
    monkeypatch.setattr(slurm_cron, "change_username_to_uid", lambda username: 1234)
    monkeypatch.setattr(
        slurm_cron,
        "job_exists",
        lambda job_id, _cluster: job_id == "101",
    )
    monkeypatch.setattr(
        slurm_cron,
        "get_starttime_info",
        lambda uid, job_id, cluster: (
            start_checks.append((uid, job_id, cluster)) or None
        ),
    )
    monkeypatch.setattr(
        slurm_cron,
        "get_endtime_info",
        lambda uid, job_id, cluster: (
            end_checks.append((uid, job_id, cluster)) or None
        ),
    )
    monkeypatch.setattr(slurm_cron, "update_start_time", lambda *_args: None)
    monkeypatch.setattr(slurm_cron, "update_end_time", lambda *_args: None)

    asyncio.run(slurm_cron.slurm_update_job_time("slurm"))

    assert start_checks == [(1234, "101", "slurm")]
    assert end_checks == [(1234, "101", "slurm")]
