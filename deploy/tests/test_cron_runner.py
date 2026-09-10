"""Tests for cron runner.py: config loading, overlay merge, job dispatch."""
import asyncio
import sys
import tempfile
from pathlib import Path

import pytest
import yaml

# Add deploy/images/cron to path for import
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "images" / "cron"))
import runner


class TestLoadConfig:
    def test_loads_yaml_file(self, tmp_path):
        config = tmp_path / "cron.yaml"
        config.write_text("jobs:\n  - name: test\n    module: os\n    function: getcwd\n    interval: 5\n    mode: delay\n")
        result = runner.load_config(config)
        assert len(result["jobs"]) == 1
        assert result["jobs"][0]["name"] == "test"

    def test_missing_file_returns_empty(self, tmp_path):
        assert runner.load_config(tmp_path / "nonexistent.yaml") == {}

    def test_empty_file_returns_empty(self, tmp_path):
        config = tmp_path / "empty.yaml"
        config.write_text("")
        assert runner.load_config(config) == {}


class TestMergeJobs:
    def test_base_only(self):
        base = {"jobs": [{"name": "a", "module": "os", "function": "getcwd", "interval": 5, "mode": "delay"}]}
        result = runner.merge_jobs(base, {})
        assert len(result) == 1
        assert result[0]["name"] == "a"

    def test_overlay_overrides_matching_job(self):
        base = {"jobs": [{"name": "a", "interval": 5, "mode": "delay"}]}
        overlay = {"jobs": [{"name": "a", "interval": 300}]}
        result = runner.merge_jobs(base, overlay)
        assert len(result) == 1
        assert result[0]["interval"] == 300
        assert result[0]["mode"] == "delay"  # original field preserved

    def test_overlay_appends_new_job(self):
        base = {"jobs": [{"name": "a", "interval": 5}]}
        overlay = {"jobs": [{"name": "b", "interval": 10}]}
        result = runner.merge_jobs(base, overlay)
        assert len(result) == 2
        assert result[0]["name"] == "a"
        assert result[1]["name"] == "b"

    def test_preserves_order(self):
        base = {"jobs": [{"name": "a"}, {"name": "b"}, {"name": "c"}]}
        overlay = {"jobs": [{"name": "b", "interval": 99}, {"name": "d"}]}
        result = runner.merge_jobs(base, overlay)
        names = [j["name"] for j in result]
        assert names == ["a", "b", "c", "d"]
        # b should have the override
        b_job = [j for j in result if j["name"] == "b"][0]
        assert b_job["interval"] == 99

    def test_empty_both(self):
        assert runner.merge_jobs({}, {}) == []


class TestResolveScript:
    def test_finds_in_jobs_subdir(self, tmp_path):
        (tmp_path / "jobs").mkdir()
        (tmp_path / "jobs" / "script.py").write_text("# test")
        result = runner.resolve_script("script.py", tmp_path)
        assert result == tmp_path / "jobs" / "script.py"

    def test_finds_in_overlay_subdir(self, tmp_path):
        (tmp_path / "overlay").mkdir()
        (tmp_path / "overlay" / "script.py").write_text("# test")
        result = runner.resolve_script("script.py", tmp_path)
        assert result == tmp_path / "overlay" / "script.py"

    def test_finds_in_base_dir(self, tmp_path):
        (tmp_path / "script.py").write_text("# test")
        result = runner.resolve_script("script.py", tmp_path)
        assert result == tmp_path / "script.py"

    def test_not_found(self, tmp_path):
        assert runner.resolve_script("nonexistent.py", tmp_path) is None


class TestRunModuleFunction:
    def test_calls_sync_function(self):
        """Verify module+function dispatch works for sync functions."""
        job = {"name": "test", "module": "os", "function": "getcwd"}
        asyncio.run(runner._run_module_function(job))

    def test_calls_async_function(self):
        """Verify module+function dispatch works for async functions."""
        job = {"name": "test", "module": "asyncio", "function": "sleep", "args": [0]}
        asyncio.run(runner._run_module_function(job))

    def test_passes_args(self):
        job = {"name": "test", "module": "os.path", "function": "join", "args": ["a", "b"]}
        asyncio.run(runner._run_module_function(job))


class TestCronYaml:
    def test_base_cron_yaml_is_valid(self):
        """Verify the shipped cron.yaml is valid YAML with expected jobs."""
        cron_yaml = Path(__file__).resolve().parent.parent / "images" / "cron" / "cron.yaml"
        config = runner.load_config(cron_yaml)
        assert "jobs" in config
        names = [j["name"] for j in config["jobs"]]
        assert "job_queue_renew" in names
        assert "job_submit" in names
        assert "reset_job_time" in names
        assert "refresh_redis_jobs" in names
        assert "slurm_job_submit" in names
        assert "slurm_update_job_state" in names
        assert "slurm_update_job_time" in names

    def test_all_jobs_have_required_fields(self):
        cron_yaml = Path(__file__).resolve().parent.parent / "images" / "cron" / "cron.yaml"
        config = runner.load_config(cron_yaml)
        for job in config["jobs"]:
            assert "name" in job
            assert "module" in job
            assert "function" in job
            assert "interval" in job
            assert "mode" in job
