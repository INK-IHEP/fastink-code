"""Regression tests for htc_system_jobs.get_system_jobs / _system_jobtypes.

Guards the fix for the hardcoded-jobtype drift: the system-job totals must be
built from the app registry unioned with the config `jobtype` section, so a
newly added app type is counted and -- critically -- a generic/quickstart deploy
(empty `jobtype: {}`) still counts the built-in apps instead of collapsing to
nothing.  Mirrors tests/test_slurm_cron_filter.py's load/monkeypatch pattern.
"""

import asyncio
import importlib
import sys


def _load_htc_system_jobs(monkeypatch, tmp_path, jobtype_block=""):
    """Import htc_system_jobs against a throwaway config.

    The inert `storage` placeholders are required because importing the module
    pulls in fastink.storage (via tools.common.utils), whose storage_init() runs
    at import time and needs a non-empty xrd_host / max_file_size -- the same
    reason deploy/tests/ci/config.ci.yml carries them.  log_path points at /tmp
    so the logger is writable outside the CI container.
    """
    config_path = tmp_path / "config.yml"
    config_path.write_text(
        "common:\n"
        "  log_path: /tmp/ink-test.log\n"
        "  krb5_enabled: false\n"
        "storage:\n"
        "  xrd_host: root://localhost:1094\n"
        "  fs_backend: xrootd\n"
        "  max_file_size: 21737418240\n"
        "computing:\n"
        "  schedd_host: scheduler@test\n"
        "jobtype:\n"
        + jobtype_block
    )
    monkeypatch.setenv("INK_CONFIG_FILE", str(config_path))

    from fastink.common import config

    config._CONFIG_CACHE = None
    config._CONFIG_MTIME = None
    sys.modules.pop("fastink.computing.htc.htc_system_jobs", None)
    return importlib.import_module("fastink.computing.htc.htc_system_jobs")


class _FakeRegistry:
    def __init__(self, names):
        self._names = list(names)

    def names(self):
        return list(self._names)


class _FakeRedis:
    """get() -> None (forces the compute branch); captures setex()."""

    def __init__(self, store):
        self._store = store

    async def get(self, _key):
        return None

    async def setex(self, key, _ttl, val):
        self._store["setex"] = (key, val)


class _RecLogger:
    def __init__(self, sink):
        self._sink = sink

    def warning(self, *a, **k):
        self._sink.append(("warning", a))

    def error(self, *a, **k):
        self._sink.append(("error", a))

    def info(self, *a, **k):
        self._sink.append(("info", a))


def test_system_jobtypes_unions_registry_and_config(monkeypatch, tmp_path):
    m = _load_htc_system_jobs(monkeypatch, tmp_path, "  openclaw: {}\n  vnc: {}\n")
    monkeypatch.setattr(m, "computing_registry", _FakeRegistry(["vscode", "vnc", "jupyter"]))
    # union, de-duplicated, sorted
    assert m._system_jobtypes() == ["jupyter", "openclaw", "vnc", "vscode"]


def test_system_jobtypes_generic_deploy_falls_back_to_registry(monkeypatch, tmp_path):
    # Empty `jobtype` (what the generic/quickstart profile renders) must NOT
    # collapse the totals: the registry baseline still lists the built-in apps.
    m = _load_htc_system_jobs(monkeypatch, tmp_path, "")  # jobtype: null
    monkeypatch.setattr(
        m, "computing_registry",
        _FakeRegistry(["vscode", "vnc", "jupyter", "rootbrowse", "enode"]),
    )
    assert m._system_jobtypes() == ["enode", "jupyter", "rootbrowse", "vnc", "vscode"]


def test_system_jobtypes_drops_non_identifier_and_warns(monkeypatch, tmp_path):
    m = _load_htc_system_jobs(monkeypatch, tmp_path, "  ok_type: {}\n  bad-type: {}\n")
    monkeypatch.setattr(m, "computing_registry", _FakeRegistry(["vscode"]))
    sink = []
    monkeypatch.setattr(m, "logger", _RecLogger(sink))

    result = m._system_jobtypes()

    assert "bad-type" not in result          # not shell-safe -> dropped
    assert "ok_type" in result and "vscode" in result
    assert any(lvl == "warning" for lvl, _ in sink), "expected a warning for the dropped type"


def test_system_jobtypes_tolerates_non_dict_jobtype_config(monkeypatch, tmp_path):
    # A `jobtype` section rendered as a list must not raise AttributeError.
    m = _load_htc_system_jobs(monkeypatch, tmp_path, "  - vscode\n  - vnc\n")
    monkeypatch.setattr(m, "computing_registry", _FakeRegistry([]))
    assert m._system_jobtypes() == ["vnc", "vscode"]


def test_get_system_jobs_counts_all_types_and_skips_unknown(monkeypatch, tmp_path):
    m = _load_htc_system_jobs(monkeypatch, tmp_path, "  openclaw: {}\n")
    monkeypatch.setattr(m, "computing_registry", _FakeRegistry(["vscode", "jupyter"]))
    store = {}
    monkeypatch.setattr(m, "redis_connect", lambda: _FakeRedis(store))

    # condor_q -af fields: Owner ClusterId ProcId RealGroup Qdate JobStatus
    #                      JobStartDate RemoteHost JobType  (idx5=status, idx8=type)
    out = "\n".join([
        "alice 1 0 undefined 100 2 200 host jupyter",    # running
        "bob 2 0 undefined 100 1 0 undefined vscode",    # queued
        "carol 3 0 undefined 100 2 200 host openclaw",   # running
        "dave 4 0 undefined 100 2 200 host BOGUSTYPE",   # unknown -> skipped
    ])
    called = {}

    async def fake_sub(command, *_a, **_k):
        called["cmd"] = command
        return out.encode()

    monkeypatch.setattr(m, "sub_command", fake_sub)

    res = asyncio.run(m.get_system_jobs())

    # registry{vscode,jupyter} U config{openclaw}, capitalized like the endpoint
    assert res == {"Vscode": 1, "Jupyter": 1, "Openclaw": 1}
    assert "Bogustype" not in res                        # unknown type not added
    for jt in ("vscode", "jupyter", "openclaw"):
        assert f'HepJob_JobType == "{jt}"' in called["cmd"]
    assert store["setex"][0] == "system_job_list"        # result cached


def test_get_system_jobs_empty_jobtypes_returns_empty_without_condor(monkeypatch, tmp_path):
    # No registered apps AND no configured types -> {} and never build the
    # malformed `condor_q -const ''` (a condor parse error).
    m = _load_htc_system_jobs(monkeypatch, tmp_path, "")
    monkeypatch.setattr(m, "computing_registry", _FakeRegistry([]))
    store = {}
    monkeypatch.setattr(m, "redis_connect", lambda: _FakeRedis(store))
    called = {"sub": False}

    async def fake_sub(command, *_a, **_k):
        called["sub"] = True
        return b""

    monkeypatch.setattr(m, "sub_command", fake_sub)

    res = asyncio.run(m.get_system_jobs())

    assert res == {}
    assert called["sub"] is False
    assert store["setex"][1] == "{}"
