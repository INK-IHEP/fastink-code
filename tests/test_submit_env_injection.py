"""Unit tests for per-app env injection in ``generate_submit_command``:
walltime / idle-kill (openchamber long-lived session support) and generic
``jobtype.<name>.env`` overrides used to pin app binaries per environment.

See docs/openchamber-long-lived-session.md section 4.2.
"""

import pytest

from fastink.computing.tools.common import utils as common_utils


def _make_command(monkeypatch, job_type, jobtype_config):
    """Call generate_submit_command with stubbed config and environment."""

    def fake_get_config(section, option=None, fallback=None, **kwargs):
        if section == "job_time":
            return {"walltime": 24, "check_interval": 900, "active_idle": 1800}[
                option
            ]
        if section == "common":
            return {"krb5_enabled": "false"}.get(option, "false")
        if section == "jobtype":
            return jobtype_config.get(job_type, fallback if fallback is not None else {})
        if section == "computing":
            return {"schedd_host": "schedd.example", "cm_host": "cm.example"}[option]
        raise AssertionError(f"unexpected config read: {section}.{option}")

    monkeypatch.setattr(common_utils, "get_config", fake_get_config)
    # Neutralise environment-dependent helpers.
    monkeypatch.setattr(
        common_utils, "change_username_to_uid", lambda username: 1000
    )
    monkeypatch.setattr(
        common_utils,
        "pwd",
        type("P", (), {"getpwuid": staticmethod(lambda uid: type("S", (), {"pw_shell": "/bin/bash"})())}),
    )
    from fastink.computing.apps import registry as _reg

    monkeypatch.setattr(_reg, "noenv_jobtypes", lambda: [])

    return common_utils.generate_submit_command(
        "alice", "/jobs/dir", job_type, "alice.sub"
    )


def test_default_app_uses_global_walltime_and_no_idle_kill(monkeypatch):
    command = _make_command(monkeypatch, "jupyter", {})
    assert "export INK_INIT_HOURS=24" in command
    assert "INK_IDLE_KILL_SEC" not in command


def test_openchamber_overrides_init_hours_and_injects_idle_kill(monkeypatch):
    command = _make_command(
        monkeypatch,
        "openchamber",
        {"openchamber": {"htc": {"init_hours": 168, "idle_kill_sec": 86400}}},
    )
    assert "export INK_INIT_HOURS=168" in command
    assert "export INK_IDLE_KILL_SEC=86400" in command
    # Global values not overridden stay as-is.
    assert "export INK_CHECK_INTERVAL=900" in command
    assert "export INK_ACTIVE_IDLE_SEC=1800" in command


def test_init_hours_override_without_idle_kill_keeps_watchdog_disabled(monkeypatch):
    command = _make_command(
        monkeypatch,
        "openchamber",
        {"openchamber": {"htc": {"init_hours": 48}}},
    )
    assert "export INK_INIT_HOURS=48" in command
    assert "INK_IDLE_KILL_SEC" not in command


def test_jobtype_env_overrides_are_exported(monkeypatch):
    command = _make_command(
        monkeypatch,
        "openchamber",
        {
            "openchamber": {
                "env": {
                    "OPENCHAMBER_BIN": "/cvmfs/software/openchamber-web-pre/bin/cli.js"
                }
            }
        },
    )
    assert (
        "export OPENCHAMBER_BIN=/cvmfs/software/openchamber-web-pre/bin/cli.js"
        in command
    )


def test_jobtype_env_values_are_shell_quoted(monkeypatch):
    command = _make_command(
        monkeypatch,
        "openchamber",
        {"openchamber": {"env": {"SOME_VAR": "a b;rm -rf /"}}},
    )
    assert "export SOME_VAR='a b;rm -rf /'" in command


def test_jobtype_env_none_value_is_skipped(monkeypatch):
    command = _make_command(
        monkeypatch,
        "openchamber",
        {"openchamber": {"env": {"UNSET_ON_PURPOSE": None, "OPENCHAMBER_BIN": "/x"}}},
    )
    assert "UNSET_ON_PURPOSE" not in command
    assert "export OPENCHAMBER_BIN=/x" in command


def test_no_env_section_exports_nothing_extra(monkeypatch):
    command = _make_command(monkeypatch, "openchamber", {})
    assert "OPENCHAMBER_BIN" not in command
