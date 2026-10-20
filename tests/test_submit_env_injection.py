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


# ---- job-side storage env injection (docs/openchamber-data-tiering.md) ----
#
# render_storage_env() lives in fastink.service.code_datadir and reads config
# through that module's own imports, so it must be patched there, not on
# common_utils.


def _patch_code_datadir(monkeypatch, service_config):
    from fastink.service import code_datadir

    monkeypatch.setattr(
        code_datadir,
        "get_config",
        lambda section, option=None, fallback=None, **kw: service_config.get(
            option, fallback
        ),
    )
    monkeypatch.setattr(code_datadir, "change_username_to_uid", lambda username: 1000)
    monkeypatch.setattr(code_datadir, "get_user_exp_group", lambda uid: ("CC", "cc"))
    return code_datadir


def test_openchamber_submit_injects_scratch_root(monkeypatch):
    _patch_code_datadir(
        monkeypatch,
        {"scratch_root": "/scratchfs2/{experiment_group_lower}/{username}/.ink"},
    )
    command = _make_command(monkeypatch, "openchamber", {})
    assert "export INK_SCRATCH_ROOT=/scratchfs2/cc/alice/.ink" in command


def test_unconfigured_site_injects_no_storage_env(monkeypatch):
    """Upstream default: no scratch root -> nothing injected."""
    code_datadir = _patch_code_datadir(monkeypatch, {})
    assert code_datadir.render_storage_env("alice") == {}


def test_jobtype_env_wins_over_rendered_scratch_root(monkeypatch):
    """Rendered values are appended first so an explicit site value overrides."""
    _patch_code_datadir(
        monkeypatch, {"scratch_root": "/scratchfs2/{experiment_group_lower}/{username}/.ink"}
    )
    command = _make_command(
        monkeypatch,
        "openchamber",
        {"openchamber": {"env": {"INK_SCRATCH_ROOT": "/pinned/by/site"}}},
    )
    assert command.index("export INK_SCRATCH_ROOT=/pinned/by/site") > command.index(
        "export INK_SCRATCH_ROOT=/scratchfs2/cc/alice/.ink"
    )


def test_opencode_gets_no_storage_env(monkeypatch):
    """Only openchamber consumes the scratch split."""
    _patch_code_datadir(
        monkeypatch, {"scratch_root": "/scratchfs2/{experiment_group_lower}/{username}/.ink"}
    )
    command = _make_command(monkeypatch, "opencode", {})
    assert "INK_SCRATCH_ROOT" not in command


def test_storage_env_failure_does_not_break_submission(monkeypatch):
    from fastink.service import code_datadir

    def boom(*args, **kwargs):
        raise ValueError("cannot resolve experiment group")

    monkeypatch.setattr(
        code_datadir,
        "get_config",
        lambda section, option=None, fallback=None, **kw: (
            "/scratchfs2/{experiment_group_lower}/{username}/.ink"
            if option == "scratch_root" else fallback
        ),
    )
    monkeypatch.setattr(code_datadir, "change_username_to_uid", boom)
    command = _make_command(monkeypatch, "openchamber", {})
    assert "export INK_INIT_HOURS=24" in command  # submission still built
    assert "INK_SCRATCH_ROOT" not in command


def test_default_app_has_no_max_runtime_cap(monkeypatch):
    # No max_hours configured -> INK_MAX_HOURS=0: renew indefinitely while
    # active (the historical behaviour, so uncapped apps are unaffected).
    command = _make_command(monkeypatch, "jupyter", {})
    assert "export INK_MAX_HOURS=0" in command


def test_max_hours_override_injects_absolute_ceiling(monkeypatch):
    # A per-app max_hours becomes the absolute runtime ceiling, independent of
    # init_hours (which stays at the global 24h default = watchdog start).
    command = _make_command(
        monkeypatch,
        "vscode",
        {"vscode": {"htc": {"max_hours": 168}}},
    )
    assert "export INK_MAX_HOURS=168" in command
    assert "export INK_INIT_HOURS=24" in command


def test_init_hours_override_without_max_hours_stays_uncapped(monkeypatch):
    # asic/asicbm style: a long init_hours but no max_hours -> no ceiling.
    command = _make_command(
        monkeypatch,
        "asic",
        {"asic": {"htc": {"init_hours": 168}}},
    )
    assert "export INK_INIT_HOURS=168" in command
    assert "export INK_MAX_HOURS=0" in command


def test_watchdog_knobs_are_not_condor_submit_attributes():
    """The watchdog knobs become INK_* env vars, never submit-file lines.

    Both render paths -- generate_condor_submit and generate_condor_sync_submit
    -- filter through _htc_submit_attrs, so exercising that helper covers both.
    (They previously had separate inline skip-sets and drifted: the sync path
    kept leaking init_hours/idle_kill_sec/arch into the submit file, which makes
    condor_submit print ``WARNING: the line '...' was unused ... Is it a typo?``
    on every submit.)
    """
    from fastink.computing.tools.common import utils as common_utils

    for key in ("init_hours", "max_hours", "idle_kill_sec", "arch"):
        assert key in common_utils.HTC_NON_SUBMIT_KEYS, key

    # A realistic site htc section renders to nothing but real condor attributes.
    htc = {
        "os": "AlmaLinux9",
        "RequestMemory": 6000,
        "RequestCpus": 1,
        "walltime": "default",
        "schedd_host": "scheduler@example",
        "cm_host": "cm.example",
        "extra_param": True,
        "workernode": "wn01",
        "arch": "aarch64",
        "init_hours": 168,
        "max_hours": 168,
        "idle_kill_sec": 86400,
    }
    assert common_utils._htc_submit_attrs(htc) == {}

    # A genuine submit attribute still passes through untouched.
    assert common_utils._htc_submit_attrs({"queue": 1}) == {"queue": 1}


def test_both_submit_renderers_share_one_skip_set():
    """Guard against the two renderers drifting apart again."""
    import inspect

    from fastink.computing.tools.common import utils as common_utils

    for fn in (
        common_utils.generate_condor_submit,
        common_utils.generate_condor_sync_submit,
    ):
        src = inspect.getsource(fn)
        assert "_htc_submit_attrs(" in src, f"{fn.__name__} bypasses the shared skip-set"
