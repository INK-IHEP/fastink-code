import importlib
import sys

import pytest


def _load_cmd_deploy():
    # conftest pins sys.modules["cmd"] to the stdlib module; swap it out
    # briefly so deploy/cmd/deploy.py can be imported under its real name.
    saved = {name: mod for name, mod in sys.modules.items()
             if name == "cmd" or name.startswith("cmd.")}
    for name in saved:
        del sys.modules[name]
    try:
        return importlib.import_module("cmd.deploy")
    finally:
        sys.modules.update(saved)


@pytest.mark.parametrize("enabled", [True, False])
def test_run_init_container_passes_slurm_client_flag(
    sample_answers, tmp_path, monkeypatch, enabled
):
    deploy_cmd = _load_cmd_deploy()

    captured = {}
    monkeypatch.setattr(
        deploy_cmd, "run_command",
        lambda cmd, cwd=None: captured.setdefault("cmd", cmd),
    )

    answers = dict(sample_answers)
    answers["enable_host_slurm_client"] = enabled
    answers["init_image"] = "ink/init:latest"
    paths = {
        "etc_init_dir": tmp_path / "etc-init",
        "keys_dir": tmp_path / "keys",
        "nginx_dir": tmp_path / "nginx",
        "xrootd_dir": tmp_path / "xrootd",
    }

    deploy_cmd.run_init_container(answers, paths)

    expected = f"FASTINK_ENABLE_HOST_SLURM_CLIENT={'true' if enabled else 'false'}"
    assert expected in captured["cmd"]
