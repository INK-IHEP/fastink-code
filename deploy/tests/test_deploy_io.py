"""Path resolution tests: resolve_deploy_paths directory conventions."""
import stat
from pathlib import Path

from deploy.lib.deploy_io import (
    deploy_file_mode,
    ensure_private_dir,
    resolve_deploy_paths,
    write_file,
)


def test_resolve_deploy_paths() -> None:
    paths = resolve_deploy_paths()
    assert paths.deploy_root.name == "deploy"
    assert paths.deploy_dir == paths.repo_root / ".deploy"
    assert paths.deploy_root == paths.repo_root / "deploy"


def test_write_file_applies_requested_mode(tmp_path: Path) -> None:
    target = tmp_path / "answers.json"
    target.write_text("old", encoding="utf-8")
    target.chmod(0o644)

    write_file(target, "new", mode=0o600)

    assert target.read_text(encoding="utf-8") == "new"
    assert stat.S_IMODE(target.stat().st_mode) == 0o600


def test_ensure_private_dir_corrects_existing_mode(tmp_path: Path) -> None:
    target = tmp_path / ".deploy"
    target.mkdir(mode=0o755)

    ensure_private_dir(target)

    assert stat.S_IMODE(target.stat().st_mode) == 0o700


def test_render_profile_secures_output_directory_before_rendering() -> None:
    source = (
        Path(__file__).resolve().parents[1] / "render_profile.py"
    ).read_text(encoding="utf-8")

    assert "ensure_private_dir(args.output_dir.resolve())" in source


def test_sensitive_deploy_files_use_private_mode() -> None:
    for relative_path in (
        ".env",
        "answers.json",
        "config.yml",
        "docker-compose.yml",
    ):
        assert deploy_file_mode(relative_path) == 0o600


def test_non_sensitive_runtime_files_keep_default_mode() -> None:
    for relative_path in (
        "condor/ink.conf",
        "nginx/default.conf",
        "xrootd/vo-list.cfg",
    ):
        assert deploy_file_mode(relative_path) is None
