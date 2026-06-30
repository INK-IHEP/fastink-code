from __future__ import annotations

import os
from pathlib import Path
from typing import NamedTuple


SENSITIVE_DEPLOY_FILES = frozenset({
    ".env",
    "answers.json",
    "config.yml",
    "docker-compose.yml",
})


class DeployPaths(NamedTuple):
    """Central path context for all cmd modules."""
    deploy_root: Path          # deploy/
    repo_root: Path            # fastink-code/
    deploy_dir: Path           # fastink-code/.deploy/


def deploy_file_mode(relative_path: str | Path) -> int | None:
    """Return the required mode for a generated deployment file."""
    if Path(relative_path).as_posix() in SENSITIVE_DEPLOY_FILES:
        return 0o600
    return None


def ensure_private_dir(path: Path) -> Path:
    """Create a deployment-state directory and restrict it to its owner."""
    path.mkdir(parents=True, exist_ok=True, mode=0o700)
    path.chmod(0o700)
    return path


def write_file(path: Path, content: str, *, mode: int | None = None) -> None:
    """Write content to path, creating parent directories if needed."""
    path.parent.mkdir(parents=True, exist_ok=True)
    if mode is None:
        path.write_text(content, encoding="utf-8")
        return

    descriptor = os.open(
        path,
        os.O_WRONLY | os.O_CREAT | os.O_TRUNC,
        mode,
    )
    with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
        os.fchmod(stream.fileno(), mode)
        stream.write(content)


def resolve_deploy_paths() -> DeployPaths:
    """Resolve standard deploy paths relative to this module's location.

    Layout:
        deploy/lib/deploy_io.py
        deploy/fastinkctl.py
        deploy/cmd/*.py
    """
    this_file = Path(__file__).resolve()
    lib_dir = this_file.parent                          # deploy/lib/
    deploy_root = lib_dir.parent                         # deploy/
    repo_root = deploy_root.parent                       # fastink-code/
    deploy_dir = repo_root / ".deploy"
    return DeployPaths(deploy_root, repo_root, deploy_dir)
