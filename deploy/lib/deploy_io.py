from __future__ import annotations

from pathlib import Path
from typing import NamedTuple


class DeployPaths(NamedTuple):
    """Central path context for all cmd modules."""
    deploy_root: Path          # deploy/
    repo_root: Path            # fastink-code/
    deploy_dir: Path           # fastink-code/.deploy/


def write_file(path: Path, content: str) -> None:
    """Write content to path, creating parent directories if needed."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


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
