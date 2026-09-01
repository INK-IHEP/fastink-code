"""Safe cleanup primitives for ``fastinkctl destroy``."""

from __future__ import annotations

import shutil
from pathlib import Path

from .compose import compose_down


def resolve_data_paths(
    answers: dict[str, object],
    deploy_dir: Path,
) -> tuple[Path, Path]:
    """Resolve DB and Redis data directories from saved deployment answers."""
    data_root_value = str(answers.get("data_root") or "").strip()
    data_root = Path(data_root_value) if data_root_value else deploy_dir / "data"
    db_value = str(answers.get("db_data_dir") or "").strip()
    redis_value = str(answers.get("redis_data_dir") or "").strip()
    db_dir = Path(db_value) if db_value else data_root / "db"
    redis_dir = Path(redis_value) if redis_value else data_root / "redis"
    return db_dir.expanduser().resolve(), redis_dir.expanduser().resolve()


def _remove_path(path: Path) -> None:
    if path.is_symlink() or path.is_file():
        path.unlink()
    elif path.is_dir():
        shutil.rmtree(path)


def _preserved_paths(root: Path, preserve_paths: set[Path]) -> set[Path]:
    root = root.resolve()
    return {
        path.expanduser().resolve()
        for path in preserve_paths
        if path.expanduser().resolve() == root
        or root in path.expanduser().resolve().parents
    }


def remove_runtime_path(path: Path, *, preserve_paths: set[Path] | None = None) -> None:
    """Remove a runtime path while retaining selected nested paths."""
    path = path.expanduser().resolve()
    if path == Path(path.anchor):
        raise RuntimeError(f"Refusing to remove filesystem root: {path}")
    if not path.exists() and not path.is_symlink():
        return

    preserved = _preserved_paths(path, preserve_paths or set())
    if path in preserved:
        return
    if not preserved:
        _remove_path(path)
        return

    for child in path.iterdir():
        child_resolved = child.resolve()
        if child_resolved in preserved:
            continue
        if any(child_resolved in kept.parents for kept in preserved):
            remove_runtime_path(child, preserve_paths=preserved)
        else:
            _remove_path(child)

    if path.exists() and not any(path.iterdir()):
        path.rmdir()


def cleanup_deploy_dir(deploy_dir: Path, *, preserve_paths: set[Path]) -> None:
    """Clean generated deployment state, optionally retaining recoverable data."""
    remove_runtime_path(deploy_dir, preserve_paths=preserve_paths)


def stop_deployment(
    project_name: str,
    compose_file: Path,
    *,
    remove_volumes: bool,
) -> None:
    """Stop Compose services or raise before destructive file cleanup."""
    returncode = compose_down(
        project_name,
        compose_file,
        remove_volumes=remove_volumes,
    )
    if returncode != 0:
        raise RuntimeError(f"docker compose down failed with exit code {returncode}")
