"""Destroy cleanup behavior for persistent DB and disposable runtime state."""

from pathlib import Path

import pytest

from deploy.lib.destroy import (
    cleanup_deploy_dir,
    remove_runtime_path,
    resolve_data_paths,
    stop_deployment,
)


def build_deploy_tree(root: Path) -> Path:
    deploy_dir = root / ".deploy"
    files = {
        "answers.json": "{}",
        "config.yml": "database: {}",
        "docker-compose.yml": "services: {}",
        "data/db/ibdata1": "db",
        "data/redis/appendonly.aof": "redis",
        "data/etc-init/passwd": "root:x:0:0",
        "keys/ssh-client/id_rsa": "private",
    }
    for relative_path, content in files.items():
        target = deploy_dir / relative_path
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(content, encoding="utf-8")
    return deploy_dir


def test_cleanup_preserves_db_and_answers(tmp_path: Path) -> None:
    deploy_dir = build_deploy_tree(tmp_path)
    db_dir = deploy_dir / "data" / "db"

    cleanup_deploy_dir(
        deploy_dir,
        preserve_paths={db_dir, deploy_dir / "answers.json"},
    )

    assert (db_dir / "ibdata1").exists()
    assert (deploy_dir / "answers.json").exists()
    assert not (deploy_dir / "data" / "redis").exists()
    assert not (deploy_dir / "data" / "etc-init").exists()
    assert not (deploy_dir / "keys").exists()
    assert not (deploy_dir / "config.yml").exists()


def test_cleanup_removes_deploy_dir_when_nothing_is_preserved(tmp_path: Path) -> None:
    deploy_dir = build_deploy_tree(tmp_path)

    cleanup_deploy_dir(deploy_dir, preserve_paths=set())

    assert not deploy_dir.exists()


def test_remove_runtime_path_preserves_nested_db(tmp_path: Path) -> None:
    data_root = tmp_path / "data"
    db_dir = data_root / "db"
    redis_dir = data_root / "redis"
    db_dir.mkdir(parents=True)
    redis_dir.mkdir()
    (db_dir / "ibdata1").write_text("db", encoding="utf-8")
    (redis_dir / "dump.aof").write_text("redis", encoding="utf-8")

    remove_runtime_path(data_root, preserve_paths={db_dir})

    assert (db_dir / "ibdata1").exists()
    assert not redis_dir.exists()


def test_resolve_data_paths_prefers_explicit_db_path(tmp_path: Path) -> None:
    deploy_dir = tmp_path / ".deploy"
    answers = {
        "data_root": str(tmp_path / "runtime"),
        "db_data_dir": str(tmp_path / "external-db"),
        "redis_data_dir": str(tmp_path / "external-redis"),
    }

    db_dir, redis_dir = resolve_data_paths(answers, deploy_dir)

    assert db_dir == (tmp_path / "external-db").resolve()
    assert redis_dir == (tmp_path / "external-redis").resolve()


def test_resolve_data_paths_uses_deploy_default_when_data_root_missing(tmp_path: Path) -> None:
    deploy_dir = tmp_path / ".deploy"

    db_dir, redis_dir = resolve_data_paths({}, deploy_dir)

    assert db_dir == (deploy_dir / "data" / "db").resolve()
    assert redis_dir == (deploy_dir / "data" / "redis").resolve()


def test_stop_deployment_raises_when_compose_down_fails(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setattr(
        "deploy.lib.destroy.compose_down",
        lambda project_name, compose_file, remove_volumes: 17,
    )

    with pytest.raises(RuntimeError, match="exit code 17"):
        stop_deployment("fastink", tmp_path / "docker-compose.yml", remove_volumes=False)
