"""路径解析测试：resolve_deploy_paths 的目录结构约定。"""
from deploy.lib.deploy_io import resolve_deploy_paths


def test_resolve_deploy_paths() -> None:
    paths = resolve_deploy_paths()
    assert paths.deploy_root.name == "deploy"
    assert paths.repo_root.name == "fastink-code"
    assert paths.deploy_dir == paths.repo_root / ".deploy"
    assert paths.deploy_root == paths.repo_root / "deploy"
