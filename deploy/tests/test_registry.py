"""命令注册表测试：register / get_module_path / known_commands。

使用 deploy.cmd 绝对路径导入，避免与 stdlib cmd 模块冲突。
"""
from deploy.cmd import register, get_module_path, known_commands


def test_register_and_lookup() -> None:
    register("test-cmd", "cmd.test", aliases=["tc"])
    assert get_module_path("test-cmd") == "cmd.test"
    assert get_module_path("tc") == "cmd.test"
    assert get_module_path("unknown") is None


def test_known_commands_excludes_aliases() -> None:
    primary = known_commands()
    assert "install" not in primary
    assert "uninstall" not in primary
    for cmd in ("deploy", "destroy", "down", "up", "status"):
        assert cmd in primary, f"missing primary command: {cmd}"
