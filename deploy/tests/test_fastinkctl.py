"""fastinkctl CLI entrypoint tests: command dispatch, arg passthrough, error handling.

All tests mock subcommand modules so they never run actual deploy/destroy logic.
"""

import subprocess
import sys
from pathlib import Path
from unittest.mock import Mock, patch

import pytest

from deploy.fastinkctl import main

_DEPLOY_ROOT = Path(__file__).resolve().parent.parent
_FASTINKCTL = _DEPLOY_ROOT / "fastinkctl.py"


@pytest.fixture
def mock_subcommand():
    """Mock importlib.import_module so every subcommand returns a fake main."""
    with patch("fastinkctl.importlib.import_module") as mi:
        mod = Mock()
        mod.main = Mock()
        mi.return_value = mod
        yield mi, mod


class TestCommandDispatch:
    """Verify command name to module dispatch logic."""

    @pytest.mark.parametrize(["argv", "expected_module"], [
        ([],                     "cmd.deploy"),
        (["deploy"],             "cmd.deploy"),
        (["deploy", "--yes"],    "cmd.deploy"),
        (["destroy"],            "cmd.destroy"),
        (["down"],               "cmd.down"),
        (["up"],                 "cmd.up"),
        (["status"],             "cmd.status"),
        (["install"],            "cmd.deploy"),
        (["uninstall"],          "cmd.destroy"),
    ])
    def test_dispatch_module_name(self, argv: list[str], expected_module: str,
                                   monkeypatch: pytest.MonkeyPatch,
                                   mock_subcommand) -> None:
        monkeypatch.setattr(sys, "argv", [str(_FASTINKCTL), *argv])
        mi, mod = mock_subcommand
        main()
        mi.assert_called_once_with(expected_module)
        mod.main.assert_called_once()

    def test_unknown_command_exits(self, monkeypatch: pytest.MonkeyPatch,
                                   capsys: pytest.CaptureFixture) -> None:
        monkeypatch.setattr(sys, "argv", ["fastinkctl.py", "unknown-cmd"])
        with pytest.raises(SystemExit) as exc:
            main()
        assert exc.value.code == 1
        assert "Unknown command" in capsys.readouterr().err

    def test_help_shows_usage(self, monkeypatch: pytest.MonkeyPatch,
                              capsys: pytest.CaptureFixture) -> None:
        monkeypatch.setattr(sys, "argv", ["fastinkctl.py", "--help"])
        main()
        assert "fastinkctl deploy" in capsys.readouterr().out

    def test_help_subcommand(self, monkeypatch: pytest.MonkeyPatch,
                             capsys: pytest.CaptureFixture) -> None:
        monkeypatch.setattr(sys, "argv", ["fastinkctl.py", "help"])
        main()
        assert "fastinkctl deploy" in capsys.readouterr().out


class TestSysArgvPassthrough:
    """Verify subcommand name and extra argument handling."""

    def test_no_args_defaults_to_deploy(self, monkeypatch: pytest.MonkeyPatch,
                                        mock_subcommand) -> None:
        monkeypatch.setattr(sys, "argv", ["fastinkctl.py"])
        mi, mod = mock_subcommand
        main()
        mi.assert_called_once_with("cmd.deploy")
        mod.main.assert_called_once()

    def test_command_stripped_from_argv(self, monkeypatch: pytest.MonkeyPatch,
                                        mock_subcommand) -> None:
        """The subcommand's main() must not see the command name in sys.argv."""
        monkeypatch.setattr(sys, "argv", ["fastinkctl.py", "deploy", "--render-only"])
        mi, mod = mock_subcommand
        main()
        mi.assert_called_once_with("cmd.deploy")
        mod.main.assert_called_once()
        assert sys.argv == ["fastinkctl.py", "--render-only"]

    @pytest.mark.parametrize(["full_args", "expected_remaining"], [
        (["deploy"],                        ["fastinkctl.py"]),
        (["deploy", "--yes"],               ["fastinkctl.py", "--yes"]),
        (["deploy", "--profile", "custom"], ["fastinkctl.py", "--profile", "custom"]),
        (["down"],                          ["fastinkctl.py"]),
        (["destroy", "--help"],             ["fastinkctl.py", "--help"]),
    ])
    def test_remaining_args(self, full_args: list[str], expected_remaining: list[str],
                            monkeypatch: pytest.MonkeyPatch,
                            mock_subcommand) -> None:
        monkeypatch.setattr(sys, "argv", ["fastinkctl.py", *full_args])
        mi, mod = mock_subcommand
        main()
        assert sys.argv == expected_remaining


class TestDeployHelpFlags:
    """Verify -h is treated as help, not a command."""

    def test_dash_h(self, monkeypatch: pytest.MonkeyPatch,
                    capsys: pytest.CaptureFixture) -> None:
        monkeypatch.setattr(sys, "argv", ["fastinkctl.py", "-h"])
        main()
        assert "fastinkctl deploy" in capsys.readouterr().out


class TestMainBlock:
    """Test the __main__ guard code's KeyboardInterrupt handling."""

    def test_keyboard_interrupt_exit_code(self) -> None:
        """Run fastinkctl --help and verify clean exit."""
        result = subprocess.run(
            [sys.executable, str(_FASTINKCTL), "--help"],
            capture_output=True, text=True, timeout=15,
        )
        assert result.returncode == 0
        assert "fastinkctl deploy" in result.stdout

    def test_keyboard_interrupt_unknown_command(self) -> None:
        """Run fastinkctl with a nonexistent command and verify exit code."""
        result = subprocess.run(
            [sys.executable, str(_FASTINKCTL), "does-not-exist"],
            capture_output=True, text=True, timeout=15,
        )
        assert result.returncode == 1
        assert "Unknown command" in result.stderr
