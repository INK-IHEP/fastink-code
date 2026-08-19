"""Docker compose operations: verify subprocess argument construction."""
import subprocess
from pathlib import Path

import pytest
from deploy.lib.compose import compose_up, compose_down, compose_ps


def _fake_run(*, returncode: int = 0, stdout: str = "") -> object:
    return type("Result", (), {"returncode": returncode, "stdout": stdout})()


def test_compose_up(monkeypatch: pytest.MonkeyPatch, compose_file: Path) -> None:
    captured: list[list[str]] = []

    def fake_subprocess(cmd, **kwargs):
        captured.append(cmd)
        return _fake_run()

    monkeypatch.setattr(subprocess, "run", fake_subprocess)
    compose_up("my-project", compose_file)

    assert captured[0] == [
        "docker", "compose", "-p", "my-project", "-f", str(compose_file),
        "up", "-d",
    ]


class TestComposeDown:
    def test_default_no_volumes(self, monkeypatch, compose_file):
        captured: list[list[str]] = []

        def fake_subprocess(cmd, **kwargs):
            captured.append(cmd)
            return _fake_run()

        monkeypatch.setattr(subprocess, "run", fake_subprocess)
        compose_down("my-project", compose_file)
        assert "-v" not in captured[0]

    def test_with_remove_volumes(self, monkeypatch, compose_file):
        captured: list[list[str]] = []

        def fake_subprocess(cmd, **kwargs):
            captured.append(cmd)
            return _fake_run()

        monkeypatch.setattr(subprocess, "run", fake_subprocess)
        compose_down("my-project", compose_file, remove_volumes=True)
        assert captured[0][-1] == "-v"


class TestComposePs:
    def test_parses_json_lines(self, monkeypatch, compose_file):
        def fake_run(cmd, **kwargs):
            return _fake_run(stdout='{"Name": "c1", "State": "running"}\n{"Name": "c2", "State": "exited"}')
        monkeypatch.setattr(subprocess, "run", fake_run)
        result = compose_ps("p", compose_file)
        assert len(result) == 2
        assert result[0]["Name"] == "c1"
        assert result[1]["State"] == "exited"

    def test_empty_when_no_stdout(self, monkeypatch, compose_file):
        def fake_run(cmd, **kwargs):
            return _fake_run(stdout="")
        monkeypatch.setattr(subprocess, "run", fake_run)
        assert compose_ps("p", compose_file) == []

    def test_empty_when_file_not_found(self, monkeypatch, compose_file):
        def fake_run(cmd, **kwargs):
            raise FileNotFoundError()
        monkeypatch.setattr(subprocess, "run", fake_run)
        assert compose_ps("p", compose_file) == []
