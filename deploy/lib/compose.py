"""Thin wrappers around ``docker compose`` subcommands.

Each function returns the process exit code (or parsed data)
so callers can inspect success/failure without exception handling.
"""

from __future__ import annotations

import json
import subprocess
from pathlib import Path
from typing import Any


def compose_down(
    project_name: str,
    compose_file: Path,
    *,
    remove_volumes: bool = False,
) -> int:
    """Run docker compose down, return exit code."""
    cmd = ["docker", "compose", "-p", project_name, "-f", str(compose_file), "down"]
    if remove_volumes:
        cmd.append("-v")
    result = subprocess.run(cmd, check=False)
    return result.returncode


def compose_up(
    project_name: str,
    compose_file: Path,
) -> int:
    """Run docker compose up -d, return exit code."""
    cmd = ["docker", "compose", "-p", project_name, "-f", str(compose_file), "up", "-d"]
    result = subprocess.run(cmd, check=False)
    return result.returncode


def compose_ps(
    project_name: str,
    compose_file: Path,
) -> list[dict[str, Any]]:
    """Run docker compose ps --format json, return parsed containers list."""
    try:
        result = subprocess.run(
            ["docker", "compose", "-p", project_name, "-f", str(compose_file), "ps", "--format", "json"],
            capture_output=True, text=True, check=False,
        )
        if result.returncode != 0 or not result.stdout.strip():
            return []
        containers: list[dict[str, Any]] = []
        for line in result.stdout.strip().splitlines():
            try:
                containers.append(json.loads(line))
            except json.JSONDecodeError:
                pass
        return containers
    except FileNotFoundError:
        return []
