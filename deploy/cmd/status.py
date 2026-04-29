#!/usr/bin/env python3
"""fastinkctl status — Show deployment status.

Usage:
  fastinkctl status
"""

import argparse
import json
import subprocess
import sys
from pathlib import Path

_HERE = Path(__file__).resolve().parent          # deploy/cmd/
_DEPLOY_ROOT = _HERE.parent                       # deploy/
_REPO_ROOT = _DEPLOY_ROOT.parent                  # fastink-code/
_DEPLOY_DIR = _REPO_ROOT / ".deploy"

if str(_DEPLOY_ROOT) not in sys.path:
    sys.path.insert(0, str(_DEPLOY_ROOT))

from lib import cli_ui


def _compose_ps(project_name: str, compose_file: Path) -> list[dict]:
    """Run docker compose ps --format json and return parsed output."""
    try:
        result = subprocess.run(
            ["docker", "compose", "-p", project_name, "-f", str(compose_file), "ps", "--format", "json"],
            capture_output=True, text=True, check=False,
        )
        if result.returncode != 0 or not result.stdout.strip():
            return []
        # docker compose ps --format json may output one JSON object per line
        containers: list[dict] = []
        for line in result.stdout.strip().splitlines():
            try:
                containers.append(json.loads(line))
            except json.JSONDecodeError:
                pass
        return containers
    except FileNotFoundError:
        return []


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Show FastINK deployment status.")
    return p.parse_args()


def main() -> None:
    args = parse_args()
    cli_ui.ensure_deps(_DEPLOY_DIR)
    cli_ui.banner()

    if not _DEPLOY_DIR.exists():
        cli_ui.warning("No deployment found")
        cli_ui.info(f"Expected directory: {_DEPLOY_DIR}")
        cli_ui.info("Run 'fastinkctl deploy' to create a new deployment.")
        return

    answers_path = _DEPLOY_DIR / "answers.json"
    if not answers_path.exists():
        cli_ui.warning(f"Deployment directory exists but no answers.json found")
        cli_ui.info(f"Directory: {_DEPLOY_DIR}")
        return

    answers = json.loads(answers_path.read_text(encoding="utf-8"))
    project_name = str(answers.get("project_name", "fastink"))
    compose_file = _DEPLOY_DIR / "docker-compose.yml"
    profile = str(answers.get("profile", "unknown"))

    # Gather info
    images: dict[str, str] = {}
    for key in ("server", "cron", "rootbrowse", "xrootd", "htcondor"):
        val = answers.get(f"{key}_image")
        if val:
            images[key] = str(val)

    containers = _compose_ps(project_name, compose_file) if compose_file.exists() else []

    cli_ui.step("Deployment status")
    cli_ui.summary_table([
        ("Project", project_name),
        ("Profile", profile),
        ("Deploy directory", str(_DEPLOY_DIR)),
    ])

    if compose_file.exists():
        cli_ui.info(f"Compose file: {compose_file}")
    else:
        cli_ui.warning("Compose file not found — deployment may be incomplete")

    if images:
        cli_ui.step("Images")
        for role, img in images.items():
            cli_ui.info(f"  {role:<12} {img}")

    if containers:
        cli_ui.step("Running containers")
        for c in containers:
            name = c.get("Name", "?")
            status = c.get("Status", "?")
            state = c.get("State", "?")
            cli_ui.info(f"  {name:<30} {state:<8} {status}")
    else:
        if compose_file.exists():
            cli_ui.info("No containers running")
            cli_ui.info("Run 'fastinkctl up' to start the deployment.")
