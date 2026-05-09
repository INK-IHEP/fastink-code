#!/usr/bin/env python3
"""fastinkctl up — Start FastINK containers.

Usage:
  fastinkctl up [options]

Starts the FastINK containers using the existing .deploy/ configuration.
Use this to restart after ``fastinkctl down``.

Options:
  --yes, -y       Skip confirmation
  --wait          Wait for health check after starting
"""

import argparse
import sys
from pathlib import Path
from typing import Optional

_HERE = Path(__file__).resolve().parent
if str(_HERE.parent) not in sys.path:
    sys.path.insert(0, str(_HERE.parent))

from lib.deploy_io import resolve_deploy_paths

_DEPLOY_DIR = resolve_deploy_paths().deploy_dir

from lib import cli_ui
from cmd.deploy import deploy_stack, load_deploy_answers, wait_for_health


def parse_args(argv: Optional[list[str]] = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Start FastINK containers.")
    p.add_argument("--yes", "-y", action="store_true", help="Skip confirmation")
    p.add_argument("--wait", action="store_true", help="Wait for health check after starting")
    return p.parse_args(argv)


def main() -> None:
    args = parse_args()
    cli_ui.ensure_deps(_DEPLOY_DIR)
    cli_ui.banner()

    if not _DEPLOY_DIR.exists():
        cli_ui.error(f"No deployment directory found at {_DEPLOY_DIR}")
        sys.exit(1)

    answers = load_deploy_answers()
    compose_file = _DEPLOY_DIR / "docker-compose.yml"

    if not compose_file.exists():
        cli_ui.error(f"Compose file not found: {compose_file}")
        cli_ui.info("Run 'fastinkctl deploy' to create a new deployment.")
        sys.exit(1)

    project_name = str(answers.get("project_name", "fastink"))
    cli_ui.step("Start containers")
    cli_ui.info(f"Project: {project_name}")

    if not args.yes:
        confirmed = cli_ui.confirm_prompt("Start containers with docker compose up", True)
        if not confirmed:
            cli_ui.warning("Aborted.")
            return

    deploy_stack(answers)

    if args.wait:
        public_base_url = str(answers.get("public_base_url", ""))
        if public_base_url:
            health_url = f"{public_base_url}/health"
            cli_ui.step(f"Wait for health check: {health_url}")
            if wait_for_health(health_url):
                cli_ui.success(f"Health check passed: {health_url}")
            else:
                cli_ui.warning(f"Health check did not pass within timeout: {health_url}")
