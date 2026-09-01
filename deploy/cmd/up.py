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
from typing import Optional

from cmd.common import DEPLOY_DIR, load_deploy_answers
from lib import cli_ui
from lib.defaults import build_health_url
from cmd.deploy import deploy_stack, wait_for_health


def parse_args(argv: Optional[list[str]] = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Start FastINK containers.")
    p.add_argument("--yes", "-y", action="store_true", help="Skip confirmation")
    p.add_argument("--wait", action="store_true", help="Wait for health check after starting")
    return p.parse_args(argv)


def main() -> None:
    args = parse_args()
    cli_ui.ensure_deps(DEPLOY_DIR)
    cli_ui.banner()

    if not DEPLOY_DIR.exists():
        cli_ui.error(f"No deployment directory found at {DEPLOY_DIR}")
        sys.exit(1)

    answers = load_deploy_answers()
    compose_file = DEPLOY_DIR / "docker-compose.yml"

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
        health_url = build_health_url(answers) if answers.get("public_base_url") else ""
        cli_ui.step(f"Wait for health check: {health_url}")
        if wait_for_health(health_url):
            cli_ui.success(f"Health check passed: {health_url}")
        else:
            cli_ui.warning(f"Health check did not pass within timeout: {health_url}")
