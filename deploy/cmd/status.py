#!/usr/bin/env python3
"""fastinkctl status — Show deployment status.

Usage:
  fastinkctl status
"""

import argparse
import sys
from cmd.common import DEPLOY_DIR, load_deploy_answers
from lib import cli_ui
from lib.compose import compose_ps


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Show FastINK deployment status.")
    return p.parse_args()


def main() -> None:
    args = parse_args()
    cli_ui.ensure_deps(DEPLOY_DIR)
    cli_ui.banner()

    if not DEPLOY_DIR.exists():
        cli_ui.warning("No deployment found")
        cli_ui.info(f"Expected directory: {DEPLOY_DIR}")
        cli_ui.info("Run 'fastinkctl deploy' to create a new deployment.")
        return

    answers_path = DEPLOY_DIR / "answers.json"
    if not answers_path.exists():
        cli_ui.warning(f"Deployment directory exists but no answers.json found")
        cli_ui.info(f"Directory: {DEPLOY_DIR}")
        return

    answers = load_deploy_answers()
    project_name = str(answers.get("project_name", "fastink"))
    compose_file = DEPLOY_DIR / "docker-compose.yml"
    profile = str(answers.get("profile", "unknown"))

    # Gather info
    images: dict[str, str] = {}
    for key in ("server", "cron", "rootbrowse", "xrootd", "htcondor"):
        val = answers.get(f"{key}_image")
        if val:
            images[key] = str(val)

    containers = compose_ps(project_name, compose_file) if compose_file.exists() else []

    cli_ui.step("Deployment status")
    cli_ui.summary_table([
        ("Project", project_name),
        ("Profile", profile),
        ("Deploy directory", str(DEPLOY_DIR)),
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
