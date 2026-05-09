#!/usr/bin/env python3
"""fastinkctl down — Stop containers without removing data.

Usage:
  fastinkctl down [options]

Stops the running FastINK containers but preserves all data
(volumes, images, .deploy/ directory). Use "fastinkctl up"
to start again later.

Options:
  --yes, -y       Skip confirmation
  -v              Also remove named volumes (database / redis data)
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
from lib.compose import compose_down
from cmd.deploy import load_deploy_answers


def parse_args(argv: Optional[list[str]] = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Stop FastINK containers, keep data.")
    p.add_argument("--yes", "-y", action="store_true", help="Skip confirmation")
    p.add_argument("-v", action="store_true", help="Also remove named volumes")
    return p.parse_args(argv)


def main() -> None:
    args = parse_args()
    cli_ui.ensure_deps(_DEPLOY_DIR)
    cli_ui.banner()

    if not _DEPLOY_DIR.exists():
        cli_ui.error(f"No deployment directory found at {_DEPLOY_DIR}")
        sys.exit(1)

    answers = load_deploy_answers()
    project_name = str(answers.get("project_name", "fastink"))
    compose_file = _DEPLOY_DIR / "docker-compose.yml"

    if not compose_file.exists():
        cli_ui.error(f"Compose file not found: {compose_file}")
        sys.exit(1)

    remove_volumes = bool(args.v)
    if remove_volumes:
        action = "Stop containers and remove volumes"
    else:
        action = "Stop containers (data preserved)"

    cli_ui.step(action)
    cli_ui.info(f"Project: {project_name}")

    if not args.yes:
        confirmed = cli_ui.confirm_prompt(action, True)
        if not confirmed:
            cli_ui.warning("Aborted.")
            return

    cli_ui.info(f"+ docker compose -p {project_name} -f {compose_file} down" + (" -v" if remove_volumes else ""))
    rc = compose_down(project_name, compose_file, remove_volumes=remove_volumes)
    if rc == 0:
        cli_ui.success("Containers stopped" + (" and volumes removed" if args.v else ""))
        cli_ui.info("Run 'fastinkctl up' to start again.")
    else:
        cli_ui.error(f"docker compose failed with exit code {rc}")
        sys.exit(1)
