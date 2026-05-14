#!/usr/bin/env python3
"""fastinkctl destroy — Tear down and clean up an existing FastINK deployment.

Usage:
  fastinkctl destroy [options]

Options:
  --yes, -y               Skip confirmation prompts (full automatic cleanup)
  --keep-answers          Keep .deploy/answers.json for re-deployment
  --keep-images           Do not remove Docker images
  --keep-dot-deploy       Do not delete the .deploy/ directory
"""

import argparse
import shutil
import subprocess
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
    p = argparse.ArgumentParser(description="Tear down an existing FastINK deployment.")
    p.add_argument("--yes", "-y", action="store_true", help="Skip confirmation prompts, full automatic cleanup")
    p.add_argument("--keep-answers", action="store_true", help="Keep .deploy/answers.json for re-deployment")
    p.add_argument("--keep-images", action="store_true", help="Do not remove Docker images")
    p.add_argument("--keep-dot-deploy", action="store_true", help="Do not delete .deploy/ directory")
    return p.parse_args(argv)


def run_cmd(cmd: list[str]) -> subprocess.CompletedProcess:
    cli_ui.info(f"+ {' '.join(cmd)}")
    return subprocess.run(cmd, check=False)


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

    cli_ui.step("Destroy deployment")
    cli_ui.summary_table([
        ("Project", project_name),
        ("Compose file", str(compose_file)),
        ("Deploy directory", str(_DEPLOY_DIR)),
    ])

    if not args.yes:
        confirmed = cli_ui.confirm_prompt(
            "Destroy this deployment? This will stop containers and optionally remove data",
            False,
        )
        if not confirmed:
            cli_ui.warning("Aborted.")
            return

    # 1. Stop containers
    cli_ui.step("Stop and remove containers")
    remove_volumes = args.yes
    if not args.yes:
        remove_volumes = cli_ui.confirm_prompt("Remove named volumes (database and redis data)", False)

    if compose_file.exists():
        cli_ui.info(f"+ docker compose -p {project_name} -f {compose_file} down" + (" -v" if remove_volumes else ""))
        compose_down(project_name, compose_file, remove_volumes=remove_volumes)
    else:
        cli_ui.warning(f"Compose file not found: {compose_file} — skipping docker compose down")
    cli_ui.success("Containers stopped" + (" and volumes removed" if remove_volumes else ""))

    # 2. Remove images
    if not args.keep_images:
        remove_images = args.yes
        if not args.yes:
            remove_images = cli_ui.confirm_prompt("Remove Docker images built by this deployment", False)
        if remove_images:
            image_fields = [k for k in answers if k.endswith("_image")]
            images: set[str] = set()
            for f in image_fields:
                v = answers.get(f)
                if isinstance(v, str) and v:
                    images.add(v)
            for image in sorted(images):
                result = run_cmd(["docker", "rmi", image])
                if result.returncode != 0:
                    cli_ui.warning(f"Failed to remove image {image} (may be in use by other deployments)")
            cli_ui.success("Images removed")

    # 3. Clean .deploy/ directory
    if not args.keep_dot_deploy:
        if args.keep_answers:
            # Preserve answers.json, delete everything else
            for item in _DEPLOY_DIR.iterdir():
                if item.name == "answers.json":
                    continue
                if item.is_dir():
                    shutil.rmtree(item)
                else:
                    item.unlink()
            cli_ui.success("Deployment files cleaned (answers.json preserved)")
        else:
            remove_deploy = args.yes
            if not args.yes:
                remove_deploy = cli_ui.confirm_prompt("Delete .deploy/ directory entirely", False)
            if remove_deploy:
                shutil.rmtree(_DEPLOY_DIR)
                cli_ui.success("Deployment directory deleted")

    cli_ui.success("Destroy complete")
