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
import subprocess
import sys
from typing import Optional

from cmd.common import DEPLOY_DIR, load_deploy_answers
from lib import cli_ui
from lib.destroy import (
    cleanup_deploy_dir,
    remove_runtime_path,
    resolve_data_paths,
    stop_deployment,
)


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
    cli_ui.ensure_deps(DEPLOY_DIR)
    cli_ui.banner()

    if not DEPLOY_DIR.exists():
        cli_ui.error(f"No deployment directory found at {DEPLOY_DIR}")
        sys.exit(1)

    answers = load_deploy_answers()
    project_name = str(answers.get("project_name", "fastink"))
    compose_file = DEPLOY_DIR / "docker-compose.yml"

    cli_ui.step("Destroy deployment")
    cli_ui.summary_table([
        ("Project", project_name),
        ("Compose file", str(compose_file)),
        ("Deploy directory", str(DEPLOY_DIR)),
    ])

    if not args.yes:
        confirmed = cli_ui.confirm_prompt(
            "Destroy this deployment? This will stop containers and optionally remove data",
            False,
        )
        if not confirmed:
            cli_ui.warning("Aborted.")
            return

    delete_db_data = args.yes
    if not args.yes:
        delete_db_data = cli_ui.confirm_prompt("Delete DB data?", False)

    # 1. Stop containers
    cli_ui.step("Stop and remove containers")
    remove_volumes = delete_db_data

    if compose_file.exists():
        cli_ui.info(f"+ docker compose -p {project_name} -f {compose_file} down" + (" -v" if remove_volumes else ""))
        try:
            stop_deployment(
                project_name,
                compose_file,
                remove_volumes=remove_volumes,
            )
        except RuntimeError as exc:
            cli_ui.error(str(exc))
            sys.exit(1)
    else:
        cli_ui.warning(f"Compose file not found: {compose_file} — skipping docker compose down")
    cli_ui.success("Containers stopped" + (" and named volumes removed" if remove_volumes else ""))

    # 2. Clean runtime data
    db_data_dir, redis_data_dir = resolve_data_paths(answers, DEPLOY_DIR)
    preserved_paths = set()
    if delete_db_data:
        remove_runtime_path(db_data_dir)
        cli_ui.success(f"DB data removed: {db_data_dir}")
    else:
        preserved_paths.update({db_data_dir, DEPLOY_DIR / "answers.json"})
        cli_ui.info(f"DB data preserved: {db_data_dir}")
        cli_ui.info(f"Deployment answers preserved for DB recovery: {DEPLOY_DIR / 'answers.json'}")

    remove_runtime_path(
        redis_data_dir,
        preserve_paths={db_data_dir} if not delete_db_data else set(),
    )
    cli_ui.success(f"Redis data removed: {redis_data_dir}")

    # 3. Remove images
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

    # 4. Clean .deploy/ directory
    if not args.keep_dot_deploy:
        if args.keep_answers:
            preserved_paths.add(DEPLOY_DIR / "answers.json")
        clean_deploy = args.yes
        if not args.yes:
            clean_deploy = cli_ui.confirm_prompt("Clean generated .deploy/ files?", False)
        if clean_deploy:
            cleanup_deploy_dir(DEPLOY_DIR, preserve_paths=preserved_paths)
            if DEPLOY_DIR.exists():
                cli_ui.success("Deployment files cleaned; retained DB recovery state")
            else:
                cli_ui.success("Deployment directory deleted")

    cli_ui.success("Destroy complete")
