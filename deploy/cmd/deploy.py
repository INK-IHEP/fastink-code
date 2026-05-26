#!/usr/bin/env python3
import argparse
import json
import shutil
import ssl
import subprocess
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path

from cmd.common import DEPLOY_DIR, DEPLOY_PATHS
from lib import cli_ui
from lib.defaults import build_health_url, default_answers, required_images
from lib.deploy_io import write_file
from lib.host_runtime import check_host_prerequisites
from lib.render import render_bundle
from lib.questionnaire import (
    apply_overrides,
    build_paths_from_answers,
    build_quickstart_answers,
    collect_answers_custom,
    load_answers_from_file,
    load_saved_answers,
    print_preparation_notes,
    try_load_saved_answers,
)
from lib.types import get_bool


REPO_ROOT = DEPLOY_PATHS.repo_root
REUSE_PREVIOUS = "__FASTINK_REUSE_PREVIOUS__"


def run_command(cmd: list[str], cwd: Path = REPO_ROOT) -> None:
    cli_ui.info(f"+ {' '.join(cmd)}")
    subprocess.run(cmd, cwd=cwd, check=True)


def check_prerequisites() -> None:
    try:
        check_host_prerequisites(require_cvmfs=True)
    except RuntimeError as exc:
        cli_ui.error(str(exc))
        sys.exit(1)


def stage_nginx_tls_material(answers: dict[str, object], paths: dict[str, Path]) -> list[str]:
    notes: list[str] = []
    if not get_bool(answers, "enable_nginx"):
        return notes

    cert_source = str(answers.pop("nginx_cert_source_path", "") or "").strip()
    key_source = str(answers.pop("nginx_key_source_path", "") or "").strip()
    cert_target = paths.get("nginx_cert_path")
    key_target = paths.get("nginx_key_path")
    if cert_target is None or key_target is None:
        return notes

    cert_target = Path(cert_target)
    key_target = Path(key_target)
    if cert_source or key_source:
        if not cert_source or not key_source:
            raise RuntimeError("Both TLS certificate path and private key path must be provided")
        cert_source_path = Path(cert_source).expanduser().resolve()
        key_source_path = Path(key_source).expanduser().resolve()
        if not cert_source_path.exists():
            raise FileNotFoundError(f"TLS certificate not found: {cert_source_path}")
        if not key_source_path.exists():
            raise FileNotFoundError(f"TLS private key not found: {key_source_path}")
        shutil.copy2(cert_source_path, cert_target)
        shutil.copy2(key_source_path, key_target)
        cert_target.chmod(0o644)
        key_target.chmod(0o600)
        notes.append(f"Using user-provided TLS certificate copied into: {cert_target}")
        notes.append(f"Using user-provided TLS private key copied into: {key_target}")
    else:
        notes.append(f"No TLS certificate provided. A self-signed certificate will be created at: {cert_target}")
        notes.append(f"The matching private key will be created at: {key_target}")
    return notes


def build_xrootd_notes(paths: dict[str, Path]) -> list[str]:
    sss_keytab = Path(paths["xrootd_sss_keytab_path"]).resolve()
    krb5_keytab = Path(paths["xrootd_krb5_keytab_path"]).resolve()
    return [
        f"xrootd shared-secret keytab: {sss_keytab}",
        f"xrootd krb5 keytab placeholder path (unused unless you copy one there manually): {krb5_keytab}",
    ]


def validate_krb5_paths(answers: dict[str, object], paths: dict[str, Path]) -> list[str]:
    notes: list[str] = []
    if not get_bool(answers, "enable_krb5"):
        return notes

    krb5_conf_host_path = Path(str(answers.get("krb5_conf_host_path", "")).strip()).expanduser().resolve()
    if not krb5_conf_host_path.exists():
        raise FileNotFoundError(f"Host krb5.conf not found: {krb5_conf_host_path}")
    notes.append(f"Using host krb5.conf from: {krb5_conf_host_path}")

    if get_bool(answers, "enable_xrootd"):
        keytab_text = str(answers.get("xrootd_krb5_keytab_source_path", "")).strip()
        if not keytab_text:
            raise RuntimeError("Kerberos-enabled xrootd requires an xrootd krb5 keytab source path")
        xrootd_krb5_keytab_source_path = Path(keytab_text).expanduser().resolve()
        if not xrootd_krb5_keytab_source_path.exists():
            raise FileNotFoundError(f"xrootd krb5 keytab not found: {xrootd_krb5_keytab_source_path}")

        # Copy keytab into .deploy/ so we can set permissions without touching the original
        keytab_target = paths["xrootd_krb5_keytab_path"]
        keytab_target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(xrootd_krb5_keytab_source_path, keytab_target)
        keytab_target.chmod(0o600)
        answers["xrootd_krb5_keytab_source_path"] = str(keytab_target)
        notes.append(f"xrootd krb5 keytab copied to: {keytab_target} (permissions set to 0600)")

        principal = str(answers.get("xrootd_krb5_principal", "")).strip()
        if not principal:
            raise RuntimeError("Kerberos-enabled xrootd requires an xrootd service principal")
        notes.append(f"Using xrootd krb5 principal: {principal}")
    return notes


def run_init_container(answers: dict[str, object], paths: dict[str, Path]) -> None:
    cli_ui.step("Initialize runtime assets")
    cmd = [
        "docker",
        "run",
        "--rm",
        "-e",
        f"FASTINK_ENABLE_NGINX={'true' if answers.get('enable_nginx') else 'false'}",
        "-e",
        f"FASTINK_ENABLE_XROOTD={'true' if answers.get('enable_xrootd') else 'false'}",
        "-e",
        f"FASTINK_ENABLE_LOCAL_HTCONDOR={'true' if answers.get('enable_local_htcondor') else 'false'}",
        "-e",
        f"FASTINK_HOST_NAME={answers.get('host_name', 'localhost')}",
        "-v",
        f"{paths['etc_init_dir'].resolve()}:/work/etc-init",
        "-v",
        "/etc/passwd:/host-etc/passwd:ro",
        "-v",
        "/etc/group:/host-etc/group:ro",
        "-v",
        "/etc/shadow:/host-etc/shadow:ro",
        "-v",
        "/etc/gshadow:/host-etc/gshadow:ro",
        "-v",
        f"{paths['keys_dir'].resolve()}:/work/keys",
    ]
    if get_bool(answers, "enable_nginx"):
        cmd.extend(["-v", f"{paths['nginx_dir'].resolve()}:/work/nginx"])
    if get_bool(answers, "enable_xrootd"):
        cmd.extend(["-v", f"{paths['xrootd_dir'].resolve()}:/work/xrootd"])
    cmd.append(str(answers["init_image"]))
    run_command(cmd)


def print_post_install_notes(
    answers: dict[str, object],
    paths: dict[str, Path],
    nginx_notes: list[str],
    xrootd_notes: list[str],
    krb5_notes: list[str],
) -> None:
    cli_ui.step("Post-install notes")

    server_private_key = paths.get("server_ssh_private_key_path")
    server_public_key = paths.get("server_ssh_public_key_path")
    if server_private_key and server_public_key:
        cli_ui.info(f"SSH private key for FastINK server: {server_private_key}")
        cli_ui.info(f"SSH public key to distribute to condor/slurm/login nodes: {server_public_key}")
        cli_ui.info("Install that public key into the remote runtime account's authorized_keys before using remote compute backends.")

    if get_bool(answers, "enable_nginx"):
        for note in nginx_notes:
            cli_ui.info(note)

    if get_bool(answers, "enable_xrootd"):
        for note in xrootd_notes:
            cli_ui.info(note)
    if get_bool(answers, "enable_krb5"):
        for note in krb5_notes:
            cli_ui.info(note)

    cli_ui.info("If you plan to use Slurm backends, install and configure a Slurm client on the host, keep sbatch/sacct/scontrol/scancel available, and expose the host munge socket plus Slurm config to the deployment.")


def wait_for_health(url: str, timeout_seconds: int = 90) -> bool:
    deadline = time.time() + timeout_seconds
    handlers = [urllib.request.ProxyHandler({})]
    if url.startswith("https://"):
        handlers.append(urllib.request.HTTPSHandler(context=ssl._create_unverified_context()))
    opener = urllib.request.build_opener(*handlers)
    with cli_ui.spinner("Waiting for health check") as prog:
        while time.time() < deadline:
            try:
                with opener.open(url, timeout=5) as response:
                    if response.status == 200:
                        return True
            except (urllib.error.URLError, TimeoutError, ConnectionError):
                time.sleep(2)
    return False


def main() -> None:
    from lib import cli_ui  # ensure cli_ui available in this scope

    # Re-parse args — fastinkctl dispatcher may have stripped the subcommand name
    parser = argparse.ArgumentParser(
        description="Deploy FastINK. Run without arguments for interactive mode.",
        epilog="Run 'fastinkctl status' to see the current deployment state.",
    )
    parser.add_argument("--profile", choices=["quickstart", "custom"])
    parser.add_argument("--answers-file", type=Path, metavar="PATH")
    parser.add_argument("--set", dest="overrides", action="append", default=[], metavar="KEY=VALUE")
    parser.add_argument("--render-only", action="store_true")
    parser.add_argument("--yes", action="store_true")
    parser.add_argument("--reuse", action="store_true")
    args = parser.parse_args()

    cli_ui.ensure_deps(DEPLOY_DIR)
    cli_ui.banner()
    check_prerequisites()

    # ---- Determine answers ----
    if args.answers_file:
        cli_ui.step("Load answers from file")
        answers = load_answers_from_file(args.answers_file.resolve())
        answers = apply_overrides(answers, args.overrides)
    elif args.reuse:
        cli_ui.step("Reuse existing deployment")
        answers = load_saved_answers()
        answers = apply_overrides(answers, args.overrides)
    else:
        print_preparation_notes()
        previous_answers = try_load_saved_answers()

        if DEPLOY_DIR.exists() and any(
            p for p in DEPLOY_DIR.iterdir() if p.name != ".deps"
        ):
            if not args.yes:
                overwrite = cli_ui.confirm_prompt(f"{DEPLOY_DIR} already exists. Overwrite generated files", True)
                if not overwrite:
                    cli_ui.warning("Aborted.")
                    sys.exit(0)

        profile = args.profile
        if profile is None:
            cli_ui.step("Choose deployment profile")
            profile = cli_ui.choice_prompt(
                "Deployment profile",
                ["quickstart", "custom"],
                "quickstart",
                str(previous_answers["profile"]) if previous_answers and previous_answers.get("profile") else None,
            )

        if profile == "quickstart":
            defaults = default_answers("quickstart", DEPLOY_DIR)
            answers = build_quickstart_answers(defaults)
            answers = apply_overrides(answers, args.overrides)
            if not args.yes:
                confirmed = cli_ui.confirm_prompt("Proceed with this configuration", True)
                if not confirmed:
                    cli_ui.warning("Aborted.")
                    sys.exit(0)
        else:
            answers = collect_answers_custom(previous_answers=previous_answers)
            answers = apply_overrides(answers, args.overrides)

    # ---- Render ----
    paths = build_paths_from_answers(answers)

    nginx_notes = stage_nginx_tls_material(answers, paths)
    xrootd_notes = build_xrootd_notes(paths) if get_bool(answers, "enable_xrootd") else []
    krb5_notes = validate_krb5_paths(answers, paths) if get_bool(answers, "enable_krb5") else []

    cli_ui.step("Render deployment files")
    bundle = render_bundle(
        str(answers["profile"]),
        answers,
        paths,
        DEPLOY_DIR,
        initialize_host_assets=False,
    )
    for relative_path, content in bundle.items():
        write_file(DEPLOY_DIR / relative_path, content)
    write_file(DEPLOY_DIR / "answers.json", json.dumps(answers, indent=2, default=str))

    if args.render_only:
        cli_ui.success(f"Render complete. Deployment files written to: {DEPLOY_DIR}")
        return

    # ---- Build and deploy ----
    build_or_pull_images(answers)
    run_init_container(answers, paths)
    deploy_stack(answers)

    # ---- Health check ----
    health_url = build_health_url(answers)
    cli_ui.step(f"Wait for health check: {health_url}")
    if wait_for_health(health_url):
        cli_ui.success(f"Deployment completed. Health check passed: {health_url}")
        print_post_install_notes(answers, paths, nginx_notes, xrootd_notes, krb5_notes)
        return

    print_post_install_notes(answers, paths, nginx_notes, xrootd_notes, krb5_notes)
    cli_ui.error(f"Services started, but health check did not pass within timeout: {health_url}")
    sys.exit(1)


def build_or_pull_images(answers: dict[str, object]) -> None:
    cli_ui.step("Prepare images")

    if answers["image_source"] == "build":
        images_to_build = [
            ("init", answers["init_image"]),
            ("server", answers["server_image"]),
            ("cron", answers["cron_image"]),
            ("rootbrowse", answers["rootbrowse_image"]),
        ]
        if answers.get("enable_local_htcondor"):
            images_to_build.append(("htcondor", answers["htcondor_image"]))

        total = len(images_to_build)
        if answers.get("enable_xrootd"):
            total += 1

        p, tid = cli_ui.progress_bar("Building images", total=total)
        try:
            for name, tag in images_to_build:
                p.advance(tid) if tid is not None else None
                cmd = ["docker", "build", "-f", f"deploy/images/{name}/Dockerfile", "-t", str(tag)]
                if name == "cron":
                    cmd.extend(["--build-arg", f"BASE_IMAGE={answers['server_image']}"])
                cmd.append(".")
                run_command(cmd)
            if answers.get("enable_xrootd"):
                p.advance(tid) if tid is not None else None
                run_command(["docker", "pull", str(answers["xrootd_image"])])
        finally:
            p.stop()
        return

    p, tid = cli_ui.progress_bar("Pulling images", total=len(list(required_images(answers))))
    try:
        for _, image in required_images(answers):
            p.advance(tid) if tid is not None else None
            run_command(["docker", "pull", image])
    finally:
        p.stop()


def deploy_stack(answers: dict[str, object]) -> None:
    cli_ui.step("Start services with docker compose")
    from lib.compose import compose_up

    project_name = str(answers["project_name"])
    compose_file = (DEPLOY_DIR / "docker-compose.yml").resolve()
    cli_ui.info(f"+ docker compose -p {project_name} -f {compose_file} up -d")
    rc = compose_up(project_name, compose_file)
    if rc != 0:
        cli_ui.error(f"docker compose up failed with exit code {rc}")
        sys.exit(1)


if __name__ == "__main__":
    main()
