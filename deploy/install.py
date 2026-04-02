#!/usr/bin/env python3
import json
import secrets
import shutil
import ssl
import subprocess
import sys
import time
import urllib.error
import urllib.request
from getpass import getpass
from pathlib import Path

from lib.defaults import default_answers, default_image_answers, normalize_answers, required_images
from lib.host_runtime import check_host_prerequisites
from lib.paths import build_runtime_paths
from lib.render import render_bundle


DEPLOY_ROOT = Path(__file__).resolve().parent
REPO_ROOT = DEPLOY_ROOT.parent
DEPLOY_DIR = REPO_ROOT / ".deploy"


def print_step(message: str) -> None:
    print(f"\n==> {message}")


def prompt_text(label: str, default: str = "") -> str:
    suffix = f" [{default}]" if default else ""
    value = input(f"{label}{suffix}: ").strip()
    return value or default


def prompt_secret(label: str, default: str = "") -> str:
    suffix = " [press enter to use generated value]" if default else ""
    value = getpass(f"{label}{suffix}: ").strip()
    return value or default


def prompt_bool(label: str, default: bool = False) -> bool:
    default_hint = "Y/n" if default else "y/N"
    while True:
        value = input(f"{label} [{default_hint}]: ").strip().lower()
        if not value:
            return default
        if value in {"y", "yes"}:
            return True
        if value in {"n", "no"}:
            return False
        print("Please answer y or n.")


def prompt_int(label: str, default: int) -> int:
    while True:
        value = prompt_text(label, str(default))
        try:
            return int(value)
        except ValueError:
            print("Please enter a valid integer.")


def prompt_choice(label: str, options: list[str], default: str) -> str:
    options_display = "/".join(options)
    while True:
        value = input(f"{label} [{options_display}] (default: {default}): ").strip().lower()
        if not value:
            return default
        if value in options:
            return value
        print(f"Please choose one of: {options_display}")


def run_command(cmd: list[str], cwd: Path = REPO_ROOT) -> None:
    print(f"+ {' '.join(cmd)}")
    subprocess.run(cmd, cwd=cwd, check=True)


def check_prerequisites() -> None:
    try:
        check_host_prerequisites(require_cvmfs=True)
    except RuntimeError as exc:
        print(str(exc), file=sys.stderr)
        sys.exit(1)


def write_file(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def annotate_runtime_asset_paths(paths: dict[str, Path]) -> None:
    server_private_key = Path(
        paths.get("server_ssh_private_key_path", paths["keys_dir"] / "ssh-client" / "id_rsa")
    ).resolve()
    paths["server_ssh_private_key_path"] = server_private_key
    paths["server_ssh_public_key_path"] = (server_private_key.parent / "id_rsa.pub").resolve()
    paths["rootbrowse_authorized_keys_path"] = Path(
        paths.get("rootbrowse_authorized_keys_path", paths["keys_dir"] / "rootbrowse_authorized_keys")
    ).resolve()


def stage_nginx_tls_material(answers: dict[str, object], paths: dict[str, Path]) -> list[str]:
    notes: list[str] = []
    if not bool(answers.get("enable_nginx")):
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
        f"If Kerberos-backed xrootd is required, ask your krb5 administrator to place a service keytab at: {krb5_keytab}",
    ]


def run_init_container(answers: dict[str, object], paths: dict[str, Path]) -> None:
    print_step("Initialize runtime assets")
    cmd = [
        "docker",
        "run",
        "--rm",
        "-e",
        f"FASTINK_ENABLE_NGINX={'true' if answers.get('enable_nginx') else 'false'}",
        "-e",
        f"FASTINK_ENABLE_XROOTD={'true' if answers.get('enable_xrootd') else 'false'}",
        "-e",
        f"FASTINK_HOST_NAME={answers.get('host_name', 'localhost')}",
        "-v",
        f"{paths['keys_dir'].resolve()}:/work/keys",
    ]
    if bool(answers.get("enable_nginx")):
        cmd.extend(["-v", f"{paths['nginx_dir'].resolve()}:/work/nginx"])
    if bool(answers.get("enable_xrootd")):
        cmd.extend(["-v", f"{paths['xrootd_dir'].resolve()}:/work/xrootd"])
    cmd.append(str(answers["init_image"]))
    run_command(cmd)



def print_post_install_notes(answers: dict[str, object], paths: dict[str, Path], nginx_notes: list[str], xrootd_notes: list[str]) -> None:
    print_step("Post-install notes")

    server_private_key = paths.get("server_ssh_private_key_path")
    server_public_key = paths.get("server_ssh_public_key_path")
    if server_private_key and server_public_key:
        print(f"SSH private key for FastINK server: {server_private_key}")
        print(f"SSH public key to distribute to condor/slurm/login nodes: {server_public_key}")
        print("Install that public key into the remote runtime account's authorized_keys before using remote compute backends.")

    if bool(answers.get("enable_nginx")):
        for note in nginx_notes:
            print(note)

    if bool(answers.get("enable_xrootd")):
        for note in xrootd_notes:
            print(note)

    print("If you plan to use Slurm backends, install and configure a Slurm client on the host, keep sbatch/sacct/scontrol/scancel available, and expose the host munge socket plus Slurm config to the deployment.")


def wait_for_health(url: str, timeout_seconds: int = 90) -> bool:
    deadline = time.time() + timeout_seconds
    handlers = [urllib.request.ProxyHandler({})]
    if url.startswith("https://"):
        handlers.append(urllib.request.HTTPSHandler(context=ssl._create_unverified_context()))
    opener = urllib.request.build_opener(*handlers)
    while time.time() < deadline:
        try:
            with opener.open(url, timeout=5) as response:
                if response.status == 200:
                    return True
        except (urllib.error.URLError, TimeoutError, ConnectionError):
            time.sleep(2)
    return False


def collect_answers() -> dict[str, object]:
    print_step("Choose deployment profile")
    profile = prompt_choice("Deployment profile", ["minimal", "full"], "minimal")
    defaults = default_answers(profile, DEPLOY_DIR)

    print_step("Choose image source")
    image_source = prompt_choice("Image source", ["build", "pull"], str(defaults["image_source"]))
    image_defaults = default_image_answers(image_source)

    if image_source == "build":
        server_image = prompt_text("Server image tag", str(image_defaults["server_image"]))
        cron_image = prompt_text("Cron image tag", str(image_defaults["cron_image"]))
        rootbrowse_image = prompt_text("Rootbrowse image tag", str(image_defaults["rootbrowse_image"]))
    else:
        server_image = prompt_text("Server image reference", str(image_defaults["server_image"]))
        cron_image = prompt_text("Cron image reference", str(image_defaults["cron_image"]))
        rootbrowse_image = prompt_text("Rootbrowse image reference", str(image_defaults["rootbrowse_image"]))
    xrootd_image = prompt_text("Xrootd image reference", str(image_defaults["xrootd_image"]))

    print_step("Basic deployment settings")
    project_name = prompt_text("Compose project name", str(defaults["project_name"]))
    data_root = Path(prompt_text("Data directory", str(defaults["data_root"]))).resolve()
    enable_nginx = prompt_bool("Enable nginx HTTPS reverse proxy", bool(defaults["enable_nginx"]))
    enable_xrootd = prompt_bool("Enable local xrootd service", bool(defaults["enable_xrootd"]))
    host_name = prompt_text("Public host name", str(defaults["host_name"]))
    host_port_default = 443 if enable_nginx and int(defaults["host_port"]) == 8000 else int(defaults["host_port"])
    host_port = prompt_int("Public HTTPS port" if enable_nginx else "Public port", host_port_default)
    rootbrowse_port = prompt_int("Rootbrowse port", int(defaults["rootbrowse_port"]))
    xrootd_port = prompt_int("Xrootd port", int(defaults["xrootd_port"]))
    workers = prompt_int("Uvicorn workers in production mode", int(defaults["workers"]))
    ink_production = prompt_bool("Run FastINK in production mode", bool(defaults["ink_production"]))
    init_database = prompt_bool("Initialize database on container start", bool(defaults["init_database"]))

    nginx_cert_source_path = ""
    nginx_key_source_path = ""
    if enable_nginx and prompt_bool("Use an existing TLS certificate and key", False):
        nginx_cert_source_path = prompt_text("TLS certificate path")
        nginx_key_source_path = prompt_text("TLS private key path")

    print_step("Runtime credentials")
    db_name = prompt_text("Database name", str(defaults["db_name"]))
    db_user = prompt_text("Database user", str(defaults["db_user"]))
    db_root_password = prompt_secret("Database root password", secrets.token_urlsafe(18))
    db_password = prompt_secret("Database user password", secrets.token_urlsafe(18))
    redis_password = prompt_secret("Redis password", secrets.token_urlsafe(18))

    answers = {
        "profile": profile,
        "image_source": image_source,
        "server_image": server_image,
        "cron_image": cron_image,
        "rootbrowse_image": rootbrowse_image,
        "xrootd_image": xrootd_image,
        "project_name": project_name,
        "data_root": data_root,
        "enable_nginx": enable_nginx,
        "enable_xrootd": enable_xrootd,
        "host_name": host_name,
        "host_port": host_port,
        "rootbrowse_port": rootbrowse_port,
        "xrootd_port": xrootd_port,
        "workers": workers,
        "ink_production": ink_production,
        "init_database": init_database,
        "db_name": db_name,
        "db_user": db_user,
        "db_root_password": db_root_password,
        "db_password": db_password,
        "redis_password": redis_password,
        "plugin_pip_packages": str(defaults["plugin_pip_packages"]),
        "plugin_editable_dirs": str(defaults["plugin_editable_dirs"]),
        "server_preload_script_dirs": str(defaults["server_preload_script_dirs"]),
        "server_preload_scripts": str(defaults["server_preload_scripts"]),
        "cron_preload_script_dirs": str(defaults["cron_preload_script_dirs"]),
        "cron_preload_scripts": str(defaults["cron_preload_scripts"]),
        "rootbrowse_preload_script_dirs": str(defaults["rootbrowse_preload_script_dirs"]),
        "rootbrowse_preload_scripts": str(defaults["rootbrowse_preload_scripts"]),
        "nginx_cert_source_path": nginx_cert_source_path,
        "nginx_key_source_path": nginx_key_source_path,
    }
    return normalize_answers(answers, profile=profile, deploy_dir=DEPLOY_DIR)


def build_or_pull_images(answers: dict[str, object]) -> None:
    print_step("Prepare images")

    if answers["image_source"] == "build":
        run_command(
            [
                "docker",
                "build",
                "-f",
                "deploy/images/init/Dockerfile",
                "-t",
                str(answers["init_image"]),
                ".",
            ]
        )
        run_command(
            [
                "docker",
                "build",
                "-f",
                "deploy/images/server/Dockerfile",
                "-t",
                str(answers["server_image"]),
                ".",
            ]
        )
        run_command(
            [
                "docker",
                "build",
                "--build-arg",
                f"BASE_IMAGE={answers['server_image']}",
                "-f",
                "deploy/images/cron/Dockerfile",
                "-t",
                str(answers["cron_image"]),
                ".",
            ]
        )
        run_command(
            [
                "docker",
                "build",
                "-f",
                "deploy/images/rootbrowse/Dockerfile",
                "-t",
                str(answers["rootbrowse_image"]),
                ".",
            ]
        )
        if answers.get("enable_xrootd"):
            run_command(["docker", "pull", str(answers["xrootd_image"])])
        return

    for _, image in required_images(answers):
        run_command(["docker", "pull", image])


def deploy_stack(answers: dict[str, object]) -> None:
    print_step("Start services with docker compose")
    run_command(
        [
            "docker",
            "compose",
            "-p",
            str(answers["project_name"]),
            "-f",
            str((DEPLOY_DIR / "docker-compose.yml").resolve()),
            "up",
            "-d",
        ]
    )


def main() -> None:
    check_prerequisites()

    if DEPLOY_DIR.exists() and any(DEPLOY_DIR.iterdir()):
        overwrite = prompt_bool(f"{DEPLOY_DIR} already exists. Overwrite generated files", True)
        if not overwrite:
            print("Aborted.")
            sys.exit(0)

    answers = collect_answers()
    _, paths = build_runtime_paths(
        output_dir=DEPLOY_DIR,
        data_root=Path(answers["data_root"]),
        enable_nginx=bool(answers["enable_nginx"]),
        enable_xrootd=bool(answers.get("enable_xrootd", False)),
        db_data_dir=Path(answers["data_root"]) / "db",
        redis_data_dir=Path(answers["data_root"]) / "redis",
        etc_init_dir=Path(answers["data_root"]) / "etc-init",
        tmp_dir=Path(answers["data_root"]) / "tmp",
        plugins_dir=DEPLOY_DIR / "plugins",
        keys_dir=DEPLOY_DIR / "keys",
        preload_server_dir=DEPLOY_DIR / "preload" / "server",
        preload_cron_dir=DEPLOY_DIR / "preload" / "cron",
        preload_rootbrowse_dir=DEPLOY_DIR / "preload" / "rootbrowse",
    )
    annotate_runtime_asset_paths(paths)

    nginx_notes = stage_nginx_tls_material(answers, paths)
    xrootd_notes = build_xrootd_notes(paths) if bool(answers.get("enable_xrootd")) else []

    print_step("Render deployment files")
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

    build_or_pull_images(answers)
    run_init_container(answers, paths)
    deploy_stack(answers)

    public_base_url = str(answers["public_base_url"])
    health_url = f"{public_base_url}/health"
    print_step(f"Wait for health check: {health_url}")
    if wait_for_health(health_url):
        print(f"Deployment completed. Health check passed: {health_url}")
        print_post_install_notes(answers, paths, nginx_notes, xrootd_notes)
        return

    print_post_install_notes(answers, paths, nginx_notes, xrootd_notes)
    print(f"Services started, but health check did not pass within timeout: {health_url}", file=sys.stderr)
    sys.exit(1)


if __name__ == "__main__":
    main()
