#!/usr/bin/env python3
import argparse
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
from typing import Optional

from lib.defaults import default_answers, default_image_answers, normalize_answers, required_images
from lib.host_runtime import check_host_prerequisites
from lib.paths import build_runtime_paths
from lib.render import render_bundle


DEPLOY_ROOT = Path(__file__).resolve().parent
REPO_ROOT = DEPLOY_ROOT.parent
DEPLOY_DIR = REPO_ROOT / ".deploy"
REUSE_PREVIOUS = "__FASTINK_REUSE_PREVIOUS__"


def print_step(message: str) -> None:
    print(f"\n==> {message}")


def ensure_default_extra_mounts_file() -> Path:
    path = DEPLOY_DIR / "extra-mounts.txt"
    path.parent.mkdir(parents=True, exist_ok=True)
    if not path.exists():
        path.write_text("/home/:/home/\n", encoding="utf-8")
    return path


def print_preparation_notes() -> None:
    extra_mounts_path = DEPLOY_DIR / "extra-mounts.txt"
    print_step("Preparation notes")
    print("Prepare these items before continuing if your deployment needs them:")
    print(f"- Optional extra mount list file (one mount per line): {extra_mounts_path}")
    print("  Format: /host/path:/container/path or /host/path:/container/path:ro")
    print("  These mounts are applied to fastink-server, fastink-redis-cron, fastink-rootbrowse, fastink-xrootd, and fastink-htcondor when enabled.")
    print(f"- Optional plugin source or packages under: {DEPLOY_DIR / 'plugins'}")
    print(f"- Optional preload scripts under: {DEPLOY_DIR / 'preload'}")
    print("- Optional existing TLS certificate and private key if you do not want a self-signed certificate.")
    print("- If Kerberos is enabled, prepare a host krb5.conf path to mount into containers.")
    print("- If both Kerberos and xrootd are enabled, prepare the xrootd service keytab path and xrootd service principal.")
    if (DEPLOY_DIR / "answers.json").exists():
        print("- During the interactive questionnaire, type 'r' to reuse the saved value from .deploy/answers.json.")


def prompt_text(label: str, default: str = "", reuse_value: Optional[str] = None) -> str:
    suffix_parts: list[str] = []
    if default:
        suffix_parts.append(default)
    if reuse_value is not None:
        suffix_parts.append("r=reuse saved value")
    suffix = f" [{', '.join(suffix_parts)}]" if suffix_parts else ""
    value = input(f"{label}{suffix}: ").strip()
    if reuse_value is not None and value.lower() == "r":
        return reuse_value
    return value or default


def prompt_secret(label: str, default: str = "", reuse_value: Optional[str] = None) -> str:
    suffix_parts: list[str] = []
    if default:
        suffix_parts.append("press enter to use generated value")
    if reuse_value is not None:
        suffix_parts.append("r=reuse saved value")
    suffix = f" [{' ; '.join(suffix_parts)}]" if suffix_parts else ""
    value = getpass(f"{label}{suffix}: ").strip()
    if reuse_value is not None and value.lower() == "r":
        return reuse_value
    return value or default


def prompt_bool(label: str, default: bool = False, reuse_value: Optional[bool] = None) -> bool:
    default_hint = "Y/n" if default else "y/N"
    if reuse_value is not None:
        saved_hint = "Y" if reuse_value else "N"
        default_hint = f"{default_hint}, r=reuse saved {saved_hint}"
    while True:
        value = input(f"{label} [{default_hint}]: ").strip().lower()
        if not value:
            return default
        if reuse_value is not None and value == "r":
            return reuse_value
        if value in {"y", "yes"}:
            return True
        if value in {"n", "no"}:
            return False
        if reuse_value is not None:
            print("Please answer y, n, or r.")
        else:
            print("Please answer y or n.")


def prompt_int(label: str, default: int, reuse_value: Optional[int] = None) -> int:
    while True:
        value = prompt_text(
            label,
            str(default),
            str(reuse_value) if reuse_value is not None else None,
        )
        try:
            return int(value)
        except ValueError:
            print("Please enter a valid integer.")


def prompt_choice(label: str, options: list[str], default: str, reuse_value: Optional[str] = None) -> str:
    options_display = "/".join(options)
    while True:
        reuse_hint = f", r=reuse saved {reuse_value}" if reuse_value is not None else ""
        value = input(f"{label} [{options_display}] (default: {default}{reuse_hint}): ").strip().lower()
        if not value:
            return default
        if reuse_value is not None and value == "r":
            return reuse_value
        if value in options:
            return value
        if reuse_value is not None:
            print(f"Please choose one of: {options_display}, or r.")
        else:
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


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Interactive installer for the generic FastINK deployment bundle."
    )
    parser.add_argument(
        "--reuse",
        action="store_true",
        help="Reuse the existing .deploy answers and docker-compose files, then start services without rerunning the interactive questionnaire.",
    )
    return parser.parse_args()


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
        f"xrootd krb5 keytab placeholder path (unused unless you copy one there manually): {krb5_keytab}",
    ]


def validate_krb5_paths(answers: dict[str, object]) -> list[str]:
    notes: list[str] = []
    if not bool(answers.get("enable_krb5")):
        return notes

    krb5_conf_host_path = Path(str(answers.get("krb5_conf_host_path", "")).strip()).expanduser().resolve()
    if not krb5_conf_host_path.exists():
        raise FileNotFoundError(f"Host krb5.conf not found: {krb5_conf_host_path}")
    notes.append(f"Using host krb5.conf from: {krb5_conf_host_path}")

    if bool(answers.get("enable_xrootd")):
        keytab_text = str(answers.get("xrootd_krb5_keytab_source_path", "")).strip()
        if not keytab_text:
            raise RuntimeError("Kerberos-enabled xrootd requires an xrootd krb5 keytab source path")
        xrootd_krb5_keytab_source_path = Path(keytab_text).expanduser().resolve()
        if not xrootd_krb5_keytab_source_path.exists():
            raise FileNotFoundError(f"xrootd krb5 keytab not found: {xrootd_krb5_keytab_source_path}")
        notes.append(f"Using xrootd krb5 keytab from: {xrootd_krb5_keytab_source_path}")
        principal = str(answers.get("xrootd_krb5_principal", "")).strip()
        if not principal:
            raise RuntimeError("Kerberos-enabled xrootd requires an xrootd service principal")
        notes.append(f"Using xrootd krb5 principal: {principal}")
    return notes


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
    if bool(answers.get("enable_nginx")):
        cmd.extend(["-v", f"{paths['nginx_dir'].resolve()}:/work/nginx"])
    if bool(answers.get("enable_xrootd")):
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
    if bool(answers.get("enable_krb5")):
        for note in krb5_notes:
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


def load_saved_answers() -> dict[str, object]:
    answers_path = DEPLOY_DIR / "answers.json"
    if not answers_path.exists():
        print(f"Saved answers file not found: {answers_path}", file=sys.stderr)
        sys.exit(1)
    try:
        answers = json.loads(answers_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        print(f"Failed to parse saved answers file {answers_path}: {exc}", file=sys.stderr)
        sys.exit(1)
    finalize_mount_answers(answers)
    profile = str(answers.get("profile") or "minimal")
    return normalize_answers(answers, profile=profile, deploy_dir=DEPLOY_DIR)


def try_load_saved_answers() -> Optional[dict[str, object]]:
    answers_path = DEPLOY_DIR / "answers.json"
    if not answers_path.exists():
        return None
    try:
        answers = json.loads(answers_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return None
    finalize_mount_answers(answers)
    profile = str(answers.get("profile") or "minimal")
    return normalize_answers(answers, profile=profile, deploy_dir=DEPLOY_DIR)


def build_paths_from_answers(answers: dict[str, object]) -> dict[str, Path]:
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
    return paths


def finalize_mount_answers(answers: dict[str, object]) -> dict[str, object]:
    if (
        bool(answers.get("enable_xrootd")) or bool(answers.get("enable_local_htcondor"))
    ) and not str(answers.get("extra_mounts_file") or "").strip():
        answers["extra_mounts_file"] = str(ensure_default_extra_mounts_file().resolve())
    return answers


def default_htcondor_internal_domain(host_name: str, fallback: str) -> str:
    host_name = host_name.strip()
    if "." not in host_name:
        return fallback
    suffix = ".".join(part for part in host_name.split(".")[1:] if part)
    return suffix or fallback


def collect_answers(previous_answers: Optional[dict[str, object]] = None) -> dict[str, object]:
    print_step("Choose deployment profile")
    profile = prompt_choice(
        "Deployment profile",
        ["minimal", "full"],
        "minimal",
        str(previous_answers["profile"]) if previous_answers and previous_answers.get("profile") else None,
    )
    defaults = default_answers(profile, DEPLOY_DIR)

    print_step("Choose image source")
    image_source = prompt_choice(
        "Image source",
        ["build", "pull"],
        str(defaults["image_source"]),
        str(previous_answers["image_source"]) if previous_answers and previous_answers.get("image_source") else None,
    )
    image_defaults = default_image_answers(image_source)

    if image_source == "build":
        server_image = prompt_text("Server image tag", str(image_defaults["server_image"]), str(previous_answers["server_image"]) if previous_answers and previous_answers.get("server_image") else None)
        cron_image = prompt_text("Cron image tag", str(image_defaults["cron_image"]), str(previous_answers["cron_image"]) if previous_answers and previous_answers.get("cron_image") else None)
        rootbrowse_image = prompt_text("Rootbrowse image tag", str(image_defaults["rootbrowse_image"]), str(previous_answers["rootbrowse_image"]) if previous_answers and previous_answers.get("rootbrowse_image") else None)
    else:
        server_image = prompt_text("Server image reference", str(image_defaults["server_image"]), str(previous_answers["server_image"]) if previous_answers and previous_answers.get("server_image") else None)
        cron_image = prompt_text("Cron image reference", str(image_defaults["cron_image"]), str(previous_answers["cron_image"]) if previous_answers and previous_answers.get("cron_image") else None)
        rootbrowse_image = prompt_text("Rootbrowse image reference", str(image_defaults["rootbrowse_image"]), str(previous_answers["rootbrowse_image"]) if previous_answers and previous_answers.get("rootbrowse_image") else None)
    xrootd_image = prompt_text("Xrootd image reference", str(image_defaults["xrootd_image"]), str(previous_answers["xrootd_image"]) if previous_answers and previous_answers.get("xrootd_image") else None)
    htcondor_image_default = str(image_defaults["htcondor_image"])

    print_step("Basic deployment settings")
    project_name = prompt_text("Compose project name", str(defaults["project_name"]), str(previous_answers["project_name"]) if previous_answers and previous_answers.get("project_name") else None)
    data_root = Path(
        prompt_text(
            "Data directory",
            str(defaults["data_root"]),
            str(previous_answers["data_root"]) if previous_answers and previous_answers.get("data_root") else None,
        )
    ).resolve()
    enable_nginx = prompt_bool("Enable nginx HTTPS reverse proxy", bool(defaults["enable_nginx"]), bool(previous_answers["enable_nginx"]) if previous_answers and previous_answers.get("enable_nginx") is not None else None)
    enable_xrootd = prompt_bool("Enable xrootd service container", bool(defaults["enable_xrootd"]), bool(previous_answers["enable_xrootd"]) if previous_answers and previous_answers.get("enable_xrootd") is not None else None)
    enable_krb5 = prompt_bool(
        "Enable Kerberos",
        bool(defaults["enable_krb5"]),
        bool(previous_answers["enable_krb5"]) if previous_answers and previous_answers.get("enable_krb5") is not None else None,
    )
    enable_local_htcondor = prompt_bool("Enable local HTCondor all-in-one container", bool(defaults["enable_local_htcondor"]), bool(previous_answers["enable_local_htcondor"]) if previous_answers and previous_answers.get("enable_local_htcondor") is not None else None)
    enable_host_slurm_client = prompt_bool(
        "Expose host Slurm client config and munge socket",
        bool(defaults["enable_host_slurm_client"]),
        bool(previous_answers["enable_host_slurm_client"]) if previous_answers and previous_answers.get("enable_host_slurm_client") is not None else None,
    )
    htcondor_image = htcondor_image_default
    if enable_local_htcondor:
        if image_source == "build":
            htcondor_image = prompt_text("HTCondor image tag", htcondor_image_default, str(previous_answers["htcondor_image"]) if previous_answers and previous_answers.get("htcondor_image") else None)
        else:
            htcondor_image = prompt_text("HTCondor image reference", htcondor_image_default, str(previous_answers["htcondor_image"]) if previous_answers and previous_answers.get("htcondor_image") else None)
    host_name = prompt_text("Public host name", str(defaults["host_name"]), str(previous_answers["host_name"]) if previous_answers and previous_answers.get("host_name") else None)
    htcondor_internal_domain = prompt_text(
        "HTCondor internal domain",
        default_htcondor_internal_domain(str(host_name), str(defaults["htcondor_internal_domain"])),
        str(previous_answers["htcondor_internal_domain"]) if previous_answers and previous_answers.get("htcondor_internal_domain") else None,
    )
    host_port_default = 443 if enable_nginx and int(defaults["host_port"]) == 8000 else int(defaults["host_port"])
    host_port = prompt_int("Public HTTPS port" if enable_nginx else "Public port", host_port_default, int(previous_answers["host_port"]) if previous_answers and previous_answers.get("host_port") is not None else None)
    rootbrowse_port = prompt_int("Rootbrowse port", int(defaults["rootbrowse_port"]), int(previous_answers["rootbrowse_port"]) if previous_answers and previous_answers.get("rootbrowse_port") is not None else None)
    xrootd_port = prompt_int("Xrootd port", int(defaults["xrootd_port"]), int(previous_answers["xrootd_port"]) if previous_answers and previous_answers.get("xrootd_port") is not None else None)
    if enable_local_htcondor:
        schedd_host = "schedd@fastink-htcondor"
        cm_host = "fastink-htcondor"
    else:
        schedd_host = prompt_text("HTCondor schedd host", str(defaults["schedd_host"]), str(previous_answers["schedd_host"]) if previous_answers and previous_answers.get("schedd_host") else None)
        cm_host = prompt_text("HTCondor collector/CM host", str(defaults["cm_host"]), str(previous_answers["cm_host"]) if previous_answers and previous_answers.get("cm_host") else None)
    htcondor_default_request_cpus = prompt_int(
        "Default HTCondor job CPUs",
        int(defaults["htcondor_default_request_cpus"]),
        int(previous_answers["htcondor_default_request_cpus"]) if previous_answers and previous_answers.get("htcondor_default_request_cpus") is not None else None,
    )
    htcondor_default_request_memory = prompt_int(
        "Default HTCondor job memory (MB)",
        int(defaults["htcondor_default_request_memory"]),
        int(previous_answers["htcondor_default_request_memory"]) if previous_answers and previous_answers.get("htcondor_default_request_memory") is not None else None,
    )
    krb5_conf_host_path = str(defaults["krb5_conf_host_path"])
    xrootd_krb5_keytab_source_path = ""
    xrootd_krb5_principal = ""
    if enable_krb5:
        krb5_conf_host_path = prompt_text(
            "Host krb5.conf path",
            str(defaults["krb5_conf_host_path"]),
            str(previous_answers["krb5_conf_host_path"]) if previous_answers and previous_answers.get("krb5_conf_host_path") else None,
        )
        if enable_xrootd:
            xrootd_krb5_keytab_source_path = prompt_text(
                "xrootd krb5 keytab source path",
                "",
                str(previous_answers["xrootd_krb5_keytab_source_path"]) if previous_answers and previous_answers.get("xrootd_krb5_keytab_source_path") else None,
            )
            xrootd_krb5_principal = prompt_text(
                "xrootd krb5 service principal",
                "",
                str(previous_answers["xrootd_krb5_principal"]) if previous_answers and previous_answers.get("xrootd_krb5_principal") else None,
            )
    slurm_conf_host_path = str(defaults["slurm_conf_host_path"])
    munge_socket_dir = str(defaults["munge_socket_dir"])
    if enable_host_slurm_client:
        slurm_conf_host_path = prompt_text(
            "Host slurm.conf path",
            str(defaults["slurm_conf_host_path"]),
            str(previous_answers["slurm_conf_host_path"]) if previous_answers and previous_answers.get("slurm_conf_host_path") else None,
        )
        munge_socket_dir = prompt_text(
            "Host munge socket directory",
            str(defaults["munge_socket_dir"]),
            str(previous_answers["munge_socket_dir"]) if previous_answers and previous_answers.get("munge_socket_dir") else None,
        )
    ink_production = prompt_bool("Run FastINK in production mode", bool(defaults["ink_production"]), bool(previous_answers["ink_production"]) if previous_answers and previous_answers.get("ink_production") is not None else None)
    workers = int(defaults["workers"])
    if ink_production:
        workers = prompt_int("Uvicorn workers in production mode", int(defaults["workers"]), int(previous_answers["workers"]) if previous_answers and previous_answers.get("workers") is not None else None)
    init_database = prompt_bool("Initialize database on container start", bool(defaults["init_database"]), bool(previous_answers["init_database"]) if previous_answers and previous_answers.get("init_database") is not None else None)

    nginx_cert_source_path = ""
    nginx_key_source_path = ""
    extra_mounts_file_default = str((DEPLOY_DIR / "extra-mounts.txt").resolve())
    extra_mounts_file = ""
    previous_extra_mounts = str(previous_answers["extra_mounts_file"]) if previous_answers and previous_answers.get("extra_mounts_file") else None
    if prompt_bool("Use an extra mount list file", False, bool(previous_extra_mounts) if previous_extra_mounts is not None else None):
        ensure_default_extra_mounts_file()
        extra_mounts_file = prompt_text("Extra mount list file path", extra_mounts_file_default, previous_extra_mounts)
    previous_has_tls = bool(previous_answers and previous_answers.get("nginx_cert_source_path") and previous_answers.get("nginx_key_source_path"))
    if enable_nginx and prompt_bool("Use an existing TLS certificate and key", False, previous_has_tls if previous_answers is not None else None):
        nginx_cert_source_path = prompt_text("TLS certificate path", "", str(previous_answers["nginx_cert_source_path"]) if previous_answers and previous_answers.get("nginx_cert_source_path") else None)
        nginx_key_source_path = prompt_text("TLS private key path", "", str(previous_answers["nginx_key_source_path"]) if previous_answers and previous_answers.get("nginx_key_source_path") else None)

    print_step("Runtime credentials")
    db_name = prompt_text("Database name", str(defaults["db_name"]), str(previous_answers["db_name"]) if previous_answers and previous_answers.get("db_name") else None)
    db_user = prompt_text("Database user", str(defaults["db_user"]), str(previous_answers["db_user"]) if previous_answers and previous_answers.get("db_user") else None)
    db_root_password = prompt_secret("Database root password", secrets.token_urlsafe(18), str(previous_answers["db_root_password"]) if previous_answers and previous_answers.get("db_root_password") else None)
    db_password = prompt_secret("Database user password", secrets.token_urlsafe(18), str(previous_answers["db_password"]) if previous_answers and previous_answers.get("db_password") else None)
    redis_password = prompt_secret("Redis password", secrets.token_urlsafe(18), str(previous_answers["redis_password"]) if previous_answers and previous_answers.get("redis_password") else None)

    answers = {
        "profile": profile,
        "image_source": image_source,
        "server_image": server_image,
        "cron_image": cron_image,
        "rootbrowse_image": rootbrowse_image,
        "xrootd_image": xrootd_image,
        "htcondor_image": htcondor_image,
        "project_name": project_name,
        "data_root": data_root,
        "enable_nginx": enable_nginx,
        "enable_xrootd": enable_xrootd,
        "enable_krb5": enable_krb5,
        "enable_local_htcondor": enable_local_htcondor,
        "enable_host_slurm_client": enable_host_slurm_client,
        "host_name": host_name,
        "htcondor_internal_domain": htcondor_internal_domain,
        "host_port": host_port,
        "rootbrowse_port": rootbrowse_port,
        "xrootd_port": xrootd_port,
        "schedd_host": schedd_host,
        "cm_host": cm_host,
        "htcondor_default_request_cpus": htcondor_default_request_cpus,
        "htcondor_default_request_memory": htcondor_default_request_memory,
        "krb5_conf_host_path": krb5_conf_host_path,
        "xrootd_krb5_keytab_source_path": xrootd_krb5_keytab_source_path,
        "xrootd_krb5_principal": xrootd_krb5_principal,
        "slurm_conf_host_path": slurm_conf_host_path,
        "munge_socket_dir": munge_socket_dir,
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
        "extra_mounts_file": extra_mounts_file,
        "server_preload_script_dirs": str(defaults["server_preload_script_dirs"]),
        "server_preload_scripts": str(defaults["server_preload_scripts"]),
        "cron_preload_script_dirs": str(defaults["cron_preload_script_dirs"]),
        "cron_preload_scripts": str(defaults["cron_preload_scripts"]),
        "rootbrowse_preload_script_dirs": str(defaults["rootbrowse_preload_script_dirs"]),
        "rootbrowse_preload_scripts": str(defaults["rootbrowse_preload_scripts"]),
        "nginx_cert_source_path": nginx_cert_source_path,
        "nginx_key_source_path": nginx_key_source_path,
    }
    finalize_mount_answers(answers)
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
        if answers.get("enable_local_htcondor"):
            run_command(
                [
                    "docker",
                    "build",
                    "-f",
                    "deploy/images/htcondor/Dockerfile",
                    "-t",
                    str(answers["htcondor_image"]),
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
    args = parse_args()
    check_prerequisites()
    if args.reuse:
        print_step("Reuse existing deployment")
        answers = load_saved_answers()
        paths = build_paths_from_answers(answers)
        nginx_notes: list[str] = []
        xrootd_notes = build_xrootd_notes(paths) if bool(answers.get("enable_xrootd")) else []
        krb5_notes = validate_krb5_paths(answers) if bool(answers.get("enable_krb5")) else []
        deploy_stack(answers)
    else:
        print_preparation_notes()
        previous_answers = try_load_saved_answers()

        if DEPLOY_DIR.exists() and any(DEPLOY_DIR.iterdir()):
            overwrite = prompt_bool(f"{DEPLOY_DIR} already exists. Overwrite generated files", True)
            if not overwrite:
                print("Aborted.")
                sys.exit(0)

        answers = collect_answers(previous_answers=previous_answers)
        paths = build_paths_from_answers(answers)

        nginx_notes = stage_nginx_tls_material(answers, paths)
        xrootd_notes = build_xrootd_notes(paths) if bool(answers.get("enable_xrootd")) else []
        krb5_notes = validate_krb5_paths(answers) if bool(answers.get("enable_krb5")) else []

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
        print_post_install_notes(answers, paths, nginx_notes, xrootd_notes, krb5_notes)
        return

    print_post_install_notes(answers, paths, nginx_notes, xrootd_notes, krb5_notes)
    print(f"Services started, but health check did not pass within timeout: {health_url}", file=sys.stderr)
    sys.exit(1)


if __name__ == "__main__":
    main()
