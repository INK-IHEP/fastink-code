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
from pathlib import Path
from typing import Optional

_HERE = Path(__file__).resolve().parent
if str(_HERE.parent) not in sys.path:
    sys.path.insert(0, str(_HERE.parent))

from lib.deploy_io import resolve_deploy_paths

_DEPLOY_PATHS = resolve_deploy_paths()

from lib import cli_ui
from lib.defaults import default_answers, default_image_answers, normalize_answers, parse_override_value, required_images
from lib.types import get_bool
from lib.host_runtime import check_host_prerequisites
from lib.paths import build_runtime_paths
from lib.render import render_bundle


REPO_ROOT = _DEPLOY_PATHS.repo_root
DEPLOY_DIR = _DEPLOY_PATHS.deploy_dir
REUSE_PREVIOUS = "__FASTINK_REUSE_PREVIOUS__"


def ensure_default_extra_mounts_file() -> Path:
    path = DEPLOY_DIR / "extra-mounts.txt"
    path.parent.mkdir(parents=True, exist_ok=True)
    if not path.exists():
        path.write_text("/home/:/home/\n", encoding="utf-8")
    return path


def print_preparation_notes() -> None:
    extra_mounts_path = DEPLOY_DIR / "extra-mounts.txt"
    cli_ui.step("Preparation notes")
    cli_ui.info("Prepare these items before continuing if your deployment needs them:")
    cli_ui.info(f"Optional extra mount list file (one mount per line): {extra_mounts_path}")
    cli_ui.info("  Format: /host/path:/container/path or /host/path:/container/path:ro")
    cli_ui.info("  These mounts are applied to fastink-server, fastink-redis-cron, fastink-rootbrowse, fastink-xrootd, and fastink-htcondor when enabled.")
    cli_ui.info(f"Optional plugin source or packages under: {DEPLOY_DIR / 'plugins'}")
    cli_ui.info(f"Optional preload scripts under: {DEPLOY_DIR / 'preload'}")
    cli_ui.info("Optional existing TLS certificate and private key if you do not want a self-signed certificate.")
    cli_ui.info("If Kerberos is enabled, prepare a host krb5.conf path to mount into containers.")
    cli_ui.info("If both Kerberos and xrootd are enabled, prepare the xrootd service keytab path and xrootd service principal.")
    if (DEPLOY_DIR / "answers.json").exists():
        cli_ui.info("During the interactive questionnaire, type 'r' to reuse the saved value from .deploy/answers.json.")


def run_command(cmd: list[str], cwd: Path = REPO_ROOT) -> None:
    cli_ui.info(f"+ {' '.join(cmd)}")
    subprocess.run(cmd, cwd=cwd, check=True)


def check_prerequisites() -> None:
    try:
        check_host_prerequisites(require_cvmfs=True)
    except RuntimeError as exc:
        cli_ui.error(str(exc))
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


def load_deploy_answers() -> dict[str, object]:
    """Load raw answers from .deploy/answers.json (no normalization).

    Used by lightweight subcommands (destroy, down, up) that only need
    project_name and a few other keys.
    """
    answers_path = DEPLOY_DIR / "answers.json"
    if not answers_path.exists():
        cli_ui.error(f"No deployment found at {DEPLOY_DIR}")
        sys.exit(1)
    try:
        return json.loads(answers_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        cli_ui.error(f"Failed to parse answers file {answers_path}: {exc}")
        sys.exit(1)


def load_saved_answers() -> dict[str, object]:
    return load_answers_from_file(DEPLOY_DIR / "answers.json")


def try_load_saved_answers() -> Optional[dict[str, object]]:
    """Load saved answers, returning None instead of exiting on error."""
    try:
        return load_saved_answers()
    except SystemExit:
        return None


def build_paths_from_answers(answers: dict[str, object]) -> dict[str, Path]:
    _, paths = build_runtime_paths(
        output_dir=DEPLOY_DIR,
        data_root=Path(answers["data_root"]),
        enable_nginx=get_bool(answers, "enable_nginx"),
        enable_xrootd=get_bool(answers, "enable_xrootd"),
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
        get_bool(answers, "enable_xrootd") or get_bool(answers, "enable_local_htcondor")
    ) and not str(answers.get("extra_mounts_file") or "").strip():
        answers["extra_mounts_file"] = str(ensure_default_extra_mounts_file().resolve())
    return answers


def default_htcondor_internal_domain(host_name: str, fallback: str) -> str:
    host_name = host_name.strip()
    if "." not in host_name:
        return fallback
    suffix = ".".join(part for part in host_name.split(".")[1:] if part)
    return suffix or fallback


def build_quickstart_answers(defaults: dict[str, object]) -> dict[str, object]:
    cli_ui.step("Quickstart profile")
    cli_ui.summary_table([
        ("Project", str(defaults["project_name"])),
        ("Host", f'{defaults["host_name"]}:{defaults["host_port"]}'),
        ("Data directory", str(defaults["data_root"])),
        ("Images", "pull (official)"),
        ("Services", "db, redis, server, cron, rootbrowse, xrootd, htcondor"),
        ("nginx", "off"),
        ("Kerberos", "off"),
        ("Host Slurm", "off"),
        ("Database name", str(defaults["db_name"])),
        ("Database user", str(defaults["db_user"])),
    ])
    print()

    answers = {
        "profile": "quickstart",
        "image_source": "pull",
        "server_image": str(defaults["server_image"]),
        "cron_image": str(defaults["cron_image"]),
        "rootbrowse_image": str(defaults["rootbrowse_image"]),
        "xrootd_image": str(defaults["xrootd_image"]),
        "htcondor_image": str(defaults["htcondor_image"]),
        "project_name": str(defaults["project_name"]),
        "data_root": defaults["data_root"],
        "enable_nginx": False,
        "enable_xrootd": True,
        "enable_local_htcondor": True,
        "enable_host_slurm_client": False,
        "enable_krb5": False,
        "host_name": str(defaults["host_name"]),
        "htcondor_internal_domain": str(defaults["htcondor_internal_domain"]),
        "host_port": int(defaults["host_port"]),
        "rootbrowse_port": int(defaults["rootbrowse_port"]),
        "xrootd_port": int(defaults["xrootd_port"]),
        "schedd_host": "schedd@fastink-htcondor",
        "cm_host": "fastink-htcondor",
        "htcondor_default_request_cpus": int(defaults["htcondor_default_request_cpus"]),
        "htcondor_default_request_memory": int(defaults["htcondor_default_request_memory"]),
        "krb5_conf_host_path": str(defaults["krb5_conf_host_path"]),
        "xrootd_krb5_keytab_source_path": "",
        "xrootd_krb5_principal": "",
        "slurm_conf_host_path": str(defaults["slurm_conf_host_path"]),
        "munge_socket_dir": str(defaults["munge_socket_dir"]),
        "workers": 1,
        "ink_production": False,
        "init_database": True,
        "db_name": str(defaults["db_name"]),
        "db_user": str(defaults["db_user"]),
        "db_root_password": secrets.token_urlsafe(18),
        "db_password": secrets.token_urlsafe(18),
        "redis_password": secrets.token_urlsafe(18),
        "plugin_pip_packages": "",
        "plugin_editable_dirs": "",
        "extra_mounts_file": str(ensure_default_extra_mounts_file().resolve()),
        "server_preload_script_dirs": str(defaults["server_preload_script_dirs"]),
        "server_preload_scripts": str(defaults["server_preload_scripts"]),
        "cron_preload_script_dirs": str(defaults["cron_preload_script_dirs"]),
        "cron_preload_scripts": str(defaults["cron_preload_scripts"]),
        "rootbrowse_preload_script_dirs": str(defaults["rootbrowse_preload_script_dirs"]),
        "rootbrowse_preload_scripts": str(defaults["rootbrowse_preload_scripts"]),
        "nginx_cert_source_path": "",
        "nginx_key_source_path": "",
    }
    return normalize_answers(answers, profile="quickstart", deploy_dir=DEPLOY_DIR)


def collect_answers_custom(previous_answers: Optional[dict[str, object]] = None) -> dict[str, object]:
    defaults = default_answers("custom", DEPLOY_DIR)

    def r(key: str):
        if previous_answers and key in previous_answers:
            return previous_answers[key]
        return None

    cli_ui.step("Choose image source")
    image_source = cli_ui.choice_prompt(
        "Image source",
        ["build", "pull"],
        str(defaults["image_source"]),
        str(r("image_source")) if r("image_source") else None,
    )
    image_defaults = default_image_answers(image_source)

    if image_source == "build":
        server_image = cli_ui.text_prompt("Server image tag", str(image_defaults["server_image"]), str(r("server_image")) if r("server_image") else None)
        cron_image = cli_ui.text_prompt("Cron image tag", str(image_defaults["cron_image"]), str(r("cron_image")) if r("cron_image") else None)
        rootbrowse_image = cli_ui.text_prompt("Rootbrowse image tag", str(image_defaults["rootbrowse_image"]), str(r("rootbrowse_image")) if r("rootbrowse_image") else None)
    else:
        server_image = cli_ui.text_prompt("Server image reference", str(image_defaults["server_image"]), str(r("server_image")) if r("server_image") else None)
        cron_image = cli_ui.text_prompt("Cron image reference", str(image_defaults["cron_image"]), str(r("cron_image")) if r("cron_image") else None)
        rootbrowse_image = cli_ui.text_prompt("Rootbrowse image reference", str(image_defaults["rootbrowse_image"]), str(r("rootbrowse_image")) if r("rootbrowse_image") else None)
    xrootd_image = cli_ui.text_prompt("Xrootd image reference", str(image_defaults["xrootd_image"]), str(r("xrootd_image")) if r("xrootd_image") else None)
    htcondor_image_default = str(image_defaults["htcondor_image"])

    cli_ui.step("Basic deployment settings")
    project_name = cli_ui.text_prompt("Compose project name", str(defaults["project_name"]), str(r("project_name")) if r("project_name") else None)
    data_root = Path(
        cli_ui.text_prompt(
            "Data directory",
            str(defaults["data_root"]),
            str(r("data_root")) if r("data_root") else None,
        )
    ).resolve()
    enable_nginx = cli_ui.confirm_prompt("Enable nginx HTTPS reverse proxy", bool(defaults["enable_nginx"]), bool(r("enable_nginx")) if r("enable_nginx") is not None else None)
    enable_xrootd = cli_ui.confirm_prompt("Enable xrootd service container", bool(defaults["enable_xrootd"]), bool(r("enable_xrootd")) if r("enable_xrootd") is not None else None)
    enable_krb5 = cli_ui.confirm_prompt(
        "Enable Kerberos",
        bool(defaults["enable_krb5"]),
        bool(r("enable_krb5")) if r("enable_krb5") is not None else None,
    )
    enable_local_htcondor = cli_ui.confirm_prompt("Enable local HTCondor all-in-one container", bool(defaults["enable_local_htcondor"]), bool(r("enable_local_htcondor")) if r("enable_local_htcondor") is not None else None)
    enable_host_slurm_client = cli_ui.confirm_prompt(
        "Expose host Slurm client config and munge socket",
        bool(defaults["enable_host_slurm_client"]),
        bool(r("enable_host_slurm_client")) if r("enable_host_slurm_client") is not None else None,
    )
    htcondor_image = htcondor_image_default
    if enable_local_htcondor:
        if image_source == "build":
            htcondor_image = cli_ui.text_prompt("HTCondor image tag", htcondor_image_default, str(r("htcondor_image")) if r("htcondor_image") else None)
        else:
            htcondor_image = cli_ui.text_prompt("HTCondor image reference", htcondor_image_default, str(r("htcondor_image")) if r("htcondor_image") else None)
    host_name = cli_ui.text_prompt("Public host name", str(defaults["host_name"]), str(r("host_name")) if r("host_name") else None)
    htcondor_internal_domain = cli_ui.text_prompt(
        "HTCondor internal domain",
        default_htcondor_internal_domain(str(host_name), str(defaults["htcondor_internal_domain"])),
        str(r("htcondor_internal_domain")) if r("htcondor_internal_domain") else None,
    )
    host_port_default = 443 if enable_nginx and int(defaults["host_port"]) == 8000 else int(defaults["host_port"])
    host_port = cli_ui.int_prompt("Public HTTPS port" if enable_nginx else "Public port", host_port_default, int(r("host_port")) if r("host_port") is not None else None)
    rootbrowse_port = cli_ui.int_prompt("Rootbrowse port", int(defaults["rootbrowse_port"]), int(r("rootbrowse_port")) if r("rootbrowse_port") is not None else None)
    xrootd_port = cli_ui.int_prompt("Xrootd port", int(defaults["xrootd_port"]), int(r("xrootd_port")) if r("xrootd_port") is not None else None)
    if enable_local_htcondor:
        schedd_host = "schedd@fastink-htcondor"
        cm_host = "fastink-htcondor"
    else:
        schedd_host = cli_ui.text_prompt("HTCondor schedd host", str(defaults["schedd_host"]), str(r("schedd_host")) if r("schedd_host") else None)
        cm_host = cli_ui.text_prompt("HTCondor collector/CM host", str(defaults["cm_host"]), str(r("cm_host")) if r("cm_host") else None)
    htcondor_default_request_cpus = cli_ui.int_prompt(
        "Default HTCondor job CPUs",
        int(defaults["htcondor_default_request_cpus"]),
        int(r("htcondor_default_request_cpus")) if r("htcondor_default_request_cpus") is not None else None,
    )
    htcondor_default_request_memory = cli_ui.int_prompt(
        "Default HTCondor job memory (MB)",
        int(defaults["htcondor_default_request_memory"]),
        int(r("htcondor_default_request_memory")) if r("htcondor_default_request_memory") is not None else None,
    )
    krb5_conf_host_path = str(defaults["krb5_conf_host_path"])
    xrootd_krb5_keytab_source_path = ""
    xrootd_krb5_principal = ""
    if enable_krb5:
        krb5_conf_host_path = cli_ui.text_prompt(
            "Host krb5.conf path",
            str(defaults["krb5_conf_host_path"]),
            str(r("krb5_conf_host_path")) if r("krb5_conf_host_path") else None,
        )
        if enable_xrootd:
            xrootd_krb5_keytab_source_path = cli_ui.text_prompt(
                "xrootd krb5 keytab source path",
                "",
                str(r("xrootd_krb5_keytab_source_path")) if r("xrootd_krb5_keytab_source_path") else None,
            )
            xrootd_krb5_principal = cli_ui.text_prompt(
                "xrootd krb5 service principal",
                "",
                str(r("xrootd_krb5_principal")) if r("xrootd_krb5_principal") else None,
            )
    slurm_conf_host_path = str(defaults["slurm_conf_host_path"])
    munge_socket_dir = str(defaults["munge_socket_dir"])
    if enable_host_slurm_client:
        slurm_conf_host_path = cli_ui.text_prompt(
            "Host slurm.conf path",
            str(defaults["slurm_conf_host_path"]),
            str(r("slurm_conf_host_path")) if r("slurm_conf_host_path") else None,
        )
        munge_socket_dir = cli_ui.text_prompt(
            "Host munge socket directory",
            str(defaults["munge_socket_dir"]),
            str(r("munge_socket_dir")) if r("munge_socket_dir") else None,
        )
    ink_production = cli_ui.confirm_prompt("Run FastINK in production mode", bool(defaults["ink_production"]), bool(r("ink_production")) if r("ink_production") is not None else None)
    workers = int(defaults["workers"])
    if ink_production:
        workers = cli_ui.int_prompt("Uvicorn workers in production mode", int(defaults["workers"]), int(r("workers")) if r("workers") is not None else None)
    init_database = cli_ui.confirm_prompt("Initialize database on container start", bool(defaults["init_database"]), bool(r("init_database")) if r("init_database") is not None else None)

    nginx_cert_source_path = ""
    nginx_key_source_path = ""
    extra_mounts_file_default = str((DEPLOY_DIR / "extra-mounts.txt").resolve())
    extra_mounts_file = ""
    if cli_ui.confirm_prompt("Use an extra mount list file", False, bool(r("extra_mounts_file")) if r("extra_mounts_file") else None):
        ensure_default_extra_mounts_file()
        extra_mounts_file = cli_ui.text_prompt("Extra mount list file path", extra_mounts_file_default, str(r("extra_mounts_file")) if r("extra_mounts_file") else None)
    if enable_nginx and cli_ui.confirm_prompt("Use an existing TLS certificate and key", False, bool(r("nginx_cert_source_path")) if r("nginx_cert_source_path") else None):
        nginx_cert_source_path = cli_ui.text_prompt("TLS certificate path", "", str(r("nginx_cert_source_path")) if r("nginx_cert_source_path") else None)
        nginx_key_source_path = cli_ui.text_prompt("TLS private key path", "", str(r("nginx_key_source_path")) if r("nginx_key_source_path") else None)

    cli_ui.step("Runtime credentials")
    db_name = cli_ui.text_prompt("Database name", str(defaults["db_name"]), str(r("db_name")) if r("db_name") else None)
    db_user = cli_ui.text_prompt("Database user", str(defaults["db_user"]), str(r("db_user")) if r("db_user") else None)
    db_root_password = cli_ui.secret_prompt("Database root password", secrets.token_urlsafe(18), str(r("db_root_password")) if r("db_root_password") else None)
    db_password = cli_ui.secret_prompt("Database user password", secrets.token_urlsafe(18), str(r("db_password")) if r("db_password") else None)
    redis_password = cli_ui.secret_prompt("Redis password", secrets.token_urlsafe(18), str(r("redis_password")) if r("redis_password") else None)

    answers = {
        "profile": "custom",
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
    return normalize_answers(answers, profile="custom", deploy_dir=DEPLOY_DIR)


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
    public_base_url = str(answers["public_base_url"])
    health_url = f"{public_base_url}/health"
    cli_ui.step(f"Wait for health check: {health_url}")
    if wait_for_health(health_url):
        cli_ui.success(f"Deployment completed. Health check passed: {health_url}")
        print_post_install_notes(answers, paths, nginx_notes, xrootd_notes, krb5_notes)
        return

    print_post_install_notes(answers, paths, nginx_notes, xrootd_notes, krb5_notes)
    cli_ui.error(f"Services started, but health check did not pass within timeout: {health_url}")
    sys.exit(1)


def load_answers_from_file(path: Path) -> dict[str, object]:
    if not path.exists():
        cli_ui.error(f"Answers file not found: {path}")
        sys.exit(1)
    try:
        answers = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        cli_ui.error(f"Failed to parse answers file {path}: {exc}")
        sys.exit(1)
    finalize_mount_answers(answers)
    profile = str(answers.get("profile", "quickstart"))
    return normalize_answers(answers, profile=profile, deploy_dir=DEPLOY_DIR)


def apply_overrides(answers: dict[str, object], overrides: list[str]) -> dict[str, object]:
    for override in overrides:
        if "=" not in override:
            cli_ui.error(f"Invalid --set override: {override} (expected KEY=VALUE)")
            sys.exit(1)
        key, value = override.split("=", 1)
        answers[key] = parse_override_value(key, value)
        cli_ui.info(f"[override] {key} = {answers[key]}")
    return answers


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
