#!/usr/bin/env python3
"""Interactive questionnaire and answer-collection for FastINK deployment.

Handles all user-facing prompts and default answer construction,
extracted from cmd/deploy.py to keep the deploy orchestration focused
on the deployment workflow rather than input collection.

Functions here produce answer dicts that are consumed by lib.defaults,
lib.paths, and lib.render.
"""

import json
import secrets
import sys
from pathlib import Path
from typing import Optional

from cmd.common import DEPLOY_DIR
from lib import cli_ui
from lib.defaults import (
    default_answers,
    default_image_answers,
    normalize_answers,
    parse_override_value,
)
from lib.paths import build_runtime_paths
from lib.types import get_bool


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def ensure_default_extra_mounts_file() -> Path:
    """Create extra-mounts.txt with the default mount if it doesn't exist."""
    path = DEPLOY_DIR / "extra-mounts.txt"
    path.parent.mkdir(parents=True, exist_ok=True)
    if not path.exists():
        path.write_text("/home/:/home/\n", encoding="utf-8")
    return path


def default_htcondor_internal_domain(host_name: str, fallback: str) -> str:
    """Derive the HTCondor internal domain suffix from a fully-qualified host name."""
    host_name = host_name.strip()
    if "." not in host_name:
        return fallback
    suffix = ".".join(part for part in host_name.split(".")[1:] if part)
    return suffix or fallback


def finalize_mount_answers(answers: dict[str, object]) -> dict[str, object]:
    """Ensure extra_mounts_file is set when xrootd or local htcondor is enabled."""
    if (
        get_bool(answers, "enable_xrootd") or get_bool(answers, "enable_local_htcondor")
    ) and not str(answers.get("extra_mounts_file") or "").strip():
        answers["extra_mounts_file"] = str(ensure_default_extra_mounts_file().resolve())
    return answers


def annotate_runtime_asset_paths(paths: dict[str, Path]) -> None:
    """Resolve and fill in SSH key paths based on the keys directory."""
    server_private_key = Path(
        paths.get("server_ssh_private_key_path", paths["keys_dir"] / "ssh-client" / "id_rsa")
    ).resolve()
    paths["server_ssh_private_key_path"] = server_private_key
    paths["server_ssh_public_key_path"] = (server_private_key.parent / "id_rsa.pub").resolve()
    paths["rootbrowse_authorized_keys_path"] = Path(
        paths.get("rootbrowse_authorized_keys_path", paths["keys_dir"] / "rootbrowse_authorized_keys")
    ).resolve()


# ---------------------------------------------------------------------------
# Answer I/O
# ---------------------------------------------------------------------------


def load_answers_from_file(path: Path) -> dict[str, object]:
    """Load and normalize answers from an arbitrary JSON file path."""
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


def load_saved_answers() -> dict[str, object]:
    """Load and normalize saved answers from .deploy/answers.json."""
    return load_answers_from_file(DEPLOY_DIR / "answers.json")


def try_load_saved_answers() -> Optional[dict[str, object]]:
    """Load saved answers, returning None instead of exiting on error."""
    try:
        return load_saved_answers()
    except SystemExit:
        return None


def apply_overrides(answers: dict[str, object], overrides: list[str]) -> dict[str, object]:
    """Apply --set KEY=VALUE CLI overrides to an answer dict."""
    for override in overrides:
        if "=" not in override:
            cli_ui.error(f"Invalid --set override: {override} (expected KEY=VALUE)")
            sys.exit(1)
        key, value = override.split("=", 1)
        answers[key] = parse_override_value(key, value)
        cli_ui.info(f"[override] {key} = {answers[key]}")
    return answers


# ---------------------------------------------------------------------------
# Path construction from answers
# ---------------------------------------------------------------------------


def build_paths_from_answers(answers: dict[str, object]) -> dict[str, Path]:
    """Construct runtime paths dict from normalized answers."""
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


# ---------------------------------------------------------------------------
# Preparation notes
# ---------------------------------------------------------------------------


def print_preparation_notes() -> None:
    """Print a summary of items to prepare before the interactive questionnaire."""
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


# ---------------------------------------------------------------------------
# Quickstart answers
# ---------------------------------------------------------------------------


def build_quickstart_answers(defaults: dict[str, object]) -> dict[str, object]:
    """Build a full answer dict for the quickstart profile (no interactive prompts)."""
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

    skeleton = _build_answer_skeleton(defaults)
    skeleton.update({
        "profile": "quickstart",
        "image_source": "pull",
        "enable_nginx": False,
        "enable_xrootd": True,
        "enable_local_htcondor": True,
        "enable_host_slurm_client": False,
        "enable_krb5": False,
        "schedd_host": "schedd@fastink-htcondor",
        "cm_host": "fastink-htcondor",
        "workers": 1,
        "ink_production": False,
        "init_database": True,
        "db_root_password": secrets.token_urlsafe(18),
        "db_password": secrets.token_urlsafe(18),
        "redis_password": secrets.token_urlsafe(18),
        "extra_mounts_file": str(ensure_default_extra_mounts_file().resolve()),
        "nginx_cert_source_path": "",
        "nginx_key_source_path": "",
    })
    return normalize_answers(skeleton, profile="quickstart", deploy_dir=DEPLOY_DIR)


# ---------------------------------------------------------------------------
# Custom (interactive) answers
# ---------------------------------------------------------------------------


def collect_answers_custom(previous_answers: Optional[dict[str, object]] = None) -> dict[str, object]:
    """Run the interactive questionnaire for the custom profile."""
    defaults = default_answers("custom", DEPLOY_DIR)

    def r(key: str):
        """Reuse helper — return previous answer value if available."""
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

    skeleton = _build_answer_skeleton(defaults)
    skeleton.update({
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
        "extra_mounts_file": extra_mounts_file,
        "nginx_cert_source_path": nginx_cert_source_path,
        "nginx_key_source_path": nginx_key_source_path,
    })
    finalize_mount_answers(skeleton)
    return normalize_answers(skeleton, profile="custom", deploy_dir=DEPLOY_DIR)


# ---------------------------------------------------------------------------
# Private helpers
# ---------------------------------------------------------------------------


def _build_answer_skeleton(defaults: dict[str, object]) -> dict[str, object]:
    """Return a dict of answer keys that share the same default source
    between quickstart and custom profiles.

    Each profile builder then overrides profile-specific and
    user-provided values on top of this skeleton.
    """
    return {
        "server_image": str(defaults["server_image"]),
        "cron_image": str(defaults["cron_image"]),
        "rootbrowse_image": str(defaults["rootbrowse_image"]),
        "xrootd_image": str(defaults["xrootd_image"]),
        "htcondor_image": str(defaults["htcondor_image"]),
        "project_name": str(defaults["project_name"]),
        "data_root": defaults["data_root"],
        "host_name": str(defaults["host_name"]),
        "htcondor_internal_domain": str(defaults["htcondor_internal_domain"]),
        "host_port": int(defaults["host_port"]),
        "rootbrowse_port": int(defaults["rootbrowse_port"]),
        "xrootd_port": int(defaults["xrootd_port"]),
        "krb5_conf_host_path": str(defaults["krb5_conf_host_path"]),
        "xrootd_krb5_keytab_source_path": "",
        "xrootd_krb5_principal": "",
        "slurm_conf_host_path": str(defaults["slurm_conf_host_path"]),
        "munge_socket_dir": str(defaults["munge_socket_dir"]),
        "plugin_pip_packages": str(defaults["plugin_pip_packages"]),
        "plugin_editable_dirs": str(defaults["plugin_editable_dirs"]),
        "server_preload_script_dirs": str(defaults["server_preload_script_dirs"]),
        "server_preload_scripts": str(defaults["server_preload_scripts"]),
        "cron_preload_script_dirs": str(defaults["cron_preload_script_dirs"]),
        "cron_preload_scripts": str(defaults["cron_preload_scripts"]),
        "rootbrowse_preload_script_dirs": str(defaults["rootbrowse_preload_script_dirs"]),
        "rootbrowse_preload_scripts": str(defaults["rootbrowse_preload_scripts"]),
    }
