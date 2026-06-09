"""Default answer values, normalization, and health-URL helpers.

Holds the canonical default configuration for both quickstart and
custom profiles, including image tags, port numbers, and boolean
feature toggles.  Also provides normalisation that fills in missing
keys and computes derived values (e.g. ``public_base_url``).
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

from lib.types import BOOL_TRUE_VALUES

BOOL_FIELDS = {
    "enable_nginx",
    "enable_xrootd",
    "enable_local_htcondor",
    "enable_host_slurm_client",
    "enable_krb5",
    "ink_production",
    "init_database",
}
INT_FIELDS = {
    "host_port",
    "rootbrowse_port",
    "xrootd_port",
    "workers",
    "htcondor_default_request_cpus",
    "htcondor_default_request_memory",
}

DEFAULT_BUILD_IMAGES = {
    "init_image": "fastink-init:local",
    "server_image": "fastink-server:local",
    "cron_image": "fastink-redis-cron:local",
    "rootbrowse_image": "fastink-rootbrowse:local",
    "htcondor_image": "fastink-htcondor:local",
    "xrootd_image": "dockerhub.ihep.ac.cn/ink/xrootd-multiuser:5.9.0-3",
}

DEFAULT_PULL_IMAGES = {
    "init_image": "dockerhub.ihep.ac.cn/ink/fastink-init:latest",
    "server_image": "dockerhub.ihep.ac.cn/ink/fastink-server:latest",
    "cron_image": "dockerhub.ihep.ac.cn/ink/fastink-redis-cron:latest",
    "rootbrowse_image": "dockerhub.ihep.ac.cn/ink/fastink-rootbrowse:latest",
    "htcondor_image": "dockerhub.ihep.ac.cn/ink/fastink-htcondor:latest",
    "xrootd_image": "dockerhub.ihep.ac.cn/ink/xrootd-multiuser:5.9.0-3",
}

COMMON_DEFAULTS = {
    "image_source": "pull",
    "project_name": "fastink",
    "host_name": "localhost",
    "htcondor_internal_domain": "local",
    "host_port": 8000,
    "rootbrowse_port": 2000,
    "xrootd_port": 1094,
    "workers": 4,
    "htcondor_default_request_cpus": 1,
    "htcondor_default_request_memory": 6000,
    "ink_production": False,
    "init_database": True,
    "schedd_host": "localhost",
    "cm_host": "localhost",
    "db_name": "inkdb",
    "db_user": "inkdbuser",
    "db_root_password": "",
    "db_password": "",
    "redis_password": "",
    "plugin_pip_packages": "",
    "plugin_editable_dirs": "",
    "extra_mounts_file": "",
    "enable_host_slurm_client": False,
    "enable_krb5": False,
    "krb5_conf_host_path": "/etc/krb5.conf",
    "xrootd_krb5_keytab_source_path": "",
    "xrootd_krb5_principal": "",
    "slurm_conf_host_path": "/etc/slurm/slurm.conf",
    "munge_socket_dir": "/var/run/munge",
    "server_preload_script_dirs": "/opt/preload/server",
    "server_preload_scripts": "",
    "cron_preload_script_dirs": "/opt/preload/cron",
    "cron_preload_scripts": "",
    "rootbrowse_preload_script_dirs": "/opt/preload/rootbrowse",
    "rootbrowse_preload_scripts": "",
}

PROFILE_DEFAULTS = {
    "quickstart": {
        "enable_nginx": False,
        "enable_xrootd": True,
        "enable_local_htcondor": True,
        "enable_host_slurm_client": False,
        "enable_krb5": False,
    },
    "custom": {
        "enable_nginx": False,
        "enable_xrootd": False,
        "enable_local_htcondor": False,
        "enable_host_slurm_client": False,
        "enable_krb5": False,
    },
}


def profile_defaults(profile: str) -> dict[str, Any]:
    try:
        return dict(PROFILE_DEFAULTS[profile])
    except KeyError as exc:
        raise ValueError(f"Unsupported deployment profile: {profile}") from exc



def default_image_answers(image_source: str) -> dict[str, str]:
    if image_source == "build":
        return dict(DEFAULT_BUILD_IMAGES)
    if image_source == "pull":
        return dict(DEFAULT_PULL_IMAGES)
    raise ValueError(f"Unsupported image source: {image_source}")



def build_public_base_url(host_name: str, host_port: int, *, enable_nginx: bool = False) -> str:
    scheme = "https" if enable_nginx else "http"
    default_port = 443 if enable_nginx else 80
    if host_port == default_port:
        return f"{scheme}://{host_name}"
    return f"{scheme}://{host_name}:{host_port}"



def build_health_url(answers: dict[str, Any]) -> str:
    """Return the health check URL for a given answers dict."""
    base = str(answers.get("public_base_url", ""))
    return f"{base}/health"


def default_answers(profile: str = "quickstart", deploy_dir: Path | None = None) -> dict[str, Any]:
    defaults: dict[str, Any] = dict(COMMON_DEFAULTS)
    defaults["profile"] = profile
    defaults.update(profile_defaults(profile))
    defaults.update(default_image_answers(str(defaults["image_source"])))
    if deploy_dir is not None:
        defaults["data_root"] = (deploy_dir / "data").resolve()
    return defaults



def normalize_answers(
    answers: dict[str, Any],
    *,
    profile: str | None = None,
    deploy_dir: Path | None = None,
) -> dict[str, Any]:
    resolved_profile = profile or str(answers.get("profile", "quickstart"))
    normalized = default_answers(resolved_profile, deploy_dir)

    image_source = str(answers.get("image_source", normalized["image_source"]))
    normalized["image_source"] = image_source
    normalized.update(default_image_answers(image_source))
    normalized.update(answers)
    normalized["profile"] = resolved_profile

    if not normalized.get("public_base_url"):
        normalized["public_base_url"] = build_public_base_url(
            str(normalized["host_name"]),
            int(normalized["host_port"]),
            enable_nginx=bool(normalized.get("enable_nginx", False)),
        )
    return normalized



def parse_override_value(key: str, value: str):
    if key in BOOL_FIELDS:
        normalized = value.strip().lower()
        if normalized in BOOL_TRUE_VALUES:
            return True
        if normalized in {"0", "false", "no", "n", "off"}:
            return False
        raise ValueError(f"Invalid boolean value: {value}")
    if key in INT_FIELDS:
        return int(value)
    return value



def required_images(answers: dict[str, Any]) -> list[tuple[str, str]]:
    images = [
        ("init", str(answers["init_image"])),
        ("server", str(answers["server_image"])),
        ("cron", str(answers["cron_image"])),
        ("rootbrowse", str(answers["rootbrowse_image"])),
    ]
    if answers.get("enable_xrootd"):
        images.append(("xrootd", str(answers["xrootd_image"])))
    if answers.get("enable_local_htcondor"):
        images.append(("htcondor", str(answers["htcondor_image"])))
    return images
