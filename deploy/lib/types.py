from __future__ import annotations

from pathlib import Path
from typing import TypedDict


# Shared set of truthy string values used across boolean parsing.
BOOL_TRUE_VALUES: frozenset[str] = frozenset({"1", "true", "yes", "y", "on"})


class DeployAnswers(TypedDict, total=False):
    """Typed deploy answers dictionary. total=False allows gradual population."""
    # Meta
    profile: str
    image_source: str

    # Images
    server_image: str
    cron_image: str
    rootbrowse_image: str
    htcondor_image: str
    xrootd_image: str
    init_image: str

    # Network
    host_name: str
    host_port: int
    rootbrowse_port: int
    xrootd_port: int
    public_base_url: str
    enable_nginx: bool

    # Storage
    data_root: str
    enable_xrootd: bool
    extra_mounts_file: str

    # Auth
    enable_krb5: bool
    krb5_conf_host_path: str
    xrootd_krb5_keytab_source_path: str
    xrootd_krb5_principal: str

    # HTCondor
    enable_local_htcondor: bool
    htcondor_internal_domain: str
    schedd_host: str
    cm_host: str
    htcondor_default_request_cpus: int
    htcondor_default_request_memory: int

    # Slurm
    enable_host_slurm_client: bool
    slurm_conf_host_path: str
    munge_socket_dir: str

    # Database
    db_name: str
    db_user: str
    db_root_password: str
    db_password: str
    init_database: bool

    # Redis
    redis_password: str

    # Runtime
    project_name: str
    workers: int
    ink_production: bool
    plugin_pip_packages: str
    plugin_editable_dirs: str
    server_preload_script_dirs: str
    server_preload_scripts: str
    cron_preload_script_dirs: str
    cron_preload_scripts: str
    rootbrowse_preload_script_dirs: str
    rootbrowse_preload_scripts: str
    nginx_cert_source_path: str
    nginx_key_source_path: str

    # Auto-filled
    db_data_dir: str
    redis_data_dir: str


class RuntimePaths(TypedDict, total=False):
    """Typed runtime paths dictionary."""
    data_root: Path
    db_data_dir: Path
    redis_data_dir: Path
    xrootd_data_dir: Path
    etc_init_dir: Path
    tmp_dir: Path
    plugins_dir: Path
    keys_dir: Path
    preload_server_dir: Path
    preload_cron_dir: Path
    preload_rootbrowse_dir: Path
    nginx_dir: Path
    nginx_cert_path: Path
    nginx_key_path: Path
    xrootd_dir: Path
    xrootd_sss_keytab_path: Path
    xrootd_krb5_keytab_path: Path
    xrootd_vo_list_path: Path
    server_ssh_private_key_path: Path
    server_ssh_public_key_path: Path
    rootbrowse_authorized_keys_path: Path


def get_bool(answers: dict, key: str, default: bool = False) -> bool:
    """Type-safe bool accessor for answer dictionaries."""
    v = answers.get(key)
    if v is None:
        return default
    if isinstance(v, bool):
        return v
    if isinstance(v, str):
        return v.lower() in BOOL_TRUE_VALUES
    return bool(v)


def get_str(answers: dict, key: str, default: str = "") -> str:
    """Type-safe string accessor for answer dictionaries."""
    v = answers.get(key)
    if v is None:
        return default
    return str(v)


def get_int(answers: dict, key: str, default: int = 0) -> int:
    """Type-safe int accessor for answer dictionaries."""
    v = answers.get(key)
    if v is None:
        return default
    return int(v)
