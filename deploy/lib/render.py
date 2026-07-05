"""Template rendering and bundle generation.

Renders Jinja2 templates from ``templates/``, substitutes variables
from a flat mapping dict, deep-merges profile and extra overlays,
and produces the final ``config.yml``, ``docker-compose.yml``,
``.env``, and associated config files.
"""

import json
import subprocess
from pathlib import Path
from typing import Optional

import yaml
from jinja2 import Environment, StrictUndefined

from lib.assets import (
    ensure_nginx_tls_material,
    ensure_rootbrowse_ssh_material,
)
from lib.types import get_bool, get_str, get_int, DeployAnswers


DEPLOY_ROOT = Path(__file__).resolve().parent.parent
TEMPLATE_ROOT = DEPLOY_ROOT / "templates"
SOURCE_ROOT = DEPLOY_ROOT.parent

_JINJA_ENV = Environment(
    undefined=StrictUndefined,
)
_JINJA_ENV.filters["to_yaml"] = lambda v: json.dumps(v, ensure_ascii=False)


def profile_chain(profile: str) -> list[str]:
    if profile == "custom":
        return ["quickstart", "custom"]
    return [profile]


def git_output(*args: str) -> str:
    return subprocess.check_output(
        ["git", "-C", str(SOURCE_ROOT), *args],
        encoding="utf-8",
        stderr=subprocess.DEVNULL,
    ).strip()


def source_version_env() -> dict[str, str]:
    env = {
        "source_commit_sha": "unknown",
        "source_commit_date": "unknown",
        "source_commit_tag": "",
    }
    if not (SOURCE_ROOT / ".git").exists():
        return env

    try:
        env["source_commit_sha"] = git_output("rev-parse", "--short", "HEAD")
    except Exception:
        pass

    try:
        env["source_commit_date"] = git_output("log", "-1", "--format=%cs")
    except Exception:
        pass

    try:
        env["source_commit_tag"] = git_output("describe", "--tags", "--exact-match", "HEAD")
    except Exception:
        pass

    return env


def load_extra_mount_entries(path_value: object) -> list[str]:
    path_text = str(path_value or "").strip()
    if not path_text:
        return []

    mount_file = Path(path_text).expanduser().resolve()
    if not mount_file.exists():
        raise FileNotFoundError(f"Extra mount list file not found: {mount_file}")

    entries: list[str] = []
    for raw_line in mount_file.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if ":" not in line:
            raise ValueError(f"Invalid mount entry (expected host:container[:mode]): {line}")
        entries.append(line)
    return entries


def parse_mount_entry(entry: str) -> tuple[str, str, str]:
    parts = entry.split(":")
    if len(parts) == 2:
        host_path, container_path = parts
        mode = ""
    elif len(parts) == 3:
        host_path, container_path, mode = parts
    else:
        raise ValueError(f"Invalid mount entry (expected host:container[:mode]): {entry}")

    host_path = host_path.strip()
    container_path = container_path.strip()
    mode = mode.strip()
    if not host_path or not container_path:
        raise ValueError(f"Invalid mount entry (expected host:container[:mode]): {entry}")
    return host_path, container_path, mode


def build_xrootd_vo_entries(extra_mount_entries: list[str]) -> list[str]:
    seen: set[str] = set()
    entries: list[str] = []
    for mount_entry in extra_mount_entries:
        _, container_path, _ = parse_mount_entry(mount_entry)
        normalized = container_path.rstrip("/") or "/"
        if normalized == "/":
            continue
        vo_entry = f"{normalized}/"
        if vo_entry in seen:
            continue
        seen.add(vo_entry)
        entries.append(vo_entry)
    return entries


def render_template_text(path: Path, mapping: dict[str, str]) -> str:
    template = _JINJA_ENV.from_string(path.read_text(encoding="utf-8"))
    return template.render(mapping)


def render_yaml_template(path: Path, mapping: dict[str, str]) -> dict:
    if not path.exists():
        return {}
    rendered = render_template_text(path, mapping).strip()
    if not rendered:
        return {}
    data = yaml.safe_load(rendered)
    return data or {}


def load_yaml_file(path: Path) -> dict:
    if not path.exists():
        return {}
    rendered = path.read_text(encoding="utf-8").strip()
    if not rendered:
        return {}
    data = yaml.safe_load(rendered)
    return data or {}


def deep_merge(base, overlay, *, list_strategy: str = "replace"):
    """Deep merge two dicts, with optional list concatenation.

    Args:
        base: Base dictionary.
        overlay: Overlay dictionary to merge on top.
        list_strategy: How to handle lists:
            "replace" (default) — overlay list replaces base list.
            "extend" — overlay list is appended to base list.
    """
    if isinstance(base, dict) and isinstance(overlay, dict):
        merged = dict(base)
        for key, value in overlay.items():
            if key in merged:
                merged[key] = deep_merge(merged[key], value, list_strategy=list_strategy)
            else:
                merged[key] = value
        return merged
    if isinstance(base, list) and isinstance(overlay, list) and list_strategy == "extend":
        return base + overlay
    return overlay


def dump_yaml(data: dict) -> str:
    return yaml.safe_dump(data, sort_keys=False, allow_unicode=True)


# ---------------------------------------------------------------------------
# Domain sub-functions that build groups of template variables.
# Each returns a flat dict; build_mapping() composes them via dict.update().
# ---------------------------------------------------------------------------

def _build_network_mapping(
    answers: dict[str, object],
    paths: dict[str, Path],
    deploy_dir: Path,
    enable_nginx: bool,
) -> dict[str, str]:
    """Network-related template variables (nginx, ports, host)."""
    nginx_conf_path = deploy_dir / "nginx" / "default.conf"
    return {
        "enable_nginx": str(enable_nginx).lower(),
        "host_name": get_str(answers, "host_name"),
        "host_port": get_str(answers, "host_port"),
        "rootbrowse_port": get_str(answers, "rootbrowse_port"),
        "public_base_url": get_str(answers, "public_base_url"),
        "nginx_conf_path": str(nginx_conf_path.resolve()),
        "nginx_cert_host_path": str(Path(paths.get("nginx_cert_path", deploy_dir / "nginx" / "cert.pem")).resolve()),
        "nginx_key_host_path": str(Path(paths.get("nginx_key_path", deploy_dir / "nginx" / "key.pem")).resolve()),
        "nginx_cert_container_path": "/etc/nginx/ssl/cert.pem",
        "nginx_key_container_path": "/etc/nginx/ssl/key.pem",
        "rootbrowse_container_port": "2000",
    }


def _build_storage_mapping(
    answers: dict[str, object],
    paths: dict[str, Path],
    deploy_dir: Path,
    enable_xrootd: bool,
    xrootd_vo_entries: list[str],
) -> dict[str, str]:
    """Storage-related template variables (xrootd, filesystem backend, data dirs)."""
    xrootd_conf_path = deploy_dir / "xrootd" / "xrootd-proxy.cfg"
    return {
        "enable_xrootd": str(enable_xrootd).lower(),
        "xrootd_image": get_str(answers, "xrootd_image", "dockerhub.ihep.ac.cn/ink/xrootd-multiuser:5.9.0-3"),
        "xrootd_conf_path": str(xrootd_conf_path.resolve()),
        "xrootd_data_dir": str(paths["xrootd_data_dir"].resolve()),
        "xrootd_sss_keytab_host_path": str(paths.get("xrootd_sss_keytab_path", paths["xrootd_data_dir"] / "sss.keytab").resolve()),
        "xrootd_sss_keytab_container_path": "/etc/xrootd/sss.keytab",
        "xrootd_vo_list_host_path": str(paths.get("xrootd_vo_list_path", paths["xrootd_data_dir"] / "vo-list.cfg").resolve()),
        "xrootd_vo_list_container_path": "/etc/xrootd/vo-list.cfg",
        "xrootd_vo_list_content": "\n".join(xrootd_vo_entries) + ("\n" if xrootd_vo_entries else ""),
        "xrootd_port": get_str(answers, "xrootd_port", "1094"),
        "xrd_host": "root://fastink-xrootd:1098" if enable_xrootd else "root://127.0.0.1:1094",
        "data_root": str(paths["data_root"]),
        "db_data_dir": str(paths["db_data_dir"].resolve()),
        "redis_data_dir": str(paths["redis_data_dir"].resolve()),
    }


def _build_auth_mapping(
    answers: dict[str, object],
    enable_krb5: bool,
) -> dict[str, str]:
    """Auth-related template variables (krb5, access control)."""
    return {
        "enable_krb5": str(enable_krb5).lower(),
        "krb5_enabled": str(enable_krb5).lower(),
        "auth_type": "krb5" if enable_krb5 else "password",
    }


def _build_computing_mapping(
    answers: dict[str, object],
    deploy_dir: Path,
    schedd_host: str,
    cm_host: str,
    enable_local_htcondor: bool,
    htcondor_internal_domain: str,
    cluster_list: list[str],
    noenv_jobtype: list[str],
    start_keywords: list[str],
    jobtype_defaults: dict,
) -> dict[str, str]:
    """Computing-related template variables (HTCondor, cluster, job types)."""
    server_condor_conf_host_path = str((deploy_dir / "condor" / "ink.conf").resolve())
    cron_condor_conf_host_path = str((deploy_dir / "condor" / "ink.conf").resolve())
    htcondor_local_conf_host_path = str((deploy_dir / "condor" / "htcondor.local.conf").resolve())
    return {
        "schedd_host": schedd_host,
        "cm_host": cm_host,
        "htcondor_auth_method": "CLAIMTOBE",
        "htcondor_fs_domain": htcondor_internal_domain,
        "htcondor_uid_domain": htcondor_internal_domain,
        "enable_local_htcondor": str(enable_local_htcondor).lower(),
        "cluster_list": cluster_list,
        "noenv_jobtype": noenv_jobtype,
        "start_keywords": start_keywords,
        "jobtype_defaults": jobtype_defaults,
        "server_condor_conf_host_path": server_condor_conf_host_path,
        "cron_condor_conf_host_path": cron_condor_conf_host_path,
        "htcondor_local_conf_host_path": htcondor_local_conf_host_path,
    }


def _build_mount_mapping(
    paths: dict[str, Path],
    enable_krb5: bool,
    xrootd_krb5_keytab_source_path: str,
    xrootd_krb5_principal: str,
) -> dict[str, str]:
    """Mount-related template variables (xrootd keytab paths).

    Dynamic volume mounts (extra-mounts, krb5.conf, slurm) are now
    handled by build_compose_volume_overlay() instead of string injection.
    """
    return {
        "xrootd_krb5_keytab_host_path": str(
            Path(xrootd_krb5_keytab_source_path).expanduser().resolve()
            if enable_krb5 and xrootd_krb5_keytab_source_path
            else paths.get("xrootd_krb5_keytab_path", paths["xrootd_data_dir"] / "krb5.keytab").resolve()
        ),
        "xrootd_krb5_keytab_container_path": "/etc/xrootd/krb5.keytab",
        "xrootd_krb5_principal": xrootd_krb5_principal,
    }


# ---------------------------------------------------------------------------
# build_mapping – public composition function
# ---------------------------------------------------------------------------

def build_mapping(
    profile: str,
    answers: DeployAnswers,
    paths: dict[str, Path],
    deploy_dir: Path,
    *,
    extra_mount_entries: Optional[list[str]] = None,
) -> dict[str, str]:
    """Compose the full template-variable mapping from domain sub-functions.

    The function computes shared values once, then delegates each logical
    group of keys to a dedicated ``_build_*_mapping`` helper.  Remaining
    general-purpose keys are added directly.

    If *extra_mount_entries* is provided it is used directly (caller
    has already loaded the file); otherwise the file is read here.
    """
    version_env = source_version_env()
    config_path = deploy_dir / "config.yml"

    # ---- shared booleans ----
    enable_nginx = get_bool(answers, "enable_nginx")
    enable_xrootd = get_bool(answers, "enable_xrootd")
    enable_krb5 = get_bool(answers, "enable_krb5")
    enable_local_htcondor = get_bool(answers, "enable_local_htcondor")

    # ---- extra mounts ----
    if extra_mount_entries is None:
        extra_mount_entries = load_extra_mount_entries(get_str(answers, "extra_mounts_file"))
    xrootd_vo_entries = build_xrootd_vo_entries(extra_mount_entries)

    # ---- computing defaults ----
    schedd_host = "schedd@fastink-htcondor" if enable_local_htcondor else get_str(answers, "schedd_host", "localhost")
    cm_host = "fastink-htcondor" if enable_local_htcondor else get_str(answers, "cm_host", "localhost")
    cluster_list = ["htcondor"]
    noenv_jobtype = ["jupyter", "vnc"]
    start_keywords = [
        "jupyterlab | extension was successfully loaded.",
        "Session server listening on",
        "Starting noVNC proxy on",
        "SSH server starting",
        "Start rootbrowse in screen session",
        "OpenClaw gateway listening on",
    ]
    htcondor_internal_domain = get_str(answers, "htcondor_internal_domain", "local")

    jobtype_defaults = {
        "vscode": {
            "htc": {
                "RequestMemory": get_int(answers, "htcondor_default_request_memory", 6000),
                "RequestCpus": get_int(answers, "htcondor_default_request_cpus", 1),
                "schedd_host": schedd_host,
                "cm_host": cm_host,
            },
        },
        "jupyter": {
            "htc": {
                "RequestMemory": get_int(answers, "htcondor_default_request_memory", 6000),
                "RequestCpus": get_int(answers, "htcondor_default_request_cpus", 1),
                "schedd_host": schedd_host,
                "cm_host": cm_host,
            },
        },
        "vnc": {
            "htc": {
                "RequestMemory": get_int(answers, "htcondor_default_request_memory", 6000),
                "RequestCpus": get_int(answers, "htcondor_default_request_cpus", 1),
                "schedd_host": schedd_host,
                "cm_host": cm_host,
            },
        },
        "rootbrowse": {
            "htc": {
                "RequestMemory": get_int(answers, "htcondor_default_request_memory", 6000),
                "RequestCpus": get_int(answers, "htcondor_default_request_cpus", 1),
                "schedd_host": schedd_host,
                "cm_host": cm_host,
            },
        },
    }

    # ---- krb5 / slurm paths (consumed by _build_mount_mapping) ----
    xrootd_krb5_keytab_source_path = get_str(answers, "xrootd_krb5_keytab_source_path").strip()
    xrootd_krb5_principal = get_str(answers, "xrootd_krb5_principal").strip()

    # ---- compose mapping from sub-functions ----
    mapping: dict[str, str] = {}
    mapping.update(_build_network_mapping(answers, paths, deploy_dir, enable_nginx))
    mapping.update(_build_storage_mapping(answers, paths, deploy_dir, enable_xrootd, xrootd_vo_entries))
    mapping.update(_build_auth_mapping(answers, enable_krb5))
    mapping.update(_build_computing_mapping(
        answers, deploy_dir, schedd_host, cm_host, enable_local_htcondor,
        htcondor_internal_domain, cluster_list, noenv_jobtype, start_keywords,
        jobtype_defaults,
    ))
    mapping.update(_build_mount_mapping(
        paths, enable_krb5,
        xrootd_krb5_keytab_source_path, xrootd_krb5_principal,
    ))

    # ---- general / remaining keys ----
    rootbrowse_keys_host_path = paths.get(
        "rootbrowse_authorized_keys_path",
        paths["keys_dir"] / "rootbrowse_authorized_keys",
    )
    server_ssh_private_key_path = paths.get(
        "server_ssh_private_key_path",
        paths["keys_dir"] / "ssh-client" / "id_rsa",
    )

    mapping.update({
        "profile": profile,
        "image_source": get_str(answers, "image_source"),
        "server_image": get_str(answers, "server_image"),
        "cron_image": get_str(answers, "cron_image"),
        "rootbrowse_image": get_str(answers, "rootbrowse_image"),
        "htcondor_image": get_str(answers, "htcondor_image", "dockerhub.ihep.ac.cn/ink/fastink-htcondor:latest"),
        "project_name": get_str(answers, "project_name"),
        "db_name": get_str(answers, "db_name"),
        "db_user": get_str(answers, "db_user"),
        "db_password": get_str(answers, "db_password"),
        "db_root_password": get_str(answers, "db_root_password"),
        "redis_password": get_str(answers, "redis_password"),
        "config_path": str(config_path.resolve()),
        "etc_init_dir": str(paths["etc_init_dir"].resolve()),
        "tmp_dir": str(paths["tmp_dir"].resolve()),
        "plugins_dir": str(paths["plugins_dir"].resolve()),
        "keys_dir": str(paths["keys_dir"].resolve()),
        "server_ssh_dir_host_path": str(Path(server_ssh_private_key_path).resolve().parent),
        "server_ssh_dir_container_path": "/root/.ssh",
        "preload_server_dir": str(paths["preload_server_dir"].resolve()),
        "preload_cron_dir": str(paths["preload_cron_dir"].resolve()),
        "preload_rootbrowse_dir": str(paths["preload_rootbrowse_dir"].resolve()),
        "rootbrowse_authorized_keys_host_path": str(rootbrowse_keys_host_path.resolve()),
        "rootbrowse_authorized_keys_container_path": "/run/fastink/rootbrowse_authorized_keys",
        "timezone": "Asia/Shanghai",
        "workers": get_str(answers, "workers"),
        "ink_production": str(get_bool(answers, "ink_production")).lower(),
        "init_database": str(get_bool(answers, "init_database")).lower(),
        "server_preload_script_dirs": get_str(answers, "server_preload_script_dirs"),
        "server_preload_scripts": get_str(answers, "server_preload_scripts"),
        "cron_preload_script_dirs": get_str(answers, "cron_preload_script_dirs"),
        "cron_preload_scripts": get_str(answers, "cron_preload_scripts"),
        "rootbrowse_preload_script_dirs": get_str(answers, "rootbrowse_preload_script_dirs"),
        "rootbrowse_preload_scripts": get_str(answers, "rootbrowse_preload_scripts"),
        "source_commit_sha": version_env["source_commit_sha"],
        "source_commit_date": version_env["source_commit_date"],
        "source_commit_tag": version_env["source_commit_tag"],
        "plugin_pip_packages": get_str(answers, "plugin_pip_packages"),
        "plugin_editable_dirs": get_str(answers, "plugin_editable_dirs"),
    })
    return mapping


def build_compose_volume_overlay(
    extra_mount_entries: list[str],
    enable_krb5: bool,
    krb5_conf_host_path: str,
    enable_host_slurm_client: bool,
    slurm_conf_host_path: str,
    munge_socket_dir: str,
    enable_xrootd: bool,
    enable_local_htcondor: bool,
) -> dict:
    """Build a compose overlay dict for dynamic volumes.

    Replaces the template variable string injection
    (``${server_extra_mounts_block}``, ``${server_krb5_conf_mount_block}``, etc.)
    with a structured dict that is deep-merged into the compose file using
    ``list_strategy="extend"`` so volumes are appended rather than replaced.
    """
    overlay: dict = {"services": {}}

    # krb5 conf mount (applies to all services)
    krb5_volumes: list[str] = []
    if enable_krb5:
        krb5_volumes = [f"{krb5_conf_host_path}:/etc/krb5.conf:ro"]

    # slurm mounts (server + cron only)
    slurm_volumes: list[str] = []
    if enable_host_slurm_client:
        slurm_volumes = [
            f"{munge_socket_dir}:/var/run/munge/",
            f"{slurm_conf_host_path}:/etc/slurm/slurm.conf:ro",
        ]

    # extra mounts from file
    extra_volumes = list(extra_mount_entries) if extra_mount_entries else []

    # fastink-server: krb5 + slurm + extra
    server_vols = krb5_volumes + slurm_volumes + extra_volumes
    if server_vols:
        overlay["services"]["fastink-server"] = {"volumes": server_vols}

    # fastink-redis-cron: krb5 + slurm + extra
    cron_vols = krb5_volumes + slurm_volumes + extra_volumes
    if cron_vols:
        overlay["services"]["fastink-redis-cron"] = {"volumes": cron_vols}

    # fastink-rootbrowse: krb5 + extra
    rootbrowse_vols = krb5_volumes + extra_volumes
    if rootbrowse_vols:
        overlay["services"]["fastink-rootbrowse"] = {"volumes": rootbrowse_vols}

    # fastink-xrootd: krb5 + extra (only when xrootd is enabled)
    if enable_xrootd:
        xrootd_vols = krb5_volumes + extra_volumes
        if xrootd_vols:
            overlay["services"]["fastink-xrootd"] = {"volumes": xrootd_vols}

    # fastink-htcondor: krb5 + extra (only when local htcondor is enabled)
    if enable_local_htcondor:
        htcondor_vols = krb5_volumes + extra_volumes
        if htcondor_vols:
            overlay["services"]["fastink-htcondor"] = {"volumes": htcondor_vols}

    return overlay


def build_compose_port_overlay(enable_nginx: bool, host_port: int) -> dict:
    """Build a compose overlay dict for the server port configuration.

    When nginx is enabled the server only exposes port 8000 internally.
    When nginx is disabled the server publishes ``host_port:8000`` directly.
    """
    if enable_nginx:
        return {"services": {"fastink-server": {"expose": ["8000"]}}}
    else:
        return {"services": {"fastink-server": {"ports": [f"{host_port}:8000"]}}}


def render_config(
    profile: str,
    mapping: dict[str, str],
    extra_overlays: Optional[list[Path]] = None,
) -> str:
    base = render_yaml_template(TEMPLATE_ROOT / "base" / "config.yml.tpl", mapping)
    merged = base
    for profile_name in profile_chain(profile):
        overlay = render_yaml_template(
            TEMPLATE_ROOT / "profiles" / profile_name / "config.overlay.yml.tpl",
            mapping,
        )
        merged = deep_merge(merged, overlay)
    for overlay_path in extra_overlays or []:
        merged = deep_merge(merged, load_yaml_file(overlay_path))
    return dump_yaml(merged)


def render_compose(
    profile: str,
    mapping: dict[str, str],
    enable_nginx: bool,
    enable_xrootd: bool,
    extra_overlays: Optional[list[Path]] = None,
    volume_overlay: Optional[dict] = None,
    port_overlay: Optional[dict] = None,
) -> str:
    base = render_yaml_template(TEMPLATE_ROOT / "base" / "docker-compose.yml.tpl", mapping)
    merged = base
    for profile_name in profile_chain(profile):
        profile_overlay = render_yaml_template(
            TEMPLATE_ROOT / "profiles" / profile_name / "compose.overlay.yml.tpl",
            mapping,
        )
        merged = deep_merge(merged, profile_overlay)
    if enable_nginx:
        merged = deep_merge(
            merged,
            render_yaml_template(TEMPLATE_ROOT / "extras" / "nginx.compose.yml.tpl", mapping),
        )
    if enable_xrootd:
        merged = deep_merge(
            merged,
            render_yaml_template(TEMPLATE_ROOT / "extras" / "xrootd.compose.yml.tpl", mapping),
        )
    if bool(mapping.get("enable_local_htcondor", "false") == "true"):
        merged = deep_merge(
            merged,
            render_yaml_template(TEMPLATE_ROOT / "extras" / "htcondor.compose.yml.tpl", mapping),
        )
    # Dynamic overlays: volumes use list_strategy="extend" to append
    if volume_overlay:
        merged = deep_merge(merged, volume_overlay, list_strategy="extend")
    if port_overlay:
        merged = deep_merge(merged, port_overlay)
    for overlay_path in extra_overlays or []:
        merged = deep_merge(merged, load_yaml_file(overlay_path))
    return dump_yaml(merged)


def render_env(mapping: dict[str, str]) -> str:
    return render_template_text(TEMPLATE_ROOT / "base" / "env.tpl", mapping)


def render_nginx_conf(mapping: dict[str, str]) -> str:
    return render_template_text(TEMPLATE_ROOT / "base" / "nginx.conf.tpl", mapping)


def render_xrootd_conf(mapping: dict[str, str]) -> str:
    template_name = "xrootd-proxy-krb5.cfg.tpl" if mapping.get("enable_krb5") == "true" else "xrootd-proxy.cfg.tpl"
    return render_template_text(TEMPLATE_ROOT / "base" / template_name, mapping)


def render_condor_conf(mapping: dict[str, str]) -> str:
    return render_template_text(TEMPLATE_ROOT / "base" / "ink.condor.conf.tpl", mapping)


def render_htcondor_local_conf(mapping: dict[str, str]) -> str:
    return render_template_text(TEMPLATE_ROOT / "base" / "htcondor.local.conf.tpl", mapping)


def render_bundle(
    profile: str,
    answers: DeployAnswers,
    paths: dict[str, Path],
    deploy_dir: Path,
    *,
    config_overlay_paths: Optional[list[Path]] = None,
    compose_overlay_paths: Optional[list[Path]] = None,
    initialize_host_assets: bool = True,
) -> dict[str, str]:
    if initialize_host_assets:
        ensure_rootbrowse_ssh_material(paths)
        ensure_nginx_tls_material(answers, paths)

    # Load extra mounts once, shared by build_mapping (xrootd VO) and
    # build_compose_volume_overlay (dynamic compose volumes).
    extra_mount_entries = load_extra_mount_entries(get_str(answers, "extra_mounts_file"))
    mapping = build_mapping(profile, answers, paths, deploy_dir,
                            extra_mount_entries=extra_mount_entries)

    # Build dynamic compose overlays (replaces template string injection)
    volume_overlay = build_compose_volume_overlay(
        extra_mount_entries=extra_mount_entries,
        enable_krb5=get_bool(answers, "enable_krb5"),
        krb5_conf_host_path=get_str(answers, "krb5_conf_host_path", "/etc/krb5.conf").strip(),
        enable_host_slurm_client=get_bool(answers, "enable_host_slurm_client"),
        slurm_conf_host_path=get_str(answers, "slurm_conf_host_path", "/etc/slurm/slurm.conf").strip(),
        munge_socket_dir=get_str(answers, "munge_socket_dir", "/var/run/munge").strip().rstrip("/"),
        enable_xrootd=get_bool(answers, "enable_xrootd"),
        enable_local_htcondor=get_bool(answers, "enable_local_htcondor"),
    )
    port_overlay = build_compose_port_overlay(
        enable_nginx=get_bool(answers, "enable_nginx"),
        host_port=get_int(answers, "host_port", 8000),
    )

    bundle = {
        "config.yml": render_config(profile, mapping, extra_overlays=config_overlay_paths),
        ".env": render_env(mapping),
        "docker-compose.yml": render_compose(
            profile,
            mapping,
            get_bool(answers, "enable_nginx"),
            get_bool(answers, "enable_xrootd"),
            extra_overlays=compose_overlay_paths,
            volume_overlay=volume_overlay,
            port_overlay=port_overlay,
        ),
    }
    if get_bool(answers, "enable_nginx"):
        bundle["nginx/default.conf"] = render_nginx_conf(mapping)
    if get_bool(answers, "enable_xrootd"):
        bundle["xrootd/xrootd-proxy.cfg"] = render_xrootd_conf(mapping)
        bundle["xrootd/vo-list.cfg"] = str(mapping.get("xrootd_vo_list_content", ""))
    # Always generate condor config for container mount
    bundle["condor/ink.conf"] = render_condor_conf(mapping)
    if get_bool(answers, "enable_local_htcondor"):
        bundle["condor/htcondor.local.conf"] = render_htcondor_local_conf(mapping)
    return bundle
