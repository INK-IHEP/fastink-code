"""Template rendering and bundle generation.

Loads Jinja-style ``string.Template`` files from ``templates/``,
substitutes variables from a flat mapping dict, deep-merges profile
and extra overlays, and produces the final ``config.yml``,
``docker-compose.yml``, ``.env``, and associated config files.
"""

import json
import shutil
import subprocess
from pathlib import Path
from string import Template
from typing import Optional

import yaml

from lib.types import get_bool, get_str, get_int


DEPLOY_ROOT = Path(__file__).resolve().parent.parent
TEMPLATE_ROOT = DEPLOY_ROOT / "templates"
SOURCE_ROOT = DEPLOY_ROOT.parent


def ensure_ssh_key_pair(private_key_path: Path, public_key_path: Path) -> None:
    private_key_path.parent.mkdir(parents=True, exist_ok=True)
    public_key_path.parent.mkdir(parents=True, exist_ok=True)

    if private_key_path.exists() and not public_key_path.exists():
        with public_key_path.open("w", encoding="utf-8") as fp:
            subprocess.run(
                ["ssh-keygen", "-y", "-f", str(private_key_path)],
                check=True,
                stdout=fp,
                stderr=subprocess.DEVNULL,
            )
    elif not private_key_path.exists() and not public_key_path.exists():
        subprocess.run(
            [
                "ssh-keygen",
                "-q",
                "-t",
                "rsa",
                "-b",
                "4096",
                "-N",
                "",
                "-f",
                str(private_key_path),
            ],
            check=True,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
    elif public_key_path.exists() and not private_key_path.exists():
        raise FileNotFoundError(f"SSH private key not found: {private_key_path}")

    private_key_path.chmod(0o600)
    public_key_path.chmod(0o644)


def ensure_self_signed_certificate(cert_path: Path, key_path: Path, host_name: str) -> None:
    cert_path.parent.mkdir(parents=True, exist_ok=True)
    key_path.parent.mkdir(parents=True, exist_ok=True)
    if shutil.which("openssl") is None:
        raise RuntimeError("OpenSSL is required to generate a self-signed nginx certificate")

    if cert_path.exists() and key_path.exists() and cert_path.stat().st_size > 0 and key_path.stat().st_size > 0:
        cert_path.chmod(0o644)
        key_path.chmod(0o600)
        return

    subprocess.run(
        [
            "openssl",
            "req",
            "-x509",
            "-nodes",
            "-newkey",
            "rsa:2048",
            "-sha256",
            "-days",
            "3650",
            "-keyout",
            str(key_path),
            "-out",
            str(cert_path),
            "-subj",
            f"/CN={host_name}",
        ],
        check=True,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    cert_path.chmod(0o644)
    key_path.chmod(0o600)


def ensure_nginx_tls_material(answers: dict[str, object], paths: dict[str, Path]) -> None:
    if not get_bool(answers, "enable_nginx"):
        return
    cert_path = Path(paths["nginx_cert_path"]).resolve()
    key_path = Path(paths["nginx_key_path"]).resolve()
    ensure_self_signed_certificate(cert_path, key_path, str(answers.get("host_name", "localhost")))


def ensure_rootbrowse_ssh_material(paths: dict[str, Path]) -> None:
    private_key_path = Path(
        paths.get("server_ssh_private_key_path", paths["keys_dir"] / "ssh-client" / "id_rsa")
    ).resolve()
    public_key_path = Path(
        paths.get("server_ssh_public_key_path", private_key_path.parent / "id_rsa.pub")
    ).resolve()
    ensure_ssh_key_pair(private_key_path, public_key_path)
    paths["server_ssh_private_key_path"] = private_key_path
    paths["server_ssh_public_key_path"] = public_key_path

    rootbrowse_keys_path = Path(
        paths.get("rootbrowse_authorized_keys_path", paths["keys_dir"] / "rootbrowse_authorized_keys")
    ).resolve()
    rootbrowse_keys_path.parent.mkdir(parents=True, exist_ok=True)
    if (not rootbrowse_keys_path.exists()) or (not rootbrowse_keys_path.read_text(encoding="utf-8").strip()):
        rootbrowse_keys_path.write_text(public_key_path.read_text(encoding="utf-8"), encoding="utf-8")
    rootbrowse_keys_path.chmod(0o600)
    paths["rootbrowse_authorized_keys_path"] = rootbrowse_keys_path


def profile_chain(profile: str) -> list[str]:
    if profile == "custom":
        return ["quickstart", "custom"]
    return [profile]


def yaml_string(value: str) -> str:
    return json.dumps(value, ensure_ascii=False)


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


def render_volume_block(entries: list[str], indent: int = 6) -> str:
    if not entries:
        return ""
    rendered = yaml.safe_dump(entries, default_flow_style=False, sort_keys=False, allow_unicode=True).rstrip()
    prefix = " " * indent
    return "\n" + "\n".join(f"{prefix}{line}" if line else line for line in rendered.splitlines())


def render_optional_single_volume_block(entry: Optional[str], indent: int = 6) -> str:
    if not entry:
        return ""
    rendered = yaml.safe_dump([entry], default_flow_style=False, sort_keys=False, allow_unicode=True).rstrip()
    prefix = " " * indent
    return "\n" + "\n".join(f"{prefix}{line}" if line else line for line in rendered.splitlines())


def render_yaml_list_block(values: list[object], indent: int = 2) -> str:
    rendered = yaml.safe_dump(values, sort_keys=False, allow_unicode=True).rstrip()
    prefix = " " * indent
    return "\n".join(f"{prefix}{line}" if line else line for line in rendered.splitlines())


def default_jobtype_config_block(
    schedd_host: str,
    cm_host: str,
    request_cpus: int,
    request_memory: int,
    indent: int = 2,
) -> str:
    jobtypes = ["vscode", "jupyter", "vnc", "rootbrowse"]
    payload = {
        name: {
            "htc": {
                "RequestMemory": request_memory,
                "RequestCpus": request_cpus,
                "walltime": "default",
                "schedd_host": schedd_host,
                "cm_host": cm_host,
                "extra_param": True,
            }
        }
        for name in jobtypes
    }
    rendered = yaml.safe_dump(payload, sort_keys=False, allow_unicode=True).rstrip()
    prefix = " " * indent
    return "\n".join(f"{prefix}{line}" if line else line for line in rendered.splitlines())


def render_template_text(path: Path, mapping: dict[str, str]) -> str:
    return Template(path.read_text(encoding="utf-8")).substitute(mapping)


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


def deep_merge(base, overlay):
    if isinstance(base, dict) and isinstance(overlay, dict):
        merged = dict(base)
        for key, value in overlay.items():
            if key in merged:
                merged[key] = deep_merge(merged[key], value)
            else:
                merged[key] = value
        return merged
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
    server_port_block: str,
) -> dict[str, str]:
    """Network-related template variables (nginx, ports, host)."""
    nginx_conf_path = deploy_dir / "nginx" / "default.conf"
    return {
        "enable_nginx": str(enable_nginx).lower(),
        "host_name": yaml_string(get_str(answers, "host_name")),
        "host_port": get_str(answers, "host_port"),
        "rootbrowse_port": get_str(answers, "rootbrowse_port"),
        "public_base_url": get_str(answers, "public_base_url"),
        "public_base_url_yaml": yaml_string(get_str(answers, "public_base_url")),
        "host_name_yaml": yaml_string(get_str(answers, "host_name")),
        "nginx_conf_path": str(nginx_conf_path.resolve()),
        "nginx_cert_host_path": str(Path(paths.get("nginx_cert_path", deploy_dir / "nginx" / "cert.pem")).resolve()),
        "nginx_key_host_path": str(Path(paths.get("nginx_key_path", deploy_dir / "nginx" / "key.pem")).resolve()),
        "nginx_cert_container_path": "/etc/nginx/ssl/cert.pem",
        "nginx_key_container_path": "/etc/nginx/ssl/key.pem",
        "server_port_block": server_port_block,
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
        "xrootd_image_raw": get_str(answers, "xrootd_image", "dockerhub.ihep.ac.cn/ink/xrootd-multiuser:5.9.0-3"),
        "xrootd_image": yaml_string(get_str(answers, "xrootd_image", "dockerhub.ihep.ac.cn/ink/xrootd-multiuser:5.9.0-3")),
        "xrootd_conf_path": str(xrootd_conf_path.resolve()),
        "xrootd_data_dir": str(paths["xrootd_data_dir"].resolve()),
        "xrootd_sss_keytab_host_path": str(paths.get("xrootd_sss_keytab_path", paths["xrootd_data_dir"] / "sss.keytab").resolve()),
        "xrootd_sss_keytab_container_path": "/etc/xrootd/sss.keytab",
        "xrootd_vo_list_host_path": str(paths.get("xrootd_vo_list_path", paths["xrootd_data_dir"] / "vo-list.cfg").resolve()),
        "xrootd_vo_list_container_path": "/etc/xrootd/vo-list.cfg",
        "xrootd_vo_list_content": "\n".join(xrootd_vo_entries) + ("\n" if xrootd_vo_entries else ""),
        "xrootd_port": get_str(answers, "xrootd_port", "1094"),
        "xrd_host": yaml_string("root://fastink-xrootd:1098" if enable_xrootd else "root://127.0.0.1:1094"),
        "fs_backend": yaml_string("xrootd"),
        "max_file_size": str(2147483648),
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
        "auth_type": yaml_string("krb5" if enable_krb5 else "password"),
        "security_access": str(False).lower(),
        "ip_whitelist_access": str(False).lower(),
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
) -> dict[str, str]:
    """Computing-related template variables (HTCondor, cluster, job types)."""
    server_condor_conf_host_path = str((deploy_dir / "condor" / "ink.conf").resolve())
    cron_condor_conf_host_path = str((deploy_dir / "condor" / "ink.conf").resolve())
    htcondor_local_conf_host_path = str((deploy_dir / "condor" / "htcondor.local.conf").resolve())
    return {
        "schedd_host": yaml_string(schedd_host),
        "cm_host": yaml_string(cm_host),
        "htcondor_host_name": yaml_string("fastink-htcondor"),
        "htcondor_host_name_plain": cm_host,
        "htcondor_schedd_name": "schedd@fastink-htcondor" if enable_local_htcondor else get_str(answers, "schedd_host", "localhost"),
        "htcondor_auth_method": "CLAIMTOBE",
        "htcondor_fs_domain": htcondor_internal_domain,
        "htcondor_uid_domain": htcondor_internal_domain,
        "enable_local_htcondor": str(enable_local_htcondor).lower(),
        "cluster_list_block": render_yaml_list_block(cluster_list),
        "noenv_jobtype_block": render_yaml_list_block(noenv_jobtype),
        "start_keywords_block": render_yaml_list_block(start_keywords),
        "jobtype_defaults_block": default_jobtype_config_block(
            schedd_host,
            cm_host,
            get_int(answers, "htcondor_default_request_cpus", 1),
            get_int(answers, "htcondor_default_request_memory", 6000),
        ),
        "site": yaml_string("generic"),
        "cluster_scripts": yaml_string("/ink/src/fastink/computing/scripts"),
        "ink_dir": yaml_string("/home/{username}"),
        "gateway_node": yaml_string("localhost"),
        "service_port": str(2000),
        "service_node_yaml": yaml_string("fastink-rootbrowse"),
        "server_condor_conf_host_path": server_condor_conf_host_path,
        "cron_condor_conf_host_path": cron_condor_conf_host_path,
        "htcondor_local_conf_host_path": htcondor_local_conf_host_path,
    }


def _build_mount_mapping(
    answers: dict[str, object],
    paths: dict[str, Path],
    extra_mounts_block: str,
    enable_krb5: bool,
    krb5_conf_host_path: str,
    enable_host_slurm_client: bool,
    slurm_conf_host_path: str,
    munge_socket_dir: str,
    xrootd_krb5_keytab_source_path: str,
    xrootd_krb5_principal: str,
) -> dict[str, str]:
    """Mount-related template variables (extra volumes, krb5, slurm, xrootd keytab)."""
    server_slurm_mounts_block = ""
    cron_slurm_mounts_block = ""
    if enable_host_slurm_client:
        server_slurm_mounts_block = (
            render_optional_single_volume_block(f"{munge_socket_dir}:/var/run/munge/")
            + render_optional_single_volume_block(f"{slurm_conf_host_path}:/etc/slurm/slurm.conf:ro")
        )
        cron_slurm_mounts_block = (
            render_optional_single_volume_block(f"{munge_socket_dir}:/var/run/munge/")
            + render_optional_single_volume_block(f"{slurm_conf_host_path}:/etc/slurm/slurm.conf:ro")
        )
    common_krb5_mount_block = ""
    xrootd_krb5_conf_mount_block = ""
    htcondor_krb5_conf_mount_block = ""
    if enable_krb5:
        common_krb5_mount_block = render_optional_single_volume_block(f"{krb5_conf_host_path}:/etc/krb5.conf:ro")
        xrootd_krb5_conf_mount_block = render_optional_single_volume_block(f"{krb5_conf_host_path}:/etc/krb5.conf:ro")
        htcondor_krb5_conf_mount_block = render_optional_single_volume_block(f"{krb5_conf_host_path}:/etc/krb5.conf:ro")
    return {
        "server_extra_mounts_block": extra_mounts_block,
        "server_krb5_conf_mount_block": common_krb5_mount_block,
        "server_slurm_mounts_block": server_slurm_mounts_block,
        "cron_extra_mounts_block": extra_mounts_block,
        "cron_krb5_conf_mount_block": common_krb5_mount_block,
        "cron_slurm_mounts_block": cron_slurm_mounts_block,
        "rootbrowse_extra_mounts_block": extra_mounts_block,
        "xrootd_extra_mounts_block": extra_mounts_block,
        "htcondor_extra_mounts_block": extra_mounts_block,
        "htcondor_krb5_conf_mount_block": htcondor_krb5_conf_mount_block,
        "xrootd_krb5_conf_mount_block": xrootd_krb5_conf_mount_block,
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
    answers: dict[str, object],
    paths: dict[str, Path],
    deploy_dir: Path,
) -> dict[str, str]:
    """Compose the full template-variable mapping from domain sub-functions.

    The function computes shared values once, then delegates each logical
    group of keys to a dedicated ``_build_*_mapping`` helper.  Remaining
    general-purpose keys are added directly.
    """
    version_env = source_version_env()
    config_path = deploy_dir / "config.yml"

    # ---- shared booleans ----
    enable_nginx = get_bool(answers, "enable_nginx")
    enable_xrootd = get_bool(answers, "enable_xrootd")
    enable_krb5 = get_bool(answers, "enable_krb5")
    enable_local_htcondor = get_bool(answers, "enable_local_htcondor")

    # ---- extra mounts ----
    extra_mount_entries = load_extra_mount_entries(get_str(answers, "extra_mounts_file"))
    extra_mounts_block = render_volume_block(extra_mount_entries)
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

    # ---- krb5 / slurm paths ----
    krb5_conf_host_path = get_str(answers, "krb5_conf_host_path", "/etc/krb5.conf").strip()
    xrootd_krb5_keytab_source_path = get_str(answers, "xrootd_krb5_keytab_source_path").strip()
    xrootd_krb5_principal = get_str(answers, "xrootd_krb5_principal").strip()
    enable_host_slurm_client = get_bool(answers, "enable_host_slurm_client")
    slurm_conf_host_path = get_str(answers, "slurm_conf_host_path", "/etc/slurm/slurm.conf").strip()
    munge_socket_dir = get_str(answers, "munge_socket_dir", "/var/run/munge").strip().rstrip("/")

    # ---- server port ----
    if enable_nginx:
        server_port_block = '    expose:\n      - "8000"'
    else:
        server_port_block = f'    ports:\n      - "{answers["host_port"]}:8000"'

    # ---- compose mapping from sub-functions ----
    mapping: dict[str, str] = {}
    mapping.update(_build_network_mapping(answers, paths, deploy_dir, enable_nginx, server_port_block))
    mapping.update(_build_storage_mapping(answers, paths, deploy_dir, enable_xrootd, xrootd_vo_entries))
    mapping.update(_build_auth_mapping(answers, enable_krb5))
    mapping.update(_build_computing_mapping(
        answers, deploy_dir, schedd_host, cm_host, enable_local_htcondor,
        htcondor_internal_domain, cluster_list, noenv_jobtype, start_keywords,
    ))
    mapping.update(_build_mount_mapping(
        answers, paths, extra_mounts_block, enable_krb5, krb5_conf_host_path,
        enable_host_slurm_client, slurm_conf_host_path, munge_socket_dir,
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
        "server_image_raw": get_str(answers, "server_image"),
        "cron_image_raw": get_str(answers, "cron_image"),
        "rootbrowse_image_raw": get_str(answers, "rootbrowse_image"),
        "htcondor_image_raw": get_str(answers, "htcondor_image", "dockerhub.ihep.ac.cn/ink/fastink-htcondor:latest"),
        "server_image": yaml_string(get_str(answers, "server_image")),
        "cron_image": yaml_string(get_str(answers, "cron_image")),
        "rootbrowse_image": yaml_string(get_str(answers, "rootbrowse_image")),
        "htcondor_image": yaml_string(get_str(answers, "htcondor_image", "dockerhub.ihep.ac.cn/ink/fastink-htcondor:latest")),
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
        "timezone": yaml_string("Asia/Shanghai"),
        "workers": get_str(answers, "workers"),
        "ink_production": str(get_bool(answers, "ink_production")).lower(),
        "init_database": str(get_bool(answers, "init_database")).lower(),
        "server_preload_script_dirs": yaml_string(get_str(answers, "server_preload_script_dirs")),
        "server_preload_scripts": yaml_string(get_str(answers, "server_preload_scripts")),
        "cron_preload_script_dirs": yaml_string(get_str(answers, "cron_preload_script_dirs")),
        "cron_preload_scripts": yaml_string(get_str(answers, "cron_preload_scripts")),
        "rootbrowse_preload_script_dirs": yaml_string(get_str(answers, "rootbrowse_preload_script_dirs")),
        "rootbrowse_preload_scripts": yaml_string(get_str(answers, "rootbrowse_preload_scripts")),
        "source_commit_sha": yaml_string(version_env["source_commit_sha"]),
        "source_commit_date": yaml_string(version_env["source_commit_date"]),
        "source_commit_tag": yaml_string(version_env["source_commit_tag"]),
        "plugin_pip_packages": yaml_string(get_str(answers, "plugin_pip_packages")),
        "plugin_editable_dirs": yaml_string(get_str(answers, "plugin_editable_dirs")),
        "db_name_yaml": yaml_string(get_str(answers, "db_name")),
        "db_user_yaml": yaml_string(get_str(answers, "db_user")),
        "db_password_yaml": yaml_string(get_str(answers, "db_password")),
        "redis_password_yaml": yaml_string(get_str(answers, "redis_password")),
        "app_plugins": yaml_string(""),
        "router_plugins": yaml_string(""),
        "unified_plugin_packages": yaml_string(""),
    })
    return mapping


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
    answers: dict[str, object],
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
    mapping = build_mapping(profile, answers, paths, deploy_dir)
    bundle = {
        "config.yml": render_config(profile, mapping, extra_overlays=config_overlay_paths),
        ".env": render_env(mapping),
        "docker-compose.yml": render_compose(
            profile,
            mapping,
            get_bool(answers, "enable_nginx"),
            get_bool(answers, "enable_xrootd"),
            extra_overlays=compose_overlay_paths,
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
