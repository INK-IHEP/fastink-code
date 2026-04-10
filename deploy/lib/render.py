import json
import shutil
import subprocess
from pathlib import Path
from string import Template
from typing import Optional

import yaml


DEPLOY_ROOT = Path(__file__).resolve().parent.parent
TEMPLATE_ROOT = DEPLOY_ROOT / "templates"


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
    if not bool(answers.get("enable_nginx")):
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
    if profile == "full":
        return ["minimal", "full"]
    return [profile]


def yaml_string(value: str) -> str:
    return json.dumps(value, ensure_ascii=False)


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
    prefix = " " * indent
    return "\n" + "\n".join(f"{prefix}- {entry}" for entry in entries)


def render_yaml_list_block(values: list[object], indent: int = 2) -> str:
    rendered = yaml.safe_dump(values, sort_keys=False, allow_unicode=True).rstrip()
    prefix = " " * indent
    return "\n".join(f"{prefix}{line}" if line else line for line in rendered.splitlines())


def default_jobtype_config_block(schedd_host: str, cm_host: str, indent: int = 2) -> str:
    jobtypes = ["vscode", "jupyter", "vnc", "rootbrowse"]
    payload = {
        name: {
            "htc": {
                "RequestMemory": 6000,
                "RequestCpus": 1,
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


def build_mapping(
    profile: str,
    answers: dict[str, object],
    paths: dict[str, Path],
    deploy_dir: Path,
) -> dict[str, str]:
    config_path = deploy_dir / "config.yml"
    nginx_conf_path = deploy_dir / "nginx" / "default.conf"
    xrootd_conf_path = deploy_dir / "xrootd" / "xrootd-proxy.cfg"
    rootbrowse_keys_host_path = paths.get(
        "rootbrowse_authorized_keys_path",
        paths["keys_dir"] / "rootbrowse_authorized_keys",
    )
    server_ssh_private_key_path = paths.get(
        "server_ssh_private_key_path",
        paths["keys_dir"] / "ssh-client" / "id_rsa",
    )
    enable_nginx = bool(answers["enable_nginx"])
    enable_xrootd = bool(answers.get("enable_xrootd", False))
    enable_local_htcondor = bool(answers.get("enable_local_htcondor", False))
    extra_mount_entries = load_extra_mount_entries(answers.get("extra_mounts_file", ""))
    extra_mounts_block = render_volume_block(extra_mount_entries)
    xrootd_vo_entries = build_xrootd_vo_entries(extra_mount_entries)
    schedd_host = "fastink-htcondor" if enable_local_htcondor else str(answers.get("schedd_host", "localhost"))
    cm_host = "fastink-htcondor" if enable_local_htcondor else str(answers.get("cm_host", "localhost"))
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

    if enable_nginx:
        server_port_block = '    expose:\n      - "8000"'
    else:
        server_port_block = f'    ports:\n      - "{answers["host_port"]}:8000"'

    return {
        "profile": profile,
        "image_source": str(answers["image_source"]),
        "server_image_raw": str(answers["server_image"]),
        "cron_image_raw": str(answers["cron_image"]),
        "rootbrowse_image_raw": str(answers["rootbrowse_image"]),
        "htcondor_image_raw": str(answers.get("htcondor_image", "dockerhub.ihep.ac.cn/ink/fastink-htcondor:latest")),
        "server_image": yaml_string(str(answers["server_image"])),
        "cron_image": yaml_string(str(answers["cron_image"])),
        "rootbrowse_image": yaml_string(str(answers["rootbrowse_image"])),
        "htcondor_image": yaml_string(str(answers.get("htcondor_image", "dockerhub.ihep.ac.cn/ink/fastink-htcondor:latest"))),
        "project_name": str(answers["project_name"]),
        "public_base_url": str(answers["public_base_url"]),
        "db_name": str(answers["db_name"]),
        "db_user": str(answers["db_user"]),
        "db_password": str(answers["db_password"]),
        "db_root_password": str(answers["db_root_password"]),
        "redis_password": str(answers["redis_password"]),
        "data_root": str(paths["data_root"]),
        "config_path": str(config_path.resolve()),
        "db_data_dir": str(paths["db_data_dir"].resolve()),
        "redis_data_dir": str(paths["redis_data_dir"].resolve()),
        "etc_init_dir": str(paths["etc_init_dir"].resolve()),
        "tmp_dir": str(paths["tmp_dir"].resolve()),
        "plugins_dir": str(paths["plugins_dir"].resolve()),
        "keys_dir": str(paths["keys_dir"].resolve()),
        "server_ssh_dir_host_path": str(Path(server_ssh_private_key_path).resolve().parent),
        "server_ssh_dir_container_path": "/root/.ssh",
        "preload_server_dir": str(paths["preload_server_dir"].resolve()),
        "preload_cron_dir": str(paths["preload_cron_dir"].resolve()),
        "preload_rootbrowse_dir": str(paths["preload_rootbrowse_dir"].resolve()),
        "nginx_conf_path": str(nginx_conf_path.resolve()),
        "nginx_cert_host_path": str(Path(paths.get("nginx_cert_path", deploy_dir / "nginx" / "cert.pem")).resolve()),
        "nginx_key_host_path": str(Path(paths.get("nginx_key_path", deploy_dir / "nginx" / "key.pem")).resolve()),
        "nginx_cert_container_path": "/etc/nginx/ssl/cert.pem",
        "nginx_key_container_path": "/etc/nginx/ssl/key.pem",
        "xrootd_conf_path": str(xrootd_conf_path.resolve()),
        "xrootd_data_dir": str(paths["xrootd_data_dir"].resolve()),
        "xrootd_sss_keytab_host_path": str(paths.get("xrootd_sss_keytab_path", paths["xrootd_data_dir"] / "sss.keytab").resolve()),
        "xrootd_sss_keytab_container_path": "/etc/xrootd/sss.keytab",
        "xrootd_krb5_keytab_host_path": str(paths.get("xrootd_krb5_keytab_path", paths["xrootd_data_dir"] / "krb5.keytab").resolve()),
        "xrootd_krb5_keytab_container_path": "/etc/xrootd/krb5.keytab",
        "xrootd_vo_list_host_path": str(paths.get("xrootd_vo_list_path", paths["xrootd_data_dir"] / "vo-list.cfg").resolve()),
        "xrootd_vo_list_container_path": "/etc/xrootd/vo-list.cfg",
        "xrootd_vo_list_content": "\n".join(xrootd_vo_entries) + ("\n" if xrootd_vo_entries else ""),
        "rootbrowse_authorized_keys_host_path": str(rootbrowse_keys_host_path.resolve()),
        "rootbrowse_authorized_keys_container_path": "/run/fastink/rootbrowse_authorized_keys",
        "timezone": yaml_string("Asia/Shanghai"),
        "workers": str(answers["workers"]),
        "ink_production": str(bool(answers["ink_production"])).lower(),
        "init_database": str(bool(answers["init_database"])).lower(),
        "enable_nginx": str(enable_nginx).lower(),
        "enable_xrootd": str(enable_xrootd).lower(),
        "host_name": yaml_string(str(answers["host_name"])),
        "host_port": str(answers["host_port"]),
        "rootbrowse_port": str(answers["rootbrowse_port"]),
        "xrootd_port": str(answers.get("xrootd_port", 1094)),
        "xrootd_image_raw": str(answers.get("xrootd_image", "dockerhub.ihep.ac.cn/ink/xrootd-multiuser:5.9.0-3")),
        "xrootd_image": yaml_string(str(answers.get("xrootd_image", "dockerhub.ihep.ac.cn/ink/xrootd-multiuser:5.9.0-3"))),
        "rootbrowse_container_port": "2000",
        "server_port_block": server_port_block,
        "server_preload_script_dirs": yaml_string(str(answers["server_preload_script_dirs"])),
        "server_preload_scripts": yaml_string(str(answers["server_preload_scripts"])),
        "cron_preload_script_dirs": yaml_string(str(answers["cron_preload_script_dirs"])),
        "cron_preload_scripts": yaml_string(str(answers["cron_preload_scripts"])),
        "rootbrowse_preload_script_dirs": yaml_string(str(answers["rootbrowse_preload_script_dirs"])),
        "rootbrowse_preload_scripts": yaml_string(str(answers["rootbrowse_preload_scripts"])),
        "plugin_pip_packages": yaml_string(str(answers.get("plugin_pip_packages", ""))),
        "plugin_editable_dirs": yaml_string(str(answers.get("plugin_editable_dirs", ""))),
        "server_extra_mounts_block": extra_mounts_block,
        "cron_extra_mounts_block": extra_mounts_block,
        "rootbrowse_extra_mounts_block": extra_mounts_block,
        "xrootd_extra_mounts_block": extra_mounts_block,
        "htcondor_extra_mounts_block": extra_mounts_block,
        "krb5_enabled": str(False).lower(),
        "security_access": str(False).lower(),
        "ip_whitelist_access": str(False).lower(),
        "db_name_yaml": yaml_string(str(answers["db_name"])),
        "db_user_yaml": yaml_string(str(answers["db_user"])),
        "db_password_yaml": yaml_string(str(answers["db_password"])),
        "redis_password_yaml": yaml_string(str(answers["redis_password"])),
        "public_base_url_yaml": yaml_string(str(answers["public_base_url"])),
        "host_name_yaml": yaml_string(str(answers["host_name"])),
        "fs_backend": yaml_string("xrootd"),
        "xrd_host": yaml_string("root://fastink-xrootd:1098" if enable_xrootd else "root://127.0.0.1:1094"),
        "max_file_size": str(2147483648),
        "site": yaml_string("generic"),
        "cluster_list_block": render_yaml_list_block(cluster_list),
        "noenv_jobtype_block": render_yaml_list_block(noenv_jobtype),
        "schedd_host": yaml_string(schedd_host),
        "cm_host": yaml_string(cm_host),
        "xrootd_path": yaml_string("root://fastink-xrootd:1098/" if enable_xrootd else "root://127.0.0.1:1094/"),
        "gateway_node": yaml_string("localhost"),
        "cluster_scripts": yaml_string("/ink/src/fastink/computing/scripts"),
        "ink_dir": yaml_string("/home/{username}"),
        "start_keywords_block": render_yaml_list_block(start_keywords),
        "jobtype_defaults_block": default_jobtype_config_block(schedd_host, cm_host),
        "app_plugins": yaml_string(""),
        "router_plugins": yaml_string(""),
        "unified_plugin_packages": yaml_string(""),
        "service_port": str(2000),
        "service_node_yaml": yaml_string("fastink-rootbrowse"),
        "enable_local_htcondor": str(enable_local_htcondor).lower(),
        "htcondor_host_name": yaml_string("fastink-htcondor"),
    }


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
    return render_template_text(TEMPLATE_ROOT / "base" / "xrootd-proxy.cfg.tpl", mapping)


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
            bool(answers["enable_nginx"]),
            bool(answers.get("enable_xrootd", False)),
            extra_overlays=compose_overlay_paths,
        ),
    }
    if bool(answers["enable_nginx"]):
        bundle["nginx/default.conf"] = render_nginx_conf(mapping)
    if bool(answers.get("enable_xrootd", False)):
        bundle["xrootd/xrootd-proxy.cfg"] = render_xrootd_conf(mapping)
        bundle["xrootd/vo-list.cfg"] = str(mapping.get("xrootd_vo_list_content", ""))
    return bundle
