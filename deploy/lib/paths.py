from __future__ import annotations

from pathlib import Path
from typing import Any



def ensure_dir(path: Path) -> Path:
    path.mkdir(parents=True, exist_ok=True)
    return path



def build_runtime_paths(
    *,
    output_dir: Path,
    data_root: Path,
    enable_nginx: bool,
    enable_xrootd: bool,
    db_data_dir: Path | None = None,
    redis_data_dir: Path | None = None,
    etc_init_dir: Path | None = None,
    tmp_dir: Path | None = None,
    plugins_dir: Path | None = None,
    keys_dir: Path | None = None,
    preload_server_dir: Path | None = None,
    preload_cron_dir: Path | None = None,
    preload_rootbrowse_dir: Path | None = None,
    rootbrowse_authorized_keys_path: Path | None = None,
    server_ssh_private_key_path: Path | None = None,
    nginx_cert_path: Path | None = None,
    nginx_key_path: Path | None = None,
) -> tuple[Path, dict[str, Path]]:
    output_dir = output_dir.resolve()
    data_root = data_root.resolve()
    paths: dict[str, Path] = {
        'data_root': data_root,
        'db_data_dir': (db_data_dir or (data_root / 'db')).resolve(),
        'redis_data_dir': (redis_data_dir or (data_root / 'redis')).resolve(),
        'xrootd_data_dir': (data_root / 'xrootd').resolve(),
        'etc_init_dir': (etc_init_dir or (output_dir / 'runtime' / 'etc-init')).resolve(),
        'tmp_dir': (tmp_dir or (output_dir / 'runtime' / 'tmp')).resolve(),
        'plugins_dir': (plugins_dir or (output_dir / 'plugins')).resolve(),
        'keys_dir': (keys_dir or (output_dir / 'keys')).resolve(),
        'preload_server_dir': (preload_server_dir or (output_dir / 'preload' / 'server')).resolve(),
        'preload_cron_dir': (preload_cron_dir or (output_dir / 'preload' / 'cron')).resolve(),
        'preload_rootbrowse_dir': (
            preload_rootbrowse_dir or (output_dir / 'preload' / 'rootbrowse')
        ).resolve(),
    }
    if enable_nginx:
        paths['nginx_dir'] = (output_dir / 'nginx').resolve()
        paths['nginx_cert_path'] = (nginx_cert_path or (paths['nginx_dir'] / 'cert.pem')).resolve()
        paths['nginx_key_path'] = (nginx_key_path or (paths['nginx_dir'] / 'key.pem')).resolve()
    if enable_xrootd:
        paths['xrootd_dir'] = (output_dir / 'xrootd').resolve()
        paths['xrootd_sss_keytab_path'] = (paths['xrootd_dir'] / 'sss.keytab').resolve()
        paths['xrootd_krb5_keytab_path'] = (paths['xrootd_dir'] / 'krb5.keytab').resolve()
        paths['xrootd_vo_list_path'] = (paths['xrootd_dir'] / 'vo-list.cfg').resolve()

    for key, path in paths.items():
        if key.endswith('_path'):
            continue
        ensure_dir(path)

    if enable_nginx:
        for key in ('nginx_cert_path', 'nginx_key_path'):
            target = paths[key]
            target.parent.mkdir(parents=True, exist_ok=True)
            if not target.exists():
                target.touch()

    if enable_xrootd:
        target = paths['xrootd_krb5_keytab_path']
        target.parent.mkdir(parents=True, exist_ok=True)
        if not target.exists():
            target.touch()
        vo_list_target = paths['xrootd_vo_list_path']
        vo_list_target.parent.mkdir(parents=True, exist_ok=True)
        if not vo_list_target.exists():
            vo_list_target.touch()

    resolved_rootbrowse_keys = (
        rootbrowse_authorized_keys_path or (paths['keys_dir'] / 'rootbrowse_authorized_keys')
    ).resolve()
    resolved_rootbrowse_keys.parent.mkdir(parents=True, exist_ok=True)
    if not resolved_rootbrowse_keys.exists():
        resolved_rootbrowse_keys.touch()
    paths['rootbrowse_authorized_keys_path'] = resolved_rootbrowse_keys

    if server_ssh_private_key_path is not None:
        paths['server_ssh_private_key_path'] = server_ssh_private_key_path.resolve()

    return output_dir, paths
