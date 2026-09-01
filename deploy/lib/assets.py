"""Host asset generation: SSH keypairs and TLS certificates.

Extracted from render.py to keep the rendering engine focused on
template substitution.  These functions are called by render_bundle()
when ``initialize_host_assets=True``.
"""

from __future__ import annotations

import shutil
import subprocess
from pathlib import Path

from lib.types import get_bool


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
