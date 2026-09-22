import subprocess
import tempfile
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Literal

from fastink.auth.backends.krb5 import get_krb5
from fastink.common.config import get_config


@dataclass(frozen=True, slots=True)
class ClusterCredential:
    """Credential material for authenticating to cluster services."""

    kind: Literal["krb5", "ssh-cert"]
    username: str
    ccache_token: str | None = None
    ccache_path: str | None = None
    certificate: str | None = None
    private_key: str | None = None
    expires_at: datetime | None = None


class KerberosProvider:
    """Adapt the existing Kerberos token provider to the delegation contract."""

    def provide(self, username: str) -> ClusterCredential:
        token = get_krb5(username)
        return ClusterCredential(
            kind="krb5", username=username, ccache_token=token
        )


def _sign_ssh_certificate(
    username: str, ca_private_key: str
) -> tuple[str, str, datetime]:
    now = datetime.now(timezone.utc)
    expires_at = now + timedelta(minutes=5)
    start = (now - timedelta(minutes=1)).strftime("%Y%m%d%H%M%S")
    end = expires_at.strftime("%Y%m%d%H%M%S")

    with tempfile.TemporaryDirectory(prefix="fastink-ssh-cert-") as directory:
        root = Path(directory)
        ca_path = root / "ca"
        user_path = root / "user"
        ca_path.write_text(ca_private_key)
        ca_path.chmod(0o600)
        subprocess.run(
            [
                "ssh-keygen",
                "-q",
                "-t",
                "ed25519",
                "-N",
                "",
                "-f",
                str(user_path),
            ],
            check=True,
            capture_output=True,
        )
        subprocess.run(
            [
                "ssh-keygen",
                "-q",
                "-s",
                str(ca_path),
                "-I",
                f"fastink:{username}",
                "-n",
                username,
                "-V",
                f"{start}:{end}",
                str(user_path) + ".pub",
            ],
            check=True,
            capture_output=True,
        )
        return (
            (user_path.with_name("user-cert.pub")).read_text(),
            user_path.read_text(),
            expires_at,
        )


class SshCaProvider:
    """Create a short-lived SSH user certificate signed by the configured CA."""

    def provide(self, username: str) -> ClusterCredential:
        delegation = get_config("auth", "delegation", fallback={})
        ca_private_key = delegation.get("ca_private_key", "")
        if not ca_private_key:
            raise ValueError("SSH certificate delegation CA is not configured")

        # ponytail: use one ephemeral keypair per credential; persistence is unnecessary here.
        certificate, private_key, expires_at = _sign_ssh_certificate(
            username, ca_private_key
        )
        return ClusterCredential(
            kind="ssh-cert",
            username=username,
            certificate=certificate,
            private_key=private_key,
            expires_at=expires_at,
        )


def get_cluster_credential(username: str) -> ClusterCredential:
    """Return the configured cluster credential for ``username``."""

    krb5_enabled = get_config(
        "common", "krb5_enabled", fallback=False, type=bool
    )
    provider = KerberosProvider() if krb5_enabled else SshCaProvider()
    return provider.provide(username)


def _ccache_expiry(ccache_token: str) -> str:
    import base64
    from fastink.common.utils import parse_ccache
    parsed = parse_ccache(base64.b64decode(ccache_token))
    return datetime.fromtimestamp(parsed["expired_at"]).isoformat()


def export_credential(principal) -> dict:
    username = principal.username
    cred = get_cluster_credential(username)
    result = {"username": username, "kind": cred.kind}
    if cred.kind == "krb5":
        result["ccache"] = cred.ccache_token
        result["expires_at"] = _ccache_expiry(cred.ccache_token)
    else:
        result["certificate"] = cred.certificate
        result["private_key"] = cred.private_key
        result["expires_at"] = cred.expires_at.isoformat() if cred.expires_at else None
    return result
