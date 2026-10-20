"""Host account sync: sync-accounts.sh behavior and compose rendering."""
import os
import stat
import subprocess
import time
from pathlib import Path

import pytest
import yaml

from deploy.lib.defaults import normalize_answers, parse_override_value
from deploy.lib.render import (
    build_compose_port_overlay,
    build_mapping,
    render_compose,
)

DEPLOY_ROOT = Path(__file__).resolve().parent.parent
INIT_DIR = DEPLOY_ROOT / "images" / "init"


@pytest.fixture
def sample_paths(tmp_path: Path) -> dict[str, Path]:
    data_root = tmp_path / "data"
    data_root.mkdir()
    keys_dir = tmp_path / "keys"
    (keys_dir / "ssh-client").mkdir(parents=True)
    preload = data_root / "preload"
    for name in ("server", "cron", "rootbrowse"):
        (preload / name).mkdir(parents=True)
    xrootd_data_dir = data_root / "xrootd"
    xrootd_data_dir.mkdir(parents=True)
    return {
        "data_root": data_root,
        "db_data_dir": data_root / "db",
        "redis_data_dir": data_root / "redis",
        "etc_init_dir": data_root / "etc-init",
        "tmp_dir": data_root / "tmp",
        "plugins_dir": data_root / "plugins",
        "keys_dir": keys_dir,
        "preload_server_dir": preload / "server",
        "preload_cron_dir": preload / "cron",
        "preload_rootbrowse_dir": preload / "rootbrowse",
        "xrootd_data_dir": xrootd_data_dir,
    }


@pytest.fixture
def sample_answers() -> dict:
    return {
        "profile": "quickstart",
        "host_name": "localhost",
        "host_port": "8000",
        "rootbrowse_port": "2001",
        "public_base_url": "http://localhost:8000",
        "image_source": "pull",
        "init_image": "dockerhub.ihep.ac.cn/ink/fastink-init:latest",
        "server_image": "dockerhub.ihep.ac.cn/ink/fastink-server:latest",
        "cron_image": "dockerhub.ihep.ac.cn/ink/fastink-cron:latest",
        "rootbrowse_image": "dockerhub.ihep.ac.cn/ink/fastink-rootbrowse:latest",
        "htcondor_image": "dockerhub.ihep.ac.cn/ink/fastink-htcondor:latest",
        "xrootd_image": "dockerhub.ihep.ac.cn/ink/xrootd-multiuser:5.9.0-3",
        "db_image": "dockerhub.ihep.ac.cn/ink/base-mariadb:10.5",
        "redis_image": "dockerhub.ihep.ac.cn/ink/base-redis:7-alpine",
        "nginx_image": "dockerhub.ihep.ac.cn/ink/base-nginx:latest",
        "project_name": "fastink",
        "db_name": "inkdb",
        "db_user": "inkdbuser",
        "db_root_password": "rootpw",
        "db_password": "dbpw",
        "redis_password": "redispw",
        "file_share_jwt_secret": "jwt-secret",
        "workers": 4,
        "ink_production": False,
        "init_database": True,
        "security_access": True,
        "enable_nginx": False,
        "enable_xrootd": False,
        "enable_krb5": False,
        "enable_local_htcondor": False,
        "enable_host_slurm_client": False,
        "enable_accounts_sync": True,
        "accounts_sync_interval": 60,
    }


def _render_compose(profile: str, answers: dict, paths: dict, deploy_dir: Path) -> dict:
    mapping = build_mapping(profile, answers, paths, deploy_dir)
    rendered = render_compose(
        profile,
        mapping,
        enable_nginx=False,
        enable_xrootd=False,
        port_overlay=build_compose_port_overlay(enable_nginx=False, host_port=8000),
    )
    return yaml.safe_load(rendered)


class TestComposeRendering:
    def test_quickstart_includes_account_sync(self, sample_answers, sample_paths, tmp_path):
        data = _render_compose("quickstart", sample_answers, sample_paths, tmp_path)
        service = data["services"]["fastink-accounts-sync"]
        assert service["image"] == sample_answers["init_image"]
        entrypoint = service["entrypoint"]
        assert entrypoint[0] == "/bin/sh"
        assert entrypoint[1] == "-c"
        assert "sync-accounts-loop.sh" in entrypoint[2]
        assert service["environment"]["SLEEP_TIME"] == "60"
        assert "/etc:/host-etc:ro" in service["volumes"]
        assert f"{sample_paths['etc_init_dir']}:/etc-init" in service["volumes"]

    def test_disabled_omits_account_sync(self, sample_answers, sample_paths, tmp_path):
        sample_answers["enable_accounts_sync"] = False
        data = _render_compose("quickstart", sample_answers, sample_paths, tmp_path)
        assert "fastink-accounts-sync" not in data["services"]

    def test_profile_defaults(self):
        quickstart = normalize_answers({"profile": "quickstart"})
        custom = normalize_answers({"profile": "custom"})
        assert quickstart["enable_accounts_sync"] is True
        assert quickstart["accounts_sync_interval"] == 60
        assert custom["enable_accounts_sync"] is False
        assert parse_override_value("enable_accounts_sync", "false") is False
        assert parse_override_value("accounts_sync_interval", "15") == 15
        assert parse_override_value("accounts_sync_interval", "5") == 5
        with pytest.raises(ValueError):
            parse_override_value("accounts_sync_interval", "0")
        # Answers files are clamped to the documented floor; garbage falls back.
        assert normalize_answers({"profile": "quickstart", "accounts_sync_interval": 0})["accounts_sync_interval"] == 5
        assert normalize_answers({"profile": "quickstart", "accounts_sync_interval": -30})["accounts_sync_interval"] == 5
        assert normalize_answers({"profile": "quickstart", "accounts_sync_interval": "nope"})["accounts_sync_interval"] == 60


def _write_host_accounts(
    host_etc: Path,
    passwd: str,
    group: str,
    shadow: str,
    gshadow: str,
) -> None:
    (host_etc / "passwd").write_text(passwd)
    (host_etc / "group").write_text(group)
    (host_etc / "shadow").write_text(shadow)
    (host_etc / "gshadow").write_text(gshadow)


def _sync_env(host_etc: Path, etc_init: Path, **extra: str) -> dict[str, str]:
    env = os.environ.copy()
    env.update({
        "FASTINK_ACCOUNTS_LIB": str(INIT_DIR / "accounts-lib.sh"),
        "FASTINK_HOST_ETC": str(host_etc),
        "FASTINK_ETC_INIT": str(etc_init),
        "FASTINK_ENABLE_LOCAL_HTCONDOR": "false",
        "FASTINK_ENABLE_HOST_SLURM_CLIENT": "false",
    })
    env.update(extra)
    return env


def _run_sync(env: dict[str, str]) -> subprocess.CompletedProcess:
    return subprocess.run(
        ["bash", str(INIT_DIR / "sync-accounts.sh")],
        env=env,
        capture_output=True,
        text=True,
    )


class TestSyncScript:
    def test_rebuilds_and_normalizes_host_accounts(self, tmp_path):
        host_etc = tmp_path / "host-etc"
        host_etc.mkdir()
        etc_init = tmp_path / "etc-init"
        etc_init.mkdir()
        _write_host_accounts(
            host_etc,
            "root:x:0:0:root:/root:/bin/bash\n"
            "inkuser:x:1000:1000::/home/inkuser:/bin/bash\n",
            "root:x:0:\ninkuser:x:1000:\n",
            "root:*:19000:0:99999:7:::\ninkuser:$6$hash:19000:0:99999:7:::\n",
            "root:!::\ninkuser:!::\n",
        )

        result = _run_sync(_sync_env(
            host_etc, etc_init, FASTINK_ENABLE_LOCAL_HTCONDOR="true"
        ))
        assert result.returncode == 0, result.stderr

        passwd = (etc_init / "passwd").read_text()
        assert "inkuser:x:1000:1000::/home/inkuser:/bin/bash" in passwd
        assert "xrootd:x:379:978:" in passwd
        assert "condor:x:64:64:" in passwd
        assert "sshd:x:74:74:" in passwd
        assert "slurm:" not in passwd
        assert stat.S_IMODE((etc_init / "passwd").stat().st_mode) == 0o644
        assert stat.S_IMODE((etc_init / "shadow").stat().st_mode) == 0o600

        # Host changes: drop inkuser, add another account, update root shell.
        _write_host_accounts(
            host_etc,
            "root:x:0:0:root:/root:/bin/zsh\n"
            "newuser:x:1001:1001::/home/newuser:/bin/bash\n",
            "root:x:0:\nnewuser:x:1001:\n",
            "root:*:19000:0:99999:7:::\nnewuser:$6$hash2:19000:0:99999:7:::\n",
            "root:!::\nnewuser:!::\n",
        )
        result = _run_sync(_sync_env(
            host_etc, etc_init, FASTINK_ENABLE_LOCAL_HTCONDOR="true"
        ))
        assert result.returncode == 0, result.stderr

        passwd = (etc_init / "passwd").read_text()
        assert "inkuser:" not in passwd
        assert "newuser:x:1001:1001:" in passwd
        assert "root:x:0:0:root:/root:/bin/zsh" in passwd
        # Fixed identities are re-applied after the rebuild.
        assert "xrootd:x:379:978:" in passwd
        assert "condor:x:64:64:" in passwd

    @pytest.mark.parametrize("conflict", ["uid379", "gid978", "uid64"])
    def test_fixed_identity_conflict_keeps_previous_view(self, tmp_path, conflict):
        host_etc = tmp_path / "host-etc"
        host_etc.mkdir()
        etc_init = tmp_path / "etc-init"
        etc_init.mkdir()

        passwd = ["root:x:0:0:root:/root:/bin/bash"]
        group = ["root:x:0:"]
        shadow = ["root:*:19000:0:99999:7:::"]
        gshadow = ["root:!::"]
        extra = {"FASTINK_ENABLE_LOCAL_HTCONDOR": "false"}

        if conflict == "uid379":
            passwd.append("other:x:379:379::/home/other:/bin/bash")
            group.append("other:x:379:")
            shadow.append("other:!:19000:0:99999:7:::")
            gshadow.append("other:!::")
        elif conflict == "gid978":
            group.append("other:x:978:")
            gshadow.append("other:!::")
        else:  # uid64, only enforced when local HTCondor normalization runs
            passwd.append("other:x:64:64::/home/other:/bin/bash")
            group.append("other:x:64:")
            shadow.append("other:!:19000:0:99999:7:::")
            gshadow.append("other:!::")
            extra["FASTINK_ENABLE_LOCAL_HTCONDOR"] = "true"

        _write_host_accounts(
            host_etc,
            "\n".join(passwd) + "\n",
            "\n".join(group) + "\n",
            "\n".join(shadow) + "\n",
            "\n".join(gshadow) + "\n",
        )

        sentinels = {
            "passwd": "root:x:0:0:root:/root:/bin/bash\n",
            "group": "root:x:0:\n",
            "shadow": "root:*:19000:0:99999:7:::\n",
            "gshadow": "root:!::\n",
        }
        for name, content in sentinels.items():
            (etc_init / name).write_text(content)

        result = _run_sync(_sync_env(host_etc, etc_init, **extra))

        assert result.returncode != 0
        assert "Conflicting host account state" in result.stderr
        # Nothing is published when the fixed-identity guard trips.
        for name, content in sentinels.items():
            assert (etc_init / name).read_text() == content

    @pytest.mark.parametrize("link", ["relative", "absolute-etc"])
    def test_host_symlinked_account_files_are_resolved(self, tmp_path, link):
        host_etc = tmp_path / "host-etc"
        host_etc.mkdir()
        etc_init = tmp_path / "etc-init"
        etc_init.mkdir()

        passwd_content = (
            "root:x:0:0:root:/root:/bin/bash\n"
            "alice:x:1000:1000::/home/alice:/bin/bash\n"
        )
        if link == "relative":
            (host_etc / "passwd.real").write_text(passwd_content)
            (host_etc / "passwd").symlink_to("passwd.real")
        else:
            (host_etc / "static").mkdir()
            (host_etc / "static" / "passwd").write_text(passwd_content)
            (host_etc / "passwd").symlink_to("/etc/static/passwd")

        for name, content in {
            "group": "root:x:0:\nalice:x:1000:\n",
            "shadow": "root:*:19000:0:99999:7:::\nalice:$6$hash:19000:0:99999:7:::\n",
            "gshadow": "root:!::\nalice:!::\n",
        }.items():
            (host_etc / name).write_text(content)

        result = _run_sync(_sync_env(host_etc, etc_init))
        assert result.returncode == 0, result.stderr
        assert not (etc_init / "passwd").is_symlink()
        assert "alice:x:1000:1000:" in (etc_init / "passwd").read_text()


    def test_empty_host_account_file_is_allowed(self, tmp_path):
        host_etc = tmp_path / "host-etc"
        host_etc.mkdir()
        etc_init = tmp_path / "etc-init"
        etc_init.mkdir()
        _write_host_accounts(
            host_etc,
            "root:x:0:0:root:/root:/bin/bash\n",
            "root:x:0:\n",
            "root:*:19000:0:99999:7:::\n",
            "",  # an empty /etc/gshadow is legal on some hosts
        )

        result = _run_sync(_sync_env(host_etc, etc_init))

        assert result.returncode == 0, result.stderr
        assert "sshd:!::" in (etc_init / "gshadow").read_text()

    def test_symlink_escaping_host_etc_is_rejected(self, tmp_path):
        host_etc = tmp_path / "host-etc"
        host_etc.mkdir()
        etc_init = tmp_path / "etc-init"
        etc_init.mkdir()
        (tmp_path / "outside-passwd").write_text("root:x:0:0:root:/root:/bin/bash\n")
        _write_host_accounts(
            host_etc,
            "placeholder\n",
            "root:x:0:\n",
            "root:*:19000:0:99999:7:::\n",
            "root:!::\n",
        )
        (host_etc / "passwd").unlink()
        (host_etc / "passwd").symlink_to("../../outside-passwd")
        (etc_init / "passwd").write_text("sentinel\n")

        result = _run_sync(_sync_env(host_etc, etc_init))

        assert result.returncode != 0
        assert "outside" in result.stderr or "refusing path" in result.stderr
        assert (etc_init / "passwd").read_text() == "sentinel\n"

    def test_missing_host_file_keeps_previous_view(self, tmp_path):
        host_etc = tmp_path / "host-etc"
        host_etc.mkdir()
        etc_init = tmp_path / "etc-init"
        etc_init.mkdir()
        (etc_init / "passwd").write_text("root:x:0:0:root:/root:/bin/bash\n")
        for name in ("group", "shadow", "gshadow"):
            (etc_init / name).write_text("")

        # Only passwd exists on the host: sync must fail before publishing.
        (host_etc / "passwd").write_text("root:x:0:0:root:/root:/bin/bash\n")
        result = _run_sync(_sync_env(host_etc, etc_init))
        assert result.returncode != 0
        assert (etc_init / "passwd").read_text() == "root:x:0:0:root:/root:/bin/bash\n"

def _wait_until(predicate, timeout: float = 15.0, interval: float = 0.2) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(interval)
    raise AssertionError(f"condition not met within {timeout:.1f}s")


class TestSyncLoop:
    def test_self_heals_view_and_stops_on_sigterm(self, tmp_path):
        """The loop rebuilds a manually corrupted view and exits on SIGTERM."""
        host_etc = tmp_path / "host-etc"
        host_etc.mkdir()
        etc_init = tmp_path / "etc-init"
        etc_init.mkdir()
        _write_host_accounts(
            host_etc,
            "root:x:0:0:root:/root:/bin/bash\n"
            "alice:x:1000:1000::/home/alice:/bin/bash\n",
            "root:x:0:\nalice:x:1000:\n",
            "root:*:19000:0:99999:7:::\nalice:$6$hash:19000:0:99999:7:::\n",
            "root:!::\nalice:!::\n",
        )
        env = _sync_env(host_etc, etc_init)
        env["SLEEP_TIME"] = "5"
        env["FASTINK_SYNC_STATE_FILE"] = str(tmp_path / "state")
        env["FASTINK_SYNC_SCRIPT"] = str(INIT_DIR / "sync-accounts.sh")

        proc = subprocess.Popen(
            ["bash", str(INIT_DIR / "sync-accounts-loop.sh")],
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
        )
        try:
            def has_alice() -> bool:
                path = etc_init / "passwd"
                return path.exists() and "alice:x:1000:1000:" in path.read_text()

            _wait_until(has_alice)
            # Manual edit (or an interrupted publish) must self-heal.
            (etc_init / "passwd").write_text("corrupted\n")
            _wait_until(has_alice)

            started = time.monotonic()
            proc.terminate()
            returncode = proc.wait(timeout=5)
            elapsed = time.monotonic() - started
            assert returncode == 0
            assert elapsed < 5
        finally:
            if proc.poll() is None:
                proc.kill()
                proc.wait()
