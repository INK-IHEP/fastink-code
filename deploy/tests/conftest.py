"""Shared test fixtures for the deploy subsystem.

Adds deploy/ to sys.path while protecting the stdlib cmd/ module
from being shadowed.
"""
import sys
from pathlib import Path

# Preload stdlib cmd before deploy/ is inserted into sys.path
import cmd as _stdlib_cmd  # noqa: E402

_DEPLOY_ROOT = Path(__file__).resolve().parent.parent
if str(_DEPLOY_ROOT) not in sys.path:
    sys.path.insert(0, str(_DEPLOY_ROOT))

# stdlib cmd must stay in sys.modules, otherwise pdb will pick up
# deploy/cmd/ (which has no cmd.Cmd) and crash
sys.modules["cmd"] = _stdlib_cmd

import pytest  # noqa: E402


@pytest.fixture
def sample_answers() -> dict[str, object]:
    return {
        "profile": "quickstart",
        "image_source": "pull",
        "server_image": "ink/server:latest",
        "cron_image": "ink/cron:latest",
        "rootbrowse_image": "ink/rootbrowse:latest",
        "host_name": "localhost",
        "host_port": 8000,
        "public_base_url": "http://localhost:8000",
        "enable_nginx": False,
        "data_root": "/data/ink",
        "enable_xrootd": False,
        "enable_krb5": False,
        "enable_local_htcondor": False,
        "enable_host_slurm_client": False,
        "project_name": "fastink-test",
        "ink_production": False,
        "init_database": True,
        "db_name": "fastink",
        "db_user": "fastink",
        "db_root_password": "root123",
        "db_password": "pass123",
        "redis_password": "redis123",
        "workers": 4,
    }


@pytest.fixture
def compose_file(tmp_path: Path) -> Path:
    return tmp_path / "docker-compose.yml"
