#! /usr/bin/python3
# FileName      : test_service.py
# Author        : HAN Xiao
# Email         : hanx@ihep.ac.cn
# Date          : Tue Jun 17 14:31:38 2025 CST
# Last modified : Thu Oct 16 17:28:00 2025 CST
# Description   :

from urllib.parse import urlparse

from fastapi.testclient import TestClient

from src.auth.krb5 import get_krb5
from src.auth.token import query_token
from src.common.config import get_config
from src.main import app
from src.routers.status import InkStatus
from src.service.common import push_root_script, remote_is_exist, remote_ssh_connect

client = TestClient(app)
test_username = str(get_config("test", "username"))
test_password = str(get_config("test", "password"))
if get_config("common", "krb5_enabled") is True:
    test_kerberos_tokens = str(get_krb5(test_username))
else:
    test_kerberos_tokens = str(query_token(test_username))

client.headers.update(
    {
        "INK-Username": test_username,
        "INK-Token": test_kerberos_tokens,
    }
)


class TestSSHConnect:
    def test_ssh_connect(self):
        client = None
        try:
            SERVICE_NODE = get_config(
                "service", "service_node", fallback="inkbrowser.ihep.ac.cn"
            )
            client = remote_ssh_connect()
            _, stdout, _ = client.exec_command("echo $HOSTNAME")
            output = stdout.read().decode()
            assert output.strip() == SERVICE_NODE

        except Exception:
            assert False
        finally:
            if client:
                client.close()

    def test_remote_is_exist(self):
        client = None
        try:
            client = remote_ssh_connect()
            assert remote_is_exist(client, "/etc/passwd")
        except Exception:
            assert False
        finally:
            if client:
                client.close()

    def test_push_root_script(self):
        RBSCRIPT = get_config(
            "service", "rootbrowse_script", fallback="/dev/shm/start-rootbrowse.sh"
        )
        RBCSCRIPT = get_config(
            "service",
            "rootbrowse_check_script",
            fallback="/dev/shm/check-rootbrowse.sh",
        )

        client = None
        try:
            client = remote_ssh_connect()
            if remote_is_exist(client, RBSCRIPT):
                client.exec_command(f"rm {RBSCRIPT}")
            if remote_is_exist(client, RBCSCRIPT):
                client.exec_command(f"rm {RBCSCRIPT}")
            assert not remote_is_exist(client, RBCSCRIPT)
            assert not remote_is_exist(client, RBSCRIPT)
            push_root_script(client)
            assert remote_is_exist(client, RBCSCRIPT)
            assert remote_is_exist(client, RBSCRIPT)
        except Exception:
            assert False
        finally:
            if client:
                client.close()


class TestServiceAPI:
    def test_get_testuser(self):
        test_username = str(get_config("test", "username"))
        assert test_username

    def test_get_testuser_kerberos_tokens(self):
        test_username = str(get_config("test", "username"))
        try:
            if get_config("common", "krb5_enabled") is True:
                test_kerberos_tokens = str(get_krb5(test_username))
            else:
                test_kerberos_tokens = str(query_token(test_username))
            assert test_kerberos_tokens
            assert len(test_kerberos_tokens) > 20
        except Exception:
            assert False

    def test_access_rootfile(self):
        url = "/api/v2/service/access_rootfile"
        json_data = {
            "filename": "gallery.root",
            "workdir": "/cvmfs/sft.cern.ch/lcg/app/releases/ROOT/6.36.00/src/tutorials",
        }
        response = client.post(url, json=json_data)
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == InkStatus.SUCCESS
        assert "successfully" in data["msg"]
        assert "https:" in data["data"]["url"]

    def test_access_rootfile_not_exist(self):
        url = "/api/v2/service/access_rootfile"
        json_data = {
            "filename": "not_exist.root",
            "workdir": "/cvmfs/sft.cern.ch/lcg/app/releases/ROOT/6.36.00/src/tutorials",
        }
        response = client.post(url, json=json_data)
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == InkStatus.ACCESS_ROOTFILE_FAILURE
        assert "does not exist" in data["msg"]
        assert data["data"] == None

    def test_access_rootfile_not_rootfile(self):
        url = "/api/v2/service/access_rootfile"
        json_data = {
            "filename": "not_exist.png",
            "workdir": "/cvmfs/sft.cern.ch/lcg/app/releases/ROOT/6.36.00/src/tutorials",
        }
        response = client.post(url, json=json_data)
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == InkStatus.ACCESS_ROOTFILE_FAILURE
        assert "Invalid file type" in data["msg"]
        assert data["data"] == None

    # no routers, skip krb5 check for shared rootfile
    def test_shared_rootfile(self):
        url = "/api/v2/service/access_shared_rootfile"
        json_data = {
            "username": test_username,
            "filename": "gallery.root",
            "workdir": "/cvmfs/sft.cern.ch/lcg/app/releases/ROOT/6.36.00/src/tutorials",
        }
        response = client.post(url, json=json_data)
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == InkStatus.SUCCESS
        assert "successfully" in data["msg"]
        try:
            url = data["data"]["url"]
        except:
            assert False
        parsed = urlparse(url)
        assert parsed.scheme == "https", f"Invalid scheme: {url}"
        assert parsed.netloc == get_config(
            "service", "nginx_node", fallback="ink.ihep.ac.cn"
        ), f"Unexpected domain: {parsed.netloc}"
        assert "win" in data["data"]["url"]
