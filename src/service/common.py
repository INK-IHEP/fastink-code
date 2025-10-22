#! /usr/bin/python3
# FileName      : common.py
# Author        : HAN Xiao
# Email         : hanx@ihep.ac.cn
# Date          : Mon Jun 16 10:44:24 2025 CST
# Last modified : Mon Oct 13 14:29:44 2025 CST
# Description   :

import base64
import os
from datetime import datetime
from shlex import quote

import paramiko

from src.auth.krb5 import get_krb5
from src.common.config import get_config
from src.common.logger import logger
from src.common.utils import query_pwd_group
from src.storage import common


def replace_https(url):
    nginx = get_config("computing", "nginx_node", fallback="https://ink.ihep.ac.cn")
    return url.replace("https://ink.ihep.ac.cn", f"{nginx}")


def remote_ssh_connect():
    SERVICE_NODE = get_config(
        "service", "service_node", fallback="inkbrowser.ihep.ac.cn"
    )
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    private_key = paramiko.RSAKey.from_private_key_file("/root/.ssh/id_rsa")
    try:
        client.connect(f"{SERVICE_NODE}", port=22, username="root", pkey=private_key)
    except Exception as e:
        raise Exception(f"SSH connect failed: {e}")

    logger.debug("ssh client connected.")
    return client


def remote_is_exist(ssh: paramiko.SSHClient, remote_file) -> bool:
    _, stdout, _ = ssh.exec_command(
        f'test -f "{quote(remote_file)}" && echo true || echo false'
    )
    result = stdout.read().decode().strip()
    return result == "true"


def push_root_script(ssh: paramiko.SSHClient):
    """
    Using SFTP to put the script to service's server
    """

    RBSCRIPT = get_config(
        "service", "rootbrowse_script", fallback="/dev/shm/start-rootbrowse.sh"
    )
    SERVICE_NODE = get_config(
        "service", "service_node", fallback="inkbrowser.ihep.ac.cn"
    )
    RBCSCRIPT = get_config(
        "service",
        "rootbrowse_check_script",
        fallback="/dev/shm/check-rootbrowse.sh",
    )
    logger.info(f"Sftp rootbrowse script to {SERVICE_NODE}:{RBSCRIPT}")

    sftp = ssh.open_sftp()
    try:
        sftp.put("src/service/scripts/start-rootbrowse.sh", RBSCRIPT)
        sftp.put("src/service/scripts/check-rootbrowse.sh", RBCSCRIPT)
        ssh.exec_command(f"chmod +x {RBSCRIPT}")
        ssh.exec_command(f"chmod +x {RBCSCRIPT}")

    except Exception as e:
        raise Exception(f"sftp put file failed. {e}")
    finally:
        sftp.close()
    return True


async def create_krb5_file(username: str):
    """
    First, obtain the krb5 credential and write it to /dev/shm.
    Then, use the src.storage interface with the krb5 to write
    it into the user's directory, ensuring the necessary
    permissions for execution.
    """

    time_stamp = datetime.now().strftime("%Y%m%d-%H%M%S")

    ink_dir = get_config("service", "ink_dir")
    xrootd_path = get_config("computing", "xrootd_path")
    user_group = query_pwd_group(username)
    krb5_dir = f"{ink_dir}/{user_group}/{username}/.ink/envs/"
    token_filename = f"/dev/shm/krb5cc_{username}_{time_stamp}"

    token = get_krb5(username)
    if token != "":
        krb5_decoded_bytes = base64.b64decode(token)
    else:
        raise Exception("Init KRB5 token failed.")

    try:
        if not os.path.exists(token_filename):
            with open(token_filename, "wb") as file:
                file.write(krb5_decoded_bytes)
    except:
        raise Exception(f"krb5cc file {token_filename} create failed.")

    try:
        is_exist, _ = await common.path_exist(
            name=krb5_dir, username=username, mgm=xrootd_path
        )
        if not is_exist:
            logger.info(f"{krb5_dir} is_exist: {is_exist}")
            await common.mkdir(
                dname=f"{krb5_dir}",
                username=username,
                mode="700",
                exist_ok=False,
                mgm=xrootd_path,
            )

        await common.upload_file(
            src_data=krb5_decoded_bytes,
            dst=f"{ink_dir}/{user_group}/{username}/.ink/envs/krb5cc_{username}",
            username=username,
            mode="600",
            mgm=xrootd_path,
        )
    except Exception as e:
        raise Exception(f"Upload krb5cc file to {krb5_dir} failed: {e}")
    finally:
        os.remove(token_filename)

    return f"{krb5_dir}/krb5cc_{username}"
