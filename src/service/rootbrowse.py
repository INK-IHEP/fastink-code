#! /usr/bin/python3
# FileName      : rootbrowse.py
# Author        : HAN Xiao
# Email         : hanx@ihep.ac.cn
# Date          : Fri Jun 13 14:41:19 2025 CST
# Last modified : Thu Oct 16 18:54:49 2025 CST
# Description   :

from shlex import quote

from src.common.config import get_config
from src.common.logger import logger
from src.service.common import (
    create_krb5_file,
    push_root_script,
    remote_is_exist,
    remote_ssh_connect,
    replace_https,
)


async def access_rootfile(username: str, workdir: str, filename: str, is_private=True):
    logger.info(f"[{username}] try to access the root file {workdir}/{filename}")
    if not filename.endswith(".root"):
        raise Exception(f"Invalid file type: {filename}")

    krb5_enabled = get_config("common", "krb5_enabled")
    krb5_path = None
    ssh = None

    # If krb5 is enabled and the file is private
    # will create a krb5cc file and upload it to the server
    if krb5_enabled and is_private:
        logger.debug(f"krb5: {krb5_enabled}")
        try:
            krb5_path = await create_krb5_file(username)
        except ValueError as e:
            raise Exception(f"Invalid kerberos data: {e}")
        except Exception as e:
            raise Exception(f"Create krb5cc file failed: {e}")

    try:
        RBSCRIPT = get_config(
            "service", "rootbrowse_script", fallback="/dev/shm/start-rootbrowse.sh"
        )
        RBCSCRIPT = get_config(
            "service",
            "rootbrowse_check_script",
            fallback="/dev/shm/check-rootbrowse.sh",
        )

        ssh = remote_ssh_connect()

        if not remote_is_exist(ssh, RBSCRIPT) or not remote_is_exist(ssh, RBCSCRIPT):
            push_root_script(ssh)
            logger.info(f"Shell script {RBSCRIPT}, {RBCSCRIPT} uploaded successfully.")

        if krb5_enabled and is_private:
            command = f'sudo -u {quote(username)} sh -c "export KRB5CCNAME={krb5_path} && /dev/shm/start-rootbrowse.sh {workdir}/{filename}"'
        else:
            command = f'sudo -u {quote(username)} sh -c "/dev/shm/start-rootbrowse.sh {workdir}/{filename}"'

        _, stdout, stderr = ssh.exec_command(command)

        output = stdout.read().decode()
        error = stderr.read().decode()
        logger.debug(f"Run script stdout: {output}")
        logger.debug(f"Run script stderr: {error}")

        # Sometimes error can be ok like aklog error
        # if error or not "http" in output:
        if not "http" in output:
            raise Exception(f"Command execution failed: {error} {output}")

    finally:
        if ssh:
            ssh.close()
        logger.debug("ssh client closed.")

    logger.info(
        f"[{username}] URL of {workdir}/{filename} created successfully. {replace_https(output)}"
    )
    return replace_https(output)
    # return {
    #     "status": "200",
    #     "msg": f"User: {username}, File: {filename}, Path: {workdir}",
    #     "data": {"url": f"{replace_https(output)}"},
    # }
