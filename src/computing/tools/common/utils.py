from fastapi import HTTPException
from src.computing.common import *
from src.computing.gateway_tools import *
from src.auth.krb5 import get_krb5
from src.computing.htc.htc_check_job import *
from datetime import datetime
from src.storage import common
from src.common.config import get_config
from src.common.logger import logger
import base64, asyncio, os, json, pwd

'''
Author:         guocq@ihep.ac.cn
Created:        2024-12-18
Last Modified:  2024-12-18
'''

def parse_info(info, key):
    try:
        json_data = json.loads(info)
        if key in json_data:
            return json_data[key]
    except json.JSONDecodeError:
        lines = info.split('\n')
        for line in lines:
            if line.strip().startswith(f'"{key}"'):
                return line.split(':')[1].strip().strip('" ,')
    except Exception as e:
        print(f"Error parsing {key}: {e}")
    return None


def replace_job_id(file_path, job_id):
    # 如果字符串中包含 %j，则替换为 job_id
    if '%j' in file_path:
        file_path = file_path.replace('%j', str(job_id))
    return file_path
    

def change_uid_to_username(uid: int):
    try:
        return pwd.getpwuid(uid).pw_name
    except KeyError:
        raise ValueError(f"No USERNAME found for UID: '{uid}'")
    

def change_username_to_uid(username: str):
    try:
        return pwd.getpwnam(username).pw_uid
    except KeyError:
        raise ValueError(f"No UID found for username '{username}'")

    
async def read_file(uid, file_path: str) -> str:

    file_content = ""
    username = change_uid_to_username(uid)
    krb5_enabled = get_config("common", "krb5_enabled")
    xrootd_path = get_config("computing", "xrootd_path")
    file_content = await common.cat_file(fname=file_path, username=username, mgm=xrootd_path, krb5_enabled=krb5_enabled)

    return file_content


def build_requirements(request_wn=None, request_arch=None) -> str:
    conds = []
    if request_wn:
        safe_wn = str(request_wn).replace('"', r'\"')
        conds.append(f'(TARGET.Machine == "{safe_wn}")')

    if request_arch:
        arch_in = "aarch64" if request_arch is True else str(request_arch).lower()
        if arch_in in ("aarch64", "arm64", "arm"):
            arch_val = "AARCH64"
        elif arch_in in ("x86_64", "amd64", "x64", "x86"):
            arch_val = "X86_64"
        else:
            arch_val = arch_in.upper()
        conds.append(f'(TARGET.Arch == "{arch_val}")')

    return "True" if not conds else " && ".join(conds)


async def connect_sshd(job_id, uid, clusterid):
    gateway_port, = get_job_iptables_status(uid, job_id, clusterid)
    GATEWAY_NODE = get_config("computing", "gateway_node")

    if gateway_port == 0:
        raise HTTPException(status_code=404, detail="The job has expired.")

    return GATEWAY_NODE, gateway_port

async def connect_jupyter_job(job_id, uid, clusterid):

    job_path, = get_job_path(uid, job_id, clusterid)
    info_file = f"{job_path}/app_login.info"

    logger.debug(f"job_path({job_path}), info_file({info_file})")
    
    try:
        login_info = await read_file(uid, info_file)
        NGINX_NODE = get_config("computing", "nginx_node")

        host = parse_info(login_info, "HOST")
        port = parse_info(login_info, "PORT")
        token = parse_info(login_info, "TOKEN")
        jupyter_url = f"{NGINX_NODE}/jupyter/{host}/{port}/lab?token={token}"

        if not host or not port or not token:
            raise HTTPException(status_code=500, detail="No host and port record in jupyter loginfile.")

        return host, port, token, jupyter_url

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


async def connect_vscode_job(job_id, uid, clusterid):

    job_path, = get_job_path(uid, job_id, clusterid)
    info_file = f"{job_path}/app_login.info"

    try:

        login_info = await read_file(uid, info_file)

        host = parse_info(login_info, "HOST")
        port = parse_info(login_info, "PORT")
        passwd = parse_info(login_info, "PASSWD")

        logger.info(f"Get User: {uid} connect info, Host: {host}, Port: {port}, Passwd: {passwd}, login_info: {login_info}")

        if not host or not port or not passwd:
            raise HTTPException(status_code=500, detail="No host and port record in vscode loginfile.")

        return host, port, passwd

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


async def connect_rootbrowse_job(job_id, uid, clusterid):

    # 切换到指定的作业目录
    job_path, = get_job_path(uid, job_id, clusterid)
    
    job_connect_info = f"{job_path}/app_login.info"

    try:
        
        login_info = await read_file(uid, job_connect_info)
        NGINX_NODE = get_config("computing", "nginx_node")
        
        # 提取 HOST, PORT, TOKEN
        host = parse_info(login_info, "HOST")
        port = parse_info(login_info, "PORT")
        token = parse_info(login_info, "TOKEN")
        rootbrowse_url = f"{NGINX_NODE}/rootbrowse/{host}/{port}/win1/?key={token}"

        if not host or not port or not token:
            raise HTTPException(status_code=500, detail="Invalid root login info format.")

        return host, port, token, rootbrowse_url
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


def generate_userotp(uid, hostname):
    
    username = change_uid_to_username(uid)
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    try:
        private_key = paramiko.RSAKey.from_private_key_file("/root/.ssh/id_rsa")
        client.connect(f"{hostname}", port=22, username="root", pkey=private_key)

        command = f"sudo -iu {username} /cvmfs/common.ihep.ac.cn/software/noVNC-master/utils/generateOTP.sh"
        stdin, stdout, stderr = client.exec_command(command)

        output = stdout.read().decode().strip()
        error = stderr.read().decode().strip()
        exit_status = stdout.channel.recv_exit_status()

        if exit_status != 0:
            raise RuntimeError(f"Generate user OTP failed, exit_status={exit_status}, stderr: {error}")
        
    except Exception as e:
        raise RuntimeError(f"Generate user OTP failed. {e}")
    
    finally:
        client.close()
        
    return output
    

async def connect_vnc_job(job_id, uid, clusterid):

    job_path, = get_job_path(uid, job_id, clusterid)
    info_file = f"{job_path}/app_login.info"
    NGINX_NODE = get_config("computing", "nginx_node")

    try:
        login_info = await read_file(uid, info_file)
        host = parse_info(login_info, "HOST")
        port = parse_info(login_info, "PORT")

        if not host or not port:
            raise HTTPException(status_code=500, detail="No host and port record in vnc loginfile.")

        userOTP = generate_userotp(uid, host)
                
        vnc_url = f"{NGINX_NODE}/vnc/{host}/{port}/vnc.html?password={userOTP}&autoconnect=true"
        
        return host, port, vnc_url

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))    
    

def create_iptables(uid, jobid, job_iptables_status, job_iptables_clean, clusterid):

    if job_iptables_clean == 0 and job_iptables_status == 0:        
        job_path, = get_job_path(uid, jobid, clusterid)
        info_file = f"{job_path}/ssh_login.info"  

        try:
            with open(info_file, 'r') as file:
                login_info = file.read()

            worker_host = parse_info(login_info, "HOST")
            worker_port = parse_info(login_info, "PORT")

        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))

        login_port = build_gateway_iptable(worker_host, worker_port, str(uid))

        update_iptable_status(uid, jobid, login_port, clusterid)  


def delete_iptables(uid, jobId, gateway_port, clusterid):
    
    delete_gateway_iptable(gateway_port)
    update_iptable_status(uid, jobId, 0, clusterid)
    update_iptable_clean(uid, jobId, 1, clusterid)


async def sub_command(command, timeoutsec, errinfo, tminfo):
    try:
        
        process = await asyncio.create_subprocess_shell(
            command,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            shell=True
        )
        
        await asyncio.wait_for(process.wait(), timeout=timeoutsec)
        stdout, stderr = await process.communicate()
        
        if process.returncode != 0:
            error_msg = stderr.decode().strip()
            raise Exception(f"{errinfo} {error_msg}")       
    except asyncio.TimeoutError as e:
        if process.returncode is None:
            process.kill()
            await process.wait()
        raise Exception(f"{tminfo} {e}")
    except Exception as e:
        raise e
    
    return stdout

