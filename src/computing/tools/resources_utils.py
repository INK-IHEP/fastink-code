from fastapi import HTTPException
from src.computing.common import *
from src.computing.gateway_tools import *
from src.auth.krb5 import get_krb5
from src.computing.htc.htc_check_job import *
from datetime import datetime
import time
import base64
import asyncio
import os, os.path
import json
from src.storage import common
import pwd, grp
from pathlib import Path
from shlex import quote
from src.common.config import get_config
from src.common.logger import logger

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


def get_user_exp_group(uid):

    # 查询用户信息 → 主组 GID
    gid = pwd.getpwuid(uid).pw_gid

    # 查询组信息 → 组名
    group_name = grp.getgrgid(gid).gr_name

    mapping = {
        # 单键直接映射
        'alicpt': 'AliCPT',
        'cms': 'CMS',
        'dyw': 'DYW',
        'gecam': 'GECAM',
        'hxmt': 'HXMT',
        'lhcb': 'LHCB',
        'panda': 'Panda',
        'higgs': 'CEPC',
        'u07': 'CC',
        'comet': 'COMET',
        'csns': 'CSNS',
        'ucas': 'OTHERS',
        'heps': 'HEPS',
        # 多键映射同一值
        **{g: 'ATLAS' for g in ('atlas', 'combination')},
        **{g: 'BES' for g in ('dqarun', 'offlinerun', 'physics')},
        **{g: 'JUNO' for g in ('juno', 'dqmtest', 'dqmjuno', 'junospecial', 'junodc', 'junogns')},
        **{g: 'LHAASO' for g in ('lhaaso', 'lhaasorun')},
        **{g: 'HERD' for g in ('herd', 'herdrun')},
    }

    return mapping.get(group_name), group_name


def generate_condor_submit(uid, executable, job_path, cpu, mem, jobtype, request_os=None, arguments=None, wn=None):

    user_exp, user_group = get_user_exp_group(uid)

    hep_job = {
        "RealGroup": user_group,
        "Experiment": user_exp,
        "JobType": jobtype,
        "SiteName": "ihep",
        "Walltime": "default"
    }

    ihep_param = {
        "RealGroup": user_group
    }

    # 条件添加 RequestOS
    if request_os is not None:
        hep_job["RequestOS"] = request_os
    
    if jobtype == "npu":
        arguments = job_path

    # 配置模板
    config = {
        "universe": "vanilla",
        "executable": executable,
        "arguments": arguments,
        "output": f"{executable}.out",
        "error": f"{executable}.err",
        "request_cpus": cpu,
        "request_memory": mem,
        "wn": wn,
        "accounting_group": f"{user_exp}.{user_group}.default",
        "hep_job": hep_job,
        "ihep_param": ihep_param,
        "concurrency_limits": f"inkjob_{uid}_{jobtype}"
    }
    
    # 构建文件内容
    content = [
        "# 基础作业配置",
        f"universe = {config['universe']}",
        f"executable = {config['executable']}",
    ]
    
    # 动态添加 arguments 行
    if config['arguments'] is not None:
        content.append(f"arguments = {config['arguments']}")
    
    #if config['wn'] != "":
    #    content.append(f"requirements = TARGET.Machine == \"{config['wn']}\"")

    if config['wn'] != "":

        requirement_expr = f'TARGET.Machine == "{config["wn"]}"'
        if config['wn'] == "lhaasogpu001.ihep.ac.cn":
            requirement_expr += ' && (TARGET.Arch == "aarch64")'
        
        content.append(f"requirements = {requirement_expr}")
    
    # 继续添加其他配置
    content += [
        f"output = {config['output']}",
        f"error = {config['error']}",
        "# 资源请求",
        f"request_cpus = {config['request_cpus']}",
        f"request_memory = {config['request_memory']}",
        f"concurrency_limits = {config['concurrency_limits']}",
        "getenv = True\n",
        
        "# 账户配置",
        f"accounting_group = {config['accounting_group']}",
        
        "\n# HepJob 属性",
        *[f"+HepJob_{key} = \"{value}\"" for key, value in config['hep_job'].items()],
        *[f"+IHEP_{key} = \"{value}\"" for key, value in config['ihep_param'].items()],
        "\nqueue"
    ]

    # 写入文件
    submit_file = f"inkjob_{uid}_{jobtype}.sub"
    with open(f"/tmp/{submit_file}", "w") as f:
        f.write("\n".join(content))

    return submit_file
        
async def submit_hpc_job(sbatch_command, job_type, job_path, uid):
    
    user_name = change_uid_to_username(uid)
    krb5_enabled = get_config("common", "krb5_enabled")
    user_info = pwd.getpwuid(uid)
    user_shell = user_info.pw_shell
    
    if krb5_enabled: 
        
        if user_shell in ["/bin/bash", "/bin/sh", "/bin/zsh"]:
            command = (
                f"su - {quote(user_name)} -c "
                f'"'
                f"cd {quote(job_path)} && "
                f"export KRB5CCNAME={quote(f'{job_path}/krb5cc_{uid}')} && "
                f"/usr/bin/aklog && "
                f'sbatch {" ".join(sbatch_command)}'
                f'"'
            ) 
        else:
            command = (
                f"su - {quote(user_name)} -c "
                f'"'
                f"cd {quote(job_path)} && "
                f"setenv KRB5CCNAME {quote(f'{job_path}/krb5cc_{uid}')} && "
                f"/usr/bin/aklog && "
                f'sbatch {" ".join(sbatch_command)}'
                f'"'
            )
    else:
        command = (
            f"su - {quote(user_name)} -c "
            f'"'
            f"cd {quote(job_path)} && "
            f'sbatch {" ".join(sbatch_command)}'
            f'"'
        )
    
    logger.info(f"Submit job command: {command}")
    stdout = await sub_command(command, 5, "submit job failed.", "submit job timeout.")
    
    job_id_line = stdout.decode().strip()
    job_id = int(job_id_line.split()[-1])  # 作业ID通常在输出的最后一个字段
    
    logger.info(f"Submit job finished, the job_id: {job_id}, job_type: {job_type}, job_path: {job_path}")

    return job_id, str(job_type), job_path


async def test_submit_hpc_job(sbatch_command, uid, job_path):
    
    user_name = change_uid_to_username(uid)
    krb5_enabled = get_config("common", "krb5_enabled")
    if krb5_enabled: 
        command = (
        f"su - {quote(user_name)} -c "
        f'"'
        f"cd {quote(job_path)} && "  # 切换目录
        f"export KRB5CCNAME={quote(f'{job_path}/krb5cc_{user_name}')} && "  # 设置票据路径
        f"/usr/bin/aklog && "  # 新增 Kerberos 认证
        f'sbatch --test-only {" ".join(sbatch_command)}'
        f'"')
    else:
        command = (
        f"su - {quote(user_name)} -c "
        f'"'
        f"cd {quote(job_path)} && "  # 切换目录
        f'sbatch --test-only {" ".join(sbatch_command)}'
        f'"')
    
    try:
        process = await asyncio.create_subprocess_shell(
            command,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            shell=True
        )
        
        await asyncio.wait_for(process.wait(), timeout=5)
        stdout, stderr = await process.communicate()
        
        if process.returncode != 0:
            error_msg = stderr.decode().strip()
            raise Exception(f"Submit test job failed. {error_msg}")    

    except asyncio.TimeoutError:
        if process.returncode is None:
            process.kill()
            await process.wait()
        raise Exception(f"Submit test job timeout.")
    
    except Exception as e:
        raise e
    
    return stderr
    
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


async def submit_htc_job(submit_file, job_type, job_path, uid):
    
    user_name = change_uid_to_username(uid)
    schedd_host = get_config("computing", "schedd_host")
    cm_host = get_config("computing", "cm_host")
    user_info = pwd.getpwuid(uid)
    user_shell = user_info.pw_shell
    
    if job_type == "jupyter":
        command = (
            f"su -s /bin/bash {quote(user_name)} -c "
            f'"'
            f"cd {quote(job_path)} && " 
            f"export KRB5CCNAME={quote(f'{job_path}/krb5cc_{uid}')} && " 
            f"/usr/bin/aklog && "
            f"condor_submit -name {quote(schedd_host)} -pool {quote(cm_host)} {quote(submit_file)}"
            f'"'
        )
    else:
        if user_shell in ["/bin/bash", "/bin/sh", "/bin/zsh"]:
            command = (
                f"su - {quote(user_name)} -c "
                f'"'
                f"cd {quote(job_path)} && "
                f"export KRB5CCNAME={quote(f'{job_path}/krb5cc_{uid}')} && "
                f"/usr/bin/aklog && "
                f"export INKPATH=$PATH && "
                f"export INKLDPATH=$LD_LIBRARY_PATH && "
                f"export PATH=/usr/bin:$PATH && "
                f"export LD_LIBRARY_PATH=/lib64:$LD_LIBRARY_PATH && "
                f"condor_submit -name {quote(schedd_host)} -pool {quote(cm_host)} {quote(submit_file)}"
                f'"'
            )  
        else:
            command = (
                f"su - {quote(user_name)} -c "
                f'"'
                f"cd {quote(job_path)} && "
                f"setenv KRB5CCNAME {quote(f'{job_path}/krb5cc_{uid}')} && "
                f"/usr/bin/aklog && "
                f"setenv INKPATH $PATH && "
                f"setenv INKLDPATH $LD_LIBRARY_PATH && "
                f"setenv PATH /usr/bin:$PATH && "
                f"setenv LD_LIBRARY_PATH /lib64:$LD_LIBRARY_PATH && "
                f"condor_submit -name {quote(schedd_host)} -pool {quote(cm_host)} {quote(submit_file)}"
                f'"'
            )    

    stdout = await sub_command(command, 10, "submit job failed.", "submit job timeout.")
    logger.info(f"Submit command: {command}")
    logger.info(f"Submit {user_name} job to queue, {stdout.decode()}")
    
    job_id_line = stdout.decode().strip()
    job_id = int(job_id_line.split()[-1].rstrip('.'))
    
    logger.info(f"Submit job finished, and the jobid is {job_id}")

    return job_id, str(job_type), job_path


async def generate_home_link(user_home_dir, ink_dir, user_group, username, token_filename):
    
    logger.info("Enter generate home link func.")
    
    if user_home_dir != f"{ink_dir}/{user_group}/{username}":
        
        ln_command = ""
        homelink = Path(f"{user_home_dir}/.ink")
        mvname = Path(f"{user_home_dir}/.inkold")
        xrootd_path = get_config("computing", "xrootd_path")
        is_exist, _ = await common.path_exist(name=str(homelink), krb5ccname=token_filename, username=username, mgm=xrootd_path)
        
        if is_exist:
            
            readlink_command = (
                f"su -s /bin/bash {quote(username)} -c "
                f'"'
                f"readlink -f {quote(str(homelink))}"
                f'"'
            )
            
            process = await asyncio.create_subprocess_shell(
                readlink_command,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )

            try:
                stdout, stderr = await asyncio.wait_for(process.communicate(), timeout=5)
                
            except asyncio.TimeoutError:
                process.kill()
                await process.wait()
                raise Exception("Get user homelink timeout")
            
            except Exception as e:
                raise Exception(f"Get user homelink failed. {e}, {stderr}")
        
            link_path = stdout.decode().strip()
            
            realdir_command = (
                f"su -s /bin/bash {quote(username)} -c "
                f'"'
                f"readlink -f {ink_dir}/{user_group}/{username}/.ink"
                f'"'
            ) 
            
            process = await asyncio.create_subprocess_shell(
                realdir_command,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )

            try:
                stdout, stderr = await asyncio.wait_for(process.communicate(), timeout=5)
                
            except asyncio.TimeoutError:
                process.kill()
                await process.wait()
                raise Exception("Get Job dir realpath timeout")
            
            except Exception as e:
                raise Exception(f"Get Job dir realpath failed. {e}, {stderr}")
        
            realinkdir = stdout.decode().strip()
            
            if str(link_path) != str(realinkdir):
                ln_command = (
                    f"su -s /bin/bash {quote(username)} -c " 
                    f'"' 
                    f"export KRB5CCNAME={quote(token_filename)} && "  
                    f"aklog && "  
                    f"rm -rf {quote(str(mvname))} && "  
                    f"mv {quote(str(homelink))} {quote(str(mvname))} && "  
                    f"ln -s {quote(ink_dir)}/{quote(user_group)}/{quote(username)}/.ink {quote(user_home_dir)}/.ink"
                    f'"' 
                )
        else:
            ln_command = (
                f"su -s /bin/bash {quote(username)} -c "
                f'"'
                f"export KRB5CCNAME={quote(token_filename)} && "
                f"aklog && "
                f"ln -s {quote(ink_dir)}/{quote(user_group)}/{quote(username)}/.ink {quote(user_home_dir)}/.ink"
                f'"'
            )
            
        logger.info(f"The ln_command: {ln_command}")
        if ln_command:
            _ = await sub_command(ln_command, 5, "Make homelink failed.", "Make homelink timeout.")


async def build_job_env(uid, jobtype, rawjobPath, jobfilename):

    time_stamp = datetime.now().strftime('%Y%m%d-%H%M%S')
    username = change_uid_to_username(uid)
    _, user_group = get_user_exp_group(uid)
    
    user_home_dir = os.path.expanduser(f'~{username}')
    ink_dir = get_config("computing", "ink_dir")
    job_dir = ""
    if ink_dir == "~":
        job_dir = f"{user_home_dir}/.ink/Jobs/{jobtype}-{time_stamp}"
    else:
        job_dir = f"{ink_dir}/{username[0]}/{username}/.ink/Jobs/{jobtype}-{time_stamp}"
    #job_dir = f"{ink_dir}/{user_group}/{username}/.ink/Jobs/{jobtype}-{time_stamp}"
    krb5_enabled = get_config("common", "krb5_enabled")
    token_filename = ""
    xrootd_path = get_config("computing", "xrootd_path")
    logger.debug(f"Build job env variables: user_home_dir({user_home_dir}), ink_dir({ink_dir}), job_dir({job_dir}), krb5_enabled({krb5_enabled}), token_filename({token_filename})")
    
    if krb5_enabled:
        
        krb_start_ts = time.monotonic()
        token = get_krb5(username)
        if token != "":  
            krb5_decoded_bytes = base64.b64decode(token)
        else:
            raise Exception("Init KRB5 token failed.")
        
        token_filename = f"/tmp/krb5cc_{uid}_{time_stamp}"
        if not os.path.exists(token_filename):
            with open(token_filename, 'wb') as file:
                file.write(krb5_decoded_bytes)

        os.environ['KRB5CCNAME'] = token_filename
        _ = await sub_command(("aklog"), 5, "init aklog failed.", "init aklog timout.")
        
        krb_end_ts = time.monotonic()
        krb_elapsed_time = krb_end_ts - krb_start_ts
        logger.debug(f"KRB5 elapsed time: {krb_elapsed_time:.3f}s")
        
        exist_start_ts = time.monotonic()
        is_exist, _ = await common.path_exist(name=job_dir, krb5ccname=token_filename, username=username, mgm=xrootd_path)
        if not is_exist:
            await common.mkdir(dname=job_dir, krb5ccname=token_filename, username=username, mode="700", exist_ok=False, mgm=xrootd_path)
        exist_end_ts = time.monotonic()
        exist_elapsed_time = exist_end_ts - exist_start_ts
        logger.debug(f"Path exist and mkdir elapsed time: {exist_elapsed_time:.3f}s")
        
        link_start_ts = time.monotonic()
        await generate_home_link(user_home_dir, ink_dir, user_group, username, token_filename)
        logger.info("Finish generate home link func.")
        link_end_ts = time.monotonic()
        logger.debug(f"Link elapsed time: {link_end_ts - link_start_ts:.3f}s")
        
        up_start_ts = time.monotonic()
        job_dir = f"{user_home_dir}/.ink/Jobs/{jobtype}-{time_stamp}"    
        await common.upload_file(src_data=krb5_decoded_bytes, dst=f"{job_dir}/krb5cc_{uid}", krb5ccname=token_filename, username=username, mgm=xrootd_path)
        up_end_ts = time.monotonic()
        logger.debug(f"Upload krb5token elapsed time: {up_end_ts - up_start_ts:.3f}s")
        
        upjob_start_ts = time.monotonic()
        with open(rawjobPath, "rb") as file:
            jobfile_content = file.read()
        await common.upload_file(src_data=jobfile_content, dst=f"{job_dir}/{jobfilename}", krb5ccname=token_filename, username=username, mgm=xrootd_path)
        upjob_end_ts = time.monotonic()
        logger.debug(f"Upload jobfile elapsed time: {upjob_end_ts - upjob_start_ts:.3f}s")

    else: 
        is_exist, _ = await common.path_exist(name=job_dir, krb5ccname=token_filename, username=username, mgm=xrootd_path)
        if not is_exist:
            await common.mkdir(dname=job_dir, krb5ccname=token_filename, username=username, mode="700", exist_ok=False, mgm=xrootd_path)
        
        job_content = ""
        with open(f"{rawjobPath}", 'rb') as file:
            job_content = file.read()
        
        await common.upload_file(src_data=job_content, dst=f"{job_dir}/{jobfilename}", krb5ccname=token_filename, username=username, mgm=xrootd_path)
        
    await common.chmod(fname=f"{job_dir}/{jobfilename}", krb5ccname=token_filename, mode="700", username=username, mgm=xrootd_path)
    
    return job_dir, token_filename 

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

        #command = f"sudo -iu {username} /cvmfs/common.ihep.ac.cn/software/noVNC-master/utils/generateOTP.sh"
        command = f"sudo -u {username} -H bash --noprofile --norc -c '/cvmfs/common.ihep.ac.cn/software/noVNC-master/utils/generateOTP.sh'"
        stdin, stdout, stderr = client.exec_command(command)

        output = stdout.read().decode().strip()
        error = stderr.read().decode().strip()
        exit_status = stdout.channel.recv_exit_status()

        if exit_status != 0:
            raise RuntimeError(f"Generate user OTP failed, exit_status={exit_status}, stderr: {error}")

        if output:
            output = output.splitlines()[-1]
        
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

