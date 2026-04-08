import asyncio, json
import importlib, grp
import pwd, os, base64
from pathlib import Path
from shlex import quote, split
from typing import Optional
from datetime import datetime
from zoneinfo import ZoneInfo
from pathlib import Path, PurePath
from fastink.storage import common
from fastink.auth.krb5 import get_krb5
from fastapi import HTTPException
from fastink.common.logger import logger
from fastink.common.config import get_config
from fastink.computing.tools.db.db_tools import *
from fastink.computing.tools.gateway.gateway_utils import *
from fastink.computing.htc.htc_check_job import *


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


def safe_get(arr, idx, default=""):
    return arr[idx] if idx < len(arr) else default


def safe_int(s: str, default: Optional[int] = None) -> Optional[int]:
    try:
        s = (s or "").strip()
        if s == "" or s.lower() in {"undefined", "null", "none"}:
            return default
        return int(s)
    except Exception:
        return default
    

def ts_to_str(ts: Optional[int]) -> str:
    if not ts or ts <= 0:
        return ""
    return datetime.fromtimestamp(ts, ZoneInfo("Asia/Shanghai")).strftime("%Y-%m-%d %H:%M:%S")

def jobid_sort_key(x: dict) -> int:
    jid = str(x.get("jobId") or "").strip()
    if not jid:
        return 10**18
    try:
        return int(jid)
    except ValueError:
        try:
            return int(jid.split(".")[0])
        except Exception:
            return 0

def clean_query_value(v: object) -> str:
    _BAD_LOWER = {"undefined", "null", "none", "unknown", "n/a", "na", "nil"}
    if v is None:
        return ""
    if isinstance(v, (bytes, bytearray)):
        v = v.decode("utf-8", errors="ignore")
    
    s = str(v).strip()
    
    if len(s) >= 2 and ((s[0] == s[-1] == '"') or (s[0] == s[-1] == "'")):
        s = s[1:-1].strip()
    
    if s.lower() in _BAD_LOWER:
        return ""
    
    return " ".join(s.split())


def jobid_sort_key(x: dict) -> int:
    jid = (x.get("jobId") or "").strip()
    if not jid:
        return 10**18
    try:
        return int(jid)
    except ValueError:
        try:
            return int(jid.split(".")[0])
        except Exception:
            return 0


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
        NGINX_NODE = get_config("computing", "nginx_node")

        host = parse_info(login_info, "HOST")
        port = parse_info(login_info, "PORT")
        passwd = parse_info(login_info, "PASSWD")
        vscode_url = f"{NGINX_NODE}/vscode/{host}/{port}/login"

        logger.info(f"Get User: {uid} connect info, Host: {host}, Port: {port}, Passwd: {passwd}, login_info: {login_info}")

        if not host or not port or not passwd:
            raise HTTPException(status_code=500, detail="No host and port record in vscode loginfile.")

        return host, port, passwd, vscode_url

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


async def connect_openclaw_job(job_id, uid, clusterid):

    job_path, = get_job_path(uid, job_id, clusterid)
    info_file = f"{job_path}/app_login.info"

    try:
        login_info = await read_file(uid, info_file)
        NGINX_NODE = get_config("computing", "nginx_node")

        host = parse_info(login_info, "HOST")
        port = parse_info(login_info, "PORT")
        base_path = parse_info(login_info, "BASE_PATH")
        token = parse_info(login_info, "TOKEN")
        if not base_path:
            username = change_uid_to_username(uid)
            base_path = f"/openclaw/{host}/{port}/{username}/"
        openclaw_url = f"{NGINX_NODE}{base_path}"
        if token:
            separator = "&" if "?" in openclaw_url else "?"
            openclaw_url = f"{openclaw_url}{separator}token={token}"

        if not host or not port:
            raise HTTPException(status_code=500, detail="No host and port record in openclaw loginfile.")

        return host, port, token, openclaw_url

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

        exit_status = stdout.channel.recv_exit_status()

        if exit_status != 0:
            error = stderr.read().decode().strip()
            raise RuntimeError(f"Generate user OTP failed, exit_status={exit_status}, stderr: {error}")
        
        raw = stdout.read().decode(errors="ignore")
        output = next((ln.strip() for ln in reversed(raw.splitlines()) if ln.strip()), "")

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
    

async def create_iptables(uid, jobid, job_iptables_status, job_iptables_clean, clusterid):

    if job_iptables_clean == 0 and job_iptables_status == 0:        
        job_path, = get_job_path(uid, jobid, clusterid)
        info_file = f"{job_path}/ssh_login.info"  

        try:
            login_info = await read_file(uid, info_file)
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


def get_user_exp_group_dir(uid: int) -> str:
    experiment_group, raw_group = get_user_exp_group(uid)
    group_dir = (experiment_group or raw_group or "").lower()
    if not group_dir:
        raise ValueError(f"Failed to resolve experiment group for uid={uid}")
    return group_dir


async def init_job_dir(username: str, job_type: str):
    
    XROOTD_PATH = get_config("computing", "xrootd_path")
    time_stamp = datetime.now().strftime('%Y%m%d-%H%M%S')
    user_home_dir = os.path.expanduser(f'~{username}')
    uid = change_username_to_uid(username)

    if user_home_dir.startswith("/afs/"):
        _, USERGROUP = get_user_exp_group(uid)
        ink_dir = get_config("computing", "ink_dir")
        ink_dir = ink_dir.format(user_group=USERGROUP, username=username)
        job_dir = f"{ink_dir}/.ink/Jobs/{job_type}-{time_stamp}"
    else:
        job_dir = f"{user_home_dir}/.ink/Jobs/{job_type}-{time_stamp}"
    
    logger.debug(f"User job_dir: {job_dir}")
    is_exist, _ = await common.path_exist(name=job_dir, username=username, mgm=XROOTD_PATH)
    if not is_exist:
        await common.mkdir(dname=job_dir, username=username, mode="700", exist_ok=False, mgm=XROOTD_PATH)
    logger.debug(f"Completed the jobdir({job_dir}) init for User({username})")

    KRB5_ENABLED = get_config("common", "krb5_enabled")
    if KRB5_ENABLED:
        token = get_krb5(username)
        if token != "":  
            krb5_decoded_bytes = base64.b64decode(token)
            await common.upload_file(src_data=krb5_decoded_bytes, dst=f"{job_dir}/krb5cc_{uid}", username=username, mgm=XROOTD_PATH, mode="600")
            logger.debug(f"Generate user:{username} KRB5 token successfully.")
        else:
            raise Exception("Generate user KRB5 token failed.")

    return job_dir


async def generate_condor_submit(
    username: str,
    cpu: int, 
    mem: int, 
    jobtype: str, 
    jobdir: str, 
    request_os: Optional[str] = None, 
    request_wn: Optional[str] = None, 
    request_arch: Optional[str] = None, 
    arguments: Optional[str] = None
):

    default_job_config = get_config("jobtype", jobtype).get("htc")
    extra_param = default_job_config.get("extra_param")
    job_cpus = default_job_config.get("RequestCpus", cpu)
    job_mem = default_job_config.get("RequestMemory", mem)
    XROOTD_PATH = get_config("computing", "xrootd_path")
    uid = change_username_to_uid(username)
    
    workernode = default_job_config.get("workernode", request_wn)
    arch = default_job_config.get("arch", request_arch)

    executable_dir = get_config("computing", "cluster_scripts")
    executable = f"{executable_dir}/{jobtype}/shell.sh"

    with open(executable, "rb") as file:
        submitfile_content = file.read()
    await common.upload_file(src_data=submitfile_content, dst=f"{jobdir}/shell.sh", username=username, mgm=XROOTD_PATH, mode="700")

    job_script = f"{executable_dir}/{jobtype}/run.sh"
    with open(job_script, "rb") as file:
        job_script_content = file.read()
    await common.upload_file(src_data=job_script_content, dst=f"{jobdir}/run.sh", username=username, mgm=XROOTD_PATH, mode="700")

    if jobtype == "npu":
        arguments = (arguments or "") + jobdir
    elif jobtype == "openclaw":
        arguments = build_openclaw_arguments(
            username=username,
            uid=uid,
            arguments=arguments,
        )
    
    config = {
        "universe": "vanilla",
        "executable": "shell.sh",
        "arguments": arguments,
        "output": f"{jobdir}/$(ClusterId).out",
        "error": f"{jobdir}/$(ClusterId).err",
        "request_cpus": job_cpus,
        "request_memory": job_mem,
        "getenv": "True",
    }    

    for key, value in default_job_config.items():
        if key in {"schedd_host", "cm_host", "RequestCpus", "RequestMemory", "walltime", "workernode", "extra_param"}:
            continue
        config[f"{key}"] = value

    if extra_param:
        job_plugin = importlib.import_module(f"fastink.computing.scripts.plugins.set_extra_config")
        groupname = grp.getgrgid(pwd.getpwuid(uid).pw_gid).gr_name
        extra_job_config = job_plugin.get_extra_job_config(username, groupname, jobtype, request_os)
        for key, value in extra_job_config.items():
            logger.info(f"key: {key}, value: {value}")
            config[f"{key}"] = value

    requirement_expr = build_requirements(workernode, arch)
    config[f"requirements"] = requirement_expr

    lines = [f"{k} = {v}" for k, v in config.items()]
    lines.append("queue")
    submitfile_bytes = ("\n".join(lines) + "\n").encode("utf-8")
    submitfile_name = f"{username}_{jobtype}.sub"

    await common.upload_file(src_data=submitfile_bytes, dst=f"{jobdir}/{submitfile_name}", username=username, mgm=XROOTD_PATH, mode="600")
    logger.debug(f"Completed the submit file initial and upload.")

    return submitfile_name

def generate_submit_command(username: str, job_dir: str, job_type: str, submitfile: str) -> str:
        
    krb5_enabled = get_config("common", "krb5_enabled")
    if isinstance(krb5_enabled, str):
        krb5_enabled = krb5_enabled.strip().lower() in {"1", "true", "yes", "on"}

    uid = change_username_to_uid(username)
    user_shell = pwd.getpwuid(uid).pw_shell
    bash_like = user_shell in {"/bin/bash", "/bin/sh", "/bin/zsh"}
    noenv_jobtype_list = get_config("computing", "noenv_jobtype")
    special_job = job_type in noenv_jobtype_list

    if special_job:
        if bash_like:
            if user_shell == "/bin/zsh":
                su_prefix = f"su -s /bin/zsh {quote(username)} -c "
            else:
                su_prefix = f"su -s /bin/bash {quote(username)} -c "
        else:
            su_prefix = f"su -s /bin/tcsh {quote(username)} -c "
    else:
        su_prefix = f"su - {quote(username)} -c "

    def env_kv(k: str, v: str) -> str:
        if bash_like:
            return f"export {k}={v}"
        else:
            return f"setenv {k} {v}"

    env_parts = [
        f"cd {quote(job_dir)}",
        env_kv("PATH", "/usr/bin:$PATH"),
        env_kv("LD_LIBRARY_PATH", "/lib64:$LD_LIBRARY_PATH"),
    ]

    if not special_job:
        env_parts.append(env_kv("INKPATH", "$PATH"))
        env_parts.append(env_kv("INKLDPATH", "$LD_LIBRARY_PATH"))

    if krb5_enabled:
        env_parts.insert(1, env_kv("KRB5CCNAME", quote(f"{job_dir}/krb5cc_{uid}")))

    SCHEDD_HOST = get_config("computing", "schedd_host")
    CM_HOST = get_config("computing", "cm_host")

    submit_part = (
        "condor_submit "
        f"-name {quote(SCHEDD_HOST)} "
        f"-pool {quote(CM_HOST)} "
        f"{quote(submitfile)}"
    )

    command = su_prefix + '"' + " && ".join(env_parts + [submit_part]) + '"'
    logger.debug(f"User {username} submit command: {command}")

    return command


def get_parent_dir(path_str: str) -> str:
    parent = str(Path(path_str).parent)
    return "" if parent == "." else parent


def build_openclaw_arguments(
    username: str,
    uid: int,
    arguments: Optional[str] = None,
) -> str:
    group_dir = get_user_exp_group_dir(uid)
    user_root_template = get_config(
        "service",
        "openclaw_user_root",
        fallback="/scratchfs/{experiment_group_lower}/{username}",
    )
    openclaw_relpath = get_config("service", "openclaw_models_relpath", fallback=".openclaw")
    image_template = get_config(
        "service",
        "openclaw_container_image",
        fallback="/home/{group_dir}/{username}/container/openclaw_ihep_latest.sif",
    )
    openclaw_user_root = user_root_template.format(
        username=username,
        experiment_group_lower=group_dir,
        group_dir=group_dir,
    )
    openclaw_dir = f"{openclaw_user_root}/{openclaw_relpath}"
    openclaw_image = image_template.format(
        group_dir=group_dir,
        username=username,
    )
    openclaw_args = [
        quote(openclaw_user_root),
        quote(openclaw_dir),
        quote(openclaw_image),
        quote(username),
    ]
    if arguments:
        openclaw_args.append(arguments)
    return " ".join(openclaw_args)


async def init_sync_job_dir(username: str, job_type: str, job_dir: Optional[str] = None, script_path: Optional[str] = None) -> str:
    
    XROOTD_PATH = get_config("computing", "xrootd_path")
    uid = change_username_to_uid(username)

    def build_default_job_dir() -> str:
        user_home_dir = os.path.expanduser(f"~{username}")
        time_stamp = datetime.now().strftime("%Y%m%d-%H%M%S")
        if user_home_dir.startswith("/afs/"):
            _, user_group = get_user_exp_group(uid)
            ink_dir = get_config("computing", "ink_dir")
            ink_dir = ink_dir.format(user_group=user_group, username=username)
            return f"{ink_dir}/.ink/Jobs/{job_type}-{time_stamp}"
        return f"{user_home_dir}/.ink/Jobs/{job_type}-{time_stamp}"

    if script_path:
        script_dir = get_parent_dir(script_path)
        final_job_dir = script_dir if script_dir else build_default_job_dir()
    elif job_dir:
        final_job_dir = job_dir
    else: 
        final_job_dir = build_default_job_dir()

    logger.debug(f"User job_dir: {final_job_dir}")

    is_exist, _ = await common.path_exist(name=final_job_dir, username=username, mgm=XROOTD_PATH)
    if not is_exist:
        await common.mkdir(dname=final_job_dir, username=username, mode="700", exist_ok=False, mgm=XROOTD_PATH)
    logger.debug(f"Completed the jobdir({final_job_dir}) init for User({username})")

    if get_config("common", "krb5_enabled"):
        token = get_krb5(username)
        if not token:
            raise Exception("Generate user KRB5 token failed.")

        krb5_decoded_bytes = base64.b64decode(token)
        await common.upload_file(src_data=krb5_decoded_bytes, dst=f"{final_job_dir}/krb5cc_{uid}", username=username, mgm=XROOTD_PATH, mode="600")
        logger.debug(f"Generate user:{username} KRB5 token successfully.")

    return final_job_dir


async def generate_condor_sync_submit(
    username: str,
    cpu: int, 
    mem: int, 
    job_dir: str,
    job_type: str,
    job_script: Optional[str] = None,
    script_path: Optional[str] = None,
    request_os: Optional[str] = None, 
    request_wn: Optional[str] = None, 
    request_arch: Optional[str] = None, 
    arguments: Optional[str] = None
):

    XROOTD_PATH = get_config("computing", "xrootd_path")
    uid = change_username_to_uid(username)
    groupname = grp.getgrgid(pwd.getpwuid(uid).pw_gid).gr_name

    if script_path:
        extra_param = True
        workernode = request_wn
        arch = request_arch
        config = {
            "universe": "vanilla",
            "executable": script_path,
            "arguments": arguments,
            "output": f"{job_dir}/$(ClusterId).out",
            "error": f"{job_dir}/$(ClusterId).err",
            "request_cpus": cpu,
            "request_memory": mem,
            "getenv": "True",
        }
    elif job_script:
        extra_param = True
        workernode = request_wn
        arch = request_arch
        submitfile_content = job_script.encode("utf-8")
        config = {
            "universe": "vanilla",
            "executable": f"{job_dir}/job.sh",
            "arguments": arguments,
            "output": f"{job_dir}/$(ClusterId).out",
            "error": f"{job_dir}/$(ClusterId).err",
            "request_cpus": cpu,
            "request_memory": mem,
            "getenv": "True",
        }
        await common.upload_file(src_data=submitfile_content, dst=f"{job_dir}/job.sh", username=username, mgm=XROOTD_PATH, mode="700")
    else:
        default_job_config = get_config("jobtype", job_type, fallback={}).get("htc")
        if default_job_config:
            extra_param = default_job_config.get("extra_param")
            job_cpus = default_job_config.get("RequestCpus", cpu)
            job_mem = default_job_config.get("RequestMemory", mem)
            workernode = default_job_config.get("workernode", request_wn)
            arch = default_job_config.get("arch", request_arch)    

        executable_dir = get_config("computing", "cluster_scripts")
        executable = f"{executable_dir}/{job_type}/shell.sh"

        with open(executable, "rb") as file:
            submitfile_content = file.read()
        await common.upload_file(src_data=submitfile_content, dst=f"{job_dir}/shell.sh", username=username, mgm=XROOTD_PATH, mode="700")

        job_script = f"{executable_dir}/{job_type}/run.sh"
        with open(job_script, "rb") as file:
            job_script_content = file.read()
        await common.upload_file(src_data=job_script_content, dst=f"{job_dir}/run.sh", username=username, mgm=XROOTD_PATH, mode="700")

        if job_type == "npu":
            arguments = (arguments or "") + job_dir
        elif job_type == "openclaw":
            arguments = build_openclaw_arguments(
                username=username,
                uid=uid,
                arguments=arguments,
            )
        
        config = {
            "universe": "vanilla",
            "executable": "shell.sh",
            "arguments": arguments,
            "output": f"{job_dir}/$(ClusterId).out",
            "error": f"{job_dir}/$(ClusterId).err",
            "request_cpus": job_cpus,
            "request_memory": job_mem,
            "getenv": "True",
        }    

        for key, value in default_job_config.items():
            if key in {"schedd_host", "cm_host", "RequestCpus", "RequestMemory", "walltime", "workernode", "extra_param"}:
                continue
            config[f"{key}"] = value

    if extra_param:
        job_plugin = importlib.import_module(f"fastink.computing.scripts.plugins.set_extra_config")
        extra_job_config = job_plugin.get_extra_job_config(username, groupname, job_type, request_os)
        for key, value in extra_job_config.items():
            logger.info(f"key: {key}, value: {value}")
            config[f"{key}"] = value

    requirement_expr = build_requirements(workernode, arch)
    config[f"requirements"] = requirement_expr

    lines = [f"{k} = {v}" for k, v in config.items()]
    lines.append("queue")
    submitfile_bytes = ("\n".join(lines) + "\n").encode("utf-8")
    submitfile_name = f"{username}_{job_type}.sub"

    await common.upload_file(src_data=submitfile_bytes, dst=f"{job_dir}/{submitfile_name}", username=username, mgm=XROOTD_PATH, mode="600")
    logger.debug(f"Completed the submit file initial and upload.")

    return submitfile_name


async def check_user_kerberos_ticket(username: str, uid: int, job_dir: str, timeout: int = 30):

    ccache_path = f"{job_dir}/krb5cc_{uid}"
    check_command = f"su - {username} -c 'export KRB5CCNAME={ccache_path} && klist'"
    try:
        stdout = await sub_command(check_command, timeout, "KRB5 Ticket Check Failed", "Klist check timeout")        
        ticket_info = stdout.decode(errors="ignore").strip()
        logger.info(f"--- Kerberos Ticket Info for {username} ---\n{ticket_info}\n--------------------------------------")
    except Exception as e:
        logger.error(f"HTC-ASYNC-LOG: Kerberos ticket is INVALID for {username}. Error: {e}")


async def sub_command(command, timeoutsec, errinfo, tminfo):
    process = await asyncio.create_subprocess_shell(
        command,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        # create_subprocess_shell 本身就用 shell 了，这里不要再传 shell=True
    )

    try:
        stdout, stderr = await asyncio.wait_for(process.communicate(), timeout=timeoutsec)

    except asyncio.TimeoutError as e:
        process.kill()
        stdout, stderr = await process.communicate()
        raise Exception(f"{tminfo} {e}. stderr={stderr.decode(errors='ignore')[:500]}")

    if process.returncode != 0:
        error_msg = stderr.decode(errors="ignore").strip()
        raise Exception(f"{errinfo} {error_msg}")

    return stdout

class PathChecker:
    
    @staticmethod
    def is_absolute_path(path: str) -> bool:
        return Path(path).is_absolute()
    
    @staticmethod
    def is_relative_path(path: str) -> bool:
        return not Path(path).is_absolute()
    
    @staticmethod
    def is_file(path: str) -> Optional[bool]:
        p = Path(path)
        return p.is_file() if p.exists() else None
    
    @staticmethod
    def is_directory(path: str) -> Optional[bool]:
        p = Path(path)
        return p.is_dir() if p.exists() else None
    
    @staticmethod
    def is_filename_only(path: str) -> Optional[bool]:
        p = PurePath(path)
    
        if str(p.parent) != '.':
            return False
        
        if any(sep in path for sep in ('/', '\\')):
            return False
        
        if len(path.parts) > 1:
            return False
        
        if path.anchor:
            return False
        
        return True
    
    @staticmethod
    def is_existed(path: str) -> Optional[bool]:
        p = Path(path)
        return p.exists()
    

def parse_sbatch_out_err(cmd: str, job_id: str | int) -> tuple[str | None, str | None]:
    ...
    """
    Parse --output and --error paths from an sbatch command
    and replace %j with the given job ID.

    :param cmd: sbatch command string
    :param job_id: Slurm job ID
    :return: (output_path, error_path)
    """
    output_path = None
    error_path = None

    tokens = split(cmd)
    job_id = str(job_id)

    it = iter(enumerate(tokens))
    for i, token in it:
        # --output=/path
        if token.startswith("--output="):
            output_path = token.split("=", 1)[1]

        # --output /path
        elif token == "--output" and i + 1 < len(tokens):
            output_path = tokens[i + 1]

        # --error=/path
        elif token.startswith("--error="):
            error_path = token.split("=", 1)[1]

        # --error /path
        elif token == "--error" and i + 1 < len(tokens):
            error_path = tokens[i + 1]

    if output_path:
        output_path = output_path.replace("%j", job_id)
    if error_path:
        error_path = error_path.replace("%j", job_id)

    return output_path, error_path
