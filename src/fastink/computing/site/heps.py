import os
import base64
import pwd
from shlex import quote
from datetime import datetime
from fastink.auth.krb5 import get_krb5
from fastink.storage import common
from fastink.common.config import get_config
from fastink.computing.site.strategy import register_site, register_submitter
from fastink.computing.tools.common.utils import change_uid_to_username, sub_command, get_user_exp_group

from fastink.computing.tools.perflog.timing import log_step
import logging
build_env_logger = logging.getLogger("ink.create_jobs.build_env")
sbatch_logger = logging.getLogger("ink.create_jobs.su_sbatch_cmd")

@register_site("heps")
async def build_job_env(uid, jobtype, rawjobPath, jobfilename):
    
    time_stamp = datetime.now().strftime('%Y%m%d-%H%M%S')
    
    with log_step("resolve_user_info", logger=build_env_logger, phase="build_job_env"):
        username = change_uid_to_username(uid)
        _, user_group = get_user_exp_group(uid)
        
    with log_step("resolve_paths", logger=build_env_logger, phase="build_job_env"):
        user_home_dir = os.path.expanduser(f'~{username}')
        ink_dir = get_config("computing", "ink_dir")
        job_dir = ""
        if ink_dir == "~":
            job_dir = f"{user_home_dir}/.ink/Jobs/{jobtype}-{time_stamp}"
        else:
            job_dir = f"{ink_dir}/{username[0]}/{username}/.ink/Jobs/{jobtype}-{time_stamp}"
        krb5_enabled = get_config("common", "krb5_enabled")
        token_filename = ""
        xrootd_path = get_config("storage", "xrd_host")
    
    build_env_logger.debug(f"Build job env variables: user_home_dir({user_home_dir}), ink_dir({ink_dir}), job_dir({job_dir}), krb5_enabled({krb5_enabled}), token_filename({token_filename})")
    
    if krb5_enabled:
        with log_step("krb5_get_token", logger=build_env_logger, phase="build_job_env"):
            token = get_krb5(username)
            if token != "":  
                krb5_decoded_bytes = base64.b64decode(token)
            else:
                raise Exception("Init KRB5 token failed.")
        
        with log_step("write_krb5_token_file", logger=build_env_logger, phase="build_job_env"):
            token_filename = f"/tmp/krb5cc_{uid}_{time_stamp}"
            if not os.path.exists(token_filename):
                with open(token_filename, 'wb') as file:
                    file.write(krb5_decoded_bytes)
        
        with log_step("xrootd_prepare_job_dir", logger=build_env_logger, phase="build_job_env"):
            is_exist, _ = await common.path_exist(name=job_dir, username=username, mgm=xrootd_path)
            if not is_exist:
                await common.mkdir(dname=job_dir, username=username, mode="700", exist_ok=False, mgm=xrootd_path)
        
        #await generate_home_link(user_home_dir, ink_dir, user_group, username, token_filename)
        #logger.info("Generate home link is done.")
        with log_step("xrootd_upload_krb5_token", logger=build_env_logger, phase="build_job_env"):
            job_dir = f"{user_home_dir}/.ink/Jobs/{jobtype}-{time_stamp}"    
            await common.upload_file(src_data=krb5_decoded_bytes, dst=f"{job_dir}/krb5cc_{uid}", username=username, mgm=xrootd_path)
        
        with log_step("upload_job_script", logger=build_env_logger, phase="build_job_env"):
            with open(rawjobPath, "rb") as file:
                jobfile_content = file.read()
            await common.upload_file(src_data=jobfile_content, dst=f"{job_dir}/{jobfilename}", username=username, mgm=xrootd_path)

    else: 
        with log_step("xrootd_prepare_job_dir", logger=build_env_logger, phase="build_job_env"):
            is_exist, _ = await common.path_exist(name=job_dir, username=username, mgm=xrootd_path)
            if not is_exist:
                await common.mkdir(dname=job_dir, username=username, mode="700", exist_ok=False, mgm=xrootd_path)
        
        with log_step("upload_job_script", logger=build_env_logger):
            job_content = ""
            with open(f"{rawjobPath}", 'rb') as file:
                job_content = file.read()
        
        await common.upload_file(src_data=job_content, dst=f"{job_dir}/{jobfilename}", username=username, mgm=xrootd_path)
        
    with log_step("xrootd_chmod_job_script", logger=build_env_logger, phase="build_job_env"):    
        await common.chmod(fname=f"{job_dir}/{jobfilename}", mode="700", username=username, mgm=xrootd_path)
    
    return job_dir, token_filename


@register_submitter("heps", "slurm")
async def submit_hpc_job(sbatch_command, job_type, job_path, uid):
    with log_step("resolve_user_info", logger=sbatch_logger, phase="sbatch_submit"):
        user_name = change_uid_to_username(uid)
        user_info = pwd.getpwuid(uid)
        user_shell = user_info.pw_shell
    
    with log_step("read_config", logger=sbatch_logger, phase="sbatch_submit"):
        krb5_enabled = get_config("common", "krb5_enabled")
    
    with log_step("build_sbatch_command", logger=sbatch_logger, phase="sbatch_submit"):
        command = (
            f"su - {quote(user_name)} -c "
            f'"'
            f"cd {quote(job_path)} && "
            f'sbatch {" ".join(sbatch_command)}'
            f'"'
        )
    
    sbatch_logger.info(f"Submit job command: {command}")
    with log_step("execute_sbatch", logger=sbatch_logger, phase="sbatch_submit"):
        stdout = await sub_command(command, 30, "submit job failed.", "submit job timeout.")
    
    with log_step("parse_job_id", logger=sbatch_logger, phase="sbatch_submit"):
        job_id_line = stdout.decode().strip()
        job_id = int(job_id_line.split()[-1]) 
    
    sbatch_logger.info(f"Submit job finished, the job_id: {job_id}, job_type: {job_type}, job_path: {job_path}")

    return job_id, str(job_type), job_path