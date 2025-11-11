import os
import time
import base64
import pwd
from shlex import quote
from datetime import datetime
from src.auth.krb5 import get_krb5
from src.storage import common
from src.common.logger import logger
from src.common.config import get_config
from src.computing.site.strategy import register_site, register_submitter
from src.computing.tools.resources_utils import change_uid_to_username, sub_command, get_user_exp_group, generate_home_link 

@register_site("heps")
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
    krb5_enabled = get_config("common", "krb5_enabled")
    token_filename = ""
    xrootd_path = get_config("computing", "xrootd_path")
    logger.debug(f"Build job env variables: user_home_dir({user_home_dir}), ink_dir({ink_dir}), job_dir({job_dir}), krb5_enabled({krb5_enabled}), token_filename({token_filename})")
    
    if krb5_enabled:
        
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
        
        is_exist, _ = await common.path_exist(name=job_dir, krb5ccname=token_filename, username=username, mgm=xrootd_path)
        if not is_exist:
            await common.mkdir(dname=job_dir, krb5ccname=token_filename, username=username, mode="700", exist_ok=False, mgm=xrootd_path)
        
        #await generate_home_link(user_home_dir, ink_dir, user_group, username, token_filename)
        #logger.info("Generate home link is done.")
        
        job_dir = f"{user_home_dir}/.ink/Jobs/{jobtype}-{time_stamp}"    
        await common.upload_file(src_data=krb5_decoded_bytes, dst=f"{job_dir}/krb5cc_{uid}", krb5ccname=token_filename, username=username, mgm=xrootd_path)
        
        with open(rawjobPath, "rb") as file:
            jobfile_content = file.read()
        await common.upload_file(src_data=jobfile_content, dst=f"{job_dir}/{jobfilename}", krb5ccname=token_filename, username=username, mgm=xrootd_path)

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


@register_submitter("heps", "slurm")
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
                f"export KRB5CCNAME={quote(f'{job_path}/krb5cc_{user_name}')} && "
                f"/usr/bin/aklog && "
                f'sbatch {" ".join(sbatch_command)}'
                f'"'
            ) 
        else:
            command = (
                f"su - {quote(user_name)} -c "
                f'"'
                f"cd {quote(job_path)} && "
                f"setenv KRB5CCNAME {quote(f'{job_path}/krb5cc_{user_name}')} && "
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
    job_id = int(job_id_line.split()[-1]) 
    
    logger.info(f"Submit job finished, the job_id: {job_id}, job_type: {job_type}, job_path: {job_path}")

    return job_id, str(job_type), job_path