import os
from shlex import quote
from datetime import datetime
from src.storage import common
from src.common.logger import logger
from src.common.config import get_config
from src.computing.site.strategy import register_site, register_submitter
from src.computing.tools.resources_utils import change_uid_to_username, sub_command

@register_site("hai")
async def build_job_env(uid, jobtype, rawjobPath, jobfilename):

    time_stamp = datetime.now().strftime('%Y%m%d-%H%M%S')
    username = change_uid_to_username(uid)
    user_home_dir = os.path.expanduser(f'~{username}')
    job_dir = f"{user_home_dir}/.ink/Jobs/{jobtype}-{time_stamp}"
    token_filename = ""
    xrootd_path = get_config("computing", "xrootd_path")
    
    is_exist, _ = await common.path_exist(name=job_dir, krb5ccname=token_filename, username=username, mgm=xrootd_path)
    if not is_exist:
        await common.mkdir(dname=job_dir, krb5ccname=token_filename, username=username, mode="700", exist_ok=False, mgm=xrootd_path)
    
    with open(rawjobPath, "rb") as file:
        jobfile_content = file.read()
    await common.upload_file(src_data=jobfile_content, dst=f"{job_dir}/{jobfilename}", krb5ccname=token_filename, username=username, mgm=xrootd_path)
    
    await common.chmod(fname=f"{job_dir}/{jobfilename}", krb5ccname=token_filename, mode="700", username=username, mgm=xrootd_path)
    
    return job_dir, token_filename 


@register_submitter("hai", "slurm")
async def submit_hpc_job(sbatch_command, job_type, job_path, uid):
    
    user_name = change_uid_to_username(uid)
    
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