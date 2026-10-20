"""Generic (site-independent) job environment builder and submitters.

Behavior is driven entirely by configuration:

- ``computing.ink_dir``: job directory layout template. ``~`` means the
  user's home directory; otherwise it supports ``{username}``,
  ``{user_group}``, ``{experiment_group}``, and
  ``{experiment_group_lower}``.
- ``common.krb5_enabled``: when true, a Kerberos ticket is created for
  the user and uploaded alongside the job script.
- ``computing.schedd_host`` / ``computing.cm_host``: HTCondor schedd and
  central manager used by the htcondor submitter.

Site plugins can register their own strategies under a different site
name; anything they do not customize falls back to these
implementations.
"""

import base64
import pwd
from datetime import datetime
from shlex import quote

from fastink.auth.backends.krb5 import get_krb5
from fastink.common.config import get_config
from fastink.common.logger import logger
from fastink.computing.site.strategy import register_site, register_submitter
from fastink.computing.tools.common.utils import (
    change_uid_to_username,
    get_user_jobs_dir,
    sub_command,
)
from fastink.storage import common


@register_site("generic")
async def build_job_env(uid, jobtype, rawjobPath, jobfilename):

    time_stamp = datetime.now().strftime('%Y%m%d-%H%M%S')
    username = change_uid_to_username(uid)

    job_dir = f"{get_user_jobs_dir(username, uid)}/{jobtype}-{time_stamp}"

    krb5_enabled = get_config("common", "krb5_enabled")
    token_filename = ""
    xrootd_path = get_config("storage", "xrd_host")

    krb5_decoded_bytes = None
    if krb5_enabled:
        token = get_krb5(username)
        if not token:
            raise Exception("Init KRB5 token failed.")
        krb5_decoded_bytes = base64.b64decode(token)

        token_filename = f"/tmp/krb5cc_{uid}_{time_stamp}"
        if not os.path.exists(token_filename):
            with open(token_filename, 'wb') as file:
                file.write(krb5_decoded_bytes)

    is_exist, _ = await common.path_exist(name=job_dir, username=username, mgm=xrootd_path)
    if not is_exist:
        await common.mkdir(dname=job_dir, username=username, mode="700", exist_ok=False, mgm=xrootd_path)

    if krb5_decoded_bytes is not None:
        await common.upload_file(src_data=krb5_decoded_bytes, dst=f"{job_dir}/krb5cc_{uid}", username=username, mgm=xrootd_path)

    with open(rawjobPath, "rb") as file:
        jobfile_content = file.read()
    await common.upload_file(src_data=jobfile_content, dst=f"{job_dir}/{jobfilename}", username=username, mgm=xrootd_path)
    await common.chmod(fname=f"{job_dir}/{jobfilename}", mode="700", username=username, mgm=xrootd_path)

    return job_dir, token_filename


@register_submitter("generic", "htcondor")
async def submit_htc_job(submit_file, job_type, job_path, uid):

    user_name = change_uid_to_username(uid)
    schedd_host = get_config("computing", "schedd_host")
    cm_host = get_config("computing", "cm_host")
    user_info = pwd.getpwuid(uid)
    user_shell = user_info.pw_shell

    if job_type == "jupyter" or job_type == "npu":
        command = (
            f"su -s /bin/bash {quote(user_name)} -c "
            f'"'
            f"cd {quote(job_path)} && "
            f"condor_submit -name {quote(schedd_host)} -pool {quote(cm_host)} {quote(submit_file)}"
            f'"'
        )
    else:
        if user_shell in ["/bin/bash", "/bin/sh", "/bin/zsh"]:
            command = (
                f"su - {quote(user_name)} -c "
                f'"'
                f"cd {quote(job_path)} && "
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
                f"setenv INKPATH $PATH && "
                f"setenv INKLDPATH $LD_LIBRARY_PATH && "
                f"setenv PATH /usr/bin:$PATH && "
                f"setenv LD_LIBRARY_PATH /lib64:$LD_LIBRARY_PATH && "
                f"condor_submit -name {quote(schedd_host)} -pool {quote(cm_host)} {quote(submit_file)}"
                f'"'
            )

    stdout = await sub_command(command, 20, "submit job failed.", "submit job timeout.")
    logger.info(f"Submit command: {command}")
    logger.info(f"Submit {user_name} job to queue, {stdout.decode()}")

    job_id_line = stdout.decode().strip()
    job_id = int(job_id_line.split()[-1].rstrip('.'))

    logger.info(f"Submit job finished, and the jobid is {job_id}")

    return job_id, str(job_type), job_path


@register_submitter("generic", "slurm")
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
    stdout = await sub_command(command, 30, "submit job failed.", "submit job timeout.")

    job_id_line = stdout.decode().strip()
    job_id = int(job_id_line.split()[-1])

    logger.info(f"Submit job finished, the job_id: {job_id}, job_type: {job_type}, job_path: {job_path}")

    return job_id, str(job_type), job_path
