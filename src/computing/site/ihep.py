import pwd, grp, asyncio, os, base64
from shlex import quote
from pathlib import Path
from datetime import datetime
from src.storage import common
from src.common.logger import logger
from src.common.config import get_config
from src.auth.krb5 import get_krb5
from src.computing.site.strategy import register_site, register_submitter
from src.computing.tools.resources_utils import change_uid_to_username, sub_command

def _get_user_exp_group(uid):

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


async def _generate_home_link(user_home_dir, ink_dir, user_group, username, token_filename):
    
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


@register_site("ihep")
async def build_job_env(uid, jobtype, rawjobPath, jobfilename):

    time_stamp = datetime.now().strftime('%Y%m%d-%H%M%S')
    username = change_uid_to_username(uid)
    _, user_group = _get_user_exp_group(uid)
    
    ink_dir = get_config("computing", "ink_dir")
    ink_dir = ink_dir.format(user_group=user_group, username=username)
    job_dir = f"{ink_dir}/.ink/Jobs/{jobtype}-{time_stamp}"

    token_filename = ""
    xrootd_path = get_config("computing", "xrootd_path")

    token = get_krb5(username)
    if not token:
        raise Exception("Init KRB5 token failed.")
    krb5_decoded_bytes = base64.b64decode(token)
    
    token_filename = f"/tmp/krb5cc_{uid}_{time_stamp}"
    if not os.path.exists(token_filename):
        with open(token_filename, 'wb') as file:
            file.write(krb5_decoded_bytes)

    os.environ['KRB5CCNAME'] = token_filename
    _ = await sub_command(("aklog"), 5, "init aklog failed.", "init aklog timout.")
    
    is_exist, _ = await common.path_exist(name=job_dir, krb5ccname=token_filename, username=username, mgm=xrootd_path)
    if not is_exist: 
        await common.mkdir(dname=job_dir, krb5ccname=token_filename, username=username, mode="700", exist_ok=False, mgm=xrootd_path)
    
    #user_home_dir = os.path.expanduser(f'~{username}')
    #await _generate_home_link(user_home_dir, ink_dir, user_group, username, token_filename)
    #job_dir = f"{user_home_dir}/.ink/Jobs/{jobtype}-{time_stamp}"    

    await common.upload_file(src_data=krb5_decoded_bytes, dst=f"{job_dir}/krb5cc_{uid}", krb5ccname=token_filename, username=username, mgm=xrootd_path)

    with open(rawjobPath, "rb") as file:
        jobfile_content = file.read()
    await common.upload_file(src_data=jobfile_content, dst=f"{job_dir}/{jobfilename}", krb5ccname=token_filename, username=username, mgm=xrootd_path)
    await common.chmod(fname=f"{job_dir}/{jobfilename}", krb5ccname=token_filename, mode="700", username=username, mgm=xrootd_path)
    
    return job_dir, token_filename


@register_submitter("ihep", "htcondor")
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
            f"export KRB5CCNAME={quote(f'{job_path}/krb5cc_{uid}')} && " 
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

    stdout = await sub_command(command, 20, "submit job failed.", "submit job timeout.")
    logger.info(f"Submit command: {command}")
    logger.info(f"Submit {user_name} job to queue, {stdout.decode()}")
    
    job_id_line = stdout.decode().strip()
    job_id = int(job_id_line.split()[-1].rstrip('.'))
    
    logger.info(f"Submit job finished, and the jobid is {job_id}")

    return job_id, str(job_type), job_path


@register_submitter("ihep", "slurm")
async def submit_hpc_job(sbatch_command, job_type, job_path, uid):
    
    user_name = change_uid_to_username(uid)
    user_info = pwd.getpwuid(uid)
    user_shell = user_info.pw_shell
    
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

    logger.info(f"Submit job command: {command}")
    stdout = await sub_command(command, 5, "submit job failed.", "submit job timeout.")
    
    job_id_line = stdout.decode().strip()
    job_id = int(job_id_line.split()[-1])
    
    logger.info(f"Submit job finished, the job_id: {job_id}, job_type: {job_type}, job_path: {job_path}")

    return job_id, str(job_type), job_path

