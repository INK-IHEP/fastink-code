import time as tm
from typing import Optional
from pydantic import BaseModel, Field
from src.computing.tools.resources_utils import *
from src.computing.common import insert_job_info
from src.common.logger import logger
from src.computing.site.strategy import get_site, get_submitter

'''
Author:         guocq@ihep.ac.cn
Created:        2024-12-18
Last Modified:  2024-12-18
'''

class HTC_JOB(BaseModel):
    job_script: str = Field(..., description="Slurm 脚本内容")  # 必填
    job_parameters: Optional[str] = Field(None, description="自定义 HTCondor 参数")  # 可选参数，-wn bws0900.ihep.ac.cn -os AlmaLinux9
    cpu: Optional[str] = Field(None, description="使用的CPU核数")  # 可选
    mem: Optional[str] = Field(None, description="内存需求")  # 可选
    os: Optional[str] = Field(None, description="操作系统镜像") # 可选
    gpu_num: Optional[str] = Field(None, description="GPU 数量")  # 可选
    job_name: str = Field(f"", description="job name")  # 可选
    wn: str = Field(f"", description="woker node host")  # 可选


async def create_htc_job(
    request: HTC_JOB,
    job_type: str,
    uid: int,
    clusterid: str):
    
    try:
        # 获取作业的参数
        username = change_uid_to_username(uid)
        CLUSTER_SCRIPTS = get_config("computing", "cluster_scripts")
        xrootd_path = get_config("computing", "xrootd_path")
        site = get_config("computing", "site")
        token_filename = None
        
        if job_type == "jupyter":
            script_file = "start-jupyterlab-token.sh"
            absolute_script_path = f"{CLUSTER_SCRIPTS}/{script_file}"

        elif job_type == "enode":
            script_file = "start-sshd.sh"
            absolute_script_path = f"{CLUSTER_SCRIPTS}/{script_file}"

        elif job_type == "vscode":
            script_file = "start-vscode.sh"
            absolute_script_path = f"{CLUSTER_SCRIPTS}/{script_file}"

        elif job_type == "rootbrowse":
            script_file = "start-rootbrowse.sh"
            absolute_script_path = f"{CLUSTER_SCRIPTS}/{script_file}"

        elif job_type == "vnc":
            script_file = "start-vnc.sh"
            absolute_script_path = f"{CLUSTER_SCRIPTS}/{script_file}"
            
        elif job_type == "ink_special":
            request.wn = get_config("computing", "special_machine")
            script_file = "start-vnc.sh"
            absolute_script_path = f"{CLUSTER_SCRIPTS}/{script_file}"

        elif job_type == "npu":
            request.wn = get_config("computing", "npu_machine")
            script_file = "start-npu.sh"
            absolute_script_path = f"{CLUSTER_SCRIPTS}/{script_file}"
            
        else:
            timestamp = int(tm.time())
            script_file = f'inkjob_{uid}_{timestamp}.sh'
            absolute_script_path = f"/tmp/{script_file}"

            with open(absolute_script_path, 'w') as f:
                f.write(request.job_script) 
            os.chmod(absolute_script_path, 0o755) 
            
        build_job_env = get_site(site)
        job_path, token_filename = await build_job_env(uid, job_type, absolute_script_path, script_file)
        logger.info(f"Build job env func finished.")

        submit_file = generate_condor_submit(uid, script_file, job_path, request.cpu, request.mem, job_type, request.os, request.job_parameters, request.wn)
        logger.info(f"Generate condor submit func finished.")
        
        with open(f"/tmp/{submit_file}", "rb") as file:
            submitfile_content = file.read()
        await common.upload_file(src_data=submitfile_content, dst=f"{job_path}/{submit_file}", krb5ccname=token_filename, username=username, mgm=xrootd_path)
             
        try:   
            submitter = get_submitter(site, clusterid)
            job_id, job_type, job_path = await submitter(submit_file, job_type, job_path, uid)
            output = f"{job_path}/{script_file}.out"
            errpath = f"{job_path}/{script_file}.err"            
            insert_job_info(uid, job_id, output, errpath, job_type, job_path, clusterid)

            return job_id, job_type, job_path
        
        except Exception as e:
            logger.error(f"Some Wrong in Submit job, {e}")
            raise HTTPException(status_code=500, detail=str(e))
        
    except Exception as e:
        logger.error(f"Some Wrong in Create job, {e}")
        raise HTTPException(status_code=500, detail=str(e))
    
    finally:
        if job_type == "common" and os.path.exists(absolute_script_path):
            os.remove(absolute_script_path)
        if token_filename and os.path.exists(token_filename):
            os.remove(token_filename)


