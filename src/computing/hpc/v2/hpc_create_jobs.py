import re
import threading
import time as tm
from pathlib import Path
from typing import Optional
from src.common.logger import logger
from pydantic import BaseModel, Field
from src.common.config import get_config
from datetime import datetime, timedelta
from src.computing.tools.resources_utils import *
from src.computing.site import hai, ihep
from src.computing.site.strategy import get_site, get_submitter


# exec "sbatch --test-only" and get estimated start time
def get_test_only_start_time(command_output):
    match = re.search(r"start at (\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})", command_output)
    if match:
        start_time_str = match.group(1)
        return datetime.strptime(start_time_str, "%Y-%m-%dT%H:%M:%S")
    return None


def is_start_within_five_minutes(start_time):
    print("in is_start_within_five_minutes")
    now = datetime.now().replace(microsecond=0)
    start_time = start_time.replace(microsecond=0) 
    
    print(f"当前时间: {now}, 预计开始时间: {start_time}")
    print(now <= start_time <= now + timedelta(minutes=5))
    
    return now <= start_time <= now + timedelta(minutes=5)

# The request model to create a job 
class JobCreateRequest(BaseModel):
    job_script: str = Field(..., description="Slurm job script")  # Mandatory
    job_parameters: Optional[str] = Field(None, description="Optional sbatch job parameters")  # optional --commit = xxx --time = xxx
    time: Optional[str] = Field(None, description="Max wall time with formt HH:MM:SS")  # optional
    partition: str = Field(..., description="Partition")  # mandatory
    nodes: Optional[str] = Field(None, description="Number of nodes")  # optional
    ntasks: Optional[str] = Field(None, description="Number of CPU cores")  # optional
    mem: Optional[str] = Field(None, description="Memory size")  # optional
    account: str = Field(..., description="Group name")  # mandatory
    qos: str = Field(..., description="QOS")  # mandatory
    gpu_name: Optional[str] = Field(None, description="gpu or dcu or npu")  # optional
    gpu_num: Optional[str] = Field(None, description="Number of GPU/DCU/NPU cards")  # optional
    gpu_type: Optional[str] = Field(None, description="GPU/DCU/NPU type")  # optional
    output_file: Optional[str] = Field(None, description="output_file name")  # optional
    error_file: Optional[str] = Field(None, description="error_file name")  # optional
    ntasks_per_node: int = Field(1, description="error_file")  # optional
    job_name: str = Field(f"", description="job name")  # optional
    env: Optional[str] = Field(None, description="Environment variables")  # optional
    pre_script: Optional[str] = Field(None, description="script execuated before job start")  # optional
    post_script: Optional[str] = Field(None, description="script execuated after job finish")  # optional

# The request model to create a job with input file path and job script path provided 
class JobCreateRequestWithPath(BaseModel):
    job_script_abs_path: str = Field(..., description="Absolute path to Slurm job script")  # Mandatory
    job_input_abs_path: str = Field(None, description="Absolute path to input file") # Optional
    job_parameters: Optional[str] = Field(None, description="Optional sbatch job parameters")  # optional --commit = xxx --time = xxx
    time: Optional[str] = Field(None, description="Max wall time with formt HH:MM:SS")  # optional
    partition: str = Field(..., description="Partition")  # mandatory
    nodes: Optional[str] = Field(None, description="Number of nodes")  # optional
    ntasks: Optional[str] = Field(None, description="Number of CPU cores")  # optional
    mem: Optional[str] = Field(None, description="Memory size")  # optional
    account: str = Field(..., description="Group name")  # mandatory
    qos: str = Field(..., description="QOS")  # mandatory
    gpu_name: Optional[str] = Field(None, description="gpu or dcu or npu")  # optional
    gpu_num: Optional[str] = Field(None, description="Number of GPU/DCU/NPU cards")  # optional
    gpu_type: Optional[str] = Field(None, description="GPU/DCU/NPU type")  # optional
    output_file: Optional[str] = Field(None, description="output_file name")  # optional
    error_file: Optional[str] = Field(None, description="error_file name")  # optional
    ntasks_per_node: int = Field(1, description="error_file")  # optional
    job_name: str = Field("", description="job name")  # optional
    env: Optional[str] = Field(None, description="Environment variables")  # optional

# route function to create job
async def create_job(
    request: JobCreateRequest,
    job_type: str,
    uid: int,
    cluster_id):

    sbatch_command = []
    CLUSTER_SCRIPTS = get_config("computing", "cluster_scripts")

    if job_type == "enode":
        script_file = "start-sshd.sh"
        absolute_script_path = f"{CLUSTER_SCRIPTS}/{script_file}"

    elif job_type == "jupyter":
        script_file = "start-jupyterlab-token.sh"
        absolute_script_path = f"{CLUSTER_SCRIPTS}/{script_file}"

    elif job_type == "rootbrowse":
            script_file = "start-rootbrowse.sh"
            absolute_script_path = f"{CLUSTER_SCRIPTS}/{script_file}"

    elif job_type == "vscode":
        script_file = "start-vscode.sh"
        absolute_script_path = f"{CLUSTER_SCRIPTS}/{script_file}"
        
    elif job_type == "vnc":
        script_file = "start-vnc.sh"
        absolute_script_path = f"{CLUSTER_SCRIPTS}/{script_file}"

    else:
        timestamp = int(tm.time())
        script_file = f'inkjob_{uid}_{timestamp}.sh'
        absolute_script_path = f"/tmp/{script_file}"

        with open(absolute_script_path, 'w') as f:
            f.write(request.job_script)
        _ = await sub_command(f"chmod +x {absolute_script_path}", timeoutsec=5, errinfo="chmod err", tminfo="chmod timeout")

    #job_path, token_filename = await build_job_env(uid, job_type, absolute_script_path, script_file)
    site = get_config("computing", "site")
    build_job_env = get_site(site)
    job_path, token_filename = await build_job_env(uid, job_type, absolute_script_path, script_file)
    logger.info(f"Completed the build job env process : job_path : f{job_path}, token_filename : f{token_filename}")

    parameters = f" --comment={job_type} "

    output_file = request.output_file
    error_file = request.error_file
    if output_file == None:
        output_file = f"{job_path}/{script_file}.out"
    else:
        output_file = f"{job_path}/{output_file}"
    if error_file == None:
        error_file = f"{job_path}/{script_file}.err"
    else:
        error_file = f"{job_path}/{error_file}"

    parameters += f"--output={output_file} "
    parameters += f"--error={error_file} "

    if request.time:
        parameters += f"--time={request.time} "
    if request.partition:
        parameters += f"--partition={request.partition} "
    if request.nodes:
        parameters += f"--nodes={request.nodes} "
    if request.ntasks:
        parameters += f"--ntasks={request.ntasks} "
    if request.mem:
        parameters += f"--mem={request.mem} "
    if request.account:
        parameters += f"--account={request.account} "
    if request.qos:
        parameters += f"--qos={request.qos} "
    if request.gpu_name:
        if request.gpu_num and request.gpu_type:
            parameters += f"--gres={request.gpu_name}:{request.gpu_type}:{request.gpu_num} "
        elif request.gpu_num:
            parameters += f"--gres={request.gpu_name}:{request.gpu_num} "
    if request.job_parameters:
        parameters += f"{request.job_parameters} "
    if request.job_name:
        parameters += f"--job-name={request.job_name} "

    if request.pre_script:
        try:
            with open(f"{job_path}/{script_file}", "r") as f:
                lines = f.readlines()
        except FileNotFoundError:
            lines = []
        
        while len(lines) < 2:
            lines.append("\n")
        
        pre_lines = [line + "\n" for line in request.pre_script.splitlines()]
        lines = lines[:2] + pre_lines + lines[2:]

        if request.post_script:
            post_script = [line + "\n" for line in request.post_script.splitlines()]
            lines.append("\n")          
            lines.extend(post_script)    

        with open(f"{job_path}/{script_file}", "w") as f:
            f.writelines(lines)

    elif request.post_script:
        try:
            with open(f"{job_path}/{script_file}", "r") as f:
                lines = f.readlines()
        except FileNotFoundError:
            lines = []
        post_script = [line + "\n" for line in request.post_script.splitlines()]
        lines.append("\n")
        lines.extend(post_script) 
        with open(f"{job_path}/{script_file}", "w") as f:
            f.writelines(lines)

    sbatch_command += parameters.split()
    sbatch_command += [f"{script_file}"]
    logger.info(f"The sbatch_command: {sbatch_command}")

    try:
        submitter = get_submitter(site, cluster_id)
        job_id, job_type, job_path = await submitter(sbatch_command, job_type, job_path, uid)
        if not job_id:
            raise ValueError("Failed to get job ID from SLURM response")
        insert_job_info(uid, job_id, output_file, error_file, job_type, job_path, cluster_id)

        def add_admincomment():
            if job_id:
                admincomment_command = f"sacctmgr -i modify job set admincomment={job_type} where jobid={job_id}"
                try:
                    asyncio.run(sub_command(admincomment_command, timeoutsec=5, errinfo="add admincomment err", tminfo="add admincomment timeout"))
                except Exception as e:
                    tm.sleep(1)
                    add_admincomment()
        threading.Thread(target=add_admincomment).start()

        return job_id, job_type, job_path

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

    finally:
        if job_type != "enode" and job_type != "jupyter" and job_type != "rootbrowse" and job_type != "vscode" and job_type != "vnc":
            try:
                if os.path.exists(absolute_script_path):
                    os.remove(absolute_script_path)
            except Exception as e:
                raise HTTPException(status_code=500, detail=str(e))


# route function to create job with input file and job script path provided
async def create_job_with_path(
    request: JobCreateRequestWithPath,
    uid: int,
    cluster_id,
    job_type="common"
):

    sbatch_command = []
    
    if not Path(request.job_script_abs_path).exists():
        raise HTTPException(status_code=404, detail=f"Job script not found with path: {request.job_script_abs_path}")
    
    job_script_abs_path = request.job_script_abs_path
    job_script_dir = Path(job_script_abs_path).parent
    script_file = Path(job_script_abs_path).name

    site = get_config("computing", "site")
    build_job_env = get_site(site)
    job_path, token_filename = await build_job_env(uid, job_type, job_script_abs_path, script_file)

    parameters = f" --comment={job_type} "

    # job_input_abs_path could be ""
    job_input_abs_path = request.job_input_abs_path
    if job_input_abs_path and not Path(job_input_abs_path).exists():
        raise HTTPException(status_code=404, detail="Job input file not found with path: {job_input_abs_path}")
    if job_input_abs_path:
        parameters += f"--input={job_input_abs_path} "
        
    output_file = request.output_file
    error_file = request.error_file
    if output_file == None:
        output_file = f"{job_path}/{script_file.split('.')[0].strip()}.out"
    elif not Path(output_file).is_absolute():
        output_file = f"{job_path}/{output_file}"
        
    if error_file == None:
        error_file = f"{job_path}/{script_file.split('.')[0].strip()}.err"
    elif not Path(error_file).is_absolute():
        error_file = f"{job_path}/{error_file}"
        
    parameters += f"--output={output_file} "
    parameters += f"--error={error_file} "

    if request.time:
        parameters += f"--time={request.time} "
    if request.partition:
        parameters += f"--partition={request.partition} "
    if request.nodes:
        parameters += f"--nodes={request.nodes} "
    if request.ntasks:
        parameters += f"--ntasks={request.ntasks} "
    if request.mem:
        parameters += f"--mem={request.mem} "
    if request.account:
        parameters += f"--account={request.account} "
    if request.qos:
        parameters += f"--qos={request.qos} "
    if request.gpu_name:
        if request.gpu_num and request.gpu_type:
            parameters += f"--gres={request.gpu_name}:{request.gpu_type}:{request.gpu_num} "
        elif request.gpu_num:
            parameters += f"--gres={request.gpu_name}:{request.gpu_num} "
    if request.job_parameters:
        parameters += f"{request.job_parameters} "
    if request.job_name:
        parameters += f"--job-name={request.job_name} "

    # combine sbatch command
    sbatch_command += parameters.split()
    sbatch_command += [f"{script_file}"]

    try:
        submitter = get_submitter(site, cluster_id)
        job_id, job_type, job_path = await submitter(sbatch_command, job_type, job_path, uid)
        
        if not job_id:
            raise ValueError("Failed to get job ID from SLURM response")

        # Insert job info into DB
        insert_job_info(uid, job_id, output_file, error_file, job_type, job_path, cluster_id)

        # Question : dead loop?
        def add_admincomment():
            if job_id:
                # add admincomment as root
                admincomment_command = f"sacctmgr -i modify job set admincomment={job_type} where jobid={job_id}"
                try:
                    asyncio.run(sub_command(admincomment_command, timeoutsec=5, errinfo="add admincomment err", tminfo="add admincomment timeout"))
                except Exception as e:
                    tm.sleep(1)
                    add_admincomment()

        # fork a thread to run the add_admincomment function
        threading.Thread(target=add_admincomment).start()
        
        # return job_id, job_type and job_path as response
        return job_id, job_type, job_path

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))