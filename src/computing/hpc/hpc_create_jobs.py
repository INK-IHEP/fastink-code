from pydantic import BaseModel, Field
from typing import Optional
import re
from datetime import datetime, timedelta
import time as tm
import threading
from src.computing.tools.resources_utils import *
from src.common.config import get_config
from src.common.logger import logger

# 提取sbatch --test-only的预计开始时间
def get_test_only_start_time(command_output):
    print("in get_test_only_start_time")
    match = re.search(r"start at (\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})", command_output)
    if match:
        start_time_str = match.group(1)
        return datetime.strptime(start_time_str, "%Y-%m-%dT%H:%M:%S")
    return None

# 比较当前时间和预计开始时间
def is_start_within_five_minutes(start_time):
    print("in is_start_within_five_minutes")
    now = datetime.now().replace(microsecond=0)
    start_time = start_time.replace(microsecond=0) 
    
    print(f"当前时间: {now}, 预计开始时间: {start_time}")
    print(now <= start_time <= now + timedelta(minutes=5))
    
    return now <= start_time <= now + timedelta(minutes=5)

# 定义请求模型
class JobCreateRequest(BaseModel):
    job_script: str = Field(..., description="Slurm 脚本内容")  # 必填
    job_parameters: Optional[str] = Field(None, description="自定义 Slurm 参数")  # 可选 --commit = xxx --time = xxx
    time: Optional[str] = Field(None, description="作业运行时间上限 (格式: HH:MM:SS)")  # 可选
    partition: str = Field(..., description="作业分区")  # 必填
    nodes: Optional[str] = Field(None, description="申请的节点数")  # 可选
    ntasks: Optional[str] = Field(None, description="使用的CPU核数")  # 可选
    mem: Optional[str] = Field(None, description="内存需求")  # 可选
    account: str = Field(..., description="组名称")  # 必填
    qos: str = Field(..., description="服务质量 (QoS)")  # 必填
    gpu_name: Optional[str] = Field(None, description="GPU or DCU")  # 可选
    gpu_num: Optional[str] = Field(None, description="GPU 数量")  # 可选
    gpu_type: Optional[str] = Field(None, description="GPU 类型")  # 可选
    output_file: str = Field(None, description="output_file name")  # 可选
    error_file: str = Field(None, description="error_file name")  # 可选
    ntasks_per_node: int = Field(1, description="error_file")  # 可选
    job_name: str = Field(f"", description="job name")  # 可选
    env: Optional[str] = Field(None, description="作业加载的具体环境")  # 可选
    pre_script: Optional[str] = Field(None, description="作业运行前执行脚本")  # 可选
    post_script: Optional[str] = Field(None, description="作业运行后执行脚本")  # 可选

# 创建作业的API
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

    job_path, token_filename = await build_job_env(uid, job_type, absolute_script_path, script_file)
    logger.info(f"Completed the build job env process.")
    

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
        # 读取原文件内容（若存在）
        try:
            with open(f"{job_path}/{script_file}", "r") as f:
                lines = f.readlines()
        except FileNotFoundError:
            lines = []
        
        # 确保文件至少有2行（第三行需要插入位置）
        while len(lines) < 2:
            lines.append("\n")
        
        # 将 pre_script 按行拆分，并确保每行有换行符
        pre_lines = [line + "\n" for line in request.pre_script.splitlines()]
        
        # 在第三行插入 pre_script 内容
        lines = lines[:2] + pre_lines + lines[2:]
        
        if request.post_script:
            post_script = [line + "\n" for line in request.post_script.splitlines()]
            lines.append("\n")          # 添加空行
            lines.extend(post_script)    # 追加 pre_script
        
        # 写入修改后的内容
        with open(f"{job_path}/{script_file}", "w") as f:
            f.writelines(lines)
    elif request.post_script:
        try:
            with open(f"{job_path}/{script_file}", "r") as f:
                lines = f.readlines()
        except FileNotFoundError:
            lines = []
        post_script = [line + "\n" for line in request.post_script.splitlines()]
        lines.append("\n")          # 添加空行
        lines.extend(post_script) 
        with open(f"{job_path}/{script_file}", "w") as f:
            f.writelines(lines)
    
        # 将脚本文件添加到 sbatch 命令
    sbatch_command += parameters.split()
    sbatch_command += [f"{script_file}"]
    
    logger.info(f"The sbatch_command: {sbatch_command}")

    try:
        # if job_type == "enode" or job_type == "jupyter" or job_type == "rootbrowse" or job_type == "vscode":
        #     test_output = await test_submit_hpc_job(sbatch_command, uid, job_path)
        #     start_time = get_test_only_start_time(test_output.decode())

        #     if start_time:
        #         if not is_start_within_five_minutes(start_time):
        #             return {"status": 400, "msg": "资源不足，请稍后再提交作业"}
        #     else:
        #         return {"status": 500, "msg": "无法获取作业的预计开始时间"}
            
        logger.info("Enter the submit hpc job.")
        job_id, job_type, job_path = await submit_hpc_job(sbatch_command, job_type, job_path, uid)
        logger.info(f"Finish the submit hpc job process, the job_id: {job_id}")
        
        if not job_id:
            raise ValueError("Failed to get job ID from SLURM response")
        
        insert_job_info(uid, job_id, output_file, error_file, job_type, job_path, cluster_id)

        def add_admincomment():
            # 获取作业 ID
            if job_id:
                # 使用 root 权限添加 admincomment
                admincomment_command = f"sacctmgr -i modify job set admincomment={job_type} where jobid={job_id}"
                try:
                    asyncio.run(sub_command(admincomment_command, timeoutsec=5, errinfo="add admincomment err", tminfo="add admincomment timeout"))
                except Exception as e:
                    tm.sleep(1)
                    add_admincomment()

        # 创建一个线程来运行 admincomment 命令
        threading.Thread(target=add_admincomment).start()

        # 返回作业提交的响应
        return job_id, job_type, job_path

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

    finally:
        if job_type != "enode" and job_type != "jupyter" and job_type != "rootbrowse" and job_type != "vscode" and job_type != "vnc":
            try:
                if os.path.exists(absolute_script_path):
                    os.remove(absolute_script_path)
            except Exception as e:
                print(f"remove script_file faild ! msg:{e}")