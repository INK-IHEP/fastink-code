import subprocess
from fastapi import HTTPException
from src.computing.tools.resources_utils import change_uid_to_username
from src.inkdb.inkdb import *
from src.computing.gateway_tools import *
import json
from src.computing.tools.resources_utils import *

async def get_job_status(jobId,uid,cluster_id):

    response_data = {
        "status": 0,
        "msg": "",
        "data": {
            "jobId": jobId,
            "jobStatus": "UNKNOWN"
            }
        }


    try:
        user_name = change_uid_to_username(uid)
        # 构建 scontrol show job 命令来查询作业状态
        scontrol_command = f"sudo -u {user_name} sacct -j {jobId} --format=JobID,state -P"

        # 执行查询命令
        result = await sub_command(scontrol_command, timeoutsec=2, errinfo="scontrol err", tminfo="scontrol timeout")
        # result = subprocess.run(scontrol_command, shell=True, capture_output=True, text=True)

        # 检查命令执行结果
        output_lines = result.strip().split('\n')
        # output_lines = result.stdout.strip().split('\n')

        if len(output_lines) < 2:
            # raise HTTPException(status_code=404, detail="Job not found")
            return response_data

        # 解析作业状态
        job_info = output_lines[1].split('|')
        job_status = job_info[1]
        if job_status == "PENDING":
            job_status = "QUEUEING"

        # 数据库相关操作
        ret, msg, job_type, db_job_status, job_iptables_status, job_iptables_clean = get_job_info(uid,jobId,cluster_id)
        print(f"job_type={job_type},db_job_status={db_job_status},job_iptables_status={job_iptables_status},job_iptables_clean={job_iptables_clean}")
        if ret<0:
            response_data["data"]["jobStatus"] = "No this job recorded"
            return response_data
        
        if job_status != db_job_status: # 根据 JOBID 更新作业在数据库里的 job_status 字段值
            ret, msg = update_job_status(uid,jobId,job_status,cluster_id)
            if ret<0:
                print(f"err :{msg}")
                response_data.status = ret
                response_data.msg = msg
                return response_data
        
        if job_type == "enode":

                jobId = jobId

                if job_status == "RUNNING":
                    response_data = create_iptables(uid, jobId,job_iptables_status, job_iptables_clean,cluster_id)
                    if response_data["ret"] < 0:
                        return {
                            "status": response_data["ret"],
                            "msg": response_data["msg"],
                            "data": ""
                        }
                
                if job_status != "QUEUEING" and job_status != "RUNNING" and job_status != "CONFIGURING":
                    ret, msg,gateway_port = get_job_iptables_status(uid,jobId,cluster_id)
                    if gateway_port != 0 and job_iptables_clean != 1:
                        response_data = delete_iptables(uid, jobId, gateway_port,cluster_id)
                        if response_data["ret"] < 0:
                            return {
                                "status": response_data["ret"],
                                "msg": response_data["msg"],
                                "data": ""
                            }

        response_data = {
            "status": 200,
            "msg": "请求成功",
            "data": {
                "jobId": jobId,
                "jobStatus": job_status
            }
        }

        return response_data


    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

def parse_info(info, key):
    try:
        # 尝试解析标准 JSON 格式
        json_data = json.loads(info)
        if key in json_data:
            return json_data[key]
    except json.JSONDecodeError:
        # 如果不是标准 JSON 格式，尝试逐行解析键值对
        lines = info.split('\n')
        for line in lines:
            if line.strip().startswith(f'"{key}"'):
                return line.split(':')[1].strip().strip('" ,')
    except Exception as e:
        print(f"Error parsing {key}: {e}")
    return None

def parse_job_status(job_info: str) -> str:
    """从 scontrol show job 输出中解析作业状态"""
    for line in job_info.splitlines():
        if "JobState=" in line:
            # 提取 JobState 字段的值
            return line.split("JobState=")[-1].split()[0]
    return "Unknown"
