from fastapi import APIRouter, HTTPException, Header, Query, Request
from src.computing.hpc import hpc_create_jobs
from src.computing.htc import htc_create_jobs
from src.computing.hpc import hpc_check_job
from src.computing.htc import htc_check_job
from src.computing.hpc import hpc_delete_job
from src.computing.htc import htc_delete_job
from src.computing.hpc import hpc_job_details
from src.computing.htc import htc_job_details
from src.computing.hpc import hpc_system_info
from src.computing.htc import htc_system_info
from src.computing.hpc import hpc_query_jobs
from src.computing.htc import htc_query_jobs
from src.computing.hpc import hpc_get_user_info
from src.computing.htc import htc_system_jobs
from src.computing.hpc import hpc_system_jobs

from src.computing.tools.resources_utils import *
from src.apps.statistic.job_statistic import *
from fastapi.responses import HTMLResponse
from src.inkdb.inkredis import *

router = APIRouter()

@router.get("/get-job-output")
async def check_common_job(
    jobId: int = Query(..., description="作业ID"),
    uid: int = Header(..., description="用户的Slurm UID"),
    email: str = Header(..., description="用户邮箱"),
    cluster_id: str = Query(..., description="集群ID，默认为所有集群")
):
    try:
        if cluster_id == "slurm":
            output_content, err_content = await hpc_check_job.get_job_output(uid=uid, job_id=jobId, cluster_id=cluster_id)
        else:
            output_content, err_content = await htc_check_job.get_job_output(uid=uid, job_id=jobId, clusterid=cluster_id)

        return {
            "status": "200",
            "msg": "请求成功",
            "data": {
                "job_id": jobId,
                "output": output_content,
                "error": err_content
            }
        }

    except Exception as e:
        logger.error(f"Get job output error, username={uid}, job_id={jobId}, cluster_id={cluster_id}")
        return {
            "status": "500",
            "msg": f"Get job output error: {e}",
            "data": ""
        }
    
    
    

@router.get("/connect-job")
async def connect_common_job(    
    jobId: int = Query(..., description="作业ID"),
    uid: int = Header(..., description="用户的Slurm UID"),
    email: str = Header(..., description="用户邮箱"),
    job_type: str = Query(..., description="作业类型"),
    cluster_id: str = Query(..., description="集群ID, 默认为所有集群")
    ):
    try:
        
        if job_type == "jupyter":
            host, port, jupyter_token, jupyter_url = await connect_jupyter_job(job_id=jobId, uid=uid, clusterid=cluster_id)
            return {
                "status": 200,
                "msg": "请求成功",
                "data": {
                    "host": host,
                    "port": port,
                    "token": jupyter_token,
                    "url": jupyter_url,
                    "jobId": jobId
                }
            }
        
        elif job_type == "enode":
            gateway_node, gateway_port = await connect_sshd(job_id=jobId, uid=uid, clusterid=cluster_id)
            return {
                "status": 200,
                "msg": "请求成功",
                "data": {
                    "host": gateway_node,
                    "gateway_port": gateway_port,
                    "jobId": jobId
                }
            }
        
        elif job_type == "rootbrowse":
            host, port, root_token, rootbrowse_url = await connect_rootbrowse_job(job_id=jobId, uid=uid, clusterid=cluster_id)
            return {
                "status": 200,
                "msg": "请求成功",
                "data": {
                    "host": host,
                    "port": port,
                    "token": root_token,
                    "url": rootbrowse_url,
                    "jobId": jobId
                }
            }
        
        elif job_type == "vscode":
            hostname, port, passwd = await connect_vscode_job(job_id=jobId, uid=uid, clusterid=cluster_id)
            return {
                "status": 200,
                "msg": "请求成功",
                "data": {
                    "host": hostname,
                    "port": port,
                    "passwd": passwd,
                    "jobId": jobId
                }
            }
        
        elif job_type == "vnc":
            host, port, vnc_url = await connect_vnc_job(job_id=jobId, uid=uid, clusterid=cluster_id)
            return {
                "status": 200,
                "msg": "请求成功",
                "data": {
                    "host": host,
                    "port": port,
                    "url": vnc_url,
                    "jobId": jobId
                }
            }
        
        elif job_type == "ink_special":
            host, port, vnc_url = await connect_vnc_job(job_id=jobId, uid=uid, clusterid=cluster_id)
            return {
                "status": 200,
                "msg": "请求成功",
                "data": {
                    "host": host,
                    "port": port,
                    "url": vnc_url,
                    "jobId": jobId
                }
            }
                
        else:
            return {
                "status": "500",
                "msg": f"Connect job failed with wrong jobtype: {job_type}",
                "data": ""
            } 
            
        
    except Exception as e:
        logger.error(f"Get job output error, username={uid}, job_id={jobId}, cluster_id={cluster_id}")
        return {
            "status": "500",
            "msg": f"Get job output error: {e}",
            "data": ""
        }


@router.post("/create-job")
async def create_common_job(
    req: Request,
    job_type: str = Query(..., description="作业类型"),
    uid: int = Header(..., description="用户的Slurm UID"),
    email: str = Header(..., description="用户邮箱"),
    cluster_id : str = Query(...,description="集群类型")
):
    try: 
        if cluster_id == "slurm":
            data = await req.json()
            job_request =  hpc_create_jobs.JobCreateRequest(**data)
            job_id, job_type, job_path = await hpc_create_jobs.create_job(request=job_request, job_type=job_type, uid=uid, cluster_id=cluster_id)
        elif cluster_id == "htcondor":
            data = await req.json()
            job_request =  htc_create_jobs.HTC_JOB(**data)
            job_id, job_type, job_path = await htc_create_jobs.create_htc_job(request=job_request, job_type=job_type, uid=uid, clusterid=cluster_id)
            logger.info(f"Create job finished, the jobid: {job_id}, jobpath: {job_path}, cluster: {cluster_id}")
        else:
            return {
                "status": "500",
                "msg": f"Create job failed with wrong cluster: {cluster_id}.",
                "data": ""
            } 
        
        return {
            "status": "200",
            "msg": f"Create job successfully, jobid: {job_id}, cluster: {cluster_id}.",
            "data": {
                "jobId": job_id,
                "jobType": job_type,
                "jobPath": job_path
            }
        }
    
    except Exception as e:
        logger.error(f"Create job failed, username: {uid}, cluster_id: {cluster_id}, details: {e}.")
        return {
            "status": "500",
            "msg": f"Create job failed: {e}",
            "data": ""
        }


@router.delete("/delete-job")
async def delete_common_job(
    jobId: str = Query(..., description="作业ID"),
    uid: int = Header(..., description="用户的Slurm UID"),
    email: str = Header(..., description="用户邮箱"),
    cluster_id: str = Query(..., description="集群类型")
):
    try:
        if cluster_id == "slurm":
            await hpc_delete_job.delete_job(jobId=jobId, uid=uid)
        elif cluster_id == "htcondor":
            await htc_delete_job.delete_htc_job(jobId, uid=uid)
        else:
            return {
                "status": "500",
                "msg": "Delete job failed with wrong cluster_id",
                "data": ""
            }
        
        return {
            "status": "200",
            "msg": f"Delete job {jobId} successfully.",
            "data": ""
        }
        
    except Exception as e:
        logger.error(f"Delete job failed, username: {uid}, cluster_id: {cluster_id}, details: {e}.")
        return {
            "status": "500",
            "msg": f"Delete job failed: {e}.",
            "data": ""
        }


@router.get("/job-details")
async def get_common_job_details(
    jobId: str = Query(..., description="作业ID"),
    uid: int = Header(..., description="用户的Slurm UID"),
    email: str = Header(..., description="用户邮箱"),
    cluster_id: str = Query(..., description="集群ID，默认为所有集群")
):
    try:
        if cluster_id == "slurm":
            jobStartTime, jobStatus, jobNodeList, jobSubmitTime, jobType, connect_sign = await hpc_job_details.get_user_jobs(uid, jobId, cluster_id)
            response_data = {
                "status": 200,
                "msg": "请求成功",
                "data": {
                    "clusterId": "slurm",
                    "jobId": int(jobId),
                    "jobStartTime": jobStartTime,
                    "jobStatus": jobStatus,
                    "jobNodeList": jobNodeList,
                    "jobSubmitTime":jobSubmitTime,
                    "jobType": jobType,
                    "connect_sign": connect_sign
                }
            }
            
        elif cluster_id == "htcondor":
            response_data = await htc_job_details.get_user_job_details(uid, jobId, cluster_id)
        else:
            return {
                "status": "500",
                "msg": "Get job details failed with wrong cluster_id.",
                "data": ""
            }
            
        return response_data
        
    except Exception as e:
        logger.error(f"Get job details failed, username: {uid}, cluster_id: {cluster_id}, details: {e}.")
        return {
            "status": "500",
            "msg": f"Get job details failed: {e}",
            "data": ""
        }


@router.get("/system-info")
async def get_common_system_info(
    uid: int = Header(..., description="用户的Slurm UID"),
    email: str = Header(..., description="用户邮箱"),
    cluster_id: str = Query(..., description="集群ID，默认为所有集群")
):
    if cluster_id == "slurm":
        result = await hpc_system_info.get_system_info(uid=uid)
        return result

    elif cluster_id == "htcondor":
        result = await htc_system_info.get_system_info()
        return result

    else:
        raise HTTPException(status_code=400, detail="无效的集群类型")


@router.get("/user-info")
async def get_common_user_info(
        uid: int = Header(..., description="用户的Slurm UID"),
        email: str = Header(..., description="用户邮箱"),
        cluster_id: str = Query(..., description="集群ID，默认为所有集群")
):
    
    result = await hpc_get_user_info.get_user_info(uid=uid, email=email)
    return result


@router.get("/query-job")
async def query_common_job(
    uid: int = Header(..., description="用户的Slurm UID"),
    email: str = Header(..., description="用户邮箱"),
    cluster_id: str = Query(..., description="集群ID，默认为所有集群"),
    page: int = Query(..., description="页码"),
    job_type: str = Query(None, description="作业类型"),
    limit: int = Query(..., description="每页数量")
):
    try: 
        if cluster_id == "slurm":
            joblist = await hpc_query_jobs.get_user_jobs(uid=uid, job_type=job_type, cluster_id=cluster_id)
        
        elif cluster_id == "htcondor":
            joblist = await htc_query_jobs.get_user_jobs(uid=uid, job_type=job_type, clusterid=cluster_id)
        
        elif cluster_id == "all":
            try:
                slurm_joblist = await hpc_query_jobs.get_user_jobs(uid=uid, job_type=job_type, cluster_id="slurm")
            except:
                return {
                    "status": "500",
                    "msg": f"Query user {uid} slurm jobs failed.",
                    "data": ""
                }
            
            try: 
                htcondor_joblist = await htc_query_jobs.get_user_jobs(uid=uid, job_type=job_type, clusterid="htcondor")
            except:
                return {
                    "status": "500",
                    "msg": f"Query user {uid} htc jobs failed.",
                    "data": ""
                } 
            
            joblist = slurm_joblist + htcondor_joblist

        else:
            return {
                "status": "500",
                "msg": f"Get user {uid} jobs failed with wrong cluster_id param.",
                "data": ""
            }
        
        # 分页处理
        start_idx = (page - 1) * limit
        end_idx = start_idx + limit
        paginated_jobs = joblist[start_idx:end_idx]
        
        logger.info(f"Get user {uid} jobs: {paginated_jobs}")

        return {
            "status": "200",
            "msg": f"Query user {uid} jobs successfully.",
            "data": paginated_jobs
        }

    except Exception as e:
        logger.error(f"Get user jobs failed, username: {uid}, cluster_id: {cluster_id}, details: {e}.")
        return {
            "status": "500",
            "msg": f"Get user {uid} jobs failed: {e}.",
            "data": ""
        }


@router.get("/system-jobs")
async def query_system_jobs():
    try: 
        r = redis_connect()
        job_total_list = r.get('system_job_list')

        if job_total_list:
            job_total_list = json.loads(job_total_list)
            formatted_job = {key.capitalize(): value for key, value in job_total_list.items()}
            return {
                "status": "200",
                "msg": "请求成功",
                "job_total_list": formatted_job
            }
        else:
            job_total_list = {"enode": 0, "jupyter": 0, "vscode": 0, "rootbrowse": 0, "vnc": 0}
            htc_queueing_list, htc_running_list = await htc_system_jobs.get_system_jobs()   
            for key in job_total_list:
                job_total_list[key] = htc_queueing_list[key] + htc_running_list[key]
                r.set('system_job_list', json.dumps(job_total_list))
                r.expire('system_job_list', 300)
                
            formatted_job = {key.capitalize(): value for key, value in job_total_list.items()}    
        
            return {
                "status": "200",
                "msg": "请求成功",    
                "job_total_list": formatted_job
            }
            
    except Exception as e:
        logger.error(f"Get system job statistic info failed and access, details: {e}.")
        return {
            "status": "500",
            "msg": f"Get system job statistic info failed: {e}.",
            "data": ""
        }

@router.get("/omat-daily-jobs")
async def get_omat_jobs_info(
    uid: int = Header(..., description="用户的Slurm UID"),
    email: str = Header(..., description="用户邮箱")
):
    job_list = await ink_omat_table()

    return {
        "status": 200,
        "msg": "请求成功",
        "data": job_list
    }
    

@router.get("/omat-stack-jobs")
async def get_omat_stack_jobs_info(
    uid: int = Header(..., description="用户的Slurm UID"),
    email: str = Header(..., description="用户邮箱")
):
    
    r = redis_connect()
    jupyter_list = r.get('jupyter_list')
    vscode_list = r.get('vscode_list')
    vnc_list = r.get('vnc_list')
    rootbrowse_list = r.get('rootbrowse_list')
    enode_list = r.get('enode_list')         
    gpu_list = r.get('gpu_list')
    
    if jupyter_list and vscode_list and vnc_list and rootbrowse_list and enode_list and gpu_list:
        jupyter_list = json.loads(jupyter_list)
        vscode_list = json.loads(vscode_list)
        vnc_list = json.loads(vnc_list)
        rootbrowse_list = json.loads(rootbrowse_list)
        enode_list = json.loads(enode_list)
        gpu_list = json.loads(gpu_list)
        
        return {
            "status": 200,
            "msg": "请求成功",
            "data":{
                "Jupyter": jupyter_list,
                "Vscode": vscode_list,
                "Vnc": vnc_list,
                "Rootbrowse": rootbrowse_list,
                "Enode": enode_list,
                "GPU": gpu_list
            }
        }
    else:
        try:
            # 将多个协程合并为一个任务组
            async def fetch_all():
                return await asyncio.gather(
                    ink_omat_stack_table("jupyter"),
                    ink_omat_stack_table("vscode"),
                    ink_omat_stack_table("vnc"),
                    ink_omat_stack_table("rootbrowse"),
                    ink_omat_stack_table("enode"),
                    omat_gpu_jobs()
                )

            # 设置总超时
            results = await asyncio.wait_for(fetch_all(), timeout=20)
            jupyter_list, vscode_list, vnc_list, rootbrowse_list, enode_list, gpu_list = results
            
            first_key = next(iter(jupyter_list))
            del jupyter_list[first_key]
            
            first_key = next(iter(vscode_list))
            del vscode_list[first_key]
            
            first_key = next(iter(rootbrowse_list))
            del rootbrowse_list[first_key]
            
            first_key = next(iter(enode_list))
            del enode_list[first_key]
        
            first_key = next(iter(jupyter_list))
            del gpu_list[first_key]
                        
            r.set('jupyter_list', json.dumps(jupyter_list))
            r.expire('jupyter_list', 1800)
            
            r.set('vscode_list', json.dumps(vscode_list))
            r.expire('vscode_list', 1800)
            
            r.set('vnc_list', json.dumps(vnc_list))
            r.expire('vnc_list', 1800)
            
            r.set('rootbrowse_list', json.dumps(rootbrowse_list))
            r.expire('rootbrowse_list', 1800)
            
            r.set('enode_list', json.dumps(enode_list))
            r.expire('enode_list', 1800)
            
            r.set('gpu_list', json.dumps(gpu_list))
            r.expire('gpu_list', 1800)
            
            
            return {
                "status": 200,
                "msg": "请求成功",
                "data":{
                    "Jupyter": jupyter_list,
                    "Vscode": vscode_list,
                    "Vnc": vnc_list,
                    "Rootbrowse": rootbrowse_list,
                    "Enode": enode_list,
                    "GPU": gpu_list
                }
            }
            
        except asyncio.TimeoutError:
            return {
                "status": 500,
                "msg": "请求超时",
                "data":{
                    "Jupyter": "",
                    "Vscode": "",
                    "Vnc": "",
                    "Rootbrowse": "",
                    "Enode": "",
                    "GPU": ""
                }
            }

    
    

    
    
    
    
    
    
    
    
    
    
