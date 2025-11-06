import asyncio
from fastapi import HTTPException
from src.computing.common import *
from src.computing.gateway_tools import *
from src.computing.tools.resources_utils import *


async def get_hpc_system_jobs():
    command = [
        "sacct",
        "--format=JobID,Partition,JobName,User,State,Elapsed,NNodes,NodeList,AdminComment,Start,submit",
        "-P"
    ]
    process = await asyncio.create_subprocess_exec(
        *command,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE
    )

    try:
        stdout, stderr = await asyncio.wait_for(process.communicate(), timeout=2)
    except asyncio.TimeoutError:
        process.terminate()
        await process.wait()
        raise HTTPException(status_code=500, detail="Command execution timed out")

    if process.returncode != 0:
        raise HTTPException(status_code=500, detail=f"Failed to run sacct command: {stderr.decode()}")

    lines = stdout.decode().strip().split('\n')
    if not lines:
        raise HTTPException(status_code=500, detail="No output from sacct command")
    headers = lines[0].split('|')

    job_queueing_map = {"enode": 0, "jupyter": 0, "vscode": 0, "rootbrowse": 0, "vnc":0}
    job_running_map = {"enode": 0, "jupyter": 0, "vscode": 0, "rootbrowse": 0, "vnc":0}

    for line in lines[1:]:
        fields = line.split('|')
        job_data = dict(zip(headers, fields))
        if not job_data.get("JobID", "").isdigit():
            continue

        admin_comment = job_data.get("AdminComment", "").strip().lower()
            
        if admin_comment not in job_queueing_map.keys():
            admin_comment = "other"

        job_status = job_data.get("State", "").strip()
        if job_status == "PENDING":
            job_status = "QUEUEING"
        if admin_comment == "other":
            continue

        if job_status == "QUEUEING" and admin_comment in job_queueing_map:
            job_queueing_map[admin_comment] += 1
        elif job_status == "RUNNING" and admin_comment in job_running_map:
            job_running_map[admin_comment] += 1
    
    return job_queueing_map, job_running_map