import asyncio
from fastapi import HTTPException
from fastink.computing.hpc.v2.hpc_job_types import (
    get_returnable_hpc_job_types,
    get_sacct_field,
    normalize_hpc_job_type,
)


async def get_hpc_system_jobs():
    command = [
        "sacct",
        "--format=JobID,Partition,JobName,User,State,Elapsed,NNodes,NodeList,WCKey,AdminComment,Start,Submit",
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

    returnable_job_types = get_returnable_hpc_job_types()
    job_queueing_map = {job_type: 0 for job_type in sorted(returnable_job_types)}
    job_running_map = {job_type: 0 for job_type in sorted(returnable_job_types)}

    for line in lines[1:]:
        fields = line.split('|')
        job_data = dict(zip(headers, fields))
        if not job_data.get("JobID", "").isdigit():
            continue

        ink_job_type = normalize_hpc_job_type(
            get_sacct_field(job_data, "WCKey", "WCkey", "WCKEY"),
            get_sacct_field(job_data, "AdminComment"),
            returnable_job_types=returnable_job_types,
        )
        if not ink_job_type:
            continue

        job_status = job_data.get("State", "").strip()
        if job_status == "PENDING":
            job_status = "QUEUEING"

        if job_status == "QUEUEING" and ink_job_type in job_queueing_map:
            job_queueing_map[ink_job_type] += 1
        elif job_status == "RUNNING" and ink_job_type in job_running_map:
            job_running_map[ink_job_type] += 1
    
    return job_queueing_map, job_running_map
