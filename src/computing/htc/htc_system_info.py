import subprocess
from src.computing.tools.resources_utils import sub_command
from src.inkdb.inkredis import *
from fastapi import HTTPException
from src.common.config import get_config

'''
Author:         guocq@ihep.ac.cn
Created:        2024-12-18
Last Modified:  2024-12-18
'''

async def get_system_info():
    try:
        
        CM_HOST = get_config("computing", "cm_host")

        total_cpu_cores = 0
        total_memory = 0 

        r = redis_connect()
        r_cpu = r.get('htc_cpu')
        r_mem = r.get('htc_mem')

        if r_cpu and r_mem:
            total_cpu_cores = r_cpu
            total_memory = r_mem
            return {
                "total_cpu_cores": total_cpu_cores,
                "total_memory_gb": total_memory  # 单位为 MB
            }
        
        command = (
                f"condor_status -pool {CM_HOST} -format \"%d \" Cpus "
                "-format \"%d\\n\" Memory | awk '{{totalCpus += $1; totalMemory +=$2}} "
                "END {{print totalCpus \" \" totalMemory}}'")
        
        result = await sub_command(command, timeoutsec=2, errinfo="condor_status err", tminfo="condor_status timeout")
        # result = subprocess.run(command, shell=True, capture_output=True, text=True)

        # if result.returncode != 0:
        #     raise Exception(f"Failed to retrieve node information: {result.stderr}")

        total_cpu, total_mem = map(int, result.strip().split(' '))
        if total_cpu == 0 and total_mem == 0:
            raise Exception("Failed to retrieve valid node information.")
        
        total_cpu_cores = total_cpu
        total_memory_gb = round(total_mem / 1024, 2)

        r.set('htc_cpu', total_cpu)
        r.set('htc_mem', total_memory_gb)
        r.expire('htc_cpu', 43200) 
        r.expire('htc_mem', 43200)


        return total_cpu_cores, total_memory_gb

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

