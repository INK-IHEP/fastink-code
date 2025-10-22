import os
import re
import subprocess
from fastapi import HTTPException, Header
from collections import deque
from src.computing.tools.resources_utils import sub_command
from src.common.logger import logger


async def get_system_info(uid: int):
    
    # set the common user id specified by uid for subprocess
    try:
        os.setuid(uid)
    except PermissionError as e:
        raise HTTPException(status_code=403, detail=f"Permission denied for UID {uid}: {str(e)}")
    try:
        # get total number of resources of the cluster 
        # including cpu cores, memory, gpu cards, dcu cards and npu cards
        total_resources = await get_total_resources()
        return total_resources["total_cpu_cores"], total_resources["total_memory"], total_resources["total_gpu_cards"], total_resources["total_dcu_cards"],total_resources["total_npu_cards"]
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# helper function of get_system_info
async def get_total_resources():
    # Counter variables for different resource types
    total_cpu_cores = 0
    total_memory = 0     # Unit : MB
    total_gpu_cards = 0
    total_dcu_cards = 0
    total_npu_cards = 0

    # scontrol is used to get node information
    command = "scontrol show node"
    result = await sub_command(command, timeoutsec=2, errinfo="scontrol err", tminfo="scontrol timeout")
    
    # parse node information
    nodes = result.decode().split("\n\n")  # delimiter(a blank line) between each node
    for node in nodes:
        # Only count nodes in dcu or gpu or npu partitions
        partition_match = re.search(r'Partitions=(\S+)', node)
        if partition_match and partition_match.group(1) not in ["dcu", "gpu", "npu"]:
            continue
        if 'CPUTot=' in node:
            total_cpu_cores += int(re.search(r'CPUTot=(\d+)', node).group(1))
        if 'RealMemory=' in node:
            total_memory += int(re.search(r'RealMemory=(\d+)', node).group(1))
        if 'Gres=gpu' in node:
            gpu_match = re.search(r'gpu:\w+:(\d+)', node)
            if gpu_match:
                total_gpu_cards += int(gpu_match.group(1))
        if 'Gres=dcu' in node:
            dcu_match = re.search(r'dcu:\w+:(\d+)', node)
            if dcu_match:
                total_dcu_cards += int(dcu_match.group(1))
        if 'Gres=npu' in node:
            npu_match = re.search(r'npu:\w+:(\d+)', node)
            if npu_match:
                total_npu_cards += int(npu_match.group(1))
    return {
        "total_cpu_cores": total_cpu_cores,
        "total_memory": int(total_memory / 1024) ,  # Unit : GB
        "total_gpu_cards": total_gpu_cards,
        "total_dcu_cards": total_dcu_cards,
        "total_npu_cards": total_npu_cards
    }
    
# Search resources not depends on "Partitions=" but "CfgTRES="
# so that all worker nodes can be counted.
# Please make sure that scontrold and login nodes not configured in slurm.conf.
async def get_total_resources_by_cfg_tres():
    # Counter variables for different resource types
    total_cpu_cores = 0
    total_memory = 0     # Unit : GB
    total_gpu_cards = 0
    total_dcu_cards = 0
    total_npu_cards = 0

    # scontrol is used to get node information
    command = "scontrol show node"
    result = await sub_command(command, timeoutsec=2, errinfo="scontrol err", tminfo="scontrol timeout")

    # parse node information
    nodes = result.split("\n\n")  # delimiter(a blank line) between each node
    for node in nodes:
        cfg_tres = re.search(r'CfgTRES=cpu=(\d+),mem=(\d+)G,billings=(\d+)(.*))', node)
        total_cpu_cores += int(cfg_tres.group(1))
        total_memory += int(cfg_tres.group(2))
        cards = cfg_tres.group(4)
        if cards is not None:
            # cards example : ",gres/gpu=1,gres/gpu:v100=1"
            skip_gpu, skip_dcu, skip_npu = False, False, False
            cards = cards.split(",")
            for card in cards:
                if "gpu" in card and not skip_gpu:
                    total_gpu_cards += int(card.split("=")[1])
                    skip_gpu = True
                elif "dcu" in card and not skip_dcu:
                    total_dcu_cards += int(card.split(":")[1])
                    skip_dcu = True
                elif "npu" in card and not skip_npu:
                    total_npu_cards += int(card.split(":")[1])
                    skip_npu = True
    
    return {
        "total_cpu_cores": total_cpu_cores, 
        "total_memory": total_memory, 
        "total_gpu_cards": total_gpu_cards, 
        "total_dcu_cards": total_dcu_cards, 
        "total_npu_cards": total_npu_cards
    }
    
# Slurm cluster generate statistics of the cluster resources periodically(10 minutes for now)
# The result is stored in a file
# file_path should be saved in the config file
# line_num should be saved in the config file if not equal to 4
def get_total_resources_from_file(file_path, line_num=4):
    # Counter variables for different resource types
    total_cpu_cores = 0
    total_memory = 0     # Unit : MB
    total_gpu_cards = 0
    total_dcu_cards = 0
    total_npu_cards = 0
    
    # read last 4 lines : 4 possible types of computing resources including cpu, gpu, dcu and npu
    # example:
    # 1750750801          all_cpu             132       0         452608              132       0         452608
    # 1750750801          all_gpu             16        1         112640              16        1         112640
    computing_resources_type = []
    last_lines = None
    with open(file_path, 'r') as file:
        last_lines = deque(file, line_num)
        for line in last_lines:
            if "all_cpu" in line:
                computing_resources_type.append('cpu')
            if "all_gpu" in line:
                computing_resources_type.append('gpu')
            if "all_dcu" in line:
                computing_resources_type.append('dcu')
            if "all_npu" in line:
                computing_resources_type.append('npu')
    
    for line in last_lines[-len(computing_resources_type):]:
        total_cpu_cores += int(line.split()[2].strip())
        if 'gpu' in computing_resources_type:
            total_gpu_cards += int(line.split()[3].strip())
        if 'dcu' in computing_resources_type:
            if 'gpu' in computing_resources_type:
                total_dcu_cards += int(line.split()[4].strip())
            else:
                total_dcu_cards += int(line.split()[3].strip())
        if 'npu' in computing_resources_type:
            if 'gpu' in computing_resources_type and 'dcu' in computing_resources_type:
                total_npu_cards += int(line.split()[5].strip())
            elif 'gpu' not in computing_resources_type or 'dcu' not in computing_resources_type:
                total_npu_cards += int(line.split()[4].strip())
            elif 'gpu' not in computing_resources_type and 'dcu' not in computing_resources_type:
                total_npu_cards += int(line.split()[3].strip())
        total_memory += int(line.split()[2 + len(computing_resources_type)].strip())
    
    return {
        "total_cpu_cores": total_cpu_cores, 
        "total_memory": int(total_memory / 1024), # Unit : GB
        "total_gpu_cards": total_gpu_cards, 
        "total_dcu_cards": total_dcu_cards, 
        "total_npu_cards": total_npu_cards
    }