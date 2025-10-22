import os
import re
import subprocess
from fastapi import HTTPException, Header
from src.computing.tools.resources_utils import sub_command

async def get_system_info(uid):
    try:
        # set the common user id specified by uid for subprocess
        try:
            os.setuid(uid)
        except PermissionError as e:
            raise HTTPException(status_code=403, detail=f"Permission denied for UID {uid}: {str(e)}")

        # get total number of resources of the cluster 
        # including cpu cores, memory, gpu cards, dcu cards and npu cards
        total_resources = await get_total_resources()

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


async def get_total_resources():
    # 初始化资源计数器
    total_cpu_cores = 0
    total_memory = 0  # 单位为 MB
    total_gpu_cards = 0
    total_dcu_cards = 0

    # 执行 scontrol 命令以获取节点信息
    command = "scontrol show node"
    result = await sub_command(command, timeoutsec=2, errinfo="scontrol err", tminfo="scontrol timeout")
    # result = subprocess.run(command, shell=True, capture_output=True, text=True)
    # if result.returncode != 0:
    #     raise Exception(f"Failed to retrieve node information: {result.stderr}")

    # 解析节点信息
    node_info = result
    # node_info = result.stdout
    nodes = node_info.split("\n\n")  # 每个节点信息之间有一个空行
    for node in nodes:
        # 只计算属于 DCU 或 GPU 分区的节点
        partition_match = re.search(r'Partitions=(\S+)', node)
        if partition_match and partition_match.group(1) not in ["dcu", "gpu"]:
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

    return {
        "total_cpu_cores": total_cpu_cores,
        "total_memory": total_memory,  # 单位为 MB
        "total_gpu_cards": total_gpu_cards,
        "total_dcu_cards": total_dcu_cards
    }
