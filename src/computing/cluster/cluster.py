from pydantic import BaseModel, Field
from typing import Optional, Literal


class Base_JOB(BaseModel):
    job_script: Optional[str] = Field(None, description="Job Script Content")
    job_parameters: Optional[str] = Field(None, description="User job parameters")
    cpu: Optional[int] = Field(1, gt=0, description="CPU requirement")
    mem: Optional[int] = Field(..., gt=0, description="Memory requirement (MB)")
    gpu_num: Optional[int] = Field(0, ge=0, description="GPU requirement")
    job_name: str = Field(f"", description="Job name")
    job_type: str = Field(f"", description="Job type")
    cluster_id: Literal["slurm", "htcondor"] 


class SLURM_JOB(Base_JOB):
    time: Optional[str] = Field(None, description="作业运行时间上限 (格式: HH:MM:SS)")  # Optional
    partition: str = Field(..., description="作业分区")  # Mandatory
    nodes: Optional[str] = Field(None, description="申请的节点数")  # Optional
    ntasks: Optional[str] = Field(None, description="使用的CPU核数")  # Optional
    account: str = Field(..., description="组名称")  # Mandatory
    qos: str = Field(..., description="服务质量 (QoS)")  # Mandatory
    gpu_name: Optional[str] = Field(None, description="GPU or DCU")  # Optional
    gpu_type: Optional[str] = Field(None, description="GPU 类型")  # Optional
    output_file: Optional[str] = Field(None, description="output_file name")  # Optional
    error_file: Optional[str] = Field(None, description="error_file name")  # Optional
    script_path: Optional[str] = Field(None, description="absolute path to the job script") # Optional
    input_path: Optional[str] = Field(None, description="Absolute path to the input file") # Optional
    cluster_id: Literal["slurm"] = "slurm"



class HTC_JOB(Base_JOB):
    os: Optional[str] = Field(None, description="操作系统镜像") # 可选
    wn: str = Field(f"", description="woker node host")  # 可选
    arch: str = Field(f"", description="woker node arch")  # 可选
    schedd: Optional[str] = Field(None, description="Schedd Host") # 可选
    cm: Optional[str] = Field(None, description="CM Host") # 可选
    cluster_id: Literal["htcondor"] = "htcondor"
