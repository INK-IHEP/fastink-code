import pwd, grp
from typing import Optional
from abc import ABC, abstractmethod
from fastink.common.config import get_config
from fastink.computing.cluster.cluster import Base_JOB
from fastink.computing.tools.common.utils import change_uid_to_username

class SchedulerBase(ABC):
    def __init__(self, uid: int):
        super().__init__()
        self.UID = uid
        self.USERNAME = change_uid_to_username(self.UID)
        self.GID = pwd.getpwuid(uid).pw_gid
        self.GROUPNAME = grp.getgrgid(self.GID).gr_name
        self.XROOTD_PATH = get_config("computing", "xrootd_path")
        self.KRB5_ENABLED = get_config("common", "krb5_enabled")
    

    @abstractmethod
    async def submit_job(self, job_data: Base_JOB) -> str:
        """提交作业并返回作业ID"""
    

    @abstractmethod
    async def query_job(self, job_type: Optional[str] = None) -> dict:
        """查询作业状态"""
    

    @abstractmethod
    async def cancel_job(self, job_id: str) -> bool:
        """取消作业"""

    







        
            
