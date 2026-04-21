import pwd, grp
from typing import Optional
from abc import ABC, abstractmethod
from fastink.common.config import get_config
from fastink.computing.cluster.cluster import Base_JOB, SubmitMode
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

    
    # =========================
    # Unified submit entrypoint (new)
    # =========================
    async def submit_job(self, job_data: Base_JOB) -> dict:
        """
        Unified job submission entrypoint.

        - sync  : submit job directly and return job_id
        - async : enqueue job into redis/mq and return immediately
        """
        submit_mode = getattr(job_data, "submit_mode", "async")

        if submit_mode is SubmitMode.SYNC:
            return await self.submit_job_sync(job_data)

        elif submit_mode is SubmitMode.ASYNC:
            return await self.submit_job_async(job_data)
            
        else:
            raise ValueError(f"Unsupported submit_mode: {submit_mode}")


    # =========================
    # Synchronous submission (must be implemented)
    # =========================
    @abstractmethod
    async def submit_job_sync(self, job_data: Base_JOB) -> dict:
        """
        Submit job synchronously.

        - Interact directly with Slurm / HTCondor
        - Return the real scheduler job_id
        """
        raise NotImplementedError


    # =========================
    # Asynchronous submission (must be implemented)
    # =========================
    @abstractmethod
    async def submit_job_async(self, job_data: Base_JOB) -> dict:
        """
        Submit job asynchronously.

        - Only enqueue job into redis / message queue
        - Do NOT return job_id
        """
        raise NotImplementedError

    # =========================
    # Other existing capabilities (unchanged)
    # =========================
    @abstractmethod
    async def query_job(self, job_type: Optional[str] = None) -> dict:
        """Query job status"""
        raise NotImplementedError


    @abstractmethod
    async def cancel_job(
        self,
        *,
        job_id: Optional[str] = None,
        submit_uuid: Optional[str] = None,
    ) -> dict:
        """
        Cancel a job.

        Supports:
        - Async jobs identified by submit_uuid
        - Sync jobs identified by job_id
        Returns a dict containing:
            - cluster
            - submit_uuid
            - job_id
            - job_status
        """
        raise NotImplementedError






        
            
