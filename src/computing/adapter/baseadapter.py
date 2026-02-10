import pwd, grp
from typing import Optional
from abc import ABC, abstractmethod
from src.common.config import get_config
from src.computing.cluster.cluster import Base_JOB, SubmitMode
from src.computing.tools.resources_utils import change_uid_to_username

class SchedulerBase(ABC):
    def __init__(self, uid: int):
        super().__init__()
        self.UID = uid
        self.USERNAME = change_uid_to_username(self.UID)
        self.GID = pwd.getpwuid(uid).pw_gid
        self.GROUPNAME = grp.getgrgid(self.GID).gr_name
        self.XROOTD_PATH = get_config("computing", "xrootd_path")
        self.KRB5_ENABLED = get_config("common", "krb5_enabled")

    
    def _need_dedup(self, job_data: Base_JOB) -> bool:
        if job_data.submit_mode is SubmitMode.SYNC:
            return False

        # async mode
        interactive_job_types = get_config("computing", "iptables_jobtype")
        return job_data.job_type in interactive_job_types
    
    
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
    # Worker interface (must be implemented)
    # =========================
    @abstractmethod
    async def submit_job_from_queue(self, job_dict: dict) -> dict:
        """
        Called by crond / worker.

        - Fetch job data from redis
        - Submit job to the real scheduler
        - Return job_id for further recording
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
        submit_uuid: Optional[str] = None,
        job_id: Optional[str] = None,
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






        
            
