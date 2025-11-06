import os, base64, pwd, grp
from src.storage import common
from src.auth.krb5 import get_krb5
from abc import ABC, abstractmethod
from src.common.logger import logger
from src.common.config import get_config
from src.computing.cluster.cluster import Base_JOB
from src.computing.tools.resources_utils import get_user_exp_group, change_uid_to_username

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
    async def query_job(self, job_id: str) -> dict:
        """查询作业状态"""
    

    @abstractmethod
    async def cancel_job(self, job_id: str) -> bool:
        """取消作业"""


    def _generate_token_file(self) -> str:
    
        token_filename = ""
        krb5_decoded_bytes = ""
        
        if self.KRB5_ENABLED:
            token = get_krb5(self.USERNAME)
            if token != "":  
                krb5_decoded_bytes = base64.b64decode(token)
                logger.info(f"Generate user:{self.USERNAME} KRB5 token successfully.")
            else:
                raise Exception("Generate user KRB5 token failed.")
        
            token_filename = f"/tmp/krb5cc_{self.UID}"
            if not os.path.exists(token_filename):
                with open(token_filename, 'wb') as file:
                    file.write(krb5_decoded_bytes)

        return token_filename, krb5_decoded_bytes
    

    async def _init_job_dir(self, job_type: str, time_stamp: str, krb5_decoded_bytes: str):
        
        user_home_dir = os.path.expanduser(f'~{self.USERNAME}')
    
        if user_home_dir.startswith("/afs/"):
            _, USERGROUP = get_user_exp_group(self.UID)
            ink_dir = get_config("computing", "ink_dir")
            ink_dir = ink_dir.format(user_group=USERGROUP, username=self.USERNAME)
            job_dir = f"{ink_dir}/.ink/Jobs/{job_type}-{time_stamp}"
        else:
            job_dir = f"{user_home_dir}/.ink/Jobs/{job_type}-{time_stamp}"
        

        logger.info(f"User job_dir: {job_dir}")
        is_exist, _ = await common.path_exist(name=job_dir, username=self.USERNAME, mgm=self.XROOTD_PATH)
        if not is_exist:
            await common.mkdir(dname=job_dir, username=self.USERNAME, mode="700", exist_ok=False, mgm=self.XROOTD_PATH)

        if self.KRB5_ENABLED:
            await common.upload_file(src_data=krb5_decoded_bytes, dst=f"{job_dir}/krb5cc_{self.UID}", username=self.USERNAME, mgm=self.XROOTD_PATH, mode="600")
        
        return job_dir





        
            
