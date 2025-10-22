from typing import Optional
from src.common.logger import logger
from src.common.config import get_config


def get_user_exp_group(group_name:str):
    mapping = {
        # 单键直接映射
        'alicpt': 'AliCPT',
        'cms': 'CMS',
        'dyw': 'DYW',
        'gecam': 'GECAM',
        'hxmt': 'HXMT',
        'lhcb': 'LHCB',
        'panda': 'Panda',
        'higgs': 'CEPC',
        'u07': 'CC',
        'comet': 'COMET',
        'csns': 'CSNS',
        'ucas': 'OTHERS',
        'heps': 'HEPS',
        # 多键映射同一值
        **{g: 'ATLAS' for g in ('atlas', 'combination')},
        **{g: 'BES' for g in ('dqarun', 'offlinerun', 'physics')},
        **{g: 'JUNO' for g in ('juno', 'dqmtest', 'dqmjuno', 'junospecial', 'junodc', 'junogns')},
        **{g: 'LHAASO' for g in ('lhaaso', 'lhaasorun')},
        **{g: 'HERD' for g in ('herd', 'herdrun')},
    }
    return mapping.get(group_name), group_name

def get_extra_job_config(username: str, groupname: str, job_type: str, request_os: Optional[str] = None):
    
    walltime = get_config("jobtype", job_type).get("htc").get("walltime")
    EXP_NAME = get_user_exp_group(groupname)[0]

    extra_job_config={
        # HTCondor default ClassAd, 
        # Below is the accounting format of IHEP
        # And site administrators can adjust it according to the configuration of their own sites
        "accounting_group" : f"{EXP_NAME}.{groupname}.{walltime}",
        # User Custom ClassAd are indicated with +
        "+HepJob_Experiment" : f"\"{EXP_NAME}\"",
        "+HepJob_JobType" : f"\"{job_type}\"",
        # IHEP controls job duration through default, mid, and long
        # Site administrators can configure them according to their own site
        "+HepJob_Walltime" : f"\"default\"",
        "+IHEP_RealGroup" : f"\"{groupname}\"",
    }

    if request_os:
        extra_job_config["+HepJob_RequestOS"] = f"\"{request_os}\""
    
    return extra_job_config




