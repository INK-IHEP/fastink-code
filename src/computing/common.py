from sqlalchemy.exc import NoResultFound
from sqlalchemy.orm import Session
from sqlalchemy.sql.expression import update, select

from src.database.sqla import models
from src.database.sqla.session import read_session, transactional_session
from src.common.logger import logger

from pathlib import Path, PurePath
from typing import Optional, Tuple

@transactional_session
def insert_job_info(
    uid: int,
    jobid: int,
    outpath: str,
    errpath: str,
    job_type: str,
    job_path: str,
    clusterid: int,
    *,
    session: Session
):
    try:
        job_info = models.JobInfo(
            uid = uid,
            jobid = jobid,
            outpath = outpath,
            errpath = errpath,
            job_type = job_type,
            job_path = job_path,
            clusterid = clusterid
        )
        job_info.save(session=session, flush=True)
        
    except Exception as e:
        raise Exception(f"Insert User({uid}) job info failed: {e}")

@read_session
def get_job_info_from_db(
    uid, 
    jobid, 
    clusterid, 
    *,
    session: Session,
):
    stmt = select(models.JobInfo)
    msg = ''

    if uid:
        stmt = stmt.where(models.JobInfo.uid == uid)
    if jobid:
        stmt = stmt.where(models.JobInfo.jobid == jobid)
    if clusterid:
        stmt = stmt.where(models.JobInfo.clusterid == clusterid)
    try:
        results = session.execute(stmt).scalar()
        results = results.to_dict()
    except AttributeError as e:
        raise NoResultFound
    return results

def get_job_info_field(
    uid, 
    jobid, 
    clusterid,
    *field_names
):

    try:
        results = get_job_info_from_db(uid, jobid, clusterid)
    except NoResultFound:
        raise NoResultFound(f"ERR : No records found for user({uid}), job({jobid}), cluster({clusterid}).")
    
    try:
        field_res = [results[field_name] for field_name in field_names]
    except Exception as e:
        raise Exception(f"ERR : {e.__str__()} for user({uid}), job({jobid}), cluster({clusterid}).")
    
    return tuple(field_res)

def get_out_err_path(
    uid, 
    jobid, 
    clusterid, 
):

    return get_job_info_field(uid, jobid, clusterid, 'outpath', 'errpath')

def get_job_type(
    uid, 
    jobid, 
    clusterid, 
):

    return get_job_info_field(uid, jobid, clusterid, 'job_type')

def get_job_path(
    uid, 
    jobid, 
    clusterid,
):

    return get_job_info_field(uid, jobid, clusterid, 'job_path')
    
def get_job_info(
    uid, 
    jobid, 
    clusterid,
):
    return get_job_info_field(uid, jobid, clusterid, 'job_type', 'job_status', 'iptable_status', 'iptable_clean')

def get_endtime_info(
    uid,
    jobid,
    clusterid,
):
    return get_job_info_field(uid, jobid, clusterid, 'job_end_time')

def get_job_connect_info(
    uid, 
    jobid, 
    clusterid,
):
    return get_job_info_field(uid, jobid, clusterid, 'connect_sign')

def get_job_iptables_status(uid, jobid, clusterid):
    return get_job_info_field(uid, jobid, clusterid, 'iptable_status')

@transactional_session
def update_jobinfo_db(
    uid, 
    jobid,
    clusterid, 
    field_name, 
    field_value, 
    *, 
    session: Session
):

    stmt = update(models.JobInfo)

    if uid:
        stmt = stmt.where(models.JobInfo.uid == uid)
    if jobid:
        stmt = stmt.where(models.JobInfo.jobid == jobid)
    if clusterid:
        stmt = stmt.where(models.JobInfo.clusterid == clusterid)
    try:
        update_valuse = {field_name:field_value}
        stmt = stmt.values(**update_valuse)
        session.execute(stmt)
        session.flush()
    
    except Exception as e:
        raise Exception(f"ERR : \'{e.__str__()}\' in update for user({uid}), job({jobid}), cluster({clusterid}).")

def update_iptable_status(uid, jobid, iptable_status, clusterid):
    return update_jobinfo_db(uid, jobid, clusterid, 'iptable_status', iptable_status)

def update_iptable_clean(uid, jobid, iptable_clean, clusterid):
    return update_jobinfo_db(uid, jobid, clusterid, 'iptable_clean', iptable_clean)

def update_job_status(uid, jobid, job_status, clusterid):
    return update_jobinfo_db(uid, jobid, clusterid, 'job_status', job_status)

def update_connect_status(uid, jobid, connect_sign, clusterid):
    return update_jobinfo_db(uid, jobid, clusterid, 'connect_sign', connect_sign) 

@read_session
def find_completed_jobs(uid, jobtype, *, session:Session):
    
    job_list = {}
    stmt = select(models.JobInfo)

    if uid:
        stmt = stmt.where(models.JobInfo.uid == uid)
    if jobtype:
        if jobtype == 'all':
            stmt = stmt.where(models.JobInfo.clusterid == 'htcondor')
        else:
            stmt = stmt.where(models.JobInfo.job_type == jobtype)

    stmt = stmt.where(models.JobInfo.job_status not in ('COMPLETED', 'CANCELED'))
    try:
        results = session.execute(stmt).scalars()
    except Exception as e:
        raise Exception(f"ERR : {e} in find completed jobs for for user({uid})")
    
    for result in results:
        job_list[result.jobid] = [result.job_type, result.iptable_status, result.iptable_clean]
        
    return job_list

class PathChecker:
    
    @staticmethod
    def is_absolute_path(path: str) -> bool:
        return Path(path).is_absolute()
    
    @staticmethod
    def is_relative_path(path: str) -> bool:
        return not Path(path).is_absolute()
    
    @staticmethod
    def is_file(path: str) -> Optional[bool]:
        p = Path(path)
        return p.is_file() if p.exists() else None
    
    @staticmethod
    def is_directory(path: str) -> Optional[bool]:
        p = Path(path)
        return p.is_dir() if p.exists() else None
    
    @staticmethod
    def is_filename_only(path: str) -> Optional[bool]:
        p = PurePath(path)
    
        if str(p.parent) != '.':
            return False
        
        if any(sep in path for sep in ('/', '\\')):
            return False
        
        if len(path.parts) > 1:
            return False
        
        if path.anchor:
            return False
        
        return True
    
    @staticmethod
    def is_existed(path: str) -> Optional[bool]:
        p = Path(path)
        return p.exists()
    
def parse_sbatch_out_err(cmd: str, job_id: str | int) -> Tuple[Optional[str], Optional[str]]:
    """
    Parse --output and --error paths from an sbatch command
    and replace %j with the given job ID.

    :param cmd: sbatch command string
    :param job_id: Slurm job ID
    :return: (output_path, error_path)
    """
    output_path = None
    error_path = None

    tokens = shlex.split(cmd)
    job_id = str(job_id)

    it = iter(enumerate(tokens))
    for i, token in it:
        # --output=/path
        if token.startswith("--output="):
            output_path = token.split("=", 1)[1]

        # --output /path
        elif token == "--output" and i + 1 < len(tokens):
            output_path = tokens[i + 1]

        # --error=/path
        elif token.startswith("--error="):
            error_path = token.split("=", 1)[1]

        # --error /path
        elif token == "--error" and i + 1 < len(tokens):
            error_path = tokens[i + 1]

    if output_path:
        output_path = output_path.replace("%j", job_id)
    if error_path:
        error_path = error_path.replace("%j", job_id)

    return output_path, error_path