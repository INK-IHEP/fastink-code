from pprint import pprint
from typing import Optional, Any
from datetime import datetime
from sqlalchemy.exc import NoResultFound
from sqlalchemy.orm import Session
from sqlalchemy.sql.expression import update, select, delete

from src.database.sqla import models
from src.database.sqla.session import read_session, transactional_session
from src.common.logger import logger

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


