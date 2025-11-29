from sqlalchemy.exc import NoResultFound
from sqlalchemy.orm import Session
#from sqlalchemy.sql.expression import update, select, delete

from sqlalchemy import select, update, delete
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
        logger.info(f"Insert User {uid} job info to DB, jobid: {jobid}, output: {outpath}, errpath: {errpath}, jobtype: {job_type}, jobpath: {job_path}, cluster: {clusterid}")
        
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

def update_start_time(uid, jobid, starttime, clusterid):
    return update_jobinfo_db(uid, jobid, clusterid, 'job_start_time', starttime) 

def update_end_time(uid, jobid, endtime, clusterid):
    return update_jobinfo_db(uid, jobid, clusterid, 'job_end_time', endtime) 


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

    stmt = stmt.where(models.JobInfo.job_status.notin_(["COMPLETED", "CANCELED"]))
    
    try:
        results = session.execute(stmt).scalars()
    except Exception as e:
        raise Exception(f"ERR : {e} in find completed jobs for for user({uid})")
    
    for result in results:
        job_list[result.jobid] = [result.job_type, result.iptable_status, result.iptable_clean]
        
    return job_list


@read_session
def needto_change_status_jobs(*, session:Session):
    
    job_list = {}
    stmt = select(models.JobInfo)
    stmt = stmt.where(models.JobInfo.clusterid == 'htcondor')

    stmt = stmt.where(models.JobInfo.job_status.notin_(["COMPLETED", "CANCELED"]))
    
    try:
        results = session.execute(stmt).scalars()
    except Exception as e:
        raise Exception(f"ERR : {e} in change_status_job func.")
    
    for result in results:
        job_list[result.jobid] = [result.job_type, result.iptable_status, result.iptable_clean]
        
    return job_list


from sqlalchemy import or_

@read_session
def get_jobs_with_null_times(
    *,
    session: Session,
) -> list:

    # 构建查询语句
    stmt = select(models.JobInfo.jobid).where(
        or_(
            models.JobInfo.job_start_time.is_(None),
            models.JobInfo.job_end_time.is_(None)
        )
    )
    
    try:
        # 执行查询并获取所有结果
        results = session.execute(stmt).scalars().all()
        
        # 将结果转换为字典列表
        return results
        
    except Exception as e:
        raise Exception(f"ERR : {e} in get job with null sql func.")


@transactional_session
def delete_jobinfo_by_jobids(jobids, *, session: Session):
    if not jobids:
        return

    stmt = delete(models.JobInfo).where(models.JobInfo.jobid.in_(jobids))
    try:
        result = session.execute(stmt)
        session.flush()
        logger.info(f"delete_jobinfo_by_jobids: deleted {result.rowcount} rows, jobids_count={len(jobids)}")
    except Exception:
        logger.exception(f"delete_jobinfo_by_jobids: failed, jobids_count={len(jobids)}")
        raise