from fastapi import APIRouter, HTTPException, Header, Query, Request
from datetime import datetime, timedelta
from elasticsearch import Elasticsearch
import pytz
from src.common.config import get_config

import urllib.request
import json

def getresultbyurl(url):
    resu = urllib.request.urlopen(url, data=None, timeout=30)
    data = json.loads(resu.read().decode())
    return data

def replace_null_with_zero(obj):
    if isinstance(obj, dict):
        return {k: replace_null_with_zero(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [replace_null_with_zero(item) for item in obj]
    elif obj is None:
        return 0
    else:
        return obj

def parse_omat_data(omat_json_data) :
    result_list=[]
    for record in omat_json_data:
        group_name=record["target"]
        points = record["datapoints"]
        data_values={}
        result_record={}
        for point in points:
            dt_object = datetime.fromtimestamp(point[1])
            time_point = dt_object.strftime('%H:%M:%S')
            data_values[time_point]=point[0]
        result_record["name"]=group_name
        result_record["data"]=data_values
        result_list.append(result_record)
    return result_list


async def ink_sta_job(cluster_id, query_type):
    job_config = get_config("graphite") 
    url=""

    if (cluster_id=="htcondor"):
        job_addr = job_config["htc_job_addr"]
        target = job_config["htc_sched"]
        target = ','.join(target)
        period = job_config["period"]
        url_template = "%s/render?format=json&target=removeEmptySeries(groupByNode(summarize(job.stat.condor.{%s}.*.%s,'1h','max'),4,'sum'))&from=-%s&until=now&format=json"
        if (query_type in ["queuejobs", "runjobs"]):
            url=url_template % (job_addr,target,query_type, period)
        else:
            err= "Errror with the parameter %s %s %s %s\n%s" %(job_addr,target,query_type, period,url)        
            return(err)
    if (cluster_id == "slurm"):
        job_addr = job_config["hpc_job_addr"]
        target = job_config["hpc_server"]
        target = ','.join(target)
        period = job_config["period"]
        url_template ="%s/render?format=json&target=removeEmptySeries(groupByNode(summarize(sortBy(transformNull(job.stat.slurm.%s.*.%s,0),'last',true),'1h','avg',false),4,'average'))&from=-%s&until=now&format=json"   
        if (query_type in ["queuejobs_cpu","queuejobs_gpu","runjobs_gpu", "runjobs_cpu"]):
            url=(url_template % (job_addr,target,query_type, period))
            print(url)  
        else:
            return("Errror with the parameter ")        
    
    result=getresultbyurl(url)
    omat_data=parse_omat_data(result)
    return_data=replace_null_with_zero(omat_data)
    
    return(return_data)
    
