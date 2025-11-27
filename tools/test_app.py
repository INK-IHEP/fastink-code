#!/usr/bin/env python3
from src.apps.statistic.htc_statistics import ink_sta_job


def gethtc():
    #result=ink_sta_job("htcondor","runjobs")
    result=ink_sta_job("slurm","runjobs_cpu")
    return result

if __name__ == "__main__":
    a=gethtc()
    
