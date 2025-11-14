#! /usr/bin/python3
# FileName      : monitor.py
# Author        : HAN Xiao
# Email         : hanx@ihep.ac.cn
# Date          : Fri Jun 13 14:41:19 2025 CST
# Last modified : Thu Nov 13 17:47:39 2025 CST
# Description   :

from src.common.config import get_config
from src.common.logger import logger


def get_monitor_url():
    logger.info("Get monitor url")
    return get_config("service", "monitor_url", fallback="/grafana/")


def get_job_monitor_url(jobid=0):
    logger.info(f"Query job({jobid}) monitor url")
    url = get_config(
        "service",
        "job_monitor_url",
        fallback=f"/grafana/d/adlgck4/ink-details-for-users?kiosk=true&var-jobid={jobid}",
    ).format(jobid=jobid)
    logger.info(f"Get job({jobid}) monitor url: {url}")
    return url
