from datetime import datetime, timedelta
from elasticsearch import Elasticsearch
import pytz
from src.common.config import get_config


def create_half_hourly_dict():
    current_time = datetime.now(pytz.timezone('Asia/Shanghai'))
    # 确保开始时间以00或30分钟为界限
    if current_time.minute < 30:
        start_time = current_time.replace(minute=0, second=0, microsecond=0) - timedelta(hours=24)
    else:
        start_time = current_time.replace(minute=30, second=0, microsecond=0) - timedelta(hours=24)

    half_hourly_dict = {}

    while start_time + timedelta(minutes=30) <= current_time:
        start_time_str = start_time.strftime('%H:%M')
        end_time = start_time + timedelta(minutes=30)
        end_time_str = end_time.strftime('%H:%M')
        time_key = f"{start_time_str}-{end_time_str}"
        half_hourly_dict[time_key] = 0
        start_time = end_time


    if start_time < current_time:
        start_time_str = start_time.strftime('%H:%M')

        if current_time.minute < 30:
            end_time = current_time.replace(minute=0, second=0, microsecond=0)
        else:
            end_time = current_time.replace(minute=30, second=0, microsecond=0)

            if end_time >= current_time:
                end_time = current_time.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1)

        end_time_str = end_time.strftime('%H:%M')
        time_key = f"{start_time_str}-{end_time_str}"

        if start_time_str != end_time_str:
            half_hourly_dict[time_key] = 0

    return half_hourly_dict


async def ink_omat_table():

    addr = get_config("omat", "addr")
    user = get_config("omat", "user")
    passwd = get_config("omat", "passwd")

    client = Elasticsearch(
        hosts=addr, 
        http_auth=(user, passwd),  
        timeout=300  
    )
    
    current_time = datetime.now(pytz.timezone('Asia/Shanghai'))

    start_time = current_time - timedelta(hours=24)
    start_time_str = start_time.isoformat()
    current_time_str = current_time.isoformat()
    half_hourly_dict = create_half_hourly_dict()
    
    SCHEDD_HOST = get_config("computing", "schedd_host").split("@", 1)[1]
    
    

    bodystr = {
        "size": 0, 
        "aggs": {
            "half_hourly_timestamp": { 
                "date_histogram": {
                    "field": "@timestamp",
                    "fixed_interval": "30m",
                    "time_zone": "Asia/Shanghai",
                    "min_doc_count": 1
                }
            }
        },
        "query": {
            "bool": {
                "filter": [
                    {
                        "term": {
                            "schedulerserver": SCHEDD_HOST
                        }
                    },
                    {
                        "bool": {
                            "should": [
                                {
                                    "match_phrase": {
                                        "HepJob_JobType": "jupyter"
                                    }
                                },
                                {
                                    "match_phrase": {
                                        "HepJob_JobType": "enode"
                                    }
                                },
                                {
                                    "match_phrase": {
                                        "HepJob_JobType": "vscode"
                                    }
                                },
                                {
                                    "match_phrase": {
                                        "HepJob_JobType": "rootbrowse"
                                    }
                                }
                            ],
                            "minimum_should_match": 1
                        }
                    },
                    {
                        "range": {
                            "@timestamp": {
                                "gte": start_time_str,
                                "lte": current_time_str,
                                "format": "strict_date_optional_time"
                            }
                        }
                    }
                ]
            }
        }
    }


    response = client.search(index='condorq', body=bodystr)
    buckets = response['aggregations']['half_hourly_timestamp']['buckets']
    
    for bucket in buckets:
        timestamp = bucket['key_as_string']
        hour = int(timestamp[11:13]) 
        minute = int(timestamp[14:16])  

        if minute < 30:
            half_hour_key = f"{hour:02d}:00-{hour:02d}:30"
        else:
            half_hour_key = f"{hour:02d}:30-{((hour + 1) % 24):02d}:00"

        half_hourly_dict[half_hour_key] = half_hourly_dict.get(half_hour_key, 0) + bucket['doc_count']

    
    slurm_bodystr = {
      "size": 0,
      "aggs": {
        "half_hourly_timestamp": {
          "date_histogram": {
            "field": "@timestamp",
            "fixed_interval": "30m",
            "time_zone": "Asia/Shanghai",
            "min_doc_count": 1
          }
        }
      },
      "query": {
        "bool": {
          "filter": [
            {
              "bool": {
                "should": [
                  {
                    "match": {
                      "servernode": "slurm04.ihep.ac.cn"
                    }
                  }
                ],
                "minimum_should_match": 1
              }
            },
            {
              "range": {
                "@timestamp": {
                  "gte": start_time_str,
                  "lte": current_time_str,
                  "format": "strict_date_optional_time"
                }
              }
            }
          ]
        }
      }
    }

    response = client.search(index='slurmprolog', body=slurm_bodystr)
    slurm_buckets = response['aggregations']['half_hourly_timestamp']['buckets']

    for bucket in slurm_buckets:

      timestamp = bucket['key_as_string']
      hour = int(timestamp[11:13])  
      minute = int(timestamp[14:16]) 

      if minute < 30:
          half_hour_key = f"{hour:02d}:00-{hour:02d}:30"
      else:
          half_hour_key = f"{hour:02d}:30-{((hour + 1) % 24):02d}:00"

      half_hourly_dict[half_hour_key] = half_hourly_dict.get(half_hour_key, 0) + bucket['doc_count']

    return half_hourly_dict



async def ink_omat_stack_table(jobtype):

    addr = get_config("omat", "addr")
    user = get_config("omat", "user")
    passwd = get_config("omat", "passwd")

    client = Elasticsearch(
        hosts=addr, 
        http_auth=(user, passwd),  
        timeout=300  
    )
    
    current_time = datetime.now(pytz.timezone('Asia/Shanghai'))
    start_time = current_time - timedelta(hours=24)
    start_time_str = start_time.isoformat()
    current_time_str = current_time.isoformat()
    half_hourly_dict = create_half_hourly_dict()
    
    SCHEDD_HOST = get_config("computing", "schedd_host").split("@", 1)[1]

    bodystr = {
        "size": 0, 
        "aggs": {
          "half_hourly_timestamp": {
            "date_histogram": {
              "field": "@timestamp",
              "fixed_interval": "30m",
              "time_zone": "Asia/Shanghai",
              "min_doc_count": 1
            },
            "aggs": {
              "unique_count": {
                "cardinality": {
                  "field": "jobid"
                }
              }
            }
          }
        },
        "query": {
            "bool": {
                "filter": [
                    {
                        "term": {
                            "schedulerserver": SCHEDD_HOST
                        }
                    },
                    {
                        "bool": {
                            "should": [
                                {
                                    "match_phrase": {
                                        "HepJob_JobType": jobtype
                                    }
                                }
                            ],
                            "minimum_should_match": 1
                        }
                    },
                    {
                        "range": {
                            "@timestamp": {
                                "gte": start_time_str,
                                "lte": current_time_str,
                                "format": "strict_date_optional_time"
                            }
                        }
                    }
                ]
            }
        }
    }

   

    response = client.search(index='condorq', body=bodystr)
    buckets = response['aggregations']['half_hourly_timestamp']['buckets']
    
    for bucket in buckets:
        timestamp = bucket['key_as_string']
        hour = int(timestamp[11:13]) 
        minute = int(timestamp[14:16])  

        if minute < 30:
            half_hour_key = f"{hour:02d}:00-{hour:02d}:30"
        else:
            half_hour_key = f"{hour:02d}:30-{((hour + 1) % 24):02d}:00"

        half_hourly_dict[half_hour_key] = half_hourly_dict.get(half_hour_key, 0) + bucket['unique_count']['value']

    return half_hourly_dict

async def omat_gpu_jobs():
    
    addr = get_config("omat", "addr")
    user = get_config("omat", "user")
    passwd = get_config("omat", "passwd")

    client = Elasticsearch(
        hosts=addr, 
        http_auth=(user, passwd),  
        timeout=300  
    )
    
    current_time = datetime.now(pytz.timezone('Asia/Shanghai'))
    start_time = current_time - timedelta(hours=24)
    start_time_str = start_time.isoformat()
    current_time_str = current_time.isoformat()

    half_hourly_dict = create_half_hourly_dict()
    
    slurm_bodystr = {
        "size": 0,
        "aggs": {
            "half_hourly_timestamp": {
            "date_histogram": {
                "field": "@timestamp",
                "fixed_interval": "30m",
                "time_zone": "Asia/Shanghai",
                "min_doc_count": 1
            }
            }
        },
        "query": {
            "bool": {
            "filter": [
                {
                "bool": {
                    "should": [
                        {
                            "match": {
                                "servernode": "slurm04.ihep.ac.cn"
                            }
                        }
                    ],
                    "minimum_should_match": 1
                }
                },
                {
                "range": {
                    "@timestamp": {
                    "gte": start_time_str,
                    "lte": current_time_str,
                    "format": "strict_date_optional_time"
                    }
                }
                }
            ]
            }
        }
    }

    response = client.search(index='slurmepilog', body=slurm_bodystr)
    slurm_buckets = response['aggregations']['half_hourly_timestamp']['buckets']

    for bucket in slurm_buckets:

      timestamp = bucket['key_as_string']
      hour = int(timestamp[11:13])  
      minute = int(timestamp[14:16]) 

      if minute < 30:
          half_hour_key = f"{hour:02d}:00-{hour:02d}:30"
      else:
          half_hour_key = f"{hour:02d}:30-{((hour + 1) % 24):02d}:00"

      half_hourly_dict[half_hour_key] = half_hourly_dict.get(half_hour_key, 0) + bucket['doc_count']
    
    return half_hourly_dict