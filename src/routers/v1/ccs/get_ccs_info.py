from datetime import datetime, date
from src.apps.user_dashboard.get_ccs_used import InfluxDBConnection
from fastapi import APIRouter, Query
import pymysql
from pymysql.cursors import DictCursor
import json

from src.common.logger import logger
import logging

influx = InfluxDBConnection()
router = APIRouter()

# http://ccs.ihep.ac.cn/jeeplus/a/bill/omat/getlaststate
# kind 是作业类型"HPC"或者"HTC"，user是用户名，返回类型是json
@router.post("/getlaststate")
async def laststate(
        kind: str = Query(..., description="作业类型"), 
        user: str = Query(..., description="用户名")
):
    try:
        ret_list = []
        # kind = "HPC" # HPC或者HTC
        # user = "zhangzhx"  # 指定用户名
        ret_list = await influx.getLastState(kind=kind, user=user)
        # ret_list = json.dumps(ret_list)
        # print(ret_list)
        return ret_list
    except Exception as e:
        print(f"Error occurred: {e}")

# http://ccs.ihep.ac.cn/jeeplus/a/bill/omat/getstatebytime
# start_time_str是开始时间，end_time_str是结束时间，kind 是作业类型"HPC"或者"HTC"，user是用户名，返回类型是json
@router.post("/getstatebytime")
async def statebytime(
    start_time_str: str = Query(..., description="开始时间"), 
    end_time_str: str = Query(..., description="结束时间"), 
    kind: str = Query(..., description="作业类型"), 
    user: str = Query(..., description="用户名")
):
    try:
        # start_time_str = "2024-12-18 08:00:00"
        # end_time_str = "2024-12-19 18:00:00"
        start_time = datetime.strptime(start_time_str, "%Y-%m-%d %H:%M:%S") if start_time_str else None
        end_time = datetime.strptime(end_time_str, "%Y-%m-%d %H:%M:%S") if end_time_str else None
        # kind = "HTC"  # HPC或者HTC
        # user = "zhangzhx"  # 指定用户名
        # user = "jiangxw"

        result = await influx.getStateByTime(start_time, end_time, user, kind)
        # result = json.dumps(result)
        # print(result)

        return result
    
    except Exception as e:
        print(f"Error occurred: {e}")

# http://ccs.ihep.ac.cn/jeeplus/a/bill/omat/getlastdisk
# user是用户名，返回类型是json
@router.post("/getlastdisk")
async def lastdisk(user: str = Query(..., description="用户名")):
    try:
        # user = "heshuaishuai"
        result = await influx.GetNowDiskFile(user)
        # result = json.dumps(result)
        # print(result)

        return result
    except Exception as e:
        print(f"Error occurred: {e}")


# http://ccs.ihep.ac.cn/jeeplus/a/bill/omat/getdiskbytime
# start_time_str是开始时间，end_time_str是结束时间，user是用户名，directory是目录，返回类型是json
@router.post("/getdiskbytime")
async def diskbytime(
    start_time_str: str = Query(..., description="开始时间"), 
    end_time_str: str = Query(..., description="结束时间"), 
    user: str = Query(..., description="用户名"), 
    directory: str = Query(..., description="目录")
):
    try:
        # start_time_str = "2024-11-20 22:27:25"
        # end_time_str = "2024-12-20 22:27:25"
        start_time = datetime.strptime(start_time_str, "%Y-%m-%d %H:%M:%S") if start_time_str else None
        end_time = datetime.strptime(end_time_str, "%Y-%m-%d %H:%M:%S") if end_time_str else None
        # user = "heshuaishuai"
        # directory = "/afs/ihep.ac.cn/users/h/heshuaishuai"

        result = await influx.GetDiskFileByTime(start_time, end_time, user, directory)
        # result = json.dumps(result)
        # print(result)
        return result
    except Exception as e:
        print(f"Error occurred: {e}")


# user是用户名，返回类型是json
@router.get("/is-effective")
async def is_effective(
    username: str = Query(..., description="用户名")
):
    logger.debug(f"I got the username: {username}")
    try:
        # 建立数据库连接
        connection = pymysql.connect(
            host='ccs.ihep.ac.cn',
            user='read',
            password='read;010',
            database='cluster',
            cursorclass=DictCursor 
        )
        
        with connection:
            with connection.cursor() as cursor:
                today = date.today()
                date_str_datetime = today.strftime("%Y-%m-%d")
                # 执行SQL查询
                query = "SELECT afsaccount FROM afsuser WHERE afsaccount='"+username+"' AND islock='0' AND del_flag='0' AND afsdate>'"+date_str_datetime+"' AND expiredate>'"+date_str_datetime+"'" 
                cursor.execute(query)
               
                # 获取查询结果
                results = cursor.fetchall()
                
                # 处理结果
                if results:
                    response_data = {
                        "status": 200,
                        "msg": "success",
                        "data": {
                            "effective": 1
                        }
                    }                
                else:
                    response_data = {
                        "status": 200,
                        "msg": "success",
                        "data": {
                            "effective": 0
                        }
                    }    
                return response_data 
    except pymysql.MySQLError as e:
        response_data = {
                        "status": 500,
                        "msg": "{e}",
                        "data": {}
                    }    
        return response_data