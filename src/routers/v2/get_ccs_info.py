from datetime import datetime, date
from src.apps.user_dashboard.get_ccs_used import InfluxDBConnection
from fastapi import APIRouter, Query,Header,Depends
import pymysql
from pymysql.cursors import DictCursor
import json
from pymysql import OperationalError, ProgrammingError
from typing import List, Dict, Tuple, Optional

from src.common.logger import logger
import logging

from src.routers.headers import get_username
from src.routers.status import InkStatus

influx = InfluxDBConnection()
router = APIRouter()

DB_CONFIG = {
    'host': 'ccs.ihep.ac.cn',
    'database': 'cluster',
    'user': 'read',
    'password': 'read;010',
    'port': 3306,
    'charset': 'utf8'
}

def create_connection() -> Optional[pymysql.connections.Connection]:
    """
    创建数据库连接
    :return: 数据库连接对象，失败则返回 None
    """
    connection = None
    try:
        connection = pymysql.connect(** DB_CONFIG)
        if connection.open:
            print(f"成功连接到 MySQL 服务器")
    except OperationalError as e:
        print(f"连接数据库失败: {e}")
    return connection


def close_connection(connection: pymysql.connections.Connection) -> None:
    """
    关闭数据库连接
    :param connection: 数据库连接对象
    """
    if connection and connection.open:
        connection.close()
        print("数据库连接已关闭")

def execute_query(connection: pymysql.connections.Connection, 
                 query: str, 
                 params: Optional[Tuple] = None) -> Optional[List[Dict]]:
    """
    执行查询类 SQL（SELECT）
    :param connection: 数据库连接对象
    :param query: SQL 查询语句
    :param params: SQL 参数（用于防止 SQL 注入）
    :return: 查询结果列表（字典形式），失败返回 None
    """
    cursor = None
    result = None
    try:
        with connection.cursor() as cursor:  # 自动关闭游标
            cursor.execute(query, params or ())
            result = cursor.fetchall()
            print(f"查询成功，返回 {len(result)} 条记录")
    except ProgrammingError as e:
        print(f"查询失败: {e}")
        print(f"SQL: {query}")
    return result

# http://ccs.ihep.ac.cn/jeeplus/a/bill/omat/getlaststate
# kind 是作业类型"HPC"或者"HTC"，user是用户名，返回类型是json
@router.get("/ccs/get_cur_userjobs")
async def laststate(
        kind: str = Query(..., description="作业类型"), 
        user: str = Depends(get_username)
):
    try:
        ret_list = []
        # kind = "HPC" # HPC或者HTC
        # user = "zhangzhx"  # 指定用户名
        ret_list = await influx.getLastState(kind=kind, user=user)
        # ret_list = json.dumps(ret_list)
        # print(ret_list)
        return {
            "status": InkStatus.SUCCESS,
            "msg": "success",
            "data": {
                "ret_list": ret_list
            }
        }
    except Exception as e:
        return {
            "status": InkStatus.INTERNAL_ERROR,
            "msg": f"Error occurred: {e}",
            "data": None
        }
         

# http://ccs.ihep.ac.cn/jeeplus/a/bill/omat/getstatebytime
# start_time_str是开始时间，end_time_str是结束时间，kind 是作业类型"HPC"或者"HTC"，user是用户名，返回类型是json
@router.get("/ccs/get_his_userjobs")
async def get_state_by_time(
    start_time_str: str = Query(..., description="开始时间"), 
    end_time_str: str = Query(..., description="结束时间"), 
    kind: str = Query(..., description="作业类型"), 
    user: str = Depends(get_username)
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

        return {
            "status": InkStatus.SUCCESS,
            "msg": "success",
            "data": {
                "result": result
            }
        }
    
    except Exception as e:
        # print(f"Error occurred: {e}")
        return {
            "status": InkStatus.INTERNAL_ERROR,
            "msg": f"Error occurred: {e}",
            "data": None
        }

# http://ccs.ihep.ac.cn/jeeplus/a/bill/omat/getlastdisk
# user是用户名，返回类型是json
@router.get("/ccs/get_cur_userdisk")
async def lastdisk(user: str = Depends(get_username)):
    try:
        # user = "heshuaishuai"
        result = await influx.GetNowDiskFile(user)
        # result = json.dumps(result)
        # print(result)

        return {
            "status": InkStatus.SUCCESS,
            "msg": "success",
            "data": {
                "result": result
            }
        }
    except Exception as e:
        # print(f"Error occurred: {e}")
        return {
            "status": InkStatus.INTERNAL_ERROR,
            "msg": f"Error occurred: {e}",
            "data": None
        }


# http://ccs.ihep.ac.cn/jeeplus/a/bill/omat/getdiskbytime
# start_time_str是开始时间，end_time_str是结束时间，user是用户名，directory是目录，返回类型是json
@router.get("/ccs/get_his_userdisk")
async def diskbytime(
    start_time_str: str = Query(..., description="开始时间"), 
    end_time_str: str = Query(..., description="结束时间"), 
    user: str = Depends(get_username), 
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
        return {
            "status": InkStatus.SUCCESS,
            "msg": "success",
            "data": {
                "result": result
            }
        }
    except Exception as e:
        # print(f"Error occurred: {e}")
        return {
            "status": InkStatus.INTERNAL_ERROR,
            "msg": f"Error occurred: {e}",
            "data": None
        }


# user是用户名，返回类型是json
@router.get("/ccs/verify_username")
async def is_effective(username: str = Depends(get_username)):
    logger.debug(f"I got the username: {username}")
    conn = create_connection()
    if not conn:
        return None
    
    today = date.today()
    date_str_datetime = today.strftime("%Y-%m-%d")
    # 用户在有效期，密码在有效期
    afs_query = "SELECT afsaccount FROM afsuser WHERE afsaccount='"+username+"' AND islock='0' AND del_flag='0' AND afsdate>'"+date_str_datetime+"'"
    afs_results = execute_query(conn, afs_query, ())
    password_query ="SELECT afsaccount FROM afsuser WHERE afsaccount='"+username+"' AND islock='0' AND del_flag='0' AND expiredate>'"+date_str_datetime+"'"
    password_results = execute_query(conn, password_query, ())    
    if afs_results and password_results:
        response_data = {
                        "status": InkStatus.SUCCESS,
                        "msg": "success",
                        "data": {
                            "effective": 1,
                            "afsdate": 1,
                            "passworddate": 1
                        }
        }      
    else:
        afsdate = 1 if afs_results else 0
        passworddate = 1 if password_results else 0
        response_data = {
            "status": InkStatus.SUCCESS,
            "msg": "success",
            "data": {
                "effective": 0,
                "afsdate": afsdate,
                "passworddate": passworddate
            }
        }
    close_connection(conn)    
    return response_data

