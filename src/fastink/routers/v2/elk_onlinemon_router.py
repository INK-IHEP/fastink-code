from fastapi import APIRouter, Query, Request, Body, Depends
from fastink.routers.headers import get_username, get_token
from fastink.alicpt.onlinemon.get_aligcs import handle_srs_data
from fastink.alicpt.onlinemon.get_aligcs import handle_mlc_data
from fastink.alicpt.onlinemon.get_aligcs import handle_compressor_data
from fastink.alicpt.onlinemon.get_aligcs import handle_ups_data
from fastink.alicpt.onlinemon.get_aligcs import handle_weather_data
from fastink.alicpt.onlinemon.get_aligcs import handle_airheater_data
from fastink.alicpt.onlinemon.get_aligcs import handle_ats_data
from fastink.alicpt.onlinemon.get_aligcs import handle_imu_data
from fastink.alicpt.onlinemon.get_aligcs import handle_tilt_data

router = APIRouter()

#srs数据接口
@router.get("/elk/get_srs")
async def get_srs_info(
    daq_start_time: str = Query(None, description="daq_start_time"),
    daq_end_time: str = Query(None, description="daq_end_time"),
    username: str = Depends(get_username),
    token: str = Depends(get_token)
):
    try:
        data = handle_srs_data(daq_start_time, daq_end_time) if (daq_start_time is not None) and (daq_end_time is not None) else handle_srs_data()
        return {
            "status": "200",
            "msg": "success",
            "data": data
        }
    except Exception as e:
        return {
            "status": "500",
            "msg": f"Exception: {e}",
            "data": []
        }
#mlc数据接口
@router.get("/elk/get_mlc")
async def get_mlc_info():
    daq_start_time: str = Query(None, description="daq_start_time"),
    daq_end_time: str = Query(None, description="daq_end_time"),
    username: str = Depends(get_username),
    token: str = Depends(get_token)
    try:
        return {
            "status": "200",
            "msg": "success",
            "data": handle_mlc_data()
        }
    except Exception as e:
        return {
            "status": "500",
            "msg": f"Exception: {e}",
            "data": []
        }

#压缩机数据接口
@router.get("/elk/get_compressor")
async def get_compressor_info():
    """Get compressor data from monitoring system"""
    daq_start_time: str = Query(None, description="daq_start_time"),
    daq_end_time: str = Query(None, description="daq_end_time"),
    username: str = Depends(get_username),
    token: str = Depends(get_token)
    try:
        return {
            "status": "200",
            "msg": "success",
            "data": handle_compressor_data()
        }
    except Exception as e:
        return {
            "status": "500",
            "msg": f"Exception: {e}",
            "data": []
        }

#ups数据接口
@router.get("/elk/get_ups")
async def get_ups_info():
    daq_start_time: str = Query(None, description="daq_start_time"),
    daq_end_time: str = Query(None, description="daq_end_time"),
    username: str = Depends(get_username),
    token: str = Depends(get_token)
    try:
        return {
            "status": "200",
            "msg": "success",
            "data": handle_ups_data()
        }
    except Exception as e:
        return {
            "status": "500",
            "msg": f"Exception: {e}",
            "data": []
        }

#天气数据接口
@router.get("/elk/get_weather")
async def get_weather_info():
    daq_start_time: str = Query(None, description="daq_start_time"),
    daq_end_time: str = Query(None, description="daq_end_time"),
    username: str = Depends(get_username),
    token: str = Depends(get_token)
    try:
        return {
            "status": "200",
            "msg": "success",
            "data": handle_weather_data()
        }
    except Exception as e:
        return {
            "status": "500",
            "msg": f"Exception: {e}",
            "data": []
        }
#获取airheater数据
@router.get("/elk/get_airheater")
async def get_airheater_info():
    daq_start_time: str = Query(None, description="daq_start_time"),
    daq_end_time: str = Query(None, description="daq_end_time"),
    username: str = Depends(get_username),
    token: str = Depends(get_token)
    try:
        return {
            "status": "200",
            "msg": "success",
            "data": handle_airheater_data()
        }
    except Exception as e:
        return {
            "status": "500",
            "msg": f"Exception: {e}",
            "data": []
        }
#获取ats数据
@router.get("/elk/get_ats")
async def get_ats_info():
    daq_start_time: str = Query(None, description="daq_start_time"),
    daq_end_time: str = Query(None, description="daq_end_time"),
    username: str = Depends(get_username),
    token: str = Depends(get_token)
    try:
        return {
            "status": "200",
            "msg": "success",   
            "data": handle_ats_data()
        }
    except Exception as e:
        return {
            "status": "500",
            "msg": f"Exception: {e}",
            "data": []
        }

#获取imu数据
@router.get("/elk/get_imu")
async def get_imu_info():
    daq_start_time: str = Query(None, description="daq_start_time"),
    daq_end_time: str = Query(None, description="daq_end_time"),
    username: str = Depends(get_username),
    token: str = Depends(get_token)
    try:
        return {
            "status": "200",
            "msg": "success",
            "data": handle_imu_data()
        }
    except Exception as e:
        return {
            "status": "500",
            "msg": f"Exception: {e}",
            "data": []
        }
#获取tilt数据
@router.get("/elk/get_tilt")
async def get_tilt_info():
    daq_start_time: str = Query(None, description="daq_start_time"),
    daq_end_time: str = Query(None, description="daq_end_time"),
    username: str = Depends(get_username),
    token: str = Depends(get_token)
    try:
        return {
            "status": "200",
            "msg": "success",
            "data": handle_tilt_data()
        }
    except Exception as e:
        return {
            "status": "500",
            "msg": f"Exception: {e}",
            "data": []
        }





