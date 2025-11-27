from fastapi import APIRouter
from  src.alicpt.onlinemon.get_aligcs  import handle_srs_data
from  src.alicpt.onlinemon.get_aligcs  import handle_mlc_data
from  src.alicpt.onlinemon.get_aligcs  import handle_compressor_data
from  src.alicpt.onlinemon.get_aligcs  import handle_ups_data
from  src.alicpt.onlinemon.get_aligcs  import handle_weather_data
from  src.alicpt.onlinemon.get_aligcs  import handle_airheater_data
from  src.alicpt.onlinemon.get_aligcs  import handle_ats_data
from  src.alicpt.onlinemon.get_aligcs  import handle_imu_data
from  src.alicpt.onlinemon.get_aligcs  import handle_tilt_data

router = APIRouter()

#srs数据接口
@router.get("/elk/get_srs")
async def get_mlc_info():
    try:
        return {
            "status": "200",
            "msg": "success",
            "data": handle_srs_data()
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





