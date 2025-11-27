#!/usr/bin/env python3

import os, traceback, urllib.parse, sys, time

from fastapi import APIRouter, UploadFile, File, Form, Header, HTTPException, Query, Depends, Request
from fastapi.responses import StreamingResponse, UJSONResponse

from src.common.utils import get_krb5cc, get_uname_from_uid
from src.storage import common
from src.storage.utils import PathType, extract_param, unquote_expand_user
from src.common.logger import logger
from src.routers.status import *
from src.sharefile.share_file import get_link
from src.sharefile.uuid_decryptor import UUIDDecryptor
import jwt

router = APIRouter()
@router.get("/sharefile/get", response_class=UJSONResponse)
async def get_info(req: Request, path: str = Query(None)) -> dict:
    try:
        secret_key = 'service-communication-shared-secret-key-123456'
        encrypted_str = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpYXQiOjE3NTU2NzA4MDAzMzYsImV4cCI6MTc1NTY3MjYwMDMzNiwib3duZXIiOiJvdWciLCJmaWxlUGF0aCI6Ii9ob21lL2NjL291Zy_lj6_ku6UudHh0In0.QZcFjVP4aH3tuERJgNLlR3zS-TzSq9mU2lg575hzYdQ'
        payload = jwt.decode(
           encrypted_str,
           key=secret_key,  
           algorithms=["HS256"],  
           options={
            "verify_signature": True,
            "verify_iat": False  
           }
        )
         # 获取存储的信息
        file_path = payload.get("filePath")
        owner = payload.get("owner")
    
        print(f"filePath: {file_path}")
        print(f"owner: {owner}")
        print("完整payload信息:", payload)
        # return payload
    except jwt.ExpiredSignatureError:
        print("令牌已过期")
    except jwt.InvalidSignatureError:
        print("签名验证失败（密钥错误或令牌被篡改）")
    except jwt.InvalidTokenError as e:
        print("无效的令牌：", e)

    # try:
    #     secret_key ='16ByteSecureKey!'  # 替换为实际密钥
    #     encrypted_str = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpYXQiOjE3NTU2NzA4MDAzMzYsImV4cCI6MTc1NTY3MjYwMDMzNiwib3duZXIiOiJvdWciLCJmaWxlUGF0aCI6Ii9ob21lL2NjL291Zy_lj6_ku6UudHh0In0.QZcFjVP4aH3tuERJgNLlR3zS-TzSq9mU2lg575hzYdQ'  
    #     payload = jwt.decode(
    #         encrypted_str,
    #         secret_key,
    #         algorithms=["HS256"],  # 从令牌头可知使用HS256算法
    #         options={"verify_signature": True}
    #     )
    #     print(payload)
    #     # link = get_link(path)
    # except Exception as err:
    #     return {
    #         "status": InkStatus.PERMISSION_QUERY_FAILURE,
    #         "msg": f"failed: {err}",
    #         "data": None,
    #     }
    link='1'
    if link:
        result = {
            "status": InkStatus.SUCCESS,
            "msg": "successfully",
            "data": {"link": link},
        }
    else:
        result = {
            "status": InkStatus.PERMISSION_QUERY_FAILURE,
            "msg": f"failed",
            "data": None,
        }
    return result