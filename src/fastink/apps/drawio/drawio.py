#!/usr/bin/env python3

import os, traceback, urllib.parse, sys, time, base64
from string import Template
from fastapi.responses import HTMLResponse

from fastink.storage import common
from fastink.storage.utils import PathType, extract_param, unquote_expand_user
from fastink.common.utils import get_krb5cc
from fastink.common.config import get_config
from fastink.common.logger import logger
from fastink.routers.status import *
from fastink.apps.drawio.template import png_template, svg_template

params = common.storage_init()
xrd_host = params['mgm_url']

async def draw(username:str, TargetPath:str, Type:str, create:bool = False):

    #### Check if we need to create
    is_exist = False
    if create:
        try:
            is_exist, path_type = await common.path_exist(name = TargetPath, username = username, mgm = xrd_host)
            if is_exist:
                logger.error("TargetPath {TargetPath} exists... Failed to created it...")
                return {"status": InkStatus.PATH_EXIST, "msg": {f"Failed to create {TargetPath}. Path exists..."}, "data": None}
            await common.upload_file(src_data = '', dst = TargetPath, username = username, mgm = xrd_host)
            is_exist, path_type = await common.path_exist(TargetPath, username = username, mgm = xrd_host)
            if is_exist and path_type == PathType.FILE:
                logger.debug(f"Created {TargetPath} successfully.")
            else:
                return {"status": InkStatus.PATH_NOT_EXIST, "msg": f"Failed to create TaretPath {TargetPath}.", "data": None}
        except Exception as e:
            logger.error(f"Failed to create TargetPath {TargetPath}. Err:{str(e)}")
            return {"status": InkStatus.PATH_NOT_EXIST, "msg": f"Failed to create TaretPath {TargetPath}. err:{str(e)}", "data": None}
    if not is_exist:
        is_exist, path_type = await common.path_exist(TargetPath, username = username, mgm = xrd_host)
        if is_exist and path_type == PathType.FILE:
            logger.debug(f"Created {TargetPath} successfully.")
        else:
            return {"status": InkStatus.PATH_NOT_EXIST, "msg": f"TaretPath {TargetPath} doesn't exist or Unknown Error.", "data": None}
    try:
        data = b""
        async for chunk in common.get_file_stream(username = username, fname = TargetPath):
            data += chunk
        data = base64.b64encode(data).decode('utf-8')
        logger.debug(f"I've got drawio files:{data}")
        if Type in ['svg', 'xml', 'drawio']:
            data_src = f"data:image/svg+xml;base64,{data}"
            html_content = svg_template.substitute(data = data_src)
        else:
            data_src = f"data:image/png;base64,{png_src}"
            html_content = png_template.substitute(data = data_src)
        logger.debug(f"I wanna return {html_content}")
        response = HTMLResponse(content=html_content, status_code=200)
        return response
    except Exception as e:
        logger.error(f"Failed to get {TargetPath}. Err:{str(e)}")
        return {"status": InkStatus.FS_UNKNOWN_ERROR, "msg": f"Failed to Open TaretPath {TargetPath}. Unknown Error.", "data": None}
