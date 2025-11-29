#!/usr/bin/env python3

import logging, math, asyncio, os, time, urllib.parse
from src.common.config import get_config
from enum import Enum
from functools import wraps

from fastapi import Request

logger=logging.getLogger(__name__)

mode_maps = ['---','--x','-w-','-wx','r--','r-x','rw-','rwx']

#### Sync and async function timer
def sync_timer(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        tm_start = time.time()
        result = func(*args, **kwargs)
        tm_elapsed  = time.time() - tm_start
        logger.debug(f"Timer. {func.__name__} cost: {tm_elapsed:.4f} seconds.")
        return result
    return wrapper

def async_timer(func):
    @wraps(func)
    async def wrapper(*args, **kwargs):
        tm_start = time.time()
        result = await func(*args, **kwargs)
        tm_elapsed  = time.time() - tm_start
        logger.debug(f"Timer. {func.__name__} cost: {tm_elapsed:.4f} seconds.")
        return result
    return wrapper

#### init storage parameters
def storage_init():
    mgm_url = get_config("storage", "xrd_host", fallback="")
    if mgm_url[0] == "'" or mgm_url[0] == '"':
        mgm_url = mgm_url[1:-1]
    if mgm_url == "":
        logger.error("mgm_url is EMPTY.")
        raise ValueError("Parameter mgm_url is EMPTY!")
    max_file_size = get_config("storage", "max_file_size")
    krb5_enabled = get_config("common", "krb5_enabled")
    fs_backend = get_config("storage","fs_backend")

    return {
        "mgm_url" : mgm_url,
        "max_file_size" : max_file_size,
        "krb5_enabled" : krb5_enabled,
        "fs_backend" : fs_backend
    }

def unquote_expand_user(dname:str, username:str, url:bool = False):
    unquoted_dname = dname
    while unquoted_dname[0] == "'" or unquoted_dname[0] == '"':
        unquoted_dname = unquoted_dname[1:-1]
    if url:
        unquoted_dname = urllib.parse.unquote(dname, encoding="utf-8")
    if unquoted_dname[0] == "~":
        unquoted_dname = os.path.expanduser(f"~{username}{unquoted_dname[1:]}")
    return unquoted_dname

#### PathType
class PathType(Enum):
    ### type: dir - 0, file - 1, link - 2, unknown - -1,
    DIR = 0
    FILE = 1
    LINK = 2
    UNKNOWN = -1

#### Permission Check
def path_stat(name, returncode, stdout, stderr):
    logger.debug(
        f"Xrdfs. Stat {name}.\nret:{stdout}\nerr:{stderr}\nprocess ret:{returncode}"
    )
    is_exist, path_type = False, PathType.UNKNOWN
    if returncode == 54 or returncode !=0 :
        logger.debug(f"Xrdfs. {name}. No such file or directory.")
    elif stdout == "" or stdout is None:
        logger.debug(f"Xrdfs. Path {name} doesn't exist.")
    elif "IsDir" in stdout:
        logger.debug(f"Xrdfs. Path {name} IS A DIRECTORY.")
        is_exist, path_type = True, PathType.DIR
    elif "IsReadable" in stdout:
        logger.debug(f"Xrdfs. Path {name} IS A FILE.")
        is_exist, path_type = True, PathType.FILE
    else:
        logger.debug(f"You may don't have permission to acess {name}.")
        is_exist, path_type = True, PathType.FILE

    return is_exist, path_type

#### Return human-readable filesize
def nice_size(size_bytes:int):
    """Format the file size in a human-readable format up to TB."""
    if size_bytes == 0:
        return "0B"
    size_name = ("B", "KB", "MB", "GB", "TB")
    i = min(int(math.floor(math.log(size_bytes, 1024))), len(size_name) - 1)
    p = math.pow(1024, i)
    s = size_bytes / p

    # Determine the number of decimal places to display
    if s.is_integer():
        # If the result is an integer, use no decimal places
        return f"{int(s)} {size_name[i]}"
    else:
        # Otherwise, round to two decimal places
        return f"{s:.2f} {size_name[i]}"

def mode_map(mode:str) -> str:
    """
    mode: 755 like
    """
    return "".join([mode_maps[int(x)] for x in mode])

#### Aynsc run
async def async_exec(cmd, env = {}, timeout = 60, decode = False, src_data = None):
    if src_data:
        process = await asyncio.create_subprocess_exec(
                *cmd,
                env=env,
                stdin=asyncio.subprocess.PIPE,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
    else:
        process = await asyncio.create_subprocess_exec(
                *cmd,
                env=env,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
    ret, err = await asyncio.wait_for(process.communicate(input=src_data), timeout=timeout)
    if decode:
        ret, err = ret.decode("utf-8"), err.decode("utf-8")
    
    return process.returncode, ret, err


async def extract_param(request: Request, params):
    try:
        body = await request.body().json()
        TargetPath = body['Targetpath']
        mode = body['mpde']
        if body['recursive']:
            recursive = bool(body['recursive'])
    except Exception as e:
        logger.error(f"Failed to extract parameters when mkdir. Err:{str(e)}")
        return {"status": InkStatus.PARAM_ERROR, "msg": f"Failed to extract parameters when mkdir. Err:{str(e)}", "data": None}