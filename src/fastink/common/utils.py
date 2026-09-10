#!/usr/bin/env python3

import asyncio
import base64
import grp
import os
import pwd
import subprocess
import time
import uuid
from datetime import datetime
from fastapi import HTTPException
from functools import wraps

from fastink.auth.backends.ccache import check_krb5_validity
from fastink.common.logger import logger
from fastink.common.exception import TokenExpiredException


def get_uid_from_name(username: str):
    try:
        return pwd.getpwnam(username).pw_uid
    except KeyError:
        return None
#### Add krb5 switch
def get_krb5cc(uid: int = None, name: str = None, krb5: bool = True):
    if uid is None and name is None:
        logger.error("uid and name cannot be both None.")
        raise ValueError(f"uid and name cannot be both None.")
    if uid is None:
        uid = get_uid_from_name(name)
    if name is None:
        name = get_uname_from_uid(uid)
    if not krb5:
        return uid, name, ""

    from fastink.auth.backends.krb5 import get_krb5

    krb5ccname = f"/tmp/krb5cc_{uid}"
    #### KRB5 token exist
    if os.path.isfile(krb5ccname):
        try:
            #### validate and refresh
            if check_krb5_validity(krb5ccname):
                logger.debug(f"Token file {krb5ccname} is valid. Just return. {uid} {name}")
                return uid, name, krb5ccname
            else:
                logger.info(f"Token ticket {krb5ccname} is expired. Remove it and Retrieve new token.")
                os.remove(krb5ccname)
        except Exception as e:
            logger.error(f"Error when check {krb5ccname}'s validity. Err:{str(e)}.")
            raise e
    #### Fetch KRB5 token from DB
    try:
        logger.debug(f"Trying to fetch latest krb5 token for {name} and save to {krb5ccname}.")
        krb5_data = get_krb5(name)
        with open(krb5ccname, "wb") as ofile:
            ofile.write(base64.b64decode(krb5_data))
        #### Re-check token validity 
        if check_krb5_validity(krb5ccname):
            logger.debug(f"Retrieved token {krb5ccname} is valid.")
        else:
            logger.warning(
                "Retrieved token for %s is expired or invalid (ccache %s). "
                "Removing ccache.",
                name,
                krb5ccname,
            )
            os.remove(krb5ccname)
            raise TokenExpiredException(f"Retrieved token for {name} is expired or invalid.")
    except Exception as e:
        logger.error(f"Failed to fetch token for {name} and save to file {krb5ccname}. Err:{str(e)}")
        raise e

    return uid, name, krb5ccname


def get_uname_from_uid(uid: int) -> str:
    if uid is None:
        raise HTTPException(status_code=300, detail="User id is None.")
    try:
        name = subprocess.check_output(
            f"id -nu {uid}", shell=True, encoding="UTF-8"
        ).rstrip("\n")
    except Exception as e:
        raise HTTPException(
            status_code=300, detail=f"Unknown error when get username from uid {uid}"
        )
    return name


def generate_uuid():
    return uuid.uuid4()


def ccachefile_to_token(ccachefile: str) -> str:
    if not os.path.exists(ccachefile):
        raise FileNotFoundError(f"ccachefile {ccachefile} does not exist")
    with open(ccachefile, "rb") as f:
        ccache = f.read()
        token = base64.b64encode(ccache).decode("utf-8")
    return token


def token_to_ccachefile(token: str, ccachefile: str) -> None:
    with open(ccachefile, "wb") as f:
        f.write(base64.b64decode(token))


def query_pwd_uid(username: str):
    try:
        uid = pwd.getpwnam(username).pw_uid
        return uid
    except:
        raise Exception(f"User '{username}' not found in passwd.")


def query_pwd_group(username: str):
    gid = pwd.getpwuid(query_pwd_uid(username)).pw_gid
    return grp.getgrgid(gid).gr_name


def convert_to_str(obj):
    if isinstance(obj, datetime):
        return obj.strftime("%Y-%m-%d %H:%M:%S")
    elif isinstance(obj, uuid.UUID):
        return str(obj)
    elif hasattr(obj, "__dict__"):
        return obj.__dict__
    else:
        raise TypeError(f"Cannot serialize {type(obj)}")


def get_version() -> str:
    ver_tag = os.environ.get("SOURCE_COMMIT_TAG")
    ver_sha = os.environ.get("SOURCE_COMMIT_SHA")

    if ver_tag:
        return ver_tag
    if ver_sha:
        return ver_sha
    return "unknown"


def get_version_date() -> str:
    ver_date = os.environ.get("SOURCE_COMMIT_DATE")

    if ver_date:
        return ver_date
    return "unknown"


def timer(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        start = time.perf_counter()
        result = func(*args, **kwargs)
        duration = time.perf_counter() - start
        logger.debug(f"{func.__name__} cost time: {duration:.6f}s")
        return result

    return wrapper
