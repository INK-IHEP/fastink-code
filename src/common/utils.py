#!/usr/bin/env python3

import asyncio
import base64
import grp
import os
import pwd
import requests
import subprocess
import sys
import time
import uuid
from datetime import datetime
from fastapi import HTTPException
from functools import wraps

from src.common.logger import logger


def get_uid_from_name(username: str):
    try:
        return pwd.getpwnam(username).pw_uid
    except KeyError:
        return None


def check_krb5_validity(krb5ccname: str) -> bool:
    try:
        tm_now = datetime.now()
        env = {"LC_TIME": ""}
        tm_exp_raw = subprocess.check_output(
            f"klist -f {krb5ccname} | awk '/krbtgt/ {{ print $3 \" \" $4}}'",
            shell=True,
            encoding="UTF-8",
            env=env,
        ).rstrip("\n")
        if tm_exp_raw == "":
            tm_exp = tm_now
        else:
            tm_exp = datetime.strptime(tm_exp_raw, "%m/%d/%y %H:%M:%S")
        tm_dif = (tm_exp - tm_now).total_seconds()
        if tm_dif >= 1800.0:
            logger.debug(f"{krb5ccname} is valid for {tm_dif} seconds.")
            return True
        else:
            logger.debug(f"{krb5ccname} is expired or invalid.")
            return False
    except Exception as e:
        raise e


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

    from src.auth.krb5 import get_krb5

    krb5ccname = f"/tmp/krb5cc_{uid}"
    #### KRB5 token exist
    if os.path.isfile(krb5ccname):
        try:
            #### validate and refresh
            if check_krb5_validity(krb5ccname):
                logger.debug(f"Token file {krb5ccname} is valid. Just return. {uid} {name}")
                return uid, name, krb5ccname
            else:
                logger.info(f"Removing expired krb5 ticket {krb5ccname}.")
                os.remove(krb5ccname)
        except Exception as e:
            raise e
    else:
        try:
            logger.debug(f"Trying to fetch recent krb5 token for {name} and save to {krb5ccname}.")
            krb5_data = get_krb5(name)
            with open(krb5ccname, "wb") as ofile:
                ofile.write(base64.b64decode(krb5_data))
        except Exception as e:
            logger.error(f"Failed to save token to file {krb5ccname}. Err:{str(e)}")
            raise FileNotFoundError(f"Failed to save token to file {krb5ccname}")

    return uid, name, krb5ccname


def get_uid_krb5_by_email(email: str):
    """
    通过调用外部API获取用户的uid和krb5凭证
    """
    try:
        # url = f"http://vmdirac04.ihep.ac.cn:5000/users/email?email={email}"
        # url = f"https://aiweb02.ihep.ac.cn:8802/users/email?email={email}"
        url = f"{krb5_server}/users/email?email={email}"
        #        url = f"https://aiweb02.ihep.ac.cn:8802/users/krb5"
        #        headers={"Content-Type":"application/json"}
        data = {"email": email}
        response = requests.get(url, data=data)

        if response.status_code == 200:
            user_data = response.json()
            if user_data["krb5"] == "null":
                logger.debug(user_data)
                raise HTTPException(
                    status_code=407,
                    detail=response.json().get(
                        "error", f"krb5 token for {user_data['uid']} is null."
                    ),
                )
            euid = os.geteuid()
            #            if euid == 0:
            #               os.seteuid(int(user_data["uid"]))
            with open(f'/tmp/krb5cc_{user_data["uid"]}', "wb") as ofile:
                ofile.write(base64.b64decode(user_data["krb5"]))
            #            if euid == 0:
            #               os.seteuid(0)
            if not check_krb5_validity(f"/tmp/krb5cc_{user_data['uid']}"):
                print(f"The krb5 ticket for {user_data['uid']} is expired")
                raise HTTPException(
                    status_code=504,
                    detail=response.json().get(
                        "error", f"krb5 token for {user_data['uid']} is expired."
                    ),
                )
            return user_data["uid"], user_data["krb5"]
        else:
            raise HTTPException(
                status_code=response.status_code,
                detail=response.json().get(
                    "error", f"Unknown error when get krb5 from {email}"
                ),
            )

    except requests.exceptions.RequestException as e:
        raise e


#        raise HTTPException(status_code=500, detail=f"Failed to connect to user service: {str(e)}")


def get_uid_krb5_token(email: str, uid: int) -> str:

    krb5ccname = f"/tmp/krb5cc_{uid}"

    #### KRB5 token exist
    if os.path.isfile(krb5ccname):
        try:
            #### validate and refresh
            if check_krb5_validity(krb5ccname):
                return krb5ccname
            else:
                euid = os.geteuid()
                if euid != 0 and euid != uid:
                    logger.error(f"Erorr. current uid is not 0 nor {uid}")
                    raise HTTPException(
                        status_code=300, detail=f"User id {uid} is invalid."
                    )
                logger.info(f"Removing expired krb5 ticket {krb5ccname}")
                os.remove(krb5ccname)
                get_uid_krb5_by_email(email)
        except Exception as e:
            #            raise HTTPException(status_code=501, detail="Unknown error when get krb5_token")
            raise e
    else:
        get_uid_krb5_by_email(email)

    return krb5ccname


#### Just get username from uid
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


def query_umt_uid(email):
    url = "https://login.ihep.ac.cn/umt/api/APIComputing?email=%s" % str(email)
    response = requests.get(url)
    return response.json()["result"][0].get("uid")


def query_umt_username(email):
    url = "https://login.ihep.ac.cn/umt/api/APIComputing?email=%s" % str(email)
    response = requests.get(url)
    return response.json()["result"][0].get("afsaccount")


def query_umt_group(email):
    url = "https://login.ihep.ac.cn/umt/api/APIComputing?email=%s" % str(email)
    response = requests.get(url)
    return response.json()["result"][0].get("group2name")


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


### Validate User krb5
def validate_user_krb5(username: str, token: str) -> bool:
    try:
        uid = query_pwd_uid(username)
        krb5ccname = f"/tmp/krb5cc_{uid}"
        token_to_ccachefile(token=token, ccachefile=krb5ccname)
        if not check_krb5_validity(krb5ccname=krb5ccname):
            logger.error(f"Invalid Token for user {username}.")
            return False
        krb5_name = subprocess.check_output(
            f"klist -f {krb5ccname} | awk '/Default/ {{ print $3 }}'",
            shell=True,
            encoding="UTF-8",
        ).split("@")[0]
        logger.debug(f"Specified User: {username}. Real User: {krb5_name}")
        if username != krb5_name:
            logger.error(
                f"User verification failed. Speicified: {username}. Real: {krb5_name}"
            )
            return False
        else:
            return True
    except Exception as e:
        logger.error(f"Error:{sys.exc_info()[0]}. Msg:{sys.exc_info()[1]}")
        raise e
