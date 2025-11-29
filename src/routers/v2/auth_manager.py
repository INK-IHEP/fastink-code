from fastapi import APIRouter, Body, Query, Header
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from src.auth import user
from src.auth.krb5 import create_krb5, get_krb5
from src.auth import token as token_manager
from src.auth.permission import query_user_permissions
from src.common.logger import logger
from src.common.config import get_config
from src.routers import headers
from src.routers.status import InkStatus

router = APIRouter()


class Credential(BaseModel):
    username: str
    password: str


@router.post("/create_token")
def create_token(credential: Credential) -> dict:
    username = credential.username
    password = credential.password
    logger.debug(f"Check user {username} existance first")
    existence = user.get_user(username=username)
    if not existence:
        try:
            logger.debug(f"Not found {username}, try to add user")
            user.add_user(username=username)
        except:
            logger.debug(f"Add user {username} failed, this should not happen")
            return {
                "status": InkStatus.USER_CREATION_FAILURE,
                "msg": "User create failed",
                "data": None,
            }
    try:
        create_krb5(username, password)
        result = {
            "status": InkStatus.SUCCESS,
            "msg": "Token updated successfully",
            "data": {"method": "krb5"},
        }
    except Exception as err:
        result = {
            "status": InkStatus.TOKEN_CREATION_FAILURE,
            "msg": f"Token update failed: {err}",
            "data": None,
        }
    return result


@router.get("/get_token")
async def get_token(username: str = Query(None)) -> dict:
    try:
        if get_config("auth", "type") == "krb5":
            token = get_krb5(username)
        elif get_config("auth", "type") == "password":
            token = token_manager.query_token(username)
        result = {
            "status": InkStatus.SUCCESS,
            "msg": "Token obtained successfully",
            "data": {"method": "krb5", "token": token},
        }
    except Exception as err:
        result = {
            "status": InkStatus.TOKEN_INVALID,
            "msg": f"Token retrieval failed: {err}",
            "data": None,
        }
    return result


@router.post("/create_and_get_token")
async def create_and_get_token(credential: Credential) -> dict:
    username = credential.username
    password = credential.password
    logger.debug(f"Check user {username} existance first")
    existence = user.get_user(username=username)
    if not existence:
        try:
            logger.debug(f"Not found {username}, try to add user")
            user.add_user(username=username, password=password)
        except:
            logger.debug(f"Add user {username} failed, this should not happen")
            return {
                "status": InkStatus.USER_CREATION_FAILURE,
                "msg": "User create failed",
                "data": None,
            }
    try:
        if get_config("auth", "type") == "krb5":
            create_krb5(username, password)
            token = get_krb5(username)
        elif get_config("auth", "type") == "password":
            token_manager.create_token(username, password)
            token = token_manager.query_token(username)
        result = {
            "status": InkStatus.SUCCESS,
            "msg": "Token updated successfully",
            "data": {"method": get_config("auth", "type"), "token": token},
        }
    except Exception as err:
        result = {
            "status": InkStatus.TOKEN_CREATION_FAILURE,
            "msg": f"Token update failed: {err}",
            "data": None,
        }
    return result


@router.post("/validate_token")
async def validate_token(
    username: str = Header(None, alias="Ink-Username"),
    token: str = Header(None, alias="Ink-Token"),
) -> dict:
    if headers.validate_token(username, token):
        return {
            "status": InkStatus.SUCCESS,
            "msg": "Token validation successful",
            "data": None,
        }
    else:
        return JSONResponse(
            status_code=401,
            content={
                "status": InkStatus.TOKEN_INVALID,
                "msg": "Token validation failed",
                "data": None,
            },
        )


@router.get("/get_permission")
async def get_permission(username: str = Query(None)) -> dict:
    try:
        permissions = query_user_permissions(username=username)
    except Exception as err:
        return {
            "status": InkStatus.PERMISSION_QUERY_FAILURE,
            "msg": f"Permission query failed: {err}",
            "data": None,
        }
    if permissions:
        result = {
            "status": InkStatus.SUCCESS,
            "msg": "Permissions obtained successfully",
            "data": {"permissions": permissions, "default": permissions[0]},
        }
        if "admin" in result["data"]["permissions"]:
            result["data"]["identity"] = ["admin", "user"]
        else:
            result["data"]["identity"] = ["user"]
    else:
        result = {
            "status": InkStatus.PERMISSION_QUERY_FAILURE,
            "msg": f"Permission query failed: No permission found for user {username}",
            "data": None,
        }
    return result


@router.post("/create_user")
async def create_user(username: str = Body(..., embed=True)) -> dict:
    try:
        user.add_user(username=username)
        result = {
            "status": InkStatus.SUCCESS,
            "msg": f"User {username} created successfully",
            "data": {"username": username},
        }
    except Exception as e:
        result = {
            "status": InkStatus.USER_CREATION_FAILURE,
            "msg": f"User {username} creation failed: {e}",
            "data": None,
        }
    return result


@router.get("/get_user")
async def get_user(username: str = Query(None)) -> dict:

    user_result = user.get_user(username=username)
    if user_result:
        result = {
            "status": InkStatus.SUCCESS,
            "msg": f"User {username} retrieved successfully",
            "data": user_result,
        }
    else:
        result = {
            "status": InkStatus.USER_QUERY_FAILED,
            "msg": f"User {username} query failed",
            "data": None,
        }
    return result
