from fastapi import APIRouter, Body, Query, Header, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from fastink.auth import permission, user
from fastink.auth.backends.errors import (
    AccountExpiredError,
    PasswordExpiredError,
    UserNotFoundError,
)
from fastink.auth.backends.registry import get_auth_backend
from fastink.auth.permission import query_user_permissions
from fastink.common.logger import logger
from fastink.common.config import get_config, get_full_config
from fastink.routers import headers
from fastink.routers.status import InkStatus

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
        get_auth_backend().create_token(username, password)
        result = {
            "status": InkStatus.SUCCESS,
            "msg": "Token updated successfully",
            "data": {"method": get_config("auth", "type")},
        }
    except NotImplementedError as err:
        result = {
            "status": InkStatus.TOKEN_CREATION_FAILURE,
            "msg": f"Token creation not supported by this auth backend: {err}",
            "data": None,
        }
    except UserNotFoundError:
        result = {
            "status": InkStatus.USER_NOT_FOUND,
            "msg": f"User {username} does not exist",
            "data": None,
        }
    except AccountExpiredError:
        result = {
            "status": InkStatus.ACCOUNT_EXPIRED,
            "msg": f"Account {username} is expired",
            "data": None,
        }
    except PasswordExpiredError:
        result = {
            "status": InkStatus.PASSWORD_EXPIRED,
            "msg": f"Password for {username} has expired",
            "data": None,
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
        token = get_auth_backend().get_token(username)
        result = {
            "status": InkStatus.SUCCESS,
            "msg": "Token obtained successfully",
            "data": {"method": get_config("auth", "type"), "token": token},
        }
    except NotImplementedError as err:
        result = {
            "status": InkStatus.TOKEN_INVALID,
            "msg": f"Token retrieval not supported by this auth backend: {err}",
            "data": None,
        }
    except UserNotFoundError:
        result = {
            "status": InkStatus.USER_NOT_FOUND,
            "msg": f"User {username} does not exist",
            "data": None,
        }
    except AccountExpiredError:
        result = {
            "status": InkStatus.ACCOUNT_EXPIRED,
            "msg": f"Account {username} is expired",
            "data": None,
        }
    except PasswordExpiredError:
        result = {
            "status": InkStatus.PASSWORD_EXPIRED,
            "msg": f"Password for {username} has expired",
            "data": None,
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
        backend = get_auth_backend()
        try:
            backend.create_token(username, password)
        except NotImplementedError as err:
            return {
                "status": InkStatus.TOKEN_CREATION_FAILURE,
                "msg": f"Token creation not supported by this auth backend: {err}",
                "data": None,
            }
        token = backend.get_token(username)
        result = {
            "status": InkStatus.SUCCESS,
            "msg": "Token updated successfully",
            "data": {"method": get_config("auth", "type"), "token": token},
        }
    except UserNotFoundError:
        result = {
            "status": InkStatus.USER_NOT_FOUND,
            "msg": f"User {username} does not exist",
            "data": None,
        }
    except AccountExpiredError:
        result = {
            "status": InkStatus.ACCOUNT_EXPIRED,
            "msg": f"Account {username} is expired",
            "data": None,
        }
    except PasswordExpiredError:
        result = {
            "status": InkStatus.PASSWORD_EXPIRED,
            "msg": f"Password for {username} has expired",
            "data": None,
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
        return {
            "status": InkStatus.TOKEN_INVALID,
            "msg": "Token validation failed",
            "data": None,
        }


@router.get("/auth_request")
async def auth_request(
    username: str = Header(None, alias="Ink-Username"),
    token: str = Header(None, alias="Ink-Token"),
):
    if not username or not token:
        return {
            "status": InkStatus.TOKEN_INVALID,
            "msg": "Ink-Username or Ink-Token is missing in request headers",
            "data": None,
        }

    if headers.validate_token(username, token):
        return JSONResponse(
            status_code=200,
            content={
                "status": InkStatus.SUCCESS,
                "msg": "Token validation successful",
                "data": None,
            },
            headers={
                "X-Auth-Request-User": username,
            },
        )

    return {
        "status": InkStatus.TOKEN_INVALID,
        "msg": "Token validation failed",
        "data": None,
    }


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


@router.get("/get_config")
def get_backend_config(
    request: Request,
    caller: str = Header(None, alias="Ink-Username"),
    token: str = Header(None, alias="Ink-Token"),
) -> dict:
    """Return the full backend config to whitelisted service IPs or admins.

    Access control lives in this handler (the path is token-exempt via
    skip_routers): requests from an IP in security.ip_whitelist are
    allowed without credentials (the inkfront reverse proxy runs on such
    a host); everyone else must present a valid Ink-Username/Ink-Token
    pair with the "admin" permission.

    The whitelist is checked against request.client.host (the TCP peer),
    NOT X-Real-IP, because the inkfront proxy forwards the browser IP in
    X-Real-IP and that header is trivially forgeable.
    """
    client_ip = request.client.host
    try:
        authorized = headers.ip_is_whitelisted(
            client_ip, get_config("security", "ip_whitelist")
        ) or (
            caller
            and token
            and headers.validate_token(caller, token)
            and permission.check_user_permission(
                username=caller, permission="admin"
            )
        )
    except Exception as error:
        logger.debug("Config access denied for %s: %s", caller, error)
        authorized = False

    if authorized:
        return {
            "status": InkStatus.SUCCESS,
            "msg": "Config obtained successfully",
            "data": get_full_config(),
        }

    return {
        "status": InkStatus.PERMMISSION_DENIED,
        "msg": "Access denied: whitelisted service IP or admin token required",
        "data": None,
    }


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
            "status": InkStatus.USER_QUERY_FAILURE,
            "msg": f"User {username} query failed",
            "data": None,
        }
    return result
