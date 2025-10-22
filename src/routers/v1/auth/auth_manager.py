from fastapi import APIRouter, HTTPException, Query, status
from pydantic import BaseModel
from typing import Optional

from src.auth import user
from src.auth.krb5 import create_krb5, get_krb5
from src.auth.permission import query_user_permissions
from src.common.logger import logger

router = APIRouter()


class Credential(BaseModel):
    username: str
    password: str
    email: Optional[str] = None


class User(BaseModel):
    username: str
    email: str
    uid: Optional[int] = None


@router.post("/auth/token")
def create_token(credential: Credential) -> dict:
    logger.debug(f"Check user {credential.username} existance first")
    existence = user.get_user(username=credential.username, email=credential.email)
    if existence == {}:
        try:
            logger.debug(f"Not found {credential.username}, try to add user")
            user.add_user(username=credential.username, email=credential.email)
        except:
            logger.debug(f"Add user {credential.username} failed, ??????")
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail=f"User create failed",
            )
    try:
        create_krb5(credential.username, credential.password)
        result = {"message": "Token updated successfully"}
    except Exception as err:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail=f"Token update failed: {err}",
        )
    return result


@router.get("/auth/token")
async def get_token(
    username: str = Query(None), email: str = Query(None), uid: int = Query(None)
) -> dict:
    try:
        token = get_krb5(username, email, uid)
        result = {"token": token}
    except Exception as err:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Token retrieval failed: {err}",
        )
    return result


@router.get("/auth/permission")
async def get_permission(
    username: str = Query(None), email: str = Query(None), uid: int = Query(None)
) -> dict:
    try:
        permissions = query_user_permissions(username=username, email=email, uid=uid)
    except Exception as err:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Permission query failed: {err}",
        )
    if permissions:
        result = {"permissions": permissions, "default": permissions[0]}
        if "admin" in result["permissions"]:
            result["identity"] = ["admin", "user"]
        else:
            result["identity"] = ["user"]
    else:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Permission query failed: No permission found for user {username}",
        )
    return result


@router.post("/auth/user")
async def create_user(new_user: User) -> dict:
    result = {"status": 200, "msg": "OK", "data": None}
    try:
        user.add_user(
            username=new_user.username, email=new_user.email, uid=new_user.uid
        )
        result["status"] = 200
        result["msg"] = "OK"
        result["data"] = f"User {new_user.username} created successfully"
    except Exception as e:
        result["status"] = 500
        result["msg"] = "Error"
        result["message"] = f"User {new_user.username} creation failed: {e}"
    return result


@router.get("/auth/user")
async def get_user(
    username: str = Query(None), email: str = Query(None), uid: int = Query(None)
) -> dict:
    result = {"status": 200, "msg": "OK", "data": None}
    user_result = user.get_user(username=username, email=email, uid=uid)
    if user_result:
        result["status"] = 200
        result["msg"] = "OK"
        result["data"] = user_result
    else:
        result["status"] = 500
        result["msg"] = "Error"
        result["data"] = f"User not found"
    return result
