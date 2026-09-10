from typing import Optional

from fastapi import APIRouter, Body, Header, Query, Request
from pydantic import BaseModel, Field, constr

from fastink.auth import common, permission, user
from fastink.auth.backends.registry import get_auth_backend
from fastink.common.config import get_config
from fastink.common.logger import logger
from fastink.routers import headers
from fastink.routers.status import InkStatus


router = APIRouter()
NonEmptyString = constr(strip_whitespace=True, min_length=1)


class CreateUser(BaseModel):
    username: NonEmptyString
    uid: Optional[int] = Field(None, ge=0)
    password: Optional[str] = None


class UpdateUser(BaseModel):
    username: NonEmptyString
    new_username: Optional[NonEmptyString] = None
    new_uid: Optional[int] = Field(None, ge=0)


class UserPermission(BaseModel):
    username: NonEmptyString
    permission: NonEmptyString


class PermissionName(BaseModel):
    permission: NonEmptyString


class GroupPermission(BaseModel):
    group_name: NonEmptyString
    permission: NonEmptyString


def _denied() -> dict:
    return {
        "status": InkStatus.PERMMISSION_DENIED,
        "msg": "Admin permission is required",
        "data": None,
    }


def _authorize(
    request: Request, caller: Optional[str], token: Optional[str]
) -> Optional[dict]:
    principal = getattr(request.state, "principal", None)
    if principal is not None:
        caller = principal.username
    try:
        if principal is None and (
            not caller or not token or not headers.validate_token(caller, token)
        ):
            return _denied()
        if not permission.check_user_permission(
            username=caller, permission="admin"
        ):
            return _denied()
    except Exception as error:
        logger.debug("Admin permission check failed for %s: %s", caller, error)
        return _denied()
    return None


def _success(message: str, data=None) -> dict:
    return {"status": InkStatus.SUCCESS, "msg": message, "data": data}


def _failure(status: InkStatus, message: str, error: Exception = None) -> dict:
    if error is not None:
        logger.warning("Admin endpoint failure: %s: %s", message, error)
    return {"status": status, "msg": message, "data": None}


def _permission_id(permission_name: str):
    return common.get_permission(permission=permission_name)["id"]


@router.get("/get_user")
def get_user(
    request: Request,
    username: str = Query(..., min_length=1),
    caller: Optional[str] = Header(None, alias="Ink-Username"),
    token: Optional[str] = Header(None, alias="Ink-Token"),
) -> dict:
    if denied := _authorize(request, caller, token):
        return denied
    try:
        result = user.get_user(username=username)
    except Exception as error:
        return _failure(InkStatus.USER_QUERY_FAILURE, "User query failed", error)
    if not result:
        return _failure(InkStatus.USER_QUERY_FAILURE, f"User {username} not found")
    return _success(f"User {username} retrieved successfully", result)


@router.get("/list_users")
def list_users(
    request: Request,
    caller: Optional[str] = Header(None, alias="Ink-Username"),
    token: Optional[str] = Header(None, alias="Ink-Token"),
) -> dict:
    if denied := _authorize(request, caller, token):
        return denied
    try:
        return _success("Users listed successfully", user.list_users())
    except Exception as error:
        return _failure(InkStatus.USER_QUERY_FAILURE, "User list failed", error)


@router.post("/create_user")
def create_user(
    request: Request,
    payload: CreateUser,
    caller: Optional[str] = Header(None, alias="Ink-Username"),
    token: Optional[str] = Header(None, alias="Ink-Token"),
) -> dict:
    if denied := _authorize(request, caller, token):
        return denied
    if get_config("auth", "type") == "password" and payload.password is None:
        return _failure(
            InkStatus.PARAM_ERROR,
            "A password is required on password-auth deployments",
        )
    try:
        options = {"uid": payload.uid}
        if payload.password is not None:
            options["password"] = payload.password
        user.add_user(username=payload.username, **options)
        return _success(
            f"User {payload.username} created successfully",
            {"username": payload.username},
        )
    except Exception as error:
        return _failure(InkStatus.USER_CREATION_FAILURE, "User creation failed", error)


@router.post("/update_user")
def update_user(
    request: Request,
    payload: UpdateUser,
    caller: Optional[str] = Header(None, alias="Ink-Username"),
    token: Optional[str] = Header(None, alias="Ink-Token"),
) -> dict:
    if denied := _authorize(request, caller, token):
        return denied
    if (
        caller
        and payload.new_username is not None
        and payload.username.strip().casefold() == caller.strip().casefold()
    ):
        return _failure(
            InkStatus.PERMMISSION_DENIED,
            "Renaming your own account is not allowed",
        )
    try:
        updated = user.update_user(
            username=payload.username,
            new_username=payload.new_username,
            new_uid=payload.new_uid,
        )
    except Exception as error:
        return _failure(InkStatus.USER_QUERY_FAILURE, "User update failed", error)
    if updated is False:
        return _failure(InkStatus.USER_QUERY_FAILURE, "User update failed")
    return _success(
        f"User {payload.username} updated successfully",
        {"username": payload.new_username or payload.username},
    )


@router.post("/delete_user")
def delete_user(
    request: Request,
    username: str = Body(..., embed=True),
    caller: Optional[str] = Header(None, alias="Ink-Username"),
    token: Optional[str] = Header(None, alias="Ink-Token"),
) -> dict:
    if denied := _authorize(request, caller, token):
        return denied
    if caller and username.strip().casefold() == caller.strip().casefold():
        return _failure(
            InkStatus.PERMMISSION_DENIED,
            "Deleting your own account is not allowed",
        )
    try:
        deleted = user.delete_user(username=username)
    except Exception as error:
        return _failure(InkStatus.USER_QUERY_FAILURE, "User deletion failed", error)
    if not deleted:
        return _failure(InkStatus.USER_QUERY_FAILURE, f"User {username} deletion failed")
    return _success(f"User {username} deleted successfully", {"username": username})


@router.get("/get_user_token")
def get_user_token(
    request: Request,
    username: str = Query(..., min_length=1),
    caller: Optional[str] = Header(None, alias="Ink-Username"),
    token: Optional[str] = Header(None, alias="Ink-Token"),
) -> dict:
    if denied := _authorize(request, caller, token):
        return denied
    try:
        user_token = get_auth_backend().get_token(username)
        return _success(
            f"Token for user {username} retrieved successfully",
            {"method": get_config("auth", "type"), "token": user_token},
        )
    except Exception as error:
        return _failure(InkStatus.TOKEN_INVALID, "User token retrieval failed", error)


@router.get("/get_user_permissions")
def get_user_permissions(
    request: Request,
    username: str = Query(..., min_length=1),
    caller: Optional[str] = Header(None, alias="Ink-Username"),
    token: Optional[str] = Header(None, alias="Ink-Token"),
) -> dict:
    if denied := _authorize(request, caller, token):
        return denied
    try:
        permissions = permission.query_user_permissions(username=username)
        return _success(
            f"Permissions for user {username} retrieved successfully", permissions
        )
    except Exception as error:
        return _failure(InkStatus.PERMISSION_QUERY_FAILURE, "Permission query failed", error)


@router.post("/add_user_permission")
def add_user_permission(
    request: Request,
    payload: UserPermission,
    caller: Optional[str] = Header(None, alias="Ink-Username"),
    token: Optional[str] = Header(None, alias="Ink-Token"),
) -> dict:
    if denied := _authorize(request, caller, token):
        return denied
    try:
        added = permission.add_user_permission(
            username=payload.username, permission=payload.permission
        )
    except Exception as error:
        return _failure(InkStatus.PERMISSION_QUERY_FAILURE, "User permission add failed", error)
    if not added:
        return _failure(InkStatus.PERMISSION_QUERY_FAILURE, "User permission add failed")
    return _success(
        "User permission added successfully",
        {"username": payload.username, "permission": payload.permission},
    )


@router.post("/delete_user_permission")
def delete_user_permission(
    request: Request,
    payload: UserPermission,
    caller: Optional[str] = Header(None, alias="Ink-Username"),
    token: Optional[str] = Header(None, alias="Ink-Token"),
) -> dict:
    if denied := _authorize(request, caller, token):
        return denied
    if (
        caller
        and payload.username.strip().casefold() == caller.strip().casefold()
        and payload.permission.strip().casefold() == "admin"
    ):
        return _failure(
            InkStatus.PERMMISSION_DENIED,
            "Removing your own admin permission is not allowed",
        )
    try:
        deleted = permission.delete_user_permission(
            username=payload.username, permission=payload.permission
        )
    except Exception as error:
        return _failure(
            InkStatus.PERMISSION_QUERY_FAILURE, "User permission deletion failed", error
        )
    if not deleted:
        return _failure(InkStatus.PERMISSION_QUERY_FAILURE, "User permission deletion failed")
    return _success(
        "User permission deleted successfully",
        {"username": payload.username, "permission": payload.permission},
    )


@router.get("/list_permissions")
def list_permissions(
    request: Request,
    caller: Optional[str] = Header(None, alias="Ink-Username"),
    token: Optional[str] = Header(None, alias="Ink-Token"),
) -> dict:
    if denied := _authorize(request, caller, token):
        return denied
    try:
        return _success("Permissions listed successfully", common.get_all_permissions())
    except Exception as error:
        return _failure(InkStatus.PERMISSION_QUERY_FAILURE, "Permission list failed", error)


@router.post("/create_permission")
def create_permission(
    request: Request,
    payload: PermissionName,
    caller: Optional[str] = Header(None, alias="Ink-Username"),
    token: Optional[str] = Header(None, alias="Ink-Token"),
) -> dict:
    if denied := _authorize(request, caller, token):
        return denied
    try:
        added = permission.add_permission(permission=payload.permission)
    except Exception as error:
        return _failure(InkStatus.PERMISSION_QUERY_FAILURE, "Permission creation failed", error)
    if not added:
        return _failure(InkStatus.PERMISSION_QUERY_FAILURE, "Permission creation failed")
    return _success("Permission created successfully", {"permission": payload.permission})


@router.post("/delete_permission")
def delete_permission(
    request: Request,
    payload: PermissionName,
    caller: Optional[str] = Header(None, alias="Ink-Username"),
    token: Optional[str] = Header(None, alias="Ink-Token"),
) -> dict:
    if denied := _authorize(request, caller, token):
        return denied
    if payload.permission.strip().casefold() == "admin":
        return _failure(
            InkStatus.PERMMISSION_DENIED,
            "The built-in admin permission cannot be deleted",
        )
    try:
        deleted = permission.delete_permission(permission=payload.permission)
    except Exception as error:
        return _failure(InkStatus.PERMISSION_QUERY_FAILURE, "Permission deletion failed", error)
    if not deleted:
        return _failure(InkStatus.PERMISSION_QUERY_FAILURE, "Permission deletion failed")
    return _success("Permission deleted successfully", {"permission": payload.permission})


@router.get("/get_users_by_permission")
def get_users_by_permission(
    request: Request,
    permission_name: str = Query(..., min_length=1, alias="permission"),
    caller: Optional[str] = Header(None, alias="Ink-Username"),
    token: Optional[str] = Header(None, alias="Ink-Token"),
) -> dict:
    if denied := _authorize(request, caller, token):
        return denied
    try:
        users = permission.get_users_by_permission_including_groups(
            permission_name=permission_name
        )
        return _success("Users by permission listed successfully", users)
    except Exception as error:
        return _failure(InkStatus.PERMISSION_QUERY_FAILURE, "Users by permission query failed", error)


@router.get("/list_group_permissions")
def list_group_permissions(
    request: Request,
    group: Optional[str] = Query(None, min_length=1),
    caller: Optional[str] = Header(None, alias="Ink-Username"),
    token: Optional[str] = Header(None, alias="Ink-Token"),
) -> dict:
    if denied := _authorize(request, caller, token):
        return denied
    try:
        mappings = common.get_all_group_permissions()
        if group:
            mappings = [mapping for mapping in mappings if mapping["group_name"] == group]
        return _success("Group permissions listed successfully", mappings)
    except Exception as error:
        return _failure(InkStatus.PERMISSION_QUERY_FAILURE, "Group permission list failed", error)


@router.post("/add_group_permission")
def add_group_permission(
    request: Request,
    payload: GroupPermission,
    caller: Optional[str] = Header(None, alias="Ink-Username"),
    token: Optional[str] = Header(None, alias="Ink-Token"),
) -> dict:
    if denied := _authorize(request, caller, token):
        return denied
    try:
        common.add_group_permission(
            group_name=payload.group_name,
            permission_id=_permission_id(payload.permission),
        )
        return _success(
            "Group permission added successfully",
            {"group_name": payload.group_name, "permission": payload.permission},
        )
    except Exception as error:
        return _failure(InkStatus.PERMISSION_QUERY_FAILURE, "Group permission add failed", error)


@router.post("/delete_group_permission")
def delete_group_permission(
    request: Request,
    payload: GroupPermission,
    caller: Optional[str] = Header(None, alias="Ink-Username"),
    token: Optional[str] = Header(None, alias="Ink-Token"),
) -> dict:
    if denied := _authorize(request, caller, token):
        return denied
    if (
        payload.permission.strip().casefold() == "admin"
        and not permission.can_remove_group_admin_source(caller, payload.group_name)
    ):
        return _failure(
            InkStatus.PERMMISSION_DENIED,
            "Removing your own admin group permission is not allowed",
        )
    try:
        deleted = common.delete_group_permission(
            group_name=payload.group_name,
            permission_id=_permission_id(payload.permission),
        )
    except Exception as error:
        return _failure(
            InkStatus.PERMISSION_QUERY_FAILURE,
            "Group permission deletion failed",
            error,
        )
    if not deleted:
        return _failure(InkStatus.PERMISSION_QUERY_FAILURE, "Group permission deletion failed")
    return _success(
        "Group permission deleted successfully",
        {"group_name": payload.group_name, "permission": payload.permission},
    )
