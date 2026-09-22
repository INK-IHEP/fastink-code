from fastapi import APIRouter, Request

from fastink.auth.oidc.principal import get_current_principal

router = APIRouter(tags=["auth"])


@router.post("/auth/validate")
async def validate_token(request: Request):
    principal = get_current_principal(request)
    return {"valid": True, "username": principal.username}


@router.get("/auth/userinfo")
async def userinfo(request: Request):
    principal = get_current_principal(request)
    return {
        "sub": principal.subject,
        "preferred_username": principal.username,
        "groups": principal.groups or [],
        "scopes": principal.scopes or [],
    }


@router.get("/auth/permissions")
async def permissions(request: Request):
    principal = get_current_principal(request)
    return {
        "username": principal.username,
        "scopes": principal.scopes or [],
    }
