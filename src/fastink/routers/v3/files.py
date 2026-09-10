from fastapi import APIRouter, HTTPException, Query

from fastink.auth.oidc.principal import Principal
from fastink.auth.scopes import require_scope
from fastink.common.config import get_config
from fastink.storage import common as storage
from fastink.storage.utils import unquote_expand_user

router = APIRouter(tags=["files"])

xrd_host = get_config("storage", "xrd_host")


@router.get("/files")
async def list_files(
    principal: Principal = require_scope("files:read"),
    path: str = Query(...),
    showhidden: bool = Query(False),
):
    username = principal.username
    workdir = unquote_expand_user(dname=path, username=username, url=True)
    if ".." in workdir:
        import os
        workdir = os.path.normpath(workdir)

    is_exist, _ = await storage.path_exist(name=workdir, username=username, mgm=xrd_host)
    if not is_exist:
        raise HTTPException(status_code=404, detail=f"Path not found: {workdir}")

    results = await storage.list_path(
        dname=workdir, username=username, long=True, recursive=False,
        showhidden=showhidden, mgm=xrd_host,
    )
    return {"path": workdir, "entries": results}


@router.get("/files/content")
async def read_file(
    principal: Principal = require_scope("files:read"),
    path: str = Query(...),
):
    username = principal.username
    workdir = unquote_expand_user(dname=path, username=username, url=True)
    content = await storage.cat_file(fname=workdir, username=username, mgm=xrd_host)
    if isinstance(content, (bytes, bytearray)):
        content = content.decode("utf-8", errors="replace")
    return {"path": workdir, "content": content}


@router.post("/files/mkdir", status_code=201)
async def create_dir(
    principal: Principal = require_scope("files:write"),
    path: str = Query(...),
):
    username = principal.username
    workdir = unquote_expand_user(dname=path, username=username, url=True)
    await storage.mkdir(dname=workdir, username=username, mgm=xrd_host)
    return {"path": workdir, "created": True}


@router.delete("/files")
async def delete_path(
    principal: Principal = require_scope("files:delete"),
    path: str = Query(...),
):
    username = principal.username
    workdir = unquote_expand_user(dname=path, username=username, url=True)
    await storage.delete_path(dname=workdir, username=username, mgm=xrd_host)
    return {"path": workdir, "deleted": True}


@router.post("/files/rename")
async def rename_path(
    principal: Principal = require_scope("files:write"),
    src: str = Query(...),
    dst: str = Query(...),
):
    username = principal.username
    src_path = unquote_expand_user(dname=src, username=username, url=True)
    dst_path = unquote_expand_user(dname=dst, username=username, url=True)
    await storage.rename(src=src_path, dst=dst_path, username=username, mgm=xrd_host)
    return {"src": src_path, "dst": dst_path}
