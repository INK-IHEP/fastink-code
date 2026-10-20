import os
import pwd
import tempfile
from urllib.parse import quote, unquote

from fastapi import APIRouter, File, Form, HTTPException, Query, UploadFile
from fastapi.responses import StreamingResponse

from fastink.auth.principal import Principal
from fastink.auth.scopes import require_scope
from fastink.common.config import get_config
from fastink.common.logger import logger
from fastink.storage import common as storage
from fastink.storage.utils import DEFAULT_MAX_FILE_SIZE, PathType, unquote_expand_user

router = APIRouter(tags=["files"])

xrd_host = get_config("storage", "xrd_host")
_UPLOAD_CHUNK = 1024 * 1024


async def _path_exist(name: str, username: str):
    try:
        return await storage.path_exist(name=name, username=username, mgm=xrd_host)
    except PermissionError:
        raise HTTPException(status_code=403, detail="Permission denied")
    except NotImplementedError:
        raise HTTPException(status_code=501, detail="Operation not supported by this storage backend")
    except Exception:
        logger.exception("v3 storage path_exist failed for %s", name)
        raise HTTPException(status_code=502, detail="Storage backend unavailable")


async def _mkdir(dname: str, username: str):
    try:
        return await storage.mkdir(
            dname=dname, username=username, exist_ok=True, mgm=xrd_host
        )
    except PermissionError:
        raise HTTPException(status_code=403, detail="Permission denied")
    except Exception:
        logger.exception("v3 storage mkdir failed for %s", dname)
        raise HTTPException(status_code=502, detail="Storage backend unavailable")


@router.get("/files")
async def list_files(
    principal: Principal = require_scope("files:read", user_only=True),
    path: str = Query(...),
    showhidden: bool = Query(False),
):
    username = principal.username
    workdir = unquote_expand_user(dname=path, username=username, url=True)
    if ".." in workdir:
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
    principal: Principal = require_scope("files:read", user_only=True),
    path: str = Query(...),
):
    username = principal.username
    workdir = unquote_expand_user(dname=path, username=username, url=True)
    content = await storage.cat_file(fname=workdir, username=username, mgm=xrd_host)
    if isinstance(content, (bytes, bytearray)):
        content = content.decode("utf-8", errors="replace")
    return {"path": workdir, "content": content}


@router.post("/files/upload", status_code=201)
async def upload_file(
    principal: Principal = require_scope("files:write", user_only=True),
    upload_dir: str = Form(...),
    file: UploadFile = File(...),
    overWrite: bool = Form(False),
):
    username = principal.username
    if not upload_dir.strip():
        raise HTTPException(status_code=400, detail="upload_dir is required")

    filename = unquote(file.filename or "")
    if not filename:
        raise HTTPException(status_code=400, detail="filename is required")
    if ".." in filename or os.path.isabs(filename) or "/" in filename or "\\" in filename:
        raise HTTPException(status_code=400, detail="Invalid filename")

    target_dir = unquote_expand_user(dname=upload_dir, username=username, url=True)
    if not target_dir:
        raise HTTPException(status_code=400, detail="upload_dir is required")
    is_exist, path_type = await _path_exist(target_dir, username)
    if not is_exist:
        await _mkdir(target_dir, username)
    elif path_type != PathType.DIR:
        raise HTTPException(status_code=400, detail=f"{target_dir} is not a directory")

    dst = os.path.join(target_dir, filename)
    is_exist, path_type = await _path_exist(dst, username)
    if is_exist:
        if path_type != PathType.FILE:
            raise HTTPException(status_code=409, detail="Target is not a file")
        if not overWrite:
            raise HTTPException(status_code=409, detail="Target already exists")

    try:
        limit = int(
            get_config("storage", "max_file_size", fallback=DEFAULT_MAX_FILE_SIZE)
            or DEFAULT_MAX_FILE_SIZE
        )
    except (TypeError, ValueError):
        limit = DEFAULT_MAX_FILE_SIZE
    if limit <= 0:
        limit = DEFAULT_MAX_FILE_SIZE
    total = 0
    tmp_path = None
    try:
        with tempfile.NamedTemporaryFile(prefix="ink-upload-", delete=False) as tmp:
            tmp_path = tmp.name
            while chunk := await file.read(_UPLOAD_CHUNK):
                total += len(chunk)
                if limit and total > limit:
                    raise HTTPException(
                        status_code=413,
                        detail=f"File exceeds max_file_size ({limit} bytes)",
                    )
                tmp.write(chunk)
        try:
            entry = pwd.getpwnam(username)
        except KeyError:
            os.chmod(tmp_path, 0o600)
        else:
            os.chown(tmp_path, entry.pw_uid, entry.pw_gid)
            os.chmod(tmp_path, 0o600)

        try:
            await storage.upload_file_from_path(
                src_path=tmp_path,
                dst=dst,
                username=username,
                mgm=xrd_host,
                overwrite=overWrite,
            )
        except NotImplementedError:
            raise HTTPException(
                status_code=501,
                detail="Upload is not supported by this storage backend",
            )
        except FileExistsError:
            raise HTTPException(status_code=409, detail="Target already exists")
        except PermissionError:
            raise HTTPException(status_code=403, detail="Permission denied")
        except HTTPException:
            raise
        except Exception:
            logger.exception("v3 upload failed for %s -> %s", username, dst)
            raise HTTPException(status_code=502, detail="Upload failed")
    finally:
        if tmp_path:
            try:
                os.unlink(tmp_path)
            except OSError:
                pass

    return {"path": dst, "bytes": total, "uploaded": True}


@router.get("/files/download")
async def download_file(
    principal: Principal = require_scope("files:read", user_only=True),
    path: str = Query(...),
):
    username = principal.username
    if not path.strip():
        raise HTTPException(status_code=400, detail="path is required")

    workdir = unquote_expand_user(dname=path, username=username, url=True)
    if not workdir:
        raise HTTPException(status_code=400, detail="path is required")
    is_exist, path_type = await _path_exist(workdir, username)
    if not is_exist:
        raise HTTPException(status_code=404, detail=f"Path not found: {workdir}")
    if path_type != PathType.FILE:
        raise HTTPException(status_code=400, detail=f"{workdir} is not a file")

    stream = storage.get_file_stream(fname=workdir, username=username, mgm=xrd_host)
    try:
        first = await stream.__anext__()
    except StopAsyncIteration:
        first = None
    except PermissionError:
        raise HTTPException(status_code=404, detail=f"Path not found: {workdir}")
    except TypeError:
        raise HTTPException(status_code=400, detail=f"{workdir} is not a file")
    except TimeoutError:
        raise HTTPException(status_code=504, detail="Download timed out")
    except (IOError, ValueError) as error:
        message = str(error)
        if "too large" in message:
            raise HTTPException(status_code=413, detail=message)
        logger.exception("v3 download failed for %s", workdir)
        raise HTTPException(status_code=502, detail="Download failed")
    except Exception:
        logger.exception("v3 download failed for %s", workdir)
        raise HTTPException(status_code=502, detail="Download failed")

    async def body():
        try:
            if first is not None:
                yield first
            async for chunk in stream:
                yield chunk
        finally:
            await stream.aclose()

    filename = os.path.basename(workdir)
    return StreamingResponse(
        body(),
        media_type="application/octet-stream",
        headers={
            "Content-Disposition": f"attachment; filename*=UTF-8''{quote(filename, safe='')}"
        },
    )


@router.post("/files/mkdir", status_code=201)
async def create_dir(
    principal: Principal = require_scope("files:write", user_only=True),
    path: str = Query(...),
):
    username = principal.username
    workdir = unquote_expand_user(dname=path, username=username, url=True)
    await storage.mkdir(dname=workdir, username=username, mgm=xrd_host)
    return {"path": workdir, "created": True}


@router.delete("/files")
async def delete_path(
    principal: Principal = require_scope("files:delete", user_only=True),
    path: str = Query(...),
):
    username = principal.username
    workdir = unquote_expand_user(dname=path, username=username, url=True)
    await storage.delete_path(name=workdir, username=username, mgm=xrd_host)
    return {"path": workdir, "deleted": True}


@router.post("/files/rename")
async def rename_path(
    principal: Principal = require_scope("files:write", user_only=True),
    src: str = Query(...),
    dst: str = Query(...),
):
    username = principal.username
    src_path = unquote_expand_user(dname=src, username=username, url=True)
    dst_path = unquote_expand_user(dname=dst, username=username, url=True)
    await storage.rename(src=src_path, username=username, dst=dst_path, mgm=xrd_host)
    return {"src": src_path, "dst": dst_path}
