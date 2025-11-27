#!/usr/bin/env python3

import os, traceback, urllib.parse, sys, time

from fastapi import APIRouter, UploadFile, File, Form, Header, HTTPException, Query, Depends, Request
from fastapi.responses import StreamingResponse, UJSONResponse

from src.common.utils import get_uname_from_uid
from src.storage import common
from src.storage.utils import PathType, extract_param, unquote_expand_user
from src.common.logger import logger
from src.routers.headers import get_username
from src.routers.status import *
import jwt

params = common.storage_init()
mgm_url, krb5_enabled = params['mgm_url'], params['krb5_enabled']

xrd_host = mgm_url

router = APIRouter()
@router.get("/get_home", response_class=UJSONResponse)
async def user_home(username: str = Depends(get_username),
                    abspath:bool = Header(default=True,description="Return absolute path")):
    logger.debug(f"User {username}'s home")
    try:
        #tm_start = time.time()
        home = os.path.expanduser(f"~{username}")
        #tm_elapsed  = time.time() - tm_start
        #logger.debug(f"Timer. Get_home for {username} cost: {tm_elapsed:.4f} seconds.")
        return {"status": InkStatus.OK, "msg": "OK", "data": {"home": home}}
    except Exception as e:
        logger.error(f"Failed to get user {username}'s home directory.\nErr:{sys.exc_info()[0]}.\nMsg:{sys.exc_info()[1]}.")
        return  {"status": InkStatus.USER_INVALID, "msg": f"Failed to get {username}'s home directory. Err:{str(e)}", "data": None}

@router.post("/create_dir", response_class=UJSONResponse)
async def mkdir( req: Request,
                 username: str = Depends(get_username)):
    recursive:bool = True
    try:
        body = await req.json()
        logger.debug(f"Create_dir: body {body}")
        TargetPath = body['TargetPath']
        mode = body['mode']
        if 'recursive' in body:
            recursive = bool(body['recursive'])
    except Exception as e:
        logger.error(f"Failed to extract parameters when mkdir. Err:{str(e)}")
        return {"status": InkStatus.PARAM_ERROR, "msg": f"Failed to extract parameters when mkdir. Err:{str(e)}", "data": None}

    if not TargetPath:
        logger.error(f"TargetPath to create is EMPTY")
        return {"status": InkStatus.EMPTY_PATH, "msg": f"TargetPath to create is EMPTY.", "data": None}
    else:
        target_path = unquote_expand_user(dname = TargetPath, username = username, url = True)
    try :
        logger.info(f"Try to create {target_path}")
        # await common.mkdir(dname = target_path, krb5ccname = krb5ccname, username = username, mode = mode, exist_ok = False, mgm = xrd_host)
        await common.mkdir(dname = target_path, username = username, mode = mode, exist_ok = False, mgm = xrd_host)
        return {"status": InkStatus.OK, "msg": f"{target_path} is created successfully.", "data": {}}
    except Exception as e:
        logger.error(f"Failed to create {target_path}. Err: {str(e)}")
        return {"status": InkStatus.DIR_CREATE_ERROR, "msg": f"Failed to create {target_path}. Err: {str(e)}", "data": None}

@router.get("/list_path", response_class=UJSONResponse)
async def fileList(workdir:str = Query(default=None, description = "Directory to list"),
                   recursive:bool = Query(default=False, description = "list in detail"),
                   showhidden:bool = Query(default=False, description = "Show hidden files/dirs"),
                   username: str = Depends(get_username)):
    logger.info(f"List {workdir}")
    try:
        # _, _, krb5ccname = get_krb5cc(uid = None, name = username, krb5 = krb5_enabled)
        logger.debug(f"Processing list request. workdir={workdir}. {type(workdir)}. {len(workdir)}")
        if not workdir or workdir == "''" or workdir == '""':
            logger.debug(f"Work dir is empty. setting to ~{username}")
            workdir = os.path.expanduser(urllib.parse.unquote(f"~{username}", encoding='utf-8'))

        work_directory = unquote_expand_user(dname = workdir, username = username, url = True)

        if '..' in work_directory:
            work_directory = os.path.normpath(work_directory)

        # _, is_dir = await common.path_exist(name = work_directory, krb5ccname = krb5ccname, username = username, mgm = xrd_host)
        _, is_dir = await common.path_exist(name = work_directory, username = username, mgm = xrd_host)
        if is_dir != PathType.DIR:
            return {"status": InkStatus.PATH_NOT_EXIST, "msg": f"Failed to list {work_directory}. Err: Path {work_directory} does not exist.", "data": None}
        logger.debug(f"Processing fileList request {work_directory}.")
        # List all entries excluding those starting with a dot
        #tm_start = time.time()
        # sorted_results = await common.list_dir(dname = work_directory, krb5ccname = krb5ccname, username = username, long = True, recursive = recursive, showhidden = showhidden, mgm = xrd_host)
        sorted_results = await common.list_dir(dname = work_directory, username = username, long = True, recursive = recursive, showhidden = showhidden, mgm = xrd_host)
        #tm_elapsed  = time.time() - tm_start
        #logger.debug(f"Timer. list_path for {username} cost: {tm_elapsed:.4f} seconds.")
    except Exception as e:
        logger.error(f"Failed to list {workdir}.\nErr:{str(e)}.")
        return {"status": InkStatus.FS_UNKNOWN_ERROR, "msg": f"Failed to list {workdir}. Err:{str(e)}", "data": None}
    return {"status": InkStatus.OK, "msg": "OK", "data": sorted_results, "num": len(sorted_results)}

#### Delete a file
@router.post("/delete_path", response_class=UJSONResponse)
async def fileDelete(req: Request,
                     username: str = Depends(get_username)):
    try:
        body = await req.json()
        TargetPath = body['TargetPath']
    except Exception as e:
        logger.error(f"Failed to get parameters when delete. Err:{str(e)}")
        return {"status": InkStatus.PARAM_ERROR, "msg": f"Failed to extract param. Err:{str(e)}", "data":None}

    logger.info(f"Try to delete {TargetPath}")
    TargetPath = urllib.parse.unquote(TargetPath, encoding='utf-8')
    try :
        # _, _, krb5ccname = get_krb5cc(uid = None, name = username, krb5 = krb5_enabled)
        if not TargetPath:
            return {"status": InkStatus.EMPTY_PATH, "msg": "TargetPath is EMPTY.", "data": None}
        target_path = unquote_expand_user(dname = TargetPath, username = username, url = True)
        if target_path[0:2] == '"\'' or target_path[0:2] == '\'"':
            logger.error(f"TargetPath {TargetPath} starting with {TargetPath[0:2]}")
            return {"status": InkStatus.PATH_INVALID, "msg": f"Failed to delete {TargetPath}. TargetPath is illegal.", "data": None}

        logger.info(f"Processing fileDelete requests {TargetPath}")
        # is_exist, _ = await common.path_exist(target_path, krb5ccname = krb5ccname, username = username)
        is_exist, _ = await common.path_exist(name = target_path, username = username)
        if not is_exist:
            logger.error(f"Path to delete {TargetPath} doesn't exist.")
            return {"status": InkStatus.PATH_NOT_EXIST, "msg": f"Failed to delete {TargetPath}. {TargetPath} doesn't exist.", "data": None}
        logger.debug(f"Try to delete {TargetPath}.")
        # status = await common.delete_path(target_path, krb5ccname = krb5ccname, username = username, mgm = xrd_host)
        status = await common.delete_path(target_path, username = username, mgm = xrd_host)
        if not status or status is None:
            logger.error(f"Failed to delete {TargetPath}")
            return {"status": InkStatus.FS_UNKNOWN_ERROR, "msg": f"Failed to delete {TargetPath}.", "data": None}
        logger.info(f"{TargetPath} is deleted.")

    except Exception as e:
        return {"status": InkStatus.FS_UNKNOWN_ERROR, "msg": f"Failed to delete file {TargetPath}. Err: {str(e)}", "data": None}

    return {"status": InkStatus.OK, "msg": "OK", "data": None}

#### Upload a file
@router.post("/upload_file")
async def fileUpload(req: Request, upload_dir: str = Form(...), file: UploadFile = File(...),
                     username: str = Depends(get_username)):
    
    overWrite: bool = False
    try:
        body = await req.json()
        if 'overWrite' in body:
            overWrite = bool(body['overWrite'])
    except Exception as e:
        logger.error(f"Failed to get parameters when download. Err:{str(e)}")
    try:
        # _, _, krb5ccname = get_krb5cc(uid = None, name = username, krb5 = krb5_enabled)
        # Decode file path
        logger.info(f"Start uploading {username}'s file to {upload_dir}/{file.filename} raw.")

        filename = urllib.parse.unquote(file.filename, encoding='utf-8')
        upload_dir = unquote_expand_user(dname = upload_dir, username = username, url = True)
        logger.debug(f"Start uploading file to {upload_dir}/{filename}")
        # Check if filename is empty
        if not filename:
            return {"status": InkStatus.EMPTY_PATH, "msg": "You didn't choose any file to upload.", "data": None}

        #### Create directory if not exist
        # is_exist, path_type = await common.path_exist(upload_dir, krb5ccname, username, mgm = xrd_host)
        is_exist, path_type = await common.path_exist(name = upload_dir, username = username, mgm = xrd_host)
        if not is_exist:
            try:
                # await common.mkdir(upload_dir, krb5ccname, username, exist_ok = True, mgm = xrd_host)
                await common.mkdir(upload_dir, username, exist_ok = True, mgm = xrd_host)
                logger.debug(f"Directory {upload_dir} created successfully!")
            except Exception as e:
                logger.error(f"Failed to create target directory {upload_dir}: {e}")
                return {"status": InkStatus.FS_UNKNOWN_ERROR, "msg": f"Failed to create target directory {upload_dir}. Err: {str(e)}", "data": None}
        elif path_type != PathType.DIR:
                logger.error(f"{upload_dir} is not a directory.")
                return {"status": InkStatus.TYPE_INVALID, "msg": f"Failed to upload file {filename}. {upload_dir} is not a directory.", "data": None}
        else:
            logger.debug(f"{upload_dir} exists. We will upload {filename} into it.")

        # Construct full path to the file to be uploaded
        file_path = os.path.join(upload_dir, filename)
        # is_exist, path_type = await common.path_exist(file_path, krb5ccname, username, mgm = xrd_host)
        is_exist, path_type = await common.path_exist(name = file_path, username = username, mgm = xrd_host)
        if is_exist and path_type == PathType.FILE and not overWrite:
            logger.error(f"Target file {filename} exists. We will not overwrite it.")
            return {"status": InkStatus.NOT_OVERWRITE, "msg": f"Target file {filename} exists. We will not overwrite it.", "data": None}
        # Forbide attack of path traversal
        if ".." in filename or os.path.isabs(filename):
            return {"status": InkStatus.PATH_INVALID, "msg": f"Failed to upload {filename}. Invalid filename.", "data": None}
        # Save the file to the target directory
        
        try:
            # await common.upload_file(src_data = file.file.read(), dst = file_path, krb5ccname = krb5ccname, username = username, mgm = xrd_host)
            await common.upload_file(src_data = file.file.read(), dst = file_path, username = username, mgm = xrd_host)
        except PermissionError as e:
            return {"status": InkStatus.PERMMISSION_DENIED, "msg": f"Failed to upload {filename}. Permission Denied.", "data": None}
        except Exception as e:
            logger.error(f"Failed to upload file {file.filename}: {e}")
            return {"status": InkStatus.FS_UNKNOWN_ERROR, "msg": f"Failed to upload {filename}. An unexpected error occurred: {str(e)}", "data": None}
        
        # Check if the file exists
        #is_exist, path_type = await common.path_exist(file_path, krb5ccname, username = username, mgm = xrd_host)
        is_exist, path_type = await common.path_exist(file_path, username = username, mgm = xrd_host)
        if is_exist and path_type == PathType.FILE:
            return {"status": InkStatus.OK, "msg": f"File {file.filename} uploaded successfully.", "data": None}
        else:
            return {"status": InkStatus.PATH_NOT_EXIST, "msg": f"Failed to upload file to {upload_dir}/{file.filename}. Cannot find it on AIFS.", "data": None}

    except Exception as e:
        logger.error(f"An unexpected error occurred: {str(e)}\n{traceback.format_exc()}")
        return {"status": InkStatus.FS_UNKNOWN_ERROR, "msg": f"Failed to upload file to {upload_dir}/{file.filename}. An unexpected error occurred:: {str(e)}", "data": None}

@router.post("/upload_dir")
async def dirUpload(upload_dir: str = Form(...), file: UploadFile = File(...)):
    pass

async def file_download(TargetPath:str, username:str, krb5_enabled:bool = True):
    logger.debug(f"Start downloading raw: {TargetPath}")
    TargetPath = urllib.parse.unquote(TargetPath, encoding='utf-8')
    logger.debug(f"Start downloading decoded: {TargetPath}")
    TargetPath = urllib.parse.unquote(TargetPath, encoding='utf-8')
    logger.debug(f"Start downloading decoded2: {TargetPath}")
    if "%252F" in TargetPath:
        TargetPath = urllib.parse.unquote(TargetPath, encoding='utf-8')
    if "%2F" in TargetPath:
        TargetPath = urllib.parse.unquote(TargetPath, encoding='utf-8')
    try:
        # if krb5_enabled:
        #    _, _, krb5ccname = get_krb5cc(uid = None, name = username, krb5 = krb5_enabled)
  
        TargetPath = unquote_expand_user(dname = TargetPath, username = username, url = True)
        if not TargetPath:
            return {"status": InkStatus.EMPTY_PATH, "msg": f"Failed to download {TargetPath}. TargetPath is empty", "data": None}
        logger.debug(f"Start downloading {TargetPath}")
        quoted_name=urllib.parse.quote(os.path.basename(TargetPath),'utf-8')
        headers = {"Content-Disposition": f'attachment; filename="{quoted_name}"'}
        # return StreamingResponse(common.get_file_stream(fname = TargetPath, krb5ccname = '', username = username, mgm = xrd_host, krb5_enabled = krb5_enabled), media_type="application/octet-stream", headers=headers)
        return StreamingResponse(common.get_file_stream(fname = TargetPath, username = username, mgm = xrd_host, krb5_enabled = krb5_enabled), media_type="application/octet-stream", headers=headers)
    except ValueError as e:
        logger.error(f'User token is expired or invalid')
        return {"status": InkStatus.TOKEN_INVALID, "msg": f"Failed to download {TargetPath}. User token is expired or invalid.", "data": None}
    except PermissionError as e:
        logger.error(f'Permission denied when access {TargetPath}')
        return {"status": InkStatus.PERMMISSION_DENIED, "msg": f"Failed to download {TargetPath}. Permission denied when access {TargetPath}.", "data": None}
    except Exception as e:
        logger.error(f'Err:{sys.exc_info()[0]}. Msg:{sys.exc_info()[1]}')
        return {"status": InkStatus.FS_UNKNOWN_ERROR, "msg": f"Failed to download {TargetPath}. An unexpected error occurred: {str(e)}", "data": None}

#### FIXME
@router.get("/download_file", response_class=StreamingResponse)
async def fileDownload(TargetPath:str = Query(..., description = "Filename to download"),
                       username: str = Depends(get_username)):
        return await file_download(TargetPath = TargetPath, username = username)

@router.get("/shared_file", response_class=StreamingResponse)
async def download_shared_file(req: Request, TargetPath:str = Query(..., description = "Filename to download")):
    try:
        # 记录jwt.decode执行前的时间
        start_time = time.time()

        # username = req.headers.get("Ink-Username")
        auth_header = req.headers.get("Authorization")
        if auth_header and auth_header.startswith("Bearer "):
            token = auth_header.split(" ")[1]  # 提取 Bearer 后面的 token
            secret_key = 'service-communication-shared-secret-key-123456'
            payload = jwt.decode(
               token,
               key=secret_key,  # 明确指定密钥
               algorithms=["HS256"],  # 必须与Java端一致（HmacSHA256对应HS256）
               options={
                    "verify_signature": True,
                    "verify_iat": False  # 禁用 iat（签发时间）校验
               }
            )
            # 获取存储的信息
            file_path = payload.get("filePath")
            owner = payload.get("owner")
    
            logger.debug(f"filePath: {file_path}")
            logger.debug(f"owner: {owner}")
            logger.debug(f"完整payload信息: {payload}")
        else:
            token = None
            logger.error(f"完整payload信息:NONE")
        # 记录jwt.decode执行后的时间并计算耗时
        end_time = time.time()
        elapsed_time_ms = (end_time - start_time) * 1000
        logger.debug(f"jwt.decode execution time: {elapsed_time_ms:.2f} ms")  

    except Exception as e:
        logger.error(f"Failed to get Ink-Username when download. Err:{str(e)}")
    logger.debug(f"Download shared file {file_path} from {owner}")
    return await file_download(TargetPath = file_path, username = owner, krb5_enabled = False)

@router.get("/view_file", response_class=UJSONResponse)
async def fileCat(TargetPath:str = Query(..., description = "Filename to cat"),
                  username: str = Depends(get_username)):
    """
    View a file
    """
    try:
        logger.debug(f"Trying to cat {TargetPath}")
        fname = unquote_expand_user(dname = TargetPath, username = username, url = True)
        if fname is None or fname == "":
            return {"status": InkStatus.EMPTY_PATH, "msg": "TargetPath is EMPTY.", "data": None}
        # _, _, krb5ccname = get_krb5cc(uid = None, name = username, krb5 = krb5_enabled)
        # contents = await common.cat_file(fname, krb5ccname, username = username, mgm = xrd_host, krb5_enabled = krb5_enabled)
        contents = await common.cat_file(fname, username = username, mgm = xrd_host, krb5_enabled = krb5_enabled)
        return {"status": InkStatus.OK, "msg": "OK", "data": contents}
    except UnicodeDecodeError:
        logger.error(f'Xrdfs. Failed to cat file {fname}.\nErr:\n{sys.exc_info()[0]}\nMsg:\n{sys.exc_info()[1]}')
        return {"status": InkStatus.TYPE_INVALID, "msg": f"File {fname} is not a text file.", "data": None}
    except FileNotFoundError:
        logger.error(f"TargetPath {TargetPath} doesn't exist")
        return {"status": InkStatus.PATH_NOT_EXIST, "msg": f"Failed to cat {TargetPath}. TargetPath {TargetPath} doesn't exist.", "data": None}
    except Exception as e:
        return {"status": InkStatus.FS_UNKNOWN_ERROR, "msg": f"Failed to cat {TargetPath}. An unexpected error: {str(e)}", "data": None}

@router.post("/chmod_path", response_class=UJSONResponse)
async def chmod(req: Request, username:str = Depends(get_username)):
    try:
        body = await req.json()
        TargetPath = body['TargetPath']
        mode = body['mode']

        fname = unquote_expand_user(dname = TargetPath, username = username, url = True)
        if fname is None or fname == "":
            return {"status": InkStatus.EMPTY_PATH, "msg": "TargetPath is EMPTY.", "data": None}

        logger.debug(f"Trying to change {fname}'s permission to {mode}.")
        # _, _, krb5ccname = get_krb5cc(uid = None, name = username, krb5 = krb5_enabled)
        # ret = await common.chmod(fname = fname, krb5ccname = krb5ccname, username = username, mode = mode, mgm = xrd_host)
        ret = await common.chmod(fname = fname, username = username, mode = mode, mgm = xrd_host)
        if ret:
            return {"status": InkStatus.OK, "msg": f"Successfully changed {fname}'s permission to {mode}", "data": None}
        else:
            return {"status": InkStatus.FS_UNKNOWN_ERROR, "msg": f"Failed to change {fname}'s permission.", "data": None}
    except Exception as e:
        logger.error(f"Failed to change {fname}'s permission to {mode}. Err: {str(e)}.")
        return {"status": InkStatus.FS_UNKNOWN_ERROR, "msg": f"Failed to change {fname}'s permission to {mode}. Err: {str(e)}.", "data": None}

@router.post("/rename_path", response_class=UJSONResponse)
async def rename(req: Request, username:str = Depends(get_username)):
    try:
        body = await req.json()
        SrcPath:str  = body['src']
        DstPath:str  = body['dst']
        isDir:bool   = bool(body['isDir'])

        src_name = unquote_expand_user(dname = SrcPath, username = username, url = False)
        dst_name = unquote_expand_user(dname = DstPath, username = username, url = False)
        if src_name is None or src_name == "":
            return {"status": InkStatus.EMPTY_PATH, "msg": "Src Path is EMPTY.", "data": None}
        if dst_name is None or dst_name == "":
            return {"status": InkStatus.EMPTY_PATH, "msg": "Dst Path is EMPTY.", "data": None}

        logger.debug(f"Trying to rename {src_name} to {dst_name}.")
        ret = await common.rename(src = src_name, dst = dst_name, username = username, mgm = xrd_host)
        if ret:
            return {"status": InkStatus.OK, "msg": f"Successfully rename {src_name} to {dst_name}", "data": None}
        else:
            return {"status": InkStatus.FS_UNKNOWN_ERROR, "msg": f"Failed to rename {src_name} to {dst_name}.", "data": None}
    except Exception as e:
        logger.error(f"Failed to perform rename operation. Err: {str(e)}.")
        return {"status": InkStatus.FS_UNKNOWN_ERROR, "msg": f"Failed to perform rename operation. Err: {str(e)}.", "data": None}

if __name__ == "__main__":

    from fastapi import FastAPI
    import uvicorn

    app = FastAPI()
    app.include_router(router,prefix="/api/v1")
    uvicorn.run(app, host="0.0.0.0", port=5001)
