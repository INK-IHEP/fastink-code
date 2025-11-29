#!/usr/bin/env python3

import os, traceback, urllib.parse, sys

from fastapi import APIRouter, UploadFile, File, Form, Header, HTTPException, Query, Depends, Body, Request
from fastapi.responses import StreamingResponse, UJSONResponse

from src.common.utils import get_krb5cc, get_uname_from_uid, validate_user_krb5
from src.storage import common
from src.storage.utils import PathType, unquote_expand_user
from src.common.logger import logger
from src.routers.headers import get_username

params = common.storage_init()
mgm_url, krb5_enabled = params['mgm_url'], params['krb5_enabled']

xrd_host = mgm_url

router = APIRouter()

@router.get("/home", response_class=UJSONResponse)
async def user_home(uid: int = Header(..., description="User's Slurm UID"),
                    email: str = Header(..., description="User's email"),
                    abspath:bool = Header(default=True,description="Return absolute path")):
    logger.debug(f"User {uid}'s home")
    try:
        uname = get_uname_from_uid(uid)
        home = os.path.expanduser(f"~{uname}")
        return {"status": 200, "msg": "OK", "data": home}
    except Exception as e:
        logger.error(f"Failed to get user {uid}'s home directory.\nErr:{sys.exc_info()[0]}.\nMsg:{sys.exc_info()[1]}.")
        raise HTTPException(status_code=512, detail=f"Failed to get user {uid}'s home.")

@router.get("/mkdir", response_class=UJSONResponse)
async def mkdir(target_path:str = Query(default=None, description = "Directory to create"),
                recursive:bool = Query(default=True, description = "Create target path recursively"),
                mode:str =  Query(default="755", description = "Directory mode"),
                uid: int = Header(..., description="User's Slurm UID"),
                email: str = Header(..., description="User's email")):
    if target_path is None or target_path == "":
        logger.error(f"Target Path to create is EMPTY")
        raise HTTPException(status_code = 505, detail = "Target Path is EMPTY.")
    try :
        _, username, krb5ccname = get_krb5cc(uid = uid, name = None, krb5 = krb5_enabled)
        logger.info(f"Try to create {target_path}")
        target_path = unquote_expand_user(dname = target_path, username = username, url = True)

        await common.mkdir(dname = target_path, krb5ccname = krb5ccname, username = username, mode = mode, exist_ok = False, mgm = xrd_host)
        return {"status": 200, "msg": "OK", "data": f"{target_path} is created successfully."}
    except :
        logger.error(f"Failed to create {target_path}.\nErr:{sys.exc_info() [0]}\nMsg:{sys.exc_info()[1]}")
        raise HTTPException(status_code = 506, detail = f"Failed to create {target_path}.")


@router.get("/list", response_class=UJSONResponse)
async def fileList(workdir:str = Query(default=None, description = "Directory to list"),
                   recursive:bool = Query(default=False, description = "list in detail"),
                   showhidden:bool = Query(default=False, description = "Show hidden files/dirs"),
                   uid: int = Header(..., description="User's Slurm UID"),
                   email: str = Header(..., description="User's email")):
    logger.info(f"List {workdir}")
    try:
        _, username, krb5ccname = get_krb5cc(uid = uid, name = None, krb5 = krb5_enabled)
        logger.debug(f"Processing fileList request. workdir={workdir}. {type(workdir)}. {len(workdir)}")
        if workdir is None or workdir == "" or workdir == "''" or workdir == '""':
            print(f"Work dir is empty. setting to ~{username}")
            workdir = os.path.expanduser(urllib.parse.unquote(f"~{username}", encoding='utf-8'))
        work_directory = unquote_expand_user(dname = workdir, username = username, url = True)
        if '..' in work_directory:
            work_directory = os.path.normpath(work_directory)

        _, is_dir = await common.path_exist(name = work_directory, krb5ccname = krb5ccname, username = username, mgm = xrd_host)
        if is_dir != PathType.DIR:
            raise HTTPException(status_code=506, detail=f"Path {work_directory} does not exist.")

        logger.debug(f"Processing fileList request {work_directory}.")
        # List all entries excluding those starting with a dot
        sorted_results = await common.list_dir(dname = work_directory, krb5ccname = krb5ccname, username = username, long = True, recursive = recursive, showhidden = showhidden, mgm = xrd_host)

    except Exception as e:
        logger.error(f"Failed to list {workdir}.\nErr:{sys.exc_info()[0]}\nMsg:{sys.exc_info()[1]}.")
        raise HTTPException(status_code=509, detail=f"Failed to list {workdir}.")
    return {"status": 200, "msg": "OK", "data": sorted_results, "num": len(sorted_results)}

#### Delete a file
@router.get("/delete", response_class=UJSONResponse)
async def fileDelete(TargetPath:str = Query(..., description = "Filepath to delete"),
                     uid: int = Header(..., description="User's Slurm UID"),
                     email: str = Header(..., description="User's email")):
    logger.info(f"Try to delete {TargetPath}")
    TargetPath = urllib.parse.unquote(TargetPath, encoding='utf-8')
    try :
        _, username, krb5ccname = get_krb5cc(uid = uid, name = None, krb5 = krb5_enabled)
        if TargetPath is None or TargetPath == "":
            raise HTTPException(status_code=505, detail=f"TargetPath is EMPTY.")

        target_path = unquote_expand_user(dname = TargetPath, username = username, url = True)
        if target_path[0:2] == '"\'' or target_path[0:2] == '\'"':
            logger.error(f"TargetPath {TargetPath} starting with {TargetPath[0:2]}")
            raise HTTPException(status_code=509, detail=f"TargetPath is illegal.")
        logger.info(f"Processing fileDelete requests {TargetPath}")

        is_exist, _ = await common.path_exist(target_path, krb5ccname = krb5ccname, username = username)
        if not is_exist:
            raise HTTPException(status_code=501, detail=f"Path '{TargetPath}' doesn't exist.")

        logger.debug(f"Try to delete {TargetPath}.")
        status = await common.delete_path(target_path, krb5ccname = krb5ccname, username = username, mgm = xrd_host)
        if not status or status is None:
            logger.error(f"Failed to delete {TargetPath}")
            raise HTTPException(status_code=504, detail=f"Failed to delete {TargetPath}")
        logger.debug(f"{TargetPath} is deleted.")

    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Failed to delete file {TargetPath}. {e}")

    return {"status": 200, "msg": "OK"}

#### Upload a file
@router.post("/upload")
async def fileUpload(upload_dir: str = Form(...), file: UploadFile = File(...),
                     overWrite:bool = Query(default=False, description = "Overwrite existing file"),
                     uid: int = Header(default=None, description="用户的 Slurm UID"),
                     email: str = Header(default=None, description="用户邮箱")):
    try:
        _, username, krb5ccname = get_krb5cc(uid = uid, name = None, krb5 = krb5_enabled)
        # Decode file path
        logger.info(f"Start uploading file to {upload_dir}/{file.filename} raw. uid:{uid}. email:{email}")

        filename = urllib.parse.unquote(file.filename, encoding='utf-8')
        upload_dir = unquote_expand_user(dname = upload_dir, username = username, url = True)

        logger.debug(f"Start uploading file to {upload_dir}/{filename}")
        # Check if filename is empty
        if not filename:
            raise HTTPException(status_code=502, detail=f"You didn't choose any file to upload.")

        #### Create directory if not exist
        is_exist, path_type = await common.path_exist(upload_dir, krb5ccname, username, mgm = xrd_host)
        if not is_exist:
            try:
                await common.mkdir(upload_dir, krb5ccname, username, exist_ok = True, mgm = xrd_host)
                logger.debug(f"Directory {upload_dir} created successfully!")
            except Exception as e:
                logger.error(f"Failed to create target directory {upload_dir}: {e}")
                raise HTTPException(status_code=503, detail=f"Failed to create target directory {upload_dir}.")
        elif path_type != PathType.DIR:
                logger.error(f"{upload_dir} is not a directory.")
                raise HTTPException(status_code=504, detail=f"Target directory {upload_dir} is not a directory.")
        else:
            logger.debug(f"{upload_dir} exists. We will upload {filename} into it.")

        # Construct full path to the file to be uploaded
        file_path = os.path.join(upload_dir, filename)
        is_exist, path_type = await common.path_exist(file_path, krb5ccname, username, mgm = xrd_host)
        if is_exist and path_type == PathType.FILE and not overWrite:
            print(f"Target file {filename} exists. We will not overwrite it.")
            raise HTTPException(status_code=505, detail=f"Target file {filename} exists. We will not overwrite it.")

        # Forbide attack of path traversal
        if ".." in filename or os.path.isabs(filename):
            raise HTTPException(status_code=506, detail=f"Invalid filename.")

        # Save the file to the target directory
        try:
            await common.upload_file(src_data = file.file.read(), dst = file_path, krb5ccname = krb5ccname, username = username, mgm = xrd_host)
        except PermissionError as e:
            raise HTTPException(status_code=513, detail=f"Failed to upload file to {file_path}. Permission Denied.")
        except Exception as e:
            logger.error(f"Failed to upload file {file.filename}: {e}")
#            raise HTTPException(status_code=506, detail=f"Permission denied for UID {uid}: {str(e)}")
            raise HTTPException(status_code=508, detail=f"An unexpected error occurred:: {str(e)}")

        # Check if the file exists
        is_exist, path_type = await common.path_exist(file_path, krb5ccname, username = username, mgm = xrd_host)
        if is_exist and path_type == PathType.FILE:
            return {"status": 200, "msg": f"File {file.filename} uploaded successfully."}
        else:
            raise HTTPException(status_code=507, detail=f"Failed to upload file to {upload_dir}/{file.filename}. Cannot find it on AIFS.")

    except Exception as e:
        logger.error(f"An unexpected error occurred: {str(e)}\n{traceback.format_exc()}")
        raise HTTPException(status_code=508, detail=f"An unexpected error occurred:: {str(e)}")

@router.post("/dirUpload")
async def dirUpload(upload_dir: str = Form(...), file: UploadFile = File(...)):
    pass

#### FIXME
@router.post("/download")
async def fileDownload(request: Request): #,
                       #TargetPath:str = Query(..., description = "Filename to download")): #,
                       #uid: int = Header(default=None, description="用户的 Slurm UID"),
                       #email: str = Header(default=None, description="用户邮箱")):

    try:
        body = await request.body()
        logger.debug(f"Request body: {body}")
        body_data = dict(p.split("=") for p in body.decode("utf-8").split("&"))
        #logger.debug(f"Decoded body: {body_data}")
        uid = body_data["uid"]
        krb5_name = body_data["Ink-Username"]
        krb5_token = urllib.parse.unquote(body_data["Ink-Token"], encoding = "utf-8")
        # logger.debug(f"User {uid}'s token: {krb5_token}")
        TargetPath = body_data['path']
    except Exception as e:
        return {"status": 403, "msg": "Invalid token", "data": {"error":str(e)}}

    if not validate_user_krb5(username = krb5_name, token = krb5_token):
        return {"status": 403, "msg": "User not verified.", "data": {"error":"Invalid username or token."}}

    _, username, krb5ccname = get_krb5cc(uid = uid, name = None, krb5 = krb5_enabled)

    logger.debug(f"Start downloading raw: {TargetPath}")
    TargetPath = urllib.parse.unquote(TargetPath, encoding='utf-8')
    logger.debug(f"Start downloading decoded: {TargetPath}")
    TargetPath = urllib.parse.unquote(TargetPath, encoding='utf-8')
    logger.debug(f"Start downloading decoded2: {TargetPath}")
    if "%252F" in TargetPath:
        TargetPath = urllib.parse.unquote(TargetPath, encoding='utf-8')
    if "%2F" in TargetPath:
        TargetPath = urllib.parse.unquote(TargetPath, encoding='utf-8')
    logger.debug(f"Verified user {username} to download {TargetPath}")

    try:
        TargetPath = unquote_expand_user(dname = TargetPath, username = username, url = True)

        if TargetPath is None or TargetPath == "":
            raise HTTPException(status_code=505, detail=f"TargetPath is EMPTY.")
        logger.debug(f"Start downloading {TargetPath}")
        quoted_name=urllib.parse.quote(os.path.basename(TargetPath),'utf-8')
        headers = {"Content-Disposition": f'attachment; filename="{quoted_name}"'}
        return StreamingResponse(common.get_file_stream(TargetPath, krb5ccname, username, mgm=xrd_host), media_type="application/octet-stream", headers=headers)
    except ValueError as e:
        logger.error(f'User token is expired or invalid')
        raise HTTPException(status_code=510, detail=f"User token is expired or invalid.")
    except PermissionError as e:
        logger.error(f'Permission denied when access {TargetPath}')
        raise HTTPException(status_code=403, detail=f'Permission denied when access {TargetPath}')
    except Exception as e:
        logger.error(f'Err:{sys.exc_info()[0]}. Msg:{sys.exc_info()[1]}')
        raise HTTPException(status_code=504, detail=f"An unexpected error occurred:: {str(e)}")

@router.get("/view", response_class=UJSONResponse)
async def fileCat(TargetPath:str = Query(..., description = "Filename to cat"),
                  uid: int = Header(..., description="User's Slurm UID"),
                  email: str = Header(..., description="User's email")):
    """
    View a file
    """
    try:
        logger.debug(f"Trying to cat {TargetPath}")
        if fname is None or fname == "":
            raise HTTPException(status_code=505, detail=f"TargetPath is EMPTY.")
        _, username, krb5ccname = get_krb5cc(uid = uid, name = None, krb5 = krb5_enabled)
        fname = unquote_expand_user(dname = TargetPath, username = username, url = True)
        contents = await common.cat_file(fname, krb5ccname, username = username, mgm = xrd_host, krb5_enabled = krb5_enabled)
        return {"status": 200, "msg": "OK", "data": contents}
    except UnicodeDecodeError:
        print(f'Xrdfs. Failed to cat file {fname}.\nErr:\n{sys.exc_info()[0]}\nMsg:\n{sys.exc_info()[1]}')
        return {"status": 506, "msg": f"File {fname} is not a text file."}
    except FileNotFoundError:
        print(f"TargetPath {TargetPath} doesn't exist")
        raise HTTPException(status_code=511, detail=f"TargetPath {TargetPath} doesn't exist.")

    except Exception as e:
        raise HTTPException(status_code=505, detail=f"Err:{sys.exc_info()[0]}\nMsg:{sys.exc_info()[1]}")

if __name__ == "__main__":

    from fastapi import FastAPI
    import uvicorn

    app = FastAPI()
    app.include_router(router,prefix="/api/v1")
    uvicorn.run(app, host="0.0.0.0", port=5001)
