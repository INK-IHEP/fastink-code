#!/usr/bin/env python3

import subprocess, os, sys, re, asyncio
from src.storage.utils import storage_init, PathType, mode_map, nice_size, async_exec, path_stat, unquote_expand_user, async_timer, sync_timer
from src.storage.fuse import get_file_stream, init_ink_space
from src.common.logger import logger
from src.common.utils import get_krb5cc

params = storage_init()
mgm_url, max_file_size, krb5_enabled = params['mgm_url'], params['max_file_size'], params['krb5_enabled']

def xrd_env(krb5ccname:str, krb5_enabled:bool = True):
    if not krb5_enabled or krb5ccname == "" or krb5ccname is None:
        return {"XrdSecPROTOCOL": "unix"}
    else:
        return {"XrdSecPROTOCOL": "krb5,unix", "KRB5CCNAME": krb5ccname}

# @async_timer
async def path_exist(
    name: str, username: str = "", mgm: str = mgm_url
):
    try:
        _, _, krb5ccname = get_krb5cc(uid = None, name = username, krb5 = krb5_enabled)
        env = xrd_env(krb5ccname = krb5ccname, krb5_enabled = krb5_enabled)
        name = unquote_expand_user(dname = name, username = username, url = False)
        cmd = ["xrdfs", mgm, "stat"]
        cmd.append(f'''{name}''') if '"' in name else cmd.append(f"""{name}""")
        logger.debug(f"Xrdfs. xrdfs {mgm} stat {name}. CMD: {cmd}")

        returncode, stdout, stderr = await async_exec(cmd = cmd, env = env, timeout = 20, decode = True)
        return path_stat(name, returncode, stdout, stderr)
    except PermissionError as e:
        logger.error(f"Permission denied when access {name}")
        raise PermissionError(f"Permission denied when access {name}")
    except TimeoutError as e:
        logger.error(f"Timeout when check if {name} exists...")
        return False, PathType.UNKNOWN
    except Exception as e:
        logger.error(f"Err:{sys.exc_info()[0]}\nMsg:{sys.exc_info()[1]}")
        return False, PathType.UNKNOWN

# @async_timer
#### Create directory
async def mkdir(
    dname: str,
    username: str = None,
    mode:str = "755",
    exist_ok: bool = True,
    mgm: str = mgm_url,
) -> bool:
    _, _, krb5ccname = get_krb5cc(uid = None, name = username, krb5 = krb5_enabled)
    env = xrd_env(krb5ccname = krb5ccname, krb5_enabled = krb5_enabled)
    status = False
    try:
        if dname[0:4] == "/afs" :
            subprocess.check_output(f"sudo -E -u {username} aklog", env = env, shell=True, timeout=2)
            cmd = f"sudo -E -u {username} mkdir -m {mode} -p".split()
        else :
            mode = mode_map(mode)
            cmd = ['xrdfs', mgm, 'mkdir', f"-m{mode}",'-p']
        dname = unquote_expand_user(dname = dname, username = username, url = False)
        cmd.append(f'''{dname}''') if '"' in dname else cmd.append(f"""{dname}""")
        logger.debug(f"CMD: {cmd}")

        returncode, ret, err = await async_exec(cmd = cmd, env = env, timeout = 20, decode = True)
        logger.debug(f"Xrd mkdir. CMD: {cmd}.\nret:{ret}. err:{err}")
        if returncode == 0 and err == "":
            logger.info(f"Created {dname} successfully.")
            status = True
        else:
            logger.error(f"Failed to create {dname}.")
            raise PermissionError(f"Failed to create {dname}.\n{err}")
    except Exception as e:
        logger.error(f"Failed to create directory {dname}")
        raise e
    return status

async def chmod(fname:str, username:str, mode:str, mgm:str = mgm_url) -> bool:
    """
    mode format: 111, 755
    """
    _, _, krb5ccname = get_krb5cc(uid = None, name = username, krb5 = krb5_enabled)
    env = xrd_env(krb5ccname = krb5ccname, krb5_enabled = krb5_enabled)
    if fname[0:4] == "/afs":
        cmd = f"sudo -E -u {username} chmod {mode} {fname}".split()
    else:
        mode = mode_map(mode)
        fname = unquote_expand_user(dname = fname, username = username, url = False)
        cmd = f"xrdfs {mgm} chmod {fname} {mode}".split()
        logger.debug(f"Xrdfs chmod: {cmd}")
    returncode, ret, err = await async_exec(cmd = cmd, env = env, timeout = 20, decode = True)
    if returncode !=0 or err != "":
        logger.error(f"Failed to change {fname}'s permission to {mode}. returncode: {returncode}. err: {err} env:{env}")
        raise PermissionError(f"Failed to change {fname}'s permission to {mode}.")
    else:
        return True

# @async_timer
async def list_dir(
    dname: str,
    username: str = "",
    long: bool = True,
    recursive: bool = False,
    showhidden: bool = False,
    mgm: str = mgm_url,
):
    _, _, krb5ccname = get_krb5cc(uid = None, name = username, krb5 = krb5_enabled)
    env = xrd_env(krb5ccname = krb5ccname, krb5_enabled = krb5_enabled)
    option = ""
    contents = []
    sorted_contents = []

    if long:
        option = option + "-l"
    if recursive:
        option = option + " -R"
    if showhidden:
        option = option + " -a"

    cmd = ["xrdfs", mgm, "ls", *option.split()]

    try:
        dname = unquote_expand_user(dname = dname, username = username, url = False)
        cmd.append(f'''{dname}''') if '"' in dname else cmd.append(f"""{dname}""")
        logger.debug(f"Executing ls {option} {dname}. CMD: {cmd}")
        _, ret, err = await async_exec(cmd = cmd, env = env, timeout = 120, decode = True)

        logger.debug(f"contents:\n{len(ret)}:\n{ret}")
        if len(ret) == 0:
            logger.debug(f"{dname} is empty.")
            return []

        for l in ret.split("\n"):
            logger.debug(f"Now processing {l}")
            if len(l) == 0 or l[0:5] == "total":
                logger.debug(f'{dname}. Skip line "{l}"')
                continue

            if l[4] == " ":
                ll = re.split(r"\s+", l, maxsplit=4)
                logger.debug(f"Link: '{l}' : '{ll}' : '{ll[4:]}'")
                fname = os.path.basename(ll[4])
                if fname[0] == "." and not showhidden:
                    continue
                else:
                    contents.append({"type": "directory", "path": ll[4]})
            else:
                if l[0] == "l":
                    idx = l.index(" -> ")
                    l = l[:idx]
                # ll = l.split(' ', 6)

                ll = re.split(r"\s+", l, maxsplit=6)
                fname = os.path.basename(ll[6])

                if fname[0] == "." and not showhidden:
                    continue
                else:
                    if ll[0][0] == "l":
                        full_path = os.path.join(dname, ll[-1])
                        if os.path.isdir(full_path):
                            ll[0] = "drwxrwxrwx"  # f'd{ll[0][1:]}'
                            logger.debug(f"Link. {full_path} is a directory")
                        else:
                            # ll[0] == '-' + ll[0][1:]
                            ll[0] = "-rwxrwxrwx"  # f'd{ll[0][1:]}'
                            logger.debug(f"Link. {full_path} is a file")

                    fsize = nice_size(int(ll[3]))
                    if ll[0][0] == "d":
                        logger.debug(f"Directory: '{l}' : '{ll}' : '{ll[6:]}'")
                        contents.append(
                            {
                                "type": "directory",
                                "permission": ll[0],
                                "user": ll[1],
                                "group": ll[2],
                                "size": fsize,
                                "time": f"{ll[4]} {ll[5]}",
                                "path": ll[6],
                            }
                        )
                    else:
                        logger.debug(f"File: '{l}' : '{ll}' : '{ll[6:]}'")
                        contents.append(
                            {
                                "type": "file",
                                "permission": ll[0],
                                "user": ll[1],
                                "group": ll[2],
                                "size": fsize,
                                "time": f"{ll[4]} {ll[5]}",
                                "path": ll[6],
                            }
                        )
        logger.debug(f"Xrdfs: Successfully ls {dname}.")
        # Sort directories before files
        if contents != []:
            sorted_contents = sorted(
                contents, key=lambda x: (x["type"] != "directory", x["path"])
            )

    except asyncio.TimeoutError as e:
        logger.error(f"Xrdfs: timeout when listing directory {dname}'s content")
        raise asyncio.TimeoutError(
            f"Xrdfs: timeout when listing directory {dname}'s content"
        )
    except:
        logger.error(f"Xrdfs: Failed to list directory {dname}'s content")
        logger.error(f"Err:{sys.exc_info()[0]}\n. Msg:{sys.exc_info()[1]}")
        raise ValueError(f"Xrdfs: Failed to list directory {dname}'s content:")
    return sorted_contents # if len(sorted_contents) <=3000 else sorted_contents[0:3000]+[{"type":"...", "permision":"...", "user":"...", "group":"...","size":"...","time":"...","path":"..."}]

async def xrd_delete_file0(name: str, str, mgm: str = mgm_url) -> bool:
    env = xrd_env(krb5ccname = krb5ccname, krb5_enabled = krb5_enabled)
    is_exist, path_type = await path_exist(name, mgm)

    if not is_exist:
        logger.error(f"PATH {name} doesn't exist.")
        return False

    # cmd = "rmdir" if is_dir else "rm"
    cmd = "rmdir" if path_type == PathType.DIR else "rm"
    try:
        cmd = f"xrdfs {mgm} {cmd} '''{name}'''" if '"' in name else f'xrdfs {mgm} {cmd} """{name}"""'
        returncode, ret, err = await async_exec(cmd = cmd, env = env, timeout = 120, decode = True)
        if returncode == 0 and ret == 0:
            msg = f"{name} is deleted."
        else:
            msg = f"Failed to delete {name}."
    except Exception as e:
        logger.error(f"Failed to deleted {name}.")
        logger.error(f"Err:\n{sys.exc_info()[0]}\n.Msg:\n{sys.exc_info()[1]}")
        raise e

# @async_timer
async def delete_path(
    name: str,
    username: str = "",
    mgm: str = mgm_url,
    krb5_enabled: bool = True,
) -> bool:
    _, _, krb5ccname = get_krb5cc(uid = None, name = username, krb5 = krb5_enabled)
    env = xrd_env(krb5ccname = krb5ccname, krb5_enabled = krb5_enabled)
    is_exist, path_type = await path_exist(name, username, mgm)

    if not is_exist:
        logger.error(f"PATH {name} doesn't exist.")
        return False

    files = []
    dirs = []
    if name[0:4] == "/afs":
        cmd = f"sudo -E -u {username} rm -rf ".split()
        cmd.append(f'''{name}''') if '"' in name else cmd.append(f"""{name}""")
        subprocess.check_output(
            f"sudo -E -u {username} aklog", env=env, shell=True, timeout=2
        )
        logger.debug(f"Xrd DEL CMD: {cmd}")
        returncode, _, err = await async_exec(cmd = cmd, env = env, timeout = 120, decode = True)
        logger.debug(f"Xrd Del. err:{err}.")
        status = True
        if returncode != 0 or err != "":
            msg = f"Failed to delete {name}."
            status = False
        return status

    if path_type == PathType.DIR:
        raw_files = await list_dir(name, long=True, recursive=True, mgm=mgm)
        for f in raw_files:
            if f["type"] == "directory":
                dirs.append(f["path"])
            else:
                files.append(f["path"])
        dirs.append(name)
        logger.debug(dirs)
        logger.debug(files)
    else:
        files.append(name)
    cmd = ""
    try:
        status = True
        #### delete files
        for f in files:
            cmd = ["xrdfs", mgm, "rm"]
            cmd.append(f'''{f}''') if '"' in f else cmd.append(f"""{f}""")
            logger.debug(f"Xrd DEL CMD: {cmd}")

            returncode, _, err = await async_exec(cmd = cmd, env = env, timeout = 120, decode = True)
            logger.debug(f"Xrd Del. {f} err:{err}.")
            if returncode == 0 and err == "":
                msg = f"{f} is deleted."
                logger.error(f"Xrd Del. {returncode}. err:{err}.")
            else:
                logger.error(f"Xrd Del. {returncode}. err:{err}.")
                msg = f"Failed to delete {f}."
                status = False
        for f in dirs:
            cmd = ["xrdfs", mgm, "rmdir"]
            cmd.append(f'''{f}''') if '"' in f else cmd.append(f"""{f}""")
            returncode, _, err = await async_exec(cmd = cmd, env = env, timeout = 120, decode = True)
            logger.debug(f"Xrd Del. err:{err}")
            if returncode != 0 or err != "":
                msg = f"Failed to delete {f}."
                status = False
        return status
    except Exception as e:
        logger.error(
            f"Failed to delete {name}. Err:\n{sys.exc_info()[0]}\n.Msg:\n{sys.exc_info()[1]}"
        )
        raise e

# @async_timer
async def upload_file(
    src_data: bytes,
    dst: str,
    username: str = "",
    mgm: str = mgm_url,
    krb5_enabled: bool = True,
    mode: str = ""
):
    _, _, krb5ccname = get_krb5cc(uid = None, name = username, krb5 = krb5_enabled)
    env = xrd_env(krb5ccname = krb5ccname, krb5_enabled = krb5_enabled)
    upload_status = False
    msg = ""
    cmd = ""
    try:
        logger.info(f'CMD: {cmd} - """{mgm}/{dst}"""')
        cmd = f"sudo -E -u {username} xrdcp -f --retry 3 -".split() if dst[0:4] == "/afs" else [ 'xrdcp', '-f', '--retry', '3', '-']
        if dst[0:4] == "/afs":
            subprocess.check_output(
                f"sudo -E -u {username} aklog", env=env, shell=True, timeout=2
            )
            cmd.append(f'''{dst}''') if '"' in dst else cmd.append(f"""{dst}""")
        else:
            dst = unquote_expand_user(dname = dst, username = username, url = False)
            cmd.append(f'''{mgm}/{dst}''') if '"' in dst else cmd.append(f"""{mgm}/{dst}""")

        logger.debug(f"env:{env}")
        returncode, ret, err = await async_exec(cmd = cmd, env = env, timeout = 1200, decode = True, src_data = src_data)

        logger.debug(f"Xrdfs. Upload_file\nret:{ret}\nerr:{err}")
        if returncode != 0:
            if "permission denied" in err:
                logger.error(
                    f"Failed uploading file to {dst}. Err:{returncode} Msg:{err}"
                )
                raise PermissionError(
                    f"Failed uploading file to {dst}. Permission Denied."
                )
            else:
                logger.error(
                    f"Failed uploading file to {dst}. Err:{returncode} Msg:{err}"
                )
                raise Exception(
                    f"Failed uploading file to {dst}. Err:{returncode} Msg:{err}"
                )
        _, path_type = await path_exist(dst, username, mgm)
        if path_type == PathType.FILE:
            msg = f"File has been uploaded to {dst} successfully."
            logger.info(msg)
        else:
            msg = f"Failed to upload file to {dst}."
            logger.error(msg)

        if mode:
            await chmod(fname = dst, username = username, mode = mode, mgm = mgm_url)

    except PermissionError as e:
        raise PermissionError(f"Failed uploading file to {dst}. Permission Denied.")
    except Exception as e:
        logger.error(
            f"Failed uploading file to {dst}.\nErr:\n{sys.exc_info()[0]}\nMsg:\n{sys.exc_info()[1]}"
        )
        raise e
    return upload_status, msg

async def upload_dir(
    dname: str, krb5cc: bytes, recursive: bool = False, mgm: str = mgm_url
):
    pass

# @async_timer
async def get_file(
    fname: str,
    username: str = "",
    mgm: str = mgm_url,
    krb5_enabled: bool = True,
):
    _, _, krb5ccname = get_krb5cc(uid = None, name = username, krb5 = krb5_enabled)
    env = xrd_env(krb5ccname = krb5ccname, krb5_enabled = krb5_enabled)
    try:
        logger.debug(f"Xrdfs. Start downloading {fname}.")
        fname = unquote_expand_user(dname = fname, username = username, url = False)
        is_exist, path_type = await path_exist(fname, username, mgm)
        if not is_exist:
            logger.error(f"{fname} doesn't exist.")
            raise PermissionError(f"Cannot access {fname}")
        elif path_type != PathType.FILE :
            logger.error(f"{fname} is not a file.")
            raise TypeError(f"{fname} is not a file.")
        else:
            logger.info(f"Start downloading {fname}.")

        cmd = ["xrdfs", mgm, "ls", "-l"]
        cmd.append(f"""{fname}""") if "'" in fname else cmd.append(f'''{fname}''')
        _, ret, err = await async_exec(cmd = cmd, env = env, timeout = 1200, decode = True)
        ret = ret.split()[3]
        logger.debug(f"XrdFS. get_file {fname}. ret: {ret}")
        if ret == "" or int(ret) >= max_file_size:
            logger.error(f"Error. {fname} is too large.")
            raise IOError(f"Error. {fname} is too large.")

        if "'" in fname:
            cmd = ["xrdcp", f"""{mgm}/{fname}""", "-"]
        else:
            cmd = ['xrdcp', f'''{mgm}/{fname}''', '-']
        returncode, ret, err = await async_exec(cmd = cmd, env = env, timeout = 1200, decode = False)
        logger.debug(f"I've got {fname}'s contents")
        if returncode == 0:
            return ret
        else:
            err = err.decode("utf-8")
            logger.error(f"Failed to download file {fname}. Err:{err}")
            raise PermissionError(f"Permission denied when download {fname}.")
    except PermissionError as e:
        logger.error(f"Permission denied when access {fname}.")
        raise e
    except Exception as e:
        logger.error(
            f"Failed to download file {fname}.\nErr:\n{sys.exc_info()[0]}\nMsg:\n{sys.exc_info()[1]}"
        )
        raise e

# @async_timer
async def cat_file(
    fname: str,
    username: str = "",
    mgm: str = mgm_url,
    krb5_enabled: bool = True,
) -> str:
    _, _, krb5ccname = get_krb5cc(uid = None, name = username, krb5 = krb5_enabled)
    env = xrd_env(krb5ccname = krb5ccname, krb5_enabled = krb5_enabled)
    try:
        fname = unquote_expand_user(dname = fname, username = username, url = False)
        is_exist, path_type = await path_exist(fname, username, mgm)
        if not is_exist:
            raise FileNotFoundError(f"Xrdfs: File {fname} not found.")
        if path_type == PathType.DIR:
            raise TypeError(f"{fname} is a directory.")

        cmd = ["xrdfs", mgm, "cat"]
        cmd.append(f"""{fname}""") if "'" in fname else cmd.append(f'''{fname}''')

        returncode, ret, err = await async_exec(cmd = cmd, env = env, timeout = 600, decode = True)
        logger.debug(f"Xrdfs. Cat {fname}.\nret:{ret}\nret:{err}")

        if returncode == 0 and err == "":
            return ret
        else:
            logger.error(f"Failed to cat file {fname}. Err:{err}")
            raise FileNotFoundError(f"Failed to cat file {fname}. Err:{err}")
    except UnicodeDecodeError as e:
        logger.error(
            f"Xrdfs. Failed to cat file {fname}.\nErr:\n{sys.exc_info()[0]}\nMsg:\n{sys.exc_info()[1]}"
        )
        raise e
    except Exception as e:
        logger.error(
            f"Xrdfs. Failed to cat file {fname}.\nErr:\n{sys.exc_info()[0]}\nMsg:\n{sys.exc_info()[1]}"
        )
        raise e

# @async_timer
async def checksum(
    fname: str, cksname: str = "adler32", mgm: str = mgm_url
) -> str:
    _, _, krb5ccname = get_krb5cc(uid = None, name = username, krb5 = krb5_enabled)
    env = xrd_env(krb5ccname)
    try:
        if cksname != "adler32" or cksname != "crc32c":
            logger.error(f"Xrd doesn't support {cksname}")
            raise TypeError(f"Xrd doesn't support {cksname}")
        fname = unquote_expand_user(dname = fname, username = username, url = False)
        cmd = f"xrd{cksname} {mgm} '{fname}".split()
        returncode, ret, err = await async_exec(cmd = cmd, env = env, timeout = 600, decode = True)
        if returncode == 0 and err == '':
            return ret.split()[0]
        else:
            logger.error(f"Failed to get {fname}'s checksum. Err:{err}")
            raise Exception(f"Failed to get {fname}'s checksum. Err:{err}")
    except Exception as e:
        logger.error(
            f"Error when get {fname}'s checksum .\nErr:\n{sys.exc_info()[0]}\nMsg:\n{sys.exc_info()[1]}"
        )
        raise e

#### Rename file or directory
async def rename(src: str, dst:str, username:str, mgm: str = mgm_url) -> bool:
    _, _, krb5ccname = get_krb5cc(uid = None, name = username, krb5 = krb5_enabled)
    env = xrd_env(krb5ccname)
    try:
        src_name = unquote_expand_user(dname = src, username = username, url = False)
        dst_name = unquote_expand_user(dname = dst, username = username, url = False)
        
        is_exist, path_type = await path_exist(src_name, username, mgm)
        if not is_exist:
            raise FileNotFoundError(f"Xrdfs: Source {src_name} not found.")
        if path_type == PathType.UNKNOWN:
            raise TypeError(f"Xrdfs. Source {src_name} UNKNOWN.")
        is_exist, path_type = await path_exist(dst_name, username, mgm)
        if is_exist:
            raise FileExistsError(f"Xrdfs: Dest {dst_name} exist.")

        cmd = f"sudo -E -u {username} mv".split()
        cmd.append(f"""{src_name}""") if "'" in src_name else cmd.append(f'''{src_name}''')
        cmd.append(f"""{dst_name}""") if "'" in dst_name else cmd.append(f'''{dst_name}''')
        returncode, ret, err = await async_exec(cmd = cmd, env = env, timeout = 600, decode = True)
        logger.debug(f"Xrdfs. Rename {src_name} to {dst_name}. \nret:{ret}\nret:{err}")

        if returncode == 0 and err == "":
            return True
        else:
            logger.error(f"Failed to rename {src_name} to {dst_name}. Err:{err}.")
            return False
    except Exception as e:
        logger.error(
            f"Xrdfs. Failed to perform rename operation.\nErr:\n{sys.exc_info()[0]}\nMsg:\n{sys.exc_info()[1]}"
        )
        raise e

if __name__ == "__main__":
    pass

