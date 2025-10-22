#!/usr/bin/env python3

import subprocess, os, sys, re, asyncio, uuid
from src.storage.utils import storage_init, PathType, nice_size, mode_map, async_exec, path_stat, unquote_expand_user, async_timer
from src.common.logger import logger
from shlex import quote

params = storage_init()
mgm_url, max_file_size = params['mgm_url'], params['max_file_size']
nsize = 1024 * 1024 * 20

async def path_exist(
    name: str, username: str = "", mgm: str = mgm_url
):
    is_exist:bool = False
    path_type = PathType.UNKNOWN
    try:
        name = unquote_expand_user(dname = name, username = username, url = False)
        logger.debug(f"Xrdfs. xrdfs {mgm} stat {name}")
        cmd = f"sudo -E -u {username} xrdfs {mgm} stat".split()
        cmd.append(f'''{name}''') if '"' in name else cmd.append(f"""{name}""")

        returncode, stdout, stderr = await async_exec(cmd = cmd, env = {}, timeout = 5, decode = True)
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

#### Create directory
async def mkdir(
    dname: str,
    username: str = "",
    mode: str = "755",
    exist_ok: bool = True,
    mgm: str = mgm_url,
) -> bool:
    status = False
    try:
        if dname[0:4] == "/afs" :
            cmd = f"sudo -E -u {username} mkdir -m {mode} -p".split()
            subprocess.check_output(f"sudo -E -u {username} aklog", shell=True, timeout=2)
        else :
            mode = mode_map(mode)
            cmd = f"sudo -E -u {username} xrdfs {mgm} mkdir -m{mode} -p".split()
        dname = unquote_expand_user(dname = dname, username = username, url = False)
        cmd.append(f'''{dname}''') if '"' in dname else cmd.append(f"""{dname}""")

        returncode, ret, err = await async_exec(cmd = cmd, env = {}, timeout = 5, decode = True)
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

#@async_timer
async def chmod(fname:str, username:str = "", mode:str = "755", mgm:str = mgm_url) -> bool:
    """
    mode format: 111, 755
    """
    if fname[0:4] == "/afs":
        cmd = f"sudo -E -u {username} chmod {mode} {fname}".split()
    else:
        mode = mode_map(mode)
        fname = unquote_expand_user(dname = fname, username = username, url = False)
        cmd = f"sudo -E -u {username} xrdfs {mgm} chmod {fname} {mode}".split()
    returncode, ret, err = await async_exec(cmd = cmd, env = {}, timeout = 5, decode = True)

    if returncode !=0 or err != "":
        logger.error(f"Failed to change {fname}'s permission to {mode}. returncode:{returncode}. err: {err}")
        raise PermissionError(f"Failed to change {fname}'s permission to {mode}. returncode:{returncode}. err: {err}")
    else:
        return True

#@async_timer
async def list_dir(
    dname: str,
    username: str = "",
    long: bool = True,
    recursive: bool = False,
    showhidden: bool = False,
    mgm: str = mgm_url,
):
    option = ""
    contents = []
    sorted_contents = []

    if long:
        option = option + "-l"
    if recursive:
        option = option + " -R"
    if showhidden:
        option = option + " -a"

    cmd = ["sudo","-E", "-u", username, "xrdfs", mgm, "ls", *option.split()]

    try:
        dname = unquote_expand_user(dname = dname, username = username, url = False)
        cmd.append(f'''{dname}''') if '"' in dname else cmd.append(f"""{dname}""")

        logger.debug(f"Executing ls {option} {dname}. CMD: {cmd}")
        _, ret, err = await async_exec(cmd = cmd, env = {}, timeout = 60, decode = True)
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
    return sorted_contents

async def xrd_delete_file0(name: str, mgm: str = mgm_url) -> bool:
    is_exist, path_type = await path_exist(name, mgm)

    if not is_exist:
        logger.error(f"PATH {name} doesn't exist.")
        return False

    cmd = "rmdir" if path_type == PathType.DIR else "rm"
    try:
        if '"' in name:
            cmd = f"xrdfs {mgm} {cmd} '''{name}'''"
        else:
            cmd = f'xrdfs {mgm} {cmd} """{name}"""'
        returncode, ret, err = await async_exec(cmd = cmd, env = {}, timeout = 60, decode = True)
        if returncode == 0 and ret == 0:
            msg = f"{name} is deleted."
        else:
            msg = f"Failed to delete {name}."
    except Exception as e:
        logger.error(f"Failed to deleted {name}.")
        logger.error(f"Err:\n{sys.exc_info()[0]}\n.Msg:\n{sys.exc_info()[1]}")
        raise e

#@async_timer
async def delete_path(
    name: str,
    username: str = "",
    mgm: str = mgm_url,
    krb5_enabled: bool = True,
) -> bool:

    if username == "" or username is None:
        logger.error(f"Username cannot be empty")
        raise ValueError("Username cannot be empty")

    name = unquote_expand_user(dname = name, username = username, url = False)
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
            f"sudo -E -u {username} aklog", shell=True, timeout=2
        )
        logger.debug(f"Xrd DEL CMD: {cmd}")
        returncode, _, err = await async_exec(cmd = cmd, env = {}, timeout = 60, decode = True)

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
            cmd = ["sudo", "-E", "-u", username, "xrdfs", mgm, "rm"]
            cmd.append(f'''{f}''') if '"' in f else cmd.append(f"""{f}""")

            logger.info(f"Xrd DEL CMD: {cmd}")
            returncode, _, err = await async_exec(cmd = cmd, env = {}, timeout = 60, decode = True)
            logger.debug(f"Xrd Del. {f} err:{err}.")
            if returncode == 0 and err == "":
                msg = f"{f} is deleted."
                logger.error(f"Xrd Del. {returncode}. err:{err}.")
            else:
                logger.error(f"Xrd Del. {returncode}. err:{err}.")
                msg = f"Failed to delete {f}."
                status = False
        for f in dirs:
            cmd = ["sudo", "-E", "-u", username, "xrdfs", mgm, "rmdir"]
            cmd.append(f'''{f}''') if '"' in f else cmd.append(f"""{f}""")
            returncode, _, err = await async_exec(cmd = cmd, env = {}, timeout = 60, decode = True)
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

#@async_timer
async def upload_file(
    src_data: bytes,
    dst: str,
    username: str = "",
    mgm: str = mgm_url,
    krb5_enabled: bool = False,
    mode: str = ""
):

    upload_status = False
    msg = ""
    cmd = ""
    try:
        logger.info(f'CMD: {cmd} - """{mgm}/{dst}"""')
        dst = unquote_expand_user(dname = dst, username = username, url = False)
        cmd = f"sudo -E -u {username} xrdcp -f --retry 3 -".split()
        if dst[0:4] == "/afs":
            subprocess.check_output(
                f"sudo -E -u {username} aklog", shell=True, timeout=2
            )
            cmd.append(f'''{dst}''') if '"' in dst else cmd.append(f"""{dst}""")
        else:
            cmd.append(f'''{mgm}/{dst}''') if '"' in dst else cmd.append(f"""{mgm}/{dst}""")

        returncode, ret, err = await async_exec(cmd = cmd, env = {}, timeout = 1200, decode = True, src_data = src_data)

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

#@async_timer
async def get_file_stream(
    fname: str,
    username: str = "",
    mgm: str = mgm_url,
    krb5_enabled: bool = True,
):
    try:
        if username == "" or username is None:
            logger.error(f"Username cannot be empty")
            raise ValueError("Username cannot be empty")
        tmpfile=f"/tmp/ink/tmp-{str(uuid.uuid1())}"
        logger.debug(f"Xrdfs. Start downloading {fname}. Tmpfile {tmpfile}")
        fname = unquote_expand_user(dname = fname, username = username, url = False)
        is_exist, path_type = await path_exist(fname, username, mgm)
        if not is_exist:
            logger.error(f"{fname} doesn't exist.")
            raise PermissionError(f"Cannot access {fname}")
        elif path_type != PathType.FILE:
            logger.error(f"{fname} is not a file.")
            raise TypeError(f"{fname} is note a file.")
        else:
            logger.info(f"Start downloading {fname}.")

        cmd = ["sudo", "-E", "-u", username, "xrdfs", mgm, "ls", "-l"]
        cmd.append(f"""{fname}""") if "'" in fname else cmd.append(f'''{fname}''')
        _, ret, err = await async_exec(cmd = cmd, env = {}, timeout = 1200, decode = True)
        fsize = ret.split()[3]
        logger.debug(f"XrdFS. get_file. ret: {ret}")

        if ret == "" or int(fsize) >= max_file_size:
            logger.error(f"Error. {fname} is too large.")
            raise IOError(f"Error. {fname} is too large.")
        if "'" in fname:
            cmd = ["sudo", "-E", "-u", username, "xrdcp", f"""{mgm}/{fname}""", tmpfile]
        else:
            cmd = ["sudo", "-E", "-u", username, 'xrdcp', f'''{mgm}/{fname}''', tmpfile]

        returncode, ret, err = await async_exec(cmd = cmd, env = {}, timeout = 1200, decode = True)
        if returncode != 0 or err != '':
            # err = err.decode("utf-8")
            logger.error(f"Failed to download file {fname}... Err:{err}")
            raise Exception(f"Failed to download file {fname}... Err:{err}")
        try:
            logger.debug(f"Now streaming {fname}...")
            cmd = f"chown root:root {tmpfile}".split()
            _, _, _  = await async_exec(cmd = cmd, env = {}, timeout = 1200, decode = True)
            with open(tmpfile, 'rb') as infile:
                while chunk := infile.read(nsize):
                    yield chunk

            cmd = f"rm -f {tmpfile}".split()
            _, _, _ = await async_exec(cmd = cmd, env = {}, timeout = 1200, decode = True)
            logger.debug(f"Temp file {tmpfile} deleted.")
        except Exception as e:
            logger.error(f"Error when downloading file {fname}.")
            raise e

        if returncode != 0:
            logger.error(f"Failed to download file {fname}. Err:{err}")
            raise PermissionError(f"Permission denied when download {fname}.")
    except PermissionError as e:
        logger.error(f"Permission denied when access {fname}.")
        raise e
    except Exception as e:
        logger.error(f"Failed to download file {fname}.\nErr:\n{sys.exc_info()[0]}\nMsg:\n{sys.exc_info()[1]}")
        raise e

#@async_timer
async def get_file(
    fname: str,
    username: str = "",
    mgm: str = mgm_url,
    krb5_enabled: bool = True,
):
    try:
        if username == "" or username is None:
            logger.error(f"Username cannot be empty")
            raise ValueError("Username cannot be empty")
        logger.debug(f"Xrdfs. Start downloading {fname}.")
        fname = unquote_expand_user(dname = fname, username = username, url = False)
        is_exist, path_type = await path_exist(fname, username, mgm)
        if not is_exist:
            logger.error(f"{fname} doesn't exist.")
            raise PermissionError(f"Cannot access {fname}")
        elif path_type != PathType.FILE:
            logger.error(f"{fname} is not a file.")
            raise TypeError(f"{fname} is note a file.")
        else:
            logger.info(f"Start downloading {fname}.")

        cmd = ["sudo", "-E", "-u", username, "xrdfs", mgm, "ls", "-l"]
        cmd.append(f"""{fname}""") if "'" in fname else cmd.append(f'''{fname}''')
        _, ret, err = await async_exec(cmd = cmd, env = {}, timeout = 1200, decode = True)
        ret = ret.split()[3]
        logger.debug(f"XrdFS. get_file. ret: {ret}")

        if ret == "" or int(ret) >= max_file_size:
            logger.error(f"Error. {fname} is too large.")
            raise IOError(f"Error. {fname} is too large.")

        if "'" in fname:
            cmd = ["sudo", "-E", "-u", username, "xrdcp", f"""{mgm}/{fname}""", "-"]
        else:
            cmd = ["sudo", "-E", "-u", username, 'xrdcp', f'''{mgm}/{fname}''', '-']
        returncode, ret, err = await async_exec(cmd = cmd, env = {}, timeout = 1200, decode = False)
        logger.debug(f"I've got {fname}'s contents")
        if returncode == 0:
            return ret
        else:
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

#@async_timer
async def cat_file(
    fname: str,
    username: str = "",
    mgm: str = mgm_url,
    krb5_enabled: bool = False,
) -> str:
    try:
        fname = unquote_expand_user(dname = fname, username = username, url = False)
        is_exist, path_type = await path_exist(fname, username, mgm)

        if not is_exist:
            raise FileNotFoundError(f"Xrdfs: File {fname} not found.")
        if path_type == PathType.DIR:
            raise TypeError(f"{fname} is a directory.")

        cmd = ["sudo", "-E", "-u", username, "xrdfs", mgm, "cat"]
        cmd.append(f"""{fname}""") if "'" in fname else cmd.append(f'''{fname}''')
        returncode, ret, err = await async_exec(cmd = cmd, env = {}, timeout = 600, decode = True)

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

#@async_timer
async def checksum(
    fname: str, cksname: str = "adler32", mgm: str = mgm_url
) -> str:
    try:
        if cksname != "adler32" or cksname != "crc32c":
            logger.error(f"Xrd doesn't support {cksname}")
            raise TypeError(f"Xrd doesn't support {cksname}")

        fname = unquote_expand_user(dname = fname, username = username, url = False)
        cmd = f"sudo -E -u {username} xrd{cksname} {mgm} '{fname}".split()
        returncode, ret, err = await async_exec(cmd = cmd, env = {}, timeout = 600, decode = True)

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

async def init_ink_space(username: str, krb5ccname:str, user_group:str, ink_dir:str):
    if not username:
        logger.error(f"init_ink_space: username is EMPTY!")
        raise ValueError(f"init_ink_space: username is EMPTY!")
    
    logger.debug("Init Ink Workspace for user {username}.")
    user_tag = os.path.expanduser(f"~{username}/.ink")
    ink_tag  = f"{ink_dir}/{user_group}/{username}/.ink"
    try:
        is_exist, _ = path_exist(ink_tag, username = username, mgm = mgm_url)
        if not is_exist:
            status = await mkdir(ink_tag, username = username, mode = "755", exist_ok = True, mgm = mgm_url)
            if not status:
                logger.error(f"Failed to create {ink_tag}.")
                return False

        if user_tag == ink_tag:
            return True

        is_exist, _ = path_exist(user_tag, username = username, mgm = mgm_url)
        to_link = True
        if is_exist :
            cmd = ["sudo", "-E", "-u", username, "readlink", "-f", quote(user_tag)]
            returncode, ret, err = await async_exec(cmd = cmd, env = {}, timeout = 10, decode = True, src_data = None)
            if ret != ink_tag:
                if user_tag[0:4] == "/afs":
                    cmd = f"su -s /bin/bash {quote(username)} -c 'export KRB5CCNAME={quote(krb5ccname)} && aklog && rm -rf {quote(user_tag_old)} && mv {quote(user_tag)} {quote(user_tag_old)}'".split()
                else:
                    user_tag_old = f"{user_tag}.old"
                    cmd = f"su -s /bin/bash {quote(username)} -c 'export KRB5CCNAME={quote(krb5ccname)} && rm -rf {quote(user_tag_old)} && mv {quote(user_tag)} {quote(user_tag_old)}'".split()
                returncode, ret, err = await async_exec(cmd, env = {}, timeout = 10, decode = True, src_data = None)
                if returncode != 0 or err != "":
                    logger.error(f"Failed to rename {quote(user_tag)} to {quote(user_tag_old)}.")
                    return False
            else:
                to_link = False
        if to_link:
            cmd = f"su -s /bin/bash {quote(username)} -c 'export KRB5CCNAME={quote(krb5ccname)} && ln -sf {quote(ink_tag)} {quote(user_tag)}'".split()
            returncode, ret, err = await async_exec(cmd = cmd , env = {}, timeout = 10, decode = True, src_data = None)
            if returncode != 0 or err != "" :
                logger.error(f"Failed to create symbolic {quote(user_tag)} -> {quote(ink_tag)}.")
                return False
            return True
    except Exception as e:
        logger.error(f"Failed to create symbolic {quote(user_tag)} -> {quote(ink_tag)}. Err: {str(e)}")
        return False


if __name__ == "__main__":
    pass
