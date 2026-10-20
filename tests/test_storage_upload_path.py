import asyncio

import pytest

from fastink.storage import xrd
from fastink.storage.utils import PathType

SRC = "/tmp/ink-src"
DST = "/tmp/ink-dst"
MGM = "https://mgm"


def _run(overwrite=True, exec_result=(0, "", ""), post_type=PathType.FILE):
    calls = []

    async def fake_exec(cmd, **kwargs):
        calls.append(cmd)
        return exec_result

    async def fake_path_exist(name, username="", mgm=None):
        return True, post_type

    async def run():
        with pytest.MonkeyPatch.context() as mp:
            mp.setattr(xrd, "get_krb5cc", lambda **kwargs: (None, None, ""))
            mp.setattr(xrd, "xrd_cmd", lambda base, **kwargs: list(base))
            mp.setattr(xrd, "async_exec", fake_exec)
            mp.setattr(xrd, "path_exist", fake_path_exist)
            return await xrd.upload_file_from_path(
                src_path=SRC, dst=DST, username="alice", mgm=MGM, overwrite=overwrite
            )

    result = asyncio.run(run())
    return result, calls


def test_upload_path_omits_force_when_not_overwriting():
    _, calls = _run(overwrite=False)
    cmd = calls[0]
    assert "-f" not in cmd
    assert cmd[:3] == ["xrdcp", "--retry", "3"]
    assert cmd[3] == SRC
    assert cmd[-1].endswith("/tmp/ink-dst")


def test_upload_path_uses_force_when_overwriting():
    _, calls = _run(overwrite=True)
    cmd = calls[0]
    assert cmd[0] == "xrdcp"
    assert "-f" in cmd
    assert cmd[-1].endswith("/tmp/ink-dst")


def test_upload_path_maps_existing_destination():
    with pytest.raises(FileExistsError):
        _run(overwrite=False, exec_result=(1, "", "file already exists"))


def test_upload_path_maps_permission_denied():
    with pytest.raises(PermissionError):
        _run(exec_result=(1, "", "Permission denied"))


def test_upload_path_raises_on_other_failure():
    with pytest.raises(Exception):
        _run(exec_result=(1, "", "boom"))


def test_upload_path_checks_post_condition():
    with pytest.raises(Exception):
        _run(post_type=PathType.UNKNOWN)


def test_upload_path_afs_branch():
    calls = []

    async def fake_exec(cmd, **kwargs):
        calls.append(cmd)
        return 0, "", ""

    async def fake_path_exist(name, username="", mgm=None):
        return True, PathType.FILE

    async def run():
        with pytest.MonkeyPatch.context() as mp:
            mp.setattr(xrd, "get_krb5cc", lambda **kwargs: (None, None, ""))
            mp.setattr(xrd, "async_exec", fake_exec)
            mp.setattr(xrd, "path_exist", fake_path_exist)
            mp.setattr(xrd.subprocess, "check_output", lambda *a, **k: b"")
            return await xrd.upload_file_from_path(
                src_path=SRC, dst="/afs/ihep.ac.cn/user/file", username="alice",
                mgm=MGM, overwrite=False,
            )

    asyncio.run(run())
    cmd = calls[0]
    assert cmd[:3] == ["sudo", "-E", "-u"]
    assert "xrdcp" in cmd
    assert "-f" not in cmd
    assert cmd[-1] == "/afs/ihep.ac.cn/user/file"
