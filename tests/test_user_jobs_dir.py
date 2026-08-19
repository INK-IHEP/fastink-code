"""Tests for /api/v2/cr/get_user_jobs_dir and the underlying resolver."""
from __future__ import annotations

from unittest import mock

from fastink.computing.tools.common.utils import get_user_ink_dir, get_user_jobs_dir


class TestResolver:
    """get_user_jobs_dir = get_user_ink_dir + /.ink/Jobs (pure logic)."""

    def test_template_with_user_group(self):
        with mock.patch(
            "fastink.computing.tools.common.utils.get_config",
            return_value="/workfs2/{user_group}/{username}",
        ), mock.patch(
            "fastink.computing.tools.common.utils.get_user_exp_group",
            return_value=("CC", "u07"),
        ):
            assert get_user_jobs_dir("alice", 1001) == "/workfs2/u07/alice/.ink/Jobs"

    def test_template_username_only(self):
        with mock.patch(
            "fastink.computing.tools.common.utils.get_config",
            return_value="/home/{username}",
        ):
            assert get_user_jobs_dir("bob", 1002) == "/home/bob/.ink/Jobs"

    def test_tilde_falls_back_to_expanduser(self):
        with mock.patch(
            "fastink.computing.tools.common.utils.get_config",
            return_value="~",
        ), mock.patch(
            "fastink.computing.tools.common.utils.os.path.expanduser",
            return_value="/afs/ihep.ac.cn/users/c/carol",
        ):
            assert get_user_ink_dir("carol") == "/afs/ihep.ac.cn/users/c/carol"
            assert get_user_jobs_dir("carol") == "/afs/ihep.ac.cn/users/c/carol/.ink/Jobs"


class TestEndpointShape:
    """The route returns the standard three-part envelope with jobs_dir only."""

    def test_success_envelope(self):
        # Call the route coroutine directly with patched resolver deps.
        import asyncio
        from fastink.routers.v2 import compute_resources as cr

        with mock.patch.object(cr, "change_username_to_uid", return_value=1001), \
             mock.patch.object(cr, "get_user_jobs_dir",
                               return_value="/workfs2/u07/alice/.ink/Jobs"):
            resp = asyncio.run(cr.get_user_jobs_dir_api(username="alice", token="t"))

        assert resp["status"] == cr.InkStatus.SUCCESS
        assert resp["data"] == {"jobs_dir": "/workfs2/u07/alice/.ink/Jobs"}

    def test_error_envelope(self):
        import asyncio
        from fastink.routers.v2 import compute_resources as cr

        with mock.patch.object(cr, "change_username_to_uid",
                               side_effect=ValueError("No UID found for username 'ghost'")):
            resp = asyncio.run(cr.get_user_jobs_dir_api(username="ghost", token="t"))

        assert resp["status"] == cr.InkStatus.SERVER_INTERNAL_ERROR
        assert "No UID found" in resp["msg"]
        assert resp["data"] == {}
