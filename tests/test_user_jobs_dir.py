"""Tests for the cluster-scoped job-directory resolver and API.

Two levels:
* Resolver ``get_user_jobs_dir(username, uid, cluster_id=None)``
  - without cluster_id -> parent ``.ink/Jobs`` (legacy layout, for
    reads against pre-migration data)
  - with cluster_id    -> per-cluster ``.ink/Jobs/<cluster>``
* Endpoint ``GET /api/v2/cr/get_user_jobs_dir``
  - returns one ``<cluster>_jobs_dir`` field per entry in
    ``computing.cluster_list``
"""
from __future__ import annotations

import asyncio
from unittest import mock

from fastink.computing.tools.common.utils import get_user_ink_dir, get_user_jobs_dir


# ---------------------------------------------------------------------------
# Resolver
# ---------------------------------------------------------------------------

class TestResolver:
    def test_parent_path_without_cluster_id(self):
        """Default (no cluster_id) returns the shared parent."""
        with mock.patch(
            "fastink.computing.tools.common.utils.get_config",
            return_value="/workfs2/{user_group}/{username}",
        ), mock.patch(
            "fastink.computing.tools.common.utils.get_user_exp_group",
            return_value=("CC", "u07"),
        ):
            assert get_user_jobs_dir("alice", 1001) == "/workfs2/u07/alice/.ink/Jobs"

    def test_htcondor_subdir(self):
        with mock.patch(
            "fastink.computing.tools.common.utils.get_config",
            return_value="/workfs2/{user_group}/{username}",
        ), mock.patch(
            "fastink.computing.tools.common.utils.get_user_exp_group",
            return_value=("CC", "u07"),
        ):
            assert get_user_jobs_dir("alice", 1001, "htcondor") \
                == "/workfs2/u07/alice/.ink/Jobs/htcondor"

    def test_slurm_subdir_with_asic_group(self):
        with mock.patch(
            "fastink.computing.tools.common.utils.get_config",
            return_value="/workfs2/{user_group}/{username}",
        ), mock.patch(
            "fastink.computing.tools.common.utils.get_user_exp_group",
            return_value=("ASIC", "asic"),
        ):
            assert get_user_jobs_dir("lixt", 21001, "slurm") \
                == "/workfs2/asic/lixt/.ink/Jobs/slurm"

    def test_tilde_falls_back_to_expanduser_still_scoped(self):
        with mock.patch(
            "fastink.computing.tools.common.utils.get_config",
            return_value="~",
        ), mock.patch(
            "fastink.computing.tools.common.utils.os.path.expanduser",
            return_value="/home/carol",
        ):
            assert get_user_ink_dir("carol") == "/home/carol"
            assert get_user_jobs_dir("carol") == "/home/carol/.ink/Jobs"
            assert get_user_jobs_dir("carol", None, "htcondor") \
                == "/home/carol/.ink/Jobs/htcondor"

    def test_sanity_guard_rejects_bad_cluster_id(self):
        """The resolver refuses cluster_id values that would escape the
        jobs directory (path-traversal or hidden dirs)."""
        import pytest
        with mock.patch(
            "fastink.computing.tools.common.utils.get_config",
            return_value="/home/{username}",
        ):
            for bad in ("..", ".", ".hidden", "../..", "foo/bar",
                        "a b", "a\x00b"):
                with pytest.raises(ValueError):
                    get_user_jobs_dir("dan", 1004, bad)

    def test_sanity_guard_accepts_reasonable_cluster_ids(self):
        with mock.patch(
            "fastink.computing.tools.common.utils.get_config",
            return_value="/home/{username}",
        ):
            for good in ("htcondor", "slurm", "slurm-cluster-2",
                         "htcondor_v2", "cluster123"):
                path = get_user_jobs_dir("erin", 1005, good)
                assert path.endswith(f"/.ink/Jobs/{good}")


# ---------------------------------------------------------------------------
# Endpoint
# ---------------------------------------------------------------------------

class TestEndpointShape:
    def test_returns_per_cluster_fields(self):
        from fastink.routers.v2 import compute_resources as cr

        # get_config called with (section, option, fallback=...) — return
        # the cluster_list based on which section/option requested
        def cfg(section, option=None, **kw):
            if (section, option) == ("computing", "cluster_list"):
                return ["htcondor", "slurm"]
            return kw.get("fallback", None)

        with mock.patch.object(cr, "change_username_to_uid", return_value=1001), \
             mock.patch.object(cr, "get_config", side_effect=cfg, create=True), \
             mock.patch.object(cr, "get_user_jobs_dir",
                               side_effect=lambda u, uid, cid: f"/workfs2/u07/{u}/.ink/Jobs/{cid}"):
            resp = asyncio.run(cr.get_user_jobs_dir_api(username="alice", token="t"))

        assert resp["status"] == cr.InkStatus.SUCCESS
        assert resp["data"] == {
            "htcondor_jobs_dir": "/workfs2/u07/alice/.ink/Jobs/htcondor",
            "slurm_jobs_dir":    "/workfs2/u07/alice/.ink/Jobs/slurm",
        }

    def test_htcondor_only_deployment(self):
        """Site with only htcondor in cluster_list omits the slurm field."""
        from fastink.routers.v2 import compute_resources as cr

        def cfg(section, option=None, **kw):
            if (section, option) == ("computing", "cluster_list"):
                return ["htcondor"]
            return kw.get("fallback", None)

        with mock.patch.object(cr, "change_username_to_uid", return_value=1001), \
             mock.patch.object(cr, "get_config", side_effect=cfg, create=True), \
             mock.patch.object(cr, "get_user_jobs_dir",
                               side_effect=lambda u, uid, cid: f"/home/{u}/.ink/Jobs/{cid}"):
            resp = asyncio.run(cr.get_user_jobs_dir_api(username="bob", token="t"))

        assert "htcondor_jobs_dir" in resp["data"]
        assert "slurm_jobs_dir" not in resp["data"]

    def test_cluster_list_as_comma_string(self):
        """Deployments that set cluster_list as a comma string still work."""
        from fastink.routers.v2 import compute_resources as cr

        def cfg(section, option=None, **kw):
            if (section, option) == ("computing", "cluster_list"):
                return "htcondor,slurm"
            return kw.get("fallback", None)

        with mock.patch.object(cr, "change_username_to_uid", return_value=1001), \
             mock.patch.object(cr, "get_config", side_effect=cfg, create=True), \
             mock.patch.object(cr, "get_user_jobs_dir",
                               side_effect=lambda u, uid, cid: f"/home/{u}/.ink/Jobs/{cid}"):
            resp = asyncio.run(cr.get_user_jobs_dir_api(username="carol", token="t"))

        assert set(resp["data"].keys()) == {"htcondor_jobs_dir", "slurm_jobs_dir"}

    def test_error_envelope(self):
        from fastink.routers.v2 import compute_resources as cr

        with mock.patch.object(cr, "change_username_to_uid",
                               side_effect=ValueError("No UID found for username 'ghost'")):
            resp = asyncio.run(cr.get_user_jobs_dir_api(username="ghost", token="t"))

        assert resp["status"] == cr.InkStatus.SERVER_INTERNAL_ERROR
        assert "No UID found" in resp["msg"]
        assert resp["data"] == {}
