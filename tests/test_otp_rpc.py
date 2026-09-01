"""Unit tests for the FS-based OTP RPC in fastink.computing.apps._helpers.

Server-side coroutine ``generate_userotp`` is exercised against a mocked
``fastink.storage.common`` module -- the real xrootd calls would need a
compute node and a running vncserver, which is Phase 3 territory.
"""
from __future__ import annotations

import asyncio
import json
import time
from unittest import mock

import pytest


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

class _FakeStorage:
    """In-memory stand-in for fastink.storage.common used by the OTP RPC.

    Tracks path_exist / cat_file / upload_file / delete_path calls the
    same way the real xrootd wrapper does, so the RPC code can drive it
    without any external service.
    """

    def __init__(self, *, initial_files: dict | None = None):
        self.files: dict[str, bytes] = dict(initial_files or {})
        self.uploaded_paths: list[str] = []
        self.deleted_paths: list[str] = []

    async def path_exist(self, name, username="", mgm=""):
        return (name in self.files, None)

    async def upload_file(self, src_data, dst, username="", mgm="", mode=""):
        self.files[dst] = src_data
        self.uploaded_paths.append(dst)

    async def cat_file(self, fname, username="", mgm="", krb5_enabled=False):
        return self.files[fname].decode("utf-8")

    async def delete_path(self, name, username="", mgm="", krb5_enabled=False):
        self.deleted_paths.append(name)
        self.files.pop(name, None)


def _patch(fake_storage: _FakeStorage):
    """Return a context manager that swaps storage + get_config + user lookup."""
    def _get_config(section, option=None, **kw):
        if (section, option) == ("storage", "xrd_host"):
            return "root://fake:1094"
        if (section, option) == ("common", "krb5_enabled"):
            return False
        return kw.get("fallback", "")
    return mock.patch.multiple(
        "fastink.computing.apps._helpers",
        _storage_common=fake_storage,
        get_config=_get_config,
        change_uid_to_username=lambda uid: f"user{uid}",
    )


def _rpc(job_path="/tmp/job1", uid=1001, host="wn01", **kw):
    from fastink.computing.apps._helpers import generate_userotp
    return generate_userotp(uid, host, job_path=job_path, **kw)


def _bad_body_repr(b: bytes) -> str:
    return b.decode("utf-8", errors="replace")[:60]


# ---------------------------------------------------------------------------
# Happy path
# ---------------------------------------------------------------------------

class TestGenerateUserotpSuccess:
    def test_returns_otp_when_response_landed_before_call(self):
        # The listener's write may race the server's poll; simulate the
        # already-there case (fastest possible round-trip).
        fake = _FakeStorage(initial_files={
            "/tmp/job1/otp/.ready": b"",
        })
        # Patch upload_file to also drop a fake response file so the very
        # first poll finds it.
        original_upload = fake.upload_file
        async def _upload_and_reply(src_data, dst, **kw):
            await original_upload(src_data, dst, **kw)
            uuid = dst.rsplit("_", 1)[-1]
            fake.files[f"/tmp/job1/otp/resp_{uuid}"] = json.dumps({
                "otp": "MYOTP123",
                "ts": int(time.time()),
            }).encode()
        fake.upload_file = _upload_and_reply

        with _patch(fake):
            otp = asyncio.run(_rpc(poll_interval=0.01, timeout=1.0))

        assert otp == "MYOTP123"
        # Server must have written a req_* file
        assert any(p.startswith("/tmp/job1/otp/req_") for p in fake.uploaded_paths)
        # And must have cleaned up its resp
        assert any(p.startswith("/tmp/job1/otp/resp_") for p in fake.deleted_paths)


# ---------------------------------------------------------------------------
# Failure modes
# ---------------------------------------------------------------------------

class TestGenerateUserotpFailures:
    def test_missing_ready_marker_gives_helpful_500(self):
        fake = _FakeStorage()  # no .ready
        with _patch(fake), pytest.raises(Exception) as ei:
            asyncio.run(_rpc(poll_interval=0.01, timeout=0.1))
        assert "not ready" in str(ei.value)

    def test_timeout_when_no_response(self):
        fake = _FakeStorage(initial_files={"/tmp/job1/otp/.ready": b""})
        # upload_file no-op reply -- nothing comes back
        with _patch(fake), pytest.raises(Exception) as ei:
            asyncio.run(_rpc(poll_interval=0.01, timeout=0.05))
        assert "Timeout" in str(ei.value)

    def test_error_file_surfaces_reason(self):
        fake = _FakeStorage(initial_files={"/tmp/job1/otp/.ready": b""})
        original_upload = fake.upload_file
        async def _upload_and_err(src_data, dst, **kw):
            await original_upload(src_data, dst, **kw)
            uuid = dst.rsplit("_", 1)[-1]
            fake.files[f"/tmp/job1/otp/resp_{uuid}.err"] = b"vncpasswd not found\n"
        fake.upload_file = _upload_and_err
        with _patch(fake), pytest.raises(Exception) as ei:
            asyncio.run(_rpc(poll_interval=0.01, timeout=1.0))
        assert "vncpasswd not found" in str(ei.value)

    def test_malformed_response_is_500(self):
        fake = _FakeStorage(initial_files={"/tmp/job1/otp/.ready": b""})
        original_upload = fake.upload_file
        async def _upload_and_bad(src_data, dst, **kw):
            await original_upload(src_data, dst, **kw)
            uuid = dst.rsplit("_", 1)[-1]
            fake.files[f"/tmp/job1/otp/resp_{uuid}"] = b"not-json"
        fake.upload_file = _upload_and_bad
        with _patch(fake), pytest.raises(Exception) as ei:
            asyncio.run(_rpc(poll_interval=0.01, timeout=1.0))
        assert "Malformed" in str(ei.value) or "OTP" in str(ei.value)

    def test_empty_otp_field_is_500(self):
        fake = _FakeStorage(initial_files={"/tmp/job1/otp/.ready": b""})
        original_upload = fake.upload_file
        async def _upload_and_empty(src_data, dst, **kw):
            await original_upload(src_data, dst, **kw)
            uuid = dst.rsplit("_", 1)[-1]
            fake.files[f"/tmp/job1/otp/resp_{uuid}"] = b'{"otp":"","ts":0}'
        fake.upload_file = _upload_and_empty
        with _patch(fake), pytest.raises(Exception) as ei:
            asyncio.run(_rpc(poll_interval=0.01, timeout=1.0))
        assert "empty" in str(ei.value)

    def test_missing_job_path_rejected(self):
        # Older callers used to pass hostname only; that's not enough now.
        fake = _FakeStorage()
        with _patch(fake), pytest.raises(Exception) as ei:
            asyncio.run(_rpc(job_path=None, poll_interval=0.01, timeout=0.1))
        assert "job_path" in str(ei.value)

    def test_non_string_otp_value_rejected(self):
        # Defense: if the on-worker listener ever emits an otp that is
        # not a JSON string (e.g. an int, list, or the key is missing),
        # surface a proper 500 instead of blowing up on .strip().
        for bad_body in (
            b'{"otp": 12345, "ts": 0}',            # int, not str
            b'{"otp": ["a", "b"], "ts": 0}',       # list, not str
            b'[]',                                 # array root, not object
            b'"just a string"',                    # scalar root
        ):
            fake = _FakeStorage(initial_files={"/tmp/job1/otp/.ready": b""})
            original_upload = fake.upload_file
            async def _upload_and_bad(src_data, dst, _body=bad_body, **kw):
                await original_upload(src_data, dst, **kw)
                uuid = dst.rsplit("_", 1)[-1]
                fake.files[f"/tmp/job1/otp/resp_{uuid}"] = _body
            fake.upload_file = _upload_and_bad
            with _patch(fake), pytest.raises(Exception) as ei:
                asyncio.run(_rpc(poll_interval=0.01, timeout=1.0))
            msg = str(ei.value)
            assert "Malformed" in msg or "empty" in msg, \
                f"unexpected error for body={_bad_body_repr(bad_body)}: {msg}"


# ---------------------------------------------------------------------------
# Unique req_id per invocation
# ---------------------------------------------------------------------------

class TestReqIdUniqueness:
    def test_each_call_uses_distinct_req_id(self):
        seen = []
        fake = _FakeStorage(initial_files={"/tmp/job1/otp/.ready": b""})
        original_upload = fake.upload_file
        async def _upload_and_reply(src_data, dst, **kw):
            await original_upload(src_data, dst, **kw)
            uuid = dst.rsplit("_", 1)[-1]
            seen.append(uuid)
            fake.files[f"/tmp/job1/otp/resp_{uuid}"] = json.dumps({
                "otp": f"OTP-{uuid[:4]}", "ts": 0,
            }).encode()
        fake.upload_file = _upload_and_reply

        with _patch(fake):
            r1 = asyncio.run(_rpc(poll_interval=0.01, timeout=1.0))
            r2 = asyncio.run(_rpc(poll_interval=0.01, timeout=1.0))
            r3 = asyncio.run(_rpc(poll_interval=0.01, timeout=1.0))

        assert len(set(seen)) == 3, f"req_ids collided: {seen}"
        assert r1 != r2 != r3
