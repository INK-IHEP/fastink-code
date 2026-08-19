"""Unit tests for the failed-submission record store
(fastink.computing.tools.common.failed_jobs).

Uses an in-memory fake of the async Redis client so no external service
is needed.  The pipeline() context manager mirrors redis.asyncio's
transaction API shape closely enough for these helpers.
"""
from __future__ import annotations

import asyncio

import pytest

from fastink.computing.tools.common.failed_jobs import (
    FAILED_JOB_TTL_SEC,
    HOLD_REASON_MAX_LEN,
    delete_failed_job,
    failed_job_query_entry,
    get_failed_job,
    list_failed_jobs,
    record_failed_job,
)


class _FakePipeline:
    def __init__(self, store):
        self._store = store
        self._ops = []

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False

    def hset(self, key, mapping=None):
        self._ops.append(("hset", key, dict(mapping or {})))

    def expire(self, key, ttl):
        self._ops.append(("expire", key, ttl))

    def sadd(self, key, *members):
        self._ops.append(("sadd", key, members))

    def delete(self, key):
        self._ops.append(("delete", key))

    def srem(self, key, *members):
        self._ops.append(("srem", key, members))

    async def execute(self):
        results = []
        for op in self._ops:
            kind = op[0]
            if kind == "hset":
                _, key, mapping = op
                self._store.hashes.setdefault(key, {}).update(mapping)
                results.append(len(mapping))
            elif kind == "expire":
                _, key, ttl = op
                self._store.ttls[key] = ttl
                results.append(1)
            elif kind == "sadd":
                _, key, members = op
                self._store.sets.setdefault(key, set()).update(members)
                results.append(len(members))
            elif kind == "delete":
                _, key = op
                existed = key in self._store.hashes
                self._store.hashes.pop(key, None)
                results.append(1 if existed else 0)
            elif kind == "srem":
                _, key, members = op
                s = self._store.sets.get(key, set())
                removed = len(s & set(members))
                s -= set(members)
                results.append(removed)
        self._ops = []
        return results


class _FakeRedis:
    def __init__(self):
        self.hashes = {}
        self.sets = {}
        self.ttls = {}

    def pipeline(self, transaction=True):
        return _FakePipeline(self)

    async def hgetall(self, key):
        return dict(self.hashes.get(key, {}))

    async def smembers(self, key):
        return set(self.sets.get(key, set()))

    async def srem(self, key, *members):
        s = self.sets.get(key, set())
        removed = len(s & set(members))
        s -= set(members)
        return removed


# ---------------------------------------------------------------------------
# record / list / get / delete round-trip
# ---------------------------------------------------------------------------

class TestRoundTrip:
    def test_record_then_list(self):
        r = _FakeRedis()
        fail_id = asyncio.run(record_failed_job(
            r, "alice", "htcondor", "vnc", "kerberos ticket expired",
            req_cpu="1", req_mem="6000", req_os="AlmaLinux9",
        ))
        assert fail_id.isdigit() and int(fail_id) > 10**12  # epoch-ms

        records = asyncio.run(list_failed_jobs(r, "alice"))
        assert len(records) == 1
        rec = records[0]
        assert rec["failId"] == fail_id
        assert rec["jobType"] == "vnc"
        assert rec["clusterId"] == "htcondor"
        assert rec["failReason"] == "kerberos ticket expired"
        assert rec["jobReqOS"] == "AlmaLinux9"

    def test_ttl_set_on_hash(self):
        r = _FakeRedis()
        fail_id = asyncio.run(record_failed_job(r, "bob", "slurm", "vscode", "x"))
        key = f"failed_jobs:bob:{fail_id}"
        assert r.ttls[key] == FAILED_JOB_TTL_SEC == 86400

    def test_get_single(self):
        r = _FakeRedis()
        fail_id = asyncio.run(record_failed_job(r, "carol", "htcondor", "asic", "boom"))
        rec = asyncio.run(get_failed_job(r, "carol", fail_id))
        assert rec is not None and rec["failReason"] == "boom"
        assert asyncio.run(get_failed_job(r, "carol", "999")) is None

    def test_delete(self):
        r = _FakeRedis()
        fail_id = asyncio.run(record_failed_job(r, "dave", "htcondor", "vnc", "x"))
        assert asyncio.run(delete_failed_job(r, "dave", fail_id)) is True
        assert asyncio.run(list_failed_jobs(r, "dave")) == []
        # Second delete: nothing there
        assert asyncio.run(delete_failed_job(r, "dave", fail_id)) is False

    def test_expired_hash_pruned_from_index(self):
        r = _FakeRedis()
        fail_id = asyncio.run(record_failed_job(r, "erin", "htcondor", "vnc", "x"))
        # Simulate TTL expiry: hash gone, index entry stale
        del r.hashes[f"failed_jobs:erin:{fail_id}"]
        assert asyncio.run(list_failed_jobs(r, "erin")) == []
        # Index cleaned as a side effect
        assert fail_id not in r.sets.get("failed_jobs:erin:ids", set())


# ---------------------------------------------------------------------------
# query entry shaping
# ---------------------------------------------------------------------------

class TestQueryEntry:
    def test_entry_shape(self):
        rec = {
            "failId": "1785582874123",
            "jobType": "vnc",
            "failTime": "2026-08-06 10:00:00",
            "failReason": "some error",
            "jobReqOS": "AlmaLinux9",
        }
        entry = failed_job_query_entry("htcondor", rec)
        assert entry["jobStatus"] == "FAILED"
        assert entry["jobId"] == "1785582874123"
        assert entry["hold_reason"] == "some error"
        assert entry["jobSubmitTime"] == "2026-08-06 10:00:00"
        assert entry["connect_sign"] == "False"
        assert entry["clusterId"] == "htcondor"

    def test_long_reason_truncated(self):
        rec = {"failId": "1", "failReason": "x" * 500}
        entry = failed_job_query_entry("htcondor", rec)
        assert len(entry["hold_reason"]) == HOLD_REASON_MAX_LEN + 3
        assert entry["hold_reason"].endswith("...")

    def test_reason_stored_full_but_capped(self):
        r = _FakeRedis()
        fail_id = asyncio.run(record_failed_job(r, "f", "htcondor", "vnc", "y" * 20000))
        rec = asyncio.run(get_failed_job(r, "f", fail_id))
        assert len(rec["failReason"]) == 10000  # storage cap
