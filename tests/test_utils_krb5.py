import base64
import logging
import os
import sys
import time
import types
from pathlib import Path

import pytest

from fastink.auth.backends import ccache
from fastink.common import utils
from fastink.common.exception import TokenExpiredException
from tests.test_ccache import _cred, build_ccache


def _write_tgt_ccache(path: Path, endtime: int) -> None:
    data = build_ccache(
        4,
        creds=(
            _cred(4, (b"EXAMPLE.COM", b"krbtgt", b"EXAMPLE.COM"), endtime=endtime),
        ),
    )
    path.write_bytes(data)


def test_check_krb5_validity_valid(tmp_path):
    ccache_path = tmp_path / "valid.ccache"
    _write_tgt_ccache(ccache_path, int(time.time()) + 3600)

    assert ccache.check_krb5_validity(str(ccache_path)) is True


def test_check_krb5_validity_expiring_soon(tmp_path):
    ccache_path = tmp_path / "expiring.ccache"
    _write_tgt_ccache(ccache_path, int(time.time()) + 1000)

    assert ccache.check_krb5_validity(str(ccache_path)) is False


def test_check_krb5_validity_expired(tmp_path):
    ccache_path = tmp_path / "expired.ccache"
    _write_tgt_ccache(ccache_path, int(time.time()) - 3600)

    assert ccache.check_krb5_validity(str(ccache_path)) is False


def test_check_krb5_validity_missing_file(tmp_path):
    ccache_path = tmp_path / "missing.ccache"

    with pytest.raises(FileNotFoundError):
        ccache.check_krb5_validity(str(ccache_path))


def test_get_krb5cc_logs_recheck_failure_warning(monkeypatch, caplog):
    uid = 2_000_000_000 + os.getpid()
    ccache_path = f"/tmp/krb5cc_{uid}"
    token = base64.b64encode(b"test-ccache").decode()
    krb5 = types.ModuleType("fastink.auth.backends.krb5")
    krb5.get_krb5 = lambda username: token
    monkeypatch.setitem(sys.modules, "fastink.auth.backends.krb5", krb5)
    monkeypatch.setattr(utils.os.path, "isfile", lambda path: False)
    monkeypatch.setattr(utils, "check_krb5_validity", lambda path: False)

    try:
        with caplog.at_level(logging.INFO, logger="ink"):
            with pytest.raises(TokenExpiredException):
                utils.get_krb5cc(uid=uid, name="alice")
    finally:
        if os.path.exists(ccache_path):
            os.remove(ccache_path)

    assert any(
        record.levelno == logging.WARNING
        and "Retrieved token for alice is expired or invalid" in record.getMessage()
        for record in caplog.records
    )
