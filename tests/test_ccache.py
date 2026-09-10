import struct
import time

import pytest

from fastink.auth.backends.ccache import has_afs_ticket, parse_ccache


def _u16(value):
    return struct.pack(">H", value)


def _u32(value):
    return struct.pack(">I", value)


def _data(value):
    return _u32(len(value)) + value


def _principal(components, version):
    return (
        _u32(1)
        + _u32(len(components) - 1)
        + b"".join(_data(component) for component in components)
    )


def _keyblock(version):
    key = b"key"
    if version == 3:
        return _u16(18) + _u16(18) + _u32(len(key)) + key
    return _u16(18) + _u32(len(key)) + key


def _cred(
    version,
    server,
    *,
    authtime=100,
    endtime=200,
    renew_until=300,
):
    client = (b"EXAMPLE.COM", b"alice")
    return (
        _principal(client, version)
        + _principal(server, version)
        + _keyblock(version)
        + struct.pack(">IIII", authtime, 150, endtime, renew_until)
        + b"\x00"
        + _u32(0)
        + _u32(0)
        + _u32(0)
        + _data(b"")
        + _data(b"")
    )


def build_ccache(version=4, *, header_fields=(), creds=()):
    default_principal = _principal((b"EXAMPLE.COM", b"alice"), version)
    if version == 4:
        fields = b"".join(
            _u16(tag) + _u16(len(value)) + value
            for tag, value in header_fields
        )
        header = (_u16(2 + len(fields)) + fields) if fields else _u16(0)
    else:
        header = b""
    return b"\x05" + bytes((version,)) + header + default_principal + b"".join(creds)


def test_v4_single_tgt_returns_default_username_and_times():
    data = build_ccache(
        4,
        creds=(_cred(4, (b"EXAMPLE.COM", b"krbtgt", b"EXAMPLE.COM"), endtime=2000, renew_until=3000),),
    )

    result = parse_ccache(data)

    assert result == {"username": "alice", "expired_at": 2000, "renew_until": 3000}


def test_v4_delta_time_header_is_skipped():
    data = build_ccache(
        4,
        header_fields=((1, _u32(12) + _u32(34)),),
        creds=(_cred(4, (b"EXAMPLE.COM", b"krbtgt", b"EXAMPLE.COM")),),
    )

    assert parse_ccache(data)["username"] == "alice"


def test_v4_zero_header_length_is_supported():
    data = build_ccache(
        4,
        header_fields=(),
        creds=(_cred(4, (b"EXAMPLE.COM", b"krbtgt", b"EXAMPLE.COM")),),
    )

    assert parse_ccache(data)["expired_at"] == 200


def test_v3_keyblock_layout_is_supported():
    data = build_ccache(
        3,
        creds=(_cred(3, (b"EXAMPLE.COM", b"krbtgt", b"EXAMPLE.COM")),),
    )

    assert parse_ccache(data) == {"username": "alice", "expired_at": 200, "renew_until": 300}


def test_config_entry_is_skipped():
    data = build_ccache(
        4,
        creds=(
            _cred(4, (b"X-CACHECONF:", b"config")),
            _cred(4, (b"EXAMPLE.COM", b"krbtgt", b"EXAMPLE.COM"), endtime=500),
        ),
    )

    assert parse_ccache(data)["expired_at"] == 500


def test_removed_entry_is_skipped():
    data = build_ccache(
        4,
        creds=(
            _cred(4, (b"EXAMPLE.COM", b"krbtgt", b"EXAMPLE.COM"), authtime=100, endtime=0),
            _cred(4, (b"EXAMPLE.COM", b"krbtgt", b"EXAMPLE.COM"), endtime=600),
        ),
    )

    assert parse_ccache(data)["expired_at"] == 600


def test_non_tgt_service_ticket_does_not_replace_tgt():
    data = build_ccache(
        4,
        creds=(
            _cred(4, (b"EXAMPLE.COM", b"host", b"server"), endtime=900),
            _cred(4, (b"EXAMPLE.COM", b"krbtgt", b"EXAMPLE.COM"), endtime=700),
        ),
    )

    assert parse_ccache(data)["expired_at"] == 700


def test_multiple_tgts_choose_the_latest_endtime():
    data = build_ccache(
        4,
        creds=(
            _cred(4, (b"EXAMPLE.COM", b"krbtgt", b"EXAMPLE.COM"), endtime=800, renew_until=1000),
            _cred(4, (b"EXAMPLE.COM", b"krbtgt", b"EXAMPLE.COM"), endtime=900, renew_until=1100),
        ),
    )

    assert parse_ccache(data) == {"username": "alice", "expired_at": 900, "renew_until": 1100}


def test_invalid_magic_raises_value_error():
    data = b"\x04" + build_ccache(4)[1:]

    with pytest.raises(ValueError, match="not a krb5 FILE ccache"):
        parse_ccache(data)


def test_missing_tgt_raises_value_error():
    data = build_ccache(4, creds=(_cred(4, (b"EXAMPLE.COM", b"host", b"server")),))

    with pytest.raises(ValueError, match="no usable krbtgt credential"):
        parse_ccache(data)


def test_has_afs_ticket_true():
    now = int(time.time())
    data = build_ccache(
        4,
        creds=(
            _cred(4, (b"EXAMPLE.COM", b"krbtgt", b"EXAMPLE.COM"), endtime=now + 3600),
            _cred(4, (b"EXAMPLE.COM", b"afs", b"EXAMPLE.COM"), endtime=now + 3600),
        ),
    )

    assert has_afs_ticket(data) is True


def test_has_afs_ticket_false_without_afs():
    now = int(time.time())
    data = build_ccache(
        4,
        creds=(
            _cred(4, (b"EXAMPLE.COM", b"krbtgt", b"EXAMPLE.COM"), endtime=now + 3600),
            _cred(4, (b"EXAMPLE.COM", b"host", b"server"), endtime=now + 3600),
        ),
    )

    assert has_afs_ticket(data) is False


def test_has_afs_ticket_false_expired():
    now = int(time.time())
    data = build_ccache(
        4,
        creds=(
            _cred(4, (b"EXAMPLE.COM", b"krbtgt", b"EXAMPLE.COM"), endtime=now + 3600),
            _cred(4, (b"EXAMPLE.COM", b"afs", b"EXAMPLE.COM"), endtime=now - 3600),
        ),
    )

    assert has_afs_ticket(data) is False


def test_has_afs_ticket_any_cell():
    """Matching is site-agnostic: an afs ticket for a non-default cell
    (e.g. someother.cell, not ihep.ac.cn) still counts — the cell is site
    configuration, never hardcoded."""
    now = int(time.time())
    data = build_ccache(
        4,
        creds=(
            _cred(4, (b"EXAMPLE.COM", b"krbtgt", b"EXAMPLE.COM"), endtime=now + 3600),
            _cred(4, (b"EXAMPLE.COM", b"afs", b"someother.cell"), endtime=now + 3600),
        ),
    )

    assert has_afs_ticket(data) is True
