import struct
import time

import pytest

from fastink.auth.backends.ccache import (
    has_afs_ticket,
    parse_ccache,
    parse_ccache_full,
)


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


def _keyblock(version, key=b"key"):
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
    client_components=(b"EXAMPLE.COM", b"alice"),
    key_bytes=b"key",
    ticket_bytes=b"",
):
    return (
        _principal(client_components, version)
        + _principal(server, version)
        + _keyblock(version, key_bytes)
        + struct.pack(">IIII", authtime, 150, endtime, renew_until)
        + b"\x00"
        + _u32(0)
        + _u32(0)
        + _u32(0)
        + _data(ticket_bytes)
        + _data(b"")
    )


def build_ccache(version=4, *, header_fields=(), creds=()):
    default_principal = _principal((b"EXAMPLE.COM", b"alice"), version)
    if version == 4:
        fields = b"".join(
            _u16(tag) + _u16(len(value)) + value
            for tag, value in header_fields
        )
        header = (_u16(len(fields)) + fields) if fields else _u16(0)
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


def test_v4_nonempty_header_uses_tlv_payload_length():
    data = build_ccache(
        4,
        header_fields=((99, b"unknown"), (100, b"also-unknown")),
        creds=(_cred(4, (b"EXAMPLE.COM", b"krbtgt", b"EXAMPLE.COM")),),
    )

    assert parse_ccache(data)["expired_at"] == 200


@pytest.mark.parametrize(
    "header",
    (
        _u16(1) + b"\x00",
        _u16(3) + b"\x00\x01\x00",
        _u16(5) + _u16(99) + _u16(2) + b"x",
    ),
)
def test_v4_truncated_header_tlv_is_rejected(header):
    data = b"\x05\x04" + header + build_ccache(4)[4:]

    with pytest.raises(ValueError, match="truncated"):
        parse_ccache(data)


def test_v4_header_boundary_overshoot_is_rejected():
    data = b"\x05\x04" + _u16(0xFFFF) + build_ccache(4)[4:]

    with pytest.raises(ValueError, match="header"):
        parse_ccache(data)


def test_v4_deltatime_payload_must_be_eight_bytes():
    data = build_ccache(
        4,
        header_fields=((1, b"short"),),
        creds=(_cred(4, (b"EXAMPLE.COM", b"krbtgt", b"EXAMPLE.COM")),),
    )

    with pytest.raises(ValueError, match="DELTATIME"):
        parse_ccache(data)


def test_parse_ccache_full_exposes_principals_ranges_and_times():
    key = b"custom-key"
    ticket = b"opaque-ticket"
    cred = _cred(
        4,
        (b"EXAMPLE.COM", b"krbtgt", b"EXAMPLE.COM"),
        client_components=(b"EXAMPLE.COM", b"bob"),
        key_bytes=key,
        ticket_bytes=ticket,
        authtime=10,
        endtime=20,
        renew_until=30,
    )
    data = build_ccache(4, creds=(cred,))

    result = parse_ccache_full(data)

    assert result["version"] == 4
    assert result["default_principal"] == {
        "realm": b"EXAMPLE.COM",
        "components": [b"alice"],
    }
    parsed = result["credentials"][0]
    assert parsed["client"] == {
        "realm": b"EXAMPLE.COM",
        "components": [b"bob"],
    }
    assert parsed["server"] == {
        "realm": b"EXAMPLE.COM",
        "components": [b"krbtgt", b"EXAMPLE.COM"],
    }
    assert data[parsed["record_range"][0] : parsed["record_range"][1]] == cred
    key_start = parsed["keyblock_range"][0]
    key_end = parsed["keyblock_range"][1]
    assert data[key_start:key_end] == _keyblock(4, key)
    assert parsed["authtime"] == 10
    assert parsed["starttime"] == 150
    assert parsed["endtime"] == 20
    assert parsed["renew_until"] == 30
    assert parsed["is_config"] is False
    assert parsed["is_removed"] is False


def test_parse_ccache_full_record_range_splices_into_fresh_cache():
    first = _cred(
        4,
        (b"EXAMPLE.COM", b"host", b"first"),
        client_components=(b"EXAMPLE.COM", b"first"),
        endtime=400,
    )
    second = _cred(
        4,
        (b"EXAMPLE.COM", b"krbtgt", b"EXAMPLE.COM"),
        client_components=(b"EXAMPLE.COM", b"second"),
        endtime=500,
    )
    original = build_ccache(4, creds=(first, second))
    parsed = parse_ccache_full(original)
    selected = parsed["credentials"][1]
    record_start, record_end = selected["record_range"]
    principal_start, principal_end = parsed["default_principal_range"]
    default_principal = original[principal_start:principal_end]
    fresh = b"\x05\x04" + _u16(0) + default_principal + original[record_start:record_end]

    reparsed = parse_ccache_full(fresh)
    reparsed_credential = reparsed["credentials"][0]

    assert reparsed_credential["client"] == selected["client"]
    assert reparsed_credential["server"] == selected["server"]
    assert reparsed_credential["endtime"] == selected["endtime"]


def test_parse_ccache_full_marks_config_and_removed_entries():
    data = build_ccache(
        4,
        creds=(
            _cred(4, (b"X-CACHECONF:", b"config"), authtime=0, endtime=0),
            _cred(
                4,
                (b"EXAMPLE.COM", b"krbtgt", b"EXAMPLE.COM"),
                authtime=1,
                endtime=0,
            ),
        ),
    )

    credentials = parse_ccache_full(data)["credentials"]

    assert credentials[0]["is_config"] is True
    assert credentials[0]["is_removed"] is False
    assert credentials[1]["is_config"] is False
    assert credentials[1]["is_removed"] is True


def test_truncation_at_every_credential_byte_is_rejected():
    prefix = build_ccache(4)
    data = prefix + _cred(4, (b"EXAMPLE.COM", b"krbtgt", b"EXAMPLE.COM"))

    for cut in range(len(prefix), len(data)):
        with pytest.raises(ValueError):
            parse_ccache(data[:cut])


def test_truncation_at_credential_boundary_is_a_valid_shorter_cache():
    first = _cred(4, (b"EXAMPLE.COM", b"krbtgt", b"EXAMPLE.COM"), endtime=400)
    second = _cred(4, (b"EXAMPLE.COM", b"krbtgt", b"EXAMPLE.COM"), endtime=500)
    data = build_ccache(4, creds=(first, second))

    assert parse_ccache(data[: -len(second)])["expired_at"] == 400


def _set_u32(data, offset, value):
    result = bytearray(data)
    result[offset : offset + 4] = _u32(value)
    return bytes(result)


def test_inflated_credential_lengths_and_counts_are_rejected():
    prefix = build_ccache(4)
    credential = _cred(
        4,
        (b"EXAMPLE.COM", b"krbtgt", b"EXAMPLE.COM"),
        key_bytes=b"custom-key",
        ticket_bytes=b"ticket",
    )
    data = prefix + credential
    credential_start = len(prefix)
    client_end = credential_start + len(_principal((b"EXAMPLE.COM", b"alice"), 4))
    server_end = client_end + len(
        _principal((b"EXAMPLE.COM", b"krbtgt", b"EXAMPLE.COM"), 4)
    )
    key_length = server_end + 2
    address_count = server_end + len(_keyblock(4, b"custom-key")) + 21
    authdata_count = address_count + 4
    ticket_length = authdata_count + 4

    mutations = (
        credential_start + 8,
        key_length,
        address_count,
        authdata_count,
        ticket_length,
    )
    for offset in mutations:
        with pytest.raises(ValueError):
            parse_ccache(_set_u32(data, offset, 0xFFFFFFFF))


def test_trailing_garbage_is_rejected():
    data = build_ccache(
        4,
        creds=(_cred(4, (b"EXAMPLE.COM", b"krbtgt", b"EXAMPLE.COM")),),
    ) + b"garbage"

    with pytest.raises(ValueError):
        parse_ccache(data)


def test_malformed_final_credential_is_rejected_after_usable_tgt():
    good = _cred(4, (b"EXAMPLE.COM", b"krbtgt", b"EXAMPLE.COM"))
    malformed = _cred(4, (b"EXAMPLE.COM", b"host", b"server"))[:-1]
    data = build_ccache(4, creds=(good,)) + malformed

    with pytest.raises(ValueError):
        parse_ccache(data)


def test_malformed_final_credential_is_rejected_after_usable_afs_ticket():
    now = int(time.time())
    good = _cred(4, (b"EXAMPLE.COM", b"afs", b"EXAMPLE.COM"), endtime=now + 3600)
    malformed = _cred(4, (b"EXAMPLE.COM", b"host", b"server"))[:-1]
    data = build_ccache(4, creds=(good,)) + malformed

    with pytest.raises(ValueError):
        has_afs_ticket(data)


def test_parse_ccache_full_auth_mode_has_one_mib_ceiling():
    with pytest.raises(ValueError, match="1 MiB"):
        parse_ccache_full(b"x" * (1024 * 1024 + 1), for_auth=True)


def test_parse_ccache_full_auth_mode_accepts_exactly_one_mib():
    empty_ticket = build_ccache(
        4,
        creds=(_cred(4, (b"EXAMPLE.COM", b"krbtgt", b"EXAMPLE.COM")),),
    )
    ticket_bytes = (1024 * 1024) - len(empty_ticket)
    data = build_ccache(
        4,
        creds=(
            _cred(
                4,
                (b"EXAMPLE.COM", b"krbtgt", b"EXAMPLE.COM"),
                ticket_bytes=b"x" * ticket_bytes,
            ),
        ),
    )

    assert len(data) == 1024 * 1024
    assert parse_ccache_full(data, for_auth=True)["version"] == 4


def test_public_helpers_do_not_apply_auth_size_ceiling():
    data = build_ccache(
        4,
        creds=(
            _cred(
                4,
                (b"EXAMPLE.COM", b"krbtgt", b"EXAMPLE.COM"),
                ticket_bytes=b"x" * (1024 * 1024),
            ),
        ),
    )

    assert parse_ccache(data)["expired_at"] == 200
