import struct
import subprocess
import time

import pytest

from fastink.auth.backends.ccache import (
    service_ticket_endtime,
    splice_tgt_only,
    parse_ccache_full,
)
from tests.test_ccache import _cred, _principal, _u16


REALM = b"EXAMPLE.COM"
CLIENT = b"alice"
TGT = (REALM, b"krbtgt", REALM)
TARGET = (REALM, b"host", b"target.example")


def _build_ccache(
    version=4,
    *,
    default_components=(REALM, CLIENT),
    header_fields=(),
    creds=(),
):
    default_principal = _principal(default_components, version)
    if version == 4:
        fields = b"".join(
            _u16(tag) + _u16(len(value)) + value
            for tag, value in header_fields
        )
        header = _u16(len(fields)) + fields
    else:
        header = b""
    return b"\x05" + bytes((version,)) + header + default_principal + b"".join(creds)


def test_splice_keeps_only_matching_local_unexpired_tgts_in_order():
    now = int(time.time())
    data = _build_ccache(
        header_fields=((1, struct.pack(">II", 12, 34)),),
        creds=(
            _cred(4, TARGET, endtime=now + 100),
            _cred(4, TGT, endtime=now + 200),
            _cred(4, TGT, endtime=now + 300),
        ),
    )

    spliced, username = splice_tgt_only(data, REALM)
    credentials = parse_ccache_full(spliced)["credentials"]

    assert username == CLIENT
    assert [credential["server"]["components"] for credential in credentials] == [
        [b"krbtgt", REALM],
        [b"krbtgt", REALM],
    ]
    assert [credential["endtime"] for credential in credentials] == [now + 200, now + 300]


def test_splice_strips_target_service_and_other_service_tickets():
    now = int(time.time())
    data = _build_ccache(
        creds=(
            _cred(4, TARGET, endtime=now + 100),
            _cred(4, (REALM, b"afs", b"cell.example"), endtime=now + 200),
            _cred(4, TGT, endtime=now + 300),
        ),
    )

    spliced, _username = splice_tgt_only(data, REALM)

    assert len(parse_ccache_full(spliced)["credentials"]) == 1
    assert parse_ccache_full(spliced)["credentials"][0]["server"]["components"] == [
        b"krbtgt",
        REALM,
    ]


def test_splice_strips_foreign_client_and_foreign_realm_tgts():
    now = int(time.time())
    data = _build_ccache(
        creds=(
            _cred(
                4,
                TGT,
                client_components=(REALM, b"bob"),
                endtime=now + 100,
            ),
            _cred(
                4,
                (b"OTHER.COM", b"krbtgt", b"OTHER.COM"),
                endtime=now + 200,
            ),
            _cred(4, TGT, endtime=now + 300),
        ),
    )

    spliced, _username = splice_tgt_only(data, REALM)

    credentials = parse_ccache_full(spliced)["credentials"]
    assert [credential["endtime"] for credential in credentials] == [now + 300]


def test_splice_strips_all_x_cacheconf_records():
    now = int(time.time())
    data = _build_ccache(
        creds=(
            _cred(4, (b"X-CACHECONF:", b"start_realm"), endtime=now + 100),
            _cred(4, (b"X-CACHECONF:", b"proxy_impersonator"), endtime=now + 200),
            _cred(4, TGT, endtime=now + 300),
        ),
    )

    spliced, _username = splice_tgt_only(data, REALM)

    assert len(parse_ccache_full(spliced)["credentials"]) == 1


def test_splice_drops_header_fields_and_emits_empty_v4_header():
    data = _build_ccache(
        header_fields=((1, struct.pack(">II", 12, 34)), (99, b"metadata")),
        creds=(_cred(4, TGT, endtime=int(time.time()) + 100),),
    )

    spliced, _username = splice_tgt_only(data, REALM)

    assert spliced[:4] == b"\x05\x04\x00\x00"
    assert parse_ccache_full(spliced)["version"] == 4


def test_splice_drops_expired_and_removed_tgts():
    now = int(time.time())
    data = _build_ccache(
        creds=(
            _cred(4, TGT, endtime=now - 1),
            _cred(4, TGT, authtime=1, endtime=0),
            _cred(4, TGT, endtime=now + 100),
        ),
    )

    spliced, _username = splice_tgt_only(data, REALM)

    assert [credential["endtime"] for credential in parse_ccache_full(spliced)["credentials"]] == [
        now + 100,
    ]


def test_splice_requires_an_eligible_tgt():
    data = _build_ccache(creds=(_cred(4, TARGET, endtime=int(time.time()) + 100),))

    with pytest.raises(ValueError, match="no eligible TGT"):
        splice_tgt_only(data, REALM)


def test_splice_requires_a_single_component_default_principal():
    data = _build_ccache(
        default_components=(REALM, b"alice", b"admin"),
        creds=(_cred(4, TGT, endtime=int(time.time()) + 100),),
    )

    with pytest.raises(ValueError, match="single principal component"):
        splice_tgt_only(data, REALM)


def test_splice_rejects_foreign_default_principal_realm():
    now = int(time.time())
    data = _build_ccache(
        default_components=(b"FOREIGN.REALM", CLIENT),
        creds=(
            _cred(
                4,
                TGT,
                client_components=(b"FOREIGN.REALM", CLIENT),
                endtime=now + 100,
            ),
        ),
    )

    with pytest.raises(ValueError, match="principal realm"):
        splice_tgt_only(data, REALM)


def test_splice_excludes_tgt_expiring_at_current_time():
    now = int(time.time())
    data = _build_ccache(creds=(_cred(4, TGT, endtime=now),))

    with pytest.raises(ValueError, match="no eligible TGT"):
        splice_tgt_only(data, REALM)


def test_splice_converts_v3_keyblock_to_v4_and_preserves_times():
    endtime = int(time.time()) + 100
    data = _build_ccache(
        3,
        creds=(
            _cred(
                3,
                TGT,
                authtime=10,
                endtime=endtime,
                renew_until=30,
                key_bytes=b"v3-key",
            ),
        ),
    )

    spliced, _username = splice_tgt_only(data, REALM)
    credential = parse_ccache_full(spliced)["credentials"][0]

    assert parse_ccache_full(spliced)["version"] == 4
    assert credential["authtime"] == 10
    assert credential["starttime"] == 150
    assert credential["endtime"] == endtime
    assert credential["renew_until"] == 30


def test_splice_output_is_accepted_by_real_klist(tmp_path):
    data = _build_ccache(creds=(_cred(4, TGT, endtime=int(time.time()) + 100),))
    spliced, _username = splice_tgt_only(data, REALM)
    path = tmp_path / "spliced.ccache"
    path.write_bytes(spliced)

    result = subprocess.run(
        ["/usr/bin/klist", "-c", f"FILE:{path}"],
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr


def test_service_ticket_endtime_returns_one_unexpired_matching_ticket():
    endtime = int(time.time()) + 100
    data = _build_ccache(
        creds=(
            _cred(4, TARGET, endtime=endtime),
            _cred(4, TGT, endtime=endtime),
        ),
    )

    assert service_ticket_endtime(data, CLIENT, [b"host", b"target.example"], REALM) == endtime


def test_service_ticket_endtime_rejects_ambiguity():
    now = int(time.time())
    data = _build_ccache(
        creds=(
            _cred(4, TARGET, endtime=now + 100),
            _cred(4, TARGET, endtime=now + 200),
        ),
    )

    with pytest.raises(ValueError, match="exactly one"):
        service_ticket_endtime(data, CLIENT, [b"host", b"target.example"], REALM)


def test_service_ticket_endtime_rejects_foreign_default_principal_realm():
    now = int(time.time())
    data = _build_ccache(
        default_components=(b"FOREIGN.REALM", CLIENT),
        creds=(
            _cred(
                4,
                TARGET,
                client_components=(b"FOREIGN.REALM", CLIENT),
                endtime=now + 100,
            ),
        ),
    )

    with pytest.raises(ValueError, match="default principal"):
        service_ticket_endtime(data, CLIENT, [b"host", b"target.example"], REALM)


def test_service_ticket_endtime_excludes_ticket_expiring_at_current_time():
    now = int(time.time())
    data = _build_ccache(creds=(_cred(4, TARGET, endtime=now),))

    with pytest.raises(ValueError, match="exactly one|expired"):
        service_ticket_endtime(data, CLIENT, [b"host", b"target.example"], REALM)


@pytest.mark.parametrize(
    "credential",
    (
        _cred(4, (b"EXAMPLE.COM", b"host", b"other"), endtime=int(time.time()) + 100),
        _cred(4, TARGET, endtime=int(time.time()) - 1),
        _cred(4, TARGET, authtime=1, endtime=0),
        _cred(4, (b"X-CACHECONF:", b"proxy_impersonator"), endtime=int(time.time()) + 100),
    ),
)
def test_service_ticket_endtime_rejects_non_matching_or_unusable_ticket(credential):
    data = _build_ccache(creds=(credential,))

    with pytest.raises(ValueError):
        service_ticket_endtime(data, CLIENT, [b"host", b"target.example"], REALM)
