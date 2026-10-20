"""Minimal MIT Kerberos FILE ccache (v3/v4) parser -- stdlib only."""

import struct
import time


def _require(data, offset, size, limit=None):
    end = len(data) if limit is None else limit
    if offset < 0 or size < 0 or offset > end - size:
        raise ValueError("truncated ccache")


def _u16(data, offset, limit=None):
    _require(data, offset, 2, limit)
    return struct.unpack_from(">H", data, offset)[0]


def _u32(data, offset, limit=None):
    _require(data, offset, 4, limit)
    return struct.unpack_from(">I", data, offset)[0]


def _bytes(data, offset, length, limit=None):
    _require(data, offset, length, limit)
    return data[offset : offset + length], offset + length


def _data(data, offset, limit=None):
    length = _u32(data, offset, limit)
    value, offset = _bytes(data, offset + 4, length, limit)
    return offset, value


def _principal(data, offset, version, limit=None):
    if version > 1:
        _require(data, offset, 8, limit)
        component_count = _u32(data, offset + 4, limit)
        offset += 8
    else:
        component_count = _u32(data, offset, limit) - 1
        offset += 4

    available = len(data) if limit is None else limit
    if component_count + 1 > (available - offset) // 4:
        raise ValueError("invalid ccache principal component count")
    realm_length = _u32(data, offset, limit)
    realm, offset = _bytes(data, offset + 4, realm_length, limit)
    components = []
    for _ in range(component_count):
        length = _u32(data, offset, limit)
        value, offset = _bytes(data, offset + 4, length, limit)
        components.append(value)
    return offset, {"realm": realm, "components": components}


def _parse_prefix(data):
    """Validate the header and parse the default principal."""
    if len(data) < 2 or data[0] != 5 or not 1 <= data[1] <= 4:
        raise ValueError("not a krb5 FILE ccache")

    version = data[1]
    if version < 3:
        raise ValueError("ccache v1/v2 (native byte order) unsupported")

    offset = 2
    if version == 4:
        header_length = _u16(data, offset)
        offset += 2
        header_end = offset + header_length
        if header_end > len(data):
            raise ValueError("ccache header exceeds input")
        while offset < header_end:
            if header_end - offset < 4:
                raise ValueError("truncated ccache header TLV")
            tag = _u16(data, offset, header_end)
            tag_length = _u16(data, offset + 2, header_end)
            offset += 4
            if tag_length > header_end - offset:
                raise ValueError("truncated ccache header TLV value")
            if tag == 1 and tag_length != 8:
                raise ValueError("invalid DELTATIME header field")
            offset += tag_length
        if offset != header_end:
            raise ValueError("ccache header boundary mismatch")

    principal_start = offset
    principal_offset, principal = _principal(data, offset, version)
    return version, principal_offset, principal, (principal_start, principal_offset)


def _counted_items(data, offset, count):
    if count > (len(data) - offset) // 6:
        raise ValueError("invalid ccache item count")
    for _ in range(count):
        _require(data, offset, 2)
        offset += 2
        offset, _value = _data(data, offset)
    return offset


def _parse_credential(data, offset, version):
    record_start = offset
    offset, client = _principal(data, offset, version)
    offset, server = _principal(data, offset, version)

    keyblock_start = offset
    _u16(data, offset)
    offset += 2
    if version == 3:
        _u16(data, offset)
        offset += 2
    key_length = _u32(data, offset)
    _key, offset = _bytes(data, offset + 4, key_length)
    keyblock_end = offset

    authtime = _u32(data, offset)
    starttime = _u32(data, offset + 4)
    endtime = _u32(data, offset + 8)
    renew_until = _u32(data, offset + 12)
    offset += 16
    _require(data, offset, 1)
    offset += 1
    _u32(data, offset)
    offset += 4

    address_count = _u32(data, offset)
    offset = _counted_items(data, offset + 4, address_count)
    authdata_count = _u32(data, offset)
    offset = _counted_items(data, offset + 4, authdata_count)
    offset, _ticket = _data(data, offset)
    offset, _second_ticket = _data(data, offset)

    return offset, {
        "client": client,
        "server": server,
        "record_range": (record_start, offset),
        "keyblock_range": (keyblock_start, keyblock_end),
        "authtime": authtime,
        "starttime": starttime,
        "endtime": endtime,
        "renew_until": renew_until,
        "is_config": server["realm"] == b"X-CACHECONF:",
        "is_removed": endtime == 0 and authtime != 0,
    }


def parse_ccache_full(data: bytes, *, for_auth: bool = False) -> dict:
    """Parse a complete v3/v4 FILE ccache into a bounded representation."""
    if for_auth and len(data) > 1024 * 1024:
        raise ValueError("ccache exceeds 1 MiB authentication limit")

    version, offset, default_principal, default_principal_range = _parse_prefix(data)
    credentials = []
    while offset < len(data):
        offset, credential = _parse_credential(data, offset, version)
        credentials.append(credential)
    return {
        "version": version,
        "default_principal": default_principal,
        "default_principal_range": default_principal_range,
        "credentials": credentials,
    }


def parse_ccache(data: bytes) -> dict:
    """Return the username and validity times from a FILE ccache."""
    parsed = parse_ccache_full(data)
    components = parsed["default_principal"]["components"]
    username = components[0].decode(errors="replace") if components else ""

    best = None
    for credential in parsed["credentials"]:
        server = credential["server"]["components"]
        if credential["is_config"] or credential["is_removed"]:
            continue
        if len(server) > 0 and server[0] == b"krbtgt":
            if best is None or credential["endtime"] > best[0]:
                best = (credential["endtime"], credential["renew_until"])

    if best is None:
        raise ValueError("no usable krbtgt credential in ccache")
    return {"username": username, "expired_at": best[0], "renew_until": best[1]}


def splice_tgt_only(data: bytes, realm: bytes) -> tuple[bytes, bytes]:
    """Return a fresh v4 ccache containing only the local unexpired TGTs."""
    parsed = parse_ccache_full(data, for_auth=True)
    default_principal = parsed["default_principal"]
    if default_principal["realm"] != realm:
        raise ValueError("default principal realm does not match configured realm")
    components = default_principal["components"]
    if len(components) != 1:
        raise ValueError("default principal must have a single principal component")

    now = int(time.time())
    records = []
    for credential in parsed["credentials"]:
        if credential["is_config"] or credential["is_removed"]:
            continue
        if credential["client"] != default_principal:
            continue
        server = credential["server"]
        if server["realm"] != realm or server["components"] != [b"krbtgt", realm]:
            continue
        if credential["endtime"] <= now:
            continue

        record_start, record_end = credential["record_range"]
        if parsed["version"] == 3:
            keyblock_start, keyblock_end = credential["keyblock_range"]
            keyblock = data[keyblock_start:keyblock_end]
            record = (
                data[record_start:keyblock_start]
                + keyblock[:2]
                + keyblock[4:]
                + data[keyblock_end:record_end]
            )
        else:
            record = data[record_start:record_end]
        records.append(record)

    if not records:
        raise ValueError("no eligible TGT in ccache")

    principal_start, principal_end = parsed["default_principal_range"]
    output = (
        b"\x05\x04\x00\x00"
        + data[principal_start:principal_end]
        + b"".join(records)
    )
    return output, components[0]


def service_ticket_endtime(
    data: bytes,
    client: bytes,
    server_components: list[bytes],
    server_realm: bytes,
) -> int:
    """Return the expiry of one unexpired service ticket for ``client``."""
    parsed = parse_ccache_full(data, for_auth=True)
    default_principal = parsed["default_principal"]
    if default_principal != {"realm": server_realm, "components": [client]}:
        raise ValueError("client does not match default principal")

    matches = []
    for credential in parsed["credentials"]:
        if credential["is_config"] or credential["is_removed"]:
            continue
        if credential["client"] != default_principal:
            continue
        server = credential["server"]
        if server["realm"] == server_realm and server["components"] == server_components:
            matches.append(credential)

    if len(matches) != 1:
        raise ValueError("expected exactly one matching service ticket")
    endtime = matches[0]["endtime"]
    if endtime <= int(time.time()):
        raise ValueError("matching service ticket is expired")
    return endtime


def has_afs_ticket(data: bytes) -> bool:
    """True if the ccache holds an unexpired afs service ticket."""
    import time

    parsed = parse_ccache_full(data)
    now = int(time.time())
    found = False
    for credential in parsed["credentials"]:
        server = credential["server"]["components"]
        if credential["is_config"] or credential["is_removed"]:
            continue
        if (
            len(server) > 0
            and server[0] == b"afs"
            and credential["endtime"] > now
        ):
            found = True
    return found


def check_krb5_validity(krb5ccname: str) -> bool:
    """True if the ccache file holds a TGT with >= 1800s remaining validity."""
    import time

    with open(krb5ccname, "rb") as f:
        tgt = parse_ccache(f.read())
    return (tgt["expired_at"] - int(time.time())) >= 1800
