"""Minimal MIT Kerberos FILE ccache (v3/v4) parser -- stdlib only."""

import struct


def _u16(data, offset):
    return struct.unpack_from(">H", data, offset)[0]


def _u32(data, offset):
    return struct.unpack_from(">I", data, offset)[0]


def _principal(data, offset, version):
    if version > 1:
        component_count = _u32(data, offset + 4)
        offset += 8
    else:
        component_count = _u32(data, offset) - 1
        offset += 4
    components = []
    for _ in range(component_count + 1):
        length = _u32(data, offset)
        offset += 4
        components.append(data[offset : offset + length])
        offset += length
    return offset, components


def _parse_prefix(data):
    """Validate the header and parse the default principal.

    Returns (version, credential_offset, default_principal_components).
    """
    if len(data) < 2 or data[0] != 5 or not 1 <= data[1] <= 4:
        raise ValueError("not a krb5 FILE ccache")

    version = data[1]
    if version < 3:
        raise ValueError("ccache v1/v2 (native byte order) unsupported")

    offset = 2
    if version == 4:
        header_length = _u16(data, offset)
        offset += 2
        header_end = offset + header_length - 2
        while offset < header_end:
            tag_length = _u16(data, offset + 2)
            offset += 4 + tag_length

    offset, default_principal = _principal(data, offset, version)
    return version, offset, default_principal


def _iter_credentials(data, offset, version):
    """Yield (server, authtime, endtime, renew_until) per credential,
    skipping config and removed entries."""
    while offset < len(data):
        offset, _client = _principal(data, offset, version)
        offset, server = _principal(data, offset, version)

        offset += 4 if version == 3 else 2
        key_length = _u32(data, offset)
        offset += 4 + key_length

        authtime, _starttime, endtime, renew_until = struct.unpack_from(
            ">IIII", data, offset
        )
        offset += 16
        offset += 5

        for _ in range(2):
            item_count = _u32(data, offset)
            offset += 4
            for _ in range(item_count):
                offset += 2
                item_length = _u32(data, offset)
                offset += 4 + item_length

        for _ in range(2):
            ticket_length = _u32(data, offset)
            offset += 4 + ticket_length

        if server[0] == b"X-CACHECONF:":
            continue
        if endtime == 0 and authtime != 0:
            continue
        yield server, authtime, endtime, renew_until


def parse_ccache(data: bytes) -> dict:
    """Return the username and validity times from a FILE ccache."""
    version, offset, default_principal = _parse_prefix(data)
    username = (
        default_principal[1].decode(errors="replace")
        if len(default_principal) > 1
        else ""
    )

    best = None
    for server, _authtime, endtime, renew_until in _iter_credentials(
        data, offset, version
    ):
        if len(server) > 1 and server[1] == b"krbtgt":
            if best is None or endtime > best[0]:
                best = (endtime, renew_until)

    if best is None:
        raise ValueError("no usable krbtgt credential in ccache")
    return {"username": username, "expired_at": best[0], "renew_until": best[1]}


def has_afs_ticket(data: bytes) -> bool:
    """True if the ccache holds an unexpired afs service ticket.

    aklog exchanges the TGT for an ``afs/<cell>@<REALM>`` service ticket;
    its first component is always ``afs`` regardless of cell or realm, so
    matching component[1] is site-agnostic — no cell name is hardcoded
    (open-source deployments may have no AFS, or a different cell).
    """
    import time

    version, offset, _default_principal = _parse_prefix(data)
    now = int(time.time())
    for server, _authtime, endtime, _renew_until in _iter_credentials(
        data, offset, version
    ):
        if len(server) > 1 and server[1] == b"afs" and endtime > now:
            return True
    return False


def check_krb5_validity(krb5ccname: str) -> bool:
    """True if the ccache file holds a TGT with >= 1800s remaining validity.

    Preserves the original semantics: >= 30 minutes remaining -> True,
    otherwise False. Parses the FILE ccache bytes directly (no klist/awk).
    Raises on unreadable/malformed ccache, matching the pre-migration
    behavior (which re-raised subprocess errors).
    """
    import time

    with open(krb5ccname, "rb") as f:
        tgt = parse_ccache(f.read())
    return (tgt["expired_at"] - int(time.time())) >= 1800
