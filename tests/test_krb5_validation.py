"""F-V2 regression: krb5 token validation must verify with the KDC.

The fixture below is a reconstructed forgery (labelled "reconstructed F-V2
regression"): a structurally valid v4 ccache containing NO real Kerberos
material — bogus key bytes, bogus ticket bytes — claiming identity "alice"
with a far-future TGT endtime. Before the fix, validate_krb5_token accepted
such a token purely on structure (the vulnerability). After the fix, the
same token must be rejected because the KDC never issued it.
"""

import base64
import struct
import time

import pytest

from fastink.auth.backends import krb5_validate_runtime as runtime
from fastink.auth.backends.ccache import parse_ccache_full
from fastink.auth.backends.krb5 import validate_krb5_token


def _u16(value):
    return struct.pack(">H", value)


def _u32(value):
    return struct.pack(">I", value)


def _data(value):
    return _u32(len(value)) + value


def _principal(realm, *components):
    return (
        _u32(1)
        + _u32(len(components))
        + _data(realm)
        + b"".join(_data(c) for c in components)
    )


SERVICE_PRINCIPAL = "xrootd/besproxy01.ihep.ac.cn@IHEPKRB5"
SERVICE_COMPONENTS = [b"xrootd", b"besproxy01.ihep.ac.cn"]


def _credential(
    *,
    client_realm=b"IHEPKRB5",
    client_components=(b"alice",),
    server_realm=b"IHEPKRB5",
    server_components=(b"xrootd", b"besproxy01.ihep.ac.cn"),
    endtime=None,
    ticket=b"ticket",
):
    now = int(time.time())
    return (
        _principal(client_realm, *client_components)
        + _principal(server_realm, *server_components)
        + _u16(18)
        + _u32(3)
        + b"key"
        + struct.pack(
            ">IIII", now, now, endtime if endtime is not None else now + 3600, 0
        )
        + b"\x00"
        + _u32(0)
        + _u32(0)
        + _u32(0)
        + _data(ticket)
        + _data(b"")
    )


def build_forged_ccache(
    username=b"alice",
    realm=b"IHEPKRB5",
    years=10,
    *,
    username_components=None,
    include_tgt=True,
    extra_credentials=b"",
):
    """Structurally valid v4 ccache with zero real Kerberos material."""
    now = int(time.time())
    header = _u16(0)  # empty v4 header (length excludes the length field)
    components = username_components or (username,)
    default_principal = _principal(realm, *components)
    credential = b""
    if include_tgt:
        credential = (
            _principal(realm, *components)  # client == claimed identity
            + _principal(realm, b"krbtgt", realm)  # server == krbtgt/<realm>@<realm>
            + _u16(18) + _u32(3) + b"key"  # keyblock with bogus key bytes
            + struct.pack(">IIII", now, now, now + years * 365 * 86400, 0)
            + b"\x00" + _u32(0)  # is_skey + ticket_flags
            + _u32(0)  # addresses count
            + _u32(0)  # authdata count
            + _data(b"not-der")  # ticket: bogus bytes, never issued by any KDC
            + _data(b"")  # second ticket
        )
    return b"\x05\x04" + header + default_principal + credential + extra_credentials


def _settings():
    return runtime.Krb5ValidateSettings(
        SERVICE_PRINCIPAL,
        "IHEPKRB5",
        600,
        5,
        5,
        8,
    )


def _validation_module(monkeypatch, runner=None):
    from fastink.auth.backends import krb5_validation

    class NoopRedis:
        def get(self, key):
            return None

        def set(self, key, value, *, ex=None, px=None):
            return None

        def delete(self, key):
            return 0

    monkeypatch.setattr(krb5_validation, "load_krb5_validate_settings", _settings)
    monkeypatch.setattr(krb5_validation, "_redis", lambda: NoopRedis())
    if runner is not None:
        monkeypatch.setattr(krb5_validation, "run_kvno_validation", runner)
    return krb5_validation


def _validated_cache(cache, *, username=b"alice", endtime=None):
    return cache + _credential(
        client_components=(username,),
        server_components=tuple(SERVICE_COMPONENTS),
        endtime=endtime,
    )


def test_forged_fixture_is_157_bytes():
    # Size pinned so this fixture stays comparable to the audit PoC.
    assert len(build_forged_ccache()) == 157


def test_forged_ccache_is_rejected(monkeypatch):
    """A ccache no KDC ever issued must not authenticate anyone."""
    token = base64.b64encode(build_forged_ccache()).decode()
    assert parse_ccache_full(build_forged_ccache())["credentials"]

    calls = []

    def reject_at_kdc(cache, settings):
        calls.append((cache, settings))
        assert parse_ccache_full(cache)["credentials"]
        raise runtime.Krb5KdcRejection(1)

    _validation_module(monkeypatch, reject_at_kdc)

    assert validate_krb5_token("alice", token) is False
    assert len(calls) == 1


def test_invalid_base64_uses_validate_semantics_and_does_not_run_kvno(monkeypatch):
    calls = []
    _validation_module(monkeypatch, lambda *args: calls.append(args))

    assert validate_krb5_token("alice", "not-base64$") is False
    assert calls == []


def test_encoded_input_over_two_mib_is_rejected_before_decode(monkeypatch):
    calls = []
    _validation_module(monkeypatch, lambda *args: calls.append(args))

    token = "A" * (2 * 1024 * 1024 + 1)

    assert validate_krb5_token("alice", token) is False
    assert calls == []


def test_decoded_input_over_one_mib_is_rejected_before_splicing(monkeypatch):
    calls = []
    _validation_module(monkeypatch, lambda *args: calls.append(args))

    token = base64.b64encode(b"x" * (1024 * 1024 + 1)).decode()

    assert validate_krb5_token("alice", token) is False
    assert calls == []


def test_wrong_realm_is_rejected_before_kvno(monkeypatch):
    calls = []
    _validation_module(monkeypatch, lambda *args: calls.append(args))
    token = base64.b64encode(build_forged_ccache(realm=b"OTHER")).decode()

    assert validate_krb5_token("alice", token) is False
    assert calls == []


def test_extra_username_component_is_rejected_before_kvno(monkeypatch):
    calls = []
    _validation_module(monkeypatch, lambda *args: calls.append(args))
    token = base64.b64encode(
        build_forged_ccache(username_components=(b"alice", b"extra"))
    ).decode()

    assert validate_krb5_token("alice", token) is False
    assert calls == []


def test_username_mismatch_is_rejected_without_running_kvno(monkeypatch):
    calls = []
    _validation_module(monkeypatch, lambda *args: calls.append(args))
    token = base64.b64encode(build_forged_ccache()).decode()

    assert validate_krb5_token("bob", token) is False
    assert calls == []


def test_missing_local_tgt_is_rejected_before_kvno(monkeypatch):
    calls = []
    _validation_module(monkeypatch, lambda *args: calls.append(args))
    token = base64.b64encode(build_forged_ccache(include_tgt=False)).decode()

    assert validate_krb5_token("alice", token) is False
    assert calls == []


def test_preseeded_target_ticket_is_stripped_before_kvno(monkeypatch):
    original = build_forged_ccache(
        extra_credentials=_credential(
            endtime=int(time.time()) + 3600,
            ticket=b"forged-target-ticket",
        )
    )
    received = []

    def issue_target(cache, settings):
        received.append(cache)
        parsed = parse_ccache_full(cache)
        assert all(
            credential["server"]["components"] != SERVICE_COMPONENTS
            for credential in parsed["credentials"]
        )
        return _validated_cache(cache)

    _validation_module(monkeypatch, issue_target)

    assert validate_krb5_token("alice", base64.b64encode(original).decode()) is True
    assert len(received) == 1


def test_kvno_rejection_returns_false(monkeypatch):
    def reject_at_kdc(cache, settings):
        raise runtime.Krb5KdcRejection(1)

    _validation_module(monkeypatch, reject_at_kdc)

    token = base64.b64encode(build_forged_ccache()).decode()
    assert validate_krb5_token("alice", token) is False


@pytest.mark.parametrize(
    "returned_cache",
    [
        lambda cache: cache,
        lambda cache: cache
        + _credential(
            server_components=(b"other", b"besproxy01.ihep.ac.cn"),
            endtime=int(time.time()) + 3600,
        ),
        lambda cache: cache
        + _credential(endtime=int(time.time()) - 1),
    ],
    ids=["missing-service-credential", "incorrect-service-credential", "expired-service-credential"],
)
def test_kvno_success_still_requires_one_unexpired_target_ticket(
    monkeypatch, returned_cache
):
    _validation_module(monkeypatch, lambda cache, settings: returned_cache(cache))

    token = base64.b64encode(build_forged_ccache()).decode()
    assert validate_krb5_token("alice", token) is False


def test_kvno_success_with_matching_unexpired_target_ticket_returns_true(monkeypatch):
    _validation_module(monkeypatch, lambda cache, settings: _validated_cache(cache))

    token = base64.b64encode(build_forged_ccache()).decode()
    assert validate_krb5_token("alice", token) is True


def test_public_validation_loads_settings_once(monkeypatch):
    module = _validation_module(monkeypatch)
    token = base64.b64encode(build_forged_ccache()).decode()
    settings = _settings()
    loads = []

    def load_settings():
        loads.append(None)
        return settings

    monkeypatch.setattr(module, "load_krb5_validate_settings", load_settings)
    monkeypatch.setattr(module, "_validate_with_kdc", lambda *args: int(time.time()) + 60)

    assert module.validate_krb5_token_real("alice", token) is True
    assert len(loads) == 1


def test_internal_exception_returns_literal_false(monkeypatch):
    def fail_inside_validation(cache, settings):
        raise RuntimeError("unexpected local failure")

    _validation_module(monkeypatch, fail_inside_validation)

    token = base64.b64encode(build_forged_ccache()).decode()
    result = validate_krb5_token("alice", token)

    assert result is False


def test_internal_validation_returns_exact_service_ticket_endtime(monkeypatch):
    endtime = int(time.time()) + 3600

    def issue_target(cache, settings):
        return _validated_cache(cache, endtime=endtime)

    module = _validation_module(monkeypatch, issue_target)
    token = base64.b64encode(build_forged_ccache()).decode()
    settings, spliced, username_bytes = module._prepare_validation(
        "alice", token
    )

    assert module._validate_with_kdc(settings, spliced, username_bytes) == endtime


@pytest.mark.parametrize(
    "error",
    [
        runtime.Krb5KdcRejection(1),
        runtime.Krb5ValidateTimeout("timeout"),
        runtime.Krb5ValidateBusy("busy"),
    ],
    ids=["kdc-rejection", "timeout", "busy"],
)
def test_internal_validation_propagates_runtime_errors(monkeypatch, error):
    def fail_runner(cache, settings):
        raise error

    module = _validation_module(monkeypatch, fail_runner)
    token = base64.b64encode(build_forged_ccache()).decode()
    settings, spliced, username_bytes = module._prepare_validation(
        "alice", token
    )

    with pytest.raises(type(error)):
        module._validate_with_kdc(settings, spliced, username_bytes)


def test_internal_validation_propagates_config_error(monkeypatch):
    module = _validation_module(monkeypatch, lambda *args: b"")
    error = runtime.Krb5ValidateConfigError("invalid settings")

    def fail_settings():
        raise error

    monkeypatch.setattr(module, "load_krb5_validate_settings", fail_settings)
    token = base64.b64encode(build_forged_ccache()).decode()

    with pytest.raises(runtime.Krb5ValidateConfigError):
        module._prepare_validation("alice", token)


def test_internal_validation_propagates_identity_value_error(monkeypatch):
    calls = []
    module = _validation_module(monkeypatch, lambda *args: calls.append(args))
    token = base64.b64encode(build_forged_ccache()).decode()

    with pytest.raises(ValueError):
        module._prepare_validation("bob", token)
    assert calls == []


def test_internal_validation_propagates_malformed_value_error(monkeypatch):
    calls = []
    module = _validation_module(monkeypatch, lambda *args: calls.append(args))

    with pytest.raises(ValueError):
        module._prepare_validation("alice", "not-base64$")
    assert calls == []
