import os
import stat
import subprocess
import threading
from types import SimpleNamespace

import pytest

from fastink.auth.backends import krb5_validate_runtime as runtime


REALM = "IHEPKRB5"
SERVICE_PRINCIPAL = "xrootd/besproxy01.ihep.ac.cn@IHEPKRB5"
KRB5_VALIDATE_INFRA = getattr(runtime, "Krb5ValidateInfra", RuntimeError)


@pytest.fixture(autouse=True)
def reset_semaphore(monkeypatch):
    monkeypatch.setattr(runtime, "_kvno_semaphore", None)
    monkeypatch.setattr(runtime, "_kvno_semaphore_max", None)


def _settings(timeout=1.0, max_concurrency=2):
    return runtime.Krb5ValidateSettings(
        SERVICE_PRINCIPAL, REALM, 600, 5, timeout, max_concurrency
    )


def test_run_kvno_uses_fixed_argv_and_returns_post_run_bytes(monkeypatch):
    settings = _settings(timeout=2.5, max_concurrency=1)
    input_bytes = b"sanitized-tgt"
    output_bytes = b"kvno-updated-cache"
    calls = []

    def fake_run(argv, **kwargs):
        calls.append((argv, kwargs))
        cache_path = argv[2][len("FILE:") :]
        with open(cache_path, "wb") as cache_file:
            cache_file.write(output_bytes)
        return SimpleNamespace(returncode=0)

    monkeypatch.setattr(subprocess, "run", fake_run)

    assert runtime.run_kvno_validation(input_bytes, settings) == output_bytes

    assert len(calls) == 1
    argv, kwargs = calls[0]
    cache_path = argv[2][len("FILE:") :]
    assert argv == ["/usr/bin/kvno", "-c", f"FILE:{cache_path}", SERVICE_PRINCIPAL]
    assert kwargs["shell"] is False
    assert kwargs["timeout"] == settings.timeout
    assert kwargs["stdout"] is subprocess.DEVNULL
    assert kwargs["stderr"] is subprocess.PIPE


def test_run_kvno_writes_private_cache_with_expected_modes(monkeypatch):
    settings = _settings(max_concurrency=1)
    input_bytes = b"cache-bytes"
    observed = {}

    def fake_run(argv, **kwargs):
        cache_path = argv[2][len("FILE:") :]
        directory = os.path.dirname(cache_path)
        observed["directory"] = directory
        observed["file"] = cache_path
        observed["directory_mode"] = stat.S_IMODE(os.stat(directory).st_mode)
        observed["file_mode"] = stat.S_IMODE(os.stat(cache_path).st_mode)
        with open(cache_path, "rb") as cache_file:
            observed["bytes"] = cache_file.read()
        return SimpleNamespace(returncode=0)

    monkeypatch.setattr(subprocess, "run", fake_run)

    assert runtime.run_kvno_validation(input_bytes, settings) == input_bytes
    assert observed["directory_mode"] == 0o700
    assert observed["file_mode"] == 0o600
    assert observed["bytes"] == input_bytes
    assert not os.path.exists(observed["directory"])


def test_run_kvno_uses_only_controlled_environment(monkeypatch):
    settings = _settings(max_concurrency=1)
    monkeypatch.setenv("KRB5_TRACE", "/tmp/should-not-be-inherited")
    parent_environment = dict(os.environ)
    observed = {}

    def fake_run(argv, **kwargs):
        observed.update(kwargs["env"])
        observed["cache_path"] = argv[2][len("FILE:") :]
        return SimpleNamespace(returncode=0)

    monkeypatch.setattr(subprocess, "run", fake_run)

    runtime.run_kvno_validation(b"cache", settings)

    assert set(observed) == {
        "PATH",
        "LC_ALL",
        "KRB5_CONFIG",
        "KRB5CCNAME",
        "KRB5_CLIENT_KTNAME",
        "KRB5_KTNAME",
        "cache_path",
    }
    cache_path = observed.pop("cache_path")
    assert observed == {
        "PATH": "/usr/bin:/bin",
        "LC_ALL": "C",
        "KRB5_CONFIG": "/etc/krb5.conf",
        "KRB5CCNAME": f"FILE:{cache_path}",
        "KRB5_CLIENT_KTNAME": "/dev/null",
        "KRB5_KTNAME": "/dev/null",
    }
    assert dict(os.environ) == parent_environment


@pytest.mark.parametrize(
    "stderr",
    [
        "Matching credential not found",
        "Ticket expired",
        "integrity check failed",
        "not found in Kerberos database while getting initial credentials",
        "Generic preauthentication failure",
    ],
)
def test_run_kvno_credential_rejection_patterns_raise_kdc_rejection(
    monkeypatch, stderr
):
    settings = _settings(max_concurrency=1)
    observed = {}

    def fake_run(argv, **kwargs):
        cache_path = argv[2][len("FILE:") :]
        observed["directory"] = os.path.dirname(cache_path)
        return SimpleNamespace(returncode=1, stderr=stderr.encode())

    monkeypatch.setattr(subprocess, "run", fake_run)

    with pytest.raises(runtime.Krb5KdcRejection):
        runtime.run_kvno_validation(b"cache", settings)

    assert not os.path.exists(observed["directory"])


def test_run_kvno_infrastructure_pattern_takes_precedence_over_rejection(
    monkeypatch,
):
    settings = _settings(max_concurrency=1)
    monkeypatch.setattr(
        subprocess,
        "run",
        lambda argv, **kwargs: SimpleNamespace(
            returncode=1,
            stderr=b"Cannot contact any KDC: Ticket expired",
        ),
    )

    with pytest.raises(KRB5_VALIDATE_INFRA):
        runtime.run_kvno_validation(b"cache", settings)


@pytest.mark.parametrize(
    "stderr",
    [
        "Cannot contact any KDC",
        "Cannot resolve",
        "No route to host",
        "Network is unreachable",
        "Connection refused",
        "timed out",
        "Configuration error",
        "Cannot open",
        "Permission denied",
        "Server not found in Kerberos database",
        "unrecognized kvno failure",
        "",
    ],
)
def test_run_kvno_infrastructure_patterns_raise_infra(monkeypatch, stderr):
    settings = _settings(max_concurrency=1)

    monkeypatch.setattr(
        subprocess,
        "run",
        lambda argv, **kwargs: SimpleNamespace(
            returncode=1,
            stderr=stderr.encode(),
        ),
    )

    with pytest.raises(KRB5_VALIDATE_INFRA):
        runtime.run_kvno_validation(b"cache", settings)


def test_run_kvno_signal_exit_is_infrastructure_failure(monkeypatch):
    settings = _settings(max_concurrency=1)
    monkeypatch.setattr(
        subprocess,
        "run",
        lambda argv, **kwargs: SimpleNamespace(returncode=-9, stderr=b""),
    )

    with pytest.raises(KRB5_VALIDATE_INFRA):
        runtime.run_kvno_validation(b"cache", settings)


def test_run_kvno_only_classifies_first_64_kib_of_stderr(monkeypatch):
    settings = _settings(max_concurrency=1)
    stderr = b"x" * (64 * 1024) + b"Matching credential not found"
    monkeypatch.setattr(
        subprocess,
        "run",
        lambda argv, **kwargs: SimpleNamespace(returncode=1, stderr=stderr),
    )

    with pytest.raises(KRB5_VALIDATE_INFRA):
        runtime.run_kvno_validation(b"cache", settings)


def test_run_kvno_timeout_cleans_private_directory(monkeypatch):
    settings = _settings(timeout=0.25, max_concurrency=1)
    observed = {}

    def fake_run(argv, **kwargs):
        cache_path = argv[2][len("FILE:") :]
        observed["directory"] = os.path.dirname(cache_path)
        raise subprocess.TimeoutExpired(argv, kwargs["timeout"])

    monkeypatch.setattr(subprocess, "run", fake_run)

    with pytest.raises(runtime.Krb5ValidateTimeout):
        runtime.run_kvno_validation(b"cache", settings)

    assert not os.path.exists(observed["directory"])


def test_run_kvno_io_error_cleans_private_directory(monkeypatch):
    settings = _settings(max_concurrency=1)
    observed = {}

    def fake_run(argv, **kwargs):
        cache_path = argv[2][len("FILE:") :]
        observed["directory"] = os.path.dirname(cache_path)
        raise OSError("synthetic subprocess I/O failure")

    monkeypatch.setattr(subprocess, "run", fake_run)

    with pytest.raises(OSError, match="synthetic"):
        runtime.run_kvno_validation(b"cache", settings)

    assert not os.path.exists(observed["directory"])


def test_run_kvno_rejects_oversized_cache_without_spawning(monkeypatch):
    settings = _settings(max_concurrency=1)
    spawned = False

    def fake_run(argv, **kwargs):
        nonlocal spawned
        spawned = True
        raise AssertionError("kvno must not spawn for an oversized cache")

    monkeypatch.setattr(subprocess, "run", fake_run)

    with pytest.raises(ValueError, match="1 MiB"):
        runtime.run_kvno_validation(b"x" * (1024 * 1024 + 1), settings)

    assert not spawned


def test_run_kvno_busy_timeout_does_not_spawn(monkeypatch):
    settings = _settings(timeout=0.02, max_concurrency=1)
    semaphore = runtime.get_kvno_semaphore(settings)
    assert semaphore.acquire(blocking=False)
    spawned = False

    def fake_run(argv, **kwargs):
        nonlocal spawned
        spawned = True

    monkeypatch.setattr(subprocess, "run", fake_run)
    try:
        with pytest.raises(runtime.Krb5ValidateBusy):
            runtime.run_kvno_validation(b"cache", settings)
    finally:
        semaphore.release()

    assert not spawned


@pytest.mark.parametrize("outcome", ["success", "rejection", "timeout", "io"])
def test_run_kvno_releases_semaphore_on_every_subprocess_path(monkeypatch, outcome):
    settings = _settings(max_concurrency=1)

    def fake_run(argv, **kwargs):
        if outcome == "rejection":
            return SimpleNamespace(
                returncode=1,
                stderr=b"Matching credential not found",
            )
        if outcome == "timeout":
            raise subprocess.TimeoutExpired(argv, kwargs["timeout"])
        if outcome == "io":
            raise OSError("synthetic")
        return SimpleNamespace(returncode=0)

    monkeypatch.setattr(subprocess, "run", fake_run)

    expected_error = {
        "rejection": runtime.Krb5KdcRejection,
        "timeout": runtime.Krb5ValidateTimeout,
        "io": OSError,
    }.get(outcome)
    if expected_error is None:
        runtime.run_kvno_validation(b"cache", settings)
    else:
        with pytest.raises(expected_error):
            runtime.run_kvno_validation(b"cache", settings)

    semaphore = runtime.get_kvno_semaphore(settings)
    assert semaphore.acquire(blocking=False)
    semaphore.release()


def test_run_kvno_never_exceeds_concurrency_limit(monkeypatch):
    settings = _settings(max_concurrency=2)
    counter_lock = threading.Lock()
    initial_batch = threading.Barrier(settings.max_concurrency + 1)
    release_initial_batch = threading.Event()
    active_count = 0
    maximum_active_count = 0
    call_count = 0
    errors = []

    def fake_run(argv, **kwargs):
        nonlocal active_count, maximum_active_count, call_count
        with counter_lock:
            call_count += 1
            call_number = call_count
            active_count += 1
            maximum_active_count = max(maximum_active_count, active_count)
        try:
            if call_number <= settings.max_concurrency:
                initial_batch.wait(timeout=5)
                release_initial_batch.wait(timeout=5)
            return SimpleNamespace(returncode=0)
        finally:
            with counter_lock:
                active_count -= 1

    monkeypatch.setattr(subprocess, "run", fake_run)

    def worker():
        try:
            runtime.run_kvno_validation(b"cache", settings)
        except BaseException as error:
            with counter_lock:
                errors.append(error)

    threads = [threading.Thread(target=worker) for _ in range(6)]
    for thread in threads:
        thread.start()

    initial_batch.wait(timeout=5)
    release_initial_batch.set()
    for thread in threads:
        thread.join(timeout=5)

    assert all(not thread.is_alive() for thread in threads)
    assert not errors
    assert maximum_active_count <= settings.max_concurrency
    assert maximum_active_count == settings.max_concurrency
