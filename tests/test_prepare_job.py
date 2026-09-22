import asyncio
import importlib
import json
from types import SimpleNamespace

import pytest
from fastapi.testclient import TestClient

import fastink.computing.adapter.hpcadapter as hpcadapter
from fastink.computing.adapter.hpcadapter import HPC_Scheduler
from fastink.computing.cluster.cluster import HTC_JOB, SLURM_JOB, SubmitMode
from fastink.computing.job_prepare import JobPreparationError
from fastink.main import app


client = TestClient(app)


@pytest.fixture(autouse=True)
def valid_auth(monkeypatch):
    from fastink.routers.v2 import compute_resources

    monkeypatch.setattr(
        compute_resources.headers,
        "validate_token",
        lambda username, token: (username, token) == ("alice", "valid-token"),
    )


def auth_headers():
    return {"Ink-Username": "alice", "Ink-Token": "valid-token"}


def test_new_endpoints_require_auth(monkeypatch):
    from fastink.routers.v2 import compute_resources

    monkeypatch.setattr(compute_resources.headers, "validate_token", lambda _u, _t: False)

    list_response = client.get("/api/v2/cr/list_job_types", headers=auth_headers())
    prepare_response = client.post(
        "/api/v2/cr/prepare_job",
        headers=auth_headers(),
        json={"job_type": "vscode"},
    )

    assert list_response.status_code == 200
    assert prepare_response.status_code == 200
    assert list_response.json()["status"] != "200"
    assert prepare_response.json()["status"] != "200"


def validate_payload(cluster_id, payload):
    model = SLURM_JOB if cluster_id == "slurm" else HTC_JOB
    return model.model_validate(payload)


def patch_prepare_config(monkeypatch, clusters, jobtype_defaults=None):
    jobtype_defaults = jobtype_defaults or {}

    def fake_get_config(section, option=None, fallback=None, **_kwargs):
        if section == "computing" and option == "cluster_list":
            return list(clusters)
        if section == "jobtype" and option:
            return jobtype_defaults.get(option, fallback)
        return fallback

    monkeypatch.setattr("fastink.computing.job_prepare.get_config", fake_get_config)


@pytest.fixture(autouse=True)
def pinned_clusters(monkeypatch):
    """Pin the ambient cluster list: tests must not depend on site config
    (CI renders htcondor-only). Per-test patch_prepare_config calls still win."""
    patch_prepare_config(monkeypatch, ["slurm", "htcondor"])


def test_slurm_walltime_conversion_supports_days():
    module = importlib.import_module("fastink.computing.job_prepare")

    payload = module.prepare_job_request(
        "vscode", "slurm", {"walltime_seconds": 90000}
    )

    assert payload["time"] == "1-01:00:00"


@pytest.mark.parametrize(
    "override",
    [
        {"cpus": 4},
        {"memory_mb": 8192},
        {"walltime_seconds": 60},
        {"gpu_count": 1},
    ],
)
def test_htcondor_resource_overrides_are_rejected(override):
    with pytest.raises(JobPreparationError) as error:
        importlib.import_module("fastink.computing.job_prepare").prepare_job_request(
            "vscode", "htcondor", override
        )

    assert "site-managed" in str(error.value)
    assert "use --payload" in str(error.value)


def test_os_override_is_rejected_on_slurm():
    with pytest.raises(JobPreparationError, match="no unambiguous mapping"):
        importlib.import_module("fastink.computing.job_prepare").prepare_job_request(
            "vscode", "slurm", {"os": "AlmaLinux9"}
        )


@pytest.mark.parametrize("os_value", [123, True, ""])
def test_htcondor_os_override_must_be_non_empty_string(os_value):
    with pytest.raises(JobPreparationError, match="must be a non-empty string"):
        importlib.import_module("fastink.computing.job_prepare").prepare_job_request(
            "vscode", "htcondor", {"os": os_value}
        )


def test_htc_payload_uses_configured_os_default(monkeypatch):
    patch_prepare_config(
        monkeypatch,
        ["htcondor"],
        {"vscode": {"htc": {"os": "AlmaLinux9"}}},
    )

    payload = importlib.import_module("fastink.computing.job_prepare").prepare_job_request(
        "vscode", "htcondor", {}
    )

    assert payload["os"] == "AlmaLinux9"


def test_htc_os_override_beats_configured_default(monkeypatch):
    patch_prepare_config(
        monkeypatch,
        ["htcondor"],
        {"vscode": {"htc": {"os": "AlmaLinux9"}}},
    )

    payload = importlib.import_module("fastink.computing.job_prepare").prepare_job_request(
        "vscode", "htcondor", {"os": "RockyLinux9"}
    )

    assert payload["os"] == "RockyLinux9"


def test_htc_requires_os_from_override_or_config(monkeypatch):
    patch_prepare_config(monkeypatch, ["htcondor"])

    with pytest.raises(
        JobPreparationError,
        match=(
            "job type 'vscode' on htcondor requires an OS: pass os explicitly "
            "or set jobtype.vscode.htc.os in config"
        ),
    ):
        importlib.import_module("fastink.computing.job_prepare").prepare_job_request(
            "vscode", "htcondor", {}
        )


def test_htc_payload_keeps_scheduler_topology_empty(monkeypatch):
    patch_prepare_config(
        monkeypatch,
        ["htcondor"],
        {
            "vscode": {
                "htc": {
                    "RequestCpus": 2,
                    "RequestMemory": 9000,
                    "os": "AlmaLinux9",
                    "schedd_host": "site-schedd",
                    "cm_host": "site-cm",
                }
            }
        },
    )

    payload = importlib.import_module("fastink.computing.job_prepare").prepare_job_request(
        "vscode", "htcondor", {}
    )

    assert "schedd" not in payload
    assert "cm" not in payload


def test_slurm_gpu_requires_site_gpu_name(monkeypatch):
    patch_prepare_config(monkeypatch, ["slurm"])

    with pytest.raises(JobPreparationError, match="gpu_count requires a site-provided gpu_name"):
        importlib.import_module("fastink.computing.job_prepare").prepare_job_request(
            "vscode", "slurm", {"gpu_count": 1}
        )


def test_slurm_gpu_defaults_reach_payload_and_sbatch(monkeypatch):
    patch_prepare_config(
        monkeypatch,
        ["slurm"],
        {"vscode": {"slurm": {"gpu_name": "a100", "gpu_type": "40g"}}},
    )
    payload = importlib.import_module("fastink.computing.job_prepare").prepare_job_request(
        "vscode", "slurm", {"gpu_count": 2}
    )

    assert payload["gpu_num"] == 2
    assert payload["gpu_name"] == "a100"
    assert payload["gpu_type"] == "40g"

    scheduler = object.__new__(HPC_Scheduler)
    scheduler.USERNAME = "alice"
    scheduler.XROOTD_PATH = ""
    scheduler.UID = 1000
    monkeypatch.setattr(HPC_Scheduler, "_get_interactive_job_types", lambda _self: [])

    async def upload_file(**_kwargs):
        return None

    monkeypatch.setattr(hpcadapter.common, "upload_file", upload_file)
    command, _, _ = asyncio.run(
        scheduler._gen_slurm_submit_cmd(
            cpu=payload["cpu"],
            mem=payload["mem"],
            jobname="",
            jobtype="vscode",
            jobdir="/jobs/vscode",
            partition=payload["partition"],
            account=payload["account"],
            qos=payload["qos"],
            gpu_num=payload["gpu_num"],
            gpu_name=payload["gpu_name"],
            gpu_type=payload["gpu_type"],
            job_content="echo ok",
        )
    )

    assert "--gres=a100:40g:2" in command


def test_prepare_rejects_falsy_non_mapping_overrides():
    with pytest.raises(JobPreparationError, match="overrides must be an object"):
        importlib.import_module("fastink.computing.job_prepare").prepare_job_request(
            "vscode", "slurm", []
        )


@pytest.mark.parametrize(
    ("job_type", "expected_cluster"),
    [("vscode", "slurm"), ("opencode", "htcondor")],
)
def test_prepare_selects_supported_default_cluster(monkeypatch, job_type, expected_cluster):
    patch_prepare_config(monkeypatch, ["slurm", "htcondor"])
    if job_type == "opencode":
        patch_prepare_config(
            monkeypatch,
            ["slurm", "htcondor"],
            {"opencode": {"htc": {"os": "AlmaLinux9"}}},
        )

    payload = importlib.import_module("fastink.computing.job_prepare").prepare_job_request(
        job_type
    )

    assert payload["cluster_id"] == expected_cluster


def test_prepare_rejects_empty_cluster_id(monkeypatch):
    patch_prepare_config(monkeypatch, ["slurm", "htcondor"])

    with pytest.raises(JobPreparationError, match="cluster_id"):
        importlib.import_module("fastink.computing.job_prepare").prepare_job_request(
            "vscode", "", {}
        )


def test_prepare_reports_ambiguous_supported_clusters(monkeypatch):
    patch_prepare_config(monkeypatch, ["other", "slurm", "htcondor"])

    with pytest.raises(JobPreparationError) as error:
        importlib.import_module("fastink.computing.job_prepare").prepare_job_request(
            "vscode"
        )

    assert "no usable default cluster" in str(error.value)
    assert "supported clusters: slurm, htcondor" in str(error.value)


def test_asicbm_like_app_without_request_memory_is_not_supported(monkeypatch):
    app = SimpleNamespace(
        name="asicbm",
        connect_type="asicbm",
        request_defaults={"htc": {"RequestCpus": 1}},
    )
    registry = importlib.import_module("fastink.computing.job_prepare").computing_registry
    monkeypatch.setattr(registry, "try_get", lambda _name: app)
    monkeypatch.setattr(registry, "names", lambda: [app.name])
    monkeypatch.setattr(registry, "all_apps", lambda: [app])
    patch_prepare_config(monkeypatch, ["htcondor"])

    with pytest.raises(JobPreparationError, match="supported clusters: none"):
        importlib.import_module("fastink.computing.job_prepare").prepare_job_request(
            "asicbm", "htcondor", {}
        )

    assert importlib.import_module(
        "fastink.computing.job_prepare"
    ).list_job_type_capabilities()["job_types"] == []


def make_slurm_job(time_limit="01:02:03", submit_mode=SubmitMode.ASYNC):
    return SLURM_JOB(
        job_script="echo ok",
        cpu=1,
        mem=6000,
        job_type="vscode",
        cluster_id="slurm",
        submit_mode=submit_mode,
        time=time_limit,
        partition="",
        account="",
        qos="",
    )


def scheduler_command_kwargs(**overrides):
    params = {
        "cpu": 1,
        "mem": 6000,
        "jobname": "",
        "jobtype": "batch",
        "jobdir": "/jobs/batch",
        "partition": "",
        "account": "",
        "qos": "",
        "job_content": "echo ok",
    }
    params.update(overrides)
    return params


def test_interactive_time_override_beats_configured_limit(monkeypatch):
    scheduler = object.__new__(HPC_Scheduler)
    scheduler.USERNAME = "alice"
    scheduler.XROOTD_PATH = ""

    def fake_get_config(section, option=None, fallback=None, **_kwargs):
        if section == "computing" and option == "interactive_job_time_limit":
            return "09:00:00"
        if section == "computing" and option == "cluster_scripts":
            return "/cluster-scripts"
        return fallback

    class FakeFile:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        def read(self):
            return b"#!/bin/sh"

    async def upload_file(**_kwargs):
        return None

    monkeypatch.setattr(hpcadapter, "get_config", fake_get_config)
    monkeypatch.setattr(HPC_Scheduler, "_get_interactive_job_types", lambda _self: ["vscode"])
    monkeypatch.setattr(hpcadapter, "open", lambda *_args, **_kwargs: FakeFile(), raising=False)
    monkeypatch.setattr(hpcadapter.common, "upload_file", upload_file)

    command, _, _ = asyncio.run(
        scheduler._gen_slurm_submit_cmd(
            **scheduler_command_kwargs(
                jobtype="vscode", time="01:30:00", job_content=None
            )
        )
    )

    assert "--time=01:30:00" in command
    assert "--time=09:00:00" not in command


def test_noninteractive_time_is_only_added_when_set(monkeypatch):
    scheduler = object.__new__(HPC_Scheduler)
    scheduler.USERNAME = "alice"
    scheduler.XROOTD_PATH = ""
    scheduler.UID = 1000

    async def upload_file(**_kwargs):
        return None

    monkeypatch.setattr(HPC_Scheduler, "_get_interactive_job_types", lambda _self: [])
    monkeypatch.setattr(hpcadapter.common, "upload_file", upload_file)

    without_time, _, _ = asyncio.run(
        scheduler._gen_slurm_submit_cmd(**scheduler_command_kwargs())
    )
    with_time, _, _ = asyncio.run(
        scheduler._gen_slurm_submit_cmd(
            **scheduler_command_kwargs(time="90000")
        )
    )

    assert "--time=" not in without_time
    assert "--time=1-01:00:00" in with_time


def test_sync_submission_passes_time_to_sbatch_generator(monkeypatch):
    scheduler = object.__new__(HPC_Scheduler)
    scheduler.USERNAME = "alice"
    scheduler.UID = 1000
    scheduler.CLUSTER_TYPE = "slurm"
    captured = {}

    async def init_job_dir(*_args, **_kwargs):
        return "/jobs/vscode"

    async def generate_command(**kwargs):
        captured.update(kwargs)
        return "submit", "out", "err"

    async def submit_command(*_args, **_kwargs):
        return b"123"

    monkeypatch.setattr(hpcadapter, "init_job_dir", init_job_dir)
    monkeypatch.setattr(scheduler, "_gen_slurm_submit_cmd", generate_command)
    monkeypatch.setattr(hpcadapter, "sub_command", submit_command)
    monkeypatch.setattr(hpcadapter, "insert_job_info", lambda *_args, **_kwargs: None)

    asyncio.run(scheduler.submit_job_sync(make_slurm_job(submit_mode=SubmitMode.SYNC)))

    assert captured["time"] == "01:02:03"


def test_async_payload_includes_time(monkeypatch):
    class Pipeline:
        def __init__(self, redis):
            self.redis = redis

        def rpush(self, key, value):
            self.redis.commands.append(("rpush", key, value))

        def set(self, key, value, **kwargs):
            self.redis.commands.append(("set", key, value, kwargs))

        async def execute(self):
            return []

        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            return False

    class Redis:
        def __init__(self):
            self.commands = []

        async def lrange(self, *_args):
            return []

        def pipeline(self, **_kwargs):
            return Pipeline(self)

    scheduler = object.__new__(HPC_Scheduler)
    scheduler.USERNAME = "alice"
    scheduler.UID = 1000
    scheduler.CLUSTER_TYPE = "slurm"
    redis = Redis()
    monkeypatch.setattr(hpcadapter, "redis_connect", lambda: redis)
    monkeypatch.setattr(scheduler, "_need_dedup", lambda _job_data: False)
    monkeypatch.setattr(hpcadapter, "get_config", lambda _section, _option, fallback=None: fallback)

    asyncio.run(scheduler.submit_job_async(make_slurm_job()))

    raw_payload = next(
        value
        for command, key, value, *_rest in redis.commands
        if command == "rpush" and key == "submitting_jobs:slurm"
    )
    assert json.loads(raw_payload)["time"] == "01:02:03"


def test_queue_submission_passes_time_to_sbatch_generator(monkeypatch):
    class Pipeline:
        def hset(self, *_args, **_kwargs):
            return None

        def sadd(self, *_args, **_kwargs):
            return None

        async def execute(self):
            return []

        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            return False

    class Redis:
        async def get(self, *_args):
            return None

        async def set(self, *_args, **_kwargs):
            return True

        async def lrange(self, *_args):
            return []

        def pipeline(self, **_kwargs):
            return Pipeline()

    scheduler = object.__new__(HPC_Scheduler)
    scheduler.CLUSTER_TYPE = "slurm"
    captured = {}
    redis = Redis()

    async def init_job_dir(*_args, **_kwargs):
        return "/jobs/vscode"

    async def generate_command(**kwargs):
        captured.update(kwargs)
        return "submit", "out", "err"

    async def submit_command(*_args, **_kwargs):
        return b"123"

    monkeypatch.setattr(hpcadapter, "redis_connect", lambda: redis)
    monkeypatch.setattr(hpcadapter, "init_job_dir", init_job_dir)
    monkeypatch.setattr(scheduler, "_gen_slurm_submit_cmd", generate_command)
    monkeypatch.setattr(hpcadapter, "sub_command", submit_command)
    monkeypatch.setattr(hpcadapter, "insert_job_info", lambda *_args, **_kwargs: None)

    asyncio.run(
        scheduler.submit_job_from_queue(
            {
                "username": "alice",
                "uid": 1000,
                "job_type": "vscode",
                "submit_uuid": "uuid",
                "cpu": 1,
                "mem": 6000,
                "job_name": "",
                "partition": "",
                "account": "",
                "qos": "",
                "ntasks": 1,
                "nodes": 1,
                "gpu_num": 0,
                "time": "02:00:00",
                "job_script": "echo ok",
            }
        )
    )

    assert captured["time"] == "02:00:00"


def test_list_job_types_shape_and_no_sensitive_fields():
    response = client.get("/api/v2/cr/list_job_types", headers=auth_headers())

    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "200"
    assert body["data"]["default_cluster"] == body["data"]["clusters"][0]
    assert set(body["data"]) == {
        "default_cluster",
        "clusters",
        "job_types",
    }
    assert body["data"]["job_types"]
    for job_type in body["data"]["job_types"]:
        assert set(job_type) == {"name", "connect_type", "clusters"}
        assert set(job_type["clusters"]).issubset(body["data"]["clusters"])


def test_prepare_defaults_validate_for_each_advertised_cluster(monkeypatch):
    listing = client.get("/api/v2/cr/list_job_types", headers=auth_headers()).json()
    jobtype_defaults = {
        item["name"]: {"htc": {"os": "AlmaLinux9"}}
        for item in listing["data"]["job_types"]
    }
    patch_prepare_config(monkeypatch, ["slurm", "htcondor"], jobtype_defaults)

    for job_type in listing["data"]["job_types"]:
        for cluster_id in job_type["clusters"]:
            response = client.post(
                "/api/v2/cr/prepare_job",
                headers=auth_headers(),
                json={"job_type": job_type["name"], "cluster_id": cluster_id},
            )
            assert response.status_code == 200
            body = response.json()
            assert body["status"] == "200"
            assert body["data"]["cluster_id"] == cluster_id
            validate_payload(cluster_id, body["data"])


def test_empty_overrides_match_explicit_empty_overrides(monkeypatch):
    patch_prepare_config(
        monkeypatch,
        ["slurm", "htcondor"],
        {"vscode": {"htc": {"os": "AlmaLinux9"}}},
    )
    without_overrides = client.post(
        "/api/v2/cr/prepare_job",
        headers=auth_headers(),
        json={"job_type": "vscode", "cluster_id": "slurm"},
    )
    with_empty_overrides = client.post(
        "/api/v2/cr/prepare_job",
        headers=auth_headers(),
        json={
            "job_type": "vscode",
            "cluster_id": "slurm",
            "overrides": {},
        },
    )

    assert without_overrides.json()["data"] == with_empty_overrides.json()["data"]
    assert without_overrides.json()["data"] == {
        "job_script": "",
        "script_path": None,
        "job_dir": "",
        "job_parameters": "",
        "cpu": 1,
        "mem": 6000,
        "gpu_num": 0,
        "job_name": "",
        "job_type": "vscode",
        "cluster_id": "slurm",
        "submit_mode": "async",
        "time": "24:00:00",
        "partition": "",
        "nodes": 1,
        "ntasks": 1,
        "account": "",
        "qos": "",
        "gpu_name": None,
        "gpu_type": None,
        "output_file": None,
        "error_file": None,
        "input_path": None,
    }

    htc_defaults = client.post(
        "/api/v2/cr/prepare_job",
        headers=auth_headers(),
        json={"job_type": "vscode", "cluster_id": "htcondor", "overrides": {}},
    ).json()["data"]
    assert htc_defaults["cpu"] == 1
    assert htc_defaults["mem"] == 6000


@pytest.mark.parametrize(
    ("cluster_id", "overrides", "expected"),
    [
        (
            "slurm",
            {
                "cpus": 4,
                "memory_mb": 8192,
                "walltime_seconds": 3661,
            },
            {"cpu": 4, "mem": 8192, "time": "01:01:01"},
        ),
    ],
)
def test_overrides_map_to_validated_payload(cluster_id, overrides, expected):
    response = client.post(
        "/api/v2/cr/prepare_job",
        headers=auth_headers(),
        json={"job_type": "vscode", "cluster_id": cluster_id, "overrides": overrides},
    )

    assert response.json()["status"] == "200"
    payload = response.json()["data"]
    validate_payload(cluster_id, payload)
    for field, value in expected.items():
        assert payload[field] == value


@pytest.mark.parametrize(
    ("body", "message"),
    [
        ({"job_type": "missing"}, "vscode"),
        ({"job_type": "vscode", "cluster_id": "missing"}, "slurm"),
        (
            {"job_type": "vscode", "cluster_id": "slurm", "overrides": {"bogus": 1}},
            "cpus",
        ),
        (
            {"job_type": "vscode", "cluster_id": "slurm", "overrides": {"cpus": 0}},
            "positive",
        ),
    ],
)
def test_invalid_prepare_requests_are_business_errors(body, message):
    response = client.post(
        "/api/v2/cr/prepare_job",
        headers=auth_headers(),
        json=body,
    )

    assert response.status_code == 200
    assert response.json()["status"] != "200"
    assert message.lower() in response.json()["msg"].lower()


def test_unsupported_cluster_and_cluster_specific_knobs_are_rejected():
    unsupported = client.post(
        "/api/v2/cr/prepare_job",
        headers=auth_headers(),
        json={"job_type": "opencode", "cluster_id": "slurm"},
    )
    unsupported_knob = client.post(
        "/api/v2/cr/prepare_job",
        headers=auth_headers(),
        json={
            "job_type": "vscode",
            "cluster_id": "htcondor",
            "overrides": {"walltime_seconds": 60},
        },
    )

    assert unsupported.json()["status"] != "200"
    assert "htcondor" in unsupported.json()["msg"]
    assert unsupported_knob.json()["status"] != "200"
    assert "site-managed" in unsupported_knob.json()["msg"]
    assert "use --payload" in unsupported_knob.json()["msg"]


def test_prepare_is_pure_and_does_not_call_scheduler(monkeypatch):
    from fastink.routers.v2 import compute_resources

    monkeypatch.setattr(
        compute_resources,
        "get_scheduler",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("scheduler called")),
    )

    response = client.post(
        "/api/v2/cr/prepare_job",
        headers=auth_headers(),
        json={"job_type": "vscode", "cluster_id": "slurm"},
    )

    assert response.json()["status"] == "200"


def test_resolver_function_is_available_as_a_pure_function():
    module = importlib.import_module("fastink.computing.job_prepare")

    payload = module.prepare_job_request("vscode", "slurm", {})

    assert payload["cluster_id"] == "slurm"
    validate_payload("slurm", payload)


def test_normalize_time_limit_passthrough_and_fallback():
    from fastink.computing.job_defaults import normalize_slurm_time_limit

    assert normalize_slurm_time_limit("1-01:00:00") == "1-01:00:00"
    assert normalize_slurm_time_limit("") == "24:00:00"


def test_interactive_empty_time_falls_back_to_configured_limit(monkeypatch):
    scheduler = object.__new__(HPC_Scheduler)
    scheduler.USERNAME = "alice"
    scheduler.XROOTD_PATH = ""

    def fake_get_config(section, option=None, fallback=None, **_kwargs):
        if section == "computing" and option == "interactive_job_time_limit":
            return "09:00:00"
        if section == "computing" and option == "cluster_scripts":
            return "/cluster-scripts"
        return fallback

    class FakeFile:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        def read(self):
            return b"#!/bin/sh"

    async def upload_file(**_kwargs):
        return None

    monkeypatch.setattr(hpcadapter, "get_config", fake_get_config)
    monkeypatch.setattr(HPC_Scheduler, "_get_interactive_job_types", lambda _self: ["vscode"])
    monkeypatch.setattr(hpcadapter, "open", lambda *_args, **_kwargs: FakeFile(), raising=False)
    monkeypatch.setattr(hpcadapter.common, "upload_file", upload_file)

    command, _, _ = asyncio.run(
        scheduler._gen_slurm_submit_cmd(
            **scheduler_command_kwargs(jobtype="vscode", time="", job_content=None)
        )
    )

    assert "--time=09:00:00" in command


def test_slurm_config_gpu_num_without_gpu_name_is_rejected(monkeypatch):
    patch_prepare_config(
        monkeypatch, ["slurm"], {"vscode": {"slurm": {"gpu_num": 2}}}
    )

    with pytest.raises(JobPreparationError, match="gpu_name"):
        importlib.import_module("fastink.computing.job_prepare").prepare_job_request(
            "vscode", "slurm", {}
        )


@pytest.mark.parametrize(
    "generator_name",
    ["generate_condor_submit", "generate_condor_sync_submit"],
)
def test_condor_generators_skip_os_from_jobtype_config(monkeypatch, tmp_path, generator_name):
    from fastink.computing.apps import registry
    from fastink.computing.tools.common import utils

    scripts_dir = tmp_path / "apps"
    scripts_dir.mkdir()
    (scripts_dir / "shell.sh").write_text("#!/bin/sh\n")
    run_script = tmp_path / "run.sh"
    run_script.write_text("#!/bin/sh\n")

    class FakeApp:
        request_defaults = {}

        def run_script_path(self):
            return run_script

        async def prepare_submit(self, **_kwargs):
            return _kwargs.get("arguments")

    def fake_get_config(section, option=None, fallback=None, **_kwargs):
        if section == "storage" and option == "xrd_host":
            return ""
        if section == "computing" and option == "cluster_scripts":
            return str(tmp_path)
        if section == "jobtype" and option == "vscode":
            return {
                "htc": {
                    "RequestCpus": 1,
                    "RequestMemory": 6000,
                    "os": "AlmaLinux9",
                }
            }
        return fallback

    uploaded = []

    async def upload_file(**kwargs):
        uploaded.append(kwargs["src_data"])

    monkeypatch.setattr(registry, "try_get", lambda _job_type: FakeApp())
    monkeypatch.setattr(utils, "get_config", fake_get_config)
    monkeypatch.setattr(utils, "change_username_to_uid", lambda _username: 0)
    monkeypatch.setattr(utils, "get_extra_job_config", lambda *_args: {})
    monkeypatch.setattr(utils.common, "upload_file", upload_file)

    if generator_name == "generate_condor_submit":
        asyncio.run(
            utils.generate_condor_submit(
                "root", 1, 6000, "vscode", "/jobs/vscode"
            )
        )
    else:
        asyncio.run(
            utils.generate_condor_sync_submit(
                "root", 1, 6000, "/jobs/vscode", "vscode"
            )
        )

    submitfile = next(data for data in uploaded if b"queue\n" in data)
    assert b"os = " not in submitfile
