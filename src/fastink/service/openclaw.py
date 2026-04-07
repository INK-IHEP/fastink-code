#! /usr/bin/python3

import json
import os
import random
import socket
import subprocess
import tempfile
from copy import deepcopy
from pathlib import Path
from shlex import quote

from fastink.common.config import get_config
from fastink.common.utils import query_pwd_group
from fastink.computing.tools.common.utils import (
    change_username_to_uid,
    get_user_exp_group,
)
from fastink.storage import common as storage_common
from fastink.service.openclaw_schema import OpenClawSyncRequest


DEFAULT_PROVIDER_KEY = "custom"
DEFAULT_ALLOWED_ORIGINS = [
    "https://ink-dev.ihep.ac.cn",
    "https://fastink-test.ihep.ac.cn",
]
DEFAULT_MODEL = {
    "id": "custom",
    "name": "custom",
    "reasoning": False,
    "input": ["text"],
    "cost": {
        "input": 0,
        "output": 0,
        "cacheRead": 0,
        "cacheWrite": 0,
    },
    "contextWindow": 16000,
    "maxTokens": 4096,
}


def _is_local_port_available(port: int) -> bool:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            sock.bind(("127.0.0.1", port))
        except OSError:
            return False
    return True


def _get_initial_gateway_port(username: str) -> int:
    uid = change_username_to_uid(username)
    if 10000 <= uid <= 65535:
        return uid

    while True:
        port = random.randint(49152, 65535)
        if _is_local_port_available(port):
            return port


def _get_template_dir() -> Path:
    return Path(
        get_config(
            "service",
            "openclaw_template_dir",
            fallback=str(Path(__file__).resolve().parent / "templates" / "openclaw"),
        )
    )


def _get_openclaw_user_root(username: str, group_dir: str) -> Path:
    path_template = get_config(
        "service",
        "openclaw_user_root",
        fallback="/scratchfs/{experiment_group_lower}/{username}",
    )
    return Path(
        path_template.format(
            username=username,
            experiment_group_lower=group_dir,
            group_dir=group_dir,
        )
    )


def _get_target_relpath() -> Path:
    return Path(
        get_config(
            "service",
            "openclaw_models_relpath",
            fallback=".openclaw",
        )
    )


def _run_as_user(username: str, command: str) -> str:
    result = subprocess.run(
        ["su", "-s", "/bin/bash", username, "-c", command],
        text=True,
        capture_output=True,
        check=False,
    )
    if result.returncode != 0:
        raise RuntimeError(result.stderr.strip() or result.stdout.strip() or "unknown error")
    return result.stdout


def _path_exists_as_user(username: str, path: Path) -> bool:
    result = subprocess.run(
        ["su", "-s", "/bin/bash", username, "-c", f"test -e {quote(str(path))}"],
        text=True,
        capture_output=True,
        check=False,
    )
    return result.returncode == 0


def _path_is_file_as_user(username: str, path: Path) -> bool:
    result = subprocess.run(
        ["su", "-s", "/bin/bash", username, "-c", f"test -f {quote(str(path))}"],
        text=True,
        capture_output=True,
        check=False,
    )
    return result.returncode == 0


def _resolve_user_experiment_group(username: str) -> str:
    uid = change_username_to_uid(username)
    experiment_group, raw_group = get_user_exp_group(uid)
    group_dir = (experiment_group or query_pwd_group(username) or raw_group or "").lower()
    if not group_dir:
        raise ValueError(f"Failed to resolve scratchfs experiment group for {username}")
    return group_dir


def _read_text_as_user(username: str, path: Path) -> str:
    return _run_as_user(username, f"cat {quote(str(path))}")


def _write_text_as_user(username: str, target_path: Path, payload: str) -> None:
    target_dir = target_path.parent

    with tempfile.NamedTemporaryFile(
        mode="w", encoding="utf-8", delete=False, dir="/tmp"
    ) as temp_file:
        temp_file.write(payload)
        temp_path = Path(temp_file.name)

    os.chmod(temp_path, 0o644)
    try:
        _run_as_user(
            username,
            f"mkdir -p {quote(str(target_dir))} && cat {quote(str(temp_path))} > {quote(str(target_path))}",
        )
    finally:
        temp_path.unlink(missing_ok=True)


def _write_json_as_user(username: str, target_path: Path, payload: dict) -> None:
    _write_text_as_user(
        username,
        target_path,
        json.dumps(payload, ensure_ascii=False, indent=2) + "\n",
    )


async def _ensure_directory_mode(username: str, target_dir: Path, expected_mode: int) -> None:
    stat_output = _run_as_user(
        username,
        f"stat -c %a {quote(str(target_dir))}",
    ).strip()
    if stat_output != f"{expected_mode:o}":
        await storage_common.chmod(
            fname=str(target_dir),
            username=username,
            mode=f"{expected_mode:o}",
            mgm=get_config("storage", "xrd_host"),
        )


def _copy_template_dir_to_target(
    template_dir: Path,
    target_dir: Path,
    username: str,
) -> None:
    with tempfile.NamedTemporaryFile(delete=False, dir="/tmp", suffix=".tar") as temp_file:
        temp_path = Path(temp_file.name)

    os.chmod(temp_path, 0o666)
    try:
        result = subprocess.run(
            ["tar", "-C", str(template_dir), "-cf", str(temp_path), "."],
            text=True,
            capture_output=True,
            check=False,
        )
        if result.returncode != 0:
            raise RuntimeError(result.stderr.strip() or "Failed to archive OpenClaw template")

        _run_as_user(
            username,
            f"mkdir -p {quote(str(target_dir))} && tar -C {quote(str(target_dir))} -xf {quote(str(temp_path))}",
        )
    finally:
        temp_path.unlink(missing_ok=True)


def _parse_primary_ref(target_config: dict) -> tuple[str, str | None]:
    primary = (
        target_config.get("agents", {})
        .get("defaults", {})
        .get("model", {})
        .get("primary", "")
    )
    if isinstance(primary, str) and "/" in primary:
        provider_key, model_id = primary.split("/", 1)
        return provider_key or DEFAULT_PROVIDER_KEY, model_id or None
    return DEFAULT_PROVIDER_KEY, None


def _find_model_by_id(models: list[dict], model_id: str) -> dict | None:
    for model in models:
        if model.get("id") == model_id:
            return model
    return None


def _select_target_model(
    existing_models: list[dict],
    target_config: dict,
    payload: OpenClawSyncRequest,
    initialize_target: bool,
) -> tuple[str, dict | None, str | None]:
    provider_key, primary_model_id = _parse_primary_ref(target_config)
    target_model = None

    if primary_model_id:
        target_model = _find_model_by_id(existing_models, primary_model_id)

    if target_model is None and initialize_target and len(existing_models) == 1:
        target_model = existing_models[0]

    if target_model is None:
        target_model = _find_model_by_id(existing_models, payload.model_id)

    return provider_key, target_model, primary_model_id


def _build_model_patch(payload: OpenClawSyncRequest) -> dict:
    patch: dict = {
        "id": payload.model_id,
    }
    if payload.model_name is not None:
        patch["name"] = payload.model_name
    if payload.model_reasoning is not None:
        patch["reasoning"] = payload.model_reasoning
    if payload.model_input is not None:
        patch["input"] = payload.model_input
    if payload.model_context_window is not None:
        patch["contextWindow"] = payload.model_context_window
    if payload.model_max_tokens is not None:
        patch["maxTokens"] = payload.model_max_tokens

    cost_patch = {}
    if payload.model_cost_input is not None:
        cost_patch["input"] = payload.model_cost_input
    if payload.model_cost_output is not None:
        cost_patch["output"] = payload.model_cost_output
    if payload.model_cost_cache_read is not None:
        cost_patch["cacheRead"] = payload.model_cost_cache_read
    if payload.model_cost_cache_write is not None:
        cost_patch["cacheWrite"] = payload.model_cost_cache_write
    if cost_patch:
        patch["cost"] = cost_patch

    return patch


def _merge_model(existing_model: dict | None, payload: OpenClawSyncRequest) -> dict:
    merged = deepcopy(existing_model) if existing_model is not None else deepcopy(DEFAULT_MODEL)
    merged["id"] = payload.model_id
    if not merged.get("name"):
        merged["name"] = payload.model_id

    model_patch = _build_model_patch(payload)
    for key, value in model_patch.items():
        if key == "cost":
            cost = merged.setdefault("cost", deepcopy(DEFAULT_MODEL["cost"]))
            cost.update(value)
        else:
            merged[key] = value

    if not merged.get("name"):
        merged["name"] = payload.model_id
    return merged


def _update_target_openclaw_config(
    username: str,
    target_openclaw_json: Path,
    target_user_root: Path,
    payload: OpenClawSyncRequest,
    initialize_target: bool,
) -> dict:
    if not _path_is_file_as_user(username, target_openclaw_json):
        raise FileNotFoundError(f"Target OpenClaw config not found: {target_openclaw_json}")

    target_config = json.loads(_read_text_as_user(username, target_openclaw_json))
    models_section = target_config.setdefault("models", {})
    providers_section = models_section.setdefault("providers", {})
    if not isinstance(providers_section, dict):
        providers_section = {}
        models_section["providers"] = providers_section

    provider_key, _ = _parse_primary_ref(target_config)
    provider_config = providers_section.setdefault(provider_key, {})
    existing_models = provider_config.get("models")
    if not isinstance(existing_models, list):
        existing_models = []
        provider_config["models"] = existing_models

    provider_key, existing_model, previous_primary_model_id = _select_target_model(
        existing_models=existing_models,
        target_config=target_config,
        payload=payload,
        initialize_target=initialize_target,
    )
    provider_config["baseUrl"] = payload.base_url
    provider_config["apiKey"] = payload.api_key
    provider_config["api"] = payload.api_name
    merged_model = _merge_model(existing_model, payload)
    if existing_model is None:
        existing_models.append(merged_model)
    else:
        existing_model.clear()
        existing_model.update(merged_model)
    if initialize_target:
        provider_config["models"] = [merged_model]
        existing_models = provider_config["models"]

    if not models_section.get("mode"):
        models_section["mode"] = "merge"

    workspace_path = str(target_user_root / _get_target_relpath() / "workspace")
    gateway = target_config.setdefault("gateway", {})
    if initialize_target:
        gateway["port"] = _get_initial_gateway_port(username)
    control_ui = gateway.setdefault("controlUi", {})
    existing_allowed_origins = control_ui.get("allowedOrigins", [])
    allowed_origins = []
    for origin in existing_allowed_origins + DEFAULT_ALLOWED_ORIGINS:
        if origin and origin not in allowed_origins:
            allowed_origins.append(origin)
    control_ui["allowedOrigins"] = allowed_origins
    control_ui["dangerouslyDisableDeviceAuth"] = True

    auth = gateway.setdefault("auth", {})
    auth["mode"] = "token"
    auth.setdefault("token", "")

    agents_defaults = target_config.setdefault("agents", {}).setdefault("defaults", {})
    primary_model = merged_model["id"]
    primary_key = f"{provider_key}/{primary_model}"
    agents_defaults["model"] = {"primary": primary_key}
    agents_models = agents_defaults.setdefault("models", {})
    if not isinstance(agents_models, dict):
        agents_models = {}
        agents_defaults["models"] = agents_models
    previous_primary_key = None
    if previous_primary_model_id:
        previous_primary_key = f"{provider_key}/{previous_primary_model_id}"
    if previous_primary_key and previous_primary_key != primary_key:
        agents_models[primary_key] = agents_models.pop(previous_primary_key, {})
    else:
        agents_models.setdefault(primary_key, {})
    if initialize_target:
        agents_defaults["models"] = {primary_key: agents_models.get(primary_key, {})}
    if initialize_target or not agents_defaults.get("workspace"):
        agents_defaults["workspace"] = workspace_path

    _write_json_as_user(username, target_openclaw_json, target_config)
    return {
        "workspace": workspace_path,
        "primary_model": primary_key,
    }


def has_openclaw_config(username: str) -> bool:
    group_dir = _resolve_user_experiment_group(username)
    target_user_root = _get_openclaw_user_root(username=username, group_dir=group_dir)
    target_config_path = target_user_root / _get_target_relpath() / "openclaw.json"
    return _path_is_file_as_user(username, target_config_path)


async def sync_openclaw_models(username: str, payload: OpenClawSyncRequest) -> dict:
    template_dir = _get_template_dir()
    if not template_dir.is_dir():
        raise FileNotFoundError(f"OpenClaw template directory not found: {template_dir}")

    group_dir = _resolve_user_experiment_group(username)
    target_user_root = _get_openclaw_user_root(username=username, group_dir=group_dir)
    if not target_user_root.is_dir():
        raise FileNotFoundError(
            f"Target OpenClaw user directory does not exist: {target_user_root}"
        )

    target_dir = target_user_root / _get_target_relpath()
    created = False
    if not _path_exists_as_user(username, target_dir):
        _copy_template_dir_to_target(template_dir, target_dir, username)
        created = True

    target_openclaw_json = target_dir / "openclaw.json"
    update_result = _update_target_openclaw_config(
        username=username,
        target_openclaw_json=target_openclaw_json,
        target_user_root=target_user_root,
        payload=payload,
        initialize_target=created,
    )
    await _ensure_directory_mode(username=username, target_dir=target_dir, expected_mode=0o700)

    return {
        "username": username,
        "group_dir": group_dir,
        "created": created,
        "provider_key": DEFAULT_PROVIDER_KEY,
        "workspace": update_result["workspace"],
        "primary_model": update_result["primary_model"],
    }
