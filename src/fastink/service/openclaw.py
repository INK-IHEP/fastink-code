#! /usr/bin/python3

import json
import os
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
from fastink.service.openclaw_schema import OpenClawModelRequest, OpenClawSyncRequest


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


def _get_template_dir() -> Path:
    return Path(
        get_config(
            "service",
            "openclaw_template_dir",
            fallback=str(Path(__file__).resolve().parent / "templates" / "openclaw"),
        )
    )


def _get_scratchfs_root() -> Path:
    return Path(get_config("service", "scratchfs_root", fallback="/scratchfs"))


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


def _normalize_model(model: OpenClawModelRequest | None) -> dict:
    normalized = deepcopy(DEFAULT_MODEL)
    if model is not None:
        raw_model = model.dict(exclude_none=True)
        for key, value in raw_model.items():
            if key == "cost" and isinstance(value, dict):
                normalized["cost"].update(value)
            else:
                normalized[key] = value

    if not normalized.get("id"):
        normalized["id"] = "custom"
    if not normalized.get("name"):
        normalized["name"] = "custom"
    return normalized


def _build_provider_config(payload: OpenClawSyncRequest) -> dict:
    raw_models = payload.models or [OpenClawModelRequest()]
    return {
        "baseUrl": payload.baseUrl,
        "apiKey": payload.apiKey,
        "api": payload.api,
        "models": [_normalize_model(model) for model in raw_models],
    }


def _update_target_openclaw_config(
    username: str,
    target_openclaw_json: Path,
    target_user_root: Path,
    provider_config: dict,
    initialize_target: bool,
) -> dict:
    if not target_openclaw_json.exists():
        raise FileNotFoundError(f"Target OpenClaw config not found: {target_openclaw_json}")

    target_config = json.loads(_read_text_as_user(username, target_openclaw_json))
    models_section = target_config.setdefault("models", {})
    providers_section = models_section.setdefault("providers", {})
    if not isinstance(providers_section, dict):
        providers_section = {}
        models_section["providers"] = providers_section

    providers_section[DEFAULT_PROVIDER_KEY] = provider_config
    if not models_section.get("mode"):
        models_section["mode"] = "merge"

    workspace_path = str(target_user_root / _get_target_relpath() / "workspace")
    gateway = target_config.setdefault("gateway", {})
    control_ui = gateway.setdefault("controlUi", {})
    existing_allowed_origins = control_ui.get("allowedOrigins", [])
    allowed_origins = []
    for origin in existing_allowed_origins + DEFAULT_ALLOWED_ORIGINS:
        if origin and origin not in allowed_origins:
            allowed_origins.append(origin)
    control_ui["allowedOrigins"] = allowed_origins

    agents_defaults = target_config.setdefault("agents", {}).setdefault("defaults", {})
    primary_model = provider_config["models"][0]["id"]
    agents_defaults["model"] = {"primary": f"{DEFAULT_PROVIDER_KEY}/{primary_model}"}
    agents_defaults["models"] = {
        f"{DEFAULT_PROVIDER_KEY}/{model['id']}": {}
        for model in provider_config["models"]
    }
    if initialize_target or not agents_defaults.get("workspace"):
        agents_defaults["workspace"] = workspace_path

    _write_json_as_user(username, target_openclaw_json, target_config)
    return {
        "workspace": workspace_path,
        "primary_model": f"{DEFAULT_PROVIDER_KEY}/{primary_model}",
    }


def sync_openclaw_models(username: str, payload: OpenClawSyncRequest) -> dict:
    template_dir = _get_template_dir()
    if not template_dir.is_dir():
        raise FileNotFoundError(f"OpenClaw template directory not found: {template_dir}")

    group_dir = _resolve_user_experiment_group(username)
    target_user_root = _get_scratchfs_root() / group_dir / username
    if not target_user_root.is_dir():
        raise FileNotFoundError(
            f"Target scratchfs user directory does not exist: {target_user_root}"
        )

    target_dir = target_user_root / _get_target_relpath()
    created = False
    if not target_dir.exists():
        _copy_template_dir_to_target(template_dir, target_dir, username)
        created = True

    target_openclaw_json = target_dir / "openclaw.json"
    provider_config = _build_provider_config(payload)
    update_result = _update_target_openclaw_config(
        username=username,
        target_openclaw_json=target_openclaw_json,
        target_user_root=target_user_root,
        provider_config=provider_config,
        initialize_target=created,
    )

    return {
        "username": username,
        "group_dir": group_dir,
        "created": created,
        "provider_key": DEFAULT_PROVIDER_KEY,
        "workspace": update_result["workspace"],
        "primary_model": update_result["primary_model"],
    }
