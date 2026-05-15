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

from sqlalchemy.exc import NoResultFound

from fastink.auth.common import get_user
from fastink.auth.platform_api import get_user_api_key
from fastink.common.config import get_config
from fastink.common.logger import logger
from fastink.common.utils import query_pwd_group
from fastink.computing.tools.common.utils import (
    change_username_to_uid,
    get_user_exp_group,
)
from fastink.storage import common as storage_common
from fastink.storage.utils import PathType
from fastink.service.openclaw_schema import OpenClawSyncRequest


DEFAULT_PROVIDER_KEY = "custom"
DEFAULT_ALLOWED_ORIGINS = [
    "https://ink-dev.ihep.ac.cn",
    "https://ink.ihep.ac.cn",
    "https://fastink.ihep.ac.cn",
    "https://fastink-test.ihep.ac.cn",
]
DEFAULT_OPENCLAW_TEMPLATES = {
    "hepai/deepseek": {
        "base_url": "https://aiapi.ihep.ac.cn/apiv2",
        "api_key": "",
        "api_name": "openai-completions",
        "model_id": "hepai/deepseek-v4-flash",
    },
    "Deepseek": {
        "base_url": "https://api.deepseek.com",
        "api_key": "",
        "api_name": "openai-completions",
        "model_id": "deepseek-v4-flash",
    },
    "Qwen coding plan": {
        "base_url": "https://coding.dashscope.aliyuncs.com/v1",
        "api_key": "",
        "api_name": "openai-completions",
        "model_id": "qwen3.6-plus",
    },
    "Z.ai (GLM) Coding Plan": {
        "base_url": "https://open.bigmodel.cn/api/anthropic",
        "api_key": "",
        "api_name": "anthropic-messages",
        "model_id": "glm-5.1",
    },
    "Minimax token plan": {
        "base_url": "https://api.minimaxi.com/anthropic",
        "api_key": "",
        "api_name": "anthropic-messages",
        "model_id": "MiniMax-M2.7",
    },
    "Xiaomi mimo token plan": {
        "base_url": "https://token-plan-cn.xiaomimimo.com/v1",
        "api_key": "",
        "api_name": "openai-completions",
        "model_id": "mimo-v2-pro",
    },
    "Custom": {
        "base_url": "",
        "api_key": "",
        "api_name": "",
        "model_id": "",
    },
}
DEFAULT_OPENCLAW_API_NAME_LIST = [
    "openai-completions",
    "openai-responses",
    "openai-codex-responses",
    "anthropic-messages",
    "google-generative-ai",
    "github-copilot",
    "bedrock-converse-stream",
    "ollama",
    "azure-openai-responses",
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
    "contextWindow": 64000,
    "maxTokens": 8192,
}
LOCAL_MODEL_CONTEXT_WINDOW = 512 * 1024
REMOTE_MODEL_CONTEXT_WINDOW = 128000


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


def _get_storage_mgm() -> str:
    return get_config("storage", "xrd_host")


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


def _normalize_base_url(value: str) -> str:
    return str(value or "").rstrip("/")


def _is_local_model(base_url: str, model_id: str) -> bool:
    return (
        _normalize_base_url(base_url) == _normalize_base_url(DEFAULT_OPENCLAW_TEMPLATES["hepai/deepseek"]["base_url"])
        and str(model_id or "").startswith("hepai/")
    )


def _get_model_context_window(base_url: str, model_id: str) -> int:
    if _is_local_model(base_url=base_url, model_id=model_id):
        return LOCAL_MODEL_CONTEXT_WINDOW
    return REMOTE_MODEL_CONTEXT_WINDOW


def _get_user_email(username: str) -> str | None:
    try:
        user = get_user(username=username)
    except NoResultFound:
        logger.info("No user record found when resolving OpenClaw API key email for %s", username)
        return None
    except Exception as exc:
        logger.warning("Failed to resolve OpenClaw API key email for %s: %s", username, exc)
        return None

    email = (user or {}).get("email")
    if not email:
        logger.info("No email found when resolving OpenClaw API key for %s", username)
        return None
    return str(email)


async def _get_user_model_api_key(username: str) -> str:
    email = _get_user_email(username)
    if not email:
        return ""

    try:
        api_key = await get_user_api_key(email)
    except Exception as exc:
        logger.warning("Failed to fetch OpenClaw API key for %s: %s", username, exc)
        return ""
    return str(api_key or "")


async def _fill_local_model_api_key(username: str, template_payload: dict) -> dict:
    templates = template_payload.get("templates")
    if not isinstance(templates, dict):
        return template_payload

    needs_api_key = any(
        isinstance(entry, dict)
        and not entry.get("api_key")
        and _is_local_model(
            base_url=entry.get("base_url", ""),
            model_id=entry.get("model_id", ""),
        )
        for entry in templates.values()
    )
    if not needs_api_key:
        return template_payload

    api_key = await _get_user_model_api_key(username)
    if not api_key:
        return template_payload

    for entry in templates.values():
        if not isinstance(entry, dict) or entry.get("api_key"):
            continue
        if _is_local_model(
            base_url=entry.get("base_url", ""),
            model_id=entry.get("model_id", ""),
        ):
            entry["api_key"] = api_key
    return template_payload


def _resolve_user_experiment_group(username: str) -> str:
    uid = change_username_to_uid(username)
    experiment_group, raw_group = get_user_exp_group(uid)
    group_dir = (experiment_group or query_pwd_group(username) or raw_group or "").lower()
    if not group_dir:
        raise ValueError(f"Failed to resolve scratchfs experiment group for {username}")
    return group_dir


async def _path_status_as_user(username: str, path: Path) -> tuple[bool, PathType]:
    return await storage_common.path_exist(
        name=str(path),
        username=username,
        mgm=_get_storage_mgm(),
    )


async def _path_exists_as_user(username: str, path: Path) -> bool:
    is_exist, _ = await _path_status_as_user(username, path)
    return is_exist


async def _path_is_file_as_user(username: str, path: Path) -> bool:
    is_exist, path_type = await _path_status_as_user(username, path)
    return is_exist and path_type == PathType.FILE


async def _read_text_as_user(username: str, path: Path) -> str:
    return await storage_common.cat_file(
        fname=str(path),
        username=username,
        mgm=_get_storage_mgm(),
    )


async def _write_text_as_user(username: str, target_path: Path, payload: str) -> None:
    await storage_common.upload_file(
        src_data=payload.encode("utf-8"),
        dst=str(target_path),
        username=username,
        mgm=_get_storage_mgm(),
    )


async def _write_json_as_user(username: str, target_path: Path, payload: dict) -> None:
    await _write_text_as_user(
        username,
        target_path,
        json.dumps(payload, ensure_ascii=False, indent=2) + "\n",
    )


async def _read_json_as_user(username: str, path: Path) -> dict:
    return json.loads(await _read_text_as_user(username, path))


async def _build_openclaw_context(username: str) -> dict:
    group_dir = _resolve_user_experiment_group(username)
    target_user_root = _get_openclaw_user_root(username=username, group_dir=group_dir)
    target_dir = target_user_root / _get_target_relpath()
    target_openclaw_json = target_dir / "openclaw.json"
    target_dir_exists = await _path_exists_as_user(username, target_dir)
    config_exists = await _path_is_file_as_user(username, target_openclaw_json)
    config = None
    config_error = None

    if config_exists:
        try:
            config = await _read_json_as_user(username, target_openclaw_json)
        except Exception as exc:
            config_error = str(exc)

    return {
        "group_dir": group_dir,
        "target_user_root": target_user_root,
        "target_dir": target_dir,
        "target_openclaw_json": target_openclaw_json,
        "target_dir_exists": target_dir_exists,
        "config_exists": config_exists,
        "config_valid": config is not None,
        "config_error": config_error,
        "config": config,
    }


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


def _get_primary_model_context(target_config: dict) -> tuple[str, dict | None, dict | None]:
    provider_key, primary_model_id = _parse_primary_ref(target_config)
    providers_section = target_config.get("models", {}).get("providers", {})
    if not isinstance(providers_section, dict):
        return provider_key, None, None
    provider_config = providers_section.get(provider_key)
    if not isinstance(provider_config, dict):
        return provider_key, None, None
    models = provider_config.get("models")
    if not isinstance(models, list):
        return provider_key, provider_config, None
    if primary_model_id:
        for model in models:
            if isinstance(model, dict) and model.get("id") == primary_model_id:
                return provider_key, provider_config, model
    return provider_key, provider_config, None


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
    merged["name"] = payload.model_name if payload.model_name is not None else payload.model_id

    model_patch = _build_model_patch(payload)
    for key, value in model_patch.items():
        if key == "cost":
            cost = merged.setdefault("cost", deepcopy(DEFAULT_MODEL["cost"]))
            cost.update(value)
        else:
            merged[key] = value

    merged["contextWindow"] = _get_model_context_window(
        base_url=payload.base_url,
        model_id=payload.model_id,
    )
    if not merged.get("name"):
        merged["name"] = payload.model_id
    return merged


def _render_openclaw_templates(
    selected_key: str | None = None,
    selected_values: dict | None = None,
) -> dict:
    selected_key = selected_key if selected_key in DEFAULT_OPENCLAW_TEMPLATES else None
    ordered_keys = []
    if selected_key:
        ordered_keys.append(selected_key)
    ordered_keys.extend(
        key for key in DEFAULT_OPENCLAW_TEMPLATES.keys() if key != selected_key
    )

    rendered = {}
    for key in ordered_keys:
        entry = deepcopy(DEFAULT_OPENCLAW_TEMPLATES[key])
        if selected_key == key and selected_values:
            entry.update(selected_values)
        rendered[key] = entry
    return {
        "templates": rendered,
        "api_name_list": deepcopy(DEFAULT_OPENCLAW_API_NAME_LIST),
    }


def _template_key_for_model(base_url: str, model_id: str) -> str:
    normalized_base_url = _normalize_base_url(base_url)
    normalized_model_id = str(model_id or "")
    for key, template in DEFAULT_OPENCLAW_TEMPLATES.items():
        if key == "custom":
            continue
        if (
            _normalize_base_url(template["base_url"]) == normalized_base_url
            and template["model_id"] == normalized_model_id
        ):
            return key
    return "custom"


async def get_openclaw_template(username: str) -> dict:
    context = await _build_openclaw_context(username)
    if not context["target_dir_exists"] or not context["config_exists"] or not context["config_valid"]:
        return await _fill_local_model_api_key(username, _render_openclaw_templates())

    target_config = context["config"]
    provider_key, provider_config, primary_model = _get_primary_model_context(target_config)
    if provider_config is None or primary_model is None:
        return await _fill_local_model_api_key(username, _render_openclaw_templates())

    selected_values = {
        "base_url": provider_config.get("baseUrl", ""),
        "api_key": provider_config.get("apiKey", ""),
        "api_name": provider_config.get("api", ""),
        "model_id": primary_model.get("id", ""),
    }
    selected_key = _template_key_for_model(
        base_url=selected_values["base_url"],
        model_id=selected_values["model_id"],
    )
    if selected_key != "custom":
        return await _fill_local_model_api_key(
            username,
            _render_openclaw_templates(
                selected_key=selected_key,
                selected_values=selected_values,
            ),
        )

    selected_values["api_key"] = provider_config.get("apiKey", "")
    return await _fill_local_model_api_key(
        username,
        _render_openclaw_templates(selected_key="custom", selected_values=selected_values),
    )


def _iter_provider_models(target_config: dict) -> list[tuple[str, dict, dict]]:
    providers_section = target_config.get("models", {}).get("providers", {})
    if not isinstance(providers_section, dict):
        return []

    items = []
    for provider_key, provider_config in providers_section.items():
        if not isinstance(provider_config, dict):
            continue
        models = provider_config.get("models")
        if not isinstance(models, list):
            continue
        for model in models:
            if isinstance(model, dict):
                items.append((provider_key, provider_config, model))
    return items


def _validate_experiment_data_models(target_config: dict) -> None:
    invalid_models = []
    for provider_key, provider_config, model in _iter_provider_models(target_config):
        base_url = provider_config.get("baseUrl", "")
        model_id = model.get("id", "")
        if not _is_local_model(base_url, model_id):
            invalid_models.append(f"{provider_key}/{model_id}")

    if invalid_models:
        raise ValueError(
            "add_experiment_data=true requires all OpenClaw models to use local IHEP models only. "
            f"Found non-local models: {', '.join(invalid_models)}"
        )


def _ensure_default_allowed_origins(control_ui: dict) -> list[str]:
    existing_allowed_origins = control_ui.get("allowedOrigins", [])
    if isinstance(existing_allowed_origins, str):
        existing_allowed_origins = [existing_allowed_origins]
    elif not isinstance(existing_allowed_origins, list):
        existing_allowed_origins = []

    allowed_origins = []
    for origin in existing_allowed_origins + DEFAULT_ALLOWED_ORIGINS:
        if origin and origin not in allowed_origins:
            allowed_origins.append(origin)
    control_ui["allowedOrigins"] = allowed_origins
    return allowed_origins


def _build_updated_openclaw_config(
    username: str,
    target_config: dict,
    target_user_root: Path,
    payload: OpenClawSyncRequest,
    initialize_target: bool,
) -> tuple[dict, dict]:
    updated_config = deepcopy(target_config)
    models_section = updated_config.setdefault("models", {})
    providers_section = models_section.setdefault("providers", {})
    if not isinstance(providers_section, dict):
        providers_section = {}
        models_section["providers"] = providers_section

    provider_key, _ = _parse_primary_ref(updated_config)
    provider_config = providers_section.setdefault(provider_key, {})
    existing_models = provider_config.get("models")
    if not isinstance(existing_models, list):
        existing_models = []
        provider_config["models"] = existing_models

    provider_key, existing_model, previous_primary_model_id = _select_target_model(
        existing_models=existing_models,
        target_config=updated_config,
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
    gateway = updated_config.setdefault("gateway", {})
    if initialize_target:
        gateway["port"] = _get_initial_gateway_port(username)
    control_ui = gateway.setdefault("controlUi", {})
    _ensure_default_allowed_origins(control_ui)
    control_ui["dangerouslyDisableDeviceAuth"] = True

    auth = updated_config.setdefault("gateway", {}).setdefault("auth", {})
    auth["mode"] = "token"
    auth.setdefault("token", "")

    agents_defaults = updated_config.setdefault("agents", {}).setdefault("defaults", {})
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

    return updated_config, {
        "workspace": workspace_path,
        "primary_model": primary_key,
    }


async def sync_openclaw_models(username: str, payload: OpenClawSyncRequest) -> dict:
    template_dir = _get_template_dir()
    if not template_dir.is_dir():
        raise FileNotFoundError(f"OpenClaw template directory not found: {template_dir}")

    context = await _build_openclaw_context(username)
    group_dir = context["group_dir"]
    target_user_root = context["target_user_root"]
    if not target_user_root.is_dir():
        raise FileNotFoundError(
            f"Target OpenClaw user directory does not exist: {target_user_root}"
        )

    target_dir = context["target_dir"]
    created = False
    if not context["target_dir_exists"] or not context["config_exists"]:
        _copy_template_dir_to_target(template_dir, target_dir, username)
        created = True
        context = await _build_openclaw_context(username)
    elif not context["config_valid"]:
        raise ValueError(
            f"Target OpenClaw config is invalid JSON: {context['target_openclaw_json']}"
        )

    if not context["config_valid"]:
        raise FileNotFoundError(
            f"Target OpenClaw config not found: {context['target_openclaw_json']}"
        )

    updated_config, update_result = _build_updated_openclaw_config(
        username=username,
        target_config=context["config"],
        target_user_root=target_user_root,
        payload=payload,
        initialize_target=created,
    )
    if payload.add_experiment_data:
        _validate_experiment_data_models(updated_config)

    await _write_json_as_user(username, context["target_openclaw_json"], updated_config)
    await _ensure_directory_mode(username=username, target_dir=target_dir, expected_mode=0o700)

    return {
        "username": username,
        "group_dir": group_dir,
        "created": created,
        "provider_key": DEFAULT_PROVIDER_KEY,
        "workspace": update_result["workspace"],
        "primary_model": update_result["primary_model"],
    }
