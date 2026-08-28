import os
import yaml
from typing import Optional, Any, Type

_CONFIG_CACHE = None
_CONFIG_MTIME = None


def _load_config(config_file: Optional[str] = None) -> dict:
    global _CONFIG_CACHE, _CONFIG_MTIME

    if not config_file:
        config_file = os.environ.get("INK_CONFIG_FILE", "src/fastink/misc/config.yml")

    mtime = os.path.getmtime(config_file)

    if _CONFIG_CACHE is None or mtime != _CONFIG_MTIME:
        with open(config_file, "r", encoding="utf-8") as f:
            _CONFIG_CACHE = yaml.safe_load(f)
        _CONFIG_MTIME = mtime

    return _CONFIG_CACHE


def _cast_value(value: Any, target_type: Type) -> Any:
    if value is None:
        return None

    if isinstance(value, target_type):
        return value

    try:
        if target_type is bool:
            if isinstance(value, str):
                v = value.lower()
                if v in {"true", "1", "yes", "y", "on"}:
                    return True
                if v in {"false", "0", "no", "n", "off"}:
                    return False
                raise ValueError
            return bool(value)

        return target_type(value)

    except Exception as e:
        raise ValueError(
            f"Failed to cast value '{value}' ({type(value).__name__}) "
            f"to {target_type.__name__}"
        ) from e


def get_config(
    section: Optional[str] = None,
    option: Optional[str] = None,
    fallback: Any = None,
    *,
    type: Optional[Type] = None,
    config_file: Optional[str] = None,
) -> Any:
    configs = _load_config(config_file)

    value: Any = None
    found = True

    # ---- section ----
    if section not in configs:
        found = False
    else:
        if option is None:
            value = configs[section]
        else:
            if option not in configs[section]:
                found = False
            else:
                value = configs[section][option]

    # ---- fallback / error ----
    if not found:
        if fallback is not None:
            value = fallback
        else:
            if option is None:
                raise ValueError(
                    f"Section '{section}' not found. "
                    f"Available sections: {list(configs.keys())}"
                )
            raise ValueError(
                f"Option '{option}' not found in section '{section}'. "
                f"Available options: {list(configs.get(section, {}).keys())}"
            )

    # ---- type casting (applies to BOTH real value and fallback) ----
    if type is not None:
        value = _cast_value(value, type)

    return value
