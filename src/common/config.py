import os
import yaml
from typing import Optional, Union, Any


def _load_config(config_file: Optional[str] = None) -> dict:
    if not config_file:
        config_file = os.environ.get("INK_CONFIG_FILE", "src/misc/config.yml")
    with open(config_file, "r", encoding="utf-8") as f:
        config_data = yaml.safe_load(f)
    return config_data


def get_config(
    section: Optional[str] = None,
    option: Optional[str] = None,
    fallback: Any = None,
    config_file: Optional[str] = None,
) -> Union[dict, Any]:
    configs = _load_config(config_file)

    # Validate section existence
    if section not in configs:
        if fallback:
            return fallback
        raise ValueError(
            f"Section '{section}' not found in config file. Available sections: {list(configs.keys())}"
        )

    # If option is None, return the entire section
    if option is None:
        return configs[section]

    # If option is provided but not found in the section
    if option not in configs[section]:
        if fallback:
            return fallback
        raise ValueError(
            f"Option '{option}' not found in section '{section}'. Available options: {list(configs[section].keys())}"
        )

    return configs[section][option]
