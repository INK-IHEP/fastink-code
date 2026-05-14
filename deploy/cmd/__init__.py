"""Command registry for fastinkctl CLI."""

from __future__ import annotations

_registry: dict[str, str] = {}


def register(name: str, module_path: str, aliases: list[str] | None = None) -> None:
    """Register a command with its module path.

    Args:
        name: Primary command name.
        module_path: Dot-separated module path, e.g. "cmd.deploy".
        aliases: Optional aliases (e.g. "install" for "deploy").
    """
    _registry[name] = module_path
    for alias in aliases or []:
        _registry[alias] = module_path


def get_module_path(command: str) -> str | None:
    """Get module path for a command, or None if unknown."""
    return _registry.get(command)


def known_commands() -> list[str]:
    """Return list of primary command names (excluding aliases)."""
    return [name for name in _registry if name not in ("install", "uninstall")]


# ---- Register built-in commands ----
register("deploy", "cmd.deploy", aliases=["install"])
register("destroy", "cmd.destroy", aliases=["uninstall"])
register("down", "cmd.down")
register("up", "cmd.up")
register("status", "cmd.status")
