"""Command registry for fastinkctl CLI."""

from __future__ import annotations

_COMMANDS = {
    "deploy": "cmd.deploy",
    "install": "cmd.deploy",
    "destroy": "cmd.destroy",
    "uninstall": "cmd.destroy",
    "down": "cmd.down",
    "up": "cmd.up",
    "status": "cmd.status",
}


def get_module_path(command: str) -> str | None:
    """Get module path for a command, or None if unknown."""
    return _COMMANDS.get(command)
