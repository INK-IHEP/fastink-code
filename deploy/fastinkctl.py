#!/usr/bin/env python3
"""FastINK CLI — manage FastINK deployments.

Usage:
  fastinkctl deploy [options]     Deploy FastINK (default command)
                                 Alias: install
  fastinkctl destroy [options]    Tear down and clean up deployment
                                 Alias: uninstall
  fastinkctl down [options]       Stop containers, keep data and files
  fastinkctl up [options]         Start containers (restart after down)
  fastinkctl status               Show deployment status
  fastinkctl help                 Show this help message

Run "fastinkctl <command> --help" for command-specific options.
"""

import importlib
import sys
from pathlib import Path

_HERE = Path(__file__).resolve().parent  # deploy/
if str(_HERE) not in sys.path:
    sys.path.insert(0, str(_HERE))

from cmd import get_module_path

USAGE = __doc__


def main() -> None:
    args = sys.argv[1:]

    if not args:
        command = "deploy"
    elif args[0] in ("-h", "--help", "help"):
        print(USAGE)
        return
    else:
        command = args[0]

    module_path = get_module_path(command)
    if module_path is None:
        print(f"Unknown command: {command}\n", file=sys.stderr)
        print(USAGE, file=sys.stderr)
        sys.exit(1)

    # Strip the command name so subcommand parsers don't see it
    sys.argv = [sys.argv[0]] + args[1:]
    module = importlib.import_module(module_path)
    module.main()


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n")
        from lib import cli_ui

        cli_ui.warning("Aborted.")
        sys.exit(1)
