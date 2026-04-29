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

import sys
from pathlib import Path

# Path setup: add deploy/ root to sys.path so cmd/* and lib/* are importable.
_HERE = Path(__file__).resolve().parent  # deploy/
if str(_HERE) not in sys.path:
    sys.path.insert(0, str(_HERE))


USAGE = __doc__


def main() -> None:
    args = sys.argv[1:]

    if not args or args[0] in ("deploy", "install"):
        from cmd.deploy import main as deploy_main

        if args and args[0] in ("deploy", "install"):
            sys.argv = [sys.argv[0]] + args[1:]
        else:
            sys.argv = [sys.argv[0]]
        deploy_main()

    elif args[0] in ("destroy", "uninstall"):
        from cmd.destroy import main as destroy_main

        sys.argv = [sys.argv[0]] + args[1:]
        destroy_main()

    elif args[0] == "down":
        from cmd.down import main as down_main

        sys.argv = [sys.argv[0]] + args[1:]
        down_main()

    elif args[0] == "up":
        from cmd.up import main as up_main

        sys.argv = [sys.argv[0]] + args[1:]
        up_main()

    elif args[0] == "status":
        from cmd.status import main as status_main

        sys.argv = [sys.argv[0]] + args[1:]
        status_main()

    elif args[0] in ("-h", "--help", "help"):
        print(USAGE)

    else:
        print(f"Unknown command: {args[0]}\n", file=sys.stderr)
        print(USAGE, file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
