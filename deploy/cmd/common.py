#!/usr/bin/env python3
"""Shared utilities for FastINK deploy CLI commands.

Performs path setup once for all commands and provides common helpers
that were previously duplicated across every cmd/*.py module.
"""

import json
import sys
from pathlib import Path

# Path setup: make deploy/ importable as a top-level package,
# avoiding the 7-line preamble duplicated in every command module.
_HERE = Path(__file__).resolve().parent
if str(_HERE.parent) not in sys.path:
    sys.path.insert(0, str(_HERE.parent))

from lib.deploy_io import resolve_deploy_paths

DEPLOY_PATHS = resolve_deploy_paths()
DEPLOY_DIR = DEPLOY_PATHS.deploy_dir


def load_deploy_answers() -> dict[str, object]:
    """Load raw answers from .deploy/answers.json (no normalization).

    Used by lightweight subcommands (destroy, down, up, status) that
    only need project_name and a few other keys.
    """
    from lib import cli_ui

    answers_path = DEPLOY_DIR / "answers.json"
    if not answers_path.exists():
        cli_ui.error(f"No deployment found at {DEPLOY_DIR}")
        sys.exit(1)
    try:
        return json.loads(answers_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        cli_ui.error(f"Failed to parse answers file {answers_path}: {exc}")
        sys.exit(1)
