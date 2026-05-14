"""CLI UI utilities for the FastINK installer.

Auto-installs rich + questionary into .deploy/.deps/ when not available,
so the user gets a polished terminal experience without manually installing
anything. Falls back to basic print/input when deps can't be installed.
"""

import shutil
import subprocess
import sys
from pathlib import Path
from typing import Optional

_DEPS_DIR: Optional[Path] = None
_HAS_DEPS = False
_console = None  # Lazy singleton for rich Console


def _get_console():
    """Return the module-level Console singleton, creating it on first call."""
    global _console
    if _console is None:
        from rich.console import Console
        _console = Console()
    return _console


def _check_importable() -> bool:
    try:
        # rich and questionary will be imported lazily inside each function
        import rich  # noqa: F401
        import questionary  # noqa: F401
        return True
    except ImportError:
        return False


_HAS_DEPS = _check_importable()


def ensure_deps(deploy_dir: Path) -> bool:
    """Install rich + questionary into deploy_dir/.deps/ if not available.

    Once installed, the directory is added to sys.path so subsequent
    imports succeed.  On subsequent runs the cached install is reused.
    Returns True when the enhanced UI is available.
    """
    global _DEPS_DIR, _HAS_DEPS

    if _HAS_DEPS:
        return True

    deps_dir = deploy_dir / ".deps"
    _DEPS_DIR = deps_dir
    deps_dir.mkdir(parents=True, exist_ok=True)

    if str(deps_dir) not in sys.path:
        sys.path.insert(0, str(deps_dir))

    if _check_importable():
        _HAS_DEPS = True
        return True

    print("→ Setting up CLI dependencies ... ", end="", flush=True)
    try:
        subprocess.run(
            [
                sys.executable,
                "-m",
                "pip",
                "install",
                "--target",
                str(deps_dir),
                "--quiet",
                "rich>=13",
                "questionary>=2",
            ],
            check=True,
            capture_output=True,
            text=True,
        )
        # Force reimport after install
        for mod in list(sys.modules.keys()):
            if mod.startswith(("rich.", "questionary.")):
                del sys.modules[mod]
        for mod in ("rich", "questionary", "rich.console", "rich.panel"):
            sys.modules.pop(mod, None)
        _HAS_DEPS = _check_importable()
        if _HAS_DEPS:
            print("done")
        else:
            print("failed — falling back to basic mode")
        return _HAS_DEPS
    except subprocess.CalledProcessError:
        print("failed — falling back to basic mode")
        return False


def cleanup_deps(deploy_dir: Path) -> None:
    """Remove the .deps directory created by ensure_deps()."""
    deps_dir = deploy_dir / ".deps"
    if deps_dir.exists():
        shutil.rmtree(deps_dir)


# ---------------------------------------------------------------------------
# Output helpers
# ---------------------------------------------------------------------------


def banner() -> None:
    """Print a welcome banner."""
    if _HAS_DEPS:
        from rich.text import Text
        from rich.style import Style

        console = _get_console()
        width = 60
        rule = Text("━" * width, style=Style(color="bright_blue", bold=True))

        body = Text()
        title_line = Text()
        title_line.append("⚡ ", style="bold yellow")
        title_line.append("F A S T I N K", style="bold bright_cyan")
        title_line.append(" ⚡", style="bold yellow")
        title_pad = (width - title_line.cell_len) // 2

        subtitle_line = Text("Interactive aNalysis worKbench", style="italic bright_blue")
        subtitle_pad = (width - subtitle_line.cell_len) // 2

        body.append(" " * title_pad)
        body.append(title_line)
        body.append("\n")
        body.append(" " * subtitle_pad)
        body.append(subtitle_line)

        console.print(rule)
        console.print(body)
        console.print(rule)
    else:
        print("FastINK — Interactive aNalysis worKbench")


def step(title: str) -> None:
    """Section header."""
    if _HAS_DEPS:
        from rich.panel import Panel

        _get_console().print(Panel(f"[bold blue]{title}[/bold blue]"))
    else:
        print(f"\n==> {title}")


def info(msg: str) -> None:
    """Info-level message."""
    if _HAS_DEPS:

        _get_console().print(f"  [cyan]•[/cyan] {msg}")
    else:
        print(f"  {msg}")


def success(msg: str) -> None:
    """Success message."""
    if _HAS_DEPS:

        _get_console().print(f"  [bold green]✓[/bold green] {msg}")
    else:
        print(f"  ✓ {msg}")


def warning(msg: str) -> None:
    """Warning message."""
    if _HAS_DEPS:

        _get_console().print(f"  [bold yellow]⚠[/bold yellow] {msg}")
    else:
        print(f"  ! {msg}")


def error(msg: str) -> None:
    """Error message."""
    if _HAS_DEPS:

        _get_console().print(f"  [bold red]✗[/bold red] {msg}")
    else:
        print(f"  ✗ {msg}")


# ---------------------------------------------------------------------------
# Summary table
# ---------------------------------------------------------------------------


def summary_table(items: list[tuple[str, str]], title: str = "") -> None:
    """Show a key-value table."""
    if _HAS_DEPS:
        from rich.table import Table

        table = Table(
            title=title or None,
            show_header=False,
            box=None,
            padding=(0, 2),
        )
        table.add_column("Key", style="bold cyan", no_wrap=True)
        table.add_column("Value", style="white")
        for key, value in items:
            table.add_row(key, str(value))
        _get_console().print(table)
    else:
        if title:
            print(f"\n  {title}")
        for key, value in items:
            print(f"  {key}: {value}")
        print()


# ---------------------------------------------------------------------------
# Progress indicators
# ---------------------------------------------------------------------------


class _NullProgress:
    """No-op context manager used when rich is unavailable."""

    def __enter__(self):
        return self

    def __exit__(self, *args):
        pass

    def advance(self, *args, **kwargs):
        pass

    def update(self, *args, **kwargs):
        pass

    def stop(self):
        pass


def spinner(message: str = "Working"):
    """Return a context manager that shows a spinner while active."""
    if _HAS_DEPS:
        from rich.progress import Progress, SpinnerColumn, TextColumn

        progress = Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            transient=False,
            console=_get_console(),
        )
        progress.add_task(description=message, total=None)
        progress.start()
        return progress
    else:
        print(f"  {message} ... ", end="", flush=True)
        return _NullProgress()


def progress_bar(description: str = "Progress", total: int = 1):
    """Create a managed progress bar.

    Returns (progress, task_id).  Call progress.advance(task_id) to
    step forward and progress.stop() when done.
    """
    if _HAS_DEPS:
        from rich.progress import (
            BarColumn,
            Progress,
            TaskID,
            TextColumn,
        )

        p = Progress(
            TextColumn("[bold blue]{task.description}"),
            BarColumn(),
            TextColumn("{task.completed}/{task.total}"),
            console=_get_console(),
        )
        tid = p.add_task(description, total=total)
        p.start()
        return p, tid
    else:
        print(f"  {description} ... ", end="", flush=True)
        return _NullProgress(), None


# ---------------------------------------------------------------------------
# Prompt helpers
# ---------------------------------------------------------------------------


def _abort():
    """Exit immediately when the user cancels a prompt (Ctrl+C)."""
    print("\n")
    warning("Aborted.")
    sys.exit(1)


def confirm_prompt(
    text: str,
    default: bool = True,
    reuse_value: Optional[bool] = None,
) -> bool:
    """Yes / no prompt rendered as a select-style choice when deps are available."""
    effective_default = reuse_value if reuse_value is not None else default
    if _HAS_DEPS:
        import questionary

        choices: list[str] = ["Yes", "No"]
        if reuse_value is not None:
            label = f"Use saved ({'Yes' if reuse_value else 'No'})"
            choices.insert(0, label)
        default_choice = "Yes" if effective_default else "No"
        result = questionary.select(text, choices=choices, default=default_choice).ask()
        if result is None:
            _abort()
        if result.startswith("Use saved"):
            return reuse_value
        return result == "Yes"
    # Basic fallback
    hint = "Y/n" if effective_default else "y/N"
    if reuse_value is not None:
        hint += f", r=reuse saved {'Y' if reuse_value else 'N'}"
    while True:
        value = input(f"{text} [{hint}]: ").strip().lower()
        if not value:
            return effective_default
        if reuse_value is not None and value == "r":
            return reuse_value
        if value in ("y", "yes"):
            return True
        if value in ("n", "no"):
            return False
        print("Please answer y or n." + (" (or r to reuse saved)" if reuse_value is not None else ""))


def text_prompt(
    text: str,
    default: str = "",
    reuse_value: Optional[str] = None,
) -> str:
    """Free-text prompt."""
    effective_default = reuse_value if reuse_value is not None else default
    if _HAS_DEPS:
        import questionary

        result = questionary.text(text, default=effective_default).ask()
        if result is None:
            _abort()
        return result.strip() if result else effective_default
    # Basic fallback
    suffix_parts: list[str] = []
    if default:
        suffix_parts.append(default)
    if reuse_value is not None:
        suffix_parts.append("r=reuse saved value")
    suffix = f" [{', '.join(suffix_parts)}]" if suffix_parts else ""
    value = input(f"{text}{suffix}: ").strip()
    if reuse_value is not None and value.lower() == "r":
        return reuse_value
    return value or default


def secret_prompt(
    text: str,
    default: str = "",
    reuse_value: Optional[str] = None,
) -> str:
    """Password / secret prompt."""
    effective_default = reuse_value if reuse_value is not None else default
    if _HAS_DEPS:
        import questionary

        result = questionary.password(text, default=effective_default).ask()
        if result is None:
            _abort()
        return result if result else effective_default
    # Basic fallback
    from getpass import getpass

    suffix_parts: list[str] = []
    if default:
        suffix_parts.append("press enter to use generated value")
    if reuse_value is not None:
        suffix_parts.append("r=reuse saved value")
    suffix = f" [{' ; '.join(suffix_parts)}]" if suffix_parts else ""
    value = getpass(f"{text}{suffix}: ").strip()
    if reuse_value is not None and value.lower() == "r":
        return reuse_value
    return value or default


def int_prompt(
    text: str,
    default: int = 0,
    reuse_value: Optional[int] = None,
) -> int:
    """Integer-only prompt."""
    effective_default = reuse_value if reuse_value is not None else default
    if _HAS_DEPS:
        import questionary

        while True:
            result = questionary.text(
                text,
                default=str(effective_default),
                validate=lambda v: v.isdigit() or (v == ""),
            ).ask()
            if result is None:
                _abort()
            if result == "":
                return effective_default
            try:
                return int(result)
            except ValueError:
                warning("Please enter a valid integer")
    # Basic fallback
    while True:
        value = text_prompt(text, str(default), str(reuse_value) if reuse_value is not None else None)
        try:
            return int(value)
        except ValueError:
            print("Please enter a valid integer.")


def choice_prompt(
    text: str,
    options: list[str],
    default: str = "",
    reuse_value: Optional[str] = None,
) -> str:
    """Select from a list of options."""
    effective_default = reuse_value if reuse_value is not None else default
    if _HAS_DEPS:
        import questionary

        result = questionary.select(
            text,
            choices=options,
            default=effective_default or options[0],
        ).ask()
        if result is None:
            _abort()
        return result or effective_default or options[0]
    # Basic fallback
    options_display = "/".join(options)
    while True:
        reuse_hint = f", r=reuse saved {reuse_value}" if reuse_value else ""
        value = input(
            f"{text} [{options_display}] (default: {default}{reuse_hint}): "
        ).strip().lower()
        if not value:
            return default
        if reuse_value is not None and value == "r":
            return reuse_value
        if value in options:
            return value
        if reuse_value is not None:
            print(f"Please choose one of: {options_display}, or r.")
        else:
            print(f"Please choose one of: {options_display}")
