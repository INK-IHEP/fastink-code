"""Discovery and lookup registry for FastINK computing job apps.

Every module under ``fastink.computing.apps.<name>`` is imported once at
process start.  The subclass of :class:`~fastink.computing.base.JobApp`
declared inside registers itself via :func:`register`.

Plugin packages provide additional (site-specific) apps the same way the
site strategies work: the plugin's ``initialize()`` imports its own apps
package, whose modules use the same :func:`register` decorator.  See
``ihep_plugin.computing_apps`` for the reference implementation
(openclaw, herddisplay, asic, ...).  Name collisions between a plugin
app and a built-in app are logged as warnings by :func:`register`.

The rest of the codebase (adapters, router, deploy render, cron loops)
consults the registry through the aggregators below rather than hard-coded
lists in ``config.yml`` or if/elif ladders in Python.
"""

from __future__ import annotations

import importlib
import pkgutil
from pathlib import Path
from typing import Dict, Iterable, List, Tuple, Type

from fastink.common.logger import logger
from fastink.computing.apps.base import JobApp

_APPS: Dict[str, JobApp] = {}
_DISCOVERED: bool = False


def register(cls: Type[JobApp]) -> Type[JobApp]:
    """Class decorator that instantiates and registers a JobApp subclass.

    Registration is idempotent: re-importing the same module (e.g. from
    tests) transparently replaces the previous instance.  A *different*
    class registering under the same ``.name`` is almost always a bug
    (e.g. a plugin accidentally shadowing a built-in app) -- we log a
    warning so the collision is visible.
    """

    if not isinstance(cls, type) or not issubclass(cls, JobApp):
        raise TypeError(f"register() expects a JobApp subclass, got {cls!r}")
    instance = cls()
    if not instance.name:
        raise ValueError(
            f"JobApp {cls.__module__}.{cls.__name__} is missing .name"
        )

    existing = _APPS.get(instance.name)
    if existing is not None and type(existing) is not cls:
        logger.warning(
            "JobApp name collision: %r previously registered by %s.%s, "
            "now being replaced by %s.%s",
            instance.name,
            type(existing).__module__, type(existing).__name__,
            cls.__module__, cls.__name__,
        )
    _APPS[instance.name] = instance
    return cls


def discover(force: bool = False) -> None:
    """Import every ``fastink.computing.apps.<name>`` package once.

    Called automatically on first lookup.  ``force=True`` re-runs the
    walk (useful for tests that add packages at runtime).

    ``_DISCOVERED`` is only set once the walk has actually finished (or
    the apps package was successfully imported).  This means that if the
    apps package itself fails to import, the next caller retries rather
    than silently returning an empty registry.
    """

    global _DISCOVERED
    if _DISCOVERED and not force:
        return

    try:
        pkg = importlib.import_module("fastink.computing.apps")
    except ImportError:
        logger.warning("fastink.computing.apps package not importable")
        return  # do NOT set _DISCOVERED -- allow retry on next call

    for _, name, ispkg in pkgutil.iter_modules(pkg.__path__):
        if not ispkg or name.startswith("_"):
            continue
        try:
            importlib.import_module(f"fastink.computing.apps.{name}")
        except Exception as exc:
            logger.exception(
                "Failed to import fastink.computing.apps.%s: %s", name, exc
            )
    _DISCOVERED = True


def _ensure() -> None:
    if not _DISCOVERED:
        discover()


# ---------------------------------------------------------------------------
# Lookups
# ---------------------------------------------------------------------------

def get(name: str) -> JobApp:
    """Return the JobApp for *name* or raise KeyError."""
    _ensure()
    try:
        return _APPS[name]
    except KeyError as exc:
        raise KeyError(f"Unknown job type: {name!r}") from exc


def try_get(name: str) -> JobApp | None:
    _ensure()
    return _APPS.get(name)


def has(name: str) -> bool:
    _ensure()
    return name in _APPS


def all_apps() -> List[JobApp]:
    _ensure()
    return list(_APPS.values())


def names() -> List[str]:
    _ensure()
    return sorted(_APPS.keys())


# ---------------------------------------------------------------------------
# Aggregators that replace flat config keys
# ---------------------------------------------------------------------------

def iptables_jobtypes() -> List[str]:
    """Replaces ``computing.iptables_jobtype``."""
    return [a.name for a in all_apps() if a.needs_iptables]


def noenv_jobtypes() -> List[str]:
    """Replaces ``computing.noenv_jobtype``."""
    return [a.name for a in all_apps() if a.noenv]


def start_keywords_for(job_type: str) -> List[str]:
    """Precise per-jobtype keyword list.

    Prefer this over :func:`start_keywords_all` in adapters that know the
    job type they are inspecting.
    """
    app = try_get(job_type)
    return list(app.start_keywords) if app else []


def start_keywords_all() -> List[str]:
    """Union of every registered app's keywords.

    Kept for legacy call sites that scan output without knowing the job
    type; new code should prefer :func:`start_keywords_for`.
    """
    out: List[str] = []
    for a in all_apps():
        out.extend(a.start_keywords)
    return out


def request_defaults() -> Dict[str, Dict[str, Dict[str, object]]]:
    """Replaces the flat ``jobtype:`` block in ``config.yml``.

    Shape: ``{app_name: {"htc": {...}}}``.
    """
    return {a.name: dict(a.request_defaults) for a in all_apps() if a.request_defaults}


def nginx_snippets() -> Iterable[Tuple[str, Path]]:
    """Yield (app_name, path_to_snippet) for every app with an nginx tpl."""
    for a in all_apps():
        snip = a.nginx_snippet_path()
        if snip is not None:
            yield a.name, snip


__all__ = [
    "register",
    "discover",
    "get",
    "try_get",
    "has",
    "all_apps",
    "names",
    "iptables_jobtypes",
    "noenv_jobtypes",
    "start_keywords_for",
    "start_keywords_all",
    "request_defaults",
    "nginx_snippets",
]
