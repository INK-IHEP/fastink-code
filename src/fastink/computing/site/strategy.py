"""Site strategy registry for job environment building and submission.

A "site" bundles two strategies selected by the ``computing.site``
config key:

- a job-environment builder registered with :func:`register_site`
- per-scheduler submitters registered with :func:`register_submitter`

The built-in ``generic`` site (``fastink.computing.site.generic``)
works for any deployment. Site-specific strategies (e.g. IHEP/HAI/HEPS)
live in external plugin packages loaded via ``unified_plugins.packages``
and register themselves on import (typically from the plugin's
``initialize()``).
"""

from typing import Callable

from fastink.common.logger import logger


_buildsite_registry: dict[str, Callable] = {}

def register_site(name: str):
    def deco(fn):
        _buildsite_registry[name] = fn
        return fn
    return deco

def get_site(name: str):
    if name not in _buildsite_registry:
        raise LookupError(
            f"Site '{name}' is not registered. "
            f"Available sites: {sorted(_buildsite_registry)}. "
            "Site strategies register on import; site-specific strategies are "
            "provided by plugin packages (unified_plugins.packages) — check "
            "that the plugin is installed and loaded."
        )
    return _buildsite_registry[name]



_submitters: dict[tuple[str,str], Callable] = {}

def register_submitter(site: str, mode: str):
    def deco(fn):
        _submitters[(site, mode)] = fn
        return fn
    return deco

def get_submitter(site: str, mode: str):
    if (site, mode) in _submitters:
        return _submitters[(site, mode)]
    # Sites only need to register the submitters they actually customize;
    # anything else falls back to the generic implementation.
    if ("generic", mode) in _submitters:
        if site != "generic":
            # Falling back for a non-generic site usually means the site
            # plugin failed to load or did not register this scheduler —
            # warn so a misconfigured production deploy is not silent.
            logger.warning(
                "No submitter registered for site '%s' scheduler '%s'; "
                "falling back to generic. If this site expects custom "
                "submit behavior, check that its plugin is installed and loaded.",
                site, mode,
            )
        return _submitters[("generic", mode)]
    raise LookupError(
        f"No submitter registered for site '{site}' and scheduler '{mode}' "
        f"(and no generic fallback). Registered: {sorted(_submitters)}."
    )

# Register the built-in generic site (kept at the bottom to avoid a
# circular import: generic.py imports the decorators from this module).
import fastink.computing.site.generic  # noqa: E402,F401
