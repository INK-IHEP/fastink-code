"""noVNC + TurboVNC interactive job.

The generic VNC desktop.  Session behaviour (``connect()``, the startup
keyword, the job script) comes from
:class:`~fastink.computing.apps.families.VncSessionApp`; only what is
specific to this job type is declared here.  ``asic``, ``asicbm`` and
``ink_special`` are the same product with different resource defaults and
worker-node pinning -- see those modules in ``ihep_plugin.computing_apps``.

Migrated from ``computing.tools.common.utils.connect_vnc_job``.
"""
from __future__ import annotations

from fastink.computing.apps.families import VncSessionApp
from fastink.computing.apps.registry import register


@register
class VncApp(VncSessionApp):
    name = "vnc"
    #: The generic desktop opts out of inheriting the submitter's shell env.
    noenv = True
    #: Publishes its own /vnc/ nginx location.
    nginx_template = "nginx.location.conf"
    request_defaults = {
        "htc": {
            "RequestMemory": 6000,
            "RequestCpus": 1,
        }
    }
