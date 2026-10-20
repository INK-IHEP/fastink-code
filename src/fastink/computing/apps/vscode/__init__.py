"""VSCode (code-server) interactive job.

The generic code-server session.  ``connect()``, the startup keyword and
the job script come from
:class:`~fastink.computing.apps.families.CodeServerApp`; only what is
specific to this job type is declared here.  ``compile`` and ``npu`` are
the same product with different resource defaults and runtime
environments -- see those modules in ``ihep_plugin.computing_apps``.

Migrated from ``computing.tools.common.utils.connect_vscode_job``.
"""
from __future__ import annotations

from fastink.computing.apps.families import CodeServerApp
from fastink.computing.apps.registry import register


@register
class VscodeApp(CodeServerApp):
    name = "vscode"
    #: Publishes its own /vscode/ nginx location.
    nginx_template = "nginx.location.conf"
    request_defaults = {
        "htc": {
            "RequestMemory": 6000,
            "RequestCpus": 1,
        }
    }
