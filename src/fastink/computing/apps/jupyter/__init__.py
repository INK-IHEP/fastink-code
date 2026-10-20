"""JupyterLab interactive job.

Migrated from ``computing.tools.common.utils.connect_jupyter_job``.
"""
from __future__ import annotations

from fastink.computing.apps.base import JobApp, ConnectResult
from fastink.computing.apps.registry import register
from fastink.computing.apps import _helpers as H


@register
class JupyterApp(JobApp):
    name = "jupyter"
    connect_type = "jupyter"
    # Two possible startup strings depending on jupyter-server version.
    start_keywords = [
        "jupyterlab | extension was successfully loaded.",
        "running on  http://",
    ]
    needs_iptables = False
    noenv = True  # jupyter historically opted out of user env
    nginx_template = "nginx.location.conf"
    request_defaults = {
        "htc": {
            "RequestMemory": 6000,
            "RequestCpus": 1,
        }
    }

    async def connect(self, job_id, uid, cluster_id):
        _, login_info = await H.read_login_info(job_id, uid, cluster_id)
        host, port = H.parse_hostport(login_info)
        token = H.parse_info(login_info, "TOKEN")
        if not host or not port or not token:
            raise H.http_500("No host and port record in jupyter loginfile.")

        nginx_node = H.get_nginx_node()
        url = f"{nginx_node}/jupyter/{host}/{port}/lab?token={token}"
        return ConnectResult(
            host=host, port=port, token=token, url=url,
            connect_type=self.connect_type,
        )
