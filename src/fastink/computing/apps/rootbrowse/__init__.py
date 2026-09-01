"""ROOT browse (screen-hosted ROOT web server) interactive job.

Migrated from ``computing.tools.common.utils.connect_rootbrowse_job``.
"""
from __future__ import annotations

from fastink.computing.apps.base import JobApp, ConnectResult
from fastink.computing.apps.registry import register
from fastink.computing.apps import _helpers as H


@register
class RootbrowseApp(JobApp):
    name = "rootbrowse"
    connect_type = "rootbrowse"
    start_keywords = ["Start rootbrowse in screen session"]
    needs_iptables = False
    noenv = False
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
            raise H.http_500("Invalid root login info format.")

        nginx_node = H.get_nginx_node()
        url = f"{nginx_node}/rootbrowse/{host}/{port}/win1/?key={token}"
        return ConnectResult(
            host=host, port=port, token=token, url=url,
            connect_type=self.connect_type,
        )
