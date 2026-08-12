"""VSCode (code-server) interactive job.

Migrated from ``computing.tools.common.utils.connect_vscode_job``.
"""
from __future__ import annotations

from fastink.computing.apps.base import JobApp, ConnectResult
from fastink.computing.apps.registry import register
from fastink.computing.apps import _helpers as H


@register
class VscodeApp(JobApp):
    name = "vscode"
    connect_type = "vscode"
    start_keywords = ["Session server listening on"]
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
        passwd = H.parse_info(login_info, "PASSWD")
        if not host or not port or not passwd:
            raise H.http_500("No host and port record in vscode loginfile.")

        nginx_node = H.get_nginx_node()
        url = f"{nginx_node}/vscode/{host}/{port}/login"
        return ConnectResult(
            host=host, port=port, passwd=passwd, url=url,
            connect_type=self.connect_type,
        )
