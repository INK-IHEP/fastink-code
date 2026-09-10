"""noVNC + TurboVNC interactive job.

Migrated from ``computing.tools.common.utils.connect_vnc_job``.
"""
from __future__ import annotations

from fastink.computing.apps.base import JobApp, ConnectResult
from fastink.computing.apps.registry import register
from fastink.computing.apps import _helpers as H


@register
class VncApp(JobApp):
    name = "vnc"
    connect_type = "vnc"
    start_keywords = ["Starting noVNC proxy on"]
    needs_iptables = False
    noenv = True
    nginx_template = "nginx.location.conf"
    request_defaults = {
        "htc": {
            "RequestMemory": 6000,
            "RequestCpus": 1,
        }
    }

    async def connect(self, job_id, uid, cluster_id):
        job_path, login_info = await H.read_login_info(job_id, uid, cluster_id)
        host, port = H.parse_hostport(login_info)
        if not host or not port:
            raise H.http_500("No host and port record in vnc loginfile.")

        otp = await H.generate_userotp(uid, host, job_path=job_path)
        nginx_node = H.get_nginx_node()
        url = f"{nginx_node}/vnc/{host}/{port}/vnc.html?password={otp}&autoconnect=true"
        return ConnectResult(
            host=host, port=port, url=url,
            connect_type=self.connect_type,
        )
