"""Interactive SSH login node ("enode") job.

Users get an ssh port on a compute worker routed via the FastINK gateway.
Migrated from ``computing.tools.common.utils.connect_sshd``.
"""
from __future__ import annotations

from fastink.computing.apps.base import JobApp, ConnectResult
from fastink.computing.apps.registry import register
from fastink.computing.apps import _helpers as H
from fastink.computing.tools.db.db_tools import get_job_iptables_status


@register
class EnodeApp(JobApp):
    name = "enode"
    connect_type = "enode"
    start_keywords = ["SSH server starting"]
    needs_iptables = True  # only enode currently goes through the gateway iptables path
    noenv = False
    nginx_template = None  # enode is TCP via gateway, no HTTP route
    request_defaults = {
        "htc": {
            "RequestMemory": 6000,
            "RequestCpus": 1,
        }
    }

    async def connect(self, job_id, uid, cluster_id):
        (gateway_port,) = get_job_iptables_status(uid, job_id, cluster_id)
        gateway_node = H.get_gateway_node()
        if gateway_port == 0:
            raise H.http_404("The job has expired.")
        return ConnectResult(
            host=gateway_node,
            port=None,
            gateway_port=gateway_port,
            connect_type=self.connect_type,
        )
