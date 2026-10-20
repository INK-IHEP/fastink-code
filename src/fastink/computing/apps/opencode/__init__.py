"""OpenCode interactive web job — session-based proxy, no --base-path.

Accessible via OpenResty on port 8447 which authenticates from the
browser session cookie, resolves the user's running opencode job via
the FastINK API, injects Basic Auth, and proxies transparently to the
worker at /.

Runs the generic (non-basepath) CVMFS opencode binary with ``serve``.
"""

from __future__ import annotations

import base64

from fastink.common.config import get_config
from fastink.computing.apps.base import JobApp, ConnectResult
from fastink.computing.apps.registry import register
from fastink.computing.apps import _helpers as H


@register
class OpenCodeApp(JobApp):
    name = "opencode"
    connect_type = "opencode"
    # ``opencode serve`` prints "opencode server listening on http://..."
    # once the listener is up; keep the old ``web`` banner as a fallback.
    start_keywords = ["server listening on", "Network access:"]
    needs_iptables = False
    noenv = False
    nginx_template = None  # session proxy reuses the shared job-proxy location
    request_defaults = {
        "htc": {
            "RequestMemory": 6000,
            "RequestCpus": 1,
        }
    }

    async def prepare_submit(self, *, username, uid, job_dir, arguments=None):
        return f"$(ClusterId)" if arguments is None else f"$(ClusterId) {arguments}"

    def _connect_result(self, host, port, passwd, *, job_id=None):
        nginx_node = H.get_nginx_node()
        proxy_port = get_config(
            "computing", "opencode_session_proxy_port", fallback=8447, type=int
        )
        job_query = f"?_ink_job_id={job_id}" if job_id is not None else ""
        url = f"{nginx_node.rstrip('/')}:{proxy_port}/{job_query}"
        return ConnectResult(
            host=host,
            port=port,
            passwd=passwd,
            url=url,
            connect_type=self.connect_type,
        )

    async def connect(self, job_id, uid, cluster_id):
        _, login_info = await H.read_login_info(job_id, uid, cluster_id)
        host = H.parse_info(login_info, "HOST")
        port = H.parse_info(login_info, "PORT")
        passwd = H.parse_info(login_info, "PASSWD")

        if not host or not port:
            raise H.http_500("No host/port in opencode login file")

        return self._connect_result(
            host,
            port,
            passwd,
            job_id=job_id,
        )

    def get_proxy_credentials(self, result):
        if not result.passwd:
            return None
        encoded = base64.b64encode(f"opencode:{result.passwd}".encode()).decode()
        return f"Basic {encoded}"
