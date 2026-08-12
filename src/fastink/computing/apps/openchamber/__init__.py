"""OpenChamber — session-based reverse proxy job (no --base-path).

Accessible via OpenResty on port 8446 which authenticates from the
browser session cookie, resolves the user's job host and port via
FastINK API, and proxies transparently to the worker at /.

Initially launches the same opencode serve binary; the run script may
be extended later to also start the OpenChamber frontend.
"""

from __future__ import annotations

import asyncio
import base64

from fastink.common.config import get_config
from fastink.common.logger import logger
from fastink.computing.apps.base import JobApp, ConnectResult
from fastink.computing.apps.registry import register
from fastink.computing.apps import _helpers as H

FALLBACK_CONNECT_TIMEOUT = 2


@register
class OpenChamberApp(JobApp):
    name = "openchamber"
    connect_type = "openchamber"
    start_keywords = ["daemon running"]
    needs_iptables = False
    noenv = False
    nginx_template = "nginx.location.conf"
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
            "computing", "opencode_proxy_port", fallback=8446, type=int
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
            raise H.http_500("No host/port in openchamber login file")

        return self._connect_result(
            host,
            port,
            passwd,
            job_id=job_id,
        )

    async def resolve_proxy_fallback(self, *, username, uid, cluster_id):
        latest = await H.read_latest_job_login_info(
            username=username,
            uid=uid,
            job_type=self.name,
        )
        if latest is None:
            return None

        job_dir, login_info = latest
        host = H.parse_info(login_info, "HOST")
        port = H.parse_info(login_info, "PORT")
        passwd = H.parse_info(login_info, "PASSWD")
        try:
            port_number = int(port)
            if not host or not passwd or not 1 <= port_number <= 65535:
                return None
        except (TypeError, ValueError):
            return None

        try:
            _, writer = await asyncio.wait_for(
                asyncio.open_connection(host, port_number),
                timeout=FALLBACK_CONNECT_TIMEOUT,
            )
            writer.close()
        except (OSError, asyncio.TimeoutError):
            logger.info(
                "OpenChamber fallback target is unreachable: user=%s dir=%s host=%s port=%s",
                username,
                job_dir,
                host,
                port_number,
            )
            return None

        try:
            await asyncio.wait_for(
                writer.wait_closed(),
                timeout=FALLBACK_CONNECT_TIMEOUT,
            )
        except (OSError, asyncio.TimeoutError):
            pass

        logger.info(
            "OpenChamber proxy resolved from latest job directory: user=%s dir=%s host=%s port=%s",
            username,
            job_dir,
            host,
            port_number,
        )
        return self._connect_result(host, str(port_number), passwd)

    def get_proxy_credentials(self, result):
        if not result.passwd:
            return None
        encoded = base64.b64encode(f"opencode:{result.passwd}".encode()).decode()
        return f"Basic {encoded}"
