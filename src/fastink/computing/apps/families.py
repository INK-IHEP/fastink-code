"""Shared base classes for apps that run the same kind of session.

Several job types are the same product wearing a different name: ``vnc``,
``asic``, ``asicbm`` and ``ink_special`` all launch TurboVNC + noVNC and
authenticate through a one-time password, while ``vscode``, ``compile``
and ``npu`` all launch code-server behind a password login.  Within each
family the ``connect()`` implementation, the startup keyword and the
whole job script were byte-identical copies.

Copying is what caused the asic/asicbm outage: a regex edit truncated two
of the three VNC ``run.sh`` copies, the shared ``is_active()`` definition
disappeared from them, and every ASIC job was reaped at its deadline.
Nothing in the toolchain compared the copies, because nothing declared
them to be the same thing.

These bases make the relationship explicit.  A family member declares
only what actually differs -- its ``name``, resource defaults, and any
per-app opt-outs -- and inherits the rest:

    @register
    class AsicApp(VncSessionApp):
        name = "asic"
        request_defaults = {"htc": {"RequestMemory": 20000, "RequestCpus": 5}}

The shell side of the same idea lives in ``apps/shell.sh``
(``ink_run_vnc_session`` / ``ink_run_codeserver_session``); a family
member's ``run.sh`` sets its app-specific environment and calls the
shared runner.

Both bases live here rather than in ``base.py`` so that ``base.py`` stays
the framework contract (what every app must provide) and this module
holds the optional, product-level groupings.  Plugin-provided apps import
them exactly as they already import ``JobApp``.
"""

from __future__ import annotations

from fastink.computing.apps import _helpers as H
from fastink.computing.apps.base import ConnectResult, JobApp


class VncSessionApp(JobApp):
    """TurboVNC + noVNC session, OTP-authenticated.

    Members: ``vnc``, ``asic``, ``asicbm``, ``ink_special``.

    The job script writes ``app_login.info`` with the noVNC listen port;
    ``connect()`` asks the running job to mint a fresh one-time password
    over the filesystem RPC and returns the auto-connecting vnc.html URL.
    """

    #: Frontend routes this as a VNC session.
    connect_type = "vnc"
    #: Emitted by ``ink_run_vnc_session`` right before noVNC starts.
    start_keywords = ["Starting noVNC proxy on"]
    #: Reached through the nginx /vnc/ location, not the gateway iptables path.
    needs_iptables = False
    #: Only ``vnc`` publishes its own nginx snippet; the site-specific
    #: members reuse the /vnc/ route and override this to None (default).
    nginx_template = None

    async def connect(self, job_id, uid, cluster_id):
        job_path, login_info = await H.read_login_info(job_id, uid, cluster_id)
        host, port = H.parse_hostport(login_info)
        if not host or not port:
            raise H.http_500(
                f"No host and port record in {self.name} loginfile."
            )

        otp = await H.generate_userotp(uid, host, job_path=job_path)
        nginx_node = H.get_nginx_node()
        url = (
            f"{nginx_node}/vnc/{host}/{port}/vnc.html"
            f"?password={otp}&autoconnect=true"
        )
        return ConnectResult(
            host=host, port=port, url=url,
            connect_type=self.connect_type,
        )


class CodeServerApp(JobApp):
    """code-server session behind a generated password.

    Members: ``vscode``, ``compile``, ``npu``.

    The job script writes ``app_login.info`` with the listen port and the
    generated password; ``connect()`` returns the /vscode/ login URL and
    the password so the frontend can fill the form.
    """

    #: Frontend routes this as a VSCode session.
    connect_type = "vscode"
    #: Printed by code-server itself once the listener is up.
    start_keywords = ["Session server listening on"]
    needs_iptables = False
    #: Only ``vscode`` publishes its own nginx snippet; ``compile`` and
    #: ``npu`` reuse the /vscode/ route.
    nginx_template = None

    async def connect(self, job_id, uid, cluster_id):
        _, login_info = await H.read_login_info(job_id, uid, cluster_id)
        host, port = H.parse_hostport(login_info)
        passwd = H.parse_info(login_info, "PASSWD")
        if not host or not port or not passwd:
            raise H.http_500(
                f"No host, port or password record in {self.name} loginfile."
            )

        nginx_node = H.get_nginx_node()
        url = f"{nginx_node}/vscode/{host}/{port}/login"
        return ConnectResult(
            host=host, port=port, passwd=passwd, url=url,
            connect_type=self.connect_type,
        )


__all__ = ["VncSessionApp", "CodeServerApp"]
