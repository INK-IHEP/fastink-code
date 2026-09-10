"""Base classes for FastINK computing app modules.

Each interactive/batch job type (vscode, vnc, jupyter, openclaw, ...) is
implemented as a subclass of :class:`JobApp` living under
``fastink.computing.apps.<name>``. A single instance of each subclass is
registered with :mod:`fastink.computing.apps.registry` at import time and becomes
the canonical source of truth for:

* the job type's connect() implementation (was ``connect_<type>_job``);
* its startup detection keywords (was flat ``computing.start_keywords``);
* whether the type participates in the iptables gateway
  (was ``computing.iptables_jobtype``);
* whether the type opts out of user env inheritance
  (was ``computing.noenv_jobtype``);
* its HTC scheduler request defaults
  (was ``jobtype.<name>.htc.*`` in ``config.yml``);
* the nginx location snippet used by the reverse proxy (was
  ``deploy/templates/base/nginx/locations/<name>.conf``);
* the per-app ``run.sh`` (was ``computing/scripts/<name>/run.sh``); every
  app now delegates to the shared ``computing/shell.sh`` for common setup.

The old flat configuration keys and per-type ``connect_*`` functions have
been removed.  Adapters, router, deploy render and nginx templates now
consult the registry directly.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, ClassVar, Dict, List, Optional


@dataclass
class ConnectResult:
    """Return type for :meth:`JobApp.connect`.

    Only ``host`` is always present.  Every other field is optional and
    only surfaces in the /connect_job response payload if the app
    populates it -- ``port`` is intentionally optional because the
    ``enode`` job type returns ``gateway_port`` instead.  ``extra`` is a
    free-form dict for job types that need to attach ad-hoc keys.
    """

    host: str
    port: Optional[Any] = None
    url: Optional[str] = None
    token: Optional[str] = None
    passwd: Optional[str] = None
    gateway_port: Optional[int] = None
    connect_type: Optional[str] = None
    extra: Dict[str, Any] = field(default_factory=dict)

    def to_response(self, job_id: int) -> Dict[str, Any]:
        data: Dict[str, Any] = {
            "host": self.host,
            "jobId": job_id,
            "connect_type": self.connect_type or "",
        }
        if self.port is not None:
            data["port"] = self.port
        if self.url is not None:
            data["url"] = self.url
        if self.token is not None:
            data["token"] = self.token
        if self.passwd is not None:
            data["passwd"] = self.passwd
        if self.gateway_port is not None:
            data["gateway_port"] = self.gateway_port
        if self.extra:
            data.update(self.extra)
        return data


class JobApp:
    """Metadata + behaviour for one job type.

    Subclasses are declared under ``fastink.computing.apps.<name>`` and
    decorated with :func:`fastink.computing.apps.registry.register`.  A single
    instance is instantiated at import time.

    Class attributes act as the manifest for that job type.  Instance
    methods (``connect``) implement runtime behaviour.
    """

    # ---- identity -------------------------------------------------------
    #: Canonical job-type name (must match ``job_type`` in the API).
    name: ClassVar[str] = ""
    #: Value returned as ``connect_type`` in the /connect_job payload.
    #: Defaults to :attr:`name` when unset.
    connect_type: ClassVar[str] = ""

    # ---- lifecycle detection -------------------------------------------
    #: Substrings searched in the job's stdout to decide that it has
    #: entered the RUNNING state (was ``computing.start_keywords``).
    start_keywords: ClassVar[List[str]] = []
    #: True if the job needs an iptables entry on the gateway to reach
    #: its sshd (was ``computing.iptables_jobtype``).
    needs_iptables: ClassVar[bool] = False
    #: True if the job should not inherit the submitter's shell env
    #: (was ``computing.noenv_jobtype``).
    noenv: ClassVar[bool] = False

    # ---- scheduler request defaults ------------------------------------
    #: HTC defaults per job type; merged into the ``.sub`` file at submit
    #: time (was ``jobtype.<name>.htc`` in ``config.yml``).
    #: Shape: ``{"htc": {"RequestMemory": int, "RequestCpus": int, ...}}``.
    request_defaults: ClassVar[Dict[str, Dict[str, Any]]] = {}

    # ---- shipped assets -------------------------------------------------
    #: File name of the per-app job script (relative to the app dir).
    run_script: ClassVar[str] = "run.sh"
    #: File name of the nginx location snippet inside the app dir; None
    #: means the app does not add a public HTTP route.
    nginx_template: ClassVar[Optional[str]] = None

    # ---- gateway routing -------------------------------------------------
    #: How the reverse proxy treats this app's HTTP route:
    #: "direct"      -- proxy_pass straight to the worker (current behaviour
    #:                  for all apps).
    #: "auth-gated"  -- reserved for the unified auth_request gateway design
    #:                  (see the corresponding fastink-code issue); not yet
    #:                  consumed by the render layer.
    route_policy: ClassVar[str] = "direct"

    # ---- helpers --------------------------------------------------------
    @property
    def resource_type(self) -> str:
        return self.connect_type or self.name

    def app_dir(self) -> Path:
        """Filesystem directory holding this app's run.sh / nginx conf."""
        module_file = __import__(
            self.__class__.__module__, fromlist=["__file__"]
        ).__file__
        return Path(module_file).resolve().parent

    def nginx_snippet_path(self) -> Optional[Path]:
        if not self.nginx_template:
            return None
        return self.app_dir() / self.nginx_template

    def run_script_path(self) -> Path:
        """Absolute path of the per-app run.sh shipped next to the class.

        Works for built-in apps and plugin-provided apps alike, since the
        location is derived from the defining module rather than from the
        ``computing.cluster_scripts`` config key.
        """
        return self.app_dir() / self.run_script

    async def prepare_submit(
        self,
        *,
        username: str,
        uid: int,
        job_dir: str,
        arguments: Optional[str] = None,
    ) -> Optional[str]:
        """Pre-submit hook: adjust arguments / stage auxiliary files.

        Called by the HTC submit-file generators right before the ``.sub``
        content is assembled.  Apps that need extra submit-time work
        (e.g. openclaw writes its bind-mount metadata file and rewrites
        the argument list) override this.  The default returns
        ``arguments`` unchanged.
        """
        return arguments

    async def connect(
        self,
        job_id: int,
        uid: int,
        cluster_id: str,
    ) -> ConnectResult:
        """Return the frontend-facing connection info for a running job.

        Subclasses must implement this.  The base implementation raises
        NotImplementedError so registering a subclass that forgot to
        override it fails fast at first call rather than silently.
        """

        raise NotImplementedError(
            f"JobApp {self.name!r} did not implement connect()"
        )

    async def resolve_proxy_fallback(
        self,
        *,
        username: str,
        uid: int,
        cluster_id: str,
    ) -> Optional[ConnectResult]:
        """Resolve proxy details when the scheduler has no usable job.

        Session-proxied apps may override this for a storage-backed fallback.
        Returning ``None`` keeps the default scheduler-only behaviour.
        """
        return None

    def get_proxy_credentials(self, result: ConnectResult) -> Optional[str]:
        """Return credentials for a reverse proxy, if this app needs them."""
        return None
