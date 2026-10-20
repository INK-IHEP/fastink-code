# =============================================================================
# FastINK Application Configuration — Canonical Defaults
# =============================================================================
#
# This file is the single source of truth for ALL config keys.
# Every key that get_config() reads at runtime must have a default here.
#
# HOW TO ADD A NEW CONFIG KEY
# ===========================
#
#   Does the value need to be provided by the person running `fastinkctl deploy`?
#   (e.g. database password, hostname, port number)
#     YES → 1. Add a template variable placeholder here
#            2. Add it to the questionnaire: deploy/lib/questionnaire.py
#            3. Add a mapping entry: deploy/lib/render.py:build_mapping()
#     NO  → Just hardcode the default value directly in this file.
#
#   Will different sites (IHEP, HAI, HEPS) need different values?
#     YES → Put the generic default here, then override it in the
#           site overlay (fastink-dev/overlay/config.ihep.yml).
#
# LAYERING (deep-merge order)
# ===========================
#   this template (after rendering)
#     → + site overlay (e.g. fastink-dev/overlay/config.ihep.yml)
#       → + dev overlay (e.g. .vscode/.generated/config.dev.overlay.yml)
#         = final config.yml
#
# ⚠  Overlays must ONLY override keys that already exist here.
#    Never add a new key exclusively in an overlay.
# =============================================================================

common:
  krb5_enabled: {{ krb5_enabled }}
  security_access: {{ security_access }}
  ip_whitelist_access: false
  log_level: INFO
  log_path: /ink/log/ink.log

database:
  host: fastink-db
  port: 3306
  user: {{ db_user | to_yaml }}
  password: {{ db_password | to_yaml }}
  dbname: {{ db_name | to_yaml }}

redis:
  host: fastink-redis
  port: 6379
  password: {{ redis_password | to_yaml }}

auth:
  type: {{ auth_type | to_yaml }}
  # Backends register by name (auth.type) via the auth-backend registry;
  # site backends (e.g. ihep_plugin's "hai") come from unified_plugins.
  issuer: ""
  client_id: null
  client_secret: ""
  # OIDC migration (fastink-code issue #30). auth.mode gates the new OIDC
  # code paths: legacy = current Ink-Username/Ink-Token only (default,
  # preview-safe); dual = Bearer and Ink headers both accepted,
  # channel-exclusive; oidc = Bearer only. Real secrets (signing_key,
  # app_key/app_secret, CA keys) are supplied via site overlay, never here.
  mode: legacy
  oidc:
    issuer: ""
    audience: ""
    algorithms: ["RS256"]
    signing_key: ""
    jwks_ttl: 300
    leeway: 60
    allowed_redirect_uris: []
    clients:
      - client_id: "ink-web"
        redirect_uris: []
        grants: ["authorization_code"]
        scopes: ["openid", "profile"]
      - client_id: "ink-cli"
        redirect_uris: []
        grants: ["urn:ietf:params:oauth:grant-type:device_code"]
        scopes: ["openid", "profile", "jobs:read", "jobs:submit", "files:read", "files:write"]
  sso:
    app_key: ""
    app_secret: ""
    redirect_uri: ""  # public URL of /authorize/sso/callback
    authorize_url: ""
    token_url: ""
    umt_api: ""
  delegation:
    ca_private_key: ""
    ccache_encrypt_key: ""
  krb5_validate:
    # REQUIRED when auth.type=krb5. An existing, non-krbtgt service
    # principal in the local realm; validation verifies the presented TGT
    # by requesting a ticket for it from the KDC (kvno). The KDC must be
    # able to issue tickets for it; the target service itself is never
    # contacted. krbtgt principals are forbidden (they would bypass the
    # KDC via the ccache's own TGT entry).
    service_principal: ""
    # REQUIRED when auth.type=krb5; empty disables successful validation.
    realm: ""
    cache_ttl: 600
    negative_backoff: 5
    timeout: 5
    max_concurrency: 8

security:
  # Login/authorize/device credential failures. Per-IP and per-user counters
  # share one budget so rotating endpoints does not reset it. A successful
  # login clears only the user counter.
  rate_limit:
    failures_per_user: 5
    # 0 disables per-IP accounting. Enable only when the proxy chain forwards
    # the real client address (otherwise all users share one budget).
    failures_per_ip: 0
    window: 300
    lockout: 900
  # 0 = non-expiring API keys are allowed; >0 caps expires_in_days.
  apikey_max_ttl_days: 0
  # Exact IPs or CIDR networks allowed when common.ip_whitelist_access is
  # true. The defaults express "trust any private network" — on a public
  # or cloud host, review and narrow this list before enabling the
  # whitelist (e.g. 10.0.0.0/8 covers an entire VPC). Keep 172.16.0.0/12:
  # it covers the compose networks used by inter-container traffic.
  ip_whitelist:
    - 127.0.0.1
    - 172.16.0.0/12
    - 192.168.0.0/16
    - 10.0.0.0/8
  ip_controlled_routers:
    - /api/v1/
    # Sensitive endpoints: auth routes that return credentials, enumerate
    # or create users, plus shared_file (a file download, not an auth
    # route — its share JWT stays the gate even when this whitelist is
    # off). Entries ALSO listed in skip_routers below are token-exempt;
    # when the whitelist is enabled it becomes their gate.
    # get_token is deliberately NOT in the base skip_routers: with
    # ip_whitelist_access off (the default) exempting it would let anyone
    # fetch a user's token by name. Sites that run with an active IP
    # whitelist may exempt it (the IHEP overlay does).
    - /api/v2/auth/get_token
    - /api/v2/auth/get_permission
    - /api/v2/auth/get_users_by_permission
    - /api/v2/auth/get_user
    - /api/v2/auth/create_user
    - /api/v2/fs/shared_file
  # Routers in ip_controlled_routers where a VALID Ink-Username/Ink-Token
  # pair bypasses the IP whitelist (unauthenticated requests still require
  # a whitelisted IP). Enables CLI clients outside the intranet. The token
  # is validated by the middleware, header presence alone is not enough.
  # Keep this list minimal: do NOT add get_token here (it returns
  # credentials). Bypass entries should also be listed in skip_routers
  # below, otherwise the token is validated twice per request (once by the
  # IP middleware, once by UserValidationMiddleware). Overlays that
  # override this list must re-add entries.
  ip_whitelist_token_bypass_routers:
    - /api/v2/auth/get_permission
  skip_routers:
    # Overlays that override this list must re-add every entry they still
    # need (list-replace semantics): dropping shared_file here silently
    # breaks file-share downloads when security_access is on.
    - /api/v1/
    # anonymous auth entrypoints (no token available before login):
    - /api/v2/auth/create_token
    - /api/v2/auth/create_and_get_token
    - /api/v2/auth/validate_token
    - /api/v2/auth/auth_request
    # token-exempt but IP-controlled (see ip_controlled_routers above):
    - /api/v2/auth/get_permission
    - /api/v2/auth/get_users_by_permission
    - /api/v2/auth/get_user
    - /api/v2/auth/create_user
    # self-authenticated file-share download: the handler requires a valid
    # share JWT (Authorization: Bearer) signed with the deploy answer
    # file_share_jwt_secret (injected at deploy time as env
    # INK_FILE_SHARE_JWT_SECRET — not a rendered config.yml key) — never
    # an Ink-Token — so it must be token-exempt here. Also IP-controlled
    # above: with ip_whitelist_access on, share recipients must come from
    # a whitelisted IP (pre-existing design; sites serving public share
    # links should weigh removing it from ip_controlled_routers).
    - /api/v2/fs/shared_file
    # full backend config: token-exempt but gated in the handler itself
    # (whitelisted service IP, or valid token + admin permission)
    - /api/v2/auth/get_config
    # public read-only dashboard aggregate: the core handler takes no
    # auth dependency and returns only per-jobtype counts (no user
    # data, no credentials) — documented as unauthenticated in the API
    # reference. Skip-only, NOT IP-controlled (IHEP overlay does the
    # same). get_stackjobs is deliberately NOT here: its payload comes
    # from the site-configured app.plugins module with no core output
    # guarantee — sites must audit their plugin and exempt it via
    # overlay (the IHEP overlay does).
    - /api/v2/cr/get_systemjobs
    - /api/v2/status/
    - /health
    - /version
    - /status

storage:
  xrd_host: {{ xrd_host | to_yaml }}
  fs_backend: xrootd
  max_file_size: 2147483648

computing:
  site: generic
  cluster_list:
{% for item in cluster_list %}
    - {{ item | to_yaml }}
{% endfor %}
  schedd_host: {{ schedd_host | to_yaml }}
  cm_host: {{ cm_host | to_yaml }}
  gateway_node: localhost
  # Root of the JobApp framework tree:
  #   <cluster_scripts>/apps/shell.sh                  ← shared launcher
  #   <cluster_scripts>/apps/<jobtype>/run.sh          ← per-app entrypoint
  #   <cluster_scripts>/apps/base.py + registry.py     ← JobApp abstraction
  # Plugin-shipped apps (fastink-plugins-ihep etc.) live under their own
  # Python package and are resolved via JobApp.run_script_path().
  cluster_scripts: /ink/src/fastink/computing
  interactive_job_time_limit: "24:00:00"
  nginx_node: {{ public_base_url | to_yaml }}
  # Per-user FastINK root. Supported placeholders: {username}, {user_group},
  # {experiment_group}, and {experiment_group_lower}. Use "~" for the
  # account's home directory.
  ink_dir: /home/{username}
  # Optional DNS suffix appended to bare node names in job listings
  # (e.g. ".example.org"). Empty = no suffix.
  node_domain_suffix: {{ node_domain_suffix | to_yaml }}
  # Slurm partitions counted in system-info summaries. Empty = all.
  system_info_partitions: []
  # Host ports of the per-job reverse proxies for opencode/openchamber
  # sessions (nginx upstreams). Must match the values the job launchers
  # register.
  opencode_proxy_port: 8446
  opencode_session_proxy_port: 8447
  # NOTE: start_keywords / noenv_jobtype / iptables_jobtype are no longer
  # part of the config -- they are derived at runtime from the JobApp
  # classes under fastink.computing.apps (via fastink.computing.apps.registry).
  # NOTE: vnc_otp_script was removed; VNC OTP minting is done by the job's
  # own otp_start_listener (see apps/shell.sh) via FS-based RPC.

crond:
  submit_workers: []
  async_submit_retries: 3
  retry_delay_seconds: 10

job_time:
  walltime: 24
  check_interval: 900
  active_idle: 1800

# Site-specific per-jobtype overrides.  The generic defaults live on the
# JobApp classes under fastink.computing.apps.<name>.request_defaults;
# anything set here wins for that app at submit time.  Also carries
# app-specific extra config (herddisplay.herd_root/xml/bin, ...).
#
# Per-app watchdog knobs under ``<jobtype>.htc`` (all optional):
#   init_hours     -- hours before the watchdog starts intervening; an active
#                     session is renewed past this (default job_time.walltime).
#   max_hours      -- absolute ceiling on total runtime; the watchdog never
#                     renews past it even if the user is still active.  0/unset
#                     = no ceiling (renew indefinitely).  e.g. 168 = 7 days.
#   idle_kill_sec  -- idle-recycle window (openchamber); 0/unset = disabled.
jobtype:
  atmo:
    # ATMO interactive web job (registered by the ihep_plugin computing app).
    # Site overlay provides the real values; empty defaults make submit fail
    # fast with a clear message until a site configures them.
    atmo_source: ""
    model_api_key_file: ""

hooks:
  modules: ""

app:
  plugins: ""

plugins:
  router_plugins: ""

unified_plugins:
  packages: ""

service:
  service_node: fastink-rootbrowse
  service_port: 2000
  ink_dir: /home/{username}
  monitor_url: {{ public_base_url | to_yaml }}
  job_monitor_url: {{ public_base_url | to_yaml }}
  rootbrowse_script: /dev/shm/start-rootbrowse.sh
  rootbrowse_check_script: /dev/shm/check-rootbrowse.sh
  # SSH private key used to reach the rootbrowse service node.
  ssh_private_key: /root/.ssh/id_rsa
  # Historical public URL prefix that may be embedded in stored URLs;
  # rewritten to computing.nginx_node when set. Empty = no rewriting.
  legacy_url_prefix: ""
  openclaw_user_root: /home/{username}
  openclaw_models_relpath: .openclaw
  # Directory of provider-template JSON files offered in the OpenClaw UI.
  # Empty = built-in templates shipped with the package
  # (src/fastink/service/templates/openclaw).
  openclaw_template_dir: ""
  # Scratch root for opencode/openchamber data directories. opencode keeps
  # its SQLite DB under ~/.local/share/opencode; AFS does not handle SQLite
  # locking well, so the on-AFS data dir is moved (or created fresh) under
  # {root}/.ink/opencode and symlinked back.
  code_datadir_root: /home/{username}
  # Optional per-user root on a large/no-backup filesystem for bulky
  # regenerable data (managed chats, opencode tool-output). Rendered like
  # code_datadir_root and handed to openchamber jobs as INK_SCRATCH_ROOT.
  # Empty = keep everything under the data dir.
  scratch_root: ""
  # Site experiment -> extra bind mounts map for OpenClaw jobs. Empty = none.
  openclaw_exp_bind_map_file: ""
  # Site-provided OpenClaw container image (apptainer .sif). Required for
  # OpenClaw jobs.
  openclaw_container_image: ""
  # Extra provider templates offered in the OpenClaw UI (same shape as the
  # built-in templates; listed first, site keys win on conflict).
  openclaw_provider_templates: {}
  # Origins always allowed for the OpenClaw control UI.
  openclaw_allowed_origins: []
  # Base URL identifying "site-local" models (used by experiment-data
  # policies). Empty = no local models.
  openclaw_local_model_base_url: ""
  openclaw_local_model_prefix: ""

statistic:
  # Slurm server node name used to filter job statistics queries
  # (Elasticsearch "servernode" field). Empty = site statistics disabled.
  slurm_servernode: ""

test:
  # Account used by the /status prober (status_page probes with
  # auth: test_user) and the integration test suite (tests/conftest.py).
  # Empty = auth-dependent probes report "unknown" and token-gated tests
  # skip. Site overlays override these values with their own account.
  username: ""
  password: ""

status_page:
  # Enable the /status uptime page and its background prober (cron container).
  enabled: true
  # Server URL from the prober's (cron container's) point of view.
  target_url: http://fastink-server:8000
  # Points kept per probe (60 s interval x 1440 = 24 h).
  history_points: 1440
  # Home path used by the fs probe ({test_home} placeholder).
  test_home: ""
  # Cluster id used by the cr probe ({cluster} placeholder).
  probe_cluster: slurm
  probes:
    - name: health
      path: /health
      auth: none
    - name: version
      path: /version
      auth: none
    - name: auth_token
      path: /api/v2/auth/get_token?username={test_user}
      # get_token is NOT token-exempt in the base skip_routers (see the
      # security section), so this probe carries the test-user token;
      # without a configured test user it reports unknown, not down.
      auth: test_user
    - name: fs_list
      path: /api/v2/fs/list_path?path={test_home}
      auth: test_user
    - name: cr_query
      path: /api/v2/cr/query_jobs?cluster_id={cluster}
      auth: test_user
    - name: fs_health
      path: /api/v2/status/get_fs_health
      auth: none

filesystem_health:
  # Container paths checked by /api/v2/status/get_fs_health. Each path is
  # verified to exist, be a directory, be readable, and be non-empty.
  # Empty by default (open-source deployments mount nothing site-specific).
  # Site overlays list their bind-mounted data paths here, e.g. the softlink
  # targets in fastink-dev/docker-compose.fs.yml (/home/bes, /besfs2, ...).
  paths: []
