# FastINK Deploy

`fastink-code/deploy/` is the shared deployment layer for FastINK.

It serves two kinds of users:

- open-source users who want to install FastINK directly
- site overlay repositories such as `../fastink-dev/`

This directory is responsible for three things:

- defining the official image build inputs
- providing the shared render core and host-side checks
- providing both interactive and non-interactive deployment entrypoints

## Directory Layout

- `images/`
  Official image definitions. Current images include `init`, `server`, `cron`, `rootbrowse`, and the optional local `htcondor` image.
- `lib/`
  Shared deployment core, including defaults, path planning, host checks, and rendering.
- `templates/`
  Layered templates.
  - `base/`: common templates
  - `profiles/`: `quickstart` and `custom` profile overlays
  - `extras/`: optional capabilities such as `nginx`, `xrootd`, and local `htcondor`
- `fastinkctl.py`
  CLI command dispatcher (root level).
- `cmd/`
  Subcommand implementations: `deploy.py`, `destroy.py`, `down.py`, `status.py`.
- `bin/fastinkctl`
  Entry point bash wrapper (`../fastinkctl.py`).
- `render_profile.py`
  Non-interactive render entrypoint for CI and site overlays.
- `check_host.py`
  Host precheck entrypoint.

## Open-Source Deployment Flow

Run from the `fastink-code/` root directory:

```bash
cd fastink-code && bash deploy/bin/fastinkctl deploy
```

### Quickstart (zero-input)

One command, zero interaction — deploys a complete FastINK stack with xrootd and HTCondor:

```bash
cd fastink-code && bash deploy/bin/fastinkctl deploy --profile quickstart --yes
```

Without `--yes`, it prints a summary and asks for a single confirmation before proceeding.

**Initial admin**: quickstart auto-detects the Linux user running the installer
(`SUDO_USER` when invoked via sudo, else the current user) and seeds it into
the database with `admin` + `ink_special` permissions on first container
start. That user logs in with their normal Linux password (password-auth
deployments). Override with `--set admin_username=<name>`; set it to empty to
seed `root` only (in the interactive custom questionnaire, enter `root` for
the same effect). If no admin is usable (e.g. the host root password is
locked), promote any existing user from inside the server container:
`docker exec <server-container> ink user permission add --username <name> --permission admin`.

Add overrides on top of quickstart defaults:

```bash
cd fastink-code && bash deploy/bin/fastinkctl deploy --profile quickstart \
  --set host_port=9090 \
  --set enable_nginx=true
```

**Re-running quickstart**: generated secrets (DB/Redis passwords, file-share
JWT key) are saved in `.deploy/answers.json` and reused on later runs — the
MariaDB data directory keeps the passwords it was first initialized with (the
entrypoint ignores `MYSQL_*` env vars on a non-empty datadir), so regenerating
them on a re-run would leave the server unable to authenticate to a surviving
database. To force brand-new secrets, run `bash deploy/bin/fastinkctl destroy`
(confirm "Delete DB data") and remove `.deploy/answers.json`, then re-deploy.

**Token enforcement**: quickstart renders `common.security_access: true` —
every API path outside `security.skip_routers` requires valid
`Ink-Username`/`Ink-Token` headers (the login endpoints issue them; frontends
and the CLI attach them automatically). For a token-free local trial, deploy
with `--set security_access=false` (not recommended on network-reachable
hosts).

**All images from one registry**: every container image — including the
third-party MariaDB, Redis and (when enabled) Nginx — defaults to the
`dockerhub.ihep.ac.cn/ink/` registry, so pull mode works out of the box on
hosts without docker.io access (mainland China). **Build-mode base images are
proxy-free too**: the Dockerfiles' base images (`ARG BASE_IMAGE`) default to
the same registry's `ink/base-*` mirrors (AlmaLinux 9, HTCondor base — snapshot
mirrors refreshed manually on the site registry), so `--set image_source=build`
does not need docker.io either. The build itself still installs RPMs from the
repositories configured under `deploy/images/repos/` (e.g. repo.almalinux.org,
EPEL, HTCondor), which must be reachable from the build host. To use different
mirrors, override per image: `--set db_image=mariadb:10.5 --set redis_image=redis:7-alpine`
(restores upstream docker.io references) or point at any other registry. These
`--set` overrides only change the images that are pulled and run; the build-time
bases are overridden at the Docker build itself
(`docker build --build-arg BASE_IMAGE=<reference>`), not through fastinkctl.

### INK Frontend file sharing

FastINK validates public file-download tokens with
`INK_FILE_SHARE_JWT_SECRET`. The value must be at least 32 characters and must
exactly match `INK_FILE_SHARE_JWT_SECRET` in the connected INK Frontend
deployment. Quickstart generates a random value; existing deployments and site
overlays should set the same value explicitly and preserve it across restarts.

`INK_SHARE_LINK_ENCRYPTION_KEY` belongs only to INK Frontend and must not be
copied into FastINK.

### Custom (interactive full configuration)

Walk through every option interactively with pre-filled default values:

```bash
cd fastink-code && bash deploy/bin/fastinkctl deploy --profile custom
```

### Scripted (from answers file)

Load answers from a JSON file, optionally overridden with `--set`:

```bash
cd fastink-code && bash deploy/bin/fastinkctl deploy \
  --answers-file ci-answers.json \
  --set host_name=myhost.example.com
```

### Render-only (CI / inspection)

Generate `.deploy/` files without building images or starting containers:

```bash
cd fastink-code && bash deploy/bin/fastinkctl deploy \
  --answers-file ci-answers.json --render-only
```

### Reuse saved configuration

Re-render from an existing `.deploy/answers.json` and restart services:

```bash
cd fastink-code && bash deploy/bin/fastinkctl deploy --reuse
```

### CLI Reference

```
bash deploy/bin/fastinkctl deploy --help
```

| Flag | Description |
|------|-------------|
| `--profile {quickstart,custom}` | Select profile (skip interactive profile prompt) |
| `--answers-file PATH` | Load answers from JSON file (skip interactive entirely) |
| `--set KEY=VALUE` | Override a single answer (repeatable) |
| `--render-only` | Generate files only, skip build/deploy |
| `--yes` | Skip confirmation prompt (for scripting) |
| `--reuse` | Re-render from `.deploy/answers.json` and restart |

The deployment flow is:

1. Determine answers (profile default, answers file, or interactive questionnaire).
2. Print preparation notes for optional inputs (extra mounts, plugins, TLS certs, etc.).
3. Render `config.yml`, `docker-compose.yml`, `.env`, keys, xrootd, nginx, and condor assets into `.deploy/`.
4. Run `docker build` or `docker pull` for the required images, including `fastink-init`.
5. Run the one-shot `fastink-init` container to generate SSH keys, TLS certificates, and `sss.keytab`.
6. Run `docker compose up -d` and wait for the health check.

## Preparation Notes Before Starting The CLI

Before running `bash deploy/bin/fastinkctl deploy`, users may want to prepare some optional inputs in advance.

Typical optional inputs include:

- an extra mount list file
- plugin source or plugin packages
- preload scripts
- an existing TLS certificate and private key
- a Kerberos keytab for xrootd if Kerberos-backed xrootd is required

The installer prints these preparation notes at startup so that users know what can be prepared ahead of time.

If `.deploy/answers.json` already exists, the interactive questionnaire also accepts `r` on prompts to reuse the saved value for that field.

## Host Commands Required

The interactive installer now keeps most one-shot asset generation inside the `fastink-init` container. The host only needs commands for host-side checks and Docker control:

Required on the host:

- `python3`
- `docker`
- `docker compose`
- `mountpoint`

Also required as host infrastructure rather than commands:

- `/cvmfs` must already be mounted
- the required `/cvmfs` paths must be readable on the host

The CVMFS check is **warn-only** during `fastinkctl deploy` — a missing
`/cvmfs` or missing paths produce a warning, not a failure.  Jobs that
need CVMFS-hosted software (Jupyter, ROOT, VNC) will not work without it,
but file browsing and other non-CVMFS features remain available.

To customize which CVMFS paths are checked, set the `cvmfs_required_paths`
answer (a list of absolute paths).  The standalone `deploy/check_host.py`
entry point fails hard on missing CVMFS unless `--skip-cvmfs` is passed.

The host no longer needs these commands just to complete initialization:

- `ssh-keygen`
- `openssl`
- `xrdsssadmin`

Those are now executed inside the one-shot `fastink-init` container.

## Optional Extra Mount List

The interactive installer now supports an optional extra mount list file.

By default, the installer creates `.deploy/extra-mounts.txt` if it does not already exist. Its initial content is:

```text
/home/:/home/
```

If enabled during the questionnaire, the user provides a file path. Each non-empty, non-comment line in that file is treated as a Docker volume rule.

Supported line format:

```text
/host/path:/container/path
/host/path:/container/path:ro
```

Lines starting with `#` are ignored.

A typical suggested location is:

```text
.deploy/extra-mounts.txt
```

Current behavior:

- the same extra mounts are applied to:
  - `fastink-server`
  - `fastink-redis-cron`
  - `fastink-rootbrowse`
  - `fastink-xrootd`
  - `fastink-htcondor` when local HTCondor is enabled
- this is intended for generic runtime filesystem mounts that should be visible in the main service containers

This mechanism is intentionally simpler than asking users to edit raw compose YAML during the interactive flow.

## Profiles And Service Layers

### `quickstart` — zero-input, batteries-included

Intended for first-time users and local testing. One command deploys a fully working FastINK.

Services:

- `fastink-db` (MariaDB)
- `fastink-redis`
- `fastink-server` (FastAPI backend)
- `fastink-redis-cron` (scheduled tasks)
- `fastink-rootbrowse` (ROOT file browser)
- `fastink-xrootd` (storage backend)
- `fastink-htcondor` (job scheduler, AIO pool)
- `fastink-accounts-sync` (keeps the container account view in sync with the host)

All defaults are auto-applied: official images via `pull`, passwords auto-generated, `extra-mounts.txt` auto-created with `/home/:/home/`. No interactive questions asked (single confirmation unless `--yes`).

### `custom` — interactive full configuration

Every option is exposed as an interactive prompt with a pre-filled default. The user decides which components to enable and what values to use.

Profile chain: `custom` inherits from `quickstart` overlays, then applies its own.

### Optional service layers (available in `custom` profile or via `--set`)

- `enable_nginx`
  - adds `fastink-nginx`
  - provides an HTTPS entrypoint in front of `fastink-server`
- `enable_xrootd`
  - adds `fastink-xrootd`
- `enable_local_htcondor`
  - adds `fastink-htcondor`
  - starts a single-container HTCondor all-in-one pool for local/open-source testing
  - automatically points `schedd_host` to `schedd@fastink-htcondor` and `cm_host` to `fastink-htcondor`
- `enable_krb5`
  - mounts host `krb5.conf` into containers
- `enable_host_slurm_client`
  - mounts host Slurm config and munge socket into containers
- `enable_accounts_sync`
  - adds `fastink-accounts-sync`, which polls the host account database and
    rebuilds the shared `/etc-init` view whenever `/etc/passwd|group|shadow|gshadow`
    change (interval: `accounts_sync_interval`, default 60s)
  - enabled by default in `quickstart`, disabled by default in `custom`
  - the host is the source of truth: entries that exist only in `etc-init`
    are removed on the next sync; fixed service ids (sshd/xrootd/condor/slurm)
    are re-applied on every rebuild

## Local HTCondor

When `enable_local_htcondor` is enabled, deploy starts one `fastink-htcondor`
container that combines:

- collector
- negotiator
- schedd
- execute/startd

Integration rules:

- it uses the same shared `/etc-init` account view as `server`, `rootbrowse`, and `xrootd`
- it receives the same extra mount list entries as the main runtime containers
- if no extra mount list is specified, deploy falls back to `.deploy/extra-mounts.txt`, whose default content is `/home/:/home/`
- the installer asks once for `HTCondor internal domain`
- deploy writes that value into both:
  - `.deploy/condor/ink.conf` for the FastINK client containers
  - `.deploy/condor/htcondor.local.conf` for the local HTCondor AIO container
- this enables shared-filesystem behavior through matching `FILESYSTEM_DOMAIN` and `UID_DOMAIN`
- the installer prompts once for the default HTCondor job CPU and memory values
- those defaults are reused for `vscode`, `jupyter`, `vnc`, and `rootbrowse`
- the default values are `1` CPU and `6000` MB
- it is intended for local/open-source testing, not for a multi-node Condor cluster deployment

### Shared Filesystem Semantics

For local HTCondor AIO, FastINK assumes that the submit side and execute side
share the same mounted job directory tree.

Current generic deploy behavior is:

- `fastink-server` and `fastink-htcondor` receive the same extra mount list
- if local HTCondor or xrootd is enabled and no mount list is provided, deploy
  auto-enables `.deploy/extra-mounts.txt`
- the default initial content is:

```text
/home/:/home/
```

- the internal domain value provided during install is used as:
  - `FILESYSTEM_DOMAIN`
  - `UID_DOMAIN`

This is important for HTCondor job execution. Without matching shared-fs domain
settings, HTCondor treats the execute side as outside the submit filesystem
domain and falls back to execute sandboxes such as `/var/lib/condor/execute/...`.

### CVMFS-backed Job Types In Local HTCondor

The local HTCondor AIO container now mounts:

```text
/cvmfs:/cvmfs:ro
```

This is enough for job types whose runtime is already hosted in CVMFS.

Current local validation status:

- `jupyter`
  - validated in local HTCondor AIO
  - launches from CVMFS-backed Jupyter environments
- `rootbrowse`
  - validated in local HTCondor AIO
  - launches ROOT/browser runtime from CVMFS-backed software stacks
- `vscode`
  - still requires `/usr/bin/code-server` inside the execute image
  - not solved by the `/cvmfs` mount alone

So, at the moment:

- CVMFS-backed job types can be validated with local HTCondor AIO
- image-backed job types still depend on the software baked into the execute image

## What `.deploy/` Contains

`.deploy/` is the persistent deployment asset for open-source users. It should usually be kept together with the deployment so that it can be copied to another machine later.

Typical contents include:

- `config.yml`
- `docker-compose.yml`
- `.env`
- `answers.json`
- `runtime/`
- `keys/`
- `plugins/`
- `preload/`
- `xrootd/`
- `nginx/`
- `extra-mounts.txt` (optional)

For open-source deployments, `.deploy/` is restricted to mode `0700`.
Generated files containing credentials or expanded secret values use mode
`0600`, including `answers.json`, `.env`, `config.yml`, and
`docker-compose.yml`.

The files that users should normally maintain directly are mainly:

- `answers.json`
- `plugins/`
- `preload/`
- runtime materials under `xrootd/` and `nginx/`
- `extra-mounts.txt` when extra host paths need to be mounted into the runtime containers

Generated outputs such as `config.yml` and `docker-compose.yml` are better regenerated than edited long-term by hand.

## Destroy And Data Retention

`fastinkctl destroy` separates durable DB state from disposable runtime state:

- `Delete DB data?` controls whether the configured DB data directory is removed.
- the default answer is `No`
- Redis data is treated as disposable and removed during destroy
- when DB data is retained, `answers.json` is also retained so the DB credentials
  and deployment settings needed for recovery remain available
- if `docker compose down` fails, destroy stops before removing images or files
- `fastinkctl destroy --yes` performs full cleanup, including DB data

`--keep-dot-deploy` keeps the generated deployment files, but does not override
the DB-data choice or the cleanup of disposable Redis state.

## Runtime Materials The User Must Handle

### 1. SSH Client Key

Deploy automatically generates:

- `.deploy/keys/ssh-client/id_rsa`
- `.deploy/keys/ssh-client/id_rsa.pub`

This key pair is used by `fastink-server` when accessing:

- `rootbrowse`
- condor nodes
- Slurm nodes
- login nodes

`rootbrowse` host keys are generated by the container itself.

Users must deploy `.deploy/keys/ssh-client/id_rsa.pub` to the appropriate remote account `authorized_keys`.

### 2. nginx TLS Certificate

If `nginx` is enabled, deploy maintains these files under `.deploy/nginx/`:

- `default.conf`
- `cert.pem`
- `key.pem`

Rules:

- if the user provides an existing certificate and key, deploy copies them into `.deploy/nginx/`
- otherwise deploy generates a self-signed certificate so nginx can still provide HTTPS encryption

### 3. xrootd Keytabs

If Kerberos is enabled, deploy mounts the host `krb5.conf` into the relevant containers. The interactive installer asks for the host path and defaults to `/etc/krb5.conf`.

If `xrootd` is enabled, deploy prepares:

- `.deploy/xrootd/sss.keytab`
- `.deploy/xrootd/krb5.keytab`

Rules:

- `sss.keytab`
  - is generated by the one-shot `fastink-init` container
  - does not require `xrdsssadmin` on the host
- `krb5.keytab`
  - is never auto-generated
  - for Kerberos-enabled xrootd, the installer asks for a host source path and mounts that file into `/etc/xrootd/krb5.keytab`
  - the installer also asks for the xrootd service principal used by the xrootd krb5 config

### 4. Slurm Host Environment

If FastINK needs Slurm access, the host must already provide a working Slurm client environment, including at least:

- `sbatch`
- `sacct`
- `scontrol`
- `scancel`
- a usable `munge` socket
- a valid `slurm.conf`

The official images provide the container-side client packages. Host-side Slurm configuration and authentication are still the user's responsibility.

If you enable `Expose host Slurm client config and munge socket` in the interactive installer, deploy will mount the host:

- `slurm.conf`
- `/var/run/munge`

into both `fastink-server` and `fastink-redis-cron`, so Slurm-backed runtime and cron jobs can use the same client state.

## Kerberos Token Validation

When the Kerberos authentication backend is enabled, FastINK validates the
presented ccache with the KDC. It first sanitizes the ccache to a TGT-only
ccache, then runs `kvno` for the configured service principal. The target
service is never contacted. A ccache that is only structurally valid, including
a forged ccache, is rejected because the KDC did not issue the requested
service ticket. The returned ccache is checked for exactly one unexpired ticket
for that service principal.

### Required Configuration

Set these values in the final `auth.krb5_validate` configuration when using
Kerberos token authentication:

```yaml
auth:
  krb5_validate:
    service_principal: "service/host@REALM"
    realm: "REALM"
    cache_ttl: 600
    negative_backoff: 5
    timeout: 5
    max_concurrency: 8
```

`realm` must be a non-empty local realm, and `service_principal` must be a
non-empty service principal in that same realm. `krbtgt` targets are forbidden.
Empty values are a fail-closed configuration error. All Kerberos token
authentication fails until both values are set correctly. The principal and
realm are validated strictly, including their separators, whitespace, control
characters, and numeric settings.

### Provisioning the Target Principal

Validation never decrypts the issued ticket, so FastINK does **not** need the
target's keytab — the proof is that the KDC agreed to issue a ticket at all.
Any existing, issuable, non-`krbtgt` principal in the realm works (the target
service is never contacted). If you prefer a dedicated principal, create one
with a random key and nothing to distribute:

```
kadmin.local -q "addprinc -randkey fastink-validate/<server-host>@<REALM>"
```

No keytab export, no service to run, nothing to rotate.

Runtime validation also requires `/usr/bin/kvno` from `krb5-workstation` in the
server image, a readable `/etc/krb5.conf`, network reachability to the KDC,
and synchronized clocks for Kerberos ticket timestamp checks.

The defaults are `cache_ttl: 600` seconds, `negative_backoff: 5` seconds,
`timeout: 5` seconds, and `max_concurrency: 8`. `timeout` applies to the
`kvno` subprocess. Waiting for a semaphore slot is also bounded by the same
timeout, so a request can take about two times the configured timeout in the
worst case.

Each worker process has its own semaphore. The aggregate limit is therefore
`workers * max_concurrency`. Changing `max_concurrency` requires a worker
restart. The process rejects a changed value rather than replacing the live
semaphore.

### Validation Cache

Positive results are keyed by the validation context and the digest of the
spliced TGT-only ccache in
`krb5val:v1:<context-fingerprint>:<sha256(spliced)>`. The spliced form
collapses differences in stripped records only; retained TGT records are kept
verbatim. Each entry stores `{principal, valid_until}`, where
`valid_until = min(validation_started_at + cache_ttl, service ticket endtime)`.
Ordinary KDC rejections use the matching negative-backoff namespace
`krb5valfail:v1:<context-fingerprint>:<sha256(spliced)>`, controlled by
`negative_backoff`. Raw-token variants that differ only in stripped records
share the same cache entries.

The validation namespaces are an authentication authority: write access to
`krb5val:v1:*` allows endorsing a forged token without any KDC-issued
credential. Restrict Redis write access exactly as you restrict the database
that stores user ccaches. The cache does not rate-limit KDC-bound work;
token-validation endpoints should additionally be rate-limited at the ingress
gateway.

`kvno` classification checks infrastructure patterns before KDC rejection
patterns, so infrastructure evidence takes precedence in mixed failures.
Negative backoff is written only when `kvno` provides affirmative credential-
rejection evidence. Ambiguous, mixed, and unknown failures default to no
backoff. Connectivity and configuration failures, signal exits, unrecognized
or empty failures, identity/configuration errors, and timeouts do not create a
negative-backoff key. The 64 KiB limit bounds the stderr classification window;
it does not limit stderr capture. A Redis failure fails closed rather than
allowing validation to continue without the cache.

Changing the realm or service target changes the context fingerprint and
self-invalidates old entries. For policy changes that alter cache semantics,
such as TTL interpretation, use this sequence:

1. Drain the old workers.
2. Delete both validation namespaces, `krb5val:v1:*` and
   `krb5valfail:v1:*`.
3. Start workers with the new policy.

A validated token remains accepted until its cache entry expires. The
revocation window is bounded by
`min(validation_started_at + cache_ttl, service ticket endtime)`.

Offline unit tests always run. Live-KDC integration tests are guarded in the
same style as `KRB5_AVAILABLE` and run only when a KDC and test credential have
been provisioned.

GSSAPI loopback is not part of this change and remains deferred.

## Official Images And yum Repositories

Open-source users should normally use the official images.

The official image build inputs are defined under `deploy/images/`. Current image builds use IHEP-managed yum repositories where needed, including the IHEP mirror for `slurm`. The official image set now also includes `fastink-init`, which is used only for one-shot deployment initialization.

If a user wants to rebuild images locally or install extra RPMs, they should review and adapt:

- `deploy/images/repos/`

## Non-Interactive Render Flow

`render_profile.py` is the non-interactive entrypoint used by CI and site overlays. It is not the main long-term interface for open-source users.

Typical usage:

```bash
python deploy/render_profile.py \
  --profile custom \
  --answers-file /path/to/render.answers.json \
  --output-dir /path/to/output \
  --config-overlay /path/to/site-config.yml
```

Its responsibilities are:

1. read an answers file
2. apply `--set` overrides
3. plan runtime directories
4. render base templates, profile overlays, and extras
5. output final `config.yml`, `docker-compose.yml`, and `.env`

`fastink-dev` consumes `deploy` through this path.

## Where To Change Things

By change type:

- public service topology and common compose structure: `templates/base/`
- `quickstart` / `custom` semantics: `templates/profiles/`
- optional capabilities such as `nginx` or `xrootd`: `templates/extras/`
- default values and profile defaults: `lib/defaults.py`
- runtime directory planning: `lib/paths.py`
- host prechecks and `/cvmfs` warmup: `lib/host_runtime.py`
- render and merge behavior: `lib/render.py`
- official image contents: `images/`
- interactive install UX: `cmd/deploy.py`
- CI / site render entrypoint: `render_profile.py`

The rule is simple:

- generic deployment semantics stay in `fastink-code/deploy`
- site-specific differences should stay in overlay repositories such as `fastink-dev`
