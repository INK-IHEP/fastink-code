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
cd fastink-code && PYTHONPATH=. python3 deploy/bin/fastinkctl deploy
```

### Quickstart (zero-input)

One command, zero interaction — deploys a complete FastINK stack with xrootd and HTCondor:

```bash
cd fastink-code && PYTHONPATH=. python3 deploy/bin/fastinkctl deploy --profile quickstart --yes
```

Without `--yes`, it prints a summary and asks for a single confirmation before proceeding.

Add overrides on top of quickstart defaults:

```bash
cd fastink-code && PYTHONPATH=. python3 deploy/bin/fastinkctl deploy --profile quickstart \
  --set host_port=9090 \
  --set enable_nginx=true
```

### Custom (interactive full configuration)

Walk through every option interactively with pre-filled default values:

```bash
cd fastink-code && PYTHONPATH=. python3 deploy/bin/fastinkctl deploy --profile custom
```

### Scripted (from answers file)

Load answers from a JSON file, optionally overridden with `--set`:

```bash
cd fastink-code && PYTHONPATH=. python3 deploy/bin/fastinkctl deploy \
  --answers-file ci-answers.json \
  --set host_name=myhost.example.com
```

### Render-only (CI / inspection)

Generate `.deploy/` files without building images or starting containers:

```bash
cd fastink-code && PYTHONPATH=. python3 deploy/bin/fastinkctl deploy \
  --answers-file ci-answers.json --render-only
```

### Reuse saved configuration

Re-render from an existing `.deploy/answers.json` and restart services:

```bash
cd fastink-code && PYTHONPATH=. python3 deploy/bin/fastinkctl deploy --reuse
```

### CLI Reference

```
python deploy/bin/fastinkctl deploy --help
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

Before running `python deploy/bin/fastinkctl deploy`, users may want to prepare some optional inputs in advance.

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

The files that users should normally maintain directly are mainly:

- `answers.json`
- `plugins/`
- `preload/`
- runtime materials under `xrootd/` and `nginx/`
- `extra-mounts.txt` when extra host paths need to be mounted into the runtime containers

Generated outputs such as `config.yml` and `docker-compose.yml` are better regenerated than edited long-term by hand.

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
