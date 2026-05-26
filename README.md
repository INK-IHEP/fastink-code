# INK

Interactive aNalysis worKbench.

## Repository roles

- `src/fastink/`
  - FastINK backend source code
- `deploy/`
  - clean publishable container images
  - shared deployment templates
  - interactive deploy CLI
  - non-interactive render entrypoint for CI and site overlays

`fastink-dev` is now a separate IHEP overlay repository which consumes the
images and render logic from this repository.

## Local backend development

```bash
pip install -e .
python -m uvicorn fastink.main:app --reload --host 0.0.0.0 --port 8001 --log-config src/fastink/misc/uvicorn_log_config.yaml
```

### VNC OTP prerequisites

For VNC connect, FastINK backend calls `generateOTP.sh` through SSH from the runtime environment.
If FastINK runs inside a container, SSH trust must be configured inside that container (not only on the host).

Required checks:

- Container can SSH to compute host as `root` (or your configured SSH user).
- Container-side key files are present and readable.
- Remote host allows `sudo -iu <username>` for OTP generation.

Optional debug switch:

- Set environment variable `INK_VNC_SSH_SELF_CHECK=true` to print SSH self-check diagnostics in backend logs.

## Deployment

### Public / generic deployment

Use the interactive deploy CLI:

```bash
python deploy/bin/fastinkctl deploy
```

This writes a durable `.deploy/` directory containing rendered compose, config,
keys, plugin mount points, and preload directories.

Generic deploy now also supports:

- optional local HTCondor all-in-one deployment for open-source testing
- optional local xrootd service
- shared-filesystem HTCondor semantics through an interactive internal-domain input
- local CVMFS-backed validation of `jupyter` and `rootbrowse`

### Non-interactive render

For CI or site overlays:

```bash
python deploy/render_profile.py \
  --profile full \
  --answers-file /path/to/answers.json \
  --output-dir /path/to/output
```

### Official image publishing

- `main` branch builds local `dev-local` images on the shared runner and triggers downstream dev deployment
- release tags build and publish official images, then trigger downstream production deployment

See:

- [deploy/README.md](/root/dev/fastink/ink-code/deploy/README.md)
- [deploy/PUBLISHING.md](/root/dev/fastink/ink-code/deploy/PUBLISHING.md)
