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

### VNC OTP flow

Every "connect" click for a VNC-family job (`vnc` / `asic` / `asicbm` /
`ink_special`) needs a fresh one-time password from TurboVNC.  FastINK
mints these via a **filesystem RPC** on the shared xrootd namespace:

1. The job's ``run.sh`` calls ``otp_start_listener`` (defined in
   ``src/fastink/computing/apps/shell.sh``) right after ``vncserver``
   comes up. That backgrounds a tiny watcher inside the job's own
   process tree and touches ``<job_dir>/otp/.ready``.
2. On each connect request the FastINK server (this backend)
   ``fastink.computing.apps._helpers.generate_userotp`` writes an
   empty ``<job_dir>/otp/req_<uuid>`` file.
3. The watcher notices the request, runs ``vncpasswd -o -display :N``
   as the user, and writes ``<job_dir>/otp/resp_<uuid>`` (or
   ``resp_<uuid>.err``) with the OTP.
4. The server reads the response and returns the ``vnc.html?password=…``
   URL to the frontend.

No SSH, no container-side ssh key, no site-provided OTP script.  The
mint always runs as the job's owner inside the running VNC session,
so its output is authoritative.

Failure modes surface concrete reasons:

- ``VNC OTP listener not ready`` — the job hasn't reached the
  ``otp_start_listener`` step yet.
- ``OTP mint failed on worker ...`` — ``vncpasswd -o`` exited non-zero
  (e.g. vncserver crashed); the worker's stderr is included.
- ``Timeout waiting for OTP ...`` — the response never landed within
  the default 8-second budget.

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
