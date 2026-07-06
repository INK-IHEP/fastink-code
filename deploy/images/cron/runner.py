#!/usr/bin/env python3
"""FastINK cron runner — loads YAML config, starts jobs, handles signals.

Replaces the 252-line bash entrypoint with a testable Python runner.
Supports two job types:
  - module+function: importlib + asyncio.run (no wrapper script needed)
  - script: subprocess for standalone .py files (IHEP-specific jobs)

Supports config overlay: base cron.yaml + site cron.overlay.yml,
merged by job name (overlay overrides matching fields, appends new jobs).
"""

from __future__ import annotations

import asyncio
import importlib
import os
import signal
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any

import yaml


BASE_DIR = Path(os.environ.get("FASTINK_CRON_BASE_DIR", "/opt/fastink-cron"))
CONFIG = Path(os.environ.get("FASTINK_CRON_CONFIG", str(BASE_DIR / "cron.yaml")))
CONFIG_OVERLAY = Path(os.environ.get("FASTINK_CRON_CONFIG_OVERLAY", ""))
LOG_DIR = Path(os.environ.get("FASTINK_CRON_LOG_DIR", "/var/log/fastink-cron"))
INK_CODE_DIR = Path(os.environ.get("INK_CODE_DIR", "/ink"))


def _log(msg: str) -> None:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    log_file = LOG_DIR / f"cron-{datetime.now().strftime('%F')}.log"
    with open(log_file, "a", encoding="utf-8") as f:
        f.write(f"{datetime.now().strftime('%F %T')} {msg}\n")


def load_config(path: Path) -> dict:
    if not path or not path.is_file():
        return {}
    data = yaml.safe_load(path.read_text(encoding="utf-8"))
    return data or {}


def merge_jobs(base: dict, overlay: dict) -> list[dict]:
    """Merge job lists by name. Overlay overrides matching jobs, appends new ones."""
    by_name: dict[str, dict] = {}
    order: list[str] = []

    for job in base.get("jobs", []):
        by_name[job["name"]] = dict(job)
        order.append(job["name"])

    for job in overlay.get("jobs", []):
        name = job["name"]
        if name in by_name:
            merged = dict(by_name[name])
            merged.update(job)
            # module/function and script are mutually exclusive
            if "script" in job and "module" in merged:
                merged.pop("module", None)
                merged.pop("function", None)
                merged.pop("args", None)
            if "module" in job and "script" in merged:
                merged.pop("script", None)
            by_name[name] = merged
        else:
            by_name[name] = dict(job)
            order.append(name)

    return [by_name[name] for name in order]


def resolve_script(script_name: str, base_dir: Path) -> Path | None:
    # Reject absolute paths to prevent traversal outside allowed dirs
    if Path(script_name).is_absolute():
        return None
    for candidate in [
        base_dir / "overlay" / script_name,
        base_dir / "jobs" / script_name,
        base_dir / script_name,
    ]:
        if candidate.is_file():
            return candidate
    return None


def install_plugins() -> None:
    packages = os.environ.get("PLUGIN_PIP_PACKAGES", "")
    if packages.strip():
        for pkg in packages.split(","):
            pkg = pkg.strip()
            if pkg:
                _log(f"Installing plugin package: {pkg}")
                subprocess.run(
                    [sys.executable, "-m", "pip", "install", "--no-cache-dir", pkg],
                    check=True, capture_output=True,
                )

    dirs = os.environ.get("PLUGIN_EDITABLE_DIRS", "")
    if dirs.strip():
        for d in dirs.split(","):
            d = d.strip()
            if d:
                _log(f"Installing editable plugin from: {d}")
                subprocess.run(
                    [sys.executable, "-m", "pip", "install", "-e", d],
                    check=True, capture_output=True,
                )


def run_preload_scripts() -> None:
    dirs = os.environ.get("PRELOAD_SCRIPT_DIRS", "")
    scripts = os.environ.get("PRELOAD_SCRIPTS", "")

    if dirs.strip():
        for d in dirs.split(","):
            d = d.strip()
            if not d:
                continue
            dpath = Path(d)
            if not dpath.is_dir():
                _log(f"ERROR: preload script directory not found: {d}")
                sys.exit(1)
            for script in sorted(dpath.iterdir()):
                if script.is_file() and not script.name.startswith("."):
                    _run_preload_script(str(script))

    if scripts.strip():
        for script in scripts.split(","):
            script = script.strip()
            if script:
                _run_preload_script(script)


def _run_preload_script(script: str) -> None:
    if not Path(script).is_file():
        _log(f"ERROR: preload script not found: {script}")
        sys.exit(1)
    _log(f"Running preload script: {script}")
    subprocess.run(["bash", script], check=True)


def install_fastink_editable() -> None:
    if os.environ.get("INSTALL_EDITABLE", "false").lower() == "true":
        _log(f"Installing fastink in editable mode from {INK_CODE_DIR}")
        if INK_CODE_DIR.is_dir():
            subprocess.run(
                [sys.executable, "-m", "pip", "install", "-e", str(INK_CODE_DIR)],
                check=True, capture_output=True,
            )
        else:
            _log(f"ERROR: INK_CODE_DIR not found: {INK_CODE_DIR}")
            sys.exit(1)


async def _run_module_function(job: dict) -> None:
    module = importlib.import_module(job["module"])
    func = getattr(module, job["function"])
    args = job.get("args", [])
    if asyncio.iscoroutinefunction(func):
        await func(*args)
    else:
        func(*args)


async def _run_script(job: dict, base_dir: Path) -> None:
    script_path = resolve_script(job["script"], base_dir)
    if script_path is None:
        _log(f"WARN: script {job['script']} not found, skip job {job['name']}")
        return
    proc = await asyncio.create_subprocess_exec(
        sys.executable, str(script_path),
        stdout=subprocess.PIPE, stderr=subprocess.PIPE,
    )
    stdout, stderr = await proc.communicate()
    if proc.returncode != 0:
        _log(f"[{job['name']}] failed with code={proc.returncode}")
        if stderr:
            _log(f"[{job['name']}] stderr: {stderr.decode()[:500]}")


async def run_job(job: dict, base_dir: Path) -> None:
    name = job.get("name", "unknown")
    interval = int(job.get("interval", 60))
    mode = job.get("mode", "fixed")

    _log(f"Starting job: {name}, interval={interval}s, mode={mode}")

    while True:
        _log(f"[{name}] run start")
        start = time.monotonic()

        try:
            if "module" in job:
                if mode == "fixed":
                    await asyncio.wait_for(_run_module_function(job), timeout=interval)
                else:
                    await _run_module_function(job)
            elif "script" in job:
                if mode == "fixed":
                    await asyncio.wait_for(_run_script(job, base_dir), timeout=interval)
                else:
                    await _run_script(job, base_dir)
        except asyncio.TimeoutError:
            _log(f"[{name}] killed by timeout ({interval}s)")
        except Exception as exc:
            _log(f"[{name}] error: {exc}")

        elapsed = time.monotonic() - start
        if mode == "fixed":
            sleep_time = max(0, interval - elapsed)
        else:
            sleep_time = interval

        _log(f"[{name}] sleep {sleep_time:.0f}s")
        await asyncio.sleep(sleep_time)


def cleanup(signum: int, frame: Any) -> None:
    _log("Stopping container, killing all jobs...")
    sys.exit(0)


async def main() -> None:
    _log("Container started")

    install_fastink_editable()
    install_plugins()
    run_preload_scripts()

    base_config = load_config(CONFIG)
    overlay_config = load_config(CONFIG_OVERLAY)
    jobs = merge_jobs(base_config, overlay_config)

    if not jobs:
        _log(f"ERROR: no jobs found in {CONFIG}")
        _log("Container idle")
        while True:
            await asyncio.sleep(3600)

    _log(f"Loaded {len(jobs)} jobs from {CONFIG}" +
         (f" + {CONFIG_OVERLAY}" if overlay_config else ""))

    tasks = [asyncio.create_task(run_job(job, BASE_DIR)) for job in jobs]
    await asyncio.gather(*tasks)


if __name__ == "__main__":
    signal.signal(signal.SIGTERM, cleanup)
    signal.signal(signal.SIGINT, cleanup)
    asyncio.run(main())
