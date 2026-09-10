"""API availability probing for the /status uptime page.

Runs inside the cron container (see deploy/images/cron/cron.yaml) on a
fixed interval. Each cycle probes the endpoints configured under
``status_page.probes``, judging success by HTTP 200 plus a successful
FastINK three-part response status, and appends one point per probe to
a capped redis list consumed by the /status page.

Authenticated probes use the ``test`` user from config. The token is
cached and only re-created when validation stops succeeding, so krb5
deployments do not re-kinit every cycle. When no token can be obtained
the probe records ``ok: null`` ("unknown") rather than a failure, so
prober credential problems are not displayed as service downtime.
Error details are logged only — never stored or exposed.
"""

import json
import time

import aiohttp

from fastink.common.config import get_config
from fastink.common.logger import logger
from fastink.inkdb.inkredis import redis_connect

REDIS_KEY_PREFIX = "status_probe:"
META_KEY = "status_probe:meta"

# In-process token cache (module lives in the long-running cron process).
_token_cache: dict = {"token": None, "obtained_at": 0}

# Business statuses accepted as "up": FastINK three-part success codes
# plus the literal "ok" returned by the /health endpoint.
_SUCCESS_STATUSES = {"200", "SUCCESS", "ok"}


def _get_probe_list() -> list[dict]:
    probes = get_config("status_page", "probes", fallback=[]) or []
    return [p for p in probes if isinstance(p, dict) and p.get("name") and p.get("path")]


def _expand_path(path: str) -> str:
    """Expand {test_user}, {test_home} and {cluster} placeholders."""
    test_user = get_config("test", "username", fallback="")
    test_home = get_config("status_page", "test_home", fallback="") or f"/home/{test_user}"
    probe_cluster = get_config("status_page", "probe_cluster", fallback="") or "slurm"
    replacements = {
        "{test_user}": test_user,
        "{test_home}": test_home,
        "{cluster}": probe_cluster,
    }
    for placeholder, value in replacements.items():
        path = path.replace(placeholder, str(value))
    return path


async def _get_test_token(session: aiohttp.ClientSession, base_url: str) -> str | None:
    """Return a cached test-user token, creating one only when needed."""
    username = get_config("test", "username", fallback="")
    password = get_config("test", "password", fallback="")
    if not username or not password:
        logger.debug("status_probe: no test user configured; auth probes will be unknown")
        return None

    if _token_cache["token"]:
        return _token_cache["token"]

    try:
        async with session.post(
            f"{base_url}/api/v2/auth/create_and_get_token",
            json={"username": username, "password": password},
            timeout=aiohttp.ClientTimeout(total=20),
        ) as resp:
            data = await resp.json()
        if str(data.get("status")) in _SUCCESS_STATUSES:
            token = (data.get("data") or {}).get("token")
            if token:
                _token_cache["token"] = token
                _token_cache["obtained_at"] = time.time()
                logger.info("status_probe: obtained test-user token")
                return token
        logger.warning("status_probe: token creation failed: %s", data.get("msg"))
    except Exception as exc:
        logger.warning("status_probe: token creation error: %s", exc)
    return None


def _invalidate_token() -> None:
    _token_cache["token"] = None


async def _run_single_probe(
    session: aiohttp.ClientSession,
    base_url: str,
    probe: dict,
    token: str | None,
) -> dict:
    """Execute one probe. Returns {"t": ..., "ok": bool|None, "ms": int}."""
    name = probe["name"]
    needs_auth = probe.get("auth") == "test_user"
    username = get_config("test", "username", fallback="")

    if needs_auth and not token:
        # Prober has no credentials — unknown, not a service failure.
        return {"t": int(time.time()), "ok": None, "ms": 0}

    headers = {}
    if needs_auth:
        headers = {"Ink-Username": username, "Ink-Token": token}

    url = f"{base_url}{_expand_path(probe['path'])}"
    started = time.monotonic()
    try:
        async with session.get(
            url, headers=headers, timeout=aiohttp.ClientTimeout(total=int(probe.get("timeout", 30)))
        ) as resp:
            elapsed_ms = int((time.monotonic() - started) * 1000)
            if resp.status != 200:
                logger.warning("status_probe[%s]: HTTP %s", name, resp.status)
                return {"t": int(time.time()), "ok": False, "ms": elapsed_ms}
            body = await resp.json()
    except Exception as exc:
        elapsed_ms = int((time.monotonic() - started) * 1000)
        logger.warning("status_probe[%s]: request error: %s", name, exc)
        return {"t": int(time.time()), "ok": False, "ms": elapsed_ms}

    status = str(body.get("status", ""))
    if needs_auth and status in {"A01", "A02"}:
        # Token rejected — invalidate cache; report unknown this cycle
        # (next cycle re-authenticates). Not a service failure.
        logger.warning("status_probe[%s]: token rejected (%s), invalidating cache", name, status)
        _invalidate_token()
        return {"t": int(time.time()), "ok": None, "ms": elapsed_ms}

    ok = judge_response(resp.status, body)
    if not ok:
        logger.warning("status_probe[%s]: business status %s: %s", name, status, body.get("msg"))
    return {"t": int(time.time()), "ok": ok, "ms": elapsed_ms}


def judge_response(http_status: int, body: dict) -> bool:
    """A probe succeeds when HTTP is 200 and the three-part response
    carries a success status. Endpoints without the three-part format
    (e.g. /health) succeed when they return valid JSON with HTTP 200."""
    if http_status != 200:
        return False
    if not isinstance(body, dict):
        return False
    if "status" in body:
        return str(body["status"]) in _SUCCESS_STATUSES
    # /health-style responses without the three-part envelope
    return True


async def run_probes() -> None:
    """One probe cycle. Called periodically by the cron runner."""
    if not get_config("status_page", "enabled", fallback=False):
        return

    base_url = get_config("status_page", "target_url", fallback="http://fastink-server:8000").rstrip("/")
    history_points = max(1, int(get_config("status_page", "history_points", fallback=1440)))
    probes = _get_probe_list()
    if not probes:
        logger.debug("status_probe: no probes configured")
        return

    async with aiohttp.ClientSession() as session:
        token = None
        if any(p.get("auth") == "test_user" for p in probes):
            token = await _get_test_token(session, base_url)

        results = {}
        for probe in probes:
            results[probe["name"]] = await _run_single_probe(session, base_url, probe, token)

    r = redis_connect()
    try:
        for name, point in results.items():
            key = f"{REDIS_KEY_PREFIX}{name}"
            await r.lpush(key, json.dumps(point))
            await r.ltrim(key, 0, history_points - 1)
        await r.hset(META_KEY, mapping={
            "last_run": str(int(time.time())),
            "probes": json.dumps([
                {"name": p["name"], "auth": p.get("auth", "none")} for p in probes
            ]),
        })
    finally:
        await r.aclose()

    up = sum(1 for v in results.values() if v["ok"] is True)
    logger.info("status_probe: cycle done, %d/%d up", up, len(results))
