"""Public status page: /status (HTML) and /api/v2/status/get_status (JSON).

Both endpoints are unauthenticated (added to security.skip_routers,
same convention as /health). They only expose probe names, up/down/unknown
state, latency and timestamps — never error details or internal hosts.

Probe data is produced by fastink.service.status_probe running in the
cron container and stored in capped redis lists.
"""

import asyncio
import json

from fastapi import APIRouter, Query
from fastapi.responses import HTMLResponse

from fastink.common.config import get_config
from fastink.common.logger import logger
from fastink.inkdb.inkredis import redis_connect
from fastink.routers.status import InkStatus
from fastink.service.status_probe import META_KEY, REDIS_KEY_PREFIX

router = APIRouter()

# Probe data older than this many seconds marks the prober itself stale.
PROBER_STALE_AFTER = 300


async def _load_status_data(limit: int) -> dict:
    r = redis_connect()
    try:
        meta = await r.hgetall(META_KEY)
        meta = {
            (k.decode() if isinstance(k, bytes) else k):
            (v.decode() if isinstance(v, bytes) else v)
            for k, v in (meta or {}).items()
        }
        probes = json.loads(meta.get("probes", "[]"))
        last_run = int(meta.get("last_run", 0))

        result = []
        for probe in probes:
            name = probe.get("name")
            # Full history for uptime accuracy; only `limit` points are returned.
            raw = await r.lrange(f"{REDIS_KEY_PREFIX}{name}", 0, -1)
            points = []
            for item in raw:
                try:
                    points.append(json.loads(item))
                except (json.JSONDecodeError, TypeError):
                    continue
            # redis list is newest-first; serve oldest-first for rendering
            points.reverse()
            ok_points = [p for p in points if p.get("ok") is True]
            known_points = [p for p in points if p.get("ok") is not None]
            uptime = (len(ok_points) / len(known_points) * 100) if known_points else None
            current = points[-1] if points else None
            result.append({
                "name": name,
                "auth": probe.get("auth", "none"),
                "current": (
                    "up" if current and current.get("ok") is True
                    else "down" if current and current.get("ok") is False
                    else "unknown"
                ),
                "latency_ms": current.get("ms") if current else None,
                # Uptime over the full retained history (24 h by default),
                # independent of how many points the caller asked for.
                "uptime_percent": round(uptime, 2) if uptime is not None else None,
                "points": points[-limit:],
            })
        return {"probes": result, "last_run": last_run}
    finally:
        await r.aclose()


@router.get("/api/v2/status/get_status")
async def get_status(limit: str = Query("1440", description="Max history points per probe")):
    try:
        try:
            limit_int = int(limit)
        except (TypeError, ValueError):
            limit_int = 1440
        limit_int = max(1, min(limit_int, 1440))
        data = await _load_status_data(limit_int)
        import time as _time
        data["prober_stale"] = (
            data["last_run"] == 0
            or (_time.time() - data["last_run"]) > PROBER_STALE_AFTER
        )
        return {"status": InkStatus.SUCCESS, "msg": "success", "data": data}
    except Exception as err:
        logger.error(f"Failed to load status data: {err}")
        return {
            "status": InkStatus.INTERNAL_ERROR,
            "msg": "Failed to load status data",
            "data": None,
        }


def _check_fs_paths(paths: list) -> dict:
    """Check each configured path exists, is a readable/searchable directory.

    Returns a summary dict with per-path state. Never raises — a path that
    cannot be checked is reported as failed, not propagated as a 500.

    NOTE: this only verifies the *container's* mount point is alive. It cannot
    detect that a host softlink was repointed while the old target still exists
    (that drift is caught at deploy time by fastink-dev/tools/check_softlinks.sh).
    Emptiness is intentionally NOT treated as unhealthy: a freshly provisioned
    or legitimately empty mount is still healthy.
    """
    import os

    results = []
    all_ok = True
    for raw in paths:
        path = str(raw)
        entry = {"path": path, "ok": False, "reason": ""}
        try:
            if not os.path.exists(path):
                entry["reason"] = "missing"
            elif not os.path.isdir(path):
                entry["reason"] = "not a directory"
            elif not os.access(path, os.R_OK | os.X_OK):
                entry["reason"] = "not readable"
            else:
                entry["ok"] = True
        except OSError as err:
            # e.g. stale NFS handle, permission denied on stat
            entry["reason"] = f"stat error: {err.strerror or err}"
        if not entry["ok"]:
            all_ok = False
        results.append(entry)
    return {"all_ok": all_ok, "paths": results}


@router.get("/api/v2/status/get_fs_health")
async def get_fs_health():
    """Report health of site-specific bind-mounted filesystem paths.

    Unauthenticated (same skip-router convention as /status and /health).
    Paths come from filesystem_health.paths in config; empty list => healthy.

    The filesystem stat calls run in a worker thread (asyncio.to_thread) so a
    hung network mount cannot block the event loop. To avoid leaking internal
    host paths to anonymous callers, the response carries only aggregate counts
    (total / healthy / unhealthy) — never the path strings. Details are logged
    server-side for operators.
    """
    try:
        paths = get_config("filesystem_health", "paths", fallback=[])
        if not isinstance(paths, list):
            logger.error("filesystem_health.paths is not a list: %r", type(paths))
            return {
                "status": InkStatus.PARAM_ERROR,
                "msg": "filesystem_health.paths misconfigured (expected a list)",
                "data": None,
            }
        # Each entry must be a non-empty absolute path with no NUL byte. A
        # blank or relative entry (config typo) would otherwise silently check
        # the process CWD and be reported healthy; a NUL byte would raise
        # ValueError deep in the stat call and surface as a 500.
        bad = [
            p for p in paths
            if not (isinstance(p, str) and p.strip() and p.startswith("/") and "\x00" not in p)
        ]
        if bad:
            logger.error("filesystem_health.paths has invalid entries: %r", bad)
            return {
                "status": InkStatus.PARAM_ERROR,
                "msg": "filesystem_health.paths entries must be non-empty absolute paths",
                "data": None,
            }

        summary = await asyncio.to_thread(_check_fs_paths, paths)
        total = len(summary["paths"])
        healthy = sum(1 for p in summary["paths"] if p["ok"])
        aggregate = {"total": total, "healthy": healthy, "unhealthy": total - healthy}

        if summary["all_ok"]:
            return {
                "status": InkStatus.SUCCESS,
                "msg": f"All {total} filesystem paths healthy",
                "data": aggregate,
            }
        # Log the specific failing paths + reasons server-side only.
        failed = [(p["path"], p["reason"]) for p in summary["paths"] if not p["ok"]]
        logger.warning("fs_health: %d/%d path(s) unhealthy: %s", len(failed), total, failed)
        # Unhealthy mounts are a filesystem-layer failure, distinct from an
        # internal endpoint error (below) so callers can tell them apart.
        return {
            "status": InkStatus.FS_UNKNOWN_ERROR,
            "msg": f"{total - healthy} filesystem path(s) unhealthy",
            "data": aggregate,
        }
    except Exception as err:
        logger.error(f"Failed to check filesystem health: {err}")
        return {
            "status": InkStatus.INTERNAL_ERROR,
            "msg": "Failed to check filesystem health",
            "data": None,
        }


_STATUS_PAGE_HTML = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>FastINK Status</title>
<style>
  :root { --up:#3bd671; --down:#dc3545; --unknown:#7f8c9b; --bg:#0d1117; --card:#161b22; --text:#e6edf3; --muted:#8b949e; }
  * { box-sizing:border-box; margin:0; padding:0; }
  body { background:var(--bg); color:var(--text); font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,sans-serif; padding:32px 16px; }
  .wrap { max-width:880px; margin:0 auto; }
  h1 { font-size:22px; margin-bottom:4px; }
  .sub { color:var(--muted); font-size:13px; margin-bottom:24px; }
  .banner { border-radius:8px; padding:14px 18px; margin-bottom:24px; font-weight:600; }
  .banner.up { background:rgba(59,214,113,.12); color:var(--up); }
  .banner.down { background:rgba(220,53,69,.12); color:var(--down); }
  .banner.unknown { background:rgba(127,140,155,.12); color:var(--unknown); }
  .card { background:var(--card); border:1px solid #21262d; border-radius:8px; padding:16px 18px; margin-bottom:14px; }
  .row { display:flex; justify-content:space-between; align-items:baseline; margin-bottom:10px; }
  .name { font-weight:600; font-size:15px; }
  .pill { font-size:12px; padding:2px 10px; border-radius:10px; font-weight:600; }
  .pill.up { background:rgba(59,214,113,.15); color:var(--up); }
  .pill.down { background:rgba(220,53,69,.15); color:var(--down); }
  .pill.unknown { background:rgba(127,140,155,.15); color:var(--unknown); }
  .meta { color:var(--muted); font-size:12px; }
  .bar { display:flex; gap:1px; height:26px; }
  .tick { flex:1; border-radius:1px; min-width:1px; }
  .tick.up { background:var(--up); }
  .tick.down { background:var(--down); }
  .tick.unknown { background:var(--unknown); opacity:.45; }
  .foot { color:var(--muted); font-size:12px; margin-top:20px; text-align:center; }
</style>
</head>
<body>
<div class="wrap">
  <h1>FastINK Status</h1>
  <div class="sub" id="updated">loading…</div>
  <div id="banner"></div>
  <div id="cards"></div>
  <div class="foot">Auto-refresh every 60 s · uptime over full retained history</div>
</div>
<script>
const MAX_TICKS = 90;   // render last 90 points (~1.5h) per bar
function stateOf(p){ return p.ok === true ? "up" : p.ok === false ? "down" : "unknown"; }
function el(tag, cls, text){
  const e = document.createElement(tag);
  if(cls) e.className = cls;
  if(text !== undefined) e.textContent = text;   // textContent: no HTML injection
  return e;
}
async function load(){
  try{
    const resp = await fetch("/api/v2/status/get_status?limit=" + MAX_TICKS);
    const body = await resp.json();
    if(body.status !== "200" && body.status !== "SUCCESS") throw new Error(body.msg);
    render(body.data);
  }catch(e){
    document.getElementById("updated").textContent = "Failed to load status data";
  }
}
function render(data){
  const probes = data.probes || [];
  const anyDown = probes.some(p => p.current === "down");
  const allUp = probes.length > 0 && probes.every(p => p.current === "up");
  const banner = document.getElementById("banner");
  if(data.prober_stale){
    banner.className = "banner unknown";
    banner.textContent = "Probe data is stale — the prober may be down";
  }else if(anyDown){
    banner.className = "banner down";
    banner.textContent = "Some systems are experiencing issues";
  }else if(allUp){
    banner.className = "banner up";
    banner.textContent = "All systems operational";
  }else{
    banner.className = "banner unknown";
    banner.textContent = "Partial data available";
  }
  const cards = document.getElementById("cards");
  cards.innerHTML = "";
  for(const p of probes){
    const card = el("div", "card");
    const row1 = el("div", "row");
    row1.appendChild(el("span", "name", p.name));
    row1.appendChild(el("span", "pill " + p.current, String(p.current).toUpperCase()));
    card.appendChild(row1);
    const bar = el("div", "bar");
    card.appendChild(bar);
    const row2 = el("div", "row");
    row2.style.marginTop = "8px"; row2.style.marginBottom = "0";
    const up = p.uptime_percent !== null ? p.uptime_percent + "% uptime" : "no data";
    const lat = p.latency_ms !== null ? p.latency_ms + " ms" : "";
    row2.appendChild(el("span", "meta", up));
    row2.appendChild(el("span", "meta", lat));
    card.appendChild(row2);
    cards.appendChild(card);
    const pts = (p.points || []).slice(-MAX_TICKS);
    for(const pt of pts){
      const t = el("div", "tick " + stateOf(pt));
      t.title = new Date(pt.t * 1000).toLocaleString() + " · " + stateOf(pt) + (pt.ms ? " · " + pt.ms + " ms" : "");
      bar.appendChild(t);
    }
  }
  const d = data.last_run ? new Date(data.last_run * 1000).toLocaleString() : "never";
  document.getElementById("updated").textContent = "Last probe: " + d;
}
load();
setInterval(load, 60000);
</script>
</body>
</html>"""


@router.get("/status", include_in_schema=False)
async def status_page():
    return HTMLResponse(content=_STATUS_PAGE_HTML)
