"""Regression tests for the Kerberos-aware runtime ceiling in apps/shell.sh.

The ceiling logic is bash, so these tests drive it in a subprocess: they source
the function-definition region of shell.sh (everything before the ``exec run.sh``
handoff) and exercise ``ink_krb_ticket_dead`` / ``ink_past_ceiling`` against a
fake ``klist`` on PATH.

They exist because the ticket expiry used to be parsed out of klist's date text.
That text is MM/DD/YY under LC_ALL=C but DD/MM/YY under other locales, and the
parser assumed DD/MM/YY *while pinning LC_ALL=C*, so a real ``10/20/26`` became
``2026-20-10`` (invalid -> the fallback silently disabled itself) and a real
``11/05/26`` became ``2026-05-11`` (valid but in the PAST -> jobs were killed
while their ticket was still good).  The implementation now uses ``klist -s``
(exit status only, no date text to misread); these tests pin that.
"""

import os
import stat
import subprocess
from pathlib import Path

SHELL_SH = (
    Path(__file__).resolve().parents[1] / "src" / "fastink" / "computing" / "apps" / "shell.sh"
)
HANDOFF_MARKER = "# -- hand off to the app's run.sh --"

# MM/DD/YY, as LC_ALL=C klist really prints it. Read as DD/MM/YY this is
# 2026-05-11 -- a date in the past -- which is exactly the misparse that used to
# kill live jobs.
POISON_DATE = "11/05/26 21:44:45"


def _defs_region(tmp_path):
    text = SHELL_SH.read_text(encoding="utf-8")
    cut = text.index(HANDOFF_MARKER)
    defs = tmp_path / "defs.sh"
    defs.write_text(text[:cut], encoding="utf-8")
    return defs


def _run(tmp_path, script, dead=False, date_text=POISON_DATE, krb5ccname="/tmp/fake-ccache"):
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir(exist_ok=True)
    klist = bin_dir / "klist"
    klist.write_text(
        "#!/bin/bash\n"
        'for a in "$@"; do\n'
        '  if [ "$a" = "-s" ]; then exit %d; fi\n'
        "done\n"
        'echo "Ticket cache: FILE:/tmp/fake"\n'
        'echo "Valid starting     Expires            Service principal"\n'
        'echo "10/19/26 20:44:45  %s  krbtgt/IHEPKRB5@IHEPKRB5"\n'
        "exit 0\n" % (1 if dead else 0, date_text),
        encoding="utf-8",
    )
    klist.chmod(klist.stat().st_mode | stat.S_IEXEC)

    env = dict(os.environ)
    env["PATH"] = "%s:%s" % (bin_dir, env["PATH"])
    if krb5ccname is None:
        env.pop("KRB5CCNAME", None)
    else:
        env["KRB5CCNAME"] = krb5ccname

    body = "source %s 2>/dev/null\n%s\n" % (_defs_region(tmp_path), script)
    return subprocess.run(
        ["bash", "-c", body], env=env, capture_output=True, text=True, timeout=60
    )


def test_ticket_dead_follows_klist_exit_status(tmp_path):
    alive = _run(tmp_path, "ink_krb_ticket_dead && echo DEAD || echo ALIVE", dead=False)
    assert alive.stdout.strip() == "ALIVE"

    dead = _run(tmp_path, "ink_krb_ticket_dead && echo DEAD || echo ALIVE", dead=True)
    assert dead.stdout.strip() == "DEAD"


def test_misparseable_date_text_does_not_move_the_ceiling(tmp_path):
    """The regression: date text must never be parsed.

    klist reports a FUTURE expiry (11/05/26 = 5 Nov) but a parser reading it as
    DD/MM/YY would compute 11 May -- already past -- and close a healthy job.
    """
    script = (
        "MAX_DEADLINE=$(( $(date +%s) + 86400 )); _INK_KRB_RENEW_FAILED=1; "
        "ink_past_ceiling $(date +%s) && echo PAST || echo NOT_PAST"
    )
    proc = _run(tmp_path, script, dead=False, date_text=POISON_DATE)
    assert proc.stdout.strip() == "NOT_PAST"


def test_dead_ticket_with_failed_renewal_ends_a_capped_job(tmp_path):
    script = (
        "MAX_DEADLINE=$(( $(date +%s) + 86400 )); _INK_KRB_RENEW_FAILED=1; "
        "ink_past_ceiling $(date +%s) && echo PAST || echo NOT_PAST"
    )
    proc = _run(tmp_path, script, dead=True)
    assert proc.stdout.strip() == "PAST"


def test_dead_ticket_alone_does_not_end_a_capped_job(tmp_path):
    """Renewal still working -> a transient klist failure must not reap the job."""
    script = (
        "MAX_DEADLINE=$(( $(date +%s) + 86400 )); _INK_KRB_RENEW_FAILED=0; "
        "ink_past_ceiling $(date +%s) && echo PAST || echo NOT_PAST"
    )
    proc = _run(tmp_path, script, dead=True)
    assert proc.stdout.strip() == "NOT_PAST"


def test_uncapped_app_survives_a_dead_ticket(tmp_path):
    """asic/asicbm: MAX_DEADLINE=0 means run the full init_hours regardless."""
    script = (
        "MAX_DEADLINE=0; _INK_KRB_RENEW_FAILED=1; "
        "ink_past_ceiling $(date +%s) && echo PAST || echo NOT_PAST"
    )
    proc = _run(tmp_path, script, dead=True)
    assert proc.stdout.strip() == "NOT_PAST"


def test_configured_cap_ends_the_job_even_with_a_live_ticket(tmp_path):
    script = (
        "MAX_DEADLINE=$(( $(date +%s) - 1 )); _INK_KRB_RENEW_FAILED=0; "
        "ink_past_ceiling $(date +%s) && echo PAST || echo NOT_PAST"
    )
    proc = _run(tmp_path, script, dead=False)
    assert proc.stdout.strip() == "PAST"


def test_missing_ccache_is_treated_as_alive(tmp_path):
    """No KRB5CCNAME -> unknown, and unknown must never kill a job."""
    script = (
        "MAX_DEADLINE=$(( $(date +%s) + 86400 )); _INK_KRB_RENEW_FAILED=1; "
        "ink_krb_ticket_dead && echo DEAD || echo ALIVE; "
        "ink_past_ceiling $(date +%s) && echo PAST || echo NOT_PAST"
    )
    proc = _run(tmp_path, script, dead=True, krb5ccname=None)
    assert proc.stdout.split() == ["ALIVE", "NOT_PAST"]


def test_helpers_survive_set_euo_pipefail(tmp_path):
    """openclaw/run.sh runs under `set -euo pipefail` and calls these helpers."""
    script = (
        "set -euo pipefail\n"
        "MAX_DEADLINE=$(( $(date +%s) + 86400 )); _INK_KRB_RENEW_FAILED=1\n"
        "ink_renew_krb\n"
        "if ink_past_ceiling $(date +%s); then echo PAST; else echo NOT_PAST; fi\n"
        "echo SURVIVED"
    )
    proc = _run(tmp_path, script, dead=True, krb5ccname=None)
    assert "SURVIVED" in proc.stdout
    assert proc.returncode == 0
