"""Upload the in-cluster half of Night Shift to the workspace folder the scheduled job reads from.

  python scripts/deploy_workspace.py                 # package + heartbeat + watch list
  python scripts/deploy_workspace.py --only heartbeat   (or: pkg | watch)

Targets (workspace paths, under Shared/nightshift):
  pkg/nightshift/*.py        the package the heartbeat imports (PKG in jobs/nightshift_heartbeat.py)
  nightshift_heartbeat.py    the Python task of job nightshift_heartbeat
  watched_merges.json        the watch list the heartbeat grades
Text files only, written through the notebook file API (the same path the workbench editor uses); the result is read
back and compared before the script reports success. No control-plane call is made.
"""
from __future__ import annotations

import argparse
import pathlib
import sys

ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from nightshift.aidp_io import AidpIO  # noqa: E402
from nightshift.config import Env  # noqa: E402

WS = "Shared/nightshift"


def targets(only: str | None) -> list[tuple[pathlib.Path, str]]:
    out: list[tuple[pathlib.Path, str]] = []
    if only in (None, "pkg"):
        out += [(p, f"{WS}/pkg/nightshift/{p.name}") for p in sorted((ROOT / "nightshift").glob("*.py"))]
    if only in (None, "heartbeat"):
        out.append((ROOT / "jobs" / "nightshift_heartbeat.py", f"{WS}/nightshift_heartbeat.py"))
    if only in (None, "watch"):
        out.append((ROOT / "jobs" / "watched_merges.json", f"{WS}/watched_merges.json"))
    return out


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--only", choices=["pkg", "heartbeat", "watch"], default=None)
    args = ap.parse_args()
    io = AidpIO(Env(), dry_run=True)
    bad = 0
    try:
        for local, remote in targets(args.only):
            text = local.read_text(encoding="utf-8")
            io.client.nb_save_file(remote, text)
            back = io.client.nb_get_contents(remote)
            got = back.get("content") if isinstance(back, dict) else None
            ok = isinstance(got, str) and got == text
            bad += 0 if ok else 1
            print(f"{'ok ' if ok else 'DIFF'} {remote} ({len(text)} chars)")
    finally:
        io.close()
    return 1 if bad else 0


if __name__ == "__main__":
    raise SystemExit(main())
