"""Run one actuator tick. Default is dry-run (no mutating control-plane calls; ledger rows are still
written through the kernel and carry dry_run=true). --apply disables dry-run.

  python scripts/run_tick.py                 # dry-run, prints the TickReport as JSON
  python scripts/run_tick.py --apply         # live actions
  python scripts/run_tick.py --no-store      # dry-run; reads clusters/jobs live but writes no ledger rows
  python scripts/run_tick.py --offline       # no platform calls at all (stub io + memory store); plan shape only
  python scripts/run_tick.py --draft         # also run the drafting pass: grade ungraded watched statements and draft
                                             # the silently wrong ones from this session's kernel (useful on instances where
                                             # the scheduled run's principal lacks Generative AI permissions; see README,
                                             # 'Identity of a scheduled run'); watch list = jobs/watched_merges.json
"""
from __future__ import annotations

import argparse
import logging
import os
import sys
from pathlib import Path
import json
from datetime import datetime, timezone


# --- env plumbing (shared shape across scripts; no secrets, auth via the OCI profile) ---------------
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
from nightshift.config import Env  # noqa: E402

_ENV_FLAGS = {
    "region": "NS_REGION", "data_lake_id": "NS_DATA_LAKE_ID", "workspace_id": "NS_WORKSPACE_ID",
    "cluster_id": "NS_CLUSTER_ID", "config_profile": "NS_OCI_PROFILE", "catalog": "NS_CATALOG",
    "ops_schema": "NS_OPS_SCHEMA", "probe_notebook": "NS_NOTEBOOK",
}


def add_env_args(parser: argparse.ArgumentParser) -> None:
    g = parser.add_argument_group("environment overrides (default: NS_* env vars, then config.py)")
    for field, var in _ENV_FLAGS.items():
        g.add_argument(f"--{field.replace('_', '-')}", dest=f"env_{field}", default=None, help=f"overrides {var}")
    parser.add_argument("--env", action="append", default=[], metavar="KEY=VALUE",
                        help="generic override, e.g. --env cluster_id=abc (repeatable)")
    parser.add_argument("-v", "--verbose", action="store_true")


def env_from_args(args: argparse.Namespace) -> Env:
    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.INFO, format="%(levelname)s %(name)s: %(message)s")
    values = {f: os.environ.get(var, getattr(Env(), f)) for f, var in _ENV_FLAGS.items()}
    for field in _ENV_FLAGS:
        if getattr(args, f"env_{field}", None):
            values[field] = getattr(args, f"env_{field}")
    for kv in args.env:
        k, _, v = kv.partition("=")
        if k not in _ENV_FLAGS:
            raise SystemExit(f"unknown --env key {k!r}; valid: {', '.join(_ENV_FLAGS)}")
        values[k] = v
    return Env(**values)
# ---------------------------------------------------------------------------------------------------


class _MemoryStore:
    """Records SQL and returns no rows; for offline dry-runs."""

    def __init__(self) -> None:
        self.sql: list[str] = []

    def run_sql(self, sql: str, timeout: int = 300) -> list[tuple]:
        self.sql.append(sql)
        return []


class _OfflineIO:
    """No platform calls: one stub cluster with the normal-tier profile, no jobs, no runs."""

    def __init__(self, env: Env, dry_run: bool = True) -> None:
        self.env, self.dry_run, self.calls = env, dry_run, []

    def get_cluster(self, key):
        return {"key": key, "displayName": "offline", "state": "ACTIVE", "minWorkers": 1, "maxWorkers": 6,
                "autoStopInMinutes": 30, "workerShape": "amd.generic"}

    def list_jobs(self):
        return []

    def list_recent_runs(self, hours=24):
        return []

    def __getattr__(self, name):  # any mutating call is recorded, never sent
        def _rec(*a, **k):
            self.calls.append((name, a, k))
            return {"dry_run": True, "op": name, "offline": True}
        return _rec

    def close(self):
        pass


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--apply", action="store_true", help="disable dry-run: actually stop/update clusters and jobs")
    p.add_argument("--no-store", action="store_true", help="do not open a kernel session; record SQL in memory")
    p.add_argument("--offline", action="store_true", help="no platform calls at all (implies --no-store, dry-run)")
    p.add_argument("--now", default=None, help="ISO timestamp to use as the tick clock (testing)")
    p.add_argument("--draft", action="store_true", help="run the drafting pass through this session's kernel (see module docstring)")
    p.add_argument("--watch-file", default=str(ROOT / "jobs" / "watched_merges.json"), help="watch list for --draft")
    p.add_argument("--state-file", default=None,
                   help="kv state JSON (default: $NS_STATE_DIR or ~/.nightshift/kv_<ws>.json; --no-store uses memory)")
    add_env_args(p)
    args = p.parse_args(argv)
    env = env_from_args(args)

    from nightshift.actuator import FileKV, MemoryKV, default_kv_store, tick
    from nightshift.aidp_io import AidpIO

    now = datetime.fromisoformat(args.now) if args.now else datetime.now(timezone.utc)
    if now.tzinfo is None:
        now = now.replace(tzinfo=timezone.utc)
    if args.offline and args.apply:
        raise SystemExit("--offline and --apply are mutually exclusive")
    if args.offline:
        args.no_store = True
    io = _OfflineIO(env) if args.offline else AidpIO(env, dry_run=not args.apply)
    store = _MemoryStore() if args.no_store else io
    kv_store = FileKV(args.state_file) if args.state_file else (MemoryKV() if args.no_store else default_kv_store(env))
    watched = None
    if args.draft:
        if args.offline or args.no_store:
            raise SystemExit("--draft needs the kernel: not with --offline or --no-store")
        watched = json.loads(Path(args.watch_file).read_text(encoding="utf-8"))
    try:
        report = tick(io, store, now=now, dry_run=not args.apply, kv_store=kv_store, draft=args.draft, watched=watched)
    finally:
        io.close()
    out = report.to_dict()
    if args.no_store:
        out["recorded_sql"] = store.sql
    print(json.dumps(out, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
