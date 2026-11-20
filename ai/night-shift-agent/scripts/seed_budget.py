"""Seed budget_ledger with a shape x hours ESTIMATE for the configured cluster (the Workbench API exposes no usage or cost data).

One row per completed day of the current month, category 'cluster_estimate' (today is seeded once it is over, because
the row key is the day and rows are never rewritten). ocpu_hours = (driver OCPUs + assumed
active workers x worker OCPUs) x 24. est_cost = ocpu_hours x NS_OCPU_RATE (default 0.05, a placeholder
labelled estimate; replace with the contracted rate). Idempotent on entry_id.

Usage: python scripts/seed_budget.py [--workers 1] [--rate 0.05] [--print-only]
"""
from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import os
import pathlib
import sys

ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from nightshift import ledger  # noqa: E402
from nightshift.aidp_io import AidpIO  # noqa: E402
from nightshift.config import Env  # noqa: E402


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--workers", type=int, default=1, help="assumed average active workers (min count when idle)")
    ap.add_argument("--rate", type=float, default=float(os.environ.get("NS_OCPU_RATE", "0.05")))
    ap.add_argument("--print-only", action="store_true")
    args = ap.parse_args()
    env = Env()
    io = AidpIO(env, dry_run=True)
    try:
        c = io.get_cluster(env.cluster_id)
        d_ocpu = int(((c.get("driverConfig") or {}).get("driverShapeConfig") or {}).get("ocpus") or 0)
        w_ocpu = int(((c.get("workerConfig") or {}).get("workerShapeConfig") or {}).get("ocpus") or 0)
        if d_ocpu <= 0 or (args.workers > 0 and w_ocpu <= 0):
            # zero-OCPU rows would read as a known budget with everything remaining, and rows are insert-if-absent
            # per day, so a later run could not correct them
            print(f"cluster {env.cluster_id}: the cluster body does not state its ocpus (driverShapeConfig.ocpus = {d_ocpu}, "
                  f"workerShapeConfig.ocpus = {w_ocpu}); nothing seeded, the budget stays unknown", file=sys.stderr)
            return 1
        ocpus = d_ocpu + args.workers * w_ocpu
        now = dt.datetime.now(dt.timezone.utc)
        month_start = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        cols = ", ".join(col for col, _ in ledger.COLUMNS["budget_ledger"])
        sqls = []
        # whole days only: the row key is the calendar day, and rows are insert-if-absent, so an estimate written for
        # part of today could never be completed by a later run. Today is seeded once it is over.
        day = month_start
        while day.date() < now.date():
            ocpu_hours = ocpus * 24.0
            row = {
                "entry_id": hashlib.sha256(f"seed|{env.cluster_id}|{day.date()}".encode()).hexdigest()[:16],
                "tick_id": None, "job_key": None, "run_key": None, "cluster_key": env.cluster_id,
                "category": "cluster_estimate", "ocpu_hours": float(ocpu_hours), "est_cost": float(ocpu_hours * args.rate),
                "window_hour": None, "window_day": day.strftime("%Y-%m-%d"), "created_at": now,
            }
            vals = ", ".join(ledger.lit(row[col]) for col, _ in ledger.COLUMNS["budget_ledger"])
            sqls.append(
                f"INSERT INTO {env.ops}.budget_ledger ({cols}) SELECT {vals} "
                f"WHERE NOT EXISTS (SELECT 1 FROM {env.ops}.budget_ledger WHERE entry_id = {ledger.lit(row['entry_id'])})"
            )
            day += dt.timedelta(days=1)
        print(f"cluster ocpus: driver {d_ocpu} + {args.workers} x {w_ocpu} workers = {ocpus}; rows: {len(sqls)}; rate {args.rate} (estimate)")
        if not sqls:
            print("no completed day in this month yet: nothing to seed. With an empty budget ledger the budget is unknown, "
                  "which the tiers treat as at most low_compute; run this again after the first full day")
            return 0
        if args.print_only:
            print(sqls[0])
            return 0
        for s in sqls:
            io.run_sql(s)
        print("budget_ledger:", io.run_sql(f"SELECT window_day, round(ocpu_hours,1), round(est_cost,2) FROM {env.ops}.budget_ledger ORDER BY window_day"))
        print("summary:", io.run_sql(ledger.budget_summary_sql(env.ops, month_start.isoformat())))
    finally:
        io.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
