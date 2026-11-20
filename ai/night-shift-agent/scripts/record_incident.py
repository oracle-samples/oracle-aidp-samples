"""Fingerprint an error text and record it live: ops.incidents (MERGE) + ops.wake_events (INSERT).

Usage: python scripts/record_incident.py --pipeline orders_silver --run-id drift-1 --error-file err.txt [--print-only]
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import pathlib
import sys

ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from nightshift import fingerprint as fp  # noqa: E402
from nightshift import ledger  # noqa: E402
from nightshift.aidp_io import AidpIO  # noqa: E402
from nightshift.config import Env  # noqa: E402


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--pipeline", required=True)
    ap.add_argument("--run-id", required=True)
    ap.add_argument("--error-file", required=True)
    ap.add_argument("--print-only", action="store_true")
    args = ap.parse_args()
    env = Env()
    text = pathlib.Path(args.error_file).read_text(encoding="utf-8")
    label, conf, key = fp.incident_fingerprint(text)
    now = dt.datetime.now(dt.timezone.utc)
    inc_cols = {c for c, _ in ledger.COLUMNS["incidents"]}
    incident = {k: v for k, v in {
        "incident_key": ledger.incident_key(key, args.pipeline),
        "fingerprint_key": key, "pipeline": args.pipeline, "label": label, "confidence": conf,
        "sample_error": text[:2000], "last_run_key": args.run_id, "first_seen": now, "last_seen": now,
        "status": "open", "occurrences": 1,
    }.items() if k in inc_cols}
    ev_cols = {c for c, _ in ledger.COLUMNS["wake_events"]}
    event = {k: v for k, v in {
        "event_id": ledger._sha16("failure", args.pipeline, args.run_id),
        "pipeline": args.pipeline, "source": "synthetic_failing_job", "reason": f"job_failed:{label}",
        "payload": {"run_id": args.run_id, "label": label, "fingerprint_key": key},
        "created_at": now,
    }.items() if k in ev_cols}
    sqls = [ledger.upsert_incident(env.ops, incident, now), ledger.insert_wake_event(env.ops, event, now)]
    print(json.dumps({"label": label, "confidence": conf, "fingerprint_key": key, "incident_key": incident["incident_key"]}, indent=1))
    if args.print_only:
        print("\n\n".join(sqls))
        return 0
    io = AidpIO(env, dry_run=True)  # dry_run governs control-plane mutations; ledger writes are SQL
    try:
        for s in sqls:
            io.run_sql(s)
        rows = io.run_sql(f"SELECT label, occurrences, status, last_run_key FROM {env.ops}.incidents WHERE pipeline = '{args.pipeline}'")
        print("incidents:", rows)
        rows = io.run_sql(f"SELECT event_id, pipeline, reason, consumed_by_tick FROM {env.ops}.wake_events ORDER BY created_at DESC LIMIT 5")
        print("wake_events:", rows)
    finally:
        io.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
