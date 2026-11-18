"""Ingest REAL failed job runs from the workspace into incident memory (fingerprint -> incidents MERGE + wake_events).

Usage: python scripts/ingest_failures.py [--hours 720] [--print-only]
Reads list_job_runs (the workspace jobRuns listing), keeps FAILED runs, fingerprints
state.stateMessage, and upserts one incident per (fingerprint, job) with occurrence counting.
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import pathlib
import sys
from collections import Counter

ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from nightshift import fingerprint as fp  # noqa: E402
from nightshift import ledger  # noqa: E402
from nightshift.aidp_io import AidpIO  # noqa: E402
from nightshift.config import Env  # noqa: E402


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--hours", type=int, default=24 * 30)
    ap.add_argument("--print-only", action="store_true")
    args = ap.parse_args()
    env = Env()
    io = AidpIO(env, dry_run=True)
    now = dt.datetime.now(dt.timezone.utc)
    inc_cols = {c for c, _ in ledger.COLUMNS["incidents"]}
    ev_cols = {c for c, _ in ledger.COLUMNS["wake_events"]}
    try:
        failures = io.recent_failures(args.hours)
        labels = Counter()
        sqls = []
        summary = []
        for r in failures:
            msg = io.run_message(r)
            label, conf, key = fp.incident_fingerprint(msg)
            job = str(r.get("jobName") or r.get("name") or "unknown")
            run_key = str(r.get("key"))
            started = r.get("startTime")
            labels[label] += 1
            summary.append({"job": job, "run": run_key[:8], "label": label, "conf": conf, "msg": msg[:160].replace("\n", " ")})
            incident = {k: v for k, v in {
                "incident_key": ledger.incident_key(key, job), "fingerprint_key": key, "pipeline": job,
                "label": label, "confidence": conf, "sample_error": msg[:2000], "last_run_key": run_key,
                "first_seen": now, "last_seen": now, "status": "open", "occurrences": 1,
            }.items() if k in inc_cols}
            event = {k: v for k, v in {
                "event_id": ledger._sha16("failure", job, run_key), "pipeline": job, "source": "list_job_runs",
                "reason": f"job_failed:{label}", "payload": {"run_key": run_key, "label": label, "fingerprint_key": key, "startTime": started},
                "created_at": now,
            }.items() if k in ev_cols}
            sqls.append(ledger.upsert_incident(env.ops, incident, now))
            sqls.append(ledger.insert_wake_event(env.ops, event, now))
        print(json.dumps({"failed_runs": len(failures), "labels": dict(labels)}, indent=1))
        for s_ in summary:
            print(" ", s_)
        if args.print_only:
            return 0
        for s_ in sqls:
            io.run_sql(s_)
        print("incidents:", io.run_sql(f"SELECT pipeline, label, occurrences, status FROM {env.ops}.incidents ORDER BY occurrences DESC"))
        print("unconsumed wake events:", io.run_sql(f"SELECT COUNT(*) FROM {env.ops}.wake_events WHERE consumed_by_tick IS NULL"))
    finally:
        io.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
