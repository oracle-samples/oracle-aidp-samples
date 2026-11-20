"""Ingest REAL failed job runs from the workspace into incident memory (fingerprint -> incidents MERGE + wake_events).

Usage: python scripts/ingest_failures.py [--hours 720] [--print-only]
Reads list_job_runs (the workspace jobRuns listing), keeps FAILED runs, fingerprints
state.stateMessage, and upserts one incident per (fingerprint, job) with occurrence counting.
A failed run is counted once: the script can be run again over the same window.
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import pathlib
import sys
from collections import Counter
from collections.abc import Callable, Collection

ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from nightshift import fingerprint as fp  # noqa: E402
from nightshift import ledger  # noqa: E402
from nightshift.aidp_io import AidpIO  # noqa: E402
from nightshift.config import Env  # noqa: E402


SOURCE = "list_job_runs"   # wake_events.source of the events this script writes


def ingested_event_ids(run_sql: Callable[[str], list], ops: str) -> set[str]:
    """Event ids of the failed runs an earlier run of this script already counted."""
    return {str(r[0]) for r in run_sql(f"SELECT event_id FROM {ops}.wake_events WHERE source = '{SOURCE}'")}


def plan_ingest(failures: list[dict], ops: str, now: dt.datetime,
                already_ingested: Collection[str] = ()) -> tuple[list[str], list[dict], Counter]:
    """The ledger statements for the failed runs not counted yet, a printable row per failed run, and the label counts.

    A run's wake event id is derived from its job and run key. A run whose event is already in the ledger is skipped,
    so a second run of the script does not raise ``occurrences``. The incident is written before its event: if the
    event insert fails, the run is counted again next time rather than left without an incident."""
    inc_cols = {c for c, _ in ledger.COLUMNS["incidents"]}
    ev_cols = {c for c, _ in ledger.COLUMNS["wake_events"]}
    counted = set(already_ingested)
    labels: Counter = Counter()
    statements: list[str] = []
    summary: list[dict] = []
    for r in failures:
        msg = AidpIO.run_message(r)
        label, conf, key = fp.incident_fingerprint(msg)
        job = str(r.get("jobName") or r.get("name") or "unknown")
        run_key = str(r.get("key"))
        event_id = ledger._sha16("failure", job, run_key)
        is_new = event_id not in counted
        labels[label] += 1
        summary.append({"job": job, "run": run_key[:8], "label": label, "conf": conf, "new": is_new,
                        "msg": msg[:160].replace("\n", " ")})
        if not is_new:
            continue
        counted.add(event_id)
        incident = {k: v for k, v in {
            "incident_key": ledger.incident_key(key, job), "fingerprint_key": key, "pipeline": job,
            "label": label, "confidence": conf, "sample_error": msg[:2000], "last_run_key": run_key,
            "first_seen": now, "last_seen": now, "status": "open", "occurrences": 1,
        }.items() if k in inc_cols}
        event = {k: v for k, v in {
            "event_id": event_id, "pipeline": job, "source": SOURCE,
            "reason": f"job_failed:{label}",
            "payload": {"run_key": run_key, "label": label, "fingerprint_key": key, "startTime": r.get("startTime")},
            "created_at": now,
        }.items() if k in ev_cols}
        statements.append(ledger.upsert_incident(ops, incident, now))
        statements.append(ledger.insert_wake_event(ops, event, now))
    return statements, summary, labels


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--hours", type=int, default=24 * 30)
    ap.add_argument("--print-only", action="store_true")
    args = ap.parse_args()
    env = Env()
    io = AidpIO(env, dry_run=True)
    now = dt.datetime.now(dt.timezone.utc)
    try:
        failures = io.recent_failures(args.hours)
        # --print-only reads no ledger, so it lists every failed run in the window as new
        already = set() if args.print_only else ingested_event_ids(io.run_sql, env.ops)
        sqls, summary, labels = plan_ingest(failures, env.ops, now, already)
        print(json.dumps({"failed_runs": len(failures), "new_runs": sum(1 for row in summary if row["new"]),
                          "labels": dict(labels)}, indent=1))
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
