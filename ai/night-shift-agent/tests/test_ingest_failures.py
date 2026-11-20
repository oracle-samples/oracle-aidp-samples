"""scripts/ingest_failures.py: a failed run is counted once, however often the script runs. The statements are the
real ledger SQL, run in duckdb; the run rows have the shape the job-runs listing returns. No platform calls."""
from __future__ import annotations

import importlib.util
from datetime import datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
NOW = datetime(2026, 11, 20, 12, 0, tzinfo=timezone.utc)
OOM = "Job aborted due to stage failure: java.lang.OutOfMemoryError: Java heap space"


def _load_script():
    spec = importlib.util.spec_from_file_location("ns_ingest_failures", ROOT / "scripts" / "ingest_failures.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def _failed_run(key: str, started: datetime, job: str = "orders_silver", message: str = OOM) -> dict:
    return {"key": key, "jobName": job, "startTime": int(started.timestamp() * 1000),
            "state": {"status": "FAILED", "stateMessage": message}}


def _ingest(mod, ops, con, failures, now=NOW):
    """One run of the script against the ledger: read what is already ingested, plan, write."""
    already = mod.ingested_event_ids(lambda q: con.execute(q).fetchall(), ops)
    statements, _, _ = mod.plan_ingest(failures, ops, now, already)
    for statement in statements:
        con.execute(statement)


def _occurrences(con, ops):
    return [r[0] for r in con.execute(f"SELECT occurrences FROM {ops}.incidents").fetchall()]


def test_a_second_ingest_of_the_same_failed_runs_does_not_count_them_again(ops_db):
    ops, con = ops_db
    mod = _load_script()
    failures = [_failed_run(f"run-{n}", NOW - timedelta(hours=n)) for n in (1, 2, 3)]

    _ingest(mod, ops, con, failures)
    _ingest(mod, ops, con, failures)

    assert _occurrences(con, ops) == [3]
    assert con.execute(f"SELECT COUNT(*) FROM {ops}.wake_events").fetchone()[0] == 3


def test_a_later_ingest_counts_only_the_runs_it_has_not_seen(ops_db):
    ops, con = ops_db
    mod = _load_script()
    old = [_failed_run("run-1", NOW - timedelta(hours=5)), _failed_run("run-2", NOW - timedelta(hours=4))]

    _ingest(mod, ops, con, old)
    _ingest(mod, ops, con, [*old, _failed_run("run-3", NOW - timedelta(hours=1))], now=NOW + timedelta(hours=1))

    assert _occurrences(con, ops) == [3]


def test_the_same_run_listed_twice_in_one_ingest_is_counted_once(ops_db):
    ops, con = ops_db
    mod = _load_script()
    run = _failed_run("run-1", NOW - timedelta(hours=1))

    _ingest(mod, ops, con, [run, dict(run)])

    assert _occurrences(con, ops) == [1]


def test_print_only_planning_reads_no_ledger_and_lists_every_run_as_new():
    mod = _load_script()
    failures = [_failed_run(f"run-{n}", NOW - timedelta(hours=n)) for n in (1, 2)]

    statements, summary, labels = mod.plan_ingest(failures, "default.nightshift_ops", NOW)

    assert len(statements) == 4 and [row["new"] for row in summary] == [True, True]
    assert dict(labels) == {"OUT_OF_MEMORY": 2}
