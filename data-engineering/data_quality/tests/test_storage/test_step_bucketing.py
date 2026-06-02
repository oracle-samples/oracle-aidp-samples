"""Tests for ``step`` bucketing in ``read_metric_history``.

Phase 2 P2.3 wires run-timestamp bucketing through the storage layer:
when a caller passes ``step='P7D'``, consecutive runs inside the same
7-day window must collapse to a single row (collection-preferred over
validation, newest timestamp inside the bucket otherwise). The four
storage backends implement this differently — SQL window functions for
SQLite / ExternalCatalog / Delta, Spark DataFrame ops for JDBC — but
the observable contract must match.

These tests lock the SQLite implementation (the other SQL backends
share the same generated pattern; Spark-backed backends are covered by
the integration test suite that runs against a real Spark session).
"""

from __future__ import annotations

import pytest

from qualifire.core.duration import parse_step_seconds
from qualifire.core.exceptions import QualifireConfigError
from qualifire.storage.sqlite_storage import SQLiteStorage


def _row(
    ts: str,
    value: float,
    *,
    record_type: str = "validation",
    table: str = "t1",
    metric: str = "count",
    partition_ts: str | None = None,
) -> dict:
    # Default partition_ts to ts so post-H1 per-partition dedup keeps
    # each row distinct unless a caller explicitly overrides.
    return {
        "run_id": f"r-{ts}-{record_type}",
        "run_timestamp": ts,
        "owner": "t",
        "bu": "t",
        "dataset_name": "ds1",
        "table_name": table,
        "metric_name": metric,
        "metric_value": value,
        "collection_type": "aggregation",
        "validation_name": "v1" if record_type == "validation" else None,
        "validation_type": "threshold" if record_type == "validation" else None,
        "validation_status": "PASS" if record_type == "validation" else None,
        "validation_message": "ok" if record_type == "validation" else None,
        "notification_channel": None,
        "notification_status": None,
        "details_json": {},
        "record_type": record_type,
        "is_active": "true",
        "collector_name": "c1" if record_type == "collection" else None,
        "dimension_value": None,
        "partition_ts": partition_ts if partition_ts is not None else ts,
    }


def _make_storage() -> SQLiteStorage:
    storage = SQLiteStorage(db_path=":memory:")
    storage.initialize()
    return storage


class TestParseStepSeconds:
    """``parse_step_seconds`` is the single entry-point every storage
    backend uses to turn a user-facing duration into the integer seconds
    the bucketing SQL needs. Its contract must cover ``None`` (no
    bucketing), valid durations, and reject zero/negative/invalid input
    up-front — storage layers assume a strictly-positive bucket width."""

    def test_none_returns_none(self):
        assert parse_step_seconds(None) is None

    def test_parses_days(self):
        assert parse_step_seconds("P7D") == 7 * 86400

    def test_parses_hours(self):
        assert parse_step_seconds("PT4H") == 4 * 3600

    def test_parses_compound(self):
        assert parse_step_seconds("P1DT4H") == 86400 + 4 * 3600

    def test_rejects_zero(self):
        with pytest.raises(QualifireConfigError, match="positive"):
            parse_step_seconds("P0D")

    def test_rejects_empty(self):
        with pytest.raises(QualifireConfigError):
            parse_step_seconds("")

    def test_rejects_garbage(self):
        with pytest.raises(QualifireConfigError):
            parse_step_seconds("not-a-duration")


class TestSqliteStepBucketing:
    def test_no_step_preserves_legacy_behavior(self):
        """Without ``step``, every row must still be returned in the
        existing order (collection-first, newest run_timestamp next).
        Verifies the bucketing branch doesn't leak into the default
        path."""
        storage = _make_storage()
        storage.write_results([
            _row("2024-01-01T00:00:00", 100.0),
            _row("2024-01-02T00:00:00", 110.0),
            _row("2024-01-03T00:00:00", 120.0),
        ])
        history = storage.read_metric_history(
            table_name="t1", metric_name="count", limit=10
        )
        assert len(history) == 3
        # Newest validation first.
        assert [float(r["metric_value"]) for r in history] == [120.0, 110.0, 100.0]

    def test_step_7d_collapses_runs_in_same_epoch_bucket(self):
        """Classic drift/trend use case: runs that fall in the same
        epoch-aligned 7D window must collapse to one row. Buckets are
        fixed-width from the Unix epoch (not rolling windows from the
        first run), so runs at 2024-01-01 and 2024-01-03 share a
        bucket, while 2024-01-08 lands in the next one. Within a
        bucket the newest run wins (validation/validation tie-break
        falls back to run_timestamp DESC)."""
        storage = _make_storage()
        storage.write_results([
            _row("2024-01-01T00:00:00", 100.0),  # bucket A
            _row("2024-01-03T00:00:00", 110.0),  # bucket A (newer) — wins
            _row("2024-01-08T00:00:00", 200.0),  # bucket B
            _row("2024-01-10T00:00:00", 210.0),  # bucket B (newer) — wins
            _row("2024-01-15T00:00:00", 300.0),  # bucket C
        ])
        history = storage.read_metric_history(
            table_name="t1", metric_name="count", step="P7D", limit=90
        )
        assert len(history) == 3
        # Sorted newest-first, so bucket-C, then bucket-B winner, then bucket-A winner.
        values = [float(r["metric_value"]) for r in history]
        assert values == [300.0, 210.0, 110.0]

    def test_latest_run_timestamp_wins_within_same_bucket(self):
        """Within a single ``step`` bucket, dedupe is now pure
        wall-clock: the row with the latest ``run_timestamp`` wins
        regardless of record_type. (record_type only acts as a
        tiebreaker when ``run_timestamp`` matches exactly — see
        the same-instant test in test_sqlite.) Mirrors the dashboard
        JS dedupe so SQL-side history feeds and the dashboard agree."""
        storage = _make_storage()
        storage.write_results([
            # Validation runs on days 1, 2, 3.
            _row("2024-01-01T00:00:00", 100.0, record_type="validation"),
            _row("2024-01-02T00:00:00", 110.0, record_type="validation"),
            _row("2024-01-03T00:00:00", 120.0, record_type="validation"),
            # Collection run on day 1 (older than the day-3 validation).
            _row("2024-01-01T06:00:00", 500.0, record_type="collection"),
        ])
        history = storage.read_metric_history(
            table_name="t1", metric_name="count", step="P7D", limit=10
        )
        # All four rows fall inside the same 7-day bucket. Day 3
        # validation has the latest run_timestamp, so it wins.
        assert len(history) == 1
        assert history[0]["record_type"] == "validation"
        assert float(history[0]["metric_value"]) == 120.0

    def test_validation_wins_tiebreak_at_same_run_timestamp_in_bucket(self):
        """Tiebreak: when a validator emits vrow + crow at the same
        instant, validation wins so the surviving row carries the
        verdict — matches the SQL dedupe rule's record_type tiebreak."""
        storage = _make_storage()
        storage.write_results([
            # Older row in the same bucket — should lose on run_timestamp.
            _row("2024-01-01T00:00:00", 100.0, record_type="collection"),
            # Same-instant pair (drift validator emitting vrow + crow).
            _row("2024-01-03T00:00:00", 120.0, record_type="validation"),
            _row("2024-01-03T00:00:00", 120.0, record_type="collection"),
        ])
        history = storage.read_metric_history(
            table_name="t1", metric_name="count", step="P7D", limit=10
        )
        assert len(history) == 1
        # Latest run_timestamp tied → validation wins on tiebreak.
        assert history[0]["record_type"] == "validation"
        assert float(history[0]["metric_value"]) == 120.0

    def test_null_run_timestamp_excluded(self):
        """Rows with NULL run_timestamp can't be placed in a bucket —
        they must be excluded rather than forming a ghost bucket."""
        storage = _make_storage()
        storage.write_results([
            _row("2024-01-01T00:00:00", 100.0),
            _row("2024-01-08T00:00:00", 200.0),
        ])
        # Directly insert a null-timestamp legacy row.
        conn = storage._get_conn()
        conn.execute(
            "INSERT INTO qualifire_history (run_id, table_name, metric_name, "
            "metric_value, record_type, run_timestamp) "
            "VALUES ('legacy', 't1', 'count', 999.0, 'validation', NULL)"
        )
        conn.commit()
        history = storage.read_metric_history(
            table_name="t1", metric_name="count", step="P7D", limit=10
        )
        assert len(history) == 2
        assert 999.0 not in [float(r["metric_value"]) for r in history]

    def test_limit_caps_bucket_count_not_raw_rows(self):
        """``limit`` is a bucket cap, not a row cap — a caller asking
        for 3 past values at ``step=7D`` should get 3 bucket-winners,
        regardless of how many raw runs per bucket exist. Using dates
        known to span 4 distinct epoch-aligned 7D buckets."""
        storage = _make_storage()
        storage.write_results([
            # bucket A: 2024-01-01..07
            _row("2024-01-01T00:00:00", 1.0),
            _row("2024-01-02T00:00:00", 2.0),
            # bucket B
            _row("2024-01-08T00:00:00", 8.0),
            _row("2024-01-09T00:00:00", 9.0),
            # bucket C
            _row("2024-01-15T00:00:00", 15.0),
            _row("2024-01-16T00:00:00", 16.0),
            # bucket D
            _row("2024-01-22T00:00:00", 22.0),
            _row("2024-01-23T00:00:00", 23.0),
        ])
        history = storage.read_metric_history(
            table_name="t1", metric_name="count", step="P7D", limit=3
        )
        assert len(history) == 3

    def test_rejects_zero_step(self):
        """``step=0D`` must fail cleanly rather than divide-by-zero in
        the SQL expression."""
        storage = _make_storage()
        storage.write_results([_row("2024-01-01T00:00:00", 100.0)])
        with pytest.raises(QualifireConfigError):
            storage.read_metric_history(
                table_name="t1", metric_name="count", step="0D"
            )

    def test_step_isolates_buckets_across_weeks(self):
        """Adjacent weeks must stay in separate buckets — not collapsed
        together by coarse-grained boundary-alignment drift. A 7D
        bucket starting at epoch: 2024-01-01 is in a different bucket
        than 2024-01-08."""
        storage = _make_storage()
        storage.write_results([
            _row("2024-01-01T00:00:00", 100.0),
            _row("2024-01-08T00:00:00", 200.0),
            _row("2024-01-15T00:00:00", 300.0),
        ])
        history = storage.read_metric_history(
            table_name="t1", metric_name="count", step="P7D", limit=10
        )
        assert len(history) == 3
