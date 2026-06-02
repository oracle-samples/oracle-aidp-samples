"""S3 SQLite soft-delete read-path acceptance tests.

Plan reference: docs/features/metrics-backfill-and-soft-delete/plan.md S3.
Locks the H2 read predicate ordering: ROW_NUMBER → rn=1 →
COALESCE(is_active, 'true') = 'true' → metric_value IS NOT NULL →
record_type filter.

Inserting NULL-valued tombstones tests the most-critical regression:
filtering metric_value before ROW_NUMBER would let stale active rows
surface beneath a newer tombstone.
"""

from __future__ import annotations

import pytest

from qualifire.storage.base import SYSTEM_TABLE_COLUMNS
from qualifire.storage.sqlite_storage import SQLiteStorage


def _make_storage() -> SQLiteStorage:
    storage = SQLiteStorage(db_path=":memory:")
    storage.initialize()
    return storage


def _row(
    *,
    run_ts: str,
    table: str = "t1",
    metric: str = "row_count",
    value: float | None = 100.0,
    record_type: str = "collection",
    validation_name: str | None = None,
    partition_ts: str = "2024-01-01T00:00:00",
    dimension_value: str | None = None,
    is_active: str = "true",
    collected_at: str | None = None,
) -> dict:
    base = {col: None for col in SYSTEM_TABLE_COLUMNS}
    base.update({
        "run_id": f"r-{run_ts}",
        "run_timestamp": run_ts,
        "owner": "t",
        "bu": "t",
        "dataset_name": "ds1",
        "table_name": table,
        "metric_name": metric,
        "metric_value": value,
        "validation_name": validation_name,
        "record_type": record_type,
        "collector_name": "c1" if record_type == "collection" else None,
        "dimension_value": dimension_value,
        "partition_ts": partition_ts,
        "collected_at": collected_at or run_ts,
        "is_active": is_active,
    })
    return base


class TestSoftDeleteReads:
    def test_active_collection_row_visible(self):
        s = _make_storage()
        s.write_results([_row(run_ts="2024-01-01T00:00:00")])
        rows = s.read_metric_history(table_name="t1", metric_name="row_count")
        assert len(rows) == 1
        assert float(rows[0]["metric_value"]) == 100.0

    def test_null_tombstone_hides_active_row(self):
        """Critical H2 test: NULL-valued tombstone must win ROW_NUMBER
        and exclude the partition. If `metric_value IS NOT NULL` ran
        before ROW_NUMBER, the active row would survive."""
        s = _make_storage()
        s.write_results([
            _row(run_ts="2024-01-01T00:00:00", value=100.0),
            _row(run_ts="2024-01-02T00:00:00", value=None,
                 is_active="false"),
        ])
        rows = s.read_metric_history(table_name="t1", metric_name="row_count")
        assert rows == []

    def test_fresh_active_row_reactivates(self):
        s = _make_storage()
        s.write_results([
            _row(run_ts="2024-01-01T00:00:00", value=100.0),
            _row(run_ts="2024-01-02T00:00:00", value=None, is_active="false"),
            _row(run_ts="2024-01-03T00:00:00", value=200.0),
        ])
        rows = s.read_metric_history(table_name="t1", metric_name="row_count")
        assert len(rows) == 1
        assert float(rows[0]["metric_value"]) == 200.0

    def test_dimensional_sibling_unaffected(self):
        s = _make_storage()
        s.write_results([
            _row(run_ts="2024-01-01T00:00:00", value=100.0,
                 dimension_value="us"),
            _row(run_ts="2024-01-01T00:00:00", value=200.0,
                 dimension_value="uk"),
            _row(run_ts="2024-01-02T00:00:00", value=None,
                 dimension_value="us", is_active="false"),
        ])
        us = s.read_metric_history(
            table_name="t1", metric_name="row_count", dimension_value="us"
        )
        uk = s.read_metric_history(
            table_name="t1", metric_name="row_count", dimension_value="uk"
        )
        assert us == []
        assert len(uk) == 1
        assert float(uk[0]["metric_value"]) == 200.0

    def test_partition_anchored_read_honours_tombstone(self):
        from datetime import datetime as _dt

        s = _make_storage()
        s.write_results([
            _row(run_ts="2024-01-01T00:00:00", partition_ts="2024-01-01T00:00:00",
                 value=100.0),
            _row(run_ts="2024-01-02T00:00:00", partition_ts="2024-01-01T00:00:00",
                 value=None, is_active="false"),
        ])
        rows = s.read_metric_history_by_partition(
            table_name="t1", metric_name="row_count",
            anchor_ts=_dt(2024, 1, 2),
            count=1,
            step="P1D",
        )
        assert rows == []

    def test_bucketed_read_hides_stale_across_buckets(self):
        """Two-stage rework: tombstone in a newer bucket hides the
        active row in the older bucket. Without the rework, the stale
        active row would surface in its original bucket."""
        s = _make_storage()
        s.write_results([
            # active row in old bucket
            _row(run_ts="2024-01-01T00:00:00", value=100.0,
                 partition_ts="2024-01-01T00:00:00"),
            # tombstone in newer bucket but for the SAME partition
            _row(run_ts="2024-01-15T00:00:00", value=None,
                 partition_ts="2024-01-01T00:00:00", is_active="false"),
        ])
        rows = s.read_metric_history(
            table_name="t1", metric_name="row_count",
            step="P7D", limit=10,
        )
        assert rows == []

    def test_health_data_hides_tombstoned_validation(self):
        s = _make_storage()
        s.write_results([
            _row(run_ts="2024-01-01T00:00:00", record_type="validation",
                 validation_name="thresh", value=100.0),
            _row(run_ts="2024-01-02T00:00:00", record_type="validation",
                 validation_name="thresh", value=None, is_active="false"),
        ])
        # health data only goes back 30 days by default; freshen up.
        from datetime import datetime, timedelta
        recent = (datetime.now() - timedelta(days=1)).isoformat()
        s2 = _make_storage()
        s2.write_results([
            _row(run_ts=recent, record_type="validation",
                 validation_name="thresh", value=100.0),
            _row(run_ts=(datetime.now() - timedelta(hours=1)).isoformat(),
                 record_type="validation", validation_name="thresh",
                 value=None, is_active="false"),
        ])
        rows = s2.read_health_data(days=7)
        assert rows == []

    def test_read_collection_metric_at_partition_active(self):
        from datetime import datetime as _dt

        s = _make_storage()
        s.write_results([_row(
            run_ts="2024-01-01T00:00:00",
            partition_ts="2024-01-01T00:00:00",
            value=42.0,
        )])
        cell = s.read_collection_metric_at_partition(
            table_name="t1", metric_name="row_count",
            anchor_ts=_dt(2024, 1, 1),
            dimension_value=None,
        )
        assert cell is not None
        assert float(cell["metric_value"]) == 42.0
        assert cell["partition_ts"] == "2024-01-01T00:00:00"

    def test_read_collection_metric_at_partition_tombstoned_misses(self):
        from datetime import datetime as _dt

        s = _make_storage()
        s.write_results([
            _row(run_ts="2024-01-01T00:00:00", value=42.0,
                 partition_ts="2024-01-01T00:00:00"),
            _row(run_ts="2024-01-02T00:00:00", value=None,
                 partition_ts="2024-01-01T00:00:00", is_active="false"),
        ])
        cell = s.read_collection_metric_at_partition(
            table_name="t1", metric_name="row_count",
            anchor_ts=_dt(2024, 1, 1),
            dimension_value=None,
        )
        assert cell is None

    def test_read_collection_metric_at_partition_dimension_isolation(self):
        from datetime import datetime as _dt

        s = _make_storage()
        s.write_results([
            _row(run_ts="2024-01-01T00:00:00", value=10.0,
                 partition_ts="2024-01-01T00:00:00", dimension_value="us"),
            _row(run_ts="2024-01-01T00:00:00", value=20.0,
                 partition_ts="2024-01-01T00:00:00", dimension_value="uk"),
        ])
        us = s.read_collection_metric_at_partition(
            table_name="t1", metric_name="row_count",
            anchor_ts=_dt(2024, 1, 1), dimension_value="us",
        )
        uk = s.read_collection_metric_at_partition(
            table_name="t1", metric_name="row_count",
            anchor_ts=_dt(2024, 1, 1), dimension_value="uk",
        )
        assert us is not None and float(us["metric_value"]) == 10.0
        assert uk is not None and float(uk["metric_value"]) == 20.0

    def test_validation_history_tombstone_hides_history(self):
        """H2 for read_validation_history."""
        s = _make_storage()
        s.write_results([
            _row(run_ts="2024-01-01T00:00:00", record_type="validation",
                 validation_name="v1", value=100.0,
                 partition_ts="2024-01-01T00:00:00"),
            _row(run_ts="2024-01-02T00:00:00", record_type="validation",
                 validation_name="v1", value=None, is_active="false",
                 partition_ts="2024-01-01T00:00:00"),
        ])
        rows = s.read_validation_history("ds1", "v1", limit=10)
        assert rows == []

    def test_validation_history_bulk_tombstone_hides(self):
        """H2 for read_validation_history_bulk(limit=1) (the engine
        path)."""
        from qualifire.core.models import ValidationKey

        s = _make_storage()
        s.write_results([
            _row(run_ts="2024-01-01T00:00:00", record_type="validation",
                 validation_name="v1", value=100.0,
                 partition_ts="2024-01-01T00:00:00"),
            _row(run_ts="2024-01-02T00:00:00", record_type="validation",
                 validation_name="v1", value=None, is_active="false",
                 partition_ts="2024-01-01T00:00:00"),
        ])
        key = ValidationKey(
            dataset_name="ds1", validation_name="v1",
            metric_name="row_count", dimension_value=None,
        )
        result = s.read_validation_history_bulk([key], limit=1)
        assert result[key] == []

    def test_read_latest_run_after_deactivation_returns_none(self):
        """H8: when the latest row for a dataset is a tombstone,
        read_latest_run returns None."""
        s = _make_storage()
        s.write_results([
            _row(run_ts="2024-01-01T00:00:00", record_type="validation",
                 validation_name="v1", value=100.0,
                 partition_ts="2024-01-01T00:00:00"),
            _row(run_ts="2024-01-02T00:00:00", record_type="validation",
                 validation_name="v1", value=None, is_active="false",
                 partition_ts="2024-01-01T00:00:00"),
        ])
        assert s.read_latest_run("ds1") is None

    def test_collected_at_drift_does_not_invert_run_timestamp_order(self):
        """H2 invariant: a stale row with future collected_at must not
        beat a newer (by run_timestamp) tombstone. Closes Codex impl R1
        finding on read_metric_history_by_partition ordering."""
        from datetime import datetime as _dt

        s = _make_storage()
        s.write_results([
            # stale active with FUTURE collected_at
            _row(run_ts="2024-01-01T00:00:00", value=100.0,
                 partition_ts="2024-01-01T00:00:00",
                 collected_at="2024-01-05T00:00:00"),
            # newer tombstone by run_timestamp, NULL collected_at
            _row(run_ts="2024-01-02T00:00:00", value=None,
                 partition_ts="2024-01-01T00:00:00",
                 is_active="false",
                 collected_at=None),
        ])
        rows = s.read_metric_history_by_partition(
            table_name="t1", metric_name="row_count",
            anchor_ts=_dt(2024, 1, 2),
            count=1, step="P1D",
        )
        assert rows == []
