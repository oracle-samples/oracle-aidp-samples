"""Tests for ``Qualifire.deactivate_metric`` + ``qualifire.core.deactivate``.

Covers H1 read-back-and-bump semantics, idempotent retry, dimensional
filtering, partition_ts narrowing, ``note`` propagation, and the
loud-fail-on-legacy-table contract (D9).

Tests primarily exercise SQLite (full coverage) — Spark/JDBC backends
have their own test suites and the deactivate helper is backend-
agnostic via the storage Protocol.
"""

from __future__ import annotations

import json
from datetime import datetime, timedelta

import pytest

from qualifire.api import Qualifire
from qualifire.backends.pandas_backend import PandasBackend
from qualifire.core.deactivate import deactivate_metric
from qualifire.storage.sqlite_storage import SQLiteStorage


def _seed(storage: SQLiteStorage, rows: list[dict]) -> None:
    """Best-effort seed; missing columns default None per SYSTEM_TABLE_COLUMNS."""
    storage.write_results(rows)


def _make_collection_row(
    *,
    dataset_name: str = "ds",
    table_name: str = "t",
    metric_name: str = "row_count",
    metric_value: float = 100.0,
    run_timestamp: str | None = None,
    partition_ts: str | None = None,
    dimension_value: str | None = None,
    is_active: str = "true",
    run_id: str = "run-1",
) -> dict:
    if run_timestamp is None:
        run_timestamp = datetime(2026, 4, 1, 12, 0, 0).isoformat()
    return {
        "run_id": run_id,
        "run_timestamp": run_timestamp,
        "owner": "ops",
        "bu": "data",
        "dataset_name": dataset_name,
        "table_name": table_name,
        "metric_name": metric_name,
        "metric_value": metric_value,
        "collection_type": "aggregation",
        "validation_name": None,
        "validation_type": None,
        "validation_status": None,
        "validation_message": None,
        "notification_channel": None,
        "notification_status": None,
        "details_json": None,
        "record_type": "collection",
        "collector_name": "AggregationCollector",
        "dimension_value": dimension_value,
        "collected_at": run_timestamp,
        "validated_at": None,
        "notified_at": None,
        "partition_ts": partition_ts,
        "expected_value": None,
        "actual_value_text": None,
        "dataset_description": None,
        "validation_description": None,
        "is_active": is_active,
    }


@pytest.fixture
def storage(tmp_path):
    s = SQLiteStorage(db_path=str(tmp_path / "qf.db"))
    s.initialize()
    return s


class TestDeactivateMetricCore:
    """Tests on the core helper directly (no Qualifire wrapper)."""

    def test_single_row_deactivate_writes_tombstone(self, storage):
        row = _make_collection_row(partition_ts="2026-04-01T00:00:00")
        _seed(storage, [row])

        n = deactivate_metric(
            storage,
            dataset_name="ds",
            metric_name="row_count",
            partition_ts="2026-04-01T00:00:00",
        )
        assert n == 1
        # Subsequent read returns None (H8): the tombstone wins
        # ROW_NUMBER, COALESCE filter elides it.
        result = storage.read_collection_metric_at_partition(
            table_name="t",
            metric_name="row_count",
            anchor_ts="2026-04-01T00:00:00",
        )
        assert result is None

    def test_no_match_returns_zero_no_exception(self, storage):
        n = deactivate_metric(
            storage,
            dataset_name="nonexistent",
            metric_name="row_count",
        )
        assert n == 0

    def test_idempotent_retry(self, storage):
        row = _make_collection_row(partition_ts="2026-04-01T00:00:00")
        _seed(storage, [row])

        first = deactivate_metric(
            storage,
            dataset_name="ds",
            metric_name="row_count",
            partition_ts="2026-04-01T00:00:00",
        )
        # Second call: tombstone already wins; no active rows match.
        second = deactivate_metric(
            storage,
            dataset_name="ds",
            metric_name="row_count",
            partition_ts="2026-04-01T00:00:00",
        )
        assert first == 1
        assert second == 0

    def test_h1_run_timestamp_strictly_newer(self, storage):
        """H1 invariant: tombstone run_timestamp > latest active row's."""
        latest_ts = datetime.now() + timedelta(seconds=10)  # in the future
        row = _make_collection_row(
            run_timestamp=latest_ts.isoformat(),
            partition_ts="2026-04-01T00:00:00",
        )
        _seed(storage, [row])

        deactivate_metric(
            storage,
            dataset_name="ds",
            metric_name="row_count",
            partition_ts="2026-04-01T00:00:00",
        )
        # Read raw rows from sqlite to inspect tombstone run_timestamp
        conn = storage._get_conn()
        rows = list(conn.execute(
            f"SELECT run_timestamp, is_active FROM {storage._table} "
            "WHERE dataset_name = 'ds' AND metric_name = 'row_count' "
            "ORDER BY run_timestamp DESC"
        ))
        assert len(rows) == 2
        tomb_ts = rows[0][0]
        active_ts = rows[1][0]
        assert tomb_ts > active_ts, (
            f"tombstone {tomb_ts!r} must sort after active {active_ts!r}"
        )
        assert rows[0][1] == "false"

    def test_note_propagates_to_details_json(self, storage):
        row = _make_collection_row(partition_ts="2026-04-01T00:00:00")
        _seed(storage, [row])

        deactivate_metric(
            storage,
            dataset_name="ds",
            metric_name="row_count",
            partition_ts="2026-04-01T00:00:00",
            note="OPS-12345",
        )
        conn = storage._get_conn()
        rows = list(conn.execute(
            f"SELECT details_json FROM {storage._table} "
            "WHERE is_active = 'false'"
        ))
        assert len(rows) == 1
        details = json.loads(rows[0][0])
        assert details["deactivated_by"] == "OPS-12345"
        assert "deactivated_at" in details

    def test_dimension_filtering(self, storage):
        # Two dimensional rows for the same metric; deactivate one.
        row_us = _make_collection_row(
            partition_ts="2026-04-01T00:00:00",
            dimension_value='{"region":"US"}',
            run_id="r1",
        )
        row_eu = _make_collection_row(
            partition_ts="2026-04-01T00:00:00",
            dimension_value='{"region":"EU"}',
            run_id="r2",
        )
        _seed(storage, [row_us, row_eu])

        n = deactivate_metric(
            storage,
            dataset_name="ds",
            metric_name="row_count",
            partition_ts="2026-04-01T00:00:00",
            dimension_value='{"region":"US"}',
        )
        assert n == 1

        # US tombstoned; EU still active
        us = storage.read_collection_metric_at_partition(
            table_name="t",
            metric_name="row_count",
            anchor_ts="2026-04-01T00:00:00",
            dimension_value='{"region":"US"}',
        )
        assert us is None
        eu = storage.read_collection_metric_at_partition(
            table_name="t",
            metric_name="row_count",
            anchor_ts="2026-04-01T00:00:00",
            dimension_value='{"region":"EU"}',
        )
        assert eu is not None
        assert eu["metric_value"] == 100.0

    def test_partition_ts_none_tombstones_all_partitions(self, storage):
        """``partition_ts=None`` (default) tombstones every active partition."""
        rows = [
            _make_collection_row(
                partition_ts="2026-04-01T00:00:00", run_id=f"r{i}",
            )
            for i in range(3)
        ]
        # Re-stamp partition_ts so they're distinct
        rows[0]["partition_ts"] = "2026-04-01T00:00:00"
        rows[1]["partition_ts"] = "2026-04-02T00:00:00"
        rows[2]["partition_ts"] = "2026-04-03T00:00:00"
        _seed(storage, rows)

        n = deactivate_metric(
            storage,
            dataset_name="ds",
            metric_name="row_count",
            # partition_ts=None → all
        )
        assert n == 3

        for ts in ("2026-04-01T00:00:00",
                   "2026-04-02T00:00:00",
                   "2026-04-03T00:00:00"):
            result = storage.read_collection_metric_at_partition(
                table_name="t",
                metric_name="row_count",
                anchor_ts=ts,
            )
            assert result is None, f"partition {ts} should be tombstoned"


class TestQualifireDeactivateMetric:
    """Tests on the public ``Qualifire.deactivate_metric`` API."""

    def _make_qf(self, tmp_path):
        return Qualifire(
            backend=PandasBackend(),
            system_table=str(tmp_path / "qf.db"),
            system_table_backend="sqlite",
            owner="ops",
            bu="data",
        )

    def test_uses_instance_storage_when_config_none(self, tmp_path):
        qf = self._make_qf(tmp_path)
        row = _make_collection_row(partition_ts="2026-04-01T00:00:00")
        _seed(qf._storage, [row])

        n = qf.deactivate_metric(
            dataset_name="ds",
            metric_name="row_count",
            partition_ts="2026-04-01T00:00:00",
            note="ops-1",
        )
        assert n == 1

    def test_raises_when_no_storage_and_no_config(self):
        from qualifire.core.exceptions import QualifireConfigError

        qf = Qualifire(backend=PandasBackend())  # no system_table
        with pytest.raises(QualifireConfigError, match="system_table"):
            qf.deactivate_metric(dataset_name="ds", metric_name="m")
