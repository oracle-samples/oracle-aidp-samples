"""Tests for the partition_ts feature: config, persistence, and partition-anchored history reads."""

from __future__ import annotations

from datetime import datetime, timedelta

import pytest

from qualifire.core.config import (
    AggregationCollectionConfig,
    DatasetConfig,
    QualifireConfig,
    ThresholdLevels,
    ThresholdRuleConfig,
    ThresholdValidationConfig,
)
from qualifire.core.duration import (
    parse_duration,
    partition_lookback_anchors,
)
from qualifire.storage.sqlite_storage import SQLiteStorage


# ---------------------------------------------------------------------------
# DatasetConfig.name optional + partition_ts
# ---------------------------------------------------------------------------


class TestDatasetConfigDefaults:
    def test_name_defaults_to_table(self):
        ds = DatasetConfig(table="catalog.schema.sales")
        assert ds.name == "catalog.schema.sales"

    def test_name_explicit_wins(self):
        ds = DatasetConfig(name="custom_name", table="catalog.schema.sales")
        assert ds.name == "custom_name"

    def test_query_dataset_requires_name(self):
        # No table → no default available; the cross-config trap (drift /
        # forecast / anomaly_detection collapsing onto a shared default
        # history key) is exactly what the explicit-name rule prevents.
        with pytest.raises(ValueError, match="name"):
            DatasetConfig(query="SELECT 1")

    def test_partition_ts_field(self):
        ds = DatasetConfig(table="t", partition_ts="event_dt")
        assert ds.partition_ts == "event_dt"

    def test_partition_ts_optional(self):
        ds = DatasetConfig(table="t")
        assert ds.partition_ts is None


# ---------------------------------------------------------------------------
# ISO 8601 duration parsing
# ---------------------------------------------------------------------------


class TestIso8601Durations:
    def test_iso_basic_smoke(self):
        # Smoke check: the canonical ISO forms parse to the expected
        # timedelta. Detailed coverage lives in test_duration.py.
        assert parse_duration("P7D") == timedelta(days=7)
        assert parse_duration("PT1H") == timedelta(hours=1)

    def test_iso_days(self):
        assert parse_duration("P7D") == timedelta(days=7)

    def test_iso_hours(self):
        assert parse_duration("PT1H") == timedelta(hours=1)
        assert parse_duration("PT24H") == timedelta(hours=24)

    def test_iso_weeks(self):
        assert parse_duration("P1W") == timedelta(weeks=1)

    def test_iso_compound(self):
        assert parse_duration("P2DT12H") == timedelta(days=2, hours=12)

    def test_iso_minutes_seconds(self):
        assert parse_duration("PT30M") == timedelta(minutes=30)
        assert parse_duration("PT45S") == timedelta(seconds=45)

    def test_iso_rejects_months(self):
        # P1M would be ambiguous (calendar months are variable-width).
        # The parser must reject it explicitly so callers see the
        # error at config-load time rather than getting a silently
        # wrong duration.
        with pytest.raises(Exception, match="not supported|Invalid"):
            parse_duration("P1M")

    def test_iso_rejects_empty_components(self):
        with pytest.raises(Exception, match="no components|Invalid"):
            parse_duration("P")
        with pytest.raises(Exception, match="no components|Invalid"):
            parse_duration("PT")


# ---------------------------------------------------------------------------
# Lookback anchor computation
# ---------------------------------------------------------------------------


class TestPartitionLookbackAnchors:
    def test_daily_step(self):
        anchor = datetime(2026, 4, 28)
        out = partition_lookback_anchors(anchor, count=3, step="P7D")
        assert out == [
            datetime(2026, 4, 21),
            datetime(2026, 4, 14),
            datetime(2026, 4, 7),
        ]

    def test_hourly_step(self):
        anchor = datetime(2026, 4, 28, 23, 0, 0)
        out = partition_lookback_anchors(anchor, count=3, step="PT7H")
        assert out == [
            datetime(2026, 4, 28, 16, 0, 0),
            datetime(2026, 4, 28, 9, 0, 0),
            datetime(2026, 4, 28, 2, 0, 0),
        ]

    def test_count_zero(self):
        assert partition_lookback_anchors(datetime(2026, 1, 1), 0, "P1D") == []

    def test_step_iso_form(self):
        # ISO 8601 step (the only accepted form). Verifies the
        # anchor − k·step formula resolves with the canonical input.
        anchor = datetime(2026, 4, 28)
        out = partition_lookback_anchors(anchor, count=2, step="P7D")
        assert out == [datetime(2026, 4, 21), datetime(2026, 4, 14)]


# ---------------------------------------------------------------------------
# Storage: schema migration + partition-anchored read
# ---------------------------------------------------------------------------


class TestSqlitePartitionTs:
    def _seed(self, anchor: datetime, partition_values: list[str]) -> SQLiteStorage:
        """Create a fresh in-memory SQLiteStorage, write one collection row
        per partition_ts in `partition_values` so the partition-anchored
        reader has data to find."""
        storage = SQLiteStorage(db_path=":memory:")
        storage.initialize()
        rows = []
        for i, pts in enumerate(partition_values):
            rows.append({
                "run_id": f"r{i}",
                "run_timestamp": (anchor - timedelta(hours=i)).isoformat(),
                "owner": "o", "bu": "b",
                "dataset_name": "ds", "table_name": "t",
                "metric_name": "m", "metric_value": float(100 + i),
                "collection_type": "aggregation",
                "validation_name": None, "validation_type": None,
                "validation_status": None, "validation_message": None,
                "notification_channel": None, "notification_status": None,
                "details_json": {},
                "record_type": "collection",
                "is_active": "true",
                "collector_name": "agg",
                "dimension_value": None,
                "collected_at": anchor.isoformat(), "validated_at": None, "notified_at": None,
                "partition_ts": pts,
            })
        storage.write_results(rows)
        return storage

    def test_schema_includes_partition_ts(self):
        storage = SQLiteStorage(db_path=":memory:")
        storage.initialize()
        cur = storage._get_conn().execute("PRAGMA table_info(qualifire_history)")
        cols = {row[1] for row in cur.fetchall()}
        assert "partition_ts" in cols

    def test_daily_lookback(self):
        anchor = datetime(2026, 4, 28)
        partitions = [
            (anchor - timedelta(days=k)).isoformat() for k in range(0, 8)
        ]
        storage = self._seed(anchor, partitions)
        rows = storage.read_metric_history_by_partition(
            table_name="t", metric_name="m",
            anchor_ts=anchor, count=3, step="P7D",
        )
        # Anchor minus 7d, 14d, 21d. Only "minus 7d" exists in partitions
        # (we seeded days 0-7), so we get one row back. This verifies the
        # IN-clause filtering — only matching partition_ts values come back.
        returned_pts = {r["partition_ts"] for r in rows}
        assert (anchor - timedelta(days=7)).isoformat() in returned_pts

    def test_hourly_lookback_three_steps(self):
        anchor = datetime(2026, 4, 28, 23, 0, 0)
        # Seed all hours from anchor down to anchor-23h
        partitions = [
            (anchor - timedelta(hours=k)).isoformat() for k in range(0, 24)
        ]
        storage = self._seed(anchor, partitions)
        rows = storage.read_metric_history_by_partition(
            table_name="t", metric_name="m",
            anchor_ts=anchor, count=3, step="PT7H",
        )
        # Anchor at 23:00, step PT7H, count 3 → 16:00, 09:00, 02:00.
        expected = {
            datetime(2026, 4, 28, 16, 0, 0).isoformat(),
            datetime(2026, 4, 28, 9, 0, 0).isoformat(),
            datetime(2026, 4, 28, 2, 0, 0).isoformat(),
        }
        actual = {r["partition_ts"] for r in rows}
        assert actual == expected

    def test_no_match_returns_empty(self):
        storage = self._seed(datetime(2026, 1, 1), [])
        rows = storage.read_metric_history_by_partition(
            table_name="t", metric_name="m",
            anchor_ts=datetime(2026, 1, 8), count=3, step="P1D",
        )
        assert rows == []

    def test_duplicate_partition_returns_latest_collected_at(self):
        """When the same identity (partition_ts, dimension_value,
        metric_name) has multiple writes — a partition rerun — the
        partition-anchored read MUST return the row with the most
        recent ``collected_at``. Old SQLite path used ORDER BY +
        Python first-occurrence dedupe, which was non-deterministic
        within a tied (record_type, partition_ts) group."""
        anchor = datetime(2026, 4, 28)
        partition = (anchor - timedelta(days=7)).isoformat()
        storage = SQLiteStorage(db_path=":memory:")
        storage.initialize()

        # Two collection writes for the same partition_ts, different
        # collected_at. The newer one's metric_value must win.
        early = (anchor - timedelta(days=2)).isoformat()
        late = (anchor - timedelta(hours=1)).isoformat()
        rows = []
        for i, (collected_at, value) in enumerate([
            (early, 100.0),    # earlier write
            (late, 200.0),     # later write — should win
        ]):
            rows.append({
                "run_id": f"r{i}",
                "run_timestamp": collected_at,
                "owner": "o", "bu": "b",
                "dataset_name": "ds", "table_name": "t",
                "metric_name": "m", "metric_value": value,
                "collection_type": "aggregation",
                "validation_name": None, "validation_type": None,
                "validation_status": None, "validation_message": None,
                "notification_channel": None, "notification_status": None,
                "details_json": {},
                "record_type": "collection",
                "is_active": "true",
                "collector_name": "agg",
                "dimension_value": None,
                "collected_at": collected_at,
                "validated_at": None, "notified_at": None,
                "partition_ts": partition,
            })
        storage.write_results(rows)

        result_rows = storage.read_metric_history_by_partition(
            table_name="t", metric_name="m",
            anchor_ts=anchor, count=1, step="P7D",
        )
        assert len(result_rows) == 1
        assert result_rows[0]["metric_value"] == 200.0, (
            "expected the later collected_at to win the dedupe; got "
            f"value={result_rows[0]['metric_value']}"
        )

    def test_dimension_filter_segments(self):
        anchor = datetime(2026, 4, 28)
        storage = SQLiteStorage(db_path=":memory:")
        storage.initialize()
        # Two rows with the SAME partition_ts but different dimension_values.
        common = {
            "run_id": "r", "run_timestamp": anchor.isoformat(),
            "owner": "o", "bu": "b",
            "dataset_name": "ds", "table_name": "t",
            "metric_name": "m",
            "collection_type": "aggregation",
            "validation_name": None, "validation_type": None,
            "validation_status": None, "validation_message": None,
            "notification_channel": None, "notification_status": None,
            "details_json": {},
            "record_type": "collection",
            "is_active": "true",
            "collector_name": "agg",
            "collected_at": anchor.isoformat(),
            "validated_at": None, "notified_at": None,
            "partition_ts": (anchor - timedelta(days=7)).isoformat(),
        }
        storage.write_results([
            {**common, "dimension_value": "us", "metric_value": 100.0},
            {**common, "dimension_value": "uk", "metric_value": 200.0},
        ])
        us_rows = storage.read_metric_history_by_partition(
            table_name="t", metric_name="m",
            anchor_ts=anchor, count=1, step="P7D",
            dimension_value="us",
        )
        assert len(us_rows) == 1
        assert float(us_rows[0]["metric_value"]) == 100.0


# ---------------------------------------------------------------------------
# End-to-end: engine persists partition_ts + validator reads it
# ---------------------------------------------------------------------------


class TestEnginePartitionTsRoundTrip:
    def test_persist_partition_ts_on_collection_row(self, tmp_path):
        """A dataset that supplies partition_ts results in collection rows
        carrying that value to the system table."""
        from unittest.mock import MagicMock

        from qualifire.core.context import QualifireContext
        from qualifire.core.engine import QualifireEngine

        backend = MagicMock()
        # qf_partition_ts comes through as the literal expression result.
        backend.get_aggregations.return_value = {
            "row_count": 42,
            "qf_partition_ts": "2026-04-28",
        }

        sqlite_path = str(tmp_path / "qf.db")
        storage = SQLiteStorage(db_path=sqlite_path)
        storage.initialize()

        ds = DatasetConfig(
            table="my_table",
            partition_ts="'2026-04-28'",
            partition_step="P1D",
            validations=[ThresholdValidationConfig(
                collection=AggregationCollectionConfig(
                    expressions={"row_count": "COUNT(*)"},
                ),
                rules=[ThresholdRuleConfig(
                    metric="row_count",
                    thresholds=ThresholdLevels(error={"min": 1}),
                )],
            )],
        )
        cfg = QualifireConfig(
            owner="o", bu="b", system_table="qf",
            datasets=[ds],
        )
        engine = QualifireEngine(
            backend=backend, storage=storage,
            context=QualifireContext(), config=cfg,
        )
        engine.run()

        # Verify the collection row landed with partition_ts populated.
        conn = storage._get_conn()
        cur = conn.execute(
            "SELECT partition_ts FROM qualifire_history "
            "WHERE record_type='collection' AND metric_name='row_count'"
        )
        rows = cur.fetchall()
        assert len(rows) == 1
        assert rows[0][0] == "2026-04-28T00:00:00"


# ---------------------------------------------------------------------------
# Multi-dimension aggregation: dimensions=[col_a, col_b] produces one
# CollectionResult per (metric, dim_combo). Verifies dim JSON shape,
# per-row partition_ts stamping under GROUP BY, and dim-aware history reads.
# ---------------------------------------------------------------------------


class TestTwoDimensionalCollection:
    def _mock_backend_two_dim(self, dim_values: list[tuple[str, str]],
                               metric_value: float, partition_ts: str):
        """Mock a SparkBackend whose execute_sql returns one row per
        dim combo with `qf_partition_ts` injected by the collector.
        """
        from unittest.mock import MagicMock

        class _FakeRow:
            def __init__(self, d):
                self._d = d
            def asDict(self):
                return dict(self._d)

        rows = []
        for region, product in dim_values:
            rows.append(_FakeRow({
                "region": region, "product": product,
                "row_count": metric_value,
                "qf_partition_ts": partition_ts,
            }))

        class _FakeDF:
            def collect(self):
                return rows

        backend = MagicMock()
        backend.execute_sql.return_value = _FakeDF()
        return backend

    def test_collector_emits_per_dim_combo(self):
        """AggregationCollector with two dimensions emits one
        CollectionResult per (metric_name, dim_combo). The dim_combo
        is JSON-encoded as `dimension_value`; partition_ts is stamped
        per row from the injected qf_partition_ts column."""
        from datetime import datetime as _dt

        from qualifire.collection.aggregation import AggregationCollector
        from qualifire.core.context import QualifireContext

        backend = self._mock_backend_two_dim(
            dim_values=[("us", "shoes"), ("us", "hats"),
                        ("uk", "shoes"), ("uk", "hats")],
            metric_value=100.0,
            partition_ts="2026-04-28",
        )

        collector = AggregationCollector(
            expressions={"row_count": "COUNT(*)"},
            dimensions=["region", "product"],
            partition_ts_expr="'2026-04-28'",
        )
        crs = collector.collect(backend, "t", QualifireContext())

        # 4 dim combos × 1 metric = 4 results.
        assert len(crs) == 4

        # Every result carries the same partition_ts (constant expression
        # evaluates to a single value across the GROUP BY rows).
        for cr in crs:
            assert isinstance(cr.partition_ts, _dt)
            assert cr.partition_ts == _dt(2026, 4, 28)

        # Each dimension_value is a JSON-encoded {region, product} object.
        # Sorted-key invariant means us:hats and uk:hats produce parseable JSON.
        import json
        dim_set = {cr.dimension_value for cr in crs}
        assert len(dim_set) == 4, f"expected 4 unique dim combos, got {len(dim_set)}"
        for dim in dim_set:
            decoded = json.loads(dim)
            assert set(decoded.keys()) == {"region", "product"}
            assert decoded["region"] in ("us", "uk")
            assert decoded["product"] in ("shoes", "hats")

    def test_dimensional_history_reads_match_per_segment(self):
        """SQLite read_metric_history_by_partition should return rows
        for the requested dim combo only — verifies that the JSON
        dimension_value persisted from a 2-dim collector is treated as
        a single string key, and per-segment isolation works as
        documented."""
        from datetime import timedelta
        import json as _json

        anchor = datetime(2026, 4, 28)
        storage = SQLiteStorage(db_path=":memory:")
        storage.initialize()

        # Encode each dim combo the same way AggregationCollector does:
        # JSON-encoded with sorted keys so writes and reads match.
        def encode_dim(d: dict) -> str:
            return _json.dumps(d, sort_keys=True)

        common = {
            "run_id": "r", "run_timestamp": anchor.isoformat(),
            "owner": "o", "bu": "b",
            "dataset_name": "ds", "table_name": "t",
            "metric_name": "row_count",
            "collection_type": "aggregation",
            "validation_name": None, "validation_type": None,
            "validation_status": None, "validation_message": None,
            "notification_channel": None, "notification_status": None,
            "details_json": {},
            "record_type": "collection",
            "is_active": "true",
            "collector_name": "agg",
            "collected_at": anchor.isoformat(),
            "validated_at": None, "notified_at": None,
            "partition_ts": (anchor - timedelta(days=7)).isoformat(),
        }
        # Seed 4 rows: 2 regions × 2 products at the anchor − 7d partition.
        rows = []
        for region in ("us", "uk"):
            for product in ("shoes", "hats"):
                dim = encode_dim({"region": region, "product": product})
                rows.append({
                    **common,
                    "dimension_value": dim,
                    "metric_value": {"us-shoes": 100.0, "us-hats": 50.0,
                                      "uk-shoes": 80.0, "uk-hats": 40.0}[
                                          f"{region}-{product}"],
                })
        storage.write_results(rows)

        # Per-dim read for us+shoes: just one row, value=100.
        wanted = encode_dim({"region": "us", "product": "shoes"})
        out = storage.read_metric_history_by_partition(
            table_name="t", metric_name="row_count",
            anchor_ts=anchor, count=1, step="P7D",
            dimension_value=wanted,
        )
        assert len(out) == 1
        assert float(out[0]["metric_value"]) == 100.0
        assert out[0]["dimension_value"] == wanted

        # Per-dim read for uk+hats: just one row, value=40.
        wanted2 = encode_dim({"region": "uk", "product": "hats"})
        out2 = storage.read_metric_history_by_partition(
            table_name="t", metric_name="row_count",
            anchor_ts=anchor, count=1, step="P7D",
            dimension_value=wanted2,
        )
        assert len(out2) == 1
        assert float(out2[0]["metric_value"]) == 40.0

        # Reads with the wrong dim shape (e.g. only one key) should miss —
        # verifies dim_value is treated as an opaque string, not a partial match.
        partial = _json.dumps({"region": "us"}, sort_keys=True)
        out3 = storage.read_metric_history_by_partition(
            table_name="t", metric_name="row_count",
            anchor_ts=anchor, count=1, step="P7D",
            dimension_value=partial,
        )
        assert out3 == []
