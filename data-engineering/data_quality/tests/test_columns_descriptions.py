"""Tests for the new system-table columns + description fields:

- partition_ts present on every persisted row (run-level fallback for
  non-aggregation flows like SLO).
- expected_value / actual_value_text columns populated.
- dataset_description / validation_description stamped from configs.
- dimension_value persisted as NULL (not "_default") for non-dimensional rows.
- SLO actual_value_text is an ISO-8601 duration; metric_value is numeric seconds.
"""

from __future__ import annotations

from datetime import datetime, timedelta, date

import pytest


def _make_engine_with_sqlite(tmp_path, dataset_config, backend_factory):
    """Build a Qualifire engine wired to a SQLite system table."""
    from qualifire.core.config import QualifireConfig
    from qualifire.core.context import QualifireContext
    from qualifire.core.engine import QualifireEngine
    from qualifire.storage.sqlite_storage import SQLiteStorage

    storage = SQLiteStorage(db_path=str(tmp_path / "qf.db"))
    storage.initialize()
    cfg = QualifireConfig(
        owner="o", bu="b", system_table="qf",
        datasets=[dataset_config],
    )
    backend = backend_factory()
    engine = QualifireEngine(
        backend=backend, storage=storage,
        context=QualifireContext(), config=cfg,
    )
    return engine, storage


class TestSloFreshnessColumns:
    def test_actual_value_text_is_iso_duration(self, tmp_path):
        """SLO emits ISO-8601 duration in actual_value_text and numeric seconds
        in metric_value. The system-table view shows both."""
        from unittest.mock import MagicMock

        from qualifire.api import Qualifire
        from qualifire.backends.spark_backend import SparkBackend
        from qualifire.core.config import (
            DatasetConfig,
            RecencyCollectionConfig,
            SLOValidationConfig,
        )
        from qualifire.core.exceptions import QualifireValidationError

        # Synth a backend that returns a recency timestamp 25h in the past.
        # RecencyCollector uses execute_sql(...).collect()[0][0], so the mock
        # needs to return that shape.
        ts_25h_ago = datetime.now() - timedelta(hours=25)

        class _FakeRow:
            def __init__(self, value):
                self._value = value
            def __getitem__(self, idx):
                return self._value

        class _FakeDF:
            def __init__(self, value):
                self._value = value
            def collect(self):
                return [_FakeRow(self._value)]

        backend = MagicMock()
        backend.execute_sql.return_value = _FakeDF(ts_25h_ago.isoformat())
        backend.get_aggregations.return_value = {
            "row_count": 1,
            "qf_partition_ts": "2026-04-28",
        }

        from qualifire.core.context import QualifireContext
        from qualifire.core.engine import QualifireEngine
        from qualifire.core.config import QualifireConfig
        from qualifire.storage.sqlite_storage import SQLiteStorage

        storage = SQLiteStorage(db_path=str(tmp_path / "qf.db"))
        storage.initialize()
        ds = DatasetConfig(
            table="t",
            description="Sales fact, refreshed by the daily ETL.",
            partition_ts="'2026-04-28'",
            partition_step="P1D",
            validations=[SLOValidationConfig(
                description="Freshness SLO: data must be no older than 24h.",
                recency=RecencyCollectionConfig(strategy="max_column", column="updated_at"),
                thresholds={"warning": "PT24H", "error": "PT48H"},
                name="slo_freshness",
            )],
        )
        cfg = QualifireConfig(owner="o", bu="b", system_table="qf", datasets=[ds])
        engine = QualifireEngine(
            backend=backend, storage=storage,
            context=QualifireContext(), config=cfg,
        )
        try:
            engine.run()
        except QualifireValidationError:
            pass  # WARNING expected; we're inspecting persisted rows

        conn = storage._get_conn()
        cur = conn.execute(
            "SELECT metric_value, actual_value_text, dataset_description, "
            "validation_description, partition_ts, dimension_value "
            "FROM qualifire_history "
            "WHERE record_type='validation' AND validation_type='slo'"
        )
        row = dict(zip([d[0] for d in cur.description], cur.fetchone()))
        # Numeric seconds (24h+ → 86400+ seconds)
        assert float(row["metric_value"]) > 86400
        # ISO-8601 duration: starts with P, has T separator, day or hour component
        assert row["actual_value_text"].startswith("P")
        assert "T" in row["actual_value_text"]
        # Descriptions stamped through
        assert row["dataset_description"] == "Sales fact, refreshed by the daily ETL."
        assert row["validation_description"] == "Freshness SLO: data must be no older than 24h."
        # partition_ts stamped on the SLO validation row from the run-level fallback
        assert row["partition_ts"] == "2026-04-28T00:00:00"
        # dimension_value is NULL (no dimension configured)
        assert row["dimension_value"] is None


class TestDimensionValueNullPersistence:
    def test_non_dimensional_row_persists_null(self, tmp_path):
        """A non-dimensional aggregation must persist dimension_value
        as SQL NULL. There is no ``"_default"`` sentinel — read APIs
        accept ``None`` and use NULL-safe equality."""
        from unittest.mock import MagicMock

        from qualifire.core.config import (
            AggregationCollectionConfig,
            DatasetConfig,
            QualifireConfig,
            ThresholdLevels,
            ThresholdRuleConfig,
            ThresholdValidationConfig,
        )
        from qualifire.core.context import QualifireContext
        from qualifire.core.engine import QualifireEngine
        from qualifire.storage.sqlite_storage import SQLiteStorage

        backend = MagicMock()
        backend.get_aggregations.return_value = {"row_count": 100}
        storage = SQLiteStorage(db_path=str(tmp_path / "qf.db"))
        storage.initialize()
        ds = DatasetConfig(
            table="t",
            validations=[ThresholdValidationConfig(
                collection=AggregationCollectionConfig(expressions={"row_count": "COUNT(*)"}),
                rules=[ThresholdRuleConfig(
                    metric="row_count",
                    thresholds=ThresholdLevels(error={"min": 1}),
                )],
            )],
        )
        cfg = QualifireConfig(owner="o", bu="b", system_table="qf", datasets=[ds])
        QualifireEngine(
            backend=backend, storage=storage,
            context=QualifireContext(), config=cfg,
        ).run()

        conn = storage._get_conn()
        cur = conn.execute(
            "SELECT record_type, dimension_value FROM qualifire_history"
        )
        for row in cur.fetchall():
            # Every row from this non-dimensional dataset must persist NULL
            # (sqlite3 returns Python None).
            assert row[1] is None, (
                f"row_type={row[0]} stored dimension_value={row[1]!r} but should be NULL"
            )


class TestPartitionTsRunLevelFallback:
    def test_validation_row_inherits_run_level_partition_ts(self, tmp_path):
        """Even when a validation row has no matching CollectionResult
        partition_ts (e.g. SLO recency or empty-data path), the run-level
        partition_ts stamps every persisted row from that dataset."""
        from unittest.mock import MagicMock

        from qualifire.api import Qualifire
        from qualifire.core.config import (
            AggregationCollectionConfig,
            DatasetConfig,
            QualifireConfig,
            ThresholdLevels,
            ThresholdRuleConfig,
            ThresholdValidationConfig,
        )
        from qualifire.core.context import QualifireContext
        from qualifire.core.engine import QualifireEngine
        from qualifire.storage.sqlite_storage import SQLiteStorage

        backend = MagicMock()
        # Aggregation produces metric AND qf_partition_ts (per-row stamp).
        backend.get_aggregations.return_value = {
            "row_count": 5,
            "qf_partition_ts": "2026-04-28",
        }
        storage = SQLiteStorage(db_path=str(tmp_path / "qf.db"))
        storage.initialize()
        ds = DatasetConfig(
            table="t",
            partition_ts="'2026-04-28'",
            partition_step="P1D",
            validations=[ThresholdValidationConfig(
                collection=AggregationCollectionConfig(expressions={"row_count": "COUNT(*)"}),
                rules=[ThresholdRuleConfig(
                    metric="row_count",
                    thresholds=ThresholdLevels(error={"min": 1}),
                )],
            )],
        )
        cfg = QualifireConfig(owner="o", bu="b", system_table="qf", datasets=[ds])
        QualifireEngine(
            backend=backend, storage=storage,
            context=QualifireContext(), config=cfg,
        ).run()

        conn = storage._get_conn()
        rows = conn.execute(
            "SELECT record_type, partition_ts FROM qualifire_history "
            "WHERE record_type IN ('collection', 'validation')"
        ).fetchall()
        assert rows, "no rows persisted"
        for record_type, partition_ts in rows:
            assert partition_ts == "2026-04-28T00:00:00", (
                f"{record_type} row missing partition_ts (got {partition_ts!r})"
            )


class TestExpectedValueColumn:
    def test_threshold_expected_value_persisted_as_json(self, tmp_path):
        """Threshold validator's expected_value (the warning/error bounds)
        lands in the expected_value column as a JSON-serialized object."""
        from unittest.mock import MagicMock
        import json

        from qualifire.core.config import (
            AggregationCollectionConfig,
            DatasetConfig,
            QualifireConfig,
            ThresholdLevels,
            ThresholdRuleConfig,
            ThresholdValidationConfig,
        )
        from qualifire.core.context import QualifireContext
        from qualifire.core.engine import QualifireEngine
        from qualifire.storage.sqlite_storage import SQLiteStorage

        backend = MagicMock()
        backend.get_aggregations.return_value = {"row_count": 50}
        storage = SQLiteStorage(db_path=str(tmp_path / "qf.db"))
        storage.initialize()
        ds = DatasetConfig(
            table="t",
            validations=[ThresholdValidationConfig(
                collection=AggregationCollectionConfig(expressions={"row_count": "COUNT(*)"}),
                rules=[ThresholdRuleConfig(
                    metric="row_count",
                    thresholds=ThresholdLevels(
                        warning={"min": 100}, error={"min": 10},
                    ),
                )],
            )],
        )
        cfg = QualifireConfig(owner="o", bu="b", system_table="qf", datasets=[ds])
        QualifireEngine(
            backend=backend, storage=storage,
            context=QualifireContext(), config=cfg,
        ).run()

        conn = storage._get_conn()
        row = conn.execute(
            "SELECT expected_value FROM qualifire_history "
            "WHERE record_type='validation' AND metric_name='row_count' "
            "LIMIT 1"
        ).fetchone()
        assert row is not None
        loaded = json.loads(row[0])
        assert "warning" in loaded
        assert "error" in loaded
        assert loaded["error"]["min"] == 10
