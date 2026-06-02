"""Tests for engine collector/validator routing through all config types."""

from datetime import datetime
from unittest.mock import MagicMock

import pytest

from tests.conftest import MockDataFrame
from qualifire.core.config import (
    AggregationCollectionConfig,
    CustomQueryCollectionConfig,
    DatasetConfig,
    HistoricalCompareConfig,
    HistoricalRuleConfig,
    HistoricalThresholds,
    HistoricalValidationConfig,
    MetricsCollectionConfig,
    ProfilingCollectionConfig,
    QualifireConfig,
    RecencyCollectionConfig,
    SLOValidationConfig,
    SampleCollectionConfig,
    SampleHistoryConfig,
    ThresholdLevels,
    ThresholdRuleConfig,
    ThresholdValidationConfig,
)
from qualifire.core.context import QualifireContext
from qualifire.core.engine import QualifireEngine
from qualifire.core.exceptions import QualifireValidationError
from qualifire.core.models import Severity


def _engine(datasets, backend=None):
    backend = backend or MagicMock()
    # Use a recent timestamp so SLO checks don't fail with "too stale"
    backend.execute_sql.return_value = MockDataFrame(
        [{"max_ts": datetime.now()}]
    )
    backend.get_aggregations.return_value = {"cnt": 500, "avg": 100.0}
    backend.get_column_profile.return_value = {
        "total_count": 100, "null_count": 0, "min_value": 1, "max_value": 100
    }
    backend.get_table_metadata.return_value = {
        "columns": [{"name": "id"}, {"name": "val"}]
    }
    backend.sample_records.return_value = MockDataFrame([{"id": 1, "val": 10}])
    config = QualifireConfig(
        owner="o", bu="b", system_table="s", datasets=datasets,
    )
    return QualifireEngine(
        backend=backend, storage=None,
        context=QualifireContext(), config=config,
    )


class TestSLORouting:
    def test_slo_validation_routed(self):
        ds = DatasetConfig(
            name="ds", table="t",
            validations=[SLOValidationConfig(
                recency=RecencyCollectionConfig(strategy="max_column", column="ts"),
                thresholds={"warning": "PT4H", "error": "PT8H"},
            )],
        )
        engine = _engine([ds])
        result = engine.run()
        assert len(result.datasets[0].validation_results) == 1
        assert result.datasets[0].validation_results[0].validation_type == "slo"


class TestProfilingRouting:
    def test_profiling_collection_routed(self):
        """Profiling collection routes through ProfileEngine."""
        import pandas as pd

        pdf = pd.DataFrame({"val": [1.0, 2.0, 3.0]})
        backend = MagicMock()
        backend.execute_sql.return_value = pdf

        ds = DatasetConfig(
            name="ds", table="t",
            validations=[ThresholdValidationConfig(
                collection=ProfilingCollectionConfig(columns=["val"]),
                rules=[ThresholdRuleConfig(
                    metric="val.count",
                    thresholds=ThresholdLevels(),
                )],
            )],
        )
        config = QualifireConfig(owner="o", bu="b", system_table="s", datasets=[ds])
        engine = QualifireEngine(
            backend=backend, storage=None,
            context=QualifireContext(), config=config,
        )
        result = engine.run()
        vr = result.datasets[0].validation_results[0]
        assert vr.severity == Severity.PASS


class TestMetricsRouting:
    def test_metrics_collection_routed(self):
        backend = MagicMock()
        backend.execute_sql.return_value = MockDataFrame([{"max_ts": datetime.now()}])
        backend.get_aggregations.return_value = {"my_kpi": 42}

        ds = DatasetConfig(
            name="ds", table="t",
            validations=[ThresholdValidationConfig(
                collection=MetricsCollectionConfig(
                    metrics={"my_kpi": "SUM(revenue)"}
                ),
                rules=[ThresholdRuleConfig(
                    metric="my_kpi",
                    thresholds=ThresholdLevels(),
                )],
            )],
        )
        config = QualifireConfig(owner="o", bu="b", system_table="s", datasets=[ds])
        engine = QualifireEngine(
            backend=backend, storage=None,
            context=QualifireContext(), config=config,
        )
        result = engine.run()
        assert result.datasets[0].validation_results[0].severity == Severity.PASS
        backend.get_aggregations.assert_called()


class TestCustomQueryRouting:
    def test_custom_query_collection_routed(self):
        backend = MagicMock()
        backend.execute_sql.return_value = MockDataFrame([{"metric_a": 99}])

        ds = DatasetConfig(
            name="ds", table="t",
            validations=[ThresholdValidationConfig(
                collection=CustomQueryCollectionConfig(
                    sql="SELECT 99 AS metric_a"
                ),
                rules=[ThresholdRuleConfig(
                    metric="metric_a",
                    thresholds=ThresholdLevels(),
                )],
            )],
        )
        config = QualifireConfig(owner="o", bu="b", system_table="s", datasets=[ds])
        engine = QualifireEngine(
            backend=backend, storage=None,
            context=QualifireContext(), config=config,
        )
        result = engine.run()
        assert result.datasets[0].validation_results[0].severity == Severity.PASS


class TestSampleRouting:
    def test_sample_collection_routed(self):
        """SampleCollectionConfig routes to SamplerCollector."""
        backend = MagicMock()
        backend.sample_records.return_value = MockDataFrame([{"x": 1}])

        ds = DatasetConfig(
            name="ds", table="t",
            partition_ts="'2024-06-15'",  # anchor for sampler past slices
            partition_step="P1D",
            validations=[ThresholdValidationConfig(
                collection=SampleCollectionConfig(
                    n_records=10,
                    # slice_column + slice_value drive anchor-mode
                    # shape / pattern history.
                    slice_column="event_dt",
                    slice_value="{{ ds }}",
                    history=SampleHistoryConfig(past_dates=1, step="P1D"),
                ),
                rules=[ThresholdRuleConfig(
                    metric="sample",
                    thresholds=ThresholdLevels(),
                )],
            )],
        )
        engine = _engine([ds], backend=backend)

        # Sample collection emits a "sample" metric with metric_value=None.
        # Post NULL-safe coercion (threshold.py), the threshold validator
        # surfaces this as a structured WARNING (its on_empty_data default)
        # rather than crashing — engine.run() now returns instead of raising.
        result = engine.run()

        # Verify sample_records was called (collection routing worked)
        backend.sample_records.assert_called()

        # Verify the result reflects the NULL-safe path: a WARNING result
        # rather than an ERROR or an unhandled exception.
        vrs = result.datasets[0].validation_results
        assert vrs, "expected at least one validation result"
        assert vrs[0].severity == Severity.WARNING
        assert "NULL" in vrs[0].message or "no data" in vrs[0].message.lower()


class TestHistoricalRouting:
    def test_historical_validation_routed(self):
        ds = DatasetConfig(
            name="ds", table="t",
            partition_ts="'2024-01-04'",
            partition_step="P1D",
            validations=[HistoricalValidationConfig(
                collection=AggregationCollectionConfig(
                    expressions={"avg": "AVG(x)"}
                ),
                rules=[HistoricalRuleConfig(
                    metric="avg",
                    compare=HistoricalCompareConfig(past_values=3, step="P1D"),
                    thresholds=HistoricalThresholds(warning={"deviation_pct": {"min": -20, "max": 20}}),
                )],
            )],
        )
        engine = _engine([ds])
        # Override _engine's default after the fact: aggregation collector
        # injects ``qf_partition_ts`` into the SELECT when
        # DatasetConfig.partition_ts is set, so the mock must return it
        # alongside the metric. Without this the drift validator surfaces
        # a missing-partition_ts ERROR before exercising the routing path.
        engine.backend.get_aggregations.return_value = {
            "avg": 100.0,
            "qf_partition_ts": "2024-01-04",
        }
        result = engine.run()
        vr = result.datasets[0].validation_results[0]
        assert vr.validation_type == "drift"
