"""Tests for all collection strategies."""

from datetime import datetime
from unittest.mock import MagicMock

import pytest

from tests.conftest import MockDataFrame, MockRow
from qualifire.core.context import QualifireContext


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_backend(**overrides):
    backend = MagicMock()
    backend.execute_sql.return_value = MockDataFrame()
    backend.get_aggregations.return_value = {}
    backend.get_column_profile.return_value = {}
    backend.get_table_metadata.return_value = {"columns": [], "partitions": []}
    backend.sample_records.return_value = MockDataFrame()
    backend.get_delta_history.return_value = None
    for k, v in overrides.items():
        setattr(backend, k, v) if not callable(v) else None
        if callable(v):
            getattr(backend, k).return_value = v()
    return backend


# ===========================================================================
# RecencyCollector
# ===========================================================================


class TestRecencyCollector:
    def test_max_column_returns_datetime(self):
        from qualifire.collection.recency import RecencyCollector

        ts = datetime(2024, 6, 15, 10, 0, 0)
        backend = MagicMock()
        backend.execute_sql.return_value = MockDataFrame([{"max_ts": ts}])

        collector = RecencyCollector(strategy="max_column", column="updated_at")
        ctx = QualifireContext()
        results = collector.collect(backend, "db.table", ctx)

        assert len(results) == 1
        assert results[0].metric_name == "recency"
        assert results[0].metric_value == ts
        # Verify the SQL contains MAX
        sql_arg = backend.execute_sql.call_args[0][0]
        assert "MAX(`updated_at`)" in sql_arg

    def test_max_column_with_filter(self):
        from qualifire.collection.recency import RecencyCollector

        ts = datetime(2024, 6, 15, 10, 0, 0)
        backend = MagicMock()
        backend.execute_sql.return_value = MockDataFrame([{"max_ts": ts}])

        collector = RecencyCollector(
            strategy="max_column", column="ts", filter_expr="date = '{{ ds }}'"
        )
        ctx = QualifireContext(extra_context={"ds": "2024-06-15"})
        collector.collect(backend, "t", ctx)

        sql_arg = backend.execute_sql.call_args[0][0]
        assert "WHERE date = '2024-06-15'" in sql_arg

    def test_max_column_null_raises(self):
        from qualifire.collection.recency import RecencyCollector

        backend = MagicMock()
        backend.execute_sql.return_value = MockDataFrame([{"max_ts": None}])

        collector = RecencyCollector(strategy="max_column", column="updated_at")
        with pytest.raises(ValueError, match="No data found"):
            collector.collect(backend, "db.table", QualifireContext())

    def test_max_column_string_timestamp(self):
        from qualifire.collection.recency import RecencyCollector

        backend = MagicMock()
        backend.execute_sql.return_value = MockDataFrame(
            [{"max_ts": "2024-06-15T10:00:00"}]
        )

        collector = RecencyCollector(strategy="max_column", column="ts")
        results = collector.collect(backend, "t", QualifireContext())
        assert results[0].metric_value == datetime(2024, 6, 15, 10, 0, 0)

    def test_delta_log_strategy(self):
        from qualifire.collection.recency import RecencyCollector

        ts = datetime(2024, 6, 15, 12, 0, 0)
        history_df = MockDataFrame([{"timestamp": ts}])
        backend = MagicMock()
        backend.get_delta_history.return_value = history_df

        collector = RecencyCollector(strategy="delta_log")
        results = collector.collect(backend, "t", QualifireContext())
        assert results[0].metric_value == ts

    def test_delta_log_no_history_raises(self):
        from qualifire.collection.recency import RecencyCollector

        backend = MagicMock()
        backend.get_delta_history.return_value = None

        collector = RecencyCollector(strategy="delta_log")
        with pytest.raises(ValueError, match="no Delta history"):
            collector.collect(backend, "t", QualifireContext())

    def test_metadata_strategy(self):
        from qualifire.collection.recency import RecencyCollector

        ts = datetime(2024, 6, 15, 8, 0, 0)
        backend = MagicMock()
        backend.get_table_metadata.return_value = {"last_modified": ts}

        collector = RecencyCollector(strategy="metadata")
        results = collector.collect(backend, "t", QualifireContext())
        assert results[0].metric_value == ts

    def test_metadata_missing_raises(self):
        from qualifire.collection.recency import RecencyCollector

        backend = MagicMock()
        backend.get_table_metadata.return_value = {}

        collector = RecencyCollector(strategy="metadata")
        with pytest.raises(ValueError, match="No last_modified"):
            collector.collect(backend, "t", QualifireContext())

    def test_custom_sql_strategy(self):
        from qualifire.collection.recency import RecencyCollector

        ts = datetime(2024, 6, 15, 9, 0, 0)
        backend = MagicMock()
        backend.execute_sql.return_value = MockDataFrame([{"result": ts}])

        collector = RecencyCollector(
            strategy="custom_sql",
            sql="SELECT MAX(ts) FROM my_table WHERE active = 1",
        )
        results = collector.collect(backend, "my_table", QualifireContext())
        assert results[0].metric_value == ts

    def test_custom_sql_null_raises(self):
        from qualifire.collection.recency import RecencyCollector

        backend = MagicMock()
        backend.execute_sql.return_value = MockDataFrame([{"result": None}])

        collector = RecencyCollector(strategy="custom_sql", sql="SELECT NULL")
        with pytest.raises(ValueError, match="Custom SQL returned NULL"):
            collector.collect(backend, "t", QualifireContext())

    def test_unknown_strategy_raises(self):
        from qualifire.collection.recency import RecencyCollector

        backend = MagicMock()
        collector = RecencyCollector(strategy="unknown")
        with pytest.raises(ValueError, match="Unknown recency strategy"):
            collector.collect(backend, "t", QualifireContext())


# ===========================================================================
# AggregationCollector
# ===========================================================================


class TestAggregationCollector:
    def test_basic_aggregation(self):
        from qualifire.collection.aggregation import AggregationCollector

        backend = MagicMock()
        backend.get_aggregations.return_value = {"avg_sales": 150.0, "row_count": 1000}

        collector = AggregationCollector(
            expressions={"avg_sales": "AVG(sales)", "row_count": "COUNT(*)"}
        )
        results = collector.collect(backend, "db.sales", QualifireContext())

        assert len(results) == 2
        names = {r.metric_name for r in results}
        assert names == {"avg_sales", "row_count"}
        assert results[0].metadata["collection_type"] == "aggregation"

    def test_jinja_in_expressions(self):
        from qualifire.collection.aggregation import AggregationCollector

        backend = MagicMock()
        backend.get_aggregations.return_value = {"cnt": 42}

        collector = AggregationCollector(expressions={"cnt": "COUNT(*)"})
        ctx = QualifireContext(extra_context={"ds": "2024-01-01"})
        collector.collect(backend, "t", ctx)

        # get_aggregations should have been called
        backend.get_aggregations.assert_called_once()

    def test_with_filter(self):
        from qualifire.collection.aggregation import AggregationCollector

        backend = MagicMock()
        backend.get_aggregations.return_value = {"cnt": 10}

        collector = AggregationCollector(
            expressions={"cnt": "COUNT(*)"},
            filter_expr="region = 'US'",
        )
        collector.collect(backend, "t", QualifireContext())

        _, kwargs = backend.get_aggregations.call_args
        # filter_expr is passed as the third positional arg
        args = backend.get_aggregations.call_args[0]
        assert args[2] == "region = 'US'"

    def test_collect_with_dimension(self):
        """Dimension-aware collection produces one result per (metric, dimension_value)."""
        from qualifire.collection.aggregation import AggregationCollector

        backend = MagicMock()
        backend.execute_sql.return_value = MockDataFrame([
            {"region": "US", "row_count": 100},
            {"region": "EU", "row_count": 80},
            {"region": "APAC", "row_count": 50},
        ])

        collector = AggregationCollector(
            expressions={"row_count": "COUNT(*)"},
            dimensions=["region"],
        )
        results = collector.collect(backend, "t", QualifireContext())

        assert len(results) == 3
        by_dim = {r.dimension_value: r.metric_value for r in results}
        assert by_dim == {'{"region":"US"}': 100, '{"region":"EU"}': 80, '{"region":"APAC"}': 50}
        assert all(r.metric_name == "row_count" for r in results)
        assert all(r.metadata.get("dimensions") == ["region"] for r in results)

        # Verify SQL has GROUP BY
        sql_arg = backend.execute_sql.call_args[0][0]
        assert "GROUP BY" in sql_arg
        assert "`region`" in sql_arg

    def test_collect_with_dimension_multiple_metrics(self):
        """Multiple aggregation expressions with dimension produce one result per (metric, dim)."""
        from qualifire.collection.aggregation import AggregationCollector

        backend = MagicMock()
        backend.execute_sql.return_value = MockDataFrame([
            {"date": "2024-01-01", "cnt": 10, "total": 500},
            {"date": "2024-01-02", "cnt": 15, "total": 750},
        ])

        collector = AggregationCollector(
            expressions={"cnt": "COUNT(*)", "total": "SUM(amount)"},
            dimensions=["date"],
        )
        results = collector.collect(backend, "t", QualifireContext())

        # 2 dates x 2 metrics = 4 results
        assert len(results) == 4
        names = {r.metric_name for r in results}
        assert names == {"cnt", "total"}
        dims = {r.dimension_value for r in results}
        assert dims == {'{"date":"2024-01-01"}', '{"date":"2024-01-02"}'}

    def test_collect_with_dimension_empty_result(self):
        """Empty query result with dimension returns no results."""
        from qualifire.collection.aggregation import AggregationCollector

        backend = MagicMock()
        backend.execute_sql.return_value = MockDataFrame([])

        collector = AggregationCollector(
            expressions={"cnt": "COUNT(*)"},
            dimensions=["region"],
        )
        results = collector.collect(backend, "t", QualifireContext())
        assert results == []


# ===========================================================================
# ProfilingCollector
# ===========================================================================


class TestProfilingCollector:
    def test_profiles_specified_columns(self):
        """ProfilingCollector profiles only the requested columns."""
        import pandas as pd
        from qualifire.collection.profiler import ProfilingCollector

        df = pd.DataFrame({"price": [10.0, 20.0, 30.0], "quantity": [1, 2, 3], "name": ["a", "b", "c"]})
        backend = MagicMock()
        backend.execute_sql.return_value = df

        collector = ProfilingCollector(columns=["price", "quantity"])
        results = collector.collect(backend, "t", QualifireContext())

        col_names = {r.metadata["column"] for r in results}
        assert col_names == {"price", "quantity"}
        assert len(results) > 0
        # Should have stats like count, null_count, min, max, etc.
        metric_names = {r.metric_name for r in results}
        assert "price.count" in metric_names
        assert "quantity.min" in metric_names

    def test_auto_discovers_columns(self):
        """Without columns specified, profiles all columns."""
        import pandas as pd
        from qualifire.collection.profiler import ProfilingCollector

        df = pd.DataFrame({"id": [1, 2], "value": [10.0, 20.0]})
        backend = MagicMock()
        backend.execute_sql.return_value = df

        collector = ProfilingCollector()
        results = collector.collect(backend, "t", QualifireContext())

        col_names = {r.metadata["column"] for r in results}
        assert col_names == {"id", "value"}

    def test_filters_stats(self):
        """Stat filter returns only the requested stat names."""
        import pandas as pd
        from qualifire.collection.profiler import ProfilingCollector

        df = pd.DataFrame({"col_a": [1.0, 2.0, None]})
        backend = MagicMock()
        backend.execute_sql.return_value = df

        collector = ProfilingCollector(columns=["col_a"], stats=["null_count"])
        results = collector.collect(backend, "t", QualifireContext())

        assert len(results) == 1
        assert results[0].metric_name == "col_a.null_count"
        assert results[0].metric_value == 1

    def test_metadata_includes_column_type(self):
        """Results include column_type in metadata."""
        import pandas as pd
        from qualifire.collection.profiler import ProfilingCollector

        df = pd.DataFrame({"amount": [10.0], "name": ["alice"]})
        backend = MagicMock()
        backend.execute_sql.return_value = df

        collector = ProfilingCollector(stats=["count"])
        results = collector.collect(backend, "t", QualifireContext())

        by_col = {r.metadata["column"]: r.metadata["column_type"] for r in results}
        assert by_col["amount"] == "numeric"
        assert by_col["name"] == "string"

    def test_dimension_field_accepted_but_unused(self):
        """ProfilingCollectionConfig accepts dimension but ProfilingCollector ignores it."""
        from qualifire.core.config import ProfilingCollectionConfig
        from qualifire.collection.profiler import ProfilingCollector
        import pandas as pd

        # Config accepts dimensions
        config = ProfilingCollectionConfig(
            columns=["amount"], stats=["count"], dimensions=["region"]
        )
        assert config.dimensions == ["region"]

        # ProfilingCollector does not accept dimension — it profiles all rows
        df = pd.DataFrame({"amount": [10.0, 20.0], "region": ["US", "EU"]})
        backend = MagicMock()
        backend.execute_sql.return_value = df

        collector = ProfilingCollector(columns=["amount"], stats=["count"])
        results = collector.collect(backend, "t", QualifireContext())

        # Non-dimensional collection: dimension_value stays None (persisted
        # as NULL; NULL-safe reads (`IS ?` on SQLite) match by NULL.
        assert len(results) == 1
        assert results[0].dimension_value is None


# ===========================================================================
# MetricsCollector
# ===========================================================================


class TestMetricsCollector:
    def test_named_metrics(self):
        from qualifire.collection.metrics import MetricsCollector

        backend = MagicMock()
        backend.get_aggregations.return_value = {
            "conversion_rate": 0.035,
            "total_revenue": 50000.0,
        }

        collector = MetricsCollector(
            metrics={
                "conversion_rate": "SUM(conversions) / SUM(visits)",
                "total_revenue": "SUM(revenue)",
            }
        )
        results = collector.collect(backend, "t", QualifireContext())

        assert len(results) == 2
        by_name = {r.metric_name: r for r in results}
        assert by_name["conversion_rate"].metric_value == 0.035
        assert by_name["total_revenue"].metric_value == 50000.0
        assert by_name["conversion_rate"].metadata["collection_type"] == "metrics"

    def test_with_filter(self):
        from qualifire.collection.metrics import MetricsCollector

        backend = MagicMock()
        backend.get_aggregations.return_value = {"cnt": 5}

        collector = MetricsCollector(
            metrics={"cnt": "COUNT(*)"},
            filter_expr="status = 'active'",
        )
        collector.collect(backend, "t", QualifireContext())

        args = backend.get_aggregations.call_args[0]
        assert args[2] == "status = 'active'"

    def test_collect_with_dimension(self):
        """Dimension-aware metrics collection produces per-dimension results."""
        from qualifire.collection.metrics import MetricsCollector

        backend = MagicMock()
        backend.execute_sql.return_value = MockDataFrame([
            {"category": "A", "revenue": 1000},
            {"category": "B", "revenue": 2000},
        ])

        collector = MetricsCollector(
            metrics={"revenue": "SUM(amount)"},
            dimensions=["category"],
        )
        results = collector.collect(backend, "t", QualifireContext())

        assert len(results) == 2
        by_dim = {r.dimension_value: r.metric_value for r in results}
        assert by_dim == {'{"category":"A"}': 1000, '{"category":"B"}': 2000}
        assert all(r.metric_name == "revenue" for r in results)
        assert all(r.metadata.get("dimensions") == ["category"] for r in results)


# ===========================================================================
# SamplerCollector
# ===========================================================================


class TestSamplerCollector:
    def test_and_combine_treats_whitespace_as_empty(self):
        """Sampler-side wiring regression for the AND-combine
        helper promotion. The pre-move local ``_and_combine``
        truthiness-skipped only empty strings; whitespace-only
        operands stayed truthy and composed to
        ``(   ) AND (...)``. The shared
        ``and_combine_filters`` (alias-imported by the sampler
        as ``_and_combine``) normalises whitespace too — pin
        the new behaviour from the sampler's import path so a
        future "restore the local helper" change can't silently
        re-break it.
        """
        from qualifire.collection.sampler import _and_combine

        assert _and_combine("region = 'us'", "   ") == "region = 'us'"
        assert _and_combine("\t", "x = 1") == "x = 1"
        assert _and_combine("  ", "  ") is None

    def test_samples_current_and_past(self):
        """``slice_column`` + ``slice_value`` auto-generates the
        per-slice predicate. Past anchors are
        ``slice_value − k·step``, not wall-clock — backfills and
        notebook re-runs pick the right slice."""
        from qualifire.collection.sampler import SamplerCollector

        backend = MagicMock()
        backend.sample_records.return_value = MockDataFrame([{"a": 1}])

        collector = SamplerCollector(
            n_records=100,
            slice_column="date",
            slice_value="{{ ds }}",
            past_dates=2,
            step="P7D",
        )
        ctx = QualifireContext(extra_context={"ds": "2024-06-15"})
        results = collector.collect(backend, "t", ctx)

        assert len(results) == 1
        assert results[0].metric_name == "sample"
        meta = results[0].metadata
        assert meta["n_records"] == 100
        assert len(meta["past_dfs"]) == 2
        assert backend.sample_records.call_count == 3
        all_filters = [
            call.args[2] for call in backend.sample_records.call_args_list
        ]
        assert all_filters == [
            "date = '2024-06-15'",
            "date = '2024-06-08'",
            "date = '2024-06-01'",
        ]

    def test_anchor_mode_requires_step(self):
        """Without ``step`` the anchor history can't compute past
        slices."""
        from qualifire.collection.sampler import SamplerCollector
        from qualifire.core.exceptions import QualifireConfigError

        backend = MagicMock()
        backend.sample_records.return_value = MockDataFrame()

        collector = SamplerCollector(
            n_records=100,
            slice_column="event_dt",
            slice_value="2024-06-15",
            past_dates=2,
            # no step
        )
        with pytest.raises(QualifireConfigError, match=r"P7D|ISO 8601"):
            collector.collect(backend, "t", QualifireContext())

    def test_slice_value_unparseable_raises(self):
        """``slice_value`` must render to an ISO date / datetime so
        the sampler can compute past anchors."""
        from qualifire.collection.sampler import SamplerCollector
        from qualifire.core.exceptions import MissingPartitionAnchorError

        backend = MagicMock()
        backend.sample_replicate = MockDataFrame()
        backend.sample_records.return_value = MockDataFrame()

        collector = SamplerCollector(
            n_records=10,
            slice_column="event_dt",
            slice_value="v42",  # not ISO-shaped
            past_dates=1,
            step="P1D",
        )
        with pytest.raises(
            MissingPartitionAnchorError, match="slice_value"
        ):
            collector.collect(backend, "t", QualifireContext())

    def test_subday_step_emits_full_timestamp(self):
        """Sub-day cadences (PT1H) emit the full ISO timestamp so
        TIMESTAMP columns match exactly."""
        from qualifire.collection.sampler import SamplerCollector

        backend = MagicMock()
        backend.sample_records.return_value = MockDataFrame()

        collector = SamplerCollector(
            n_records=10,
            slice_column="event_ts",
            slice_value="2024-06-15T12:00:00",
            past_dates=2,
            step="PT1H",
        )
        collector.collect(backend, "t", QualifireContext())

        all_filters = [
            call.args[2] for call in backend.sample_records.call_args_list
        ]
        assert all_filters == [
            "event_ts = '2024-06-15T12:00:00'",
            "event_ts = '2024-06-15T11:00:00'",
            "event_ts = '2024-06-15T10:00:00'",
        ]

    def test_explicit_history_filters(self):
        from qualifire.collection.sampler import SamplerCollector

        backend = MagicMock()
        backend.sample_records.return_value = MockDataFrame([{"x": 1}])

        collector = SamplerCollector(
            n_records=50,
            history_filters=["date = '2024-06-08'", "date = '2024-06-01'"],
        )
        results = collector.collect(backend, "t", QualifireContext())

        meta = results[0].metadata
        assert len(meta["past_dfs"]) == 2
        assert meta["past_dfs"][0][0] == "past_1"
        assert meta["past_dfs"][1][0] == "past_2"

    def test_no_anchor_no_history_filters_raises(self):
        """Anchor mode without ``slice_column`` would draw the entire
        table for every past slice — refuse rather than producing
        AUC ≈ 0.5 nonsense."""
        from qualifire.collection.sampler import SamplerCollector
        from qualifire.core.exceptions import MissingPartitionAnchorError

        backend = MagicMock()
        backend.sample_records.return_value = MockDataFrame()

        collector = SamplerCollector(
            n_records=500,
            past_dates=1,
            step="P1D",
        )
        with pytest.raises(MissingPartitionAnchorError, match="slice_column"):
            collector.collect(backend, "t", QualifireContext())

    def test_filter_expr_and_combines_with_each_slice(self):
        """Dataset-level ``filter_expr`` applies unchanged to current
        and every past slice."""
        from qualifire.collection.sampler import SamplerCollector

        backend = MagicMock()
        backend.sample_records.return_value = MockDataFrame()

        collector = SamplerCollector(
            n_records=10,
            slice_column="event_dt",
            slice_value="2024-06-15",
            filter_expr="region = 'us'",
            past_dates=1,
            step="P1D",
        )
        collector.collect(backend, "t", QualifireContext())

        all_filters = [
            call.args[2] for call in backend.sample_records.call_args_list
        ]
        assert all_filters == [
            "(event_dt = '2024-06-15') AND (region = 'us')",
            "(event_dt = '2024-06-14') AND (region = 'us')",
        ]

    def test_filter_expr_alone_when_no_anchor(self):
        """``filter_expr`` alone (no slice_column) produces a bare
        WHERE — useful for past_dates=0 ad-hoc sampling."""
        from qualifire.collection.sampler import SamplerCollector

        backend = MagicMock()
        backend.sample_records.return_value = MockDataFrame()

        collector = SamplerCollector(
            n_records=10,
            filter_expr="region = 'us'",
            past_dates=0,
        )
        collector.collect(backend, "t", QualifireContext())

        assert (
            backend.sample_records.call_args_list[0].args[2]
            == "region = 'us'"
        )

    def test_slice_column_and_slice_value_must_be_set_together(self):
        """One without the other is incoherent — reject at construct
        time."""
        from qualifire.collection.sampler import SamplerCollector

        with pytest.raises(ValueError, match="set together"):
            SamplerCollector(slice_column="event_dt")
        with pytest.raises(ValueError, match="set together"):
            SamplerCollector(slice_value="{{ ds }}")

    def test_filter_expr_is_jinja_rendered(self):
        """``DatasetConfig.filter`` may contain Jinja (``region =
        '{{ params.region }}'``) — the sampler must render it before
        AND-combining, otherwise the predicate reaches the backend
        unrendered (R3 review fix)."""
        from qualifire.collection.sampler import SamplerCollector

        backend = MagicMock()
        backend.sample_records.return_value = MockDataFrame()

        collector = SamplerCollector(
            n_records=10,
            slice_column="event_dt",
            slice_value="{{ ds }}",
            filter_expr="region = '{{ region }}'",
            past_dates=1,
            step="P1D",
        )
        ctx = QualifireContext(extra_context={
            "ds": "2024-06-15", "region": "us"
        })
        collector.collect(backend, "t", ctx)

        all_filters = [
            call.args[2] for call in backend.sample_records.call_args_list
        ]
        assert all_filters == [
            "(event_dt = '2024-06-15') AND (region = 'us')",
            "(event_dt = '2024-06-14') AND (region = 'us')",
        ]

    def test_date_only_slice_value_keeps_date_format_for_past(self):
        """When ``slice_value`` renders to a bare ISO date, past
        slices must also emit ISO date — otherwise current /
        DATE-typed column compares against a TIMESTAMP literal and
        the past slice silently returns zero rows (R3 review fix)."""
        from qualifire.collection.sampler import SamplerCollector

        backend = MagicMock()
        backend.sample_records.return_value = MockDataFrame()

        collector = SamplerCollector(
            n_records=10,
            slice_column="event_dt",
            slice_value="2024-06-15",   # bare ISO date
            past_dates=1,
            step="P7D",
        )
        collector.collect(backend, "t", QualifireContext())

        all_filters = [
            call.args[2] for call in backend.sample_records.call_args_list
        ]
        assert all_filters == [
            "event_dt = '2024-06-15'",
            "event_dt = '2024-06-08'",
        ]

    def test_history_filters_bypass_filter_expr(self):
        """``history_filters`` is the verbatim escape hatch; the
        operator owns the full WHERE clause and ``filter_expr`` is
        deliberately not applied."""
        from qualifire.collection.sampler import SamplerCollector

        backend = MagicMock()
        backend.sample_records.return_value = MockDataFrame()

        collector = SamplerCollector(
            n_records=10,
            filter_expr="region = 'us'",
            history_filters=["version = 'v3'"],
        )
        collector.collect(backend, "t", QualifireContext())

        # Past call must NOT have region appended.
        past_filter = backend.sample_records.call_args_list[1].args[2]
        assert past_filter == "version = 'v3'"


# ===========================================================================
# CustomQueryCollector
# ===========================================================================


class TestCustomQueryCollector:
    def test_returns_columns_as_results(self):
        from qualifire.collection.custom_query import CustomQueryCollector

        backend = MagicMock()
        backend.execute_sql.return_value = MockDataFrame(
            [{"total": 42, "avg_price": 9.99}]
        )

        collector = CustomQueryCollector(
            sql="SELECT COUNT(*) AS total, AVG(price) AS avg_price FROM {{ table }}"
        )
        results = collector.collect(backend, "my_table", QualifireContext())

        assert len(results) == 2
        by_name = {r.metric_name: r for r in results}
        assert by_name["total"].metric_value == 42
        assert by_name["avg_price"].metric_value == 9.99

        # Verify {{ table }} was rendered
        sql_arg = backend.execute_sql.call_args[0][0]
        assert "my_table" in sql_arg

    def test_empty_result(self):
        from qualifire.collection.custom_query import CustomQueryCollector

        backend = MagicMock()
        backend.execute_sql.return_value = MockDataFrame([])

        collector = CustomQueryCollector(sql="SELECT 1 WHERE 1=0")
        results = collector.collect(backend, "t", QualifireContext())
        assert results == []

    def test_jinja_rendering(self):
        from qualifire.collection.custom_query import CustomQueryCollector

        backend = MagicMock()
        backend.execute_sql.return_value = MockDataFrame([{"cnt": 5}])

        collector = CustomQueryCollector(
            sql="SELECT COUNT(*) AS cnt FROM {{ table }} WHERE ds = '{{ ds }}'"
        )
        ctx = QualifireContext(extra_context={"ds": "2024-01-01"})
        collector.collect(backend, "sales", ctx)

        sql_arg = backend.execute_sql.call_args[0][0]
        assert "sales" in sql_arg
        assert "2024-01-01" in sql_arg

    def test_collect_with_dimension(self):
        """Dimension-aware custom query treats named column as dimension key."""
        from qualifire.collection.custom_query import CustomQueryCollector

        backend = MagicMock()
        backend.execute_sql.return_value = MockDataFrame([
            {"date": "2024-01-01", "row_count": 100},
            {"date": "2024-01-02", "row_count": 120},
            {"date": "2024-01-03", "row_count": 90},
        ])

        collector = CustomQueryCollector(
            sql="SELECT date, COUNT(*) AS row_count FROM {{ table }} GROUP BY date",
            dimensions=["date"],
        )
        results = collector.collect(backend, "t", QualifireContext())

        assert len(results) == 3
        by_dim = {r.dimension_value: r.metric_value for r in results}
        assert by_dim == {'{"date":"2024-01-01"}': 100, '{"date":"2024-01-02"}': 120, '{"date":"2024-01-03"}': 90}
        # dimension column itself should NOT appear as a metric
        assert all(r.metric_name == "row_count" for r in results)
        assert all(r.metadata.get("dimensions") == ["date"] for r in results)


# ===========================================================================
# Multi-Dimension Tests
# ===========================================================================


class TestMultiDimension:
    """Tests for multi-dimension GROUP BY across collectors."""

    def test_aggregation_multi_dimension(self):
        from qualifire.collection.aggregation import AggregationCollector

        backend = MagicMock()
        backend.execute_sql.return_value = MockDataFrame([
            {"region": "US", "category": "electronics", "revenue": 1000},
            {"region": "US", "category": "clothing", "revenue": 500},
            {"region": "EU", "category": "electronics", "revenue": 800},
        ])

        collector = AggregationCollector(
            expressions={"revenue": "SUM(amount)"},
            dimensions=["region", "category"],
        )
        results = collector.collect(backend, "t", QualifireContext())

        assert len(results) == 3
        # JSON with sorted keys
        dim_vals = {r.dimension_value for r in results}
        assert '{"category":"electronics","region":"US"}' in dim_vals
        assert '{"category":"clothing","region":"US"}' in dim_vals
        assert '{"category":"electronics","region":"EU"}' in dim_vals

        # Verify GROUP BY clause has both columns
        sql_arg = backend.execute_sql.call_args[0][0]
        assert "`region`" in sql_arg
        assert "`category`" in sql_arg

    def test_metrics_multi_dimension(self):
        from qualifire.collection.metrics import MetricsCollector

        backend = MagicMock()
        backend.execute_sql.return_value = MockDataFrame([
            {"region": "US", "channel": "web", "revenue": 500},
            {"region": "EU", "channel": "app", "revenue": 300},
        ])

        collector = MetricsCollector(
            metrics={"revenue": "SUM(amount)"},
            dimensions=["region", "channel"],
        )
        results = collector.collect(backend, "t", QualifireContext())

        assert len(results) == 2
        dim_vals = {r.dimension_value for r in results}
        assert '{"channel":"web","region":"US"}' in dim_vals
        assert '{"channel":"app","region":"EU"}' in dim_vals

    def test_custom_query_multi_dimension(self):
        from qualifire.collection.custom_query import CustomQueryCollector

        backend = MagicMock()
        backend.execute_sql.return_value = MockDataFrame([
            {"date": "2024-01-01", "source": "api", "count": 10},
            {"date": "2024-01-01", "source": "web", "count": 20},
        ])

        collector = CustomQueryCollector(
            sql="SELECT date, source, COUNT(*) AS count FROM {{ table }} GROUP BY date, source",
            dimensions=["date", "source"],
        )
        results = collector.collect(backend, "t", QualifireContext())

        assert len(results) == 2
        # Both dimension columns excluded from metrics
        assert all(r.metric_name == "count" for r in results)
        dim_vals = {r.dimension_value for r in results}
        assert '{"date":"2024-01-01","source":"api"}' in dim_vals
        assert '{"date":"2024-01-01","source":"web"}' in dim_vals

    def test_config_dimensions_single(self):
        """Single-element dimensions list works."""
        from qualifire.core.config import AggregationCollectionConfig

        config = AggregationCollectionConfig(
            expressions={"cnt": "COUNT(*)"},
            dimensions=["region"],
        )
        assert config.dimensions == ["region"]

    def test_config_dimensions_multiple(self):
        """`dimensions` field accepts a list of multiple columns."""
        from qualifire.core.config import AggregationCollectionConfig

        config = AggregationCollectionConfig(
            expressions={"cnt": "COUNT(*)"},
            dimensions=["region", "category"],
        )
        assert config.dimensions == ["region", "category"]

    def test_config_dimensions_validates_identifiers(self):
        """Each dimension entry must be a valid SQL identifier."""
        from qualifire.core.config import AggregationCollectionConfig

        with pytest.raises(ValueError, match="valid SQL column identifier"):
            AggregationCollectionConfig(
                expressions={"cnt": "COUNT(*)"},
                dimensions=["valid_col", "1; DROP TABLE"],
            )

    def test_json_encoding_is_deterministic(self):
        """Dimension value encoding is deterministic regardless of input order."""
        from qualifire.collection.aggregation import AggregationCollector

        backend = MagicMock()
        backend.execute_sql.return_value = MockDataFrame([
            {"z_col": "z", "a_col": "a", "metric": 42},
        ])

        collector = AggregationCollector(
            expressions={"metric": "SUM(x)"},
            dimensions=["z_col", "a_col"],
        )
        results = collector.collect(backend, "t", QualifireContext())

        # Keys should be alphabetically sorted in JSON
        assert results[0].dimension_value == '{"a_col":"a","z_col":"z"}'
