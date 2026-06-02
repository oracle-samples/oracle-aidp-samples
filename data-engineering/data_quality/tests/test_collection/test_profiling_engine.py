"""Tests for single-pass profiling engine with real PySpark and Pandas."""

from datetime import datetime

import numpy as np
import pandas as pd
import pytest

from qualifire.collection.profiling import (
    BooleanAnalyzer,
    NumericAnalyzer,
    ProfileEngine,
    TextAnalyzer,
    TimestampAnalyzer,
)


# ===========================================================================
# Pandas profiling tests
# ===========================================================================


@pytest.fixture
def pandas_df():
    return pd.DataFrame({
        "id": [1, 2, 3, 4, 5],
        "amount": [10.0, 20.0, 30.0, None, 50.0],
        "name": ["alice", "bob", "charlie", "", None],
        "active": [True, False, True, True, None],
        "created_at": pd.to_datetime(["2024-01-01", "2024-01-02", "2024-01-03", None, "2024-01-05"]),
    })


class TestPandasProfiling:
    def test_profiles_all_columns(self, pandas_df):
        engine = ProfileEngine()
        result = engine.profile(pandas_df, table_name="test")

        assert result.row_count == 5
        assert "id" in result.columns
        assert "amount" in result.columns
        assert "name" in result.columns
        assert "active" in result.columns
        assert "created_at" in result.columns

    def test_numeric_stats(self, pandas_df):
        engine = ProfileEngine()
        result = engine.profile(pandas_df)

        amount = result.columns["amount"].stats
        assert amount["count"] == 5
        assert amount["null_count"] == 1
        assert amount["null_pct"] == 20.0
        assert amount["min"] == 10.0
        assert amount["max"] == 50.0
        assert amount["approx_distinct"] == 4
        assert "p25" in amount
        assert "p50" in amount
        assert "p75" in amount
        assert "mean" in amount
        assert "stddev" in amount
        assert "variance" in amount

    def test_text_stats(self, pandas_df):
        engine = ProfileEngine()
        result = engine.profile(pandas_df)

        name = result.columns["name"].stats
        assert name["count"] == 5
        assert name["null_count"] == 1
        assert name["empty_count"] == 1
        assert name["approx_distinct"] == 4
        assert name["min_length"] == 0  # empty string
        assert name["max_length"] == 7  # "charlie"

    def test_text_top_k(self, pandas_df):
        engine = ProfileEngine(analyzers=[TextAnalyzer(top_k=3)])
        result = engine.profile(pandas_df, columns=["name"])

        top_k = result.columns["name"].stats["top_k"]
        assert isinstance(top_k, list)
        assert len(top_k) <= 3
        assert "value" in top_k[0]
        assert "count" in top_k[0]

    def test_boolean_stats(self, pandas_df):
        engine = ProfileEngine()
        result = engine.profile(pandas_df)

        active = result.columns["active"].stats
        assert active["count"] == 5
        assert active["null_count"] == 1
        assert active["true_count"] == 3
        assert active["false_count"] == 1

    def test_timestamp_stats(self, pandas_df):
        engine = ProfileEngine()
        result = engine.profile(pandas_df)

        ts = result.columns["created_at"].stats
        assert ts["count"] == 5
        assert ts["null_count"] == 1
        assert ts["approx_distinct"] == 4

    def test_column_filter(self, pandas_df):
        engine = ProfileEngine()
        result = engine.profile(pandas_df, columns=["amount", "name"])

        assert set(result.columns.keys()) == {"amount", "name"}

    def test_stat_filter(self, pandas_df):
        engine = ProfileEngine()
        result = engine.profile(pandas_df, stats=["count", "null_count"])

        for profile in result.columns.values():
            assert set(profile.stats.keys()) <= {"count", "null_count"}

    def test_filter_expr(self, pandas_df):
        engine = ProfileEngine()
        result = engine.profile(pandas_df, filter_expr="id <= 3")

        assert result.row_count == 3

    def test_to_flat_dict(self, pandas_df):
        engine = ProfileEngine()
        result = engine.profile(pandas_df, columns=["amount"], stats=["count", "min"])

        flat = result.to_flat_dict()
        assert "amount.count" in flat
        assert "amount.min" in flat
        assert "_row_count" in flat

    def test_custom_quantiles(self, pandas_df):
        engine = ProfileEngine(analyzers=[NumericAnalyzer(quantiles=[0.1, 0.9])])
        result = engine.profile(pandas_df, columns=["amount"])

        stats = result.columns["amount"].stats
        assert "p10" in stats
        assert "p90" in stats
        assert "p50" not in stats  # only requested p10 and p90

    def test_top_k_disabled(self, pandas_df):
        engine = ProfileEngine(analyzers=[TextAnalyzer(top_k=0)])
        result = engine.profile(pandas_df, columns=["name"])

        assert "top_k" not in result.columns["name"].stats

    def test_type_classification(self, pandas_df):
        engine = ProfileEngine()
        result = engine.profile(pandas_df)

        assert result.columns["id"].column_type == "numeric"
        assert result.columns["amount"].column_type == "numeric"
        assert result.columns["name"].column_type == "string"
        assert result.columns["active"].column_type == "boolean"
        assert result.columns["created_at"].column_type == "timestamp"


# ===========================================================================
# Spark profiling tests
# ===========================================================================


@pytest.fixture(scope="module")
def spark():
    from pyspark.sql import SparkSession

    s = (
        SparkSession.builder
        .master("local[1]")
        .appName("profiling-test")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "1")
        .getOrCreate()
    )
    yield s
    s.stop()


@pytest.fixture
def spark_df(spark):
    from pyspark.sql.types import (
        BooleanType,
        DoubleType,
        IntegerType,
        StringType,
        StructField,
        StructType,
        TimestampType,
    )
    from datetime import datetime

    schema = StructType([
        StructField("id", IntegerType(), False),
        StructField("amount", DoubleType(), True),
        StructField("name", StringType(), True),
        StructField("active", BooleanType(), True),
        StructField("created_at", TimestampType(), True),
    ])
    data = [
        (1, 10.0, "alice", True, datetime(2024, 1, 1)),
        (2, 20.0, "bob", False, datetime(2024, 1, 2)),
        (3, 30.0, "charlie", True, datetime(2024, 1, 3)),
        (4, None, "", True, None),
        (5, 50.0, None, None, datetime(2024, 1, 5)),
    ]
    return spark.createDataFrame(data, schema)


class TestSparkProfiling:
    def test_single_pass_all_types(self, spark_df):
        """Verify single-pass profiling works for all column types."""
        engine = ProfileEngine()
        result = engine.profile(spark_df, table_name="test")

        assert result.row_count == 5
        assert len(result.columns) == 5

    def test_numeric_stats(self, spark_df):
        engine = ProfileEngine()
        result = engine.profile(spark_df)

        amount = result.columns["amount"].stats
        assert amount["count"] == 5
        assert amount["null_count"] == 1
        assert amount["min"] == 10.0
        assert amount["max"] == 50.0
        assert amount["approx_distinct"] >= 4
        assert "mean" in amount
        assert "stddev" in amount
        assert "variance" in amount
        assert "skewness" in amount
        assert "kurtosis" in amount
        assert "p25" in amount
        assert "p50" in amount
        assert "p75" in amount

    def test_text_stats(self, spark_df):
        engine = ProfileEngine()
        result = engine.profile(spark_df)

        name = result.columns["name"].stats
        assert name["count"] == 5
        assert name["null_count"] == 1
        assert name["empty_count"] == 1
        assert "min_length" in name
        assert "max_length" in name
        assert "avg_length" in name
        assert name["approx_distinct"] >= 3

    def test_text_top_k(self, spark_df):
        engine = ProfileEngine(analyzers=[TextAnalyzer(top_k=3)])
        result = engine.profile(spark_df, columns=["name"])

        top_k = result.columns["name"].stats["top_k"]
        assert isinstance(top_k, list)
        assert len(top_k) <= 3

    def test_boolean_stats(self, spark_df):
        engine = ProfileEngine()
        result = engine.profile(spark_df)

        active = result.columns["active"].stats
        assert active["count"] == 5
        assert active["null_count"] == 1
        assert active["true_count"] == 3
        assert active["false_count"] == 1

    def test_timestamp_stats(self, spark_df):
        engine = ProfileEngine()
        result = engine.profile(spark_df)

        ts = result.columns["created_at"].stats
        assert ts["count"] == 5
        assert ts["null_count"] == 1
        assert ts["approx_distinct"] >= 4

    def test_column_subset(self, spark_df):
        engine = ProfileEngine()
        result = engine.profile(spark_df, columns=["amount", "name"])

        assert set(result.columns.keys()) == {"amount", "name"}

    def test_stat_filter(self, spark_df):
        engine = ProfileEngine()
        result = engine.profile(spark_df, stats=["count", "null_count"])

        for profile in result.columns.values():
            assert set(profile.stats.keys()) <= {"count", "null_count"}

    def test_spark_filter(self, spark_df):
        engine = ProfileEngine()
        result = engine.profile(spark_df, filter_expr="id <= 3")

        assert result.row_count == 3

    def test_custom_quantiles(self, spark_df):
        engine = ProfileEngine(analyzers=[NumericAnalyzer(quantiles=[0.10, 0.90])])
        result = engine.profile(spark_df, columns=["amount"])

        stats = result.columns["amount"].stats
        assert "p10" in stats
        assert "p90" in stats

    def test_type_classification(self, spark_df):
        engine = ProfileEngine()
        result = engine.profile(spark_df)

        assert result.columns["id"].column_type == "numeric"
        assert result.columns["amount"].column_type == "numeric"
        assert result.columns["name"].column_type == "string"
        assert result.columns["active"].column_type == "boolean"
        assert result.columns["created_at"].column_type == "timestamp"


# ===========================================================================
# Custom analyzer extensibility
# ===========================================================================


class TestCustomAnalyzer:
    def test_custom_numeric_analyzer(self, pandas_df):
        """Users can create custom analyzers."""
        from qualifire.collection.profiling.analyzers import StatAnalyzer

        class RangeAnalyzer(StatAnalyzer):
            @property
            def name(self):
                return "range"

            @property
            def applicable_types(self):
                return {"numeric"}

            def spark_expressions(self, col_name):
                from pyspark.sql import functions as F
                c = F.col(f"`{col_name}`")
                return [("range", F.max(c) - F.min(c))]

            def pandas_compute(self, series, col_name):
                non_null = series.dropna()
                return {"range": non_null.max() - non_null.min() if len(non_null) > 0 else None}

        engine = ProfileEngine(analyzers=[RangeAnalyzer()])
        result = engine.profile(pandas_df, columns=["amount"])

        assert result.columns["amount"].stats["range"] == 40.0


# ===========================================================================
# Stat include/exclude filtering
# ===========================================================================


class TestGlobalExcludeStats:
    def test_exclude_top_k_globally(self, pandas_df):
        """exclude_stats removes stats from all columns."""
        engine = ProfileEngine()
        result = engine.profile(pandas_df, exclude_stats=["top_k"])

        for profile in result.columns.values():
            assert "top_k" not in profile.stats

    def test_exclude_multiple_stats(self, pandas_df):
        """Can exclude multiple stats at once."""
        engine = ProfileEngine()
        result = engine.profile(
            pandas_df,
            columns=["amount"],
            exclude_stats=["skewness", "kurtosis", "variance"],
        )
        stats = result.columns["amount"].stats
        assert "skewness" not in stats
        assert "kurtosis" not in stats
        assert "variance" not in stats
        assert "count" in stats  # other stats still present
        assert "mean" in stats

    def test_exclude_quantiles(self, pandas_df):
        engine = ProfileEngine()
        result = engine.profile(
            pandas_df, columns=["amount"], exclude_stats=["p25", "p50", "p75"]
        )
        stats = result.columns["amount"].stats
        assert "p25" not in stats
        assert "p50" not in stats
        assert "p75" not in stats
        assert "min" in stats  # others still present

    def test_stats_include_overrides_exclude(self, pandas_df):
        """When stats (include) is set, exclude_stats is irrelevant."""
        engine = ProfileEngine()
        result = engine.profile(
            pandas_df,
            columns=["amount"],
            stats=["count", "null_count"],
            exclude_stats=["count"],  # should be ignored since stats is set
        )
        stats = result.columns["amount"].stats
        # stats include list wins — only count and null_count
        assert set(stats.keys()) == {"count", "null_count"}


class TestPerColumnOverrides:
    def test_column_include_override(self, pandas_df):
        """Per-column stats list limits that column to only those stats."""
        from qualifire.collection.profiling.engine import ColumnProfileOverrides

        engine = ProfileEngine()
        result = engine.profile(
            pandas_df,
            columns=["amount", "name"],
            column_overrides={
                "amount": ColumnProfileOverrides(stats=["count", "min", "max"]),
            },
        )
        amount_stats = set(result.columns["amount"].stats.keys())
        assert amount_stats == {"count", "min", "max"}

        # name column should still have all its default stats
        name_stats = result.columns["name"].stats
        assert "count" in name_stats
        assert "approx_distinct" in name_stats

    def test_column_exclude_override(self, pandas_df):
        """Per-column exclude_stats removes only those stats for that column."""
        from qualifire.collection.profiling.engine import ColumnProfileOverrides

        engine = ProfileEngine()
        result = engine.profile(
            pandas_df,
            columns=["amount", "name"],
            column_overrides={
                "name": ColumnProfileOverrides(exclude_stats=["top_k", "empty_count", "empty_pct"]),
            },
        )
        name_stats = result.columns["name"].stats
        assert "top_k" not in name_stats
        assert "empty_count" not in name_stats
        assert "count" in name_stats  # other stats present

        # amount is unaffected
        amount_stats = result.columns["amount"].stats
        assert "mean" in amount_stats

    def test_column_override_takes_precedence(self, pandas_df):
        """Per-column overrides take precedence over global exclude."""
        from qualifire.collection.profiling.engine import ColumnProfileOverrides

        engine = ProfileEngine()
        result = engine.profile(
            pandas_df,
            columns=["amount", "name"],
            exclude_stats=["top_k"],  # globally exclude top_k
            column_overrides={
                "amount": ColumnProfileOverrides(stats=["count"]),  # override: only count
            },
        )
        # amount: per-column include wins
        assert set(result.columns["amount"].stats.keys()) == {"count"}

        # name: global exclude applies (no per-column override)
        assert "top_k" not in result.columns["name"].stats
        assert "count" in result.columns["name"].stats

    def test_mixed_overrides(self, pandas_df):
        """Different columns can have different override types."""
        from qualifire.collection.profiling.engine import ColumnProfileOverrides

        engine = ProfileEngine()
        result = engine.profile(
            pandas_df,
            columns=["amount", "name", "id"],
            column_overrides={
                "amount": ColumnProfileOverrides(stats=["count", "mean"]),
                "name": ColumnProfileOverrides(exclude_stats=["top_k"]),
                # id: no override, uses defaults
            },
        )
        assert set(result.columns["amount"].stats.keys()) == {"count", "mean"}
        assert "top_k" not in result.columns["name"].stats
        assert "count" in result.columns["id"].stats
        assert "mean" in result.columns["id"].stats


class TestSparkStatFiltering:
    """Verify stat filtering works in the Spark path too."""

    def test_spark_exclude_stats(self, spark_df):
        engine = ProfileEngine()
        result = engine.profile(spark_df, exclude_stats=["top_k", "skewness", "kurtosis"])

        for profile in result.columns.values():
            assert "top_k" not in profile.stats
            assert "skewness" not in profile.stats
            assert "kurtosis" not in profile.stats

    def test_spark_column_override(self, spark_df):
        from qualifire.collection.profiling.engine import ColumnProfileOverrides

        engine = ProfileEngine()
        result = engine.profile(
            spark_df,
            columns=["amount", "name"],
            column_overrides={
                "amount": ColumnProfileOverrides(stats=["count", "null_count"]),
            },
        )
        assert set(result.columns["amount"].stats.keys()) == {"count", "null_count"}
        # name still has all its stats
        assert "approx_distinct" in result.columns["name"].stats
