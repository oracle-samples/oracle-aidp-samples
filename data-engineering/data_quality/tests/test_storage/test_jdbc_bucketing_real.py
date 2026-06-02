"""Real-SparkSession regressions for JDBC bucketed history reads.

The round-10 adversarial review flagged that
``_bucketed_history_read`` used ``F.unix_timestamp(col)`` without an
explicit format. Spark's default pattern is ``yyyy-MM-dd HH:mm:ss``,
which in the default (CORRECTED) timeParserPolicy either throws
``SparkUpgradeException`` or returns NULL for timestamp strings with
trailing fractional seconds or a ``T`` separator — i.e. precisely
what the engine writes via ``str(datetime.now())`` and
``datetime.now().isoformat()``. The result: JDBC bucketed drift /
forecast reads silently yield zero rows or crash the caller for any
real production history.

The fix is to ``cast("timestamp")`` the string column before handing
it to ``unix_timestamp``; Catalyst's ISO-8601 parser accepts both
separators and optional ``.ffffff`` fractional seconds. These tests
exercise the real Spark code path against engine-style timestamps to
pin the behavior."""

from __future__ import annotations

import pytest


pytest.importorskip("pyspark")

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DoubleType,
    StringType,
    StructField,
    StructType,
)

from qualifire.storage.jdbc_storage import JDBCStorage


_HISTORY_SCHEMA = StructType([
    StructField("metric_name", StringType(), True),
    StructField("metric_value", DoubleType(), True),
    StructField("run_timestamp", StringType(), True),
    StructField("validation_status", StringType(), True),
    StructField("record_type", StringType(), True),
    StructField("collector_name", StringType(), True),
    StructField("dimension_value", StringType(), True),
    # partition_ts + is_active are projected by the JDBC reader as of
    # the K-storage-parity work — the bucketed history Spark-side
    # dedup uses partition_ts in its first ROW_NUMBER stage, and the
    # post-dedup `is_active` filter consumes that column.
    StructField("partition_ts", StringType(), True),
    StructField("is_active", StringType(), True),
])


@pytest.fixture(scope="session")
def spark():
    # ``session`` scope so the SparkContext remains live for any
    # downstream test file that assumes a context exists (some older
    # JDBC-storage unit tests rely on a live context left over from
    # test ordering). Stopping the session mid-run would pull that
    # context out from under them.
    s = (
        SparkSession.builder
        .master("local[1]")
        .appName("qualifire-jdbc-bucketing")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    yield s
    s.stop()


def _make_storage(spark_session) -> JDBCStorage:
    return JDBCStorage(
        spark=spark_session,
        table="qf_history",
        jdbc_url="jdbc:postgresql://unused/db",
        connection_properties={"user": "u", "password": "p"},
    )


def _rows(timestamps: list[str]) -> list[dict]:
    """Build rows with the same columns ``_bucketed_history_read``
    emits from the JDBC subquery projection."""
    rows = []
    for i, ts in enumerate(timestamps):
        rows.append({
            "metric_name": "count",
            "metric_value": float(100 + i),
            "run_timestamp": ts,
            "validation_status": "PASS",
            "record_type": "validation",
            "collector_name": None,
            "dimension_value": None,
            # Each fixture row carries a unique partition_ts so the
            # per-partition dedup ROW_NUMBER stage doesn't collapse
            # rows in the same time bucket.
            "partition_ts": ts,
            "is_active": "true",
        })
    return rows


class TestJDBCBucketingParsesEngineTimestamps:
    """Run ``read_metric_history`` against a real SparkSession with
    ``_bucketed_history_read`` stubbed to return engine-style
    timestamp strings. Exercises the exact ``cast("timestamp")`` path
    the fix introduced."""

    def test_str_datetime_format_with_microseconds_is_bucketed(self, spark):
        """``str(datetime.now())`` — space separator + microseconds —
        is what ``_append_rows`` stringifies writes into the system
        table. Pre-fix, ``unix_timestamp`` on this format dropped /
        crashed every row; post-fix, bucketing must succeed and
        return one row per 7-day bucket."""
        storage = _make_storage(spark)
        ts_strings = [
            "2024-01-01 00:00:00.000001",  # bucket A
            "2024-01-03 12:34:56.789012",  # bucket A (newer) wins
            "2024-01-08 01:23:45.111111",  # bucket B
            "2024-01-15 05:00:00.999999",  # bucket C
        ]
        rows = _rows(ts_strings)
        df = spark.createDataFrame(rows, schema=_HISTORY_SCHEMA)

        captured: dict = {}

        def fake_bucketed_read(where_clause, limit, step_seconds):
            captured["where"] = where_clause
            captured["limit"] = limit
            captured["step_seconds"] = step_seconds
            return df

        storage._bucketed_history_read = fake_bucketed_read

        history = storage.read_metric_history(
            table_name="t1", metric_name="count", limit=10, step="P7D"
        )
        # 4 input rows → 3 distinct 7D buckets. The microsecond-
        # suffixed timestamps must survive parsing; pre-fix this
        # would have been [] or an exception.
        assert len(history) == 3, f"expected 3 buckets, got {history!r}"

        returned_ts = sorted(r["run_timestamp"] for r in history)
        # Bucket A winner is the newer of Jan 1 / Jan 3.
        assert returned_ts[0] == "2024-01-03 12:34:56.789012"
        assert returned_ts[1] == "2024-01-08 01:23:45.111111"
        assert returned_ts[2] == "2024-01-15 05:00:00.999999"

    def test_isoformat_with_T_separator_is_bucketed(self, spark):
        """``datetime.now().isoformat()`` (with ``T`` separator) is the
        other format written through engine/context paths. It must
        bucket identically to the space-separated form."""
        storage = _make_storage(spark)
        ts_strings = [
            "2024-01-01T00:00:00.000001",
            "2024-01-03T12:34:56.789012",  # same 7D bucket — wins
            "2024-01-08T00:00:00.000000",
        ]
        rows = _rows(ts_strings)
        df = spark.createDataFrame(rows, schema=_HISTORY_SCHEMA)

        storage._bucketed_history_read = (
            lambda where, limit, step_seconds: df
        )
        history = storage.read_metric_history(
            table_name="t1", metric_name="count", limit=10, step="P7D"
        )
        assert len(history) == 2, f"expected 2 buckets, got {history!r}"

    def test_mixed_separators_do_not_silently_drop_rows(self, spark):
        """Defense in depth: a mixed-format history (some rows written
        by the engine with ``T``, some with space) must still parse.
        The fix uses Catalyst's permissive ISO-8601 cast which handles
        both. Verifies that no variant silently vanishes from the
        baseline."""
        storage = _make_storage(spark)
        ts_strings = [
            "2024-01-01 00:00:00",               # no fractional, space
            "2024-01-08T00:00:00.500000",        # T + microseconds
            "2024-01-15 12:00:00.123",           # space + milliseconds
            "2024-01-22T05:30:00",               # T, no fractional
        ]
        rows = _rows(ts_strings)
        df = spark.createDataFrame(rows, schema=_HISTORY_SCHEMA)

        storage._bucketed_history_read = (
            lambda where, limit, step_seconds: df
        )
        history = storage.read_metric_history(
            table_name="t1", metric_name="count", limit=10, step="P7D"
        )
        # 4 distinct 7D buckets → 4 rows returned. Pre-fix, the
        # fractional-second rows would drop out, leaving at most 2.
        assert len(history) == 4, f"expected 4 buckets, got {history!r}"

    def test_distinct_bucket_count_handles_microsecond_timestamps(self, spark):
        """``_count_distinct_buckets`` runs the same parse expression
        as ``_bucketed_history_read``. It must count the right number
        of buckets when the incoming strings have microseconds —
        otherwise the sparse-history fallback trips on every read and
        needlessly scans the full unbounded history."""
        storage = _make_storage(spark)
        ts_strings = [
            "2024-01-01 00:00:00.000001",
            "2024-01-03 12:34:56.789012",  # same bucket as above
            "2024-01-08 01:23:45.111111",
            "2024-01-15 05:00:00.999999",
        ]
        rows = _rows(ts_strings)
        df = spark.createDataFrame(rows, schema=_HISTORY_SCHEMA)
        assert storage._count_distinct_buckets(df, 7 * 86400) == 3
