"""Real-SparkSession regressions for ExternalCatalogStorage.

The R6 round-2 adversarial review flagged that the all-null
optional-columns coverage was fully mocked — the test proved a
schema argument was passed to ``createDataFrame``, but not that
PySpark actually accepts the constructed ``Row`` + ``StructType``
shape and that ``saveAsTable`` writes rows that round-trip
through a ``SELECT *`` against the live catalog. This file pins
the round-1 schema-inference fix against a real Spark session so
a regression that, e.g., drops a column from
``_system_table_spark_schema()`` or stops parsing ISO timestamps
inline, fails here rather than only in production.

Tests use a per-invocation table name (``time.monotonic_ns()``)
so an interrupted run can't pollute a subsequent run via stale
state in the session-scoped catalog. Setup cleanup is hard-fail
(if the catalog rejects the cleanup DROP, the test should know);
teardown cleanup stays best-effort.
"""

from __future__ import annotations

import time

import pytest


pytest.importorskip("pyspark")

from pyspark.sql import SparkSession  # noqa: E402

from qualifire.storage.external_catalog import ExternalCatalogStorage  # noqa: E402


def _aidataplatform_available(spark: SparkSession) -> bool:
    """Return True iff the ``aidataplatform`` data source class is
    on the Spark classpath. Vanilla PySpark without the AIDP
    connector jar returns False; AIDP Workbench returns True.

    Probe technique: a no-op read. If the source class is missing
    Spark raises immediately with ``"Failed to find data source"``
    / ``ClassNotFoundException``. If the class is present we get a
    different error (auth / catalog) — the class IS available.
    """
    try:
        spark.read.format("aidataplatform").load()
        return True
    except Exception as e:
        msg = str(e)
        if "Failed to find data source" in msg or "aidataplatform.DefaultSource" in msg:
            return False
        return True


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    s = (
        SparkSession.builder
        .master("local[1]")
        .appName("qualifire-external-catalog")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    # Codex R1 D1: stop the session BEFORE raising pytest.skip
    # so a local vanilla-PySpark run doesn't leak an active
    # SparkSession into later tests in the same process.
    if not _aidataplatform_available(s):
        s.stop()
        pytest.skip(
            "aidataplatform data source not on classpath — these "
            "end-to-end tests can only run inside AIDP Workbench",
            allow_module_level=False,
        )
    yield s
    s.stop()


def _unique_table(prefix: str) -> str:
    """Per-invocation 3-part table name to avoid cross-run /
    retry pollution. The external_catalog backend requires the
    standard ``catalog.schema.table`` shape, so we use Spark's
    in-memory ``spark_catalog`` + ``default`` namespace."""
    return f"spark_catalog.default.{prefix}_{time.monotonic_ns()}"


def _drop_table_strict(spark: SparkSession, table: str) -> None:
    """Setup-side cleanup: fail loud if a stale table refuses to
    drop. A silent setup-failure would let the test proceed
    against polluted state and surface the real failure as
    misleading row-count or table-exists symptoms."""
    spark.sql(f"DROP TABLE IF EXISTS {table}")


def _drop_table_best_effort(spark: SparkSession, table: str) -> None:
    """Teardown-side cleanup: best-effort. A test that's already
    failed shouldn't have its diagnostic eaten by a cleanup
    exception."""
    try:
        spark.sql(f"DROP TABLE IF EXISTS {table}")
    except Exception:
        pass


def test_write_results_round_trip_with_all_null_optional_columns(spark):
    """All-None optional columns + the explicit ``StructType`` from
    ``_system_table_spark_schema()`` must produce a successful
    ``saveAsTable`` write; the row must round-trip through
    ``SELECT *`` with the right shape (``metric_value`` as DOUBLE,
    timestamp columns as TIMESTAMP, optional columns as NULL).

    This is the gap the R6 round-2 adversarial review correctly
    flagged: the mocked test proved a schema argument was passed,
    but not that PySpark actually accepts the constructed Row +
    StructType combination."""
    table = _unique_table("ec_real_round_trip")
    _drop_table_strict(spark, table)
    try:
        storage = ExternalCatalogStorage(spark=spark, table=table)
        # Full lifecycle: ``initialize()`` probes DESCRIBE TABLE
        # (fails — table doesn't exist on a fresh fixture), then
        # eagerly creates the system table via an empty-DataFrame
        # ``saveAsTable`` write.
        storage.initialize()

        # Production-realistic shape: a collection-only write
        # where every notification / validation field is None.
        storage.write_results([{
            "run_id": "real_r1",
            "run_timestamp": "2024-06-15T12:00:00",
            "owner": "test", "bu": "qa",
            "dataset_name": "ds", "table_name": "tbl",
            "metric_name": "row_count",
            "metric_value": 4242.0,
            "collection_type": "aggregation",
            "collected_at": "2024-06-15T12:00:01",
            "partition_ts": "2024-06-15T00:00:00",
            "record_type": "collection",
            "is_active": "true",
            "collector_name": "agg.ds",
            # Every optional column intentionally None — the
            # exact shape that crashed schema inference before
            # the explicit StructType fix.
            "validation_name": None, "validation_type": None,
            "validation_status": None, "validation_message": None,
            "notification_channel": None, "notification_status": None,
            "details_json": None, "expected_value": None,
            "actual_value_text": None, "dimension_value": None,
            "validated_at": None, "notified_at": None,
            "dataset_description": None, "validation_description": None,
        }])

        # Row landed and reads back.
        rows = spark.sql(f"SELECT * FROM {table}").collect()
        assert len(rows) == 1
        row = rows[0].asDict()
        assert row["run_id"] == "real_r1"
        assert row["metric_value"] == 4242.0
        assert row["validation_status"] is None  # NULL preserved
        assert row["notification_channel"] is None
        # TIMESTAMP coercion: ISO string was parsed to datetime
        # by write_results, then Spark stored it as TIMESTAMP.
        from datetime import datetime
        assert isinstance(row["run_timestamp"], datetime)
        assert isinstance(row["partition_ts"], datetime)
    finally:
        _drop_table_best_effort(spark, table)


def test_write_results_raises_on_malformed_timestamp(spark):
    """Round-2 review fail-loud contract: a non-ISO timestamp
    string in a TIMESTAMP-typed column raises ``ValueError`` with
    column + value context. The real-Spark variant verifies the
    raise happens BEFORE any catalog write — the system table
    must not be left in a half-written state for a
    producer-side data-contract bug.

    Round-3 adversarial review: the test exercises the FULL
    initialized-storage lifecycle so the assertion is meaningful.

    ``initialize()`` eagerly creates the table via an empty-
    DataFrame ``saveAsTable`` write, so the table exists *before*
    ``write_results`` runs — independent of any write outcome.
    The correct invariant is "the malformed batch produced zero
    rows in the eagerly-created empty table". A pre-write SELECT
    shows an empty table; the failed write doesn't add rows; the
    post-write SELECT also shows an empty table.
    """
    table = _unique_table("ec_real_bad_ts")
    _drop_table_strict(spark, table)
    try:
        storage = ExternalCatalogStorage(spark=spark, table=table)
        storage.initialize()  # eager saveAsTable

        # Table exists after initialize() (eager creation).
        assert spark.catalog.tableExists(table), (
            "initialize() must eagerly create the system table via "
            "empty-DataFrame saveAsTable"
        )
        # Empty before any successful write.
        pre_count = spark.sql(f"SELECT COUNT(*) AS c FROM {table}").collect()[0]["c"]
        assert pre_count == 0

        with pytest.raises(ValueError, match=r"run_timestamp.*non-ISO"):
            storage.write_results([{
                "run_id": "real_bad",
                "run_timestamp": "yesterday at noon",  # not ISO
                "owner": "t", "bu": "q",
                "dataset_name": "ds", "table_name": "tbl",
                "metric_name": "x", "metric_value": 1.0,
                "collection_type": "agg",
            }])

        # The malformed batch must NOT have left rows behind — the
        # ValueError fires before any insert. Post-write count is
        # still zero, even though the table now exists.
        post_count = spark.sql(f"SELECT COUNT(*) AS c FROM {table}").collect()[0]["c"]
        assert post_count == 0, (
            "malformed timestamp must abort the write before any rows "
            "land in the system table"
        )
    finally:
        _drop_table_best_effort(spark, table)


def test_read_metric_history_by_partition_finds_prior_anchor(spark):
    """``read_metric_history_by_partition`` must match a TIMESTAMP-typed
    ``partition_ts`` against the IN list emitted by the storage layer.

    Regression for the production silent-zero-match observed on ADW
    external catalog: the IN list previously used the ISO ``T``-
    separator string form (``'2026-04-26T00:00:00'``); Oracle's
    default ``NLS_TIMESTAMP_FORMAT`` rejects the ``T`` separator and
    the comparison silently failed every time, so drift / forecast /
    anomaly cold-started forever despite the underlying collection
    rows being present.

    The fix moved both spots (external_catalog + jdbc) to the ANSI
    ``TIMESTAMP '...'`` literal with a space separator. This test
    pins it end-to-end: write a collection row, read it back via
    the partition-anchored lookup, expect exactly one row.

    Datetimes are passed *tz-aware* (UTC) so the test result doesn't
    depend on the JVM's default time zone. PySpark converts a naive
    Python datetime by interpreting it in the JVM's local TZ before
    storing — on UTC+N developer machines that off-by-N-hours shift
    would silently fail the assertion for the wrong reason.
    """
    from datetime import datetime, timezone

    table = _unique_table("history_by_partition")
    _drop_table_strict(spark, table)
    try:
        storage = ExternalCatalogStorage(spark=spark, table=table)
        storage.initialize()

        # Tz-aware ISO strings mean ``datetime.fromisoformat()`` (used
        # inside ``write_results``) gives back tz-aware values, which
        # PySpark stores at the documented UTC instant rather than
        # local-TZ-shifting.
        run_ts = datetime(2026, 4, 26, 6, 0, 0, tzinfo=timezone.utc).isoformat()
        partition_ts_iso = datetime(2026, 4, 26, tzinfo=timezone.utc).isoformat()
        storage.write_results([{
            "run_id": "r-26",
            "owner": "demo", "bu": "demo",
            "dataset_name": "marketing_campaign_events",
            "table_name": "dataquality.marketing.campaign_events",
            "metric_name": "sessions_total",
            "metric_value": 5911.0,
            "collection_type": "trend",
            "record_type": "collection",
            "run_timestamp": run_ts,
            "collected_at": run_ts,
            "partition_ts": partition_ts_iso,
            "is_active": "true",
        }])

        # Anchor=2026-04-27, count=1, step=P1D ⇒ lookback list =
        # [2026-04-26]. Pass tz-aware so the IN-clause anchor strings
        # are computed against the same instant the row was written.
        rows = storage.read_metric_history_by_partition(
            table_name="dataquality.marketing.campaign_events",
            metric_name="sessions_total",
            anchor_ts=datetime(2026, 4, 27, tzinfo=timezone.utc),
            count=1,
            step="P1D",
        )
        assert len(rows) == 1, (
            f"Expected exactly 1 row for the prior partition (Apr 26); got "
            f"{len(rows)}. Empty result indicates the IN clause silently "
            "failed to match — likely a regression of the ISO-T vs. "
            "TIMESTAMP-literal fix in read_metric_history_by_partition."
        )
        assert rows[0]["metric_name"] == "sessions_total"
        assert rows[0]["metric_value"] == 5911.0
    finally:
        _drop_table_best_effort(spark, table)


def test_read_metric_history_by_partition_emits_ansi_timestamp_literal():
    """Pin the literal *form* of the SQL emitted, independent of
    whether Spark's local default-catalog accepts the IN-clause
    cast at execution time.

    Hostile downstream dialects (Oracle / ADW) only accept
    ``TIMESTAMP 'YYYY-MM-DD HH:MI:SS'`` for an unambiguous
    string-to-timestamp comparison. We capture the SQL string the
    storage layer hands to ``self._spark.sql(...)`` and assert:
      * ANSI ``TIMESTAMP '...'`` keyword is present (one per anchor)
      * the space separator is used (not the ``T`` separator)
    Together these guarantee the regression that motivated the
    fix can't silently re-introduce.

    Codex R1 D2: this test only exercises the SQL-string
    formatting via a captured ``spark.sql(...)`` call — no real
    Spark session required. Decoupling from the AIDP-gated
    ``spark`` fixture keeps the regression signal alive on local
    CI, where ``aidataplatform`` isn't on the classpath.
    """
    from datetime import datetime

    captured: dict[str, str] = {}

    class _FakeDF:
        def collect(self):
            return []

    def _fake_sql(sql: str):
        captured["sql"] = sql
        return _FakeDF()

    fake_spark = type("S", (), {"sql": staticmethod(_fake_sql)})()
    storage = ExternalCatalogStorage(spark=fake_spark, table="cat.sch.tbl")

    storage.read_metric_history_by_partition(
        table_name="dq.r.s_fact",
        metric_name="avg_amount",
        anchor_ts=datetime(2026, 4, 27),
        count=2,
        step="P1D",
    )
    sql = captured["sql"]
    # Two anchors → two TIMESTAMP literals.
    assert sql.count("TIMESTAMP '") == 2, (
        f"Expected 2 ANSI TIMESTAMP literals in IN clause; SQL was:\n{sql}"
    )
    # No bare ISO-T strings should appear in the IN list.
    assert "'2026-04-26T00:00:00'" not in sql, (
        f"ISO ``T`` separator must not appear as a bare string literal; SQL was:\n{sql}"
    )
    assert "'2026-04-25T00:00:00'" not in sql
    # Space-separator forms must be present inside the TIMESTAMP literals.
    assert "TIMESTAMP '2026-04-26 00:00:00'" in sql
    assert "TIMESTAMP '2026-04-25 00:00:00'" in sql
