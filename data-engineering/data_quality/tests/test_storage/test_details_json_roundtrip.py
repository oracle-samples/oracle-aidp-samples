"""Cross-backend round-trip parity for ``details_json``.

A single ``_KITCHEN_SINK`` dict covers every nested detail block
qualifire validators emit today (audit captured in
``docs/features/details-json-backend-roundtrip-parity/plan.md``).
Each backend's ``write_results → read_health_data`` round-trip must
surface the payload structurally identical (full-dict equality on
the JSON-decoded value).

Convention: tests must not mutate ``_KITCHEN_SINK``. The ``_row()``
helper materializes a fresh ``dict(_KITCHEN_SINK)`` before write.

When a new validator adds a nested ``details_json`` block, extend
``_KITCHEN_SINK`` and ``_assert_kitchen_sink_roundtrip`` together —
the parity check is only meaningful if the fixture stays in
lock-step with the emitted shapes. The plan-time audit table in the
plan.md is the canonical reference.

Float / string round-trip note: JSON int/float values stored in a
STRING/TEXT column round-trip identically because no intermediate
cast happens — ``json.dumps(0.506)`` → ``'0.506'`` →
``json.loads('0.506')`` → ``0.506``. If a backend ever changes
``details_json`` to a JSON-typed column (Postgres ``JSONB``,
Oracle 23 ``JSON``), this test would still pass because
``json.loads`` is the canonical decoder.

NO module-level ``pytest.importorskip("pyspark")``. The SQLite
test in this file MUST run regardless of PySpark availability;
each Spark-touching class scopes its own importorskip inside its
fixture (codex plan-review R1, MEDIUM).
"""
from __future__ import annotations

import json
import os
import time
from datetime import datetime, timezone

import pytest


# ---------------------------------------------------------------------
# Kitchen-sink: one entry per validator-emitted nested block.
# Audit table reproduced from plan.md:
#   pattern_check, isolation_forest, forecast, historical, slo,
#   validation/base, core/deactivate, core/engine, wap/pattern.
# ---------------------------------------------------------------------

_KITCHEN_SINK = {
    # validation/pattern_check.py
    "value_drift_explainer": [
        {
            "feature": "amount", "source_column": "amount", "kind": "numeric",
            "current": {"count": 100, "null_pct": 0.0, "mean": 120.5, "std": 45.0,
                        "p25": 80.0, "p50": 100.0, "p75": 150.0,
                        "p95": 280.0, "p99": 500.0, "min": 0.0, "max": 999.0},
            "past": {"count": 200, "null_pct": 0.0, "mean": 80.0, "std": 22.0,
                     "p25": 60.0, "p50": 70.0, "p75": 95.0,
                     "p95": 180.0, "p99": 300.0, "min": 0.0, "max": 700.0},
            "delta": {"mean_pct": 0.506, "p99_pct": 0.667, "null_pct_abs": 0.0},
            "summary": "amount; p50 100 vs 70; mean +51%; p99 +67%",
        },
        {
            "feature": "channel_mobile", "source_column": "channel",
            "kind": "onehot", "category": "mobile", "is_null_bin": False,
            "current": {"count": 100, "rate": 0.6},
            "past": {"count": 200, "rate": 0.3},
            "delta": {"rate_pp": 0.3},
            "summary": "channel='mobile' rate 60.0% vs 30.0% (+30.0pp)",
        },
    ],
    "value_drift_explainer_truncated": True,
    # Production emits an INT (count of mapping errors), not a list —
    # see qualifire/validation/pattern_check.py:442 and
    # isolation_forest.py:313 (codex impl-review R2 MEDIUM).
    "value_drift_explainer_mapping_errors": 3,
    "value_drift_explainer_error": "shap_explainer_failed: model not converged",
    "explanation_error": "ValueError: SHAP requires non-NaN inputs",
    "top_contributing_features": [
        {"feature": "amount", "importance": 0.55},
        {"feature": "channel_mobile", "importance": 0.32},
    ],
    "new_columns": ["region_code", "device_type"],
    "dropped_columns": ["legacy_session_id"],
    "inconsistent_past_columns": ["amount"],
    "n_current": 1024,
    "n_past": 2048,
    "n_features": 42,
    "cv_folds": 5,
    "auc": 0.91,
    "auc_std": 0.018,
    "auc_folds": [0.91, 0.89, 0.92, 0.88, 0.93],
    # validation/isolation_forest.py
    "anomaly_count": 7,
    "total_current": 1024,
    "anomaly_ratio": 0.0068,
    # validation/forecast.py
    "history_count": 90,
    "changepoint_prior_scale": 0.05,
    "seasonality_mode": "additive",
    # validation/historical.py
    "past_values": [100.0, 110.0, 120.0, 95.0, 105.0],
    "mean_past": 106.0,
    "stddev": 9.4,
    "deviation_pct": 0.13,
    "deviation_abs": 14.0,
    "z_score": 1.49,
    "rate_of_change_pct": 0.05,
    "rate_of_change_abs": 5.0,
    # cold_start (forecast / pattern / historical / isolation_forest)
    "cold_start": False,
    # validation/slo.py
    "recency_timestamp": "2026-05-10T12:00:00",
    "delta_seconds": 240,
    "delta_iso": "PT4M",
    # validation/base.py — missing partition anchor diagnostic
    "missing_partition_anchor": False,
    "reason": "n/a",
    # core/deactivate.py
    "deactivated_at": "2026-05-10T08:00:00",
    "deactivated_by": "ops_ticket_INC-12345",
    # core/engine.py
    "qualifire_internal_failure": False,
    "error": "",
    "message": "validation completed",
    "collected_via": "backfill",
    "raw_details": "<original-non-dict-payload>",
    "query": "SELECT * FROM marketing_events WHERE date='2026-05-10'",
    # wap/pattern.py
    "staging_table": "stg_marketing_events_2026_05_10",
    # Tripwire edge cases beyond validator emission.
    "edge_cases": {
        "unicode": "Здравствуй, 世界 🚀",
        "control_chars": "\t\n\r quoted-\"-and-\\backslash",
        "html_breakout": "</script><!--",
        "long_string": "x" * 4096,
    },
}


def _assert_kitchen_sink_roundtrip(decoded: dict, *, where: str) -> None:
    """Structural equality + per-block sanity, with ``where`` prefixing
    the failure message so a backend regression is unambiguous.
    """
    assert decoded == _KITCHEN_SINK, (
        f"[{where}] kitchen-sink mismatch.\n"
        f"  expected keys: {sorted(_KITCHEN_SINK.keys())}\n"
        f"  decoded keys:  {sorted(decoded.keys())}\n"
        "  full decoded payload follows for diagnosis:\n"
        f"  {json.dumps(decoded, indent=2, ensure_ascii=False)}"
    )
    assert decoded["value_drift_explainer"][0]["feature"] == "amount"
    assert decoded["edge_cases"]["unicode"].startswith("Здравствуй")
    assert decoded["edge_cases"]["html_breakout"] == "</script><!--"
    assert len(decoded["edge_cases"]["long_string"]) == 4096


def _row(details: dict) -> dict:
    """Build the canonical row shape with a fresh-now run_timestamp.

    Hardcoded dates would become time-bombs against
    ``read_health_data``'s ``run_timestamp >= now() - {days}``
    window; using ``datetime.now(UTC)`` keeps the test evergreen.

    Every column carries a non-None value: this test focuses on
    ``details_json`` fidelity; the all-None optional-columns edge
    case is covered separately by
    ``test_external_catalog_real.py::test_write_results_round_trip_with_all_null_optional_columns``.
    Crucially, ``JDBCStorage.write_results`` infers schema via
    PySpark's ``createDataFrame(spark_rows)`` without an explicit
    StructType; columns that are ``None`` in every row in a single
    write trip ``CANNOT_DETERMINE_TYPE`` — populating every column
    avoids that orthogonal failure mode here.
    """
    now_iso = datetime.now(timezone.utc).replace(tzinfo=None).isoformat()
    return {
        "run_id": f"kitchen_sink_{time.monotonic_ns()}",
        "run_timestamp": now_iso,
        "owner": "test", "bu": "test",
        "dataset_name": "ds_kitchen", "table_name": "tbl_kitchen",
        "metric_name": "auc", "metric_value": 0.91,
        "collection_type": "sample",
        "validation_name": "kitchen_sink_check",
        "validation_type": "pattern",
        "validation_status": "ERROR",
        "validation_message": "kitchen-sink fixture",
        "notification_channel": "slack", "notification_status": "sent",
        "details_json": dict(details),
        "record_type": "validation",
        "collector_name": "agg.ds_kitchen",
        "dimension_value": "all",
        "collected_at": now_iso,
        "validated_at": now_iso,
        "notified_at": now_iso,
        "partition_ts": now_iso,
        "expected_value": "0.0",
        "actual_value_text": "0.91",
        "dataset_description": "kitchen-sink test dataset",
        "validation_description": "kitchen-sink test validation",
        "is_active": "true",
    }


def test_kitchen_sink_size_under_cap():
    """Defensive 32 KB growth alarm — not a backend constraint
    (Spark STRING and SQLite TEXT are unbounded; Spark's JDBC
    auto-create on Oracle defaults to CLOB). Catches a future
    contributor padding the fixture to maintenance-burden size.
    """
    serialized = json.dumps(_KITCHEN_SINK)
    size_bytes = len(serialized.encode("utf-8"))
    assert size_bytes < 32 * 1024, (
        f"_KITCHEN_SINK serialized size {size_bytes} exceeds the 32 KB "
        "growth alarm; trim or split before merging."
    )


# ---------------------------------------------------------------------
# SQLite — always runs.
# ---------------------------------------------------------------------

class TestSQLiteRoundTrip:
    def test_kitchen_sink_roundtrip(self):
        from qualifire.storage.sqlite_storage import SQLiteStorage

        storage = SQLiteStorage(db_path=":memory:")
        storage.initialize()
        storage.write_results([_row(_KITCHEN_SINK)])
        rows = storage.read_health_data(days=365)
        assert len(rows) == 1, (
            f"expected 1 row from read_health_data; got {len(rows)}. "
            "If 0, the 'now() - days' window may be excluding the test "
            "row (likely the row uses a stale hardcoded run_timestamp)."
        )
        decoded = json.loads(rows[0]["details_json"])
        _assert_kitchen_sink_roundtrip(decoded, where="SQLite")


# ---------------------------------------------------------------------
# ExternalCatalogStorage — local SparkSession + spark_catalog
# (mirrors test_external_catalog_real.py pattern).
# ---------------------------------------------------------------------

@pytest.fixture(scope="module")
def _ec_spark():
    pytest.importorskip("pyspark")
    from pyspark.sql import SparkSession

    s = (
        SparkSession.builder
        .master("local[1]")
        .appName("qualifire-details-json-roundtrip")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.legacy.createHiveTableByDefault", "false")
        .getOrCreate()
    )
    # ExternalCatalogStorage.initialize() now forces
    # format("aidataplatform") to route CREATE TABLE through the
    # AIDP connector into the underlying Autonomous Database.
    # Local CI ships vanilla PySpark without the connector jar, so
    # this end-to-end roundtrip can only run inside AIDP Workbench.
    try:
        s.read.format("aidataplatform").load()
    except Exception as e:
        if "Failed to find data source" in str(e) or "aidataplatform.DefaultSource" in str(e):
            s.stop()
            pytest.skip(
                "aidataplatform data source not on classpath — "
                "ExternalCatalogStorage roundtrip can only run inside AIDP"
            )
    yield s
    s.stop()


class TestExternalCatalogRoundTrip:
    def test_kitchen_sink_roundtrip(self, _ec_spark):
        from qualifire.storage.external_catalog import ExternalCatalogStorage

        table = f"spark_catalog.default.qf_details_rt_{time.monotonic_ns()}"
        _ec_spark.sql(f"DROP TABLE IF EXISTS {table}")
        try:
            storage = ExternalCatalogStorage(spark=_ec_spark, table=table)
            storage.initialize()
            storage.write_results([_row(_KITCHEN_SINK)])
            rows = storage.read_health_data(days=365)
            assert len(rows) == 1
            decoded = json.loads(rows[0]["details_json"])
            _assert_kitchen_sink_roundtrip(decoded, where="ExternalCatalog")
        finally:
            try:
                _ec_spark.sql(f"DROP TABLE IF EXISTS {table}")
            except Exception:
                pass


# ---------------------------------------------------------------------
# DeltaStorage — local SparkSession + delta-spark.
# ---------------------------------------------------------------------

@pytest.fixture(scope="module")
def _delta_spark():
    """Delta-enabled SparkSession.

    PySpark sessions are JVM-process-global: calling ``getOrCreate()``
    returns the existing session and silently DROPS new configs
    ("Using an existing Spark session; only runtime SQL
    configurations will take effect"). Since other fixtures in this
    file may have already created a non-Delta session, we must
    stop that session before configuring delta extensions —
    otherwise ``USING DELTA`` DDL would fail at write time even
    though delta-spark is on the classpath.
    """
    pytest.importorskip("pyspark")
    pytest.importorskip("delta")
    try:
        from delta import configure_spark_with_delta_pip
    except Exception:
        pytest.skip("delta-spark configuration helper unavailable")

    from pyspark.sql import SparkSession

    active = SparkSession.getActiveSession()
    if active is not None:
        active.stop()

    builder = (
        SparkSession.builder
        .master("local[1]")
        .appName("qualifire-details-json-delta")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.sql.session.timeZone", "UTC")
        .config(
            "spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension",
        )
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
    )
    try:
        s = configure_spark_with_delta_pip(builder).getOrCreate()
    except Exception as e:
        pytest.skip(f"delta-spark session unavailable: {e}")
    yield s
    s.stop()


class TestDeltaRoundTrip:
    def test_kitchen_sink_roundtrip(self, _delta_spark):
        from qualifire.storage.delta_storage import DeltaStorage

        table = f"qf_details_rt_delta_{time.monotonic_ns()}"
        _delta_spark.sql(f"DROP TABLE IF EXISTS {table}")
        try:
            storage = DeltaStorage(spark=_delta_spark, table=table)
            storage.initialize()
            storage.write_results([_row(_KITCHEN_SINK)])
            rows = storage.read_health_data(days=365)
            assert len(rows) == 1
            decoded = json.loads(rows[0]["details_json"])
            _assert_kitchen_sink_roundtrip(decoded, where="Delta")
        finally:
            try:
                _delta_spark.sql(f"DROP TABLE IF EXISTS {table}")
            except Exception:
                pass


# ---------------------------------------------------------------------
# JDBCStorage — Spark-side serialization parity (no driver needed).
#
# Captures the DataFrame produced inside ``write_results`` before it
# hits the JDBC writer, materializes it via ``df.collect()``, and
# round-trips the captured ``details_json`` string via ``json.loads``.
# Catches a regression in ``cleaned[col] = json.dumps(v)`` and in
# the Row → DataFrame → STRING column enforcement layer.
# ---------------------------------------------------------------------

@pytest.fixture(scope="module")
def _jdbc_spark():
    pytest.importorskip("pyspark")
    from pyspark.sql import SparkSession

    s = (
        SparkSession.builder
        .master("local[1]")
        .appName("qualifire-details-json-jdbc")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    yield s
    s.stop()


class TestJDBCSparkSideSerialization:
    def test_kitchen_sink_serialization(self, _jdbc_spark, monkeypatch):
        from pyspark.sql.readwriter import DataFrameWriter

        from qualifire.storage.jdbc_storage import JDBCStorage

        # Class-level swallow: every DataFrameWriter.jdbc call is a
        # no-op for the duration of the test.
        monkeypatch.setattr(
            DataFrameWriter, "jdbc", lambda *a, **kw: None,
        )

        storage = JDBCStorage(
            spark=_jdbc_spark, table="qf_details_rt_jdbc_mock",
            jdbc_url="jdbc:postgresql://unused/db",
            connection_properties={"user": "u", "password": "p"},
        )

        captured: dict = {}
        original_create = _jdbc_spark.createDataFrame

        def capturing_create(*args, **kwargs):
            df = original_create(*args, **kwargs)
            captured["rows"] = df.collect()
            return df

        monkeypatch.setattr(
            _jdbc_spark, "createDataFrame", capturing_create,
        )

        storage.write_results([_row(_KITCHEN_SINK)])
        assert "rows" in captured, (
            "createDataFrame was never called inside write_results — "
            "the patch target is wrong or write_results changed shape."
        )
        serialized = captured["rows"][0].asDict()["details_json"]
        assert isinstance(serialized, str), (
            f"details_json must be persisted as a STRING; got "
            f"{type(serialized).__name__}"
        )
        _assert_kitchen_sink_roundtrip(
            json.loads(serialized), where="JDBC-spark-side",
        )


# ---------------------------------------------------------------------
# JDBCStorage — env-gated real-driver round-trip.
#
# Documented contract: SQLite-via-JDBC for local dev (``sqlite-jdbc``
# jar on classpath), e.g.
#   QUALIFIRE_JDBC_TEST_URL=jdbc:sqlite:/tmp/qf_test.db
# Stale tables accumulate across runs in the underlying file; clear
# with ``rm /tmp/qf_test.db`` between local runs. Not for Oracle /
# ADW — those run in separate live tests outside this suite.
# ---------------------------------------------------------------------

@pytest.fixture
def _jdbc_real_storage(_jdbc_spark):
    jdbc_url = os.environ.get("QUALIFIRE_JDBC_TEST_URL")
    if not jdbc_url:
        pytest.skip(
            "JDBC real-driver round-trip skipped — set "
            "QUALIFIRE_JDBC_TEST_URL (documented contract: "
            "SQLite-via-JDBC for local dev, e.g. "
            "jdbc:sqlite:/tmp/qf_test.db with sqlite-jdbc jar on "
            "classpath. Not for Oracle / ADW)."
        )
    from qualifire.storage.jdbc_storage import JDBCStorage

    table = f"qf_details_rt_real_{time.monotonic_ns()}"
    storage = JDBCStorage(
        spark=_jdbc_spark, table=table, jdbc_url=jdbc_url,
        connection_properties={},
    )
    storage.initialize()
    yield storage


class TestJDBCRealDriverRoundTrip:
    def test_kitchen_sink_roundtrip(self, _jdbc_real_storage):
        _jdbc_real_storage.write_results([_row(_KITCHEN_SINK)])
        rows = _jdbc_real_storage.read_health_data(days=365)
        assert len(rows) == 1
        decoded = json.loads(rows[0]["details_json"])
        _assert_kitchen_sink_roundtrip(decoded, where="JDBC-real")
