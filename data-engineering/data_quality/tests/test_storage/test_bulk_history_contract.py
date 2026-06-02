"""Item 3 (backfill-followups-and-polish): cross-backend
``read_validation_history_bulk`` contract tests.

Two-stage CTE: per ``(input_idx, partition_ts)`` pick latest version,
drop tombstones, re-rank per ``input_idx``, apply ``LIMIT``.

T3.1 — limit=1 regression (returns 1 row per active key).
T3.2 — limit=3 returns up to 3 most-recent active rows per key.
T3.3 — partition-tombstone hides only its partition.
T3.4 — same-partition tombstone hides the entire partition.
T3.5 — mixed keys (some live, some all-tombstoned).
T3.6 — limit=0 / negative rejected.

T3.7 (JDBC-only) lives in test_bulk_suppression.py since it covers
the per-key-fallback contract.

Codex impl-review R2 MEDIUM: parametrized over SQLite (always)
and ExternalCatalog (when PySpark is importable). Delta uses the
same SQL shape as ExternalCatalog (one Spark backend pinning the
shape covers both). The JDBC backend's contract is covered by
``test_bulk_suppression.py`` which exercises the per-key fallback
path with `limit=3`.
"""
from __future__ import annotations

import time

import pytest

from qualifire.core.models import ValidationKey
from qualifire.storage.sqlite_storage import SQLiteStorage


def _row(*, run_ts: str, validated_at: str = None, run_id: str,
         partition_ts: str, dataset: str = "ds",
         validation: str = "v", metric: str = "m",
         dim: str = None, status: str = "PASS",
         is_active: str = "true",
         record_type: str = "validation") -> dict:
    return {
        "run_id": run_id,
        "run_timestamp": run_ts,
        "owner": "o", "bu": "b",
        "dataset_name": dataset, "table_name": "t",
        "metric_name": metric, "metric_value": None,
        "collection_type": None,
        "validation_name": validation,
        "validation_type": "threshold",
        "validation_status": status,
        "validation_message": "ok",
        "notification_channel": None, "notification_status": None,
        "details_json": None,
        "record_type": record_type,
        "collector_name": None,
        "dimension_value": dim,
        "collected_at": None,
        "validated_at": validated_at or run_ts,
        "notified_at": None,
        "partition_ts": partition_ts,
        "expected_value": None, "actual_value_text": None,
        "dataset_description": None, "validation_description": None,
        "is_active": is_active,
    }


@pytest.fixture
def storage():
    s = SQLiteStorage(db_path=":memory:")
    s.initialize()
    return s


class TestSqliteBulkContract:
    """SQLite is the in-tree no-Spark backend; runs the full
    cross-backend contract today. Spark backends inherit this via
    their dedicated test files."""

    def test_limit_1_returns_one_active_row_per_key(self, storage):
        # T3.1
        storage.write_results([
            _row(run_ts="2026-04-01T10", run_id="r1",
                 partition_ts="2026-04-01"),
            _row(run_ts="2026-04-02T10", run_id="r2",
                 partition_ts="2026-04-02"),
        ])
        keys = [ValidationKey(
            dataset_name="ds", validation_name="v",
            metric_name="m", dimension_value=None,
        )]
        out = storage.read_validation_history_bulk(keys, limit=1)
        assert len(out[keys[0]]) == 1
        # Most-recent partition wins.
        assert out[keys[0]][0]["run_timestamp"] == "2026-04-02T10"

    def test_limit_3_returns_up_to_3_per_key(self, storage):
        # T3.2: 5 partitions → limit=3 returns top 3.
        for i in range(5):
            storage.write_results([_row(
                run_ts=f"2026-04-0{i+1}T10",
                run_id=f"r{i}",
                partition_ts=f"2026-04-0{i+1}",
            )])
        keys = [ValidationKey(
            dataset_name="ds", validation_name="v",
            metric_name="m", dimension_value=None,
        )]
        out = storage.read_validation_history_bulk(keys, limit=3)
        assert len(out[keys[0]]) == 3
        # Sorted by run_timestamp DESC.
        ts = [r["run_timestamp"] for r in out[keys[0]]]
        assert ts == sorted(ts, reverse=True)

    def test_partition_tombstone_hides_only_its_partition(self, storage):
        # T3.3: 5 partitions, the latest partition has a tombstone
        # in its latest version → that partition contributes 0,
        # the older 4 partitions still surface up to limit=3.
        for i in range(4):
            storage.write_results([_row(
                run_ts=f"2026-04-0{i+1}T10",
                run_id=f"r{i}",
                partition_ts=f"2026-04-0{i+1}",
            )])
        # Partition #5 has an active row, then a tombstone (latest).
        storage.write_results([_row(
            run_ts="2026-04-05T10", run_id="r5",
            partition_ts="2026-04-05",
        )])
        storage.write_results([_row(
            run_ts="2026-04-05T11", run_id="r5b",
            partition_ts="2026-04-05",
            is_active="false",
        )])
        keys = [ValidationKey(
            dataset_name="ds", validation_name="v",
            metric_name="m", dimension_value=None,
        )]
        out = storage.read_validation_history_bulk(keys, limit=3)
        # 3 most-recent ACTIVE partitions = 2026-04-04, 04-03, 04-02.
        # 04-05's latest is a tombstone → excluded.
        partition_tss = [r["partition_ts"] for r in out[keys[0]]]
        assert partition_tss == [
            "2026-04-04", "2026-04-03", "2026-04-02",
        ]

    def test_same_partition_tombstone_hides_partition(self, storage):
        # T3.4: 5 versions of the SAME partition; latest is a
        # tombstone → partition contributes 0 rows for that key.
        for i in range(5):
            is_active = "false" if i == 4 else "true"
            storage.write_results([_row(
                run_ts=f"2026-04-01T1{i}",
                run_id=f"r{i}",
                partition_ts="2026-04-01",
                is_active=is_active,
            )])
        keys = [ValidationKey(
            dataset_name="ds", validation_name="v",
            metric_name="m", dimension_value=None,
        )]
        out = storage.read_validation_history_bulk(keys, limit=3)
        assert out[keys[0]] == []

    def test_mixed_keys_some_live_some_tombstoned(self, storage):
        # T3.5: key A has 2 active partitions; key B is tombstoned
        # in its only partition. Result: A returns 2, B returns 0.
        for i in range(2):
            storage.write_results([_row(
                run_ts=f"2026-04-0{i+1}T10",
                run_id=f"a{i}", validation="va", metric="ma",
                partition_ts=f"2026-04-0{i+1}",
            )])
        # B: single partition, latest is tombstone.
        storage.write_results([_row(
            run_ts="2026-04-01T10", run_id="b1",
            validation="vb", metric="mb",
            partition_ts="2026-04-01",
        )])
        storage.write_results([_row(
            run_ts="2026-04-01T11", run_id="b2",
            validation="vb", metric="mb",
            partition_ts="2026-04-01",
            is_active="false",
        )])
        ka = ValidationKey(
            dataset_name="ds", validation_name="va",
            metric_name="ma", dimension_value=None,
        )
        kb = ValidationKey(
            dataset_name="ds", validation_name="vb",
            metric_name="mb", dimension_value=None,
        )
        out = storage.read_validation_history_bulk([ka, kb], limit=3)
        assert len(out[ka]) == 2
        assert out[kb] == []

    def test_limit_zero_negative_rejected(self, storage):
        # T3.6
        keys = [ValidationKey(
            dataset_name="ds", validation_name="v",
            metric_name="m", dimension_value=None,
        )]
        with pytest.raises(ValueError, match=">= 1"):
            storage.read_validation_history_bulk(keys, limit=0)
        with pytest.raises(ValueError, match=">= 1"):
            storage.read_validation_history_bulk(keys, limit=-1)


# Spark-backed parametrization (codex R2 MEDIUM). The
# importorskip lives INSIDE the fixture (codex R3 MEDIUM) so the
# SQLite contract above runs unconditionally — only the EC tests
# skip when PySpark is absent. Delta inherits the same SQL string.


@pytest.fixture(scope="module")
def _ec_storage():
    pytest.importorskip("pyspark")
    from pyspark.sql import SparkSession
    from qualifire.storage.external_catalog import ExternalCatalogStorage

    s = (
        SparkSession.builder
        .master("local[1]")
        .appName("qualifire-bulk-history-contract")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.legacy.createHiveTableByDefault", "false")
        .getOrCreate()
    )
    # ExternalCatalogStorage.initialize() now forces
    # format("aidataplatform") so the AIDP connector routes
    # CREATE TABLE into ADW. Local CI doesn't ship the connector
    # jar; skip cleanly when absent.
    try:
        s.read.format("aidataplatform").load()
    except Exception as e:
        if "Failed to find data source" in str(e) or "aidataplatform.DefaultSource" in str(e):
            s.stop()
            pytest.skip(
                "aidataplatform data source not on classpath — "
                "ExternalCatalog bulk-contract test can only run inside AIDP"
            )
    table = f"spark_catalog.default.qf_bulk_contract_{time.monotonic_ns()}"
    s.sql(f"DROP TABLE IF EXISTS {table}")
    storage = ExternalCatalogStorage(spark=s, table=table)
    storage.initialize()
    yield storage
    s.sql(f"DROP TABLE IF EXISTS {table}")
    s.stop()


class TestExternalCatalogBulkContract:
    """Codex R2 MEDIUM: at least one Spark backend pins the
    cross-dialect shape. Delta uses the identical CTE; covering
    ExternalCatalog covers the dialect."""

    def test_limit_3_partition_aware(self, _ec_storage):
        # Combined T3.2 + T3.3: 5 partitions, latest partition's
        # latest version tombstoned → limit=3 returns 3 active
        # partitions from the older 4.
        for i in range(4):
            _ec_storage.write_results([_row(
                run_ts=f"2026-04-0{i+1}T10",
                run_id=f"r{i}",
                partition_ts=f"2026-04-0{i+1}",
            )])
        _ec_storage.write_results([_row(
            run_ts="2026-04-05T10", run_id="r5",
            partition_ts="2026-04-05",
        )])
        _ec_storage.write_results([_row(
            run_ts="2026-04-05T11", run_id="r5b",
            partition_ts="2026-04-05",
            is_active="false",
        )])
        keys = [ValidationKey(
            dataset_name="ds", validation_name="v",
            metric_name="m", dimension_value=None,
        )]
        out = _ec_storage.read_validation_history_bulk(keys, limit=3)
        # Codex R3 MEDIUM: assert the exact ordered sequence so a
        # dialect-specific ordering bug (e.g., active-filter applied
        # before per-partition ranking) gets caught — length-only
        # would let the wrong rows pass.
        # Spark surfaces run_timestamp as a datetime; normalize to
        # ISO prefix for the comparison.
        actual = [str(r["run_timestamp"])[:13] for r in out[keys[0]]]
        # Stored as "2026-04-0NT10" → str(datetime) gives
        # "2026-04-0N 10:00:00" so ISO-prefix is "2026-04-0N 10".
        assert actual == [
            "2026-04-04 10", "2026-04-03 10", "2026-04-02 10",
        ], actual

    def test_limit_zero_negative_rejected(self, _ec_storage):
        keys = [ValidationKey(
            dataset_name="ds", validation_name="v",
            metric_name="m", dimension_value=None,
        )]
        with pytest.raises(ValueError, match=">= 1"):
            _ec_storage.read_validation_history_bulk(keys, limit=0)
