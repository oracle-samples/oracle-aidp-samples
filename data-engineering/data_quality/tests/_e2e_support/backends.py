"""Shared E2E harness: backends, storage, engine runner, history seeder.

Extracted verbatim from ``tests/test_e2e.py`` for reuse by the industry
packs under ``tests/test_e2e_industries/``. Behavior is unchanged — any
semantic edit here is a regression against the shipped 33-test baseline.
"""

from __future__ import annotations

import json
from datetime import datetime, timedelta
from typing import Any

import pandas as pd

from qualifire.backends.pandas_backend import PandasBackend
from qualifire.core.config import QualifireConfig
from qualifire.core.context import QualifireContext
from qualifire.core.engine import QualifireEngine
from qualifire.core.exceptions import QualifireValidationError
from qualifire.storage.sqlite_storage import SQLiteStorage


class _SparkRow:
    """Mimics a Spark Row for PandasBackend compatibility."""

    def __init__(self, series: pd.Series):
        self._data = series.to_dict()
        for k, v in self._data.items():
            setattr(self, k, v)

    def __getitem__(self, key):
        if isinstance(key, int):
            return list(self._data.values())[key]
        return self._data[key]

    def asDict(self):
        return dict(self._data)


class E2EPandasBackend(PandasBackend):
    """PandasBackend with .collect() on SQL results for Spark-style collectors."""

    def execute_sql(self, sql: str) -> pd.DataFrame:
        df = super().execute_sql(sql)

        class _CDF(pd.DataFrame):
            def collect(self):
                return [_SparkRow(row) for _, row in self.iterrows()]

        return _CDF(df)


def _make_spark_backend(tables: dict[str, pd.DataFrame]) -> Any:
    """Create a SparkBackend with tables registered as temp views."""
    from pyspark.sql import SparkSession

    from qualifire.backends.spark_backend import SparkBackend

    spark = (
        SparkSession.builder
        .master("local[1]")
        .appName("qualifire-e2e")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "1")
        .getOrCreate()
    )

    for name, pdf in tables.items():
        # Spark's DoubleType NaN is distinct from NULL and propagates
        # through AVG/SUM. Normalize pandas NaN → Python None so Spark
        # infers proper nullable columns where COUNT/AVG skip missing
        # values (the pandas SQL path already does this). Scoped to
        # pandas→spark ingestion so engine-native Spark tables are
        # untouched.
        if pdf.isna().any().any():
            pdf = pdf.astype(object).where(pdf.notna(), None)
        sdf = spark.createDataFrame(pdf)
        sdf.createOrReplaceTempView(name)

    return SparkBackend(spark)


def _make_backend(backend_type: str, tables: dict[str, pd.DataFrame]) -> Any:
    """Create the appropriate backend based on type."""
    if backend_type == "spark":
        return _make_spark_backend(tables)
    return E2EPandasBackend(tables=tables)


def _make_storage() -> SQLiteStorage:
    storage = SQLiteStorage(db_path=":memory:")
    storage.initialize()
    return storage


def _run(backend, storage, datasets, run_id="e2erun001"):
    """Run engine, return result regardless of ERROR severity."""
    has_pts = any(getattr(ds, "partition_ts", None) for ds in datasets)
    config_kwargs = dict(
        owner="e2e_test",
        bu="test_bu",
        system_table="qualifire_history",
        datasets=datasets,
    )
    if has_pts:
        config_kwargs["partition_step"] = "P1D"
    config = QualifireConfig(**config_kwargs)
    engine = QualifireEngine(
        backend=backend,
        storage=storage,
        context=QualifireContext(run_id=run_id),
        config=config,
    )
    try:
        return engine.run()
    except QualifireValidationError as e:
        return e.result


BASE_DATE = datetime(2024, 5, 1)


def _seed_history(storage, table_name, metric_name, values, step_days=7,
                  base_date: datetime | None = None):
    """Seed storage with historical metric values.

    ``base_date`` defaults to May 1, 2024 to preserve the exact
    timestamps used by the shipped ``test_e2e.py`` suite. Industry
    packs pass ``base_date=`` to anchor seed timestamps relative to
    ``datetime.now()``.

    Each seed row carries ``partition_ts = run_timestamp``: under the
    partition-anchored history-read contract (run_timestamp fallback
    removed), drift / forecast lookups read by partition_ts. Tests
    align the current-run anchor against these seeded partition_ts
    values via the dataset's ``partition_ts`` expression.
    """
    anchor = base_date if base_date is not None else BASE_DATE
    rows = []
    for i, val in enumerate(values):
        # Microsecond precision matches ``datetime.now().isoformat()`` in
        # the engine write path — without this, pandas' ``to_datetime``
        # infers a strict format from the first row and fails on mixed
        # resolutions, seen as "time data … doesn't match format
        # '%Y-%m-%dT%H:%M:%S.%f'" in the trend validator.
        ts = (anchor + timedelta(days=i * step_days)).isoformat(
            timespec="microseconds"
        )
        # partition_ts must round-trip with `datetime.isoformat()` (no
        # timespec) — that's what storage uses to serialize anchors in
        # `read_metric_history_by_partition`. For zero-microsecond
        # datetimes the two forms differ (".000000" vs ""), and the
        # IN-clause comparison would silently miss the seed.
        partition_iso = (anchor + timedelta(days=i * step_days)).isoformat()
        rows.append({
            "run_id": f"seed_{i}",
            "run_timestamp": ts,
            "owner": "e2e_test",
            "bu": "test_bu",
            "dataset_name": "ds",
            "table_name": table_name,
            "metric_name": metric_name,
            "metric_value": val,
            "collection_type": "aggregation",
            "validation_name": None,
            "validation_type": None,
            "validation_status": None,
            "validation_message": None,
            "notification_channel": None,
            "notification_status": None,
            "details_json": json.dumps({}),
            "record_type": "collection",
            "collector_name": "seed",
            "dimension_value": None,
            "collected_at": ts,
            "validated_at": None,
            "notified_at": None,
            "partition_ts": partition_iso,
            "is_active": "true",
        })
    storage.write_results(rows)


def _seed_partition_ts(base_date: datetime, step_days: int, n: int) -> str:
    """Compute the literal `partition_ts` string the current run should
    use so its lookback (anchor − k·step for k=1..n) lands exactly on
    seed rows written by `_seed_history(base_date=base_date,
    step_days=step_days, values=[v0, v1, …, v_{n-1}])`.

    Seeded row i has run_timestamp / partition_ts = ``base + i·step_days``.
    The most recent seed is at ``base + (n-1)·step_days``. We want
    ``current_anchor − step = base + (n-1)·step_days``, so
    ``current_anchor = base + n·step_days``. Returns that as a
    Jinja-quoted ISO literal ready to drop into ``partition_ts=``.
    """
    anchor = base_date + timedelta(days=n * step_days)
    return f"'{anchor.isoformat()}'"
