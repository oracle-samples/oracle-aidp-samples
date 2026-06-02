"""Tests for the engine's ``skip_recollection`` pre-pass.

Contract (per skip-recollection plan v4):
- Default-False: collector runs as today. (Regression check
  against existing collector tests is the implicit guard.)
- Flag=True + every expected ``(metric, dim)`` combination has
  an active non-NULL row at the resolved partition_ts → replay
  synthetic ``CollectionResult``s with
  ``metadata["from_cache"]=True``; collector NOT run.
- Flag=True + any expected combination missing → fall through
  to collector.
- Tombstoned (`is_active='false'`) rows hide the live row at
  the same key → cache miss → collector runs.
- Pattern / AnomalyDetection / SLO have no
  ``expected_metrics()`` → fall through to collector.
- Dimensional collector: enumerate dim values from system
  table; require every persisted dim has an active row.
  Empty enumeration = cache miss (cold partition / new dim).
- Filter scope NOT in natural key — a row persisted under no
  filter replays against a current run with a filter set
  (documented trade-off; same shape as ``skip_revalidation``).
- ``Qualifire.run_config[_parsed]`` / ``Qualifire.backfill`` /
  ``Qualifire.validate`` all surface the kwarg.
"""
from __future__ import annotations

from datetime import datetime
from unittest.mock import MagicMock

import pandas as pd
import pytest

from qualifire.api import Qualifire
from qualifire.backends.pandas_backend import PandasBackend
from qualifire.core.config import (
    AggregationCollectionConfig,
    DatasetConfig,
    QualifireConfig,
    ThresholdLevels,
    ThresholdRuleConfig,
    ThresholdValidationConfig,
)


def _make_qf_and_cfg(tmp_path, *, dimensions: list[str] | None = None):
    db_path = str(tmp_path / "qf.db")
    backend = PandasBackend()
    backend.register_table("t", pd.DataFrame({
        "event_date": ["2026-04-01"] * 10,
        "region": ["US"] * 5 + ["EU"] * 5,
        "x": list(range(10)),
    }))
    qf = Qualifire(
        backend=backend, system_table=db_path,
        system_table_backend="sqlite", owner="o", bu="b",
    )
    coll = AggregationCollectionConfig(
        expressions={"cnt": "COUNT(*)"},
        dimensions=dimensions,
    )
    ds = DatasetConfig(
        name="ds", table="t",
        partition_ts="2026-04-01",
        partition_step="P1D",
        validations=[ThresholdValidationConfig(
            collection=coll,
            rules=[ThresholdRuleConfig(
                metric="cnt",
                thresholds=ThresholdLevels(),
            )],
        )],
    )
    cfg = QualifireConfig(
        owner="o", bu="b", system_table=db_path, datasets=[ds],
    )
    return qf, cfg


class TestSkipRecollectionBehavior:
    def test_T1_persisted_row_replays_collector_not_called(
        self, tmp_path, monkeypatch,
    ):
        """T1: with skip_recollection=True and a persisted
        collection row, the AggregationCollector's `collect`
        method is NOT called; the persisted value replays."""
        qf, cfg = _make_qf_and_cfg(tmp_path)
        qf.run_config_parsed(cfg)  # priming

        from qualifire.collection.aggregation import AggregationCollector
        original = AggregationCollector.collect
        cnt = [0]

        def counting(self, *a, **kw):
            cnt[0] += 1
            return original(self, *a, **kw)

        monkeypatch.setattr(AggregationCollector, "collect", counting)

        result = qf.run_config_parsed(cfg, skip_recollection=True)
        assert cnt[0] == 0, (
            f"collector should not run; got {cnt[0]} call(s)"
        )
        # Validator still ran; result has the metric.
        all_vrs = [
            vr for ds in result.datasets for vr in ds.validation_results
        ]
        assert all_vrs

    def test_T2_default_false_collector_runs(
        self, tmp_path, monkeypatch,
    ):
        qf, cfg = _make_qf_and_cfg(tmp_path)
        qf.run_config_parsed(cfg)

        from qualifire.collection.aggregation import AggregationCollector
        original = AggregationCollector.collect
        cnt = [0]

        def counting(self, *a, **kw):
            cnt[0] += 1
            return original(self, *a, **kw)

        monkeypatch.setattr(AggregationCollector, "collect", counting)

        qf.run_config_parsed(cfg)  # default skip_recollection=False
        assert cnt[0] >= 1

    def test_T3_no_persisted_row_collector_runs(
        self, tmp_path, monkeypatch,
    ):
        """No priming run → no persisted state. Pre-pass returns
        None and collector runs."""
        qf, cfg = _make_qf_and_cfg(tmp_path)

        from qualifire.collection.aggregation import AggregationCollector
        original = AggregationCollector.collect
        cnt = [0]

        def counting(self, *a, **kw):
            cnt[0] += 1
            return original(self, *a, **kw)

        monkeypatch.setattr(AggregationCollector, "collect", counting)

        qf.run_config_parsed(cfg, skip_recollection=True)
        assert cnt[0] >= 1

    def test_T4_tombstoned_row_falls_through_to_collector(
        self, tmp_path, monkeypatch,
    ):
        qf, cfg = _make_qf_and_cfg(tmp_path)
        qf.run_config_parsed(cfg)  # priming

        # Tombstone the persisted collection row.
        storage = qf._storage
        conn = storage._get_conn()
        conn.execute(
            f"UPDATE {storage._table} SET is_active='false' "
            f"WHERE record_type='collection'"
        )
        conn.commit()

        from qualifire.collection.aggregation import AggregationCollector
        original = AggregationCollector.collect
        cnt = [0]

        def counting(self, *a, **kw):
            cnt[0] += 1
            return original(self, *a, **kw)

        monkeypatch.setattr(AggregationCollector, "collect", counting)
        qf.run_config_parsed(cfg, skip_recollection=True)
        assert cnt[0] >= 1


class TestDimensionalEnumeration:
    def test_T5_dimensional_collector_replays_all_dim_rows(
        self, tmp_path, monkeypatch,
    ):
        """Dimensional collector with persisted rows for each
        dim value → pre-pass replays one row per dim."""
        qf, cfg = _make_qf_and_cfg(tmp_path, dimensions=["region"])
        qf.run_config_parsed(cfg)  # priming — persists 2 dim rows (US, EU)

        from qualifire.collection.aggregation import AggregationCollector
        original = AggregationCollector.collect
        cnt = [0]

        def counting(self, *a, **kw):
            cnt[0] += 1
            return original(self, *a, **kw)

        monkeypatch.setattr(AggregationCollector, "collect", counting)

        result = qf.run_config_parsed(cfg, skip_recollection=True)
        assert cnt[0] == 0, (
            "dimensional pre-pass should replay all persisted "
            f"dim rows; got {cnt[0]} collector call(s)"
        )

    def test_T6_empty_dim_enumeration_falls_through(
        self, tmp_path, monkeypatch,
    ):
        """Cold partition (no priming run) for a dimensional
        collector → enumeration returns empty list → cache miss
        → collector runs (NOT vacuously satisfied)."""
        qf, cfg = _make_qf_and_cfg(tmp_path, dimensions=["region"])

        from qualifire.collection.aggregation import AggregationCollector
        original = AggregationCollector.collect
        cnt = [0]

        def counting(self, *a, **kw):
            cnt[0] += 1
            return original(self, *a, **kw)

        monkeypatch.setattr(AggregationCollector, "collect", counting)

        qf.run_config_parsed(cfg, skip_recollection=True)
        assert cnt[0] >= 1, (
            "empty dim enumeration must fall through to collector, "
            "NOT skip vacuously"
        )


class TestStorageHelperContract:
    def test_dim_values_helper_returns_distinct_actives(self, tmp_path):
        """Pin the new storage helper's contract: returns distinct
        active dim values, excludes tombstones, excludes NULL
        metric_value rows."""
        from qualifire.storage.sqlite_storage import SQLiteStorage

        s = SQLiteStorage(db_path=":memory:")
        s.initialize()
        # 3 dim values, one tombstoned, one with NULL value.
        common = {
            "owner": "o", "bu": "b",
            "dataset_name": "ds", "table_name": "t",
            "metric_name": "cnt", "validation_name": None,
            "validation_type": None, "validation_status": None,
            "validation_message": None,
            "partition_ts": "2026-04-01T00:00:00",
            "record_type": "collection",
            "details_json": {},
        }
        s.write_results([
            {**common, "run_id": "r1", "run_timestamp": "2026-04-01T10",
             "metric_value": 5.0, "dimension_value": "US",
             "is_active": "true"},
            {**common, "run_id": "r2", "run_timestamp": "2026-04-01T10",
             "metric_value": 5.0, "dimension_value": "EU",
             "is_active": "true"},
            {**common, "run_id": "r3", "run_timestamp": "2026-04-01T11",
             "metric_value": 5.0, "dimension_value": "EU",
             "is_active": "false"},  # tombstone for EU
            {**common, "run_id": "r4", "run_timestamp": "2026-04-01T10",
             "metric_value": None, "dimension_value": "APAC",
             "is_active": "true"},  # NULL metric_value
        ])
        dims = s.read_collection_dim_values_at_partition(
            table_name="t", metric_name="cnt",
            partition_ts="2026-04-01T00:00:00",
        )
        # US is active + non-NULL → included.
        # EU is tombstoned (latest is_active='false') → excluded.
        # APAC's metric_value IS NULL → excluded.
        assert dims == ["US"], f"got {dims}"


class TestParallelBackfillFlagPlumbing:
    def test_T_parallel_backfill_threads_flag(
        self, tmp_path, monkeypatch,
    ):
        """The recurring indent-bug check: every parallel backfill
        worker's QualifireContext receives skip_recollection=True."""
        qf, cfg = _make_qf_and_cfg(tmp_path)
        from qualifire.core import backfill as bf_mod

        captured: list[bool] = []
        original = bf_mod._run_anchor_once

        def capturing(*args, **kwargs):
            captured.append(kwargs.get("skip_recollection", False))
            return original(*args, **kwargs)

        monkeypatch.setattr(bf_mod, "_run_anchor_once", capturing)
        qf.backfill(
            cfg, partition_ts=("2026-04-01", "2026-04-03"),
            parallelism=4, skip_recollection=True,
        )
        assert captured
        assert all(captured), (
            f"all workers must receive skip_recollection=True; got {captured}"
        )


class TestFilterScopeReplay:
    def test_filter_changes_still_replay_documented_tradeoff(
        self, tmp_path, monkeypatch,
    ):
        """Plan v4 Locked Decision 3: filter NOT in natural key.
        A row persisted under no filter is replayed against a
        current run with a filter set. Documented trade-off."""
        qf, cfg_no_filter = _make_qf_and_cfg(tmp_path)
        qf.run_config_parsed(cfg_no_filter)  # priming, filter=None

        # Build a SECOND config with a filter set on the dataset.
        ds_filtered = cfg_no_filter.datasets[0].model_copy(update={
            "filter": "x > 0",
        })
        cfg_filtered = cfg_no_filter.model_copy(update={
            "datasets": [ds_filtered],
        })

        from qualifire.collection.aggregation import AggregationCollector
        original = AggregationCollector.collect
        cnt = [0]

        def counting(self, *a, **kw):
            cnt[0] += 1
            return original(self, *a, **kw)

        monkeypatch.setattr(AggregationCollector, "collect", counting)

        qf.run_config_parsed(cfg_filtered, skip_recollection=True)
        assert cnt[0] == 0, (
            "filter NOT in natural key (Locked Decision 3): a "
            "row persisted under no filter REPLAYS against a "
            "current run with a filter set. This pins the "
            "documented trade-off."
        )

        # Control: same filtered config without skip_recollection
        # — collector DOES run, and (if we unpatched) would emit
        # a different metric value reflecting the filter
        # (`x > 0` excludes the row with x=0). Codex impl-review
        # R1 LOW: shows the "scope replay tradeoff" is concrete,
        # not a tautology.
        cnt[0] = 0
        qf.run_config_parsed(cfg_filtered)  # default False
        assert cnt[0] >= 1, (
            "control: filtered config without skip_recollection "
            "MUST invoke the collector (otherwise the trade-off "
            "claim above is vacuous)"
        )
