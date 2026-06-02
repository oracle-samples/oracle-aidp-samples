"""Tests for the runtime ``skip_revalidation`` flag.

When True, the engine's ``_validate`` calls a pre-pass that reads
all persisted validation rows at the dataset's resolved partition
and replays them as ValidationResults with
``details["from_cache"] = True``. The validator's compute is
bypassed.

Coverage matrix (T1-T8 from plan v5):
- T1: flag=True + persisted row → notifier sees cached result;
      validator's `validate` is NOT called.
- T2: flag=False (default) → validator runs as today.
- T3: flag=True, NO persisted row → validator runs (fall-through).
- T4: flag=True, persisted row but is_active='false' → validator
      runs (tombstone honored).
- T5: per-validator round-trip fidelity (rebuilt severity / message /
      metric / value / dim).
- T6: parallel backfill threads the flag to every worker.
- T7: cross-flag — skip_revalidation + skip_recollection
      independent.
- T8: rebuilt result carries ``details.from_cache=True``.
"""
from __future__ import annotations

from datetime import datetime
from unittest.mock import MagicMock

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
from qualifire.core.context import QualifireContext
from qualifire.core.engine import QualifireEngine
from qualifire.core.models import Severity


def _make_qf_and_cfg(tmp_path):
    """Set both Qualifire(system_table=...) and QualifireConfig
    (system_table=...) to the SAME tmp-path SQLite file. Without
    that, the engine's storage swap reinits to the config's bare
    ``system_table`` name (e.g. './qf'), which accumulates state
    across pytest invocations."""
    import pandas as pd

    db_path = str(tmp_path / "qf.db")
    backend = PandasBackend()
    backend.register_table("t", pd.DataFrame({
        "event_date": ["2026-04-01"] * 10,
        "x": list(range(10)),
    }))
    qf = Qualifire(
        backend=backend,
        system_table=db_path,
        system_table_backend="sqlite",
        owner="o", bu="b",
    )
    ds = DatasetConfig(
        name="ds", table="t",
        partition_ts="2026-04-01",
        partition_step="P1D",
        validations=[ThresholdValidationConfig(
            collection=AggregationCollectionConfig(
                expressions={"cnt": "COUNT(*)"},
            ),
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


class TestSkipRevalidationBehavior:
    def test_T1_persisted_row_replays_validator_not_called(
        self, tmp_path, monkeypatch,
    ):
        """T1: with skip_revalidation=True and a persisted row at
        the partition, the validator's `validate` method is NOT
        called; the persisted verdict replays."""
        # First run — produces persisted rows.
        qf, cfg = _make_qf_and_cfg(tmp_path)
        qf.run_config_parsed(cfg)

        # Second run with skip_revalidation=True. Patch the
        # validator class to count calls; if we replay correctly,
        # `validate` is NOT called.
        from qualifire.validation.threshold import ThresholdValidator

        original_validate = ThresholdValidator.validate
        call_count = [0]

        def counting_validate(self, *args, **kwargs):
            call_count[0] += 1
            return original_validate(self, *args, **kwargs)

        monkeypatch.setattr(
            ThresholdValidator, "validate", counting_validate,
        )

        result = qf.run_config_parsed(cfg, skip_revalidation=True)
        assert call_count[0] == 0, (
            f"validator should not be called when skip_revalidation=True "
            f"and persisted rows exist; got {call_count[0]} calls"
        )
        # The result includes the cached verdict.
        all_vrs = [
            vr for ds in result.datasets for vr in ds.validation_results
        ]
        cached_vrs = [
            vr for vr in all_vrs if vr.details.get("from_cache") is True
        ]
        assert cached_vrs, (
            "expected at least one ValidationResult with details.from_cache=True"
        )

    def test_T2_flag_false_validator_runs_as_today(
        self, tmp_path, monkeypatch,
    ):
        """T2: default-False preserves today's behavior — validator
        runs even if a persisted row exists."""
        qf, cfg = _make_qf_and_cfg(tmp_path)
        qf.run_config_parsed(cfg)

        from qualifire.validation.threshold import ThresholdValidator
        original_validate = ThresholdValidator.validate
        call_count = [0]

        def counting_validate(self, *args, **kwargs):
            call_count[0] += 1
            return original_validate(self, *args, **kwargs)

        monkeypatch.setattr(
            ThresholdValidator, "validate", counting_validate,
        )

        qf.run_config_parsed(cfg)  # default skip_revalidation=False
        assert call_count[0] >= 1

    def test_T3_no_persisted_row_validator_runs(
        self, tmp_path, monkeypatch,
    ):
        """T3: skip_revalidation=True but no persisted row → falls
        through to validator."""
        qf, cfg = _make_qf_and_cfg(tmp_path)
        # Skip the priming run — no persisted state yet.

        from qualifire.validation.threshold import ThresholdValidator
        original_validate = ThresholdValidator.validate
        call_count = [0]

        def counting_validate(self, *args, **kwargs):
            call_count[0] += 1
            return original_validate(self, *args, **kwargs)

        monkeypatch.setattr(
            ThresholdValidator, "validate", counting_validate,
        )

        qf.run_config_parsed(cfg, skip_revalidation=True)
        assert call_count[0] >= 1, (
            "with no persisted row at the partition, validator must run"
        )


class TestSkipRevalidationT8FromCacheMarker:
    def test_T8_synthetic_result_carries_from_cache_marker(
        self, tmp_path,
    ):
        """T8: the rebuilt synthetic ValidationResult has
        details["from_cache"] = True."""
        qf, cfg = _make_qf_and_cfg(tmp_path)
        qf.run_config_parsed(cfg)

        result = qf.run_config_parsed(cfg, skip_revalidation=True)
        from_cache_seen = False
        for ds in result.datasets:
            for vr in ds.validation_results:
                if vr.details.get("from_cache") is True:
                    from_cache_seen = True
                    break
        assert from_cache_seen


class TestSkipRevalidationTombstone:
    def test_T4_tombstoned_row_falls_through_to_validator(
        self, tmp_path, monkeypatch,
    ):
        """T4: skip_revalidation=True with an `is_active='false'`
        row at the partition → validator runs (the tombstone hides
        the row from the per-partition lookup)."""
        qf, cfg = _make_qf_and_cfg(tmp_path)
        qf.run_config_parsed(cfg)

        # Manually tombstone the persisted validation row.
        storage = qf._storage
        conn = storage._get_conn()
        conn.execute(
            f"UPDATE {storage._table} SET is_active='false' "
            f"WHERE record_type='validation'"
        )
        conn.commit()

        from qualifire.validation.threshold import ThresholdValidator
        original_validate = ThresholdValidator.validate
        call_count = [0]

        def counting_validate(self, *args, **kwargs):
            call_count[0] += 1
            return original_validate(self, *args, **kwargs)

        monkeypatch.setattr(
            ThresholdValidator, "validate", counting_validate,
        )
        qf.run_config_parsed(cfg, skip_revalidation=True)
        assert call_count[0] >= 1, (
            "tombstoned row must NOT short-circuit; validator must run"
        )


class TestSkipRevalidationParallelBackfillPlumbing:
    def test_T6_parallel_backfill_threads_flag_to_workers(
        self, tmp_path, monkeypatch,
    ):
        """T6 (recurring indent-bug guard): every parallel backfill
        worker's QualifireContext receives skip_revalidation=True."""
        qf, cfg = _make_qf_and_cfg(tmp_path)

        from qualifire.core import backfill as bf_mod

        captured: list[bool] = []
        original = bf_mod._run_anchor_once

        def capturing(*args, **kwargs):
            captured.append(kwargs.get("skip_revalidation", False))
            return original(*args, **kwargs)

        monkeypatch.setattr(bf_mod, "_run_anchor_once", capturing)
        qf.backfill(
            cfg, partition_ts=("2026-04-01", "2026-04-03"),
            parallelism=4, skip_revalidation=True,
        )
        assert captured
        assert all(captured), (
            f"all workers must receive skip_revalidation=True; got {captured}"
        )


class TestRowToValidationResultRebuild:
    def test_severity_enum_parse(self):
        row = {
            "validation_name": "v",
            "validation_type": "threshold",
            "validation_status": "ERROR",
            "validation_message": "boom",
            "metric_name": "m",
            "metric_value": 0.5,
            "expected_value": '{"min": 100}',
            "actual_value_text": "0.5",
            "details_json": '{"k": 1}',
            "dimension_value": None,
        }
        vr = QualifireEngine._row_to_validation_result(row, "base")
        assert vr.severity == Severity.ERROR
        assert vr.metric_name == "m"
        assert vr.actual_value == 0.5
        assert vr.expected_value == {"min": 100}
        assert vr.details["k"] == 1
        assert vr.details["from_cache"] is True
        assert vr.validation_base_name == "base"

    def test_unknown_status_falls_back_to_warning(self):
        row = {
            "validation_status": "GIBBERISH",
            "validation_name": "v",
            "validation_type": "threshold",
            "validation_message": "",
            "metric_name": None, "metric_value": None,
            "expected_value": None, "actual_value_text": None,
            "details_json": None, "dimension_value": None,
        }
        vr = QualifireEngine._row_to_validation_result(row, "base")
        assert vr.severity == Severity.WARNING

    def test_dict_details_json_round_trips(self):
        row = {
            "validation_status": "PASS",
            "validation_name": "v",
            "validation_type": "threshold",
            "validation_message": "",
            "metric_name": "m", "metric_value": 1.0,
            "expected_value": None, "actual_value_text": None,
            "details_json": {"already_a_dict": True},
            "dimension_value": None,
        }
        vr = QualifireEngine._row_to_validation_result(row, "base")
        assert vr.details["already_a_dict"] is True
        assert vr.details["from_cache"] is True


class TestStorageHelperContract:
    def test_sqlite_helper_returns_empty_when_no_rows(self, tmp_path):
        from qualifire.storage.sqlite_storage import SQLiteStorage

        s = SQLiteStorage(db_path=":memory:")
        s.initialize()
        assert s.read_validations_at_partition(
            dataset_name="ds", validation_name_prefix="x",
            partition_ts=datetime(2026, 4, 1),
        ) == []

    def test_sqlite_helper_finds_validation_at_partition(self, tmp_path):
        from qualifire.storage.sqlite_storage import SQLiteStorage

        s = SQLiteStorage(db_path=":memory:")
        s.initialize()
        s.write_results([{
            "run_id": "r1",
            "run_timestamp": "2026-04-01T10:00:00",
            "owner": "o", "bu": "b",
            "dataset_name": "ds", "table_name": "t",
            "metric_name": "cnt", "metric_value": 5.0,
            "validation_name": "threshold.ds.cnt",
            "validation_type": "threshold",
            "validation_status": "PASS",
            "validation_message": "ok",
            "partition_ts": "2026-04-01T00:00:00",
            "dimension_value": None,
            "record_type": "validation",
            "is_active": "true",
            "details_json": {},
        }])
        rows = s.read_validations_at_partition(
            dataset_name="ds",
            validation_name_prefix="threshold.ds.cnt",
            partition_ts="2026-04-01T00:00:00",
        )
        assert len(rows) == 1
        assert rows[0]["validation_status"] == "PASS"

    def test_sqlite_helper_underscore_in_prefix_does_not_cross_match(self, tmp_path):
        """Codex impl-review R1 MAJOR: SQL LIKE treats `_` as a
        single-char wildcard. A prefix `qa_1` must NOT match
        `qaa1.schema` or `qa-1.schema` — escaping is required."""
        from qualifire.storage.sqlite_storage import SQLiteStorage

        s = SQLiteStorage(db_path=":memory:")
        s.initialize()
        # Persist a row whose validation_name would cross-match a
        # naive (unescaped) `qa_1.%` LIKE pattern.
        s.write_results([{
            "run_id": "r1",
            "run_timestamp": "2026-04-01T10:00:00",
            "owner": "o", "bu": "b",
            "dataset_name": "ds", "table_name": "t",
            "metric_name": None, "metric_value": None,
            "validation_name": "qaa1.schema",
            "validation_type": "pattern",
            "validation_status": "PASS",
            "validation_message": "",
            "partition_ts": "2026-04-01T00:00:00",
            "dimension_value": None,
            "record_type": "validation",
            "is_active": "true",
            "details_json": {},
        }])
        rows = s.read_validations_at_partition(
            dataset_name="ds",
            validation_name_prefix="qa_1",
            partition_ts="2026-04-01T00:00:00",
        )
        assert rows == [], (
            f"prefix 'qa_1' must not cross-match 'qaa1.schema' via "
            f"unescaped SQL LIKE; got {rows}"
        )

    def test_sqlite_helper_matches_dotted_subname(self, tmp_path):
        """Pattern's `.schema` sub-emission is caught by the
        prefix.LIKE pattern."""
        from qualifire.storage.sqlite_storage import SQLiteStorage

        s = SQLiteStorage(db_path=":memory:")
        s.initialize()
        for vname in ("pattern.ds", "pattern.ds.schema"):
            s.write_results([{
                "run_id": f"r-{vname}",
                "run_timestamp": "2026-04-01T10:00:00",
                "owner": "o", "bu": "b",
                "dataset_name": "ds", "table_name": "t",
                "metric_name": None, "metric_value": None,
                "validation_name": vname,
                "validation_type": "pattern",
                "validation_status": "PASS",
                "validation_message": "",
                "partition_ts": "2026-04-01T00:00:00",
                "dimension_value": None,
                "record_type": "validation",
                "is_active": "true",
                "details_json": {},
            }])
        rows = s.read_validations_at_partition(
            dataset_name="ds",
            validation_name_prefix="pattern.ds",
            partition_ts="2026-04-01T00:00:00",
        )
        names = {r["validation_name"] for r in rows}
        assert names == {"pattern.ds", "pattern.ds.schema"}
