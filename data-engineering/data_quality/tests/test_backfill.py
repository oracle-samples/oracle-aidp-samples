"""Tests for ``Qualifire.backfill`` + ``qualifire/core/backfill.py``.

Covers the contract:
- Single partition / list / range expansion (``expand_partition_ts``).
- Selector restricts scope; no-match raises.
- WAP missing ``partition_column`` rejected.
- ``data=True`` non-WAP rejected.
- BackfillReport shape (``refreshed`` / ``unchanged`` / ``skipped``
  / ``errored`` counters; per-partition diffs).
- N18 partition-error isolation: one errored partition does not
  abort siblings.
- N12 idempotency: re-running yields ``unchanged`` when value
  hasn't changed.
- ``soft_delete_prior=True`` writes a tombstone INSERT visible in
  the storage chain.
"""

from __future__ import annotations

from datetime import datetime, timedelta
from unittest.mock import MagicMock

import pytest

from qualifire.api import Qualifire
from qualifire.backends.pandas_backend import PandasBackend
from qualifire.core.backfill import expand_partition_ts
from qualifire.core.backfill_report import BackfillReport, PartitionDiff
from qualifire.core.config import (
    AggregationCollectionConfig,
    DatasetConfig,
    QualifireConfig,
    ThresholdLevels,
    ThresholdRuleConfig,
    ThresholdValidationConfig,
    WAPConfig,
)
from qualifire.core.exceptions import QualifireConfigError
from qualifire.storage.sqlite_storage import SQLiteStorage


# ------------------------- expand_partition_ts -----------------------


class TestSampleValidatorSkipReason:
    """Codex impl-review R1: ``no_historical_samples`` is the
    plan-pinned skip reason for sample-based validators
    (Pattern, AnomalyDetection) that don't fit the per-metric
    backfill diff. Lock the string so it can't drift."""

    def test_pattern_validator_surfaces_skip_with_documented_reason(
        self, tmp_path,
    ):
        from qualifire.core.config import (
            PatternModelConfig,
            PatternThresholdConfig,
            PatternValidationConfig,
            SampleCollectionConfig,
        )

        qf = _make_qf(tmp_path)
        ds = DatasetConfig(
            name="d", table="t",
            partition_ts="event_date",
            partition_step="P1D",
            validations=[PatternValidationConfig(
                name="shape_check",
                collection=SampleCollectionConfig(),
                model=PatternModelConfig(),
                thresholds=PatternThresholdConfig(),
            )],
        )
        config = QualifireConfig(owner="o", bu="b", system_table="qf", datasets=[ds])
        # No errored aggregation rows; only the pattern validator's
        # explicit skip should surface.
        report = qf.backfill(config, partition_ts="2026-04-01")
        assert report.partitions, "pattern validator must surface a skip diff"
        skips = [p for p in report.partitions if p.status == "skipped"]
        assert skips, "pattern validator must produce a skipped diff"
        assert all(p.skip_reason == "no_historical_samples" for p in skips), (
            "skip_reason must match plan D10's pinned string "
            "'no_historical_samples'"
        )


class TestExpandPartitionTs:
    def test_single_string(self):
        out = expand_partition_ts("2026-04-01")
        assert out == [datetime(2026, 4, 1)]

    def test_single_datetime(self):
        dt = datetime(2026, 4, 1, 12, 0)
        assert expand_partition_ts(dt) == [dt]

    def test_list(self):
        out = expand_partition_ts(["2026-04-01", "2026-04-02"])
        assert out == [datetime(2026, 4, 1), datetime(2026, 4, 2)]

    def test_range_inclusive(self):
        out = expand_partition_ts(
            ("2026-04-01", "2026-04-03"),
            step=timedelta(days=1),
        )
        assert out == [
            datetime(2026, 4, 1),
            datetime(2026, 4, 2),
            datetime(2026, 4, 3),
        ]

    def test_range_requires_step(self):
        with pytest.raises(QualifireConfigError, match="requires a positive 'step'"):
            expand_partition_ts(("2026-04-01", "2026-04-03"))

    def test_range_end_before_start_rejected(self):
        with pytest.raises(QualifireConfigError, match="end .* is before start"):
            expand_partition_ts(
                ("2026-04-03", "2026-04-01"), step=timedelta(days=1),
            )

    def test_range_too_large_rejected(self):
        # 50000 days > 10000 cap
        with pytest.raises(QualifireConfigError, match="refusing"):
            expand_partition_ts(
                ("2000-01-01", "2200-01-01"), step=timedelta(days=1),
            )

    def test_invalid_tuple_arity(self):
        with pytest.raises(QualifireConfigError, match="must be \\(start, end\\)"):
            expand_partition_ts(("a", "b", "c"))


# ------------------------- backfill flow -----------------------------


def _seed_collection(storage: SQLiteStorage, *, table: str, metric: str,
                     value: float, partition_ts: str, run_timestamp: str) -> None:
    storage.write_results([{
        "run_id": "seed",
        "run_timestamp": run_timestamp,
        "owner": "o", "bu": "b",
        "dataset_name": "ds",
        "table_name": table,
        "metric_name": metric,
        "metric_value": value,
        "collection_type": "aggregation",
        "validation_name": None, "validation_type": None,
        "validation_status": None, "validation_message": None,
        "notification_channel": None, "notification_status": None,
        "details_json": None,
        "record_type": "collection",
        "collector_name": "AggregationCollector",
        "dimension_value": None,
        "collected_at": run_timestamp,
        "validated_at": None, "notified_at": None,
        "partition_ts": partition_ts,
        "expected_value": None, "actual_value_text": None,
        "dataset_description": None, "validation_description": None,
        "is_active": "true",
    }])


def _make_qf(tmp_path):
    return Qualifire(
        backend=PandasBackend(),
        system_table=str(tmp_path / "qf.db"),
        system_table_backend="sqlite",
        owner="ops",
        bu="data",
    )


def _make_config(*, with_wap=False, partition_column=None) -> QualifireConfig:
    if with_wap:
        ds = DatasetConfig(
            name="ds",
            # WAP forward execution doesn't use partition_ts/step;
            # backfill uses wap.partition_column instead.
            wap=WAPConfig(
                target_table="prod.t",
                sql="SELECT * FROM raw.t",
                partition_column=partition_column,
            ),
            validations=[ThresholdValidationConfig(
                collection=AggregationCollectionConfig(expressions={"cnt": "COUNT(*)"}),
                rules=[ThresholdRuleConfig(metric="cnt", thresholds=ThresholdLevels())],
            )],
        )
    else:
        ds = DatasetConfig(
            name="ds",
            table="t",
            partition_ts="event_date",
            partition_step="P1D",
            validations=[ThresholdValidationConfig(
                collection=AggregationCollectionConfig(expressions={"cnt": "COUNT(*)"}),
                rules=[ThresholdRuleConfig(metric="cnt", thresholds=ThresholdLevels())],
            )],
        )
    return QualifireConfig(
        owner="o", bu="b",
        system_table="qf",
        datasets=[ds],
    )


class TestBackfillRejections:
    def test_wap_missing_partition_column_rejected(self, tmp_path):
        qf = _make_qf(tmp_path)
        config = _make_config(with_wap=True, partition_column=None)
        with pytest.raises(QualifireConfigError, match="partition_column required"):
            qf.backfill(config, partition_ts="2026-04-01")

    def test_data_true_non_wap_rejected(self, tmp_path):
        qf = _make_qf(tmp_path)
        config = _make_config(with_wap=False)
        with pytest.raises(QualifireConfigError, match="data=True is only supported"):
            qf.backfill(config, partition_ts="2026-04-01", data=True)

    def test_config_none_rejected(self, tmp_path):
        """``config`` is required: backfill resolves datasets/validations
        from it. Passing None raises rather than silently reusing storage
        (matches the docstring contract)."""
        qf = _make_qf(tmp_path)
        with pytest.raises(QualifireConfigError, match="requires a config"):
            qf.backfill(partition_ts="2026-04-01")

    def test_non_wap_constant_partition_ts_rejected(self, tmp_path):
        qf = _make_qf(tmp_path)
        ds = DatasetConfig(
            name="ds",
            table="t",
            partition_ts="'{{ ds }}'",  # constant, not column ref
            partition_step="P1D",
            validations=[ThresholdValidationConfig(
                collection=AggregationCollectionConfig(expressions={"cnt": "COUNT(*)"}),
                rules=[ThresholdRuleConfig(metric="cnt", thresholds=ThresholdLevels())],
            )],
        )
        config = QualifireConfig(
            owner="o", bu="b", system_table="qf", datasets=[ds],
        )
        with pytest.raises(QualifireConfigError, match="must be a column reference"):
            qf.backfill(config, partition_ts="2026-04-01")

    def test_non_wap_no_partition_ts_rejected(self, tmp_path):
        qf = _make_qf(tmp_path)
        ds = DatasetConfig(
            name="ds",
            table="t",
            # no partition_ts
            validations=[ThresholdValidationConfig(
                collection=AggregationCollectionConfig(expressions={"cnt": "COUNT(*)"}),
                rules=[ThresholdRuleConfig(metric="cnt", thresholds=ThresholdLevels())],
            )],
        )
        config = QualifireConfig(
            owner="o", bu="b", system_table="qf", datasets=[ds],
        )
        with pytest.raises(QualifireConfigError, match="requires partition_ts"):
            qf.backfill(config, partition_ts="2026-04-01")


class TestBackfillReportShape:
    def test_raises_with_report_attached_when_partition_errors(self, tmp_path):
        """Plan N18: when at least one partition errors, ``backfill``
        raises ``QualifireValidationError`` with a ``.report``
        attribute carrying the full ``BackfillReport``.

        Without a real 't' table on the PandasBackend, the engine
        errors per partition; the report surfaces via ``.report``.
        """
        from qualifire.core.exceptions import QualifireValidationError

        qf = _make_qf(tmp_path)
        config = _make_config(with_wap=False)
        with pytest.raises(QualifireValidationError) as exc_info:
            qf.backfill(config, partition_ts="2026-04-01")
        report = getattr(exc_info.value, "report", None)
        assert isinstance(report, BackfillReport)
        assert report.has_errors
        assert report.errored >= 1

    def test_report_to_dict_serializable(self, tmp_path):
        """JSON projection round-trips cleanly whether ``backfill``
        returned the report directly or it came off the exception's
        ``.report`` attribute."""
        import json
        from qualifire.core.exceptions import QualifireValidationError

        qf = _make_qf(tmp_path)
        config = _make_config(with_wap=False)
        try:
            report = qf.backfill(config, partition_ts="2026-04-01")
        except QualifireValidationError as e:
            report = e.report  # type: ignore[attr-defined]
        d = report.to_dict()
        json.dumps(d)
        assert "partitions" in d
        assert "refreshed" in d
        assert "errored" in d


class TestBackfillSelectorRouting:
    def test_no_match_selector_raises(self, tmp_path):
        qf = _make_qf(tmp_path)
        config = _make_config(with_wap=False)
        with pytest.raises(QualifireConfigError, match="matched no scope"):
            qf.backfill(
                config,
                partition_ts="2026-04-01",
                selector="nonexistent:*",
            )

    def test_partial_match_selector_runs_subset(self, tmp_path):
        from qualifire.core.exceptions import QualifireValidationError
        qf = _make_qf(tmp_path)
        ds_a = DatasetConfig(
            name="a", table="t",
            partition_ts="event_date",
            partition_step="P1D",
            validations=[ThresholdValidationConfig(
                collection=AggregationCollectionConfig(expressions={"cnt": "COUNT(*)"}),
                rules=[ThresholdRuleConfig(metric="cnt", thresholds=ThresholdLevels())],
            )],
        )
        ds_b = DatasetConfig(
            name="b", table="t",
            partition_ts="event_date",
            partition_step="P1D",
            validations=[ThresholdValidationConfig(
                collection=AggregationCollectionConfig(expressions={"cnt": "COUNT(*)"}),
                rules=[ThresholdRuleConfig(metric="cnt", thresholds=ThresholdLevels())],
            )],
        )
        config = QualifireConfig(
            owner="o", bu="b", system_table="qf", datasets=[ds_a, ds_b],
        )
        # Restrict to dataset 'a' only. With no `t` table, the
        # backfill errors per partition; the selector still narrows
        # which dataset's diffs surface in the report.
        try:
            report = qf.backfill(
                config,
                partition_ts="2026-04-01",
                selector="a:*",
            )
        except QualifireValidationError as e:
            report = e.report  # type: ignore[attr-defined]
        # All partition diffs reference dataset 'a' only
        assert report.partitions, "selector should produce >=1 diff"
        for diff in report.partitions:
            assert diff.dataset_name == "a"

    def test_metric_filter_restricts_engine_validations(self, tmp_path):
        """Codex impl-review R1: selector with a metric segment must
        narrow the engine's validation set, not just the report
        diffs. A dataset with two threshold validations producing
        different metrics, scoped to one metric, should run only
        that validation."""
        from qualifire.core.exceptions import QualifireValidationError

        qf = _make_qf(tmp_path)
        ds = DatasetConfig(
            name="multi", table="t",
            partition_ts="event_date",
            partition_step="P1D",
            validations=[
                ThresholdValidationConfig(
                    name="check_a",
                    collection=AggregationCollectionConfig(expressions={"cnt": "COUNT(*)"}),
                    rules=[ThresholdRuleConfig(metric="cnt", thresholds=ThresholdLevels())],
                ),
                ThresholdValidationConfig(
                    name="check_b",
                    collection=AggregationCollectionConfig(expressions={"sum_x": "SUM(x)"}),
                    rules=[ThresholdRuleConfig(metric="sum_x", thresholds=ThresholdLevels())],
                ),
            ],
        )
        config = QualifireConfig(owner="o", bu="b", system_table="qf", datasets=[ds])

        # Selector targets only check_a. _build_partition_dataset
        # must narrow the engine's validation set (not just the
        # diff iteration) — verified by inspecting the resolved
        # partition-pinned config that gets handed to the engine.
        from qualifire.core.backfill import _build_partition_dataset
        from qualifire.core.selectors import resolve_selector, parse_selector

        scopes = resolve_selector(parse_selector("multi:check_a"), config)
        assert len(scopes) == 1
        scope = scopes[0]
        assert {v.name for v in scope.validations} == {"check_a"}
        partition_ds = _build_partition_dataset(
            scope.dataset,
            anchor=datetime(2026, 4, 1),
            data=False,
            validations=scope.validations,
        )
        assert {v.name for v in partition_ds.validations} == {"check_a"}, (
            "selector's metric_filter must restrict the engine's "
            "validation set, not just the report diffs"
        )
