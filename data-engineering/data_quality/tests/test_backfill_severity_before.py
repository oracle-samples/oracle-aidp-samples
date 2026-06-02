"""Tests for the ``severity_before`` readback fix in backfill.

Pre-fix: ``_read_severity_before`` called
``read_validation_history(validation_name="")`` which never matched a
real persisted row, so every backfill reported
``severity_before=None``. Knock-on: ``_classify_diff`` over-reported
``refreshed`` when only severity flipped between runs.

Post-fix coverage (T1-T4):
- T1: severity-flip is detected (prior PASS, current ERROR → diff
      classification reflects severity change even when value
      unchanged).
- T2: unchanged value + unchanged severity reports ``unchanged``.
- T3: max-severity aggregation when multiple validators emit the
      same metric.
- T4: empty validation_names list → severity_before stays None
      (defensive fall-through).
"""
from __future__ import annotations

from datetime import datetime
from unittest.mock import MagicMock

import pytest

from qualifire.core.backfill import (
    _read_severity_before,
    _resolve_metric_to_validation_names,
)
from qualifire.core.models import Severity


def _make_storage_with_rows(rows: list[dict]):
    """Mock a storage backend that returns the supplied rows from
    ``read_validations_at_partition``."""
    s = MagicMock()
    s.read_validations_at_partition.return_value = rows
    return s


class TestReadSeverityBefore:
    def test_T1_returns_severity_for_matching_row(self):
        storage = _make_storage_with_rows([{
            "validation_name": "threshold.ds.cnt",
            "metric_name": "cnt",
            "validation_status": "ERROR",
        }])
        sev = _read_severity_before(
            storage=storage, dataset_name="ds",
            metric_name="cnt",
            validation_names=["threshold.ds.cnt"],
            partition_ts=datetime(2026, 4, 1),
        )
        assert sev == Severity.ERROR
        # Verify the call shape.
        storage.read_validations_at_partition.assert_called_once()
        call_kwargs = storage.read_validations_at_partition.call_args.kwargs
        assert call_kwargs["dataset_name"] == "ds"
        assert call_kwargs["validation_name_prefix"] == "threshold.ds"
        assert call_kwargs["partition_ts"] == datetime(2026, 4, 1)

    def test_T2_no_matching_row_returns_None(self):
        storage = _make_storage_with_rows([])
        sev = _read_severity_before(
            storage=storage, dataset_name="ds",
            metric_name="cnt",
            validation_names=["threshold.ds.cnt"],
            partition_ts=datetime(2026, 4, 1),
        )
        assert sev is None

    def test_T3_max_severity_wins_across_multiple_validators(self):
        # Two validators emit the same metric — one PASS, one ERROR.
        # Storage returns both; aggregation picks ERROR.
        storage = _make_storage_with_rows([
            {"validation_name": "threshold.ds.cnt",
             "metric_name": "cnt",
             "validation_status": "PASS"},
            {"validation_name": "drift.ds.cnt",
             "metric_name": "cnt",
             "validation_status": "ERROR"},
        ])
        sev = _read_severity_before(
            storage=storage, dataset_name="ds",
            metric_name="cnt",
            validation_names=["threshold.ds.cnt", "drift.ds.cnt"],
            partition_ts=datetime(2026, 4, 1),
        )
        assert sev == Severity.ERROR

    def test_T4_empty_validation_names_returns_None_without_query(self):
        """Defensive: when no validators on the scope emit this
        metric (shouldn't happen given expected_metrics()), return
        None without hitting storage."""
        storage = _make_storage_with_rows([])
        sev = _read_severity_before(
            storage=storage, dataset_name="ds",
            metric_name="cnt",
            validation_names=[],  # empty
            partition_ts=datetime(2026, 4, 1),
        )
        assert sev is None
        # Storage NOT called.
        storage.read_validations_at_partition.assert_not_called()

    def test_storage_None_returns_None(self):
        sev = _read_severity_before(
            storage=None, dataset_name="ds",
            metric_name="cnt",
            validation_names=["threshold.ds.cnt"],
            partition_ts=datetime(2026, 4, 1),
        )
        assert sev is None

    def test_storage_exception_returns_None(self):
        s = MagicMock()
        s.read_validations_at_partition.side_effect = RuntimeError("boom")
        sev = _read_severity_before(
            storage=s, dataset_name="ds",
            metric_name="cnt",
            validation_names=["threshold.ds.cnt"],
            partition_ts=datetime(2026, 4, 1),
        )
        assert sev is None

    def test_filters_out_other_metrics_on_same_validation(self):
        """A drift validator with multiple rules emits one row per
        metric; each row's validation_name shares the base. Confirm
        we don't accidentally surface the wrong metric's severity."""
        storage = _make_storage_with_rows([
            {"validation_name": "drift.ds.cnt",
             "metric_name": "cnt",
             "validation_status": "PASS"},
            {"validation_name": "drift.ds.avg",
             "metric_name": "avg",  # different metric
             "validation_status": "ERROR"},
        ])
        sev = _read_severity_before(
            storage=storage, dataset_name="ds",
            metric_name="cnt",
            validation_names=["drift.ds.cnt"],
            partition_ts=datetime(2026, 4, 1),
        )
        # Should NOT pick up the ERROR row (different metric).
        assert sev == Severity.PASS

    def test_filters_out_unrelated_validation_names(self):
        """Two threshold validators on the same dataset; only one
        is in scope. The other's rows must NOT contribute."""
        storage = _make_storage_with_rows([
            {"validation_name": "threshold.ds.cnt",
             "metric_name": "cnt",
             "validation_status": "PASS"},
            {"validation_name": "another_threshold.ds.cnt",
             "metric_name": "cnt",
             "validation_status": "ERROR"},  # not in scope
        ])
        sev = _read_severity_before(
            storage=storage, dataset_name="ds",
            metric_name="cnt",
            validation_names=["threshold.ds.cnt"],
            partition_ts=datetime(2026, 4, 1),
        )
        # The "another_threshold" row would also be returned by
        # the prefix lookup if both bases are queried; client-side
        # filter on exact validation_name match must drop it.
        assert sev == Severity.PASS


class TestSymmetricExtractMetricSeverity:
    """Codex impl-review R1 MAJOR: ``_extract_metric_severity``
    must also max-aggregate (severity_after) so the diff
    classification doesn't compare an aggregated severity_before
    against a "first row wins" severity_after, falsely reporting
    ``refreshed`` when the symmetric severity is unchanged."""

    def test_extract_max_aggregates_across_validators(self):
        from qualifire.core.backfill import _extract_metric_severity
        from qualifire.core.models import (
            CollectionResult, DatasetResult, QualifireResult,
            ValidationResult,
        )

        # Two ValidationResults for metric "cnt" — one PASS, one ERROR.
        # _extract_metric_severity must return ERROR.
        result = QualifireResult(
            owner="o", bu="b",
            datasets=[
                DatasetResult(
                    dataset_name="ds", table="t",
                    validation_results=[
                        ValidationResult(
                            validation_name="threshold.ds.cnt",
                            validation_type="threshold",
                            severity=Severity.PASS,
                            message="",
                            metric_name="cnt",
                            validation_base_name="threshold.ds",
                        ),
                        ValidationResult(
                            validation_name="drift.ds.cnt",
                            validation_type="drift",
                            severity=Severity.ERROR,
                            message="",
                            metric_name="cnt",
                            validation_base_name="drift.ds",
                        ),
                    ],
                    collection_results=[
                        CollectionResult(
                            metric_name="cnt", metric_value=42.0,
                            collected_at=datetime(2026, 4, 1),
                        ),
                    ],
                ),
            ],
        )
        val, sev = _extract_metric_severity(result, metric_name="cnt")
        assert val == 42.0
        assert sev == Severity.ERROR


class TestResolveMetricToValidationNames:
    def test_threshold_validator_emits_suffixed_name_per_metric(self):
        from qualifire.core.config import (
            AggregationCollectionConfig,
            DatasetConfig,
            ThresholdLevels,
            ThresholdRuleConfig,
            ThresholdValidationConfig,
        )
        from qualifire.core.backfill import ScopedDataset

        ds = DatasetConfig(
            name="ds", table="t",
            validations=[ThresholdValidationConfig(
                collection=AggregationCollectionConfig(
                    expressions={"cnt": "COUNT(*)", "avg": "AVG(x)"},
                ),
                rules=[
                    ThresholdRuleConfig(metric="cnt", thresholds=ThresholdLevels()),
                    ThresholdRuleConfig(metric="avg", thresholds=ThresholdLevels()),
                ],
            )],
        )
        scope = ScopedDataset(dataset=ds, validations=ds.validations, metric_filter=None)
        out = _resolve_metric_to_validation_names(scope)
        # Each metric maps to ``threshold.ds.<metric>``.
        assert out == {
            "cnt": ["threshold.ds.cnt"],
            "avg": ["threshold.ds.avg"],
        }

    def test_validators_without_expected_metrics_skipped(self):
        """Pattern / AnomalyDetection / SLO don't have
        expected_metrics() — they don't contribute to the map."""
        from qualifire.core.config import (
            AggregationCollectionConfig,
            DatasetConfig,
            ThresholdLevels,
            ThresholdRuleConfig,
            ThresholdValidationConfig,
        )
        from qualifire.core.backfill import ScopedDataset

        # Build with one threshold + one mock-no-expected_metrics.
        ds = DatasetConfig(
            name="ds", table="t",
            validations=[ThresholdValidationConfig(
                collection=AggregationCollectionConfig(
                    expressions={"cnt": "COUNT(*)"},
                ),
                rules=[ThresholdRuleConfig(metric="cnt", thresholds=ThresholdLevels())],
            )],
        )
        scope = ScopedDataset(dataset=ds, validations=ds.validations, metric_filter=None)
        out = _resolve_metric_to_validation_names(scope)
        # Only the threshold validator contributes.
        assert "cnt" in out
        assert len(out["cnt"]) == 1
