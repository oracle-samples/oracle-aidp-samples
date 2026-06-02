"""S1 tests — `partition_step` config field + H3 invariant.

Plan: docs/features/metrics-backfill-and-soft-delete/plan.md S1.
"""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from qualifire.core.config import (
    DatasetConfig,
    QualifireConfig,
    effective_partition_step,
    effective_partition_ts,
)


def _make_config(*, run_pts=None, run_pstep=None, datasets=None):
    return QualifireConfig(
        owner="t",
        bu="t",
        system_table="qf.system",
        partition_ts=run_pts,
        partition_step=run_pstep,
        datasets=datasets or [],
    )


def test_effective_partition_step_dataset_only():
    cfg = _make_config(
        datasets=[
            DatasetConfig(
                name="ds", table="t",
                partition_ts="event_dt", partition_step="P1D",
            )
        ]
    )
    assert effective_partition_step(cfg.datasets[0], cfg) == "P1D"


def test_effective_partition_step_run_level_only():
    cfg = _make_config(
        run_pts="event_dt", run_pstep="P1D",
        datasets=[DatasetConfig(name="ds", table="t")],
    )
    assert effective_partition_step(cfg.datasets[0], cfg) == "P1D"


def test_effective_partition_step_dataset_wins_over_run():
    cfg = _make_config(
        run_pts="event_dt", run_pstep="P1D",
        datasets=[
            DatasetConfig(
                name="ds", table="t",
                partition_step="PT1H", partition_ts="event_ts",
            )
        ],
    )
    assert effective_partition_step(cfg.datasets[0], cfg) == "PT1H"


def test_effective_partition_step_neither_set_resolves_none():
    cfg = _make_config(datasets=[DatasetConfig(name="ds", table="t")])
    assert effective_partition_step(cfg.datasets[0], cfg) is None
    assert effective_partition_ts(cfg.datasets[0], cfg) is None


def test_partition_ts_without_partition_step_is_allowed():
    """``partition_ts`` alone is valid for any normal validation
    run — it stamps persisted rows and anchors history reads.
    ``partition_step`` is only load-bearing for backfill /
    deactivate, which gate themselves at invocation time."""
    cfg = _make_config(
        datasets=[DatasetConfig(name="sales", table="t", partition_ts="event_dt")]
    )
    assert cfg.datasets[0].partition_ts == "event_dt"
    assert cfg.datasets[0].partition_step is None


def test_partition_step_without_partition_ts_rejected():
    with pytest.raises(ValidationError) as exc:
        _make_config(
            datasets=[DatasetConfig(name="ds", table="t", partition_step="P1D")]
        )
    msg = str(exc.value)
    assert "ds" in msg and "partition_step" in msg and "partition_ts" in msg


def test_run_level_partition_ts_without_step_is_allowed():
    """Same as the dataset-level case: run-level ``partition_ts``
    without ``partition_step`` is fine for normal validation runs."""
    cfg = _make_config(
        run_pts="event_dt",
        datasets=[DatasetConfig(name="d", table="t")],
    )
    assert cfg.partition_ts == "event_dt"
    assert cfg.partition_step is None


def test_run_level_partition_step_with_no_partition_ts_anywhere_rejected():
    with pytest.raises(ValidationError) as exc:
        _make_config(
            run_pstep="P1D",
            datasets=[DatasetConfig(name="d", table="t")],
        )
    assert "partition_step" in str(exc.value) and "partition_ts" in str(exc.value)


def test_invalid_iso_duration_rejected_at_field_level():
    with pytest.raises(ValidationError):
        DatasetConfig(name="d", table="t", partition_step="P1M")
    with pytest.raises(ValidationError):
        DatasetConfig(name="d", table="t", partition_step="foo")


def test_invalid_run_level_partition_step_rejected():
    with pytest.raises(ValidationError):
        _make_config(run_pstep="P1Y")


def test_run_level_step_and_ts_with_dataset_using_them():
    cfg = _make_config(
        run_pts="event_dt", run_pstep="P1D",
        datasets=[DatasetConfig(name="d", table="t")],
    )
    assert effective_partition_ts(cfg.datasets[0], cfg) == "event_dt"
    assert effective_partition_step(cfg.datasets[0], cfg) == "P1D"
