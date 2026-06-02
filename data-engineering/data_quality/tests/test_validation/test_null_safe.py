"""Regression: validators must handle metric_value=None without crashing.

NULL aggregations are common (divide-by-zero, all-NULL SUM, AVG over an
empty filter). Pre-fix, `float(cr.metric_value)` raised TypeError, the
exception bubbled to the engine, and the user saw a synthetic
``threshold_error`` ValidationResult with a low-level message instead of
a structured WARNING/ERROR honouring on_empty_data.
"""

from __future__ import annotations

from datetime import datetime

from qualifire.core.models import CollectionResult, Severity
from qualifire.validation.forecast import ForecastValidator
from qualifire.validation.historical import HistoricalValidator
from qualifire.validation.threshold import ThresholdValidator


def _null_cr(metric: str = "m", dim: str | None = None) -> CollectionResult:
    return CollectionResult(
        metric_name=metric,
        metric_value=None,
        collected_at=datetime(2026, 5, 6),
        dimension_value=dim,
    )


class TestThresholdNullSafe:
    def test_emits_warning_not_crash(self):
        validator = ThresholdValidator(
            rules=[{
                "metric": "m",
                "thresholds": {"error": {"min": 1}},
            }],
            name="check",
        )
        out = validator.validate([_null_cr()])
        assert len(out) == 1
        assert out[0].severity == Severity.WARNING  # default on_empty_data
        assert "NULL" in out[0].message

    def test_dim_label_in_message(self):
        validator = ThresholdValidator(
            rules=[{"metric": "m", "thresholds": {"error": {"min": 1}}}],
            name="check",
        )
        out = validator.validate([_null_cr(dim="us")])
        assert "@us" in out[0].message


class TestHistoricalNullSafe:
    def test_emits_warning_not_crash(self):
        validator = HistoricalValidator(
            rules=[{
                "metric": "m",
                "compare": {"past_values": 3},
                "thresholds": {"warning": {"deviation_pct": {"min": -10, "max": 10}}},
            }],
            storage=None,
            table_name="t",
            name="drift",
        )
        out = validator.validate([_null_cr()])
        assert len(out) == 1
        assert out[0].severity == Severity.WARNING


class TestForecastNullSafe:
    def test_emits_warning_not_crash(self):
        # Forecast validator is import-guarded on prophet/pandas; reach
        # the NULL path before any prophet code runs.
        try:
            validator = ForecastValidator(
                rules=[{
                    "metric": "m",
                    "model": {"history_count": 30},
                }],
                storage=None,
                table_name="t",
                name="trend",
            )
        except Exception:
            # Prophet not installed in this env — skip.
            import pytest
            pytest.skip("prophet not available")
            return
        out = validator.validate([_null_cr()])
        assert len(out) == 1
        assert out[0].severity == Severity.WARNING
