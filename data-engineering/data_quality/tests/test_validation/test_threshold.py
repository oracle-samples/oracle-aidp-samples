"""Tests for threshold validator."""

from datetime import datetime

from qualifire.core.models import CollectionResult, Severity
from qualifire.validation.threshold import ThresholdValidator


def _make_collected(name: str, value: float) -> list[CollectionResult]:
    return [
        CollectionResult(
            metric_name=name, metric_value=value, collected_at=datetime.now()
        )
    ]


class TestThresholdValidator:
    def test_pass(self):
        rules = [{"metric": "count", "thresholds": {"warning": {"min": 10}, "error": {"min": 5}}}]
        validator = ThresholdValidator(rules=rules)
        results = validator.validate(_make_collected("count", 100))
        assert results[0].severity == Severity.PASS

    def test_warning_min(self):
        rules = [{"metric": "count", "thresholds": {"warning": {"min": 100}, "error": {"min": 10}}}]
        validator = ThresholdValidator(rules=rules)
        results = validator.validate(_make_collected("count", 50))
        assert results[0].severity == Severity.WARNING

    def test_error_min(self):
        rules = [{"metric": "count", "thresholds": {"warning": {"min": 100}, "error": {"min": 10}}}]
        validator = ThresholdValidator(rules=rules)
        results = validator.validate(_make_collected("count", 5))
        assert results[0].severity == Severity.ERROR

    def test_max_threshold(self):
        rules = [{"metric": "rate", "thresholds": {"error": {"max": 0.05}}}]
        validator = ThresholdValidator(rules=rules)
        results = validator.validate(_make_collected("rate", 0.10))
        assert results[0].severity == Severity.ERROR

    def test_eq_threshold(self):
        rules = [{"metric": "status", "thresholds": {"error": {"eq": 1}}}]
        validator = ThresholdValidator(rules=rules)
        results = validator.validate(_make_collected("status", 0))
        assert results[0].severity == Severity.ERROR

    def test_missing_metric(self):
        rules = [{"metric": "missing", "thresholds": {"error": {"min": 10}}}]
        validator = ThresholdValidator(rules=rules)
        results = validator.validate(_make_collected("other", 100))
        assert results[0].severity == Severity.ERROR
        assert "not found" in results[0].message

    def test_multiple_rules(self):
        rules = [
            {"metric": "a", "thresholds": {"error": {"min": 10}}},
            {"metric": "b", "thresholds": {"warning": {"max": 100}}},
        ]
        collected = [
            CollectionResult(metric_name="a", metric_value=20, collected_at=datetime.now()),
            CollectionResult(metric_name="b", metric_value=50, collected_at=datetime.now()),
        ]
        validator = ThresholdValidator(rules=rules)
        results = validator.validate(collected)
        assert len(results) == 2
        assert results[0].severity == Severity.PASS
        assert results[1].severity == Severity.PASS


# ===========================================================================
# expected_value compactness — engine-side exclude_none plumbing
# ===========================================================================


def test_expected_value_compact_via_engine():
    """End-to-end through the engine: ``ThresholdValidationConfig
    .expected_value_compact=True`` (the default) means the
    ``ValidationResult.expected_value`` carries only the bounds
    the operator actually set — no ``None`` for unset operator
    fields and no ``warning: None`` for an unused level.

    Defends against the bloated dict that ended up in the system
    table + alert bodies when ``model_dump()`` was called without
    ``exclude_none=True``."""
    from qualifire import Qualifire
    from qualifire.core.exceptions import QualifireValidationError
    from qualifire.backends.pandas_backend import PandasBackend
    import tempfile
    import os
    import pandas as pd

    backend = PandasBackend(tables={"sales": pd.DataFrame({"amount": [50.0, 60.0, 70.0]})})
    with tempfile.NamedTemporaryFile(suffix=".sqlite", delete=False) as f:
        db_path = f.name
    try:
        qf = Qualifire(
            backend=backend,
            system_table=db_path,
            system_table_backend="sqlite",
        )
        try:
            qf.validate(
                table="sales",
                partition_ts="'2026-05-08'",
                partition_step="P1D",
                validations=[
                    Qualifire.threshold_check(
                        name="t",
                        aggregations={"row_count": "COUNT(*)"},
                        rules=[{"metric": "row_count", "thresholds": {"error": {"min": 100}}}],
                    )
                ],
            )
        except QualifireValidationError as e:
            result = e.result
        vrs = [v for ds in result.datasets for v in ds.validation_results]
        assert vrs, "expected at least one ValidationResult"
        ev = vrs[0].expected_value
        # Compact form: only "error" present, only "min" inside.
        assert ev == {"error": {"min": 100}}, ev
    finally:
        os.unlink(db_path)
