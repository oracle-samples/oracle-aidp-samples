"""Tests for SLO validator."""

from datetime import datetime, timedelta

import pytest

from qualifire.core.models import CollectionResult, Severity
from qualifire.validation.slo import SLOValidator


class TestSLOValidator:
    def _make_recency(self, ts: datetime) -> list[CollectionResult]:
        return [
            CollectionResult(
                metric_name="recency",
                metric_value=ts,
                collected_at=datetime.now(),
            )
        ]

    def test_pass(self):
        ts = datetime.now() - timedelta(hours=1)
        validator = SLOValidator(warning_duration="PT4H", error_duration="PT8H")
        results = validator.validate(self._make_recency(ts))
        assert len(results) == 1
        assert results[0].severity == Severity.PASS

    def test_warning(self):
        ts = datetime.now() - timedelta(hours=5)
        validator = SLOValidator(warning_duration="PT4H", error_duration="PT8H")
        results = validator.validate(self._make_recency(ts))
        assert results[0].severity == Severity.WARNING

    def test_error(self):
        ts = datetime.now() - timedelta(hours=10)
        validator = SLOValidator(warning_duration="PT4H", error_duration="PT8H")
        results = validator.validate(self._make_recency(ts))
        assert results[0].severity == Severity.ERROR

    def test_warning_only(self):
        ts = datetime.now() - timedelta(hours=5)
        validator = SLOValidator(warning_duration="PT4H")
        results = validator.validate(self._make_recency(ts))
        assert results[0].severity == Severity.WARNING

    def test_no_recency_metric(self):
        collected = [
            CollectionResult(
                metric_name="other", metric_value=42, collected_at=datetime.now()
            )
        ]
        validator = SLOValidator(warning_duration="PT4H")
        results = validator.validate(collected)
        assert len(results) == 0

    def test_string_timestamp(self):
        ts_str = (datetime.now() - timedelta(hours=1)).isoformat()
        collected = [
            CollectionResult(
                metric_name="recency", metric_value=ts_str, collected_at=datetime.now()
            )
        ]
        validator = SLOValidator(warning_duration="PT4H", error_duration="PT8H")
        results = validator.validate(collected)
        assert results[0].severity == Severity.PASS
