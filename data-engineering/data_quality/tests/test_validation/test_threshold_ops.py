"""Tests for all threshold operators (neq, gt, lt, gte, lte)."""

from datetime import datetime

from qualifire.core.models import CollectionResult, Severity
from qualifire.validation.threshold import ThresholdValidator


def _collected(value: float) -> list[CollectionResult]:
    return [CollectionResult(metric_name="m", metric_value=value, collected_at=datetime.now())]


class TestNeqOperator:
    def test_neq_violated(self):
        rules = [{"metric": "m", "thresholds": {"error": {"neq": 42}}}]
        r = ThresholdValidator(rules=rules).validate(_collected(42))
        assert r[0].severity == Severity.ERROR

    def test_neq_pass(self):
        rules = [{"metric": "m", "thresholds": {"error": {"neq": 42}}}]
        r = ThresholdValidator(rules=rules).validate(_collected(99))
        assert r[0].severity == Severity.PASS


class TestGtOperator:
    def test_gt_violated(self):
        rules = [{"metric": "m", "thresholds": {"error": {"gt": 100}}}]
        r = ThresholdValidator(rules=rules).validate(_collected(50))
        assert r[0].severity == Severity.ERROR

    def test_gt_boundary(self):
        rules = [{"metric": "m", "thresholds": {"error": {"gt": 100}}}]
        r = ThresholdValidator(rules=rules).validate(_collected(100))
        assert r[0].severity == Severity.ERROR  # not strictly greater

    def test_gt_pass(self):
        rules = [{"metric": "m", "thresholds": {"error": {"gt": 100}}}]
        r = ThresholdValidator(rules=rules).validate(_collected(101))
        assert r[0].severity == Severity.PASS


class TestLtOperator:
    def test_lt_violated(self):
        rules = [{"metric": "m", "thresholds": {"error": {"lt": 10}}}]
        r = ThresholdValidator(rules=rules).validate(_collected(50))
        assert r[0].severity == Severity.ERROR

    def test_lt_boundary(self):
        rules = [{"metric": "m", "thresholds": {"error": {"lt": 10}}}]
        r = ThresholdValidator(rules=rules).validate(_collected(10))
        assert r[0].severity == Severity.ERROR  # not strictly less

    def test_lt_pass(self):
        rules = [{"metric": "m", "thresholds": {"error": {"lt": 10}}}]
        r = ThresholdValidator(rules=rules).validate(_collected(5))
        assert r[0].severity == Severity.PASS


class TestGteOperator:
    def test_gte_violated(self):
        rules = [{"metric": "m", "thresholds": {"error": {"gte": 100}}}]
        r = ThresholdValidator(rules=rules).validate(_collected(50))
        assert r[0].severity == Severity.ERROR

    def test_gte_boundary_pass(self):
        rules = [{"metric": "m", "thresholds": {"error": {"gte": 100}}}]
        r = ThresholdValidator(rules=rules).validate(_collected(100))
        assert r[0].severity == Severity.PASS


class TestLteOperator:
    def test_lte_violated(self):
        rules = [{"metric": "m", "thresholds": {"error": {"lte": 10}}}]
        r = ThresholdValidator(rules=rules).validate(_collected(50))
        assert r[0].severity == Severity.ERROR

    def test_lte_boundary_pass(self):
        rules = [{"metric": "m", "thresholds": {"error": {"lte": 10}}}]
        r = ThresholdValidator(rules=rules).validate(_collected(10))
        assert r[0].severity == Severity.PASS
