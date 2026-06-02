"""Additional tests for historical validator: z_score, substitute strategy, abs deviation."""

from datetime import datetime

from qualifire.core.models import CollectionResult, Severity
from qualifire.validation.historical import HistoricalValidator


# Drift requires partition_ts on the collected row + step on the rule.
# Pinned values keep the partition-anchored read deterministic.
_ANCHOR = datetime(2024, 1, 5)
_STEP = "P1D"


def _collected(name: str, value: float) -> list[CollectionResult]:
    return [CollectionResult(
        metric_name=name, metric_value=value,
        collected_at=datetime.now(),
        partition_ts=_ANCHOR,
    )]


def _with_step(rules: list[dict]) -> list[dict]:
    out = []
    for r in rules:
        compare = dict(r.get("compare") or {})
        compare.setdefault("step", _STEP)
        out.append({**r, "compare": compare})
    return out


class MockStorage:
    """Mock that satisfies the partition-anchored read interface used by the
    history-backed validators."""

    def __init__(self, history):
        self._history = history

    def read_metric_history(self, **kwargs):
        return self._history

    def read_metric_history_by_partition(self, **kwargs):
        return self._history


class TestZScoreThreshold:
    def test_pass_z_score(self):
        past = [
            {"metric_value": 100, "run_timestamp": "2024-01-01"},
            {"metric_value": 102, "run_timestamp": "2024-01-02"},
            {"metric_value": 98, "run_timestamp": "2024-01-03"},
            {"metric_value": 101, "run_timestamp": "2024-01-04"},
        ]
        rules = [{
            "metric": "m",
            "compare": {"past_values": 4},
            "thresholds": {"warning": {"z_score": {"min": -2, "max": 2}}, "error": {"z_score": {"min": -3, "max": 3}}},
        }]
        validator = HistoricalValidator(rules=_with_step(rules), storage=MockStorage(past), table_name="t")
        results = validator.validate(_collected("m", 101))
        assert results[0].severity == Severity.PASS

    def test_warning_z_score(self):
        past = [
            {"metric_value": 100, "run_timestamp": "2024-01-01"},
            {"metric_value": 100, "run_timestamp": "2024-01-02"},
            {"metric_value": 100, "run_timestamp": "2024-01-03"},
        ]
        # stddev is 0 for identical values, z_score = 0 always
        # Use slightly varying values to get meaningful stddev
        past = [
            {"metric_value": 100, "run_timestamp": "2024-01-01"},
            {"metric_value": 101, "run_timestamp": "2024-01-02"},
            {"metric_value": 99, "run_timestamp": "2024-01-03"},
            {"metric_value": 100, "run_timestamp": "2024-01-04"},
        ]
        rules = [{
            "metric": "m",
            "compare": {"past_values": 4},
            "thresholds": {"warning": {"z_score": {"min": -1.5, "max": 1.5}}, "error": {"z_score": {"min": -10, "max": 10}}},
        }]
        validator = HistoricalValidator(rules=_with_step(rules), storage=MockStorage(past), table_name="t")
        # mean=100, stddev~=0.816; value=102 -> z~=2.45, violates warning (1.5) but not error (10)
        results = validator.validate(_collected("m", 102))
        assert results[0].severity == Severity.WARNING


class TestDeviationAbsThreshold:
    def test_deviation_abs_warning(self):
        past = [
            {"metric_value": 100, "run_timestamp": "2024-01-01"},
            {"metric_value": 100, "run_timestamp": "2024-01-02"},
        ]
        rules = [{
            "metric": "m",
            "compare": {"past_values": 2},
            "thresholds": {"warning": {"deviation_abs": {"min": -10, "max": 10}}, "error": {"deviation_abs": {"min": -50, "max": 50}}},
        }]
        validator = HistoricalValidator(rules=_with_step(rules), storage=MockStorage(past), table_name="t")
        # deviation_abs = |120 - 100| = 20 > 10 warning
        results = validator.validate(_collected("m", 120))
        assert results[0].severity == Severity.WARNING
        assert results[0].details["deviation_abs"] == 20.0


class TestMissingStrategySubstitute:
    def test_substitute_fills_with_mean(self):
        """substitute strategy fills missing values with mean of available."""
        past = [
            {"metric_value": 100, "run_timestamp": "2024-01-01"},
        ]
        rules = [{
            "metric": "m",
            "compare": {"past_values": 3, "missing_strategy": "substitute"},
            "thresholds": {"warning": {"deviation_pct": {"min": -10, "max": 10}}},
        }]
        validator = HistoricalValidator(rules=_with_step(rules), storage=MockStorage(past), table_name="t")
        # 1 value available, 2 substituted with mean=100
        # current=100 -> deviation=0% -> PASS
        results = validator.validate(_collected("m", 100))
        assert results[0].severity == Severity.PASS

    def test_substitute_still_detects_deviation(self):
        past = [{"metric_value": 100, "run_timestamp": "2024-01-01"}]
        rules = [{
            "metric": "m",
            "compare": {"past_values": 3, "missing_strategy": "substitute"},
            "thresholds": {"warning": {"deviation_pct": {"min": -10, "max": 10}}, "error": {"deviation_pct": {"min": -30, "max": 30}}},
        }]
        validator = HistoricalValidator(rules=_with_step(rules), storage=MockStorage(past), table_name="t")
        results = validator.validate(_collected("m", 150))
        assert results[0].severity == Severity.ERROR  # 50% deviation > 30%


class TestMissingMetric:
    def test_metric_not_found(self):
        rules = [{"metric": "missing", "thresholds": {"error": {"deviation_pct": {"min": -10, "max": 10}}}}]
        validator = HistoricalValidator(rules=_with_step(rules), storage=MockStorage([]), table_name="t")
        results = validator.validate(_collected("other", 100))
        assert results[0].severity == Severity.ERROR
        assert "not found" in results[0].message
