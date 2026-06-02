"""Tests for historical comparison validator."""

from datetime import datetime

import pytest

from qualifire.core.models import CollectionResult, Severity
from qualifire.validation.historical import HistoricalValidator


# Pinned anchor + step so partition-anchored history reads are deterministic.
# Drift requires both — the run_timestamp fallback was removed.
_ANCHOR = datetime(2024, 1, 4)
_STEP = "P1D"


def _make_collected(name: str, value: float) -> list[CollectionResult]:
    return [
        CollectionResult(
            metric_name=name, metric_value=value,
            collected_at=datetime.now(),
            partition_ts=_ANCHOR,
        )
    ]


def _with_step(rule: dict) -> dict:
    """Inject the canonical step into a rule's `compare` block, preserving
    other compare fields the test set."""
    compare = dict(rule.get("compare") or {})
    compare.setdefault("step", _STEP)
    return {**rule, "compare": compare}


class MockStorage:
    """Mock that satisfies the partition-anchored read interface.

    Both methods point at the same canned history — the validator now goes
    through `read_metric_history_by_partition`, but the legacy
    `read_metric_history` is preserved so tests that exercise the
    storage-side step-passthrough wrapper keep working.
    """

    def __init__(self, history: list[dict]):
        self._history = history
        self.last_call: dict | None = None

    def read_metric_history(self, **kwargs):
        self.last_call = {"method": "read_metric_history", **kwargs}
        return self._history

    def read_metric_history_by_partition(self, **kwargs):
        self.last_call = {"method": "read_metric_history_by_partition", **kwargs}
        return self._history


class TestHistoricalValidator:
    def test_pass_within_deviation(self):
        past = [
            {"metric_value": 100, "run_timestamp": "2024-01-01"},
            {"metric_value": 102, "run_timestamp": "2024-01-02"},
            {"metric_value": 98, "run_timestamp": "2024-01-03"},
        ]
        rules = [
            {
                "metric": "avg",
                "compare": {"past_values": 3, "missing_strategy": "ignore"},
                "thresholds": {"warning": {"deviation_pct": {"min": -20, "max": 20}}, "error": {"deviation_pct": {"min": -50, "max": 50}}},
            }
        ]
        validator = HistoricalValidator(
            rules=[_with_step(r) for r in rules], storage=MockStorage(past), table_name="t"
        )
        results = validator.validate(_make_collected("avg", 105))
        assert results[0].severity == Severity.PASS

    def test_warning_deviation(self):
        past = [
            {"metric_value": 100, "run_timestamp": "2024-01-01"},
            {"metric_value": 100, "run_timestamp": "2024-01-02"},
            {"metric_value": 100, "run_timestamp": "2024-01-03"},
        ]
        rules = [
            {
                "metric": "avg",
                "thresholds": {"warning": {"deviation_pct": {"min": -10, "max": 10}}, "error": {"deviation_pct": {"min": -50, "max": 50}}},
            }
        ]
        validator = HistoricalValidator(
            rules=[_with_step(r) for r in rules], storage=MockStorage(past), table_name="t"
        )
        results = validator.validate(_make_collected("avg", 125))
        assert results[0].severity == Severity.WARNING

    def test_error_deviation(self):
        past = [{"metric_value": 100, "run_timestamp": "2024-01-01"}]
        rules = [
            {
                "metric": "avg",
                "thresholds": {"warning": {"deviation_pct": {"min": -10, "max": 10}}, "error": {"deviation_pct": {"min": -30, "max": 30}}},
            }
        ]
        validator = HistoricalValidator(
            rules=[_with_step(r) for r in rules], storage=MockStorage(past), table_name="t"
        )
        results = validator.validate(_make_collected("avg", 200))
        assert results[0].severity == Severity.ERROR

    def test_no_history_passes_by_default(self):
        """Default on_missing_history='ignore' returns PASS when no history."""
        rules = [
            {
                "metric": "avg",
                "thresholds": {"warning": {"deviation_pct": {"min": -10, "max": 10}}},
            }
        ]
        validator = HistoricalValidator(
            rules=[_with_step(r) for r in rules], storage=MockStorage([]), table_name="t"
        )
        results = validator.validate(_make_collected("avg", 100))
        assert results[0].severity == Severity.PASS
        assert "No historical data" in results[0].message
        assert results[0].details.get("cold_start") is True

    def test_no_history_on_missing_history_warn(self):
        """on_missing_history='warn' returns WARNING when no history."""
        rules = [
            {
                "metric": "avg",
                "compare": {"on_missing_history": "warn"},
                "thresholds": {"warning": {"deviation_pct": {"min": -10, "max": 10}}},
            }
        ]
        validator = HistoricalValidator(
            rules=[_with_step(r) for r in rules], storage=MockStorage([]), table_name="t",
        )
        results = validator.validate(_make_collected("avg", 100))
        assert results[0].severity == Severity.WARNING
        assert results[0].details.get("cold_start") is True

    def test_no_history_on_missing_history_error(self):
        """on_missing_history='error' returns ERROR when no history."""
        rules = [
            {
                "metric": "avg",
                "compare": {"on_missing_history": "error"},
                "thresholds": {"warning": {"deviation_pct": {"min": -10, "max": 10}}},
            }
        ]
        validator = HistoricalValidator(
            rules=[_with_step(r) for r in rules], storage=MockStorage([]), table_name="t",
        )
        results = validator.validate(_make_collected("avg", 100))
        assert results[0].severity == Severity.ERROR
        assert results[0].details.get("cold_start") is True

    def test_ignore_mode_uses_partial_history(self):
        """on_missing_history='ignore' uses available data even if fewer than past_values."""
        past = [{"metric_value": 100, "run_timestamp": "2024-01-01"}]
        rules = [
            {
                "metric": "avg",
                "compare": {"past_values": 5, "on_missing_history": "ignore"},
                "thresholds": {"warning": {"deviation_pct": {"min": -10, "max": 10}}, "error": {"deviation_pct": {"min": -50, "max": 50}}},
            }
        ]
        validator = HistoricalValidator(
            rules=[_with_step(r) for r in rules], storage=MockStorage(past), table_name="t"
        )
        results = validator.validate(_make_collected("avg", 105))
        # Should proceed with 1 value (not skip), 5% deviation < 10% warning
        assert results[0].severity == Severity.PASS
        assert "cold_start" not in results[0].details  # not cold start — data was used

    def test_no_history_cold_start_includes_actual_value(self):
        """Cold-start result includes actual_value for downstream tracking."""
        rules = [
            {
                "metric": "avg",
                "thresholds": {"warning": {"deviation_pct": {"min": -10, "max": 10}}},
            }
        ]
        validator = HistoricalValidator(
            rules=[_with_step(r) for r in rules], storage=MockStorage([]), table_name="t"
        )
        results = validator.validate(_make_collected("avg", 42.5))
        assert results[0].actual_value == 42.5

    def test_missing_strategy_error(self):
        past = [{"metric_value": 100, "run_timestamp": "2024-01-01"}]
        rules = [
            {
                "metric": "avg",
                "compare": {"past_values": 3, "missing_strategy": "error"},
                "thresholds": {"error": {"deviation_pct": {"min": -50, "max": 50}}},
            }
        ]
        validator = HistoricalValidator(
            rules=[_with_step(r) for r in rules], storage=MockStorage(past), table_name="t"
        )
        results = validator.validate(_make_collected("avg", 100))
        assert results[0].severity == Severity.ERROR
        assert "Expected 3" in results[0].message


class _StorageThatCapturesStep:
    """Test double that records ``step`` so the validator-level wiring
    can be asserted without pulling in a real storage backend.

    Captures from ``read_metric_history_by_partition`` — the
    canonical history-read entrypoint after the run_timestamp fallback
    was removed. ``read_metric_history`` is also implemented so a stray
    legacy caller still works in unit tests.
    """

    def __init__(self, history: list[dict]):
        self._history = history
        self.last_step = "sentinel-unset"
        self.last_anchor = "sentinel-unset"

    def read_metric_history(self, **kwargs):
        self.last_step = kwargs.get("step", "sentinel-missing")
        return self._history

    def read_metric_history_by_partition(self, **kwargs):
        self.last_step = kwargs.get("step", "sentinel-missing")
        self.last_anchor = kwargs.get("anchor_ts", "sentinel-missing")
        return self._history


class TestHistoricalValidatorStepPassthrough:
    """``rule.compare.step`` must flow from the rule dict through the
    validator into ``read_metric_history_by_partition``. Without that
    wiring, partition-anchored reads would silently default and every
    drift validator would land at the same lookback granularity
    regardless of what the YAML says.

    The run_timestamp-based fallback was removed: a missing step now
    produces a structured ERROR rather than silently bucketing the
    raw history.
    """

    def test_explicit_step_is_forwarded_to_storage(self):
        past = [{"metric_value": 100.0, "run_timestamp": "2024-01-01"}]
        storage = _StorageThatCapturesStep(past)
        rules = [
            {
                "metric": "avg",
                "compare": {"past_values": 3, "step": "P7D"},
                "thresholds": {"warning": {"deviation_pct": {"min": -10, "max": 10}}},
            }
        ]
        validator = HistoricalValidator(
            rules=rules, storage=storage, table_name="t"
        )
        validator.validate(_make_collected("avg", 100))
        assert storage.last_step == "P7D"
        assert storage.last_anchor == _ANCHOR

    def test_omitted_step_returns_error(self):
        """No step in the rule + no Pydantic default for raw-dict rules
        means the validator can't form a partition-anchored read.
        Surface ERROR with a reason, not a silent run_timestamp
        fallback."""
        past = [{"metric_value": 100.0, "run_timestamp": "2024-01-01"}]
        storage = _StorageThatCapturesStep(past)
        rules = [
            {
                "metric": "avg",
                "compare": {"past_values": 3},      # no `step`
                "thresholds": {"warning": {"deviation_pct": {"min": -10, "max": 10}}},
            }
        ]
        validator = HistoricalValidator(
            rules=rules, storage=storage, table_name="t"
        )
        results = validator.validate(_make_collected("avg", 100))
        assert results[0].severity == Severity.ERROR
        assert results[0].details.get("missing_partition_anchor") is True
        # Storage should never have been queried.
        assert storage.last_step == "sentinel-unset"

    def test_pydantic_rejects_compare_without_step(self):
        """``HistoricalCompareConfig.step`` is required and
        non-nullable. Pydantic must reject a YAML / programmatic
        config that omits ``compare.step`` at config-load time —
        before any validator runs."""
        from pydantic import ValidationError

        from qualifire.core.config import HistoricalCompareConfig

        with pytest.raises(ValidationError, match="step"):
            HistoricalCompareConfig(past_values=3)  # no step

    def test_raw_dict_without_step_surfaces_runtime_error(self):
        """The Pydantic layer enforces step presence, but the
        validator's ``rules`` parameter accepts raw dicts (used by
        callers that bypass Pydantic for testing or scripted
        scenarios). For that path, an omitted ``compare.step`` must
        still surface a structured ERROR with
        ``missing_partition_anchor=True`` rather than silently
        bucketing on a default."""
        past = [{"metric_value": 100.0, "run_timestamp": "2024-01-01"}]
        storage = _StorageThatCapturesStep(past)
        validator = HistoricalValidator(
            rules=[{
                "metric": "avg",
                "compare": {"past_values": 3},  # no step
                "thresholds": {"warning": {"deviation_pct": {"min": -10, "max": 10}}},
            }],
            storage=storage,
            table_name="t",
        )
        results = validator.validate(_make_collected("avg", 100))
        assert results[0].severity == Severity.ERROR
        assert results[0].details.get("missing_partition_anchor") is True
        # Storage was never queried — the validator short-circuited.
        assert storage.last_step == "sentinel-unset"
