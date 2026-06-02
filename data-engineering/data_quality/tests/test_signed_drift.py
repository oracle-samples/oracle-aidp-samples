"""Tests for signed historical comparisons + rate-of-change measure +
{min, max} threshold form.
"""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any

import pytest

from qualifire.core.models import CollectionResult, Severity
from qualifire.validation.historical import HistoricalValidator


class _StubStorage:
    """Minimal storage stub: returns canned past values."""

    def __init__(self, past_values: list[float]):
        self._past = past_values

    def read_metric_history(self, **kwargs: Any) -> list[dict[str, Any]]:
        return [{"metric_value": v, "run_timestamp": "2026-01-01"} for v in self._past]

    def read_metric_history_by_partition(self, **kwargs: Any) -> list[dict[str, Any]]:
        return self.read_metric_history(**kwargs)


_ANCHOR = datetime(2026, 5, 1)


def _cr(value: float, dim: str | None = None) -> CollectionResult:
    # partition_ts is mandatory for history-backed validators (drift /
    # forecast) — anchors the lookback at exactly partition_ts − k·step.
    # We pin a fixed anchor here so tests are deterministic.
    return CollectionResult(
        metric_name="m", metric_value=value, collected_at=datetime.now(),
        dimension_value=dim, partition_ts=_ANCHOR,
    )


class TestSignedDeviation:
    def test_negative_deviation_pct_reports_signed(self):
        # Mean of past = 100, current = 60 → deviation_pct = -40%.
        # With bare-number bound 50 (legacy symmetric), |−40| < 50 → PASS.
        storage = _StubStorage([100.0, 100.0, 100.0])
        v = HistoricalValidator(
            rules=[{
                "metric": "m",
                "compare": {"past_values": 3, "step": "P1D"},
                "thresholds": {"warning": {"deviation_pct": {"min": -50, "max": 50}}},
            }],
            storage=storage,
            table_name="t",
            name="drift",
        )
        results = v.validate([_cr(60.0)])
        assert results[0].severity == Severity.PASS
        assert results[0].details["deviation_pct"] == pytest.approx(-40.0)

    def test_negative_deviation_pct_breaches_signed_min_bound(self):
        # Same -40%, but now `error: {deviation_pct: {min: -25}}` should fire
        # because -40 < -25. Asymmetric: only drops are alerted.
        storage = _StubStorage([100.0, 100.0, 100.0])
        v = HistoricalValidator(
            rules=[{
                "metric": "m",
                "compare": {"past_values": 3, "step": "P1D"},
                "thresholds": {"error": {"deviation_pct": {"min": -25}}},
            }],
            storage=storage,
            table_name="t",
            name="drift",
        )
        results = v.validate([_cr(60.0)])
        assert results[0].severity == Severity.ERROR

    def test_positive_deviation_doesnt_breach_min_only_bound(self):
        # +40% deviation, only `min: -25` configured → should PASS.
        storage = _StubStorage([100.0, 100.0, 100.0])
        v = HistoricalValidator(
            rules=[{
                "metric": "m",
                "compare": {"past_values": 3, "step": "P1D"},
                "thresholds": {"error": {"deviation_pct": {"min": -25}}},
            }],
            storage=storage,
            table_name="t",
            name="drift",
        )
        results = v.validate([_cr(140.0)])
        assert results[0].severity == Severity.PASS

class TestRateOfChange:
    def test_rate_of_change_pct_against_immediate_prior(self):
        # past_values returned most-recent-first: [110, 105, 100].
        # Immediate prior = 110; current = 121 → +10% rate_of_change_pct.
        storage = _StubStorage([110.0, 105.0, 100.0])
        v = HistoricalValidator(
            rules=[{
                "metric": "m",
                "compare": {"past_values": 3, "step": "P1D"},
                "thresholds": {"warning": {"rate_of_change_pct": {"max": 5}}},
            }],
            storage=storage,
            table_name="t",
            name="drift",
        )
        results = v.validate([_cr(121.0)])
        assert results[0].severity == Severity.WARNING
        assert results[0].details["rate_of_change_pct"] == pytest.approx(10.0)

    def test_rate_of_change_negative_signed(self):
        # +10% drop is allowed (max only); −20% drop should fire on `min: -10`.
        storage = _StubStorage([100.0, 100.0, 100.0])
        v = HistoricalValidator(
            rules=[{
                "metric": "m",
                "compare": {"past_values": 3, "step": "P1D"},
                "thresholds": {"error": {"rate_of_change_pct": {"min": -10}}},
            }],
            storage=storage,
            table_name="t",
            name="drift",
        )
        results = v.validate([_cr(80.0)])
        assert results[0].severity == Severity.ERROR
        assert results[0].details["rate_of_change_pct"] == pytest.approx(-20.0)

    def test_rate_of_change_abs(self):
        storage = _StubStorage([100.0, 100.0, 100.0])
        v = HistoricalValidator(
            rules=[{
                "metric": "m",
                "compare": {"past_values": 3, "step": "P1D"},
                "thresholds": {"error": {"rate_of_change_abs": {"min": -5, "max": 5}}},
            }],
            storage=storage,
            table_name="t",
            name="drift",
        )
        # 92 vs prev 100 → -8 → outside [-5, 5].
        assert v.validate([_cr(92.0)])[0].severity == Severity.ERROR
        # 103 vs prev 100 → +3 → within bounds.
        assert v.validate([_cr(103.0)])[0].severity == Severity.PASS


class TestSignedZScore:
    def test_z_score_signed_in_details(self):
        # Mean=100, stddev>0; current=60 → negative z-score in details.
        storage = _StubStorage([100.0, 110.0, 90.0])
        v = HistoricalValidator(
            rules=[{
                "metric": "m",
                "compare": {"past_values": 3, "step": "P1D"},
                "thresholds": {"error": {"z_score": {"max": -3.0}}},  # impossible bound — never violated
            }],
            storage=storage,
            table_name="t",
            name="drift",
        )
        results = v.validate([_cr(60.0)])
        assert results[0].details["z_score"] < 0

    def test_z_score_min_bound_fires_on_strong_negative(self):
        storage = _StubStorage([100.0, 100.0, 100.0, 100.0])
        # Stddev = 0 → z_score defaults to 0. Use varying history to get nonzero stddev.
        storage = _StubStorage([100.0, 105.0, 95.0])
        v = HistoricalValidator(
            rules=[{
                "metric": "m",
                "compare": {"past_values": 3, "step": "P1D"},
                "thresholds": {"error": {"z_score": {"min": -2.0}}},
            }],
            storage=storage,
            table_name="t",
            name="drift",
        )
        # current=70 with mean=100, stddev~5 → z_score ~ -6
        results = v.validate([_cr(70.0)])
        assert results[0].severity == Severity.ERROR


class TestThresholdsCompositeBoundForms:
    def test_min_only_bound(self):
        storage = _StubStorage([100.0, 100.0, 100.0])
        v = HistoricalValidator(
            rules=[{
                "metric": "m",
                "compare": {"past_values": 3, "step": "P1D"},
                "thresholds": {"error": {"deviation_abs": {"min": -10}}},
            }],
            storage=storage,
            table_name="t",
            name="drift",
        )
        # current=85 → deviation_abs = -15 < -10 → ERROR
        assert v.validate([_cr(85.0)])[0].severity == Severity.ERROR
        # current=92 → deviation_abs = -8 → PASS
        assert v.validate([_cr(92.0)])[0].severity == Severity.PASS

    def test_max_only_bound(self):
        storage = _StubStorage([100.0, 100.0, 100.0])
        v = HistoricalValidator(
            rules=[{
                "metric": "m",
                "compare": {"past_values": 3, "step": "P1D"},
                "thresholds": {"error": {"deviation_abs": {"max": 10}}},
            }],
            storage=storage,
            table_name="t",
            name="drift",
        )
        # current=120 → deviation_abs = +20 > 10 → ERROR
        assert v.validate([_cr(120.0)])[0].severity == Severity.ERROR
        # current=85 → deviation_abs = -15 → PASS (no min bound)
        assert v.validate([_cr(85.0)])[0].severity == Severity.PASS


class TestMixedMeasureAsymmetricThresholds:
    """Multi-measure rule with **different** bound shapes per measure
    in a single tier. Pins that the validator evaluates each measure
    independently and trips on the first hit."""

    def _validator(self, thresholds):
        # 4 prior partitions of 100, last value 100 → mean=100, last=100.
        # Drives deviation_pct + rate_of_change_pct measures cleanly.
        storage = _StubStorage([100.0, 100.0, 100.0, 100.0])
        return HistoricalValidator(
            rules=[{
                "metric": "m",
                "compare": {"past_values": 4, "step": "P1D"},
                "thresholds": thresholds,
            }],
            storage=storage,
            table_name="t",
            name="drift",
        )

    def test_only_deviation_fires_when_rate_within_band(self):
        v = self._validator({
            "warning": {
                # Asymmetric: tighter on the way down than up.
                "deviation_pct":      {"min": -5, "max": 50},
                "rate_of_change_pct": {"min": -30, "max": 30},
            },
        })
        # current=80 → deviation_pct = -20 (< min=-5 → fires);
        # rate_of_change_pct = -20 (within ±30 → does NOT fire).
        result = v.validate([_cr(80.0)])[0]
        assert result.severity == Severity.WARNING
        # Both measure values surface in details regardless of which
        # one tripped — pin them so we know the validator computed both
        # before deciding severity.
        assert result.details["deviation_pct"] == -20.0
        assert result.details["rate_of_change_pct"] == -20.0
        # Message must name the firing measure so the alert tells the
        # operator which side blew up. (rate_of_change_pct is within
        # band so it shouldn't be the one called out.)
        assert "deviation_pct" in result.message

    def test_both_measures_fire_at_different_tiers(self):
        v = self._validator({
            "warning": {
                "deviation_pct":      {"min": -10, "max": 10},
                "rate_of_change_pct": {"min": -5,  "max": 5},
            },
            "error": {
                "deviation_pct":      {"min": -50, "max": 50},
            },
        })
        # current=60 → deviation_pct=-40 (within ±50 error band, fires
        # warning), rate_of_change_pct=-40 (fires warning). Highest
        # severity is WARNING since deviation didn't trip the error band.
        result = v.validate([_cr(60.0)])[0]
        assert result.severity == Severity.WARNING

    def test_bare_number_and_dict_form_can_coexist_per_measure(self):
        v = self._validator({
            "warning": {
                # Mix legacy bare-number with new dict form in the same
                # tier — they MUST resolve independently.
                "deviation_pct": {"min": -25, "max": 25},                  # symmetric ±25
                "rate_of_change_pct": {"min": -100, "max": 100},
            },
        })
        # current=120 → deviation_pct = +20 → within ±25 → PASS for
        # that measure; rate_of_change_pct = +20 → within ±100 → PASS.
        assert v.validate([_cr(120.0)])[0].severity == Severity.PASS
        # current=130 → deviation_pct = +30 → trips bare-number 25.
        assert v.validate([_cr(130.0)])[0].severity == Severity.WARNING


class TestValidateDfWithoutPartitionTs:
    """Regression test for the df-mode entry point.

    ``validate(df=...)`` materializes the DataFrame as a temp view and
    swaps the dataset's ``table=`` with the view name. The swap MUST
    carry ``partition_ts`` forward — otherwise drift / forecast
    validators surface a ``missing_partition_anchor`` ERROR even when
    the caller passed ``partition_ts=`` to ``validate()``.
    """

    def test_validate_df_without_partition_ts_emits_missing_anchor_error(
        self, tmp_path
    ):
        import pandas as pd

        from qualifire.api import Qualifire
        from qualifire.backends.pandas_backend import PandasBackend
        from qualifire.core.exceptions import QualifireValidationError
        from qualifire.storage.sqlite_storage import SQLiteStorage

        storage = SQLiteStorage(db_path=str(tmp_path / "qf.db"))
        storage.initialize()
        backend = PandasBackend(tables={})
        qf = Qualifire(
            backend=backend, owner="o", bu="b",
            system_table=str(tmp_path / "qf.db"),
            system_table_backend="sqlite",
        )

        drift = Qualifire.drift_check(
            aggregations={"row_count": "COUNT(*)"},
            rules=[{
                "metric": "row_count",
                "compare": {"past_values": 3, "step": "P1D"},
                "thresholds": {"warning": {"deviation_pct": {"min": -10, "max": 10}}},
            }],
        )

        df = pd.DataFrame({"id": list(range(10))})
        # No partition_ts on the call — drift surfaces the structured
        # ERROR instead of falling back to run_timestamp.
        try:
            result = qf.validate(df=df, name="ephemeral", validations=[drift])
        except QualifireValidationError as e:
            result = e.result

        drift_results = [
            vr for ds in result.datasets
            for vr in ds.validation_results
            if vr.validation_type == "drift"
        ]
        assert drift_results, "expected at least one drift result"
        for vr in drift_results:
            assert vr.severity == Severity.ERROR
            assert vr.details.get("missing_partition_anchor") is True

    def test_validate_df_with_partition_ts_does_not_error(self, tmp_path):
        """Companion to the test above: the same call with
        ``partition_ts=`` set takes the cold-start path (no seed
        history) and PASSes — proves the engine is forwarding
        partition_ts through the temp-view swap."""
        import pandas as pd

        from qualifire.api import Qualifire
        from qualifire.backends.pandas_backend import PandasBackend
        from qualifire.core.exceptions import QualifireValidationError
        from qualifire.storage.sqlite_storage import SQLiteStorage

        storage = SQLiteStorage(db_path=str(tmp_path / "qf.db"))
        storage.initialize()
        backend = PandasBackend(tables={})
        qf = Qualifire(
            backend=backend, owner="o", bu="b",
            system_table=str(tmp_path / "qf.db"),
            system_table_backend="sqlite",
        )

        drift = Qualifire.drift_check(
            aggregations={"row_count": "COUNT(*)"},
            rules=[{
                "metric": "row_count",
                "compare": {"past_values": 3, "step": "P1D"},
                "thresholds": {"warning": {"deviation_pct": {"min": -10, "max": 10}}},
            }],
        )
        df = pd.DataFrame({"id": list(range(10))})
        try:
            result = qf.validate(
                df=df, name="ephemeral",
                validations=[drift],
                partition_ts="'2026-05-07'",
                partition_step="P1D",
            )
        except QualifireValidationError as e:
            result = e.result

        drift_results = [
            vr for ds in result.datasets
            for vr in ds.validation_results
            if vr.validation_type == "drift"
        ]
        assert drift_results
        for vr in drift_results:
            assert vr.severity != Severity.ERROR
            # Cold-start (no seed history) must be the reason —
            # partition_ts must NOT be reported as missing.
            assert not vr.details.get("missing_partition_anchor")
