"""Tests for Prophet forecast validator.

Uses mocked Prophet to avoid requiring the heavy cmdstanpy dependency.
"""

from datetime import datetime
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from qualifire.core.models import CollectionResult, Severity
from qualifire.validation.forecast import ForecastValidator


# Forecast requires partition_ts on the collected row (Prophet's `ds`
# axis). Pin a fixed anchor — the run_timestamp fallback was removed.
_ANCHOR = datetime(2024, 2, 1)


def _make_collected(name: str, value: float) -> list[CollectionResult]:
    return [
        CollectionResult(
            metric_name=name, metric_value=value,
            collected_at=datetime.now(),
            partition_ts=_ANCHOR,
        )
    ]


class MockStorage:
    """Mock satisfying the partition-anchored read interface used by the
    forecast validator."""

    def __init__(self, history: list[dict]):
        self._history = history

    def read_metric_history(self, **kwargs):
        return self._history

    def read_metric_history_by_partition(self, **kwargs):
        return self._history


def _make_history(n: int, base_value: float = 100.0) -> list[dict]:
    """Generate n historical data points with `partition_ts` (the canonical
    time axis Prophet now sees)."""
    return [
        {
            "metric_value": base_value + (i % 5),
            "partition_ts": f"2024-01-{i + 1:02d}T00:00:00",
            "run_timestamp": f"2024-01-{i + 1:02d}T00:00:00",
            "validation_status": "PASS",
        }
        for i in range(n)
    ]


class TestForecastValidator:
    def test_missing_metric_returns_error(self):
        """When the metric isn't in collected data, return ERROR."""
        with patch("qualifire.validation.forecast._import_prophet") as mock_import:
            mock_import.return_value = MagicMock()

            validator = ForecastValidator(
                rules=[{"metric": "missing_metric", "model": {"step": "P1D"}}],
                storage=MockStorage([]),
            )
            results = validator.validate(_make_collected("other_metric", 100))

            assert len(results) == 1
            assert results[0].severity == Severity.ERROR
            assert "not found" in results[0].message

    def test_insufficient_history_passes_by_default(self):
        """Default on_missing_history='ignore' with < 2 points returns PASS."""
        with patch("qualifire.validation.forecast._import_prophet") as mock_import:
            mock_import.return_value = MagicMock()

            validator = ForecastValidator(
                rules=[{"metric": "avg_sales", "model": {"step": "P1D"}}],
                storage=MockStorage(_make_history(1)),
                table_name="t",
            )
            results = validator.validate(_make_collected("avg_sales", 100))

            assert len(results) == 1
            assert results[0].severity == Severity.PASS
            assert "Insufficient history" in results[0].message
            assert results[0].details.get("cold_start") is True

    def test_insufficient_history_on_missing_history_warn(self):
        """on_missing_history='warn' with < 10 points returns WARNING."""
        with patch("qualifire.validation.forecast._import_prophet") as mock_import:
            mock_import.return_value = MagicMock()

            validator = ForecastValidator(
                rules=[{"metric": "avg_sales", "model": {"step": "P1D", "on_missing_history": "warn"}}],
                storage=MockStorage(_make_history(5)),
                table_name="t",
            )
            results = validator.validate(_make_collected("avg_sales", 100))

            assert results[0].severity == Severity.WARNING
            assert "Insufficient history" in results[0].message
            assert results[0].details.get("cold_start") is True

    def test_insufficient_history_on_missing_history_error(self):
        """on_missing_history='error' with < 10 points returns ERROR."""
        with patch("qualifire.validation.forecast._import_prophet") as mock_import:
            mock_import.return_value = MagicMock()

            validator = ForecastValidator(
                rules=[{"metric": "avg_sales", "model": {"step": "P1D", "on_missing_history": "error"}}],
                storage=MockStorage(_make_history(5)),
                table_name="t",
            )
            results = validator.validate(_make_collected("avg_sales", 100))

            assert results[0].severity == Severity.ERROR
            assert results[0].details.get("cold_start") is True

    def test_no_storage_passes_by_default(self):
        """With no storage, history is empty -> on_missing_history='ignore' -> PASS."""
        with patch("qualifire.validation.forecast._import_prophet") as mock_import:
            mock_import.return_value = MagicMock()

            validator = ForecastValidator(
                rules=[{"metric": "m", "model": {"step": "P1D"}}],
                storage=None,
            )
            results = validator.validate(_make_collected("m", 50))

            assert results[0].severity == Severity.PASS
            assert "Insufficient history" in results[0].message
            assert results[0].details.get("cold_start") is True

    def test_pass_within_prediction_band(self):
        """Value inside both bands -> PASS."""
        import pandas as pd

        mock_prophet_cls = MagicMock()

        # Mock the Prophet model instance
        mock_model = MagicMock()
        mock_prophet_cls.return_value = mock_model

        # make_future_dataframe returns a df
        future_df = pd.DataFrame({"ds": pd.date_range("2024-01-01", periods=31)})
        mock_model.make_future_dataframe.return_value = future_df

        # predict returns a df with yhat, yhat_lower, yhat_upper
        # Both warning and error bands contain the current value 102
        forecast_df = pd.DataFrame({
            "ds": future_df["ds"],
            "yhat": [100.0] * 31,
            "yhat_lower": [80.0] * 31,
            "yhat_upper": [120.0] * 31,
        })
        mock_model.predict.return_value = forecast_df

        with patch("qualifire.validation.forecast._import_prophet", return_value=mock_prophet_cls):
            validator = ForecastValidator(
                rules=[{"metric": "avg_sales", "model": {"step": "P1D", "history_count": 30}}],
                storage=MockStorage(_make_history(30)),
                table_name="t",
            )
            results = validator.validate(_make_collected("avg_sales", 102))

        assert len(results) == 1
        assert results[0].severity == Severity.PASS
        assert results[0].actual_value == 102
        assert "predicted" in results[0].message

    def test_warning_outside_narrow_band(self):
        """Value outside warning band but inside error band -> WARNING."""
        import pandas as pd

        mock_prophet_cls = MagicMock()
        mock_model = MagicMock()
        mock_prophet_cls.return_value = mock_model

        future_df = pd.DataFrame({"ds": pd.date_range("2024-01-01", periods=31)})
        mock_model.make_future_dataframe.return_value = future_df

        # Call count: first call is error model (wide band), second is warning model (narrow band)
        call_count = {"n": 0}

        def mock_predict(future):
            call_count["n"] += 1
            if call_count["n"] == 1:
                # Error model: wide band — 130 is inside [50, 150]
                return pd.DataFrame({
                    "ds": future["ds"],
                    "yhat": [100.0] * len(future),
                    "yhat_lower": [50.0] * len(future),
                    "yhat_upper": [150.0] * len(future),
                })
            else:
                # Warning model: narrow band — 130 is outside [80, 120]
                return pd.DataFrame({
                    "ds": future["ds"],
                    "yhat": [100.0] * len(future),
                    "yhat_lower": [80.0] * len(future),
                    "yhat_upper": [120.0] * len(future),
                })

        mock_model.predict.side_effect = mock_predict

        with patch("qualifire.validation.forecast._import_prophet", return_value=mock_prophet_cls):
            validator = ForecastValidator(
                rules=[{"metric": "m", "model": {"step": "P1D", "history_count": 30}}],
                storage=MockStorage(_make_history(30)),
                table_name="t",
            )
            results = validator.validate(_make_collected("m", 130))

        assert results[0].severity == Severity.WARNING

    def test_error_outside_wide_band(self):
        """Value outside both bands -> ERROR."""
        import pandas as pd

        mock_prophet_cls = MagicMock()
        mock_model = MagicMock()
        mock_prophet_cls.return_value = mock_model

        future_df = pd.DataFrame({"ds": pd.date_range("2024-01-01", periods=31)})
        mock_model.make_future_dataframe.return_value = future_df

        # Both bands: value 200 is outside [50, 150] and [80, 120]
        def mock_predict(future):
            return pd.DataFrame({
                "ds": future["ds"],
                "yhat": [100.0] * len(future),
                "yhat_lower": [80.0] * len(future),
                "yhat_upper": [120.0] * len(future),
            })

        mock_model.predict.side_effect = mock_predict

        with patch("qualifire.validation.forecast._import_prophet", return_value=mock_prophet_cls):
            validator = ForecastValidator(
                rules=[{"metric": "m", "model": {"step": "P1D", "history_count": 30}}],
                storage=MockStorage(_make_history(30)),
                table_name="t",
            )
            results = validator.validate(_make_collected("m", 200))

        assert results[0].severity == Severity.ERROR

    def test_result_contains_expected_value_details(self):
        """Verify the result has yhat, band bounds, and details."""
        import pandas as pd

        mock_prophet_cls = MagicMock()
        mock_model = MagicMock()
        mock_prophet_cls.return_value = mock_model

        future_df = pd.DataFrame({"ds": pd.date_range("2024-01-01", periods=31)})
        mock_model.make_future_dataframe.return_value = future_df

        forecast_df = pd.DataFrame({
            "ds": future_df["ds"],
            "yhat": [100.0] * 31,
            "yhat_lower": [85.0] * 31,
            "yhat_upper": [115.0] * 31,
        })
        mock_model.predict.return_value = forecast_df

        with patch("qualifire.validation.forecast._import_prophet", return_value=mock_prophet_cls):
            validator = ForecastValidator(
                rules=[{
                    "metric": "m",
                    "model": {
                        "step": "P1D",
                        "history_count": 30,
                        "changepoint_prior_scale": 0.1,
                        "seasonality_mode": "multiplicative",
                    },
                }],
                storage=MockStorage(_make_history(30)),
                table_name="t",
            )
            results = validator.validate(_make_collected("m", 100))

        vr = results[0]
        assert vr.expected_value["yhat"] == 100.0
        assert "warning_lower" in vr.expected_value
        assert "error_upper" in vr.expected_value
        assert vr.details["history_count"] == 30
        assert vr.details["changepoint_prior_scale"] == 0.1
        assert vr.details["seasonality_mode"] == "multiplicative"

    def test_import_error_message(self):
        """When prophet is not installed, a clear ImportError is raised."""
        with patch("qualifire.validation.forecast._import_prophet", side_effect=ImportError("Prophet is required")):
            validator = ForecastValidator(rules=[{"metric": "m", "model": {"step": "P1D"}}])
            with pytest.raises(ImportError, match="Prophet is required"):
                validator.validate(_make_collected("m", 100))


class _StorageThatCapturesStep:
    """Test double that records ``step`` so the validator-level wiring
    can be asserted without pulling in a real storage backend.

    Captures from ``read_metric_history_by_partition`` — the canonical
    history-read entrypoint after the run_timestamp fallback was
    removed. ``read_metric_history`` is also implemented for
    legacy-caller resilience.
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


class TestForecastValidatorStepPassthrough:
    """Phase 2 P2.3: ``rule.model.step`` must flow through the forecast
    validator into ``read_metric_history``. Without this wiring the
    Prophet fit would always run against raw per-run points regardless
    of the YAML."""

    def test_explicit_step_is_forwarded_to_storage(self):
        storage = _StorageThatCapturesStep(_make_history(30))

        mock_prophet = MagicMock()
        mock_instance = MagicMock()
        mock_prophet.return_value = mock_instance
        mock_forecast = MagicMock()
        mock_forecast.iloc = MagicMock()
        mock_forecast.iloc.__getitem__ = MagicMock(
            return_value={"yhat": 100.0, "yhat_lower": 90.0, "yhat_upper": 110.0}
        )
        mock_instance.predict.return_value = mock_forecast
        mock_instance.make_future_dataframe.return_value = None

        with patch("qualifire.validation.forecast._import_prophet", return_value=mock_prophet):
            validator = ForecastValidator(
                rules=[{
                    "metric": "m",
                    "model": {"history_count": 30, "step": "P1D"},
                }],
                storage=storage,
                table_name="t",
            )
            validator.validate(_make_collected("m", 100))
        assert storage.last_step == "P1D"

    def test_pydantic_rejects_model_without_step(self):
        """``ForecastModelConfig.step`` is required and non-nullable.
        Pydantic must reject a YAML / programmatic config that omits
        ``model.step`` at config-load time — before any validator
        runs."""
        from pydantic import ValidationError

        from qualifire.core.config import ForecastModelConfig

        with pytest.raises(ValidationError, match="step"):
            ForecastModelConfig(history_count=30)  # no step

    def test_omitted_step_returns_error(self):
        """No step in the rule + no Pydantic default for raw-dict rules
        means the validator can't form a partition-anchored read.
        Surface ERROR with a reason rather than silently bucketing on
        run_timestamp (the removed fallback)."""
        storage = _StorageThatCapturesStep(_make_history(30))

        mock_prophet = MagicMock()
        with patch("qualifire.validation.forecast._import_prophet", return_value=mock_prophet):
            validator = ForecastValidator(
                rules=[{
                    "metric": "m",
                    "model": {"history_count": 30},          # no step
                }],
                storage=storage,
                table_name="t",
            )
            results = validator.validate(_make_collected("m", 100))
        assert results[0].severity == Severity.ERROR
        assert results[0].details.get("missing_partition_anchor") is True
        # Storage should never have been queried.
        assert storage.last_step == "sentinel-unset"


class TestForecastPredictsAtPartitionTs:
    """Prophet's prediction frame must be ``cr.partition_ts``, not
    ``last_seen_partition + step``. When a recent partition is missing
    from history but older partitions exist, the validator otherwise
    aligns the comparison to the wrong period."""

    def test_predicts_for_current_partition_when_immediate_prior_missing(self):
        import pandas as pd

        mock_prophet_cls = MagicMock()
        mock_model = MagicMock()
        mock_prophet_cls.return_value = mock_model
        # Stub predict to echo whatever future frame the validator
        # passes — we'll inspect it.
        captured: dict[str, Any] = {}

        def _predict(future_df):
            captured["ds"] = future_df["ds"].tolist()
            return pd.DataFrame({
                "ds": future_df["ds"],
                "yhat": [100.0] * len(future_df),
                "yhat_lower": [80.0] * len(future_df),
                "yhat_upper": [120.0] * len(future_df),
            })

        mock_model.predict.side_effect = _predict
        # make_future_dataframe should NOT be consulted; the
        # validator builds its own frame at cr.partition_ts.
        mock_model.make_future_dataframe.side_effect = AssertionError(
            "make_future_dataframe must not be called — predict at "
            "cr.partition_ts directly"
        )

        # History has a gap: anchor − 7d is missing but earlier days
        # exist. With make_future_dataframe(periods=1), Prophet would
        # predict for the day after the LAST seen training row, which
        # would NOT be the current anchor. Asserting the captured ds
        # equals cr.partition_ts proves the fix.
        anchor = datetime(2024, 1, 30)
        history = [
            {
                "metric_value": 100.0,
                "partition_ts": f"2024-01-{i:02d}T00:00:00",
                "run_timestamp": f"2024-01-{i:02d}T00:00:00",
                "validation_status": "PASS",
            }
            for i in range(1, 24)  # 1..23, skipping 24-29
        ]

        with patch(
            "qualifire.validation.forecast._import_prophet",
            return_value=mock_prophet_cls,
        ):
            validator = ForecastValidator(
                rules=[{
                    "metric": "m",
                    "model": {"step": "P1D", "history_count": 30},
                }],
                storage=MockStorage(history),
                table_name="t",
            )
            cr = CollectionResult(
                metric_name="m", metric_value=100,
                collected_at=datetime.now(),
                partition_ts=anchor,
            )
            validator.validate([cr])

        # Prediction frame must contain exactly the current partition_ts.
        assert len(captured["ds"]) == 1
        assert pd.Timestamp(captured["ds"][0]) == pd.Timestamp(anchor)
