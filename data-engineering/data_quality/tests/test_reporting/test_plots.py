"""Tests for plot generation functions.

Verifies that plot functions return matplotlib Figure objects without errors.
Does not test visual correctness.
"""

import pytest

from qualifire.core.models import (
    DatasetResult,
    QualifireResult,
    Severity,
    ValidationResult,
)


@pytest.fixture
def sample_history():
    return [
        {"run_timestamp": f"2024-01-{i:02d}", "metric_value": 100 + i, "validation_status": "PASS"}
        for i in range(1, 11)
    ]


@pytest.fixture
def sample_result():
    return QualifireResult(
        owner="o", bu="b", run_id="r",
        datasets=[
            DatasetResult(
                dataset_name="ds", table="t", run_id="r",
                validation_results=[
                    ValidationResult(validation_name="v1", validation_type="threshold",
                                     severity=Severity.PASS, message="ok",
                                     validation_base_name="v1"),
                    ValidationResult(validation_name="v2", validation_type="slo",
                                     severity=Severity.WARNING, message="warn",
                                     validation_base_name="v2"),
                    ValidationResult(validation_name="v3", validation_type="threshold",
                                     severity=Severity.ERROR, message="bad",
                                     validation_base_name="v3"),
                ],
            ),
        ],
    )


class TestPlotMetricHistory:
    def test_returns_figure(self, sample_history):
        import matplotlib

        matplotlib.use("Agg")  # non-interactive backend
        from qualifire.reporting.plots import plot_metric_history

        fig = plot_metric_history(sample_history, metric_name="test_metric")
        assert fig is not None
        assert hasattr(fig, "savefig")  # it's a Figure

    def test_custom_title(self, sample_history):
        import matplotlib

        matplotlib.use("Agg")
        from qualifire.reporting.plots import plot_metric_history

        fig = plot_metric_history(sample_history, title="Custom Title")
        assert fig is not None


class TestPlotValidationSummary:
    def test_returns_figure(self, sample_result):
        import matplotlib

        matplotlib.use("Agg")
        from qualifire.reporting.plots import plot_validation_summary

        fig = plot_validation_summary(sample_result)
        assert fig is not None
        assert hasattr(fig, "savefig")


class TestPlotAnomalyShap:
    def test_with_features(self):
        import matplotlib

        matplotlib.use("Agg")
        from qualifire.reporting.plots import plot_anomaly_shap

        details = {
            "top_contributing_features": [
                {"feature": "amount", "importance": 0.5},
                {"feature": "count", "importance": 0.3},
            ]
        }
        fig = plot_anomaly_shap(details)
        assert fig is not None

    def test_empty_features(self):
        import matplotlib

        matplotlib.use("Agg")
        from qualifire.reporting.plots import plot_anomaly_shap

        fig = plot_anomaly_shap({})
        assert fig is not None  # shows "No SHAP data" text


class TestPlotForecast:
    def test_returns_figure(self, sample_history):
        import matplotlib

        matplotlib.use("Agg")
        from qualifire.reporting.plots import plot_forecast

        forecast_details = {
            "yhat": 110.0,
            "warning_lower": 95.0,
            "warning_upper": 125.0,
            "error_lower": 85.0,
            "error_upper": 135.0,
        }
        fig = plot_forecast(sample_history, forecast_details, 112.0, metric_name="m")
        assert fig is not None
        assert hasattr(fig, "savefig")


class TestPlotValueDrift:
    def test_returns_figure_for_all_kinds(self):
        import matplotlib
        matplotlib.use("Agg")
        from qualifire.reporting.plots import plot_value_drift

        explainer = [
            {
                "feature": "amount", "kind": "numeric",
                "current": {"mean": 100.0}, "past": {"mean": 80.0},
                "summary": "p50 +25%",
            },
            {
                "feature": "flag", "kind": "boolean",
                "current": {"true_rate": 0.7}, "past": {"true_rate": 0.5},
                "summary": "true_rate +20pp",
            },
            {
                "feature": "channel_mobile", "kind": "onehot",
                "current": {"rate": 0.6}, "past": {"rate": 0.3},
                "summary": "channel='mobile' +30pp",
            },
            {
                "feature": "user_id", "kind": "label_encoded",
                "current": {"top": [{"value": "bot", "rate": 0.4}]},
                "past": {"top": [{"value": "human", "rate": 0.9}]},
                "summary": "top: bot=40%",
            },
            {
                "feature": "ts_timestamp", "kind": "datetime",
                "current": {"min": "2026-01-01", "max": "2026-01-31"},
                "past": {"min": "2025-12-01", "max": "2025-12-31"},
                "summary": "range shifted",
            },
        ]
        fig = plot_value_drift(explainer, title="Drift")
        assert fig is not None
        assert hasattr(fig, "savefig")

    def test_handles_empty_input(self):
        import matplotlib
        matplotlib.use("Agg")
        from qualifire.reporting.plots import plot_value_drift

        fig = plot_value_drift([])
        assert fig is not None

    def test_skips_unknown_and_truncated(self):
        import matplotlib
        matplotlib.use("Agg")
        from qualifire.reporting.plots import plot_value_drift

        fig = plot_value_drift([
            {"feature": "x", "kind": "unknown", "summary": "n/a"},
            {"feature": "y", "kind": "truncated", "summary": "payload truncated"},
        ])
        assert fig is not None
