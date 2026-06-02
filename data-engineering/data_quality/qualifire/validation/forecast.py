"""Prophet-based time-series forecast validator.

Requires: qualifire[forecast] (prophet)
"""

from __future__ import annotations

import logging
from typing import Any

from qualifire.core.models import CollectionResult, Severity, ValidationResult
from qualifire.validation.base import Validator

logger = logging.getLogger(__name__)


def _import_prophet():
    """Lazy import of Prophet with a clear error message."""
    try:
        from prophet import Prophet

        return Prophet
    except ImportError:
        raise ImportError(
            "Prophet is required for forecast validation. "
            "Install it with: pip install 'qualifire[forecast]'"
        )


class ForecastValidator(Validator):
    """Validates metrics against Prophet time-series predictions.

    Reads historical values from the system table, fits a Prophet model,
    and checks if the current value falls within the prediction interval.

    Two interval widths: warning (default 0.80) and error (default 0.95).
    A value outside the error band triggers ERROR; outside warning but
    inside error triggers WARNING.

    Args:
        rules: List of forecast rule configs.
        storage: System table storage for reading historical data.
        table_name: Name of the monitored table.
        name: Validation name.
    """

    def __init__(
        self,
        rules: list[dict[str, Any]],
        storage: Any = None,
        table_name: str = "",
        name: str = "trend_check",
        on_empty_data: Severity = Severity.WARNING,
    ):
        super().__init__(on_empty_data=on_empty_data)
        self.rules = rules
        self.storage = storage
        self.table_name = table_name
        self.name = name

    def validate(
        self,
        collected: list[CollectionResult],
        **kwargs: Any,
    ) -> list[ValidationResult]:
        Prophet = _import_prophet()
        import pandas as pd

        metrics_by_name: dict[str, list[CollectionResult]] = {}
        for cr in collected:
            metrics_by_name.setdefault(cr.metric_name, []).append(cr)
        results: list[ValidationResult] = []

        for rule in self.rules:
            metric_name = rule["metric"]
            model_config = rule.get("model", {})

            crs = metrics_by_name.get(metric_name)
            if not crs:
                results.append(
                    ValidationResult(
                        validation_name=f"{self.name}.{metric_name}",
                        validation_type="trend",
                        severity=Severity.ERROR,
                        message=f"Metric '{metric_name}' not found in collected data",
                        validation_base_name=self.name,
                        metric_name=metric_name,
                    )
                )
                continue

            for cr in crs:
                results.extend(
                    self._validate_one(
                        Prophet=Prophet,
                        pd=pd,
                        metric_name=metric_name,
                        cr=cr,
                        model_config=model_config,
                    )
                )

        return results

    def _validate_one(
        self,
        *,
        Prophet: Any,
        pd: Any,
        metric_name: str,
        cr: CollectionResult,
        model_config: dict[str, Any],
    ) -> list[ValidationResult]:
        results: list[ValidationResult] = []
        history_count = model_config.get("history_count", 90)
        step = model_config.get("step")
        changepoint_prior_scale = model_config.get("changepoint_prior_scale", 0.05)
        seasonality_prior_scale = model_config.get("seasonality_prior_scale", 10.0)
        seasonality_mode = model_config.get("seasonality_mode", "additive")
        interval_width_cfg = model_config.get("interval_width", {})
        warning_interval = interval_width_cfg.get("warning", 0.80)
        error_interval = interval_width_cfg.get("error", 0.95)
        on_missing_history = model_config.get("on_missing_history", "ignore")

        if cr.metric_value is None:
            seg_label = (
                f"@{cr.dimension_value}"
                if cr.dimension_value
                else ""
            )
            results.append(self._empty_data_result(
                validation_name=f"{self.name}.{metric_name}",
                validation_type="trend",
                message=(
                    f"{metric_name}{seg_label} is NULL — no data or NULL aggregation. "
                    "Forecast comparison skipped."
                ),
            ))
            return results
        current_value = float(cr.metric_value)

        # Partition-anchored history is mandatory for forecast. Prophet
        # needs a meaningful time axis; run_timestamp clusters every
        # historical row at the same wall-clock moment (notebook re-runs,
        # backfills, replays) and produces nonsense fits. If the dataset
        # lacks partition_ts or the rule lacks compare.step, refuse to
        # forecast — surfacing a clear ERROR is better than fitting on
        # garbage.
        if cr.partition_ts is None:
            results.append(self._missing_partition_ts_result(
                metric_name=metric_name, cr=cr, current_value=current_value,
                reason="cr.partition_ts is None — set DatasetConfig.partition_ts",
                validation_type="trend",
            ))
            return results
        if not step:
            results.append(self._missing_partition_ts_result(
                metric_name=metric_name, cr=cr, current_value=current_value,
                reason="model.step is unset — set rule.model.step (e.g. 'P1D')",
                validation_type="trend",
            ))
            return results

        history: list[dict[str, Any]] = []
        if self.storage:
            history = self.storage.read_metric_history_by_partition(
                table_name=self.table_name,
                metric_name=metric_name,
                anchor_ts=cr.partition_ts,
                count=history_count,
                step=step,
                dimension_value=cr.dimension_value,
            )

        min_points = 2 if on_missing_history == "ignore" else 10
        if len(history) < min_points:
            severity_map = {"ignore": Severity.PASS, "warn": Severity.WARNING, "error": Severity.ERROR}
            results.append(
                ValidationResult(
                    validation_name=f"{self.name}.{metric_name}",
                    validation_type="trend",
                    severity=severity_map[on_missing_history],
                    message=(
                        f"Insufficient history for '{metric_name}' "
                        f"({len(history)} points, need >= {min_points}), skipping forecast"
                    ),
                    validation_base_name=self.name,
                    metric_name=metric_name,
                    dimension_value=cr.dimension_value,
                    actual_value=current_value,
                    details={"cold_start": True},
                )
            )
            return results

        # Prophet's `ds` axis is the partition timestamp — the logical
        # time the metric describes. read_metric_history_by_partition
        # always returns partition_ts; if a row is missing it (legacy
        # write before partition_ts existed), drop it rather than
        # falling back to run_timestamp.
        df = pd.DataFrame(history)
        df = df.rename(columns={"partition_ts": "ds", "metric_value": "y"})
        df["ds"] = pd.to_datetime(df["ds"], errors="coerce")
        df["y"] = pd.to_numeric(df["y"], errors="coerce")
        df = df.dropna(subset=["ds", "y"])
        df = df.sort_values("ds").reset_index(drop=True)
        # Collapse duplicate partition_ts — possible when an operator
        # ran the same partition multiple times. Keep the most recent
        # write per partition (the read already preferred collection
        # over validation rows).
        if df["ds"].duplicated().any():
            df = df.drop_duplicates(subset="ds", keep="last").reset_index(drop=True)

        # Predict explicitly at ``cr.partition_ts`` rather than asking
        # Prophet for "one period after the last training row".
        # ``make_future_dataframe`` would predict for
        # ``last_seen_partition + step`` — when the immediate prior
        # partition is missing but older history exists, that's not
        # the same as the current run's partition_ts and the
        # comparison would silently align with the wrong period.
        future = pd.DataFrame({"ds": [pd.Timestamp(cr.partition_ts)]})

        model_error = Prophet(
            changepoint_prior_scale=changepoint_prior_scale,
            seasonality_prior_scale=seasonality_prior_scale,
            seasonality_mode=seasonality_mode,
            interval_width=error_interval,
        )
        model_error.fit(df)
        forecast_error = model_error.predict(future)
        last_error = forecast_error.iloc[-1]

        model_warning = Prophet(
            changepoint_prior_scale=changepoint_prior_scale,
            seasonality_prior_scale=seasonality_prior_scale,
            seasonality_mode=seasonality_mode,
            interval_width=warning_interval,
        )
        model_warning.fit(df)
        forecast_warning = model_warning.predict(future)
        last_warning = forecast_warning.iloc[-1]

        error_violated = (
            current_value < last_error["yhat_lower"]
            or current_value > last_error["yhat_upper"]
        )
        warning_violated = (
            current_value < last_warning["yhat_lower"]
            or current_value > last_warning["yhat_upper"]
        )

        severity = self.determine_severity(current_value, warning_violated, error_violated)

        yhat = last_error["yhat"]
        seg_label = (
            f"@{cr.dimension_value}"
            if cr.dimension_value
            else ""
        )
        message = (
            f"{metric_name}{seg_label} = {current_value:.4f} "
            f"(predicted = {yhat:.4f}, "
            f"warning band = [{last_warning['yhat_lower']:.4f}, {last_warning['yhat_upper']:.4f}], "
            f"error band = [{last_error['yhat_lower']:.4f}, {last_error['yhat_upper']:.4f}])"
        )

        results.append(
            ValidationResult(
                validation_name=f"{self.name}.{metric_name}",
                validation_type="trend",
                severity=severity,
                message=message,
                validation_base_name=self.name,
                metric_name=metric_name,
                dimension_value=cr.dimension_value,
                expected_value={
                    "yhat": yhat,
                    "warning_lower": last_warning["yhat_lower"],
                    "warning_upper": last_warning["yhat_upper"],
                    "error_lower": last_error["yhat_lower"],
                    "error_upper": last_error["yhat_upper"],
                },
                actual_value=current_value,
                details={
                    "history_count": len(history),
                    "changepoint_prior_scale": changepoint_prior_scale,
                    "seasonality_mode": seasonality_mode,
                },
                collected_at=cr.collected_at,
            )
        )
        return results
