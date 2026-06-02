"""Historical comparison validator: compare current values against past values."""

from __future__ import annotations

import math
from typing import Any

from qualifire.core.models import CollectionResult, Severity, ValidationResult
from qualifire.validation.base import Validator


class HistoricalValidator(Validator):
    """Validates metrics by comparing against historical values from the system table.

    Comparison modes:
        deviation_pct: Percentage deviation from mean of past values.
        deviation_abs: Absolute deviation from mean of past values.
        z_score: Z-score against past values distribution.

    Missing value strategies:
        ignore: Skip missing values (default).
        substitute: Replace missing values with the mean of available values.
        error: Raise error if any values are missing.

    Args:
        rules: List of rule configs with metric, compare, and thresholds.
        storage: System table storage for reading historical data.
        name: Validation name.
    """

    def __init__(
        self,
        rules: list[dict[str, Any]],
        storage: Any = None,
        table_name: str = "",
        name: str = "drift_check",
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
        # Group by metric name; a multi-dim aggregation collector
        # emits one CollectionResult per (metric, dimension).
        metrics_by_name: dict[str, list[CollectionResult]] = {}
        for cr in collected:
            metrics_by_name.setdefault(cr.metric_name, []).append(cr)
        results: list[ValidationResult] = []

        for rule in self.rules:
            metric_name = rule["metric"]
            compare = rule.get("compare", {})
            thresholds = rule.get("thresholds", {})
            past_values_count = compare.get("past_values", 3)
            missing_strategy = compare.get("missing_strategy", "ignore")
            on_missing_history = compare.get("on_missing_history", "ignore")
            step = compare.get("step")

            crs = metrics_by_name.get(metric_name)
            if not crs:
                results.append(
                    ValidationResult(
                        validation_name=f"{self.name}.{metric_name}",
                        validation_type="drift",
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
                        rule=rule,
                        cr=cr,
                        metric_name=metric_name,
                        past_values_count=past_values_count,
                        missing_strategy=missing_strategy,
                        on_missing_history=on_missing_history,
                        step=step,
                        thresholds=thresholds,
                    )
                )

        return results

    def _validate_one(
        self,
        *,
        rule: dict[str, Any],
        cr: CollectionResult,
        metric_name: str,
        past_values_count: int,
        missing_strategy: str,
        on_missing_history: str,
        step: str | None,
        thresholds: dict[str, Any],
    ) -> list[ValidationResult]:
        """Validate one (metric, dimension) segment."""
        results: list[ValidationResult] = []
        if cr.metric_value is None:
            seg_label = (
                f"@{cr.dimension_value}"
                if cr.dimension_value
                else ""
            )
            results.append(self._empty_data_result(
                validation_name=f"{self.name}.{metric_name}",
                validation_type="drift",
                message=(
                    f"{metric_name}{seg_label} is NULL — no data or NULL aggregation. "
                    "Drift comparison skipped."
                ),
            ))
            return results
        current_value = float(cr.metric_value)

        # Drift requires a partition-anchored lookback: read the metric
        # values at exactly `partition_ts − k·step` for k = 1..past_values.
        # Both inputs are mandatory — the run_timestamp-based fallback
        # was removed because run_timestamp is the *wall-clock when the
        # validation executed*, not the *logical time the metric
        # describes*. Backfills, replays, and notebook re-runs all
        # cluster run_timestamps within seconds of each other, so
        # ordering history by run_timestamp produces a degenerate time
        # axis. If either piece is missing, refuse to run with a clear
        # message — partition_ts is part of the dataset config, step
        # is part of the rule's compare block.
        if cr.partition_ts is None:
            results.append(self._missing_partition_ts_result(
                metric_name=metric_name, cr=cr, current_value=current_value,
                reason="cr.partition_ts is None — set DatasetConfig.partition_ts",
                validation_type="drift",
            ))
            return results
        if not step:
            results.append(self._missing_partition_ts_result(
                metric_name=metric_name, cr=cr, current_value=current_value,
                reason="compare.step is unset — set rule.compare.step (e.g. 'P1D')",
                validation_type="drift",
            ))
            return results

        past_rows: list[dict[str, Any]] = []
        if self.storage:
            past_rows = self.storage.read_metric_history_by_partition(
                table_name=self.table_name,
                metric_name=metric_name,
                anchor_ts=cr.partition_ts,
                count=past_values_count,
                step=step,
                dimension_value=cr.dimension_value,
            )

        past_values = [
            float(r["metric_value"])
            for r in past_rows
            if r.get("metric_value") is not None
        ]

        if len(past_values) < past_values_count:
            if missing_strategy == "error":
                results.append(
                    ValidationResult(
                        validation_name=f"{self.name}.{metric_name}",
                        validation_type="drift",
                        severity=Severity.ERROR,
                        message=(
                            f"Expected {past_values_count} historical values for "
                            f"'{metric_name}', found {len(past_values)}"
                        ),
                        validation_base_name=self.name,
                        metric_name=metric_name,
                        dimension_value=cr.dimension_value,
                        actual_value=current_value,
                    )
                )
                return results
            elif missing_strategy == "substitute" and past_values:
                mean_val = sum(past_values) / len(past_values)
                while len(past_values) < past_values_count:
                    past_values.append(mean_val)
            # "ignore" — just use what we have

        if not past_values:
            severity_map = {"ignore": Severity.PASS, "warn": Severity.WARNING, "error": Severity.ERROR}
            results.append(
                ValidationResult(
                    validation_name=f"{self.name}.{metric_name}",
                    validation_type="drift",
                    severity=severity_map[on_missing_history],
                    message=f"No historical data for '{metric_name}', skipping comparison",
                    validation_base_name=self.name,
                    metric_name=metric_name,
                    dimension_value=cr.dimension_value,
                    actual_value=current_value,
                    details={"cold_start": True},
                )
            )
            return results

        mean_past = sum(past_values) / len(past_values)
        # Signed deviations — preserve direction. -10% drift and +10% drift
        # are not the same in many domains (revenue, fraud rates, etc.), so
        # validators report signed values and users specify signed bounds
        # via {"min": ..., "max": ...}.
        deviation_abs = current_value - mean_past
        deviation_pct = (deviation_abs / abs(mean_past) * 100) if mean_past != 0 else 0.0

        stddev = 0.0
        if len(past_values) > 1:
            variance = sum((v - mean_past) ** 2 for v in past_values) / (len(past_values) - 1)
            stddev = math.sqrt(variance)
        z_score = (current_value - mean_past) / stddev if stddev > 0 else 0.0

        # Rate-of-change against the immediate-prior partition (past_values[0],
        # which is most-recent-first per the storage read contract). Signed.
        prev_value = past_values[0]
        rate_of_change_abs = current_value - prev_value
        rate_of_change_pct = (
            (rate_of_change_abs / abs(prev_value) * 100) if prev_value != 0 else 0.0
        )

        comparison = {
            "deviation_pct": deviation_pct,
            "deviation_abs": deviation_abs,
            "z_score": z_score,
            "rate_of_change_pct": rate_of_change_pct,
            "rate_of_change_abs": rate_of_change_abs,
        }

        warning_thresholds = thresholds.get("warning", {})
        error_thresholds = thresholds.get("error", {})

        warning_violation = self._first_violation(comparison, warning_thresholds)
        error_violation = self._first_violation(comparison, error_thresholds)
        warning_violated = warning_violation is not None
        error_violated = error_violation is not None

        severity = self.determine_severity(current_value, warning_violated, error_violated)

        seg_label = (
            f"@{cr.dimension_value}" if cr.dimension_value else ""
        )
        message = (
            f"{metric_name}{seg_label} = {current_value} "
            f"(historical mean = {mean_past:.4f}, "
            f"deviation = {deviation_pct:+.1f}%, z-score = {z_score:+.2f}, "
            f"rate_of_change = {rate_of_change_pct:+.1f}% vs prev)"
        )
        if severity != Severity.PASS:
            # Surface the specific breached measure + bound rather than a
            # generic "exceeds error threshold" — answers "what tripped?"
            # in the alert without forcing the reader to consult the YAML.
            violation = error_violation if error_violated else warning_violation
            level = "error" if error_violated else "warning"
            measure, observed, op, bound = violation
            message += (
                f" — exceeds {level} threshold "
                f"({measure}={observed:+.4f}, {op}={bound})"
            )

        results.append(
            ValidationResult(
                validation_name=f"{self.name}.{metric_name}",
                validation_type="drift",
                severity=severity,
                message=message,
                validation_base_name=self.name,
                metric_name=metric_name,
                dimension_value=cr.dimension_value,
                expected_value={"warning": warning_thresholds, "error": error_thresholds},
                actual_value=current_value,
                details={
                    "past_values": past_values,
                    "mean_past": mean_past,
                    "stddev": stddev,
                    "deviation_pct": deviation_pct,
                    "deviation_abs": deviation_abs,
                    "z_score": z_score,
                    "rate_of_change_pct": rate_of_change_pct,
                    "rate_of_change_abs": rate_of_change_abs,
                },
                collected_at=cr.collected_at,
            )
        )
        return results

    @classmethod
    def _check_thresholds(
        cls,
        comparison: dict[str, float],
        thresholds: dict[str, float | dict[str, float]] | None,
    ) -> bool:
        """Return True if any comparison measure violates its bound.

        Boolean wrapper over :meth:`_first_violation` for callers that only
        need yes/no. See that method for the bound-form semantics.
        """
        return cls._first_violation(comparison, thresholds) is not None

    @staticmethod
    def _first_violation(
        comparison: dict[str, float],
        thresholds: dict[str, float | dict[str, float]] | None,
    ) -> tuple[str, float, str, float] | None:
        """Return the first (measure, observed_value, op, bound) tuple that
        violates, or None if nothing violates.

        Bound shape per measure (mirroring HistoricalThresholds):
        ``{"deviation_pct": {"min": -10, "max": 25}}`` → fail when
        ``measure < min`` (op ``"min"``) or ``measure > max``
        (op ``"max"``). Either bound is optional.

        Unknown measure names are silently ignored (forward-compat for adding
        new measures without breaking older validator versions).
        """
        if not thresholds:
            return None
        if hasattr(thresholds, "model_dump"):
            thresholds = thresholds.model_dump(exclude_none=True)
        for measure, bound in thresholds.items():
            if bound is None or measure not in comparison:
                continue
            value = comparison[measure]
            lo = bound.get("min")
            hi = bound.get("max")
            if lo is not None and value < float(lo):
                return measure, value, "min", float(lo)
            if hi is not None and value > float(hi):
                return measure, value, "max", float(hi)
        return None
