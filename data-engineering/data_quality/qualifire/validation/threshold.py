"""Threshold validator: compare metrics against static bounds."""

from __future__ import annotations

from typing import Any

from qualifire.core.models import CollectionResult, Severity, ValidationResult
from qualifire.validation.base import Validator


def _check_bounds(value: float, bounds: dict[str, float | None]) -> bool:
    """Return True if value VIOLATES any bound."""
    return _first_violation(value, bounds) is not None


def _first_violation(
    value: float, bounds: dict[str, float | None] | None,
) -> tuple[str, float] | None:
    """Return the (op, threshold) of the first bound the value violates,
    or None if none violated. Drives both the boolean check_bounds path
    and the human-readable message ("violates max=0.5", not the whole dict).
    """
    if not bounds:
        return None
    for op, threshold in bounds.items():
        if threshold is None:
            continue
        if op == "min" and value < threshold:
            return op, threshold
        if op == "max" and value > threshold:
            return op, threshold
        if op == "eq" and value != threshold:
            return op, threshold
        if op == "neq" and value == threshold:
            return op, threshold
        if op == "gt" and not (value > threshold):
            return op, threshold
        if op == "lt" and not (value < threshold):
            return op, threshold
        if op == "gte" and not (value >= threshold):
            return op, threshold
        if op == "lte" and not (value <= threshold):
            return op, threshold
    return None


def _format_bounds(bounds: dict[str, float | None] | None) -> str:
    """Render bounds as `key=value, key=value`, dropping None entries.

    Compact, message-friendly form for ValidationResult.message — the
    full-fidelity bound dict still lives in `expected_value` for callers
    that want every key.
    """
    if not bounds:
        return "{}"
    return ", ".join(f"{k}={v}" for k, v in bounds.items() if v is not None)


class ThresholdValidator(Validator):
    """Validates metrics against static warning/error bounds.

    Supports operators: min, max, eq, neq, gt, lt, gte, lte.

    Args:
        rules: List of rule dicts with 'metric' and 'thresholds' keys.
            thresholds has 'warning' and/or 'error' keys, each a dict of bounds.
        name: Validation name for reporting.
    """

    def __init__(
        self,
        rules: list[dict[str, Any]],
        name: str = "threshold_check",
        on_empty_data: Severity = Severity.WARNING,
    ):
        super().__init__(on_empty_data=on_empty_data)
        self.rules = rules
        self.name = name

    def validate(
        self,
        collected: list[CollectionResult],
        **kwargs: Any,
    ) -> list[ValidationResult]:
        # Group collected rows by metric name. Multi-dimension
        # collectors emit one row per (metric, dimension); we
        # validate each segment independently and emit one
        # ValidationResult per (metric, dimension) pair.
        metrics_by_name: dict[str, list[CollectionResult]] = {}
        for cr in collected:
            metrics_by_name.setdefault(cr.metric_name, []).append(cr)
        results: list[ValidationResult] = []

        for rule in self.rules:
            metric_name = rule["metric"]
            thresholds = rule["thresholds"]
            warning_bounds = thresholds.get("warning")
            error_bounds = thresholds.get("error")

            crs = metrics_by_name.get(metric_name)
            if not crs:
                results.append(
                    ValidationResult(
                        validation_name=f"{self.name}.{metric_name}",
                        validation_type="threshold",
                        severity=Severity.ERROR,
                        message=f"Metric '{metric_name}' not found in collected data",
                        validation_base_name=self.name,
                        metric_name=metric_name,
                    )
                )
                continue

            # Convert Pydantic models to dicts if needed (once per rule)
            wb = warning_bounds if isinstance(warning_bounds, dict) else (
                warning_bounds.model_dump(exclude_none=True) if warning_bounds and hasattr(warning_bounds, "model_dump") else warning_bounds
            )
            eb = error_bounds if isinstance(error_bounds, dict) else (
                error_bounds.model_dump(exclude_none=True) if error_bounds and hasattr(error_bounds, "model_dump") else error_bounds
            )

            for cr in crs:
                # Aggregations can yield NULL (divide-by-zero, all-NULL SUM,
                # AVG over an empty filter, …). float(None) crashes; emit a
                # structured result honouring the validator's on_empty_data
                # policy instead.
                if cr.metric_value is None:
                    seg_label = (
                        f"@{cr.dimension_value}"
                        if cr.dimension_value
                        else ""
                    )
                    results.append(self._empty_data_result(
                        validation_name=f"{self.name}.{metric_name}",
                        validation_type="threshold",
                        message=(
                            f"{metric_name}{seg_label} is NULL — no data, divide-by-zero, "
                            "or all-NULL aggregation. Threshold not evaluated."
                        ),
                    ))
                    continue
                value = float(cr.metric_value)
                warning_violation = _first_violation(value, wb)
                error_violation = _first_violation(value, eb)
                warning_violated = warning_violation is not None
                error_violated = error_violation is not None
                severity = self.determine_severity(value, warning_violated, error_violated)

                message = f"{metric_name} = {value}"
                if cr.dimension_value:
                    message = f"{metric_name}@{cr.dimension_value} = {value}"
                if severity != Severity.PASS:
                    # Surface the specific breached bound (e.g.
                    # "violates error: max=0.5") rather than dumping the
                    # full ThresholdBounds dict (which would include every
                    # unset op as `None` and clutter the alert).
                    violation = error_violation if error_violated else warning_violation
                    violated_level = "error" if error_violated else "warning"
                    op, bound = violation
                    message += f" (violates {violated_level} threshold: {op}={bound})"

                # Compact ``expected_value`` — drop the top-level
                # level (``warning`` / ``error``) when its bounds
                # aren't set. Mirrors the per-bound ``exclude_none``
                # the engine already applies when dumping rules; keeps
                # the system-table row + alert body free of
                # ``warning: None`` / ``error: None`` noise.
                expected_value: dict[str, Any] = {}
                if wb:
                    expected_value["warning"] = wb
                if eb:
                    expected_value["error"] = eb
                results.append(
                    ValidationResult(
                        validation_name=f"{self.name}.{metric_name}",
                        validation_type="threshold",
                        severity=severity,
                        message=message,
                        validation_base_name=self.name,
                        metric_name=metric_name,
                        dimension_value=cr.dimension_value,
                        expected_value=expected_value,
                        actual_value=value,
                        collected_at=cr.collected_at,
                    )
                )

        return results
