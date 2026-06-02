"""Abstract base validator with severity determination."""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any

from qualifire.core.models import CollectionResult, Severity, ValidationResult


class Validator(ABC):
    """Base class for all validators.

    Subclasses implement `validate()` to check collected data and return
    ValidationResult(s) with appropriate severity.

    Args:
        on_empty_data: Severity to use when there is insufficient data to
            validate. Defaults to WARNING.
    """

    def __init__(self, *, on_empty_data: Severity = Severity.WARNING) -> None:
        self.on_empty_data = on_empty_data

    def _empty_data_result(
        self,
        validation_name: str,
        validation_type: str,
        message: str,
        validation_base_name: str | None = None,
    ) -> ValidationResult:
        """Build a ValidationResult for insufficient/empty data using on_empty_data severity.

        ``validation_base_name`` defaults to the validator's
        ``self.name`` when present (the typical case). Tests that
        construct validators without `name=` may pass it
        explicitly.
        """
        if validation_base_name is None:
            validation_base_name = getattr(self, "name", validation_name)
        return ValidationResult(
            validation_name=validation_name,
            validation_type=validation_type,
            severity=self.on_empty_data,
            message=message,
            validation_base_name=validation_base_name,
        )

    def _missing_partition_ts_result(
        self,
        *,
        metric_name: str,
        cr: CollectionResult,
        current_value: float | None,
        reason: str,
        validation_type: str,
    ) -> ValidationResult:
        """Structured ERROR for history-backed validators (drift, trend)
        when partition-anchored history isn't available.

        run_timestamp ordering is no longer used for history reads —
        it's the wall-clock when the validation executed, not the
        logical time the metric describes, so it produces a degenerate
        time axis under any backfill / replay / notebook re-run flow.
        Both `cr.partition_ts` (set on DatasetConfig.partition_ts) and
        the rule's step (e.g. compare.step / model.step) are required.
        Refusing to fit on garbage is better than silent nonsense.
        """
        seg_label = f"@{cr.dimension_value}" if cr.dimension_value else ""
        return ValidationResult(
            validation_name=f"{getattr(self, 'name', validation_type)}.{metric_name}",
            validation_type=validation_type,
            severity=Severity.ERROR,
            message=(
                f"{metric_name}{seg_label}: {validation_type} requires "
                f"partition-anchored history. {reason}."
            ),
            validation_base_name=getattr(self, "name", validation_type),
            metric_name=metric_name,
            dimension_value=cr.dimension_value,
            actual_value=current_value,
            details={"missing_partition_anchor": True, "reason": reason},
        )

    @abstractmethod
    def validate(
        self,
        collected: list[CollectionResult],
        **kwargs: Any,
    ) -> list[ValidationResult]:
        """Run validation logic on collected data."""
        ...

    @staticmethod
    def determine_severity(
        value: float,
        warning_check: bool,
        error_check: bool,
    ) -> Severity:
        """Determine severity based on warning/error threshold checks.

        Args:
            value: The actual metric value (for reporting).
            warning_check: True if the value VIOLATES the warning threshold.
            error_check: True if the value VIOLATES the error threshold.
        """
        if error_check:
            return Severity.ERROR
        if warning_check:
            return Severity.WARNING
        return Severity.PASS
