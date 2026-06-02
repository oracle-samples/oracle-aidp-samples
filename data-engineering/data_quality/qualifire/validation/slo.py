"""SLO validator: freshness checks against duration thresholds."""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any

from qualifire.core.duration import format_duration_iso, parse_duration
from qualifire.core.models import CollectionResult, Severity, ValidationResult
from qualifire.validation.base import Validator


class SLOValidator(Validator):
    """Validates data freshness against warning/error duration thresholds.

    Computes delta = now - recency_timestamp, then compares against
    configured ISO 8601 durations (e.g., warning: ``"PT4H"``, error:
    ``"PT8H"``).

    Args:
        warning_duration: Duration string for warning threshold.
        error_duration: Duration string for error threshold.
        name: Validation name for reporting.
    """

    def __init__(
        self,
        warning_duration: str | timedelta | None = None,
        error_duration: str | timedelta | None = None,
        name: str = "slo_check",
        on_empty_data: Severity = Severity.WARNING,
    ):
        super().__init__(on_empty_data=on_empty_data)
        self.warning_td = parse_duration(warning_duration) if warning_duration else None
        self.error_td = parse_duration(error_duration) if error_duration else None
        self.name = name

    def validate(
        self,
        collected: list[CollectionResult],
        **kwargs: Any,
    ) -> list[ValidationResult]:
        results: list[ValidationResult] = []

        for cr in collected:
            if cr.metric_name != "recency":
                continue

            recency_ts = cr.metric_value
            if isinstance(recency_ts, str):
                recency_ts = datetime.fromisoformat(recency_ts)

            now = datetime.now()
            delta = now - recency_ts

            # Check thresholds
            warning_violated = self.warning_td is not None and delta > self.warning_td
            error_violated = self.error_td is not None and delta > self.error_td

            severity = self.determine_severity(
                delta.total_seconds(), warning_violated, error_violated
            )

            expected = []
            if self.warning_td:
                expected.append(f"warning < {format_duration_iso(self.warning_td)}")
            if self.error_td:
                expected.append(f"error < {format_duration_iso(self.error_td)}")

            message = (
                f"Data freshness: {format_duration_iso(delta)} since last update "
                f"(threshold: {', '.join(expected)})"
            )

            # Freshness is best surfaced two ways:
            #   - numeric seconds in `actual_value` → engine writes it to
            #     `metric_value` (charts, SQL filters, math).
            #   - ISO-8601 duration string ("P1DT2H30M") in
            #     `actual_value_text` → engine writes it to `actual_value_text`
            #     so dashboards can render the human form without re-deriving
            #     it from seconds.
            delta_seconds = float(delta.total_seconds())
            iso_delta = format_duration_iso(delta)
            results.append(
                ValidationResult(
                    validation_name=self.name,
                    validation_type="slo",
                    severity=severity,
                    message=message,
                    validation_base_name=self.name,
                    metric_name="recency",
                    dimension_value=cr.dimension_value,
                    expected_value={
                        "warning": format_duration_iso(self.warning_td) if self.warning_td else None,
                        "error": format_duration_iso(self.error_td) if self.error_td else None,
                    },
                    actual_value=delta_seconds,
                    actual_value_text=iso_delta,
                    details={
                        "recency_timestamp": recency_ts.isoformat(),
                        "delta_seconds": delta_seconds,
                        "delta_iso": iso_delta,
                    },
                    collected_at=cr.collected_at,
                )
            )

        return results
