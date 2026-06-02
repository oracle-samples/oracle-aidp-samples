"""Health report generation from system table data."""

from __future__ import annotations

import json
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any


def _row_is_internal_failure(row: dict[str, Any]) -> bool:
    """Recognise rows tagged
    ``details_json['qualifire_internal_failure'] = True`` —
    persistence-infrastructure failures and validator-execution
    exceptions that the engine surfaces as severity=ERROR but
    that operationally are qualifire bugs / outages, not data-
    quality findings.

    Tolerates both shapes the system table exposes:
    * dict (in-memory engine path)
    * JSON string (re-read from the persisted TEXT column)

    Dual-shape support so the dashboard never misclassifies a
    persisted internal-failure row as a normal ERROR.

    **Sibling predicates (keep in lockstep on shape changes):**
    * ``qualifire.core.engine._is_qualifire_internal_failure`` —
      operates on ``ValidationResult.details`` (in-memory dict
      OR JSON string); used by the engine + dispatcher.
    * ``qualifire.notification.base._is_qualifire_internal_failure_row`` —
      same predicate, separated to break a circular-import
      between ``notification.base`` and ``core.engine``.

    All three share the marker shape
    ``details['qualifire_internal_failure'] is truthy``. Future
    contributors changing the marker key MUST update all three
    (grep for ``qualifire_internal_failure`` substring).
    """
    details = row.get("details_json")
    if details is None:
        return False
    if isinstance(details, str):
        try:
            details = json.loads(details)
        except (json.JSONDecodeError, ValueError):
            return False
    if not isinstance(details, dict):
        return False
    return bool(details.get("qualifire_internal_failure"))


@dataclass
class HealthReport:
    """Aggregated data quality health metrics.

    ``internal_error_count`` separates qualifire-internal failures
    (persistence-infrastructure outages, validator-execution
    exceptions) from genuine data-quality ERRORs. Operators reading
    the dashboard at a glance can tell whether an ERROR spike is a
    real finding (paged-on) or a library bug (suppressed). Both
    counts contribute to ``error_count`` / ``error_rate`` for
    backward compatibility — pre-existing dashboards that don't
    know about the distinction still render unchanged.
    """

    total_checks: int = 0
    pass_count: int = 0
    warning_count: int = 0
    error_count: int = 0
    internal_error_count: int = 0   # subset of error_count
    pass_rate: float = 0.0
    warning_rate: float = 0.0
    error_rate: float = 0.0
    by_dataset: list[dict[str, Any]] = field(default_factory=list)
    by_check_type: list[dict[str, Any]] = field(default_factory=list)
    trend: list[dict[str, Any]] = field(default_factory=list)
    worst_offenders: list[dict[str, Any]] = field(default_factory=list)
    days: int = 30


class HealthReporter:
    """Generates health reports from system table data.

    Args:
        storage: A SystemTableStorage instance with ``read_health_data()``.
    """

    def __init__(self, storage: Any):
        self.storage = storage

    def generate(self, days: int = 30) -> HealthReport:
        """Query storage and compute health metrics."""
        rows = self.storage.read_health_data(days=days)
        report = HealthReport(days=days)

        if not rows:
            return report

        # --- Summary ---
        report.total_checks = len(rows)
        for r in rows:
            status = (r.get("validation_status") or "").upper()
            if status == "PASS":
                report.pass_count += 1
            elif status == "WARNING":
                report.warning_count += 1
            elif status == "ERROR":
                report.error_count += 1
                # Subset count: qualifire-internal failures are
                # still ERRORs but operationally distinct from
                # data-quality findings. Phase 2 of external-catalog-
                # system-table-hardening: surface a separate counter
                # so dashboards can split the two visually.
                if _row_is_internal_failure(r):
                    report.internal_error_count += 1

        total = report.total_checks or 1
        report.pass_rate = round(report.pass_count / total * 100, 1)
        report.warning_rate = round(report.warning_count / total * 100, 1)
        report.error_rate = round(report.error_count / total * 100, 1)

        # --- By dataset ---
        ds_counts: dict[str, dict[str, int]] = defaultdict(lambda: {"pass": 0, "warning": 0, "error": 0, "total": 0})
        for r in rows:
            ds = r.get("dataset_name") or "unknown"
            status = (r.get("validation_status") or "").lower()
            ds_counts[ds]["total"] += 1
            if status in ds_counts[ds]:
                ds_counts[ds][status] += 1

        report.by_dataset = sorted(
            [{"dataset": ds, **counts} for ds, counts in ds_counts.items()],
            key=lambda x: x["error"],
            reverse=True,
        )

        # --- Worst offenders (top 10 by error count) ---
        report.worst_offenders = [d for d in report.by_dataset if d["error"] > 0][:10]

        # --- By check type ---
        type_counts: dict[str, dict[str, int]] = defaultdict(lambda: {"pass": 0, "warning": 0, "error": 0, "total": 0})
        for r in rows:
            vtype = r.get("validation_type") or "unknown"
            status = (r.get("validation_status") or "").lower()
            type_counts[vtype]["total"] += 1
            if status in type_counts[vtype]:
                type_counts[vtype][status] += 1

        report.by_check_type = [
            {
                "type": vtype,
                **counts,
                "pass_rate": round(counts["pass"] / max(counts["total"], 1) * 100, 1),
            }
            for vtype, counts in sorted(type_counts.items())
        ]

        # --- Trend (daily pass rate) ---
        daily: dict[str, dict[str, int]] = defaultdict(lambda: {"pass": 0, "total": 0})
        for r in rows:
            ts = r.get("run_timestamp") or ""
            day = str(ts)[:10]  # YYYY-MM-DD
            if not day:
                continue
            daily[day]["total"] += 1
            if (r.get("validation_status") or "").upper() == "PASS":
                daily[day]["pass"] += 1

        report.trend = [
            {
                "date": day,
                "pass_rate": round(counts["pass"] / max(counts["total"], 1) * 100, 1),
                "total": counts["total"],
            }
            for day, counts in sorted(daily.items())
        ]

        return report
