"""Abstract notifier, plugin registry, and alert deduplication."""

from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from typing import Any

from qualifire.core.models import DatasetResult, NotificationResult, Severity

logger = logging.getLogger(__name__)

# Global notifier registry
_REGISTRY: dict[str, type[Notifier]] = {}


class Notifier(ABC):
    """Base class for notification channels."""

    @abstractmethod
    def send(
        self,
        dataset_result: DatasetResult,
        severity: Severity,
        owner: str,
        bu: str,
        datasets: list[DatasetResult] | None = None,
    ) -> NotificationResult:
        """Send a notification and return the result.

        Args:
            dataset_result: Primary dataset result (or synthetic summary for groups).
            severity: The severity level being notified.
            owner: Team/owner name.
            bu: Business unit.
            datasets: Original dataset list for grouped notifications.
                When provided with >1 entry, use grouped formatting.
        """
        ...

    @property
    @abstractmethod
    def channel_name(self) -> str:
        """Return the channel identifier (e.g., 'email', 'slack')."""
        ...

    def _format_message(
        self,
        dataset_result: DatasetResult,
        severity: Severity,
        owner: str,
        bu: str,
        datasets: list[DatasetResult] | None = None,
    ) -> str:
        """Format notification text, using grouped format when multiple datasets are present."""
        if datasets and len(datasets) > 1:
            run_id = datasets[0].run_id if datasets else ""
            return format_grouped_notification_message(
                datasets, severity, owner, bu, run_id=run_id
            )
        return format_notification_message(dataset_result, severity, owner, bu)


def register_notifier(name: str, cls: type[Notifier]) -> None:
    """Register a notifier class in the global registry."""
    _REGISTRY[name] = cls


def get_notifier(name: str) -> type[Notifier]:
    """Get a notifier class by name."""
    if name not in _REGISTRY:
        available = ", ".join(_REGISTRY.keys())
        raise KeyError(f"Unknown notifier '{name}'. Available: {available}")
    return _REGISTRY[name]


def get_registered_notifiers() -> dict[str, type[Notifier]]:
    """Return a copy of the notifier registry."""
    return dict(_REGISTRY)


def should_suppress(
    storage: Any,
    dataset_name: str,
    validation_name: str,
    current_severity: Severity,
) -> bool:
    """Check if an alert should be suppressed (deduplication).

    Suppresses if the most recent historical result for the same
    dataset + validation has the same severity.
    """
    if storage is None:
        return False
    try:
        history = storage.read_validation_history(
            dataset_name=dataset_name,
            validation_name=validation_name,
            limit=1,
        )
        if history:
            last_status = history[0].get("validation_status", "")
            return last_status == current_severity.value
    except Exception as e:
        logger.warning("Alert deduplication check failed: %s", e)
    return False


def _is_qualifire_internal_failure_row(vr: Any) -> bool:
    """Predicate used by message-builder filters to skip
    qualifire-internal failure rows (persistence-infrastructure
    outages and validator-execution exceptions).

    Mirrors ``qualifire.core.engine._is_qualifire_internal_failure``
    — same shape, same dict/string-payload tolerance — but kept
    in this module to avoid a circular-import between
    ``notification.base`` and ``core.engine``. Future contributors:
    if you change the marker shape, update both predicates
    (search for ``qualifire_internal_failure`` substring).
    """
    import json as _json
    details = getattr(vr, "details", None)
    if details is None:
        return False
    if isinstance(details, str):
        try:
            details = _json.loads(details)
        except (_json.JSONDecodeError, ValueError):
            return False
    if not isinstance(details, dict):
        return False
    return bool(details.get("qualifire_internal_failure"))


def format_notification_message(
    dataset_result: DatasetResult,
    severity: Severity,
    owner: str,
    bu: str,
) -> str:
    """Format a notification message for a single dataset.

    Skips rows tagged with ``details['qualifire_internal_failure']
    = True`` so persistence-infra failures and validator-execution
    exceptions don't appear in operator-facing alert bodies. Those
    rows are persisted for forensics and the run still raises;
    paging operators about library bugs is the wrong contract.
    """
    # ``Partition:`` slots between ``Table`` and ``Owner`` so
    # operators see *which* partition tripped the check at the
    # top of the message (the body's per-validation lines may
    # reference the same partition implicitly via metric values,
    # but the explicit header avoids guesswork). Omitted when the
    # dataset declares no partition_ts or uses a per-row
    # expression (column ref, SQL function) that doesn't render
    # to a literal — the notifier never emits an empty line.
    lines = [
        f"Qualifire Alert — {severity.value}",
        f"Dataset: {dataset_result.dataset_name}",
        f"Table: {dataset_result.table or 'N/A'}",
    ]
    if dataset_result.partition_ts is not None:
        lines.append(f"Partition: {dataset_result.partition_ts.isoformat()}")
    lines += [
        f"Owner: {owner} | BU: {bu}",
        f"Run: {dataset_result.run_id}",
        "",
        "Validation Results:",
    ]
    for vr in dataset_result.validation_results:
        if _is_qualifire_internal_failure_row(vr):
            continue
        if severity == Severity.PASS or vr.severity >= severity:
            lines.append(f"  [{vr.severity.value}] {vr.validation_name}: {vr.message}")
            lines.extend(format_validation_details(vr, indent="    "))

    return "\n".join(lines)


def format_grouped_notification_message(
    datasets: list[DatasetResult],
    severity: Severity,
    owner: str,
    bu: str,
    run_id: str = "",
) -> str:
    """Format a grouped notification message across multiple datasets.

    Same qualifire-internal-failure filter as the single-dataset
    formatter — see ``format_notification_message``.
    """
    if len(datasets) == 1:
        return format_notification_message(datasets[0], severity, owner, bu)

    label = "Success" if severity == Severity.PASS else severity.value
    lines = [
        f"Qualifire Alert — {label}",
        f"Owner: {owner} | BU: {bu}",
        f"Run: {run_id}",
        f"Datasets: {len(datasets)}",
        "",
    ]
    for ds in datasets:
        # Per-dataset partition stamp inline with the section
        # header — across-dataset groupings can mix partitions
        # (different datasets, different run anchors) so the
        # top-level header doesn't carry a single value; the
        # per-section stamp keeps each dataset's partition
        # identity intact in one alert.
        header = f"--- {ds.dataset_name} ({ds.table or 'N/A'})"
        if ds.partition_ts is not None:
            header += f" @ {ds.partition_ts.isoformat()}"
        header += " ---"
        lines.append(header)
        matching = [
            vr for vr in ds.validation_results
            if not _is_qualifire_internal_failure_row(vr)
            and (severity == Severity.PASS or vr.severity >= severity)
        ]
        if matching:
            for vr in matching:
                lines.append(f"  [{vr.severity.value}] {vr.validation_name}: {vr.message}")
                lines.extend(format_validation_details(vr, indent="    "))
        else:
            lines.append(f"  All validations passed")
        lines.append("")

    return "\n".join(lines)


def _compact_threshold(expected: Any) -> str | None:
    """Render a threshold ``expected_value`` for the alert body
    without ``warning: None`` / ``error: None`` noise.

    The engine prunes unset operator fields inside each level via
    ``model_dump(exclude_none=True)``. This helper drops the
    redundant outer-level None entries so the rendered string
    shows only what the operator configured. Returns ``None`` if
    nothing meaningful is left (caller skips the line).
    """
    if expected is None:
        return None
    if not isinstance(expected, dict):
        return str(expected)
    pruned = {k: v for k, v in expected.items() if v is not None}
    if not pruned:
        return None
    return str(pruned)


def format_validation_details(vr: Any, indent: str = "  ") -> list[str]:
    """Render the per-validator diagnostic details as text lines.

    The headline ``[severity] name: message`` summary is fine for a
    quick scan, but operators need the underlying numbers /
    contributing features to act. This helper extracts the most
    actionable fields out of ``vr.details`` for each known
    validation_type and returns indented bullet lines suitable for
    Slack / email bodies. Unknown types fall back to the actual /
    expected pair.

    Empty list when there's nothing useful to add — the caller
    extends the line list directly without checking length.

    Bad-data resilience: ``vr.details`` should be a mapping but
    third-party validators or corrupt persistence might present a
    non-mapping value; nested SHAP lists may carry malformed items.
    Every defensive guard here is "skip this line" rather than
    "raise" — a dropped diagnostic is recoverable, an aborted
    notification body during an incident is not.
    """
    from collections.abc import Mapping

    raw_details = vr.details
    details: Mapping = raw_details if isinstance(raw_details, Mapping) else {}
    out: list[str] = []
    vtype = vr.validation_type

    # Dimension and partition stamps apply to every type when present.
    if vr.dimension_value:
        out.append(f"{indent}- dimension: {vr.dimension_value}")
    pts = details.get("partition_ts")
    if pts:
        out.append(f"{indent}- partition_ts: {pts}")

    if vtype in ("shape", "pattern"):
        # SHAP top contributors. Show all of them — the validator
        # already caps at 5; truncating further hides signal.
        feats = details.get("top_contributing_features") or []
        if not isinstance(feats, list):
            feats = []
        # Drift explainer entries are parallel-list with feats. Cap
        # inline summaries at top-3 to keep the body readable
        # (plan: "Size budgets — Notification body caps"). The
        # 240-char per-line cap applies to every appended drift
        # body line — feature bullet, summary arrow, error,
        # truncation marker — so the cap holds even if a future
        # validator slips a long feature name or stack trace into
        # the dict.
        drift = details.get("value_drift_explainer") or []
        if not isinstance(drift, list):
            drift = []
        _DRIFT_INLINE_CAP = 3
        _DRIFT_LINE_MAX = 240

        def _capline(s: str) -> str:
            return s if len(s) <= _DRIFT_LINE_MAX else s[: _DRIFT_LINE_MAX - 3] + "..."

        if feats:
            out.append(f"{indent}- top contributing features:")
            for i, f in enumerate(feats):
                if not isinstance(f, Mapping):
                    out.append(_capline(f"{indent}    • {f}"))
                    continue
                imp = f.get("importance")
                imp_str = f"{imp:.4f}" if isinstance(imp, (int, float)) else str(imp)
                out.append(_capline(f"{indent}    • {f.get('feature')} ({imp_str})"))
                if i < _DRIFT_INLINE_CAP and i < len(drift):
                    entry = drift[i]
                    if isinstance(entry, Mapping):
                        summary = entry.get("summary") or ""
                        if isinstance(summary, str) and summary:
                            out.append(_capline(f"{indent}      → {summary}"))
            if details.get("value_drift_explainer_truncated"):
                out.append(_capline(
                    f"{indent}- value drift payload truncated"
                    " (full set in details_json)"
                ))
            if "value_drift_explainer_error" in details:
                out.append(_capline(
                    f"{indent}- value drift unavailable: "
                    f"{details['value_drift_explainer_error']}"
                ))
        elif "explanation_error" in details:
            out.append(f"{indent}- explanation unavailable: {details['explanation_error']}")
        # Pattern adds AUC; shape adds anomaly_ratio.
        if vtype == "pattern":
            auc = details.get("auc")
            auc_std = details.get("auc_std")
            if auc is not None:
                std_str = f" ± {auc_std:.3f}" if isinstance(auc_std, (int, float)) else ""
                out.append(f"{indent}- AUC: {auc:.3f}{std_str}")
            if details.get("n_current") is not None:
                out.append(
                    f"{indent}- sample sizes: current={details.get('n_current')}, "
                    f"past={details.get('n_past')}"
                )
        else:  # shape
            ratio = details.get("anomaly_ratio")
            if ratio is not None:
                out.append(f"{indent}- anomaly ratio: {ratio:.3f}")
    elif vtype == "drift":
        # Historical comparison signs: deviation, z-score, rate-of-change.
        cur = details.get("current_value", vr.actual_value)
        mean = details.get("mean_past")
        if cur is not None and mean is not None:
            out.append(f"{indent}- current: {cur} | past mean: {mean}")
        for k in ("deviation_pct", "deviation_abs", "z_score",
                  "rate_of_change_pct", "rate_of_change_abs"):
            v = details.get(k)
            if v is not None:
                out.append(f"{indent}- {k}: {v}")
    elif vtype == "trend":
        # Forecast: prediction band vs observed.
        yhat = details.get("yhat")
        lower = details.get("yhat_lower")
        upper = details.get("yhat_upper")
        if yhat is not None:
            out.append(f"{indent}- observed: {vr.actual_value} | predicted: {yhat}")
        if lower is not None and upper is not None:
            out.append(f"{indent}- prediction interval: [{lower}, {upper}]")
    elif vtype == "threshold":
        if vr.actual_value is not None:
            out.append(f"{indent}- actual: {vr.actual_value}")
        # ``vr.message`` already names the specific bound that
        # tripped (e.g. ``violates error threshold: max=0.5``); the
        # ``- threshold: …`` line is the configured-bounds context.
        # Strip top-level None branches so ``warning: None`` doesn't
        # show up when only ``error`` was configured. The
        # validator-side ``exclude_none=True`` already prunes unset
        # operator fields inside each level.
        threshold_disp = _compact_threshold(vr.expected_value)
        if threshold_disp:
            out.append(f"{indent}- threshold: {threshold_disp}")
    elif vtype == "slo":
        # SLO: age vs target.
        age = details.get("age") or details.get("freshness_age")
        target = details.get("threshold")
        if target is None:
            target = vr.expected_value
        if age is not None:
            out.append(f"{indent}- data age: {age}")
        if target is not None:
            out.append(f"{indent}- threshold: {target}")
    else:
        # Fallback for unknown / future validators.
        if vr.actual_value is not None:
            out.append(f"{indent}- actual: {vr.actual_value}")
        if vr.expected_value is not None:
            out.append(f"{indent}- expected: {vr.expected_value}")

    return out
