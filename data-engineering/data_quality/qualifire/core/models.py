"""Core data models shared across all Qualifire components."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any


def encode_dimension_value(values: dict[str, Any] | None) -> str | None:
    """Serialize a per-row dimension dict into the canonical
    persisted form.

    Returns ``None`` when ``values`` is ``None`` or empty so the
    storage layer persists the row with SQL ``NULL`` (the no-dimension
    case). Otherwise:

    * Column names (the dict keys) are **lower-cased**.
    * Values are coerced to ``str`` exactly as the column produced
      them — case preserved on the value side.
    * Keys are sorted ascending by the lower-cased name (``sort_keys=True``).
    * Compact separators so two equivalent dicts produce
      byte-identical strings.

    The canonical form drives identity equality in
    ``read_metric_history_by_partition`` (lookups compare strings)
    and in the dashboard / reporting layer (group keys compare
    strings). Two rows produced by separate runs of the same
    dimensional aggregation will have byte-identical
    ``dimension_value`` strings only if every collector follows this
    helper — which is why all four collectors call it.
    """
    if not values:
        return None
    normalized = {k.lower(): str(values[k]) for k in values}
    return json.dumps(normalized, sort_keys=True, separators=(",", ":"))


class Severity(Enum):
    """Validation result severity."""

    PASS = "PASS"
    WARNING = "WARNING"
    ERROR = "ERROR"

    def __gt__(self, other: Severity) -> bool:
        order = {Severity.PASS: 0, Severity.WARNING: 1, Severity.ERROR: 2}
        return order[self] > order[other]

    def __ge__(self, other: Severity) -> bool:
        return self == other or self > other

    def __lt__(self, other: Severity) -> bool:
        return not self >= other

    def __le__(self, other: Severity) -> bool:
        return not self > other


@dataclass
class CollectionResult:
    """Result from a data collection step.

    `partition_ts` is the resolved partition timestamp for the row — the
    value of the dataset's `partition_ts` expression evaluated against
    the source data (e.g. `event_dt`, `CAST(event_ts AS DATE)`,
    `'{{ ds }}'`, `CURRENT_TIMESTAMP`). Used as the anchor for
    partition-anchored historical reads (anchor − k·step). When the
    dataset doesn't declare a partition_ts, this stays None and the
    row carries no partition identity.
    """

    metric_name: str
    metric_value: Any
    collected_at: datetime
    metadata: dict[str, Any] = field(default_factory=dict)
    # NULL when the dataset / collector has no dimension configured.
    # The persistence layer stores SQL NULL and reads use NULL-safe
    # equality (``IS`` on SQLite, ``<=>`` on Spark, parameterized
    # ``IS NULL`` on JDBC).
    dimension_value: str | None = None
    partition_ts: datetime | None = None


@dataclass
class ValidationResult:
    """Result from a single validation check.

    `validation_name` is the full reporting name (e.g.
    ``"my_check.row_count"``); `validation_base_name` is the
    base identity (e.g. ``"my_check"``). Routing and payload
    filtering use the base; suppression uses the full
    `validation_name`. See `ValidationKey` for the key shape.

    `dimension_value` mirrors `CollectionResult.dimension_value`
    so per-segment validation results round-trip through
    persistence with their segment intact. Defaults to ``None``
    (SQL NULL) for non-dimensioned datasets.
    """

    validation_name: str
    validation_type: str
    severity: Severity
    message: str
    metric_name: str | None = None
    expected_value: Any = None
    actual_value: Any = None
    details: dict[str, Any] = field(default_factory=dict)
    collected_at: datetime | None = None
    validated_at: datetime | None = None
    # ``validation_base_name`` carries the routing-/payload-filter identity
    # (the base name resolved by the engine, e.g. ``"my_check"``). It is
    # NOT used for suppression — suppression uses the full persisted
    # ``validation_name`` (e.g. ``"my_check.row_count"``) plus
    # ``metric_name``/``dimension_value`` via ``ValidationKey``.
    #
    # Required keyword-only. Plan §P1.1 explicitly rejects an empty
    # default — a default lets missed emit sites collapse onto each
    # other under "" and silently mis-route alerts. Every constructor
    # site must name the field explicitly. PR-6 review blocking.
    validation_base_name: str = field(kw_only=True)
    # NULL when the validation has no dimension. See CollectionResult
    # for the NULL-safe read invariant.
    dimension_value: str | None = None
    # Optional human-readable form of the actual value (e.g. ISO duration
    # for SLO freshness). Persisted to `actual_value_text` column.
    # `actual_value` itself stays free-typed (numeric / dict / etc.) so
    # validators don't have to choose between numeric and string forms.
    actual_value_text: str | None = None


@dataclass
class DatasetResult:
    """Aggregated results for a single dataset.

    ``partition_ts`` carries the resolved partition stamp for this
    run — the same value the engine writes to each per-row
    ``partition_ts`` column in the system table. Populated when the
    dataset declares a literal partition expression
    (``'{{ ds }}'``, ``'2026-04-28'``, etc.); ``None`` for datasets
    that resolve partition_ts per-row (column refs, SQL functions)
    or that don't declare one at all. Notifiers render it in the
    alert header so operators see *which* partition tripped a
    check without having to drill into the system table.
    """

    dataset_name: str
    table: str | None
    validation_results: list[ValidationResult] = field(default_factory=list)
    collection_results: list[CollectionResult] = field(default_factory=list)
    run_id: str = ""
    run_timestamp: datetime | None = None
    partition_ts: datetime | None = None

    @property
    def overall_severity(self) -> Severity:
        if not self.validation_results:
            return Severity.PASS
        return max(
            (r.severity for r in self.validation_results),
            key=lambda s: {Severity.PASS: 0, Severity.WARNING: 1, Severity.ERROR: 2}[s],
        )

    @property
    def has_errors(self) -> bool:
        return any(r.severity == Severity.ERROR for r in self.validation_results)

    @property
    def has_warnings(self) -> bool:
        return any(r.severity == Severity.WARNING for r in self.validation_results)


@dataclass
class NotificationResult:
    """Result from a notification attempt."""

    channel: str
    severity: Severity
    status: str  # "sent", "failed", "skipped"
    message: str = ""
    notified_at: datetime | None = None


@dataclass
class QualifireResult:
    """Top-level result from a Qualifire run.

    `engine_warnings` carries engine-level events (persistence
    failures, suppression-read failures, WAP cleanup leaks)
    that aren't tied to any user-defined dataset. They route
    via `QualifireConfig.engine_notify` and may be persisted as
    ``record_type="validation"`` rows under
    ``dataset_name="qualifire.engine"`` so their suppression
    history works on subsequent runs.
    """

    owner: str
    bu: str
    datasets: list[DatasetResult] = field(default_factory=list)
    notifications: list[NotificationResult] = field(default_factory=list)
    engine_warnings: list[ValidationResult] = field(default_factory=list)
    run_id: str = ""

    @property
    def overall_severity(self) -> Severity:
        sources = list(self.datasets)
        order = {Severity.PASS: 0, Severity.WARNING: 1, Severity.ERROR: 2}
        ds_max = max((ds.overall_severity for ds in sources), key=lambda s: order[s], default=Severity.PASS)
        if not self.engine_warnings:
            return ds_max
        ew_max = max((vr.severity for vr in self.engine_warnings), key=lambda s: order[s])
        return ds_max if order[ds_max] >= order[ew_max] else ew_max

    @property
    def has_errors(self) -> bool:
        if any(ds.has_errors for ds in self.datasets):
            return True
        return any(vr.severity == Severity.ERROR for vr in self.engine_warnings)

    @property
    def has_warnings(self) -> bool:
        if any(ds.has_warnings for ds in self.datasets):
            return True
        return any(vr.severity == Severity.WARNING for vr in self.engine_warnings)


# --- Identity keys (P1.4) ----------------------------------------------------


@dataclass(frozen=True)
class MetricKey:
    """Identity for a `CollectionResult`: dataset / metric / segment.

    Does NOT carry validation_name — a single metric can feed
    multiple validations on the same dataset. ``dimension_value``
    is ``None`` when the dataset has no dimension; equality is
    structural so two ``None`` values match.
    """

    dataset_name: str
    metric_name: str
    dimension_value: str | None = None


@dataclass(frozen=True)
class ValidationKey:
    """Identity for a `ValidationResult`: dataset / check / metric / segment.

    `validation_name` is the **full persisted suffixed** name
    (e.g. ``"my_check.row_count"``) — not the base. Routing and
    payload filtering use `ValidationResult.validation_base_name`
    (a separate field). This split lets per-rule suppression
    work without renaming the base when a custom name is set.
    """

    dataset_name: str
    validation_name: str
    metric_name: str | None = None
    dimension_value: str | None = None


def metric_key_for(dataset_name: str, cr: CollectionResult) -> MetricKey:
    """Construct a `MetricKey` from a collection result + dataset context.

    Passes ``cr.dimension_value`` through unchanged (including ``None``
    for non-dimensioned datasets). The persistence layer stores
    ``None`` as SQL NULL and reads use NULL-safe equality.
    """
    return MetricKey(
        dataset_name=dataset_name,
        metric_name=cr.metric_name,
        dimension_value=cr.dimension_value,
    )


def validation_key_for(dataset_name: str, vr: ValidationResult) -> ValidationKey:
    """Construct a `ValidationKey` from a validation result + dataset context.

    Passes ``vr.dimension_value`` through unchanged (including ``None``).
    """
    return ValidationKey(
        dataset_name=dataset_name,
        validation_name=vr.validation_name,
        metric_name=vr.metric_name,
        dimension_value=vr.dimension_value,
    )
