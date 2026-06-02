"""``BackfillReport`` and ``PartitionDiff`` dataclasses returned by
:meth:`Qualifire.backfill`.

The report shape is a stable, public contract (plan N17). Fields
are additive — future versions may add fields but never remove or
change existing semantics. Operators and CLI consumers can rely on
the existing attribute set.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Literal

from qualifire.core.models import Severity


@dataclass(frozen=True)
class PartitionDiff:
    """One row of a backfill diff: original → backfilled per
    ``(dataset, metric, dimension, partition_ts)`` natural key.

    Status assignment (plan D8):
    - ``refreshed`` — backfill produced a different value or
      different severity than the prior active row.
    - ``unchanged`` — both equal within numeric tolerance
      (``math.isclose(rel_tol=1e-9)``).
    - ``skipped`` — explicit skip path (sample collector with no
      historical data; non-WAP dataset given ``data=True``; etc.).
      ``skip_reason`` is set.
    - ``errored`` — exception during collection or validation.
      ``error`` is set to ``repr(exc)``. Sibling partitions in the
      same backfill continue (partition-error isolation, plan N18).
    """

    dataset_name: str
    metric_name: str
    dimension_value: str | None
    partition_ts: datetime
    original_value: float | None
    backfilled_value: float | None
    severity_before: Severity | None
    severity_after: Severity | None
    status: Literal["refreshed", "unchanged", "skipped", "errored"]
    error: str | None = None
    skip_reason: str | None = None


@dataclass(frozen=True)
class BackfillReport:
    """Summary of a ``Qualifire.backfill(...)`` invocation.

    ``partitions`` carries one entry per
    ``(dataset, metric, dimension, partition_ts)`` tuple touched by
    the backfill. Aggregate counters (``refreshed``, ``unchanged``,
    ``skipped``, ``errored``) match the corresponding ``status``
    values across ``partitions`` and are exposed as fields rather
    than computed properties so JSON-serializing the report
    round-trips.
    """

    partitions: list[PartitionDiff] = field(default_factory=list)
    refreshed: int = 0
    unchanged: int = 0
    skipped: int = 0
    errored: int = 0
    # Item 1 visibility (backfill-followups-and-polish): True when
    # ``parallelism > 1`` forced ``notifiers={}`` for the inner
    # engine call. False otherwise (including parallelism=1).
    # Surfaces the silent-suppression contract so operators piping
    # through tooling can detect when alerts didn't fire.
    notifications_suppressed: bool = False

    @property
    def total(self) -> int:
        return self.refreshed + self.unchanged + self.skipped + self.errored

    @property
    def has_errors(self) -> bool:
        return self.errored > 0

    def to_dict(self) -> dict:
        """JSON-serializable representation suitable for ``--json``
        CLI output. Datetime + enum values are rendered as strings.
        """
        return {
            "partitions": [
                {
                    "dataset_name": p.dataset_name,
                    "metric_name": p.metric_name,
                    "dimension_value": p.dimension_value,
                    "partition_ts": p.partition_ts.isoformat(),
                    "original_value": p.original_value,
                    "backfilled_value": p.backfilled_value,
                    "severity_before": (
                        p.severity_before.value if p.severity_before else None
                    ),
                    "severity_after": (
                        p.severity_after.value if p.severity_after else None
                    ),
                    "status": p.status,
                    "error": p.error,
                    "skip_reason": p.skip_reason,
                }
                for p in self.partitions
            ],
            "refreshed": self.refreshed,
            "unchanged": self.unchanged,
            "skipped": self.skipped,
            "errored": self.errored,
            "total": self.total,
            "has_errors": self.has_errors,
            "notifications_suppressed": self.notifications_suppressed,
        }
