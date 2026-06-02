"""Selector grammar for scoping qualifire runs to specific
validations / metrics.

Grammar
-------
A selector is a colon-separated triple where each segment is either a
literal name or the wildcard ``*``::

    <dataset>:<validation>[:<metric>]

The third segment is optional; when omitted it means "all metrics
produced by the matched validation."

Examples::

    sales:*                  # every validation on dataset `sales`
    *:row_count_check        # `row_count_check` across every dataset
    sales:row_count_check    # one validation
    sales:*:row_count        # all validations on `sales` producing
                             # metric `row_count`
    *:*:revenue              # every validation in the run producing
                             # metric `revenue`

Multiple selectors are comma-separated; each is parsed and resolved
independently and the results are concatenated (deduplicated by the
``(dataset.name, validation.id)`` tuple).

Resolution scope
----------------
Selectors resolve against the parsed ``QualifireConfig`` only — they
do NOT query the system table. A selector that matches no validation
in the config raises :class:`qualifire.core.exceptions.QualifireConfigError`
("selector matched no scope: ..."). No silent no-op.

Name-character contract
-----------------------
Dataset and validation identifiers are constrained by
``_SAFE_NAME_RE = ^[A-Za-z0-9._-]+$`` (already enforced at config-load
time on ``DatasetConfig.name``). Colons are not legal identifier
characters, so the selector ``:`` separator is unambiguously parseable.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from qualifire.core.config import DatasetConfig, QualifireConfig
from qualifire.core.exceptions import QualifireConfigError

_WILDCARD = "*"


@dataclass(frozen=True)
class SelectorPart:
    """One parsed selector triple.

    Each field is either a literal identifier or :data:`_WILDCARD`
    (``*``). ``metric`` is ``None`` when the selector omitted the
    third segment entirely.
    """

    dataset: str
    validation: str
    metric: str | None


@dataclass(frozen=True)
class ScopedDataset:
    """A dataset paired with the validations selected from it.

    ``metric_filter`` is ``None`` when no metric segment was supplied
    (= "all metrics") and otherwise a set of metric names the
    consumer should restrict to.
    """

    dataset: DatasetConfig
    validations: list[Any]
    metric_filter: set[str] | None


def parse_selector(s: str) -> list[SelectorPart]:
    """Parse a selector string into one or more :class:`SelectorPart`.

    Returns one ``SelectorPart`` per comma-separated entry. Each entry
    is whitespace-stripped before parsing. Raises
    :class:`QualifireConfigError` on malformed input.
    """
    if s is None or not str(s).strip():
        raise QualifireConfigError(
            "selector cannot be empty; expected '<dataset>:<validation>[:<metric>]'"
        )
    parts: list[SelectorPart] = []
    for raw in s.split(","):
        entry = raw.strip()
        if not entry:
            raise QualifireConfigError(
                f"selector entry cannot be empty (in {s!r})"
            )
        segments = entry.split(":")
        if len(segments) < 2:
            raise QualifireConfigError(
                f"selector {entry!r} is missing the validation segment; "
                f"expected '<dataset>:<validation>[:<metric>]'"
            )
        if len(segments) > 3:
            raise QualifireConfigError(
                f"selector {entry!r} has too many segments; "
                f"expected '<dataset>:<validation>[:<metric>]'"
            )
        dataset_seg, validation_seg, *rest = segments
        if not dataset_seg:
            raise QualifireConfigError(
                f"selector {entry!r} dataset segment may not be empty"
            )
        if not validation_seg:
            raise QualifireConfigError(
                f"selector {entry!r} validation segment may not be empty"
            )
        metric_seg: str | None = None
        if rest:
            (metric_seg,) = rest
            if not metric_seg:
                raise QualifireConfigError(
                    f"selector {entry!r} metric segment may not be empty "
                    f"(omit the trailing ':' to match all metrics)"
                )
        parts.append(SelectorPart(
            dataset=dataset_seg,
            validation=validation_seg,
            metric=metric_seg,
        ))
    return parts


def _validation_id(val: Any) -> str:
    """Stable identifier for a validation config.

    Validators expose ``name`` (operator-supplied) or fall back to
    ``type``. This pairs with ``dataset.name`` for the dedup key.
    """
    name = getattr(val, "name", None)
    return name or getattr(val, "type", str(id(val)))


def _matches(literal_or_star: str, candidate: str) -> bool:
    return literal_or_star == _WILDCARD or literal_or_star == candidate


def resolve_selector(
    parts: list[SelectorPart],
    config: QualifireConfig,
) -> list[ScopedDataset]:
    """Resolve selector parts against a parsed config.

    Returns one :class:`ScopedDataset` per dataset that matched at
    least one selector entry, with the matched validations collected
    and the metric filter narrowed if any entry supplied a metric.

    Raises :class:`QualifireConfigError` when zero scopes match.
    Multiple selector parts that overlap (e.g. ``sales:*``,
    ``sales:row_count``) deduplicate by ``(dataset.name, validation_id)``.
    """
    if not parts:
        raise QualifireConfigError(
            "selector parts list is empty; nothing to resolve"
        )

    # Per-dataset accumulator. Order-preserving (insertion order
    # follows config.datasets) so two operators with identical configs
    # see identical resolved-scope ordering.
    by_dataset: dict[str, dict[str, Any]] = {}

    for part in parts:
        for ds in config.datasets:
            ds_name = ds.name or ""
            if not _matches(part.dataset, ds_name):
                continue
            for val in ds.validations:
                if not _matches(part.validation, _validation_id(val)):
                    continue
                bucket = by_dataset.setdefault(ds_name, {
                    "dataset": ds,
                    "validations": {},  # id -> validation
                    "metric_filters": [],
                })
                bucket["validations"][_validation_id(val)] = val
                bucket["metric_filters"].append(part.metric)

    if not by_dataset:
        raise QualifireConfigError(
            f"selector matched no scope: "
            f"{','.join(f'{p.dataset}:{p.validation}' + (f':{p.metric}' if p.metric else '') for p in parts)}"
        )

    scopes: list[ScopedDataset] = []
    for entry in by_dataset.values():
        # Combine metric filters: if any selector for this dataset
        # had no metric segment OR a wildcard ``*``, the final filter
        # is "all metrics" (None). Otherwise the filter is the union
        # of literal metric names.
        filters = entry["metric_filters"]
        if any(m is None or m == _WILDCARD for m in filters):
            metric_filter: set[str] | None = None
        else:
            metric_filter = {m for m in filters if m is not None}
        scopes.append(ScopedDataset(
            dataset=entry["dataset"],
            validations=list(entry["validations"].values()),
            metric_filter=metric_filter,
        ))
    return scopes
