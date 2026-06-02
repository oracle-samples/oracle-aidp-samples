"""Backfill loop driver — per-partition source selection, soft-delete
coupling, and ``BackfillReport`` construction.

Ships as a separate module so the engine's forward-execution path
(``QualifireEngine.run``) stays untouched. The backfill driver
constructs partition-specific ``DatasetConfig`` clones that pin the
forward engine to the past partition's data source — for WAP
datasets that's ``wap.target_table WHERE partition_column = P``;
for non-WAP it's ``dataset.table`` filtered to the partition value.

Per-partition isolation: an exception during one partition's
collection / validation does not abort sibling partitions in the
same backfill (plan N18). The report records the error; after all
partitions complete, ``Qualifire.backfill`` raises
``QualifireValidationError`` with the report attached when
``report.has_errors`` is true.
"""

from __future__ import annotations

import dataclasses
import logging
import math
import re
from datetime import datetime, timedelta
from typing import Any

# Stricter than `_SAFE_NAME_RE` in core/config.py (which permits
# leading digits, dots, and hyphens for identifiers like partition
# columns). Auto-detect interpolates the result *unquoted* into SQL
# filter clauses, so we restrict to bare unqualified identifiers only.
_BARE_IDENT_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")

from qualifire.core.backfill_report import BackfillReport, PartitionDiff
from qualifire.core.config import DatasetConfig, QualifireConfig
from qualifire.core.context import QualifireContext
from qualifire.core.exceptions import QualifireConfigError
from qualifire.core.models import (
    DatasetResult,
    QualifireResult,
    Severity,
    ValidationResult,
)
from qualifire.core.selectors import (
    ScopedDataset,
    parse_selector,
    resolve_selector,
)

logger = logging.getLogger(__name__)


def _resolve_partition_anchors(
    dataset: DatasetConfig, run_config: QualifireConfig,
) -> tuple[str | None, str | None]:
    """Resolve the effective ``(partition_column, partition_ts_expr)``
    pair used by the backfill source-select preflight and the
    partition-pinned dataset builder.

    Single source of truth (Item 2 + missed-HIGH #7) — every
    consumer that previously read ``ds.wap.partition_column`` or
    ``ds.partition_ts`` directly now goes through this helper so
    run-level inheritance (`QualifireConfig.partition_ts`) is
    honoured uniformly.

    Returns:
        (effective_partition_column, effective_partition_ts_expr).

        ``effective_partition_column``: explicit
        ``wap.partition_column`` if set; else, on a WAP dataset,
        the result of auto-detect from the effective
        ``partition_ts`` when it matches ``_BARE_IDENT_RE``; else
        ``None``. Always ``None`` for non-WAP datasets.

        ``effective_partition_ts_expr``: the run-level-inheritance
        result of :func:`effective_partition_ts`. May be a column
        reference (matches ``_BARE_IDENT_RE``), a Jinja constant,
        or any other SQL expression; the caller decides what's
        safe to interpolate.

    Does NOT mutate the user's ``DatasetConfig`` / ``WAPConfig``
    instance (HS7).
    """
    from qualifire.core.config import effective_partition_ts

    pt_expr = effective_partition_ts(dataset, run_config)

    if dataset.wap is not None:
        # WAP path: explicit wins.
        if dataset.wap.partition_column:
            return dataset.wap.partition_column, pt_expr
        # Auto-detect from the effective partition_ts when it's a
        # bare identifier. Stricter than the existing
        # `_SAFE_NAME_RE` to avoid leading-digit / hyphen / dotted
        # forms that interpolate to invalid or mis-targeted SQL.
        if pt_expr and _BARE_IDENT_RE.match(pt_expr.strip()):
            return pt_expr.strip(), pt_expr
        return None, pt_expr

    # Non-WAP: no `partition_column` concept; return only the
    # effective partition_ts so the caller can build the filter.
    return None, pt_expr


def expand_partition_ts(
    partition_ts: Any,
    *,
    step: timedelta | None = None,
    max_partitions: int = 10000,
) -> list[datetime]:
    """Normalize the operator-supplied ``partition_ts`` into a list
    of discrete partition anchors.

    Accepts:
    - Single string / datetime → ``[ts]``.
    - List of strings / datetimes → ``[ts1, ts2, ...]``.
    - Tuple ``(start, end)`` → expanded by ``step`` (inclusive on
      both ends).

    Range expansion requires ``step`` to be supplied. Otherwise
    raises :class:`QualifireConfigError`. Out-of-order or malformed
    ranges are rejected with explicit diagnostics.
    """
    if isinstance(partition_ts, tuple):
        if len(partition_ts) != 2:
            raise QualifireConfigError(
                f"partition_ts tuple must be (start, end); got {partition_ts!r}"
            )
        if step is None or step.total_seconds() <= 0:
            raise QualifireConfigError(
                "partition_ts range expansion requires a positive 'step' "
                "(derived from dataset / run-level partition_step)"
            )
        start = _parse_one(partition_ts[0])
        end = _parse_one(partition_ts[1])
        if end < start:
            raise QualifireConfigError(
                f"partition_ts range end ({end}) is before start ({start})"
            )
        if max_partitions < 1:
            raise QualifireConfigError(
                f"max_partitions must be >= 1; got {max_partitions}"
            )
        out: list[datetime] = []
        cur = start
        count = 0
        while cur <= end:
            out.append(cur)
            cur = cur + step
            count += 1
            if count > max_partitions:
                raise QualifireConfigError(
                    f"partition_ts range expanded to >{max_partitions} "
                    f"partitions; refusing — narrow the range or pass "
                    f"explicit list."
                )
        return out

    if isinstance(partition_ts, list):
        return [_parse_one(item) for item in partition_ts]

    return [_parse_one(partition_ts)]


def _parse_one(item: Any) -> datetime:
    if isinstance(item, datetime):
        return item
    return datetime.fromisoformat(str(item))


@dataclasses.dataclass
class _WorkUnit:
    """One backfill work unit — covers a single ``(scope, anchor)``
    pair. The metric-loop runs INSIDE the worker (engine-once
    contract); see :func:`_process_anchor`."""

    seq_idx: int           # original linearization order
    scope: ScopedDataset
    anchor: datetime
    eff_col: str | None    # resolved partition_column
    eff_pt: str | None     # resolved partition_ts expression


@dataclasses.dataclass
class _PreEngine:
    """Pre-engine state captured per metric inside Pass 1 of the
    anchor worker. Held in memory for the duration of one anchor
    so Pass 2 (post-engine diff build) has access to the
    pre-collection ``original_value`` and ``severity_before``."""

    table: str
    original_value: float | None
    severity_before: Severity | None


def run_backfill(
    *,
    config: QualifireConfig,
    storage: Any,
    backend: Any,
    notifiers: dict[str, Any] | None,
    partition_ts: Any,
    selector: str | None,
    data: bool,
    skip_recollection: bool,
    skip_renotification: bool = False,
    skip_revalidation: bool = False,
    soft_delete_prior: bool,
    parallelism: int = 1,
    max_partitions: int = 10000,
    owner: str = "",
    bu: str = "",
    instance_redacted_columns: list[str] | None = None,
    instance_allowlist_columns: list[str] | None = None,
) -> BackfillReport:
    """Execute the backfill loop and return a ``BackfillReport``.

    Builds one ``PartitionDiff`` per ``(dataset, metric, dim,
    partition_ts)`` tuple touched. Forwards to the existing engine
    for collection + validation per scope+partition; sets
    ``context.backfill=True`` so emitted rows carry
    ``details_json.collected_via='backfill'``.

    Engine-once contract (Item 1 carve-out A): the engine runs
    exactly **once** per ``(scope, anchor)`` pair regardless of
    ``parallelism``. Today's per-metric loop made N redundant
    engine.run() calls per anchor; the new shape cuts to 1.
    Per-anchor write/notification counts drop accordingly.

    Parallelism: ``parallelism > 1`` fans out distinct
    ``(scope, anchor)`` units across a thread pool. Forces
    ``notifiers={}`` for the inner engine call to avoid
    suppression races; ``BackfillReport.notifications_suppressed``
    flags this.
    """
    # Resolve selector (or default: every validation in the config).
    scopes = _resolve_scopes(config, selector)

    # Build the linearized work list (preserves serial-mode order
    # via ``seq_idx`` so parallelism doesn't reshuffle the report).
    work: list[_WorkUnit] = []
    seq = 0
    for scope in scopes:
        ds = scope.dataset
        from qualifire.core.config import effective_partition_step
        from qualifire.core.duration import parse_duration
        step_str = effective_partition_step(ds, config)
        step_td: timedelta | None = None
        if step_str is not None:
            step_td = parse_duration(step_str)

        if data and ds.wap is None:
            raise QualifireConfigError(
                f"data=True is only supported for WAP datasets; "
                f"dataset {ds.name!r} is non-WAP. Either set "
                f"data=False (metrics-only backfill) or wrap the "
                f"dataset in a wap: block."
            )
        # Backfill-mode source selection requires partition column /
        # filter. WAP target-mode needs partition_column; non-WAP
        # needs partition_ts as a column ref.
        _validate_backfill_source(ds, config, data=data)

        eff_col, eff_pt = _resolve_partition_anchors(ds, config)

        anchors = expand_partition_ts(
            partition_ts, step=step_td, max_partitions=max_partitions,
        )

        for anchor in anchors:
            work.append(_WorkUnit(
                seq_idx=seq, scope=scope, anchor=anchor,
                eff_col=eff_col, eff_pt=eff_pt,
            ))
            seq += 1

    # Force-empty notifiers under parallelism>1 to avoid suppression
    # races (multiple workers reading + writing
    # read_validation_history_bulk concurrently). Item 1 visibility:
    # surface this via BackfillReport.notifications_suppressed.
    notifications_suppressed = parallelism > 1
    notifiers_for_workers = {} if notifications_suppressed else (notifiers or {})

    # Process the work list. Each result is
    # ``(seq_idx, list[PartitionDiff], counters)``.
    if parallelism == 1:
        results = [
            _process_anchor(
                unit, config=config, backend=backend, storage=storage,
                notifiers=notifiers_for_workers, data=data,
                skip_recollection=skip_recollection,
                skip_renotification=skip_renotification,
                skip_revalidation=skip_revalidation,
                soft_delete_prior=soft_delete_prior,
                owner=owner, bu=bu,
                instance_redacted_columns=instance_redacted_columns,
                instance_allowlist_columns=instance_allowlist_columns,
            )
            for unit in work
        ]
    else:
        from concurrent.futures import ThreadPoolExecutor, as_completed
        with ThreadPoolExecutor(max_workers=parallelism) as ex:
            futures = [
                ex.submit(
                    _process_anchor, unit,
                    config=config, backend=backend, storage=storage,
                    notifiers=notifiers_for_workers, data=data,
                    skip_recollection=skip_recollection,
                    skip_renotification=skip_renotification,
                    skip_revalidation=skip_revalidation,
                    soft_delete_prior=soft_delete_prior,
                    owner=owner, bu=bu,
                    instance_redacted_columns=instance_redacted_columns,
                    instance_allowlist_columns=instance_allowlist_columns,
                )
                for unit in work
            ]
            try:
                results = [f.result() for f in as_completed(futures)]
            except KeyboardInterrupt:
                ex.shutdown(wait=True, cancel_futures=True)
                raise

    # Sort by seq_idx to preserve byte-for-byte serial-mode order
    # regardless of completion order under parallelism.
    results.sort(key=lambda t: t[0])

    # Flatten per-anchor diffs and aggregate counters.
    partitions: list[PartitionDiff] = []
    refreshed = unchanged = skipped = errored = 0
    for _seq_idx, diffs, ctr in results:
        partitions.extend(diffs)
        refreshed += ctr.get("refreshed", 0)
        unchanged += ctr.get("unchanged", 0)
        skipped += ctr.get("skipped", 0)
        errored += ctr.get("errored", 0)

    return BackfillReport(
        partitions=partitions,
        refreshed=refreshed,
        unchanged=unchanged,
        skipped=skipped,
        errored=errored,
        notifications_suppressed=notifications_suppressed,
    )


def _process_anchor(
    unit: _WorkUnit,
    *,
    config: QualifireConfig,
    backend: Any,
    storage: Any,
    notifiers: dict[str, Any],
    data: bool,
    skip_recollection: bool,
    skip_renotification: bool = False,
    skip_revalidation: bool = False,
    soft_delete_prior: bool,
    owner: str,
    bu: str,
    instance_redacted_columns: list[str] | None = None,
    instance_allowlist_columns: list[str] | None = None,
) -> tuple[int, list[PartitionDiff], dict[str, int]]:
    """Execute one (scope, anchor) work unit.

    Two-pass structure:
      - Pass 1 (pre-engine): read ``original_value`` /
        ``severity_before`` per metric; write prior tombstones
        if ``soft_delete_prior``. Any failure aborts the anchor —
        engine.run() is skipped, every metric reports ``errored``
        with ``pre-engine[<metric>]: <reason>``. Sibling anchors
        unaffected.
      - Single ``_run_anchor_once`` call (engine.run() ONCE).
      - Pass 2 (post-engine): build per-metric ``PartitionDiff``
        from the cached ``QualifireResult``.

    The post-loop sample-validator "skipped" emission preserves
    today's contract (plan D10).
    """
    diffs: list[PartitionDiff] = []
    ctr: dict[str, int] = {}
    pre: dict[str, _PreEngine] = {}
    ds = unit.scope.dataset
    metric_names = unit.scope.metric_filter or _all_metric_names(unit.scope)
    table = _resolve_table_name(storage, ds.name)
    metric_to_validation_names = _resolve_metric_to_validation_names(unit.scope)

    # Pass 1a: per-metric pre-engine reads. Failures here abort the
    # anchor (engine.run() skipped, no rows written).
    for m in metric_names:
        try:
            original_value = _read_original_value(
                storage=storage, table_name=table,
                metric_name=m, anchor=unit.anchor,
            )
            severity_before = _read_severity_before(
                storage=storage, dataset_name=ds.name or "",
                metric_name=m,
                validation_names=metric_to_validation_names.get(m, []),
                partition_ts=unit.anchor,
            )
            pre[m] = _PreEngine(
                table=table,
                original_value=original_value,
                severity_before=severity_before,
            )
        except Exception as e:
            logger.warning(
                "backfill: pre-engine read failure on (dataset=%s, "
                "metric=%s, partition=%s): %s",
                ds.name, m, unit.anchor, e,
            )
            for mm in metric_names:
                diffs.append(PartitionDiff(
                    dataset_name=ds.name or "",
                    metric_name=mm,
                    dimension_value=None,
                    partition_ts=unit.anchor,
                    original_value=None,
                    backfilled_value=None,
                    severity_before=None,
                    severity_after=None,
                    status="errored",
                    error=f"pre-engine[{m}]: {e!r}",
                ))
                ctr["errored"] = ctr.get("errored", 0) + 1
            return unit.seq_idx, diffs, ctr

    # Pass 1b: build all tombstone rows in memory, then write them
    # in ONE bulk call. All-or-nothing: if the bulk write fails, no
    # tombstones land — anchor aborts cleanly without leaving a
    # partial-tombstone state. Codex impl-review R1 BLOCKER fix:
    # earlier per-metric writes could leave metric A tombstoned and
    # metric B not, with the engine then skipped on B's failure.
    if soft_delete_prior:
        from qualifire.core.deactivate import build_prior_tombstone_rows
        all_tombstones: list[dict[str, Any]] = []
        try:
            for m in metric_names:
                all_tombstones.extend(build_prior_tombstone_rows(
                    storage,
                    dataset_name=ds.name or "",
                    metric_name=m,
                    partition_ts=unit.anchor,
                    note="backfill: soft_delete_prior",
                    owner=owner, bu=bu,
                ))
            if all_tombstones:
                storage.write_results(all_tombstones)
        except Exception as e:
            logger.warning(
                "backfill: tombstone-write failure (atomic, no rows "
                "landed) on (dataset=%s, partition=%s): %s",
                ds.name, unit.anchor, e,
            )
            for mm in metric_names:
                diffs.append(PartitionDiff(
                    dataset_name=ds.name or "",
                    metric_name=mm,
                    dimension_value=None,
                    partition_ts=unit.anchor,
                    original_value=None,
                    backfilled_value=None,
                    severity_before=None,
                    severity_after=None,
                    status="errored",
                    error=f"pre-engine[tombstone-write]: {e!r}",
                ))
                ctr["errored"] = ctr.get("errored", 0) + 1
            return unit.seq_idx, diffs, ctr

    # Single engine call per anchor. (engine-once carve-out)
    # Skip engine work when there are no metric-based validators
    # — preserves today's contract where a scope of only sample-
    # based validators (Pattern / AnomalyDetection) emits skipped
    # diffs WITHOUT triggering forward collection.
    if not metric_names:
        engine_result = None  # type: ignore[assignment]
    else:
      try:
        engine_result = _run_anchor_once(
            config=config, backend=backend, storage=storage,
            notifiers=notifiers, scope=unit.scope, anchor=unit.anchor,
            data=data, skip_recollection=skip_recollection,
            skip_renotification=skip_renotification,
            skip_revalidation=skip_revalidation,
            eff_col=unit.eff_col, eff_pt=unit.eff_pt,
            instance_redacted_columns=instance_redacted_columns,
            instance_allowlist_columns=instance_allowlist_columns,
        )
      except Exception as e:
        logger.warning(
            "backfill: engine failure on (dataset=%s, partition=%s): %s",
            ds.name, unit.anchor, e,
        )
        for m in metric_names:
            diffs.append(PartitionDiff(
                dataset_name=ds.name or "",
                metric_name=m,
                dimension_value=None,
                partition_ts=unit.anchor,
                original_value=pre[m].original_value,
                backfilled_value=None,
                severity_before=pre[m].severity_before,
                severity_after=None,
                status="errored",
                error=f"engine: {e!r}",
            ))
            ctr["errored"] = ctr.get("errored", 0) + 1
        # Sample-validator skipped block still runs even when the
        # engine errored on the metric-bearing validators —
        # preserves today's "skipped" surface.
        if unit.scope.metric_filter is None:
            for sample_name in _sample_validator_names(unit.scope):
                diffs.append(PartitionDiff(
                    dataset_name=ds.name or "",
                    metric_name=sample_name,
                    dimension_value=None,
                    partition_ts=unit.anchor,
                    original_value=None, backfilled_value=None,
                    severity_before=None, severity_after=None,
                    status="skipped",
                    skip_reason="no_historical_samples",
                ))
                ctr["skipped"] = ctr.get("skipped", 0) + 1
        return unit.seq_idx, diffs, ctr

    # Pass 2: per-metric diff build from the cached result.
    # ``engine_result`` is None when ``metric_names`` was empty
    # (only sample validators on this scope) — the loop below
    # is then a no-op and the sample-validator block at the end
    # produces the operator-visible "skipped" diffs.
    for m in metric_names:
        backfilled_value, severity_after = _extract_metric_severity(
            engine_result, metric_name=m,
        )
        status = _classify_diff(
            original_value=pre[m].original_value,
            backfilled_value=backfilled_value,
            severity_before=pre[m].severity_before,
            severity_after=severity_after,
        )
        diffs.append(PartitionDiff(
            dataset_name=ds.name or "",
            metric_name=m,
            dimension_value=None,
            partition_ts=unit.anchor,
            original_value=pre[m].original_value,
            backfilled_value=backfilled_value,
            severity_before=pre[m].severity_before,
            severity_after=severity_after,
            status=status,
        ))
        ctr[status] = ctr.get(status, 0) + 1

    # Sample-based validators (Pattern / AnomalyDetection) don't
    # fit the per-metric_value diff loop — surface them as
    # "skipped" entries (plan D10, preserved).
    if unit.scope.metric_filter is None:
        for sample_name in _sample_validator_names(unit.scope):
            diffs.append(PartitionDiff(
                dataset_name=ds.name or "",
                metric_name=sample_name,
                dimension_value=None,
                partition_ts=unit.anchor,
                original_value=None,
                backfilled_value=None,
                severity_before=None,
                severity_after=None,
                status="skipped",
                skip_reason="no_historical_samples",
            ))
            ctr["skipped"] = ctr.get("skipped", 0) + 1

    return unit.seq_idx, diffs, ctr


def _run_anchor_once(
    *,
    config: QualifireConfig,
    backend: Any,
    storage: Any,
    notifiers: dict[str, Any],
    scope: ScopedDataset,
    anchor: datetime,
    data: bool,
    skip_recollection: bool,
    skip_renotification: bool = False,
    skip_revalidation: bool = False,
    eff_col: str | None,
    eff_pt: str | None,
    instance_redacted_columns: list[str] | None = None,
    instance_allowlist_columns: list[str] | None = None,
) -> QualifireResult:
    """Execute the forward engine ONCE for a (scope, anchor) pair.

    Returns the full ``QualifireResult``; per-metric extraction
    happens in Pass 2 of :func:`_process_anchor`.
    """
    from qualifire.core.engine import QualifireEngine

    backfill_ds = _build_partition_dataset(
        scope.dataset, anchor=anchor, data=data,
        validations=scope.validations,
        effective_partition_column=eff_col,
        effective_partition_ts_expr=eff_pt,
    )
    backfill_config = config.model_copy(update={"datasets": [backfill_ds]})

    ctx = QualifireContext()
    ctx.backfill = True
    ctx.skip_recollection = skip_recollection
    ctx.skip_renotification = skip_renotification
    ctx.skip_revalidation = skip_revalidation
    ctx.instance_redacted_columns = instance_redacted_columns
    ctx.instance_allowlist_columns = instance_allowlist_columns

    engine = QualifireEngine(
        backend=backend, storage=storage, context=ctx,
        config=backfill_config, notifiers=notifiers,
    )
    return engine.run()


def _resolve_scopes(
    config: QualifireConfig, selector: str | None,
) -> list[ScopedDataset]:
    """Resolve the operator-supplied ``selector`` into a list of
    ``ScopedDataset`` records. ``None`` selector defaults to every
    dataset / validation in the config.
    """
    if selector is not None:
        parts = parse_selector(selector)
        return resolve_selector(parts, config)
    # Default scope: every dataset, every validation, all metrics.
    return [
        ScopedDataset(
            dataset=ds,
            validations=list(ds.validations),
            metric_filter=None,
        )
        for ds in config.datasets
        if ds.validations
    ]


def _all_metric_names(scope: ScopedDataset) -> list[str]:
    """Resolve every metric name the scope's validations consume.

    Calls ``expected_metrics()`` per validator (Step 6 / sub-feature
    J). Validators that don't expose the resolver (sample-based —
    Pattern, AnomalyDetection) contribute nothing here; the
    backfill driver surfaces them via :func:`_sample_validator_names`
    so the operator's report carries an explicit "skipped" entry
    per sample-based validator (plan D10).
    """
    seen: list[str] = []
    for v in scope.validations:
        if hasattr(v, "expected_metrics"):
            for m in v.expected_metrics():
                if m not in seen:
                    seen.append(m)
    return seen


def _sample_validator_names(scope: ScopedDataset) -> list[str]:
    """Return the names of sample-based validators on the scope.

    Sample-based collectors (Pattern, AnomalyDetection) read row
    samples — not single ``metric_value``s — so they don't fit the
    per-metric backfill diff. The plan (D10) says these surface in
    the report as ``status='skipped'`` rows with
    ``skip_reason='no_historical_samples'`` so operators
    see them in the diff and aren't silently confused that they
    "ran" without a metric value. Returns one entry per such
    validator, named after the validator (or its type if unnamed).
    """
    out: list[str] = []
    for v in scope.validations:
        # Validators with expected_metrics() are aggregation-shaped.
        # Anything without expected_metrics() is sample-shaped (per
        # plan D10); skip them with a structured reason.
        if hasattr(v, "expected_metrics"):
            continue
        name = getattr(v, "name", None) or getattr(v, "type", None) or "<unknown>"
        if name not in out:
            out.append(name)
    return out


def _validate_backfill_source(
    ds: DatasetConfig, run_config: QualifireConfig, *, data: bool,
) -> None:
    """Per-partition source-select preflight (plan D9).

    Item 2 (backfill-followups-and-polish) auto-detects
    ``WAPConfig.partition_column`` from a bare-identifier
    ``effective_partition_ts(ds, run_config)``. Item 2 missed-
    HIGH #7 also routes the non-WAP path through the same
    effective-partition_ts resolver so run-level inheritance
    is honoured.
    """
    eff_col, eff_pt = _resolve_partition_anchors(ds, run_config)

    if ds.wap is not None:
        if not data and not eff_col:
            raise QualifireConfigError(
                f"WAPConfig.partition_column required for backfill on "
                f"dataset {ds.name!r}: add partition_column='<col>' to "
                f"the dataset's wap config, or set partition_ts to a "
                f"bare column identifier so it can be auto-derived."
            )
        return
    # non-WAP path: partition_ts must be a column reference. A
    # constant Jinja expression like '{{ ds }}' renders to the
    # partition value and produces a no-op WHERE clause. Reject.
    pt = eff_pt or ""
    if not pt:
        raise QualifireConfigError(
            f"non-WAP backfill on dataset {ds.name!r} requires "
            f"partition_ts to be a column reference (e.g. "
            f"partition_ts='event_date')"
        )
    if pt.strip().startswith("'") or "{{" in pt:
        raise QualifireConfigError(
            f"non-WAP backfill on dataset {ds.name!r}: partition_ts "
            f"must be a column reference, got {pt!r}. Constant Jinja "
            f"expressions (e.g. '{{{{ ds }}}}') are ambiguous in "
            f"backfill mode — they'd produce a no-op filter."
        )


def _resolve_table_name(storage: Any, dataset_name: str | None) -> str:
    if not dataset_name:
        return ""
    latest = storage.read_latest_run(dataset_name) if storage else None
    if not latest:
        return ""
    return str(latest.get("table_name") or "")


def _read_original_value(
    *,
    storage: Any,
    table_name: str,
    metric_name: str,
    anchor: datetime,
) -> float | None:
    if storage is None or not table_name:
        return None
    row = storage.read_collection_metric_at_partition(
        table_name=table_name,
        metric_name=metric_name,
        anchor_ts=anchor,
        dimension_value=None,
    )
    if row is None:
        return None
    v = row.get("metric_value")
    return float(v) if v is not None else None


def _read_severity_before(
    *,
    storage: Any,
    dataset_name: str,
    metric_name: str,
    validation_names: list[str],
    partition_ts: datetime,
) -> Severity | None:
    """Read the prior validation severity for ``metric_name`` at
    ``partition_ts`` on ``dataset_name``.

    Pre-fix this helper called
    ``read_validation_history(validation_name="")`` which the
    storage backends' exact-match filter rejected — every backfill
    run reported ``severity_before=None``. Codex impl-review R4
    (HIGH) on the parent ``backfill-followups-and-polish`` plan
    flagged it; this fix lands the per-feature follow-up.

    Approach: per the parent
    ``_resolve_metric_to_validation_names`` map, walk every
    persisted validation row at the exact partition for the
    candidate validation_names; client-side filter by exact
    validation_name + metric_name; aggregate max severity
    (ERROR > WARNING > PASS).

    Returns ``None`` when:
      - storage is None
      - validation_names is empty (no validators on the scope
        emit this metric — defensive; shouldn't occur given the
        caller's expected_metrics() resolution)
      - the per-partition lookup raises
      - no surviving row matches the (validation_name,
        metric_name) tuple
    """
    if storage is None or not validation_names:
        return None
    # Use one read per distinct base (the helper's prefix matches
    # base AND base.<sub>); then client-side filter to exact match.
    # Most scopes have one validation_name per metric, so this is
    # one read per metric on the hot path.
    bases = {vn.rsplit(".", 1)[0] if "." in vn else vn
             for vn in validation_names}
    seen_severities: list[Severity] = []
    for base in bases:
        try:
            rows = storage.read_validations_at_partition(
                dataset_name=dataset_name,
                validation_name_prefix=base,
                partition_ts=partition_ts,
            )
        except Exception:
            continue
        for row in rows:
            row_vname = row.get("validation_name")
            row_metric = row.get("metric_name")
            if row_vname not in validation_names:
                continue
            if row_metric != metric_name:
                continue
            status = row.get("validation_status")
            if not status:
                continue
            try:
                seen_severities.append(Severity(status))
            except ValueError:
                continue
    if not seen_severities:
        return None
    # Severity enum implements __gt__ with PASS < WARNING < ERROR
    # ordering (qualifire/core/models.py:48). Python's max() invokes
    # __gt__, so this returns the most-severe entry.
    return max(seen_severities)


def _resolve_metric_to_validation_names(
    scope: ScopedDataset,
) -> dict[str, list[str]]:
    """Build a map ``metric_name -> [validation_name, ...]`` from
    the scope's validations.

    For threshold / historical / forecast validators, the engine
    emits each row as ``validation_name=f"{base}.{rule.metric}"``
    (verified across qualifire/validation/{threshold,historical,
    forecast}.py). The base name is the validator's resolved
    `name`; for un-named validators the engine falls back to
    `f"{val_config.type}.{ds.name}"` per
    ``_resolve_validation_base_name``.

    Validators without ``expected_metrics()`` (Pattern,
    AnomalyDetection, SLO) contribute nothing here — they don't
    flow through the backfill per-metric driver.
    """
    out: dict[str, list[str]] = {}
    ds_name = scope.dataset.name or ""
    for v in scope.validations:
        if not hasattr(v, "expected_metrics"):
            continue
        # Resolve the base name the same way the engine does.
        # Local import to avoid a top-level cycle.
        from qualifire.core.engine import _resolve_validation_base_name
        base = _resolve_validation_base_name(v, scope.dataset)
        for m in v.expected_metrics():
            full_name = f"{base}.{m}"
            out.setdefault(m, []).append(full_name)
    return out


def _classify_diff(
    *,
    original_value: float | None,
    backfilled_value: float | None,
    severity_before: Severity | None,
    severity_after: Severity | None,
) -> str:
    """Apply the plan D8 status assignment rules.

    - ``refreshed`` if values OR severities differ.
    - ``unchanged`` if both equal within tolerance.
    """
    values_equal = (
        original_value == backfilled_value
        or (
            original_value is not None
            and backfilled_value is not None
            and math.isclose(
                original_value, backfilled_value, rel_tol=1e-9, abs_tol=1e-12,
            )
        )
    )
    severities_equal = severity_before == severity_after
    if values_equal and severities_equal:
        return "unchanged"
    return "refreshed"


def _build_partition_dataset(
    ds: DatasetConfig,
    *,
    anchor: datetime,
    data: bool,
    validations: list[Any] | None = None,
    effective_partition_column: str | None = None,
    effective_partition_ts_expr: str | None = None,
) -> DatasetConfig:
    """Build a partition-pinned ``DatasetConfig`` for the backfill.

    For WAP datasets in metrics-only mode (data=False, default):
    - swap ``wap`` away (we read directly from target_table).
    - pin ``table`` to ``wap.target_table``.
    - add a ``filter`` filtering by ``wap.partition_column = anchor``.

    For non-WAP datasets:
    - augment ``filter`` with the partition_ts equality check.

    For WAP with ``data=True``: keep the wap block (full cycle
    runs against the past partition's data) — this is the rarer
    path; most operators want metrics-only.

    Literal generation: a quoted ISO-8601 string (``'2026-04-01'``
    or ``'2026-04-01T00:00:00'``) works on every supported backend
    — Spark accepts it for both STRING and TIMESTAMP columns
    (implicit cast); SQLite compares it as text (correct for the
    way qualifire writes ``partition_ts`` as an ISO string); JDBC
    Postgres / MySQL / Oracle treat it as a string. We do NOT emit
    the dialect-specific ``TIMESTAMP '...'`` prefix because that's
    a Spark-only syntax and breaks Pandas / SQLite backends.
    """
    anchor_lit = _anchor_literal(anchor, ds=ds)
    pt_expr = _anchor_partition_ts_expr(anchor)
    # Default to the dataset's full validation set; selector-scoped
    # callers narrow it.
    effective_validations = (
        list(validations) if validations is not None else list(ds.validations)
    )

    if ds.wap is not None and not data:
        target = ds.wap.target_table
        # Item 2 (backfill-followups-and-polish): consume the
        # resolved value from `_resolve_partition_anchors`. Codex
        # impl-review R1 LOW: no legacy fallback to
        # ``ds.wap.partition_column`` — every caller threads the
        # resolver result. Empty string means "no column derivable"
        # → falls through to the no-filter branch (preserved).
        col = effective_partition_column or ""
        partition_filter = f"{col} = {anchor_lit}" if col else None
        existing = ds.filter
        new_filter = (
            f"({existing}) AND ({partition_filter})"
            if existing and partition_filter else (partition_filter or existing)
        )
        return DatasetConfig(
            name=ds.name,
            description=ds.description,
            table=target,
            dimensions=ds.dimensions,
            measures=ds.measures,
            partition_ts=pt_expr,
            partition_step=ds.partition_step,
            filter=new_filter,
            validations=effective_validations,
            redacted_columns=ds.redacted_columns,
            allowlist_columns=ds.allowlist_columns,
            # No wap — we already pinned the source to target_table
        )

    if ds.wap is not None and data:
        # Full WAP cycle: keep the wap block but pin partition_ts.
        return ds.model_copy(update={
            "partition_ts": pt_expr,
            "validations": effective_validations,
        })

    # Non-WAP: filter by partition_ts column = anchor.
    # Item 2 missed-HIGH #7: use effective expr (run-level
    # inheritance) — every caller routes through
    # `_resolve_partition_anchors` and passes the resolved value.
    # Codex impl-review R1 LOW: no legacy fallback to
    # ``ds.partition_ts``.
    pt = effective_partition_ts_expr or ""
    partition_filter = f"{pt} = {anchor_lit}"
    existing = ds.filter
    new_filter = (
        f"({existing}) AND ({partition_filter})"
        if existing else partition_filter
    )
    return ds.model_copy(update={
        "filter": new_filter,
        "partition_ts": pt_expr,
        "validations": effective_validations,
    })


def _anchor_literal(anchor: datetime, *, ds: DatasetConfig) -> str:
    """SQL literal for the partition value used in WHERE filters.

    Default to a quoted ISO-8601 string — works on every supported
    backend. Date-only anchors (no time component) emit
    ``'YYYY-MM-DD'``; datetimes emit the full ISO-8601 form.
    """
    if (
        anchor.hour == 0 and anchor.minute == 0
        and anchor.second == 0 and anchor.microsecond == 0
    ):
        return f"'{anchor.date().isoformat()}'"
    return f"'{anchor.isoformat()}'"


def _anchor_partition_ts_expr(anchor: datetime) -> str:
    """SQL expression that evaluates to the partition timestamp.

    Used as ``partition_ts`` on the partition-pinned DatasetConfig
    so collectors stamp it on every emitted row. A quoted constant
    ISO-8601 string is unambiguous across backends.
    """
    if (
        anchor.hour == 0 and anchor.minute == 0
        and anchor.second == 0 and anchor.microsecond == 0
    ):
        return f"'{anchor.date().isoformat()}'"
    return f"'{anchor.isoformat()}'"


def _extract_metric_severity(
    result: QualifireResult, *, metric_name: str,
) -> tuple[float | None, Severity | None]:
    """Pluck ``(metric_value, severity)`` for a given metric out of
    the engine's result tree.

    When multiple validations on the dataset produce a row for the
    same metric (e.g. a threshold AND a drift validator both keyed
    on ``cnt``), the severity is **max-aggregated** so the diff
    classification matches the symmetric readback policy in
    ``_read_severity_before`` (codex impl-review R1 MAJOR — without
    the max-aggregation here, ``severity_before=ERROR`` from
    aggregation would unfairly compare against ``severity_after=PASS``
    from "first row wins" and falsely report ``refreshed``).
    """
    severities: list[Severity] = []
    val: float | None = None
    for ds_result in result.datasets:
        # Pluck the metric value once from collection_results
        # (collection_results are deduped per-metric — one row per
        # metric for non-dimensional collectors). For dimensional
        # collectors, the per-dim value plumbing is captured at the
        # validation_result level, not here.
        if val is None:
            for cr in ds_result.collection_results:
                if cr.metric_name == metric_name:
                    val = (
                        float(cr.metric_value)
                        if cr.metric_value is not None else None
                    )
                    break
        for vr in ds_result.validation_results:
            if vr.metric_name == metric_name:
                severities.append(vr.severity)
    if not severities:
        return None, None
    return val, max(severities)
