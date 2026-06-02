"""Qualifire engine: orchestrates collect -> validate -> notify -> persist."""

from __future__ import annotations

import hashlib
import json
import logging
import re
import threading
from dataclasses import dataclass, field
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from enum import Enum
from typing import Any, Callable

from qualifire.backends.base import Backend
from qualifire.collection._filters import and_combine_filters
from qualifire.collection.aggregation import AggregationCollector
from qualifire.collection.custom_query import CustomQueryCollector
from qualifire.collection.metrics import MetricsCollector
from qualifire.collection.profiler import ProfilingCollector
from qualifire.collection.recency import RecencyCollector
from qualifire.collection.sampler import SamplerCollector
from qualifire.core.config import (
    AggregationCollectionConfig,
    AnomalyDetectionValidationConfig,
    CustomQueryCollectionConfig,
    DatasetConfig,
    ForecastValidationConfig,
    HistoricalValidationConfig,
    MetricsCollectionConfig,
    NotifyConfig,
    PatternValidationConfig,
    ProfilingCollectionConfig,
    QualifireConfig,
    SampleCollectionConfig,
    SLOValidationConfig,
    ThresholdValidationConfig,
)
from qualifire.core.context import QualifireContext
from qualifire.core.exceptions import (
    MissingPartitionAnchorError,
    QualifireInternalError,
    QualifireValidationError,
)


def _format_partition_ts(value: datetime | None) -> str | None:
    """Format a datetime as ISO-8601 for system-table persistence, or None."""
    return value.isoformat() if value is not None else None


def _partition_ts_for_validation(vr: Any, ds: Any) -> datetime | None:
    """Look up the CollectionResult that produced this validation row and
    return its partition_ts, falling back to None.

    Validators emit one ValidationResult per (metric_name, dimension_value)
    in the source collected list. Match on that pair so the persisted
    validation row carries the same partition identity as its source
    collection row.
    """
    target_metric = getattr(vr, "metric_name", None)
    target_dim = getattr(vr, "dimension_value", None)
    if target_metric is None:
        return None
    for cr in getattr(ds, "collection_results", []) or []:
        if cr.metric_name == target_metric and cr.dimension_value == target_dim:
            return cr.partition_ts
    return None


def _resolve_run_level_partition_ts(
    raw_expr: str | None, ctx: Any,
) -> datetime | None:
    """Render a partition_ts expression and try to parse it as a literal datetime.

    Recognized forms after Jinja rendering:
      - ``'2026-04-28'`` (single-quoted ISO date)
      - ``'2026-04-28T12:00:00'`` (single-quoted ISO datetime)
      - ``2026-04-28`` / ``2026-04-28T12:00:00`` (unquoted ISO)

    Anything else (column refs like `event_dt`, SQL functions like
    `CURRENT_TIMESTAMP`) is non-literal and returns None — those must be
    resolved per-row by the collector via `qf_partition_ts AS …` injection.
    """
    if not raw_expr:
        return None
    try:
        rendered = ctx.render(raw_expr).strip()
    except Exception:
        return None
    if not rendered:
        return None
    # Strip surrounding quotes (single or double).
    if (
        len(rendered) >= 2
        and rendered[0] == rendered[-1]
        and rendered[0] in "'\""
    ):
        rendered = rendered[1:-1]
    try:
        return datetime.fromisoformat(rendered)
    except ValueError:
        try:
            from datetime import date as _date
            d = _date.fromisoformat(rendered)
            return datetime(d.year, d.month, d.day)
        except ValueError:
            return None


from qualifire.core.models import (
    CollectionResult,
    DatasetResult,
    NotificationResult,
    QualifireResult,
    Severity,
    ValidationKey,
    ValidationResult,
    validation_key_for,
)
from qualifire.notification.base import (
    Notifier,
    format_grouped_notification_message,
    format_notification_message,
    get_notifier,
    should_suppress,
)
from qualifire.storage.base import SYSTEM_TABLE_COLUMNS
from qualifire.validation.forecast import ForecastValidator
from qualifire.validation.historical import HistoricalValidator
from qualifire.validation.isolation_forest import IsolationForestValidator
from qualifire.validation.pattern_check import PatternCheckValidator
from qualifire.validation.slo import SLOValidator
from qualifire.validation.threshold import ThresholdValidator
from qualifire.wap.pattern import WAPExecutor

logger = logging.getLogger(__name__)


@dataclass
class PipelineResult:
    """Result from a single collect→validate pipeline."""

    validation_results: list[ValidationResult] = field(default_factory=list)
    collection_results: list[CollectionResult] = field(default_factory=list)


# ---------------------------------------------------------------------------
# Qualifire-internal failure classification
# ---------------------------------------------------------------------------
# Two engine paths produce ValidationResult rows that look like
# data-quality findings (severity=ERROR) but are actually qualifire-
# internal failures: persistence-infrastructure outage and
# validator-execution exceptions. Both are tagged with
# ``details['qualifire_internal_failure'] = True`` so the notification
# dispatcher can short-circuit (no false-positive Slack/email pages
# for library bugs or storage outages) while the rows are still
# persisted for forensics and the run still raises.


class PersistenceOutcomeKind(Enum):
    """Outcome category for ``_persist_data_rows``."""

    OK = "ok"
    ROW_LEVEL_WARNING = "row_warning"   # some rows rejected, others persisted
    INFRA_FAILURE = "infra_failure"     # entire write failed; system table unreachable


@dataclass(frozen=True)
class PersistenceOutcome:
    """Result of a ``_persist_data_rows`` call.

    ``exc`` is populated only on ``INFRA_FAILURE`` so the engine main
    loop can re-raise with proper exception chaining (PEP 3134)
    preserving the original catalog / connection / driver exception
    as ``__cause__``.
    """

    kind: PersistenceOutcomeKind
    exc: Exception | None = None


def _is_qualifire_internal_failure(vr: Any) -> bool:
    """Predicate the notification dispatcher uses to skip rows that
    represent qualifire-internal failures (persistence infra or
    validator-execution exceptions) rather than data-quality findings.

    Mirrored at every notification routing site (severity bucketing
    + per-channel message building) so a marked row never appears in
    notification message bodies and never drives notifier dispatch.

    Survives both dict and string ``details`` shapes — the in-memory
    engine path passes a dict, but rows re-read from the persisted
    system table arrive as a JSON string in some readers.
    """
    import json
    details = getattr(vr, "details", None)
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


def _validator_execution_error_result(
    val_config, ds_config, exc, *, validation_name: str | None = None,
) -> ValidationResult:
    """Wrap an unhandled exception from validator code as a
    ValidationResult marked as qualifire-internal so the
    notification dispatcher skips it.

    Used at three engine call sites:

    * ``_run_dataset`` parallel-pipeline branch (validator threw)
    * ``_run_pipeline`` sequential branch (validator threw)
    * ``run`` parallel-dataset branch (whole dataset threw)

    Nested wrappers are acceptable: a dataset-level error caused by a
    validator-execution exception will produce two rows in the system
    table — one ``<type>_error`` row from the validator wrapper plus
    one ``qualifire.dataset_error`` row from the dataset wrapper. Both
    are filtered from notifications by the shared
    ``_is_qualifire_internal_failure`` predicate. Each row records a
    different layer's view of the same root cause; the duplication is
    the right shape for forensic review, not a bug.

    Args:
        val_config: The validation config (used for ``.type`` and
            base-name resolution).
        ds_config: The dataset config (used for base-name fallback).
        exc: The unhandled exception thrown by validator code.
        validation_name: Override for the synthesised
            ``validation_name``. Default is ``f"{val_config.type}_error"``;
            the dataset-level wrapper passes
            ``"qualifire.dataset_error"`` instead.
    """
    name = validation_name or f"{val_config.type}_error"
    return ValidationResult(
        validation_name=name,
        validation_type=val_config.type,
        severity=Severity.ERROR,
        message=f"Validation execution error: {exc}",
        validation_base_name=_resolve_validation_base_name(val_config, ds_config),
        details={
            "qualifire_internal_failure": True,
            "error": str(exc),
        },
    )


# Stable default-name mapping. Config types are
# `slo`/`threshold`/`drift`/`trend`/`anomaly_detection`/`pattern`,
# but the historical default name uses the validator-suffix form
# (`historical.X`, `forecast.X`, `anomaly.X`). Existing system-table
# rows are keyed on those defaults; using val_config.type directly
# would change the keys and cold-start drift baselines.
_FALLBACK_PREFIX: dict[type, str] = {
    SLOValidationConfig: "slo",
    ThresholdValidationConfig: "threshold",
    HistoricalValidationConfig: "historical",
    ForecastValidationConfig: "forecast",
    AnomalyDetectionValidationConfig: "anomaly",
    PatternValidationConfig: "pattern",
}


def _resolve_validation_base_name(val_config: Any, ds_config: DatasetConfig) -> str:
    """Return the validator's base name.

    Custom name from `val_config.name` wins. Otherwise use the
    historical default `<prefix>.<dataset>` so existing
    suppression/history rows keep matching.
    """
    custom = getattr(val_config, "name", None)
    if custom:
        return custom
    prefix = _FALLBACK_PREFIX.get(type(val_config))
    if prefix is None:
        raise ValueError(f"No fallback prefix registered for {type(val_config).__name__}")
    return f"{prefix}.{ds_config.name}"


class _SampleCache:
    """Thread-safe dataset-scoped cache for SampleCollectionConfig results.

    Ensures every sample-based validator on the same sample spec receives
    the same rows — on Spark, sample_records uses unseeded ORDER BY RAND()
    so independent calls would otherwise produce divergent drift/anomaly
    signals for the same dataset.

    Per-key locking: concurrent misses on the same key serialize onto a
    single collection; misses on distinct keys proceed in parallel.
    """

    def __init__(self) -> None:
        self.data: dict[str, list[CollectionResult]] = {}
        self._locks: dict[str, threading.Lock] = {}
        self._meta_lock = threading.Lock()

    def _lock_for(self, key: str) -> threading.Lock:
        with self._meta_lock:
            lock = self._locks.get(key)
            if lock is None:
                lock = threading.Lock()
                self._locks[key] = lock
            return lock

    def get_or_collect(
        self,
        key: str,
        factory: Callable[[], list[CollectionResult]],
    ) -> list[CollectionResult]:
        """Return cached results for ``key``; otherwise run ``factory`` once.

        If ``factory`` raises, the exception propagates and the key stays
        unset so a later caller may retry. Successful calls are cached so
        every subsequent validator on the same key shares the same rows.
        """
        cached = self.data.get(key)
        if cached is not None:
            return cached
        with self._lock_for(key):
            cached = self.data.get(key)
            if cached is not None:
                return cached
            result = factory()
            self.data[key] = result
            return result


def _with_auto_slice_column_excluded(
    operator_excludes: list[str] | None, collection: Any,
) -> list[str]:
    """Append the sample collection's ``slice_column`` to the
    operator-supplied exclude list (deduped).

    Reason: when ``slice_column`` is the partition column (e.g.
    ``sale_date``), today's rows have ``slice_column = '<today>'``
    and every past-slice row has a different value. The IF tree
    (shape) and RF classifier (pattern) split on the column
    trivially → 100% current-period rows flagged / AUC = 1
    regardless of true distribution drift. Auto-excluding closes
    that footgun without operator action; operators still need
    to list per-row identifiers (``sale_id``) and ingestion
    timestamps (``updated_at``) themselves.
    """
    excludes = list(operator_excludes or [])
    slice_col = getattr(collection, "slice_column", None)
    if slice_col and slice_col not in excludes:
        excludes.append(slice_col)
    return excludes


class QualifireEngine:
    """Core orchestration engine.

    Processes datasets: collect -> validate -> notify -> persist to system table.
    On ERROR severity, raises QualifireValidationError after all datasets are processed.

    Args:
        backend: Data backend (Spark or Pandas).
        storage: System table storage backend (optional).
        context: Jinja2 rendering context.
        config: Parsed QualifireConfig.
        notifiers: Mapping of channel name -> instantiated Notifier.
    """

    def __init__(
        self,
        backend: Backend,
        storage: Any | None,
        context: QualifireContext,
        config: QualifireConfig,
        notifiers: dict[str, Notifier] | None = None,
    ):
        self.backend = backend
        self.storage = storage
        self.context = context
        self.config = config
        self.notifiers = notifiers or {}

    def run(self) -> QualifireResult:
        """Execute all datasets, optionally in parallel."""
        result = QualifireResult(
            owner=self.config.owner,
            bu=self.config.bu,
            run_id=self.context.run_id,
        )

        parallelism = self.config.dataset_parallelism
        ds_configs = self.config.datasets

        # `_run_wap` returns (DatasetResult, list[ValidationResult]) so
        # WAP cleanup warnings reach `result.engine_warnings` before
        # notification routing. `_run_dataset` returns just DatasetResult.
        # Both dispatch paths normalize to the tuple shape.
        def _dispatch(ds_config):
            if ds_config.wap:
                return self._run_wap(ds_config)
            return self._run_dataset(ds_config), []

        if parallelism <= 1 or len(ds_configs) <= 1:
            # Sequential
            for ds_config in ds_configs:
                ds_result, ew = _dispatch(ds_config)
                result.datasets.append(ds_result)
                result.engine_warnings.extend(ew)
        else:
            # Parallel dataset execution
            ds_outcomes: dict[int, tuple[DatasetResult, list[ValidationResult]]] = {}
            with ThreadPoolExecutor(max_workers=parallelism) as pool:
                futures = {
                    pool.submit(_dispatch, ds_config): idx
                    for idx, ds_config in enumerate(ds_configs)
                }
                for future in as_completed(futures):
                    idx = futures[future]
                    try:
                        ds_outcomes[idx] = future.result()
                    except Exception as e:
                        logger.error("Dataset %s failed: %s", ds_configs[idx].name, e)
                        # Tag dataset-level errors with the same
                        # qualifire-internal-failure marker so the
                        # dispatcher skips them. Different
                        # validation_name (qualifire.dataset_error)
                        # for forensic distinguishability, but same
                        # internal-failure semantics. See
                        # _validator_execution_error_result's
                        # docstring on nested-wrapper acceptance.
                        ds_outcomes[idx] = (
                            DatasetResult(
                                dataset_name=ds_configs[idx].name,
                                table=ds_configs[idx].table,
                                run_id=self.context.run_id,
                                run_timestamp=datetime.now(),
                                validation_results=[ValidationResult(
                                    validation_name="qualifire.dataset_error",
                                    validation_type="engine",
                                    severity=Severity.ERROR,
                                    message=f"Dataset execution error: {e}",
                                    validation_base_name="qualifire.dataset_error",
                                    details={
                                        "qualifire_internal_failure": True,
                                        "error": str(e),
                                    },
                                )],
                            ),
                            [],
                        )
            # Preserve original dataset order
            for i in range(len(ds_configs)):
                ds_result, ew = ds_outcomes[i]
                result.datasets.append(ds_result)
                result.engine_warnings.extend(ew)

        # Two-pass persistence (P4.1) + qualifire-internal-failure
        # classification (Phase 2 of external-catalog-system-table-
        # hardening):
        #   1. Pre-fetch suppression snapshot (pre-run state).
        #   2. Persist data + engine_warnings accumulated so far.
        #   3. On INFRA_FAILURE: skip notifications (no false-positive
        #      pages on storage outage), best-effort retry the
        #      engine_warning, persist any prior notifications, then
        #      re-raise with PEP 3134 chaining + data-findings
        #      summary.
        #   4. On OK / ROW_LEVEL_WARNING: send notifications using the
        #      snapshot (no fresh reads), persist notification rows,
        #      then raise QualifireValidationError if ERROR severity
        #      surfaced anywhere.
        snapshot = self._prefetch_suppression_snapshot(result)
        outcome = self._persist_data_rows(result)
        if outcome.kind != PersistenceOutcomeKind.INFRA_FAILURE:
            result.notifications = self._send_grouped_notifications(
                result, snapshot=snapshot,
            )
        else:
            self._best_effort_persist_engine_warning(result)
        self._persist_notification_rows(result)

        if outcome.kind == PersistenceOutcomeKind.INFRA_FAILURE:
            # Surface data-quality findings in the same exception so
            # operators don't lose them when persistence ALSO fails.
            # Findings were not persisted (storage broken) and not
            # notified (intentional — no false-positive paging on
            # infra failure), so the raised exception is the
            # operator's only signal.
            data_errors = [
                vr for ds in result.datasets
                for vr in ds.validation_results
                if vr.severity == Severity.ERROR
                and not _is_qualifire_internal_failure(vr)
            ]
            if data_errors:
                def _name_for_summary(vr) -> str:
                    return (
                        getattr(vr, "validation_base_name", None)
                        or getattr(vr, "validation_name", None)
                        or "<unknown>"
                    )
                summary = ", ".join(
                    f"{_name_for_summary(vr)}({vr.severity.value})"
                    for vr in data_errors[:5]
                )
                extra = (
                    f" + {len(data_errors) - 5} more"
                    if len(data_errors) > 5 else ""
                )
                msg = (
                    f"System-table persistence failed for run "
                    f"{result.run_id} AND {len(data_errors)} data-quality "
                    f"ERROR finding(s) could not be delivered: "
                    f"[{summary}{extra}]. Findings were NOT persisted "
                    f"(storage broken) and NOT notified (infra-failure "
                    f"suppression)."
                )
            else:
                msg = (
                    f"System-table persistence failed for run "
                    f"{result.run_id}; no data-quality ERROR findings "
                    f"to deliver."
                )
            # Persistence-infra failure is a qualifire-internal error
            # (storage backend broken). Distinct sibling class from
            # QualifireValidationError so callers can route to a
            # different on-call queue. ``__cause__`` preserves the
            # original storage exception via PEP 3134.
            raise QualifireInternalError(msg, result=result) from outcome.exc

        # Classify the run's ERROR-severity rows: real data-quality
        # findings vs qualifire-internal failures (validator-execution
        # exceptions tagged with ``qualifire_internal_failure=True``).
        if result.has_errors:
            data_errors = [
                vr for ds in result.datasets
                for vr in ds.validation_results
                if vr.severity == Severity.ERROR
                and not _is_qualifire_internal_failure(vr)
            ]
            internal_errors = [
                vr for ds in result.datasets
                for vr in ds.validation_results
                if vr.severity == Severity.ERROR
                and _is_qualifire_internal_failure(vr)
            ]
            if internal_errors and not data_errors:
                # All ERRORs are qualifire-internal — raise the
                # internal-error class so callers route to engineering
                # on-call rather than the data-team queue.
                raise QualifireInternalError(
                    f"Run {result.run_id} aborted due to qualifire-"
                    f"internal errors ({len(internal_errors)} synthesised "
                    f"failure row(s); no data-quality findings). Inspect "
                    f"result.datasets[*].validation_results[*] for rows "
                    f"with details['qualifire_internal_failure']=True.",
                    result=result,
                )
            # Real data findings exist (with or without internal
            # failures alongside) — the run is fundamentally about
            # the data findings; raise QualifireValidationError so
            # the data team is paged. Operators who want to also
            # route the internal failures to engineering can post-
            # facto check ``isinstance(_, QualifireInternalError)``
            # is False and walk ``e.result`` for marked rows.
            mixed_note = (
                f" ({len(internal_errors)} qualifire-internal failure(s) "
                f"alongside)" if internal_errors else ""
            )
            raise QualifireValidationError(
                f"Validation failed with ERROR severity for run "
                f"{result.run_id}{mixed_note}",
                result=result,
            )

        return result

    def _run_dataset(self, ds_config: DatasetConfig) -> DatasetResult:
        """Run all collect+validate pipelines for a dataset, optionally in parallel.

        Lifecycle:
            0. (query / df datasets) Register as temp view, validate schema
            1. Cache the dataset (if configured) — once, before any pipelines
            2. Run each (collect → validate) pipeline, sequentially or in parallel
            3. Unpersist cache (if cached)
            4. (query / df datasets) Drop the temp view

        validation_parallelism controls how many collect+validate pairs run
        concurrently. Each pipeline's collect and validate are sequential
        (validate depends on collect output), but independent pipelines
        can overlap.
        """
        temp_view_name = None
        original_query = ds_config.query
        # Logical table identity — what callers, history lookups, and the
        # system table reference. For plain table datasets this is the
        # user's ``table``. For query-backed datasets it's the dataset
        # ``name`` (no logical table exists). For df-backed datasets it's
        # the caller-supplied ``table``, captured before we swap in the
        # per-run temp view. Without this, every ``validate(df=...)``
        # would persist under a unique temp-view name and history reads
        # would cold-start forever.
        if ds_config.query:
            logical_table = ds_config.name
        else:
            logical_table = ds_config.table or ds_config.name

        # --- Query materialization phase ---
        if ds_config.query:
            if ds_config.filter:
                logger.warning(
                    "Dataset '%s': 'filter' is ignored for query-based datasets; "
                    "include filtering in the query itself.",
                    ds_config.name,
                )
            try:
                temp_view_name, query_df = self._materialize_query(ds_config)
            except Exception as e:
                rendered_sql = ds_config.query
                try:
                    rendered_sql = self.context.render(ds_config.query)
                except Exception:
                    pass
                return DatasetResult(
                    dataset_name=ds_config.name,
                    table=ds_config.name,
                    run_id=self.context.run_id,
                    run_timestamp=datetime.now(),
                    partition_ts=_resolve_run_level_partition_ts(
                        self._resolve_partition_ts_expr(ds_config), self.context
                    ),
                    validation_results=[ValidationResult(
                        validation_name="qualifire.query",
                        validation_type="engine",
                        severity=Severity.ERROR,
                        message=f"Query execution failed: {e}",
                        validation_base_name="qualifire.query",
                        details={"query": rendered_sql[:2000]},
                    )],
                )

            schema_errors = self._validate_query_schema(ds_config, query_df)
            if schema_errors:
                self._drop_temp_view(temp_view_name)
                return DatasetResult(
                    dataset_name=ds_config.name,
                    table=ds_config.name,
                    run_id=self.context.run_id,
                    run_timestamp=datetime.now(),
                    partition_ts=_resolve_run_level_partition_ts(
                        self._resolve_partition_ts_expr(ds_config), self.context
                    ),
                    validation_results=schema_errors,
                )

            # Swap in temp view as effective table for downstream collectors.
            # Preserve `partition_ts` and `description` so they survive into
            # the rebuilt config (lost otherwise — would silently disable
            # partition-anchored history reads on query datasets).
            ds_config = DatasetConfig(
                name=ds_config.name,
                description=ds_config.description,
                table=temp_view_name,
                partition_ts=ds_config.partition_ts,
                cache=ds_config.cache,
                cache_storage_level=ds_config.cache_storage_level,
                validation_parallelism=ds_config.validation_parallelism,
                validations=ds_config.validations,
                redacted_columns=ds_config.redacted_columns,
                allowlist_columns=ds_config.allowlist_columns,
            )

        # --- DataFrame materialization phase ---
        elif ds_config.df is not None:
            try:
                temp_view_name = self._materialize_df(ds_config)
            except Exception as e:
                return DatasetResult(
                    dataset_name=ds_config.name,
                    table=ds_config.table or ds_config.name,
                    run_id=self.context.run_id,
                    run_timestamp=datetime.now(),
                    partition_ts=_resolve_run_level_partition_ts(
                        self._resolve_partition_ts_expr(ds_config), self.context
                    ),
                    validation_results=[ValidationResult(
                        validation_name="qualifire.df",
                        validation_type="engine",
                        severity=Severity.ERROR,
                        message=f"DataFrame registration failed: {e}",
                        validation_base_name="qualifire.df",
                    )],
                )

            # Swap in temp view as effective table; carry filter forward so
            # validate(df=..., filter_expr=...) still applies. ``partition_ts``
            # must also survive the swap — drift / forecast validators read
            # the dataset-level partition expression to anchor their history
            # lookups; dropping it here forces them onto the run_timestamp
            # fallback, which has been removed.
            ds_config = DatasetConfig(
                name=ds_config.name,
                table=temp_view_name,
                filter=ds_config.filter,
                partition_ts=ds_config.partition_ts,
                cache=ds_config.cache,
                cache_storage_level=ds_config.cache_storage_level,
                validation_parallelism=ds_config.validation_parallelism,
                validations=ds_config.validations,
                redacted_columns=ds_config.redacted_columns,
                allowlist_columns=ds_config.allowlist_columns,
            )

        # --- Standard collect+validate phase ---
        # Resolve the partition stamp once per dataset run. Literal
        # forms ('{{ ds }}', '2026-04-28', '2026-04-28T12:00:00')
        # parse to a datetime; column refs / SQL functions return
        # None and the notifier omits the line. Resolved against the
        # run-merged expression (dataset-level wins, run-level
        # fallback) so single-dataset alerts carry the partition
        # the operator actually targeted.
        resolved_partition_ts = _resolve_run_level_partition_ts(
            self._resolve_partition_ts_expr(ds_config), self.context
        )
        ds_result = DatasetResult(
            dataset_name=ds_config.name,
            table=logical_table,
            run_id=self.context.run_id,
            run_timestamp=datetime.now(),
            partition_ts=resolved_partition_ts,
        )

        # Cache BEFORE any collect/validate work
        cached_df = None
        if ds_config.cache and ds_config.table:
            cached_df = self._cache_table(ds_config.table, ds_config.cache_storage_level)

        try:
            parallelism = ds_config.validation_parallelism
            val_configs = ds_config.validations

            # Memoize sample-collection results by (filter, collection config)
            # within this dataset so that shape + pattern configured against
            # the same sample spec see identical rows instead of independent
            # unseeded ORDER BY RAND() draws. Priming is a best-effort
            # optimization; if it fails, the fallback path in _collect()
            # writes back to the same cache under a per-key lock so concurrent
            # validators still share a single sample.
            sample_cache = _SampleCache()
            self._prime_sample_cache(ds_config, val_configs, sample_cache)

            if parallelism <= 1 or len(val_configs) <= 1:
                # Sequential
                for val_config in val_configs:
                    pipeline = self._run_single_validation(
                        ds_config, val_config, logical_table, sample_cache
                    )
                    ds_result.validation_results.extend(pipeline.validation_results)
                    ds_result.collection_results.extend(pipeline.collection_results)
            else:
                # Parallel validation execution
                pipeline_map: dict[int, PipelineResult] = {}
                with ThreadPoolExecutor(max_workers=parallelism) as pool:
                    futures = {
                        pool.submit(
                            self._run_single_validation,
                            ds_config, vc, logical_table, sample_cache,
                        ): idx
                        for idx, vc in enumerate(val_configs)
                    }
                    for future in as_completed(futures):
                        idx = futures[future]
                        try:
                            pipeline_map[idx] = future.result()
                        except Exception as e:
                            logger.error("Validation %s failed: %s", val_configs[idx].type, e)
                            pipeline_map[idx] = PipelineResult(
                                validation_results=[
                                    _validator_execution_error_result(
                                        val_configs[idx], ds_config, e,
                                    )
                                ],
                            )
                # Preserve original validation order
                for i in range(len(val_configs)):
                    ds_result.validation_results.extend(pipeline_map[i].validation_results)
                    ds_result.collection_results.extend(pipeline_map[i].collection_results)
        finally:
            if cached_df is not None:
                self._uncache(cached_df)
            if temp_view_name:
                self._drop_temp_view(temp_view_name)

        # Attach query context to results for system table traceability
        if original_query:
            rendered_sql = self.context.render(original_query)[:2000]
            for vr in ds_result.validation_results:
                vr.details.setdefault("query", rendered_sql)
            for cr in ds_result.collection_results:
                cr.metadata.setdefault("query", rendered_sql)

        return ds_result

    _DDL_DML_RE = re.compile(
        r"\b(INSERT|UPDATE|DELETE|DROP|ALTER|CREATE|TRUNCATE|MERGE|"
        r"GRANT|REVOKE|REPLACE|RENAME|COPY|LOAD|UNLOAD|SET|RESET|"
        r"MSCK|VACUUM|OPTIMIZE)\b",
        re.IGNORECASE,
    )

    @staticmethod
    def _validate_readonly_sql(sql: str) -> None:
        """Reject SQL that is not a read-only SELECT/WITH statement."""
        stripped = sql.strip()
        if not re.match(r"(?i)^\s*(SELECT|WITH)\b", stripped):
            raise ValueError(
                f"Query must be a SELECT statement, got: {stripped[:100]!r}"
            )
        if ";" in stripped:
            raise ValueError("Multiple SQL statements (semicolons) are not allowed in query field")
        if QualifireEngine._DDL_DML_RE.search(stripped):
            raise ValueError(
                "Query contains disallowed DDL/DML keywords. Only SELECT queries are permitted."
            )

    _VIEW_NAME_UNSAFE_RE = re.compile(r"[^A-Za-z0-9_]")

    def _make_view_name(self, dataset_name: str, instance_key: str = "") -> str:
        """Build a SQL-safe, collision-free, injective temp view name.

        User-provided dataset names commonly contain dots or hyphens
        (``db.prod_table``, ``orders-snapshot``). Embedding those
        verbatim produces identifiers Spark parses as qualified
        references instead of a single view name.

        Naive sanitization (map non-alphanum to ``_`` + truncate) is not
        injective: ``orders.us`` and ``orders-us`` would both collapse
        to ``orders_us``, and two long names that diverge only after the
        truncation boundary would share a prefix. Under
        ``dataset_parallelism > 1`` two datasets in the same run would
        then collide on the same temp view — one could overwrite or
        drop the other's data mid-validation.

        To preserve injectivity we mix a short sha1 digest of the
        *full, unsanitized* dataset name into the identifier. The
        sanitized prefix remains human-readable; the digest guarantees
        distinct inputs map to distinct view names. Overall length is
        kept under 64 chars so it fits within tight identifier limits
        (e.g. PostgreSQL's 63-char cap).

        The ``run_id`` fragment is also sanitized: callers can set a
        custom ``run_id`` (via ``QualifireContext(run_id=...)`` or
        ``context["run_id"]``), and human-readable values like
        ``"2026-04-20"`` or ``"prod.daily"`` would embed ``-``/``.``
        directly into the identifier. We map the raw 8-char slice
        through the same non-alphanum regex so the final view name is
        always ``[A-Za-z0-9_]+``.

        ``instance_key`` disambiguates temp-view names when two
        ``DatasetConfig`` instances share the same ``name`` within a
        single run. Duplicate dataset names are now a deprecation
        warning at config load (not a hard error), so the engine still
        has to cope — under ``dataset_parallelism > 1`` same-name
        datasets would otherwise race on one temp view. Caller passes
        a short hex fragment derived from ``id(ds_config)`` or the
        dataset's index in ``config.datasets``.
        """
        # Budget (63-char PostgreSQL limit): "_qf_"(4) + sanitized(36) +
        # "_"(1) + digest(10) + "_"(1) + instance_key(up to 4) +
        # "_"(1) + run_id(8) = 63.
        digest = hashlib.sha1(dataset_name.encode("utf-8")).hexdigest()[:10]
        sanitized = self._VIEW_NAME_UNSAFE_RE.sub("_", dataset_name)[:36]
        run_fragment = self._VIEW_NAME_UNSAFE_RE.sub("_", self.context.run_id[:8])
        if instance_key:
            # Same regex budget: instance_key is pre-hex (safe), but
            # guard anyway — callers should never exceed 4 chars.
            safe_inst = self._VIEW_NAME_UNSAFE_RE.sub("_", instance_key)[:4]
            return f"_qf_{sanitized}_{digest}_{safe_inst}_{run_fragment}"
        return f"_qf_{sanitized}_{digest}_{run_fragment}"

    def _instance_key_for(self, ds_config: DatasetConfig) -> str:
        """Short hex fragment that disambiguates a ``DatasetConfig``
        instance from any same-named sibling in ``self.config.datasets``.

        Must use **identity** (``is``), not equality: Pydantic
        ``BaseModel`` compares by value, so ``list.index(ds_config)``
        returns the same index for two value-equal ``DatasetConfig``
        objects and collapses their view names back onto each other.
        We scan the configured list for the first entry that *is*
        ``ds_config`` and fall back to ``id(ds_config)`` for datasets
        synthesized outside the config list (e.g. the WAP staging shim
        in ``Qualifire.write_audit_publish``).
        """
        try:
            for idx, cfg in enumerate(self.config.datasets):
                if cfg is ds_config:
                    return f"{idx:04x}"
        except AttributeError:
            pass
        return hashlib.sha1(str(id(ds_config)).encode("utf-8")).hexdigest()[:4]

    def _materialize_query(
        self, ds_config: DatasetConfig
    ) -> tuple[str, Any]:
        """Materialize a SQL query as a temp view and return (view_name, df)."""
        rendered_sql = self.context.render(ds_config.query)
        self._validate_readonly_sql(rendered_sql)
        view_name = self._make_view_name(ds_config.name, self._instance_key_for(ds_config))

        df = self.backend.execute_sql(rendered_sql)

        # Register through the Backend Protocol — Pandas stores in its
        # in-memory dict; Spark wraps `createOrReplaceTempView`.
        self.backend.register_table(view_name, df)

        logger.info("Materialized query for dataset '%s' as '%s'", ds_config.name, view_name)
        return view_name, df

    def _materialize_df(self, ds_config: DatasetConfig) -> str:
        """Register a pre-built DataFrame as a temp view and return the view name.

        Uses the same backend-agnostic registration branch as
        ``_materialize_query``: ``register_table`` for PandasBackend, else
        Spark ``createOrReplaceTempView``. The view name is built via
        ``_make_view_name`` so it is always a single SQL-safe identifier
        and never reuses the caller-supplied ``table``.
        """
        view_name = self._make_view_name(ds_config.name, self._instance_key_for(ds_config))
        df = ds_config.df

        self.backend.register_table(view_name, df)

        logger.info("Materialized df for dataset '%s' as '%s'", ds_config.name, view_name)
        return view_name

    def _validate_query_schema(
        self, ds_config: DatasetConfig, df: Any
    ) -> list[ValidationResult]:
        """Validate that declared dimension/measure columns exist in the query result."""
        declared = list(ds_config.dimensions or []) + list(ds_config.measures or [])
        if not declared:
            return []

        actual_columns = set(df.columns)
        errors = []
        for col in declared:
            if col not in actual_columns:
                errors.append(ValidationResult(
                    validation_name="qualifire.query_schema",
                    validation_type="engine",
                    severity=Severity.ERROR,
                    message=(
                        f"Declared column '{col}' not found in query result. "
                        f"Available: {sorted(actual_columns)}"
                    ),
                    validation_base_name="qualifire.query_schema",
                ))
        return errors

    def _drop_temp_view(self, view_name: str) -> None:
        """Drop a temp view (backend-agnostic cleanup via Protocol)."""
        try:
            self.backend.drop_temp_view(view_name)
            logger.debug("Dropped temp view '%s'", view_name)
        except Exception as e:
            logger.warning("Failed to drop temp view %s: %s", view_name, e)

    def _run_single_validation(
        self,
        ds_config: DatasetConfig,
        val_config: Any,
        logical_table: str,
        sample_cache: _SampleCache | None = None,
    ) -> PipelineResult:
        """Run one collect→validate pipeline. Thread-safe for parallel execution.

        ``logical_table`` is the caller's view of the dataset's table — used
        for history lookups and persistence so those keys stay stable even
        when ``ds_config.table`` has been swapped to a per-run temp view
        (for df/query datasets).

        ``sample_cache`` is an optional dataset-scoped dict of precomputed
        sample-collection results, keyed by a stable hash of the collection
        config. Populated once serially in ``_run_dataset`` so that
        shape + pattern on the same sample spec see identical rows.
        """
        try:
            collected = self._collect(ds_config, val_config, sample_cache)
            validation_results = self._validate(ds_config, val_config, collected, logical_table)
            validated_at = datetime.now()
            if not validation_results:
                on_empty_severity = self._parse_on_empty_data(val_config)
                base_name = _resolve_validation_base_name(val_config, ds_config)
                val_name = getattr(val_config, "name", None) or val_config.type
                validation_results = [ValidationResult(
                    validation_name=val_name,
                    validation_type=val_config.type,
                    severity=on_empty_severity,
                    message="Check produced no validation output — no data to validate",
                    validation_base_name=base_name,
                )]
            for vr in validation_results:
                vr.validated_at = validated_at
            return PipelineResult(
                validation_results=validation_results,
                collection_results=collected,
            )
        except MissingPartitionAnchorError as e:
            # Sampler / collectors signal "this rule needs a partition
            # anchor that wasn't supplied". Surface the same structured
            # ValidationResult shape drift / forecast use, so dashboards
            # and alert filters can detect the misconfiguration via
            # ``details.missing_partition_anchor`` regardless of which
            # validator family it came from.
            base_name = _resolve_validation_base_name(val_config, ds_config)
            val_name = getattr(val_config, "name", None) or val_config.type
            logger.error(
                "Missing partition anchor for %s: %s", val_config.type, e
            )
            return PipelineResult(
                validation_results=[ValidationResult(
                    validation_name=val_name,
                    validation_type=val_config.type,
                    severity=Severity.ERROR,
                    message=str(e),
                    validation_base_name=base_name,
                    details={
                        "missing_partition_anchor": True,
                        "reason": str(e),
                    },
                )],
            )
        except Exception as e:
            logger.error("Validation failed for %s: %s", val_config.type, e)
            return PipelineResult(
                validation_results=[
                    _validator_execution_error_result(val_config, ds_config, e)
                ],
            )

    def _cache_table(self, table: str, storage_level: str = "MEMORY_AND_DISK") -> Any:
        """Cache a table's DataFrame and return it for later unpersist.

        Two distinct failure modes (P4.2 split):
          - **Backend has no Spark / PySpark not installed** —
            configuration error: caller asked for Spark caching but
            the runtime cannot provide it. Raise ``RuntimeError`` with
            the install hint.
          - **Spark caching itself failed** (memory pressure, executor
            lost, etc.) — runtime degradation: log + return None so
            the validation can proceed uncached.

        PandasBackend supports ``cache=True`` semantically as a no-op
        (data is already in memory): this method returns ``None`` and
        the validation continues without caching. Detected via
        ``getattr(self.backend, "spark", None)``.
        """
        spark = getattr(self.backend, "spark", None)
        if spark is None:
            # PandasBackend or any non-Spark Backend: cache=True is a
            # no-op rather than an error.
            return None

        try:
            from pyspark import StorageLevel
        except ImportError as e:
            raise RuntimeError(
                "PySpark is required for cache=True on a Spark-capable "
                "backend; install with: pip install 'qualifire[spark]'"
            ) from e

        df = None
        try:
            level = getattr(StorageLevel, storage_level, StorageLevel.MEMORY_AND_DISK)
            df = self.backend.execute_sql(f"SELECT * FROM {table}")
            df.persist(level)
            # Force materialization so subsequent queries hit the cache
            df.count()
            logger.info("Cached table %s with storage level %s", table, storage_level)
            return df
        except Exception as e:
            logger.warning("Failed to cache table %s (Spark error): %s", table, e)
            # ``persist`` may have registered ``df`` in the cache before
            # ``count()`` failed. The caller only ``_uncache``s a non-None
            # return, so release it here to avoid orphaning it for the
            # rest of the session.
            if df is not None:
                self._uncache(df)
            return None

    @staticmethod
    def _uncache(df: Any) -> None:
        """Unpersist a cached DataFrame."""
        try:
            df.unpersist()
        except Exception:
            pass

    @staticmethod
    def _sample_cache_key(
        ds_config: DatasetConfig, collection: Any
    ) -> str:
        """Stable key for memoizing a SampleCollectionConfig run.

        Keyed by the effective table, the dataset-level filter, and the
        full collection config (pydantic JSON dump). Two validators with
        identical sample specs share the same sample; differing specs
        (different n_records, filters, history slices) produce distinct
        entries.
        """
        return "|".join([
            ds_config.table or "",
            ds_config.filter or "",
            type(collection).__name__,
            collection.model_dump_json(),
        ])

    def _prime_sample_cache(
        self,
        ds_config: DatasetConfig,
        val_configs: list[Any],
        sample_cache: _SampleCache,
    ) -> None:
        """Pre-compute every unique SampleCollectionConfig once, serially.

        Running this before the validation fan-out ensures that multiple
        sample-based validators (shape + pattern) share exactly the same
        rows — on Spark, sample_records uses ORDER BY RAND() which is
        unseeded, so independent calls return different rows and produce
        contradictory drift/anomaly results for the same dataset.

        Priming is best-effort: if a collection raises, leave the key
        unset. The fallback path in ``_collect`` will then serialize the
        retry via ``_SampleCache.get_or_collect`` so matching validators
        still share a single sample.
        """
        seen: set[str] = set()
        for vc in val_configs:
            collection = getattr(vc, "collection", None)
            if not isinstance(collection, SampleCollectionConfig):
                continue
            key = self._sample_cache_key(ds_config, collection)
            if key in seen:
                continue
            seen.add(key)
            try:
                sample_cache.data[key] = self._collect_sample(
                    ds_config, vc, collection
                )
            except Exception as e:
                logger.warning(
                    "Sample pre-collection failed for %s: %s",
                    getattr(vc, "name", None) or vc.type, e,
                )

    def _collect_sample(
        self,
        ds_config: DatasetConfig,
        val_config: Any,
        collection: Any,
    ) -> list[CollectionResult]:
        """Run a SamplerCollector and attach the collector_name tag."""
        table = ds_config.table or ""
        filter_expr = ds_config.filter
        collector_name = (
            getattr(val_config, "name", None)
            or f"{val_config.type}.{ds_config.name}"
        )
        history = collection.history
        collector = SamplerCollector(
            n_records=collection.n_records,
            slice_column=collection.slice_column,
            slice_value=collection.slice_value,
            filter_expr=filter_expr,
            past_dates=history.past_dates if history else 3,
            step=history.step if history else None,
            history_filters=history.filters if history else None,
        )
        results = collector.collect(self.backend, table, self.context)
        for cr in results:
            cr.metadata["collector_name"] = collector_name
        return results

    def _resolve_partition_ts_expr(self, ds_config: DatasetConfig) -> str | None:
        """Return the partition_ts expression for this dataset, with run-level
        fallback. Per-dataset wins; run-level kicks in when the dataset omits it.
        Returns the raw expression — Jinja rendering happens inside the collector.
        """
        if ds_config.partition_ts:
            return ds_config.partition_ts
        return getattr(self.config, "partition_ts", None)

    def _collect(
        self,
        ds_config: DatasetConfig,
        val_config: Any,
        sample_cache: _SampleCache | None = None,
    ) -> list[CollectionResult]:
        """Run the appropriate collector for a validation config.

        ``skip_recollection`` pre-pass: when the context flag is set
        AND the validator exposes ``expected_metrics()`` AND an
        active non-NULL row exists for every expected ``(metric,
        dim)`` combination at the resolved partition_ts, this
        method returns synthetic ``CollectionResult`` rows built
        from the persisted values WITHOUT running the collector.
        Validators always re-evaluate against the cached values —
        the cache short-circuits collection only.
        """
        # Pre-compose the combined filter for the three filtered
        # collector types. The value is consumed by the collector
        # dispatch below (Aggregation/Profiling/Metrics receive
        # ``filter_expr=pre_composed_filter``). NOT consumed by
        # ``_try_skip_recollection`` post skip-recollection
        # feature — the pre-pass keys on ``(table, metric,
        # partition_ts, dim)`` only and is filter-scope-agnostic
        # by design (filter not in natural key; documented
        # trade-off in CHANGELOG). ``None`` for non-filtered
        # collector types or when both filters resolve to empty.
        filter_expr = ds_config.filter
        collection_for_compose = getattr(val_config, "collection", None)
        pre_composed_filter: str | None = None
        if isinstance(
            collection_for_compose,
            (
                AggregationCollectionConfig,
                ProfilingCollectionConfig,
                MetricsCollectionConfig,
            ),
        ):
            pre_composed_filter = self._compose_collector_filter(
                ds_config, filter_expr, collection_for_compose,
            )

        cached_results = self._try_skip_recollection(
            ds_config, val_config,
        )
        if cached_results is not None:
            return cached_results

        table = ds_config.table or ""
        collector_name = getattr(val_config, "name", None) or f"{val_config.type}.{ds_config.name}"

        if isinstance(val_config, SLOValidationConfig):
            rc = val_config.recency
            collector = RecencyCollector(
                strategy=rc.strategy,
                column=rc.column,
                sql=rc.sql,
                filter_expr=filter_expr,
            )
            results = collector.collect(self.backend, table, self.context)
            for cr in results:
                cr.metadata["collector_name"] = collector_name
            return results

        collection = val_config.collection
        dimensions = getattr(collection, "dimensions", None)

        if isinstance(collection, AggregationCollectionConfig):
            collector = AggregationCollector(
                expressions=collection.expressions,
                filter_expr=pre_composed_filter,
                dimensions=dimensions,
                partition_ts_expr=self._resolve_partition_ts_expr(ds_config),
            )
        elif isinstance(collection, ProfilingCollectionConfig):
            col_profiles = None
            if collection.column_profiles:
                col_profiles = {
                    col: {"stats": cp.stats, "exclude_stats": cp.exclude_stats}
                    for col, cp in collection.column_profiles.items()
                }
            collector = ProfilingCollector(
                columns=collection.columns,
                stats=collection.stats,
                exclude_stats=collection.exclude_stats,
                column_profiles=col_profiles,
                filter_expr=pre_composed_filter,
                top_k=collection.top_k,
                quantiles=collection.quantiles,
                partition_ts_expr=self._resolve_partition_ts_expr(ds_config),
            )
        elif isinstance(collection, MetricsCollectionConfig):
            collector = MetricsCollector(
                metrics=collection.metrics,
                filter_expr=pre_composed_filter,
                dimensions=dimensions,
                partition_ts_expr=self._resolve_partition_ts_expr(ds_config),
            )
        elif isinstance(collection, SampleCollectionConfig):
            if sample_cache is not None:
                key = self._sample_cache_key(ds_config, collection)
                # get_or_collect serializes concurrent misses onto a single
                # collection and writes the result back, so matching
                # validators share the same rows even when priming failed.
                cached = sample_cache.get_or_collect(
                    key,
                    lambda: self._collect_sample(ds_config, val_config, collection),
                )
                # Relabel metadata so the stored collector_name matches
                # the consuming validation (two validators share the
                # same rows but keep their own names for traceability).
                return [
                    CollectionResult(
                        metric_name=cr.metric_name,
                        metric_value=cr.metric_value,
                        collected_at=cr.collected_at,
                        metadata={**cr.metadata, "collector_name": collector_name},
                    )
                    for cr in cached
                ]
            # Sample cache disabled entirely — collect directly.
            return self._collect_sample(ds_config, val_config, collection)
        elif isinstance(collection, CustomQueryCollectionConfig):
            collector = CustomQueryCollector(
                sql=collection.sql,
                dimensions=dimensions,
                partition_ts_expr=self._resolve_partition_ts_expr(ds_config),
            )
        else:
            raise ValueError(f"Unknown collection type: {type(collection)}")

        results = collector.collect(self.backend, table, self.context)
        for cr in results:
            cr.metadata["collector_name"] = collector_name
        return results

    def _compose_collector_filter(
        self,
        ds_config: DatasetConfig,
        ds_filter: str | None,
        collection: Any,
    ) -> str | None:
        """AND-combine the dataset-level and collector-level filters
        post-render.

        Pre-2026-05-09 the engine's three filtered collector call
        sites composed these with a Python-truthiness override
        (``collection.filter or filter_expr``) that silently
        dropped the dataset-level safety guard whenever a
        validator declared its own filter. Today's contract:

        1. Render each side independently via the engine's Jinja
           context. This catches the rendered-empty trap
           (``filter: "{{ optional }}"`` with ``optional=""``)
           that pre-render compose would otherwise produce as
           ``(rendered_ds) AND ()`` (invalid SQL).
        2. Hand the rendered values to ``and_combine_filters``,
           which normalises empty / whitespace operands to
           ``None`` before composing.
        3. The combined string flows into the collector's
           ``filter_expr`` kwarg. The collector's existing
           internal ``context.render(self.filter_expr)`` becomes
           idempotent on resolved SQL — no ``{{ }}`` markers
           remain to substitute.

        Only called for the three filtered collector types
        (``AggregationCollectionConfig``,
        ``ProfilingCollectionConfig``,
        ``MetricsCollectionConfig``). ``SampleCollectionConfig``
        composes filters internally; ``CustomQueryCollectionConfig``
        rejects ``filter`` at config load and is excluded here.

        See ``docs/features/and-combine-collector-filter/plan.md``
        Pin 7 for the design rationale.
        """
        co_filter = getattr(collection, "filter", None)
        ds_rendered = self.context.render(ds_filter) if ds_filter else None
        co_rendered = self.context.render(co_filter) if co_filter else None
        # DEBUG log carries presence flags only — never the
        # filter contents (they can include tenant identifiers /
        # PII predicates that don't belong in default DEBUG
        # output). Decision Pin 3.
        logger.debug(
            "filter compose: dataset=%s collection=%s "
            "ds_filter_present=%s co_filter_present=%s",
            ds_config.name, getattr(collection, "type", None),
            ds_filter is not None,
            co_filter is not None,
        )
        return and_combine_filters(ds_rendered, co_rendered)

    def _try_skip_recollection(
        self,
        ds_config: DatasetConfig,
        val_config: Any,
    ) -> list[CollectionResult] | None:
        """Attempt a data-presence-driven short-circuit of collection.

        Returns a list of synthetic :class:`CollectionResult` rows
        (one per ``(metric, dim)`` the validator expects, with
        ``metadata['from_cache'] = True``) when every expected
        natural key has an active non-NULL row at the resolved
        partition anchor. Returns ``None`` to fall through to the
        normal collector when any condition is unmet:

        * ``context.skip_recollection`` is False (default).
        * The validator does not expose ``expected_metrics()``
          (Pattern / AnomalyDetection / SLO fall through here —
          their compute path stays authoritative; see plan v4
          Locked Decision 7 for the SLO carve-out rationale).
        * The dataset's ``partition_ts`` is unset or doesn't render
          to a parseable timestamp literal.
        * No storage handle is configured.
        * Any expected ``(metric, dim)`` combination has no
          eligible row.
        * Dimensional collector with empty dim enumeration — treated
          as cache miss, not vacuously satisfied (codex plan-review
          R1 MAJOR).

        Filter scope is intentionally **not** in the natural key —
        a row persisted under one filter scope replays against a
        different filter scope at the same `(table, metric,
        partition_ts, dim)`. Same trade-off as ``skip_revalidation``;
        documented in CHANGELOG.
        """
        if not getattr(self.context, "skip_recollection", False):
            return None
        if not hasattr(val_config, "expected_metrics"):
            return None
        collection = getattr(val_config, "collection", None)
        if collection is None:
            return None
        if not ds_config.partition_ts:
            return None
        if self.storage is None:
            return None
        table = ds_config.table or ""
        if not table:
            return None

        # Render partition_ts and parse to a datetime. If the
        # expression is not a constant literal (e.g. `{{ ds }}` with
        # ``ds`` provided in the context), it'll render to a string
        # we can parse with ``datetime.fromisoformat``.
        try:
            anchor_str = self.context.render(ds_config.partition_ts).strip().strip("'\"")
            anchor = datetime.fromisoformat(anchor_str)
        except Exception:
            return None

        collector_name = (
            getattr(val_config, "name", None)
            or f"{val_config.type}.{ds_config.name}"
        )
        # Determine dim enumeration up-front: dimensional collector
        # → enumerate persisted dim values per metric; non-dim →
        # the single None placeholder.
        is_dimensional = bool(getattr(collection, "dimensions", None))
        results: list[CollectionResult] = []
        for metric_name in val_config.expected_metrics():
            try:
                if is_dimensional:
                    dim_values = self.storage.read_collection_dim_values_at_partition(
                        table_name=table,
                        metric_name=metric_name,
                        partition_ts=anchor,
                    )
                else:
                    dim_values = [None]
            except Exception:
                logger.debug(
                    "skip_recollection: dim enumeration failed for %s/%s; "
                    "falling through to collector",
                    table, metric_name,
                )
                return None
            if not dim_values:
                # Codex plan-review R1 MAJOR: empty enumeration =
                # cache miss (cold partition / new dim), NOT
                # vacuously satisfied. Fall through to collector.
                return None
            for dim_value in dim_values:
                try:
                    cached = self.storage.read_collection_metric_at_partition(
                        table_name=table,
                        metric_name=metric_name,
                        anchor_ts=anchor,
                        dimension_value=dim_value,
                    )
                except Exception:
                    logger.debug(
                        "skip_recollection: storage lookup failed for %s/%s/%s; "
                        "falling through to collector",
                        table, metric_name, dim_value,
                    )
                    return None
                if cached is None:
                    return None
                collected_at = cached.get("collected_at")
                collected_at_dt = (
                    collected_at if isinstance(collected_at, datetime)
                    else (datetime.fromisoformat(str(collected_at))
                          if collected_at else datetime.now())
                )
                results.append(CollectionResult(
                    metric_name=metric_name,
                    metric_value=cached.get("metric_value"),
                    collected_at=collected_at_dt,
                    metadata={
                        "table": table,
                        "collection_type": "aggregation",
                        "collector_name": collector_name,
                        "from_cache": True,
                    },
                    partition_ts=anchor,
                    dimension_value=dim_value,
                ))

        # Mirror the cache hits to context.cached_metrics for
        # downstream observability / report generation.
        if self.context.cached_metrics is None:
            self.context.cached_metrics = {}
        for r in results:
            self.context.cached_metrics[
                (table, r.metric_name, anchor.isoformat(), r.dimension_value)
            ] = {"metric_value": r.metric_value, "from_cache": True}
        logger.info(
            "skip_recollection: short-circuited %d row(s) for "
            "validator %r on dataset %r",
            len(results), collector_name, ds_config.name,
        )
        return results

    def _try_skip_revalidation(
        self,
        ds_config: DatasetConfig,
        val_config: Any,
        base_name: str,
    ) -> list[ValidationResult] | None:
        """Pre-pass for the ``skip_revalidation`` runtime flag.

        Returns ``None`` and falls through to the validator when:
          - ``self.context.skip_revalidation`` is False (the gate)
          - ``self.storage`` is None (no persisted state)
          - The dataset's resolved ``partition_ts`` can't be
            computed (no anchor → can't do an exact-partition
            lookup)
          - ``read_validations_at_partition`` returns an empty list

        Otherwise returns a list of synthetic ``ValidationResult``s
        rebuilt from the persisted rows, each carrying
        ``details["from_cache"] = True`` so downstream consumers can
        distinguish replay from re-compute.
        """
        if not getattr(self.context, "skip_revalidation", False):
            return None
        if self.storage is None:
            return None
        anchor = _resolve_run_level_partition_ts(
            self._resolve_partition_ts_expr(ds_config), self.context,
        )
        if anchor is None:
            return None

        try:
            rows = self.storage.read_validations_at_partition(
                dataset_name=ds_config.name or "",
                validation_name_prefix=base_name,
                partition_ts=anchor,
            )
        except Exception as e:
            logger.warning(
                "skip_revalidation: storage lookup failed for %s/%s: %s",
                ds_config.name, base_name, e,
            )
            return None
        if not rows:
            return None

        results = [self._row_to_validation_result(row, base_name) for row in rows]
        logger.info(
            "skip_revalidation: replayed %d row(s) for %s/%s @ %s",
            len(results), ds_config.name, base_name, anchor,
        )
        return results

    @staticmethod
    def _row_to_validation_result(
        row: dict[str, Any], base_name: str,
    ) -> ValidationResult:
        """Rebuild a ValidationResult from a persisted system-table row.

        Per skip-revalidation Locked Decision 7: explicit per-field
        mapping. Severity string parses into the enum;
        expected_value / details_json are JSON-decoded.
        validation_base_name is derived from the current val_config
        (passed in as ``base_name``) — not in the persisted row.
        """
        status_str = row.get("validation_status") or ""
        try:
            severity = Severity(status_str)
        except ValueError:
            severity = Severity.WARNING

        expected_raw = row.get("expected_value")
        expected_value: Any = None
        if expected_raw is not None:
            try:
                expected_value = json.loads(expected_raw)
            except Exception:
                expected_value = expected_raw  # leave as string

        details_raw = row.get("details_json")
        details: dict = {}
        if isinstance(details_raw, dict):
            details = dict(details_raw)
        elif isinstance(details_raw, str) and details_raw:
            try:
                decoded = json.loads(details_raw)
                if isinstance(decoded, dict):
                    details = decoded
            except Exception:
                pass
        details["from_cache"] = True

        metric_value = row.get("metric_value")
        try:
            actual_value = float(metric_value) if metric_value is not None else None
        except (TypeError, ValueError):
            actual_value = None

        return ValidationResult(
            validation_name=row.get("validation_name") or base_name,
            validation_type=row.get("validation_type") or "engine",
            severity=severity,
            message=row.get("validation_message") or "",
            metric_name=row.get("metric_name"),
            expected_value=expected_value,
            actual_value=actual_value,
            actual_value_text=row.get("actual_value_text"),
            details=details,
            validation_base_name=base_name,
            dimension_value=row.get("dimension_value"),
        )

    @staticmethod
    def _parse_on_empty_data(val_config: Any) -> Severity:
        """Map the config string to a Severity enum value."""
        _ON_EMPTY_MAP = {"pass": Severity.PASS, "warning": Severity.WARNING, "error": Severity.ERROR}
        raw = getattr(val_config, "on_empty_data", "warning")
        severity = _ON_EMPTY_MAP.get(raw)
        if severity is None:
            raise ValueError(
                f"Invalid on_empty_data value: {raw!r}. Must be one of: 'pass', 'warning', 'error'"
            )
        return severity

    def _validate(
        self,
        ds_config: DatasetConfig,
        val_config: Any,
        collected: list[CollectionResult],
        logical_table: str,
    ) -> list[ValidationResult]:
        """Run the appropriate validator.

        ``logical_table`` is threaded through so history-backed validators
        key their lookups and writes on the caller's stable identifier
        rather than a per-run temp view name.
        """
        from qualifire.core._redaction import RedactionPolicy

        on_empty_data = self._parse_on_empty_data(val_config)
        base_name = _resolve_validation_base_name(val_config, ds_config)

        # skip-revalidation pre-pass: when the runtime flag is set
        # and active validation rows exist at the resolved
        # partition_ts for this validator's base name, replay them
        # as ValidationResults instead of running the validator's
        # compute. Returns None when the flag is off, storage is
        # missing, no anchor resolves, or no rows are found —
        # falls through to validator dispatch.
        cached = self._try_skip_revalidation(
            ds_config, val_config, base_name,
        )
        if cached is not None:
            return cached
        # webhook-payload-redaction: build the per-dataset effective
        # redaction policy from instance-level (carried on the
        # context) + dataset-level lists. Only SHAP-emitting
        # validators (PatternCheck / IsolationForest) consume it;
        # others ignore.
        redaction_policy = RedactionPolicy.build(
            instance_denylist=getattr(
                self.context, "instance_redacted_columns", None,
            ),
            instance_allowlist=getattr(
                self.context, "instance_allowlist_columns", None,
            ),
            dataset_denylist=getattr(ds_config, "redacted_columns", None),
            dataset_allowlist=getattr(ds_config, "allowlist_columns", None),
        )

        if isinstance(val_config, SLOValidationConfig):
            validator = SLOValidator(
                warning_duration=val_config.thresholds.get("warning"),
                error_duration=val_config.thresholds.get("error"),
                name=base_name,
                on_empty_data=on_empty_data,
            )
        elif isinstance(val_config, ThresholdValidationConfig):
            # ``exclude_none=True`` strips unset operator fields (min,
            # max, eq, neq, gt, lt, gte, lte) and unused threshold
            # levels (warning / error) so ``expected_value`` only
            # carries the bounds the operator actually set.
            rules = [
                r.model_dump(exclude_none=True) for r in val_config.rules
            ]
            validator = ThresholdValidator(
                rules=rules,
                name=base_name,
                on_empty_data=on_empty_data,
            )
        elif isinstance(val_config, HistoricalValidationConfig):
            rules = [r.model_dump() for r in val_config.rules]
            validator = HistoricalValidator(
                rules=rules,
                storage=self.storage,
                table_name=logical_table,
                name=base_name,
                on_empty_data=on_empty_data,
            )
        elif isinstance(val_config, ForecastValidationConfig):
            rules = [r.model_dump() for r in val_config.rules]
            validator = ForecastValidator(
                rules=rules,
                storage=self.storage,
                table_name=logical_table,
                name=base_name,
                on_empty_data=on_empty_data,
            )
        elif isinstance(val_config, AnomalyDetectionValidationConfig):
            model = val_config.model
            thresholds = val_config.thresholds
            warning_score = (thresholds.warning or {}).get("anomaly_score", 0.6)
            error_score = (thresholds.error or {}).get("anomaly_score", 0.8)
            validator = IsolationForestValidator(
                n_estimators=model.n_estimators,
                contamination=model.contamination,
                warning_threshold=warning_score,
                error_threshold=error_score,
                explain=model.explain,
                drop_complex=model.drop_complex,
                alert_on_schema_change=model.alert_on_schema_change,
                exclude_columns=_with_auto_slice_column_excluded(
                    model.exclude_columns, val_config.collection,
                ),
                name=base_name,
                on_empty_data=on_empty_data,
                on_missing_history=model.on_missing_history,
                explain_value_drift=model.explain_value_drift,
                drift_breakdown_by_slice=getattr(
                    model, "drift_breakdown_by_slice", False,
                ),
                redaction_policy=redaction_policy,
            )
        elif isinstance(val_config, PatternValidationConfig):
            model = val_config.model
            thresholds = val_config.thresholds
            warning_auc = (thresholds.warning or {}).get("auc", 0.65)
            error_auc = (thresholds.error or {}).get("auc", 0.80)
            validator = PatternCheckValidator(
                n_estimators=model.n_estimators,
                max_depth=model.max_depth,
                class_weight=model.class_weight,
                random_state=model.random_state,
                cv_folds=model.cv_folds,
                warning_threshold=warning_auc,
                error_threshold=error_auc,
                explain=model.explain,
                drop_complex=model.drop_complex,
                alert_on_schema_change=model.alert_on_schema_change,
                exclude_columns=_with_auto_slice_column_excluded(
                    model.exclude_columns, val_config.collection,
                ),
                name=base_name,
                on_empty_data=on_empty_data,
                on_missing_history=model.on_missing_history,
                explain_value_drift=model.explain_value_drift,
                drift_breakdown_by_slice=getattr(
                    model, "drift_breakdown_by_slice", False,
                ),
                redaction_policy=redaction_policy,
            )
        else:
            raise ValueError(f"Unknown validation type: {type(val_config)}")

        return validator.validate(collected)

    # --- Engine-warning identities reserved for the suppression contract -----
    _ENGINE_DATASET_NAME = "qualifire.engine"
    _RESERVED_ENGINE_WARNINGS = (
        "qualifire.persistence",
        "qualifire.suppression_read",
        "qualifire.wap_cleanup",
    )

    def _prefetch_suppression_snapshot(
        self, result: QualifireResult
    ) -> dict[ValidationKey, list[dict[str, Any]]]:
        """Pre-fetch the suppression snapshot before any current-run write.

        Snapshot collects keys from THREE sources:
          (i) every dataset's ValidationResults,
          (ii) any engine_warnings already accumulated (e.g. WAP
              cleanup warnings drained from `_run_wap`),
          (iii) well-known engine warning keys that may be emitted
              later in the run (persistence + suppression-read).

        On `storage is None`: returns an empty dict and does NOT
        emit a `qualifire.suppression_read` warning (storage being
        absent is a configuration choice, not an observability
        failure). On storage configured but the read raises:
        returns an empty dict AND appends a
        `qualifire.suppression_read` engine warning to the result.

        Gate on ``self.context.skip_renotification``: when the
        runtime flag is False (the default), the snapshot is never
        consumed by ``_send_grouped_notifications``, so the bulk
        read is wasted work. Short-circuit to ``{}`` to avoid the
        per-run round-trip on every default invocation.
        """
        if self.storage is None:
            return {}
        if not getattr(self.context, "skip_renotification", False):
            return {}

        keys: list[ValidationKey] = []
        seen: set[tuple[str, str, str | None, str]] = set()

        def _add(k: ValidationKey) -> None:
            sig = (k.dataset_name, k.validation_name, k.metric_name, k.dimension_value)
            if sig in seen:
                return
            seen.add(sig)
            keys.append(k)

        for ds in result.datasets:
            for vr in ds.validation_results:
                _add(validation_key_for(ds.dataset_name, vr))
        for vr in result.engine_warnings:
            _add(validation_key_for(self._ENGINE_DATASET_NAME, vr))
        for warn_name in self._RESERVED_ENGINE_WARNINGS:
            _add(ValidationKey(
                dataset_name=self._ENGINE_DATASET_NAME,
                validation_name=warn_name,
                metric_name=None,
                dimension_value=None,
            ))

        try:
            return self.storage.read_validation_history_bulk(keys, limit=1)
        except Exception as e:
            logger.error("Suppression pre-fetch failed: %s", e)
            result.engine_warnings.append(ValidationResult(
                validation_name="qualifire.suppression_read",
                validation_type="engine",
                severity=Severity.WARNING,
                message=f"Suppression history unavailable; alerts may re-fire: {e}",
                validation_base_name="qualifire.suppression_read",
                details={"error": str(e)},
            ))
            return {}

    @staticmethod
    def _is_suppressed(
        snapshot: dict[ValidationKey, list[dict[str, Any]]],
        key: ValidationKey,
        severity: Severity,
    ) -> bool:
        """Snapshot-based suppression. Returns True if the most
        recent prior history row for this key carries the same
        severity (the historical dedup contract from
        `notification.base.should_suppress`)."""
        history = snapshot.get(key)
        if not history:
            return False
        last_status = history[0].get("validation_status", "")
        return last_status == severity.value

    def _send_grouped_notifications(
        self,
        result: QualifireResult,
        snapshot: dict[ValidationKey, list[dict[str, Any]]] | None = None,
    ) -> list[NotificationResult]:
        """Send notifications grouped by (channel, severity, base_name).

        Per-validation routing: a (channel, severity, base_name)
        triple gets ONE notification; the payload contains only the
        validation results matching that base_name across all
        affected datasets. Cross-dataset grouping is preserved
        (one Slack message for two datasets sharing the same
        check + channel + severity).

        Per-key suppression (only when the runtime
        ``skip_renotification`` flag is True): each rule-emitted
        result is checked individually against the pre-fetched
        snapshot; the gate is global per-run, not per-validation.

        Engine warnings (qualifire.engine) route via
        `config.engine_notify`; on persistence-failure runs,
        the warning is appended after the normal data persist
        attempt and before notifications fire.
        """
        snapshot = snapshot or {}
        notifications: list[NotificationResult] = []

        # Build a synthetic engine dataset+config so the rest of
        # the routing code reuses one path. Its validations carry
        # the engine-level routing (config.engine_notify); the
        # runtime ``skip_renotification`` flag is checked below.
        eng_notify = self.config.engine_notify or NotifyConfig()
        engine_ds_config = self._build_synthetic_engine_dataset_config(
            result, eng_notify
        )
        engine_ds_result = DatasetResult(
            dataset_name=self._ENGINE_DATASET_NAME,
            table=None,
            validation_results=list(result.engine_warnings),
            run_id=result.run_id,
        )

        # Pair every dataset_config with its dataset_result (engine
        # synthetic last so engine warnings render after datasets).
        ds_pairs: list[tuple[Any, DatasetResult]] = list(
            zip(self.config.datasets, result.datasets)
        )
        if engine_ds_result.validation_results:
            ds_pairs.append((engine_ds_config, engine_ds_result))

        # Map (channel_name, severity, base_name) -> list of (DatasetResult-with-filtered-results)
        groups: dict[tuple[str, Severity, str], list[DatasetResult]] = {}

        for ds_config, ds_result in ds_pairs:
            # Index ValidationResult by base_name (single iteration over
            # validation_results — robust against duplicates).
            results_by_base: dict[str, list[ValidationResult]] = {}
            for vr in ds_result.validation_results:
                results_by_base.setdefault(vr.validation_base_name, []).append(vr)

            # Engine-emitted dataset rows (qualifire.query, qualifire.df,
            # qualifire.dataset_error, qualifire.query_schema) live in
            # ds_result.validation_results but have no matching val_config.
            # Route them via the union of all val_configs' error channels
            # so the dataset's owners still see the failure. We synthesize
            # a per-dataset "validation config" for each engine-error
            # base name and feed it into the same routing loop below.
            expected_bases = {
                self._validation_config_base_name(vc, ds_config)
                for vc in ds_config.validations
            }
            engine_emitted_bases = [
                b for b in results_by_base
                if b.startswith("qualifire.") and b not in expected_bases
            ]
            if engine_emitted_bases:
                # Build a union NotifyConfig from all val_configs in the
                # dataset; engine errors propagate to every channel that
                # the dataset's checks might route to. Engine warnings
                # configured at the top-level (engine_notify) are ALSO
                # added so dedicated engine routing reaches these rows.
                merged = NotifyConfig()
                for vc in ds_config.validations:
                    n = vc.notify
                    merged = NotifyConfig(
                        on_success=list({*merged.on_success, *n.on_success}),
                        warning=list({*merged.warning, *n.warning}),
                        error=list({*merged.error, *n.error}),
                    )
                eng_top = self.config.engine_notify
                if eng_top is not None:
                    merged = NotifyConfig(
                        on_success=list({*merged.on_success, *eng_top.on_success}),
                        warning=list({*merged.warning, *eng_top.warning}),
                        error=list({*merged.error, *eng_top.error}),
                    )

                @dataclass
                class _SyntheticEngineVC:
                    name: str
                    notify: NotifyConfig

                # Inline-extend the iteration with synthetic configs.
                ds_config_extra_validations = [
                    _SyntheticEngineVC(name=b, notify=merged)
                    for b in engine_emitted_bases
                ]
            else:
                ds_config_extra_validations = []

            # For each validation config in this dataset, decide routing
            # using the config's own `notify` and the runtime
            # ``skip_renotification`` gate.
            for val_config in list(ds_config.validations) + ds_config_extra_validations:
                base = (
                    val_config.name
                    if hasattr(val_config, "name") and getattr(val_config, "name", None) and val_config in ds_config_extra_validations
                    else self._validation_config_base_name(val_config, ds_config)
                )
                vrs = results_by_base.get(base, [])
                if not vrs:
                    continue
                notify_cfg = val_config.notify
                channels_by_severity: dict[Severity, list[str]] = {
                    Severity.PASS: list(notify_cfg.on_success),
                    Severity.WARNING: list(notify_cfg.warning),
                    Severity.ERROR: list(notify_cfg.error),
                }
                # Runtime gate: filter against the prior-history
                # snapshot only when the operator opts in via
                # Qualifire.run_config(skip_renotification=True) /
                # qualifire run --skip-renotification. Default-False
                # means "always re-page on retry" — explicit opt-in
                # for backfill replays / CI rehearsals.
                suppress_flag = getattr(self.context, "skip_renotification", False)

                # Group payloads per (channel, severity).
                for severity in (Severity.PASS, Severity.WARNING, Severity.ERROR):
                    channels = channels_by_severity.get(severity, [])
                    if not channels:
                        continue
                    # Exact severity match: ERROR-only results route to the
                    # ERROR channel only, not also to WARNING. Pre-fix this
                    # used `vr.severity >= severity` which sent ERROR rows
                    # to both warning and error tiers. PR-6 review.
                    #
                    # PLUS: filter qualifire-internal-failure rows
                    # (Phase 2 of external-catalog-system-table-
                    # hardening). Persistence-infrastructure failures
                    # and validator-execution exceptions are still
                    # severity=ERROR (so the run raises) but they
                    # represent qualifire bugs / outages, not data-
                    # quality findings. Sending Slack/email pages for
                    # them produces false-positive alerts.
                    matching_vrs = [
                        vr for vr in vrs
                        if vr.severity == severity
                        and not _is_qualifire_internal_failure(vr)
                    ]
                    if not matching_vrs:
                        continue
                    if suppress_flag and severity != Severity.PASS:
                        # Per-key suppression. Filter out individual
                        # ValidationKeys that already have a matching
                        # severity in history; keep the un-seen rows so
                        # partial-new alerts still fire (R1 review fix:
                        # all-or-nothing was wrong — US suppressed +
                        # EU new previously dropped both).
                        unsuppressed = [
                            vr for vr in matching_vrs
                            if not self._is_suppressed(
                                snapshot,
                                validation_key_for(ds_result.dataset_name, vr),
                                severity,
                            )
                        ]
                        suppressed_count = len(matching_vrs) - len(unsuppressed)
                        if suppressed_count and not unsuppressed:
                            # Everything skipped → emit the synthetic
                            # `skipped` notification record and continue.
                            notifications.append(NotificationResult(
                                channel="*",
                                severity=severity,
                                status="skipped",
                                message=(
                                    f"Duplicate alert skipped for "
                                    f"{ds_result.dataset_name}/{base}"
                                ),
                            ))
                            continue
                        matching_vrs = unsuppressed

                    filtered_ds = DatasetResult(
                        dataset_name=ds_result.dataset_name,
                        table=ds_result.table,
                        validation_results=matching_vrs,
                        run_id=ds_result.run_id,
                        # Preserve partition stamp into the routing
                        # snapshot — the notifier renders it just
                        # below ``Table:`` so an alert says *which*
                        # partition tripped the check, not just
                        # which dataset.
                        partition_ts=ds_result.partition_ts,
                    )
                    for ch_name in channels:
                        groups.setdefault((ch_name, severity, base), []).append(filtered_ds)

        # Send one notification per (channel, severity, base_name) group.
        for (ch_name, severity, _base), datasets in groups.items():
            notifier = self.notifiers.get(ch_name)
            if not notifier:
                logger.warning("Notifier '%s' not configured", ch_name)
                continue

            if len(datasets) == 1:
                summary = datasets[0]
            else:
                merged = []
                for ds in datasets:
                    merged.extend(ds.validation_results)
                summary = DatasetResult(
                    dataset_name=f"{len(datasets)} dataset(s)",
                    table=None,
                    validation_results=merged,
                    run_id=result.run_id,
                )

            nr = notifier.send(
                summary, severity, self.config.owner, self.config.bu,
                datasets=datasets if len(datasets) > 1 else None,
            )
            nr.notified_at = datetime.now()
            notifications.append(nr)

        return notifications

    @staticmethod
    def _validation_config_base_name(val_config: Any, ds_config: DatasetConfig) -> str:
        """Resolve the base name a validation_config will emit under."""
        return _resolve_validation_base_name(val_config, ds_config)

    def _build_synthetic_engine_dataset_config(
        self,
        result: QualifireResult,
        eng_notify: NotifyConfig,
    ) -> Any:
        """Build a stand-in DatasetConfig-like object for engine warnings.

        Carries one synthetic ValidationConfig per emitted base
        name so the same routing/suppression code path applies.
        Uses lightweight stand-ins (dataclass / dict) — engine
        warnings don't go through validators or Pydantic.
        """
        bases = sorted({vr.validation_base_name for vr in result.engine_warnings})

        @dataclass
        class _SyntheticVC:
            name: str
            notify: NotifyConfig

        @dataclass
        class _SyntheticDC:
            name: str
            validations: list[Any]

        validations = [
            _SyntheticVC(name=b, notify=eng_notify)
            for b in bases
        ]
        return _SyntheticDC(name=self._ENGINE_DATASET_NAME, validations=validations)

    # --- Persistence (two-pass per P4.1) ----------------------------------------

    def _persist_results(self, result: QualifireResult) -> None:
        """Backward-compatible wrapper: persists data rows + notification
        rows in one call. The new orchestration flow uses
        ``_persist_data_rows`` + ``_persist_notification_rows``
        directly so it can pre-fetch the suppression snapshot
        between them; this method exists for test fixtures that call
        the persistence step directly.
        """
        self._persist_data_rows(result)
        self._persist_notification_rows(result)

    def _persist_data_rows(self, result: QualifireResult) -> PersistenceOutcome:
        """Persist validation + collection rows for datasets and engine
        warnings.

        Returns a structured ``PersistenceOutcome``:
        * ``OK`` — rows persisted (or no rows / no storage).
        * ``ROW_LEVEL_WARNING`` — reserved future-use; today's
          all-or-nothing path doesn't emit this. Storage backends
          that support partial-failure semantics will populate it.
        * ``INFRA_FAILURE`` — entire write failed; system table
          unreachable. Captures the original exception in
          ``outcome.exc`` so the engine main loop can re-raise with
          PEP 3134 chaining.

        On ``INFRA_FAILURE`` the engine warning is also tagged with
        the qualifire-internal-failure marker so the notification
        dispatcher's predicate filters it out.
        """
        if not self.storage:
            return PersistenceOutcome(kind=PersistenceOutcomeKind.OK)
        rows = self._build_validation_and_collection_rows(result)
        if not rows:
            return PersistenceOutcome(kind=PersistenceOutcomeKind.OK)
        try:
            self.storage.write_results(rows)
            return PersistenceOutcome(kind=PersistenceOutcomeKind.OK)
        except Exception as e:
            logger.error("Failed to persist data rows: %s", e)
            result.engine_warnings.append(ValidationResult(
                validation_name="qualifire.persistence",
                validation_type="engine",
                severity=Severity.WARNING,
                message=f"System-table write failed: {e}",
                validation_base_name="qualifire.persistence",
                details={
                    "qualifire_internal_failure": True,
                    "error": str(e),
                },
            ))
            return PersistenceOutcome(
                kind=PersistenceOutcomeKind.INFRA_FAILURE,
                exc=e,
            )

    def _best_effort_persist_engine_warning(self, result: QualifireResult) -> None:
        """Pass B retry: persist just the qualifire.persistence warning
        row(s) appended after Pass A failure. If Pass B fails too, log
        and proceed (Pass A failure already in result; future runs
        may also lack the row and cold-start).
        """
        if not self.storage or not result.engine_warnings:
            return
        # Persist only engine_warnings (already-persisted dataset rows
        # would double-write). Filter to warnings appended *after*
        # the data-pass — currently this means qualifire.persistence
        # rows. Defensive check: persist all engine_warnings
        # rebuilds anyway, idempotent on the system table since
        # rows are uniquely identified by (run_id, ...).
        rows = []
        for vr in result.engine_warnings:
            rows.append(self._engine_warning_row(result, vr))
        try:
            self.storage.write_results(rows)
        except Exception as e:
            logger.error("Best-effort engine-warning persist also failed: %s", e)

    def _persist_notification_rows(self, result: QualifireResult) -> None:
        """Persist notification audit rows. If this fails, log only —
        the run already returned successfully on the validation side.
        """
        if not self.storage or not result.notifications:
            return
        rows = []
        for nr in result.notifications:
            rows.append({
                "run_id": result.run_id,
                "run_timestamp": datetime.now().isoformat(),
                "owner": result.owner,
                "bu": result.bu,
                "dataset_name": None,
                "table_name": None,
                "metric_name": None,
                "metric_value": None,
                "collection_type": None,
                "validation_name": None,
                "validation_type": None,
                "validation_status": None,
                "validation_message": None,
                "notification_channel": nr.channel,
                "notification_status": nr.status,
                "details_json": {"message": nr.message},
                "record_type": "notification",
                "collector_name": None,
                "dimension_value": None,
                "collected_at": None,
                "validated_at": None,
                "notified_at": nr.notified_at.isoformat() if nr.notified_at else None,
                "partition_ts": None,
                "expected_value": None,
                "actual_value_text": None,
                "dataset_description": None,
                "validation_description": None,
                "is_active": "true",
            })
        try:
            self.storage.write_results(rows)
        except Exception as e:
            logger.error("Notification-row persistence failed: %s", e)

    @staticmethod
    def _json_safe_or_str(v: Any) -> Any:
        """Coerce a value to a JSON-serializable form.

        Used by ``_tag_with_backfill`` to keep non-JSON-serializable
        payloads (datetime, custom objects) from breaking the
        downstream ``json.dumps`` in the persistence layer when
        wrapped under ``raw_details``. Falls through to ``str(v)``
        for anything ``json.dumps`` can't handle natively.
        """
        import json as _json
        try:
            _json.dumps(v)
            return v
        except (TypeError, ValueError):
            return str(v)

    def _tag_with_backfill(self, details: Any) -> Any:
        """Stamp ``details_json.collected_via='backfill'`` when the
        engine ran in backfill mode.

        Plan invariant N12 / D12: every row a backfill emits carries
        the breadcrumb so dashboards / consumers can distinguish
        retrofitted rows from forward runs.

        ``details`` may be:
        - dict — augment in place (return a copy with the marker).
        - None — return the marker dict.
        - JSON string (rare; some validators / engine paths
          pre-stringify). Parse, augment, re-encode. If JSON parse
          fails, fall back to wrapping the original in a dict so
          the marker is still emitted (operators get the
          breadcrumb; the original payload is preserved under
          ``raw_details``).
        - Anything else: wrap in a marker dict so the breadcrumb
          surfaces no matter what shape arrived.
        """
        if not getattr(self.context, "backfill", False):
            return details
        if details is None:
            return {"collected_via": "backfill"}
        if isinstance(details, dict):
            new = dict(details)
            new.setdefault("collected_via", "backfill")
            return new
        if isinstance(details, str):
            # Try to parse as JSON; on success re-augment + re-emit
            # as dict (the persistence layer json.dumps's downstream
            # of this method, so we hand it a dict either way).
            import json as _json
            try:
                decoded = _json.loads(details)
            except (TypeError, ValueError):
                decoded = None
            if isinstance(decoded, dict):
                decoded.setdefault("collected_via", "backfill")
                return decoded
            # Non-JSON-decodable string (or JSON that wasn't a dict
            # — e.g., a list / scalar). Preserve under a sub-key so
            # operators still see the breadcrumb without losing the
            # original payload. Strings are JSON-serializable so no
            # coercion needed.
            return {
                "collected_via": "backfill",
                "raw_details": details,
            }
        # Anything else (list / scalar / object): wrap so the
        # breadcrumb surfaces. Coerce ``raw_details`` to a string so
        # the persistence layer's downstream ``json.dumps`` can't
        # explode on non-JSON-serializable payloads (e.g., datetime,
        # custom objects). Plan N12 invariant: every backfill row
        # carries the breadcrumb; persistence must not hard-fail.
        return {
            "collected_via": "backfill",
            "raw_details": self._json_safe_or_str(details),
        }

    def _build_validation_and_collection_rows(
        self, result: QualifireResult
    ) -> list[dict[str, Any]]:
        """Build the row payload for Pass A (data + pre-persist
        engine warnings)."""
        rows: list[dict[str, Any]] = []
        # Map dataset_name → (description, run-level partition_ts, validations)
        # so we can stamp dataset_description/validation_description and the
        # run-level partition fallback on every row without re-scanning the
        # config per row.
        ds_descriptions: dict[str, str | None] = {}
        ds_run_partitions: dict[str, datetime | None] = {}
        ds_validation_descriptions: dict[
            tuple[str, str], str | None
        ] = {}  # (dataset_name, validation_base_name) → description
        for ds_cfg in self.config.datasets:
            ds_descriptions[ds_cfg.name] = ds_cfg.description
            run_part = _resolve_run_level_partition_ts(
                self._resolve_partition_ts_expr(ds_cfg), self.context,
            )
            ds_run_partitions[ds_cfg.name] = run_part
            for vc in ds_cfg.validations:
                vc_name = getattr(vc, "name", None) or getattr(vc, "type", None) or ""
                ds_validation_descriptions[(ds_cfg.name, vc_name)] = getattr(
                    vc, "description", None,
                )

        def _validation_description(ds_name: str, vr: ValidationResult) -> str | None:
            # Prefer a match on the validation's base name; fall back to the
            # full name (covers per-rule emissions like ``check.row_count``).
            return (
                ds_validation_descriptions.get((ds_name, vr.validation_base_name))
                or ds_validation_descriptions.get((ds_name, vr.validation_name))
            )

        for ds in result.datasets:
            run_part = ds_run_partitions.get(ds.dataset_name)
            ds_desc = ds_descriptions.get(ds.dataset_name)
            for vr in ds.validation_results:
                # Per-row partition_ts wins; otherwise use the dataset's
                # resolved run-level value. SLO/sample/empty-data rows that
                # never went through an aggregation collector still get a
                # partition stamp this way.
                row_part = (
                    _partition_ts_for_validation(vr, ds) or run_part
                )
                rows.append({
                    "run_id": result.run_id,
                    "run_timestamp": ds.run_timestamp.isoformat() if ds.run_timestamp else None,
                    "owner": result.owner,
                    "bu": result.bu,
                    "dataset_name": ds.dataset_name,
                    "table_name": ds.table,
                    "metric_name": vr.metric_name,
                    "metric_value": vr.actual_value if isinstance(vr.actual_value, (int, float)) else None,
                    "collection_type": vr.validation_type,
                    "validation_name": vr.validation_name,
                    "validation_type": vr.validation_type,
                    "validation_status": vr.severity.value,
                    "validation_message": vr.message,
                    "notification_channel": None,
                    "notification_status": None,
                    "details_json": self._tag_with_backfill(vr.details),
                    "record_type": "validation",
                    "collector_name": None,
                    "dimension_value": vr.dimension_value,
                    "collected_at": None,
                    "validated_at": vr.validated_at.isoformat() if vr.validated_at else None,
                    "notified_at": None,
                    "partition_ts": _format_partition_ts(row_part),
                    "expected_value": vr.expected_value,
                    "actual_value_text": vr.actual_value_text or (
                        str(vr.actual_value)
                        if vr.actual_value is not None and not isinstance(vr.actual_value, (int, float))
                        else None
                    ),
                    "dataset_description": ds_desc,
                    "validation_description": _validation_description(ds.dataset_name, vr),
                    "is_active": "true",
                })
            for cr in ds.collection_results:
                if cr.metric_value is None:
                    continue
                metric_value = cr.metric_value
                if isinstance(metric_value, datetime):
                    metric_value = metric_value.timestamp()
                if not isinstance(metric_value, (int, float)):
                    continue
                rows.append({
                    "run_id": result.run_id,
                    "run_timestamp": ds.run_timestamp.isoformat() if ds.run_timestamp else None,
                    "owner": result.owner,
                    "bu": result.bu,
                    "dataset_name": ds.dataset_name,
                    "table_name": ds.table,
                    "metric_name": cr.metric_name,
                    "metric_value": float(metric_value),
                    "collection_type": cr.metadata.get("collection_type"),
                    "validation_name": None,
                    "validation_type": None,
                    "validation_status": None,
                    "validation_message": None,
                    "notification_channel": None,
                    "notification_status": None,
                    "details_json": self._tag_with_backfill(cr.metadata),
                    "record_type": "collection",
                    "collector_name": cr.metadata.get("collector_name", ""),
                    "dimension_value": cr.dimension_value,
                    "collected_at": cr.collected_at.isoformat() if cr.collected_at else None,
                    "validated_at": None,
                    "notified_at": None,
                    "partition_ts": _format_partition_ts(cr.partition_ts or run_part),
                    "expected_value": None,
                    "actual_value_text": None,
                    "dataset_description": ds_desc,
                    "validation_description": None,
                    "is_active": "true",
                })
        # Engine warnings already accumulated (e.g. wap_cleanup) ride this pass.
        for vr in result.engine_warnings:
            rows.append(self._engine_warning_row(result, vr))
        return rows

    def _engine_warning_row(
        self, result: QualifireResult, vr: ValidationResult
    ) -> dict[str, Any]:
        """Build a system-table row for an engine warning under the
        synthetic ``qualifire.engine`` dataset."""
        return {
            "run_id": result.run_id,
            "run_timestamp": datetime.now().isoformat(),
            "owner": result.owner,
            "bu": result.bu,
            "dataset_name": self._ENGINE_DATASET_NAME,
            "table_name": None,
            "metric_name": vr.metric_name,
            "metric_value": None,
            "collection_type": vr.validation_type,
            "validation_name": vr.validation_name,
            "validation_type": vr.validation_type,
            "validation_status": vr.severity.value,
            "validation_message": vr.message,
            "notification_channel": None,
            "notification_status": None,
            "details_json": self._tag_with_backfill(vr.details),
            "record_type": "validation",
            "collector_name": None,
            "dimension_value": vr.dimension_value,
            "collected_at": None,
            "validated_at": vr.validated_at.isoformat() if vr.validated_at else None,
            "notified_at": None,
            "partition_ts": None,
            "expected_value": vr.expected_value,
            "actual_value_text": vr.actual_value_text,
            "dataset_description": None,
            "validation_description": None,
            "is_active": "true",
        }

    def _run_wap(self, ds_config: DatasetConfig) -> tuple[DatasetResult, list[ValidationResult]]:
        """Run Write-Audit-Publish pattern for a dataset.

        Returns ``(dataset_result, engine_warnings)``. The
        engine_warnings list carries any cleanup warnings the
        ``WAPExecutor`` accumulated (e.g.
        ``qualifire.wap_cleanup`` — staging-table-leak when publish
        succeeded but cleanup failed). ``engine.run`` drains them
        into ``QualifireResult.engine_warnings`` before notification
        routing.
        """
        wap_config = ds_config.wap

        # df / sql / sql_file three-way mutual exclusion is enforced
        # by WAPConfig. Pass df ONLY when set; never pass both.
        if wap_config.df is not None:
            executor = WAPExecutor(
                backend=self.backend,
                target_table=wap_config.target_table,
                write_options=wap_config.write_options,
                context=self.context,
                df=wap_config.df,
                cache=ds_config.cache,
                cache_storage_level=ds_config.cache_storage_level,
                allow_create=wap_config.allow_create,
            )
        else:
            # Reads ``effective_sql`` so ``sql_file`` (resolved at
            # config-load time on a PrivateAttr) is honoured.
            executor = WAPExecutor(
                backend=self.backend,
                target_table=wap_config.target_table,
                sql=wap_config.effective_sql,
                write_options=wap_config.write_options,
                context=self.context,
                cache=ds_config.cache,
                cache_storage_level=ds_config.cache_storage_level,
                allow_create=wap_config.allow_create,
            )

        # WAP cleanup is exception-safe. Any unhandled exception from
        # write/audit/publish triggers rollback; if rollback itself
        # raises, the rollback failure is logged but the primary
        # exception propagates.
        try:
            # Write phase (caches + registers temp view if cache=True)
            executor.write()

            # Audit phase: run validations against staging table/view
            # Don't cache again in _run_dataset — WAP already handles caching.
            #
            # Propagate dataset-level fields so per-dataset overrides
            # (partition_ts, dimensions, description, ...) work for WAP
            # too. The wap field is deliberately stripped — staging audit
            # must not recurse into another WAP cycle (N4). filter,
            # query, df are NOT forwarded — staging is a fresh just-
            # written subset; forwarding source-side semantics is wrong.
            # cache and validation_parallelism are NOT forwarded —
            # WAPExecutor handles caching; orthogonal otherwise.
            staging_ds_config = DatasetConfig(
                name=ds_config.name,
                description=ds_config.description,
                table=executor.staging_table,
                dimensions=ds_config.dimensions,
                measures=ds_config.measures,
                partition_ts=ds_config.partition_ts,
                partition_step=ds_config.partition_step,
                validations=ds_config.validations,
                redacted_columns=ds_config.redacted_columns,
                allowlist_columns=ds_config.allowlist_columns,
            )
            ds_result = self._run_dataset(staging_ds_config)
            ds_result.table = wap_config.target_table

            # Publish or rollback (cleanup unpersists + drops view/table)
            if ds_result.has_errors:
                executor.rollback()
            else:
                executor.publish()
        except Exception:
            try:
                executor.rollback()
            except Exception as cleanup_err:
                logger.error("WAP cleanup failed during rollback: %s", cleanup_err)
                # Do NOT re-raise — primary exception must propagate.
            raise

        # Drain executor warnings (publish-cleanup-only failures land here)
        engine_warnings = list(getattr(executor, "cleanup_warnings", []) or [])
        return ds_result, engine_warnings
