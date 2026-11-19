"""Strategy executors for the content-pack runner.

Two strategies are implemented in v0.3:

* :func:`execute_replace` — ``CREATE OR REPLACE TABLE <target> USING DELTA AS
  <select_sql>``. Used for seed-mode and for nodes whose grain forbids
  incremental MERGE (``supplier_spend``, ``ap_aging``, ``dim_calendar``).
* :func:`execute_merge` — full ``MERGE INTO`` with NULL-safe natural-key
  join, optional payload-diff predicate, and target schema reconciliation.
  Also probes for empty source delta and skips the MERGE when there's
  nothing to merge (preserves prior watermark).

Both executors:

* Accept the already-rendered :class:`~oracle_ai_data_platform_fusion_autopilot.orchestrator.sql_renderer.RenderedSql`
  produced by :func:`execute_node` — never re-render.
* Thread profile values through Spark parameter markers via
  ``spark.sql(stmt, args=rendered.params)``. **Never** ``spark.sql(stmt)``
  alone.
* Return a typed :class:`StrategyExecutionResult` carrying the rows
  scanned / written count + a "merge happened" flag the caller uses for
  watermark advancement.

The MERGE path must preserve NULL-safe joins and empty-delta watermark
semantics.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from ..schema.medallion_pack import NodeYaml
from . import merge_helpers
from .sql_renderer import RenderedSql, RunContext

if TYPE_CHECKING:
    from pyspark.sql import SparkSession


# ---------------------------------------------------------------------------
# AIDPF error codes
# ---------------------------------------------------------------------------

AIDPF_4030_UNSUPPORTED_STRATEGY = "AIDPF-4030"
"""Strategy declared by node not supported in v0.3 (append, replace_partition,
aggregate_merge, snapshot, scd2, custom)."""

AIDPF_4031_TARGET_IDENTIFIER_REJECTED = "AIDPF-4031"
"""Target identifier failed the strategy-executor's allowlist check."""


class StrategyExecutorError(Exception):
    """Base error for strategy executor failures."""


class UnsupportedStrategyError(StrategyExecutorError):
    """Strategy not implemented by the content-pack runner (AIDPF-4030)."""


class TargetIdentifierError(StrategyExecutorError):
    """Target identifier failed the allowlist check (AIDPF-4031)."""


# ---------------------------------------------------------------------------
# Result dataclass
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class StrategyExecutionResult:
    """Result of a single strategy executor invocation.

    Attributes:
        strategy: ``"replace"`` or ``"merge"`` (echoes ``node.refresh.
            <mode>.strategy`` for audit / state-row writing).
        rows_scanned: rows the source SELECT returned (the delta size
            for incremental ``merge``; the full row count for seed
            ``replace``). The caller uses this to decide watermark
            advancement (``delta_row_count = rows_scanned``).
        merge_skipped_empty_delta: True iff the strategy was ``merge``
            and the source produced 0 rows; in that case no MERGE was
            issued and the prior watermark must be preserved.
        target: the fully-qualified target table identifier the
            executor wrote / would have written.
    """

    strategy: str
    rows_scanned: int
    merge_skipped_empty_delta: bool
    target: str


# ---------------------------------------------------------------------------
# Identifier validation for the target table
# ---------------------------------------------------------------------------

import re

# Allow up to three dotted segments (catalog.schema.table). Each segment
# follows the SQL identifier allowlist from the renderer.
_TARGET_IDENT_SEGMENT = re.compile(r"^[A-Za-z_][A-Za-z0-9_]{0,62}$")


def _check_target_identifier(target: str) -> str:
    """Validate ``target`` against the dotted-identifier allowlist."""
    if not isinstance(target, str) or not target:
        raise TargetIdentifierError(
            f"{AIDPF_4031_TARGET_IDENTIFIER_REJECTED}: target identifier "
            f"{target!r} is empty or not a string."
        )
    segments = target.split(".")
    if len(segments) > 3:
        raise TargetIdentifierError(
            f"{AIDPF_4031_TARGET_IDENTIFIER_REJECTED}: target identifier "
            f"{target!r} has too many dotted segments (max 3: catalog.schema.table)."
        )
    for segment in segments:
        if not _TARGET_IDENT_SEGMENT.match(segment):
            raise TargetIdentifierError(
                f"{AIDPF_4031_TARGET_IDENTIFIER_REJECTED}: target identifier "
                f"segment {segment!r} fails the allowlist "
                f"`^[A-Za-z_][A-Za-z0-9_]{{0,62}}$`."
            )
    return target


# ---------------------------------------------------------------------------
# Helpers — Spark invocation that always passes args=rendered.params
# ---------------------------------------------------------------------------


def _spark_sql_with_params(
    spark: "SparkSession", sql: str, params: dict[str, Any]
) -> Any:
    """Invoke ``spark.sql`` with parameter-marker bindings.

    Every SQL string from the renderer or composer is passed to Spark
    via ``args=`` so untrusted profile / run-id /
    watermark values are bound as literals, not concatenated. If
    ``params`` is empty Spark accepts the empty dict.
    """
    return spark.sql(sql, args=params)


# ---------------------------------------------------------------------------
# execute_replace
# ---------------------------------------------------------------------------


def execute_replace(
    spark: "SparkSession",
    node: NodeYaml,
    rendered: RenderedSql,
    target: str,
    ctx: RunContext,
) -> StrategyExecutionResult:
    """Execute the ``replace`` refresh strategy.

    Issues ``CREATE OR REPLACE TABLE <target> USING DELTA AS <rendered.sql>``
    through Spark's parameter-marker API. All rows in the source SELECT
    are written; the prior table contents (if any) are atomically
    swapped out by Delta.

    Used for:

    * Seed-mode runs of every node.
    * Incremental-mode runs of nodes whose grain forbids row-level
      MERGE (``supplier_spend``, ``ap_aging``, ``dim_calendar``).

    Args:
        spark: live Spark session.
        node: the validated NodeYaml (used for identity / audit only —
            the actual SQL came from ``rendered``).
        rendered: the rendered SQL + parameter bindings from
            ``render_node_sql``.
        target: fully-qualified target table (catalog.schema.table).
        ctx: render context (unused here; threaded through for
            consistency with ``execute_merge`` which uses it for
            ``prior_watermark`` lookups).

    Returns:
        :class:`StrategyExecutionResult` with strategy=``"replace"`` and
        ``rows_scanned`` = post-write row count. ``merge_skipped_empty_delta``
        is always False for replace.

    Raises:
        TargetIdentifierError: AIDPF-4031 — target failed the allowlist.
    """
    _check_target_identifier(target)
    stmt = (
        f"CREATE OR REPLACE TABLE {target} USING DELTA AS\n"
        f"{rendered.sql}"
    )
    _spark_sql_with_params(spark, stmt, dict(rendered.params))

    # Post-write row count. We accept the round-trip cost (one extra
    # query) so callers don't have to recompute the count themselves; the
    # state-row writer expects a concrete number.
    row_count_df = _spark_sql_with_params(
        spark, f"SELECT COUNT(*) AS n FROM {target}", {}
    )
    rows_scanned = int(row_count_df.collect()[0][0]) if row_count_df is not None else 0

    return StrategyExecutionResult(
        strategy="replace",
        rows_scanned=rows_scanned,
        merge_skipped_empty_delta=False,
        target=target,
    )


# ---------------------------------------------------------------------------
# execute_merge
# ---------------------------------------------------------------------------


def execute_merge(
    spark: "SparkSession",
    node: NodeYaml,
    rendered: RenderedSql,
    target: str,
    ctx: RunContext,
) -> StrategyExecutionResult:
    """Execute the ``merge`` refresh strategy.

    Flow:

    1. Validate ``target`` identifier.
    2. Resolve the natural key from
       ``node.refresh.incremental.natural_key``. Empty → ValueError
       (caller's responsibility to pre-validate).
    3. **Empty-delta probe**: run the rendered source SELECT through
       Spark with ``args=rendered.params`` and probe the row count via
       ``LIMIT 1``. If 0 rows, skip the MERGE entirely, return
       ``merge_skipped_empty_delta=True`` so the caller preserves the
       prior ``output_watermark``.
    4. **Schema reconciliation**: call
       :func:`merge_helpers.ensure_target_schema_for_merge` so any new
       nullable columns in the rendered source are added to the target.
    5. **Compose and execute the MERGE**: assemble via
       :func:`merge_helpers.compose_merge_sql` with a NULL-safe natural-key
       join (``<=>``). This runner does NOT supply a payload-diff predicate
       for the silver/gold merge — the predicate is a bronze-layer
       optimisation; silver/gold's row count is small enough that
       unconditional UPDATE is fine. (Future bronze-strategy work can
       extend this signature.)
    6. Run ``spark.sql(stmt, args=rendered.params)`` — params bound via
       parameter markers, NOT inlined.
    7. Return rows_scanned = post-execute target row count for audit.

    Args:
        spark: live Spark session.
        node: the validated NodeYaml — ``refresh.incremental.natural_key``
            drives the join predicate.
        rendered: rendered source SELECT + params.
        target: fully-qualified target identifier.
        ctx: run context (unused directly here but accepted for the
            uniform strategy-executor signature).

    Returns:
        :class:`StrategyExecutionResult` with strategy=``"merge"``.
        ``merge_skipped_empty_delta`` True iff the source had 0 rows.

    Raises:
        TargetIdentifierError: AIDPF-4031 — target failed allowlist.
        ValueError: natural_key empty (caller bug — schema validator
            should have rejected this).
    """
    _check_target_identifier(target)

    inc = node.refresh.incremental
    if inc is None or not inc.natural_key:
        raise ValueError(
            f"execute_merge: node {node.id!r} has no "
            f"refresh.incremental.natural_key. AIDPF-2020 "
            f"should have rejected this at validation."
        )
    natural_key = list(inc.natural_key)

    # Empty-delta probe — same params as the actual MERGE source. The
    # probe MUST use args=rendered.params for consistency with the MERGE.
    probe_stmt = f"SELECT 1 FROM ({rendered.sql}) AS _probe LIMIT 1"
    probe_df = _spark_sql_with_params(spark, probe_stmt, dict(rendered.params))
    probe_rows = probe_df.collect() if probe_df is not None else []
    if not probe_rows:
        # No source rows → no MERGE, no schema reconciliation, watermark
        # preserved by the caller (Step 11). Return early.
        return StrategyExecutionResult(
            strategy="merge",
            rows_scanned=0,
            merge_skipped_empty_delta=True,
            target=target,
        )

    # Schema reconciliation — auto-add new nullable columns in source to
    # target. We materialise the source as a temp view here so the
    # helper (which takes a DataFrame) can inspect its schema. The temp
    # view's name is run_id-scoped to avoid collisions across concurrent
    # tests.
    temp_view = f"_content_pack_merge_source_{_sanitise_view_segment(ctx.run_id)}"
    source_df = _spark_sql_with_params(spark, rendered.sql, dict(rendered.params))
    source_df.createOrReplaceTempView(temp_view)
    try:
        merge_helpers.ensure_target_schema_for_merge(
            spark=spark,
            target=target,
            source_columns=source_df.schema.names,
            source_schema_struct=source_df.schema,
        )

        # No payload-diff predicate at the silver/gold layer: silver/gold
        # deltas are small enough that unconditional UPDATE is fine; the
        # optimisation lives in bronze MERGE.
        merge_stmt = merge_helpers.compose_merge_sql(
            target=target,
            source_sql=f"SELECT * FROM {temp_view}",
            natural_key=natural_key,
            payload_diff_predicate=None,
        )
        _spark_sql_with_params(spark, merge_stmt, {})

        # Post-merge row count for state audit.
        row_count_df = _spark_sql_with_params(
            spark, f"SELECT COUNT(*) AS n FROM {target}", {}
        )
        rows_scanned = (
            int(row_count_df.collect()[0][0]) if row_count_df is not None else 0
        )
    finally:
        # Drop the temp view so subsequent runs in the same SparkSession
        # don't collide.
        try:
            spark.catalog.dropTempView(temp_view)
        except Exception:  # noqa: BLE001 — drop is cleanup; failure is benign
            pass

    return StrategyExecutionResult(
        strategy="merge",
        rows_scanned=rows_scanned,
        merge_skipped_empty_delta=False,
        target=target,
    )


_VIEW_SAFE_RE = re.compile(r"[^A-Za-z0-9_]")


def _sanitise_view_segment(segment: str) -> str:
    """Make a string safe for use as a Spark temp view identifier."""
    return _VIEW_SAFE_RE.sub("_", segment)


# ---------------------------------------------------------------------------
# Dispatcher — single entry point used by execute_node
# ---------------------------------------------------------------------------


def execute_strategy(
    spark: "SparkSession",
    *,
    node: NodeYaml,
    rendered: RenderedSql,
    target: str,
    ctx: RunContext,
    mode: str,
) -> StrategyExecutionResult:
    """Dispatch to the executor matching ``node.refresh.<mode>.strategy``.

    Supports only ``replace`` and ``merge``. Any other
    declared strategy (``append``, ``replace_partition``,
    ``aggregate_merge``, ``snapshot``, ``scd2``, ``custom``) raises
    :class:`UnsupportedStrategyError` with AIDPF-4030.

    Args:
        spark: live Spark session.
        node: the validated NodeYaml whose refresh block was used to
            produce ``rendered``.
        rendered: rendered SQL + params.
        target: fully-qualified target table.
        ctx: render context.
        mode: ``"seed"`` or ``"incremental"`` — picks which leg of
            ``node.refresh.{seed,incremental}.strategy`` to read.

    Returns:
        :class:`StrategyExecutionResult` from the dispatched executor.

    Raises:
        UnsupportedStrategyError: AIDPF-4030.
        TargetIdentifierError: AIDPF-4031 (propagated from the executor).
    """
    if mode == "seed":
        strategy = node.refresh.seed.strategy
    elif mode == "incremental":
        if node.refresh.incremental is None:
            raise ValueError(
                f"execute_strategy: node {node.id!r} has no refresh.incremental "
                f"block but mode='incremental' was requested."
            )
        strategy = node.refresh.incremental.strategy
    else:
        raise ValueError(f"execute_strategy: unknown mode {mode!r}")

    if strategy == "replace":
        return execute_replace(spark, node, rendered, target, ctx)
    if strategy == "merge":
        return execute_merge(spark, node, rendered, target, ctx)

    raise UnsupportedStrategyError(
        f"{AIDPF_4030_UNSUPPORTED_STRATEGY}: strategy {strategy!r} not "
        f"supported. The content-pack runner supports only 'replace' and 'merge'. "
        f"Deferred: append, replace_partition, aggregate_merge, snapshot, "
        f"scd2, custom."
    )
