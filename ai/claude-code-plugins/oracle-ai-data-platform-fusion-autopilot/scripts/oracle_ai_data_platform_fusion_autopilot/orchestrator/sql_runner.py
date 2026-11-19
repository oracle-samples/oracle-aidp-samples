"""Content-pack execution backend — ``execute_node`` entry point.

This is the orchestrator's per-node runner. The content-pack runner
calls ``execute_node`` once per node;
``execute_node`` performs the full lifecycle (preflight → render →
plan-hash drift gate → strategy dispatch → quality tests → materialised
schema assertion → atomic state commit) and returns a typed result.

Critical ordering invariant
---------------------------

The plan-hash drift gate compares the *expected* content-pack plan-hash
(which includes ``rendered_sql_hash``) against the last successful
state row. The expected hash can only be computed AFTER the SQL has
been rendered with profile params. The flow is therefore:

1. Static schema validation; trusted from the loader.
2. Preflight — metadata + bronze DESCRIBE only, no render.
3. **Render SQL** — happens exactly once per execute_node call.
4. **Compute expected content-pack plan-hash** including the
   rendered_sql_hash.
5. **Plan-hash drift gate** (incremental only) — block resume on
   AIDPF-4040 BEFORE any Spark write.
6. Dispatch by strategy, reusing the same RenderedSql.
7. Quality tests — failures block cursor advance.
8. Materialised-schema assertion — Spark target schema must match
   ``node.outputSchema`` (AIDPF-4070).
9. Compute output_watermark.
10. Assemble the full state-row list (primary + every lookup) in memory.
11. ONE atomic batch state write via ``write_state_rows_hard``.
12. Return.

Render is deterministic; no LLM or operator interaction is allowed during
seed/incremental execution.
"""

from __future__ import annotations

import hashlib
import logging
import re
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any, Literal

from . import plan_hash as plan_hash_module
from . import state as state_module
from . import state_phase2
from .spark_types import _normalise_spark_type, _SPARK_TYPE_SYNONYMS  # noqa: F401

logger = logging.getLogger(__name__)
from .node_preflight import preflight_node
from .quality_runner import run_quality_tests
from .sql_renderer import (
    RenderedSql,
    RunContext,
    SqlRendererError,
    compute_rendered_sql_hash,
    render_node_sql,
)
from .strategy_executors import StrategyExecutorError, execute_strategy

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

    from ..config.paths import TablePaths
    from ..schema.medallion_pack import NodeYaml
    from ..schema.tenant_profile import TenantProfile
    from .content_pack import ResolvedPack


# ---------------------------------------------------------------------------
# AIDPF error codes
# ---------------------------------------------------------------------------

AIDPF_4040_PLAN_HASH_DRIFT = "AIDPF-4040"
"""Plan-hash drift on resume — rendered SQL, output schema, or profile hash
changed since the last successful run. AIDPF-4040 blocks resume."""

AIDPF_4070_MATERIALIZED_SCHEMA_DRIFT = "AIDPF-4070"
"""Materialised target schema does not match node.outputSchema.

Detected post-execution via DESCRIBE TABLE; differs from AIDPF-4040
which catches YAML-author-induced drift pre-dispatch. Both gates fire
independently; if both conditions hold, the pre-dispatch gate fires first
and the SQL is never executed."""

AIDPF_4071_BRONZE_SOURCE_COLUMN_MISSING = "AIDPF-4071"
"""A column the pack wants from a bronze PVO is absent from the live source.

Detected PRE-ingest (Step 3 of the bronze_extract flow) via a metadata-only
BICC ``inferSchema`` probe — fails fast before the multi-minute extract that
would otherwise fail the post-write AIDPF-4070 subset check anyway.
Presence-only + case-insensitive: types are BICC's to decide (silver casts),
so AIDPF-4071 fires only on a genuinely missing/renamed column — the
columnAlias / medallion-author case, not a type or casing difference."""

AIDPF_5014_UNKNOWN_BUILTIN_DISPATCH = "AIDPF-5014"
"""Content-pack execute_node dispatched a ``type: builtin`` node whose
``implementation.callable`` is not in the builtin registry. The
registry is the allowlist; missing entries fail fast rather than
auto-importing arbitrary callables."""

class ExecuteNodeError(Exception):
    """Base error class for execute_node failures."""


class UnknownBuiltinDispatchError(ExecuteNodeError):
    """Builtin node's callable id not in ``_BUILTIN_REGISTRY`` (AIDPF-5014)."""


class PlanHashDriftError(ExecuteNodeError):
    """Plan-hash drift detected (AIDPF-4040)."""


class MaterializedSchemaDriftError(ExecuteNodeError):
    """Materialised target schema mismatch (AIDPF-4070)."""


# ---------------------------------------------------------------------------
# Result dataclass
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class NodeExecutionResult:
    """Result of one execute_node invocation.

    Attributes:
        status: ``'success'`` / ``'preflight_blocked'`` /
            ``'render_failed'`` / ``'resume_drift_blocked'`` /
            ``'quality_failed'`` / ``'output_schema_drift'`` /
            ``'state_commit_failed'``.
        row_count: rows scanned / written (0 for non-success paths).
        output_watermark: primary source's output watermark for this run,
            or None if the run did not advance the cursor.
        materialized_schema_hash: post-execution hash of the target's
            actual Spark schema (None on non-success paths).
        error_message: human-readable diagnostic for non-success paths.
        plan_hash: expected_plan_hash computed during the run.
        diagnostic: optional structured failure context (a JSON-able dict)
            the orchestrator collects into ``RunSummary.diagnostics`` and
            the laptop dispatcher persists under ``.aidp/diagnostics/``
            for skill consumption (e.g. the AIDPF-4071 payload).
    """

    status: str
    row_count: int = 0
    output_watermark: datetime | None = None
    materialized_schema_hash: str | None = None
    error_message: str = ""
    plan_hash: str = ""
    diagnostic: dict | None = None


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def execute_node(
    spark: "SparkSession",
    *,
    node: "NodeYaml",  # noqa: F821
    pack: "ResolvedPack",  # noqa: F821
    profile: "TenantProfile",  # noqa: F821
    ctx: RunContext,
    paths: "TablePaths",  # noqa: F821
    mode: Literal["seed", "incremental"],
    profile_hash: str,
    prior_plan_hash: str | None = None,
    target_override: str | None = None,
    repin_plan_hash: bool = False,
    coa_inc: "Any | None" = None,
    prior_run_id: str | None = None,
) -> NodeExecutionResult:
    """Execute one content-pack node end-to-end.

    Args:
        spark: live Spark session.
        node: validated NodeYaml whose SQL template is the unit of work.
        pack: assembled ResolvedPack carrying per-node provenance.
        profile: validated TenantProfile (variation-point picks + free-form values).
        ctx: render-time context (catalog/schemas/run_id/prior_watermark/mode/
            bronze_table_for_source).
        paths: TablePaths from the bundle (passed through to state-row
            assembly).
        mode: ``'seed'`` or ``'incremental'``.
        profile_hash: pre-computed profile hash (Step 2's
            ``compute_profile_hash``) — passed in to avoid recomputing
            inside the per-node loop.
        prior_plan_hash: the last successful state row's ``plan_hash``
            for this node, or ``None`` (seed mode / first run / no
            prior state). When non-None and incremental mode, drives
            the resume drift gate.
        target_override: fully-qualified target identifier override.
            When ``None``, the executor uses ``<catalog>.<silver|gold_schema>.<node.target>``
            from ``ctx``.
        repin_plan_hash: hidden break-glass (``--repin-plan-hash``). When
            True and the AIDPF-4040 continuity gate would fire, the gate is
            bypassed, a ``mode='plan_hash_repin'`` audit row is written, and
            execution proceeds — re-pinning the new plan-hash. For deliberate
            SQL/profile/adapter edits where a full re-seed isn't wanted.
            Production/SOX runs MUST NOT use this.

    Returns:
        :class:`NodeExecutionResult` describing the outcome. The caller
        (orchestrator.run) decides how to surface the result — for
        success/failure-with-state-row paths the function has already
        written the state rows itself; for hard programmer errors it
        re-raises.
    """
    # Dispatch on implementation type. SQL nodes render templates,
    # builtin nodes call registered adapters, and bronze_extract nodes
    # execute the BICC extraction adapter.
    impl_type = node.implementation.type
    if impl_type == "builtin":
        return _execute_builtin_node(
            spark,
            node=node,
            pack=pack,
            profile=profile,
            ctx=ctx,
            paths=paths,
            mode=mode,
            profile_hash=profile_hash,
            prior_plan_hash=prior_plan_hash,
            target_override=target_override,
            repin_plan_hash=repin_plan_hash,
            coa_inc=coa_inc,
            prior_run_id=prior_run_id,
        )
    if impl_type == "bronze_extract":
        # Content-pack-driven bronze follows the builtin lifecycle, but
        # the adapter returns
        # ``(target_df, bronze_output_watermark)`` because bronze cursor
        # semantics are extraction-time, not source-row-max.
        return _execute_bronze_extract_node(
            spark,
            node=node,
            pack=pack,
            profile=profile,
            ctx=ctx,
            paths=paths,
            mode=mode,
            profile_hash=profile_hash,
            prior_plan_hash=prior_plan_hash,
            target_override=target_override,
            repin_plan_hash=repin_plan_hash,
            coa_inc=coa_inc,
            prior_run_id=prior_run_id,
        )
    if impl_type != "sql":
        # Defensive — the loader's discriminated union already rejects
        # everything outside {sql, builtin, bronze_extract}. Reaching
        # here means a future implementation type slipped through the
        # loader gate without being wired in. Hard-raise so the bug is
        # visible.
        raise ValueError(
            f"execute_node: unsupported implementation.type={impl_type!r} "
            f"for node {node.id!r}. Expected 'sql', 'builtin', or "
            f"'bronze_extract'."
        )

    # ----- Static schema validation is done by the loader. ----------

    # ----- Step 2: preflight ------------------------------------------
    preflight = preflight_node(spark, node, pack, profile, ctx)
    if not preflight.ok:
        message = "; ".join(f"[{e.code}] {e.message}" for e in preflight.errors)
        _safe_write_preflight_blocked_row(
            spark, paths, node=node, ctx=ctx, message=message, profile=profile,
        )
        return NodeExecutionResult(
            status="preflight_blocked",
            error_message=message,
        )

    # ----- Step 3: render SQL (exactly once) --------------------------
    try:
        rendered = render_node_sql(node, pack, profile, ctx)
    except SqlRendererError as exc:
        message = f"render_failed: {exc}"
        _safe_write_render_failed_row(
            spark, paths, node=node, ctx=ctx, message=message, profile=profile,
        )
        return NodeExecutionResult(status="render_failed", error_message=message)

    rendered_sql_hash = compute_rendered_sql_hash(rendered)
    output_schema_hash = plan_hash_module.compute_output_schema_hash(node)

    # ----- Step 4: compute expected content-pack plan-hash ------------
    expected_plan_hash = plan_hash_module.compute_content_pack_plan_hash(
        pack=pack,
        node=node,
        profile=profile,
        rendered_sql_hash=rendered_sql_hash,
        output_schema_hash=output_schema_hash,
        profile_hash=profile_hash,
    )

    # ----- Step 5: plan-hash drift gate (incremental only) -----------
    # Additive-COA fast path: if the gate would fire, try to prove the drift is
    # a purely additive chart-of-accounts change. The prior-equivalent hash
    # re-renders THIS node under the prior COA baseline (its rendered SQL DOES
    # depend on the COA mapping) and hashes with the stored prior profile_hash.
    def _sql_recompute_prior_equiv(prior_coa, prior_profile_hash):
        prior_profile = _profile_with_prior_coa(profile, prior_coa)
        prior_rendered = render_node_sql(node, pack, prior_profile, ctx)
        return plan_hash_module.compute_content_pack_plan_hash(
            pack=pack, node=node, profile=profile,
            rendered_sql_hash=compute_rendered_sql_hash(prior_rendered),
            output_schema_hash=output_schema_hash,
            profile_hash=prior_profile_hash,
        )

    coa_accept_reason = _coa_additive_reason(
        coa_inc=coa_inc, node=node, prior_run_id=prior_run_id,
        prior_plan_hash=prior_plan_hash, expected_plan_hash=expected_plan_hash,
        mode=mode, recompute_hash=_sql_recompute_prior_equiv,
    )
    drift_result = _resume_drift_or_repin(
        spark, paths, node=node, ctx=ctx, profile=profile, mode=mode,
        expected_plan_hash=expected_plan_hash, prior_plan_hash=prior_plan_hash,
        repin_plan_hash=repin_plan_hash,
        revert_hint="revert the YAML / SQL / profile change",
        coa_additive_accept=coa_accept_reason is not None,
    )
    if drift_result is not None:
        return drift_result

    # ----- Step 6: dispatch by strategy, reusing RenderedSql ----------
    target = target_override or _build_target_identifier(node, ctx, paths)
    try:
        strategy_result = execute_strategy(
            spark, node=node, rendered=rendered, target=target, ctx=ctx, mode=mode,
        )
    except StrategyExecutorError as exc:
        message = f"strategy_failed: {exc}"
        _safe_write_strategy_failed_row(
            spark, paths, node=node, ctx=ctx, message=message, profile=profile,
            plan_hash=expected_plan_hash,
        )
        return NodeExecutionResult(
            status="strategy_failed",
            error_message=message,
            plan_hash=expected_plan_hash,
        )

    # ----- Step 7: quality tests --------------------------------------
    target_df = spark.table(target)
    quality_report = run_quality_tests(spark, node, target_df, ctx)
    if not quality_report.ok:
        message = "; ".join(f"[{f.test_type}] {f.message}" for f in quality_report.failures)
        _safe_write_quality_failed_row(
            spark, paths, node=node, ctx=ctx, message=message, profile=profile,
            plan_hash=expected_plan_hash,
        )
        return NodeExecutionResult(
            status="quality_failed",
            error_message=message,
            plan_hash=expected_plan_hash,
        )

    # ----- Step 8: materialised-schema assertion ----------------------
    try:
        materialized_schema_hash = _assert_materialized_matches_declared(
            spark, target, node
        )
    except MaterializedSchemaDriftError as exc:
        message = str(exc)
        _safe_write_schema_drift_row(
            spark, paths, node=node, ctx=ctx, message=message, profile=profile,
            plan_hash=expected_plan_hash,
        )
        return NodeExecutionResult(
            status="output_schema_drift",
            error_message=message,
            plan_hash=expected_plan_hash,
        )

    # ----- Step 9: compute output_watermark ---------------------------
    output_watermark = _compute_output_watermark(
        spark, node, ctx, paths, rendered, strategy_result,
    )

    # ----- Step 10: assemble state rows (primary + lookups) ----------
    state_rows = _assemble_success_state_rows(
        node=node,
        ctx=ctx,
        pack=pack,
        profile=profile,
        mode=mode,
        rendered_sql_hash=rendered_sql_hash,
        output_schema_hash=output_schema_hash,
        profile_hash=profile_hash,
        plan_hash=expected_plan_hash,
        strategy_result=strategy_result,
        output_watermark=output_watermark,
        coa_additive_accept_reason=coa_accept_reason,
    )

    # ----- Step 11: ONE atomic batch state write ----------------------
    try:
        state_phase2.write_state_rows_hard(spark, paths, state_rows)
    except state_phase2.StateCommitError as exc:
        message = f"state_commit_failed: {exc}"
        # Do NOT attempt a soft fallback — the contract is hard-commit
        # for cursor-advancing rows.
        return NodeExecutionResult(
            status="state_commit_failed",
            error_message=message,
            plan_hash=expected_plan_hash,
            row_count=strategy_result.rows_scanned,
        )

    # ----- Step 12: return success result -----------------------------
    return NodeExecutionResult(
        status="success",
        row_count=strategy_result.rows_scanned,
        output_watermark=output_watermark,
        materialized_schema_hash=materialized_schema_hash,
        plan_hash=expected_plan_hash,
    )


# ---------------------------------------------------------------------------
# Materialised-schema assertion (Step 8 of the execute_node flow)
# ---------------------------------------------------------------------------


def _assert_materialized_matches_declared(
    spark: "SparkSession", target: str, node: "NodeYaml",  # noqa: F821
    *,
    subset: bool = False,
) -> str:
    """Validate the materialised target's Spark schema against node.outputSchema.

    Two modes:

    * **Exact** (``subset=False``, the default — SQL silver/gold nodes):
      the materialised table must match ``node.outputSchema`` column-for-
      column (count + order + name + type). A SQL node ``SELECT``s an exact
      column list, so any divergence is a genuine contract break.
    * **Subset** (``subset=True`` — ``bronze_extract`` nodes): bronze writes
      the FULL raw PVO (hundreds of tenant-/release-dependent columns), while
      ``outputSchema`` is the MINIMUM guaranteed contract the silver layer
      consumes. We assert every declared column is present with a matching
      type; extra raw columns are allowed. AIDPF-4070 fires only on a
      missing or mistyped declared column.

    This gate validates the contract against LIVE data. The companion
    design-time gate ``validate_column_contracts`` (AIDPF-2045) validates the
    other side — that downstream consumers only demand columns the upstream
    ``outputSchema`` actually guarantees — offline at ``content-pack validate``,
    making the "contract" above load-bearing rather than aspirational.

    Returns a sha256 of the canonicalised materialised schema on
    success — the caller threads it into the success state row for
    audit.
    """
    # A "successful" execute that left no target table (empty/degenerate
    # extract, or a write that silently no-op'd) makes DESCRIBE raise
    # TABLE_OR_VIEW_NOT_FOUND. Convert it to a graceful per-node
    # MaterializedSchemaDriftError (the callers catch it → output_schema_drift)
    # rather than letting the raw AnalysisException propagate and abort the
    # entire run, taking unrelated nodes down with it.
    try:
        rows = spark.sql(f"DESCRIBE TABLE {target}").collect()
    except Exception as exc:  # noqa: BLE001 — missing target / broken describe
        raise MaterializedSchemaDriftError(
            f"{AIDPF_4070_MATERIALIZED_SCHEMA_DRIFT}: target {target!r} could not "
            f"be described after execution — the node produced no table (empty "
            f"or failed extract/write). Underlying: "
            f"{type(exc).__name__}: {str(exc)[:200]}"
        ) from exc
    materialized: list[tuple[str, str]] = []
    for r in rows:
        try:
            name = r["col_name"] if isinstance(r, dict) else r[0]
            dtype = r["data_type"] if isinstance(r, dict) else r[1]
        except (KeyError, IndexError, TypeError):
            continue
        if not name or name.startswith("#"):
            break
        materialized.append((name, str(dtype).lower()))

    declared = [
        (col.name, col.type.lower()) for col in node.output_schema.columns
    ]

    if subset:
        # Case-insensitive name match: Spark/Delta resolve column names
        # case-insensitively, and the Fusion bronze write lowercases every
        # column while the pack declares Fusion-native PascalCase
        # (e.g. declared ``ApInvoicesInvoiceId`` vs materialised
        # ``apinvoicesinvoiceid``). Mirrors the case-insensitive contract in
        # node_preflight._check_required_columns — the two gates MUST agree.
        materialized_ci = {n.lower(): t for n, t in materialized}
        for d_name, d_type in declared:
            m_type = materialized_ci.get(d_name.lower())
            if m_type is None:
                raise MaterializedSchemaDriftError(
                    f"{AIDPF_4070_MATERIALIZED_SCHEMA_DRIFT}: target {target!r} is "
                    f"missing declared column {d_name!r}. Materialised columns "
                    f"({len(materialized)}): {sorted(n for n, _ in materialized)!r}."
                )
            if _normalise_spark_type(m_type) != _normalise_spark_type(d_type):
                raise MaterializedSchemaDriftError(
                    f"{AIDPF_4070_MATERIALIZED_SCHEMA_DRIFT}: column {d_name!r} type "
                    f"mismatch — materialised={m_type!r} declared={d_type!r}."
                )
    else:
        if len(materialized) != len(declared):
            raise MaterializedSchemaDriftError(
                f"{AIDPF_4070_MATERIALIZED_SCHEMA_DRIFT}: target {target!r} has "
                f"{len(materialized)} column(s) but node declares {len(declared)}. "
                f"Declared: {declared!r}. Materialised: {materialized!r}."
            )
        for (m_name, m_type), (d_name, d_type) in zip(materialized, declared):
            if m_name != d_name:
                raise MaterializedSchemaDriftError(
                    f"{AIDPF_4070_MATERIALIZED_SCHEMA_DRIFT}: column name mismatch — "
                    f"materialised={m_name!r} declared={d_name!r}."
                )
            if _normalise_spark_type(m_type) != _normalise_spark_type(d_type):
                raise MaterializedSchemaDriftError(
                    f"{AIDPF_4070_MATERIALIZED_SCHEMA_DRIFT}: column {m_name!r} type "
                    f"mismatch — materialised={m_type!r} declared={d_type!r}."
                )

    canonical = "\n".join(f"{n}|{t}" for n, t in materialized)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


# ``_SPARK_TYPE_SYNONYMS`` / ``_normalise_spark_type`` now live in the Spark-free
# :mod:`orchestrator.spark_types` so the design-time column-contract gate
# (AIDPF-2045) shares the exact comparison used by the AIDPF-4070 runtime gate
# below. Re-imported here under their original names — the 4070 gate is unchanged.


# ---------------------------------------------------------------------------
# Helpers — target identifier + watermark + state-row assembly
# ---------------------------------------------------------------------------


def _build_target_identifier(
    node: "NodeYaml",  # noqa: F821
    ctx: RunContext,
    paths: "TablePaths",  # noqa: F821
) -> str:
    """Build the fully-qualified target identifier for a node.

    All three layers route through ``TablePaths.bronze`` / ``.silver`` /
    ``.gold`` so identifier validation
    (``^[A-Za-z_][A-Za-z0-9_]*$``) fires centrally — malformed
    ``node.target`` raises ``ValueError`` BEFORE any executor logic
    runs (no BICC call, no Spark write, no state-row write attempt).

    ``paths`` is required so the runner never composes raw catalog/schema
    strings without identifier validation.
    """
    layer = node.layer
    if layer == "bronze":
        return paths.bronze(node.target)
    if layer == "silver":
        return paths.silver(node.target)
    if layer == "gold":
        return paths.gold(node.target)
    raise ValueError(
        f"_build_target_identifier: unsupported layer={layer!r} for "
        f"node {node.id!r}"
    )


def _compute_output_watermark(
    spark: "SparkSession",
    node: "NodeYaml",  # noqa: F821
    ctx: RunContext,
    paths: "TablePaths",  # noqa: F821
    rendered: RenderedSql,
    strategy_result,
) -> datetime | None:
    """Compute the primary source's max watermark over the rows scanned.

    For ``replace`` strategy: probe the target's max watermark column
    value. For ``merge`` with non-empty delta: probe the source SELECT.
    For ``merge`` with empty delta: preserve the prior watermark (None
    here; the caller threads the prior value when writing the row).

    Defensive against missing watermark column — returns None if the
    column doesn't exist (which means the node has no incremental
    semantics, in which case the watermark field is informational).
    """
    if strategy_result.merge_skipped_empty_delta:
        return ctx.prior_watermark.get(
            node.refresh.incremental.watermark.source
            if node.refresh.incremental and node.refresh.incremental.watermark
            else None,
        )

    inc = node.refresh.incremental
    if inc is None or inc.watermark is None:
        return None
    column = inc.watermark.column
    target = _build_target_identifier(node, ctx, paths)
    try:
        df = spark.sql(f"SELECT MAX({column}) AS wm FROM {target}")
        rows = df.collect()
        if not rows:
            return None
        wm = rows[0][0]
        if isinstance(wm, datetime):
            return wm
        if wm is None:
            return None
        # Spark may return strings for timestamp columns in some
        # configurations; defensive coercion.
        try:
            return datetime.fromisoformat(str(wm))
        except (TypeError, ValueError):
            return None
    except Exception:  # noqa: BLE001 — informational; missing column shouldn't fail the run
        return None


def _assemble_success_state_rows(
    *,
    node,
    ctx: RunContext,
    pack,
    profile,
    mode: str,
    rendered_sql_hash: str,
    output_schema_hash: str,
    profile_hash: str,
    plan_hash: str,
    strategy_result,
    output_watermark: datetime | None,
    coa_additive_accept_reason: str | None = None,
) -> list[dict[str, Any]]:
    """Assemble the full state-row list (primary + every lookup).

    The full list is built in memory BEFORE the atomic batch write, making
    Delta append atomicity a true all-or-nothing commit.

    Single-source nodes produce exactly one row (primary). Multi-source
    nodes produce N rows: one primary + one per lookup source.
    """
    now = datetime.now(timezone.utc)
    primary_source_id = _resolve_primary_source_id(node)

    common = {
        "run_id": ctx.run_id,
        "dataset_id": node.id,
        "layer": node.layer,
        "mode": mode,
        "last_run_at": now,
        "status": "success",
        "row_count": strategy_result.rows_scanned,
        "error_message": None,
        "skip_reason": None,
        "duration_seconds": None,
        "plan_hash": plan_hash,
        "plan_snapshot": None,
        # Content-pack state columns.
        "pack_id": pack.pack.id,
        "pack_version": pack.pack.version,
        "node_version": None,
        "node_implementation_type": node.implementation.type,
        "rendered_sql_hash": rendered_sql_hash,
        "output_schema_hash": output_schema_hash,
        "profile_hash": profile_hash,
        "tenant_fingerprint": profile.tenant,
        "fusion_version": None,
        "bronze_schema_fingerprint": profile.bronze_schema_fingerprint,
        "input_watermark_start": None,
        "input_watermark_end": None,
        # Additive-COA fast-path acceptance trace (NULL on every non-accepted
        # row). Observability only — re-derivable from the paired manifest.
        "coa_additive_accept_reason": coa_additive_accept_reason,
    }

    rows: list[dict[str, Any]] = []

    # Primary source row — advances last_watermark.
    primary_row = {
        **common,
        "source_id": primary_source_id,
        "source_role": "primary",
        "last_watermark": output_watermark,
        "output_watermark": output_watermark,
        "consumed_version": None,
        "delta_row_count": strategy_result.rows_scanned,
    }
    rows.append(primary_row)

    # Lookup source rows — one per declared dependency beyond the primary.
    lookup_sources = _resolve_lookup_source_ids(node, primary_source_id)
    for lookup_id in lookup_sources:
        rows.append({
            **common,
            "source_id": lookup_id,
            "source_role": "lookup",
            # Lookup rows do NOT drive watermark advancement.
            "last_watermark": None,
            "output_watermark": None,
            "consumed_version": now,
            "delta_row_count": None,
        })

    return rows


def _resolve_primary_source_id(node) -> str | None:
    """Identify the primary source for a node."""
    inc = node.refresh.incremental
    if inc is not None and inc.watermark is not None:
        return inc.watermark.source
    deps = getattr(node, "depends_on", None)
    if deps and deps.bronze:
        return deps.bronze[0].id
    return None


def _resolve_lookup_source_ids(node, primary_id: str | None) -> list[str]:
    """List source IDs that aren't the primary — every other declared dep."""
    deps = getattr(node, "depends_on", None)
    if not deps:
        return []
    ids: list[str] = []
    for src in list(deps.bronze) + list(deps.silver):
        if src.id != primary_id:
            ids.append(src.id)
    return ids


# ---------------------------------------------------------------------------
# Soft state-row writers for failure paths
# ---------------------------------------------------------------------------
#
# All four use a single helper. They MUST NOT raise on a write failure
# — diagnostic state rows are best-effort; a Spark write failure here
# only loses the audit trail, not the cursor-advancement semantics
# (which require the hard atomic batch in the success path).


def _safe_write_failure_row(
    spark, paths, *, node, ctx, status: str, message: str, profile, plan_hash: str = ""
) -> None:
    now = datetime.now(timezone.utc)
    row = {
        "run_id": ctx.run_id,
        "dataset_id": node.id,
        "layer": node.layer,
        "mode": ctx.mode,
        "last_watermark": ctx.prior_watermark.get(_resolve_primary_source_id(node)),
        "last_run_at": now,
        "status": status,
        "row_count": 0,
        "error_message": message,
        "skip_reason": None,
        "duration_seconds": None,
        "plan_hash": plan_hash,
        "plan_snapshot": None,
        "pack_id": None,
        "pack_version": None,
        "node_version": None,
        "node_implementation_type": node.implementation.type,
        "rendered_sql_hash": None,
        "output_schema_hash": None,
        "profile_hash": None,
        "tenant_fingerprint": profile.tenant if profile is not None else None,
        "fusion_version": None,
        "bronze_schema_fingerprint": profile.bronze_schema_fingerprint if profile is not None else None,
        "source_id": _resolve_primary_source_id(node),
        "source_role": "primary",
        "input_watermark_start": None,
        "input_watermark_end": None,
        "output_watermark": None,
        "consumed_version": None,
        "delta_row_count": None,
    }
    try:
        state_phase2.write_state_rows_hard(spark, paths, [row])
    except Exception:  # noqa: BLE001 — diagnostic write is best-effort
        return


def _safe_write_preflight_blocked_row(spark, paths, *, node, ctx, message, profile) -> None:
    _safe_write_failure_row(
        spark, paths, node=node, ctx=ctx, status="preflight_blocked",
        message=message, profile=profile,
    )


def _safe_write_render_failed_row(spark, paths, *, node, ctx, message, profile) -> None:
    _safe_write_failure_row(
        spark, paths, node=node, ctx=ctx, status="render_failed",
        message=message, profile=profile,
    )


def _safe_write_resume_drift_row(
    spark, paths, *, node, ctx, message, profile, expected_plan_hash, prior_plan_hash,
) -> None:
    _safe_write_failure_row(
        spark, paths, node=node, ctx=ctx, status="resume_drift_blocked",
        message=message, profile=profile, plan_hash=expected_plan_hash,
    )


def _safe_write_strategy_failed_row(
    spark, paths, *, node, ctx, message, profile, plan_hash,
) -> None:
    _safe_write_failure_row(
        spark, paths, node=node, ctx=ctx, status="strategy_failed",
        message=message, profile=profile, plan_hash=plan_hash,
    )


def _safe_write_quality_failed_row(
    spark, paths, *, node, ctx, message, profile, plan_hash,
) -> None:
    _safe_write_failure_row(
        spark, paths, node=node, ctx=ctx, status="quality_failed",
        message=message, profile=profile, plan_hash=plan_hash,
    )


def _safe_write_schema_drift_row(
    spark, paths, *, node, ctx, message, profile, plan_hash,
) -> None:
    _safe_write_failure_row(
        spark, paths, node=node, ctx=ctx, status="output_schema_drift",
        message=message, profile=profile, plan_hash=plan_hash,
    )


def _safe_write_plan_hash_repin_row(
    spark, paths, *, node, ctx, expected_plan_hash, prior_plan_hash,
) -> None:
    """Best-effort ``--repin-plan-hash`` audit row (mode='plan_hash_repin').

    Like the other soft writers, MUST NOT raise — losing the audit row only
    loses the SOX trail, not the bypass semantics (the node's success row,
    written next, pins the new plan-hash regardless).
    """
    try:
        state_module.write_plan_hash_repin_row(
            spark, paths,
            run_id=ctx.run_id,
            dataset_id=node.id,
            layer=node.layer,
            expected_plan_hash=expected_plan_hash,
            prior_plan_hash=prior_plan_hash,
        )
    except Exception:  # noqa: BLE001 — audit write is best-effort
        return


def _resume_drift_or_repin(
    spark, paths, *, node, ctx, profile, mode, expected_plan_hash,
    prior_plan_hash, repin_plan_hash, revert_hint,
    coa_additive_accept: bool = False,
) -> "NodeExecutionResult | None":
    """Apply the AIDPF-4040 continuity gate with the --repin-plan-hash hatch.

    Returns a ``resume_drift_blocked`` :class:`NodeExecutionResult` to
    short-circuit on, or ``None`` to proceed with execution.

    When the gate would fire (incremental + diverged prior hash):

    * ``coa_additive_accept=True`` — the caller PROVED (feature:
      incremental-coa-chart-onboarding) that the node's entire plan-hash delta is
      an additive chart-of-accounts change (per-node prior-equivalent proof +
      additive verdict + staged checkpoint). Proceed and re-pin the new hash; the
      acceptance reason is persisted on the node's atomic success row by the
      caller. This is a content-checked, fail-closed acceptance — NOT the blunt
      ``--repin-plan-hash`` bypass.
    * ``repin_plan_hash=True`` — operator break-glass: write a
      ``mode='plan_hash_repin'`` audit row, WARN, and return ``None`` so
      execution proceeds and re-pins the new hash.
    * otherwise — write the drift row and return the blocked result.

    ``revert_hint`` tailors the per-site remediation text (SQL/profile edit
    vs adapter-version bump).
    """
    if not (mode == "incremental" and prior_plan_hash and prior_plan_hash != expected_plan_hash):
        return None

    if coa_additive_accept:
        logger.info(
            "%s accepted on node %r as a proven additive chart-of-accounts change "
            "— plan-hash advanced from %s... to %s... (reason recorded on the "
            "success row).",
            AIDPF_4040_PLAN_HASH_DRIFT, node.id,
            prior_plan_hash[:16], expected_plan_hash[:16],
        )
        return None

    if repin_plan_hash:
        logger.warning(
            "%s bypassed by --repin-plan-hash on node %r — plan-hash repinned "
            "from %s... to %s... (audit row mode='plan_hash_repin'). This "
            "asserts the plan edit was deliberate; production/SOX runs MUST NOT "
            "use this flag.",
            AIDPF_4040_PLAN_HASH_DRIFT, node.id,
            prior_plan_hash[:16], expected_plan_hash[:16],
        )
        _safe_write_plan_hash_repin_row(
            spark, paths, node=node, ctx=ctx,
            expected_plan_hash=expected_plan_hash, prior_plan_hash=prior_plan_hash,
        )
        return None

    message = (
        f"{AIDPF_4040_PLAN_HASH_DRIFT}: plan-hash drift on resume — "
        f"expected={expected_plan_hash[:16]}... prior={prior_plan_hash[:16]}... "
        f"Re-run with --mode seed (or {revert_hint}), or pass --repin-plan-hash "
        f"if the change was deliberate."
    )
    _safe_write_resume_drift_row(
        spark, paths, node=node, ctx=ctx, message=message, profile=profile,
        expected_plan_hash=expected_plan_hash, prior_plan_hash=prior_plan_hash,
    )
    return NodeExecutionResult(
        status="resume_drift_blocked",
        error_message=message,
        plan_hash=expected_plan_hash,
    )


# ---------------------------------------------------------------------------
# Additive-COA fast-path acceptance (feature: incremental-coa-chart-onboarding)
# ---------------------------------------------------------------------------


def _coa_additive_reason(
    *,
    coa_inc,
    node,
    prior_run_id,
    prior_plan_hash,
    expected_plan_hash,
    mode,
    recompute_hash,
) -> "str | None":
    """Return the acceptance reason if this node's AIDPF-4040 drift is a proven
    additive COA change, else ``None``. No-op (returns ``None``) unless the gate
    would actually fire (incremental + diverged prior hash) and an active
    :class:`CoaIncrementalContext` was threaded in.

    ``recompute_hash(prior_coa_projection, prior_profile_hash)`` is path-specific
    (the caller owns rendering) and returns this node's plan-hash as it WOULD have
    been under the prior COA baseline.
    """
    if coa_inc is None or not getattr(coa_inc, "active", False):
        return None
    if not (mode == "incremental" and prior_plan_hash and prior_plan_hash != expected_plan_hash):
        return None
    return coa_inc.coa_accept_reason(
        node=node,
        prior_run_id=prior_run_id,
        stored_prior_hash=prior_plan_hash,
        recompute_hash=recompute_hash,
    )


def _profile_with_prior_coa(profile, prior_coa_projection):
    """Deep-clone ``profile`` and swap ONLY its ``chartOfAccounts`` for the prior
    COA baseline, so a node can be re-rendered under the prior mapping without
    mutating live profile state (per-node prior-equivalent proof)."""
    from .coa_change import projection_to_coa_dict

    clone = profile.model_copy(deep=True)
    clone.profile = dict(clone.profile or {})
    clone.profile["chartOfAccounts"] = projection_to_coa_dict(prior_coa_projection)
    return clone


# ---------------------------------------------------------------------------
# Builtin dispatch
# ---------------------------------------------------------------------------


def _build_dim_calendar_adapter_entry():
    """Lazily import the dim_calendar adapter to avoid an orchestrator-load-time
    dependency cycle (``dim_calendar`` imports from ``config.paths`` which
    transitively imports parts of ``orchestrator``)."""
    from .builtins import dim_calendar_adapter
    return (dim_calendar_adapter.run, dim_calendar_adapter.VERSION)


_BUILTIN_REGISTRY: dict[str, "tuple[Any, str]"] = {
    # Keyed by NodeImpl.callable (the dotted ``<module>:<func>`` form authored
    # in node YAML). Value is (adapter_func, version_string). The adapter
    # function has uniform signature
    # ``(spark, *, node, pack, profile, ctx) -> DataFrame``; the version flows
    # into the content-pack plan-hash as the rendered_sql_hash substitute.
    #
    # Initial entry is dim_calendar per ADR-0011. New builtins MUST be
    # listed here — auto-importing arbitrary callables is the AIDPF-5014
    # surface this gate prevents.
}


def _ensure_registry_populated() -> None:
    """Populate :data:`_BUILTIN_REGISTRY` on first dispatch.

    Lazy population sidesteps an orchestrator-load-time circular import
    (sql_runner ← orchestrator ← dim_calendar) without forcing the
    adapter import at module top.
    """
    if not _BUILTIN_REGISTRY:
        _BUILTIN_REGISTRY[
            "oracle_ai_data_platform_fusion_autopilot.dimensions.dim_calendar:build"
        ] = _build_dim_calendar_adapter_entry()


def _builtin_rendered_sql_hash_substitute(callable_id: str, version: str) -> str:
    """Compute the rendered_sql_hash substitute for a builtin dispatch.

    The content-pack plan-hash signature requires a ``rendered_sql_hash``
    string. For SQL nodes that's the canonical hash of the rendered
    template + bound params; for builtins it's a sha256 of
    ``<callable_id>:<version>``. Bumping the adapter's VERSION constant
    flips this hash, triggering the AIDPF-4040 drift gate just like a
    SQL-template edit does.

    Used for ``type: builtin`` and ``type: bronze_extract`` dispatch
    where the callable is plugin-internal (dim_calendar adapter,
    bronze_extract_adapter) — its source code moves with the plugin
    version, so the version constant is a sufficient drift signal.
    """
    return hashlib.sha256(f"{callable_id}:{version}".encode("utf-8")).hexdigest()


def _execute_builtin_node(
    spark: "SparkSession",
    *,
    node: "NodeYaml",  # noqa: F821
    pack: "ResolvedPack",  # noqa: F821
    profile: "TenantProfile",  # noqa: F821
    ctx: RunContext,
    paths: "TablePaths",  # noqa: F821
    mode: Literal["seed", "incremental"],
    profile_hash: str,
    prior_plan_hash: str | None,
    target_override: str | None,
    repin_plan_hash: bool = False,
    coa_inc: "Any | None" = None,
    prior_run_id: str | None = None,
) -> NodeExecutionResult:
    """Execute a ``type: builtin`` node via the registry.

    Lifecycle mirrors the SQL path
    (preflight → plan-hash → drift gate → execute → quality → schema
    assertion → state-row write) but skips ``render_node_sql`` (builtins
    have no SQL template) and substitutes the builtin's (callable,
    version) for the rendered_sql_hash so the drift gate stays uniform.
    """
    _ensure_registry_populated()

    # ----- Step 1: static validation done by the loader. -------------

    # ----- Step 2: preflight (column probes + identity validation). --
    # For builtins with empty bronze deps (dim_calendar), preflight is a
    # no-op pass. Keeping the call uniform avoids special-casing.
    preflight = preflight_node(spark, node, pack, profile, ctx)
    if not preflight.ok:
        message = "; ".join(f"[{e.code}] {e.message}" for e in preflight.errors)
        _safe_write_preflight_blocked_row(
            spark, paths, node=node, ctx=ctx, message=message, profile=profile,
        )
        return NodeExecutionResult(status="preflight_blocked", error_message=message)

    # ----- Step 3: resolve the builtin adapter ----------------------
    callable_id = node.implementation.callable  # type: ignore[union-attr]
    entry = _BUILTIN_REGISTRY.get(callable_id)
    if entry is None:
        message = (
            f"{AIDPF_5014_UNKNOWN_BUILTIN_DISPATCH}: builtin callable "
            f"{callable_id!r} not in _BUILTIN_REGISTRY. Registered: "
            f"{sorted(_BUILTIN_REGISTRY.keys())!r}. Add an adapter under "
            f"orchestrator/builtins/ and register it before content-pack "
            f"dispatch can run."
        )
        _safe_write_render_failed_row(
            spark, paths, node=node, ctx=ctx, message=message, profile=profile,
        )
        return NodeExecutionResult(status="render_failed", error_message=message)
    adapter_func, adapter_version = entry

    # ----- Step 4: compute plan-hash inputs --------------------------
    rendered_sql_hash = _builtin_rendered_sql_hash_substitute(callable_id, adapter_version)
    output_schema_hash = plan_hash_module.compute_output_schema_hash(node)
    expected_plan_hash = plan_hash_module.compute_content_pack_plan_hash(
        pack=pack,
        node=node,
        profile=profile,
        rendered_sql_hash=rendered_sql_hash,
        output_schema_hash=output_schema_hash,
        profile_hash=profile_hash,
    )

    # ----- Step 5: plan-hash drift gate (incremental only) ----------
    # A builtin's rendered-hash substitute does NOT depend on the COA mapping,
    # so its prior-equivalent hash reuses the current substitute + the stored
    # prior profile_hash (only profile_hash flips under a COA-only change).
    def _builtin_recompute_prior_equiv(prior_coa, prior_profile_hash):
        return plan_hash_module.compute_content_pack_plan_hash(
            pack=pack, node=node, profile=profile,
            rendered_sql_hash=rendered_sql_hash,
            output_schema_hash=output_schema_hash,
            profile_hash=prior_profile_hash,
        )

    coa_accept_reason = _coa_additive_reason(
        coa_inc=coa_inc, node=node, prior_run_id=prior_run_id,
        prior_plan_hash=prior_plan_hash, expected_plan_hash=expected_plan_hash,
        mode=mode, recompute_hash=_builtin_recompute_prior_equiv,
    )
    drift_result = _resume_drift_or_repin(
        spark, paths, node=node, ctx=ctx, profile=profile, mode=mode,
        expected_plan_hash=expected_plan_hash, prior_plan_hash=prior_plan_hash,
        repin_plan_hash=repin_plan_hash,
        revert_hint="revert the YAML / adapter version change",
        coa_additive_accept=coa_accept_reason is not None,
    )
    if drift_result is not None:
        return drift_result

    # ----- Step 6: invoke the adapter --------------------------------
    target = target_override or _build_target_identifier(node, ctx, paths)
    try:
        adapter_func(spark, node=node, pack=pack, profile=profile, ctx=ctx)
    except Exception as exc:  # noqa: BLE001 — surface any adapter failure uniformly
        message = f"builtin_failed: {exc}"
        _safe_write_strategy_failed_row(
            spark, paths, node=node, ctx=ctx, message=message, profile=profile,
            plan_hash=expected_plan_hash,
        )
        return NodeExecutionResult(
            status="strategy_failed",
            error_message=message,
            plan_hash=expected_plan_hash,
        )

    # ----- Step 7: quality tests -------------------------------------
    target_df = spark.table(target)
    quality_report = run_quality_tests(spark, node, target_df, ctx)
    if not quality_report.ok:
        message = "; ".join(f"[{f.test_type}] {f.message}" for f in quality_report.failures)
        _safe_write_quality_failed_row(
            spark, paths, node=node, ctx=ctx, message=message, profile=profile,
            plan_hash=expected_plan_hash,
        )
        return NodeExecutionResult(
            status="quality_failed",
            error_message=message,
            plan_hash=expected_plan_hash,
        )

    # ----- Step 8: materialised-schema assertion ---------------------
    try:
        materialized_schema_hash = _assert_materialized_matches_declared(
            spark, target, node
        )
    except MaterializedSchemaDriftError as exc:
        message = str(exc)
        _safe_write_schema_drift_row(
            spark, paths, node=node, ctx=ctx, message=message, profile=profile,
            plan_hash=expected_plan_hash,
        )
        return NodeExecutionResult(
            status="output_schema_drift",
            error_message=message,
            plan_hash=expected_plan_hash,
        )

    # ----- Step 9: assemble + write success state rows --------------
    # Builtins are parameter-driven (no bronze source → no watermark);
    # we synthesise a thin strategy_result-shaped object so the existing
    # _assemble_success_state_rows helper handles the row layout.
    class _BuiltinStrategyResult:
        rows_scanned = target_df.count() if hasattr(target_df, "count") else 0
        merge_skipped_empty_delta = False

    strategy_result = _BuiltinStrategyResult()
    state_rows = _assemble_success_state_rows(
        node=node,
        ctx=ctx,
        pack=pack,
        profile=profile,
        mode=mode,
        rendered_sql_hash=rendered_sql_hash,
        output_schema_hash=output_schema_hash,
        profile_hash=profile_hash,
        plan_hash=expected_plan_hash,
        strategy_result=strategy_result,
        output_watermark=None,  # builtins are parameter-driven, no cursor.
        coa_additive_accept_reason=coa_accept_reason,
    )

    try:
        state_phase2.write_state_rows_hard(spark, paths, state_rows)
    except state_phase2.StateCommitError as exc:
        message = f"state_commit_failed: {exc}"
        return NodeExecutionResult(
            status="state_commit_failed",
            error_message=message,
            plan_hash=expected_plan_hash,
            row_count=strategy_result.rows_scanned,
        )

    return NodeExecutionResult(
        status="success",
        row_count=strategy_result.rows_scanned,
        output_watermark=None,
        materialized_schema_hash=materialized_schema_hash,
        plan_hash=expected_plan_hash,
    )


# ---------------------------------------------------------------------------
# Bronze extract dispatch
# ---------------------------------------------------------------------------


def _own_required_columns(
    node: "NodeYaml",  # noqa: F821
    pack: "ResolvedPack",  # noqa: F821
    profile: "TenantProfile | None",  # noqa: F821
) -> set[str]:
    """The bronze node's OWN ``requiredColumns`` resolved to physical names.

    A bronze node's ``requiredColumns`` are columns the extract *asserts exist*
    in the live PVO. Historically they were a subset of ``outputSchema`` (so the
    AIDPF-4071 gate covered them via the outputSchema check), but the
    bronze-required-columns overlay can now add a required column that is NOT in
    ``outputSchema`` — those must still be probed against the live PVO. Resolved
    via the shared resolver so ``$column.*`` / ``$coa.*`` entries map to the
    tenant's physical columns (unresolved refs drop, exactly as the run-level
    gates do). Keys in ``requiredColumns`` are source ids; for a bronze extract
    they refer to this node's own PVO, so we union across all of them.
    """
    from .required_column_resolver import resolve_required_column_entries

    own: set[str] = set()
    for cols in (getattr(node, "required_columns", None) or {}).values():
        own |= resolve_required_column_entries(
            cols, resolved_pack=pack, tenant_profile=profile
        )
    return own


def _source_schema_miss(
    node: "NodeYaml",  # noqa: F821
    st: "object | None",
    *,
    run_id: str,
    tenant: str | None,
    extra_required: "set[str] | frozenset[str]" = frozenset(),
) -> tuple[str, dict] | None:
    """Given a bronze node and its live PVO ``StructType`` (or None),
    return ``(message, AIDPF-4071 diagnostic)`` if any *wanted* column is
    absent from the PVO (presence-only, case-insensitive); ``None`` when
    all present / nothing to check.

    *Wanted* = the node's declared non-audit ``outputSchema`` columns,
    PLUS ``extra_required`` — which the callers populate with BOTH the
    bronze node's OWN resolved ``requiredColumns`` (so an overlay-added
    required column absent from ``outputSchema`` is still probed) AND the
    columns in-scope silver/gold nodes need from this bronze source (so a
    downstream need the PVO can't satisfy fails BEFORE extraction, not
    after a 21-minute pull). Audit columns (``_``-prefixed) are excluded
    from both: they're adapter-generated and always present post-extract.

    Shared by the per-node gate and the batch gate so the message +
    diagnostic shape stay identical.
    """
    wanted = {
        col.name for col in node.output_schema.columns
        if not col.name.startswith("_")  # audit cols are adapter-generated
    }
    wanted |= {c for c in extra_required if not c.startswith("_")}
    if not wanted or st is None:
        return None
    present_ci = {f.name.lower() for f in st.fields}
    missing = sorted(c for c in wanted if c.lower() not in present_ci)
    if not missing:
        return None
    sample = sorted(f.name for f in st.fields)[:40]
    message = (
        f"{AIDPF_4071_BRONZE_SOURCE_COLUMN_MISSING}: bronze node {node.id!r} "
        f"(or an in-scope silver/gold node) requires column(s) absent from "
        f"live PVO {node.implementation.datastore!r}: {missing!r}. The PVO "
        f"exposes {len(st.fields)} column(s) (sample: {sample!r}). Extract "
        f"skipped to avoid a multi-minute pull that can't feed the medallion. "
        f"Fix the declared column names or author a columnAlias overlay "
        f"(run /medallion-author)."
    )
    diagnostic = {
        "schemaVersion": 1,
        "runId": run_id,
        "tenant": tenant,
        "errorCode": AIDPF_4071_BRONZE_SOURCE_COLUMN_MISSING,
        "errorMessage": message,
        "generatedAt": datetime.now(timezone.utc).isoformat(),
        "node": node.id,
        "datastore": node.implementation.datastore,
        "missingColumns": missing,
        "pvoColumns": [
            {"name": f.name, "type": f.dataType.simpleString(),
             "nullable": bool(f.nullable)}
            for f in st.fields
        ],
    }
    return message, diagnostic


def _source_type_miss(
    node: "NodeYaml",  # noqa: F821
    st: "object | None",
    *,
    run_id: str,
    tenant: str | None,
) -> tuple[str, dict] | None:
    """Given a bronze node and its live PVO ``StructType`` (or None), return
    ``(message, AIDPF-4070 diagnostic)`` if any declared non-audit
    ``outputSchema`` column that IS present in the PVO carries a type that
    differs from the declared type (after :func:`_normalise_spark_type`);
    ``None`` when every present column's type matches / nothing to check.

    Columns *absent* from the PVO are :data:`AIDPF-4071`'s responsibility and
    are skipped here (run the name gate first). Audit columns (``_``-prefixed)
    are adapter-generated and excluded.

    This HOISTS the post-write ``AIDPF-4070`` assertion
    (:func:`_assert_materialized_matches_declared` ``subset=True``) to a
    pre-extract, metadata-only check, so declared-vs-live *type* drift fails
    in seconds — before the multi-minute pull that would otherwise
    materialise and then be rejected post-write. It uses the SAME
    case-insensitive name match + ``_normalise_spark_type`` comparison so the
    two gates agree. The post-write assertion stays as the authoritative net
    for any residual inferSchema-vs-materialised divergence.
    """
    if st is None:
        return None
    present_ci = {f.name.lower(): f.dataType.simpleString() for f in st.fields}
    mismatches: list[dict] = []
    for col in node.output_schema.columns:
        if col.name.startswith("_"):  # audit cols are adapter-generated
            continue
        live = present_ci.get(col.name.lower())
        if live is None:
            continue  # absence is AIDPF-4071's job, not this gate's
        if _normalise_spark_type(live) != _normalise_spark_type(col.type):
            mismatches.append(
                {"column": col.name, "declared": col.type, "materialised": live}
            )
    if not mismatches:
        return None
    summary = ", ".join(
        f"{m['column']} (declared={m['declared']} live={m['materialised']})"
        for m in mismatches
    )
    message = (
        f"{AIDPF_4070_MATERIALIZED_SCHEMA_DRIFT}: bronze node {node.id!r} "
        f"declares outputSchema type(s) that differ from live PVO "
        f"{node.implementation.datastore!r}: {summary}. Extract skipped "
        f"(metadata-only preflight) to avoid a multi-minute pull that would "
        f"fail the post-write schema assertion. Fix the declared outputSchema "
        f"type(s) to match the live PVO (or author a columnAlias/type overlay)."
    )
    diagnostic = {
        "schemaVersion": 1,
        "runId": run_id,
        "tenant": tenant,
        "errorCode": AIDPF_4070_MATERIALIZED_SCHEMA_DRIFT,
        "errorMessage": message,
        "generatedAt": datetime.now(timezone.utc).isoformat(),
        "node": node.id,
        "datastore": node.implementation.datastore,
        "typeMismatches": mismatches,
        "pvoColumns": [
            {"name": f.name, "type": f.dataType.simpleString(),
             "nullable": bool(f.nullable)}
            for f in st.fields
        ],
    }
    return message, diagnostic


def check_bronze_source_schemas(
    spark: "SparkSession",  # noqa: F821
    *,
    pack: "ResolvedPack",  # noqa: F821
    bundle: "object",
    profile: "TenantProfile",  # noqa: F821
    bronze_node_ids: "list[str]",
    run_id: str,
    downstream_required: "dict[str, set[str]] | None" = None,
) -> list[dict]:
    """Batch pre-ingest source-schema gate (AIDPF-4071) for the whole run.

    ONE metadata-only ``probe_bronze_schemas`` call over every in-scope
    bronze node, BEFORE the orchestrator extracts anything. For each bronze
    node it checks that the live PVO can supply BOTH (a) the node's declared
    ``outputSchema`` columns AND (b) ``downstream_required[node_id]`` — the
    columns in-scope silver/gold nodes need from it (transitively resolved
    by the caller). Returns ``{"node", "message", "diagnostic"}`` per bad
    node; empty when all clear.

    Running this before the per-node loop means a node whose source can't
    feed the medallion aborts the run in *seconds* — before the
    multi-minute bronze extract — and reports every gap at once. Degrades
    to ``[]`` on any probe failure (the per-node gate / real extract then
    surface connectivity/auth problems with a better diagnostic).
    """
    from .builtins import bronze_extract_adapter as _bronze_adapter
    from .runtime import _resolve_password

    if not bronze_node_ids or bundle is None:
        return []
    downstream_required = downstream_required or {}
    try:
        pw = _resolve_password(bundle.fusion.password).get_secret_value()
        schemas = _bronze_adapter.probe_bronze_schemas(
            spark, pack=pack, bundle=bundle,
            resolved_password=pw, dataset_ids=list(bronze_node_ids),
        )
    except Exception:  # noqa: BLE001 — probe failure → defer to per-node/extract
        return []
    tenant = getattr(profile, "tenant", None)
    failures: list[dict] = []
    for nid in bronze_node_ids:
        node = pack.bronze.get(nid)
        if node is None:
            continue
        st = schemas.get(nid)
        extra = set(downstream_required.get(nid, frozenset()))
        extra |= _own_required_columns(node, pack, profile)
        res = _source_schema_miss(
            node, st, run_id=run_id, tenant=tenant, extra_required=extra,
        )
        if res is None:
            # Names all present → validate declared types against the live
            # PVO before extraction (hoisted AIDPF-4070).
            res = _source_type_miss(node, st, run_id=run_id, tenant=tenant)
        if res is not None:
            msg, diag = res
            failures.append({"node": nid, "message": msg, "diagnostic": diag})
    return failures


def _bronze_source_schema_gate(
    spark: "SparkSession",  # noqa: F821
    *,
    node: "NodeYaml",  # noqa: F821
    pack: "ResolvedPack",  # noqa: F821
    profile: "TenantProfile",  # noqa: F821
    ctx: RunContext,
) -> tuple[str, dict] | None:
    """Per-node pre-ingest source-schema gate (AIDPF-4071) — defense-in-depth.

    Metadata-only BICC ``inferSchema`` probe over THIS node's PVO (no row
    pull, no write). Returns ``(message, diagnostic)`` when a declared
    non-audit ``outputSchema`` column is absent from the live PVO, else
    ``None``. The orchestrator normally catches this earlier via the batch
    :func:`check_bronze_source_schemas`; this per-node check still guards
    direct ``execute_node`` callers that bypass the batch pass.

    Degrades to ``None`` (proceed) on any probe failure — the real extract
    surfaces a connectivity/auth problem with a better diagnostic.
    """
    from .builtins import bronze_extract_adapter as _bronze_adapter
    from .runtime import _resolve_password

    bundle = ctx.bundle
    if bundle is None:
        return None  # run() raises with a precise contract error downstream
    try:
        pw = _resolve_password(bundle.fusion.password).get_secret_value()
        schemas = _bronze_adapter.probe_bronze_schemas(
            spark, pack=pack, bundle=bundle,
            resolved_password=pw, dataset_ids=[node.id],
        )
    except Exception:  # noqa: BLE001 — probe failure → defer to the real extract
        return None
    st = schemas.get(node.id)
    tenant = getattr(profile, "tenant", None)
    name_miss = _source_schema_miss(
        node, st, run_id=ctx.run_id, tenant=tenant,
        extra_required=_own_required_columns(node, pack, profile),
    )
    if name_miss is not None:
        return name_miss
    # Names present → validate declared types vs the live PVO (hoisted 4070).
    return _source_type_miss(node, st, run_id=ctx.run_id, tenant=tenant)


def _execute_bronze_extract_node(
    spark: "SparkSession",
    *,
    node: "NodeYaml",  # noqa: F821
    pack: "ResolvedPack",  # noqa: F821
    profile: "TenantProfile",  # noqa: F821
    ctx: RunContext,
    paths: "TablePaths",  # noqa: F821
    mode: Literal["seed", "incremental"],
    profile_hash: str,
    prior_plan_hash: str | None,
    target_override: str | None,
    repin_plan_hash: bool = False,
    coa_inc: "Any | None" = None,
    prior_run_id: str | None = None,
) -> NodeExecutionResult:
    """Execute a ``type: bronze_extract`` node via the bronze adapter.

    Lifecycle mirrors :func:`_execute_builtin_node` (preflight →
    plan-hash → drift gate → invoke → quality → schema assertion →
    state-row write); the adapter returns
    ``(target_df, bronze_output_watermark)`` instead of a bare
    DataFrame so the cursor (extraction-time, not source-row-max) can
    be passed straight to ``_assemble_success_state_rows``.
    """
    from .builtins import bronze_extract_adapter as _bronze_adapter

    # ----- Step 1: static validation done by the loader. -------------

    # ----- Step 2: preflight (identity + bundle-side validation). ----
    preflight = preflight_node(spark, node, pack, profile, ctx)
    if not preflight.ok:
        message = "; ".join(f"[{e.code}] {e.message}" for e in preflight.errors)
        _safe_write_preflight_blocked_row(
            spark, paths, node=node, ctx=ctx, message=message, profile=profile,
        )
        return NodeExecutionResult(status="preflight_blocked", error_message=message)

    # ----- Step 3: pre-ingest source-schema gate (AIDPF-4071) --------
    # Metadata-only PVO probe BEFORE the extract: fail fast (seconds) if a
    # column the pack wants is absent from the live PVO, rather than after
    # a multi-minute pull. Presence-only + case-insensitive (silver casts;
    # BICC lowercases). Runs for seed AND incremental.
    source_gate = _bronze_source_schema_gate(
        spark, node=node, pack=pack, profile=profile, ctx=ctx
    )
    if source_gate is not None:
        source_gate_msg, source_gate_diag = source_gate
        # A type mismatch reports as the same status as the post-write 4070
        # assertion (output_schema_drift); a missing/renamed column keeps
        # source_schema_missing. Both abort the node before extraction.
        _gate_status = (
            "output_schema_drift"
            if source_gate_diag.get("errorCode") == AIDPF_4070_MATERIALIZED_SCHEMA_DRIFT
            else "source_schema_missing"
        )
        _safe_write_failure_row(
            spark, paths, node=node, ctx=ctx,
            status=_gate_status, message=source_gate_msg,
            profile=profile,
        )
        return NodeExecutionResult(
            status=_gate_status, error_message=source_gate_msg,
            diagnostic=source_gate_diag,
        )

    # ----- Step 4: compute plan-hash inputs --------------------------
    callable_id = f"bronze_extract:{node.implementation.datastore}"
    rendered_sql_hash = _builtin_rendered_sql_hash_substitute(
        callable_id, _bronze_adapter.VERSION
    )
    output_schema_hash = plan_hash_module.compute_output_schema_hash(node)
    expected_plan_hash = plan_hash_module.compute_content_pack_plan_hash(
        pack=pack,
        node=node,
        profile=profile,
        rendered_sql_hash=rendered_sql_hash,
        output_schema_hash=output_schema_hash,
        profile_hash=profile_hash,
    )

    # ----- Step 5: plan-hash drift gate (incremental only) ----------
    # The bronze adapter's rendered-hash substitute does NOT depend on the COA
    # mapping (gl_coa is the COA SOURCE, it does not consume the mapping), so its
    # prior-equivalent hash reuses the current substitute + the stored prior
    # profile_hash. gl_coa is a COA SOURCE, so its acceptance does not require the
    # post-land checkpoint (which hasn't run yet) — see CoaIncrementalContext.
    def _bronze_recompute_prior_equiv(prior_coa, prior_profile_hash):
        return plan_hash_module.compute_content_pack_plan_hash(
            pack=pack, node=node, profile=profile,
            rendered_sql_hash=rendered_sql_hash,
            output_schema_hash=output_schema_hash,
            profile_hash=prior_profile_hash,
        )

    coa_accept_reason = _coa_additive_reason(
        coa_inc=coa_inc, node=node, prior_run_id=prior_run_id,
        prior_plan_hash=prior_plan_hash, expected_plan_hash=expected_plan_hash,
        mode=mode, recompute_hash=_bronze_recompute_prior_equiv,
    )
    drift_result = _resume_drift_or_repin(
        spark, paths, node=node, ctx=ctx, profile=profile, mode=mode,
        expected_plan_hash=expected_plan_hash, prior_plan_hash=prior_plan_hash,
        repin_plan_hash=repin_plan_hash,
        revert_hint="revert the YAML / adapter version change",
        coa_additive_accept=coa_accept_reason is not None,
    )
    if drift_result is not None:
        return drift_result

    # ----- Step 6: invoke the bronze adapter --------------------------
    # Pass `paths` so the bronze branch routes through TablePaths.bronze and
    # post-write schema assertion describes the actual bronze table.
    target = target_override or _build_target_identifier(node, ctx, paths)
    try:
        target_df, bronze_output_watermark = _bronze_adapter.run(
            spark,
            node=node,
            pack=pack,
            profile=profile,
            ctx=ctx,
            paths=paths,
            mode=mode,
        )
    except Exception as exc:  # noqa: BLE001 — surface any adapter failure uniformly
        message = f"bronze_extract_failed: {exc}"
        _safe_write_strategy_failed_row(
            spark, paths, node=node, ctx=ctx, message=message, profile=profile,
            plan_hash=expected_plan_hash,
        )
        return NodeExecutionResult(
            status="strategy_failed",
            error_message=message,
            plan_hash=expected_plan_hash,
        )

    # ----- Step 7: quality tests -------------------------------------
    quality_report = run_quality_tests(spark, node, target_df, ctx)
    if not quality_report.ok:
        message = "; ".join(f"[{f.test_type}] {f.message}" for f in quality_report.failures)
        _safe_write_quality_failed_row(
            spark, paths, node=node, ctx=ctx, message=message, profile=profile,
            plan_hash=expected_plan_hash,
        )
        return NodeExecutionResult(
            status="quality_failed",
            error_message=message,
            plan_hash=expected_plan_hash,
        )

    # ----- Step 8: materialised-schema assertion ---------------------
    # Subset semantics: bronze writes the full raw PVO; outputSchema is
    # the minimum guaranteed contract, not an exact mirror. Assert every
    # declared column is present + typed; allow extra raw columns.
    try:
        materialized_schema_hash = _assert_materialized_matches_declared(
            spark, target, node, subset=True
        )
    except MaterializedSchemaDriftError as exc:
        message = str(exc)
        _safe_write_schema_drift_row(
            spark, paths, node=node, ctx=ctx, message=message, profile=profile,
            plan_hash=expected_plan_hash,
        )
        return NodeExecutionResult(
            status="output_schema_drift",
            error_message=message,
            plan_hash=expected_plan_hash,
        )

    # ----- Step 9: state rows -----------------------------------------
    # Bronze cursor is extraction-time (not source-row-max), so we
    # use the adapter's returned value directly — NOT
    # _compute_output_watermark.
    rows_scanned = target_df.count() if hasattr(target_df, "count") else 0

    class _BronzeStrategyResult:
        merge_skipped_empty_delta = False

    _BronzeStrategyResult.rows_scanned = rows_scanned
    strategy_result = _BronzeStrategyResult()

    state_rows = _assemble_success_state_rows(
        node=node,
        ctx=ctx,
        pack=pack,
        profile=profile,
        mode=mode,
        rendered_sql_hash=rendered_sql_hash,
        output_schema_hash=output_schema_hash,
        profile_hash=profile_hash,
        plan_hash=expected_plan_hash,
        strategy_result=strategy_result,
        output_watermark=bronze_output_watermark,
        coa_additive_accept_reason=coa_accept_reason,
    )

    try:
        state_phase2.write_state_rows_hard(spark, paths, state_rows)
    except state_phase2.StateCommitError as exc:
        message = f"state_commit_failed: {exc}"
        return NodeExecutionResult(
            status="state_commit_failed",
            error_message=message,
            plan_hash=expected_plan_hash,
            row_count=rows_scanned,
        )

    return NodeExecutionResult(
        status="success",
        row_count=rows_scanned,
        output_watermark=bronze_output_watermark,
        materialized_schema_hash=materialized_schema_hash,
        plan_hash=expected_plan_hash,
    )
