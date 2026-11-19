"""Bronze content-pack extract adapter.

Implements the bronze extraction algorithm as a content-pack adapter.
The adapter keeps extraction, cursoring, payload diffing, schema
reconciliation, and write strategy in one bronze-owned surface.

The adapter exposes two surfaces:

* :func:`run` — execute a single bronze node end-to-end and return
  ``(target_df, bronze_output_watermark)``. The dispatcher
  (``_execute_bronze_extract_node`` in ``sql_runner``) threads the
  output watermark into the state row directly, replacing
  ``_compute_output_watermark`` (which is source-row-max semantics —
  correct for silver/gold, wrong for bronze).
* :func:`probe_bronze_schemas` — metadata-only BICC ``inferSchema``
  probe over the bronze nodes in a resolved plan. Backs the
  ``AIDPF-2072`` PVO drift gate. Reads ``NodeYaml`` directly off the
  resolved pack; no engine-spec lookup involved.

Algorithm:

1. Construct ``PvoEntry``-equivalent descriptor from node YAML fields
   (no ``fusion_catalog.get()`` lookup — pack YAML is self-contained
   so customer overlay packs work without a catalog entry).
2. Resolve effective BICC offering schema: tenant
   ``bundle.fusion.schemaOverrides.<id>`` > node ``schemaOverride`` >
   node ``biccSchema``.
3. Determine behavior from (mode, incremental_capable, prior_cursor,
   target_exists). First-incremental + no prior cursor downgrades to
   seed-shape replace regardless of ``incremental_capable``.
4. Capture ``extract_started_at`` BEFORE BICC. Persisted cursor =
   ``extract_started_at - safety_window`` on non-empty extract.
5. Call BICC ``extract_pvo`` with effective schema + push-down
   (None for seed / first-incremental / incremental_capable=False).
6. Payload-diff for ``incremental_capable=False`` PVOs with prior cursor.
7. Schema reconciliation BEFORE write.
8. Add deterministic audit cols
   (``_extract_ts``, ``_source_pvo``, ``_run_id``, ``_watermark_used``).
9. Write via strategy (replace for seed / first-incremental;
   payload-diff-gated MERGE for incremental).
10. Empty-delta preserves prior cursor.
11. Return ``(target_df, output_watermark)``.

Error codes registered here:

* ``AIDPF-2092 — BRONZE_CURSOR_TARGET_DESYNC`` — bronze adapter found
  a prior persisted cursor with no matching target table, indicating
  state corruption; raises rather than silently degrading.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any, Iterable

if TYPE_CHECKING:  # pragma: no cover
    from pyspark.sql import DataFrame, SparkSession
    from pyspark.sql.types import StructType

    from ...config.paths import TablePaths
    from ...schema.bundle import Bundle
    from ...schema.medallion_pack import NodeYaml
    from ...schema.tenant_profile import TenantProfile
    from ..content_pack import ResolvedPack
    from ..sql_renderer import RunContext


_LOG = logging.getLogger(__name__)


VERSION: str = "1.0.0"
"""Adapter version constant. Flows into the content-pack plan-hash
substitute for bronze_extract nodes — bumping this triggers the same
drift gate as a SQL-template edit."""


# Error codes documented in docs/aidpf-error-codes.md.
AIDPF_2092_BRONZE_CURSOR_TARGET_DESYNC = "AIDPF-2092"


class BronzeCursorTargetDesyncError(Exception):
    """Bronze prior cursor persisted without matching target table —
    state corruption; do not silently degrade."""

    code = AIDPF_2092_BRONZE_CURSOR_TARGET_DESYNC


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _resolve_effective_schema(node: "NodeYaml", bundle: "Bundle") -> str:
    """Resolve BICC offering schema precedence:

    1. ``bundle.fusion.schemaOverrides.<node.id>`` (tenant-level)
    2. ``node.implementation.schemaOverride``
    3. ``node.implementation.biccSchema`` (pack default)
    """
    impl = node.implementation
    fusion_overrides = getattr(bundle.fusion, "schema_overrides", {}) or {}
    tenant_override = fusion_overrides.get(node.id)
    if tenant_override:
        return tenant_override
    if getattr(impl, "schema_override", None):
        return impl.schema_override
    return impl.bicc_schema


def _natural_key_tuple(node: "NodeYaml") -> tuple[str, ...]:
    """Extract the bronze natural-key list from the node YAML."""
    inc = node.refresh.incremental
    if inc is None:
        return ()
    return tuple(inc.natural_key or ())


def _table_exists(spark: "SparkSession", target: str) -> bool:
    """Best-effort check that ``target`` resolves to an existing Delta table."""
    try:
        # spark.catalog.tableExists is the cleanest; falls back if absent.
        if hasattr(spark, "catalog") and hasattr(spark.catalog, "tableExists"):
            return bool(spark.catalog.tableExists(target))
        spark.sql(f"DESCRIBE TABLE {target}").take(1)
        return True
    except Exception:  # noqa: BLE001 — best-effort
        return False


def _to_bicc_iso(wm: datetime) -> str:
    """ISO-8601 UTC string for the BICC ``fusion.initial.extract-date`` option."""
    return wm.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


# ---------------------------------------------------------------------------
# probe_bronze_schemas
# ---------------------------------------------------------------------------


def probe_bronze_schemas(
    spark: "SparkSession",
    *,
    pack: "ResolvedPack",
    bundle: "Bundle",
    resolved_password: str,
    dataset_ids: Iterable[str] | None = None,
) -> dict[str, "StructType"]:
    """Metadata-only BICC ``inferSchema`` probe over the pack's bronze nodes.

    Returns per-dataset live ``StructType`` for callers; the
    ``AIDPF-2072`` drift gate consumes this map.

    Args:
        spark: live Spark session.
        pack: assembled ResolvedPack; iterates ``pack.bronze.values()``.
        bundle: bundle for ``fusion.service_url`` / ``fusion.username`` /
            ``fusion.external_storage`` / schema override map.
        resolved_password: BICC password value (resolved upstream).
        dataset_ids: optional subset filter. When ``None``, probes every
            bronze node in ``pack.bronze``.

    Returns:
        ``{dataset_id: StructType}`` for every successfully-probed node.
        Failures raise individually so the caller can collect errors.
    """
    from ...extractors import bicc as bicc_extractor
    from ...schema.fusion_catalog import PvoEntry

    live_schemas: dict[str, "StructType"] = {}
    candidate_ids = (
        set(dataset_ids)
        if dataset_ids is not None
        else set(pack.bronze.keys())
    )

    for node_id in candidate_ids:
        node = pack.bronze.get(node_id)
        if node is None:
            continue
        impl = node.implementation
        if impl.type != "bronze_extract":
            continue
        effective_schema = _resolve_effective_schema(node, bundle)
        # Build an in-memory PvoEntry-equivalent for the extractor.
        # The extractor expects ``pvo.kind`` and ``pvo.datastore`` /
        # ``pvo.schema`` / ``pvo.id`` only; build the descriptor directly
        # from the YAML — no curated-catalog lookup.
        from ...schema.fusion_catalog import PvoKind

        descriptor = PvoEntry(
            id=node.id,
            datastore=impl.datastore,
            schema=impl.bicc_schema,
            bronze_table_name=node.target,
            description=f"bronze content-pack node {node.id}",
            kind=PvoKind.EXTRACT_PVO,
            confirmed=False,
            incremental_capable=impl.incremental_capable,
            natural_key=_natural_key_tuple(node),
        )

        df = bicc_extractor.extract_pvo(
            spark, descriptor,
            fusion_service_url=bundle.fusion.service_url,
            username=bundle.fusion.username,
            password=resolved_password,
            fusion_external_storage=bundle.fusion.external_storage,
            schema=effective_schema,
        )
        # Trigger inferSchema (metadata-only roundtrip).
        live_schemas[node_id] = df.schema

    return live_schemas


# ---------------------------------------------------------------------------
# run — the adapter dispatch surface
# ---------------------------------------------------------------------------


def run(
    spark: "SparkSession",
    *,
    node: "NodeYaml",
    pack: "ResolvedPack",
    profile: "TenantProfile",
    ctx: "RunContext",
    paths: "TablePaths",
    mode: str,
) -> "tuple[DataFrame, datetime | None]":
    """Execute a single bronze_extract node end-to-end.

    Implements the bronze algorithm:

    * Capture ``extract_started_at`` BEFORE BICC pull.
    * Persisted cursor = ``extract_started_at - safety_window`` on
      non-empty extract; ``prior_watermark`` carried forward on
      empty-delta / MERGE-noop.
    * Push prior cursor to BICC AS-IS (already discounted at prior run;
      no double safety-window subtraction).
    * Incremental + ``incremental_capable=False`` + prior cursor:
      payload-diff-gated MERGE (content-hash predicate suppresses
      no-op UPDATEs so unchanged rows keep their existing
      ``_extract_ts``).
    * First-incremental with no prior cursor downgrades to seed-shape
      replace regardless of ``incremental_capable`` (no prior
      content-hash baseline to diff against).
    * Schema reconciliation BEFORE write (target-wider columns
      preserved; source-wider columns ALTER-added).

    Args:
        spark: live Spark session.
        node: the bronze NodeYaml (``implementation.type: bronze_extract``).
        pack: assembled ResolvedPack.
        profile: validated TenantProfile.
        ctx: RunContext — supplies ``catalog`` / ``bronze_schema`` /
            ``run_id`` / ``prior_watermark[node.id]``.
        paths: TablePaths — fully validates the target identifier
            via ``paths.bronze(node.target)``.
        mode: ``"seed"`` or ``"incremental"``.

    Returns:
        ``(target_df, output_watermark)`` — the dispatcher threads
        ``output_watermark`` into the state row directly (replacing
        ``_compute_output_watermark`` which is silver/gold semantics).

    Raises:
        BronzeCursorTargetDesyncError: prior cursor exists but target
            table does not (state corruption).
    """
    # Lazy imports to dodge orchestrator/sql_runner ↔ builtins cycles.
    from .. import strategy_executors  # noqa: F401 — keep import shape
    from ..merge_sql import (
        build_explicit_when_matched_clause,
        build_explicit_when_not_matched_clause,
    )
    from ..runtime import (
        BRONZE_AUDIT_COLUMNS,
        _resolve_password,
        _resolve_safety_window,
        enrich_bronze_audit_cols,
    )
    from ..state import (
        _ensure_target_schema_for_merge,
        _ensure_target_table_exists,
    )

    impl = node.implementation
    bundle = ctx.bundle  # threaded onto RunContext by the dispatcher
    if bundle is None:
        raise ValueError(
            "bronze_extract_adapter.run: ctx.bundle is None. The "
            "dispatcher MUST set ctx.bundle when constructing the "
            "RunContext for bronze nodes — bundle.fusion fields drive "
            "BICC connection + schemaOverrides resolution."
        )

    # Step 1: build PvoEntry-equivalent descriptor.
    from ...schema.fusion_catalog import PvoEntry, PvoKind

    descriptor = PvoEntry(
        id=node.id,
        datastore=impl.datastore,
        schema=impl.bicc_schema,
        bronze_table_name=node.target,
        description=f"bronze content-pack node {node.id}",
        kind=PvoKind.EXTRACT_PVO,
        confirmed=False,
        incremental_capable=impl.incremental_capable,
        natural_key=_natural_key_tuple(node),
    )

    # Step 2: effective schema.
    effective_schema = _resolve_effective_schema(node, bundle)

    # Resolve target identifier through TablePaths (Step 2.5
    # centralised validation — raises ValueError on malformed targets).
    target = paths.bronze(node.target)

    # Step 3: prior cursor + target existence.
    prior_watermark = ctx.prior_watermark.get(node.id) if ctx.prior_watermark else None
    target_exists = _table_exists(spark, target)
    if prior_watermark is not None and not target_exists:
        raise BronzeCursorTargetDesyncError(
            f"{AIDPF_2092_BRONZE_CURSOR_TARGET_DESYNC}: bronze node "
            f"{node.id!r} found prior persisted cursor "
            f"{prior_watermark.isoformat()!r} but target table {target!r} "
            f"does not exist. Likely state corruption — operator must "
            f"reconcile fusion_autopilot_state with target tables before "
            f"the next run."
        )

    # Decision matrix:
    # - mode=seed → full pull + replace (overwriteSchema=true).
    # - mode=incremental + no prior cursor → seed-shape replace
    #   regardless of incremental_capable.
    # - mode=incremental + prior cursor + incremental_capable=True →
    #   BICC pushdown + MERGE.
    # - mode=incremental + prior cursor + incremental_capable=False →
    #   full pull (no pushdown) + payload-diff-gated MERGE.
    use_seed_shape = mode == "seed" or prior_watermark is None
    if use_seed_shape:
        bicc_watermark: str | None = None
    elif not impl.incremental_capable:
        bicc_watermark = None
    else:
        bicc_watermark = _to_bicc_iso(prior_watermark)

    # Step 4: capture extract instant + persisted cursor formula.
    safety_window = _resolve_safety_window(bundle)
    extract_started_at = datetime.now(timezone.utc)
    next_persisted_cursor = extract_started_at - safety_window

    # Step 5: invoke BICC.
    from ...extractors import bicc as bicc_extractor

    resolved_password_obj = _resolve_password(bundle.fusion.password)
    df = bicc_extractor.extract_pvo(
        spark,
        descriptor,
        fusion_service_url=bundle.fusion.service_url,
        username=bundle.fusion.username,
        password=resolved_password_obj.get_secret_value(),
        fusion_external_storage=bundle.fusion.external_storage,
        schema=effective_schema,
        watermark=bicc_watermark,
    )

    # Step 8 (audit cols): _extract_ts as deterministic literal,
    # _source_pvo, _run_id, _watermark_used (NULL on seed-shape /
    # incremental_capable=False; prior_watermark on incremental with
    # pushdown).
    df = enrich_bronze_audit_cols(
        df,
        source_pvo=descriptor.datastore,
        run_id=ctx.run_id,
        watermark=prior_watermark if bicc_watermark is not None else None,
        extract_ts=extract_started_at,
    )

    df.cache()
    try:
        source_delta_count = df.count()

        if use_seed_shape:
            # Step 9 (seed-shape replace): overwriteSchema=true
            # creates / overwrites the target. Identical to mode=seed.
            df.write.format("delta").mode("overwrite").option(
                "overwriteSchema", "true"
            ).saveAsTable(target)
            materialized_df = spark.table(target)
            # Advance the cursor ONLY on a non-empty extract — the adapter
            # contract (steps 4 + 10) is "cursor = extract_started_at -
            # safety_window on non-empty extract, carry forward prior on
            # empty delta". This holds regardless of mode: an empty seed
            # must NOT persist `extract_started_at - safety_window`, or the
            # next incremental would skip late-arriving source records
            # older than that bogus cursor. An empty first seed carries
            # forward prior_watermark (None) → next run re-seeds (full
            # pull), which is correct.
            output_watermark = (
                next_persisted_cursor
                if source_delta_count > 0
                else prior_watermark
            )
            return materialized_df, output_watermark

        # Incremental MERGE branch. target exists (we asserted above
        # via the prior-cursor invariant).
        _ensure_target_table_exists(spark, target, df.schema)
        reconcile = _ensure_target_schema_for_merge(
            spark, target, df.schema.names, df.schema,
        )

        if source_delta_count == 0:
            # Step 10: empty-delta cursor preservation.
            materialized_df = spark.table(target)
            return materialized_df, prior_watermark

        df.createOrReplaceTempView("_p117_bronze_src")

        # Step 6/9: build MERGE shape with payload-diff guard.
        # Inline the helpers locally because orchestrator/__init__.py
        # owns _natural_key_join_sql / _payload_diff_predicate_sql and
        # we can't import them here (cycle). Re-implement the same
        # contract — verbatim semantics.
        natural_key = _natural_key_tuple(node)
        if not natural_key:
            raise ValueError(
                f"bronze node {node.id!r}: incremental MERGE requires a "
                f"non-empty refresh.incremental.naturalKey."
            )
        # natural_key + data_cols interpolate unquoted into the MERGE ON /
        # payload-diff predicates below. The pack-load validator (AIDPF-2082)
        # covers the declared naturalKey, but data_cols are live source-DataFrame
        # column names (from the customer's Fusion PVO), so validate both here
        # before they reach SQL — reject injection and cryptic Spark errors.
        from oracle_ai_data_platform_fusion_autopilot.config.paths import (
            _validate_identifier,
        )

        for c in natural_key:
            _validate_identifier(f"bronze node {node.id!r} naturalKey", c)
        join_predicate = " AND ".join(
            f"target.{c} <=> src.{c}" for c in natural_key
        )
        data_cols = [c for c in df.schema.names if c not in BRONZE_AUDIT_COLUMNS]
        for c in data_cols:
            _validate_identifier(f"bronze node {node.id!r} source column", c)
        payload_diff: str | None = (
            " OR ".join(
                f"target.{c} IS DISTINCT FROM src.{c}" for c in data_cols
            )
            if data_cols
            else None
        )

        if reconcile.target_only_columns:
            merge_cols = (
                reconcile.common_columns + reconcile.source_only_columns
            )
            when_matched_clause = build_explicit_when_matched_clause(
                merge_cols, payload_diff=payload_diff,
            )
            when_not_matched_clause = build_explicit_when_not_matched_clause(
                merge_cols,
            )
        elif payload_diff is not None:
            when_matched_clause = (
                f"WHEN MATCHED AND ({payload_diff}) THEN UPDATE SET *"
            )
            when_not_matched_clause = "WHEN NOT MATCHED THEN INSERT *"
        else:
            when_matched_clause = "WHEN MATCHED THEN UPDATE SET *"
            when_not_matched_clause = "WHEN NOT MATCHED THEN INSERT *"

        spark.sql(
            f"""
            MERGE INTO {target} AS target
            USING _p117_bronze_src AS src
            ON {join_predicate}
            {when_matched_clause}
            {when_not_matched_clause}
            """
        )
        materialized_df = spark.table(target)
        return materialized_df, next_persisted_cursor
    finally:
        df.unpersist()


__all__ = [
    "VERSION",
    "AIDPF_2092_BRONZE_CURSOR_TARGET_DESYNC",
    "BronzeCursorTargetDesyncError",
    "probe_bronze_schemas",
    "run",
]
