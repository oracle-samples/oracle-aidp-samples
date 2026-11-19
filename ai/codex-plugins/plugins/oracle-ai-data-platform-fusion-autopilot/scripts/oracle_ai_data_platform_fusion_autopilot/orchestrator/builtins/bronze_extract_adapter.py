"""Bronze content-pack extract adapter.

Implements the bronze extraction algorithm as a content-pack adapter.
The adapter keeps extraction, cursoring, payload diffing, schema
reconciliation, and write strategy in one bronze-owned surface.

The adapter exposes two surfaces:

* :func:`run` — execute a single bronze node end-to-end and return
  a :class:`BronzeAdapterResult` (target df, bronze output watermark,
  effective schemaPatches columns). The dispatcher
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
11. Return :class:`BronzeAdapterResult`.

Error codes registered here:

* ``AIDPF-2092 — BRONZE_CURSOR_TARGET_DESYNC`` — bronze adapter found
  a prior persisted cursor with no matching target table, indicating
  state corruption; raises rather than silently degrading.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
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


@dataclass(frozen=True)
class BronzeAdapterResult:
    """`run()`'s result. ``applied_patch_columns`` is the EFFECTIVE
    schemaPatches cast-back plan (no-op patches already dropped) — the
    provenance source for ``RunSummary.applied_schema_patches``; reporting
    the configured map instead would over-report (FR-9)."""

    df: "DataFrame"
    output_watermark: "datetime | None"
    applied_patch_columns: tuple[str, ...] = ()


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


def _spark_type_for_patch(ddl: str):
    """Map an allowlisted schemaPatches type string to a Spark DataType —
    a pure mapping over the small validated domain (no DDL parser, no
    session dependency; ``_parse_datatype_string`` is session-bound)."""
    from pyspark.sql import types as T

    norm = ddl.strip().lower()
    simple = {
        "bigint": T.LongType,
        "long": T.LongType,
        "int": T.IntegerType,
        "integer": T.IntegerType,
        "double": T.DoubleType,
        "float": T.FloatType,
        "string": T.StringType,
        "boolean": T.BooleanType,
        "date": T.DateType,
        "timestamp": T.TimestampType,
    }
    if norm in simple:
        return simple[norm]()
    if norm.startswith("decimal(") and norm.endswith(")"):
        precision, scale = norm[len("decimal("):-1].split(",")
        return T.DecimalType(int(precision), int(scale.strip()))
    raise ValueError(f"unsupported schemaPatches type {ddl!r}")


def _patches_for(node_id: str, bundle: "Bundle") -> dict[str, str]:
    """The dataset's ``schemaPatches`` map from the bundle entry (already
    validated fail-closed at bundle load); ``{}`` when absent."""
    datasets = getattr(bundle, "datasets", None)
    if not isinstance(datasets, (list, tuple)):
        # Fake/mock bundles in tests (and defensive posture generally):
        # no iterable datasets → no patches.
        return {}
    for ds in datasets:
        if getattr(ds, "id", None) == node_id:
            patches = getattr(ds, "schema_patches", None)
            return dict(patches) if isinstance(patches, dict) else {}
    return {}


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
) -> "BronzeAdapterResult":
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

    # Step 5: invoke BICC — with an optional READ-side schema patch
    # (feature bronze-extract-schema-patch): a PVO column whose
    # connector-produced values mismatch the connector's own declared type
    # (AIDPF-2093 class) is read as its RUNTIME type and cast back to the
    # declared type below, integrity-guarded (AIDPF-2094).
    from ...extractors import bicc as bicc_extractor
    from ...schema.bronze_schema_patch import (
        FieldDescriptor,
        apply_schema_patches,
        collision_free_temp_names,
    )

    resolved_password_obj = _resolve_password(bundle.fusion.password)
    _conn_kwargs = dict(
        fusion_service_url=bundle.fusion.service_url,
        username=bundle.fusion.username,
        password=resolved_password_obj.get_secret_value(),
        fusion_external_storage=bundle.fusion.external_storage,
        schema=effective_schema,
    )

    patches = _patches_for(node.id, bundle)
    user_schema = None
    cast_back: list = []
    base_field_order: list[str] = []
    if patches:
        from pyspark.sql.types import StructField, StructType  # cluster-side

        # Metadata-only schema roundtrip (same mechanism the PVO drift
        # probe uses) — ONLY when patches exist (NFR-1).
        base_schema = bicc_extractor.extract_pvo(
            spark, descriptor, **_conn_kwargs
        ).schema
        descriptors = [
            FieldDescriptor(
                name=f.name,
                ddl_type=f.dataType.simpleString(),
                nullable=f.nullable,
                metadata=dict(f.metadata or {}),
            )
            for f in base_schema.fields
        ]
        base_field_order = [f.name for f in base_schema.fields]
        _patched_fields, cast_back = apply_schema_patches(descriptors, patches)
        if cast_back:
            # Reuse the base StructFields VERBATIM (name casing, nullable,
            # metadata all preserved); swap ONLY the patched fields'
            # dataType via the pure allowlist mapping — no DDL parsing, no
            # session dependency.
            patch_type_by_col = {
                e.column.casefold(): e.patch_type for e in cast_back
            }
            user_schema = StructType([
                (
                    StructField(
                        f.name,
                        _spark_type_for_patch(
                            patch_type_by_col[f.name.casefold()]
                        ),
                        f.nullable,
                        dict(f.metadata or {}),
                    )
                    if f.name.casefold() in patch_type_by_col
                    else f
                )
                for f in base_schema.fields
            ])
            _LOG.warning(
                "bronze %s: applying schemaPatches %s (read-side; declared "
                "types restored + integrity-guarded before landing)",
                node.id,
                sorted(e.column for e in cast_back),
            )

    df = bicc_extractor.extract_pvo(
        spark,
        descriptor,
        **_conn_kwargs,
        watermark=bicc_watermark,
        user_schema=user_schema,
    )

    # Cast back to declared types, keeping RAW patched values under
    # GENERATED temp names (collision-checked case-insensitively against
    # the live schema) until the integrity guard has run.
    raw_names: list[str] = []
    if cast_back:
        from pyspark.sql import functions as F

        raw_names = collision_free_temp_names(
            [f.name for f in df.schema.fields], len(cast_back)
        )
        for entry, raw in zip(cast_back, raw_names):
            df = df.withColumnRenamed(entry.column, raw)
            df = df.withColumn(
                entry.column, F.col(raw).cast(entry.declared_type)
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
    _cached_handle = df
    try:
        if cast_back:
            # §5a cast-integrity guard: ONE aggregate on the cached df
            # computes the row count PLUS, per patched column,
            # null-preservation and null-safe round-trip counters — a
            # wrong-but-scannable patch type (overflow → NULL, fractional
            # rounding, malformed → NULL; live-verified permissive casts)
            # must fail BEFORE the write, never land silently.
            from pyspark.sql import functions as F

            from ...schema.bronze_schema_patch import (
                AIDPF_2094_CAST_INTEGRITY,
                CastIntegrityError,
            )

            aggs = [F.count(F.lit(1)).alias("__n")]
            for entry, raw in zip(cast_back, raw_names):
                aggs.append(
                    F.sum(
                        F.when(
                            F.col(raw).isNotNull()
                            & F.col(entry.column).isNull(),
                            1,
                        ).otherwise(0)
                    ).alias(f"__nulled_{raw}")
                )
                aggs.append(
                    F.sum(
                        F.when(
                            ~F.col(entry.column)
                            .cast(entry.patch_type)
                            .eqNullSafe(F.col(raw)),
                            1,
                        ).otherwise(0)
                    ).alias(f"__rt_{raw}")
                )
            guard_row = df.agg(*aggs).collect()[0]
            source_delta_count = guard_row["__n"]
            for entry, raw in zip(cast_back, raw_names):
                nulled = guard_row[f"__nulled_{raw}"] or 0
                mismatched = guard_row[f"__rt_{raw}"] or 0
                if nulled or mismatched:
                    raise CastIntegrityError(
                        f"{AIDPF_2094_CAST_INTEGRITY}: schemaPatches "
                        f"cast-integrity violation on {node.id}."
                        f"{entry.column}: nulled={nulled}, "
                        f"roundtrip_mismatch={mismatched} — the patch type "
                        f"{entry.patch_type!r} is wrong for this column's "
                        f"values; re-run `bronze diagnose-encode --dataset "
                        f"{node.id}` and fix the patch. Nothing was written."
                    )
            # Guard clean → drop temps, restore field metadata in-flight
            # (live-verified to survive the atomic overwrite), and restore
            # the connector's column ORDER (withColumn appended the
            # cast-back columns at the end).
            df = df.drop(*raw_names)
            for entry in cast_back:
                if entry.metadata:
                    df = df.withMetadata(entry.column, dict(entry.metadata))
            base_set = set(base_field_order)
            ordered = [n for n in base_field_order if n in set(df.columns)] + [
                c for c in df.columns if c not in base_set
            ]
            df = df.select(*ordered)
        else:
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
            return BronzeAdapterResult(
                df=materialized_df,
                output_watermark=output_watermark,
                applied_patch_columns=tuple(e.column for e in cast_back),
            )

        # Incremental MERGE branch. target exists (we asserted above
        # via the prior-cursor invariant).
        _ensure_target_table_exists(spark, target, df.schema)
        reconcile = _ensure_target_schema_for_merge(
            spark, target, df.schema.names, df.schema,
        )

        if source_delta_count == 0:
            # Step 10: empty-delta cursor preservation.
            materialized_df = spark.table(target)
            return BronzeAdapterResult(
                df=materialized_df,
                output_watermark=prior_watermark,
                applied_patch_columns=tuple(e.column for e in cast_back),
            )

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
        return BronzeAdapterResult(
            df=materialized_df,
            output_watermark=next_persisted_cursor,
            applied_patch_columns=tuple(e.column for e in cast_back),
        )
    finally:
        _cached_handle.unpersist()


__all__ = [
    "VERSION",
    "AIDPF_2092_BRONZE_CURSOR_TARGET_DESYNC",
    "BronzeAdapterResult",
    "BronzeCursorTargetDesyncError",
    "probe_bronze_schemas",
    "run",
]
