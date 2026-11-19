"""Bundle orchestrator: DAG, state, run loop. Public surface = ``run()``.

This module owns:
  - ``run()`` — the public entry point.
  - ``_dispatch_content_pack_run()`` — content-pack runner orchestration
    (load pack → resume hydrate → PVO drift gate → per-node dispatch).
  - ``_run_content_pack_backend()`` — the per-node loop that fans into
    ``sql_runner.execute_node`` for every silver/gold/bronze node.
  - ``_bootstrap_spark()`` — sentinel-typed Spark session bootstrapper.

Modules ``runtime`` / ``state`` / ``errors`` are imports. The v1
``_execute_node`` dispatcher + ``Spec`` dataclasses were deleted in the
ADR-0022 cleanup.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING, Any

from oracle_ai_data_platform_fusion_autopilot.config.paths import TablePaths
from oracle_ai_data_platform_fusion_autopilot.schema.bundle import Bundle
from oracle_ai_data_platform_fusion_autopilot.schema.run_summary import (
    PlanNode,
    PrereqNode,
)

from . import state
from .state import SchemaReconcileResult
from .errors import (
    IncrementalCursorMissingError,
    IncrementalTargetMissingError,
    MissingDependencyError,
    OrchestratorConfigError,
    SchemaEvolutionTypeConflictError,
    StateReadFailedError,
    UnsupportedModeError,
    WatermarkMonotonicityError,
)
from .runtime import (
    ExternalDep,
    RunStep,
    RunSummary,
    WATERMARK_SAFETY_WINDOW,
    _new_run_id,
    _preflight_external_deps,
    _resolve_password,
    _resolve_safety_window,
    _safe_write_state_row,
    _utc_now,
    _VALID_MODES,
    BRONZE_AUDIT_COLUMNS,
    enrich_bronze_audit_cols,
    load_bundle,
)

if TYPE_CHECKING:  # pragma: no cover
    from collections.abc import Iterable

    from pyspark.sql import DataFrame, SparkSession


# Re-export errors module-level so __init__ acts as the public face
from .errors import (  # noqa: E402  (re-export at module level)
    BronzeSchemaProbeError,
    BundleLoadError,
    BundleVersionMismatchError,
    CredentialResolutionError,
    MultipleNaturalKeyError,
    MultipleUpstreamWatermarkError,
    OrchestratorRuntimeError,
    PrerequisiteError,
    WatermarkMonotonicityError,
)

_log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Bronze MERGE helpers (re-exported via orchestrator/merge_helpers.py)
# ---------------------------------------------------------------------------


def _natural_key_join_sql(
    natural_key: "str | tuple[str, ...]",
    *,
    target_alias: str = "target",
    src_alias: str = "src",
) -> str:
    """Build the MERGE ON predicate for a single- or multi-column natural key.

    Uses Spark's NULL-safe equality operator ``<=>`` instead of ``=`` so
    composite keys with NULL components (e.g. ``gl_period_balances`` on
    ``BalanceTranslatedFlag``) still match
    NULL-vs-NULL rows. The operator is identical to ``=`` for non-NULL
    values; the NULL-safety is the only behavioral difference.

    Single-column key → ``target.k <=> src.k``.
    Composite tuple → ``target.k1 <=> src.k1 AND target.k2 <=> src.k2 AND ...``.
    Empty string / empty tuple raises — caller must validate the spec
    has a populated natural_key before invoking MERGE.
    """
    if isinstance(natural_key, str):
        if not natural_key:
            raise ValueError(
                "natural_key is empty — cannot construct MERGE ON predicate. "
                "Populate spec.natural_key / PvoEntry.natural_key before MERGE."
            )
        cols: tuple[str, ...] = (natural_key,)
    else:
        if len(natural_key) == 0:
            raise ValueError(
                "natural_key is empty tuple — cannot construct MERGE ON "
                "predicate. Populate spec.natural_key / PvoEntry.natural_key "
                "before MERGE."
            )
        cols = tuple(natural_key)
    # Defense in depth: natural-key columns interpolate unquoted into the ON
    # predicate. Pack-loaded specs are validated at schema-load (AIDPF-2082),
    # but validate here too so no caller path can reach SQL with an unsafe
    # identifier.
    from oracle_ai_data_platform_fusion_autopilot.config.paths import _validate_identifier

    for c in cols:
        _validate_identifier("natural_key", c)
    return " AND ".join(
        f"{target_alias}.{c} <=> {src_alias}.{c}" for c in cols
    )


def _payload_diff_predicate_sql(
    data_columns: "Iterable[str]",
    *,
    target_alias: str = "target",
    src_alias: str = "src",
) -> str | None:
    """Build the payload-diff predicate for a bronze MERGE's WHEN MATCHED clause.

    Bronze incremental MERGE under V1 used unconditional
    ``WHEN MATCHED THEN UPDATE SET *``, which rewrites every matched row's
    ``_extract_ts`` on every cycle. For PVOs flagged ``incremental_capable=False``
    (full re-extract every cycle — ``gl_period_balances``, ``gl_coa``,
    ``ap_aging_periods``), the rewritten ``_extract_ts`` propagates downstream:
    silver/gold's ``WHERE bronze_extract_ts > <prior_silver_watermark>`` source
    predicate matches every row, forcing silver/gold MERGE to run unconditionally
    even when nothing materially changed.

    This helper builds the predicate that gates the UPDATE: an OR-joined
    ``IS DISTINCT FROM`` clause across every non-audit DATA column. When no
    payload column has changed for a matched row, the predicate evaluates
    ``false``, the UPDATE is suppressed, ``_extract_ts`` is NOT rewritten,
    and downstream silver/gold MERGE source filters match zero rows.

    Why ``IS DISTINCT FROM`` instead of ``<>``: Spark's ``<>`` is NULL-unsafe
    (``NULL <> NULL`` → NULL, treated as false in a WHEN clause). Bronze data
    often carries NULLs in optional columns (e.g., ``gl_period_balances``'s
    ``BalanceTranslatedFlag``). ``IS DISTINCT FROM``
    is the NULL-safe inequality: ``NULL IS DISTINCT FROM NULL`` → false;
    ``NULL IS DISTINCT FROM 1`` → true. Mirrors the NULL-safe ``<=>`` used
    in :func:`_natural_key_join_sql` — the two helpers have a coherent
    NULL-handling story.

    Why audit columns are excluded: ``_extract_ts`` and ``_run_id`` carry
    this run's literal values, which always differ from the prior run's
    literals — including them in the diff would force every cycle's UPDATE,
    defeating the whole optimization. ``_source_pvo`` and ``_watermark_used``
    are similarly cycle-constant or cycle-distinct and contribute nothing
    useful to a diff. The four are excluded by symbolic reference to
    :data:`BRONZE_AUDIT_COLUMNS`.

    Natural-key columns are included in the predicate even though, on a
    matched row (where the ON predicate matched), the natural-key columns
    are by construction NULL-safe-equal between target and src. The
    ``target.k IS DISTINCT FROM src.k`` clause evaluates ``false`` for those
    columns; their inclusion is harmless and keeps this helper decoupled
    from the node YAML (it doesn't need to know the natural key).

    Args:
        data_columns: An iterable of bronze schema column names — typically
            ``df.schema.names`` of the source DataFrame after audit-column
            enrichment.
        target_alias: SQL alias of the MERGE target. Defaults to ``"target"``.
        src_alias: SQL alias of the MERGE source. Defaults to ``"src"``.

    Returns:
        The OR-joined ``IS DISTINCT FROM`` predicate, or ``None`` if no data
        column remains after excluding :data:`BRONZE_AUDIT_COLUMNS`. ``None``
        signals the caller to fall back to the V1 unconditional ``UPDATE SET *``
        shape — defensive against a malformed bronze schema that wouldn't
        reach this code in practice.

    Examples:
        >>> _payload_diff_predicate_sql(["SEGMENT1", "VENDORID", "_extract_ts"])
        'target.SEGMENT1 IS DISTINCT FROM src.SEGMENT1 OR target.VENDORID IS DISTINCT FROM src.VENDORID'
        >>> _payload_diff_predicate_sql(["_extract_ts", "_source_pvo"])
        # → None  (all columns are audit; caller falls back to V1 shape)
    """
    # Preserve source order from the input iterable; do NOT sort. Source order
    # is deterministic per (extractor, PVO) and makes golden-snapshot SQL tests
    # trivially stable. Sorting would risk nondeterminism if a future Spark
    # version changes column-iteration semantics.
    data_cols = [c for c in data_columns if c not in BRONZE_AUDIT_COLUMNS]
    if not data_cols:
        return None
    # data_cols are live source-DataFrame column names — validate before they
    # interpolate unquoted into the payload-diff predicate.
    from oracle_ai_data_platform_fusion_autopilot.config.paths import _validate_identifier

    for c in data_cols:
        _validate_identifier("payload-diff column", c)
    return " OR ".join(
        f"{target_alias}.{c} IS DISTINCT FROM {src_alias}.{c}" for c in data_cols
    )



# ---------------------------------------------------------------------------
# Spark bootstrap (overridable)
# ---------------------------------------------------------------------------


def _bootstrap_spark() -> "SparkSession":
    """Construct (or get) a SparkSession. Callers can pass ``spark=...`` to
    ``run()`` to inject their own (notebook session uses the AIDP-injected
    one); standalone laptop callers fall through to ``builder.getOrCreate``.
    """
    from pyspark.sql import SparkSession  # type: ignore[import-not-found]

    return SparkSession.builder.appName("aidp-fusion-autopilot-orchestrator").getOrCreate()


def _effective_bundle_scope(bundle: "Any") -> set[str]:
    """Compute the cross-layer scope the resolver should treat as roots.

    ``bundle.datasets[]`` is the operator's high-level
    intent list. It can reference bronze / silver / gold ids; implicit
    transitive include pulls dependencies. Two legacy bundle fields —
    ``bundle.dimensions.build`` and ``bundle.gold.marts`` — pre-date the
    cross-layer ``datasets[]`` contract; when the YAML actually carries
    those blocks they fold into the scope so old bundles keep working.

    **Presence-aware**: the Pydantic schema ships non-empty defaults for
    ``dimensions.build`` (``dim_supplier``, ``dim_account``,
    ``dim_calendar``, ``dim_org``) and ``gold.marts`` (``ar_aging``,
    ``ap_aging``, ``gl_balance``, ``po_backlog``). A bundle that
    omits the blocks entirely would otherwise have those default ids
    smuggled into the scope. The check uses ``bundle.model_fields_set``
    — Pydantic's "fields the constructor was given" record — to fold
    only when the YAML actually authored the block. An author who
    explicitly writes ``dimensions: { build: [] }`` (or a non-empty
    list) marks ``dimensions`` as set and the inner ``build`` list is
    honored regardless of contents.

    Disabled datasets (``DatasetSpec.enabled = False``) are excluded —
    same contract the legacy resolver honored.

    Returns the SET of declared root ids. The resolver consumes this
    as ``bundle_scope=`` and:
      * Uses it as the implicit root set when no CLI ``--datasets``
        filter is given (so a no-filter run executes only declared
        roots plus transitive dependencies, NOT every pack node).
      * Validates CLI ``--datasets`` is a subset of it; ids outside
        the scope raise ``AIDPF-1043 CLI_DATASET_OUTSIDE_BUNDLE_SCOPE``.
    """
    scope: set[str] = set()
    for ds in getattr(bundle, "datasets", []) or []:
        if getattr(ds, "enabled", True):
            scope.add(ds.id)
    bundle_fields_set = getattr(bundle, "model_fields_set", set()) or set()
    # ``dimensions.build`` only folds when the YAML carries a
    # ``dimensions:`` block. Without this guard, Pydantic's non-empty
    # ``DimensionsSpec.build`` default would smuggle dim_supplier /
    # dim_account / dim_calendar / dim_org into the scope of every
    # bundle that omits the block.
    if "dimensions" in bundle_fields_set:
        dims = getattr(bundle, "dimensions", None)
        if dims is not None:
            for name in getattr(dims, "build", None) or []:
                scope.add(str(name))
    # Same guard for ``gold.marts`` (defaults to
    # ar_aging / ap_aging / gl_balance / po_backlog).
    if "gold" in bundle_fields_set:
        gold = getattr(bundle, "gold", None)
        if gold is not None:
            for name in getattr(gold, "marts", None) or []:
                scope.add(str(name))
    return scope


# ---------------------------------------------------------------------------
# Public API — run()
# ---------------------------------------------------------------------------


def run(
    bundle_path: Path,
    *,
    spark: "SparkSession | None" = None,
    mode: str | None = None,
    datasets: list[str] | None = None,
    layers: list[str] | None = None,
    dry_run: bool = False,
    resume_run_id: str | None = None,
    # Legacy `execution_backend` kwarg retained for backwards
    # compatibility with callers (tests, programmatic uses) that pass it
    # explicitly; the value is IGNORED. The only execution path is
    # content-pack now (v1 modules deleted). See ADR-0022.
    execution_backend: str = "content-pack",
    resolved_pack: "Any | None" = None,
    tenant_profile: "Any | None" = None,
    # Runtime drift gate bypass (dev/sandbox; hidden flag).
    force_fingerprint_skip: bool = False,
    # Plan-hash continuity gate bypass (dev/sandbox; hidden flag).
    # When True, a diverged AIDPF-4040 plan-hash on an incremental is
    # repinned (audit row + proceed) instead of blocking. For deliberate
    # SQL/profile/adapter edits; production/SOX runs MUST NOT use it.
    repin_plan_hash: bool = False,
    # Opt-out of implicit-transitive-include in the plan
    # resolver. When True, declared roots must include every transitive
    # dep explicitly; missing deps raise AIDPF-1042. Tri-state: None = omitted
    # (fresh → False; resume → adopt the manifest value); an explicit value
    # that conflicts with a manifest-backed resume raises AIDPF-1047.
    strict_scope: bool | None = None,
    # Shared run_id contract retained for resume
    # semantics. Private contract; the CLI never passes this directly.
    _forced_run_id: str | None = None,
) -> RunSummary:
    """Materialize bronze + silver + gold per the bundle.yaml plan.

    Args:
        bundle_path: path to ``bundle.yaml``.
        spark: optional pre-existing SparkSession (notebook callers pass
            the AIDP-injected one; standalone callers leave None to use
            ``_bootstrap_spark``).
        mode: ``"seed"`` (full overwrite per layer) or
            ``"incremental"`` (bronze MERGE + row-level
            silver/gold MERGE; exempt marts `supplier_spend`,
            `ap_aging`, `dim_calendar` always run seed-shape).
        datasets: ``--datasets`` CSV filter, classified across registries.
        layers: ``--layers`` filter, e.g. ``["gold"]``.
        dry_run: skip execution; return ``RunSummary.empty(..., plan=...)``
            with the would-run plan and extra-plan prereqs populated.
        resume_run_id: when set, resume the named run_id from its
            checkpoint. Reads ``fusion_autopilot_state``, skips datasets
            whose latest terminal row is ``success`` or
            ``resumed_skipped``, re-attempts the rest under the
            original ``run_id``. Bundle drift raises
            ``ResumeBundleMismatchError``; unknown / non-resumable
            runs raise ``ResumeRunNotFoundError`` /
            ``ResumeRunNotResumableError``.

    Returns:
        ``RunSummary`` with one ``RunStep`` per plan node (or empty for
        dry-run / empty-bundle paths).

    Raises:
        UnsupportedModeError: mode not in ``{"seed", "incremental"}``.
        IncrementalCursorMissingError: ``mode="incremental"`` requested
            but one or more silver/gold nodes lack a prior cursor in
            ``fusion_autopilot_state``. Run ``--mode seed`` first.
        BundleLoadError: any bundle.yaml load failure.
        CredentialResolutionError: ``bundle.fusion.password`` unresolvable.
        MissingDependencyError: typo in datasets/dims/marts.
        PrerequisiteError: extra-plan dependency missing on disk.
        ResumeRunNotFoundError: ``resume_run_id`` has no rows in
            ``fusion_autopilot_state``.
        ResumeRunNotResumableError: ``resume_run_id`` exists but
            lacks ``plan_hash`` or ``plan_snapshot`` (legacy row or
            partially-migrated write path).
        ResumeBundleMismatchError: stored vs current plan hash diverge.
    """
    # Mode resolution + validation (feature: fail-fast-seed-validation). A
    # FRESH run resolves the tri-state ``--mode`` here (omitted → "seed",
    # unchanged default). A RESUME keeps ``mode`` as passed (possibly None) and
    # resolves it in the dispatcher against the run manifest / legacy execution
    # history (needs state I/O) — a bare resume must NOT silently default to
    # "seed" over an interrupted incremental.
    if resume_run_id is None:
        from .run_manifest import resolve_run_mode
        mode = resolve_run_mode(mode, is_resume=False)
    if mode is not None and mode not in _VALID_MODES:
        raise UnsupportedModeError(
            f"mode={mode!r} is not supported. Valid modes: "
            f"{sorted(_VALID_MODES)}. "
            f"(The retired alias 'full' is now called 'seed'.)"
        )
    # Incremental mode dispatches the bronze MERGE + silver/gold MERGE
    # pipeline. Write strategy and state contract are validated together to
    # keep the destructive-write blast radius contained.

    # Single execution path is content-pack. `execution_backend` is
    # retained for backwards compatibility with programmatic callers,
    # but content-pack is the only supported dispatcher (ADR-0022).
    if execution_backend == "content-pack":
        return _dispatch_content_pack_run(
            bundle_path=bundle_path,
            spark=spark,
            mode=mode,
            datasets=datasets,
            layers=layers,
            dry_run=dry_run,
            resume_run_id=resume_run_id,
            resolved_pack=resolved_pack,
            tenant_profile=tenant_profile,
            force_fingerprint_skip=force_fingerprint_skip,
            repin_plan_hash=repin_plan_hash,
            strict_scope=strict_scope,
        )

    # v1 main loop deleted (ADR-0022). Reaching this point
    # means execution_backend != "content-pack" was passed, which is
    # not a supported value anymore.
    raise OrchestratorConfigError(
        f"execution_backend={execution_backend!r} is not supported; "
        f"the v1 dispatch path was removed by ADR-0022. "
        f"Use the default content-pack backend."
    )


# ---------------------------------------------------------------------------
# Content-pack top-level dispatcher (scope, shared run_id, gates)
# ---------------------------------------------------------------------------


def _dispatch_content_pack_run(
    *,
    bundle_path: "Path",
    spark: "SparkSession | None",
    mode: str,
    datasets: list[str] | None,
    layers: list[str] | None,
    resume_run_id: str | None,
    resolved_pack: "Any | None",
    tenant_profile: "Any | None",
    force_fingerprint_skip: bool,
    repin_plan_hash: bool = False,
    dry_run: bool = False,
    strict_scope: bool | None = None,
) -> RunSummary:
    """Single-path content-pack dispatcher.

    Bronze + silver + gold all dispatch through the content-pack runner
    (``_run_content_pack_backend``). Bronze is a first-class layer in
    ``pack.bronze`` and ``resolve_content_pack_plan`` walks all three
    layers uniformly.

    Sequence:

    1. Load bundle + validate ``contentPack`` block present (AIDPF-1031
       / AIDPF-1030).
    2. Resolve resume context (when ``resume_run_id`` is supplied).
    3. Dry-run path: emit the content-pack plan + return.
    4. Mint a single shared ``run_id`` (or adopt the resume id).
    5. Run the Fusion PVO drift gate (AIDPF-2072) when bronze nodes
       are in scope — fires BEFORE any state write.
    6. Delegate to ``_run_content_pack_backend`` with the full
       ``(datasets, layers)`` filter.
    """
    from datetime import datetime as _dt, timezone as _tz
    from ..schema.bundle import (
        AIDPF_1030_PROFILE_MISSING,
        AIDPF_1031_CONTENT_PACK_MISSING,
        load_bundle as _load_bundle_v2,
    )

    bundle, paths = _load_bundle_v2(bundle_path)

    if bundle.content_pack is None:
        raise OrchestratorConfigError(
            f"{AIDPF_1031_CONTENT_PACK_MISSING}: bundle.yaml has no "
            f"`contentPack:` block; the content-pack backend "
            f"requires it. Add the `contentPack:` block to bundle.yaml."
        )
    if bundle.content_pack.profile is None:
        raise OrchestratorConfigError(
            f"{AIDPF_1030_PROFILE_MISSING}: bundle.yaml's "
            f"contentPack.profile field is missing."
        )

    # Resume context resolution — read fusion_autopilot_state to:
    #   1. Reject unknown run_ids via ResumeRunNotFoundError.
    #   2. Reconstruct (datasets, layers) when a bare --resume is supplied.
    #   3. Surface succeeded nodes so the per-node loop emits
    #      resumed_skip instead of re-dispatching.
    # Dry-run skips state I/O.
    resume_context = None
    if resume_run_id is not None and not dry_run:
        from . import run_manifest as _rmf
        from . import state_phase2 as _state_phase2
        from .resume import check_identity_drift, reconstruct_resume_scope

        # Capture the operator's EXPLICIT scope before any reconstruction
        # overwrites it (for the manifest scope-conflict check, AIDPF-1047).
        _explicit_datasets, _explicit_layers = datasets, layers
        _explicit_strict_scope = strict_scope  # tri-state: None = omitted

        spark = spark or _bootstrap_spark()
        state.ensure_state_table(spark, paths)
        _state_phase2.ensure_state_columns_v2(spark, paths)
        resume_context = state.read_content_pack_resumable_state(
            spark, paths, resume_run_id,
        )

        # Parse the durable manifest, if this run wrote one. Branch on
        # ``is not None`` (NOT truthiness): the state reader already rejects an
        # empty/blank payload as AIDPF-4022, but branching on truthiness here
        # would ALSO route an empty string to the legacy path — so a
        # present-but-malformed manifest fails closed (AIDPF-4022 via
        # parse_manifest), never legacy-fallback.
        manifest = (
            _rmf.parse_manifest(resume_context.run_manifest_raw)
            if resume_context.run_manifest_raw is not None
            else None
        )

        # Mode resolution (feature: fail-fast-seed-validation). Adopt the
        # manifest mode / legacy-infer a single mode; a MIXED legacy history or
        # an explicit conflict is rejected (AIDPF-1046) — a bare resume never
        # silently flips seed↔incremental.
        mode = _rmf.resolve_run_mode(
            mode,
            is_resume=True,
            manifest_mode=(manifest.get("mode") if manifest else None),
            historical_exec_modes=list(resume_context.historical_exec_modes),
        )
        # Re-validate the RESOLVED resume mode before any dispatch. run()'s
        # pre-dispatch _VALID_MODES check ran with mode=None on a resume, so the
        # value adopted here (from the manifest / legacy history) has not yet
        # been checked — a malformed value must never reach the destructive
        # seed-overwrite vs incremental-merge branch.
        if mode not in _VALID_MODES:
            raise UnsupportedModeError(
                f"resume resolved an unsupported mode={mode!r}. Valid modes: "
                f"{sorted(_VALID_MODES)}. The recorded run state is corrupt; "
                f"start a fresh `--mode seed`."
            )

        if manifest is not None:
            # A manifest-backed resume is scoped BY THE MANIFEST. An explicit
            # scope filter (datasets / layers / strict-scope) must exactly equal
            # it, else AIDPF-1047 (never silently narrow). The CAPTURED explicit
            # strict-scope (tri-state None = omitted) is passed so an explicit
            # value conflicting with the manifest is caught, not silently
            # ignored; an omitted value adopts the manifest.
            _rmf.check_scope_conflict(
                _explicit_datasets, _explicit_layers, _explicit_strict_scope,
                manifest_inputs=manifest["resolver_inputs"],
            )
            # Identity / profile / exec-policy drift → fresh seed (AIDPF-1048).
            from oracle_ai_data_platform_fusion_autopilot import __version__ as _pv
            from ..schema.tenant_profile import compute_profile_hash as _cph
            from .plan_hash import _identity_dict

            # Additive-COA split (feature: incremental-coa-chart-onboarding). Only
            # when the profile hash actually differs (the only case the split
            # matters) do the Spark-side work: compute the non-COA semantic hash +
            # classify the COA change against the RESUMED manifest's projection and
            # the live `dim_account` protected charts. An unreadable dim or a v1
            # baseline leaves the verdict None → the pure gate falls back to the
            # conservative whole-profile_hash 1048 (fail-closed).
            _cur_profile_hash = _cph(tenant_profile)
            _cur_non_coa_hash = None
            _coa_verdict = None
            if _cur_profile_hash != manifest.get("profile_hash"):
                from .coa_change import (
                    classify_coa_change,
                    coa_projection_of,
                    non_coa_semantic_hash,
                )
                from .coa_incremental import read_protected_charts

                _cur_non_coa_hash = non_coa_semantic_hash(tenant_profile, resolved_pack)
                _prior_coa = manifest.get("coa_projection")
                if isinstance(_prior_coa, dict):
                    _protected = read_protected_charts(spark, resolved_pack, paths)
                    if _protected is not None:
                        _coa_verdict = classify_coa_change(
                            _prior_coa, coa_projection_of(tenant_profile), _protected
                        )
            _rmf.check_identity_profile_drift(
                current_identity=_identity_dict(bundle, paths, _pv),
                current_profile_hash=_cur_profile_hash,
                current_allow_unprovable_coa=bool(
                    getattr(
                        getattr(bundle, "content_pack", None),
                        "allow_unprovable_coa",
                        False,
                    )
                ),
                manifest=manifest,
                current_non_coa_semantic_hash=_cur_non_coa_hash,
                coa_verdict=_coa_verdict,
            )
            # Replay the ORIGINAL resolver inputs (preserves --layers; no
            # AIDPF-1043), then guard topology + node-definition drift.
            _ri = manifest["resolver_inputs"]
            datasets = _ri.get("datasets")
            layers = _ri.get("layers")
            strict_scope = bool(_ri.get("strict_scope"))
            _replay_plan = _resolve_plan_for_manifest(
                resolved_pack, datasets, layers, strict_scope, bundle
            )
            _replay_topo = _manifest_topology_for_plan(
                _replay_plan, resolved_pack, bundle
            )
            _rmf.check_topology_drift(
                _replay_topo, manifest_topology=manifest["topology"]
            )
            _rmf.check_node_definition_drift(
                _replay_topo,
                _rmf.compute_pack_fingerprint(
                    resolved_pack,
                    getattr(
                        getattr(bundle, "content_pack", None), "profile", None
                    ),
                ),
                manifest_topology=manifest["topology"],
                manifest_pack_fingerprint=manifest["pack_fingerprint"],
            )
        else:
            # Legacy no-manifest resume — existing behaviour (identity drift via
            # the bronze snapshot + row-reconstructed scope for a bare resume).
            if resume_context.bronze_plan_snapshot is not None:
                from oracle_ai_data_platform_fusion_autopilot import __version__ as _pv
                check_identity_drift(
                    resume_context.bronze_plan_snapshot,
                    bundle=bundle, paths=paths, plugin_version=_pv,
                    run_id=resume_context.run_id,
                )
            if datasets is None and layers is None:
                if resume_context.bronze_plan_snapshot is not None:
                    datasets, layers = reconstruct_resume_scope(
                        resume_context.bronze_plan_snapshot,
                    )
                else:
                    datasets = list(resume_context.scope_datasets)
                    layers = list(resume_context.scope_layers)

    # Safety net: a resume dry-run skips the state read above (guarded by
    # ``not dry_run``), so ``mode`` can still be None here. Resolve to the
    # display default rather than surfacing ``mode=None`` in the preview.
    if mode is None:
        mode = "seed"

    # Resolve the tri-state ``strict_scope`` to a concrete bool for the resolver
    # / backend. The manifest-backed branch above already set it from the
    # manifest's resolver_inputs; a fresh run (resume skipped) or a legacy
    # resume leaves it None → False (the historical default).
    strict_scope = bool(strict_scope)

    # Dry-run — emit the would-run plan and return.
    if dry_run:
        plan_nodes = _build_content_pack_dry_run_plan(
            resolved_pack=resolved_pack,
            datasets=datasets,
            layers=layers,
            strict_scope=strict_scope,
            bundle_scope=_effective_bundle_scope(bundle),
        )
        return RunSummary.empty(
            bundle_project=bundle.project, mode=mode, plan=plan_nodes,
        )

    # Mint the shared run_id.
    if resume_context is not None:
        shared_run_id = resume_context.run_id
    elif resume_run_id is not None:
        shared_run_id = resume_run_id
    else:
        shared_run_id = _new_run_id()

    started_at = _dt.now(_tz.utc)

    # Fusion PVO drift gate (AIDPF-2072). Fires BEFORE state writes
    # when bronze nodes are in scope.
    #
    # Enumerate in-scope bronze ids from the resolved plan, not from
    # the raw (datasets, layers) filter. Implicit transitive include
    # adds bronze deps for silver/gold roots, so ``--datasets
    # supplier_spend`` or ``--layers gold`` still executes
    # ``ap_invoices`` + ``erp_suppliers``.
    bundle_scope = _effective_bundle_scope(bundle)
    in_scope_bronze: set[str] = set()
    if resolved_pack is not None:
        try:
            from .content_pack_plan_resolver import resolve_content_pack_plan
            gate_plan = resolve_content_pack_plan(
                resolved_pack,
                datasets=datasets, layers=layers,
                strict_scope=strict_scope,
                bundle_scope=bundle_scope,
            )
            in_scope_bronze = {n.id for n in gate_plan if n.layer == "bronze"}
        except Exception:  # noqa: BLE001 — resolver failures surface
            # again from _run_content_pack_backend; the gate just
            # degrades to "no bronze in scope" here.
            in_scope_bronze = set()
        # Legacy bronze.yaml fallback: a pack that hasn't migrated to
        # per-file bronze/<id>.yaml carries its bronze ids only in
        # pack.bronze_yaml. Resolver returns them as part of the plan
        # already (resolve_content_pack_plan walks pack.bronze), so the
        # set above is complete; this loop is belt-and-braces.
        bronze_yaml = getattr(resolved_pack, "bronze_yaml", None) or {}
        legacy_ids = {
            str(ds["id"]) for ds in bronze_yaml.get("datasets", []) or []
            if isinstance(ds, dict) and "id" in ds
        }
        if legacy_ids:
            # Apply the same filter shape as the resolver would have.
            if datasets is not None:
                legacy_ids &= set(datasets)
            if layers is not None and "bronze" not in {l.lower() for l in layers}:
                legacy_ids = set()
            in_scope_bronze |= legacy_ids

    if in_scope_bronze:
        in_scope_bronze = _filter_resume_succeeded(in_scope_bronze, resume_context)
    if in_scope_bronze:
        gate_step = _run_fusion_pvo_drift_gate(
            bundle=bundle,
            bundle_path=bundle_path,
            spark=spark,
            bronze_filter=(sorted(in_scope_bronze), None),
            cp_filter=None,
            resolved_pack=resolved_pack,
            tenant_profile=tenant_profile,
            run_id=shared_run_id,
            mode=mode,
        )
        if gate_step is not None:
            return RunSummary(
                run_id=shared_run_id,
                started_at=started_at,
                finished_at=_dt.now(_tz.utc),
                bundle_project=bundle.project,
                mode=mode,
                steps=(gate_step,),
            )

    return _run_content_pack_backend(
        bundle_path=bundle_path,
        spark=spark,
        mode=mode,
        datasets=datasets,
        layers=layers,
        dry_run=False,
        resume_run_id=resume_run_id,
        resolved_pack=resolved_pack,
        tenant_profile=tenant_profile,
        force_fingerprint_skip=force_fingerprint_skip,
        repin_plan_hash=repin_plan_hash,
        shared_run_id=shared_run_id,
        enable_bronze_readiness_gate=False,
        shared_resume_context=resume_context,
        strict_scope=strict_scope,
    )


def _filter_resume_succeeded(
    bronze_ids: set[str], resume_context: "Any | None",
) -> set[str]:
    """Drop bronze ids whose latest state row is already success."""
    if resume_context is None:
        return bronze_ids
    succeeded = getattr(resume_context, "succeeded", None) or set()
    return {b for b in bronze_ids if b not in succeeded}


# ---------------------------------------------------------------------------
# Fusion PVO drift gate wiring (AIDPF-2072)
# ---------------------------------------------------------------------------


def _struct_type_to_columns_map(
    struct_type: "Any",
) -> dict[str, str]:
    """Flatten a Spark ``StructType`` to ``{col_name_lower: type_string}``.

    Used to feed ``assert_fusion_pvo_compatibility`` which expects the
    live schema in dict form (case-insensitive keys, simple type
    strings). Resilient to test fakes that don't expose
    ``.fields`` — falls back to ``.names`` + ``.dataType`` if needed.
    """
    out: dict[str, str] = {}
    fields = getattr(struct_type, "fields", None)
    if fields is None:
        return out
    for f in fields:
        name = getattr(f, "name", None)
        if name is None:
            continue
        dtype = getattr(f, "dataType", None)
        if dtype is None:
            type_str = ""
        else:
            simple = getattr(dtype, "simpleString", None)
            type_str = simple() if callable(simple) else str(dtype)
        out[name.lower()] = type_str
    return out


def _run_fusion_pvo_drift_gate(
    *,
    bundle: "Any",
    bundle_path: "Path",
    spark: "SparkSession | None",
    bronze_filter: tuple[list[str] | None, list[str] | None],
    cp_filter: tuple[list[str] | None, list[str] | None] | None,
    resolved_pack: "Any | None",
    tenant_profile: "Any | None",
    run_id: str,
    mode: str,
) -> "RunStep | None":
    """Fire the AIDPF-2072 PVO drift gate.

    Runs before the bronze branch in ``_dispatch_content_pack_run``.
    Probes the live Fusion PVO schemas via the metadata-only BICC
    primitive ``preflight_bronze_schemas`` (no row transfer), loads the
    pinned per-dataset snapshot if present, then hands the pair off to
    ``assert_fusion_pvo_compatibility``.

    Args:
        bundle: loaded ``Bundle``.
        bundle_path: path to ``bundle.yaml`` (used to resolve the
            snapshot file under ``profiles/``).
        spark: caller-supplied session or ``None``. Bootstrapped if None.
        bronze_filter: ``scope.bronze_filter`` from ``split_run_scope``;
            limits which bronze ids the gate complains about.
        cp_filter: ``scope.cp_filter`` from ``split_run_scope``;
            narrows the silver/gold required-column union.
        resolved_pack: loaded ``ResolvedPack`` or ``None`` (bronze-only
            run — required-column check is skipped).
        tenant_profile: loaded ``TenantProfile`` or ``None``.
        run_id: shared run identifier; threaded into the diagnostic path.
        mode: ``"seed"`` or ``"incremental"`` (carried on the
            ``gate_failed`` RunStep).

    Returns:
        ``None`` when the gate passes (or has nothing to do — empty
        bronze plan, all probes failed and surfaced elsewhere).
        A synthetic :class:`RunStep` with ``status='failed'`` carrying
        AIDPF-2072 when the gate detects drift. The dispatcher consumes
        this and returns a one-step ``RunSummary`` — bronze never runs.

    Notes:
        * The dispatcher-level preflight call is intentionally distinct
          from the legacy bronze path's own preflight inside the
          recursive ``run()``. Both are metadata-only and idempotent;
          the double-probe is wasteful but correct, and lifting the
          preflight result down into the legacy path would require a
          new ``_skip_preflight`` kwarg layered through ``run()``.
          TODO: factor the preflight to a single dispatcher-owned probe and
          skip the duplicate run.
        * A snapshot YAML that's absent OR unparseable degrades the
          gate to missing-column / renamed-column detection only —
          matches the contract in ``fusion_pvo_drift.py``.
        * Failures during preflight itself (BronzeSchemaProbeError,
          credential failures) are NOT caught here — they propagate so
          the operator sees the real probe error, not a synthetic
          gate-failure step that hides the real cause.
    """
    from .fusion_pvo_drift import (
        AIDPF_2072_FUSION_PVO_DRIFT_GATE_FAILED,
        FusionPvoDriftError,
        assert_fusion_pvo_compatibility,
    )
    from .builtins.bronze_extract_adapter import probe_bronze_schemas
    from ..schema.bronze_schema_snapshot import (
        BronzeSchemaSnapshotSchemaError,
        load_bronze_schema_snapshot,
        resolve_snapshot_path,
    )

    bundle_inst, paths = load_bundle(bundle_path)

    # Enumerate bronze ids from the resolved pack. Honors both per-file
    # pack.bronze and the legacy single-file pack.bronze_yaml fallback.
    bronze_node_ids = set(resolved_pack.bronze.keys()) if resolved_pack else set()
    if resolved_pack is not None:
        legacy_bronze = getattr(resolved_pack, "bronze_yaml", None) or {}
        for ds in legacy_bronze.get("datasets", []) or []:
            if isinstance(ds, dict) and "id" in ds:
                bronze_node_ids.add(str(ds["id"]))

    # Narrow to the scope's bronze filter.
    bronze_datasets, bronze_layers = bronze_filter
    if bronze_datasets is not None:
        bronze_node_ids &= set(bronze_datasets)
    if not bronze_node_ids:
        return None

    # Probe live PVO schemas via the bronze adapter (rehomed from the
    # deleted orchestrator/preflight.py). Metadata-only roundtrip — no
    # row transfer. Failures propagate so the operator sees them.
    spark_session = spark or _bootstrap_spark()
    resolved_password = _resolve_password(bundle_inst.fusion.password).get_secret_value()
    live_pvo_schemas = probe_bronze_schemas(
        spark_session,
        pack=resolved_pack,
        bundle=bundle_inst,
        resolved_password=resolved_password,
        dataset_ids=bronze_node_ids,
    )

    # Convert per-PVO ``StructType`` -> ``{col_name_lower: type_string}``.
    live_pvo_columns: dict[str, dict[str, str]] = {}
    for ds_id, struct_type in live_pvo_schemas.items():
        live_pvo_columns[ds_id] = _struct_type_to_columns_map(struct_type)

    # Load the pinned snapshot. Absent / unparseable means degraded mode
    # (None), which limits drift diagnostics to what can be inferred live.
    schema_snapshot = None
    profile_name = (
        bundle_inst.content_pack.profile if bundle_inst.content_pack else None
    )
    if profile_name is not None:
        try:
            snapshot_path = resolve_snapshot_path(bundle_path, profile_name)
            if snapshot_path.exists():
                schema_snapshot = load_bronze_schema_snapshot(snapshot_path)
        except (BronzeSchemaSnapshotSchemaError, OSError):
            schema_snapshot = None

    diagnostics_root = bundle_path.resolve().parent / ".aidp" / "diagnostics"

    try:
        assert_fusion_pvo_compatibility(
            live_pvo_columns=live_pvo_columns,
            resolved_pack=resolved_pack,
            cp_filter=cp_filter,
            bronze_filter=bronze_filter,
            schema_snapshot=schema_snapshot,
            run_id=run_id,
            diagnostics_root=diagnostics_root,
            tenant_profile=tenant_profile,
        )
    except FusionPvoDriftError as exc:
        return RunStep.gate_failed(
            run_id=run_id,
            mode=mode,
            layer="bronze",
            gate_dataset_id="__fusion_pvo_drift_gate__",
            aidpf_code=AIDPF_2072_FUSION_PVO_DRIFT_GATE_FAILED,
            error_message=str(exc),
        )
    return None


# ---------------------------------------------------------------------------
# Resume helpers (dispatcher-side narrowing + skip emission)
# ---------------------------------------------------------------------------


def _resolve_scope_bronze_ids(
    bundle: "Any",
    bronze_filter: tuple[list[str] | None, list[str] | None],
) -> set[str]:
    """Return the set of bronze ids covered by ``bronze_filter``.

    ``(None, ["bronze"])`` → every enabled bronze id in the bundle.
    ``(["ap_invoices", "gl_coa"], None)`` → that intersection with
    the enabled set (so a typo dataset never sneaks in).
    """
    datasets, _layers = bronze_filter
    enabled_bronze_ids = {ds.id for ds in bundle.datasets if ds.enabled}
    if datasets is None:
        return enabled_bronze_ids
    return {d for d in datasets if d in enabled_bronze_ids}


def _narrow_bronze_filter_to_reattempt(
    bronze_filter: tuple[list[str] | None, list[str] | None],
    bundle: "Any",
    resume_context: "Any | None",  # state.ResumeContext | None
) -> tuple[list[str] | None, list[str] | None] | None:
    """Return a bronze filter narrowed to bronze ids that still need work.

    No resume → pass the filter through unchanged. With a resume
    context: subtract the ``succeeded`` set from the scope's bronze
    ids and rebuild a positive-list filter. All succeeded → return
    ``None`` (nothing left to dispatch on the bronze branch).
    """
    if resume_context is None:
        return bronze_filter
    scope_ids = _resolve_scope_bronze_ids(bundle, bronze_filter)
    reattempt_ids = sorted(scope_ids - resume_context.succeeded)
    if not reattempt_ids:
        return None
    return (reattempt_ids, None)


def _build_content_pack_dry_run_plan(
    *,
    resolved_pack: "Any",
    datasets: list[str] | None,
    layers: list[str] | None,
    strict_scope: bool = False,
    bundle_scope: set[str] | None = None,
) -> tuple[Any, ...]:
    """Return a tuple of :class:`PlanNode` for the content-pack dry-run path.

    When ``bundle_scope`` is supplied, the resolver treats it as the
    declared-root ceiling. Without it, the resolver falls back to
    "every pack node is a root" — which lies to the operator when the
    bundle declares only a subset. Production callers
    (``_dispatch_content_pack_run`` dry-run + REST dispatch) must pass
    ``bundle_scope=_effective_bundle_scope(bundle)``.

    The implementation walks ``resolve_content_pack_plan`` (the same
    resolver the runtime uses) so the dry-run plan is byte-equivalent
    to what would actually run — minus the side effects.
    """
    from .content_pack_plan_resolver import resolve_content_pack_plan
    from .node_preflight import coa_applicable_sources, order_coa_source_first

    plan = resolve_content_pack_plan(
        resolved_pack, datasets=datasets, layers=layers,
        strict_scope=strict_scope,
        bundle_scope=bundle_scope,
    )
    # Dry-run parity: preview the SAME gl_coa-first order the exec loop uses, so
    # the previewed order does not diverge from execution.
    plan = order_coa_source_first(plan, coa_applicable_sources(resolved_pack, plan))
    plan_nodes = tuple(
        PlanNode(
            dataset_id=node.id,
            layer=node.layer,
            status="eligible",
            reason=None,
        )
        for node in plan
    )
    return plan_nodes


# ---------------------------------------------------------------------------
# Pack-driven node discovery
# ---------------------------------------------------------------------------


class PackNodeNotFoundError(OrchestratorRuntimeError):
    """Requested node id is not in the resolved pack's silver/gold maps.

    Raised by :func:`_resolve_node_from_pack` when a caller hands in a
    layer + node_id pair that doesn't exist on the loaded pack. Surfaces
    pack-author mistakes (typo in a YAML id) without conflating with
    the registry-lookup errors raised under the legacy backend.
    """


def _resolve_node_from_pack(
    pack: "Any",  # ResolvedPack — typed without import to avoid load-time cycles
    layer: str,
    node_id: str,
) -> "Any":  # NodeYaml
    """Look up a content-pack node by ``(layer, node_id)``.

    The orchestrator's per-node dispatch loop (
    :func:`_run_content_pack_backend`) walks ``resolve_content_pack_plan``'s
    output directly — that path already returns ``NodeYaml`` objects.
    This helper exists so that direct callers (tests, future
    integrations, dry-run plan renderers) can ask the pack the same
    question without re-walking the plan resolver: "give me the
    ``NodeYaml`` for silver/dim_supplier".

    Per-node ``implementation.type`` (``sql`` / ``builtin`` /
    ``bronze_extract``) discriminates the runtime path; the dispatch
    itself is inside ``sql_runner.execute_node``.

    Args:
        pack: the resolved content pack (``ResolvedPack``).
        layer: ``"bronze"`` / ``"silver"`` / ``"gold"``.
        node_id: pack-author node id (matches ``NodeYaml.id``).

    Returns:
        The :class:`NodeYaml` for that ``(layer, node_id)``.

    Raises:
        ValueError: ``layer`` not in ``{"bronze", "silver", "gold"}``.
        PackNodeNotFoundError: ``node_id`` is absent from
            ``pack.bronze`` / ``pack.silver`` / ``pack.gold``.
    """
    if layer == "bronze":
        bucket = getattr(pack, "bronze", {})
    elif layer == "silver":
        bucket = getattr(pack, "silver", {})
    elif layer == "gold":
        bucket = getattr(pack, "gold", {})
    else:
        raise ValueError(
            f"_resolve_node_from_pack: layer={layer!r} not in "
            f"{{'bronze', 'silver', 'gold'}}."
        )
    if node_id not in bucket:
        available = sorted(bucket.keys())
        raise PackNodeNotFoundError(
            f"_resolve_node_from_pack: pack has no {layer} node "
            f"{node_id!r}. Available {layer} node ids: {available!r}. "
            f"Check the pack's {layer}/*.yaml files."
        )
    return bucket[node_id]


# ---------------------------------------------------------------------------
# Content-pack execution backend dispatcher
# ---------------------------------------------------------------------------


def _is_mart_only_run(layers: "list[str] | None") -> bool:
    """True when the operator scoped the run to silver/gold only —
    ``bronze`` is NOT in the requested ``layers`` — i.e. a mart run against
    *pre-existing* bronze tables.

    Drives off the operator's REQUESTED layers, not the resolved plan:
    implicit transitive include always pulls a mart's bronze deps into the
    plan for lineage, but a mart-only run must NOT *execute* or re-seed
    them. For such runs the orchestrator skips bronze nodes and the
    pre-extraction PVO gate, and instead fires the readiness gate to
    validate the LANDED bronze tables before any mart runs.

    ``layers`` falsy (no filter) means "all layers" → full run → False.
    """
    if not layers:
        return False
    requested = {layer.strip().lower() for layer in layers}
    return ("bronze" not in requested) and bool(requested & {"silver", "gold"})


def _run_content_pack_backend(
    *,
    bundle_path: "Path",
    spark: "SparkSession | None",
    mode: str,
    datasets: "list[str] | None",
    layers: "list[str] | None",
    dry_run: bool,
    resume_run_id: str | None,
    resolved_pack: "Any | None",
    tenant_profile: "Any | None",
    force_fingerprint_skip: bool = False,
    # Plan-hash continuity gate bypass (hidden --repin-plan-hash).
    # Threaded into each cp_execute_node call so a diverged AIDPF-4040
    # incremental is repinned (audit + proceed) instead of blocked.
    repin_plan_hash: bool = False,
    # Shared run_id contract. When the top-level
    # dispatcher (the caller) already minted a run_id (because bronze
    # + content-pack must share one), pass it in and the content-pack
    # backend will adopt it instead of minting `cp-<timestamp>-<hex>`.
    shared_run_id: str | None = None,
    # Enable the bronze readiness gate.
    # Default off so unit tests / direct callers that don't pre-seed
    # bronze tables don't trip on missing tables; the top-level
    # dispatcher in `run()` flips this on for full-medallion invocations.
    enable_bronze_readiness_gate: bool = False,
    # Resume support. When the top-level dispatcher
    # read fusion_autopilot_state to build a ResumeContext, it threads
    # the snapshot through here so the per-node loop can short-circuit
    # already-succeeded nodes (emit ``resumed_skip`` instead of
    # re-dispatching) and the bronze-readiness gate (above) narrows
    # to the reattempt-only cp_filter. ``None`` outside a resume.
    shared_resume_context: "Any | None" = None,
    # Disable transitive include in the plan resolver.
    strict_scope: bool = False,
) -> RunSummary:
    """Execute bronze + silver + gold via the content-pack runner.

    ``bronze`` / ``silver`` / ``gold`` nodes all dispatch through
    ``sql_runner.execute_node``; the bronze adapter
    (``orchestrator/builtins/bronze_extract_adapter.py``) handles the
    BICC extract that the v1 dispatcher used to own.

    Args:
        bundle_path: path to ``bundle.yaml``.
        spark: optional pre-existing SparkSession.
        mode: ``"seed"`` or ``"incremental"``.
        datasets / layers: content-pack node-id and layer filters
            (interpreted by :func:`resolve_content_pack_plan`).
        dry_run: returns an empty RunSummary without dispatching.
        resume_run_id: hydrates per-node resumable state.
        resolved_pack: pre-loaded ``ResolvedPack``. CLI / inline
            passes the laptop-resolved pack; REST notebook passes the
            cluster-side reconstructed pack from
            ``materialize_staged_pack`` + ``load_full_chain``.
        tenant_profile: pre-loaded ``TenantProfile``. Same shape as
            above.

    Returns:
        Standard :class:`RunSummary` with one :class:`RunStep` per
        executed node.

    Raises:
        ValueError: ``resolved_pack`` or ``tenant_profile`` is None.
    """
    # Lazy imports — the content-pack deps don't load on bare-package
    # consumers (eg. dispatch / schema utilities).
    from datetime import datetime as _dt, timezone as _tz
    from uuid import uuid4
    from .content_pack_plan_resolver import resolve_content_pack_plan
    from .sql_runner import execute_node as cp_execute_node
    from .sql_renderer import RunContext as CpRunContext
    from .state_phase2 import ensure_state_columns_v2
    from ..schema.bundle import (
        AIDPF_1032_RESUME_NOT_SUPPORTED,
        load_bundle as _load_bundle_v2,
    )
    from ..schema.tenant_profile import compute_profile_hash

    # ``--resume`` on the content-pack backend is supported by:
    #   1. Adopting the supplied ``resume_run_id`` as the shared run_id
    #      (the per-node loop's prior-state hydration + plan-hash drift
    #      gate already enforce the resume contract).
    #   2. Falling through to the normal per-node dispatch — nodes whose
    #      latest state row is already ``success`` for this run_id are
    #      idempotent in the atomic-commit model; non-success nodes
    #      retry through the same dispatcher path.
    # No bespoke "resume planner" is needed because the content-pack
    # backend's per-node atomicity (each ``execute_node`` is a full
    # preflight → render → drift → execute → quality → state commit) is
    # the resume unit.
    if resolved_pack is None:
        raise ValueError(
            "_run_content_pack_backend: resolved_pack is None. The CLI / "
            "inline path is responsible for loading the pack via "
            "load_full_chain(...) and passing it in. REST dispatch passes "
            "the cluster-side reconstructed pack."
        )
    if tenant_profile is None:
        raise ValueError(
            "_run_content_pack_backend: tenant_profile is None. The CLI / "
            "inline path loads the profile via load_tenant_profile(...); "
            "REST dispatch reconstructs it via load_tenant_profile_from_string."
        )

    bundle, paths = _load_bundle_v2(bundle_path)
    bundle_project = bundle.project

    # Every resolver call from this point on uses the bundle's declared
    # scope as the implicit root set. A no-CLI-filter run executes only
    # bundle-declared roots + transitive deps, NOT every pack node.
    bundle_scope = _effective_bundle_scope(bundle)

    if dry_run:
        # Populate the content-pack dry-run plan so the
        # renderer can show operators which silver/gold nodes would run +
        # how each would be dispatched. Plan resolution is cheap (pure
        # data walk; no Spark / BICC).
        plan_nodes = _build_content_pack_dry_run_plan(
            resolved_pack=resolved_pack,
            datasets=datasets,
            layers=layers,
            strict_scope=strict_scope,
            bundle_scope=bundle_scope,
        )
        return RunSummary.empty(
            bundle_project=bundle_project, mode=mode, plan=plan_nodes,
        )

    spark = spark or _bootstrap_spark()

    # Mint run_id BEFORE the drift gate so the drift artifact, any
    # force-skip audit row, and the RunSummary all carry the same id.
    #
    # When the top-level dispatcher minted a shared
    # run_id (so bronze + cp join cleanly on run_id), adopt it instead
    # of minting a `cp-`-prefixed one. The prefix loses meaning once
    # the same run also extracts bronze through the legacy path.
    #
    # Also adopt ``resume_run_id`` when supplied so
    # the resumed run writes state rows under the same id as the
    # original failed run (joining cleanly with the prior state).
    # Precedence: explicit shared_run_id > resume_run_id > newly minted.
    if shared_run_id is not None:
        run_id = shared_run_id
    elif resume_run_id is not None:
        run_id = resume_run_id
    else:
        run_id = f"cp-{_dt.now(_tz.utc).strftime('%Y%m%d%H%M%S')}-{uuid4().hex[:8]}"

    # Bronze-schema fingerprint drift gate. Runs BEFORE any
    # Spark write and BEFORE state.ensure_state_table. Returns a
    # `PreflightOutcome`; raises only via the SchemaDriftDetectedError
    # constructor here (the helper itself never raises drift-typed
    # exceptions — that's the CLI-mapping boundary).
    from .preflight_evidence import check_bronze_fingerprint_drift
    from ..schema.errors import SchemaDriftDetectedError

    preflight = check_bronze_fingerprint_drift(
        spark=spark,
        bundle=bundle,
        bundle_path=bundle_path,
        pack=resolved_pack,
        profile=tenant_profile,
        run_id=run_id,
        mode=mode,
        workdir=bundle_path.resolve().parent,
        force_skip=force_fingerprint_skip,
    )
    if preflight.kind == "drift":
        raise SchemaDriftDetectedError(
            run_id=run_id,
            diagnostic_path=preflight.diagnostic_path,  # type: ignore[arg-type]
            summary=preflight.summary,
            prior_fingerprint=preflight.prior_fingerprint,  # type: ignore[arg-type]
            current_fingerprint=preflight.current_fingerprint,  # type: ignore[arg-type]
        )

    # State-table setup + content-pack additive migration. ensure_state_table
    # creates the base table if needed; ensure_state_columns_v2 adds the
    # content-pack columns + redeploys the latest view with the
    # widened grain.
    state.ensure_state_table(spark, paths)
    ensure_state_columns_v2(spark, paths)

    # NOTE: the ``--force-fingerprint-skip`` audit row is DEFERRED to AFTER the
    # run-manifest write (below), so the manifest stays the FIRST hard write.
    # Otherwise a crash between this point and the manifest would leave a
    # terminal audit row but NO manifest — a state a resume could misread. The
    # row uses the reserved ``__fingerprint_skip__`` id + ``fingerprint_skip``
    # mode, so the reader excludes it either way.

    # Build the run context the renderer needs. ``active_profile_name``
    # is the bundle's contentPack.profile — keyed by the renderer + builtin
    # adapters into pack.pack.profiles for pack-default lookups. Required
    # field (no default); the content-pack backend has already validated
    # that bundle.content_pack and bundle.content_pack.profile exist.
    active_profile_name = bundle.content_pack.profile  # type: ignore[union-attr]
    # Build the source-id -> bronze-table map from the resolved pack's
    # bronze nodes, using each node's ``target`` (not ``id``). The pack
    # contract permits id != target, e.g. gl_journal_lines can target
    # gl_journal_headers.
    bronze_table_for_source: dict[str, str] = {
        node_id: paths.bronze(node.target)
        for node_id, node in resolved_pack.bronze.items()
    }
    # Legacy pack.bronze_yaml fallback for packs that haven't
    # migrated to per-file bronze/<id>.yaml).
    legacy_bronze = getattr(resolved_pack, "bronze_yaml", None) or {}
    for ds in legacy_bronze.get("datasets", []) or []:
        if not isinstance(ds, dict):
            continue
        ds_id = ds.get("id")
        if not ds_id or ds_id in bronze_table_for_source:
            continue
        # Legacy YAML carries the bronze table name as "target" or
        # "pvo" depending on pack vintage; fall back to id.
        table_name = ds.get("target") or ds.get("pvo") or ds_id
        bronze_table_for_source[ds_id] = paths.bronze(table_name)
    ctx = CpRunContext(
        catalog=bundle.aidp.catalog,
        bronze_schema=bundle.aidp.bronze_schema,
        silver_schema=bundle.aidp.silver_schema,
        gold_schema=bundle.aidp.gold_schema,
        run_id=run_id,
        active_profile_name=active_profile_name,
        prior_watermark={},
        mode=mode,
        bronze_table_for_source=bronze_table_for_source,
        # Bundle threaded so bronze_extract_adapter can read
        # bundle.fusion.{service_url, username, password,
        # external_storage} + bundle.fusion.schemaOverrides.<id>.
        bundle=bundle,
    )

    profile_hash = compute_profile_hash(tenant_profile)

    plan = resolve_content_pack_plan(
        resolved_pack, datasets=datasets, layers=layers,
        strict_scope=strict_scope,
        bundle_scope=bundle_scope,
    )

    # Bronze readiness gate. Verify every in-scope
    # silver/gold node's transitive bronze dependencies exist AND
    # surface every required column BEFORE dispatching any node.
    # When the gate fails, return a RunSummary with the (otherwise
    # empty) plan plus a synthetic gate-failure RunStep so the CLI
    # exits non-zero AND operators see the gap. No silver/gold state
    # rows are written.
    # Silver/gold-only run = marts in scope but NO bronze nodes this run
    # (bronze pre-exists in AIDP). There's no pre-extraction PVO gate to
    # lean on (nothing is being extracted), so batch-validate every in-scope
    # mart's required columns against the LANDED bronze tables (DESCRIBE)
    # upfront — fail fast, all gaps at once, before any mart runs. Full
    # seeds deliberately DON'T trigger this: the pre-extraction PVO source
    # gate already fail-fasts them, and an all-or-nothing gate here would
    # regress their per-node cascade (independent marts proceeding past one
    # bronze failure).
    _mart_only = _is_mart_only_run(layers)

    # COA fail-fast (feature: fail-fast-seed-validation). Order gl_coa (COA-source
    # bronze) FIRST so no expensive PVO is extracted until COA is proven, and
    # compute applicability + the escape-hatch flag once for reuse by the
    # pre-extraction structural gate, the in-loop checkpoint, and the mart-only
    # checkpoint below.
    from .node_preflight import (
        coa_applicable_sources,
        evaluate_coa_checkpoint,
        order_coa_source_first,
    )

    _coa_sources = coa_applicable_sources(resolved_pack, plan)
    _allow_unprovable = bool(
        getattr(getattr(bundle, "content_pack", None), "allow_unprovable_coa", False)
    )
    plan = order_coa_source_first(plan, _coa_sources)

    def _coa_gate_abort(
        result: "CoaCheckpointResult", prior_steps: "list[RunStep]"
    ) -> RunSummary:
        """Build a fail-fast RunSummary for a hard COA outcome.

        Includes any already-emitted steps (e.g. a successfully-landed gl_coa on
        an in-loop abort) plus one synthetic ``__coa_gate__`` gate-failure step
        carrying the offending code(s). No later (expensive) bronze dispatches;
        no silver/gold state rows written.
        """
        codes = sorted({e.code for e in result.blocking})
        detail = "; ".join(
            f"{e.code} ({e.source}): {e.message}" for e in result.blocking
        )
        gate_step = RunStep.gate_failed(
            run_id=run_id,
            mode=mode,
            layer="silver",
            gate_dataset_id="__coa_gate__",
            aidpf_code=codes[0],
            error_message=detail,
        )
        gate_now = _dt.now(_tz.utc)
        return RunSummary(
            run_id=run_id,
            started_at=gate_now,
            finished_at=gate_now,
            bundle_project=bundle_project,
            mode=mode,  # type: ignore[arg-type]
            steps=(*prior_steps, gate_step),
        )

    if (enable_bronze_readiness_gate or _mart_only) and not dry_run:
        from .bronze_readiness import (
            BronzeReadinessGateError,
            AIDPF_2071_BRONZE_READINESS_GATE_FAILED,
            assert_bronze_readiness,
        )
        # On resume, narrow the gate's cp_filter to the reattempt
        # subset of nodes. A succeeded node's bronze dependency that
        # was manually dropped post-success is not the resume's
        # problem; gating over it would block recovery of unrelated
        # silver/gold work. All-succeeded → skip the gate entirely
        # (no node will dispatch this run).
        gate_cp_filter: tuple[list[str] | None, list[str] | None] | None
        if shared_resume_context is not None:
            reattempt_ids = [
                node.id for node in plan
                if node.id not in shared_resume_context.succeeded
            ]
            gate_cp_filter = (reattempt_ids, None) if reattempt_ids else None
        else:
            gate_cp_filter = (datasets, layers)

        if gate_cp_filter is not None:
            try:
                assert_bronze_readiness(
                    spark,
                    resolved_pack=resolved_pack,
                    cp_filter=gate_cp_filter,
                    paths=paths,
                    run_id=run_id,
                    diagnostics_root=(bundle_path.resolve().parent / ".aidp" / "diagnostics"),
                    tenant_profile=tenant_profile,
                )
            except BronzeReadinessGateError as gate_exc:
                gate_step = RunStep.gate_failed(
                    run_id=run_id,
                    mode=mode,
                    layer="silver",
                    gate_dataset_id="__bronze_readiness_gate__",
                    aidpf_code=AIDPF_2071_BRONZE_READINESS_GATE_FAILED,
                    error_message=str(gate_exc),
                )
                gate_now = _dt.now(_tz.utc)
                return RunSummary(
                    run_id=run_id,
                    started_at=gate_now,
                    finished_at=gate_now,
                    bundle_project=bundle_project,
                    mode=mode,  # type: ignore[arg-type]
                    steps=(gate_step,),
                )

    # COA checkpoint (feature: fail-fast-seed-validation). A COA source is
    # already MATERIALIZED at this point — so the in-loop data checkpoint (row
    # 4b) will NOT fire for it — when either (a) this is a mart-only run (bronze
    # skipped), or (b) this is a RESUME and the COA source already succeeded (it
    # will be resumed-skipped in the loop). For those sources the FULL landed-
    # data checkpoint MUST run HERE, before any unfinished non-COA bronze
    # dispatches — otherwise a run originally aborted at the in-loop checkpoint
    # (AIDPF-2018 / AIDPF-2074) could resume straight into the expensive bronze
    # with COA still unproven. A COA source that will still LAND in-loop only
    # needs the structural gate here (its data probes run at 4b after it lands).
    # True once the FULL (data) COA checkpoint has passed for already-landed COA
    # sources BEFORE the loop (mart-only / resume where gl_coa pre-exists). Lets
    # downstream COA consumers accept an additive-COA plan-hash advance from the
    # first node (incremental-coa-chart-onboarding). A fresh incremental leaves
    # this False; the in-loop post-land checkpoint flips it after gl_coa lands.
    _coa_preloop_data_ckpt_ok = False
    if _coa_sources and not dry_run:
        from .node_preflight import split_landed_coa_sources

        _succeeded_now: frozenset[str] = (
            shared_resume_context.succeeded
            if shared_resume_context is not None
            else frozenset()
        )
        _landed_coa, _pending_coa = split_landed_coa_sources(
            _coa_sources, mart_only=_mart_only, succeeded=_succeeded_now
        )
        # Should-fix: only RE-probe a landed COA source that still has an
        # UNFINISHED in-scope COA consumer. If every COA-consuming node already
        # succeeded, COA was already proven in the original run — re-running the
        # data probes would let a transient probe failure block a documented
        # idempotent complete resume, or block recovery of an unrelated failed
        # mart after dim_account completed. (The F1 abort case still fires: an
        # original COA abort means dim_account never ran, so it is unfinished
        # and its source stays in _needed_coa.)
        _needed_coa = coa_applicable_sources(
            resolved_pack, [n for n in plan if n.id not in _succeeded_now]
        )
        _landed_coa &= _needed_coa
        for _srcs, _structural_only in ((_landed_coa, False), (_pending_coa, True)):
            if not _srcs:
                continue
            _coa_ckpt = evaluate_coa_checkpoint(
                spark,
                pack=resolved_pack,
                profile=tenant_profile,
                bronze_table_for_source=bronze_table_for_source,
                coa_sources=_srcs,
                allow_unprovable=_allow_unprovable,
                structural_only=_structural_only,
            )
            for _w in _coa_ckpt.warnings:
                _log.warning("COA checkpoint (allowUnprovableCOA): %s", _w)
            if not _coa_ckpt.ok:
                return _coa_gate_abort(_coa_ckpt, [])
        # A non-empty landed set means the FULL data checkpoint (structural_only
        # =False) ran and passed above (a failure would have aborted).
        _coa_preloop_data_ckpt_ok = bool(_landed_coa)

    # Per-node execution loop. execute_node writes its own state rows
    # (success + failure paths) and returns a NodeExecutionResult; we
    # translate that into RunStep entries for the RunSummary.
    #
    # Two contracts enforced in this loop:
    #
    #   1. Prior state hydration. Before each node's execute_node, we
    #      look up the latest successful primary state row to populate
    #      ctx.prior_watermark[<source_id>] (so {{ watermark_predicate }}
    #      filters the source delta instead of evaluating 1=1 and
    #      scanning the full source) AND prior_plan_hash (so the
    #      AIDPF-4040 drift gate can fire on incremental resume).
    #
    #   2. Cascade-abort on failure. The plan is topologically ordered
    #      (resolve_content_pack_plan sorts silver-then-gold with
    #      explicit silver->silver and silver->gold dependencies
    #      threaded through). When a node returns any non-success
    #      status, downstream nodes that depend on it (directly or
    #      transitively) MUST NOT be dispatched — they'd read stale
    #      pre-existing upstream tables and commit success rows after
    #      the current run's upstream failed. We track failed node IDs
    #      and skip-cascade any dependent.
    # Upfront bronze source-schema gate (AIDPF-4071, batch). Probe every
    # in-scope bronze PVO's schema in ONE metadata-only call BEFORE
    # extracting anything; if any node declares a column the live PVO
    # lacks, abort the whole run now — fail-fast before seeding the
    # healthy nodes ahead of it (a per-node gate would only spare that one
    # node's extract, not the nodes before it). Skipped on dry-run, and on
    # mart-only runs (bronze isn't being extracted — the readiness gate
    # above validated the LANDED tables instead).
    if not dry_run and not _mart_only:
        from .bronze_readiness import (
            _compute_required_columns,
            _resolve_in_scope_nodes,
        )
        from .sql_runner import check_bronze_source_schemas

        _bronze_ids = [
            n.id for n in plan
            if n.layer == "bronze" and n.implementation.type == "bronze_extract"
        ]
        # What in-scope silver/gold need from each bronze source (transitive
        # silver->silver->bronze; $column aliases resolved). Folding this into
        # the source probe means a silver/gold column its bronze PVO can't
        # supply aborts BEFORE extraction — not after the bronze pull lands.
        _in_scope_sg = _resolve_in_scope_nodes(resolved_pack, (datasets, layers))
        _downstream_required = _compute_required_columns(
            _in_scope_sg, resolved_pack, tenant_profile
        )
        _src_failures = check_bronze_source_schemas(
            spark, pack=resolved_pack, bundle=bundle, profile=tenant_profile,
            bronze_node_ids=_bronze_ids, run_id=run_id,
            downstream_required=_downstream_required,
        )
        if _src_failures:
            _failed = {f["node"] for f in _src_failures}
            _msg_by = {f["node"]: f["message"] for f in _src_failures}
            _gnow = _dt.now(_tz.utc)
            _gsteps: list[RunStep] = []
            for n in plan:
                if n.id in _failed:
                    _gsteps.append(RunStep(
                        run_id=run_id, dataset_id=n.id, layer=n.layer, mode=mode,
                        status="failed", row_count=0, duration_seconds=0.0,
                        error_message=_msg_by[n.id], watermark_used=None,
                    ))
                else:
                    _gsteps.append(RunStep(
                        run_id=run_id, dataset_id=n.id, layer=n.layer, mode=mode,
                        status="skipped", row_count=None, duration_seconds=0.0,
                        error_message=None, watermark_used=None,
                        skip_reason="aborted",
                    ))
            return RunSummary(
                run_id=run_id, started_at=_gnow, finished_at=_gnow,
                bundle_project=bundle_project, mode=mode,
                steps=tuple(_gsteps),
                diagnostics=tuple(f["diagnostic"] for f in _src_failures),
            )

    # Durable pre-execution run manifest (gate-ordering row 0). Written ONCE on
    # a FRESH run (never on resume — the manifest is immutable), AFTER the
    # read-only pre-write gates and BEFORE the first node dispatch, so a
    # manifest-commit failure aborts cleanly with nothing extracted (AIDPF-4022).
    if resume_run_id is None and not dry_run:
        from .run_manifest import AIDPF_4022_MANIFEST_COMMIT_FAILED
        try:
            _manifest_json = _build_run_manifest_json(
                plan=plan,
                resolved_pack=resolved_pack,
                bundle=bundle,
                paths=paths,
                profile_hash=profile_hash,
                tenant_profile=tenant_profile,
                mode=mode,
                datasets=datasets,
                layers=layers,
                strict_scope=strict_scope,
                allow_unprovable_coa=_allow_unprovable,
            )
            _write_run_manifest_row(
                spark, paths, run_id=run_id, mode=mode,
                manifest_json=_manifest_json,
            )
        except Exception as _manifest_exc:
            _mnow = _dt.now(_tz.utc)
            return RunSummary(
                run_id=run_id,
                started_at=_mnow,
                finished_at=_mnow,
                bundle_project=bundle_project,
                mode=mode,  # type: ignore[arg-type]
                steps=(
                    RunStep.gate_failed(
                        run_id=run_id,
                        mode=mode,
                        layer="silver",
                        gate_dataset_id="__run_manifest__",
                        aidpf_code=AIDPF_4022_MANIFEST_COMMIT_FAILED,
                        error_message=(
                            f"run manifest commit failed before extraction: "
                            f"{_manifest_exc}. Nothing was extracted; re-run "
                            f"`--mode seed`."
                        ),
                    ),
                ),
            )

    # Deferred ``--force-fingerprint-skip`` audit row — written AFTER the
    # manifest (Finding 3) so the manifest is the first hard write. Uses the
    # reserved ``__fingerprint_skip__`` id + ``fingerprint_skip`` mode, so the
    # resume reader excludes it from succeeded / scope / mode inference.
    if preflight.kind == "skip_force_flag":
        state.write_fingerprint_skip_row(
            spark, paths,
            run_id=run_id,
            prior_fingerprint=preflight.prior_fingerprint,  # type: ignore[arg-type]
            current_fingerprint=preflight.current_fingerprint,  # type: ignore[arg-type]
        )

    started_at = _dt.now(_tz.utc)
    steps: list[RunStep] = []
    diagnostics: list[dict] = []
    failed_node_ids: set[str] = set()

    # Additive-COA fast path (feature: incremental-coa-chart-onboarding). Built
    # once per run; consulted by each node's AIDPF-4040 gate. Active only on
    # incremental. `coa_checkpoint_passed` starts False and is flipped True after
    # the post-land COA data checkpoint succeeds — so the COA-SOURCE node
    # (gl_coa) accepts pre-checkpoint (it produces the data) while downstream
    # consumers wait for it. `protected_charts=None` (unreadable dim) fails
    # closed. Inactive → every gate keeps its pre-feature behaviour.
    coa_inc = None
    if mode == "incremental" and not dry_run:
        from .coa_change import coa_projection_of
        from .coa_incremental import (
            CoaIncrementalContext,
            manifest_for_run,
            read_protected_charts,
        )

        coa_inc = CoaIncrementalContext(
            active=True,
            incoming_coa=coa_projection_of(tenant_profile),
            protected_charts=read_protected_charts(spark, resolved_pack, paths),
            coa_source_ids=frozenset(_coa_sources),
            coa_checkpoint_passed=_coa_preloop_data_ckpt_ok,
            manifest_by_run_id=lambda rid: manifest_for_run(spark, paths, rid),
        )

    for node in plan:
        # Mart-only run: bronze is in the plan for lineage but must NOT be
        # re-seeded — the marts run against the pre-existing landed tables
        # (already validated by the readiness gate above). Skip executing
        # bronze nodes entirely (no state row, no re-extract).
        if _mart_only and node.layer == "bronze":
            continue
        # Resume short-circuit. Nodes whose latest
        # terminal state row under this run_id is 'success' (or a
        # carry-forwarded 'resumed_skipped') emit a fresh
        # resumed_skipped step instead of re-dispatching. The
        # ResumeContext is the source of truth — even if the operator
        # manually dropped the node's table between runs, we trust
        # state; the bronze-readiness gate above catches a dropped
        # upstream that a reattempt node actually reads.
        if (
            shared_resume_context is not None
            and node.id in shared_resume_context.succeeded
        ):
            _emit_content_pack_resumed_skip(
                steps=steps,
                spark=spark, paths=paths,
                node=node, run_id=run_id, mode=mode,
                resume_context=shared_resume_context,
                tenant_profile=tenant_profile,
                resolved_pack=resolved_pack,
            )
            continue

        # Cascade-abort check — if any of this node's silver-deps is in
        # failed_node_ids, skip it with a 'cascade' RunStep instead of
        # dispatching to execute_node. Write a best-effort soft state
        # row for the skipped node so the persisted audit trail records
        # the cascade — without this, status/audit readers would still
        # show the node's previous successful run (or no record at all)
        # for the current run_id, violating the v1 audit-completeness
        # invariant.
        cascade_blocking = _find_cascade_blocker(node, failed_node_ids)
        if cascade_blocking:
            _safe_write_content_pack_cascade_skip_row(
                spark=spark,
                paths=paths,
                node=node,
                run_id=run_id,
                mode=mode,
                blocker_id=cascade_blocking,
                tenant_profile=tenant_profile,
                resolved_pack=resolved_pack,
            )
            steps.append(
                RunStep(
                    run_id=run_id,
                    dataset_id=node.id,
                    layer=node.layer,
                    mode=mode,  # type: ignore[arg-type]
                    status="skipped",
                    row_count=None,
                    duration_seconds=0.0,
                    error_message=f"cascade: upstream {cascade_blocking!r} failed",
                    watermark_used=None,
                    last_watermark=None,
                    skip_reason="cascade",
                    plan_hash=None,
                    plan_snapshot=None,
                )
            )
            # The skipped node itself is also part of the failed set so
            # transitive dependents (gold depending on a skipped silver)
            # propagate the skip.
            failed_node_ids.add(node.id)
            continue

        # Prior-state hydration for the drift gate + watermark predicate.
        # ``mode`` is threaded in so the helper can fail closed on
        # incremental reads: a state-read
        # failure in incremental mode must NOT silently degrade to
        # seed semantics.
        prior_plan_hash, prior_watermark_for_node, prior_run_id_for_node = (
            _read_prior_state_for_node(spark, paths, node, mode=mode)
        )
        # Build a per-node ctx that carries the prior watermark for the
        # primary source. We rebuild the ctx (instead of mutating
        # ctx.prior_watermark) so it stays a clean immutable dataclass.
        node_ctx = CpRunContext(
            catalog=ctx.catalog,
            bronze_schema=ctx.bronze_schema,
            silver_schema=ctx.silver_schema,
            gold_schema=ctx.gold_schema,
            run_id=ctx.run_id,
            active_profile_name=ctx.active_profile_name,
            prior_watermark=prior_watermark_for_node,
            mode=ctx.mode,
            bronze_table_for_source=ctx.bronze_table_for_source,
            bundle=ctx.bundle,
        )

        node_started = _dt.now(_tz.utc)
        result = cp_execute_node(
            spark,
            node=node,
            pack=resolved_pack,
            profile=tenant_profile,
            ctx=node_ctx,
            paths=paths,
            mode=mode,  # type: ignore[arg-type]
            profile_hash=profile_hash,
            prior_plan_hash=prior_plan_hash,
            repin_plan_hash=repin_plan_hash,
            coa_inc=coa_inc,
            prior_run_id=prior_run_id_for_node,
        )
        node_duration = (_dt.now(_tz.utc) - node_started).total_seconds()
        status: str = "success" if result.status == "success" else "failed"
        if status != "success":
            failed_node_ids.add(node.id)
        # Collect any structured failure context (e.g. AIDPF-4071) so the
        # laptop dispatcher can persist it under .aidp/diagnostics/ for
        # skill consumption. Rides RunSummary.diagnostics → the marker.
        if getattr(result, "diagnostic", None):
            diagnostics.append(result.diagnostic)
        steps.append(
            RunStep(
                run_id=run_id,
                dataset_id=node.id,
                layer=node.layer,
                mode=mode,  # type: ignore[arg-type]
                status=status,  # type: ignore[arg-type]
                row_count=result.row_count,
                duration_seconds=node_duration,
                error_message=result.error_message or None,
                watermark_used=None,
                last_watermark=result.output_watermark,
                plan_hash=result.plan_hash or None,
                plan_snapshot=None,
            )
        )

        # COA fail-fast (rows 4a/4b). A COA-source bronze node runs FIRST; act on
        # its outcome BEFORE any later (expensive) bronze dispatches — the
        # resilient per-node cascade would NOT stop them (gl_coa feeds a silver
        # mart, not the other bronze PVOs).
        if node.id in _coa_sources:
            # 4a — a COA-source NODE failure (extract/encode/write/state) aborts
            # the run now, carrying the node's own failure code (already in
            # `steps`). No later bronze dispatches; a bare --resume reruns the
            # full unfinished closure incl. this failed node.
            if status != "success":
                _log.warning(
                    "COA-source node %r failed; aborting before later bronze "
                    "extracts (COA unproven).",
                    node.id,
                )
                return RunSummary(
                    run_id=run_id,
                    started_at=started_at,
                    finished_at=_dt.now(_tz.utc),
                    bundle_project=bundle_project,
                    mode=mode,
                    steps=tuple(steps),
                    diagnostics=tuple(diagnostics),
                )
            # 4b — gl_coa landed: run the data-probe checkpoint against it before
            # the next bronze node dispatches.
            _ckpt = evaluate_coa_checkpoint(
                spark,
                pack=resolved_pack,
                profile=tenant_profile,
                bronze_table_for_source=ctx.bronze_table_for_source,
                coa_sources={node.id},
                allow_unprovable=_allow_unprovable,
                structural_only=False,
            )
            for _w in _ckpt.warnings:
                _log.warning("COA checkpoint (allowUnprovableCOA): %s", _w)
            if not _ckpt.ok:
                return _coa_gate_abort(_ckpt, steps)
            # Post-land COA data checkpoint passed → downstream COA CONSUMERS may
            # now accept an additive-COA plan-hash advance (incremental-coa-chart-
            # onboarding). The COA SOURCE already ran without this signal.
            if coa_inc is not None:
                coa_inc.coa_checkpoint_passed = True

    finished_at = _dt.now(_tz.utc)
    return RunSummary(
        run_id=run_id,
        started_at=started_at,
        finished_at=finished_at,
        bundle_project=bundle_project,
        mode=mode,
        steps=tuple(steps),
        diagnostics=tuple(diagnostics),
    )


def _compute_sem_by_id(
    plan: "list[Any]", resolved_pack: "Any", bundle: "Any"
) -> dict[str, str]:
    """Per-node ``sem`` fingerprints for ``plan`` (shared by the manifest WRITE
    and the resume REPLAY so they can never diverge). Reads each SQL node's
    template bytes best-effort — an unreadable template degrades to a NULL-sql
    sem rather than aborting."""
    from . import run_manifest as _rmf

    overrides = getattr(getattr(bundle, "fusion", None), "schema_overrides", {}) or {}
    sem_by_id: dict[str, str] = {}
    for node in plan:
        sql_bytes: bytes | None = None
        if getattr(node.implementation, "type", None) == "sql":
            try:
                sql_path = resolved_pack.root_for(
                    f"{node.layer}/{node.id}"
                ) / node.implementation.sql
                sql_bytes = sql_path.read_bytes()
            except Exception:  # pragma: no cover — best-effort sem input
                sql_bytes = None
        sem_by_id[node.id] = _rmf.compute_node_sem(
            node, sql_bytes=sql_bytes, schema_override=overrides.get(node.id)
        )
    return sem_by_id


def _resolve_plan_for_manifest(
    resolved_pack: "Any",
    datasets: "list[str] | None",
    layers: "list[str] | None",
    strict_scope: bool,
    bundle: "Any",
) -> "list[Any]":
    """Replay the manifest's resolver inputs against the CURRENT pack graph."""
    from .content_pack_plan_resolver import resolve_content_pack_plan

    return resolve_content_pack_plan(
        resolved_pack,
        datasets=datasets,
        layers=layers,
        strict_scope=strict_scope,
        bundle_scope=_effective_bundle_scope(bundle),
    )


def _manifest_topology_for_plan(
    plan: "list[Any]", resolved_pack: "Any", bundle: "Any"
) -> "list[dict]":
    """Canonical topology (with sem) for a plan — used to compare against the
    stored manifest topology on resume."""
    from . import run_manifest as _rmf

    return _rmf.canonical_topology(
        plan, sem_by_id=_compute_sem_by_id(plan, resolved_pack, bundle)
    )


def _build_run_manifest_json(
    *,
    plan: "list[Any]",
    resolved_pack: "Any",
    bundle: "Any",
    paths: "Any",
    profile_hash: str,
    tenant_profile: "Any",
    mode: str,
    datasets: "list[str] | None",
    layers: "list[str] | None",
    strict_scope: bool,
    allow_unprovable_coa: bool,
) -> str:
    """Assemble + serialize the durable run manifest (feature:
    fail-fast-seed-validation; v2 COA baseline for incremental-coa-chart-onboarding)."""
    from oracle_ai_data_platform_fusion_autopilot import __version__ as _pv

    from . import run_manifest as _rmf
    from .coa_change import coa_projection_of, non_coa_semantic_hash
    from .plan_hash import _identity_dict

    topology = _rmf.canonical_topology(
        plan, sem_by_id=_compute_sem_by_id(plan, resolved_pack, bundle)
    )
    manifest = _rmf.build_manifest(
        datasets=datasets,
        layers=layers,
        strict_scope=strict_scope,
        topology=topology,
        mode=mode,
        identity=_identity_dict(bundle, paths, _pv),
        pack_fingerprint=_rmf.compute_pack_fingerprint(
            resolved_pack,
            getattr(getattr(bundle, "content_pack", None), "profile", None),
        ),
        profile_hash=profile_hash,
        allow_unprovable_coa=allow_unprovable_coa,
        # v2 COA baseline (incremental-coa-chart-onboarding): the durable
        # prior-COA mapping + the allowlist non-COA semantic hash a later
        # resume/incremental proves an additive change against.
        coa_projection=coa_projection_of(tenant_profile),
        non_coa_semantic_hash=non_coa_semantic_hash(tenant_profile, resolved_pack),
    )
    return _rmf.serialize_manifest(manifest)


def _write_run_manifest_row(
    spark: "Any", paths: "Any", *, run_id: str, mode: str, manifest_json: str
) -> None:
    """HARD-write the single reserved ``__run_manifest__`` state row.

    Stored ``status='deferred'`` / ``skip_reason='aborted'`` (a resumable-
    terminal, non-``succeeded`` status) with the manifest JSON in the dedicated
    ``run_manifest`` column. Raises (via ``write_state_rows_hard``) on a Delta
    failure so the caller can abort with AIDPF-4022 before any node dispatches.
    """
    from datetime import datetime as _dt2, timezone as _tz2

    from .run_manifest import RUN_MANIFEST_DATASET_ID
    from .state_phase2 import write_state_rows_hard

    write_state_rows_hard(
        spark,
        paths,
        [
            {
                "run_id": run_id,
                "dataset_id": RUN_MANIFEST_DATASET_ID,
                "layer": "silver",
                "mode": mode,
                "status": "deferred",
                "skip_reason": "aborted",
                "last_run_at": _dt2.now(_tz2.utc),
                "duration_seconds": 0.0,
                "run_manifest": manifest_json,
            }
        ],
    )


def _safe_write_content_pack_cascade_skip_row(
    *,
    spark: "Any",
    paths: "Any",
    node: "Any",
    run_id: str,
    mode: str,
    blocker_id: str,
    tenant_profile: "Any | None",
    resolved_pack: "Any | None",
) -> None:
    """Best-effort soft state row for a cascade-skipped content-pack node.

    Mirrors sql_runner's _safe_write_failure_row pattern: assemble the
    row dict + call state_phase2.write_state_rows_hard, but wrap the
    write in try/except so a Spark failure here only loses the audit
    trail — never raises. Cursor advancement is preserved (no
    output_watermark on the row); the prior run's last_watermark is
    not touched because we leave the field NULL.

    Carries the upstream blocker id in ``error_message`` so audit
    readers can trace which dep triggered the cascade.
    """
    from datetime import datetime as _dt, timezone as _tz
    from . import state_phase2 as _sp2

    primary_source = _resolve_primary_source_id_for_state_read(node)
    now = _dt.now(_tz.utc)
    pack_id = getattr(getattr(resolved_pack, "pack", None), "id", None)
    pack_version = getattr(getattr(resolved_pack, "pack", None), "version", None)
    tenant = getattr(tenant_profile, "tenant", None)
    fingerprint = getattr(tenant_profile, "bronze_schema_fingerprint", None)

    row = {
        "run_id": run_id,
        "dataset_id": node.id,
        "layer": node.layer,
        "mode": mode,
        "last_watermark": None,
        "last_run_at": now,
        "status": "skipped",
        "row_count": None,
        "error_message": f"cascade: upstream {blocker_id!r} failed",
        "skip_reason": "cascade",
        "duration_seconds": None,
        "plan_hash": None,
        "plan_snapshot": None,
        "pack_id": pack_id,
        "pack_version": pack_version,
        "node_version": None,
        "node_implementation_type": getattr(node.implementation, "type", None),
        "rendered_sql_hash": None,
        "output_schema_hash": None,
        "profile_hash": None,
        "tenant_fingerprint": tenant,
        "fusion_version": None,
        "bronze_schema_fingerprint": fingerprint,
        "source_id": primary_source,
        "source_role": "primary",
        "input_watermark_start": None,
        "input_watermark_end": None,
        "output_watermark": None,
        "consumed_version": None,
        "delta_row_count": None,
    }
    try:
        _sp2.write_state_rows_hard(spark, paths, [row])
    except Exception:  # noqa: BLE001 — diagnostic write is best-effort
        return


def _emit_content_pack_resumed_skip(
    *,
    steps: "list[RunStep]",
    spark: "Any",
    paths: "Any",
    node: "Any",
    run_id: str,
    mode: str,
    resume_context: "Any",
    tenant_profile: "Any | None",
    resolved_pack: "Any | None",
) -> None:
    """Append a ``resumed_skipped`` step + best-effort soft state row.

    Used by ``_run_content_pack_backend``'s per-node loop when a node's
    id is in ``resume_context.succeeded``. The shape mirrors the v1
    resume path (RunStep.resumed_skip + a state row carrying the
    original run's ``plan_hash`` / ``plan_snapshot`` so the resumed
    row's drift-gate metadata is consistent with the prior success
    row).

    Carry-forwarded ``row_count`` / ``last_watermark`` come from
    ``resume_context``'s tuple-keyed dicts so the
    ``fusion_autopilot_state_latest`` projection preserves the original
    logical row count and bronze cursor instead of regressing them to
    NULL.

    State write is best-effort (matches the cascade-skip pattern at
    :func:`_safe_write_content_pack_cascade_skip_row`).
    """
    from datetime import datetime as _dt, timezone as _tz
    from . import state_phase2 as _sp2

    key = (node.id, node.layer)
    row_count = resume_context.succeeded_row_counts.get(key)
    last_watermark = resume_context.succeeded_last_watermarks.get(key)
    # CPResumeContext: no run-level plan_hash (CP writes per-node).
    # bronze_plan_snapshot lifted from any v1-shape bronze row; None
    # for pure silver/gold runs. read_content_pack_resumable_state
    # tolerates both fields being NULL on a resumed-skip row.
    plan_hash = None
    plan_snapshot = resume_context.bronze_plan_snapshot

    primary_source = _resolve_primary_source_id_for_state_read(node)
    now = _dt.now(_tz.utc)
    pack_id = getattr(getattr(resolved_pack, "pack", None), "id", None)
    pack_version = getattr(getattr(resolved_pack, "pack", None), "version", None)
    tenant = getattr(tenant_profile, "tenant", None)
    fingerprint = getattr(tenant_profile, "bronze_schema_fingerprint", None)

    steps.append(
        RunStep(
            run_id=run_id,
            dataset_id=node.id,
            layer=node.layer,
            mode=mode,  # type: ignore[arg-type]
            status="resumed_skipped",
            row_count=row_count,
            duration_seconds=0.0,
            error_message=(
                f"resume({run_id!r}): node already succeeded under this "
                f"run_id — carrying forward."
            ),
            watermark_used=None,
            last_watermark=last_watermark,
            skip_reason="resume-skip",
            plan_hash=plan_hash,
            plan_snapshot=plan_snapshot,
        )
    )

    row = {
        "run_id": run_id,
        "dataset_id": node.id,
        "layer": node.layer,
        "mode": mode,
        "last_watermark": last_watermark,
        "last_run_at": now,
        "status": "resumed_skipped",
        "row_count": row_count,
        "error_message": (
            f"resume({run_id!r}): node already succeeded under this "
            f"run_id — carrying forward."
        ),
        "skip_reason": "resume-skip",
        "duration_seconds": None,
        "plan_hash": plan_hash,
        "plan_snapshot": plan_snapshot,
        "pack_id": pack_id,
        "pack_version": pack_version,
        "node_version": None,
        "node_implementation_type": getattr(node.implementation, "type", None),
        "rendered_sql_hash": None,
        "output_schema_hash": None,
        "profile_hash": None,
        "tenant_fingerprint": tenant,
        "fusion_version": None,
        "bronze_schema_fingerprint": fingerprint,
        "source_id": primary_source,
        "source_role": "primary",
        "input_watermark_start": None,
        "input_watermark_end": None,
        "output_watermark": None,
        "consumed_version": None,
        "delta_row_count": None,
    }
    try:
        _sp2.write_state_rows_hard(spark, paths, [row])
    except Exception:  # noqa: BLE001 — diagnostic write is best-effort
        return


def _find_cascade_blocker(node: Any, failed_node_ids: set[str]) -> str | None:
    """Return a failed upstream node id if this node depends on one, else None.

    Walks both ``dependsOn.bronze`` and ``dependsOn.silver`` (intra-pack
    dependencies). Since bronze nodes run in the same plan, a failed
    bronze extract must cascade-skip its silver/gold consumers —
    otherwise a downstream node would dispatch, read the stale
    pre-existing bronze table, and commit a success row after its upstream
    failed. Bronze deps are checked first so the reported blocker is the
    earliest layer in the chain.
    """
    deps = getattr(node, "depends_on", None)
    if deps is None:
        return None
    bronze_deps = getattr(deps, "bronze", None) or []
    for dep in bronze_deps:
        if dep.id in failed_node_ids:
            return dep.id
    silver_deps = getattr(deps, "silver", None) or []
    for dep in silver_deps:
        if dep.id in failed_node_ids:
            return dep.id
    return None


def _read_prior_state_for_node(
    spark: "Any", paths: "Any", node: "Any", *, mode: str,
) -> "tuple[str | None, dict[str, Any]]":
    """Read the latest successful primary state row for a content-pack node.

    Returns ``(prior_plan_hash, prior_watermark_by_source)``.

    Empty result set (no prior successful row exists — the common
    first-run case) yields ``(None, {})`` in both modes:

    * ``prior_plan_hash=None`` makes the AIDPF-4040 drift gate a no-op
      (correct semantics; nothing to drift against).
    * Empty ``prior_watermark`` makes the renderer emit
      ``{{ watermark_predicate }}`` as ``1=1`` (correct semantics for
      seed mode AND first incremental — both legitimately have no prior
      cursor).

    Failure modes differ by mode:

    * ``mode == "seed"`` — Spark-side read failures (table missing on
      first run, transient connection blip) are SWALLOWED and the
      function returns ``(None, {})``. Seed semantics are "full
      rebuild from bronze" — no cursor needed; a benign read failure
      shouldn't fail the run.

    * ``mode == "incremental"`` — Spark-side read failures FAIL the
      run with ``StateReadFailedError``. An incremental run cannot
      proceed without verifying the prior cursor + plan hash, because
      falling through to ``(None, {})`` would silently full-scan the
      source AND skip the AIDPF-4040 drift gate. The reviewer's
      example: metastore/permission/schema error on the latest-view
      read would otherwise let the run commit despite being unable to
      verify state.

    Args:
        spark: live Spark session.
        paths: TablePaths.
        node: validated NodeYaml whose prior state we're reading.
        mode: ``"seed"`` or ``"incremental"`` — drives the
            fail-open / fail-closed decision.

    Returns:
        ``(prior_plan_hash, {source_id: prior_output_watermark})``.

    Raises:
        StateReadFailedError: ``mode == "incremental"`` AND the
            underlying Spark query raised. Carries the original
            exception as ``__cause__``.
    """
    primary_source = _resolve_primary_source_id_for_state_read(node)
    if primary_source is None:
        return None, {}, None

    try:
        # Read the latest primary-role row for this node from the
        # Content-pack latest view. The view's grain is (run_id, dataset_id,
        # layer, source_id) so we additionally filter by source_role
        # to disambiguate. ``run_id`` is read so the caller can pair this
        # node's prior plan-hash with the manifest of the run that WROTE it
        # (incremental-coa-chart-onboarding per-run pairing).
        from . import state as v1_state
        view_path = v1_state._state_latest_view_path(paths)
        df = spark.sql(
            f"SELECT plan_hash, output_watermark, source_id, status, run_id "
            f"FROM {view_path} "
            f"WHERE dataset_id = '{node.id}' AND layer = '{node.layer}' "
            f"AND source_role = 'primary' AND status = 'success' "
            f"ORDER BY last_run_at DESC LIMIT 1"
        )
        rows = df.collect()
    except Exception as exc:  # noqa: BLE001 — re-wrap based on mode
        if mode == "incremental":
            # Fail closed — caller cannot verify prior cursor / plan hash.
            # Use the existing StateReadFailedError class (same shape v1
            # preflight uses); operators see a consistent diagnostic
            # regardless of which backend triggered the failure.
            from . import state as v1_state
            raise StateReadFailedError(
                dataset_id=node.id,
                layer=node.layer,
                table_path=v1_state._state_latest_view_path(paths),
                cause=exc,
            ) from exc
        # Seed mode — table-missing on first run is benign; fall through.
        return None, {}, None

    if not rows:
        return None, {}, None

    row = rows[0]
    # Spark Row supports both attribute and index access; use index
    # for resilience to fake-Spark tuples used in unit tests.
    try:
        plan_hash = row["plan_hash"]
        output_watermark = row["output_watermark"]
    except (KeyError, TypeError):
        try:
            plan_hash, output_watermark = row[0], row[1]
        except (IndexError, TypeError):
            return None, {}, None
    # prior_run_id is best-effort (only used by the additive-COA fast path);
    # a fake-Spark tuple without it degrades to None (feature inactive).
    try:
        prior_run_id = row["run_id"]
    except (KeyError, TypeError, IndexError):
        try:
            prior_run_id = row[4]
        except (KeyError, IndexError, TypeError):
            prior_run_id = None

    prior_watermark = {primary_source: output_watermark} if output_watermark is not None else {}
    return plan_hash, prior_watermark, prior_run_id


def _resolve_primary_source_id_for_state_read(node: "Any") -> "str | None":
    """Mirror sql_runner._resolve_primary_source_id (kept private here to
    avoid a cross-module import cycle into the dispatcher)."""
    inc = node.refresh.incremental
    if inc is not None and inc.watermark is not None:
        return inc.watermark.source
    deps = getattr(node, "depends_on", None)
    if deps and deps.bronze:
        return deps.bronze[0].id
    return None


__all__ = [
    "run",
    "RunStep",
    "RunSummary",
    "ExternalDep",
    # Exception re-exports for `_run_inline`'s catch clause + downstream callers
    "OrchestratorConfigError",
    "BundleLoadError",
    "BundleVersionMismatchError",
    "UnsupportedModeError",
    "MissingDependencyError",
    "PrerequisiteError",
    "CredentialResolutionError",
    "BronzeSchemaProbeError",
    # Incremental config errors
    "IncrementalCursorMissingError",
    "MultipleNaturalKeyError",
    # Dropped-target preflight + strict state read
    "IncrementalTargetMissingError",
    "StateReadFailedError",
    # Runtime errors
    "OrchestratorRuntimeError",
    "WatermarkMonotonicityError",
    "MultipleUpstreamWatermarkError",
    # Bronze MERGE payload-diff predicate
    "BRONZE_AUDIT_COLUMNS",
    # Schema evolution under MERGE
    "SchemaEvolutionTypeConflictError",
    "SchemaReconcileResult",
]
