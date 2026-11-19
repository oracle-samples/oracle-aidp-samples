"""Per-node preflight for the content-pack runner.

Preflight runs in :func:`sql_runner.execute_node` *after* static schema
validation and *before* SQL rendering. Its job is to fail fast on
runtime-only conditions that the static validator can't see:

* Required columns missing in the live bronze schema.
* Watermark column missing in the source bronze (for merge strategy).
* Partition columns missing on the target (validate-only for deferred
  ``replace_partition`` strategy).

**Crucial ordering invariant**: preflight does NOT render SQL. The
renderer is invoked exactly once in ``execute_node``, *after*
this preflight passes. This separation is what makes the
``preflight-blocked`` unit test branch assert "renderer never called".

Preflight covers required-column, watermark-column, and partition-column
gates only; SQL rendering and execution stay in ``sql_runner``.
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from pydantic import ValidationError

from ..schema.coa_roles import SUPPORTED_COA_ROLES, coa_role_aliases
from ..schema.medallion_pack import ChartOfAccountsProfile, NodeYaml
from . import coa_gate
from .required_column_resolver import coa_role_union

_log = logging.getLogger(__name__)

_COA_REF_PREFIX = "$coa."
_SEMANTIC_REF_PREFIX = "$semantic."

AIDPF_5001_IDENTIFIER_ALLOWLIST = "AIDPF-5001"
"""A COA role column from the tenant profile fails the SQL identifier allowlist.
Mirrors ``sql_renderer._check_identifier`` so a bad/hand-edited
``profile.chartOfAccounts`` value is blocked BEFORE it reaches probe SQL."""

# Same allowlist as orchestrator.sql_renderer._IDENTIFIER_RE.
_COA_IDENT_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]{0,62}$")

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

    from ..schema.tenant_profile import TenantProfile
    from .content_pack import ResolvedPack
    from .sql_renderer import RunContext


# ---------------------------------------------------------------------------
# AIDPF error codes
# ---------------------------------------------------------------------------

AIDPF_2042_REQUIRED_COLUMN_MISSING = "AIDPF-2042"
"""Required column declared in ``node.requiredColumns.<source>`` is absent
from the live bronze ``DESCRIBE TABLE`` schema."""

AIDPF_2043_WATERMARK_COLUMN_MISSING = "AIDPF-2043"
"""Watermark column declared in ``node.refresh.incremental.watermark.column``
is absent from the source bronze schema (merge strategy)."""

AIDPF_2044_PARTITION_COLUMN_MISSING = "AIDPF-2044"
"""``replace_partition`` strategy partition column missing on target
(deferred strategy; validate-only)."""

AIDPF_2046_REQUIRED_COLUMN_UNRESOLVED_REF = "AIDPF-2046"
"""A ``requiredColumns`` entry uses the ``$column.<key>`` reference
syntax but the key is either (a) not declared in ``pack.yaml``'s
``columnAliases``, or (b) declared but missing from the tenant profile's
``resolved.column`` map (bootstrap not run, or alias was added after
last bootstrap)."""

AIDPF_2013_STRUCTURAL_COA = "AIDPF-2013"
"""The tenant profile's ``chartOfAccounts`` is MISSING / EMPTY / structurally
INVALID while an in-scope node consumes a COA source. A hard, pre-extraction
block (NOT a no-op, NOT ``allowUnprovableCOA``-eligible — the hatch covers only
a probe that cannot EXECUTE, never a malformed/absent mapping). The shape
contract is enforced by parsing through ``ChartOfAccountsProfile`` (accepts both
the flat/legacy and nested ``default`` shapes), plus numeric ``byChart`` keys and
role completeness."""

AIDPF_2074_COA_UNPROVABLE = "AIDPF-2074"
"""A COA correctness PROBE could not EXECUTE (e.g. a constrained Spark session),
so COA correctness is UNPROVEN. Blocks by default; downgrades to a loud WARN only
when ``contentPack.allowUnprovableCOA: true`` AND no COA VIOLATION was retained."""

# byChart keys must be numeric chart_of_accounts_id values — mirrors
# sql_renderer._COA_CHART_ID_RE so an invalid key is rejected pre-extraction
# rather than at render time.
_COA_CHART_ID_RE = re.compile(r"^[0-9]{1,18}$")


_COLUMN_REF_PREFIX = "$column."
"""YAML prefix marking a ``requiredColumns`` entry as a reference to a
``columnAliases.<key>`` resolved value in the tenant profile, rather
than a literal column name. Backward-compatible: entries without the
prefix are still treated as literals."""


# ---------------------------------------------------------------------------
# Preflight result
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class PreflightError:
    """One preflight failure for a node.

    Attributes:
        code: AIDPF error code (e.g. ``AIDPF-2042``).
        source: bronze/silver source id where the failure was detected,
            or ``None`` for target-level checks.
        message: human-readable diagnostic naming the missing column /
            source / live bronze schema for triage.
    """

    code: str
    source: str | None
    message: str


@dataclass(frozen=True)
class PreflightReport:
    """Aggregated preflight result for a node.

    Attributes:
        errors: tuple of :class:`PreflightError` — blocking failures.
            Non-empty → :func:`execute_node` writes a
            ``status='preflight_blocked'`` soft state row and returns
            failure WITHOUT invoking the renderer.
        ok: convenience boolean — ``True`` iff ``errors`` is empty.
    """

    errors: tuple[PreflightError, ...] = ()

    @property
    def ok(self) -> bool:
        return not self.errors


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def preflight_node(
    spark: "SparkSession",
    node: NodeYaml,
    pack: "ResolvedPack",  # noqa: F821 — forward ref
    profile: "TenantProfile",  # noqa: F821
    ctx: "RunContext",  # noqa: F821
) -> PreflightReport:
    """Run per-node preflight against the live Spark catalog.

    Performs metadata + bronze ``DESCRIBE TABLE`` introspection only.
    **Does NOT render SQL** — the renderer runs separately in
    :func:`execute_node` step 3, *after* this preflight passes.

    Args:
        spark: live Spark session for ``DESCRIBE TABLE`` calls.
        node: validated NodeYaml.
        pack: assembled ResolvedPack (consulted for source-id → table
            mapping when ctx doesn't already carry it).
        profile: validated TenantProfile (used for column-alias resolution).
        ctx: render context — used for ``bronze_table_for_source`` map
            (which the introspection calls use to identify the live
            bronze tables to DESCRIBE).

    Returns:
        :class:`PreflightReport` carrying any collected errors. Empty
        ``errors`` → preflight passed and ``execute_node`` may proceed
        to render the SQL.

    Notes:
        Does NOT raise on errors — collects them so the report-row writer
        in ``execute_node`` can record the full diagnostic. Programmer-
        error conditions (missing live table entirely, Spark session
        broken) still raise — the caller treats those as a different
        failure class than "expected column missing on this tenant".
    """
    errors: list[PreflightError] = []

    # bronze_extract nodes CREATE their target table from the live PVO —
    # they don't read a pre-existing bronze table. The checks below all
    # `DESCRIBE` the node's bronze table, which doesn't exist yet on a
    # first-ever seed (or after a drop) and would raise an uncaught
    # AnalysisException. The bronze source is validated against the PVO by
    # the AIDPF-4071 source gate + the post-write AIDPF-4070 assertion, so
    # there's nothing for table-introspection preflight to do here.
    if getattr(node.implementation, "type", None) == "bronze_extract":
        return PreflightReport(errors=())

    # 1. Required columns on each declared source.
    errors.extend(_check_required_columns(spark, node, pack, profile, ctx))

    # 2. Watermark column for merge-strategy nodes.
    if _is_merge_strategy(node):
        errors.extend(_check_watermark_column(spark, node, ctx))

    # 3. Partition columns for replace_partition (deferred strategy —
    #    schema accepts it but execution path doesn't run in v0.3; we
    #    still validate the partition shape so an early customer who
    #    declares one gets a useful diagnostic, not a NotImplementedError
    #    much later in execute_node).
    if _is_replace_partition_strategy(node):
        errors.extend(_check_partition_columns(spark, node, ctx))

    # 4. COA semantic-role plausibility + multi-COA gate (feature
    #    coa-role-segment-resolution M2). No-ops unless the pack declares COA
    #    semanticRole aliases on a bronze source this node consumes.
    errors.extend(_check_coa_gate(spark, node, pack, profile, ctx))

    return PreflightReport(errors=tuple(errors))


# ---------------------------------------------------------------------------
# Internals
# ---------------------------------------------------------------------------


def _is_merge_strategy(node: NodeYaml) -> bool:
    inc = node.refresh.incremental
    return inc is not None and inc.strategy == "merge"


def _is_replace_partition_strategy(node: NodeYaml) -> bool:
    inc = node.refresh.incremental
    return inc is not None and inc.strategy == "replace_partition"


def _check_required_columns(
    spark: "SparkSession",
    node: NodeYaml,
    pack: "ResolvedPack",  # noqa: F821
    profile: "TenantProfile",  # noqa: F821
    ctx: "RunContext",
) -> list[PreflightError]:
    """For each entry in ``node.requiredColumns.<source>`` that names a **bronze**
    source, DESCRIBE the source's live bronze table and assert the column exists.

    The ``requiredColumns`` map is keyed by source id (matching
    ``dependsOn.bronze[*].id`` / ``dependsOn.silver[*].id``). Each value is a list
    of column names that MUST be present. Entries beginning with ``$column.`` are
    references into ``pack.columnAliases`` — resolved against the tenant profile's
    ``resolved.column`` map before the live-column check.

    **Bronze-only live gate.** This live-DESCRIBE check exists to catch drift in
    Fusion-extracted **bronze** tables — schemas the pack does not control. A
    silver/gold dependency source (e.g. a gold mart requiring columns from
    ``dim_supplier``) is *pack-built*: its schema is the producer node's own
    declared ``outputSchema``, already gated design-time by AIDPF-2045
    (``requiredColumns`` ⊆ ``outputSchema``, with type-compat) and at runtime by
    the producer's AIDPF-4070/4071 materialization gate. Re-DESCRIBE-ing it here
    would be redundant — and ``ctx.bronze_table_for_source`` only carries bronze
    sources, so a live probe isn't even available. A source is treated as
    pack-built (and skipped) iff it is a declared ``pack.silver`` / ``pack.gold``
    node; everything else gets the live DESCRIBE, including a bronze source
    declared only via the legacy ``bronze.yaml`` datasets path.
    """
    errors: list[PreflightError] = []
    required = getattr(node, "required_columns", None) or {}
    pack_alias_keys = set(pack.pack.column_aliases.keys())

    for source_id, required_cols in required.items():
        # Skip silver/gold dependency sources — pack-built, gated by
        # AIDPF-2045 (static) + the producer's AIDPF-4070/4071 (runtime). The
        # discriminator is positive membership in pack.silver / pack.gold (NOT
        # "absent from pack.bronze"): a bronze source declared only via the
        # legacy bronze.yaml datasets path lives in ctx.bronze_table_for_source
        # but not in pack.bronze, and must still get the live-drift DESCRIBE.
        if source_id in pack.silver or source_id in pack.gold:
            continue
        table = ctx.bronze_table_for_source.get(source_id)
        if table is None:
            errors.append(
                PreflightError(
                    code=AIDPF_2042_REQUIRED_COLUMN_MISSING,
                    source=source_id,
                    message=(
                        f"required-columns preflight could not find a bronze table "
                        f"identifier for source {source_id!r} in "
                        f"ctx.bronze_table_for_source. Confirm the source is "
                        f"declared in bundle.yaml + bronze.yaml."
                    ),
                )
            )
            continue
        present = _describe_columns(spark, table)
        present_ci = {c.lower(): c for c in present}
        for entry in required_cols:
            # $coa.<role> expands to the UNION of that role's columns across
            # default + byChart arms; every one must be present in bronze.
            if entry.startswith(_COA_REF_PREFIX):
                union = coa_role_union(entry[len(_COA_REF_PREFIX) :], profile)
                if not union:
                    errors.append(
                        PreflightError(
                            code=AIDPF_2046_REQUIRED_COLUMN_UNRESOLVED_REF,
                            source=source_id,
                            message=(
                                f"requiredColumns entry {entry!r} could not resolve to "
                                f"any column: the tenant profile has no "
                                f"`chartOfAccounts` mapping for this role. Run "
                                f"`aidp-fusion-autopilot bootstrap`."
                            ),
                        )
                    )
                    continue
                for col in sorted(union):
                    if col.lower() not in present_ci:
                        errors.append(
                            PreflightError(
                                code=AIDPF_2042_REQUIRED_COLUMN_MISSING,
                                source=source_id,
                                message=(
                                    f"COA column {col!r} (resolved from {entry!r}) "
                                    f"missing from live bronze for source {source_id!r} "
                                    f"(table {table!r}). Extend the gl_coa bronze "
                                    f"contract / re-seed bronze."
                                ),
                            )
                        )
                continue
            # $semantic.<key> resolves to the physical column the ACTIVE
            # semanticVariants candidate detects (detect.columnExists), via
            # profile.resolved.semantic — same semantics as the run-level
            # resolve_required_column_entries, so the per-node gate doesn't treat
            # it as a literal and false-fail AIDPF-2042.
            if entry.startswith(_SEMANTIC_REF_PREFIX):
                key = entry[len(_SEMANTIC_REF_PREFIX) :]
                variant = pack.pack.semantic_variants.get(key)
                cand_id = (getattr(profile.resolved, "semantic", None) or {}).get(key)
                sem_col = None
                if variant is not None and cand_id:
                    for cand in variant.candidates:
                        if cand.id == cand_id and cand.detect and cand.detect.column_exists:
                            sem_col = cand.detect.column_exists
                            break
                if sem_col is None:
                    errors.append(
                        PreflightError(
                            code=AIDPF_2046_REQUIRED_COLUMN_UNRESOLVED_REF,
                            source=source_id,
                            message=(
                                f"requiredColumns entry {entry!r} could not resolve: "
                                f"semanticVariants key {key!r} has no active candidate "
                                f"in the tenant profile's `resolved.semantic`. Run "
                                f"`aidp-fusion-autopilot bootstrap`."
                            ),
                        )
                    )
                    continue
                if sem_col.lower() not in present_ci:
                    errors.append(
                        PreflightError(
                            code=AIDPF_2042_REQUIRED_COLUMN_MISSING,
                            source=source_id,
                            message=(
                                f"semantic column {sem_col!r} (resolved from {entry!r}) "
                                f"missing from live bronze for source {source_id!r} "
                                f"(table {table!r}). Live columns: {sorted(present)!r}."
                            ),
                        )
                    )
                continue
            resolved, ref_error = _resolve_required_column_entry(
                entry, profile, source_id, pack_alias_keys
            )
            if ref_error is not None:
                errors.append(ref_error)
                continue
            assert resolved is not None  # mypy: ref_error None ⇒ resolved set
            if resolved.lower() not in present_ci:
                # Diagnostic names BOTH the YAML entry (for traceability
                # back to the pack source) and the resolved physical
                # column (for "go look in DESCRIBE"). When `entry` is a
                # literal these are the same — the message stays terse.
                resolved_hint = (
                    f" (resolved from {entry!r})" if entry != resolved else ""
                )
                errors.append(
                    PreflightError(
                        code=AIDPF_2042_REQUIRED_COLUMN_MISSING,
                        source=source_id,
                        message=(
                            f"required column {resolved!r}{resolved_hint} missing "
                            f"from live bronze schema for source {source_id!r} "
                            f"(table {table!r}). Live columns: {sorted(present)!r}."
                        ),
                    )
                )
    return errors


def _resolve_required_column_entry(
    entry: str,
    profile: "TenantProfile",  # noqa: F821
    source_id: str,
    pack_alias_keys: set[str],
) -> tuple[str | None, PreflightError | None]:
    """Resolve a ``requiredColumns`` entry to a physical column name.

    Returns ``(resolved, None)`` on success, or ``(None, error)`` when
    the entry uses ``$column.<key>`` syntax but the key cannot be
    resolved. A literal entry (no prefix) returns ``(entry, None)``
    unchanged — backward-compatible with v0.3 packs.
    """
    if not entry.startswith(_COLUMN_REF_PREFIX):
        return entry, None
    key = entry[len(_COLUMN_REF_PREFIX) :]
    if key not in pack_alias_keys:
        return None, PreflightError(
            code=AIDPF_2046_REQUIRED_COLUMN_UNRESOLVED_REF,
            source=source_id,
            message=(
                f"requiredColumns entry {entry!r} references columnAlias key "
                f"{key!r} which is not declared in pack.yaml's `columnAliases`. "
                f"Known keys: {sorted(pack_alias_keys)!r}. "
                f"Fix the pack YAML — either declare the alias or use a literal "
                f"column name."
            ),
        )
    resolved = profile.resolved.column.get(key)
    if not resolved:
        return None, PreflightError(
            code=AIDPF_2046_REQUIRED_COLUMN_UNRESOLVED_REF,
            source=source_id,
            message=(
                f"requiredColumns entry {entry!r} references columnAlias key "
                f"{key!r} declared in pack.yaml, but the tenant profile has no "
                f"resolved value for it. Re-run `aidp-fusion-autopilot bootstrap` "
                f"to populate the profile."
            ),
        )
    return resolved, None


def _check_watermark_column(
    spark: "SparkSession", node: NodeYaml, ctx: "RunContext"
) -> list[PreflightError]:
    """For merge-strategy nodes, confirm the declared watermark column
    exists in the source bronze schema. Missing → AIDPF-2043.
    """
    inc = node.refresh.incremental
    if inc is None or inc.watermark is None:
        # Static validator should have rejected merge without watermark
        # config (AIDPF-2050); defensive check anyway.
        return []
    source_id = inc.watermark.source
    column = inc.watermark.column
    table = ctx.bronze_table_for_source.get(source_id)
    if table is None:
        return [
            PreflightError(
                code=AIDPF_2043_WATERMARK_COLUMN_MISSING,
                source=source_id,
                message=(
                    f"merge-strategy watermark preflight could not find a "
                    f"bronze table for source {source_id!r}. Confirm the source "
                    f"appears in ctx.bronze_table_for_source."
                ),
            )
        ]
    present = _describe_columns(spark, table)
    present_ci = {c.lower(): c for c in present}
    if column.lower() not in present_ci:
        return [
            PreflightError(
                code=AIDPF_2043_WATERMARK_COLUMN_MISSING,
                source=source_id,
                message=(
                    f"merge-strategy watermark column {column!r} missing from "
                    f"source {source_id!r} (table {table!r}). Live columns: "
                    f"{sorted(present)!r}."
                ),
            )
        ]
    return []


def _check_partition_columns(
    spark: "SparkSession", node: NodeYaml, ctx: "RunContext"
) -> list[PreflightError]:
    """For replace_partition strategy, confirm partition columns exist
    on the target. v0.3 doesn't execute this strategy, but we run the
    check so customers declaring it get an early diagnostic.
    """
    inc = node.refresh.incremental
    if inc is None:
        return []
    if not inc.partition_columns:
        # Static validator (R6) should have rejected this; defensive.
        return [
            PreflightError(
                code=AIDPF_2044_PARTITION_COLUMN_MISSING,
                source=None,
                message=(
                    f"replace_partition strategy declared without "
                    f"`partitionColumns` (AIDPF-2054). Static "
                    f"validator should reject this earlier."
                ),
            )
        ]
    # v0.3: target table may not exist yet on first run; we don't fail
    # closed here — just record a soft warning that the runtime check
    # will repeat once the target materialises.
    return []


_COA_DISCRIMINANT = "CodeCombinationChartOfAccountsId"
_COA_ACCOUNT_TYPE = "CodeCombinationAccountType"
_COA_ENABLED_FLAG = "CodeCombinationEnabledFlag"


# The COA role filter now lives in the neutral schema layer
# (``schema.coa_roles``) so ``schema.plan_resolver``'s dry-run COA-first
# ordering can share ONE definition without importing this engine-side module
# (which would break the dispatch import boundary). Re-exported here under the
# module-private names this file's COA gate already uses.
_SUPPORTED_COA_ROLES = SUPPORTED_COA_ROLES
_coa_role_aliases = coa_role_aliases


def _node_consumes_source(node: NodeYaml, source_id: str) -> bool:
    deps = getattr(node, "depends_on", None)
    if deps is None:
        return False
    for dep in (getattr(deps, "bronze", None) or []):
        if dep.id == source_id:
            return True
    return False


@dataclass(frozen=True)
class CoaEvalResult:
    """Structured outcome of :func:`_evaluate_coa`.

    Separating retained VIOLATIONS from probe-EXECUTION failures is the key
    correctness property: a raise in a LATER probe can never discard a violation
    an EARLIER probe already found (each probe runs in its own ``try/except``).
    The caller dispositions in a fixed order — violations block ALWAYS; a
    probe-execution failure blocks with AIDPF-2074 UNLESS ``allowUnprovableCOA``.
    """

    violations: list[PreflightError] = field(default_factory=list)
    probe_failures: list[str] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return not self.violations and not self.probe_failures


def _coa_roles_needed(pack: "ResolvedPack", source_id: str) -> set[str]:
    """The COA role tokens the pack declares against ``source_id``."""
    return {
        role
        for (role, src) in _coa_role_aliases(pack).values()
        if src == source_id
    }


def _normalize_coa_structure(
    coa_raw: object, roles_needed: set[str], source_id: str
) -> list[PreflightError]:
    """Structural COA gate (AIDPF-2013) — profile-structural, needs NO landed data.

    Runs FIRST and HARD whenever an in-scope node consumes a COA source. Reuses
    the ``ChartOfAccountsProfile`` model (accepts BOTH the flat/legacy and nested
    ``default`` shapes; rejects the mixed form, an incomplete arm, and
    ``byChart`` without an effective default) and layers on the extra strictness
    the model does not itself enforce (numeric ``byChart`` keys pre-extraction,
    an effective default present, role completeness for the pack's declared
    roles). Any failure → a single AIDPF-2013 (never a no-op, never
    ``allowUnprovableCOA``-eligible). Returns ``[]`` when the mapping is sound.
    """

    def _fail(detail: str) -> list[PreflightError]:
        return [
            PreflightError(
                code=AIDPF_2013_STRUCTURAL_COA,
                source=source_id,
                message=(
                    f"{AIDPF_2013_STRUCTURAL_COA}: `profile.chartOfAccounts` is "
                    f"invalid for the COA-consuming source {source_id!r}: {detail} "
                    f"Declare a complete `chartOfAccounts` (flat "
                    f"`balancing/costCenter/naturalAccountSegment` OR a nested "
                    f"`default` mapping, optional numeric `byChart` arms) — run "
                    f"`bootstrap` to resolve COA roles."
                ),
            )
        ]

    if not isinstance(coa_raw, dict):
        return _fail("the mapping is missing or not a dict.")
    if not coa_raw:
        return _fail("the mapping is empty.")
    try:
        coa = ChartOfAccountsProfile.model_validate(coa_raw)
    except ValidationError as exc:
        return _fail(f"it does not match the chartOfAccounts contract ({exc}).")

    default = coa.resolved_default()
    if default is None:
        return _fail("no effective `default` role mapping is present.")

    # Numeric byChart keys — rejected HERE, pre-extraction (today only caught at
    # render, sql_renderer.py). The model permits arbitrary string keys.
    for chart_id in (coa.by_chart or {}):
        if not _COA_CHART_ID_RE.match(str(chart_id)):
            return _fail(
                f"byChart key {chart_id!r} is not a valid numeric "
                f"chart_of_accounts_id."
            )

    # Role completeness: every arm must supply the roles the pack declares. The
    # model already requires all three role columns per arm, so this only bites
    # if roles_needed ever names a role outside the model's fixed set.
    for arm_id, mapping in coa.arms().items():
        cols = {f"coa.{k}": v for k, v in mapping.columns().items()}
        missing = sorted(r for r in roles_needed if not cols.get(r))
        if missing:
            return _fail(f"arm {arm_id!r} is missing role(s) {missing!r}.")

    return []


def _check_coa_gate(
    spark: "SparkSession",
    node: NodeYaml,
    pack: "ResolvedPack",  # noqa: F821
    profile: "TenantProfile",  # noqa: F821
    ctx: "RunContext",
) -> list[PreflightError]:
    """COA gate BACKSTOP in per-node preflight (defense in depth).

    The structural gate (AIDPF-2013) now runs FIRST and HARD — replacing the old
    ``return []`` no-op on a missing/empty/invalid mapping. When the structure is
    sound, the shared :func:`_evaluate_coa` runs the full ordered sequence
    (5001 → 2016 → 2042 → 2018 → 2017) and this backstop returns its retained
    VIOLATIONS as blocking errors while a probe-EXECUTION failure downgrades to a
    logged WARN (legacy behaviour, retained as defense-in-depth — the
    pre-extraction COA CHECKPOINT is the layer that hard-blocks on an unprovable
    probe / applies ``allowUnprovableCOA``).
    """
    aliases = _coa_role_aliases(pack)
    if not aliases:
        return []
    # All COA roles in the starter share one source (gl_coa). Group by source.
    sources = {src for (_role, src) in aliases.values()}
    coa_raw = (profile.profile or {}).get("chartOfAccounts")

    errors: list[PreflightError] = []
    for source_id in sources:
        if not _node_consumes_source(node, source_id):
            continue

        roles_needed = _coa_roles_needed(pack, source_id)
        # Structural gate FIRST — a missing/empty/invalid mapping hard-blocks
        # (AIDPF-2013), and we do NOT probe a malformed mapping.
        structural = _normalize_coa_structure(coa_raw, roles_needed, source_id)
        if structural:
            errors.extend(structural)
            continue

        table = ctx.bronze_table_for_source.get(source_id)
        if table is None:
            continue

        result = _evaluate_coa(spark, table, source_id, coa_raw, roles_needed)
        errors.extend(result.violations)
        for detail in result.probe_failures:
            _log.warning(
                "COA data probe on %s skipped (%s); structural checks still applied.",
                table,
                detail,
            )
    return errors


def _coa_arms(coa_raw: dict) -> dict[str, dict[str, str]]:
    """Normalise a profile chartOfAccounts dict to {arm_id: {role_token: col}}."""

    def _mapping(block: dict) -> dict[str, str]:
        return {
            role: block[alias]
            for role, alias in (
                ("coa.balancing", "balancingSegment"),
                ("coa.cost_center", "costCenterSegment"),
                ("coa.natural_account", "naturalAccountSegment"),
            )
            if alias in block and block[alias] is not None
        }

    arms: dict[str, dict[str, str]] = {}
    default_block = coa_raw.get("default", coa_raw)
    default = _mapping(default_block)
    if default:
        arms["default"] = default
    for chart_id, block in (coa_raw.get("byChart") or {}).items():
        arms[str(chart_id)] = _mapping(block)
    return arms


def _coa_chart_active(spark: "SparkSession", table: str) -> dict[str, int]:
    """Active (enabled) gl_coa row count per chart_of_accounts_id."""
    rows = spark.sql(
        f"SELECT CAST({_COA_DISCRIMINANT} AS STRING) AS chart_id, COUNT(*) AS n "
        f"FROM {table} "
        f"WHERE {_COA_DISCRIMINANT} IS NOT NULL "
        f"AND COALESCE({_COA_ENABLED_FLAG}, 'Y') <> 'N' "
        f"GROUP BY CAST({_COA_DISCRIMINANT} AS STRING)"
    ).collect()
    return {str(r[0]): int(r[1]) for r in rows if r[0] is not None}


def _evaluate_coa(
    spark: "SparkSession",
    table: str,
    source_id: str,
    coa_raw: dict,
    roles_needed: set[str],
) -> CoaEvalResult:
    """Run the COMPLETE ordered COA gate against a LANDED ``gl_coa`` table and
    return a structured :class:`CoaEvalResult`.

    Assumes the structural gate (:func:`_normalize_coa_structure`, AIDPF-2013)
    already passed, so ``coa_raw`` is well-formed. The ordered sequence:

    1. **AIDPF-5001** identifier allowlist (pure; a HARD SECURITY BOUNDARY, runs
       FIRST — if any identifier fails, the data probes that interpolate columns
       into SQL MUST NOT run).
    2. **AIDPF-2016** distinctness (pure).
    3. **AIDPF-2042** existence union (DESCRIBE — own ``try``).
    4. **AIDPF-2018** multi-COA + completeness (data probe — own ``try``).
    5. **AIDPF-2017** Tier-B natural-account contradiction (data probe — each
       per-chart query in its OWN ``try``).

    Every probe runs in its own ``try/except`` so a raise in a LATER probe cannot
    discard a VIOLATION an earlier probe accumulated (the multi-COA 2018 result
    is retained even if a per-chart Tier-B 2017 query then raises). Violations
    land in ``violations``; each probe-execution failure lands in
    ``probe_failures`` — the caller dispositions them.
    """
    violations: list[PreflightError] = []
    probe_failures: list[str] = []
    arms = _coa_arms(coa_raw)

    # 1. AIDPF-5001 — identifier allowlist (pure, FIRST, hard security boundary).
    ident_ok = True
    for arm_id, mapping in arms.items():
        for role, col in mapping.items():
            if not _COA_IDENT_RE.match(col or ""):
                ident_ok = False
                violations.append(
                    PreflightError(
                        code=AIDPF_5001_IDENTIFIER_ALLOWLIST,
                        source=source_id,
                        message=(
                            f"COA mapping for arm {arm_id!r} role {role!r} resolves "
                            f"to {col!r}, which fails the identifier allowlist "
                            f"`^[A-Za-z_][A-Za-z0-9_]{{0,62}}$`. Fix "
                            f"`profile.chartOfAccounts` — a COA role must bind a "
                            f"plain column name."
                        ),
                    )
                )

    # 2. AIDPF-2016 — distinctness (pure).
    for code, msg in coa_gate.check_distinctness(arms):
        violations.append(PreflightError(code=code, source=source_id, message=msg))

    # 3. AIDPF-2042 — existence union (DESCRIBE; own try so a describe failure is
    # a probe_failure, not a lost violation).
    try:
        referenced = {c for m in arms.values() for c in m.values()}
        present = _describe_columns(spark, table)
        for code, msg in coa_gate.check_existence_union(referenced, present):
            violations.append(
                PreflightError(code=code, source=source_id, message=msg)
            )
    except Exception as exc:  # pragma: no cover — constrained-session guard
        probe_failures.append(f"existence-union DESCRIBE on {table}: {exc}")

    # Data probes (4–5) interpolate COA columns into SQL — run ONLY when every
    # identifier passed the allowlist (matching the renderer's contract).
    if not ident_ok:
        return CoaEvalResult(violations=violations, probe_failures=probe_failures)

    # 4. AIDPF-2018 — multi-COA + completeness (own try; retained even if a later
    # Tier-B probe raises).
    chart_active: dict[str, int] | None = None
    try:
        chart_active = _coa_chart_active(spark, table)
        by_chart = coa_raw.get("byChart") or {}
        has_by_chart = bool(by_chart)
        # Strict-bool consumption (never `bool("false")`): the structural gate
        # guarantees a native bool, so anything other than True is False.
        singleton_accepted = coa_raw.get("singletonAccepted") is True
        for code, msg in coa_gate.check_multi_coa(
            chart_active,
            singleton_accepted=singleton_accepted,
            has_by_chart=has_by_chart,
        ):
            violations.append(
                PreflightError(code=code, source=source_id, message=msg)
            )
        # L3.4 completeness: when byChart is declared, every present (active)
        # chart must have an arm — an unmapped chart would hit CASE ELSE
        # raise_error at render time.
        if has_by_chart:
            mapped = {str(k) for k in by_chart}
            for chart_id, n in chart_active.items():
                if (
                    n >= coa_gate.MULTI_COA_MIN_ACTIVE_ROWS
                    and chart_id not in mapped
                ):
                    violations.append(
                        PreflightError(
                            code=coa_gate.AIDPF_2018_MULTI_COA_UNCONFIGURED,
                            source=source_id,
                            message=(
                                f"chart_of_accounts_id {chart_id!r} has active "
                                f"gl_coa rows but no `chartOfAccounts.byChart` arm. "
                                f"Declare it (the rendered CASE would otherwise "
                                f"raise at runtime)."
                            ),
                        )
                    )
    except Exception as exc:  # pragma: no cover — constrained-session guard
        probe_failures.append(f"multi-COA probe on {table}: {exc}")

    # 5. AIDPF-2017 — Tier-B natural-account contradiction, per chart. Each query
    # in its OWN try so one chart's failure neither drops other charts' findings
    # nor the retained 2018 violations above.
    if chart_active is not None:
        for chart_id, active_rows in chart_active.items():
            mapping = arms.get(chart_id) or arms.get("default") or {}
            na_col = mapping.get("coa.natural_account")
            if not na_col:
                continue
            try:
                agg = spark.sql(
                    f"SELECT "
                    f"COUNT(*) AS total, "
                    f"SUM(CASE WHEN t > 1 THEN 1 ELSE 0 END) AS ambiguous "
                    f"FROM (SELECT {na_col} AS na, "
                    f"COUNT(DISTINCT {_COA_ACCOUNT_TYPE}) AS t "
                    f"FROM {table} "
                    f"WHERE CAST({_COA_DISCRIMINANT} AS STRING) = '{chart_id}' "
                    f"AND {na_col} IS NOT NULL "
                    f"AND COALESCE({_COA_ENABLED_FLAG}, 'Y') <> 'N' "
                    f"GROUP BY {na_col})"
                ).collect()
            except Exception as exc:  # pragma: no cover — constrained-session
                probe_failures.append(
                    f"Tier-B natural-account probe on {table} chart {chart_id}: {exc}"
                )
                continue
            if not agg:
                continue
            total = int(agg[0][0] or 0)
            ambiguous = int(agg[0][1] or 0)
            probe = coa_gate.ChartProbe(
                chart_id=chart_id,
                active_row_count=active_rows,
                natural_account_distinct=total,
                natural_account_ambiguous=ambiguous,
            )
            res = coa_gate.check_natural_account(probe)
            for code, msg in res.errors:
                violations.append(
                    PreflightError(code=code, source=source_id, message=msg)
                )
            for warning in res.warnings:
                _log.warning("COA gate: %s", warning)

    return CoaEvalResult(violations=violations, probe_failures=probe_failures)


# ---------------------------------------------------------------------------
# Pre-extraction / in-loop COA checkpoint (fail-fast-seed-validation)
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class CoaCheckpointResult:
    """Disposition of a COA checkpoint (structural gate + data probes).

    ``blocking`` holds hard-block errors (AIDPF-2013 structural, or a retained
    5001/2016/2042/2018/2017 violation, or AIDPF-2074 for an unprovable probe
    when the hatch is off). ``warnings`` holds probe-execution failures that
    were downgraded because ``allowUnprovableCOA`` is set AND no violation was
    retained. The caller aborts the run iff ``blocking`` is non-empty.
    """

    blocking: list[PreflightError] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return not self.blocking


def coa_applicable_sources(
    pack: "ResolvedPack", plan_nodes: "list[NodeYaml]"
) -> set[str]:
    """COA source ids consumed by an in-scope silver/gold node in ``plan_nodes``.

    Empty when the pack declares no COA ``semanticRole`` aliases, or no in-scope
    mart consumes a COA source — in which case the COA gate is inert (the whole
    checkpoint no-ops). This is the shared applicability guard used by the
    pre-extraction gate, the in-loop checkpoint, and the mart-only checkpoint.
    """
    aliases = _coa_role_aliases(pack)
    if not aliases:
        return set()
    coa_sources = {src for (_role, src) in aliases.values()}
    consumed: set[str] = set()
    for node in plan_nodes:
        if getattr(node, "layer", None) not in ("silver", "gold"):
            continue
        for src in coa_sources:
            if _node_consumes_source(node, src):
                consumed.add(src)
    return consumed


def order_coa_source_first(
    plan: "list[NodeYaml]", coa_sources: set[str]
) -> "list[NodeYaml]":
    """Return ``plan`` reordered so COA-source bronze nodes run FIRST.

    Bronze nodes are independent leaves, so hoisting the COA source(s) to the
    front of the plan preserves every other dependency edge (bronze still
    precedes silver/gold; relative order within each group is kept). This makes
    the pre-extraction COA gate a true gate for everything it guards — no
    expensive PVO is extracted until COA is proven. Used by BOTH the exec loop
    and the dry-run plan builder so the previewed order matches execution.
    """
    if not coa_sources:
        return list(plan)
    coa_first = [n for n in plan if n.layer == "bronze" and n.id in coa_sources]
    if not coa_first:
        return list(plan)
    rest = [
        n for n in plan
        if not (n.layer == "bronze" and n.id in coa_sources)
    ]
    return coa_first + rest


def split_landed_coa_sources(
    coa_sources: set[str], *, mart_only: bool, succeeded: "frozenset[str] | set[str]"
) -> "tuple[set[str], set[str]]":
    """Split COA sources into (already-LANDED, still-PENDING) for the
    pre-extraction checkpoint.

    A COA source is already MATERIALIZED at pre-extraction time — so the in-loop
    data checkpoint (row 4b) will NOT fire for it — when either the run is
    mart-only (all bronze skipped) or this is a resume and the source already
    succeeded (it will be resumed-skipped). Those sources MUST get the FULL
    landed-data checkpoint pre-extraction; a source that will still land in-loop
    only needs the structural gate now (its data probes run at 4b). Without this
    split, a run originally aborted at the in-loop checkpoint (AIDPF-2018/2074)
    could resume straight into the expensive bronze with COA unproven.
    """
    landed = {s for s in coa_sources if mart_only or s in succeeded}
    return landed, coa_sources - landed


def evaluate_coa_checkpoint(
    spark: "SparkSession",
    *,
    pack: "ResolvedPack",
    profile: "TenantProfile",
    bronze_table_for_source: dict[str, str],
    coa_sources: set[str],
    allow_unprovable: bool,
    structural_only: bool = False,
) -> CoaCheckpointResult:
    """Run the COA checkpoint over ``coa_sources`` and disposition the outcome.

    Order (per source), with the disposition the hatch cannot reorder:

    1. **Structural gate** (:func:`_normalize_coa_structure`, AIDPF-2013) — runs
       ALWAYS, needs no landed data; a malformed/absent mapping hard-blocks and
       the data probes do NOT run for that source. NEVER hatch-eligible.
    2. If ``structural_only`` (the pre-extraction gate, before ``gl_coa`` lands)
       → stop after the structural gate.
    3. Else :func:`_evaluate_coa` against the LANDED table, then disposition:
       - ``violations`` non-empty → **HARD BLOCK ALWAYS** (regardless of the
         hatch, and even if a probe ALSO failed);
       - else ``probe_failures`` non-empty → **AIDPF-2074** unless
         ``allow_unprovable`` downgrades it to a WARN.
    """
    coa_raw = (profile.profile or {}).get("chartOfAccounts")
    blocking: list[PreflightError] = []
    warnings: list[str] = []

    for source_id in sorted(coa_sources):
        roles_needed = _coa_roles_needed(pack, source_id)

        # 1. Structural gate FIRST — never hatch-eligible.
        structural = _normalize_coa_structure(coa_raw, roles_needed, source_id)
        if structural:
            blocking.extend(structural)
            continue  # do not probe a malformed mapping

        if structural_only:
            continue

        table = bronze_table_for_source.get(source_id)
        if table is None:
            # No landed table to probe (should not happen post-landing / on a
            # mart-only run where the readiness gate confirmed it). Treat an
            # absent table as an unprovable probe rather than silently passing.
            detail = f"COA source {source_id!r} has no landed table to probe."
            if allow_unprovable:
                warnings.append(detail)
            else:
                blocking.append(
                    PreflightError(
                        code=AIDPF_2074_COA_UNPROVABLE,
                        source=source_id,
                        message=f"{AIDPF_2074_COA_UNPROVABLE}: {detail}",
                    )
                )
            continue

        result = _evaluate_coa(spark, table, source_id, coa_raw, roles_needed)
        # Disposition step 1 — a retained VIOLATION blocks ALWAYS.
        if result.violations:
            blocking.extend(result.violations)
            continue
        # Disposition step 2 — probe-execution failure → 2074 unless the hatch.
        if result.probe_failures:
            joined = "; ".join(result.probe_failures)
            if allow_unprovable:
                warnings.append(
                    f"COA correctness UNPROVEN on {source_id!r} "
                    f"(allowUnprovableCOA): {joined}"
                )
            else:
                blocking.append(
                    PreflightError(
                        code=AIDPF_2074_COA_UNPROVABLE,
                        source=source_id,
                        message=(
                            f"{AIDPF_2074_COA_UNPROVABLE}: COA correctness could not "
                            f"be proven for {source_id!r} — a probe failed to "
                            f"execute ({joined}). Set "
                            f"`contentPack.allowUnprovableCOA: true` to proceed "
                            f"with a logged WARN (correctness then rests on the "
                            f"per-node backstop)."
                        ),
                    )
                )

    return CoaCheckpointResult(blocking=blocking, warnings=warnings)


def _describe_columns(spark: "SparkSession", table: str) -> set[str]:
    """Return the set of column names from ``DESCRIBE TABLE <table>``.

    Filters out partition-info metadata rows that ``DESCRIBE TABLE``
    emits in some Spark versions (rows whose ``col_name`` starts with
    ``#``).
    """
    df = spark.sql(f"DESCRIBE TABLE {table}")
    rows = df.collect() if df is not None else []
    out: set[str] = set()
    for row in rows:
        # DESCRIBE TABLE returns col_name / data_type / comment.
        # Access via index 0 (works for both Row and tuple mocks).
        try:
            name = row[0]
        except (IndexError, TypeError):
            continue
        if not isinstance(name, str):
            continue
        if name.startswith("#") or name == "":
            continue
        out.add(name)
    return out
