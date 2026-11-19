"""Required-column alias resolver shared by run-level + per-node preflight gates.

``requiredColumns`` entries in a content-pack node may be one of two
shapes:

* A **literal** column name like ``ApInvoicesVendorId`` — used as-is.
* A ``$column.<key>`` **reference** like ``$column.invoice_currency_code``
  — resolved against ``pack.column_aliases.<key>`` (which lists the
  candidate physical column names per tenant) and the tenant profile's
  ``resolved.column[<key>]`` (which records which candidate was picked
  at bootstrap).

The per-node preflight at :mod:`orchestrator.node_preflight` already
resolves these references before checking the live bronze schema.
Without an equivalent step in the run-level gates
(:mod:`orchestrator.bronze_readiness` for AIDPF-2071,
:mod:`orchestrator.fusion_pvo_drift` for AIDPF-2072), those gates
compare literal ``$column.*`` strings against live source columns
and false-fail the run for any pack that uses the alias syntax — the
starter pack at ``content_packs/fusion-finance-starter/`` does, so
this is on the default-flipped path.

This module factors the resolver into one helper so both gates can
reuse it without duplicating the ``$column.`` prefix logic.

The per-node equivalent is
:func:`orchestrator.node_preflight._resolve_required_column_entry`.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:  # pragma: no cover
    from ..schema.tenant_profile import TenantProfile
    from .content_pack import ResolvedPack


_COLUMN_REF_PREFIX = "$column."
"""Same constant as :data:`orchestrator.node_preflight._COLUMN_REF_PREFIX`;
duplicated here so the run-level gates don't import the per-node
preflight module (which has heavier Spark imports)."""

_SEMANTIC_REF_PREFIX = "$semantic."
"""A ``$semantic.<key>`` reference resolves to the physical column the active
``semanticVariants.<key>`` candidate detects (``detect.columnExists``), via the
tenant profile's ``resolved.semantic[<key>]`` pick. So a ``{{ semantic.<key> }}``
read declared as ``$semantic.<key>`` flows into the live required-column gates."""

_COA_REF_PREFIX = "$coa."
"""A ``$coa.<role>`` reference expands to the UNION of that COA role's column
across ``profile.chartOfAccounts.default`` + every ``byChart`` arm. The union
(not a single column) is what the run-level / preflight gates must validate,
because the rendered ``{{ coa.<role> }}`` CASE can reference any arm's column."""

_COA_ROLE_FIELD = {
    "balancing": "balancingSegment",
    "cost_center": "costCenterSegment",
    "natural_account": "naturalAccountSegment",
}


def coa_role_union(role: str, tenant_profile: "TenantProfile | None") -> set[str]:
    """Union of physical columns bound to ``role`` across default + byChart arms."""
    if tenant_profile is None:
        return set()
    coa = (getattr(tenant_profile, "profile", None) or {}).get("chartOfAccounts")
    if not isinstance(coa, dict):
        return set()
    field = _COA_ROLE_FIELD.get(role)
    if field is None:
        return set()
    cols: set[str] = set()
    default = coa.get("default", coa)
    if isinstance(default, dict) and isinstance(default.get(field), str):
        cols.add(default[field])
    for arm in (coa.get("byChart") or {}).values():
        if isinstance(arm, dict) and isinstance(arm.get(field), str):
            cols.add(arm[field])
    return cols


def resolve_required_column_entries(
    entries: "list[str] | set[str] | tuple[str, ...]",
    *,
    resolved_pack: "ResolvedPack | None",
    tenant_profile: "TenantProfile | None",
) -> set[str]:
    """Resolve a list of ``requiredColumns`` entries to physical column names.

    For each entry:

    * Literal (no prefix) → kept as-is.
    * ``$column.<key>`` and ``<key>`` is in both ``pack.column_aliases``
      AND ``profile.resolved.column`` → use the resolved value.
    * ``$column.<key>`` but the key cannot be resolved → **drop silently**.

    The "drop silently" branch is deliberate. The run-level gates run
    BEFORE the per-node preflight; per-node preflight is the canonical
    place to surface AIDPF-2046 (``$column.*`` reference unresolved).
    Re-raising here would either (a) duplicate the AIDPF-2046
    diagnostic, or (b) consume an alias-resolution failure as a fake
    AIDPF-2071/2072 "column missing in live bronze" which obscures the
    real cause. Drop the unresolvable entry and let per-node preflight
    issue the proper AIDPF-2046 diagnostic when the run actually
    dispatches the affected node.

    When ``resolved_pack`` or ``tenant_profile`` is ``None``, every
    ``$column.*`` entry is dropped — the resolver has nothing to look
    in. Literals pass through. This is the "bronze-only run with no
    cp scope" path: AIDPF-2072 still wants the literal cols, just not
    the alias refs.

    Args:
        entries: the raw ``requiredColumns`` value list as it appears
            on a ``NodeYaml``.
        resolved_pack: the loaded pack (for ``column_aliases`` keys).
            ``None`` when no pack is in scope.
        tenant_profile: the loaded tenant profile (for
            ``resolved.column`` values). ``None`` when no profile is
            in scope.

    Returns:
        Set of resolved physical column names, with ``$column.*`` refs
        substituted via the profile's pinned value where possible.
        Literals pass through verbatim.
    """
    resolved: set[str] = set()
    pack_alias_keys: set[str] = set()
    if resolved_pack is not None:
        # Guard a missing / non-dict ``column_aliases`` (malformed pack, or a
        # mock pack in tests) — treat it as "no aliases" rather than crashing.
        # Literal entries still pass through; ``$column.*`` refs then drop.
        aliases = getattr(getattr(resolved_pack, "pack", None), "column_aliases", None)
        if isinstance(aliases, dict):
            pack_alias_keys = set(aliases.keys())

    for entry in entries:
        if entry.startswith(_COA_REF_PREFIX):
            # $coa.<role> → union of that role's columns across all arms.
            # Drop silently when unresolvable (same rationale as $column.*).
            resolved.update(coa_role_union(entry[len(_COA_REF_PREFIX):], tenant_profile))
            continue
        if entry.startswith(_SEMANTIC_REF_PREFIX):
            # $semantic.<key> → the active candidate's detect.columnExists column.
            # Drop silently when unresolvable (per-node gate is authoritative).
            if tenant_profile is None or resolved_pack is None:
                continue
            key = entry[len(_SEMANTIC_REF_PREFIX):]
            variants = getattr(getattr(resolved_pack, "pack", None), "semantic_variants", None)
            variant = variants.get(key) if isinstance(variants, dict) else None
            cand_id = (getattr(tenant_profile.resolved, "semantic", None) or {}).get(key)
            if variant is None or not cand_id:
                continue
            for cand in variant.candidates:
                if cand.id == cand_id and cand.detect and cand.detect.column_exists:
                    resolved.add(cand.detect.column_exists)
                    break
            continue
        if not entry.startswith(_COLUMN_REF_PREFIX):
            resolved.add(entry)
            continue
        if tenant_profile is None or resolved_pack is None:
            continue
        key = entry[len(_COLUMN_REF_PREFIX):]
        if key not in pack_alias_keys:
            continue
        physical = tenant_profile.resolved.column.get(key)
        if not physical:
            continue
        resolved.add(physical)

    return resolved


__all__ = ["resolve_required_column_entries", "coa_role_union"]
