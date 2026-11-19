"""Additive-vs-mutating chart-of-accounts (COA) change classification.

Feature: incremental-coa-chart-onboarding. Pure (no Spark, no I/O) so every rule
is unit-testable. The orchestrator supplies the live ``protected_charts`` set
(charts materialised in ``dim_account``) and the prior COA baseline (from the
paired run manifest); this module decides whether an incoming COA profile change
is a safe, insert-only ADDITIVE change (a new chart) or an unsafe MUTATING change
(an existing chart's mapping moved/removed) that requires a fresh seed.

Two projections are defined here and persisted in the manifest (v2):

* ``coa_projection_of`` — the raw ``{default, byChart, singletonAccepted}`` COA
  mapping, normalised to role→column maps. This is what the classifier compares.
* ``non_coa_semantic_hash`` — an ALLOWLIST hash of the semantically-meaningful
  NON-COA profile identity, excluding volatile refresh metadata (``pinnedAt`` /
  provenance) and all COA-derived mirrors. A change here is a real profile change
  → still AIDPF-1048; a change confined to the COA runs the classifier.
"""

from __future__ import annotations

import hashlib
import json
from typing import TYPE_CHECKING, Any, Final, Iterable, Literal

if TYPE_CHECKING:  # pragma: no cover
    from ..schema.medallion_pack import ResolvedPack
    from ..schema.tenant_profile import TenantProfile


# COA role token -> profile segment field. Keep in sync with
# ``sql_renderer._COA_ROLE_FIELD``, ``required_column_resolver._COA_ROLE_FIELD``,
# and ``node_preflight._coa_arms``.
_COA_ROLE_FIELD: Final[dict[str, str]] = {
    "coa.balancing": "balancingSegment",
    "coa.cost_center": "costCenterSegment",
    "coa.natural_account": "naturalAccountSegment",
}


class _Unmapped:
    """Sentinel: a chart that the rendered SQL would send to the ``ELSE
    raise_error(...)`` arm (no mapping). It must NEVER compare equal to a real
    role→column mapping."""

    _instance: "_Unmapped | None" = None

    def __new__(cls) -> "_Unmapped":
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __repr__(self) -> str:  # pragma: no cover - debug aid
        return "UNMAPPED"


UNMAPPED: Final[_Unmapped] = _Unmapped()

CoaVerdict = Literal["identical", "additive", "mutating"]


def _role_mapping(block: Any) -> dict[str, str]:
    """Normalise one COA block (``default`` or a ``byChart`` arm) to
    ``{role_token: physical_column}`` for the roles it declares."""
    if not isinstance(block, dict):
        return {}
    return {
        role: block[field]
        for role, field in _COA_ROLE_FIELD.items()
        if isinstance(block.get(field), str)
    }


def coa_projection_of(tenant_profile: "TenantProfile") -> dict[str, Any]:
    """Extract the normalised COA projection from a tenant profile.

    Returns ``{"default": {role: col}, "byChart": {chart_id: {role: col}},
    "singletonAccepted": bool}``. Accepts both the flat/legacy shape (roles at
    the top of ``chartOfAccounts``) and the nested ``default`` shape. The raw
    ``byChart`` emptiness is preserved (an empty ``byChart`` means the renderer
    uses the bare default column, a load-bearing distinction for
    :func:`resolve_rendered`).
    """
    coa_raw = (getattr(tenant_profile, "profile", None) or {}).get("chartOfAccounts")
    if not isinstance(coa_raw, dict):
        return {"default": {}, "byChart": {}, "singletonAccepted": False}
    default_block = coa_raw.get("default", coa_raw)
    by_chart_raw = coa_raw.get("byChart") or {}
    by_chart = {
        str(chart_id): _role_mapping(block)
        for chart_id, block in by_chart_raw.items()
    }
    return {
        "default": _role_mapping(default_block),
        "byChart": by_chart,
        "singletonAccepted": bool(coa_raw.get("singletonAccepted", False)),
    }


def resolve_rendered(
    projection: dict[str, Any], chart_id: str
) -> "dict[str, str] | _Unmapped":
    """Renderer-equivalent resolution of one chart's role→column mapping.

    Mirrors ``sql_renderer._render_coa_role`` exactly:

    * no ``byChart`` at all → the default columns (bare-default render);
    * ``byChart`` contains the chart → that arm;
    * ``byChart`` present but missing the chart → :data:`UNMAPPED` (the render
      would reach ``ELSE raise_error(...)``).
    """
    by_chart = projection.get("byChart") or {}
    if not by_chart:
        return dict(projection.get("default") or {})
    c = str(chart_id)
    if c in by_chart:
        return dict(by_chart[c])
    return UNMAPPED


def classify_coa_change(
    prior: dict[str, Any],
    incoming: dict[str, Any],
    protected_charts: Iterable[str],
) -> CoaVerdict:
    """Classify a COA projection change against the materialised (protected) charts.

    ``protected_charts`` are the ``chart_of_accounts_id`` values with rows already
    in ``dim_account`` — the rows an incremental MERGE will NOT revisit. The change
    is **additive** iff every protected chart's renderer-equivalent mapping is
    byte-identical before and after (and none becomes UNMAPPED); otherwise it is
    **mutating**. ``identical`` when the projections are equal.

    New charts (not in ``protected_charts``) impose no constraint — their rows
    were never materialised, so mapping them is a first-time insert, not a
    reclassification.
    """
    for chart_id in protected_charts:
        prior_map = resolve_rendered(prior, chart_id)
        incoming_map = resolve_rendered(incoming, chart_id)
        # A protected chart that becomes UNMAPPED (arm removed) would raise at
        # render time; any change to a protected chart's mapping leaves its
        # already-materialised rows stale. Either is mutating.
        if incoming_map is UNMAPPED or incoming_map != prior_map:
            return "mutating"
    return "identical" if prior == incoming else "additive"


def projection_to_coa_dict(projection: dict[str, Any]) -> dict[str, Any]:
    """Inverse of :func:`coa_projection_of` — rebuild a raw ``chartOfAccounts``
    dict from a normalised projection so a node can be RE-RENDERED under a prior
    COA baseline (the per-node prior-equivalent plan-hash proof).

    Emits the nested ``default`` shape (the renderer accepts both flat and
    nested). Role tokens map back to profile segment fields via
    :data:`_COA_ROLE_FIELD`.
    """
    field_by_role = _COA_ROLE_FIELD

    def _block(mapping: dict[str, str]) -> dict[str, str]:
        return {
            field_by_role[role]: col
            for role, col in mapping.items()
            if role in field_by_role
        }

    coa: dict[str, Any] = {"default": _block(projection.get("default") or {})}
    by_chart = projection.get("byChart") or {}
    if by_chart:
        coa["byChart"] = {
            str(chart_id): _block(mapping) for chart_id, mapping in by_chart.items()
        }
    if projection.get("singletonAccepted"):
        coa["singletonAccepted"] = True
    return coa


def non_coa_semantic_hash(
    tenant_profile: "TenantProfile", pack: "ResolvedPack"
) -> str:
    """Allowlist hash of the semantically-meaningful NON-COA profile identity.

    INCLUDES (and only these): ``tenant``, ``bronze_schema_fingerprint``, the
    non-COA ``profile.*`` values, the non-COA ``resolved.column`` entries, and
    ``resolved.semantic``. EXCLUDES volatile refresh metadata (``pinnedAt``,
    ``runId`` / ``provenance.*``) and every COA-derived mirror
    (``profile.chartOfAccounts`` and the ``resolved.column`` entries pinned for
    the pack's COA ``semanticRole`` aliases).

    Allowlist (not denylist) so a future profile field defaults to "not part of
    the semantic identity" rather than silently tripping the non-COA drift gate.
    ``pack`` is needed to identify which ``resolved.column`` alias keys are
    COA-derived (they are keyed by alias name, not a ``coa_`` prefix).
    """
    from ..schema.coa_roles import coa_role_aliases

    coa_alias_names = set(coa_role_aliases(pack).keys())

    profile_dict = getattr(tenant_profile, "profile", None) or {}
    profile_non_coa = {
        k: v for k, v in profile_dict.items() if k != "chartOfAccounts"
    }

    resolved = getattr(tenant_profile, "resolved", None)
    resolved_column = dict(getattr(resolved, "column", {}) or {})
    resolved_column_non_coa = {
        k: v for k, v in resolved_column.items() if k not in coa_alias_names
    }
    resolved_semantic = dict(getattr(resolved, "semantic", {}) or {})

    payload = {
        "tenant": getattr(tenant_profile, "tenant", None),
        "bronze_schema_fingerprint": getattr(
            tenant_profile, "bronze_schema_fingerprint", None
        ),
        "profile_non_coa": profile_non_coa,
        "resolved_column_non_coa": resolved_column_non_coa,
        "resolved_semantic": resolved_semantic,
    }
    blob = json.dumps(payload, sort_keys=True, default=str).encode("utf-8")
    return hashlib.sha256(blob).hexdigest()


__all__ = [
    "UNMAPPED",
    "CoaVerdict",
    "classify_coa_change",
    "coa_projection_of",
    "non_coa_semantic_hash",
    "projection_to_coa_dict",
    "resolve_rendered",
]
