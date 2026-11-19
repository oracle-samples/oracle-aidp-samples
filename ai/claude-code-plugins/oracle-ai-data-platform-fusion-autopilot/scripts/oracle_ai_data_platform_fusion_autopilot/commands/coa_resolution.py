"""Pure-Python COA semantic-role resolution ladder.

COA role segments (``coa.balancing`` / ``coa.cost_center`` /
``coa.natural_account``) are NOT resolved by physical column existence -- on
Fusion every ``CodeCombinationSegment{N}`` coexists, so existence cannot prove
*which* segment carries a business role. They are resolved from explicit tenant
configuration via this ladder, with honest per-role provenance (never
``auto_resolve``).

This module is the algorithm only -- no Spark, no I/O. Bootstrap composes it:
it gathers the inputs (existing profile, explicit config, pack defaults, flags),
calls :func:`resolve_coa_roles`, and persists the result into the tenant profile
(``profile.chartOfAccounts`` canonical; ``resolved.column.coa_*`` derived;
``provenance.chartOfAccounts.roles`` per-role).

Resolution ladder (precedence), per the feature diagnostic §6e:

1. Existing tenant profile values (on ``--refresh``) -- carried forward with
   their recorded mechanism.
2. Explicit operator config / CLI -- ``config_resolved``.
3. Interactive prompt with conventional defaults shown -- ``operator_confirmed``.
4. Pack conventional defaults, only when explicitly accepted -- ``defaulted_convention``.
5. Legacy back-derivation from a pre-existing silent ``resolved.column.coa_*``
   pin -- ``legacy_unverified`` (never silently a convention) + a remediation warning.
6. Otherwise -- fail closed (:class:`CoaResolutionError`).

``metadata_resolved`` (Fusion flexfield segment qualifiers) is reserved for a
future spike and is intentionally not implemented here; nothing in this ladder
depends on it.
"""

from __future__ import annotations

from dataclasses import dataclass, field

# Canonical role tokens -> the CoaRoleMapping field that carries them.
ROLE_FIELD = {
    "coa.balancing": "balancing_segment",
    "coa.cost_center": "cost_center_segment",
    "coa.natural_account": "natural_account_segment",
}

# Canonical role token -> the alias-name suffix convention used in the pack and
# the legacy ``resolved.column`` keys (e.g. ``coa.balancing`` <-> ``coa_balancing_segment``).
ROLE_TO_LEGACY_COLUMN_KEY = {
    "coa.balancing": "coa_balancing_segment",
    "coa.cost_center": "coa_cost_center_segment",
    "coa.natural_account": "coa_natural_account_segment",
}

AIDPF_2013_COA_ROLE_UNRESOLVED = "AIDPF-2013"
"""A semanticRole COA mapping could not be resolved and the run is
non-interactive without an accepted convention -- fail closed."""


class CoaResolutionError(Exception):
    """Fail-closed COA resolution (carries an AIDPF-2013 actionable message)."""


@dataclass(frozen=True)
class CoaResolutionInput:
    """Everything the ladder needs, gathered by bootstrap."""

    # alias-name -> role token, e.g. {"coa_balancing_segment": "coa.balancing"}.
    semantic_role_aliases: dict[str, str]

    # `profile.chartOfAccounts` from an existing profile (refresh), or None.
    existing_chart_of_accounts: dict | None = None
    # `provenance.chartOfAccounts.roles` from an existing profile, or None.
    existing_role_provenance: dict | None = None
    # Legacy `resolved.column` map from an existing profile (for back-derivation).
    existing_resolved_column: dict | None = None

    # Operator-supplied chartOfAccounts (CLI / resolutions input), or None.
    explicit_config: dict | None = None
    # Pack `profiles.<active>.chartOfAccounts` default, or None.
    pack_default: dict | None = None

    interactive: bool = False
    accept_convention: bool = False
    accept_singleton: bool = False
    is_refresh: bool = False


@dataclass
class CoaResolutionResult:
    """Outputs bootstrap persists into the tenant profile."""

    # alias-name -> resolved physical column (derived; feeds resolved.column).
    column_map: dict[str, str]
    # Canonical chartOfAccounts block to persist under profile.profile.
    chart_of_accounts: dict
    # Per-role provenance: role-name -> {column, mechanism, source}.
    role_provenance: dict
    warnings: list[str] = field(default_factory=list)


def _normalise_default_mapping(coa: dict) -> dict[str, str]:
    """Extract the effective default role->column mapping from either the flat
    or nested ``chartOfAccounts`` shape. Returns {role_token: column}."""
    src = coa.get("default", coa)
    out: dict[str, str] = {}
    for role, field_alias in (
        ("coa.balancing", "balancingSegment"),
        ("coa.cost_center", "costCenterSegment"),
        ("coa.natural_account", "naturalAccountSegment"),
    ):
        if field_alias in src and src[field_alias] is not None:
            out[role] = src[field_alias]
    return out


def resolve_coa_roles(inp: CoaResolutionInput) -> CoaResolutionResult:
    """Run the ladder. Returns a :class:`CoaResolutionResult` or raises
    :class:`CoaResolutionError` (fail-closed) when no safe value exists in a
    non-interactive run.
    """
    roles_needed = sorted(set(inp.semantic_role_aliases.values()))
    if not roles_needed:
        return CoaResolutionResult(column_map={}, chart_of_accounts={}, role_provenance={})

    warnings: list[str] = []

    # --- Pick the source of the mapping, by precedence -------------------
    source: str
    mechanism: str
    mapping: dict[str, str]

    if inp.explicit_config is not None:
        # Tier 2 -- explicit config / CLI.
        mapping = _normalise_default_mapping(inp.explicit_config)
        mechanism, source = "config_resolved", "config"
        # Cross-validate against an existing pin: a disagreement is a red flag.
        if inp.existing_chart_of_accounts is not None:
            existing = _normalise_default_mapping(inp.existing_chart_of_accounts)
            conflict = {
                r: (existing[r], mapping[r])
                for r in roles_needed
                if r in existing and r in mapping and existing[r] != mapping[r]
            }
            if conflict and not inp.is_refresh:
                raise CoaResolutionError(
                    f"{AIDPF_2013_COA_ROLE_UNRESOLVED}: explicit COA config conflicts "
                    f"with the pinned profile {conflict!r}. Re-run `bootstrap --refresh` "
                    "to repin deliberately, or reconcile the values."
                )
    elif inp.is_refresh and inp.existing_chart_of_accounts is not None:
        # Tier 1 -- carry forward the existing pinned mapping + its mechanism.
        mapping = _normalise_default_mapping(inp.existing_chart_of_accounts)
        prior = inp.existing_role_provenance or {}
        # Reuse a uniform prior mechanism if present, else treat as config.
        prior_mechs = {
            (prior.get(_role_short(r), {}) or {}).get("mechanism") for r in roles_needed
        }
        prior_mechs.discard(None)
        if len(prior_mechs) == 1:
            mechanism = next(iter(prior_mechs))
        else:
            mechanism = "config_resolved"
        source = "existing_profile"
    elif (
        inp.existing_resolved_column
        and inp.existing_chart_of_accounts is None
        and any(
            ROLE_TO_LEGACY_COLUMN_KEY[r] in inp.existing_resolved_column
            for r in roles_needed
        )
    ):
        # Tier 5 -- legacy back-derivation from a silent pin. NEVER a convention.
        mapping = {}
        for r in roles_needed:
            key = ROLE_TO_LEGACY_COLUMN_KEY[r]
            if key in inp.existing_resolved_column:
                mapping[r] = inp.existing_resolved_column[key]
        if not inp.accept_convention:
            mechanism, source = "legacy_unverified", "legacy_back_derived"
            warnings.append(
                "COA roles were back-derived from a legacy silent resolution and are "
                "UNVERIFIED. Confirm `profile.chartOfAccounts` is correct for this "
                "tenant's chart of accounts and reseed affected silver/gold marts. "
                "Pass --accept-coa-convention once verified."
            )
        else:
            mechanism, source = "operator_confirmed", "legacy_accepted"
    elif inp.pack_default is not None and (inp.accept_convention or inp.interactive):
        # Tier 4 -- pack conventional default, recorded as `defaulted_convention`
        # (never `auto_resolve`). Used when the operator explicitly accepts the
        # convention, or in an interactive run (low friction for conventional
        # tenants); a non-interactive run with no accepted convention fails
        # closed (the `else` below). We do NOT claim `operator_confirmed` for an
        # interactive run unless a real confirmation occurred -- so an
        # unaccepted interactive default carries a verify-warning.
        mapping = _normalise_default_mapping(inp.pack_default)
        mechanism, source = "defaulted_convention", "pack"
        if not inp.accept_convention:
            warnings.append(
                "Using the pack's conventional COA default segment mapping. Verify "
                "it matches this tenant's chart of accounts; declare an explicit "
                "`profile.chartOfAccounts` (or pass --accept-coa-convention) to make "
                "this deliberate."
            )
    else:
        # Tier 6 -- fail closed.
        raise CoaResolutionError(
            f"{AIDPF_2013_COA_ROLE_UNRESOLVED}: no COA role configuration for "
            f"{roles_needed!r} and the run is non-interactive without an accepted "
            "convention. Declare `profile.chartOfAccounts.<role>Segment` (physical "
            "column names), or re-run interactively, or pass --accept-coa-convention "
            "to accept the pack's conventional default."
        )

    # --- Validate completeness: every needed role must have a column -----
    missing = [r for r in roles_needed if r not in mapping]
    if missing:
        raise CoaResolutionError(
            f"{AIDPF_2013_COA_ROLE_UNRESOLVED}: COA mapping (source={source}) is "
            f"missing roles {missing!r}. Every semanticRole must map to a physical "
            "column."
        )

    # --- Build outputs ----------------------------------------------------
    column_map: dict[str, str] = {}
    for alias_name, role in inp.semantic_role_aliases.items():
        column_map[alias_name] = mapping[role]

    role_provenance: dict = {}
    for role in roles_needed:
        role_provenance[_role_short(role)] = {
            "column": mapping[role],
            "mechanism": mechanism,
            "source": source,
        }

    chart_of_accounts = {
        "default": {
            "balancingSegment": mapping["coa.balancing"],
            "costCenterSegment": mapping["coa.cost_center"],
            "naturalAccountSegment": mapping["coa.natural_account"],
        }
    }
    # Carry forward any byChart arms from an explicit/existing source verbatim,
    # AND record per-chart/per-role provenance for them (honest provenance must
    # cover the chart-specific bindings, not only the default). The byChart arms
    # came from the same source/mechanism as the default mapping above.
    for src_block in (inp.explicit_config, inp.existing_chart_of_accounts):
        if src_block and "byChart" in src_block:
            by_chart = src_block["byChart"]
            chart_of_accounts["byChart"] = by_chart
            by_chart_prov: dict = {}
            for chart_id, arm in (by_chart or {}).items():
                arm_prov: dict = {}
                for role, alias in (
                    ("balancing", "balancingSegment"),
                    ("cost_center", "costCenterSegment"),
                    ("natural_account", "naturalAccountSegment"),
                ):
                    col = (arm or {}).get(alias)
                    if col is not None:
                        arm_prov[role] = {
                            "column": col,
                            "mechanism": mechanism,
                            "source": source,
                        }
                by_chart_prov[str(chart_id)] = arm_prov
            role_provenance["byChart"] = by_chart_prov
            break

    # singletonAccepted: set by --accept-singleton-coa, or carried forward.
    existing_singleton = any(
        bool((b or {}).get("singletonAccepted"))
        for b in (inp.explicit_config, inp.existing_chart_of_accounts)
    )
    if inp.accept_singleton or existing_singleton:
        chart_of_accounts["singletonAccepted"] = True

    return CoaResolutionResult(
        column_map=column_map,
        chart_of_accounts=chart_of_accounts,
        role_provenance=role_provenance,
        warnings=warnings,
    )


def _role_short(role_token: str) -> str:
    """``coa.balancing`` -> ``balancing`` (provenance key)."""
    return role_token.split(".", 1)[1] if "." in role_token else role_token


__all__ = [
    "AIDPF_2013_COA_ROLE_UNRESOLVED",
    "CoaResolutionError",
    "CoaResolutionInput",
    "CoaResolutionResult",
    "resolve_coa_roles",
    "ROLE_FIELD",
    "ROLE_TO_LEGACY_COLUMN_KEY",
]
