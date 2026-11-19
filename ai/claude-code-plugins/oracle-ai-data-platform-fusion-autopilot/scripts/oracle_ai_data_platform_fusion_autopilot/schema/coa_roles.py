"""Neutral (schema-layer) COA semantic-role helpers.

Extracts the COA ``semanticRole`` column-alias bindings from a
``ResolvedPack``. This is pure schema-layer logic — it reads only
``medallion_pack`` models — so both the engine-side
``orchestrator.node_preflight`` COA gate AND the dispatch-side
``schema.plan_resolver`` dry-run ordering can share ONE definition.

Why it lives here and not in ``orchestrator/*``: ``schema.plan_resolver``
runs on the laptop dry-run path and MUST NOT import from ``orchestrator/*``
(that pulls extractors / dimensions / transforms / the registry into
``sys.modules`` and breaks the dispatch import boundary locked by
``tests/unit/dispatch/test_imports.py``). Keeping the COA role filter in a
neutral schema module lets the resolver reuse it without crossing that line.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Final

if TYPE_CHECKING:  # pragma: no cover
    from .medallion_pack import ResolvedPack


SUPPORTED_COA_ROLES: Final[frozenset[str]] = frozenset(
    ("coa.balancing", "coa.cost_center", "coa.natural_account")
)
"""The COA semantic roles the COA gate understands. ``coa_role_aliases``
filters to these so a non-COA (or typo'd) ``semanticRole`` alias is NEVER
treated as a COA source — otherwise COA probe SQL would be interpolated
against a non-COA column. Keep in sync with ``coa_resolution.ROLE_FIELD``
and the render map in ``sql_renderer._COA_ROLE_FIELD``."""


def coa_role_aliases(pack: "ResolvedPack") -> dict[str, tuple[str, str]]:
    """Return ``{alias_name: (role_token, applies_to_source)}`` for COA
    ``semanticRole`` aliases declared in the pack. Empty when the pack has none.

    ONLY the supported ``coa.*`` roles (:data:`SUPPORTED_COA_ROLES`) are
    returned — a ``semanticRole`` alias with an unrecognised / non-COA ``role``
    is skipped, so the COA gate / probes never run against a non-COA source.
    """
    out: dict[str, tuple[str, str]] = {}
    for name, spec in pack.pack.column_aliases.items():
        resolution = getattr(spec, "resolution", "columnExistence")
        role = getattr(spec, "role", None)
        if resolution != "semanticRole" or not isinstance(role, str):
            continue
        if role not in SUPPORTED_COA_ROLES:
            continue
        applies_to = getattr(spec, "appliesTo", "")
        source = applies_to.split(".", 1)[1] if "." in applies_to else applies_to
        out[name] = (role, source)
    return out


__all__ = ["SUPPORTED_COA_ROLES", "coa_role_aliases"]
