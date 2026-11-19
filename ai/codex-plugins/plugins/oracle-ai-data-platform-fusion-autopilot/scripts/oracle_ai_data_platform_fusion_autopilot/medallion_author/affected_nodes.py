"""VP-to-consuming-nodes map for the medallion-author skill.

Given a variation-point name + kind, returns the set of silver/gold
node IDs whose SQL templates reference the VP via
``{{ column.<name> }}`` or ``{{ semantic.<name> }}`` tokens.

Used by:

* :mod:`runbook` — Option D's drafter targets exactly these node IDs
  (rebuilding the surrogate state would touch all the right marts).
* :mod:`drafter` — the overlay's ``provenance.incrementalImpact``
  records the affected-nodes set per VP for the audit trail.

The scan operates on a :class:`ResolvedPack` (post-overlay-merge) so
overlay-added / overlay-overridden nodes are included automatically.
Builtin nodes (``implementation.type: builtin``) are skipped — they
don't reference VP tokens.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path
from typing import Literal

from ..orchestrator.content_pack import ResolvedPack
from ..schema.medallion_pack import NodeYaml, SqlImpl


@dataclass(frozen=True)
class AffectedNodes:
    """The set of silver + gold node IDs consuming a single VP.

    Qualified IDs use the ``<layer>.<node_id>`` form (e.g.
    ``silver.supplier_spend``). Bare IDs are also available for the
    CLI's ``--datasets`` filter (which takes bare node IDs).
    """

    qualified: frozenset[str]
    """``{"silver.supplier_spend", "gold.ap_aging"}`` shape — for the
    overlay's `provenance.incrementalImpact.affectedNodes` audit field."""

    silver_ids: frozenset[str]
    """Bare silver node IDs (no ``silver.`` prefix). Empty if no
    silver nodes reference the VP."""

    gold_ids: frozenset[str]
    """Bare gold node IDs (no ``gold.`` prefix). Empty if no gold
    nodes reference the VP."""

    @property
    def bare_ids(self) -> frozenset[str]:
        """Union of silver + gold bare IDs — for the CLI ``--datasets``
        filter (which takes node IDs without the layer prefix on both
        backends)."""
        return self.silver_ids | self.gold_ids


def compute_affected_nodes(
    pack: ResolvedPack,
    *,
    vp_name: str,
    kind: Literal["columnAliases", "semanticVariants"],
) -> AffectedNodes:
    """Scan ``pack``'s silver + gold SQL templates for ``{{ column.<vp_name> }}``
    or ``{{ semantic.<vp_name> }}`` tokens.

    Args:
        pack: a resolved pack (overlay chain already merged).
        vp_name: the variation-point name (e.g. ``invoice_currency_code``).
        kind: ``columnAliases`` → search for ``{{ column.<name> }}``;
            ``semanticVariants`` → ``{{ semantic.<name> }}``.

    Returns:
        :class:`AffectedNodes` with the consuming-node sets. Empty
        sets are valid (the VP is declared but no node consumes it
        — diagnostic surface for the operator).
    """
    # A COA semantic-role alias is consumed via the `{{ coa.<role> }}` token,
    # not `{{ column.<vp_name> }}` — map to the role short name.
    coa_role: str | None = None
    if kind == "columnAliases":
        spec = pack.pack.column_aliases.get(vp_name)
        role = getattr(spec, "role", None) if spec is not None else None
        if getattr(spec, "resolution", None) == "semanticRole" and isinstance(role, str):
            coa_role = role.split(".", 1)[1] if "." in role else role

    pattern = _token_pattern(kind=kind, vp_name=vp_name, coa_role=coa_role)

    silver: set[str] = set()
    gold: set[str] = set()

    for node_id, node in pack.silver.items():
        if _node_references_token(pack, node, qualified_id=f"silver/{node_id}", pattern=pattern):
            silver.add(node_id)

    for node_id, node in pack.gold.items():
        if _node_references_token(pack, node, qualified_id=f"gold/{node_id}", pattern=pattern):
            gold.add(node_id)

    qualified = frozenset(
        {f"silver.{i}" for i in silver} | {f"gold.{i}" for i in gold}
    )
    return AffectedNodes(
        qualified=qualified,
        silver_ids=frozenset(silver),
        gold_ids=frozenset(gold),
    )


def _token_pattern(
    *, kind: str, vp_name: str, coa_role: str | None = None
) -> re.Pattern[str]:
    """Build the regex matching ``{{ column.<name> }}`` /
    ``{{ semantic.<name> }}`` / ``{{ coa.<role> }}`` with arbitrary inner
    whitespace.

    When ``coa_role`` is set (a COA semantic-role alias), match the
    ``{{ coa.<role> }}`` token instead of ``{{ column.<vp_name> }}`` — that
    is how the role is actually consumed in SQL.
    """
    if coa_role is not None:
        return re.compile(r"\{\{\s*coa\." + re.escape(coa_role) + r"\s*\}\}")
    namespace = "column" if kind == "columnAliases" else "semantic"
    return re.compile(
        r"\{\{\s*" + re.escape(namespace) + r"\." + re.escape(vp_name) + r"\s*\}\}"
    )


def _node_references_token(
    pack: ResolvedPack,
    node: NodeYaml,
    *,
    qualified_id: str,
    pattern: re.Pattern[str],
) -> bool:
    """Read the node's SQL file and search for the token.

    Skips non-SQL implementations (``builtin``, ``bronze_extract``) —
    they don't render via the SQL renderer and don't use VP tokens.
    """
    if not isinstance(node.implementation, SqlImpl):
        return False

    pack_root: Path = pack.root_for(qualified_id)
    sql_path = pack_root / node.implementation.sql
    if not sql_path.is_file():
        # Pack validation should have caught this earlier — defensive
        # skip rather than raise (the skill is a best-effort scanner).
        return False

    try:
        text = sql_path.read_text(encoding="utf-8")
    except OSError:
        return False
    return bool(pattern.search(text))


__all__ = ["AffectedNodes", "compute_affected_nodes"]
