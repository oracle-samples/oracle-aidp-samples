"""Unit tests for :mod:`medallion_author.affected_nodes`.

Runs the scanner against the real starter-pack SQL templates so the
test confirms the regex matches the actual token shapes used in
``content_packs/fusion-finance-starter/silver/*.sql`` and
``.../gold/*.sql``.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from oracle_ai_data_platform_fusion_autopilot.medallion_author.affected_nodes import (
    compute_affected_nodes,
)
from oracle_ai_data_platform_fusion_autopilot.orchestrator.content_pack import (
    load_pack,
)


REPO_ROOT = Path(__file__).resolve().parents[2]
PACK_ROOT = (
    REPO_ROOT
    / "scripts"
    / "oracle_ai_data_platform_fusion_autopilot"
    / "content_packs"
    / "fusion-finance-starter"
)


@pytest.fixture(scope="module")
def starter_pack():
    return load_pack(PACK_ROOT)


class TestAffectedNodes:
    def test_invoice_currency_code_affects_supplier_spend_and_ap_aging(
        self, starter_pack
    ) -> None:
        result = compute_affected_nodes(
            starter_pack, vp_name="invoice_currency_code", kind="columnAliases"
        )
        # invoice_currency_code is referenced by supplier_spend + ap_aging.
        assert "supplier_spend" in result.bare_ids
        assert "ap_aging" in result.bare_ids
        # Not referenced by dim_supplier or dim_account (different bronze).
        assert "dim_supplier" not in result.bare_ids
        assert "dim_account" not in result.bare_ids

    def test_qualified_ids_carry_layer_prefix(self, starter_pack) -> None:
        result = compute_affected_nodes(
            starter_pack, vp_name="invoice_currency_code", kind="columnAliases"
        )
        for q in result.qualified:
            assert q.startswith("silver.") or q.startswith("gold.")

    def test_coa_balancing_segment_affects_dim_account(self, starter_pack) -> None:
        result = compute_affected_nodes(
            starter_pack, vp_name="coa_balancing_segment", kind="columnAliases"
        )
        assert "dim_account" in result.bare_ids

    def test_cancelled_status_semantic_variant(self, starter_pack) -> None:
        result = compute_affected_nodes(
            starter_pack, vp_name="cancelled_status", kind="semanticVariants"
        )
        # ap_aging's cancelled-status detect-clause is the only consumer
        # in the starter pack.
        assert "ap_aging" in result.bare_ids

    def test_unknown_vp_name_returns_empty(self, starter_pack) -> None:
        result = compute_affected_nodes(
            starter_pack, vp_name="this_does_not_exist", kind="columnAliases"
        )
        assert result.bare_ids == frozenset()
        assert result.qualified == frozenset()

    def test_regex_special_chars_in_vp_name_handled_safely(
        self, starter_pack
    ) -> None:
        # Defensive — VP names shouldn't have regex specials but we
        # escape them anyway. This MUST NOT raise.
        result = compute_affected_nodes(
            starter_pack, vp_name="foo.*bar[evil]", kind="columnAliases"
        )
        assert result.bare_ids == frozenset()
