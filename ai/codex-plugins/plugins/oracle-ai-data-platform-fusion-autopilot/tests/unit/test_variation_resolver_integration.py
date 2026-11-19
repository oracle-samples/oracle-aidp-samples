"""Integration test for bronze probe + walker against the starter pack.

Feeds a mocked Spark whose DESCRIBE returns the demo-pod fixture
columns; asserts the walker resolves all seven currently-declared
variation points to the values in
``examples/profiles/finance-default.yaml``.

This is the round-trip contract: bootstrap (Step 8) MUST produce the
same `resolved.column.*` / `resolved.semantic.*` map on a clean
demo-pod fixture as the hand-authored gold reference.
"""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

import pytest

from oracle_ai_data_platform_fusion_autopilot.commands.bronze_probe import (
    describe_bronze,
)
from oracle_ai_data_platform_fusion_autopilot.commands.variation_resolver import (
    AutoResolved,
    MultiMatch,
    walk_column_alias,
    walk_semantic_variant,
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


# demo-pod / finance-default fixture: the column set bootstrap will
# observe on a conventional-Fusion tenant. Mirrors what the starter pack
# expects in `pack.yaml` columnAliases + semanticVariants.
_DEMO_POD_BRONZE: dict[str, list[str]] = {
    "erp_suppliers": [
        "VENDORID",
        "SEGMENT1",
        # CodeCombination, AmountValue, etc. — but only the columns the
        # walker references matter.
    ],
    "ap_invoices": [
        "ApInvoicesInvoiceCurrencyCode",
        # NB: ApInvoicesCurrencyCode is ALSO present on demo-pod — this
        # makes invoice_currency_code a multi-match case. For the
        # AutoResolved integration test we omit it; the multi-match
        # case lives in `test_starter_pack_multi_match_currency`.
        "ApInvoicesCancelledDate",
    ],
    "gl_coa": [
        "CodeCombinationSegment1",
        "CodeCombinationSegment2",
        "CodeCombinationSegment3",
        "CodeCombinationSegment4",
        "CodeCombinationSegment5",
        "CodeCombinationSegment6",
    ],
}


def _row(col_name: str, data_type: str = "string"):
    return {"col_name": col_name, "data_type": data_type, "comment": None}


def _mock_spark(per_table_columns: dict[str, list[str]]) -> MagicMock:
    spark = MagicMock(name="spark")

    def _sql(query: str):
        target = query.split()[-1]
        dataset = target.split(".")[-1]
        cols = per_table_columns.get(dataset, [])
        df = MagicMock(name=f"df_{dataset}")
        df.collect.return_value = [_row(c) for c in cols]
        return df

    spark.sql.side_effect = _sql
    return spark


class TestDemoPodFixtureResolvesAllStarterVariationPoints:
    """Every starter-pack variation point must resolve to the value
    `examples/profiles/finance-default.yaml` pins."""

    def test_all_seven_variation_points_resolved(self) -> None:
        pack = load_pack(PACK_ROOT)
        spark = _mock_spark(_DEMO_POD_BRONZE)
        observed = describe_bronze(
            spark,
            catalog="cat",
            bronze_schema="bronze",
            dataset_ids=list(_DEMO_POD_BRONZE.keys()),
        )

        def cols_for(applies_to: str) -> set[str]:
            # appliesTo = "bronze.<dataset>" → dataset.
            dataset = applies_to.split(".", 1)[1]
            return {c.name for c in observed[dataset]}

        # 3 existence-based columnAliases. The 3 COA roles are now
        # `resolution: semanticRole` and resolved from explicit
        # profile.chartOfAccounts config via the COA ladder, NOT by column
        # existence — so they are skipped here (see test_coa_resolution.py).
        resolutions: dict[str, str] = {}
        for name, spec in pack.pack.column_aliases.items():
            if spec.resolution == "semanticRole":
                # Existence cannot prove a business role; over the full
                # Segment1..6 allowed domain the walk would MultiMatch.
                assert isinstance(
                    walk_column_alias(spec, cols_for(spec.appliesTo)), MultiMatch
                )
                continue
            result = walk_column_alias(spec, cols_for(spec.appliesTo))
            assert isinstance(result, AutoResolved), (
                f"variation point {name!r} did not auto-resolve on demo-pod "
                f"fixture (got {type(result).__name__})"
            )
            resolutions[name] = result.chosen

        # 1 semantic variant.
        for name, spec in pack.pack.semantic_variants.items():
            result = walk_semantic_variant(spec, cols_for(spec.appliesTo))
            assert isinstance(result, AutoResolved), (
                f"semantic variant {name!r} did not auto-resolve on "
                f"demo-pod fixture (got {type(result).__name__})"
            )
            resolutions[name] = result.chosen

        # Round-trip target — the existence-resolved values (COA roles are
        # resolved separately by the COA ladder).
        assert resolutions == {
            "supplier_natural_key": "SEGMENT1",
            "vendor_id": "VENDORID",
            "invoice_currency_code": "ApInvoicesInvoiceCurrencyCode",
            "cancelled_status": "cancelled_date",
        }


class TestStarterPackMultiMatchCurrency:
    """When BOTH currency-code candidates are present (the realistic
    demo-pod case), bootstrap must surface a MultiMatch outcome so
    the operator picks. The auto-resolve test above strips the second
    candidate to isolate the round-trip; this test re-adds it."""

    def test_invoice_currency_code_is_multi_match_with_both_columns(
        self,
    ) -> None:
        from oracle_ai_data_platform_fusion_autopilot.commands.variation_resolver import (
            MultiMatch,
        )

        bronze = dict(_DEMO_POD_BRONZE)
        bronze["ap_invoices"] = bronze["ap_invoices"] + ["ApInvoicesCurrencyCode"]
        spark = _mock_spark(bronze)
        observed = describe_bronze(
            spark,
            catalog="cat",
            bronze_schema="bronze",
            dataset_ids=["ap_invoices"],
        )

        pack = load_pack(PACK_ROOT)
        spec = pack.pack.column_aliases["invoice_currency_code"]
        result = walk_column_alias(spec, {c.name for c in observed["ap_invoices"]})

        assert isinstance(result, MultiMatch)
        # Priority order from pack.yaml: long-form name first.
        assert result.matched == [
            "ApInvoicesInvoiceCurrencyCode",
            "ApInvoicesCurrencyCode",
        ]


class TestStarterPackNoMatchOnMissingColumn:
    """Non-conventional tenant — synthesise a tenant missing VENDORID.
    Walker returns NoMatch; bootstrap (Step 8) maps to AIDPF-2010."""

    def test_no_match_on_missing_vendor_id(self) -> None:
        from oracle_ai_data_platform_fusion_autopilot.commands.variation_resolver import (
            NoMatch,
        )

        bronze = dict(_DEMO_POD_BRONZE)
        bronze["erp_suppliers"] = ["SEGMENT1"]  # VENDORID dropped
        spark = _mock_spark(bronze)
        observed = describe_bronze(
            spark,
            catalog="cat",
            bronze_schema="bronze",
            dataset_ids=["erp_suppliers"],
        )

        pack = load_pack(PACK_ROOT)
        spec = pack.pack.column_aliases["vendor_id"]
        result = walk_column_alias(spec, {c.name for c in observed["erp_suppliers"]})

        assert isinstance(result, NoMatch)
        assert [a.candidate for a in result.candidates_tried] == ["VENDORID"]
        assert result.candidates_tried[0].outcome == "column_not_found"
