"""Unit tests for :mod:`medallion_author.reasoner` + ``knowledge_base``.

Covers the deterministic scoring + risk-classification primitives the
LLM calls. The LLM-driven part (reasoning paragraphs, candidate
shortlist selection) is not tested here — that's exercised by the E2E
manual smoke in Step 10.
"""

from __future__ import annotations

from datetime import datetime, timezone

import pytest

from oracle_ai_data_platform_fusion_autopilot.medallion_author.knowledge_base import (
    KNOWN_DELTAS_PATH,
    column_matches_pattern,
    load_known_deltas,
)
from oracle_ai_data_platform_fusion_autopilot.medallion_author.reasoner import (
    classify_incremental_risk,
    score_candidates,
)
from oracle_ai_data_platform_fusion_autopilot.schema.diagnostic_artifact import (
    AIDPF_2010_COLUMN_ALIAS_UNRESOLVED,
    CandidateProbeOutcome,
    ObservedColumn,
    VariationPointFailure,
)


@pytest.fixture(scope="module")
def kb():
    return load_known_deltas()


def _failure(
    *,
    name: str = "invoice_currency_code",
    kind: str = "columnAliases",
    tried: list[str] | None = None,
    observed: list[ObservedColumn] | None = None,
) -> VariationPointFailure:
    return VariationPointFailure(
        name=name,
        kind=kind,
        appliesTo="bronze.ap_invoices",
        candidatesTried=[
            CandidateProbeOutcome(candidate=c, outcome="column_not_found")
            for c in (tried or ["ApInvoicesInvoiceCurrencyCode", "ApInvoicesCurrencyCode"])
        ],
        observedBronzeSchema=observed or [],
    )


class TestKnowledgeBase:
    def test_seed_kb_has_three_deltas(self, kb) -> None:
        assert len(kb.deltas) == 3
        ids = {d.id for d in kb.deltas}
        assert "ap-cancel-status-cancel-date-variant" in ids
        assert "currency-code-casing-variants" in ids
        assert "coa-deeper-than-6-segments" in ids

    def test_for_variation_point_filters_by_name_and_kind(self, kb) -> None:
        deltas = kb.for_variation_point(
            name="invoice_currency_code", kind="columnAliases"
        )
        assert len(deltas) == 1
        assert deltas[0].id == "currency-code-casing-variants"

    def test_for_variation_point_supports_vp_glob(self, kb) -> None:
        # KB declares variationPoint: "coa_*_segment"; should match any.
        deltas = kb.for_variation_point(
            name="coa_balancing_segment", kind="columnAliases"
        )
        assert any(d.id == "coa-deeper-than-6-segments" for d in deltas)

    def test_pattern_alternation(self) -> None:
        assert column_matches_pattern("ApInvoicesCurrencyCode", "*CurrencyCode|*CcyCode")
        assert column_matches_pattern("ApInvoicesXCurrCode", "*CurrencyCode|*CcyCode|*XCurrCode")
        assert not column_matches_pattern("UnrelatedColumn", "*CurrencyCode|*CcyCode")


class TestScoreCandidates:
    def test_skips_already_tried_candidates(self, kb) -> None:
        failure = _failure(
            tried=["ApInvoicesInvoiceCurrencyCode"],
            observed=[ObservedColumn(name="ApInvoicesInvoiceCurrencyCode", type="string")],
        )
        proposals = score_candidates(failed_vp=failure, observed=failure.observed_bronze_schema, kb=kb)
        # Observed col == tried col → no proposal.
        assert proposals == []

    def test_kb_hint_drives_high_confidence(self, kb) -> None:
        failure = _failure(
            tried=["ApInvoicesInvoiceCurrencyCode", "ApInvoicesCurrencyCode"],
            observed=[
                ObservedColumn(name="ApInvoicesXCurrCode", type="string"),
                ObservedColumn(name="UnrelatedColumn", type="string"),
            ],
        )
        proposals = score_candidates(failed_vp=failure, observed=failure.observed_bronze_schema, kb=kb)
        # The KB's `currency-code-casing-variants` delta matches XCurrCode.
        # It should rank first with high confidence.
        assert proposals[0].candidate == "ApInvoicesXCurrCode"
        assert proposals[0].confidence == "high"
        assert proposals[0].kb_hint_id == "currency-code-casing-variants"

    def test_name_similarity_drives_proposal_without_kb_hint(self, kb) -> None:
        failure = _failure(
            name="vendor_id",
            tried=["VENDORID"],
            observed=[
                ObservedColumn(name="VENDOR_ID", type="string"),  # close
                ObservedColumn(name="Description", type="string"),  # far
            ],
        )
        proposals = score_candidates(failed_vp=failure, observed=failure.observed_bronze_schema, kb=kb)
        # VENDOR_ID is high-similarity to VENDORID; should rank first.
        assert proposals[0].candidate == "VENDOR_ID"
        # No KB hint for vendor_id.
        assert proposals[0].kb_hint_id is None

    def test_top_k_truncates(self, kb) -> None:
        failure = _failure(
            observed=[ObservedColumn(name=f"Col{i}", type="string") for i in range(10)],
        )
        proposals = score_candidates(
            failed_vp=failure,
            observed=failure.observed_bronze_schema,
            kb=kb,
            top_k=3,
        )
        assert len(proposals) <= 3

    def test_no_observed_columns_returns_empty(self, kb) -> None:
        failure = _failure(observed=[])
        proposals = score_candidates(failed_vp=failure, observed=[], kb=kb)
        assert proposals == []


class TestClassifyIncrementalRisk:
    def test_no_prior_pinned_is_rename(self, kb) -> None:
        result = classify_incremental_risk(
            prior_pinned=None,
            proposed="ApInvoicesXCurrCode",
            kb=kb,
            vp_name="invoice_currency_code",
            kind="columnAliases",
        )
        assert result == "likely-rename"

    def test_kb_match_with_rename_in_notes_is_rename(self, kb) -> None:
        # CurrencyCode → XCurrCode: KB hits both via the same delta;
        # delta notes mention "renamed". Should classify as rename.
        result = classify_incremental_risk(
            prior_pinned="ApInvoicesCurrencyCode",
            proposed="ApInvoicesXCurrCode",
            kb=kb,
            vp_name="invoice_currency_code",
            kind="columnAliases",
        )
        assert result == "likely-rename"

    def test_high_similarity_no_kb_is_rename(self, kb) -> None:
        # Both names match no KB pattern, but >= 0.7 similarity.
        result = classify_incremental_risk(
            prior_pinned="VENDORID",
            proposed="VENDOR_ID",
            kb=kb,
            vp_name="vendor_id",
            kind="columnAliases",
        )
        assert result == "likely-rename"

    def test_low_similarity_no_kb_is_different_semantics(self, kb) -> None:
        # ratio("OldColumnName", "TotallyDifferentName") is well below 0.4.
        result = classify_incremental_risk(
            prior_pinned="OldColumnName",
            proposed="TotallyDifferentSemantic",
            kb=kb,
            vp_name="some_amount",
            kind="columnAliases",
        )
        assert result == "likely-different-semantics"

    def test_short_names_unknown(self, kb) -> None:
        result = classify_incremental_risk(
            prior_pinned="AB",
            proposed="CD",
            kb=kb,
            vp_name="x",
            kind="columnAliases",
        )
        assert result == "unknown"

    def test_mid_similarity_no_kb_unknown(self, kb) -> None:
        # Similarity ~0.5 — fall in the unknown band.
        result = classify_incremental_risk(
            prior_pinned="SupplierFullName",
            proposed="VendorName",
            kb=kb,
            vp_name="supplier_name",
            kind="columnAliases",
        )
        assert result == "unknown"
