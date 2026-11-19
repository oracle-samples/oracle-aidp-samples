"""Unit tests for the pure-Python candidate walker
(:mod:`oracle_ai_data_platform_fusion_autopilot.commands.variation_resolver`).

Every outcome × every kind:

* 1 candidate, exists → :class:`AutoResolved`.
* 1 candidate, missing → :class:`NoMatch`.
* 3 candidates, only middle one exists → :class:`AutoResolved`.
* 3 candidates, 2 exist → :class:`MultiMatch` (priority order
  preserved).
* 3 candidates, none exist → :class:`NoMatch`.
* SemanticVariant ``detect.columnExists`` parallels.

Mock-free; the walker takes a ``set[str]`` of observed columns.
"""

from __future__ import annotations

import pytest

from oracle_ai_data_platform_fusion_autopilot.commands.variation_resolver import (
    AutoResolved,
    MultiMatch,
    NoMatch,
    walk_column_alias,
    walk_semantic_variant,
)
from oracle_ai_data_platform_fusion_autopilot.schema.medallion_pack import (
    ColumnAlias,
    SemanticVariant,
    SemanticVariantCandidate,
    SemanticVariantDetect,
)


def _ca(candidates: list[str]) -> ColumnAlias:
    return ColumnAlias(appliesTo="bronze.ap_invoices", required=True, candidates=candidates)


def _sv(candidates: list[tuple[str, str]]) -> SemanticVariant:
    """Build a semantic variant; each tuple is ``(id, detect_column)``."""
    return SemanticVariant(
        appliesTo="bronze.ap_invoices",
        required=True,
        candidates=[
            SemanticVariantCandidate(
                id=cid,
                detect=SemanticVariantDetect(columnExists=col),
                fragment=f"{{table}}.{col} IS NULL",
            )
            for cid, col in candidates
        ],
    )


class TestColumnAliasWalker:
    def test_single_candidate_exists_auto_resolves(self) -> None:
        result = walk_column_alias(_ca(["VENDORID"]), {"VENDORID", "SEGMENT1"})
        assert isinstance(result, AutoResolved)
        assert result.chosen == "VENDORID"

    def test_single_candidate_missing_no_match(self) -> None:
        result = walk_column_alias(_ca(["VENDORID"]), {"SEGMENT1"})
        assert isinstance(result, NoMatch)
        assert len(result.candidates_tried) == 1
        assert result.candidates_tried[0].candidate == "VENDORID"
        assert result.candidates_tried[0].outcome == "column_not_found"

    def test_three_candidates_only_middle_exists(self) -> None:
        result = walk_column_alias(
            _ca(["First", "Middle", "Last"]),
            {"Middle"},
        )
        assert isinstance(result, AutoResolved)
        assert result.chosen == "Middle"

    def test_three_candidates_two_exist_multi_match_preserves_priority(
        self,
    ) -> None:
        result = walk_column_alias(
            _ca(["ApInvoicesInvoiceCurrencyCode", "ApInvoicesCurrencyCode", "Stale"]),
            {"ApInvoicesInvoiceCurrencyCode", "ApInvoicesCurrencyCode"},
        )
        assert isinstance(result, MultiMatch)
        # Priority order: declared order, not observed-set order.
        assert result.matched == [
            "ApInvoicesInvoiceCurrencyCode",
            "ApInvoicesCurrencyCode",
        ]

    def test_three_candidates_none_exist_no_match(self) -> None:
        result = walk_column_alias(
            _ca(["A", "B", "C"]),
            {"X", "Y"},
        )
        assert isinstance(result, NoMatch)
        attempts = [a.candidate for a in result.candidates_tried]
        assert attempts == ["A", "B", "C"]
        assert all(a.outcome == "column_not_found" for a in result.candidates_tried)

    def test_walker_is_case_insensitive(self) -> None:
        """Fusion BICC ships UPPERCASE; spark connectors often lowercase.
        The walker normalises both sides."""
        result = walk_column_alias(_ca(["VENDORID"]), {"vendorid"})
        assert isinstance(result, AutoResolved)
        assert result.chosen == "VENDORID"

    def test_walker_never_raises(self) -> None:
        """Walker is pure — outcome is a return value, not an exception.
        Bootstrap is responsible for mapping outcomes to AIDPF codes."""
        # Empty observed set + empty-ish — still returns NoMatch.
        result = walk_column_alias(_ca(["X"]), set())
        assert isinstance(result, NoMatch)


class TestSemanticVariantWalker:
    def test_single_candidate_detect_exists(self) -> None:
        result = walk_semantic_variant(
            _sv([("cancelled_date", "ApInvoicesCancelledDate")]),
            {"ApInvoicesCancelledDate"},
        )
        assert isinstance(result, AutoResolved)
        # NB: returns the candidate ID, not the detect column name.
        assert result.chosen == "cancelled_date"

    def test_single_candidate_detect_missing(self) -> None:
        result = walk_semantic_variant(
            _sv([("cancelled_date", "ApInvoicesCancelledDate")]),
            {"SomeOtherColumn"},
        )
        assert isinstance(result, NoMatch)
        assert result.candidates_tried[0].outcome == "detect_clause_failed"
        # Detail includes the detect column for skill consumption.
        assert "ApInvoicesCancelledDate" in (result.candidates_tried[0].detail or "")

    def test_two_candidates_first_matches(self) -> None:
        result = walk_semantic_variant(
            _sv(
                [
                    ("cancelled_date", "ApInvoicesCancelledDate"),
                    ("cancelled_flag", "ApInvoicesCancelledFlag"),
                ]
            ),
            {"ApInvoicesCancelledDate"},
        )
        assert isinstance(result, AutoResolved)
        assert result.chosen == "cancelled_date"

    def test_two_candidates_both_match_multi_match(self) -> None:
        result = walk_semantic_variant(
            _sv(
                [
                    ("cancelled_date", "ApInvoicesCancelledDate"),
                    ("cancelled_flag", "ApInvoicesCancelledFlag"),
                ]
            ),
            {"ApInvoicesCancelledDate", "ApInvoicesCancelledFlag"},
        )
        assert isinstance(result, MultiMatch)
        assert result.matched == ["cancelled_date", "cancelled_flag"]

    def test_two_candidates_none_match(self) -> None:
        result = walk_semantic_variant(
            _sv(
                [
                    ("cancelled_date", "ApInvoicesCancelledDate"),
                    ("cancelled_flag", "ApInvoicesCancelledFlag"),
                ]
            ),
            {"UnrelatedColumn"},
        )
        assert isinstance(result, NoMatch)
        assert [a.candidate for a in result.candidates_tried] == [
            "cancelled_date",
            "cancelled_flag",
        ]
