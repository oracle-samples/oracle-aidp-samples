"""Unit tests for :mod:`medallion_author.drafter`.

Covers:

* Happy path: columnAliases + semanticVariants overlays validate +
  write under ``overlays/<name>/``.
* ``write_resolutions`` is **conditional** — emits for MultiMatch /
  RefreshChange picks, returns None for initial AutoResolved.
* Validator catches §9.5.6 MAY-NOTs.
* Path-traversal hardening (overlay name rejection).
* Skill-evidence trail written separately from pack provenance.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest
import yaml

from oracle_ai_data_platform_fusion_autopilot.medallion_author.drafter import (
    OverlayValidationError,
    ProposedCandidate,
    draft_overlay,
    validate_overlay,
    write_overlay,
    write_resolutions,
    write_skill_evidence,
)
from oracle_ai_data_platform_fusion_autopilot.orchestrator.content_pack import (
    load_pack,
)
from oracle_ai_data_platform_fusion_autopilot.schema.incremental_impact import (
    IncrementalImpact,
    RemediationRecord,
)
from oracle_ai_data_platform_fusion_autopilot.schema.path_segment import (
    UnsafePathSegmentError,
)


REPO_ROOT = Path(__file__).resolve().parents[2]
STARTER_PACK = (
    REPO_ROOT
    / "scripts"
    / "oracle_ai_data_platform_fusion_autopilot"
    / "content_packs"
    / "fusion-finance-starter"
)


@pytest.fixture(scope="module")
def starter():
    return load_pack(STARTER_PACK)


def _basic_proposal(
    *,
    outcome: str = "AutoResolved",
    impact: IncrementalImpact | None = None,
) -> ProposedCandidate:
    return ProposedCandidate(
        vp_name="invoice_currency_code",
        kind="columnAliases",
        applies_to="bronze.ap_invoices",
        candidate="ApInvoicesXCurrCode",
        confidence="high",
        reasoning="Fusion 25C rename observed.",
        outcome=outcome,  # type: ignore[arg-type]
        incremental_impact=impact,
    )


class TestDraftOverlayHappyPath:
    def test_column_alias_overlay_validates(self, starter) -> None:
        draft = draft_overlay(
            overlay_name="test-currency",
            base_pack_id="fusion-finance-starter",
            base_pack_version="0.1.0",
            base_column_aliases=starter.pack.column_aliases,
            base_semantic_variants=starter.pack.semantic_variants,
            proposed=[_basic_proposal()],
            diagnostic_run_id="run-abc",
            model_id="claude-opus-4-7",
        )
        ca = draft.pack_yaml.column_aliases["invoice_currency_code"]
        # Inherits base candidates + appends new one.
        assert "ApInvoicesXCurrCode" in ca.candidates
        assert "ApInvoicesInvoiceCurrencyCode" in ca.candidates
        assert ca.candidates[-1] == "ApInvoicesXCurrCode"
        # Provenance fully populated.
        assert draft.pack_yaml.provenance.skill_id == "aidp-fusion-medallion-author"
        assert draft.pack_yaml.provenance.diagnostic_run_id == "run-abc"

    def test_semantic_variant_overlay_validates(self, starter) -> None:
        proposal = ProposedCandidate(
            vp_name="cancelled_status",
            kind="semanticVariants",
            applies_to="bronze.ap_invoices",
            candidate="cancelled_short",
            detect_column="ApInvoicesCancelDate",
            fragment="{table}.ApInvoicesCancelDate IS NULL",
            confidence="high",
            reasoning="Fusion 22D shortened name variant.",
        )
        draft = draft_overlay(
            overlay_name="test-cancel-short",
            base_pack_id="fusion-finance-starter",
            base_pack_version="0.1.0",
            base_column_aliases=starter.pack.column_aliases,
            base_semantic_variants=starter.pack.semantic_variants,
            proposed=[proposal],
            diagnostic_run_id="run-abc",
            model_id="claude-opus-4-7",
        )
        sv = draft.pack_yaml.semantic_variants["cancelled_status"]
        assert any(c.id == "cancelled_short" for c in sv.candidates)
        # Base candidates preserved.
        assert any(c.id == "cancelled_date" for c in sv.candidates)


class TestValidateOverlay:
    def test_semantic_proposal_missing_detect_rejected(self, starter) -> None:
        bad = ProposedCandidate(
            vp_name="cancelled_status",
            kind="semanticVariants",
            applies_to="bronze.ap_invoices",
            candidate="cancelled_x",
            detect_column=None,  # missing!
            fragment=None,
        )
        with pytest.raises(OverlayValidationError, match="detect_column"):
            draft_overlay(
                overlay_name="test-bad",
                base_pack_id="fusion-finance-starter",
                base_pack_version="0.1.0",
                base_column_aliases=starter.pack.column_aliases,
                base_semantic_variants=starter.pack.semantic_variants,
                proposed=[bad],
                diagnostic_run_id="run-abc",
                model_id="claude-opus-4-7",
            )

    def test_empty_proposals_rejected(self, starter) -> None:
        with pytest.raises(OverlayValidationError, match="nothing to draft"):
            draft_overlay(
                overlay_name="test-empty",
                base_pack_id="fusion-finance-starter",
                base_pack_version="0.1.0",
                base_column_aliases={},
                base_semantic_variants={},
                proposed=[],
                diagnostic_run_id="run-abc",
                model_id="claude-opus-4-7",
            )

    def test_unsafe_overlay_name_rejected(self, starter) -> None:
        with pytest.raises(UnsafePathSegmentError):
            draft_overlay(
                overlay_name="../../escape",
                base_pack_id="fusion-finance-starter",
                base_pack_version="0.1.0",
                base_column_aliases=starter.pack.column_aliases,
                base_semantic_variants=starter.pack.semantic_variants,
                proposed=[_basic_proposal()],
                diagnostic_run_id="run-abc",
                model_id="claude-opus-4-7",
            )


class TestWriters:
    def test_write_overlay_writes_under_overlays_dir(
        self, starter, tmp_path: Path
    ) -> None:
        draft = draft_overlay(
            overlay_name="test-write",
            base_pack_id="fusion-finance-starter",
            base_pack_version="0.1.0",
            base_column_aliases=starter.pack.column_aliases,
            base_semantic_variants=starter.pack.semantic_variants,
            proposed=[_basic_proposal()],
            diagnostic_run_id="run-abc",
            model_id="claude-opus-4-7",
        )
        result = write_overlay(draft, workdir=tmp_path)
        assert result == tmp_path / "overlays" / "test-write" / "pack.yaml"
        loaded = yaml.safe_load(result.read_text(encoding="utf-8"))
        assert loaded["id"] == "test-write"
        assert loaded["extends"] == "fusion-finance-starter@0.1.0"
        assert loaded["provenance"]["skillId"] == "aidp-fusion-medallion-author"

    def test_write_overlay_refuses_existing_without_overwrite(
        self, starter, tmp_path: Path
    ) -> None:
        draft = draft_overlay(
            overlay_name="test-existing",
            base_pack_id="fusion-finance-starter",
            base_pack_version="0.1.0",
            base_column_aliases=starter.pack.column_aliases,
            base_semantic_variants=starter.pack.semantic_variants,
            proposed=[_basic_proposal()],
            diagnostic_run_id="run-abc",
            model_id="claude-opus-4-7",
        )
        write_overlay(draft, workdir=tmp_path)
        with pytest.raises(FileExistsError):
            write_overlay(draft, workdir=tmp_path)


class TestConditionalResolutions:
    def test_no_resolutions_for_initial_autoresolved(
        self, starter, tmp_path: Path
    ) -> None:
        """Initial AIDPF-2010 → extended candidate list will
        AutoResolve on next bootstrap → resolutions.json would be
        rejected by feature #2's validator. Drafter returns None."""
        draft = draft_overlay(
            overlay_name="test-no-res",
            base_pack_id="fusion-finance-starter",
            base_pack_version="0.1.0",
            base_column_aliases=starter.pack.column_aliases,
            base_semantic_variants=starter.pack.semantic_variants,
            proposed=[_basic_proposal(outcome="AutoResolved")],
            diagnostic_run_id="run-abc",
            model_id="claude-opus-4-7",
        )
        result = write_resolutions(draft, workdir=tmp_path, tenant="finance-default")
        assert result is None

    def test_resolutions_emitted_for_multimatch(
        self, starter, tmp_path: Path
    ) -> None:
        draft = draft_overlay(
            overlay_name="test-multi-res",
            base_pack_id="fusion-finance-starter",
            base_pack_version="0.1.0",
            base_column_aliases=starter.pack.column_aliases,
            base_semantic_variants=starter.pack.semantic_variants,
            proposed=[_basic_proposal(outcome="MultiMatch")],
            diagnostic_run_id="run-abc",
            model_id="claude-opus-4-7",
        )
        result = write_resolutions(draft, workdir=tmp_path, tenant="finance-default")
        assert result is not None
        loaded = json.loads(result.read_text(encoding="utf-8"))
        assert loaded["tenant"] == "finance-default"
        assert loaded["resolutions"][0]["chosenCandidate"] == "ApInvoicesXCurrCode"

    def test_resolutions_emitted_for_refresh_change(
        self, starter, tmp_path: Path
    ) -> None:
        impact = IncrementalImpact(
            changeKind="promotion",
            priorPinned="ApInvoicesCurrencyCode",
            newCandidate="ApInvoicesXCurrCode",
            riskLabel="likely-rename",
            affectedNodes=["silver.supplier_spend"],
            remediation=RemediationRecord(
                recommended="D",
                operatorChose="D",
                rationale="rename",
            ),
        )
        draft = draft_overlay(
            overlay_name="test-refresh-res",
            base_pack_id="fusion-finance-starter",
            base_pack_version="0.1.0",
            base_column_aliases=starter.pack.column_aliases,
            base_semantic_variants=starter.pack.semantic_variants,
            proposed=[_basic_proposal(outcome="RefreshChange", impact=impact)],
            diagnostic_run_id="run-abc",
            model_id="claude-opus-4-7",
        )
        result = write_resolutions(draft, workdir=tmp_path, tenant="finance-default")
        assert result is not None


class TestSkillEvidence:
    def test_skill_evidence_includes_proposals_and_metadata(
        self, starter, tmp_path: Path
    ) -> None:
        draft = draft_overlay(
            overlay_name="test-evidence",
            base_pack_id="fusion-finance-starter",
            base_pack_version="0.1.0",
            base_column_aliases=starter.pack.column_aliases,
            base_semantic_variants=starter.pack.semantic_variants,
            proposed=[_basic_proposal()],
            diagnostic_run_id="run-abc",
            model_id="claude-opus-4-7",
        )
        result = write_skill_evidence(draft, workdir=tmp_path)
        assert result.exists()
        payload = json.loads(result.read_text(encoding="utf-8"))
        assert payload["skillId"] == "aidp-fusion-medallion-author"
        assert payload["diagnosticRunId"] == "run-abc"
        assert payload["proposed"][0]["candidate"] == "ApInvoicesXCurrCode"
