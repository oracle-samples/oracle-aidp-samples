"""medallion-author COA-depth operator-input mode (sub-plan D4).

Proves the skill drafts a complete COA-depth overlay (candidate lists + gl_coa
outputSchema) from OPERATOR INPUT with NO runtime diagnostic, and that the
operator-input provenance (operatorInputId + trigger, no diagnosticRunId) passes
validate_overlay — while the XOR is enforced (neither/both rejected).
"""

from __future__ import annotations

from pathlib import Path

import pytest

from oracle_ai_data_platform_fusion_autopilot.medallion_author.drafter import (
    OverlayValidationError,
    draft_coa_depth_overlay,
    validate_overlay,
)
from oracle_ai_data_platform_fusion_autopilot.orchestrator.content_pack import (
    load_full_chain,
    load_pack,
)
from oracle_ai_data_platform_fusion_autopilot.orchestrator.content_pack_validators import (
    validate_coa_semantic_roles,
)

REPO = Path(__file__).resolve().parents[2]
SHIPPED = (
    REPO / "scripts" / "oracle_ai_data_platform_fusion_autopilot"
    / "content_packs" / "fusion-finance-starter"
)


def _draft(segments=(7, 8, 9, 10)):
    base = load_pack(SHIPPED)
    return draft_coa_depth_overlay(
        overlay_name="acme-coa-depth",
        base_pack_id="fusion-finance-starter",
        base_pack_version="0.1.0",
        base_column_aliases=base.pack.column_aliases,
        segments=list(segments),
        operator_input_id="operator-input-acme-20260623",
        model_id="claude-opus-4-8",
        tenant="acme-prod",
        roles={"natural_account": "CodeCombinationSegment10"},
    )


def test_drafts_complete_overlay_from_operator_input_no_diagnostic() -> None:
    """No `.aidp/diagnostics/` involved — pure operator input."""
    draft = _draft()
    pack = draft.pack_yaml
    # Candidate lists extended for all three roles (inherit + deep cols).
    for alias in ("coa_balancing_segment", "coa_cost_center_segment", "coa_natural_account_segment"):
        ca = pack.column_aliases[alias]
        assert ca.resolution == "semanticRole"
        assert "inherit" in ca.candidates and "CodeCombinationSegment10" in ca.candidates
    # gl_coa outputSchema extended too (the coordinated half).
    ov = pack.overrides["bronze/gl_coa"]
    cols = {c.name for c in ov.output_schema.columns}
    assert {"CodeCombinationSegment7", "CodeCombinationSegment10"} <= cols


def test_operator_input_provenance_shape() -> None:
    prov = _draft().pack_yaml.provenance
    assert prov.operator_input_id == "operator-input-acme-20260623"
    assert prov.trigger == "operator_input"
    assert prov.diagnostic_run_id is None
    assert prov.skill_id and prov.skill_version and prov.model_id
    assert prov.evidence and prov.evidence.get("trigger") == "operator_input"
    assert prov.evidence.get("segments") == [7, 8, 9, 10]


def test_operator_input_overlay_passes_validate_overlay() -> None:
    # draft_coa_depth_overlay calls validate_overlay internally; assert no raise.
    draft = _draft()
    validate_overlay(draft)  # idempotent re-check


def test_validate_overlay_rejects_both_ids() -> None:
    draft = _draft()
    # Inject a diagnosticRunId alongside operatorInputId → both → reject.
    object.__setattr__(draft.pack_yaml.provenance, "diagnostic_run_id", "run-123")
    with pytest.raises(OverlayValidationError):
        validate_overlay(draft)


def test_validate_overlay_rejects_neither_id() -> None:
    draft = _draft()
    object.__setattr__(draft.pack_yaml.provenance, "operator_input_id", None)
    with pytest.raises(OverlayValidationError):
        validate_overlay(draft)


def test_drafted_overlay_merges_and_validates(tmp_path: Path) -> None:
    """Write the draft to disk, merge onto the shipped base, and confirm the
    merged chain passes COA validation (domain extended, contract-backed)."""
    from oracle_ai_data_platform_fusion_autopilot.medallion_author.drafter import write_overlay

    draft = _draft()
    pack_yaml_path = write_overlay(draft, workdir=tmp_path, overwrite=True)
    overlay_root = Path(pack_yaml_path).parent  # write_overlay returns the file

    def _resolver(ref):
        return SHIPPED if ref.name == "fusion-finance-starter" else None

    merged = load_full_chain(overlay_root, base_resolver=_resolver)
    assert validate_coa_semantic_roles(merged) == []
    bal = merged.pack.column_aliases["coa_balancing_segment"]
    assert "CodeCombinationSegment10" in bal.candidates
