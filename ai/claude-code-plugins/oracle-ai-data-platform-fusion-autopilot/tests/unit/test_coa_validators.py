"""Validator guards for COA semantic roles (AIDPF-2014 / AIDPF-2015).

Feature coa-role-segment-resolution, M1 L1.3b + L1.8: the shipped pack passes;
a COA role modeled as a bare existence alias is rejected; an out-of-contract
binding is rejected.
"""

from __future__ import annotations

from pathlib import Path

from oracle_ai_data_platform_fusion_autopilot.orchestrator.content_pack import load_pack
from oracle_ai_data_platform_fusion_autopilot.orchestrator.content_pack_validators import (
    AIDPF_2014_COA_ROLE_AS_EXISTENCE_ALIAS,
    AIDPF_2015_COA_BINDING_OUT_OF_CONTRACT,
    validate_coa_semantic_roles,
)

PACK_ROOT = (
    Path(__file__).resolve().parents[2]
    / "scripts"
    / "oracle_ai_data_platform_fusion_autopilot"
    / "content_packs"
    / "fusion-finance-starter"
)


def _codes(errors) -> set[str]:
    return {e.code for e in errors}


def test_shipped_pack_passes_coa_role_validation() -> None:
    pack = load_pack(PACK_ROOT)
    errors = validate_coa_semantic_roles(pack)
    assert errors == [], [e.message for e in errors]


def test_coa_role_as_bare_existence_alias_rejected() -> None:
    pack = load_pack(PACK_ROOT)
    # Demote balancing back to an existence alias (the anti-pattern).
    spec = pack.pack.column_aliases["coa_balancing_segment"]
    object.__setattr__(spec, "resolution", "columnExistence")
    object.__setattr__(spec, "role", None)
    errors = validate_coa_semantic_roles(pack)
    assert AIDPF_2014_COA_ROLE_AS_EXISTENCE_ALIAS in _codes(errors)


def test_out_of_contract_candidate_rejected() -> None:
    pack = load_pack(PACK_ROOT)
    spec = pack.pack.column_aliases["coa_balancing_segment"]
    object.__setattr__(spec, "candidates", [*spec.candidates, "CodeCombinationSegment99"])
    errors = validate_coa_semantic_roles(pack)
    assert AIDPF_2015_COA_BINDING_OUT_OF_CONTRACT in _codes(errors)
