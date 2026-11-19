"""Tests for the AIDPF-4070 bronze type-mismatch diagnostic plumbing and the
medallion-author type-overlay drafter (feature: bronze-column-type-overlay,
Steps 6b/6c/6d)."""

from __future__ import annotations

from pathlib import Path

import pytest

from oracle_ai_data_platform_fusion_autopilot.medallion_author.drafter import (
    OverlayDraft,
    OverlayValidationError,
    draft_type_overlay,
    validate_overlay,
)
from oracle_ai_data_platform_fusion_autopilot.medallion_author.reader import read_run
from oracle_ai_data_platform_fusion_autopilot.schema.diagnostic_artifact import (
    BronzeTypeMismatchV1,
    write_bronze_type_mismatch_diagnostic,
)
from oracle_ai_data_platform_fusion_autopilot.schema.medallion_pack import PackYaml


_DIAG = {
    "schemaVersion": 1,
    "runId": "20260621T000000Z-abc",
    "tenant": "acme",
    "errorCode": "AIDPF-4070",
    "errorMessage": "VENDORID type drift",
    "generatedAt": "2026-06-21T00:00:00Z",
    "node": "erp_suppliers",
    "datastore": "Fscm.PozBiccExtractAM.SupplierExtractPVO",
    "typeMismatches": [
        {"column": "VENDORID", "declared": "decimal(38,30)", "materialised": "decimal(18,0)"},
        {"column": "PARTYID", "declared": "decimal(38,30)", "materialised": "decimal(18,0)"},
    ],
    "pvoColumns": [{"name": "VENDORID", "type": "decimal(18,0)", "nullable": True}],
}


def _provenance() -> dict:
    return {
        "skillId": "aidp-fusion-medallion-author",
        "skillVersion": "1",
        "modelId": "m",
        "diagnosticRunId": "R1",
        "proposals": {},
    }


# ---- 6c: artifact model + persistence + reader ----------------------------


def test_bronze_type_mismatch_model_round_trips() -> None:
    art = BronzeTypeMismatchV1.model_validate(_DIAG)
    assert art.node == "erp_suppliers"
    assert art.type_mismatches[0].materialised == "decimal(18,0)"


def test_write_and_read_type_mismatch_diagnostic(tmp_path: Path) -> None:
    art = BronzeTypeMismatchV1.model_validate(_DIAG)
    path = write_bronze_type_mismatch_diagnostic(tmp_path, _DIAG["runId"], art)
    assert path.name == "AIDPF-4070__erp_suppliers.json"
    res = read_run(tmp_path / ".aidp" / "diagnostics", _DIAG["runId"])
    assert [m.node for m in res.type_mismatch_failures] == ["erp_suppliers"]
    assert res.type_mismatch_failures[0].type_mismatches[1].column == "PARTYID"


def test_4070_only_run_is_actionable(tmp_path: Path) -> None:
    """A run dir with ONLY an AIDPF-4070 diagnostic must be actionable —
    medallion-author drafts a type-overlay from it."""
    art = BronzeTypeMismatchV1.model_validate(_DIAG)
    write_bronze_type_mismatch_diagnostic(tmp_path, _DIAG["runId"], art)
    res = read_run(tmp_path / ".aidp" / "diagnostics", _DIAG["runId"])
    assert res.is_empty is False
    assert res.can_proceed() is True
    assert res.has_drift_only is False


# ---- 6d: drafter ----------------------------------------------------------


def test_draft_type_overlay_emits_retype_override() -> None:
    art = BronzeTypeMismatchV1.model_validate(_DIAG)
    draft = draft_type_overlay(
        overlay_name="fix-supplier-types",
        base_pack_id="fusion-finance-starter",
        base_pack_version="0.1.0",
        mismatch=art,
        diagnostic_run_id="R1",
        model_id="m1",
    )
    ovr = draft.pack_yaml.overrides["bronze/erp_suppliers"]
    cols = {c.name: c.type for c in ovr.output_schema.columns}
    assert cols == {"VENDORID": "decimal(18,0)", "PARTYID": "decimal(18,0)"}
    assert draft.pack_yaml.extends == "fusion-finance-starter@0.1.0"
    assert draft.pack_yaml.provenance.skill_id  # provenance carried


def test_draft_type_overlay_empty_mismatch_rejected() -> None:
    art = BronzeTypeMismatchV1.model_validate({**_DIAG, "typeMismatches": []})
    with pytest.raises(OverlayValidationError):
        draft_type_overlay(
            overlay_name="x", base_pack_id="fusion-finance-starter",
            base_pack_version="0.1.0", mismatch=art, diagnostic_run_id="R1", model_id="m",
        )


# ---- 6b: validate_overlay relax ------------------------------------------


def _draft_with_overrides(overrides: dict) -> OverlayDraft:
    pack = PackYaml.model_validate({
        "id": "ov", "version": "0.1.0", "extends": "fusion-finance-starter@0.1.0",
        "compatibility": {"pluginMinVersion": "0.3.0", "fusionFamilies": ["ERP"]},
        "provenance": _provenance(),
        "overrides": overrides,
    })
    return OverlayDraft(
        overlay_name="ov", base_pack_id="fusion-finance-starter", base_pack_version="0.1.0",
        diagnostic_run_id="R1", model_id="m", proposed=(), pack_yaml=pack,
    )


def test_validate_overlay_allows_sanctioned_bronze_output_schema() -> None:
    draft = _draft_with_overrides({"bronze/erp_suppliers": {
        "outputSchema": {"columns": [{"name": "VENDORID", "type": "decimal(18,0)"}]}}})
    validate_overlay(draft)  # no raise


def test_validate_overlay_rejects_sql_override() -> None:
    draft = _draft_with_overrides({"silver/dim_x": {"sql": "silver/x.sql"}})
    with pytest.raises(OverlayValidationError):
        validate_overlay(draft)


def test_validate_overlay_rejects_nonbronze_output_schema() -> None:
    draft = _draft_with_overrides({"gold/ap_aging": {
        "outputSchema": {"columns": [{"name": "x", "type": "int"}]}}})
    with pytest.raises(OverlayValidationError):
        validate_overlay(draft)
