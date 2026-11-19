"""Policy tests for the medallion-author bronze override allowlist.

`validate_overlay()` is the skill's policy boundary. These tests pin the explicit
allowlist: a bronze override is sanctioned iff it carries at least one of
`outputSchema` / `requiredColumns` / `relaxRequiredColumns` and none of
`sql`/`profile`/`quality`/`replaceNode`; a silver/gold `replaceNode` is the only
other sanctioned shape. Engine-level support for the bronze mechanisms is covered
separately by test_bronze_outputschema_overlay.py and
test_bronze_required_columns_overlay.py — this module is drafter-policy only.
"""

from __future__ import annotations

import pytest

from oracle_ai_data_platform_fusion_autopilot.medallion_author.drafter import (
    OverlayDraft,
    OverlayValidationError,
    validate_overlay,
)
from oracle_ai_data_platform_fusion_autopilot.schema.medallion_pack import PackYaml


def _draft(overrides: dict) -> OverlayDraft:
    """A minimal, provenance-valid overlay draft carrying `overrides`."""
    pack = PackYaml.model_validate({
        "id": "acme-finance",
        "version": "0.1.0",
        "extends": "fusion-finance-starter@0.1.0",
        "compatibility": {"pluginMinVersion": "0.3.0", "fusionFamilies": ["ERP"]},
        "provenance": {
            "skillId": "aidp-fusion-medallion-author",
            "skillVersion": "0.1.0",
            "modelId": "claude-opus-4-8",
            "generatedAt": "2026-06-30T00:00:00+00:00",
            "diagnosticRunId": "run-test",
            "trigger": "diagnostic",
        },
        "overrides": overrides,
    })
    return OverlayDraft(
        overlay_name="acme-finance",
        base_pack_id="fusion-finance-starter",
        base_pack_version="0.1.0",
        diagnostic_run_id="run-test",
        model_id="claude-opus-4-8",
        proposed=(),
        pack_yaml=pack,
        skill_evidence={},
    )


_OUTPUT_SCHEMA = {"columns": [{"name": "VENDORID", "type": "decimal(18,0)"}]}
_REQUIRED = {"erp_suppliers": ["EXTRACOL"]}
_RELAX = {"erp_suppliers": [{"column": "VENDORID", "reason": "tenant PVO lacks it"}]}
_REPLACE_NODE = {
    "reason": "rewrite; T-1",
    "forkedFrom": {"sqlSha256": "a", "contractSha256": "b", "packVersion": "0.1.0"},
}


# ---------------------------------------------------------------------------
# Accepted bronze shapes
# ---------------------------------------------------------------------------


def test_bronze_output_schema_only_accepted() -> None:
    validate_overlay(_draft({"bronze/erp_suppliers": {"outputSchema": _OUTPUT_SCHEMA}}))


def test_bronze_required_columns_only_accepted() -> None:
    validate_overlay(_draft({"bronze/erp_suppliers": {"requiredColumns": _REQUIRED}}))


def test_bronze_relax_required_columns_only_accepted() -> None:
    validate_overlay(
        _draft({"bronze/erp_suppliers": {"relaxRequiredColumns": _RELAX}})
    )


def test_bronze_output_schema_plus_required_columns_accepted() -> None:
    validate_overlay(_draft({
        "bronze/erp_suppliers": {
            "outputSchema": _OUTPUT_SCHEMA,
            "requiredColumns": _REQUIRED,
        }
    }))


# ---------------------------------------------------------------------------
# Rejected bronze shapes
# ---------------------------------------------------------------------------


def test_bronze_sql_override_rejected() -> None:
    with pytest.raises(OverlayValidationError):
        validate_overlay(_draft({"bronze/erp_suppliers": {"sql": "bronze/erp_suppliers.sql"}}))


def test_bronze_profile_override_rejected() -> None:
    with pytest.raises(OverlayValidationError):
        validate_overlay(_draft({
            "bronze/erp_suppliers": {"outputSchema": _OUTPUT_SCHEMA, "profile": "acme"}
        }))


def test_bronze_quality_override_rejected() -> None:
    with pytest.raises(OverlayValidationError):
        validate_overlay(_draft({
            "bronze/erp_suppliers": {
                "outputSchema": _OUTPUT_SCHEMA,
                "quality": {"tests": [{"type": "not_null", "columns": ["VENDORID"]}]},
            }
        }))


def test_bronze_replace_node_rejected() -> None:
    """replaceNode is silver/gold-only — a bronze replaceNode is not sanctioned."""
    with pytest.raises(OverlayValidationError):
        validate_overlay(_draft({"bronze/erp_suppliers": {"replaceNode": _REPLACE_NODE}}))


def test_empty_bronze_override_rejected() -> None:
    """A bronze entry carrying none of the three bronze keys is a no-op."""
    with pytest.raises(OverlayValidationError):
        validate_overlay(_draft({"bronze/erp_suppliers": {}}))


# ---------------------------------------------------------------------------
# Silver/gold replaceNode — unchanged (regression guard for guarded-mart)
# ---------------------------------------------------------------------------


def test_silver_replace_node_still_accepted() -> None:
    validate_overlay(_draft({"silver/dim_supplier": {"replaceNode": _REPLACE_NODE}}))


def test_gold_replace_node_still_accepted() -> None:
    validate_overlay(_draft({"gold/gl_balance": {"replaceNode": _REPLACE_NODE}}))


def test_silver_non_replace_override_rejected() -> None:
    """A silver/gold override that is NOT a replaceNode (e.g. sql) is forbidden."""
    with pytest.raises(OverlayValidationError):
        validate_overlay(_draft({"silver/dim_supplier": {"sql": "silver/dim_supplier.sql"}}))
