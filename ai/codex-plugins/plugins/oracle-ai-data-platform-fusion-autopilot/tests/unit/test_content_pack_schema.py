"""Unit tests for content pack Pydantic schema validation.

Covers Steps 2, 3, 4 of v2-phase-1-content-pack-schema:
    - PackYaml top-level (identity, compatibility, defaults, profiles,
      columnAliases, semanticVariants, extends, overrides).
    - NodeYaml (silver/gold) + the 13-rule strategy validation matrix
      (R1-R13 from PLAN §11.3 → error codes AIDPF-2020, 2030, 2050-2060).
    - DashboardYaml (delivery, requires, validation queries, security).

Each strategy rule has its own test asserting both the rejection and the
specific error code per the allocation table in plan.md.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from pydantic import ValidationError

from oracle_ai_data_platform_fusion_autopilot.schema.medallion_pack import (
    AIDPF_2001_ORPHAN_OVERRIDE,
    AIDPF_2002_INVALID_SEMVER,
    ColumnAlias,
    PackOverlayRef,
    PackYaml,
    SemanticVariant,
)

# Path to the package-data JSON Schema artifact. Tests read it via the
# importable package so the same path works in editable + installed modes.
import oracle_ai_data_platform_fusion_autopilot as _pkg

PACK_SCHEMA_JSON = Path(_pkg.__file__).parent / "pack.schema.json"


def _errors_contain(exc: ValidationError, code: str) -> bool:
    """True when at least one Pydantic validation error message references the code."""
    for err in exc.errors():
        if code in str(err.get("msg", "")) or code in str(err.get("ctx", {})):
            return True
    return False


def _minimal_pack_dict() -> dict:
    """Smallest pack.yaml-shaped dict that validates."""
    return {
        "id": "fusion-finance-starter",
        "version": "0.1.0",
        "compatibility": {
            "pluginMinVersion": "0.3.0",
            "fusionFamilies": ["ERP"],
        },
    }


def test_pack_yaml_minimal_valid() -> None:
    """A minimal pack.yaml with required fields validates."""
    pack = PackYaml.model_validate(_minimal_pack_dict())
    assert pack.id == "fusion-finance-starter"
    assert pack.version == "0.1.0"
    assert pack.compatibility.plugin_min_version == "0.3.0"
    assert pack.compatibility.aidp.requires_delta is True
    assert pack.defaults.sql_dialect == "spark-sql"
    assert pack.defaults.run_id_column.bronze == "_run_id"
    assert pack.defaults.run_id_column.silver == "silver_run_id"
    assert pack.defaults.run_id_column.gold == "gold_run_id"
    assert pack.extends is None
    assert pack.overrides == {}


def test_pack_yaml_invalid_semver_rejected() -> None:
    """Non-SemVer `version:` rejected with AIDPF-2002."""
    data = _minimal_pack_dict()
    data["version"] = "not-a-version"
    with pytest.raises(ValidationError) as exc:
        PackYaml.model_validate(data)
    assert _errors_contain(exc.value, AIDPF_2002_INVALID_SEMVER)


def test_pack_yaml_invalid_plugin_min_version_rejected() -> None:
    """Non-SemVer `compatibility.pluginMinVersion` rejected with AIDPF-2002."""
    data = _minimal_pack_dict()
    data["compatibility"]["pluginMinVersion"] = "v0.3"  # leading 'v' is invalid SemVer
    with pytest.raises(ValidationError) as exc:
        PackYaml.model_validate(data)
    assert _errors_contain(exc.value, AIDPF_2002_INVALID_SEMVER)


def test_pack_yaml_unknown_top_level_key_rejected() -> None:
    """Strict mode: unknown top-level keys rejected."""
    data = _minimal_pack_dict()
    data["unknown_key"] = "should be rejected"
    with pytest.raises(ValidationError):
        PackYaml.model_validate(data)


def test_pack_yaml_extends_valid_form() -> None:
    """`extends: <pack>@<version>` parses and round-trips."""
    data = _minimal_pack_dict()
    data["extends"] = "fusion-finance-starter@0.2.0"
    pack = PackYaml.model_validate(data)
    ref = pack.parsed_extends()
    assert ref is not None
    assert ref.name == "fusion-finance-starter"
    assert ref.version == "0.2.0"
    assert ref.to_string() == "fusion-finance-starter@0.2.0"


def test_pack_yaml_extends_invalid_form_rejected() -> None:
    """`extends:` without `@version` rejected."""
    data = _minimal_pack_dict()
    data["extends"] = "fusion-finance-starter"  # missing @<version>
    with pytest.raises(ValidationError) as exc:
        PackYaml.model_validate(data)
    assert _errors_contain(exc.value, AIDPF_2002_INVALID_SEMVER)


def test_pack_yaml_extends_invalid_semver_rejected() -> None:
    """`extends: <pack>@<not-semver>` rejected with AIDPF-2002."""
    data = _minimal_pack_dict()
    data["extends"] = "fusion-finance-starter@not-semver"
    with pytest.raises(ValidationError) as exc:
        PackYaml.model_validate(data)
    assert _errors_contain(exc.value, AIDPF_2002_INVALID_SEMVER)


def test_base_pack_with_overrides_rejected() -> None:
    """A pack with no `extends:` cannot declare `overrides:` (AIDPF-2001)."""
    data = _minimal_pack_dict()
    data["overrides"] = {"silver/dim_supplier": {"profile": "finance-default"}}
    with pytest.raises(ValidationError) as exc:
        PackYaml.model_validate(data)
    assert _errors_contain(exc.value, AIDPF_2001_ORPHAN_OVERRIDE)


def test_overlay_pack_with_overrides_accepted() -> None:
    """An overlay pack (extends set) may declare overrides."""
    data = _minimal_pack_dict()
    data["id"] = "acme-finance"
    data["extends"] = "fusion-finance-starter@0.1.0"
    data["overrides"] = {"silver/dim_supplier": {"profile": "acme-prod"}}
    pack = PackYaml.model_validate(data)
    assert pack.extends == "fusion-finance-starter@0.1.0"
    assert "silver/dim_supplier" in pack.overrides
    assert pack.overrides["silver/dim_supplier"].profile == "acme-prod"


def test_column_alias_requires_candidates() -> None:
    """`columnAliases.<vp>.candidates` must contain at least one entry."""
    data = _minimal_pack_dict()
    data["columnAliases"] = {
        "invoice_currency_code": {
            "appliesTo": "bronze.ap_invoices",
            "required": True,
            "candidates": [],
        }
    }
    with pytest.raises(ValidationError):
        PackYaml.model_validate(data)


def test_column_alias_valid() -> None:
    """Valid columnAliases entry parses cleanly."""
    ca = ColumnAlias.model_validate(
        {
            "appliesTo": "bronze.ap_invoices",
            "required": True,
            "candidates": ["ApInvoicesInvoiceCurrencyCode", "ApInvoicesCurrencyCode"],
        }
    )
    assert ca.appliesTo == "bronze.ap_invoices"
    assert ca.required is True
    assert ca.candidates == [
        "ApInvoicesInvoiceCurrencyCode",
        "ApInvoicesCurrencyCode",
    ]


def test_semantic_variant_valid() -> None:
    """Valid semanticVariants entry with two candidates parses cleanly."""
    sv = SemanticVariant.model_validate(
        {
            "appliesTo": "bronze.ap_invoices",
            "required": True,
            "candidates": [
                {
                    "id": "cancelled_date",
                    "detect": {"columnExists": "ApInvoicesCancelledDate"},
                    "fragment": "{table}.ApInvoicesCancelledDate IS NULL",
                },
                {
                    "id": "cancelled_flag",
                    "detect": {"columnExists": "ApInvoicesCancelledFlag"},
                    "fragment": "COALESCE({table}.ApInvoicesCancelledFlag, 'N') != 'Y'",
                },
            ],
        }
    )
    assert len(sv.candidates) == 2
    assert sv.candidates[0].id == "cancelled_date"
    assert sv.candidates[1].id == "cancelled_flag"


def test_overlay_ref_parse_round_trip() -> None:
    """`<id>@<semver>` round-trips through `PackOverlayRef`."""
    ref = PackOverlayRef.parse("fusion-finance-starter@0.2.0")
    assert ref.name == "fusion-finance-starter"
    assert ref.version == "0.2.0"
    assert ref.to_string() == "fusion-finance-starter@0.2.0"


def test_overlay_ref_invalid_no_at_sign() -> None:
    """`<id>` without `@<version>` rejected with AIDPF-2002."""
    with pytest.raises(ValueError) as exc:
        PackOverlayRef.parse("fusion-finance-starter")
    assert AIDPF_2002_INVALID_SEMVER in str(exc.value)


def test_overlay_ref_invalid_semver_in_at_clause() -> None:
    """`<id>@<not-semver>` rejected."""
    with pytest.raises(ValueError) as exc:
        PackOverlayRef.parse("fusion-finance-starter@v0.2")
    assert AIDPF_2002_INVALID_SEMVER in str(exc.value)


def test_pack_yaml_with_profile() -> None:
    """A pack with a `finance-default` profile validates with nested calendar + COA."""
    data = _minimal_pack_dict()
    data["profiles"] = {
        "finance-default": {
            "calendar": {
                "startDate": "2020-01-01",
                "endDate": "2030-12-31",
                "fiscalStartMonth": 1,
            },
            "chartOfAccounts": {
                "balancingSegment": "segment1",
                "costCenterSegment": "segment2",
                "naturalAccountSegment": "segment3",
            },
        }
    }
    pack = PackYaml.model_validate(data)
    assert "finance-default" in pack.profiles
    profile = pack.profiles["finance-default"]
    assert profile.calendar is not None
    assert profile.calendar.start_date == "2020-01-01"
    assert profile.chart_of_accounts is not None
    assert profile.chart_of_accounts.balancing_segment == "segment1"


def test_pack_schema_json_matches_models() -> None:
    """pack.schema.json on disk must match `PackYaml.model_json_schema()`.

    Regenerate with::

        python -c 'import json; from pathlib import Path;
        from oracle_ai_data_platform_fusion_autopilot.schema.medallion_pack import PackYaml;
        out = Path("scripts/oracle_ai_data_platform_fusion_autopilot/pack.schema.json");
        out.write_text(json.dumps(PackYaml.model_json_schema(), indent=2, sort_keys=True) + chr(10))'

    Per PLAN §6.2 acceptance: "CI fails if [the JSON Schema artifact] drifts
    from the Pydantic models."
    """
    assert PACK_SCHEMA_JSON.exists(), (
        f"pack.schema.json missing at {PACK_SCHEMA_JSON}. Regenerate it from "
        "PackYaml.model_json_schema()."
    )
    on_disk = json.loads(PACK_SCHEMA_JSON.read_text())
    from_models = PackYaml.model_json_schema()
    assert on_disk == from_models, (
        "pack.schema.json drifted from the current Pydantic models. "
        "Regenerate the file (see this test's docstring)."
    )


def test_pack_yaml_with_variation_points() -> None:
    """A pack declaring columnAliases + semanticVariants validates."""
    data = _minimal_pack_dict()
    data["columnAliases"] = {
        "invoice_currency_code": {
            "appliesTo": "bronze.ap_invoices",
            "required": True,
            "candidates": ["ApInvoicesInvoiceCurrencyCode", "ApInvoicesCurrencyCode"],
        }
    }
    data["semanticVariants"] = {
        "cancelled_status": {
            "appliesTo": "bronze.ap_invoices",
            "required": True,
            "candidates": [
                {
                    "id": "cancelled_date",
                    "detect": {"columnExists": "ApInvoicesCancelledDate"},
                    "fragment": "{table}.ApInvoicesCancelledDate IS NULL",
                },
            ],
        }
    }
    pack = PackYaml.model_validate(data)
    assert "invoice_currency_code" in pack.column_aliases
    assert "cancelled_status" in pack.semantic_variants


# ---------------------------------------------------------------------------
# Strategy validation matrix (PLAN §11.3 R1-R13)
# ---------------------------------------------------------------------------

from oracle_ai_data_platform_fusion_autopilot.schema.medallion_pack import (
    AIDPF_2020_MERGE_NO_NATURAL_KEY,
    AIDPF_2030_OUTPUT_SCHEMA_NO_PII,
    AIDPF_2050_MERGE_NO_WATERMARK,
    AIDPF_2051_MERGE_ZERO_PRIMARY,
    AIDPF_2052_MERGE_MULTI_PRIMARY,
    AIDPF_2053_MERGE_MULTI_BRONZE_NO_ROLE,
    AIDPF_2054_REPLACE_PARTITION_NO_COLUMNS,
    AIDPF_2055_REPLACE_PARTITION_MULTI_PRIMARY,
    AIDPF_2056_APPEND_UNIQUE_NO_KEY,
    AIDPF_2057_AGGREGATE_MERGE_DEFERRED,
    AIDPF_2058_SNAPSHOT_NO_UNIQUE_TEST,
    AIDPF_2059_SCD2_NO_TRACKED_COLUMNS,
    NodeYaml,
)


def _minimal_silver_node_dict(**overrides) -> dict:
    """Minimal valid silver node dict; overrides merged in for negative tests."""
    base = {
        "id": "dim_supplier",
        "layer": "silver",
        "implementation": {"type": "sql", "sql": "silver/dim_supplier.sql"},
        "target": "dim_supplier",
        "dependsOn": {
            "bronze": [
                {
                    "id": "erp_suppliers",
                    "watermark": {"column": "_extract_ts"},
                }
            ]
        },
        "refresh": {
            "seed": {"strategy": "replace"},
            "incremental": {
                "strategy": "merge",
                "watermark": {"source": "erp_suppliers", "column": "_extract_ts"},
                "naturalKey": ["supplier_number"],
            },
        },
        "outputSchema": {
            "columns": [
                {"name": "supplier_key", "type": "bigint", "nullable": False, "pii": "none"},
                {"name": "supplier_number", "type": "string", "nullable": False, "pii": "low"},
            ]
        },
    }
    base.update(overrides)
    return base


def test_minimal_silver_node_valid() -> None:
    """Sanity: a minimal valid silver node parses cleanly."""
    node = NodeYaml.model_validate(_minimal_silver_node_dict())
    assert node.id == "dim_supplier"
    assert node.refresh.incremental.strategy == "merge"


# ----- R1: merge without naturalKey -----------------------------------------


def test_R1_merge_without_naturalKey_rejected() -> None:
    data = _minimal_silver_node_dict()
    del data["refresh"]["incremental"]["naturalKey"]
    with pytest.raises(ValidationError) as exc:
        NodeYaml.model_validate(data)
    assert _errors_contain(exc.value, AIDPF_2020_MERGE_NO_NATURAL_KEY)


# ----- R2: merge without watermark ------------------------------------------


def test_R2_merge_without_watermark_rejected() -> None:
    data = _minimal_silver_node_dict()
    del data["refresh"]["incremental"]["watermark"]
    with pytest.raises(ValidationError) as exc:
        NodeYaml.model_validate(data)
    assert _errors_contain(exc.value, AIDPF_2050_MERGE_NO_WATERMARK)


# ----- R3: merge with zero role:primary on multi-source node ----------------


def test_R3_merge_zero_primary_rejected() -> None:
    data = _minimal_silver_node_dict()
    data["dependsOn"]["bronze"] = [
        {"id": "erp_suppliers", "role": "lookup"},
        {"id": "other_source", "role": "lookup"},
    ]
    with pytest.raises(ValidationError) as exc:
        NodeYaml.model_validate(data)
    # Could trip R3 or R5 depending on validation order; both are correct rejection codes.
    assert _errors_contain(exc.value, AIDPF_2051_MERGE_ZERO_PRIMARY) or _errors_contain(
        exc.value, AIDPF_2053_MERGE_MULTI_BRONZE_NO_ROLE
    )


# ----- R4: merge with multiple role:primary ---------------------------------


def test_R4_merge_multi_primary_rejected() -> None:
    data = _minimal_silver_node_dict()
    data["dependsOn"]["bronze"] = [
        {"id": "erp_suppliers", "role": "primary", "watermark": {"column": "_extract_ts"}},
        {"id": "another_source", "role": "primary", "watermark": {"column": "_extract_ts"}},
    ]
    with pytest.raises(ValidationError) as exc:
        NodeYaml.model_validate(data)
    assert _errors_contain(exc.value, AIDPF_2052_MERGE_MULTI_PRIMARY)


# ----- R5: merge with multi-bronze deps and no explicit role ----------------


def test_R5_merge_multi_bronze_no_role_rejected() -> None:
    data = _minimal_silver_node_dict()
    data["dependsOn"]["bronze"] = [
        {"id": "erp_suppliers"},  # no role
        {"id": "other_source"},  # no role
    ]
    with pytest.raises(ValidationError) as exc:
        NodeYaml.model_validate(data)
    assert _errors_contain(exc.value, AIDPF_2053_MERGE_MULTI_BRONZE_NO_ROLE)


# ----- R6: replace_partition without partitionColumns ------------------------


def test_R6_replace_partition_without_partition_columns_rejected() -> None:
    data = _minimal_silver_node_dict()
    data["refresh"]["incremental"] = {
        "strategy": "replace_partition",
        "watermark": {"source": "erp_suppliers", "column": "_extract_ts"},
    }
    with pytest.raises(ValidationError) as exc:
        NodeYaml.model_validate(data)
    assert _errors_contain(exc.value, AIDPF_2054_REPLACE_PARTITION_NO_COLUMNS)


# ----- R7: replace_partition with multi-source role:primary -----------------


def test_R7_replace_partition_multi_primary_rejected() -> None:
    data = _minimal_silver_node_dict()
    data["dependsOn"]["bronze"] = [
        {"id": "erp_suppliers", "role": "primary", "watermark": {"column": "_extract_ts"}},
        {"id": "another", "role": "primary", "watermark": {"column": "_extract_ts"}},
    ]
    data["refresh"]["incremental"] = {
        "strategy": "replace_partition",
        "partitionColumns": ["partition_col"],
        "watermark": {"source": "erp_suppliers", "column": "_extract_ts"},
    }
    with pytest.raises(ValidationError) as exc:
        NodeYaml.model_validate(data)
    assert _errors_contain(exc.value, AIDPF_2055_REPLACE_PARTITION_MULTI_PRIMARY)


# ----- R8: append with `unique` quality test but no naturalKey --------------


def test_R8_append_unique_no_key_rejected() -> None:
    data = _minimal_silver_node_dict()
    data["refresh"]["incremental"] = {
        "strategy": "append",
        "watermark": {"source": "erp_suppliers", "column": "_extract_ts"},
    }
    data["quality"] = {"tests": [{"type": "unique", "columns": ["some_id"]}]}
    with pytest.raises(ValidationError) as exc:
        NodeYaml.model_validate(data)
    assert _errors_contain(exc.value, AIDPF_2056_APPEND_UNIQUE_NO_KEY)


# ----- R9: aggregate_merge (deferred) ---------------------------------------


def test_R9_aggregate_merge_deferred_rejected() -> None:
    data = _minimal_silver_node_dict()
    data["refresh"]["incremental"] = {
        "strategy": "aggregate_merge",
        "watermark": {"source": "erp_suppliers", "column": "_extract_ts"},
        "naturalKey": ["supplier_number"],
    }
    with pytest.raises(ValidationError) as exc:
        NodeYaml.model_validate(data)
    assert _errors_contain(exc.value, AIDPF_2057_AGGREGATE_MERGE_DEFERRED)


# ----- R10: snapshot without unique-on-snapshot test (deferred) -------------


def test_R10_snapshot_without_unique_test_rejected() -> None:
    data = _minimal_silver_node_dict()
    data["refresh"]["incremental"] = {
        "strategy": "snapshot",
        "watermark": {"source": "erp_suppliers", "column": "_extract_ts"},
        "naturalKey": ["supplier_number"],
    }
    # No unique-on-snapshot test in quality block.
    with pytest.raises(ValidationError) as exc:
        NodeYaml.model_validate(data)
    assert _errors_contain(exc.value, AIDPF_2058_SNAPSHOT_NO_UNIQUE_TEST)


# ----- R11: scd2 without trackedColumns (deferred) --------------------------


def test_R11_scd2_without_tracked_columns_rejected() -> None:
    data = _minimal_silver_node_dict()
    data["refresh"]["incremental"] = {
        "strategy": "scd2",
        "watermark": {"source": "erp_suppliers", "column": "_extract_ts"},
        "naturalKey": ["supplier_number"],
    }
    with pytest.raises(ValidationError) as exc:
        NodeYaml.model_validate(data)
    assert _errors_contain(exc.value, AIDPF_2059_SCD2_NO_TRACKED_COLUMNS)


# ----- R12: outputSchema column missing pii ---------------------------------


def test_R12_output_schema_missing_pii_rejected() -> None:
    data = _minimal_silver_node_dict()
    # Remove pii from one of the columns.
    data["outputSchema"]["columns"][0] = {
        "name": "supplier_key",
        "type": "bigint",
        "nullable": False,
        # pii intentionally omitted
    }
    with pytest.raises(ValidationError) as exc:
        NodeYaml.model_validate(data)
    # Pydantic surfaces this as a generic missing-field error; the AIDPF code
    # is the documented remediation rather than the literal error string.
    # Confirm the rejection happens at the pii field.
    errors = exc.value.errors()
    assert any("pii" in str(e.get("loc", ())) for e in errors), (
        f"expected pii-related rejection (AIDPF-2030), got errors: {errors}"
    )


# Phase 9: R13 (python_legacy invariants) deleted with the
# python_legacy implementation type itself. ADR-0022 documents the
# customer-migration paths.


# ---------------------------------------------------------------------------
# Dashboard pack (Step 4)
# ---------------------------------------------------------------------------

from oracle_ai_data_platform_fusion_autopilot.schema.dashboard_pack import DashboardYaml


def _minimal_dashboard_dict(**overrides) -> dict:
    base = {
        "id": "executive_cfo",
        "title": "Executive CFO Dashboard",
        "version": "0.1.0",
        "delivery": {
            "type": "oac-snapshot",
            "barObject": "dashboards/executive-cfo.bar",
            "oac": {
                "projectName": "Executive CFO",
                "folderPath": "/Shared Folders/AIDP Fusion/Finance",
                "connectionName": "aidp-fusion-gold",
            },
        },
        "requires": {
            "pack": {"id": "fusion-finance-starter", "minVersion": "0.1.0"},
            "tables": ["gold.gl_balance", "gold.ap_aging"],
            "columns": {
                "gold.gl_balance": [
                    {"name": "ledger_id", "type": "bigint"},
                    {"name": "ending_balance", "type": "decimal(28,8)"},
                ],
            },
        },
        "validationQueries": [
            {
                "id": "gl_balance_has_data",
                "sql": "SELECT COUNT(*) AS row_count FROM {{ catalog }}.{{ gold_schema }}.gl_balance",
                "expect": {"row_count_min": 1},
                "severity": "error",
            }
        ],
    }
    base.update(overrides)
    return base


def test_dashboard_yaml_minimal_valid() -> None:
    dashboard = DashboardYaml.model_validate(_minimal_dashboard_dict())
    assert dashboard.id == "executive_cfo"
    assert dashboard.delivery.type == "oac-snapshot"
    assert dashboard.requires.tables == ["gold.gl_balance", "gold.ap_aging"]


def test_dashboard_yaml_unknown_key_rejected() -> None:
    data = _minimal_dashboard_dict()
    data["unknown_field"] = "should be rejected"
    with pytest.raises(ValidationError):
        DashboardYaml.model_validate(data)


def test_dashboard_yaml_invalid_delivery_type_rejected() -> None:
    data = _minimal_dashboard_dict()
    data["delivery"]["type"] = "tableau-twbx"  # not supported in v0.3
    with pytest.raises(ValidationError):
        DashboardYaml.model_validate(data)


def test_dashboard_yaml_invalid_severity_rejected() -> None:
    data = _minimal_dashboard_dict()
    data["validationQueries"][0]["severity"] = "info"  # not in Literal
    with pytest.raises(ValidationError):
        DashboardYaml.model_validate(data)


def test_dashboard_yaml_security_allowed_columns() -> None:
    data = _minimal_dashboard_dict()
    data["security"] = {
        "allowedColumns": {
            "gold.gl_balance": ["ledger_id", "ending_balance"],
        }
    }
    dashboard = DashboardYaml.model_validate(data)
    assert dashboard.security.allowed_columns["gold.gl_balance"] == [
        "ledger_id",
        "ending_balance",
    ]


def test_dashboard_yaml_freshness_block_valid() -> None:
    data = _minimal_dashboard_dict()
    data["requires"]["freshness"] = {
        "gold.gl_balance": {"maxAgeHours": 168},
        "gold.ap_aging": {"maxAgeHours": 24},
    }
    dashboard = DashboardYaml.model_validate(data)
    assert dashboard.requires.freshness["gold.ap_aging"].max_age_hours == 24


# Tests for AIDPF-8002 (PII high in requires.columns / allowedColumns) and
# AIDPF-7001 (requires.tables missing gold node) live in
# test_content_pack_validators.py — they need cross-pack validation that goes
# beyond schema-level checks. Step 6 wires those validators.
