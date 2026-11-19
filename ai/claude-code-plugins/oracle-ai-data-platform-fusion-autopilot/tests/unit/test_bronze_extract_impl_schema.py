"""Unit tests for BronzeExtractImpl Pydantic schema (Phase 9 Step 1).

Covers the bronze content-pack node type added in v2-phase-9-full-v1-deletion.
"""

from __future__ import annotations

import logging

import pytest
from pydantic import ValidationError

from oracle_ai_data_platform_fusion_autopilot.schema.medallion_pack import (
    AIDPF_2020_MERGE_NO_NATURAL_KEY,
    AIDPF_2050_MERGE_NO_WATERMARK,
    AIDPF_2080_BRONZE_EXTRACT_PVO_NOT_IN_CATALOG,
    AIDPF_2081_BUNDLE_DATASET_NOT_IN_PACK,
    BronzeExtractImpl,
    NodeYaml,
)


def _minimal_bronze_node(**impl_overrides) -> dict:
    impl = {
        "type": "bronze_extract",
        "datastore": "SupplierExtractPVO",
        "biccSchema": "Financial",
    }
    impl.update(impl_overrides)
    return {
        "id": "erp_suppliers",
        "layer": "bronze",
        "implementation": impl,
        "target": "erp_suppliers",
        "dependsOn": {"bronze": [], "silver": []},
        "refresh": {
            "seed": {"strategy": "replace"},
            "incremental": {
                "strategy": "merge",
                "watermark": {"source": "erp_suppliers", "column": "LASTUPDATEDATE"},
                "naturalKey": ["VENDORID", "PARTYID"],
            },
        },
        "requiredColumns": {},
        "outputSchema": {
            "columns": [
                {"name": "VENDORID", "type": "long", "nullable": False, "pii": "low"},
                {"name": "_extract_ts", "type": "timestamp", "nullable": False, "pii": "none"},
                {"name": "_source_pvo", "type": "string", "nullable": False, "pii": "none"},
                {"name": "_run_id", "type": "string", "nullable": False, "pii": "none"},
                {"name": "_watermark_used", "type": "timestamp", "nullable": True, "pii": "none"},
            ]
        },
        "quality": {"tests": []},
    }


class TestBronzeExtractImpl:
    def test_minimal_valid_bronze_impl_round_trips(self):
        impl = BronzeExtractImpl.model_validate(
            {
                "type": "bronze_extract",
                "datastore": "SupplierExtractPVO",
                "biccSchema": "Financial",
            }
        )
        assert impl.datastore == "SupplierExtractPVO"
        assert impl.bicc_schema == "Financial"
        assert impl.incremental_capable is True
        assert impl.audit_columns_mode == "bronze_v1"
        assert impl.pvo_id is None
        assert impl.schema_override is None

    def test_all_fields_round_trip(self):
        impl = BronzeExtractImpl.model_validate(
            {
                "type": "bronze_extract",
                "datastore": "SupplierExtractPVO",
                "pvo_id": "FscmTopModelAM.PrcExtractAM.PozBiccExtractAM.SupplierExtractPVO",
                "biccSchema": "Financial",
                "schemaOverride": "FinancialOverride",
                "incrementalCapable": False,
                "auditColumnsMode": "bronze_v1",
            }
        )
        assert impl.pvo_id.endswith(".SupplierExtractPVO")
        assert impl.schema_override == "FinancialOverride"
        assert impl.incremental_capable is False

    def test_missing_datastore_raises(self):
        with pytest.raises(ValidationError):
            BronzeExtractImpl.model_validate(
                {"type": "bronze_extract", "biccSchema": "Financial"}
            )

    def test_missing_bicc_schema_raises(self):
        with pytest.raises(ValidationError):
            BronzeExtractImpl.model_validate(
                {"type": "bronze_extract", "datastore": "SupplierExtractPVO"}
            )

    def test_extra_fields_forbidden(self):
        with pytest.raises(ValidationError):
            BronzeExtractImpl.model_validate(
                {
                    "type": "bronze_extract",
                    "datastore": "SupplierExtractPVO",
                    "biccSchema": "Financial",
                    "unknown_field": "x",
                }
            )

    def test_camel_case_aliases_accepted(self):
        impl = BronzeExtractImpl.model_validate(
            {
                "type": "bronze_extract",
                "datastore": "X",
                "biccSchema": "Y",
                "schemaOverride": "Z",
                "incrementalCapable": True,
                "auditColumnsMode": "bronze_v1",
            }
        )
        assert impl.bicc_schema == "Y"
        assert impl.schema_override == "Z"


class TestNodeYamlBronzeLayer:
    def test_valid_bronze_node_round_trips(self):
        node = NodeYaml.model_validate(_minimal_bronze_node())
        assert node.layer == "bronze"
        assert node.implementation.type == "bronze_extract"
        assert node.implementation.datastore == "SupplierExtractPVO"

    def test_bronze_seed_must_be_replace(self):
        data = _minimal_bronze_node()
        data["refresh"]["seed"]["strategy"] = "merge"
        with pytest.raises(ValueError, match="refresh.seed.strategy must be 'replace'"):
            NodeYaml.model_validate(data)

    def test_bronze_incremental_must_be_merge(self):
        data = _minimal_bronze_node()
        data["refresh"]["incremental"]["strategy"] = "append"
        with pytest.raises(ValueError, match="refresh.incremental.strategy"):
            NodeYaml.model_validate(data)

    def test_bronze_merge_requires_natural_key(self):
        data = _minimal_bronze_node()
        data["refresh"]["incremental"]["naturalKey"] = []
        with pytest.raises(ValueError, match=AIDPF_2020_MERGE_NO_NATURAL_KEY):
            NodeYaml.model_validate(data)

    def test_bronze_merge_requires_watermark(self):
        data = _minimal_bronze_node()
        data["refresh"]["incremental"].pop("watermark")
        with pytest.raises(ValueError, match=AIDPF_2050_MERGE_NO_WATERMARK):
            NodeYaml.model_validate(data)

    def test_bronze_node_without_incremental_is_ok(self):
        data = _minimal_bronze_node()
        data["refresh"].pop("incremental")
        node = NodeYaml.model_validate(data)
        assert node.refresh.incremental is None

    def test_bronze_layer_literal_accepted(self):
        node = NodeYaml.model_validate(_minimal_bronze_node())
        assert node.layer == "bronze"


def test_error_code_constants_exist():
    assert AIDPF_2080_BRONZE_EXTRACT_PVO_NOT_IN_CATALOG == "AIDPF-2080"
    assert AIDPF_2081_BUNDLE_DATASET_NOT_IN_PACK == "AIDPF-2081"
