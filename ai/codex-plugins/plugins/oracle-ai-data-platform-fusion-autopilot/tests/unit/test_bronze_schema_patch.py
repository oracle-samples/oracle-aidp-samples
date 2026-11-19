"""Pure-core tests for ``schema.bronze_schema_patch`` (100% branch, NFR-3).

The nullable/metadata fixture is load-bearing (review round 2/3): only
``ddl_type`` may ever change; ``nullable=False`` and non-empty field
``metadata`` must survive patch application bit-for-bit.
"""

from __future__ import annotations

import pytest

from oracle_ai_data_platform_fusion_autopilot.schema.bronze_schema_patch import (
    CastBackEntry,
    FieldDescriptor,
    SchemaPatchError,
    apply_schema_patches,
    collision_free_temp_names,
    validate_patch_type,
    validate_schema_patches,
)

BASE = [
    FieldDescriptor("ItemBasePEOInventoryItemId", "decimal(18,0)"),
    FieldDescriptor("ItemBasePEOMaterialCost", "decimal(38,0)"),
    FieldDescriptor("ItemBasePEOItemType", "string"),
    # Non-default attributes MUST pass through untouched.
    FieldDescriptor("StrictCol", "bigint", nullable=False,
                    metadata={"comment": "load-bearing"}),
]


class TestValidatePatchType:
    @pytest.mark.parametrize("t", ["bigint", "LONG", "int", "integer",
                                   "double", "float", "string", "boolean",
                                   "date", "timestamp", " Bigint "])
    def test_simple_types_accepted_normalized(self, t: str) -> None:
        assert validate_patch_type(t) == t.strip().lower()

    @pytest.mark.parametrize("t", ["decimal(38,0)", "decimal(10, 2)",
                                   "decimal(1,0)", "decimal(38,38)"])
    def test_valid_decimals_accepted(self, t: str) -> None:
        assert validate_patch_type(t).startswith("decimal(")

    @pytest.mark.parametrize("t", ["decimal(99,99)", "decimal(0,0)",
                                   "decimal(39,0)", "decimal(10,11)"])
    def test_semantic_decimal_bounds_rejected(self, t: str) -> None:
        with pytest.raises(SchemaPatchError):
            validate_patch_type(t)

    @pytest.mark.parametrize("t", ["binary", "array<int>", "struct<a:int>",
                                   "DROP TABLE x", "", "decimal(38)"])
    def test_disallowed_types_rejected(self, t: str) -> None:
        with pytest.raises(SchemaPatchError):
            validate_patch_type(t)


class TestValidateSchemaPatches:
    def test_valid_map_normalizes_types(self) -> None:
        out = validate_schema_patches({"ItemBasePEOMaterialCost": "BIGINT"})
        assert out == {"ItemBasePEOMaterialCost": "bigint"}

    def test_bad_identifier_key_rejected(self) -> None:
        with pytest.raises(SchemaPatchError):
            validate_schema_patches({"bad-name": "bigint"})

    def test_casefold_duplicate_keys_rejected(self) -> None:
        with pytest.raises(SchemaPatchError) as exc:
            validate_schema_patches({"Amount": "bigint", "amount": "string"})
        assert "collide case-insensitively" in str(exc.value)

    def test_non_string_key_rejected(self) -> None:
        with pytest.raises(SchemaPatchError):
            validate_schema_patches({1: "bigint"})  # type: ignore[dict-item]


class TestApplySchemaPatches:
    def test_live_pair_patches_only_ddl_type(self) -> None:
        patched, plan = apply_schema_patches(
            BASE, {"ItemBasePEOMaterialCost": "bigint"}
        )
        assert [f.name for f in patched] == [f.name for f in BASE]
        mc = next(f for f in patched if f.name == "ItemBasePEOMaterialCost")
        assert mc.ddl_type == "bigint"
        assert plan == [CastBackEntry(
            column="ItemBasePEOMaterialCost", patch_type="bigint",
            declared_type="decimal(38,0)", metadata={},
        )]

    def test_nullable_and_metadata_pass_through_bit_for_bit(self) -> None:
        patched, plan = apply_schema_patches(BASE, {"StrictCol": "string"})
        sc = next(f for f in patched if f.name == "StrictCol")
        assert sc.nullable is False
        assert dict(sc.metadata) == {"comment": "load-bearing"}
        assert plan[0].metadata == {"comment": "load-bearing"}
        # Unmatched fields are the SAME descriptors (untouched).
        assert patched[0] is BASE[0]

    def test_case_insensitive_match_keeps_connector_casing(self) -> None:
        patched, plan = apply_schema_patches(
            BASE, {"itembasepeomaterialcost": "bigint"}
        )
        assert plan[0].column == "ItemBasePEOMaterialCost"
        assert any(f.name == "ItemBasePEOMaterialCost" and f.ddl_type == "bigint"
                   for f in patched)

    def test_noop_patch_dropped_from_both_outputs(self) -> None:
        patched, plan = apply_schema_patches(
            BASE, {"ItemBasePEOMaterialCost": "decimal(38,0)"}
        )
        assert plan == []
        mc = next(f for f in patched if f.name == "ItemBasePEOMaterialCost")
        assert mc.ddl_type == "decimal(38,0)"

    def test_unknown_column_raises(self) -> None:
        with pytest.raises(SchemaPatchError) as exc:
            apply_schema_patches(BASE, {"NoSuchColumn": "bigint"})
        assert "NoSuchColumn" in str(exc.value)

    def test_invalid_patch_type_raises_before_matching(self) -> None:
        with pytest.raises(SchemaPatchError):
            apply_schema_patches(BASE, {"ItemBasePEOItemType": "array<int>"})

    def test_empty_patches_are_identity(self) -> None:
        patched, plan = apply_schema_patches(BASE, {})
        assert patched == list(BASE) and plan == []


class TestCollisionFreeTempNames:
    def test_plain_generation(self) -> None:
        assert collision_free_temp_names(["a", "b"], 2) == [
            "__patch_raw_0", "__patch_raw_1",
        ]

    def test_collision_with_pvo_column_skipped_case_insensitively(self) -> None:
        names = collision_free_temp_names(
            ["__PATCH_RAW_0", "x"], 2
        )
        assert names == ["__patch_raw_1", "__patch_raw_2"]
        assert all(n.casefold() != "__patch_raw_0" for n in names)

    def test_zero_count(self) -> None:
        assert collision_free_temp_names(["a"], 0) == []


class TestDatasetSpecKnob:
    """`DatasetSpec.schemaPatches` — bundle-load fail-closed validation."""

    def _spec(self, patches):
        from oracle_ai_data_platform_fusion_autopilot.schema.bundle import (
            DatasetSpec,
        )

        return DatasetSpec.model_validate(
            {"id": "scm_items", "mode": "full", "schemaPatches": patches}
        )

    def test_valid_patch_accepted_and_normalized(self) -> None:
        spec = self._spec({"ItemBasePEOMaterialCost": "BIGINT"})
        assert spec.schema_patches == {"ItemBasePEOMaterialCost": "bigint"}

    def test_default_is_empty(self) -> None:
        from oracle_ai_data_platform_fusion_autopilot.schema.bundle import (
            DatasetSpec,
        )

        assert DatasetSpec.model_validate({"id": "gl_coa"}).schema_patches == {}

    @pytest.mark.parametrize("patches", [
        {"Amount": "bigint", "amount": "string"},   # casefold duplicate
        {"bad-name": "bigint"},                     # identifier violation
        {"Col": "decimal(99,99)"},                  # semantic precision
        {"Col": "decimal(10,11)"},                  # semantic scale
        {"Col": "decimal(0,0)"},                    # semantic precision low
        {"Col": "array<int>"},                      # disallowed type
    ])
    def test_invalid_patches_fail_bundle_load(self, patches) -> None:
        with pytest.raises(Exception) as exc:
            self._spec(patches)
        assert "schemaPatches" in str(exc.value)

    def test_extra_forbid_untouched(self) -> None:
        from oracle_ai_data_platform_fusion_autopilot.schema.bundle import (
            DatasetSpec,
        )

        with pytest.raises(Exception):
            DatasetSpec.model_validate({"id": "x", "nope": 1})

    def test_round_trip_by_alias(self) -> None:
        spec = self._spec({"C": "bigint"})
        dumped = spec.model_dump(by_alias=True)
        assert dumped["schemaPatches"] == {"C": "bigint"}
