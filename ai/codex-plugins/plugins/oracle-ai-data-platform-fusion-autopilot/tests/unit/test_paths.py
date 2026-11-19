"""Unit tests for ``config/paths.py`` — the P1.5b foundation.

Tests cover:
* Default values match the pre-refactor module-level constants byte-for-byte.
* ``from_bundle`` accepts raw dicts (yaml.safe_load output), Pydantic models
  (via duck-typed ``model_dump``), and rejects everything else.
* Identifier validation rejects non-strings, empty values, leading-digit
  identifiers, hyphens, dots, and SQL-injection-shaped strings.
* ``TablePaths`` is frozen + hash-equal under same fields.
"""

from __future__ import annotations

from dataclasses import FrozenInstanceError

import pytest
from oracle_ai_data_platform_fusion_autopilot.config.paths import (
    DEFAULT_PATHS,
    TablePaths,
)


class TestDefaultPathsMatchPreRefactorConstants:
    """1. Defaults must reproduce every shipped module's ``Final[str]``
    constant byte-for-byte so backwards-compat is preserved.
    """

    def test_bronze_table_strings(self) -> None:
        assert DEFAULT_PATHS.bronze("ap_invoices")        == "fusion_catalog.bronze.ap_invoices"
        assert DEFAULT_PATHS.bronze("gl_period_balances") == "fusion_catalog.bronze.gl_period_balances"
        assert DEFAULT_PATHS.bronze("gl_coa")             == "fusion_catalog.bronze.gl_coa"
        assert DEFAULT_PATHS.bronze("erp_suppliers")      == "fusion_catalog.bronze.erp_suppliers"

    def test_silver_table_strings(self) -> None:
        assert DEFAULT_PATHS.silver("dim_supplier") == "fusion_catalog.silver.dim_supplier"
        assert DEFAULT_PATHS.silver("dim_account")  == "fusion_catalog.silver.dim_account"
        assert DEFAULT_PATHS.silver("dim_calendar") == "fusion_catalog.silver.dim_calendar"

    def test_gold_table_strings(self) -> None:
        assert DEFAULT_PATHS.gold("supplier_spend")              == "fusion_catalog.gold.supplier_spend"
        assert DEFAULT_PATHS.gold("gl_balance")                  == "fusion_catalog.gold.gl_balance"
        assert DEFAULT_PATHS.gold("ap_aging")                    == "fusion_catalog.gold.ap_aging"
        assert DEFAULT_PATHS.gold("ap_outstanding_by_invoice_age") == "fusion_catalog.gold.ap_outstanding_by_invoice_age"


class TestFromBundle:
    """2-7. ``from_bundle`` behavior across input variations."""

    def test_full_aidp_block(self) -> None:
        paths = TablePaths.from_bundle({
            "aidp": {
                "catalog":      "my_lake",
                "bronzeSchema": "raw",
                "silverSchema": "clean",
                "goldSchema":   "marts",
            }
        })
        assert paths == TablePaths(
            catalog="my_lake", bronze_schema="raw",
            silver_schema="clean", gold_schema="marts",
        )

    def test_empty_dict_uses_defaults(self) -> None:
        assert TablePaths.from_bundle({}) == TablePaths()

    def test_aidp_is_none_uses_defaults(self) -> None:
        assert TablePaths.from_bundle({"aidp": None}) == TablePaths()

    def test_partial_aidp_block(self) -> None:
        paths = TablePaths.from_bundle({"aidp": {"catalog": "my_lake"}})
        assert paths.catalog       == "my_lake"
        assert paths.bronze_schema == "bronze"   # default
        assert paths.silver_schema == "silver"   # default
        assert paths.gold_schema   == "gold"     # default

    def test_snake_case_keys(self) -> None:
        """Pydantic ``model_dump(by_alias=False)`` emits snake_case."""
        paths = TablePaths.from_bundle({
            "aidp": {
                "catalog":       "my_lake",
                "bronze_schema": "raw",
                "silver_schema": "clean",
                "gold_schema":   "marts",
            }
        })
        assert paths == TablePaths("my_lake", "raw", "clean", "marts")

    def test_camel_case_keys(self) -> None:
        """Raw YAML / Pydantic ``model_dump(by_alias=True)`` emits camelCase."""
        paths = TablePaths.from_bundle({
            "aidp": {
                "catalog":      "my_lake",
                "bronzeSchema": "raw",
                "silverSchema": "clean",
                "goldSchema":   "marts",
            }
        })
        assert paths == TablePaths("my_lake", "raw", "clean", "marts")


class TestFromBundleValidation:
    """8, 9, 10. Rejection paths for non-string / non-mapping / wrong-shape input."""

    def test_rejects_non_string_value(self) -> None:
        """Reviewer round-9: NO ``str()`` coercion. Non-string config values
        raise TypeError naming the offending key — Spark SQL interpolation is
        unsafe with type-confused inputs.
        """
        with pytest.raises(TypeError, match=r"catalog"):
            TablePaths.from_bundle({"aidp": {"catalog": 42}})

    def test_rejects_non_string_bronze_schema(self) -> None:
        with pytest.raises(TypeError, match=r"bronzeSchema"):
            TablePaths.from_bundle({"aidp": {"bronzeSchema": True}})

    def test_accepts_pydantic_style_model(self) -> None:
        """Duck-typed: any object exposing ``.model_dump()`` is normalised."""

        class FakeBundle:
            def model_dump(self, by_alias: bool = False) -> dict:
                # Mirrors what Pydantic v2 returns with by_alias=True.
                return {
                    "aidp": {
                        "catalog":      "from_model",
                        "bronzeSchema": "bz",
                        "silverSchema": "sv",
                        "goldSchema":   "gd",
                    }
                }

        paths = TablePaths.from_bundle(FakeBundle())
        assert paths == TablePaths("from_model", "bz", "sv", "gd")

    def test_rejects_unsupported_input_type(self) -> None:
        """A bare list / int / string isn't a config dict — raise."""
        with pytest.raises(TypeError, match=r"Mapping"):
            TablePaths.from_bundle(["not", "a", "dict"])  # type: ignore[arg-type]

    def test_rejects_non_mapping_aidp_field(self) -> None:
        with pytest.raises(TypeError, match=r"aidp"):
            TablePaths.from_bundle({"aidp": "not-a-dict"})  # type: ignore[dict-item]


class TestHelpersAndValidation:
    """11-14. The bronze/silver/gold helpers + identifier validation."""

    def test_bronze_silver_gold_helpers_emit_three_part(self) -> None:
        p = TablePaths(catalog="cat", bronze_schema="b", silver_schema="s", gold_schema="g")
        assert p.bronze("t") == "cat.b.t"
        assert p.silver("t") == "cat.s.t"
        assert p.gold("t")   == "cat.g.t"

    def test_construction_rejects_empty_component(self) -> None:
        with pytest.raises(ValueError, match=r"catalog"):
            TablePaths(catalog="")
        with pytest.raises(ValueError, match=r"bronze_schema"):
            TablePaths(bronze_schema="")

    def test_construction_rejects_invalid_identifier(self) -> None:
        for bad_catalog in ("my-lake", "123abc", "a; DROP TABLE x;--", "a.b"):
            with pytest.raises(ValueError, match=r"valid unquoted SQL identifier"):
                TablePaths(catalog=bad_catalog)

    def test_construction_rejects_non_string_field(self) -> None:
        """Even bypassing from_bundle, direct construction with non-string
        types must fail (defends against programmatic misuse).
        """
        with pytest.raises(TypeError, match=r"catalog"):
            TablePaths(catalog=42)  # type: ignore[arg-type]

    def test_helpers_reject_invalid_table_name(self) -> None:
        p = TablePaths()
        for bad_table in ("123abc", "a.b", "with space", "a-b", "a; DROP TABLE x"):
            with pytest.raises(ValueError, match=r"valid unquoted SQL identifier"):
                p.bronze(bad_table)


class TestFrozenAndEquality:
    """15, 16. ``TablePaths`` is immutable + hash-equal under same fields."""

    def test_is_frozen(self) -> None:
        p = TablePaths()
        with pytest.raises(FrozenInstanceError):
            p.catalog = "new_cat"  # type: ignore[misc]

    def test_hash_equality(self) -> None:
        a = TablePaths("cat", "bz", "sv", "gd")
        b = TablePaths("cat", "bz", "sv", "gd")
        c = TablePaths("cat", "bz", "sv", "different")
        assert a == b
        assert hash(a) == hash(b)
        assert a != c


class TestModuleExports:
    """17, 18. The module's public surface + singleton identity."""

    def test_default_paths_is_module_constant(self) -> None:
        from oracle_ai_data_platform_fusion_autopilot.config import paths as paths_mod
        assert paths_mod.DEFAULT_PATHS is DEFAULT_PATHS

    def test_exports_listed_in_all(self) -> None:
        from oracle_ai_data_platform_fusion_autopilot.config import paths as paths_mod
        assert "TablePaths"   in paths_mod.__all__
        assert "DEFAULT_PATHS" in paths_mod.__all__
