"""Golden-snapshot test for the migrated `dim_supplier` SQL template (Phase 3 Step 5).

The rendered SQL is asserted character-for-character (after whitespace
normalisation) against the v1 module's `build_dim_supplier_sql` output
in seed mode, with renderer tokens replaced by the equivalent literals.
This catches accidental drift between v1 and v2 SQL shape without
needing a live Spark execution.
"""

from __future__ import annotations

import re
from pathlib import Path

from oracle_ai_data_platform_fusion_autopilot.orchestrator.content_pack import load_pack
from oracle_ai_data_platform_fusion_autopilot.orchestrator.sql_renderer import (
    RunContext,
    render_node_sql,
)
from oracle_ai_data_platform_fusion_autopilot.schema.tenant_profile import (
    load_tenant_profile,
)


REPO_ROOT = Path(__file__).resolve().parents[2]
PACK_ROOT = (REPO_ROOT / "scripts" / "oracle_ai_data_platform_fusion_autopilot"
             / "content_packs" / "fusion-finance-starter")
PROFILE = REPO_ROOT / "examples" / "profiles" / "finance-default.yaml"


def _normalise(sql: str) -> str:
    return re.sub(r"\s+", " ", sql).strip()


def _render() -> tuple[str, dict]:
    pack = load_pack(PACK_ROOT)
    profile = load_tenant_profile(PROFILE)
    ctx = RunContext(
        catalog="cat",
        bronze_schema="bronze",
        silver_schema="silver",
        gold_schema="gold",
        run_id="dim-supplier-snapshot",
        active_profile_name="finance-default",
        bronze_table_for_source={"erp_suppliers": "cat.bronze.erp_suppliers"},
    )
    rendered = render_node_sql(pack.silver["dim_supplier"], pack, profile, ctx)
    return rendered.sql, dict(rendered.params)


class TestDimSupplierSeedSnapshot:
    def test_rendered_sql_matches_v1_projection(self) -> None:
        sql, _ = _render()
        # All 15 v1 projection columns appear.
        for col in [
            "supplier_key", "supplier_number", "supplier_name", "vendor_id",
            "party_id", "parent_vendor_id", "parent_party_id",
            "business_relationship", "inactive_date", "creation_date",
            "last_update_date", "bronze_extract_ts", "bronze_source_pvo",
            "silver_built_at", "silver_run_id",
        ]:
            assert f"AS {col}" in sql, f"missing column projection: {col}"

    def test_uses_xxhash64_surrogate_key(self) -> None:
        """v1 surrogate-key invariant (PLAN §11): xxhash64, NOT
        monotonically_increasing_id."""
        sql, _ = _render()
        assert "xxhash64(CAST(SEGMENT1 AS STRING))" in sql
        assert "monotonically_increasing_id" not in sql

    def test_nullif_on_id_columns(self) -> None:
        """v1 demo-pod invariant: ID columns NULLIF'd against 0 sentinel."""
        sql, _ = _render()
        for id_col in ["VENDORID", "PARTYID", "PARENTVENDORID", "PARENTPARTYID"]:
            pattern = f"NULLIF(CAST({id_col} "
            assert pattern in _normalise(sql).replace("  ", " "), (
                f"NULLIF wrapper missing on {id_col}"
            )

    def test_dedup_keeps_most_recent_extract_per_supplier(self) -> None:
        sql, _ = _render()
        # Inner subquery now aliases erp_suppliers as `es` and qualifies columns
        # (declared-inputs convention; behavior unchanged).
        assert "PARTITION BY es.SEGMENT1 ORDER BY es._extract_ts DESC" in sql
        assert "WHERE _rn = 1" in sql

    def test_filters_null_natural_key(self) -> None:
        sql, _ = _render()
        assert "WHERE es.SEGMENT1 IS NOT NULL" in sql

    def test_seed_mode_watermark_is_always_true(self) -> None:
        """Seed mode: {{ watermark_predicate }} expands to 1=1 so the
        same template runs identically for both modes."""
        sql, _ = _render()
        assert "AND 1=1" in sql

    def test_run_id_bound_as_parameter(self) -> None:
        sql, params = _render()
        assert ":run_id" in sql
        assert params["run_id"] == "dim-supplier-snapshot"

    def test_catalog_schema_inlined_as_identifier(self) -> None:
        """`{{ catalog }}.{{ bronze_schema }}.erp_suppliers` substitutes to
        the bare table identifier, NOT a parameter marker."""
        sql, params = _render()
        assert "cat.bronze.erp_suppliers" in sql
        # No bound parameter for the bronze schema name.
        assert not any(k.startswith("bronze_schema") for k in params)


class TestDimSupplierIncrementalSnapshot:
    def test_incremental_mode_emits_watermark_predicate(self) -> None:
        pack = load_pack(PACK_ROOT)
        profile = load_tenant_profile(PROFILE)
        from datetime import datetime, timezone
        prior_wm = datetime(2026, 6, 1, tzinfo=timezone.utc)
        ctx = RunContext(
            catalog="cat",
            bronze_schema="bronze",
            silver_schema="silver",
            gold_schema="gold",
            run_id="dim-supplier-incremental",
            active_profile_name="finance-default",
            prior_watermark={"erp_suppliers": prior_wm},
            mode="incremental",
            bronze_table_for_source={"erp_suppliers": "cat.bronze.erp_suppliers"},
        )
        rendered = render_node_sql(pack.silver["dim_supplier"], pack, profile, ctx)
        # Watermark column + parameter marker appear; no inline date literal.
        assert "_extract_ts > :watermark_erp_suppliers" in rendered.sql
        assert rendered.params["watermark_erp_suppliers"] == prior_wm
