"""Unit tests for ``orchestrator/merge_helpers.py`` (Phase 2 Step 4).

The module is a re-export shim over the v1 helpers plus new Phase 2
composer functions. Tests cover:

* Re-exports are the same callable objects as the v1 originals.
* Public-named wrappers (``build_*``) match v1 behaviour exactly.
* ``compose_merge_sql`` assembles a NULL-safe MERGE statement with
  and without the optional payload-diff predicate, and rejects empty
  natural keys.
"""

from __future__ import annotations

import pytest

from oracle_ai_data_platform_fusion_autopilot.orchestrator import (
    _natural_key_join_sql as v1_natural_key,
    _payload_diff_predicate_sql as v1_payload_diff,
)
from oracle_ai_data_platform_fusion_autopilot.orchestrator import merge_helpers


# ---------------------------------------------------------------------------
# Re-export identity (the Phase 2 module exports the SAME callables)
# ---------------------------------------------------------------------------


class TestReExports:
    def test_natural_key_join_sql_re_export_is_same_callable(self) -> None:
        assert merge_helpers._natural_key_join_sql is v1_natural_key

    def test_payload_diff_predicate_re_export_is_same_callable(self) -> None:
        assert merge_helpers._payload_diff_predicate_sql is v1_payload_diff

    def test_ensure_target_schema_for_merge_re_exported(self) -> None:
        from oracle_ai_data_platform_fusion_autopilot.orchestrator import state as state_mod
        assert (
            merge_helpers._ensure_target_schema_for_merge
            is state_mod._ensure_target_schema_for_merge
        )


# ---------------------------------------------------------------------------
# Public-named wrappers — identical output to v1
# ---------------------------------------------------------------------------


class TestPublicWrappers:
    def test_build_natural_key_join_sql_single_column(self) -> None:
        sql = merge_helpers.build_natural_key_join_sql(["supplier_number"])
        assert sql == "target.supplier_number <=> src.supplier_number"

    def test_build_natural_key_join_sql_composite_key(self) -> None:
        sql = merge_helpers.build_natural_key_join_sql(["a", "b", "c"])
        assert (
            sql
            == "target.a <=> src.a AND target.b <=> src.b AND target.c <=> src.c"
        )

    def test_build_natural_key_join_sql_accepts_tuple(self) -> None:
        sql = merge_helpers.build_natural_key_join_sql(("a", "b"))
        assert sql == "target.a <=> src.a AND target.b <=> src.b"

    def test_build_natural_key_join_sql_accepts_string(self) -> None:
        sql = merge_helpers.build_natural_key_join_sql("solo_col")
        assert sql == "target.solo_col <=> src.solo_col"

    def test_build_natural_key_join_sql_custom_aliases(self) -> None:
        sql = merge_helpers.build_natural_key_join_sql(
            ["k"], target_alias="t", src_alias="s"
        )
        assert sql == "t.k <=> s.k"

    def test_build_natural_key_join_sql_empty_list_raises(self) -> None:
        with pytest.raises(ValueError):
            merge_helpers.build_natural_key_join_sql([])

    def test_build_payload_diff_predicate_skips_audit_columns(self) -> None:
        # _extract_ts is in BRONZE_AUDIT_COLUMNS; should be excluded.
        pred = merge_helpers.build_payload_diff_predicate_sql(
            ["payload_col", "_extract_ts"]
        )
        assert pred == "target.payload_col IS DISTINCT FROM src.payload_col"

    def test_build_payload_diff_predicate_all_audit_returns_none(self) -> None:
        from oracle_ai_data_platform_fusion_autopilot.orchestrator import (
            BRONZE_AUDIT_COLUMNS,
        )
        assert (
            merge_helpers.build_payload_diff_predicate_sql(list(BRONZE_AUDIT_COLUMNS))
            is None
        )


# ---------------------------------------------------------------------------
# compose_merge_sql — Phase 2 statement assembly
# ---------------------------------------------------------------------------


class TestComposeMergeSql:
    def test_basic_merge_shape(self) -> None:
        sql = merge_helpers.compose_merge_sql(
            target="catalog.silver.dim_thing",
            source_sql="SELECT * FROM staging WHERE 1=1",
            natural_key=["thing_id"],
        )
        # Shape: MERGE INTO target AS target USING (source) AS src ON ... WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *
        assert "MERGE INTO catalog.silver.dim_thing AS target" in sql
        assert "USING (SELECT * FROM staging WHERE 1=1) AS src" in sql
        assert "ON target.thing_id <=> src.thing_id" in sql
        assert "WHEN MATCHED THEN UPDATE SET *" in sql
        assert "WHEN NOT MATCHED THEN INSERT *" in sql
        # No trailing semicolon (PLAN §9.4 — one statement, no terminator).
        assert not sql.rstrip().endswith(";")

    def test_composite_key_uses_null_safe_join(self) -> None:
        sql = merge_helpers.compose_merge_sql(
            target="t",
            source_sql="SELECT 1",
            natural_key=["a", "b"],
        )
        assert "ON target.a <=> src.a AND target.b <=> src.b" in sql

    def test_with_payload_diff_predicate_gates_update(self) -> None:
        sql = merge_helpers.compose_merge_sql(
            target="t",
            source_sql="SELECT 1",
            natural_key=["k"],
            payload_diff_predicate="target.col IS DISTINCT FROM src.col",
        )
        # MATCHED clause becomes conditional.
        assert (
            "WHEN MATCHED AND (target.col IS DISTINCT FROM src.col) THEN UPDATE SET *"
            in sql
        )
        # WHEN NOT MATCHED THEN INSERT * is unchanged.
        assert "WHEN NOT MATCHED THEN INSERT *" in sql

    def test_without_payload_diff_uses_unconditional_update(self) -> None:
        sql = merge_helpers.compose_merge_sql(
            target="t",
            source_sql="SELECT 1",
            natural_key=["k"],
            payload_diff_predicate=None,
        )
        # No AND clause in the WHEN MATCHED.
        assert "WHEN MATCHED THEN UPDATE SET *" in sql
        assert "AND" not in sql.split("WHEN MATCHED")[1].split("THEN")[0]

    def test_custom_aliases(self) -> None:
        sql = merge_helpers.compose_merge_sql(
            target="t",
            source_sql="SELECT 1",
            natural_key=["k"],
            target_alias="tgt",
            src_alias="incoming",
        )
        assert "MERGE INTO t AS tgt" in sql
        assert "USING (SELECT 1) AS incoming" in sql
        assert "ON tgt.k <=> incoming.k" in sql

    def test_empty_natural_key_raises(self) -> None:
        with pytest.raises(ValueError, match="natural_key is empty"):
            merge_helpers.compose_merge_sql(
                target="t",
                source_sql="SELECT 1",
                natural_key=[],
            )

    def test_source_with_parameter_markers_passes_through(self) -> None:
        """Param markers in source_sql are preserved verbatim — Spark binds
        them when the caller passes args=..."""
        sql = merge_helpers.compose_merge_sql(
            target="t",
            source_sql="SELECT * FROM s WHERE _extract_ts > :watermark_x",
            natural_key=["k"],
        )
        assert ":watermark_x" in sql
