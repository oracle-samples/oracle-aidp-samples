"""Unit tests for ``dimensions/dim_calendar.py``.

Target the SQL string output of :func:`build_dim_calendar_sql` and the input
validation. Spark wrapper :func:`build` is not unit-tested directly (same
"compose vs execute" split as ``dim_supplier``).
"""

from __future__ import annotations

import re

import pytest
from oracle_ai_data_platform_fusion_autopilot.dimensions import dim_calendar
from oracle_ai_data_platform_fusion_autopilot.dimensions.dim_calendar import (
    DEFAULT_END_DATE,
    DEFAULT_FISCAL_START_MONTH,
    DEFAULT_START_DATE,
    TARGET_SILVER_TABLE,
    build_dim_calendar_sql,
)


class TestConstants:
    def test_target_silver_table_three_part(self) -> None:
        assert TARGET_SILVER_TABLE == "fusion_catalog.silver.dim_calendar"

    def test_default_dates_iso_format(self) -> None:
        assert re.match(r"^\d{4}-\d{2}-\d{2}$", DEFAULT_START_DATE)
        assert re.match(r"^\d{4}-\d{2}-\d{2}$", DEFAULT_END_DATE)
        assert DEFAULT_START_DATE < DEFAULT_END_DATE

    def test_default_fiscal_start_month_is_calendar(self) -> None:
        assert DEFAULT_FISCAL_START_MONTH == 1


class TestSqlBuilder:
    def test_uses_create_or_replace_delta(self) -> None:
        sql = build_dim_calendar_sql()
        assert "CREATE OR REPLACE TABLE" in sql
        assert "USING DELTA" in sql

    def test_uses_sequence_for_date_range(self) -> None:
        sql = build_dim_calendar_sql(start_date="2020-01-01", end_date="2030-12-31")
        assert "EXPLODE(" in sql
        assert "sequence(DATE'2020-01-01', DATE'2030-12-31', INTERVAL 1 DAY)" in sql

    def test_surrogate_key_is_yyyymmdd(self) -> None:
        """calendar_key must be deterministic — date in YYYYMMDD form, cast BIGINT."""
        sql = build_dim_calendar_sql()
        assert re.search(
            r"CAST\(date_format\(calendar_date, 'yyyyMMdd'\) AS BIGINT\)\s+AS calendar_key",
            sql,
        )

    def test_emits_all_calendar_columns(self) -> None:
        sql = build_dim_calendar_sql()
        for col in (
            "calendar_key", "calendar_date", "year", "quarter", "month",
            "month_name", "day_of_month", "day_of_week", "day_name",
            "week_of_year", "is_weekend", "fiscal_year", "fiscal_period",
            "fiscal_quarter", "silver_built_at",
        ):
            assert f"AS {col}" in sql, f"missing column: {col}"

    def test_is_weekend_uses_dayofweek_1_or_7(self) -> None:
        sql = build_dim_calendar_sql()
        assert re.search(
            r"DAYOFWEEK\(calendar_date\)\s+IN\s*\(1,\s*7\)", sql
        ), "is_weekend should mark Sunday(1) and Saturday(7) per Spark DAYOFWEEK semantics"

    def test_fiscal_calendar_year_branch_is_noop(self) -> None:
        """fiscal_start_month=1 → fiscal_year math should not advance the year."""
        sql = build_dim_calendar_sql(fiscal_start_month=1)
        # The fy_advance constant should be 0 when fiscal_start_month=1 — verify literal `+ 0`
        assert re.search(r"YEAR\(calendar_date\)\s*\+\s*0", sql), (
            "fiscal_start_month=1 should produce `YEAR(calendar_date) + 0` (no advance)"
        )

    def test_fiscal_july_start_advances_year(self) -> None:
        """fiscal_start_month=7 (Jul-Jun) → Jul-Dec → FY+1, Jan-Jun → FY."""
        sql = build_dim_calendar_sql(fiscal_start_month=7)
        # fy_advance = 1 → expression includes `+ 1`
        assert re.search(r"YEAR\(calendar_date\)\s*\+\s*1", sql)
        # And the WHEN branch threshold is 7
        assert re.search(r"MONTH\(calendar_date\)\s*>=\s*7", sql)

    def test_fiscal_period_uses_correct_arithmetic(self) -> None:
        """fiscal_period = (month - start + 1) when month >= start, else (month + 12 - start + 1)."""
        sql = build_dim_calendar_sql(fiscal_start_month=7)
        # Forward branch: MONTH - 7 + 1
        assert re.search(r"MONTH\(calendar_date\)\s*-\s*7\s*\+\s*1", sql)
        # Wrap-around branch: MONTH + 12 - 7 + 1
        assert re.search(r"MONTH\(calendar_date\)\s*\+\s*12\s*-\s*7\s*\+\s*1", sql)

    def test_fiscal_quarter_uses_ceil_div_3(self) -> None:
        sql = build_dim_calendar_sql()
        assert "CEIL(" in sql
        assert "/ 3.0" in sql

    def test_invalid_fiscal_start_month_raises(self) -> None:
        for bad in (0, 13, -1, 100):
            with pytest.raises(ValueError, match="fiscal_start_month"):
                build_dim_calendar_sql(fiscal_start_month=bad)

    def test_uses_custom_table_name(self) -> None:
        sql = build_dim_calendar_sql(silver_table="x.y.z_calendar")
        assert "CREATE OR REPLACE TABLE x.y.z_calendar" in sql
        assert "fusion_catalog.silver.dim_calendar" not in sql

    def test_emits_silver_built_at(self) -> None:
        sql = build_dim_calendar_sql()
        assert "current_timestamp()" in sql
        assert "AS silver_built_at" in sql


class TestModuleExports:
    def test_all_includes_public_surface(self) -> None:
        expected = {
            "TARGET_SILVER_TABLE",
            "DEFAULT_START_DATE",
            "DEFAULT_END_DATE",
            "DEFAULT_FISCAL_START_MONTH",
            "build",
            "build_dim_calendar_sql",
        }
        assert expected.issubset(set(dim_calendar.__all__))


class TestPathsThreading:
    """P1.5b — tenant-aware table-path resolution."""

    def test_paths_none_matches_pre_refactor_defaults(self) -> None:
        sql = build_dim_calendar_sql()
        assert "fusion_catalog.silver.dim_calendar" in sql

    def test_paths_threading_replaces_catalog(self) -> None:
        from oracle_ai_data_platform_fusion_autopilot.config.paths import TablePaths
        sql = build_dim_calendar_sql(paths=TablePaths(catalog="my_lake"))
        assert "my_lake.silver.dim_calendar" in sql
        assert "fusion_catalog" not in sql

    def test_explicit_silver_table_wins_over_paths(self) -> None:
        from oracle_ai_data_platform_fusion_autopilot.config.paths import TablePaths
        sql = build_dim_calendar_sql(
            paths=TablePaths(catalog="my_lake"),
            silver_table="explicit.thing.Y",
        )
        assert "explicit.thing.Y" in sql
        assert "my_lake.silver.dim_calendar" not in sql
