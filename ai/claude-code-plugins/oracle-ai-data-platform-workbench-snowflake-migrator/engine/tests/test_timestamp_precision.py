"""Nanosecond timestamps lose their last three digits, and must say so.

Review 2026-09-25. catalog._columns SELECTED datetime_precision and then
nothing read it: map_type had no parameter for it, TIMESTAMP_NTZ in the
default preserve mode got no warning at all, and the LTZ/TZ warnings spoke
only of timezone semantics. Precision 9 is Snowflake's DEFAULT (600 live
columns in artifacts-scale), and Spark stores microseconds, so every
sub-microsecond part is dropped at the read -- while the plan showed the
column supported with no caveat, and the counts+sums verification sums
DECIMAL columns only, so nothing downstream would notice either.

Live-verified: INFORMATION_SCHEMA.COLUMNS reports DATETIME_PRECISION 9 for
TIMESTAMP_TZ(9). TIME is already carried as STRING (text preserved), so it
gets no precision warning.
"""
import pathlib

import pytest

from fake_sql import FakeSql
from snowflake_source.dialect.types import map_type
from snowflake_source.extract.catalog import build_inventory
from snowflake_source.extract.manifest import inventory_from_manifest

from test_catalog import _base_responses, _col
from test_data_migration_scripts import _FakeSource, discover  # noqa: F401


@pytest.mark.parametrize("dt", ["TIMESTAMP_NTZ", "TIMESTAMP_LTZ",
                                "TIMESTAMP_TZ"])
def test_precision_above_microseconds_warns_and_names_it(dt):
    m = map_type(dt, datetime_precision=9)
    assert m.warning and "precision 9" in m.warning, m.warning
    assert "microsecond" in m.warning and "truncated" in m.warning


@pytest.mark.parametrize("precision", [None, 0, 3, 6])
def test_microseconds_or_less_is_no_precision_warning(precision):
    m = map_type("TIMESTAMP_NTZ", datetime_precision=precision)
    assert m.warning is None


def test_the_timezone_warning_is_kept_alongside_the_precision_one():
    m = map_type("TIMESTAMP_TZ", datetime_precision=9)
    assert "timezone" in m.warning and "precision 9" in m.warning


def test_the_ntz_downgrade_keeps_both_warnings():
    m = map_type("TIMESTAMP_NTZ", datetime_precision=9,
                 timestamp_ntz="timestamp")
    assert m.spark_type == "TIMESTAMP"
    assert "TIMEZONE SEMANTICS DIFFER" in m.warning
    assert "precision 9" in m.warning


def test_time_is_text_so_it_gets_no_precision_warning():
    m = map_type("TIME", datetime_precision=9)
    assert "precision" not in m.warning


def test_the_live_read_carries_the_warning_onto_the_record():
    col = {**_col("T", name="TS", dtype="TIMESTAMP_NTZ"),
           "CHARACTER_MAXIMUM_LENGTH": None, "DATETIME_PRECISION": 9}
    rec = build_inventory(FakeSql(_base_responses(
        tables=[{"name": "T", "rows": 1}], columns=[col])),
        row_counts="none")["inventory"][0]
    assert any(w.startswith("TS:") and "precision 9" in w
               for w in rec["warnings"]), rec["warnings"]
    assert rec["compatibility_status"] == "supported", "a warning, not a block"


def test_the_manifest_bridge_reaches_the_same_verdict():
    m = {"schemas": [{"name": "S", "views": [], "errors": [], "tables": [
        {"name": "T", "source_rows": 1, "columns": [
            {"name": "TS", "type": "TIMESTAMP_NTZ", "nullable": True,
             "data_type": "TIMESTAMP_NTZ", "numeric_precision": None,
             "numeric_scale": None, "character_maximum_length": None,
             "datetime_precision": 9}]}]}]}
    rec = inventory_from_manifest(m, database="DB")["inventory"][0]
    live = map_type("TIMESTAMP_NTZ", datetime_precision=9)
    assert rec["warnings"] == [f"TS: {live.warning}"]


def test_discovery_selects_the_precision_and_writes_it(discover):  # noqa: F811
    sql = (pathlib.Path(__file__).resolve().parents[1] / "dataplane"
           / "00_discover_snowflake.py").read_text(encoding="utf-8")
    block = sql[sql.index("_COLUMNS_SQL = ("):]
    block = block[:block.index("\n\n")]
    assert "DATETIME_PRECISION" in block
    source = _FakeSource(
        tables=[{"TABLE_SCHEMA": "S", "TABLE_NAME": "T",
                 "TABLE_TYPE": "BASE TABLE", "ROW_COUNT": 1, "BYTES": 1}],
        columns=[{"TABLE_SCHEMA": "S", "TABLE_NAME": "T",
                  "COLUMN_NAME": "TS", "ORDINAL_POSITION": 1,
                  "DATA_TYPE": "TIMESTAMP_TZ", "IS_NULLABLE": "YES",
                  "NUMERIC_PRECISION": None, "NUMERIC_SCALE": None,
                  "CHARACTER_MAXIMUM_LENGTH": None,
                  "DATETIME_PRECISION": 9}])
    schemas = discover.discover_via_connector(source, wanted=None,
                                              exclude=set())
    assert schemas[0]["tables"][0]["columns"][0]["datetime_precision"] == 9


def test_the_truncation_reaches_the_ddl_plan_statement():
    """The reviewer reads DDL_PLAN.md, not inventory.json."""
    from target.ddl import build_create_table
    col = {**_col("T", name="TS", dtype="TIMESTAMP_NTZ"),
           "CHARACTER_MAXIMUM_LENGTH": None, "DATETIME_PRECISION": 9}
    rec = build_inventory(FakeSql(_base_responses(
        tables=[{"name": "T", "rows": 1}], columns=[col])),
        row_counts="none")["inventory"][0]
    res = build_create_table(rec, "cat.sch.t")
    assert not res.blocked
    assert any("precision 9" in w for w in res.warnings), res.warnings
