"""A collated text column is not a plain STRING, and must not read as one.

Review 2026-09-25. Neither column read selected COLLATION_NAME and map_type
had no collation input, so `VARCHAR(20) COLLATE 'en-ci'` was mapped to a
bytewise Delta STRING with only "declared length 20 is not enforced" to say
anything had happened, and the table was reported `supported`. The live
artifacts-types T_COLLATE came out exactly that way. After cutover a
case-insensitive column compares case-sensitively: 'abc' = 'ABC' goes from
true to false, and joins, GROUP BY / DISTINCT, ORDER BY and uniqueness all
move with it -- while row-count reconciliation still passes.

Live-verified: INFORMATION_SCHEMA.COLUMNS reports COLLATION_NAME 'en-ci' for
VARCHAR(20) COLLATE 'en-ci'. A collation is a WARNING, not a block: the
text itself arrives intact, but the comparison semantics do not.
"""
import pathlib

from fake_sql import FakeSql
from snowflake_source.dialect.types import map_type
from snowflake_source.extract.catalog import build_inventory
from snowflake_source.extract.manifest import inventory_from_manifest

from test_catalog import _base_responses, _col
from test_data_migration_scripts import _FakeSource, discover  # noqa: F401


def test_a_collated_text_column_warns_and_names_the_collation():
    m = map_type("TEXT", char_length=20, collation="en-ci")
    assert m.spark_type == "STRING" and not m.blocked
    assert "en-ci" in m.warning
    assert "binary" in m.warning and "case" in m.warning
    for what in ("comparison", "sorting", "uniqueness"):
        assert what in m.warning, what
    # The length warning is not lost to the collation one.
    assert "declared length 20" in m.warning


def test_no_collation_is_no_collation_warning():
    for none in (None, ""):
        m = map_type("TEXT", char_length=20, collation=none)
        assert "collat" not in (m.warning or "").lower()


def test_a_collation_is_only_a_text_concern():
    assert map_type("NUMBER", precision=38, scale=0,
                    collation="en-ci").warning is None


def test_the_live_read_selects_the_collation_and_carries_it_to_the_record():
    col = {**_col("T", name="CODE"), "COLLATION_NAME": "en-ci"}
    fake = FakeSql(_base_responses(tables=[{"name": "T", "rows": 1}],
                                   columns=[col]))
    rec = build_inventory(fake, row_counts="none")["inventory"][0]
    sql = [c for c in fake.calls if "information_schema.columns" in c.lower()]
    assert "collation_name" in sql[0].lower()
    assert any("CODE" in w and "en-ci" in w for w in rec["warnings"]), \
        rec["warnings"]
    assert rec["compatibility_status"] == "supported", "a warning, not a block"


def test_the_manifest_bridge_reaches_the_same_verdict():
    m = {"schemas": [{"name": "S", "views": [], "errors": [], "tables": [
        {"name": "T", "source_rows": 1, "columns": [
            {"name": "CODE", "type": "TEXT(20)", "nullable": True,
             "data_type": "TEXT", "numeric_precision": None,
             "numeric_scale": None, "character_maximum_length": 20,
             "collation": "en-ci"}]}]}]}
    rec = inventory_from_manifest(m, database="DB")["inventory"][0]
    live = map_type("TEXT", char_length=20, collation="en-ci")
    assert rec["warnings"] == [f"CODE: {live.warning}"]


def test_discovery_selects_the_collation_and_writes_it(discover):  # noqa: F811
    sql = (pathlib.Path(__file__).resolve().parents[1] / "dataplane"
           / "00_discover_snowflake.py").read_text(encoding="utf-8")
    block = sql[sql.index("_COLUMNS_SQL = ("):]
    block = block[:block.index("\n\n")]
    assert "COLLATION_NAME" in block
    source = _FakeSource(
        tables=[{"TABLE_SCHEMA": "S", "TABLE_NAME": "T",
                 "TABLE_TYPE": "BASE TABLE", "ROW_COUNT": 1, "BYTES": 1}],
        columns=[{"TABLE_SCHEMA": "S", "TABLE_NAME": "T",
                  "COLUMN_NAME": "CODE", "ORDINAL_POSITION": 1,
                  "DATA_TYPE": "TEXT", "IS_NULLABLE": "YES",
                  "NUMERIC_PRECISION": None, "NUMERIC_SCALE": None,
                  "CHARACTER_MAXIMUM_LENGTH": 20,
                  "COLLATION_NAME": "en-ci"}])
    schemas = discover.discover_via_connector(source, wanted=None,
                                              exclude=set())
    assert schemas[0]["tables"][0]["columns"][0]["collation"] == "en-ci"
