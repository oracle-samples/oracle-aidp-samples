"""Every planned table says, per column, how to READ it from Snowflake and
how to CONVERT it on AIDP -- the per-column copy spec the copy
stage builds its one qualified pushdown from.

Why the read cannot stay `SELECT "COL"`, all live 2026-09-29:

  * NUMBER through the connector is lossy: NUMBER(38,37) 0.1234567890123...
    arrived as 0.1234567890000..., and a 38-digit integer failed outright
    (DECIMAL_PRECISION_EXCEEDS). `"COL"::VARCHAR` then Spark
    `CAST(.. AS DECIMAL(p,s))` was exact.
  * FLOAT: `TO_VARCHAR(COL, 'TME')` then CAST AS DOUBLE was exact.
  * TIME(3) arrived as "12:34:56"; TIMESTAMP_NTZ(9) and TIMESTAMP_TZ lost
    their fraction and offset. TO_VARCHAR with FF digits (and TZH:TZM) was
    exact.
  * VECTOR / MAP / structured OBJECT: the connector could not open the
    table at all. `"COL"::VARIANT::VARCHAR` (VECTOR: `::ARRAY::VARCHAR`)
    then `from_json(col, '<spark type>')` gave typed values.
  * GEOGRAPHY: `ST_ASWKT` gave WKT.

The contract's shape: `read_expr` is Snowflake SQL over the quoted source
column and is NOT aliased (the copy stage aliases it); `convert_expr` is
Spark SQL over a column named exactly the source name, backtick-quoted, and
produces `target_type`.
"""
import pytest

from snowflake_source.conn import assert_read_only
from snowflake_source.dialect.types import copy_expressions
from snowflake_source.extract.catalog import build_inventory
from target.ddl import build_create_table, build_ddl_payload
from report.render import render_ddl_plan
from fake_sql import FakeSql
from test_type_detail import _describe_row, _is_col, _responses


def _spec(data_type, target, name="C", **kw):
    read, convert = copy_expressions(data_type, target, name=name, **kw)
    return read, convert


# ------------------------------------------------ the contract's examples

def test_number_is_read_as_text_and_cast_exactly():
    assert _spec("NUMBER", "DECIMAL(38,37)", name="N") == (
        '"N"::VARCHAR', "CAST(`N` AS DECIMAL(38,37))")


def test_a_float_vector_is_read_as_array_text_and_parsed():
    assert _spec("VECTOR", "ARRAY<FLOAT>", name="V",
                 type_detail="VECTOR(FLOAT, 4)") == (
        '"V"::ARRAY::VARCHAR', "from_json(`V`, 'array<float>')")


def test_time_is_read_with_every_digit_it_holds():
    # FF3 for TIME(3): the probe's own read, which gave "12:34:56.789" --
    # the text Snowflake shows for the value, not a nine-digit padding.
    assert _spec("TIME", "STRING", name="T", datetime_precision=3) == (
        "TO_VARCHAR(\"T\", 'HH24:MI:SS.FF3')", "`T`")


# ----------------------------------------------------- the other shapes

def test_float_is_read_through_its_text_form():
    assert _spec("FLOAT", "DOUBLE", name="F") == (
        "TO_VARCHAR(\"F\", 'TME')", "CAST(`F` AS DOUBLE)")


@pytest.mark.parametrize("target", ["TIMESTAMP_NTZ", "TIMESTAMP"])
def test_timestamp_ntz_is_read_at_nanoseconds_and_cast_to_the_planned_type(
        target):
    assert _spec("TIMESTAMP_NTZ", target, name="TS") == (
        "TO_VARCHAR(\"TS\", 'YYYY-MM-DD HH24:MI:SS.FF9')",
        f"CAST(`TS` AS {target})")


@pytest.mark.parametrize("dt", ["TIMESTAMP_TZ", "TIMESTAMP_LTZ"])
def test_a_zoned_timestamp_is_read_with_its_offset(dt):
    read, convert = _spec(dt, "TIMESTAMP", name="TZ")
    # ISO-8601 with the offset attached: the form Spark's CAST parses.
    assert read == "TO_VARCHAR(\"TZ\", 'YYYY-MM-DD\"T\"HH24:MI:SS.FF9TZH:TZM')"
    assert convert == "CAST(`TZ` AS TIMESTAMP)"


def test_a_map_is_parsed_into_its_typed_map():
    assert _spec("MAP", "MAP<STRING, DECIMAL(38,0)>", name="M",
                 type_detail="MAP(VARCHAR(16777216), NUMBER(38,0))") == (
        '"M"::VARIANT::VARCHAR', "from_json(`M`, 'map<string, decimal(38,0)>')")


def test_a_struct_keeps_its_field_names_exactly():
    # from_json matches JSON keys to struct fields by name, and Snowflake
    # wrote them X and Y: lower-casing the schema would null every field.
    assert _spec("OBJECT", "STRUCT<X: DECIMAL(38,0), Y: STRING>", name="O",
                 type_detail="OBJECT(X NUMBER(38,0), Y VARCHAR(16777216))") == (
        '"O"::VARIANT::VARCHAR',
        "from_json(`O`, 'STRUCT<X: DECIMAL(38,0), Y: STRING>')")


@pytest.mark.parametrize("dt", ["VARIANT", "OBJECT", "ARRAY"])
def test_untyped_json_is_read_as_json_text(dt):
    # TO_JSON, not ::VARCHAR: a VARIANT holding the string "1" and one
    # holding the number 1 are different JSON, and ::VARCHAR prints both 1.
    assert _spec(dt, "STRING", name="P") == ('TO_JSON("P"::VARIANT)', "`P`")


def test_geography_as_wkt_and_as_geojson():
    assert _spec("GEOGRAPHY", "STRING", name="G", geospatial="wkt") == (
        'ST_ASWKT("G")', "`G`")
    assert _spec("GEOMETRY", "STRING", name="G", geospatial="string") == (
        'ST_ASGEOJSON("G")::VARCHAR', "`G`")


@pytest.mark.parametrize("dt,target", [("TEXT", "STRING"),
                                       ("BOOLEAN", "BOOLEAN"),
                                       ("DATE", "DATE"),
                                       ("BINARY", "BINARY")])
def test_a_type_the_connector_carries_exactly_is_read_as_is(dt, target):
    # The same shape the copy stage falls back to for a plan with no spec.
    assert _spec(dt, target, name="C") == ('"C"', "`C`")


def test_awkward_names_are_quoted_for_each_side():
    read, convert = _spec("NUMBER", "DECIMAL(38,0)", name='a"b`c')
    assert read == '"a""b`c"::VARCHAR'
    assert convert == "CAST(`a\"b``c` AS DECIMAL(38,0))"


# ---------------------------------------------------- in the ddl plan

def _inventory():
    columns = [
        _is_col("T_ALL", "ID", 1, "NUMBER", NUMERIC_PRECISION=38,
                NUMERIC_SCALE=0, IS_NULLABLE="NO"),
        _is_col("T_ALL", "TINY", 2, "NUMBER", NUMERIC_PRECISION=38,
                NUMERIC_SCALE=37),
        _is_col("T_ALL", "V", 3, "VECTOR"),
        _is_col("T_ALL", "T", 4, "TIME", DATETIME_PRECISION=3),
        _is_col("T_ALL", "NAME", 5, "TEXT", CHARACTER_MAXIMUM_LENGTH=20),
        _is_col("T_ALL", "G", 6, "GEOGRAPHY"),
    ]
    return build_inventory(FakeSql(_responses(
        columns, ["T_ALL"],
        describe=[_describe_row("V", "VECTOR(FLOAT, 4)"),
                  _describe_row("G", "GEOGRAPHY")])), ["SNOWMIG_COVERAGE"],
        geospatial="wkt")


def _plan(inv):
    ids = [r["source_identifier"] for r in inv["inventory"]]
    return {"waves": [ids], "clone_targets": ids,
            "target_names": {i: "bronze_cat." + ".".join(
                i.split(".")[1:]).lower() for i in ids}}


def test_every_table_statement_carries_the_spec_for_every_column():
    inv = _inventory()
    payload = build_ddl_payload(inv, _plan(inv))
    stmt = payload["statements"][0]
    names = [c["name"] for c in stmt["expected_columns"]]
    assert [c["name"] for c in stmt["columns"]] == names
    by = {c["name"]: c for c in stmt["columns"]}
    assert by["TINY"] == {"name": "TINY", "source_type": "decimal(38,37)",
                          "target_type": "DECIMAL(38,37)",
                          "read_expr": '"TINY"::VARCHAR',
                          "convert_expr": "CAST(`TINY` AS DECIMAL(38,37))"}
    assert by["V"]["convert_expr"] == "from_json(`V`, 'array<float>')"
    assert by["T"]["read_expr"] == "TO_VARCHAR(\"T\", 'HH24:MI:SS.FF3')"
    assert by["NAME"]["read_expr"] == '"NAME"'
    # The inventory's own geospatial decision decides the read.
    assert by["G"]["read_expr"] == 'ST_ASWKT("G")'
    for c in stmt["columns"]:
        assert c["target_type"] == next(
            e["type"] for e in stmt["expected_columns"] if e["name"] == c["name"])


def test_the_whole_read_is_one_read_only_select():
    # I1: the copy stage wraps these into ONE qualified pushdown. Built the
    # way the contract says, it must pass both read-only guards.
    import sys, pathlib
    sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]
                           / "dataplane"))
    from snowmig_source import assert_pushdown_read_only
    inv = _inventory()
    stmt = build_ddl_payload(inv, _plan(inv))["statements"][0]
    sql = ("SELECT " + ", ".join(
        f'{c["read_expr"]} AS "{c["name"]}"' for c in stmt["columns"])
        + ' FROM "SNOWMIG_COVERAGE"."TYPES"."T_ALL"')
    assert_read_only(sql)
    assert_pushdown_read_only(sql)
    for c in stmt["columns"]:
        assert " AS " not in c["read_expr"].upper().replace("::", " "), \
            "read_expr must not carry its own alias"


def test_the_ddl_plan_names_every_read_that_is_not_a_plain_select():
    inv = _inventory()
    md = render_ddl_plan(build_ddl_payload(inv, _plan(inv)))
    assert "R04_EXACT_READ" in md
    assert '"TINY"::VARCHAR' in md and "CAST(`TINY` AS DECIMAL(38,37))" in md
    assert '`NAME`' not in md.split("R04_EXACT_READ")[1].split("\n")[0], \
        "a plain column is not listed as a rewritten read"


def test_a_view_statement_carries_no_copy_spec():
    rec = {"source_identifier": "D.S.V", "object_type": "VIEW",
           "source_database": "D", "source_schema": "S",
           "view_ddl_get_ddl": "create view V as select 1 as A",
           "columns": [{"COLUMN_NAME": "A", "DATA_TYPE": "NUMBER",
                        "target_type": "DECIMAL(38,0)", "ORDINAL_POSITION": 1}],
           "source_metadata": {}}
    inv = {"inventory": [rec]}
    payload = build_ddl_payload(inv, {"waves": [["D.S.V"]],
                                      "clone_targets": ["D.S.V"],
                                      "target_names": {"D.S.V": "c.s.v"}})
    assert "columns" not in payload["statements"][0]


def test_an_older_inventory_without_modes_still_gets_a_spec():
    res = build_create_table(
        {"source_identifier": "D.S.T", "object_type": "TABLE",
         "compatibility_status": "supported", "source_metadata": {},
         "columns": [{"COLUMN_NAME": "G", "DATA_TYPE": "GEOGRAPHY",
                      "target_type": "STRING", "ORDINAL_POSITION": 1}]},
        "c.s.t")
    # No recorded geospatial mode: the STRING it was mapped to under the
    # only text mode that existed then, GeoJSON.
    assert res.copy_columns == [{"name": "G", "source_type": "geography",
                                 "target_type": "STRING",
                                 "read_expr": 'ST_ASGEOJSON("G")::VARCHAR',
                                 "convert_expr": "`G`"}]
