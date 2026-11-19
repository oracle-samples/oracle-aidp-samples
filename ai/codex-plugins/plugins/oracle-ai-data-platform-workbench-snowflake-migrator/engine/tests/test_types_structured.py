"""Structured Snowflake types map to TYPED Spark types once their detail is
known -- and say exactly what the typed mapping does not carry.

Live 2026-09-29 (AIDP Spark 3.5.0 / Delta 3.1.0), the facts this rests on:

  * The connector cannot even open a table holding VECTOR, MAP or a
    structured OBJECT ("Type:50003 is not a valid Types.java value";
    CONNECTOR_0095). A qualified pushdown of `"COL"::VARIANT::VARCHAR`
    (`"COL"::ARRAY::VARCHAR` for a VECTOR) followed by Spark `from_json`
    gave typed values: [1.5, 2.0, 3.0, 4.0] as ARRAY<FLOAT>, [1, 2, 3] as
    ARRAY<INT>, {"a": 1} as MAP<STRING, DECIMAL>, Row(X=1, Y="a") as a
    STRUCT. Delta 3.1 created and read ARRAY/MAP/STRUCT columns.
  * Plain VARIANT / OBJECT / ARRAY have no element type to find: they stay
    JSON text (or blocked), as before.
  * GEOGRAPHY / GEOMETRY read as GeoJSON text; `ST_ASWKT` gave
    `POINT(-122.35 37.55)`.
  * TIME(3) through the connector arrived as "12:34:56" -- the fraction
    dropped; `TO_VARCHAR(.., 'HH24:MI:SS.FF3')` gave "12:34:56.789".
  * TIMESTAMP_TZ's offset is not a Spark TIMESTAMP property at all.

A typed mapping is not blocked by `semi_structured=block`: that switch is
the decision about UNTYPED JSON, and a VECTOR(FLOAT, 4) is not untyped.
"""
import pytest

import snowmig
from fake_sql import FakeSql
from migration_config import ConfigError, MAPPING_DEFAULTS, mapping_block
from snowflake_source.dialect.types import GEOSPATIAL_MODES, map_type
from snowflake_source.extract.catalog import build_inventory
from snowflake_source.extract.manifest import inventory_from_manifest
from test_type_detail import _describe_row, _is_col, _responses


def _spark_describe_form(spark_type: str) -> str:
    # 01_create_structure compares what DESCRIBE reads back with the plan,
    # ignoring case and whitespace only (_norm_type). Spark renders a struct
    # as `struct<X:decimal(38,0),Y:string>`, so the plan's type has to
    # reduce to the same string.
    return "".join(spark_type.split()).upper()


# ------------------------------------------------------------- VECTOR

def test_a_float_vector_is_a_typed_float_array():
    m = map_type("VECTOR", type_detail="VECTOR(FLOAT, 4)")
    assert (m.spark_type, m.blocked) == ("ARRAY<FLOAT>", False)
    # Snowflake's VECTOR FLOAT is 32-bit, as Spark FLOAT is: no widening.
    assert "dimension 4" in m.warning and "not enforced" in m.warning


def test_an_int_vector_is_a_typed_int_array():
    m = map_type("VECTOR", type_detail="VECTOR(INT, 3)")
    assert m.spark_type == "ARRAY<INT>"


def test_a_typed_vector_is_not_held_by_the_json_switch():
    m = map_type("VECTOR", type_detail="VECTOR(FLOAT, 4)",
                 semi_structured="block")
    assert not m.blocked


def test_a_vector_whose_detail_was_never_read_stays_blocked_and_says_why():
    m = map_type("VECTOR", semi_structured="string",
                 type_detail_unread="DESCRIBE TABLE failed: denied")
    assert m.blocked
    assert "element type" in m.reason and "denied" in m.reason


# ---------------------------------------------------------------- MAP

def test_a_map_is_a_typed_map():
    m = map_type("MAP", type_detail="MAP(VARCHAR(16777216), NUMBER(38,0))")
    assert m.spark_type == "MAP<STRING, DECIMAL(38,0)>"
    assert not m.blocked


def test_a_numeric_map_key_is_carried_as_text_and_says_so():
    # JSON object keys are text and Spark's from_json reads map keys as
    # strings, so the key's type narrows to its exact decimal text.
    m = map_type("MAP", type_detail="MAP(NUMBER(38,0), VARCHAR(16777216))")
    assert m.spark_type == "MAP<STRING, STRING>"
    assert "key" in m.warning and "NUMBER(38,0)" in m.warning


def test_a_map_without_its_detail_is_blocked_rather_than_guessed():
    m = map_type("MAP", semi_structured="string")
    assert m.blocked and "element type" in m.reason


# ----------------------------------------------------- OBJECT and ARRAY

def test_a_structured_object_is_a_struct():
    m = map_type("OBJECT",
                 type_detail="OBJECT(X NUMBER(38,0), Y VARCHAR(16777216))")
    assert m.spark_type == "STRUCT<X: DECIMAL(38,0), Y: STRING>"
    assert _spark_describe_form(m.spark_type) == \
        "STRUCT<X:DECIMAL(38,0),Y:STRING>"


def test_a_structured_array_is_a_typed_array():
    m = map_type("ARRAY", type_detail="ARRAY(NUMBER(38,0))")
    assert m.spark_type == "ARRAY<DECIMAL(38,0)>"


def test_nested_structured_types_nest():
    m = map_type("OBJECT", type_detail=(
        "OBJECT(A ARRAY(OBJECT(B FLOAT)), M MAP(VARCHAR(16777216), BOOLEAN), "
        "N NUMBER(10,2))"))
    assert m.spark_type == ("STRUCT<A: ARRAY<STRUCT<B: DOUBLE>>, "
                            "M: MAP<STRING, BOOLEAN>, N: DECIMAL(10,2)>")


def test_a_field_name_spark_cannot_read_bare_is_backticked():
    m = map_type("OBJECT", type_detail='OBJECT("my key" VARCHAR(10))')
    assert m.spark_type == "STRUCT<`my key`: STRING>"


def test_a_not_null_inside_a_structured_type_is_named_as_not_carried():
    m = map_type("ARRAY", type_detail="ARRAY(NUMBER(38,0) NOT NULL)")
    assert m.spark_type == "ARRAY<DECIMAL(38,0)>"
    assert "NOT NULL" in m.warning


@pytest.mark.parametrize("detail", ["ARRAY", "OBJECT"])
def test_a_plain_semi_structured_detail_is_json_text_as_before(detail):
    assert map_type(detail, type_detail=detail).blocked
    m = map_type(detail, type_detail=detail, semi_structured="string")
    assert m.spark_type == "STRING" and "JSON" in m.warning


def test_a_structured_type_holding_what_a_struct_cannot_carry_falls_back():
    detail = "OBJECT(AT TIMESTAMP_NTZ(9), X NUMBER(38,0))"
    blocked = map_type("OBJECT", type_detail=detail)
    assert blocked.blocked and "TIMESTAMP_NTZ" in blocked.reason
    m = map_type("OBJECT", type_detail=detail, semi_structured="string")
    assert m.spark_type == "STRING"
    assert "TIMESTAMP_NTZ" in m.warning and "JSON" in m.warning


def test_an_object_whose_detail_went_unread_says_its_shape_is_unknown():
    m = map_type("OBJECT", semi_structured="string",
                 type_detail_unread="GET_DDL failed: timeout")
    assert m.spark_type == "STRING"
    assert "UNREAD" in m.warning and "timeout" in m.warning


def test_an_older_inventory_without_the_read_maps_exactly_as_before():
    # No detail and no unread marker: the extractor never tried. Nothing is
    # claimed either way, and the verdict is the one it always was.
    before = map_type("OBJECT", semi_structured="string")
    assert before.spark_type == "STRING" and "UNREAD" not in before.warning


# --------------------------------------------------------- geospatial

def test_wkt_is_a_geospatial_mode():
    assert "wkt" in GEOSPATIAL_MODES


@pytest.mark.parametrize("dt", ["GEOGRAPHY", "GEOMETRY"])
def test_geospatial_as_wkt_is_text_and_says_what_it_is_not(dt):
    m = map_type(dt, geospatial="wkt")
    assert m.spark_type == "STRING" and not m.blocked
    assert "WKT" in m.warning and "spatial" in m.warning


def test_geometry_as_wkt_names_the_srid_it_drops():
    assert "SRID" in map_type("GEOMETRY", geospatial="wkt").warning


def test_geospatial_as_string_says_geojson():
    assert "GeoJSON" in map_type("GEOGRAPHY", geospatial="string").warning


def test_geospatial_still_blocks_by_default():
    assert map_type("GEOGRAPHY").blocked


# --------------------------------------------------------------- TIME

def test_time_carries_its_fraction_exactly_and_says_how():
    m = map_type("TIME", datetime_precision=3)
    assert m.spark_type == "STRING"
    assert "HH24:MI:SS.FF3" in m.warning


def test_time_precision_comes_from_the_detail_when_the_column_row_lacks_it():
    m = map_type("TIME", type_detail="TIME(3)")
    assert "HH24:MI:SS.FF3" in m.warning


def test_a_zoned_timestamp_names_the_offset_it_does_not_keep():
    m = map_type("TIMESTAMP_TZ", datetime_precision=9)
    assert "offset" in m.warning and "timezone" in m.warning.lower()


# ------------------------------------- both extraction paths, one verdict

def test_assess_maps_a_vector_table_as_supported_and_typed():
    inv = build_inventory(FakeSql(_responses(
        [_is_col("T_VECTOR_F", "V", 1, "VECTOR")], ["T_VECTOR_F"],
        describe=[_describe_row("V", "VECTOR(FLOAT, 4)")])), ["DB"])
    rec = inv["inventory"][0]
    assert rec["compatibility_status"] == "supported"
    assert rec["columns"][0]["target_type"] == "ARRAY<FLOAT>"


def test_the_manifest_path_reaches_the_same_verdict():
    manifest = {"schemas": [{"name": "TYPES", "errors": [], "views": [],
                             "tables": [{"name": "T_VECTOR_F", "columns": [
        {"name": "V", "data_type": "VECTOR", "nullable": True,
         "ordinal_position": 1, "type_detail": "VECTOR(FLOAT, 4)"}]}]}]}
    rec = inventory_from_manifest(manifest, database="DB")["inventory"][0]
    assert rec["compatibility_status"] == "supported"
    assert rec["columns"][0]["target_type"] == "ARRAY<FLOAT>"


def test_assess_with_geospatial_wkt_records_the_mode():
    inv = build_inventory(FakeSql(_responses(
        [_is_col("T_GEO", "G", 1, "GEOGRAPHY")], ["T_GEO"],
        describe=[_describe_row("G", "GEOGRAPHY")])), ["DB"],
        geospatial="wkt")
    assert inv["geospatial_mode"] == "wkt"
    assert inv["inventory"][0]["columns"][0]["target_type"] == "STRING"


# ------------------------------------------------------------- config

def test_geospatial_is_a_config_mapping_decision_defaulting_to_block():
    assert MAPPING_DEFAULTS["geospatial"] == "block"
    assert mapping_block({"mapping": {"geospatial": "wkt"}})["geospatial"] == "wkt"
    assert mapping_block({"mapping": {"geospatial": "wkt",
                                      "enabled": False}})["geospatial"] == "block"
    with pytest.raises(ConfigError):
        mapping_block({"mapping": {"geospatial": "shapefile"}})


def _cfg(tmp_path, text):
    path = tmp_path / "snowmig-config.yaml"
    path.write_text(text, encoding="utf-8")
    return str(path)


def test_the_config_value_reaches_assess_and_the_flag_still_wins(tmp_path):
    cfg = _cfg(tmp_path, "mapping:\n  geospatial: wkt\n")
    args = snowmig.build_parser().parse_args(["assess", "--config", cfg])
    assert snowmig._mapping_resolution(args)["geospatial"] == {
        "value": "wkt", "source": "config"}
    args = snowmig.build_parser().parse_args(
        ["assess", "--config", cfg, "--geospatial", "string"])
    assert snowmig._mapping_resolution(args)["geospatial"] == {
        "value": "string", "source": "flag"}


def test_ingest_takes_wkt_too(tmp_path):
    args = snowmig.build_parser().parse_args(
        ["ingest", "--manifest", "m.json", "--database-name", "DB",
         "--geospatial", "wkt"])
    assert args.geospatial == "wkt"
