"""The copy reads each table with ONE qualified pushdown built from the plan.

Live 2026-09-29 (AIDP Spark 3.5.0, Delta 3.1.0, the AIDP Snowflake
connector), probes 1 and 3:

* the connector's own table read cost 126-241 s PER TABLE -- a metadata
  lookup, paid even for one row -- where a qualified pushdown SELECT of the
  same table took ~8.5 s;
* the connector's typing is lossy: NUMBER(38,37) came back cut to ten
  significant digits, TIME(3) and TIMESTAMP_NTZ(9) lost their fractions,
  TIMESTAMP_TZ its offset, and VECTOR / MAP / structured OBJECT tables
  could not be opened at all (Type 50003, CONNECTOR_0095);
* the exact reads work: `"N"::VARCHAR` then Spark `CAST(... AS
  DECIMAL(p,s))`, `TO_VARCHAR("T", 'HH24:MI:SS.FF9')`, `"V"::ARRAY::VARCHAR`
  then `from_json(..., 'array<float>')` -- verified value for value.

So the copy no longer reads through `read_table`. The approved plan carries
each column's Snowflake read expression and Spark convert expression
(`columns` on every TABLE statement); the copy
sends `SELECT <read_expr> AS "<name>", ... FROM "DB"."SCHEMA"."TABLE"` as
one pushdown, and inserts `SELECT <convert_expr> AS `<name>`, ...` into the
target in the target's column order. An older plan with no `columns` falls
back to reading every column bare -- the connector's typing, exactly what
the copy did before -- and the table's record says so, column by column.

Everything the copy guaranteed before still holds on this path: the live
source's columns (read from INFORMATION_SCHEMA, one query per chunk) are
compared with the target's by name, a narrower DECIMAL is refused, and a
target that cannot be DESCRIBEd is a failure with its error.
"""
import decimal
import json

import pytest

from dataplane.snowmig_source import assert_pushdown_read_only
from fake_pushdown import FakeLakeSpark, FakeSnowflake, source_for
from test_data_migration_scripts import _inject_spark, _load

D = decimal.Decimal
_TINY = D("0.1234567890123456789012345678901234567")
_BIG = D("12345678901234567890123456789012345678")


@pytest.fixture(scope="module")
def copy_schema():
    return _load("02_copy_schema")


def _estate(extra=None):
    tables = {
        ("SNOWMIG_DB", "TYPES", "T_NUM_NEG"): {
            "columns": [("N", "NUMBER", 38, 37)], "rows": [{"N": _TINY}]},
        ("SNOWMIG_DB", "TYPES", "T_TIME3"): {
            "columns": [("T", "TIME", None, None)],
            "rows": [{"T": "12:34:56.789"}]},
        ("SNOWMIG_DB", "TYPES", "T_VECTOR_F"): {
            "columns": [("V", "VECTOR", None, None)],
            "rows": [{"V": [1.5, 2.0, 3.0, 4.0]}]},
        ("SNOWMIG_DB", "TYPES", "T_MIXED"): {
            "columns": [("ID", "NUMBER", 38, 0), ("NAME", "TEXT", None, None)],
            "rows": [{"ID": _BIG, "NAME": "a"}, {"ID": D(2), "NAME": "b"}]},
    }
    tables.update(extra or {})
    return tables


_LAKE = {
    "`lake`.`types`.`T_NUM_NEG`": [("N", "decimal(38,37)")],
    "`lake`.`types`.`T_TIME3`": [("T", "string")],
    "`lake`.`types`.`T_VECTOR_F`": [("V", "array<float>")],
    "`lake`.`types`.`T_MIXED`": [("ID", "decimal(38,0)"), ("NAME", "string")],
}

# The column spec, exactly as `ddl` writes it into ddl_plan.json.
_SPEC = {
    "T_NUM_NEG": [{"name": "N", "target_type": "DECIMAL(38,37)",
                   "read_expr": '"N"::VARCHAR',
                   "convert_expr": "CAST(`N` AS DECIMAL(38,37))"}],
    "T_TIME3": [{"name": "T", "target_type": "STRING",
                 "read_expr": "TO_VARCHAR(\"T\", 'HH24:MI:SS.FF9')",
                 "convert_expr": "`T`"}],
    "T_VECTOR_F": [{"name": "V", "target_type": "ARRAY<FLOAT>",
                    "read_expr": '"V"::ARRAY::VARCHAR',
                    "convert_expr": "from_json(`V`, 'array<float>')"}],
    "T_MIXED": [{"name": "ID", "target_type": "DECIMAL(38,0)",
                 "read_expr": '"ID"::VARCHAR',
                 "convert_expr": "CAST(`ID` AS DECIMAL(38,0))"},
                {"name": "NAME", "target_type": "STRING",
                 "read_expr": '"NAME"', "convert_expr": "`NAME`"}],
}


def _spark(tables=None, lake=None, **kw):
    return FakeLakeSpark(FakeSnowflake(tables or _estate()),
                         lake or _LAKE, **kw)


def _copy(copy_schema, spark, table, *, spec=None, verify="counts",
          mode="append"):
    source = source_for(spark)
    live = source.live_columns("TYPES", [table]).get(table)
    return copy_schema.copy_table(
        source, "TYPES", table, f"`lake`.`types`.`{table}`", mode=mode,
        verify=verify, retries=0, retry_base_delay=0, live_columns=live,
        column_spec=spec)


def _column_reads(spark):
    return [s for s in spark.pushdowns
            if "INFORMATION_SCHEMA" not in s and "SNOWMIG_TABLE" not in s]


# --------------------------------------------------- exact, typed values

@pytest.mark.parametrize("table,column,expected", [
    ("T_NUM_NEG", "N", _TINY),
    ("T_TIME3", "T", "12:34:56.789"),
    ("T_VECTOR_F", "V", [1.5, 2.0, 3.0, 4.0]),
])
def test_the_plans_column_spec_lands_every_value_exactly(
        copy_schema, table, column, expected):
    spark = _spark()
    out = _copy(copy_schema, spark, table, spec=_SPEC[table])
    assert out["status"] == "verified", out
    assert spark.rows[f"`lake`.`types`.`{table}`"] == [{column: expected}]
    assert out["read"]["from_plan"] == [column]
    assert out["read"]["bare"] == []


def test_a_38_digit_integer_survives_the_copy(copy_schema):
    """Through the connector's typing a 38-digit integer failed outright
    (DECIMAL_PRECISION_EXCEEDS); ::VARCHAR then CAST is exact."""
    spark = _spark()
    out = _copy(copy_schema, spark, "T_MIXED", spec=_SPEC["T_MIXED"])
    assert out["status"] == "verified", out
    assert spark.rows["`lake`.`types`.`T_MIXED`"][0]["ID"] == _BIG


def test_each_table_is_one_qualified_read_only_pushdown(copy_schema):
    spark = _spark()
    _copy(copy_schema, spark, "T_MIXED", spec=_SPEC["T_MIXED"])
    reads = _column_reads(spark)
    assert reads == [
        'select "ID"::VARCHAR as "ID", "NAME" as "NAME" from '
        '"SNOWMIG_DB"."TYPES"."T_MIXED"'], reads
    for sql in spark.pushdowns:
        assert_pushdown_read_only(sql)
    assert spark.table_reads == [], \
        "the connector's table read (126-241 s per table) is not used"


def test_the_insert_converts_each_column_in_the_targets_order(copy_schema):
    spark = _spark(tables=_estate({
        ("SNOWMIG_DB", "TYPES", "T_MIXED"): {
            "columns": [("NAME", "TEXT", None, None), ("ID", "NUMBER", 38, 0)],
            "rows": [{"ID": _BIG, "NAME": "a"}]}}))
    out = _copy(copy_schema, spark, "T_MIXED", spec=_SPEC["T_MIXED"])
    assert out["status"] == "verified", out
    insert = next(s for s in spark.statements if s.startswith("INSERT"))
    assert insert.startswith(
        "INSERT INTO `lake`.`types`.`T_MIXED` SELECT CAST(`ID` AS "
        "DECIMAL(38,0)) AS `ID`, `NAME` AS `NAME` FROM `snowmig_src_"), insert
    assert spark.rows["`lake`.`types`.`T_MIXED`"] == [{"ID": _BIG,
                                                        "NAME": "a"}]


# ------------------------------------------- an older plan: the fallback

def test_an_older_plan_reads_every_column_bare_and_says_so(copy_schema):
    """No `columns` on the statement: the copy reads `"N"` -- the connector's
    typing, what it always did -- and the record names each column read that
    way and what that does not carry. Nothing claims exactness it lacks."""
    spark = _spark()
    out = _copy(copy_schema, spark, "T_NUM_NEG", spec=None)
    assert _column_reads(spark) == [
        'select "N" as "N" from "SNOWMIG_DB"."TYPES"."T_NUM_NEG"']
    assert spark.rows["`lake`.`types`.`T_NUM_NEG`"] == [
        {"N": D("0.1234567890000000000000000000000000000")}], \
        "the connector's ten significant digits: the premise, pinned"
    assert out["read"]["from_plan"] == []
    assert out["read"]["bare"] == ["N"]
    note = out["read"]["note"]
    assert "connector" in note and "NOT carried exactly" in note


def test_an_older_plan_cannot_open_a_vector_table_and_fails_loud(copy_schema):
    spark = _spark()
    out = _copy(copy_schema, spark, "T_VECTOR_F", spec=None)
    assert out["status"] == "failed"
    assert "Type:50003" in out["reason"]
    assert spark.rows["`lake`.`types`.`T_VECTOR_F`"] == []


def test_a_target_column_the_spec_does_not_cover_is_read_bare(copy_schema):
    spark = _spark()
    out = _copy(copy_schema, spark, "T_MIXED", spec=_SPEC["T_MIXED"][:1])
    assert out["status"] == "verified", out
    assert out["read"]["from_plan"] == ["ID"]
    assert out["read"]["bare"] == ["NAME"]


# ------------------------------------------------- earlier guarantees kept

def test_a_column_added_to_the_source_since_the_plan_is_drift(copy_schema):
    spark = _spark(tables=_estate({
        ("SNOWMIG_DB", "TYPES", "T_MIXED"): {
            "columns": [("ID", "NUMBER", 38, 0), ("NAME", "TEXT", None, None),
                        ("EMAIL", "TEXT", None, None)],
            "rows": [{"ID": D(1), "NAME": "a", "EMAIL": "e"}]}}))
    out = _copy(copy_schema, spark, "T_MIXED", spec=_SPEC["T_MIXED"])
    assert out["status"] == "type_drift", out
    assert out["layout_drift"] == {"not_on_target": ["EMAIL"],
                                   "not_in_source": []}
    assert _column_reads(spark) == [], "refused before any row is read"
    assert not any(s.startswith("INSERT") for s in spark.statements)


def test_a_narrower_target_decimal_is_refused_before_any_read(copy_schema):
    lake = dict(_LAKE)
    lake["`lake`.`types`.`T_NUM_NEG`"] = [("N", "decimal(10,2)")]
    spark = _spark(lake=lake)
    out = _copy(copy_schema, spark, "T_NUM_NEG", spec=_SPEC["T_NUM_NEG"])
    assert out["status"] == "type_drift", out
    assert out["type_drift"] == {"N": {"source": "decimal(38,37)",
                                       "target": "decimal(10,2)"}}
    assert _column_reads(spark) == []


def test_a_table_information_schema_does_not_list_is_failed(copy_schema):
    spark = _spark()
    source = source_for(spark)
    out = copy_schema.copy_table(
        source, "TYPES", "T_MIXED", "`lake`.`types`.`T_MIXED`",
        mode="append", verify="counts", retries=0, retry_base_delay=0,
        live_columns=[], column_spec=_SPEC["T_MIXED"])
    assert out["status"] == "failed"
    assert "INFORMATION_SCHEMA" in out["reason"]
    assert "NOT copied" in out["reason"]
    assert _column_reads(spark) == []


def test_a_target_describe_error_is_a_failure_with_its_text(copy_schema):
    err = "[INSUFFICIENT_PERMISSIONS] User does not have USE on lake.types"
    spark = _spark(describe_raises={"`lake`.`types`.`T_MIXED`": err})
    out = _copy(copy_schema, spark, "T_MIXED", spec=_SPEC["T_MIXED"])
    assert out["status"] == "failed"
    assert "[INSUFFICIENT_PERMISSIONS]" in out["reason"]
    assert _column_reads(spark) == []


def test_skip_existing_still_never_writes(copy_schema):
    spark = _spark()
    spark.rows["`lake`.`types`.`T_NUM_NEG`"] = [{"N": _TINY}]
    out = _copy(copy_schema, spark, "T_NUM_NEG", spec=_SPEC["T_NUM_NEG"],
                mode="skip-existing")
    assert out["status"] == "skipped_nonempty"
    assert _column_reads(spark) == []


def test_the_temp_view_is_dropped_and_unique_per_table(copy_schema):
    """Two tables whose names differ only in case must not share a temp
    view: Spark resolves view names case-insensitively, and with tables
    copied in parallel one table's rows would land in the other."""
    assert copy_schema._view_name("S", "Orders") != \
        copy_schema._view_name("S", "ORDERS")
    spark = _spark()
    _copy(copy_schema, spark, "T_MIXED", spec=_SPEC["T_MIXED"])
    assert spark.views == {}


# ----------------------------------------------------- the whole stage

def _plan(tmp_path, spec=_SPEC, views=()):
    statements = []
    for table, columns in spec.items():
        stmt = {"source_identifier": f"SNOWMIG_DB.TYPES.{table}",
                "object_type": "TABLE",
                "target_fqn": f"lake.types.{table}",
                "expected_columns": [{"name": c["name"],
                                      "type": c["target_type"]}
                                     for c in columns]}
        if columns and "read_expr" in columns[0]:
            stmt["columns"] = columns
        statements.append(stmt)
    for view in views:
        statements.append({"source_identifier": f"SNOWMIG_DB.TYPES.{view}",
                           "object_type": "VIEW",
                           "target_fqn": f"lake.types.{view}",
                           "sql": "CREATE VIEW x AS SELECT 1"})
    reports = tmp_path / "reports"
    reports.mkdir()
    (tmp_path / "plan").mkdir()
    (tmp_path / "plan" / "ddl_plan.json").write_text(
        json.dumps({"statements": statements}), encoding="utf-8")
    (reports / "discovery_manifest.json").write_text(json.dumps({"schemas": [
        {"name": "TYPES", "tables": [{"name": t} for t in spec],
         "views": [{"name": v} for v in views], "errors": []}]}),
        encoding="utf-8")
    config = tmp_path / "source.json"
    config.write_text(json.dumps({"snowflake": {
        "account": "acct", "warehouse": "WH", "database": "SNOWMIG_DB",
        "user": "svc", "auth": "password", "password": "p",
        "schema": "TYPES"}}), encoding="utf-8")
    return reports, config


def _main(monkeypatch, spark, reports, config, *argv):
    _inject_spark(monkeypatch, spark)
    module = _load("02_copy_schema")
    rc = module.main(["--target-catalog", "lake", "--schema", "TYPES",
                      "--reports-dir", str(reports), "--source-config",
                      str(config), "--output-dir", "", *argv])
    report = json.loads((reports / "copy_report_types.json").read_text(
        encoding="utf-8"))
    return rc, report


def test_the_stage_copies_every_planned_table_exactly(monkeypatch, tmp_path):
    reports, config = _plan(tmp_path, views=["V_TYPES"])
    spark = _spark()
    rc, report = _main(monkeypatch, spark, reports, config,
                       "--mode", "append")
    assert rc == 0, report
    assert sorted(report["tables"]) == sorted(_SPEC), \
        "views are never copied"
    assert {r["status"] for r in report["tables"].values()} == {"verified"}
    assert spark.rows["`lake`.`types`.`T_NUM_NEG`"] == [{"N": _TINY}]
    assert spark.rows["`lake`.`types`.`T_VECTOR_F`"] == [
        {"V": [1.5, 2.0, 3.0, 4.0]}]
    assert spark.table_reads == []
    info = [s for s in spark.pushdowns if "INFORMATION_SCHEMA" in s]
    assert len(info) == 1, "one INFORMATION_SCHEMA read for the chunk"
    assert '"SNOWMIG_DB".INFORMATION_SCHEMA.COLUMNS' in info[0]


def test_an_information_schema_failure_fails_every_table_it_covered(
        monkeypatch, tmp_path):
    reports, config = _plan(tmp_path)
    spark = FakeLakeSpark(FakeSnowflake(
        _estate(), fail_info_schema="CONNECTOR_0099 - warehouse suspended"),
        _LAKE)
    rc, report = _main(monkeypatch, spark, reports, config)
    assert rc == 1
    for rec in report["tables"].values():
        assert rec["status"] == "failed"
        assert "CONNECTOR_0099" in rec["reason"]
        assert "NOT copied" in rec["reason"]
    assert not any(s.startswith("INSERT") for s in spark.statements)
