"""A source column whose type changed after the plan was approved.

The plan's per-column spec was decided for the type the column had at
`assess`. Run on another type -- a DECIMAL rebuilt as FLOAT, a code column
rebuilt as NUMBER -- its read and conversion can round values or turn them
NULL with the row count intact, and the table was still recorded
`verified`: the pre-copy check looked only at the source's DECIMAL columns.

Now every column's live type is compared with the spec's `source_type`.
`mapping.source_type_drift` (recorded in ddl_plan.json by `ddl`) decides:
`refuse` (default) records the table `type_drift` and copies nothing;
`convert` reads the column under its NEW type into the existing target
column and records the table `verified_with_conversion`, never `verified`.
"""
import decimal
import json

import pytest

from dataplane.snowmig_source import live_copy_expressions
from fake_pushdown import FakeLakeSpark, FakeSnowflake, source_for
from snowflake_source.dialect.types import copy_expressions, source_type_key
from test_copy_exact_reads import _column_reads, _main
from test_data_migration_scripts import _load

D = decimal.Decimal
TABLE = "T_AMOUNT"
FQN = f"`lake`.`types`.`{TABLE}`"
LAKE = {FQN: [("ID", "decimal(38,0)"), ("AMOUNT", "decimal(12,2)")]}
# The spec as `ddl` writes it: AMOUNT was NUMBER(12,2) when planned.
SPEC = [{"name": "ID", "source_type": "decimal(38,0)",
         "target_type": "DECIMAL(38,0)", "read_expr": '"ID"::VARCHAR',
         "convert_expr": "CAST(`ID` AS DECIMAL(38,0))"},
        {"name": "AMOUNT", "source_type": "decimal(12,2)",
         "target_type": "DECIMAL(12,2)", "read_expr": '"AMOUNT"::VARCHAR',
         "convert_expr": "CAST(`AMOUNT` AS DECIMAL(12,2))"}]


@pytest.fixture(scope="module")
def copy_schema():
    return _load("02_copy_schema")


def _spark(amount_type=("FLOAT", None, None), value=12.5):
    return FakeLakeSpark(FakeSnowflake({("SNOWMIG_DB", "TYPES", TABLE): {
        "columns": [("ID", "NUMBER", 38, 0), ("AMOUNT", *amount_type)],
        "rows": [{"ID": D(1), "AMOUNT": value}]}}), LAKE)


def _copy(copy_schema, spark, *, spec=SPEC, drift="refuse"):
    source = source_for(spark)
    live = source.live_columns("TYPES", [TABLE]).get(TABLE)
    return copy_schema.copy_table(
        source, "TYPES", TABLE, FQN, mode="append", verify="counts",
        retries=0, retry_base_delay=0, live_columns=live, column_spec=spec,
        source_type_drift=drift)


def test_a_decimal_rebuilt_as_float_is_refused_before_any_read(copy_schema):
    """The old check keyed off the SOURCE's decimals, so a column that is no
    longer one was never looked at."""
    spark = _spark()
    out = _copy(copy_schema, spark)
    assert out["status"] == "type_drift", out
    assert out["source_type_drift"] == {
        "AMOUNT": {"planned": "decimal(12,2)", "live": "float"}}
    assert "re-run assess and plan" in out["reason"].lower()
    assert _column_reads(spark) == [], "refused before any row is read"
    assert not any(s.startswith("INSERT") for s in spark.statements)


def test_a_non_decimal_change_is_caught_too():
    spec = [dict(SPEC[0]), {"name": "AMOUNT", "source_type": "text",
                            "target_type": "STRING", "read_expr": '"AMOUNT"',
                            "convert_expr": "`AMOUNT`"}]
    copy_schema = _load("02_copy_schema")
    spark = FakeLakeSpark(FakeSnowflake({("SNOWMIG_DB", "TYPES", TABLE): {
        "columns": [("ID", "NUMBER", 38, 0), ("AMOUNT", "BOOLEAN", None,
                                              None)],
        "rows": [{"ID": D(1), "AMOUNT": True}]}}),
        {FQN: [("ID", "decimal(38,0)"), ("AMOUNT", "string")]})
    out = _copy(copy_schema, spark, spec=spec)
    assert out["status"] == "type_drift", out
    assert out["source_type_drift"]["AMOUNT"] == {"planned": "text",
                                                  "live": "boolean"}


def test_convert_copies_under_the_new_type_and_never_says_plain_verified(
        copy_schema):
    spark = _spark()
    out = _copy(copy_schema, spark, drift="convert")
    assert out["status"] == "verified_with_conversion", out
    assert spark.rows[FQN] == [{"ID": D(1), "AMOUNT": D("12.5")}]
    drift = out["source_type_drift"]["AMOUNT"]
    assert drift["planned"] == "decimal(12,2)" and drift["live"] == "float"
    assert drift["target"] == "decimal(12,2)"
    assert drift["read_expr"] == "TO_VARCHAR(\"AMOUNT\", 'TME')"
    assert "never reviewed" in drift["warning"]
    assert out["read"]["converted"] == out["source_type_drift"]


def test_convert_never_overrides_the_narrower_decimal_refusal(copy_schema):
    """NUMBER(12,2) widened to NUMBER(20,2) cannot land in DECIMAL(12,2)
    without overflow, whatever the drift mode."""
    spark = _spark(amount_type=("NUMBER", 20, 2), value=D("1.50"))
    out = _copy(copy_schema, spark, drift="convert")
    assert out["status"] == "type_drift", out
    assert "AMOUNT" in out["type_drift"]


def test_an_unchanged_table_is_plain_verified(copy_schema):
    spark = _spark(amount_type=("NUMBER", 12, 2), value=D("1.50"))
    out = _copy(copy_schema, spark, drift="convert")
    assert out["status"] == "verified", out
    assert "source_type_drift" not in out


def test_a_spec_without_source_type_is_not_compared(copy_schema):
    """A plan written before `source_type` was recorded: nothing to compare
    with, so the copy behaves as it did."""
    old = [{k: v for k, v in c.items() if k != "source_type"} for c in SPEC]
    spark = _spark(amount_type=("NUMBER", 12, 2), value=D("1.50"))
    assert _copy(copy_schema, spark, spec=old)["status"] == "verified"


# ----------------------------------------------------- the whole stage

def _stage_plan(tmp_path, drift=None):
    stmt = {"source_identifier": f"SNOWMIG_DB.TYPES.{TABLE}",
            "object_type": "TABLE", "target_fqn": f"lake.types.{TABLE}",
            "expected_columns": [{"name": c["name"], "type": c["target_type"]}
                                 for c in SPEC],
            "columns": SPEC}
    plan = {"statements": [stmt]}
    if drift:
        plan["source_type_drift"] = drift
    reports = tmp_path / "reports"
    reports.mkdir()
    (tmp_path / "plan").mkdir()
    (tmp_path / "plan" / "ddl_plan.json").write_text(json.dumps(plan),
                                                     encoding="utf-8")
    (reports / "discovery_manifest.json").write_text(json.dumps({"schemas": [
        {"name": "TYPES", "tables": [{"name": TABLE}], "views": [],
         "errors": []}]}), encoding="utf-8")
    config = tmp_path / "source.json"
    config.write_text(json.dumps({"snowflake": {
        "account": "acct", "warehouse": "WH", "database": "SNOWMIG_DB",
        "user": "svc", "auth": "password", "password": "p",
        "schema": "TYPES"}}), encoding="utf-8")
    return reports, config


def test_the_stage_refuses_by_default_and_exits_1(monkeypatch, tmp_path):
    reports, config = _stage_plan(tmp_path)
    rc, report = _main(monkeypatch, _spark(), reports, config,
                       "--mode", "append")
    assert rc == 1
    assert report["tables"][TABLE]["status"] == "type_drift"


def test_the_stage_converts_when_the_plan_says_so_and_resumes_past_it(
        monkeypatch, tmp_path):
    reports, config = _stage_plan(tmp_path, drift="convert")
    spark = _spark()
    rc, report = _main(monkeypatch, spark, reports, config,
                       "--mode", "append")
    assert rc == 0, report
    assert report["tables"][TABLE]["status"] == "verified_with_conversion"
    # A re-run in append mode must not copy it again (that would double it).
    rc, report = _main(monkeypatch, spark, reports, config,
                       "--mode", "append")
    assert rc == 0
    assert spark.rows[FQN] == [{"ID": D(1), "AMOUNT": D("12.5")}]


def test_reconcile_reports_the_conversion_as_its_own_verdict():
    reconcile = _load("03_reconcile")
    assert "MIGRATED_WITH_CONVERSION" not in reconcile.PROBLEM_VERDICTS


# ----------------------------------------------------- parity with the plan

@pytest.mark.parametrize("data_type,precision,scale", [
    ("NUMBER", 38, 0), ("NUMBER", 12, 2), ("FLOAT", None, None),
    ("TEXT", None, None), ("TIMESTAMP_NTZ", None, None),
    ("VECTOR", None, None), ("BOOLEAN", None, None)])
def test_the_planned_and_live_type_spellings_agree(data_type, precision,
                                                   scale):
    spark = FakeLakeSpark(FakeSnowflake({("SNOWMIG_DB", "TYPES", "T"): {
        "columns": [("C", data_type, precision, scale)], "rows": []}}), {})
    live = source_for(spark).live_columns("TYPES", ["T"])["T"]["C"]
    assert live == source_type_key(data_type, precision, scale)


@pytest.mark.parametrize("data_type,target", [
    ("NUMBER(12,2)", "decimal(12,2)"), ("FLOAT", "decimal(12,2)"),
    ("FLOAT", "double"), ("TEXT", "string"), ("TIME", "string"),
    ("TIMESTAMP_NTZ", "timestamp"), ("TIMESTAMP_TZ", "timestamp"),
    ("TIMESTAMP_LTZ", "timestamp"), ("VARIANT", "string"),
    ("OBJECT", "string"), ("VECTOR", "array<float>"),
    ("VARIANT", "map<string, string>"), ("GEOGRAPHY", "string"),
    ("BOOLEAN", "boolean"), ("DATE", "date")])
def test_the_live_conversion_is_the_planned_one_for_the_same_type(data_type,
                                                                  target):
    """The notebook cannot import the engine, so the rules are mirrored;
    this holds the mirror to the original."""
    assert live_copy_expressions(data_type, target, name='A"b') == \
        copy_expressions(data_type, target, name='A"b')
