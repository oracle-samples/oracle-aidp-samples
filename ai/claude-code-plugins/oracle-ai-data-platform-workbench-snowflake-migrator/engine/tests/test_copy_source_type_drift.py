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
    # The spec lives in ddl_plan.json, which only `ddl` writes: re-running
    # assess and plan alone would be refused again.
    assert "re-run assess, plan and ddl" in out["reason"].lower()
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
    with, so the copy behaves as it did -- and its record says the check
    did not run, never leaving it to read as checked."""
    old = [{k: v for k, v in c.items() if k != "source_type"} for c in SPEC]
    spark = _spark(amount_type=("NUMBER", 12, 2), value=D("1.50"))
    out = _copy(copy_schema, spark, spec=old)
    assert out["status"] == "verified"
    assert out["read"]["source_type_unchecked"] == ["ID", "AMOUNT"]
    assert "re-run" in out["read"]["source_type_note"].lower()
    assert "source_type_unchecked" not in _copy(
        copy_schema, _spark(amount_type=("NUMBER", 12, 2), value=D("1.50")))[
            "read"]


def test_a_type_change_the_conversion_is_unchanged_by_is_not_called_lossy(
        copy_schema):
    """NUMBER(12,2) re-created as NUMBER(10,2) still fits DECIMAL(12,2)
    and reads the same way: refused as a change, not as a rounding."""
    spark = _spark(amount_type=("NUMBER", 10, 2), value=D("1.50"))
    out = _copy(copy_schema, spark)
    assert out["status"] == "type_drift", out
    assert "could round" not in out["reason"]
    out = _copy(copy_schema, _spark(amount_type=("NUMBER", 10, 2),
                                    value=D("1.50")), drift="convert")
    warning = out["source_type_drift"]["AMOUNT"]["warning"]
    assert "mapping rules for its new type" not in warning.lower()
    assert "fixed read for the live type" in warning


# ----------------------------------------------------- TIME precision

TIME_FQN = "`lake`.`types`.`T_TIME`"


def _time_spark(live_precision):
    return FakeLakeSpark(FakeSnowflake({("SNOWMIG_DB", "TYPES", "T_TIME"): {
        "columns": [("T", "TIME", live_precision, None)],
        "rows": [{"T": "12:34:56.123456789"}]}}),
        {TIME_FQN: [("T", "string")]})


def _time_spec(planned_precision):
    from target.ddl import copy_spec
    return copy_spec([{"COLUMN_NAME": "T", "DATA_TYPE": "TIME",
                       "DATETIME_PRECISION": planned_precision,
                       "target_type": "STRING"}])


def _time_copy(copy_schema, spark, spec):
    source = source_for(spark)
    return copy_schema.copy_table(
        source, "TYPES", "T_TIME", TIME_FQN, mode="append", verify="counts",
        retries=0, retry_base_delay=0,
        live_columns=source.live_columns("TYPES", ["T_TIME"])["T_TIME"],
        column_spec=spec)


def test_a_time_whose_precision_rose_is_refused(copy_schema):
    """The planned read formats TIME(3) with FF3: run on a TIME(9) it
    drops the last six digits into a STRING with the count intact."""
    spec = _time_spec(3)
    assert spec[0]["read_expr"] == "TO_VARCHAR(\"T\", 'HH24:MI:SS.FF3')"
    out = _time_copy(copy_schema, _time_spark(9), spec)
    assert out["status"] == "type_drift", out
    assert out["source_type_drift"] == {
        "T": {"planned": "time(3)", "live": "time(9)"}}


def test_a_time_planned_without_its_precision_reads_all_nine_digits(
        copy_schema):
    """No recorded precision: the planned read already takes all nine
    digits, so no live precision can lose any."""
    spec = _time_spec(None)
    assert spec[0]["source_type"] == "time"
    out = _time_copy(copy_schema, _time_spark(9), spec)
    assert out["status"] == "verified", out
    assert _time_copy(copy_schema, _time_spark(3), _time_spec(3))[
        "status"] == "verified"


def test_the_time_precision_comes_from_the_detail_when_it_is_all_there_is():
    from target.ddl import copy_spec
    spec = copy_spec([{"COLUMN_NAME": "T", "DATA_TYPE": "TIME",
                       "type_detail": "TIME(6)", "target_type": "STRING"}])
    assert spec[0]["read_expr"] == "TO_VARCHAR(\"T\", 'HH24:MI:SS.FF6')"
    assert spec[0]["source_type"] == "time(6)"


@pytest.mark.parametrize("precision", [0, 3, 9])
def test_the_planned_and_live_time_spellings_agree(precision):
    spark = _time_spark(precision)
    live = source_for(spark).live_columns("TYPES", ["T_TIME"])["T_TIME"]["T"]
    assert live == source_type_key("TIME", datetime_precision=precision)
    assert live == f"time({precision})"


# ----------------------------------------------------- external-catalog mode

EXT = "`ext`.`TYPES`.`T_AMOUNT`"


class _ExternalSource:
    mode = "external-catalog"
    external_catalog = "ext"

    def __init__(self, spark):
        self.spark = spark

    def register_temp_view(self, schema, table, view):
        return EXT

    def drop_temp_view(self, view):
        pass


def _external(copy_schema, amount_type, drift="refuse"):
    from test_data_migration_scripts import _CatalogSpark
    spark = _CatalogSpark({EXT: [("ID", "decimal(38,0)"),
                                 ("AMOUNT", amount_type)],
                           FQN: [("ID", "decimal(38,0)"),
                                 ("AMOUNT", "decimal(12,2)")]})
    spark.counts = {EXT: 1, FQN: 0}
    out = copy_schema.copy_table(
        _ExternalSource(spark), "TYPES", TABLE, FQN, mode="append",
        verify="counts", retries=0, retry_base_delay=0, column_spec=SPEC,
        source_type_drift=drift)
    return spark, out


def test_external_catalog_refuses_a_float_into_a_decimal_column(copy_schema):
    """The plan maps only NUMBER to DECIMAL; a source column that is now a
    double would be rounded by the INSERT's store-assignment cast, and the
    sums never look at it (they follow the source's decimals)."""
    spark, out = _external(copy_schema, "double")
    assert out["status"] == "type_drift", out
    assert out["source_type_drift"]["AMOUNT"]["live"] == "double"
    assert not any(s.startswith("INSERT") for s in spark.statements)


def test_external_catalog_convert_copies_and_says_so(copy_schema):
    spark, out = _external(copy_schema, "double", drift="convert")
    assert out["status"] == "verified_with_conversion", out
    assert "never reviewed" in out["source_type_drift"]["AMOUNT"]["warning"]


def test_external_catalog_leaves_integer_and_decimal_sources_alone(
        copy_schema):
    for amount_type in ("decimal(12,2)", "bigint", "int"):
        _spark_, out = _external(copy_schema, amount_type)
        assert out["status"] == "verified", (amount_type, out)
        assert "source_type_drift" not in out


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


def test_the_stage_says_when_the_plan_carries_no_source_type(
        monkeypatch, tmp_path, capsys):
    """The log said drifted columns are refused even when no spec could be
    compared."""
    reports, config = _stage_plan(tmp_path)
    plan_path = tmp_path / "plan" / "ddl_plan.json"
    plan = json.loads(plan_path.read_text(encoding="utf-8"))
    for c in plan["statements"][0]["columns"]:
        del c["source_type"]
    plan_path.write_text(json.dumps(plan), encoding="utf-8")
    spark = _spark(amount_type=("NUMBER", 12, 2), value=D("1.50"))
    capsys.readouterr()
    rc, report = _main(monkeypatch, spark, reports, config,
                       "--mode", "append")
    assert rc == 0
    log = capsys.readouterr().out
    assert "NOT checked for 1 table(s)" in log
    assert report["tables"][TABLE]["read"]["source_type_unchecked"]


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
