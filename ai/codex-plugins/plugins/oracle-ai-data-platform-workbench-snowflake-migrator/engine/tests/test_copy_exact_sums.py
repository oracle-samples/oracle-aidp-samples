"""`--verify counts+sums` sums the SOURCE in Snowflake, exactly.

The decimal sums were both Spark sums: one over the target, one over the
source as the copy had READ it. Live 2026-09-29 the connector's reading of
NUMBER is lossy -- NUMBER(38,37) arrives cut to ten significant digits --
so a copy that lost digits summed the same lost digits on both sides and
came back `verified`: the check agreed with the defect it exists to catch.
It also read every row of the source a second time, through Spark, to add
them up.

Now the source side is ONE qualified pushdown per table,
`select sum("C")::VARCHAR as "C", ... from "DB"."SCHEMA"."TABLE"`:
Snowflake adds its own NUMBERs exactly and the `::VARCHAR` carries the
total past the connector's typing untouched (the same live-verified trick
the copy's reads use). The target side is Spark's SUM over the Delta
column cast to DECIMAL(38, source scale). The two totals are compared as
exact decimals, so `1.50` and `1.5` are the same number and nothing is
rounded.
"""
import decimal
import json

import pytest

from dataplane.snowmig_source import assert_pushdown_read_only
from fake_pushdown import FakeLakeSpark, FakeSnowflake, source_for
from test_data_migration_scripts import _inject_spark, _load

D = decimal.Decimal
_TINY = D("0.1234567890123456789012345678901234567")


@pytest.fixture(scope="module")
def copy_schema():
    return _load("02_copy_schema")


def _estate(rows):
    return {("SNOWMIG_DB", "TYPES", "T"): {
        "columns": [("ID", "NUMBER", 38, 0), ("N", "NUMBER", 38, 37),
                    ("NAME", "TEXT", None, None)],
        "rows": rows}}


_LAKE = {"`lake`.`types`.`T`": [("ID", "decimal(38,0)"),
                                 ("N", "decimal(38,37)"), ("NAME", "string")]}
_SPEC = [{"name": "ID", "target_type": "DECIMAL(38,0)",
          "read_expr": '"ID"::VARCHAR',
          "convert_expr": "CAST(`ID` AS DECIMAL(38,0))"},
         {"name": "N", "target_type": "DECIMAL(38,37)",
          "read_expr": '"N"::VARCHAR',
          "convert_expr": "CAST(`N` AS DECIMAL(38,37))"},
         {"name": "NAME", "target_type": "STRING", "read_expr": '"NAME"',
          "convert_expr": "`NAME`"}]


def _copy(copy_schema, spark, spec):
    source = source_for(spark)
    live = source.live_columns("TYPES", ["T"]).get("T")
    return copy_schema.copy_table(
        source, "TYPES", "T", "`lake`.`types`.`T`", mode="append",
        verify="counts+sums", retries=0, retry_base_delay=0,
        live_columns=live, column_spec=spec)


def _sum_pushdowns(spark):
    return [s for s in spark.pushdowns if s.startswith("select sum(")]


def test_the_source_is_summed_in_snowflake_in_one_read(copy_schema):
    spark = FakeLakeSpark(FakeSnowflake(_estate(
        [{"ID": D(1), "N": _TINY, "NAME": "a"},
         {"ID": D(2), "N": _TINY, "NAME": "b"}])), _LAKE)
    out = _copy(copy_schema, spark, _SPEC)
    assert out["status"] == "verified", out
    assert _sum_pushdowns(spark) == [
        'select sum("ID")::VARCHAR as "ID", sum("N")::VARCHAR as "N" '
        'from "SNOWMIG_DB"."TYPES"."T"']
    assert_pushdown_read_only(_sum_pushdowns(spark)[0])
    assert not any("SUM(" in s and "snowmig_src_" in s
                   for s in spark.statements), \
        "the source is no longer re-read through Spark to be summed"
    assert any("SUM(" in s and "`lake`.`types`.`T`" in s
               for s in spark.statements)
    assert out["decimal_columns_checked"] == ["ID", "N"]
    assert "Snowflake" in out["sum_method"]


def test_digits_the_read_lost_are_a_sum_mismatch_not_a_pass(copy_schema):
    """An older plan reads N bare: the connector keeps ten significant
    digits. The target then sums to 0.2469135780000...; the source, summed
    in Snowflake, to 0.2469135780246... -- the loss is caught. Summed on
    both sides in Spark, both were the lossy number and it passed."""
    spark = FakeLakeSpark(FakeSnowflake(_estate(
        [{"ID": D(1), "N": _TINY, "NAME": "a"},
         {"ID": D(2), "N": _TINY, "NAME": "b"}])), _LAKE)
    out = _copy(copy_schema, spark, None)
    assert out["status"] == "sum_mismatch", out
    drift = out["sum_drift"]["N"]
    assert D(drift["source"]) == D("0.2469135780246913578024691357802469134"),         "twice _TINY, all 37 decimals"
    assert D(drift["target"]) == D("0.2469135780000000000000000000000000000")
    assert "ID" not in out["sum_drift"], "small integers survive the connector"


def test_an_empty_table_sums_to_null_on_both_sides(copy_schema):
    spark = FakeLakeSpark(FakeSnowflake(_estate([])), _LAKE)
    out = _copy(copy_schema, spark, _SPEC)
    assert out["status"] == "verified", out


@pytest.mark.parametrize("a,b,equal", [
    ("1.50", "1.5", True), ("0", "0.00", True), (None, None, True),
    ("1.51", "1.5", False), (None, "0", False),
    ("12345678901234567890123456789012345678",
     "12345678901234567890123456789012345678", True),
    ("12345678901234567890123456789012345678",
     "12345678901234567890123456789012345679", False)])
def test_totals_compare_as_exact_decimals(copy_schema, a, b, equal):
    assert copy_schema._sums_equal(a, b) is equal


def test_the_stage_verifies_sums_the_same_in_parallel(monkeypatch, tmp_path):
    reports = tmp_path / "reports"
    reports.mkdir()
    (tmp_path / "plan").mkdir()
    (tmp_path / "plan" / "ddl_plan.json").write_text(json.dumps({"statements": [
        {"source_identifier": "SNOWMIG_DB.TYPES.T", "object_type": "TABLE",
         "target_fqn": "lake.types.T", "columns": _SPEC,
         "expected_columns": [{"name": c["name"], "type": c["target_type"]}
                              for c in _SPEC]}]}), encoding="utf-8")
    (reports / "discovery_manifest.json").write_text(json.dumps({"schemas": [
        {"name": "TYPES", "tables": [{"name": "T"}], "views": [],
         "errors": []}]}), encoding="utf-8")
    config = tmp_path / "source.json"
    config.write_text(json.dumps({
        "account": "acct", "warehouse": "WH", "database": "SNOWMIG_DB",
        "user": "svc", "auth": "password", "password": "p",
        "schema": "TYPES"}), encoding="utf-8")
    spark = FakeLakeSpark(FakeSnowflake(_estate(
        [{"ID": D(7), "N": _TINY, "NAME": "a"}])), _LAKE)
    _inject_spark(monkeypatch, spark)
    rc = _load("02_copy_schema").main(
        ["--target-catalog", "lake", "--schema", "TYPES", "--reports-dir",
         str(reports), "--source-config", str(config), "--output-dir", "",
         "--mode", "append", "--verify", "counts+sums", "--parallel", "8"])
    rec = json.loads((reports / "copy_report_types.json").read_text(
        encoding="utf-8"))["tables"]["T"]
    assert rc == 0, rec
    assert rec["status"] == "verified"
    assert rec["decimal_columns_checked"] == ["ID", "N"]
    assert len(_sum_pushdowns(spark)) == 1
