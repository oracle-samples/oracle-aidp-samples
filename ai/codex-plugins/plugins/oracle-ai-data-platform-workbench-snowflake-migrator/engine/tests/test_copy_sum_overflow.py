"""A decimal total past 38 digits is `sum_not_comparable`, said as such.

`--verify counts+sums` sums every source DECIMAL column at the source's
scale on both sides: in Snowflake (`sum("C")::VARCHAR`, round 4) and in
Spark over the target (`SUM(CAST(c AS DECIMAL(38, s)))`). Both engines
keep that total at precision 38 and the column's own scale, so a
high-scale column runs out of integer digits fast: a NUMBER(38,37) holds
one digit before the point, and 100 rows of 0.12 add up to 12. Then

* Snowflake's SUM errors ("Number out of representable range") -- and the
  table was recorded `failed`, insert_completed, "verification raised,
  re-copy with --mode overwrite", a re-copy that fails the same way every
  time; or, where it returns the total, Spark's SUM over the target does
  not: non-ANSI Spark (AIDP runs Spark 3.5.0 with ANSI off) returns NULL on
  a DECIMAL overflow, and the table read as `sum_mismatch` with a target
  of None and nothing saying why;
* before round 4 both sides were Spark sums, both NULL, and NULL == NULL
  read as `verified`: a check that never ran, recorded as a pass.

Neither is a defect in the copy, and neither is a pass. The column's total
cannot be held in 38 digits by either engine, so it is recorded under
`sums_not_comparable` with the reason, the other decimal columns are still
compared, and the table is `sum_not_comparable` -- a problem status (the
check the operator asked for did not run in full), whose reason names the
way out: `--verify counts` accepts the count check for that table.

The error text and the NULL are what Snowflake's and Spark's documented
DECIMAL rules produce; neither overflow has been reproduced on the live
estate. The fakes now do what those rules say: the fake
Spark's DECIMAL(38,s) SUM overflows to NULL instead of being exact at 80
digits, and the fake Snowflake's SUM errors past NUMBER(38,s).
"""
import decimal
import json

import pytest

from dataplane.snowmig_source import assert_pushdown_read_only
from fake_pushdown import FakeLakeSpark, FakeSnowflake, source_for
from test_data_migration_scripts import (
    _CatalogSpark, _inject_spark, _load, _manifest, _seeded_copy_report)

D = decimal.Decimal
_TGT = "`lake`.`types`.`T`"


@pytest.fixture(scope="module")
def copy_schema():
    return _load("02_copy_schema")


def _estate(n=100, value="0.12"):
    return {("SNOWMIG_DB", "TYPES", "T"): {
        "columns": [("ID", "NUMBER", 38, 0), ("N", "NUMBER", 38, 37)],
        "rows": [{"ID": D(i), "N": D(value)} for i in range(n)]}}


_LAKE = {_TGT: [("ID", "decimal(38,0)"), ("N", "decimal(38,37)")]}
_SPEC = [{"name": "ID", "target_type": "DECIMAL(38,0)",
          "read_expr": '"ID"::VARCHAR',
          "convert_expr": "CAST(`ID` AS DECIMAL(38,0))"},
         {"name": "N", "target_type": "DECIMAL(38,37)",
          "read_expr": '"N"::VARCHAR',
          "convert_expr": "CAST(`N` AS DECIMAL(38,37))"}]


def _copy(copy_schema, spark):
    source = source_for(spark)
    live = source.live_columns("TYPES", ["T"]).get("T")
    return copy_schema.copy_table(
        source, "TYPES", "T", _TGT, mode="append", verify="counts+sums",
        retries=0, retry_base_delay=0, live_columns=live, column_spec=_SPEC)


# ------------------------------------------- the fakes follow the rules

def test_the_fake_spark_decimal_38_sum_overflows_to_null(copy_schema):
    spark = FakeLakeSpark(FakeSnowflake({}), _LAKE)
    spark.rows[_TGT] = [{"ID": D(i), "N": D("0.12")} for i in range(100)]
    row = spark.sql(
        "SELECT CAST(SUM(CAST(`N` AS DECIMAL(38,37))) AS STRING) AS `N`, "
        "COUNT(`N`) AS `snowmig_nonnull_0`, "
        "CAST(SUM(CAST(`ID` AS DECIMAL(38,0))) AS STRING) AS `ID`, "
        f"COUNT(`ID`) AS `snowmig_nonnull_1` FROM {_TGT}").collect()[0]
    assert row["N"] is None, "12 needs two integer digits; DECIMAL(38,37) has one"
    assert row["snowmig_nonnull_0"] == 100
    assert D(row["ID"]) == D(4950)
    spark.rows[_TGT] = spark.rows[_TGT][:8]
    row = spark.sql("SELECT CAST(SUM(CAST(`N` AS DECIMAL(38,37))) AS STRING) "
                    f"AS `N` FROM {_TGT}").collect()[0]
    assert D(row["N"]) == D("0.96"), "under the limit the sum is exact"


def test_the_fake_snowflake_sum_errors_past_number_38():
    spark = FakeLakeSpark(FakeSnowflake(_estate()), _LAKE)
    with pytest.raises(RuntimeError, match="out of representable range"):
        spark.snowflake.run('select sum("N")::VARCHAR as "N" from '
                            '"SNOWMIG_DB"."TYPES"."T"', spark)


# ------------------------------------------------------ connector mode

def test_a_snowflake_sum_overflow_is_not_comparable_not_a_recopy(copy_schema):
    spark = FakeLakeSpark(FakeSnowflake(_estate()), _LAKE)
    out = _copy(copy_schema, spark)
    assert out["status"] == "sum_not_comparable", out
    assert "insert_completed" not in out, \
        "not 'verification raised': a re-copy fails the same way every time"
    assert out["target_count"] == out["source_count"] == 100
    assert list(out["sums_not_comparable"]) == ["N"]
    assert "38 digits" in out["sums_not_comparable"]["N"]
    assert out["decimal_columns_checked"] == ["ID"], \
        "the other decimal column is still compared"
    assert "sum_drift" not in out
    assert "--verify counts" in out["reason"]
    assert "38 digits" in out["sum_method"]
    sums = [s for s in spark.pushdowns if s.startswith("select sum(")]
    assert sums == [
        'select sum("ID")::VARCHAR as "ID", sum("N")::VARCHAR as "N" '
        'from "SNOWMIG_DB"."TYPES"."T"',
        'select sum("ID")::VARCHAR as "ID" from "SNOWMIG_DB"."TYPES"."T"',
        'select sum("N")::VARCHAR as "N" from "SNOWMIG_DB"."TYPES"."T"'], \
        "on overflow each column is summed alone, to find which one"
    for s in sums:
        assert_pushdown_read_only(s)


def test_a_total_snowflake_returns_past_38_digits_is_not_comparable(
        copy_schema):
    """Where Snowflake returns the wide total, Spark's SUM over the target
    is NULL: not a mismatch with a target of None."""
    spark = FakeLakeSpark(FakeSnowflake(_estate(), sum_overflow="wide"), _LAKE)
    out = _copy(copy_schema, spark)
    assert out["status"] == "sum_not_comparable", out
    assert list(out["sums_not_comparable"]) == ["N"]
    assert "38 digits" in out["sums_not_comparable"]["N"]
    assert "sum_drift" not in out


def test_a_sum_error_that_is_not_an_overflow_still_raises(copy_schema):
    spark = FakeLakeSpark(FakeSnowflake(_estate(8)), _LAKE)
    real = spark.snowflake.run

    def run(sql, spark_):
        if sql.startswith("select sum("):
            raise RuntimeError("CONNECTOR_0007 - session expired")
        return real(sql, spark_)
    spark.snowflake.run = run
    out = _copy(copy_schema, spark)
    assert out["status"] == "failed"
    assert out["insert_completed"] is True, "that one IS a verification failure"


def test_totals_that_fit_are_still_compared_exactly(copy_schema):
    spark = FakeLakeSpark(FakeSnowflake(_estate(8)), _LAKE)
    out = _copy(copy_schema, spark)
    assert out["status"] == "verified", out
    assert out["decimal_columns_checked"] == ["ID", "N"]
    assert "sums_not_comparable" not in out


# --------------------------------------------- Spark sums on both sides

def _sum_check(copy_schema, src_rows, tgt_rows, source_sums=None):
    spark = FakeLakeSpark(FakeSnowflake({}), _LAKE)
    spark.views["src"] = (["ID", "N"], src_rows)
    spark.rows[_TGT] = tgt_rows
    return copy_schema._sum_check(
        spark, "`src`", _TGT, {"ID": "decimal(38,0)", "N": "decimal(38,37)"},
        source_sums)


def test_two_null_spark_sums_over_real_values_are_not_a_pass(copy_schema):
    """The external-catalog path sums both sides in Spark: both overflow to
    NULL, and NULL == NULL was `verified`."""
    rows = [{"ID": D(i), "N": D("0.12")} for i in range(100)]
    sums = _sum_check(copy_schema, rows, [dict(r) for r in rows])
    assert sums["drift"] == {}
    assert list(sums["not_comparable"]) == ["N"]
    out = copy_schema._settle({}, source_count=100, src_after=100,
                              tgt_after=100, sums=sums)
    assert out["status"] == "sum_not_comparable"


def test_an_empty_or_all_null_column_is_still_null_equals_null(copy_schema):
    sums = _sum_check(copy_schema, [{"ID": D(1), "N": None}],
                      [{"ID": D(1), "N": None}])
    assert sums["drift"] == {} and sums["not_comparable"] == {}


def test_a_target_sum_that_overflowed_while_the_source_fits(copy_schema):
    sums = _sum_check(copy_schema, [],
                      [{"ID": D(i), "N": D("0.12")} for i in range(100)],
                      source_sums=lambda cols: {"ID": "4950", "N": "1.08"})
    assert list(sums["not_comparable"]) == ["N"]
    assert "target" in sums["not_comparable"]["N"]


# ---------------------------------------------------------- the stage

def _files(tmp_path):
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
    return reports, config


def _main(reports, config, *argv):
    rc = _load("02_copy_schema").main(
        ["--target-catalog", "lake", "--schema", "TYPES", "--reports-dir",
         str(reports), "--source-config", str(config), "--output-dir", "",
         *argv])
    return rc, json.loads((reports / "copy_report_types.json").read_text(
        encoding="utf-8"))["tables"]["T"]


def test_the_stage_records_it_and_a_rerun_does_not_soften_it(
        monkeypatch, tmp_path):
    reports, config = _files(tmp_path)
    spark = FakeLakeSpark(FakeSnowflake(_estate()), _LAKE)
    _inject_spark(monkeypatch, spark)
    rc, rec = _main(reports, config, "--mode", "append", "--verify",
                    "counts+sums", "--parallel", "8")
    assert rc == 1
    assert rec["status"] == "sum_not_comparable"
    rc, rec = _main(reports, config, "--verify", "counts+sums")
    assert rc == 1
    assert rec["status"] == "sum_not_comparable", \
        "a skip-existing re-run re-verified nothing"
    rc, rec = _main(reports, config, "--mode", "overwrite", "--verify",
                    "counts")
    assert rc == 0 and rec["status"] == "verified", \
        "--verify counts is the named way to accept the count check"


def test_reconcile_reads_it_as_a_copy_problem(tmp_path):
    reconcile = _load("03_reconcile")
    _seeded_copy_report(tmp_path, "T", {"status": "sum_not_comparable",
                                        "reason": "N: exceeds 38 digits"})
    spark = _CatalogSpark({"`lake`.`SALES`.`T`": [("A", "string")]})
    rec = reconcile.reconcile(spark, manifest=_manifest("T"),
                              target_catalog="lake", reports=tmp_path,
                              counts=False)
    assert rec["schemas"][0]["tables"][0]["verdict"] == \
        "STRUCTURE_ONLY_COPY_FAILED"
