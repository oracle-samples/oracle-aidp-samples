"""The copy and the reconcile address the schema the structure step created.

01_create_structure takes the target schema from the approved plan's
`target_fqn` (lower-cased by the bronze naming, or `db_<schema>` under a
prefix). 02_copy_schema still derived it from `--schema`, and 03_reconcile
from whichever report was on disk, and both compared targets as exact
strings. Reproduced on the real stage code:

  * a prefix plan: 01 created `lake.db_core.*`; 02 looked in `lake.CORE`,
    recorded every table `target_missing`, said "no structure report for
    lake.CORE was found", and exited 0 with 0 rows copied;
  * a tenancy holding the pre-fix `lake.CORE` copy report (the live state
    after 2026-09-24): 02 skipped everything as "already verified", 03
    reported MIGRATED_VERIFIED and exited 0 -- while the approved tables held
    0 rows;
  * the default plan (`lake.core`): the structure report's target `lake.core`
    did not string-equal the copy's `lake.CORE`, so the report was dropped,
    the scope fell back to the whole manifest, and a table 01 had flagged
    TYPE DRIFT was INSERTed into and recorded verified.

The stages run the real `main()` here, over a fake catalog that resolves
names case-insensitively the way Spark does.
"""
import json
import re

import pytest

from test_data_migration_scripts import (
    _CatalogSpark, _inject_spark, _load, _report)

_DOTTED = re.compile(r"(?:`[^`]*`\.)+`[^`]*`")


class _CaselessSpark(_CatalogSpark):
    """`_CatalogSpark`, but a qualified name resolves case-insensitively, as
    it does in Spark: `lake`.`CORE`.`T` and `lake`.`core`.`t` are one table.
    Keys in `catalog` and `counts` are held lower-cased."""

    def __init__(self, catalog=None):
        super().__init__({k.lower(): v for k, v in (catalog or {}).items()})

    def sql(self, statement):
        return super().sql(
            _DOTTED.sub(lambda m: m.group(0).lower(), statement))


class _Source:
    """External-catalog shaped `SnowflakeSource`: the source is `ext`."""

    def __init__(self, spark, **_kwargs):
        self.spark = spark

    def describe(self):
        return {"mode": "fake"}

    def source_counts(self, schema, tables):
        return {t: self.spark.counts.get(f"`ext`.`{schema}`.`{t}`".lower(), 0)
                for t in tables}

    def register_temp_view(self, schema, table, view):
        return f"`ext`.`{schema}`.`{table}`"

    def drop_temp_view(self, view):
        pass


_COLS = [{"name": "ID", "type": "DECIMAL(38,0)"},
         {"name": "NOTE", "type": "STRING"}]
_SPARK_COLS = [("ID", "decimal(38,0)"), ("NOTE", "string")]


def _estate(tmp_path, tables, targets, *, columns=None):
    """A manifest for schema CORE and a ddl_plan whose `target_fqn` places
    each table: `targets` is {table: "catalog.schema.name"}."""
    reports = tmp_path / "reports"
    reports.mkdir(parents=True)
    (reports / "discovery_manifest.json").write_text(json.dumps(
        {"schemas": [{"name": "CORE",
                      "tables": [{"name": t, "columns": []} for t in tables],
                      "views": [], "errors": []}]}), encoding="utf-8")
    plan_dir = tmp_path / "plan"
    plan_dir.mkdir()
    (plan_dir / "ddl_plan.json").write_text(json.dumps({"statements": [
        {"source_identifier": f"DB.CORE.{t}", "object_type": "TABLE",
         "target_fqn": fqn,
         "expected_columns": (columns or {}).get(t, _COLS)}
        for t, fqn in targets.items()]}), encoding="utf-8")
    return reports


def _run(name, monkeypatch, spark, argv):
    _inject_spark(monkeypatch, spark)
    module = _load(name)
    if hasattr(module, "SnowflakeSource"):
        monkeypatch.setattr(module, "SnowflakeSource", _Source)
    return module.main(argv)


def _structure(monkeypatch, spark, reports, *extra):
    return _run("01_create_structure", monkeypatch, spark,
                ["--target-catalog", "lake", "--schema", "CORE",
                 "--reports-dir", str(reports), *extra])


def _copy(monkeypatch, spark, reports, *extra):
    return _run("02_copy_schema", monkeypatch, spark,
                ["--target-catalog", "lake", "--schema", "CORE",
                 "--reports-dir", str(reports), *extra])


def _reconcile(monkeypatch, spark, reports):
    rc = _run("03_reconcile", monkeypatch, spark,
              ["--target-catalog", "lake", "--reports-dir", str(reports)])
    return rc, _report(reports, "reconciliation.json")


def _inserts(spark):
    return [s for s in spark.statements if s.upper().startswith("INSERT")]


# ------------------------------------------------------------ a prefix plan

def test_the_copy_lands_rows_in_the_schema_the_plan_named(
        monkeypatch, tmp_path, capsys):
    reports = _estate(tmp_path, ["T"], {"T": "lake.db_core.t"})
    spark = _CaselessSpark({"`ext`.`CORE`.`T`": _SPARK_COLS})
    spark.counts = {"`ext`.`core`.`t`": 4}
    assert _structure(monkeypatch, spark, reports) == 0
    assert "`lake`.`db_core`.`t`" in spark.catalog

    rc = _copy(monkeypatch, spark, reports)
    out = capsys.readouterr().out
    report = _report(reports, "copy_report_core.json")
    assert report["target"].lower() == "lake.db_core"
    assert report["tables"]["T"]["status"] == "verified", report
    assert spark.counts.get("`lake`.`db_core`.`t`") == 4, \
        "the rows land where 01 created the table"
    assert "no structure report" not in out
    assert rc == 0

    rc, rec = _reconcile(monkeypatch, spark, reports)
    schema = rec["schemas"][0]
    assert schema["target_schema"].lower() == "db_core"
    assert schema["tables"][0]["verdict"] == "MIGRATED_VERIFIED"
    assert rc == 0


def test_a_table_the_structure_step_created_but_the_copy_cannot_find_fails(
        monkeypatch, tmp_path):
    """`target_missing` was never counted as a failure, so a copy that found
    none of the tables 01 had just created exited 0 with 0 rows moved. For a
    table the structure report says is there, it is a problem."""
    reports = _estate(tmp_path, ["T"], {"T": "lake.db_core.t"})
    spark = _CaselessSpark({"`ext`.`CORE`.`T`": _SPARK_COLS})
    spark.counts = {"`ext`.`core`.`t`": 4}
    assert _structure(monkeypatch, spark, reports) == 0
    del spark.catalog["`lake`.`db_core`.`t`"]         # gone since 01 ran

    rc = _copy(monkeypatch, spark, reports)
    assert _report(reports, "copy_report_core.json")["tables"]["T"][
        "status"] == "target_missing"
    assert rc == 1, "a job that copied nothing into a created table failed"


# ------------------------------- the stale pre-fix report on the live tenant

def test_a_copy_report_for_the_old_schema_is_not_evidence_for_the_plans(
        monkeypatch, tmp_path, capsys):
    reports = _estate(tmp_path, ["CUSTOMERS"],
                      {"CUSTOMERS": "lake.db_core.customers"})
    (reports / "copy_report_core.json").write_text(json.dumps(
        {"schema": "CORE", "target": "lake.CORE",
         "tables": {"CUSTOMERS": {"status": "verified", "source_count": 5,
                                  "target_count": 5}}}), encoding="utf-8")
    spark = _CaselessSpark({"`ext`.`CORE`.`CUSTOMERS`": _SPARK_COLS,
                            # what the pre-fix run left behind
                            "`lake`.`CORE`.`CUSTOMERS`": _SPARK_COLS})
    spark.counts = {"`ext`.`core`.`customers`": 5,
                    "`lake`.`core`.`customers`": 5}
    assert _structure(monkeypatch, spark, reports) == 0

    # Reconcile BEFORE the copy re-runs: the approved table holds 0 rows.
    rc, rec = _reconcile(monkeypatch, spark, reports)
    schema = rec["schemas"][0]
    assert schema["target_schema"].lower() == "db_core"
    row = schema["tables"][0]
    assert row["verdict"] != "MIGRATED_VERIFIED", row
    assert row["copy"] == "not_attempted"
    assert schema["reports_ignored_for_other_catalog"] == {"copy": "lake.CORE"}
    assert "MIGRATED_VERIFIED" not in rec["totals"]

    capsys.readouterr()
    rc = _copy(monkeypatch, spark, reports)
    out = capsys.readouterr().out
    assert "already verified" not in out
    assert spark.counts.get("`lake`.`db_core`.`customers`") == 5
    assert rc == 0


# --------------------------------------- the default plan: case, not prefix

def test_a_structure_report_in_another_case_still_scopes_the_copy(
        monkeypatch, tmp_path):
    """Default naming lower-cases the schema: the structure report says
    `lake.core`. A drifted STAFF (same types, reordered) must stay out of
    the copy's scope however `--schema` is spelled."""
    reports = _estate(tmp_path, ["ORDERS", "STAFF"],
                      {"ORDERS": "lake.core.orders",
                       "STAFF": "lake.core.staff"},
                      columns={"STAFF": [{"name": "FIRST_NAME", "type": "STRING"},
                                         {"name": "LAST_NAME", "type": "STRING"}]})
    spark = _CaselessSpark({
        "`ext`.`CORE`.`ORDERS`": _SPARK_COLS,
        "`ext`.`CORE`.`STAFF`": [("FIRST_NAME", "string"),
                                 ("LAST_NAME", "string")],
        "`lake`.`core`.`staff`": [("LAST_NAME", "string"),
                                  ("FIRST_NAME", "string")]})
    spark.counts = {"`ext`.`core`.`orders`": 3, "`ext`.`core`.`staff`": 7}
    assert _structure(monkeypatch, spark, reports) == 1
    assert _report(reports, "structure_report_core.json")["objects"][
        "STAFF"]["status"] == "type_drift"

    rc = _copy(monkeypatch, spark, reports)
    report = _report(reports, "copy_report_core.json")
    assert not any("staff" in s.lower() for s in _inserts(spark)), \
        _inserts(spark)
    assert report["tables"].get("STAFF", {}).get("status") != "verified"
    assert report["tables"]["ORDERS"]["status"] == "verified"
    assert rc == 0


# ------------------------------------------------ what the operator passes

def test_a_target_schema_that_contradicts_the_plan_is_refused(
        monkeypatch, tmp_path, capsys):
    reports = _estate(tmp_path, ["T"], {"T": "lake.db_core.t"})
    spark = _CaselessSpark({"`ext`.`CORE`.`T`": _SPARK_COLS})
    rc = _copy(monkeypatch, spark, reports, "--target-schema", "elsewhere")
    assert rc == 1
    assert "contradicts" in capsys.readouterr().out
    assert not _inserts(spark)


def test_a_target_schema_in_another_case_is_the_plans_schema(
        monkeypatch, tmp_path):
    reports = _estate(tmp_path, ["T"], {"T": "lake.db_core.t"})
    spark = _CaselessSpark({"`ext`.`CORE`.`T`": _SPARK_COLS})
    spark.counts = {"`ext`.`core`.`t`": 2}
    assert _structure(monkeypatch, spark, reports) == 0
    rc = _copy(monkeypatch, spark, reports, "--target-schema", "DB_CORE")
    assert rc == 0
    assert spark.counts.get("`lake`.`db_core`.`t`") == 2


def test_a_structure_report_for_another_target_is_refused_not_widened(
        monkeypatch, tmp_path, capsys):
    """The plan is silent (no target_fqn) and 01 ran with --target-schema:
    its report targets `lake.custom`, this copy resolves `lake.CORE`. The
    copy used to drop the report and take the whole manifest as its scope."""
    reports = tmp_path / "reports"
    reports.mkdir()
    (reports / "discovery_manifest.json").write_text(json.dumps(
        {"schemas": [{"name": "CORE", "tables": [{"name": "T", "columns": []},
                                                 {"name": "U", "columns": []}],
                      "views": [], "errors": []}]}), encoding="utf-8")
    (reports / "structure_report_core.json").write_text(json.dumps(
        {"schema": "CORE", "target": "lake.custom",
         "objects": {"T": {"status": "created"}}}), encoding="utf-8")
    spark = _CaselessSpark({"`ext`.`CORE`.`T`": _SPARK_COLS,
                            "`ext`.`CORE`.`U`": _SPARK_COLS,
                            "`lake`.`CORE`.`U`": _SPARK_COLS})
    rc = _copy(monkeypatch, spark, reports)
    out = capsys.readouterr().out
    assert rc == 1
    assert "lake.custom" in out and "--target-schema" in out
    assert not _inserts(spark)


@pytest.mark.parametrize("spelling", ["lake.CORE", "LAKE.core"])
def test_a_copy_report_in_another_case_is_resumed(monkeypatch, tmp_path,
                                                  spelling, capsys):
    reports = _estate(tmp_path, ["T"], {"T": "lake.core.t"})
    (reports / "copy_report_core.json").write_text(json.dumps(
        {"schema": "CORE", "target": spelling,
         "tables": {"T": {"status": "verified", "source_count": 2,
                          "target_count": 2}}}), encoding="utf-8")
    spark = _CaselessSpark({"`ext`.`CORE`.`T`": _SPARK_COLS,
                            "`lake`.`core`.`t`": _SPARK_COLS})
    rc = _copy(monkeypatch, spark, reports)
    assert rc == 0
    assert "already verified" in capsys.readouterr().out
    assert not _inserts(spark)


# ------------------------- the plan names the table, the copy cannot find it
#
# `target_missing` counted as a failure only when the STRUCTURE REPORT listed
# the table as there. Two paths reach the copy with no such report for this
# target, and on both the approved plan still names the table here:
#
#   * `--tables T` beside a pre-fix structure report for `lake.CORE`: 02 set
#     that report aside ("--tables sets the scope"), recorded T
#     target_missing in `lake.db_core`, and exited 0 with 0 rows -- then 03
#     said NOT_MIGRATED and exited 0 too;
#   * 02 run before 01 (no structure report at all): the same, exit 0.
#
# That is "the job reads SUCCESS with 0 rows copied" for a table the reviewed
# plan puts at this target. The plan is evidence enough that it should exist.

def test_tables_beside_a_stale_structure_report_fails_a_planned_table(
        monkeypatch, tmp_path, capsys):
    reports = _estate(tmp_path, ["T"], {"T": "lake.db_core.t"})
    (reports / "structure_report_core.json").write_text(json.dumps(
        {"schema": "CORE", "target": "lake.CORE",
         "objects": {"T": {"status": "created"}}}), encoding="utf-8")
    spark = _CaselessSpark({"`ext`.`CORE`.`T`": _SPARK_COLS,
                            # what the pre-fix run left behind
                            "`lake`.`CORE`.`T`": _SPARK_COLS})
    spark.counts = {"`ext`.`core`.`t`": 4}

    rc = _copy(monkeypatch, spark, reports, "--tables", "T")
    rec = _report(reports, "copy_report_core.json")["tables"]["T"]
    assert rec["status"] == "target_missing"
    assert not _inserts(spark)
    assert rc == 1, "a planned table the copy could not find is a failure"
    assert "the approved plan places" in rec["reason"]
    assert "lake.db_core" in rec["reason"].lower()


def test_a_copy_before_the_structure_step_fails_a_planned_table(
        monkeypatch, tmp_path):
    reports = _estate(tmp_path, ["T"], {"T": "lake.db_core.t"})
    spark = _CaselessSpark({"`ext`.`CORE`.`T`": _SPARK_COLS})
    spark.counts = {"`ext`.`core`.`t`": 4}

    rc = _copy(monkeypatch, spark, reports)
    assert _report(reports, "copy_report_core.json")["tables"]["T"][
        "status"] == "target_missing"
    assert rc == 1


def test_a_table_the_plan_leaves_out_is_still_not_a_failure(
        monkeypatch, tmp_path):
    """The guard: `target_missing` for a table the plan does NOT place here
    (not_in_plan upstream) stays a finding, not a failed job."""
    reports = _estate(tmp_path, ["T", "U"], {"T": "lake.db_core.t"})
    spark = _CaselessSpark({"`ext`.`CORE`.`T`": _SPARK_COLS,
                            "`ext`.`CORE`.`U`": _SPARK_COLS})
    spark.counts = {"`ext`.`core`.`t`": 4, "`ext`.`core`.`u`": 1}
    assert _structure(monkeypatch, spark, reports) == 0

    rc = _copy(monkeypatch, spark, reports, "--tables", "T", "U")
    tables = _report(reports, "copy_report_core.json")["tables"]
    assert tables["T"]["status"] == "verified"
    assert tables["U"]["status"] == "target_missing"
    assert rc == 0


def test_the_refusal_offers_no_target_schema_the_plan_would_refuse(
        monkeypatch, tmp_path, capsys):
    """01 ran in --mode manifest (it reads no plan) and created lake.CORE;
    the plan on disk names lake.db_core. 02 refuses -- correctly -- but its
    hint said `pass --target-schema CORE where the plan names no target`.
    The plan here DOES name one, so that flag is refused as a contradiction:
    the hint pointed at a dead end."""
    reports = _estate(tmp_path, ["T"], {"T": "lake.db_core.t"})
    (reports / "structure_report_core.json").write_text(json.dumps(
        {"schema": "CORE", "target": "lake.CORE",
         "objects": {"T": {"status": "created"}}}), encoding="utf-8")
    spark = _CaselessSpark({"`ext`.`CORE`.`T`": _SPARK_COLS,
                            "`lake`.`CORE`.`T`": _SPARK_COLS})
    rc = _copy(monkeypatch, spark, reports)
    out = capsys.readouterr().out
    assert rc == 1
    assert "lake.CORE" in out and "01_create_structure" in out
    assert "--target-schema" not in out, out
    assert not _inserts(spark)
