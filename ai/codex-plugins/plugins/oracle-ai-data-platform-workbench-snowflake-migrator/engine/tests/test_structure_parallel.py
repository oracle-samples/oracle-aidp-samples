"""`01_create_structure --parallel N` creates N tables at once; views after.

Live 2026-09-29 (probe 3): CREATE TABLE on a warm cluster cost ~4 s a
table serially and ~2 s a table at 8 threads -- and a schema is created
one table at a time, each a DESCRIBE, a CREATE and a read-back. So the
stage runs its per-table work (`ddl-plan`, `manifest` and `ctas` alike) in
a thread pool, default 8, `--parallel 1` being the serial stage.

Two things must not move with the thread count:

* the record -- each table's status is decided by the same
  create-and-read-back on the same facts, so the structure report at
  --parallel 8 is the one --parallel 1 writes;
* the views -- they read tables, in other schemas too, and each other; they
  are still created after EVERY table exists, one at a time, in the plan's
  (dependency) order. The fake below refuses a view whose tables are not
  there yet, as Spark does.
"""
import copy
import json
import threading
import time

import pytest

from test_data_migration_scripts import _inject_spark, _load, _report
from test_structure_views import _ViewSpark

_COLS = [{"name": "ID", "type": "DECIMAL(38,0)"},
         {"name": "NOTE", "type": "STRING"}]


class _Timed(_ViewSpark):
    """Each CREATE TABLE takes a moment and counts what is in flight; one
    named table's CREATE raises."""

    def __init__(self, catalog=None, *, raises_on=None):
        super().__init__(catalog)
        self._lock = threading.Lock()
        self._flight = 0
        self.max_flight = 0
        self.raises_on = raises_on

    def sql(self, statement):
        if not statement.upper().startswith("CREATE TABLE"):
            return super().sql(statement)
        with self._lock:
            self._flight += 1
            self.max_flight = max(self.max_flight, self._flight)
        try:
            time.sleep(0.02)
            if self.raises_on and self.raises_on in statement:
                raise RuntimeError("[DELTA_METADATA_CHANGED] concurrent update")
            with self._lock:
                return super().sql(statement)
        finally:
            with self._lock:
                self._flight -= 1


def _estate(tmp_path, n=12, *, views=()):
    tables = [f"T{i:02d}" for i in range(n)]
    statements = [{"source_identifier": f"DB.SALES.{t}", "object_type": "TABLE",
                   "target_fqn": f"lake.SALES.{t}", "expected_columns": _COLS}
                  for t in tables if t != "T07"]         # T07: not in plan
    for name, reads in views:
        statements.append({
            "source_identifier": f"DB.SALES.{name}", "object_type": "VIEW",
            "target_fqn": f"lake.SALES.{name}",
            "sql": (f"CREATE VIEW IF NOT EXISTS `lake`.`SALES`.`{name}` AS "
                    f"SELECT ID, NOTE FROM {reads}"),
            "expected_columns": _COLS})
    reports = tmp_path / "reports"
    reports.mkdir(parents=True)
    (reports / "discovery_manifest.json").write_text(json.dumps({"schemas": [
        {"name": "SALES", "tables": [{"name": t, "columns": []}
                                     for t in tables],
         "views": [{"name": v} for v, _r in views], "errors": []}]}),
        encoding="utf-8")
    (tmp_path / "plan").mkdir()
    (tmp_path / "plan" / "ddl_plan.json").write_text(
        json.dumps({"statements": statements}), encoding="utf-8")
    return reports


def _pre_existing():
    """T02 already there as planned; T04 there with another layout."""
    return {"`lake`.`SALES`.`T02`": [("ID", "decimal(38,0)"),
                                      ("NOTE", "string")],
            "`lake`.`SALES`.`T04`": [("NOTE", "string"),
                                      ("ID", "decimal(38,0)")]}


def _run(monkeypatch, reports, spark, *argv):
    _inject_spark(monkeypatch, spark)
    return _load("01_create_structure").main(
        ["--target-catalog", "lake", "--schema", "SALES", "--reports-dir",
         str(reports), "--output-dir", "", *argv])


def _stable(report):
    out = copy.deepcopy(report)
    out.pop("updated_at", None)
    return out


def test_the_report_at_parallel_8_is_the_report_at_parallel_1(
        monkeypatch, tmp_path):
    got = {}
    for parallel in ("1", "8"):
        reports = _estate(tmp_path / parallel)
        spark = _Timed(_pre_existing(), raises_on="`T09`")
        rc = _run(monkeypatch, reports, spark, "--parallel", parallel)
        got[parallel] = (rc, _stable(_report(reports,
                                             "structure_report_sales.json")))
    assert got["1"] == got["8"]
    rc, report = got["8"]
    assert rc == 1
    statuses = {n: r["status"] for n, r in report["objects"].items()}
    assert statuses["T02"] == "already_existed"
    assert statuses["T04"] == "type_drift"
    assert statuses["T07"] == "not_in_plan"
    assert statuses["T09"] == "failed"
    assert "DELTA_METADATA_CHANGED" in report["objects"]["T09"]["reason"]
    assert [n for n, s in statuses.items() if s == "created"] == [
        f"T{i:02d}" for i in (0, 1, 3, 5, 6, 8, 10, 11)]
    assert list(report["objects"]) == [f"T{i:02d}" for i in range(12)], \
        "recorded in the manifest's order, whatever finished first"


def test_the_tables_are_created_at_once(monkeypatch, tmp_path):
    reports = _estate(tmp_path, n=16)
    spark = _Timed()
    assert _run(monkeypatch, reports, spark, "--parallel", "8") == 0
    assert 1 < spark.max_flight <= 8


def test_parallel_1_is_serial(monkeypatch, tmp_path):
    reports = _estate(tmp_path, n=6)
    spark = _Timed()
    assert _run(monkeypatch, reports, spark, "--parallel", "1") == 0
    assert spark.max_flight == 1


def test_views_come_after_every_table_in_the_plans_order(monkeypatch,
                                                         tmp_path):
    """V_B reads V_A, which reads T11 -- the last table in the manifest. At
    8 threads T11 may be the last CREATE to finish; the views still see it,
    and V_A still goes before the V_B that reads it."""
    reports = _estate(tmp_path, views=(
        ("V_A", "`lake`.`SALES`.`T11`"), ("V_B", "`lake`.`SALES`.`V_A`")))
    spark = _Timed()
    rc = _run(monkeypatch, reports, spark, "--parallel", "8")
    assert rc == 0
    report = _report(reports, "structure_report_sales.json")
    assert {v: r["status"] for v, r in report["views"].items()} == {
        "V_A": "created", "V_B": "created"}
    stmts = spark.statements
    last_table = max(i for i, s in enumerate(stmts)
                     if s.upper().startswith("CREATE TABLE"))
    views = [i for i, s in enumerate(stmts)
             if s.upper().startswith("CREATE VIEW")]
    assert views and min(views) > last_table
    assert "`V_A`" in stmts[views[0]] and "`V_B`" in stmts[views[1]]


def test_a_dry_run_in_parallel_creates_nothing(monkeypatch, tmp_path):
    reports = _estate(tmp_path)
    spark = _Timed()
    assert _run(monkeypatch, reports, spark, "--parallel", "8",
                "--dry-run") == 0
    assert not any(s.upper().startswith("CREATE TABLE")
                   for s in spark.statements)


def test_parallel_below_one_is_refused(monkeypatch, tmp_path, capsys):
    reports = _estate(tmp_path)
    assert _run(monkeypatch, reports, _Timed(), "--parallel", "0") == 1
    assert "--parallel" in capsys.readouterr().out


def test_parallel_defaults_to_eight_and_is_a_stage_param():
    from target.stage_notebooks import STAGES
    assert _load("01_create_structure").DEFAULT_PARALLEL == 8
    spec = next(s for s in STAGES if s.key == "structure")
    assert "parallel" in spec.params


def test_a_ctas_temp_view_is_unique_per_exact_table_name():
    """ctas registers the source as a temp view; Spark resolves view names
    case-insensitively, so `Orders` and `ORDERS` created in parallel would
    read each other's source."""
    module = _load("01_create_structure")
    assert module._view_name("S", "Orders") != module._view_name("S", "ORDERS")
