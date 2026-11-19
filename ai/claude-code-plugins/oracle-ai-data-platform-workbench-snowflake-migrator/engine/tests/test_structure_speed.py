"""01_create_structure spends its time on round trips, not work: make fewer,
and overlap the rest.

Live 2026-09-29 the structure job took ~25 s per created table: a DESCRIBE
that fails for every new table, the CREATE, its read-backs, and the whole
schema report rewritten to /Workspace after every table (the plan's 90
left-out tables included). Now:

* one SHOW TABLES per schema tells which tables are absent, and those skip
  the before-DESCRIBE (the CREATE is still read back);
* creates run --parallel at a time within a schema, each verified on its
  own; CTAS and dry runs stay one at a time;
* the report is written at most every few seconds and at the end of each
  schema.
"""
import json
import threading

import pytest

from test_data_migration_scripts import _CatalogSpark, _inject_spark, _load

_COLS = [{"name": "ID", "type": "DECIMAL(38,0)"},
         {"name": "NOTE", "type": "STRING"}]


def _estate(tmp_path, n=6, planned=None):
    reports = tmp_path / "reports"
    reports.mkdir(parents=True)
    names = [f"T{i:02d}" for i in range(n)]
    planned = names if planned is None else planned
    (reports / "discovery_manifest.json").write_text(json.dumps({"schemas": [
        {"name": "SALES", "tables": [{"name": t, "columns": []}
                                     for t in names],
         "views": [], "errors": []}]}), encoding="utf-8")
    (tmp_path / "plan").mkdir()
    (tmp_path / "plan" / "ddl_plan.json").write_text(json.dumps(
        {"statements": [{"source_identifier": f"DB.SALES.{t}",
                         "object_type": "TABLE",
                         "target_fqn": f"lake.SALES.{t}",
                         "expected_columns": _COLS} for t in planned]}),
        encoding="utf-8")
    return reports


class _Counting(_CatalogSpark):
    """Thread-safe enough for a pool, and counts what it is asked."""

    def __init__(self, *a, show_fails=False, **kw):
        super().__init__(*a, **kw)
        self.lock = threading.Lock()
        self.show_fails = show_fails

    def sql(self, statement):
        with self.lock:
            low = " ".join(statement.split()).lower()
            if low.startswith("show tables in") and self.show_fails:
                self.statements.append(" ".join(statement.split()))
                raise RuntimeError("SHOW TABLES is not available")
            return super().sql(statement)

    def count(self, prefix):
        return sum(1 for s in self.statements
                   if s.lower().startswith(prefix))


def _structure(monkeypatch, reports, spark, *extra):
    _inject_spark(monkeypatch, spark)
    return _load("01_create_structure").main(
        ["--target-catalog", "lake", "--schema", "SALES",
         "--reports-dir", str(reports), "--output-dir", "", *extra])


def _objects(reports):
    return json.loads((reports / "structure_report_sales.json")
                      .read_text(encoding="utf-8"))["objects"]


@pytest.mark.parametrize("parallel", ["1", "4"])
def test_parallel_and_one_at_a_time_create_and_verify_the_same(
        monkeypatch, tmp_path, parallel):
    reports = _estate(tmp_path)
    spark = _Counting()
    assert _structure(monkeypatch, reports, spark,
                      "--parallel", parallel) == 0
    objects = _objects(reports)
    assert sorted(objects) == [f"T{i:02d}" for i in range(6)]
    assert {o["status"] for o in objects.values()} == {"created"}
    assert spark.count("create table") == 6


def test_one_listing_replaces_the_failing_describe_for_absent_tables(
        monkeypatch, tmp_path):
    reports = _estate(tmp_path)
    spark = _Counting()
    assert _structure(monkeypatch, reports, spark) == 0
    assert spark.count("show tables in") == 1
    # One read-back DESCRIBE per created table, and no before-DESCRIBE.
    assert spark.count("describe") == 6


def test_an_unreadable_listing_falls_back_to_describe_per_table(
        monkeypatch, tmp_path):
    reports = _estate(tmp_path)
    spark = _Counting(show_fails=True)
    assert _structure(monkeypatch, reports, spark) == 0
    assert spark.count("describe") == 12        # before and after, each
    assert {o["status"] for o in _objects(reports).values()} == {"created"}


def test_a_table_already_there_is_still_compared_with_the_plan(
        monkeypatch, tmp_path):
    reports = _estate(tmp_path, n=2)
    spark = _Counting({"`lake`.`SALES`.`T00`": [("ID", "int")]})  # drifted
    assert _structure(monkeypatch, reports, spark) == 1
    objects = _objects(reports)
    assert objects["T00"]["status"] == "type_drift"
    assert objects["T01"]["status"] == "created"


def test_the_final_report_is_complete_even_with_writes_throttled(
        monkeypatch, tmp_path):
    reports = _estate(tmp_path, n=8, planned=["T00", "T01", "T02"])
    module = _load("01_create_structure")
    monkeypatch.setattr(module._Flusher, "FLUSH_SECONDS", 3600.0)
    spark = _Counting()
    _inject_spark(monkeypatch, spark)
    assert module.main(["--target-catalog", "lake", "--schema", "SALES",
                        "--reports-dir", str(reports),
                        "--output-dir", ""]) == 0
    objects = _objects(reports)
    assert len(objects) == 8
    assert sum(o["status"] == "created" for o in objects.values()) == 3
    assert sum(o["status"] == "not_in_plan" for o in objects.values()) == 5


def test_a_dry_run_stays_one_at_a_time_and_creates_nothing(
        monkeypatch, tmp_path, capsys):
    reports = _estate(tmp_path)
    spark = _Counting()
    assert _structure(monkeypatch, reports, spark, "--dry-run") == 0
    assert spark.count("create table") == 0
    assert "at a time" not in capsys.readouterr().out
