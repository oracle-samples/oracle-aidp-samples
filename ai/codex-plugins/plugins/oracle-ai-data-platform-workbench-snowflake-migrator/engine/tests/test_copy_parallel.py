"""`02_copy_schema --parallel N` copies N tables at once, and says the same.

Live 2026-09-29 (probe 3, 100-row tables, qualified pushdown): 4 tables
copied serially took 70.1 s (17.5 s a table); 16 tables in 8 threads took
94.0 s (5.9 s a table). The copy at the time ran ~280 s a table. So the
stage now runs a thread pool over each chunk of tables, default 8, and
`--parallel 1` is the serial copy.

What must NOT change with the thread count is the record: each table's
result is decided by the same functions on the same facts, so a report
written at --parallel 8 is the report --parallel 1 writes (timestamps
aside). The source counts stay batched -- one qualified UNION ALL per chunk
BEFORE the chunk is copied and one AFTER it, never a count per table --
and a source that grew while its table was copied is still caught: the
after-count is read once the chunk's INSERTs are done.
"""
import copy
import decimal
import json
import threading
import time

import pytest

from fake_pushdown import FakeLakeSpark, FakeSnowflake
from test_data_migration_scripts import _inject_spark, _load

D = decimal.Decimal


def _estate(n, *, rows=3):
    tables, lake, statements = {}, {}, []
    for i in range(n):
        name = f"T{i:03d}"
        tables[("SNOWMIG_DB", "BULK", name)] = {
            "columns": [("ID", "NUMBER", 38, 0), ("AMT", "NUMBER", 18, 2)],
            "rows": [{"ID": D(r), "AMT": D(f"{r}.25")} for r in range(rows)]}
        lake[f"`lake`.`bulk`.`{name}`"] = [("ID", "decimal(38,0)"),
                                           ("AMT", "decimal(18,2)")]
        statements.append({
            "source_identifier": f"SNOWMIG_DB.BULK.{name}",
            "object_type": "TABLE", "target_fqn": f"lake.bulk.{name}",
            "expected_columns": [{"name": "ID", "type": "DECIMAL(38,0)"},
                                 {"name": "AMT", "type": "DECIMAL(18,2)"}],
            "columns": [
                {"name": "ID", "target_type": "DECIMAL(38,0)",
                 "read_expr": '"ID"::VARCHAR',
                 "convert_expr": "CAST(`ID` AS DECIMAL(38,0))"},
                {"name": "AMT", "target_type": "DECIMAL(18,2)",
                 "read_expr": '"AMT"::VARCHAR',
                 "convert_expr": "CAST(`AMT` AS DECIMAL(18,2))"}]})
    return tables, lake, statements


def _files(tmp_path, statements, names):
    reports = tmp_path / "reports"
    reports.mkdir(parents=True)
    (tmp_path / "plan").mkdir()
    (tmp_path / "plan" / "ddl_plan.json").write_text(
        json.dumps({"statements": statements}), encoding="utf-8")
    (reports / "discovery_manifest.json").write_text(json.dumps({"schemas": [
        {"name": "BULK", "tables": [{"name": t} for t in names],
         "views": [], "errors": []}]}), encoding="utf-8")
    config = tmp_path / "source.json"
    config.write_text(json.dumps({
        "account": "acct", "warehouse": "WH", "database": "SNOWMIG_DB",
        "user": "svc", "auth": "password", "password": "p",
        "schema": "BULK"}), encoding="utf-8")
    return reports, config


def _run(monkeypatch, tmp_path, spark, statements, names, *argv):
    reports, config = _files(tmp_path, statements, names)
    _inject_spark(monkeypatch, spark)
    rc = _load("02_copy_schema").main(
        ["--target-catalog", "lake", "--schema", "BULK", "--reports-dir",
         str(reports), "--source-config", str(config), "--output-dir", "",
         "--retries", "0", "--mode", "append", *argv])
    report = json.loads((reports / "copy_report_bulk.json").read_text(
        encoding="utf-8"))
    return rc, report


def _stable(report):
    """The per-table records without the moments they were written."""
    out = copy.deepcopy(report["tables"])
    for rec in out.values():
        for key in ("started_at", "finished_at"):
            rec.pop(key, None)
    return out


class _Concurrent(FakeLakeSpark):
    """Records how many INSERTs were in flight at once; each takes a moment,
    as a real one does, so overlapping ones are seen overlapping."""

    def __init__(self, *a, **kw):
        super().__init__(*a, **kw)
        self._flight = 0
        self.max_flight = 0
        self._flight_lock = threading.Lock()
        self.after_insert = None

    def sql(self, statement):
        if not statement.startswith("INSERT"):
            return super().sql(statement)
        with self._flight_lock:
            self._flight += 1
            self.max_flight = max(self.max_flight, self._flight)
        try:
            time.sleep(0.02)
            out = super().sql(statement)
            if self.after_insert:
                self.after_insert(statement)
            return out
        finally:
            with self._flight_lock:
                self._flight -= 1


def _mixed(n=12):
    """Verified tables, one drifted, one the plan does not place."""
    tables, lake, statements = _estate(n)
    tables[("SNOWMIG_DB", "BULK", "T003")]["columns"].append(
        ("EXTRA", "TEXT", None, None))
    for row in tables[("SNOWMIG_DB", "BULK", "T003")]["rows"]:
        row["EXTRA"] = "x"
    del lake["`lake`.`bulk`.`T005`"]
    statements = [s for s in statements
                  if not s["source_identifier"].endswith(".T005")]
    names = [f"T{i:03d}" for i in range(n)]
    return tables, lake, statements, names


# ------------------------------------------------ same record, any count

def test_the_report_at_parallel_8_is_the_report_at_parallel_1(
        monkeypatch, tmp_path):
    reports = {}
    for parallel in ("1", "8"):
        tables, lake, statements, names = _mixed()
        spark = FakeLakeSpark(FakeSnowflake(tables), lake)
        rc, report = _run(monkeypatch, tmp_path / parallel, spark,
                          statements, names, "--parallel", parallel,
                          "--verify", "counts+sums")
        reports[parallel] = (rc, _stable(report))
    assert reports["1"] == reports["8"]
    rc, tables = reports["8"]
    assert rc == 1, "the drifted table is a failure at any thread count"
    assert tables["T003"]["status"] == "type_drift"
    assert tables["T005"]["status"] == "target_missing"
    assert {tables[n]["status"] for n in tables
            if n not in ("T003", "T005")} == {"verified"}


def test_the_tables_are_copied_at_once(monkeypatch, tmp_path):
    tables, lake, statements = _estate(16)
    spark = _Concurrent(FakeSnowflake(tables), lake)
    rc, report = _run(monkeypatch, tmp_path, spark, statements,
                      sorted(t for _d, _s, t in tables), "--parallel", "8")
    assert rc == 0
    assert spark.max_flight > 1, "8 threads, and never two INSERTs at once"
    assert spark.max_flight <= 8


def test_parallel_1_is_serial(monkeypatch, tmp_path):
    tables, lake, statements = _estate(6)
    spark = _Concurrent(FakeSnowflake(tables), lake)
    rc, _report = _run(monkeypatch, tmp_path, spark, statements,
                       sorted(t for _d, _s, t in tables), "--parallel", "1")
    assert rc == 0
    assert spark.max_flight == 1


# -------------------------------------- counts: batched before and after

def _count_queries(spark):
    return [s for s in spark.pushdowns if "SNOWMIG_TABLE" in s]


def test_counts_are_one_batched_query_before_and_one_after_each_chunk(
        monkeypatch, tmp_path):
    tables, lake, statements = _estate(60)
    spark = FakeLakeSpark(FakeSnowflake(tables), lake)
    rc, report = _run(monkeypatch, tmp_path, spark, statements,
                      sorted(t for _d, _s, t in tables))
    assert rc == 0
    counts = _count_queries(spark)
    branches = [q.count("count(*)") for q in counts]
    assert branches == [50, 50, 10, 10], \
        "chunk 1 before and after, chunk 2 before and after -- no per-table count"
    for q in counts:
        assert q.count('"SNOWMIG_DB"."BULK".') == q.count("count(*)")
    assert {r["status"] for r in report["tables"].values()} == {"verified"}
    assert {r["source_count"] for r in report["tables"].values()} == {3}


def test_a_source_that_grew_during_its_copy_is_still_caught(
        monkeypatch, tmp_path):
    """The after-count is read once the chunk is done: a row added to the
    source while T002 was being copied makes it count_mismatch, with the
    source named as the one that moved -- at any thread count."""
    for parallel in ("1", "8"):
        tables, lake, statements = _estate(6)
        spark = _Concurrent(FakeSnowflake(tables), lake)

        def grow(statement, tables=tables):
            if "`T002`" in statement.split(" SELECT ")[0]:
                tables[("SNOWMIG_DB", "BULK", "T002")]["rows"].append(
                    {"ID": D(99), "AMT": D("1.00")})
        spark.after_insert = grow
        rc, report = _run(monkeypatch, tmp_path / parallel, spark,
                          statements, sorted(t for _d, _s, t in tables),
                          "--parallel", parallel)
        rec = report["tables"]["T002"]
        assert rc == 1
        assert rec["status"] == "count_mismatch", rec
        assert rec["source_count"] == 4 and rec["target_count"] == 3
        assert "3 -> 4" in rec["source_moved_during_copy"]


def test_an_after_count_that_cannot_be_read_says_the_rows_landed(
        monkeypatch, tmp_path):
    tables, lake, statements = _estate(3)
    spark = FakeLakeSpark(FakeSnowflake(tables), lake)
    real = spark.snowflake.run
    calls = {"n": 0}

    def run(sql, spark_):
        if "SNOWMIG_TABLE" in sql:
            calls["n"] += 1
            if calls["n"] > 1:       # every count after the first batch
                raise RuntimeError("CONNECTOR_0007 - session expired")
        return real(sql, spark_)
    spark.snowflake.run = run
    rc, report = _run(monkeypatch, tmp_path, spark, statements,
                      ["T000", "T001", "T002"])
    assert rc == 1
    for rec in report["tables"].values():
        assert rec["status"] == "failed"
        assert rec["insert_completed"] is True
        assert "not append" in rec["reason"]
        assert "_pending" not in rec


def test_one_table_raising_does_not_stop_the_others(monkeypatch, tmp_path):
    tables, lake, statements = _estate(5)
    spark = FakeLakeSpark(FakeSnowflake(tables), lake)
    real = spark.sql

    def sql(statement):
        if statement.startswith("INSERT INTO `lake`.`bulk`.`T001`"):
            raise RuntimeError("DELTA_CONCURRENT_APPEND")
        return real(statement)
    spark.sql = sql
    rc, report = _run(monkeypatch, tmp_path, spark, statements,
                      [f"T{i:03d}" for i in range(5)])
    assert rc == 1
    assert report["tables"]["T001"]["status"] == "failed"
    assert "DELTA_CONCURRENT_APPEND" in report["tables"]["T001"]["reason"]
    assert [report["tables"][f"T{i:03d}"]["status"] for i in (0, 2, 3, 4)] \
        == ["verified"] * 4


# --------------------------------------------------------- the flag itself

def test_parallel_below_one_is_refused(monkeypatch, tmp_path, capsys):
    tables, lake, statements = _estate(1)
    reports, config = _files(tmp_path, statements, ["T000"])
    _inject_spark(monkeypatch, FakeLakeSpark(FakeSnowflake(tables), lake))
    rc = _load("02_copy_schema").main(
        ["--target-catalog", "lake", "--schema", "BULK", "--reports-dir",
         str(reports), "--source-config", str(config), "--parallel", "0"])
    assert rc == 1
    assert "--parallel" in capsys.readouterr().out


def test_parallel_defaults_to_eight_and_is_a_stage_param():
    from target.stage_notebooks import STAGES
    module = _load("02_copy_schema")
    assert module.DEFAULT_PARALLEL == 8
    spec = next(s for s in STAGES if s.key == "copy_schema")
    assert "parallel" in spec.params
