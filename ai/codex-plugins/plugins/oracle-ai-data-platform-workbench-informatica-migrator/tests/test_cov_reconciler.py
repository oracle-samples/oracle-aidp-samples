"""DataReconciler orchestration, YAML config loading, the generated
reconciliation notebook, and ReconcileReport rendering.

The orchestrator is exercised with the connector factories replaced by
in-memory fakes (no database, no Spark); what is pinned is the combined
verdict a caller gets back and the config a YAML file turns into. The
generated notebook is executed for real against a local Spark session
with the JDBC read and ``dbutils`` stubbed, because a notebook that reads
correctly and does not run is the failure mode string assertions miss.
"""
from __future__ import annotations

import contextlib
import csv
import io
import json
import os

import pytest

from infa2aidp.reconciler import reconciler as reconciler_mod
from infa2aidp.reconciler.models import (
    AggregateResult,
    ColumnDiff,
    DataSourceType,
    ReconcileConfig,
    ReconcileResult,
    ReconcileType,
    RowDiff,
    SchemaColumnDiff,
    SourceConfig,
    TargetConfig,
)
from infa2aidp.reconciler.reconciler import DataReconciler
from infa2aidp.reconciler.report import ReconcileReport, _format_key, _status_icon

try:  # pyspark is test-only; without it the notebook-execution tests skip
    import pyspark  # noqa: F401
    from tests.conftest_spark import spark  # noqa: F401  (session fixture)
    _HAVE_SPARK = True
except ImportError:
    _HAVE_SPARK = False


class FakeSide:
    def __init__(self, count=0, schema=None, rows=None, aggs=None):
        self.count, self.schema, self.rows, self.aggs = count, schema or [], rows or [], aggs or []

    def get_row_count(self, table="", where=""):
        return self.count

    def get_schema(self, table=""):
        return self.schema

    def get_data(self, table="", columns=None, where="", limit=0):
        return self.rows

    def get_aggregates(self, table="", columns=None, metrics=None):
        return self.aggs


def _patch_connectors(monkeypatch, source, target):
    seen = {}

    def make_source(cfg, *, use_spark=False):
        seen["use_spark"] = use_spark
        return source

    monkeypatch.setattr(reconciler_mod, "create_source_connector", make_source)
    monkeypatch.setattr(reconciler_mod, "create_target_connector", lambda cfg: target)
    return seen


SCHEMA = [{"name": "ID", "type": "IntegerType()", "nullable": False},
          {"name": "AMT", "type": "DoubleType()", "nullable": True}]
SRC_SCHEMA = [{"name": "ID", "type": "NUMBER(10)", "nullable": False},
              {"name": "AMT", "type": "BINARY_DOUBLE", "nullable": True}]
ROWS = [{"ID": 1, "AMT": 1.0}, {"ID": 2, "AMT": 2.0}]
AGGS = [{"column": "AMT", "metric": "SUM", "value": 3.0}]


def _cfg(rtype=ReconcileType.ALL, **kw):
    return ReconcileConfig(name="t1", reconcile_type=rtype, key_columns=["ID"],
                           source=SourceConfig(table_name="S.T"),
                           target=TargetConfig(full_table_name="c.s.t"), **kw)


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------

class TestReconcile:
    def test_all_checks_passing_is_a_combined_pass(self, monkeypatch):
        seen = _patch_connectors(
            monkeypatch,
            FakeSide(2, SRC_SCHEMA, ROWS, AGGS), FakeSide(2, SCHEMA, ROWS, AGGS))
        r = DataReconciler(use_spark=True).reconcile(_cfg())
        assert seen["use_spark"] is True
        assert r.status == "PASSED" and r.overall_match
        assert r.reconcile_type == "all" and r.config_name == "t1"
        assert r.match_percentage == 100.0
        assert (r.source_row_count, r.target_row_count, r.row_count_match) == (2, 2, True)
        assert r.schema_match and r.total_rows_compared == 2 and r.matching_rows == 2
        assert len(r.aggregate_results) == 1
        assert r.started_at and r.completed_at and r.duration_seconds >= 0

    def test_one_failing_check_fails_the_run_and_averages_the_score(self, monkeypatch):
        _patch_connectors(
            monkeypatch,
            FakeSide(2, SRC_SCHEMA, ROWS, AGGS), FakeSide(1, SCHEMA, ROWS, AGGS))
        r = DataReconciler().reconcile(_cfg())
        assert r.status == "FAILED" and not r.overall_match
        assert not r.row_count_match
        # row count 50%, schema/aggregate/data 100% -> 87.5
        assert r.match_percentage == 87.5

    @pytest.mark.parametrize("rtype,field", [
        (ReconcileType.ROW_COUNT, "source_row_count"),
        (ReconcileType.SCHEMA, "schema_match"),
        (ReconcileType.DATA, "matching_rows"),
        (ReconcileType.AGGREGATE, "aggregate_results"),
    ])
    def test_single_type_runs_only_that_check(self, monkeypatch, rtype, field):
        _patch_connectors(
            monkeypatch,
            FakeSide(2, SRC_SCHEMA, ROWS, AGGS), FakeSide(2, SCHEMA, ROWS, AGGS))
        r = DataReconciler().reconcile(_cfg(rtype))
        assert r.status == "PASSED" and r.reconcile_type == rtype.value
        assert getattr(r, field)
        others = {"source_row_count", "schema_match", "matching_rows", "aggregate_results"} - {field}
        assert not any(getattr(r, f) for f in others)

    def test_a_sub_check_error_is_the_run_error(self, monkeypatch):
        _patch_connectors(monkeypatch, FakeSide(2, SRC_SCHEMA, ROWS), FakeSide(2, SCHEMA, ROWS))
        cfg = _cfg(ReconcileType.DATA)
        cfg.key_columns = []
        r = DataReconciler().reconcile(cfg)
        assert r.status == "ERROR" and not r.overall_match
        assert "key_columns" in r.error_message

    def test_connector_exception_becomes_error_result(self, monkeypatch):
        class Broken(FakeSide):
            def get_row_count(self, table="", where=""):
                raise ConnectionError("ORA-12541: no listener")
        _patch_connectors(monkeypatch, Broken(), FakeSide())
        r = DataReconciler().reconcile(_cfg(ReconcileType.ROW_COUNT))
        assert r.status == "ERROR"
        assert "ORA-12541" in r.error_message
        assert r.completed_at

    def test_batch_returns_one_result_per_config(self, monkeypatch):
        _patch_connectors(monkeypatch, FakeSide(1), FakeSide(1))
        out = DataReconciler().reconcile_batch([_cfg(ReconcileType.ROW_COUNT)] * 3)
        assert [r.status for r in out] == ["PASSED"] * 3

    def test_merge_of_no_sub_results_is_a_pass(self):
        combined = ReconcileResult()
        DataReconciler._merge_results(combined, [])
        assert combined.status == "PASSED" and combined.match_percentage == 100.0

    def test_checks_for_type(self):
        assert DataReconciler._checks_for_type(ReconcileType.ALL) == [
            "row_count", "schema", "aggregate", "data"]
        assert DataReconciler._checks_for_type(ReconcileType.SCHEMA) == ["schema"]


# ---------------------------------------------------------------------------
# YAML config
# ---------------------------------------------------------------------------

class TestLoadConfig:
    def test_full_entry_with_env_expansion(self, tmp_path, monkeypatch):
        monkeypatch.setenv("RECON_HOST", "db.example")
        monkeypatch.setenv("RECON_PASS", "s3cret")
        monkeypatch.delenv("RECON_UNSET", raising=False)
        p = tmp_path / "recon.yaml"
        p.write_text(
            "reconciliations:\n"
            "  - name: orders\n"
            "    type: data\n"
            "    source:\n"
            "      type: postgresql\n"
            "      host: ${RECON_HOST}\n"
            "      port: 5432\n"
            "      database: sales\n"
            "      schema: public\n"
            "      table: orders\n"
            "      username: app\n"
            "      password: ${RECON_PASS}\n"
            "      query: select 1\n"
            "    target:\n"
            "      catalog: cat\n"
            "      schema: sch\n"
            "      table: orders_${RECON_UNSET}x\n"
            "    key_columns: [order_id]\n"
            "    compare_columns: [amount]\n"
            "    ignore_columns: [etl_ts]\n"
            "    tolerance: 0.5\n"
            "    date_tolerance_seconds: 3\n"
            "    sample_size: 1000\n"
            "    where_clause: region = '${RECON_HOST}'\n"
            "    column_mapping: {amount: amt}\n"
            "  - name: minimal\n",
            encoding="utf-8")
        cfgs = DataReconciler().load_config(str(p))
        assert len(cfgs) == 2
        c = cfgs[0]
        assert c.name == "orders" and c.reconcile_type == ReconcileType.DATA
        assert c.source.source_type == DataSourceType.POSTGRESQL
        assert (c.source.host, c.source.port, c.source.password) == ("db.example", 5432, "s3cret")
        assert c.source.get_qualified_table() == "public.orders"
        assert c.source.query == "select 1"
        assert c.target.get_full_name() == "cat.sch.orders_x"   # unset var expands to ""
        assert c.key_columns == ["order_id"] and c.compare_columns == ["amount"]
        assert c.ignore_columns == ["etl_ts"] and c.column_mapping == {"amount": "amt"}
        assert (c.tolerance, c.date_tolerance_seconds, c.sample_size) == (0.5, 3, 1000)
        assert c.where_clause == "region = 'db.example'"
        m = cfgs[1]
        assert m.reconcile_type == ReconcileType.ALL
        assert m.source.source_type == DataSourceType.ORACLE and m.source.port == 1521
        assert m.tolerance == 0.0001 and m.sample_size == 0

    def test_unknown_source_type_is_rejected(self, tmp_path):
        p = tmp_path / "bad.yaml"
        p.write_text("reconciliations:\n  - name: x\n    source: {type: teradata}\n", encoding="utf-8")
        with pytest.raises(ValueError):
            DataReconciler().load_config(str(p))

    def test_no_reconciliations_key_is_empty(self, tmp_path):
        p = tmp_path / "empty.yaml"
        p.write_text("other: 1\n", encoding="utf-8")
        assert DataReconciler().load_config(str(p)) == []

    def test_expand_multiple_and_plain(self, monkeypatch):
        monkeypatch.setenv("A_X", "1")
        monkeypatch.setenv("B_X", "2")
        assert DataReconciler._expand("${A_X}-${B_X}") == "1-2"
        assert DataReconciler._expand("plain") == "plain"
        assert DataReconciler._expand("") == ""


# ---------------------------------------------------------------------------
# Generated notebook
# ---------------------------------------------------------------------------

def _nb_cfg(**kw):
    kw.setdefault("key_columns", ["ID"])
    return ReconcileConfig(
        name="orders",
        source=SourceConfig(source_type=DataSourceType.ORACLE, host="h", port=1521,
                            database="db", schema="HR", table_name="ORDERS"),
        target=TargetConfig(catalog="cat", schema="sch", table_name="orders"),
        **kw)


class TestNotebookText:
    def test_notebook_is_valid_python_and_names_both_sides(self):
        nb = DataReconciler().generate_reconcile_notebook(
            _nb_cfg(ignore_columns=["ETL_TS"], where_clause="REGION = 'EU'", tolerance=0.01))
        compile(nb, "<recon-notebook>", "exec")
        assert 'SOURCE_TABLE = "HR.ORDERS"' in nb
        assert 'TARGET_TABLE = "cat.sch.orders"' in nb
        assert 'SOURCE_JDBC_URL = "jdbc:oracle:thin:@h:1521/db"' in nb
        assert 'KEY_COLUMNS = ["ID"]' in nb and 'IGNORE_COLUMNS = ["ETL_TS"]' in nb
        assert "TOLERANCE = 0.01" in nb
        assert "source_df.where(\"REGION = 'EU'\")" in nb

    def test_credentials_come_from_secrets_not_the_config(self):
        cfg = _nb_cfg()
        cfg.source.password = "hunter2"
        nb = DataReconciler().generate_reconcile_notebook(cfg)
        assert "hunter2" not in nb
        assert 'dbutils.secrets.get(scope="reconciler", key="source_pass")' in nb


class _StubReader:
    def __init__(self, df):
        self.df, self.options = df, {}

    def format(self, fmt):
        return self

    def option(self, k, v):
        self.options[k] = v
        return self

    def load(self):
        return self.df


class _StubSpark:
    """spark.read.format('jdbc')...load() -> source DataFrame; spark.table -> target."""

    def __init__(self, src_df, tgt_df):
        self.src_df, self.tgt_df = src_df, tgt_df

    @property
    def read(self):
        return _StubReader(self.src_df)

    def table(self, name):
        return self.tgt_df


class _StubDbutils:
    class secrets:
        @staticmethod
        def get(scope, key):
            return f"<{key}>"


def _values_df(spark, rows, columns):
    """A DataFrame built in the JVM from a VALUES list -- no Python worker
    round trip (createDataFrame from Python rows needs one, and local
    Windows Spark cannot always open it)."""
    def lit(v):
        if isinstance(v, str):
            return "'" + v.replace("'", "''") + "'"
        return repr(v)
    values = ", ".join("(" + ", ".join(lit(v) for v in r) + ")" for r in rows)
    return spark.sql(f"SELECT * FROM VALUES {values} AS t({', '.join(columns)})")


def _run_notebook(spark, cfg, src_rows, tgt_rows, columns=("ID", "NAME", "AMT")):
    nb = DataReconciler().generate_reconcile_notebook(cfg)
    stub = _StubSpark(_values_df(spark, src_rows, columns), _values_df(spark, tgt_rows, columns))
    out = io.StringIO()
    with contextlib.redirect_stdout(out):
        exec(compile(nb, "<recon-notebook>", "exec"),  # noqa: S102
             {"spark": stub, "dbutils": _StubDbutils})
    assert "=== Reconciliation complete ===" in out.getvalue()
    return out.getvalue()


@pytest.mark.skipif(not _HAVE_SPARK, reason="notebook execution needs pyspark (test-only)")
class TestNotebookExecution:
    def test_runs_and_reports_counts_missing_rows_and_sums(self, spark):
        out = _run_notebook(
            spark, _nb_cfg(),
            [(1, "a", 1.0), (2, "b", 2.0), (3, "c", 3.0)],
            [(1, "a", 1.0), (2, "b", 2.0)])
        assert "Source rows: 3" in out and "Target rows: 2" in out
        assert "Row count match: False" in out
        assert "Rows missing in target: 1" in out
        assert "Rows missing in source: 0" in out
        assert "AMT SUM: src=6.0, tgt=3.0" in out and "[FAIL]" in out
        assert "=== Reconciliation complete ===" in out

    def test_where_clause_filters_both_sides(self, spark):
        out = _run_notebook(
            spark, _nb_cfg(where_clause="ID < 3"),
            [(1, "a", 1.0), (2, "b", 2.0), (3, "c", 3.0)],
            [(1, "a", 1.0), (2, "b", 2.0)])
        assert "Row count match: True" in out
        assert "Rows missing in target: 0" in out

    def test_value_mismatch_on_a_matched_row_is_reported(self, spark):
        out = _run_notebook(
            spark, _nb_cfg(),
            [(1, "a", 1.0), (2, "b", 2.0)],
            [(1, "a", 1.0), (2, "CHANGED", 2.0)])
        assert "Matched rows for value comparison: 2" in out  # the run itself worked
        lines = [ln for ln in out.splitlines() if "mismatch" in ln.lower()]
        assert lines and any("1" in ln for ln in lines), out


# ---------------------------------------------------------------------------
# Report rendering
# ---------------------------------------------------------------------------

def _full_result():
    return ReconcileResult(
        config_name="orders", reconcile_type="all", status="FAILED",
        source_row_count=1200, target_row_count=1199, row_count_match=False,
        schema_match=False,
        schema_diffs=[SchemaColumnDiff("NAME", "VARCHAR2", "IntegerType()", "TYPE_MISMATCH")],
        total_rows_compared=1200, matching_rows=1197, mismatched_rows=2,
        missing_in_target=1, missing_in_source=0,
        sample_diffs=[
            RowDiff({"ID": 7}, [ColumnDiff("AMT", "1.0", "2.0", "VALUE_MISMATCH")], "VALUE_MISMATCH"),
            RowDiff({"ID": 9}, [], "MISSING_IN_TARGET"),
        ],
        aggregate_results=[
            AggregateResult("AMT", "SUM", 10.0, 12.0, 2.0, False),
            AggregateResult("AMT", "MAX", 5.0, 5.0, 0.0, True),
        ],
        match_percentage=74.5, duration_seconds=1.25,
        started_at="2026-01-01T00:00:00", completed_at="2026-01-01T00:00:01",
    )


class TestReconcileReport:
    def test_markdown_summary_and_every_detail_section(self):
        md = ReconcileReport([_full_result()]).to_markdown()
        assert "| orders | ALL | FAIL FAILED | 74.5% | 1,200 | 1,199 |" in md
        assert "- **Row Count:** FAILED (source: 1,200, target: 1,199, diff: 1)" in md
        assert "- **Schema:** FAILED" in md
        assert "  - NAME: TYPE_MISMATCH (source=VARCHAR2, target=IntegerType())" in md
        assert "- **Aggregates:** 1 mismatches" in md
        assert "  - AMT SUM: source=10.0, target=12.0, diff=2.000000" in md
        assert "- **Data:** 1,197 matching, 2 mismatched, 1 missing in target, 0 missing in source" in md
        assert "| ID=7 | AMT | 1.0 | 2.0 | VALUE_MISMATCH |" in md
        assert "| ID=9 | - | - | - | MISSING_IN_TARGET |" in md
        assert "- **Duration:** 1.2s" in md or "- **Duration:** 1.3s" in md

    def test_markdown_error_result_shows_only_the_error(self):
        r = ReconcileResult(config_name="x", reconcile_type="all", status="ERROR",
                            error_message="ORA-01017: invalid username/password")
        md = ReconcileReport([r]).to_markdown()
        assert "**Error:** ORA-01017" in md
        assert "**Duration:**" not in md.split("### x", 1)[1]

    def test_all_aggregates_matching_says_passed(self):
        r = ReconcileResult(config_name="x", reconcile_type="aggregate", status="PASSED",
                            aggregate_results=[AggregateResult("A", "SUM", 1, 1, 0, True)])
        assert "- **Aggregates:** PASSED" in ReconcileReport([r]).to_markdown()

    def test_json_round_trips_the_verdict(self):
        data = json.loads(ReconcileReport([_full_result()]).to_json())
        assert data["generated_at"]
        (r,) = data["results"]
        assert r["status"] == "FAILED" and r["match_percentage"] == 74.5
        assert r["schema_diffs"] == [{"column": "NAME", "source_type": "VARCHAR2",
                                      "target_type": "IntegerType()", "diff_type": "TYPE_MISMATCH"}]
        assert r["aggregate_results"][0] == {"column": "AMT", "metric": "SUM", "source_value": 10.0,
                                             "target_value": 12.0, "difference": 2.0, "match": False}
        assert r["sample_diffs"][0] == {"key": {"ID": 7}, "diff_type": "VALUE_MISMATCH",
                                        "columns": [{"name": "AMT", "source": "1.0",
                                                     "target": "2.0", "type": "VALUE_MISMATCH"}]}
        assert r["missing_in_target"] == 1

    def test_json_caps_sample_diffs_at_20(self):
        r = ReconcileResult(sample_diffs=[RowDiff({"ID": i}) for i in range(30)])
        data = json.loads(ReconcileReport([r]).to_json())
        assert len(data["results"][0]["sample_diffs"]) == 20

    def test_csv_has_one_row_per_result(self):
        ok = ReconcileResult(config_name="b", reconcile_type="row_count", status="PASSED",
                             source_row_count=5, target_row_count=5, row_count_match=True,
                             match_percentage=100.0)
        rows = list(csv.DictReader(io.StringIO(ReconcileReport([_full_result(), ok]).to_csv())))
        assert [r["config_name"] for r in rows] == ["orders", "b"]
        assert rows[0]["match_percentage"] == "74.50" and rows[0]["status"] == "FAILED"
        assert rows[1]["row_count_match"] == "True" and rows[1]["duration_seconds"] == "0.000"

    def test_save_all_writes_three_readable_files(self, tmp_path):
        paths = ReconcileReport([_full_result()]).save_all(str(tmp_path / "out"), prefix="run1")
        assert set(paths) == {"markdown", "json", "csv"}
        assert paths["json"].endswith("run1.json")
        assert json.loads(open(paths["json"], encoding="utf-8").read())["results"]
        assert open(paths["markdown"], encoding="utf-8").read().startswith("# Data Reconciliation Report")
        assert open(paths["csv"], encoding="utf-8").read().startswith("config_name,")

    def test_helpers(self):
        assert _status_icon("PASSED") == "PASS" and _status_icon("weird") == "    "
        assert _format_key({}) == "-"
        assert _format_key({"A": 1, "B": "x"}) == "A=1, B=x"

    def test_rows_missing_in_source_are_reported_even_with_empty_source(self):
        r = ReconcileResult(config_name="x", reconcile_type="data", status="FAILED",
                            total_rows_compared=0, missing_in_source=5,
                            sample_diffs=[RowDiff({"ID": 1}, [], "MISSING_IN_SOURCE")])
        md = ReconcileReport([r]).to_markdown()
        assert "5 missing in source" in md
