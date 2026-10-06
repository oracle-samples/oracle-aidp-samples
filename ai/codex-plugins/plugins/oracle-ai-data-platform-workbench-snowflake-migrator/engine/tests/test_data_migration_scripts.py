"""The in-AIDP data-migration scripts, exercised offline.

They run on a cluster, so they were the one part of this plugin with no test
coverage — and the first thing a review found there was a `fail()` that
recursed into itself, which would have turned every refusal into a
RecursionError instead of one message and exit 1.

Spark is injected as a fake here (the scripts take `spark` from the session
they are run in, and everything else through `SnowflakeSource`), so the
decisions are testable without a cluster: which statements are issued, what
gets refused, and what the reports record.
"""
import importlib.util
import json
import pathlib
import re
import sys
import types

import pytest

# The canonical stage sources. They live under engine/ because the shipped
# artifact is now a generated notebook (see target/stage_notebooks.py) and
# `data-migration-scripts/` holds only `.ipynb`.
SCRIPTS = (pathlib.Path(__file__).resolve().parents[1] / "dataplane")


def _load(name: str):
    """Import one script by path. They are not a package on purpose: each is
    uploaded to the workspace as a single file."""
    sys.path.insert(0, str(SCRIPTS))
    spec = importlib.util.spec_from_file_location(
        f"snowmig_script_{name}", SCRIPTS / f"{name}.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    # No step files under /Workspace/report/output (C:\Workspace on Windows)
    # unless a test passes --output-dir.
    if hasattr(module, "DEFAULT_OUTPUT_DIR"):
        module.DEFAULT_OUTPUT_DIR = ""
    return module


@pytest.fixture(scope="module")
def discover():
    return _load("00_discover_snowflake")


@pytest.fixture(scope="module")
def structure():
    return _load("01_create_structure")


@pytest.fixture(scope="module")
def copy_schema():
    return _load("02_copy_schema")


@pytest.fixture(scope="module")
def reconcile():
    return _load("03_reconcile")


# --- fail(): the helper every refusal path goes through -------------------

@pytest.mark.parametrize("name", ["00_discover_snowflake",
                                  "01_create_structure",
                                  "02_copy_schema", "03_reconcile"])
def test_fail_returns_one_and_prints_on_both_streams(name, capsys):
    module = _load(name)
    assert module.fail("a refusal") == 1
    captured = capsys.readouterr()
    # stdout is what a notebook task captures; stderr is what a shell run
    # shows. A refusal that reaches only one of them is invisible in the
    # other, which is how a live job failed with no explanation anywhere.
    assert "a refusal" in captured.out
    assert "a refusal" in captured.err


# --- discovery ------------------------------------------------------------

def test_decimal_from_snowflake_survives_the_json_write(discover):
    import decimal
    # `json.dumps` refuses Decimal, and it did so AFTER a successful
    # 1065-relation read, losing the whole discovery.
    assert discover._plain(decimal.Decimal("38")) == 38
    assert isinstance(discover._plain(decimal.Decimal("38")), int)
    assert discover._plain(decimal.Decimal("1.5")) == "1.5"
    json.dumps({"n": discover._plain(decimal.Decimal("1250000"))})


def test_source_types_are_recorded_with_their_precision(discover):
    import decimal
    D = decimal.Decimal
    assert discover._snowflake_type(
        {"DATA_TYPE": "NUMBER", "NUMERIC_PRECISION": D(38),
         "NUMERIC_SCALE": D(0)}) == "NUMBER(38,0)"
    assert discover._snowflake_type(
        {"DATA_TYPE": "TEXT", "CHARACTER_MAXIMUM_LENGTH": D(200)}) == "TEXT(200)"
    # Unqualified types pass through rather than being invented.
    assert discover._snowflake_type({"DATA_TYPE": "BOOLEAN"}) == "BOOLEAN"


class _FakeDF:
    def __init__(self, rows):
        self._rows = rows

    def collect(self):
        class Row(dict):
            def asDict(self):
                return dict(self)
        return [Row(r) for r in self._rows]

    def count(self):
        return len(self._rows)


class _FakeSource:
    """A SnowflakeSource stand-in that records what was asked of it."""

    def __init__(self, *, tables=(), columns=(), mode="connector"):
        self.mode = mode
        self.external_catalog = "ext"
        self.spark = _FakeSpark()
        self.queries: list[str] = []
        self._tables, self._columns = list(tables), list(columns)
        self.session_schema = "PUBLIC"

    def pushdown(self, sql, schema=None):
        self.queries.append(sql)
        return _FakeDF(self._columns if "COLUMNS" in sql else self._tables)

    def describe(self):
        return {"mode": self.mode, "database": "DB", "host": "h",
                "user": "u", "warehouse": "w", "role": "r",
                "session_schema": self.session_schema, "auth": "KeyPair"}

    def database(self):
        return "DB"


class _FakeSpark:
    def __init__(self):
        self.statements: list[str] = []
        self.counts: dict[str, int] = {}

    def sql(self, statement):
        self.statements.append(" ".join(statement.split()))
        low = statement.lower()
        if "count(*)" in low:
            for fqn, n in self.counts.items():
                if fqn in statement:
                    return _FakeDF([{"n": n}])
            return _FakeDF([{"n": 0}])
        if low.startswith("describe"):
            return _FakeDF([{"col_name": "A", "data_type": "string"}])
        return _FakeDF([])


class _CatalogSpark(_FakeSpark):
    """A fake with a CATALOG, so the scripts' read-backs mean something.

    `catalog` maps a backticked three-part name to its [(column, type)] list.
    DESCRIBE answers from it (and raises for a name it lacks, as Spark does);
    CREATE TABLE IF NOT EXISTS adds a table only when absent, with the types
    lower-cased the way Delta reports them; SHOW TABLES lists a schema; an
    INSERT lands the source count on the target (or `insert_lands` rows, to
    fake a short copy); COUNT(*) reads `counts`.
    """

    def __init__(self, catalog=None):
        super().__init__()
        self.catalog: dict[str, list[tuple[str, str]]] = dict(catalog or {})
        self.insert_lands: int | None = None
        self.sums: dict[str, dict[str, str]] = {}   # fqn -> {column: sum}

    def sql(self, statement):
        flat = " ".join(statement.split())
        self.statements.append(flat)
        low = flat.lower()
        if "count(*)" in low:
            for fqn, n in self.counts.items():
                if fqn in flat:
                    return _FakeDF([{"n": n}])
            return _FakeDF([{"n": 0}])
        if low.startswith("select") and "sum(" in low:
            fqn = flat.rsplit(" FROM ", 1)[1]
            cols = re.findall(r"AS STRING\) AS `([^`]+)`", flat)
            return _FakeDF([{c: self.sums.get(fqn, {}).get(c, "0")
                             for c in cols}])
        if low.startswith("describe"):
            fqn = flat.split(None, 1)[1].strip()
            if fqn not in self.catalog:
                raise RuntimeError(f"[TABLE_OR_VIEW_NOT_FOUND] {fqn}")
            return _FakeDF([{"col_name": n, "data_type": t}
                            for n, t in self.catalog[fqn]])
        if low.startswith("create table if not exists"):
            m = re.match(r"create table if not exists (\S+) \((.*)\) using delta$",
                         flat, re.IGNORECASE)
            if m:
                fqn, cols = m.group(1), m.group(2)
                if fqn not in self.catalog:
                    self.catalog[fqn] = [
                        (part.split(None, 1)[0].strip("`"),
                         part.split(None, 1)[1].strip().lower())
                        for part in re.split(r",\s*(?=`)", cols)]
            else:                                    # CTAS: types unknown
                fqn = flat.split()[5]
                self.catalog.setdefault(fqn, [("A", "string")])
            return _FakeDF([])
        if low.startswith("show tables in"):
            prefix = re.split(r"\s+in\s+", flat, maxsplit=1,
                              flags=re.IGNORECASE)[1] + "."
            return _FakeDF([{"namespace": "x",
                             "tableName": fqn[len(prefix):].strip("`"),
                             "isTemporary": False}
                            for fqn in self.catalog if fqn.startswith(prefix)])
        if low.startswith("insert"):
            # The copy names the source's columns (in the target's order);
            # the fake lands the count either way.
            m = re.match(r"insert (?:into|overwrite) (\S+) select .*? from "
                         r"(\S+)$", flat, re.IGNORECASE)
            tgt, src = m.group(1), m.group(2)
            landed = (self.counts.get(src, 0) if self.insert_lands is None
                      else self.insert_lands)
            if low.startswith("insert into"):
                landed += self.counts.get(tgt, 0)
            self.counts[tgt] = landed
            return _FakeDF([])
        return _FakeDF([])


def _inject_spark(monkeypatch, spark):
    """The scripts do `from pyspark.sql import SparkSession` inside main();
    pyspark is not installed here, so a stub module hands them `spark`."""
    class _Builder:
        @staticmethod
        def getOrCreate():
            return spark

    class SparkSession:
        builder = _Builder()

    pyspark = types.ModuleType("pyspark")
    pyspark_sql = types.ModuleType("pyspark.sql")
    pyspark_sql.SparkSession = SparkSession
    pyspark.sql = pyspark_sql
    monkeypatch.setitem(sys.modules, "pyspark", pyspark)
    monkeypatch.setitem(sys.modules, "pyspark.sql", pyspark_sql)


def _write_estate(reports: pathlib.Path, schemas: dict, plan: dict | None = None,
                  views: dict | None = None) -> pathlib.Path:
    """discovery_manifest.json under `reports`, and plan/ddl_plan.json beside
    it. `schemas` is {schema: [table, ...]}; `plan` is {(schema, table):
    [{name, type}]}; `views` is {schema: [view, ...]}."""
    reports.mkdir(parents=True, exist_ok=True)
    manifest = {"schemas": [
        {"name": s, "tables": [{"name": t, "columns": []} for t in tables],
         "views": [{"name": v} for v in (views or {}).get(s, [])],
         "errors": []}
        for s, tables in schemas.items()]}
    (reports / "discovery_manifest.json").write_text(
        json.dumps(manifest), encoding="utf-8")
    if plan is not None:
        statements = [{"source_identifier": f"DB.{s}.{t}", "object_type": "TABLE",
                       "expected_columns": cols}
                      for (s, t), cols in plan.items()]
        plan_dir = reports.parent / "plan"
        plan_dir.mkdir(parents=True, exist_ok=True)
        (plan_dir / "ddl_plan.json").write_text(
            json.dumps({"statements": statements}), encoding="utf-8")
    return reports


def _report(reports: pathlib.Path, name: str) -> dict:
    return json.loads((reports / name).read_text(encoding="utf-8"))


_PLAN_COLS = [{"name": "ID", "type": "DECIMAL(38,0)"},
              {"name": "AMOUNT", "type": "DECIMAL(18,2)"},
              {"name": "NOTE", "type": "STRING"}]


def test_discovery_reads_the_whole_estate_in_two_queries(discover):
    source = _FakeSource(
        tables=[{"TABLE_SCHEMA": "SALES", "TABLE_NAME": "ORDERS",
                 "TABLE_TYPE": "BASE TABLE", "ROW_COUNT": 10, "BYTES": 99},
                {"TABLE_SCHEMA": "SALES", "TABLE_NAME": "V_ORDERS",
                 "TABLE_TYPE": "VIEW", "ROW_COUNT": None, "BYTES": None},
                {"TABLE_SCHEMA": "INFORMATION_SCHEMA", "TABLE_NAME": "TABLES",
                 "TABLE_TYPE": "VIEW", "ROW_COUNT": None, "BYTES": None}],
        columns=[{"TABLE_SCHEMA": "SALES", "TABLE_NAME": "ORDERS",
                  "COLUMN_NAME": "ID", "ORDINAL_POSITION": 1,
                  "DATA_TYPE": "NUMBER", "IS_NULLABLE": "NO",
                  "NUMERIC_PRECISION": 38, "NUMERIC_SCALE": 0,
                  "CHARACTER_MAXIMUM_LENGTH": None},
                 {"TABLE_SCHEMA": "SALES", "TABLE_NAME": "V_ORDERS",
                  "COLUMN_NAME": "ID", "ORDINAL_POSITION": 1,
                  "DATA_TYPE": "NUMBER", "IS_NULLABLE": "YES",
                  "NUMERIC_PRECISION": 38, "NUMERIC_SCALE": 0,
                  "CHARACTER_MAXIMUM_LENGTH": None}])
    schemas = discover.discover_via_connector(
        source, wanted=None, exclude={"information_schema"})
    assert len(source.queries) == 2, "the whole estate, in two queries"
    assert [s["name"] for s in schemas] == ["SALES"], \
        "INFORMATION_SCHEMA is never a migration target"
    sales = schemas[0]
    assert [t["name"] for t in sales["tables"]] == ["ORDERS"]
    assert [v["name"] for v in sales["views"]] == ["V_ORDERS"]
    assert sales["tables"][0]["columns"][0]["type"] == "NUMBER(38,0)"
    assert sales["errors"] == []


def test_a_relation_with_no_columns_is_incomplete_not_empty(discover):
    source = _FakeSource(
        tables=[{"TABLE_SCHEMA": "S", "TABLE_NAME": "T",
                 "TABLE_TYPE": "BASE TABLE", "ROW_COUNT": 1, "BYTES": 1}],
        columns=[])
    schemas = discover.discover_via_connector(source, wanted=None, exclude=set())
    assert schemas[0]["errors"], "a column-less relation must be flagged"
    assert "INCOMPLETE" in schemas[0]["errors"][0]["error"]


# --- structure ------------------------------------------------------------

def test_the_approved_plan_is_the_authority(structure):
    plan = {"statements": [
        {"source_identifier": "DB.SALES.ORDERS",
         "expected_columns": [{"name": "ID", "type": "DECIMAL(38,0)"}]},
        {"source_identifier": "DB.SALES.BLOCKED"},          # no columns
        {"source_identifier": "SHORT.NAME"}]}               # malformed
    columns = structure.columns_from_ddl_plan(plan)
    assert list(columns) == [("SALES", "ORDERS")]
    assert columns[("SALES", "ORDERS")][0]["type"] == "DECIMAL(38,0)"


def test_snowflake_types_in_a_manifest_are_refused_not_translated(structure):
    # A connector-mode manifest records SOURCE types on purpose; feeding them
    # to Delta would be a silent mistranslation.
    assert structure._looks_like_snowflake_types(
        [{"name": "A", "type": "NUMBER(38,0)"}]) is True
    assert structure._looks_like_snowflake_types(
        [{"name": "A", "type": "VARIANT"}]) is True
    assert structure._looks_like_snowflake_types(
        [{"name": "A", "type": "decimal(38,0)"}]) is False


def test_create_table_from_columns_is_if_not_exists_and_delta(structure):
    spark = _CatalogSpark()
    status = structure.create_table_from_columns(
        spark, [{"name": "ID", "type": "DECIMAL(38,0)"}], "lake", "sales", "t")
    statement = next(s for s in spark.statements if "CREATE TABLE" in s)
    assert "CREATE TABLE IF NOT EXISTS" in statement
    assert "USING DELTA" in statement
    assert "`lake`.`sales`.`t`" in statement
    assert "DROP" not in statement.upper()
    assert status == "created"


# --- structure: the read-back after CREATE TABLE IF NOT EXISTS -------------
#
# `CREATE TABLE IF NOT EXISTS` on a table that is already there is a silent
# no-op, so recording `created` after it certified layouts this run never
# applied -- and the copy then INSERTs positionally into whatever was there.
# The claim is the read-back (invariant I2), as it already was for `deploy`.

def _structure_run(monkeypatch, tmp_path, spark, *, plan=None, argv=()):
    reports = _write_estate(tmp_path / "reports", {"SALES": ["ORDERS"]},
                            plan={("SALES", "ORDERS"): _PLAN_COLS}
                            if plan is None else plan)
    _inject_spark(monkeypatch, spark)
    module = _load("01_create_structure")
    rc = module.main(["--target-catalog", "lake", "--schema", "SALES",
                      "--reports-dir", str(reports), *argv])
    return rc, _report(reports, "structure_report_sales.json")


def test_a_fresh_table_is_created_and_read_back(structure, monkeypatch, tmp_path):
    spark = _CatalogSpark()
    rc, report = _structure_run(monkeypatch, tmp_path, spark)
    assert rc == 0
    assert report["objects"]["ORDERS"]["status"] == "created"
    create = next(i for i, s in enumerate(spark.statements)
                  if s.startswith("CREATE TABLE"))
    assert any(s.startswith("DESCRIBE `lake`.`SALES`.`ORDERS`")
               for s in spark.statements[create + 1:]), \
        "the claim is the read-back, not the CREATE returning"


def test_a_pre_existing_table_with_a_different_layout_is_type_drift_not_created(
        structure, monkeypatch, tmp_path, capsys):
    spark = _CatalogSpark({"`lake`.`SALES`.`ORDERS`": [("ID", "bigint"),
                                                      ("AMOUNT", "bigint")]})
    rc, report = _structure_run(monkeypatch, tmp_path, spark)
    assert rc == 1, "a layout the plan did not produce is a problem state"
    rec = report["objects"]["ORDERS"]
    assert rec["status"] == "type_drift"
    assert "column count differs: planned 3, found 2" in rec["reason"]
    assert "TYPE DRIFT" in capsys.readouterr().out
    # Left as found: nothing here drops or alters.
    assert spark.catalog["`lake`.`SALES`.`ORDERS`"] == [("ID", "bigint"),
                                                        ("AMOUNT", "bigint")]


def test_same_count_reordered_columns_is_type_drift(structure, monkeypatch,
                                                    tmp_path):
    # The silent false-PASS: same column count, same types, different order.
    # A positional INSERT lands every row in the wrong columns and the row
    # counts still match.
    spark = _CatalogSpark({"`lake`.`SALES`.`ORDERS`": [
        ("AMOUNT", "decimal(18,2)"), ("ID", "decimal(38,0)"), ("NOTE", "string")]})
    rc, report = _structure_run(monkeypatch, tmp_path, spark)
    assert rc == 1
    rec = report["objects"]["ORDERS"]
    assert rec["status"] == "type_drift"
    assert "position 1" in rec["reason"]


def test_a_pre_existing_matching_table_is_already_existed(structure, monkeypatch,
                                                          tmp_path):
    spark = _CatalogSpark({"`lake`.`SALES`.`ORDERS`": [
        ("ID", "decimal(38,0)"), ("AMOUNT", "decimal(18,2)"), ("NOTE", "string")]})
    rc, report = _structure_run(monkeypatch, tmp_path, spark)
    assert rc == 0
    assert report["objects"]["ORDERS"]["status"] == "already_existed"
    assert not any(s.startswith("CREATE TABLE") for s in spark.statements), \
        "a table that is already there and matches the plan needs no CREATE"


def test_type_drift_is_rechecked_on_resume_but_created_and_existing_are_skipped(
        structure, monkeypatch, tmp_path):
    reports = _write_estate(tmp_path / "reports", {"SALES": ["T1", "T2", "T3"]},
                            plan={("SALES", t): _PLAN_COLS
                                  for t in ("T1", "T2", "T3")})
    (reports / "structure_report_sales.json").write_text(json.dumps(
        {"schema": "SALES", "target": "lake.SALES",
         "objects": {"T1": {"status": "created"},
                     "T2": {"status": "already_existed"},
                     "T3": {"status": "type_drift", "reason": "old"}}}),
        encoding="utf-8")
    spark = _CatalogSpark({f"`lake`.`SALES`.`{t}`": [
        ("ID", "decimal(38,0)"), ("AMOUNT", "decimal(18,2)"), ("NOTE", "string")]
        for t in ("T1", "T2", "T3")})
    _inject_spark(monkeypatch, spark)
    rc = _load("01_create_structure").main(
        ["--target-catalog", "lake", "--schema", "SALES",
         "--reports-dir", str(reports)])
    touched = {s.split("`")[5] for s in spark.statements
               if s.startswith("DESCRIBE")}
    assert touched == {"T3"}, "only the drifted table is looked at again"
    report = _report(reports, "structure_report_sales.json")
    assert report["objects"]["T3"]["status"] == "already_existed", \
        "the operator fixed the table; the record follows what is there now"
    assert rc == 0


# --- structure: a run that creates nothing is not a success ---------------
#
# `not_in_plan` is the right per-table record, but a run in which EVERY table
# is not_in_plan created nothing -- the plan and the requested schema do not
# overlap (a plan for another estate, another wave, or a plan in which the
# engine blocked every table). Exit 0 there gave three SUCCESS jobs that
# created, copied and reconciled nothing. The check is run-wide, not
# per-schema: a canary plan legitimately leaves the other schemas untouched.

def test_ctas_already_existed_says_the_layout_was_not_compared(
        monkeypatch, tmp_path):
    """`--mode ctas` derives types from the source read and has no plan to
    compare a pre-existing table with, so `already_existed` there is a
    weaker claim than in ddl-plan mode. The record has to say the layout
    was NOT compared: the copy takes `already_existed` into its default
    scope, and reconcile prints the reason beside the verdict."""
    spark = _CatalogSpark({"`lake`.`SALES`.`ORDERS`": [("A", "string")]})

    class Source:
        def __init__(self, *args, **kwargs):
            self.spark = spark

        def register_temp_view(self, schema, name, view):
            return f"`{view}`"

        def drop_temp_view(self, view):
            pass

    reports = _write_estate(tmp_path / "reports", {"SALES": ["ORDERS"]})
    _inject_spark(monkeypatch, spark)
    module = _load("01_create_structure")
    monkeypatch.setattr(module, "SnowflakeSource", Source)
    rc = module.main(["--target-catalog", "lake", "--schema", "SALES",
                      "--reports-dir", str(reports), "--mode", "ctas"])
    rec = _report(reports, "structure_report_sales.json")["objects"]["ORDERS"]
    assert rc == 0
    assert rec["status"] == "already_existed"
    assert "not compared" in (rec.get("reason") or "").lower(), rec
    assert not any("CREATE TABLE" in s for s in spark.statements)

def test_a_structure_run_that_creates_nothing_is_not_a_success(
        structure, monkeypatch, tmp_path, capsys):
    reports = _write_estate(tmp_path / "reports",
                            {"SALES": ["ORDERS", "CUSTOMERS"]},
                            plan={("FINANCE", "LEDGER"): _PLAN_COLS})
    spark = _CatalogSpark()
    _inject_spark(monkeypatch, spark)
    rc = _load("01_create_structure").main(
        ["--target-catalog", "lake", "--reports-dir", str(reports)])
    assert rc == 1
    assert not any(s.startswith("CREATE TABLE") for s in spark.statements)
    report = _report(reports, "structure_report_sales.json")
    assert {r["status"] for r in report["objects"].values()} == {"not_in_plan"}
    out = capsys.readouterr().out
    assert "created 0" in out
    assert "do not overlap" in out


def test_a_canary_plan_covering_one_schema_still_exits_zero(
        structure, monkeypatch, tmp_path):
    reports = _write_estate(tmp_path / "reports",
                            {"SALES": ["ORDERS"], "FINANCE": ["LEDGER"]},
                            plan={("FINANCE", "LEDGER"): _PLAN_COLS})
    spark = _CatalogSpark()
    _inject_spark(monkeypatch, spark)
    rc = _load("01_create_structure").main(
        ["--target-catalog", "lake", "--reports-dir", str(reports)])
    assert rc == 0, "a scoped first wave is the documented way to start"
    assert sum(1 for s in spark.statements if s.startswith("CREATE TABLE")) == 1
    sales = _report(reports, "structure_report_sales.json")
    assert sales["objects"]["ORDERS"]["status"] == "not_in_plan"


def test_a_resumed_structure_run_with_everything_created_exits_zero(
        structure, monkeypatch, tmp_path):
    reports = _write_estate(tmp_path / "reports",
                            {"SALES": ["ORDERS", "CUSTOMERS"]}, plan={})
    (reports / "structure_report_sales.json").write_text(json.dumps(
        {"schema": "SALES", "target": "lake.SALES",
         "objects": {"ORDERS": {"status": "created"},
                     "CUSTOMERS": {"status": "already_existed"}}}),
        encoding="utf-8")
    _inject_spark(monkeypatch, _CatalogSpark())
    rc = _load("01_create_structure").main(
        ["--target-catalog", "lake", "--reports-dir", str(reports)])
    assert rc == 0, "skipping work already done is not creating nothing"


def test_dry_run_is_not_failed_for_an_empty_plan(structure, monkeypatch,
                                                 tmp_path):
    reports = _write_estate(tmp_path / "reports", {"SALES": ["ORDERS"]},
                            plan={("FINANCE", "LEDGER"): _PLAN_COLS})
    _inject_spark(monkeypatch, _CatalogSpark())
    rc = _load("01_create_structure").main(
        ["--target-catalog", "lake", "--reports-dir", str(reports),
         "--dry-run"])
    assert rc == 0


class _MainSource:
    """Stands in for `SnowflakeSource` when 02_copy_schema.main() builds one:
    external-catalog shaped, so a source table is a three-part name the fake
    catalog can count. `fail_on` names tables whose source read raises."""
    fail_on: tuple = ()
    counts_raise = False

    def __init__(self, spark, *, mode="connector", config=None,
                 external_catalog=None):
        self.spark = spark
        self.mode = "external-catalog"
        self.external_catalog = "ext"

    def describe(self):
        return {"mode": "fake"}

    def source_counts(self, schema, tables):
        if self.counts_raise:
            raise RuntimeError("no batched counts today")
        return {t: self.spark.counts.get(f"`ext`.`{schema}`.`{t}`", 0)
                for t in tables}

    def register_temp_view(self, schema, table, view):
        if table in self.fail_on:
            raise RuntimeError("DATA_ACCESS_LAYER_0007 - Login has timed out")
        return f"`ext`.`{schema}`.`{table}`"

    def drop_temp_view(self, view):
        pass


def _copy_run(monkeypatch, reports, spark, *, argv=(), source_cls=_MainSource):
    _inject_spark(monkeypatch, spark)
    module = _load("02_copy_schema")
    monkeypatch.setattr(module, "SnowflakeSource", source_cls)
    rc = module.main(["--target-catalog", "lake", "--schema", "SALES",
                      "--reports-dir", str(reports), *argv])
    return rc, _report(reports, "copy_report_sales.json")


def test_the_copy_scope_excludes_a_drifted_table(copy_schema, monkeypatch,
                                                 tmp_path, capsys):
    """A `type_drift` table has a layout the plan did not produce, and the
    copy is a positional INSERT INTO ... SELECT *: it must never be in the
    default scope. A matching pre-existing table is the approved layout."""
    reports = _write_estate(tmp_path / "reports", {"SALES": ["A", "B", "C"]})
    (reports / "structure_report_sales.json").write_text(json.dumps(
        {"schema": "SALES", "target": "lake.SALES",
         "objects": {"A": {"status": "created"},
                     "B": {"status": "already_existed"},
                     "C": {"status": "type_drift", "reason": "x"}}}),
        encoding="utf-8")
    spark = _CatalogSpark({f"`{cat}`.`SALES`.`{t}`": [("A", "string")]
                           for t in "ABC" for cat in ("lake", "ext")})
    spark.counts = {f"`ext`.`SALES`.`{t}`": 3 for t in "ABC"}
    rc, report = _copy_run(monkeypatch, reports, spark, argv=["--mode", "append"])
    assert rc == 0
    assert sorted(report["tables"]) == ["A", "B"]
    assert not any("`C`" in s for s in spark.statements if "INSERT" in s)
    assert "scope: 2 table(s)" in capsys.readouterr().out


def _copy_main(monkeypatch, reports, spark, *, argv=()):
    """`_copy_run` without the report read: a run that refuses before the
    first table writes no copy report."""
    _inject_spark(monkeypatch, spark)
    module = _load("02_copy_schema")
    monkeypatch.setattr(module, "SnowflakeSource", _MainSource)
    return module.main(["--target-catalog", "lake", "--schema", "SALES",
                        "--reports-dir", str(reports), *argv])


_REORDERED = {
    "`ext`.`SALES`.`T`": [("FIRST_NAME", "string"), ("LAST_NAME", "string")],
    "`lake`.`SALES`.`T`": [("LAST_NAME", "string"), ("FIRST_NAME", "string")]}


def test_an_all_drift_schema_is_refused_not_copied_from_the_manifest(
        copy_schema, monkeypatch, tmp_path, capsys):
    """The `created` filter only ran when the structure step created
    something. A report recording every table `type_drift` -- a re-plan over
    tables that all pre-exist with the old layout -- fell back to the whole
    manifest, and the positional INSERT landed rows in the wrong columns with
    matching counts: `verified`, exit 0. The copy's own pre-flight cannot
    catch a same-count reorder of STRING columns."""
    reports = _write_estate(tmp_path / "reports", {"SALES": ["T"]})
    (reports / "structure_report_sales.json").write_text(json.dumps(
        {"schema": "SALES", "target": "lake.SALES",
         "objects": {"T": {"status": "type_drift", "reason": "x"}}}),
        encoding="utf-8")
    spark = _CatalogSpark(_REORDERED)
    spark.counts = {"`ext`.`SALES`.`T`": 3}
    rc = _copy_main(monkeypatch, reports, spark)
    assert rc == 1, "an empty scope is a refusal, not a job that did nothing"
    assert not any("INSERT" in s for s in spark.statements), spark.statements
    recorded = (_report(reports, "copy_report_sales.json")["tables"]
                if (reports / "copy_report_sales.json").exists() else {})
    assert recorded.get("T", {}).get("status") != "verified"
    out = capsys.readouterr().out
    assert "type_drift" in out and "nothing to copy" in out


def test_the_manifest_fallback_still_excludes_a_drifted_table(
        copy_schema, monkeypatch, tmp_path, capsys):
    """Nothing created, one table drifted, one not in the plan: the fallback
    copies what is left and says what it left out."""
    reports = _write_estate(tmp_path / "reports", {"SALES": ["T", "U"]})
    (reports / "structure_report_sales.json").write_text(json.dumps(
        {"schema": "SALES", "target": "lake.SALES",
         "objects": {"T": {"status": "type_drift", "reason": "x"},
                     "U": {"status": "not_in_plan"}}}), encoding="utf-8")
    spark = _CatalogSpark(_REORDERED)
    spark.counts = {"`ext`.`SALES`.`T`": 3}
    rc, report = _copy_run(monkeypatch, reports, spark)
    assert not any("INSERT" in s and "`T`" in s for s in spark.statements), \
        spark.statements
    assert "T" not in report["tables"]
    assert report["tables"]["U"]["status"] == "target_missing"
    assert "1 type_drift table(s) excluded" in capsys.readouterr().out


def test_copy_says_the_truth_when_the_structure_report_created_nothing(
        copy_schema, monkeypatch, tmp_path, capsys):
    """The structure report exists and records every table `not_in_plan`;
    the scope log used to say "no structure report ... was found", which is
    false and points the operator away from the actual cause (the plan)."""
    reports = _write_estate(tmp_path / "reports", {"SALES": ["A", "B"]})
    (reports / "structure_report_sales.json").write_text(json.dumps(
        {"schema": "SALES", "target": "lake.SALES",
         "objects": {"A": {"status": "not_in_plan"},
                     "B": {"status": "not_in_plan"}}}), encoding="utf-8")
    rc, report = _copy_run(monkeypatch, reports, _CatalogSpark())
    out = capsys.readouterr().out
    assert "no structure report" not in out
    assert "0 created" in out and "not_in_plan" in out
    assert {r["status"] for r in report["tables"].values()} == {"target_missing"}


def test_copy_still_falls_back_to_the_manifest_when_no_report_exists(
        copy_schema, monkeypatch, tmp_path, capsys):
    reports = _write_estate(tmp_path / "reports", {"SALES": ["A", "B"]})
    rc, report = _copy_run(monkeypatch, reports, _CatalogSpark())
    assert "no structure report" in capsys.readouterr().out
    assert sorted(report["tables"]) == ["A", "B"]


def test_an_empty_column_list_raises_rather_than_creating_nothing(structure):
    with pytest.raises(ValueError, match="no column list"):
        structure.create_table_from_columns(_FakeSpark(), [], "c", "s", "t")


def test_a_report_from_a_different_target_is_not_reused(structure, tmp_path):
    # Resumability is keyed by SOURCE schema, so a prior record against
    # another destination must not let this run skip every create -- observed
    # live, against an empty target schema.
    path = tmp_path / "structure_report_sales.json"
    path.write_text(json.dumps({"schema": "SALES", "target": "lake.old",
                                "objects": {"T": {"status": "created"}}}), encoding="utf-8")
    fresh = structure._load_report(path, "SALES", "lake.new")
    assert fresh["objects"] == {}
    assert fresh["target"] == "lake.new"
    # The old record is kept, not destroyed.
    assert list(tmp_path.glob("structure_report_sales.lake_old.json"))


def test_a_report_for_the_same_target_is_resumed(structure, tmp_path):
    path = tmp_path / "structure_report_sales.json"
    path.write_text(json.dumps({"schema": "SALES", "target": "lake.new",
                                "objects": {"T": {"status": "created"}}}), encoding="utf-8")
    prior = structure._load_report(path, "SALES", "lake.new")
    assert prior["objects"]["T"]["status"] == "created"


# --- copy -----------------------------------------------------------------

def test_the_copy_never_drops_and_overwrite_rewrites_rows(copy_schema):
    spark = _FakeSpark()
    spark.counts = {"`src`": 3, "`lake`.`s`.`t`": 0}
    out = copy_schema._copy(spark, "`src`", "`lake`.`s`.`t`",
                            mode="overwrite", verify="counts",
                            retries=0, retry_base_delay=0, started="now")
    blob = " ".join(spark.statements).upper()
    assert "INSERT OVERWRITE" in blob
    assert "DROP" not in blob and "TRUNCATE" not in blob
    assert out["status"] in ("verified", "count_mismatch")


def test_skip_existing_leaves_a_nonempty_target_alone(copy_schema):
    spark = _FakeSpark()
    spark.counts = {"`src`": 3, "`lake`.`s`.`t`": 3}
    out = copy_schema._copy(spark, "`src`", "`lake`.`s`.`t`",
                            mode="skip-existing", verify="counts",
                            retries=0, retry_base_delay=0, started="now")
    assert out["status"] == "skipped_nonempty"
    assert not any("INSERT" in s.upper() for s in spark.statements)


# --- copy: a re-run must never soften a recorded failure -------------------
#
# A table whose copy ended `count_mismatch` is still in `todo` on the next
# run. In the default skip-existing mode `_copy` saw target_rows > 0 and
# returned `skipped_nonempty`, which OVERWROTE the mismatch record; reconcile
# maps that to PRESENT_NOT_REVERIFIED, not a problem, exit 0. Re-running the
# failed job unchanged -- the most natural reaction -- erased the failure
# without moving a row.

def test_skip_existing_on_a_short_target_is_a_count_mismatch_not_a_skip(
        copy_schema):
    spark = _FakeSpark()
    spark.counts = {"`src`": 91, "`lake`.`s`.`t`": 90}
    out = copy_schema._copy(spark, "`src`", "`lake`.`s`.`t`",
                            mode="skip-existing", verify="counts",
                            retries=0, retry_base_delay=0, started="now")
    assert out["status"] == "count_mismatch"
    assert out["target_count"] == 90 and out["source_count"] == 91
    assert "NOT verified" in out["reason"]
    assert not any("INSERT" in s.upper() for s in spark.statements), \
        "skip-existing still never writes"


def _seeded_copy_report(reports, table, record):
    (reports / "copy_report_sales.json").write_text(json.dumps(
        {"schema": "SALES", "target": "lake.SALES",
         "tables": {table: record}}), encoding="utf-8")


def _both_sides(table, columns=(("A", "string"),)):
    """The same table on the source (`ext`) and target (`lake`) side."""
    return {f"`ext`.`SALES`.`{table}`": list(columns),
            f"`lake`.`SALES`.`{table}`": list(columns)}


def test_a_rerun_does_not_downgrade_a_prior_count_mismatch(
        copy_schema, monkeypatch, tmp_path):
    reports = _write_estate(tmp_path / "reports", {"SALES": ["T"]})
    _seeded_copy_report(reports, "T", {"status": "count_mismatch",
                                       "source_count": 91, "target_count": 90,
                                       "reason": "target has 90 row(s), source has 91. NOT verified."})
    spark = _CatalogSpark(_both_sides("T"))
    spark.counts = {"`ext`.`SALES`.`T`": 91, "`lake`.`SALES`.`T`": 90}
    rc, report = _copy_run(monkeypatch, reports, spark)
    assert rc == 1
    assert report["tables"]["T"]["status"] == "count_mismatch"
    assert not any("INSERT" in s for s in spark.statements)


def test_a_rerun_keeps_a_prior_sum_mismatch_when_only_counts_now_agree(
        copy_schema, monkeypatch, tmp_path):
    """Counts alone cannot clear a sum mismatch: the resident rows were never
    re-verified, so the record stays until a real re-copy verifies them."""
    reports = _write_estate(tmp_path / "reports", {"SALES": ["T"]})
    _seeded_copy_report(reports, "T", {"status": "sum_mismatch",
                                       "source_count": 91, "target_count": 91,
                                       "reason": "1 decimal column(s) do not sum equal. NOT verified."})
    spark = _CatalogSpark(_both_sides("T"))
    spark.counts = {"`ext`.`SALES`.`T`": 91, "`lake`.`SALES`.`T`": 91}
    rc, report = _copy_run(monkeypatch, reports, spark)
    assert rc == 1
    rec = report["tables"]["T"]
    assert rec["status"] == "sum_mismatch"
    assert "left the target untouched" in rec["reason"]
    assert not any("INSERT" in s for s in spark.statements)


def test_force_under_the_default_mode_is_refused_not_a_silent_downgrade(
        copy_schema, monkeypatch, tmp_path, capsys):
    """--force puts verified tables back in scope, but skip-existing cannot
    re-copy a non-empty table: the flag copied nothing and overwrote a
    `verified` record with `skipped_nonempty`. Refuse the pair instead of
    guessing that the operator meant overwrite."""
    reports = _write_estate(tmp_path / "reports", {"SALES": ["T"]})
    _seeded_copy_report(reports, "T", {"status": "verified",
                                       "source_count": 91, "target_count": 91})
    spark = _CatalogSpark(_both_sides("T"))
    spark.counts = {"`ext`.`SALES`.`T`": 91, "`lake`.`SALES`.`T`": 91}
    rc, report = _copy_run(monkeypatch, reports, spark, argv=["--force"])
    assert rc == 1
    assert "--mode overwrite" in capsys.readouterr().out
    assert report["tables"]["T"]["status"] == "verified"
    assert not any("INSERT" in s for s in spark.statements)


@pytest.mark.parametrize("target_count,verdict", [
    (90, "STRUCTURE_ONLY_COPY_FAILED"), (91, "PRESENT_NOT_REVERIFIED")])
def test_reconcile_reads_the_counts_a_skipped_record_carries(
        reconcile, tmp_path, target_count, verdict):
    # Belt and braces: an older or hand-edited copy report that says
    # `skipped_nonempty` over unequal counts cannot render as fine.
    _seeded_copy_report(tmp_path, "T", {"status": "skipped_nonempty",
                                        "source_count": 91,
                                        "target_count": target_count})
    spark = _CatalogSpark({"`lake`.`SALES`.`T`": [("A", "string")]})
    rec = reconcile.reconcile(spark, manifest=_manifest("T"),
                              target_catalog="lake", reports=tmp_path,
                              counts=False)
    assert rec["schemas"][0]["tables"][0]["verdict"] == verdict


# --- copy: a per-table failure is recorded and the run continues -----------
#
# Only the INSERT was guarded. A source read (a Snowflake login in connector
# mode), a COUNT(*) or a SUM that raised propagated out of the per-table loop:
# the job died with a traceback, the failing table had no record, and every
# table after it was never attempted -- and reconcile then showed those as
# "not attempted yet", not as the failure that happened.

def _three_tables(tmp_path):
    reports = _write_estate(tmp_path / "reports", {"SALES": ["T1", "T2", "T3"]})
    catalog = {}
    for t in ("T1", "T2", "T3"):
        catalog.update(_both_sides(t))
    spark = _CatalogSpark(catalog)
    spark.counts = {f"`ext`.`SALES`.`{t}`": 5 for t in ("T1", "T2", "T3")}
    return reports, spark


def test_a_source_read_failure_is_recorded_and_the_run_continues(
        copy_schema, monkeypatch, tmp_path):
    reports, spark = _three_tables(tmp_path)

    class Flaky(_MainSource):
        fail_on = ("T2",)

    rc, report = _copy_run(monkeypatch, reports, spark,
                           argv=["--mode", "append"], source_cls=Flaky)
    assert rc == 1
    assert report["tables"]["T2"]["status"] == "failed"
    assert "DATA_ACCESS_LAYER_0007" in report["tables"]["T2"]["reason"]
    assert report["tables"]["T1"]["status"] == "verified"
    assert report["tables"]["T3"]["status"] == "verified", \
        "the table after the failure is still attempted"
    assert any("INSERT INTO `lake`.`SALES`.`T3`" in s for s in spark.statements)


def test_a_target_count_failure_is_recorded_and_the_run_continues(
        copy_schema, monkeypatch, tmp_path):
    reports, spark = _three_tables(tmp_path)

    class NoCount(_CatalogSpark):
        def sql(self, statement):
            if "count(*)" in statement.lower() and "`lake`.`SALES`.`T2`" in statement:
                self.statements.append(" ".join(statement.split()))
                raise RuntimeError("[INSUFFICIENT_PERMISSIONS] on lake.SALES.T2")
            return super().sql(statement)

    denied = NoCount(spark.catalog)
    denied.counts = spark.counts
    rc, report = _copy_run(monkeypatch, reports, denied, argv=["--mode", "append"])
    assert rc == 1
    assert report["tables"]["T2"]["status"] == "failed"
    assert "INSUFFICIENT_PERMISSIONS" in report["tables"]["T2"]["reason"]
    assert report["tables"]["T3"]["status"] == "verified"


def test_a_failure_after_the_insert_landed_says_so(copy_schema):
    """A `failed` record after a completed INSERT must not look like a failed
    INSERT: an `append` re-run would duplicate every row."""
    class CountDiesAfterInsert(_CatalogSpark):
        def sql(self, statement):
            out = super().sql(statement)
            if "count(*)" in statement.lower() and _TGT in statement \
                    and any(s.startswith("INSERT") for s in self.statements):
                raise RuntimeError("executor lost")
            return out

    spark = CountDiesAfterInsert({_SRC: [("A", "string")], _TGT: [("A", "string")]})
    spark.counts = {_SRC: 3, _TGT: 0}
    out = _copy_typed(copy_schema, spark, verify="counts")
    assert out["status"] == "failed"
    assert out["insert_completed"] is True
    assert "executor lost" in out["reason"]
    assert "--mode overwrite" in out["reason"]
    assert any(s.startswith("INSERT") for s in spark.statements)


# --- copy: the DECIMAL check is keyed off the SOURCE, not the target -------
#
# `--verify counts+sums` derived both the DECIMAL column set and the cast
# scale from the TARGET's DESCRIBE. A source NUMBER(18,2) whose target column
# was bigint (a pre-existing or hand-made layout) was simply not summed:
# INSERT cast 12.99 -> 12 on every row, counts matched, and the record read
# `verified` with `decimal_columns_checked: []` -- which looks like "no
# decimals", not "could not compare". A target decimal(18,0) rounded BOTH
# sides to scale 0 before comparing and passed with the column listed.

_SRC, _TGT = "`ext`.`SALES`.`ORDERS`", "`lake`.`SALES`.`ORDERS`"
_SRC_TYPES = [("ORDER_ID", "decimal(38,0)"), ("AMOUNT", "decimal(18,2)"),
              ("NOTE", "string")]


def _typed(target_types, *, src_sum="38.97", tgt_sum="38.97"):
    spark = _CatalogSpark({_SRC: _SRC_TYPES, _TGT: target_types})
    spark.counts = {_SRC: 3, _TGT: 0}
    spark.sums = {_SRC: {"ORDER_ID": "6", "AMOUNT": src_sum},
                  _TGT: {"ORDER_ID": "6", "AMOUNT": tgt_sum}}
    return spark


def _copy_typed(copy_schema, spark, verify="counts+sums", mode="append"):
    return copy_schema._copy(spark, _SRC, _TGT, mode=mode, verify=verify,
                             retries=0, retry_base_delay=0, started="now")


@pytest.mark.parametrize("verify", ["counts", "counts+sums"])
def test_a_source_decimal_that_is_not_decimal_on_the_target_is_type_drift(
        copy_schema, verify):
    spark = _typed([("ORDER_ID", "bigint"), ("AMOUNT", "bigint"),
                    ("NOTE", "string")])
    out = _copy_typed(copy_schema, spark, verify=verify)
    assert out["status"] == "type_drift"
    assert set(out["type_drift"]) == {"ORDER_ID", "AMOUNT"}
    assert out["type_drift"]["AMOUNT"] == {"source": "decimal(18,2)",
                                           "target": "bigint"}
    assert "NOT copied" in out["reason"]
    assert not any("INSERT" in s for s in spark.statements), \
        "the pre-flight runs before the write, in both verify modes"


@pytest.mark.parametrize("target_type,why", [
    ("decimal(18,0)", "scale narrowed: cents are rounded away"),
    ("decimal(10,2)", "precision narrowed: large values overflow"),
    ("decimal(19,4)", "integer digits narrowed: 15 where the source has 16")])
def test_a_narrower_target_decimal_is_type_drift(copy_schema, target_type, why):
    spark = _typed([("ORDER_ID", "decimal(38,0)"), ("AMOUNT", target_type),
                    ("NOTE", "string")])
    out = _copy_typed(copy_schema, spark)
    assert out["status"] == "type_drift", why
    assert list(out["type_drift"]) == ["AMOUNT"]


def test_decimal_sums_use_the_source_column_set_and_scale(copy_schema):
    # A WIDER target (more integer digits AND more scale) is fine, and the
    # cast scale stays the source's on both sides, so the comparison is exact
    # rather than rounded to whatever the target happens to be.
    spark = _typed([("ORDER_ID", "decimal(38,0)"), ("AMOUNT", "decimal(20,4)"),
                    ("NOTE", "string")])
    out = _copy_typed(copy_schema, spark)
    assert out["status"] == "verified"
    assert out["decimal_columns_checked"] == ["ORDER_ID", "AMOUNT"]
    sums = [s for s in spark.statements if "SUM(" in s]
    assert len(sums) == 2 and any(s.endswith(_SRC) for s in sums) \
        and any(s.endswith(_TGT) for s in sums)
    for s in sums:
        assert "CAST(`AMOUNT` AS DECIMAL(38,2))" in s, s
        assert "DECIMAL(38,4)" not in s


def test_a_sum_mismatch_is_not_verified(copy_schema):
    spark = _typed([("ORDER_ID", "decimal(38,0)"), ("AMOUNT", "decimal(18,2)"),
                    ("NOTE", "string")], tgt_sum="36.00")
    out = _copy_typed(copy_schema, spark)
    assert out["status"] == "sum_mismatch"
    assert out["sum_drift"] == {"AMOUNT": {"source": "38.97", "target": "36.00"}}
    assert "NOT verified" in out["reason"]


def test_a_source_with_no_decimal_columns_says_so(copy_schema):
    spark = _CatalogSpark({_SRC: [("NOTE", "string")], _TGT: [("NOTE", "string")]})
    spark.counts = {_SRC: 3, _TGT: 0}
    out = _copy_typed(copy_schema, spark)
    assert out["status"] == "verified"
    assert out["decimal_columns_checked"] == []
    assert "no DECIMAL" in out["decimal_columns_note"], \
        "an empty list must read as 'none to check', not 'could not compare'"


def test_type_drift_reconciles_as_a_copy_failure(reconcile, tmp_path):
    _seeded_copy_report(tmp_path, "T", {"status": "type_drift",
                                        "reason": "1 DECIMAL column(s) ..."})
    spark = _CatalogSpark({"`lake`.`SALES`.`T`": [("A", "string")]})
    rec = reconcile.reconcile(spark, manifest=_manifest("T"),
                              target_catalog="lake", reports=tmp_path,
                              counts=False)
    assert rec["schemas"][0]["tables"][0]["verdict"] == "STRUCTURE_ONLY_COPY_FAILED"


def test_a_count_mismatch_is_not_verified(copy_schema):
    class Mismatch(_FakeSpark):
        def sql(self, statement):
            out = super().sql(statement)
            if "count(*)" in statement.lower() and "src" in statement:
                return _FakeDF([{"n": 9}])
            return out

    spark = Mismatch()
    spark.counts = {"`lake`.`s`.`t`": 0}
    out = copy_schema._copy(spark, "`src`", "`lake`.`s`.`t`", mode="append",
                            verify="counts", retries=0, retry_base_delay=0,
                            started="now")
    assert out["status"] == "count_mismatch"
    assert "NOT verified" in out["reason"]


def test_a_batched_source_count_is_used_instead_of_a_fresh_one(copy_schema):
    spark = _FakeSpark()
    spark.counts = {"`lake`.`s`.`t`": 0}
    out = copy_schema._copy(spark, "`src`", "`lake`.`s`.`t`", mode="append",
                            verify="counts", retries=0, retry_base_delay=0,
                            started="now", source_count=0)
    # The pre-copy source count came from the caller's batch, so only the
    # post-copy verification counts the source again.
    assert sum(1 for s in spark.statements
               if "count(*)" in s.lower() and "`src`" in s) == 1
    assert out["status"] == "verified"


# --- reconcile ------------------------------------------------------------

def test_an_unreadable_target_schema_is_not_reported_as_empty(reconcile):
    class NoList(_FakeSpark):
        def sql(self, statement):
            if statement.lower().startswith("show tables"):
                raise RuntimeError("denied")
            return super().sql(statement)

    manifest = {"schemas": [{"name": "SALES",
                             "tables": [{"name": "ORDERS", "columns": []}],
                             "views": [], "errors": []}]}
    rec = reconcile.reconcile(NoList(), manifest=manifest,
                              target_catalog="lake",
                              reports=pathlib.Path("/nonexistent"),
                              counts=False)
    schema = rec["schemas"][0]
    assert schema["target_readable"] is False
    assert schema["tables"][0]["verdict"] == "TARGET_UNREADABLE"
    assert "UNREADABLE" in reconcile.render(rec)


def test_a_table_a_report_claims_but_the_catalog_lacks_is_flagged(reconcile,
                                                                  tmp_path):
    (tmp_path / "copy_report_sales.json").write_text(json.dumps(
        {"schema": "SALES", "target": "lake.sales",
         "tables": {"ORDERS": {"status": "verified"}}}), encoding="utf-8")
    manifest = {"schemas": [{"name": "SALES",
                             "tables": [{"name": "ORDERS", "columns": []}],
                             "views": [], "errors": []}]}
    rec = reconcile.reconcile(_FakeSpark(), manifest=manifest,
                              target_catalog="lake", reports=tmp_path,
                              counts=False)
    row = rec["schemas"][0]["tables"][0]
    assert row["verdict"] == "MISSING_DESPITE_REPORT"
    assert rec["totals"]["MISSING_DESPITE_REPORT"] == 1


# --- reconcile: a report is evidence about the catalog it was written for --
#
# 01 and 02 both refuse to reuse a report whose `target` differs from this
# run's. 03 split the report's target on the first '.' and kept only the
# schema half, so a copy report verified against a TEST catalog was applied
# to the PRODUCTION catalog: after re-pointing the structure stage, the
# deliverable said every empty production table was MIGRATED_VERIFIED.

def test_a_report_written_for_another_catalog_is_not_evidence_here(
        reconcile, tmp_path, capsys):
    (tmp_path / "copy_report_sales.json").write_text(json.dumps(
        {"schema": "SALES", "target": "lake_test.SALES",
         "tables": {"ORDERS": {"status": "verified"}}}), encoding="utf-8")
    (tmp_path / "structure_report_sales.json").write_text(json.dumps(
        {"schema": "SALES", "target": "lake_prod.SALES",
         "objects": {"ORDERS": {"status": "created"}}}), encoding="utf-8")
    spark = _CatalogSpark({"`LAKE_PROD`.`SALES`.`ORDERS`": [("A", "string")]})
    rec = reconcile.reconcile(spark, manifest=_manifest("ORDERS"),
                              target_catalog="LAKE_PROD", reports=tmp_path,
                              counts=False)
    s = rec["schemas"][0]
    row = s["tables"][0]
    assert row["copy"] == "not_attempted"
    assert row["structure"] == "created", \
        "the same-catalog report (case-insensitively) is still used"
    assert row["verdict"] == "STRUCTURE_ONLY"
    assert "MIGRATED_VERIFIED" not in rec["totals"]
    assert s["reports_ignored_for_other_catalog"] == {"copy": "lake_test.SALES"}
    assert "lake_test.SALES" in capsys.readouterr().out
    assert "lake_test.SALES" in reconcile.render(rec)


def test_a_report_without_a_target_key_is_still_trusted(reconcile, tmp_path):
    # Older reports carry no `target`; 02 trusts those too (prior.get("target")
    # in (None, target)), and so does reconcile.
    (tmp_path / "copy_report_sales.json").write_text(json.dumps(
        {"schema": "SALES", "tables": {"ORDERS": {"status": "verified"}}}),
        encoding="utf-8")
    spark = _CatalogSpark({"`lake`.`SALES`.`ORDERS`": [("A", "string")]})
    rec = reconcile.reconcile(spark, manifest=_manifest("ORDERS"),
                              target_catalog="lake", reports=tmp_path,
                              counts=False)
    assert rec["schemas"][0]["tables"][0]["verdict"] == "MIGRATED_VERIFIED"
    assert rec["schemas"][0]["reports_ignored_for_other_catalog"] == {}


def _manifest(*tables, views=()):
    return {"schemas": [{"name": "SALES",
                         "tables": [{"name": t, "columns": []} for t in tables],
                         "views": [{"name": v} for v in views],
                         "errors": []}]}


def test_reconcile_treats_already_existed_like_created_and_flags_type_drift(
        reconcile, tmp_path):
    (tmp_path / "structure_report_sales.json").write_text(json.dumps(
        {"schema": "SALES", "target": "lake.SALES",
         "objects": {"GONE": {"status": "already_existed"},
                     "DRIFT": {"status": "type_drift",
                               "reason": "position 1: planned ID ..."}}}),
        encoding="utf-8")
    (tmp_path / "copy_report_sales.json").write_text(json.dumps(
        {"schema": "SALES", "target": "lake.SALES",
         "tables": {"DRIFT": {"status": "verified"}}}), encoding="utf-8")
    spark = _CatalogSpark({"`lake`.`SALES`.`DRIFT`": [("A", "string")]})
    rec = reconcile.reconcile(spark, manifest=_manifest("GONE", "DRIFT"),
                              target_catalog="lake", reports=tmp_path,
                              counts=False)
    by_name = {t["table"]: t for t in rec["schemas"][0]["tables"]}
    assert by_name["GONE"]["verdict"] == "MISSING_DESPITE_REPORT", \
        "a table the structure report says was there must still be there"
    # The copy verified row counts into a layout the plan did not produce;
    # counts match when data lands in the wrong columns, so this is not a pass.
    assert by_name["DRIFT"]["verdict"] == "STRUCTURE_TYPE_DRIFT"
    assert "STRUCTURE_TYPE_DRIFT" in reconcile.PROBLEM_VERDICTS
    assert "position 1" in by_name["DRIFT"]["reason"]


# --- views: not created by this path, and never omitted from the report ----
#
# The plan carries CREATE VIEW statements and the manifest lists views, but
# the job path creates tables only. Views were absent from every report, so
# an estate whose every view was missing read "No table is in a problem
# state"; and a view that WAS deployed (catalog API) was subtracted only
# against manifest tables and reported as someone else's object.

def test_views_in_the_plan_are_not_counted_as_planned_tables(structure):
    plan = {"statements": [
        {"source_identifier": "DB.SALES.ORDERS", "object_type": "TABLE",
         "expected_columns": [{"name": "ID", "type": "DECIMAL(38,0)"}]},
        {"source_identifier": "DB.SALES.V_ORDERS", "object_type": "VIEW",
         "expected_columns": [{"name": "ID", "type": "DECIMAL(38,0)"}],
         "sql": "CREATE VIEW IF NOT EXISTS `lake`.`sales`.`v_orders` AS SELECT 1"}]}
    assert list(structure.columns_from_ddl_plan(plan)) == [("SALES", "ORDERS")]
    assert structure.views_from_ddl_plan(plan) == {("SALES", "V_ORDERS")}


def test_the_structure_report_lists_manifest_views_it_did_not_create(
        structure, monkeypatch, tmp_path, capsys):
    reports = _write_estate(tmp_path / "reports", {"SALES": ["ORDERS"]},
                            plan={("SALES", "ORDERS"): _PLAN_COLS},
                            views={"SALES": ["V_ORDERS"]})
    spark = _CatalogSpark()
    _inject_spark(monkeypatch, spark)
    rc = _load("01_create_structure").main(
        ["--target-catalog", "lake", "--schema", "SALES",
         "--reports-dir", str(reports)])
    assert rc == 0, "a view the plan does not carry is not a failure"
    report = _report(reports, "structure_report_sales.json")
    assert list(report["objects"]) == ["ORDERS"], "objects stays table-only"
    # --mode ddl-plan creates the plan's views; this one is not in the plan.
    assert report["views"]["V_ORDERS"]["status"] == "not_in_plan"
    assert report["views"]["V_ORDERS"]["in_plan"] is False
    assert not any("VIEW" in s.upper() for s in spark.statements)
    assert "V_ORDERS" in capsys.readouterr().out


def test_a_manifest_view_present_in_target_is_not_someone_elses_object(
        reconcile, tmp_path):
    (tmp_path / "copy_report_sales.json").write_text(json.dumps(
        {"schema": "SALES", "target": "lake.SALES",
         "tables": {"ORDERS": {"status": "verified"}}}), encoding="utf-8")
    spark = _CatalogSpark({"`lake`.`SALES`.`ORDERS`": [("A", "string")],
                           "`lake`.`SALES`.`V_ORDERS`": [("A", "string")]})
    rec = reconcile.reconcile(spark, manifest=_manifest("ORDERS", views=["V_ORDERS"]),
                              target_catalog="lake", reports=tmp_path,
                              counts=False)
    s = rec["schemas"][0]
    assert s["in_target_but_not_in_manifest"] == []
    assert s["tables"][0]["verdict"] == "MIGRATED_VERIFIED"
    assert rec["totals"]["MIGRATED_VERIFIED"] == 1
    view = s["views"][0]
    assert view["view"] == "V_ORDERS"
    assert view["verdict"] == "VIEW_NOT_CREATED_BY_THIS_PATH"
    assert view["exists_in_target"] is True
    md = reconcile.render(rec)
    assert "V_ORDERS" in md
    assert "someone else's objects" not in md


def test_a_manifest_view_absent_from_target_appears_in_the_report(
        reconcile, tmp_path):
    spark = _CatalogSpark({"`lake`.`SALES`.`ORDERS`": [("A", "string")]})
    rec = reconcile.reconcile(spark, manifest=_manifest("ORDERS", views=["V_ORDERS"]),
                              target_catalog="lake", reports=tmp_path,
                              counts=False)
    view = rec["schemas"][0]["views"][0]
    assert view["verdict"] == "VIEW_NOT_CREATED_BY_THIS_PATH"
    # SHOW TABLES lists views on some catalogs only, so "not listed" is not
    # "absent": could not look never renders as no.
    assert view["exists_in_target"] is None
    assert "VIEW_NOT_CREATED_BY_THIS_PATH" not in reconcile.PROBLEM_VERDICTS
    assert rec["totals"]["VIEW_NOT_CREATED_BY_THIS_PATH"] == 1
    md = reconcile.render(rec)
    assert "V_ORDERS" in md
    assert "deploy --execute" in md, "the report says how views DO get created"


def test_views_are_not_counted_as_tables_pending_migration(
        reconcile, monkeypatch, tmp_path, capsys):
    reports = _write_estate(tmp_path / "reports", {"SALES": ["ORDERS"]},
                            views={"SALES": ["V_ORDERS"]})
    (reports / "copy_report_sales.json").write_text(json.dumps(
        {"schema": "SALES", "target": "lake.SALES",
         "tables": {"ORDERS": {"status": "verified"}}}), encoding="utf-8")
    _inject_spark(monkeypatch, _CatalogSpark(
        {"`lake`.`SALES`.`ORDERS`": [("A", "string")]}))
    rc = _load("03_reconcile").main(["--target-catalog", "lake",
                                     "--reports-dir", str(reports)])
    assert rc == 0
    out = capsys.readouterr().out
    assert "not migrated yet" not in out
    assert "V_ORDERS" in (reports / "MIGRATION_REPORT.md").read_text(encoding="utf-8")


@pytest.mark.parametrize("views", [[], ["V_ORDERS"]])
def test_a_report_claim_is_checked_the_same_with_views_present(
        reconcile, tmp_path, views):
    (tmp_path / "copy_report_sales.json").write_text(json.dumps(
        {"schema": "SALES", "target": "lake.sales",
         "tables": {"ORDERS": {"status": "verified"}}}), encoding="utf-8")
    rec = reconcile.reconcile(_FakeSpark(), manifest=_manifest("ORDERS", views=views),
                              target_catalog="lake", reports=tmp_path,
                              counts=False)
    assert rec["schemas"][0]["tables"][0]["verdict"] == "MISSING_DESPITE_REPORT"
    assert rec["totals"]["MISSING_DESPITE_REPORT"] == 1


def test_a_table_with_no_target_is_a_finding_not_a_crash(copy_schema):
    """Live, the copy died on the sixth table of a schema: the approved plan
    covered five, the manifest listed a thousand, and the missing target took
    the whole run with it. A missing target is now recorded and skipped."""
    class NoTarget(_FakeSpark):
        def sql(self, statement):
            if statement.lower().startswith("describe"):
                raise RuntimeError("TABLE_OR_VIEW_NOT_FOUND")
            return super().sql(statement)

    source = _FakeSource()
    source.spark = NoTarget()
    out = copy_schema.copy_table(source, "SALES", "ORDERS",
                                 "`lake`.`s`.`orders`", mode="append",
                                 verify="counts", retries=0, retry_base_delay=0)
    assert out["status"] == "target_missing"
    assert "not in the approved plan" in out["reason"]
    assert not any("INSERT" in s.upper() for s in source.spark.statements)


def test_not_migrated_is_pending_not_a_problem(reconcile):
    """A migration runs schema by schema, so most of the estate is "not
    attempted yet" for most of the project. Exiting non-zero on that would
    make every partial run look broken — and that is how a real signal gets
    ignored."""
    assert "NOT_MIGRATED" not in reconcile.PROBLEM_VERDICTS
    assert "STRUCTURE_ONLY" not in reconcile.PROBLEM_VERDICTS
    for verdict in ("MISSING_DESPITE_REPORT", "STRUCTURE_FAILED",
                    "STRUCTURE_TYPE_DRIFT", "STRUCTURE_ONLY_COPY_FAILED",
                    "COUNT_DRIFT", "TARGET_UNREADABLE"):
        assert verdict in reconcile.PROBLEM_VERDICTS


def test_a_structure_create_that_raised_is_a_problem_not_pending(reconcile,
                                                                 tmp_path):
    """01_create_structure records `failed` when CREATE raised on the cluster.
    That table is absent from the target, but it is not "never attempted" --
    the operator has to act on it -- yet it rendered NOT_MIGRATED, the same
    verdict as an untouched table, under "No table is in a problem state"."""
    (tmp_path / "structure_report_sales.json").write_text(json.dumps(
        {"schema": "SALES", "target": "lake.SALES",
         "objects": {"GOOD": {"status": "created"},
                     "BAD": {"status": "failed",
                             "reason": "DataType decimal(45,2) is not supported"},
                     "SKIPPED": {"status": "not_in_plan",
                                 "reason": "the approved ddl_plan carries no "
                                           "columns for this table"}}}),
        encoding="utf-8")
    spark = _CatalogSpark({"`lake`.`SALES`.`GOOD`": [("A", "string")]})
    rec = reconcile.reconcile(
        spark, manifest=_manifest("GOOD", "BAD", "SKIPPED", "UNTOUCHED"),
        target_catalog="lake", reports=tmp_path, counts=False)
    by_name = {t["table"]: t for t in rec["schemas"][0]["tables"]}
    assert by_name["BAD"]["verdict"] == "STRUCTURE_FAILED"
    assert by_name["BAD"]["reason"].startswith("DataType decimal(45,2)")
    assert by_name["UNTOUCHED"]["verdict"] == "NOT_MIGRATED"
    assert by_name["SKIPPED"]["verdict"] == "NOT_IN_PLAN", \
        "a table the plan leaves out is neither pending nor broken"
    assert "NOT_IN_PLAN" not in reconcile.PROBLEM_VERDICTS
    assert by_name["GOOD"]["verdict"] == "STRUCTURE_ONLY"
    md = reconcile.render(rec)
    assert "1 object(s) need attention" in md
    assert "No table is in a problem state" not in md


# --- reconcile --counts: the live count is compared, not just printed ------
#
# With --counts the live COUNT(*) was fetched and rendered in the row, but the
# verdict came solely from the copy report's `verified`: a table emptied or
# changed out of band after the copy rendered "MIGRATED_VERIFIED (0 rows)"
# under "No table is in a problem state", exit 0.

def _verified_orders(reports, **extra):
    _seeded_copy_report(reports, "ORDERS",
                        {"status": "verified", "source_count": 5,
                         "target_count": 5, **extra})


def _orders_spark(live_count):
    spark = _CatalogSpark({"`lake`.`SALES`.`ORDERS`": [("A", "string")]})
    spark.counts = {"`lake`.`SALES`.`ORDERS`": live_count}
    return spark


@pytest.mark.parametrize("live", [0, 7])
def test_a_verified_table_whose_live_count_drifted_is_a_problem(reconcile,
                                                                tmp_path, live):
    _verified_orders(tmp_path)
    rec = reconcile.reconcile(_orders_spark(live), manifest=_manifest("ORDERS"),
                              target_catalog="lake", reports=tmp_path,
                              counts=True)
    row = rec["schemas"][0]["tables"][0]
    assert row["verdict"] == "COUNT_DRIFT"
    assert "COUNT_DRIFT" in reconcile.PROBLEM_VERDICTS
    assert rec["totals"]["COUNT_DRIFT"] == 1
    assert "5" in row["reason"] and str(live) in row["reason"]
    md = reconcile.render(rec)
    assert "1 object(s) need attention" in md
    assert "No table is in a problem state" not in md


def test_a_verified_table_whose_live_count_matches_stays_verified(reconcile,
                                                                  tmp_path):
    _verified_orders(tmp_path)
    rec = reconcile.reconcile(_orders_spark(5), manifest=_manifest("ORDERS"),
                              target_catalog="lake", reports=tmp_path,
                              counts=True)
    assert rec["schemas"][0]["tables"][0]["verdict"] == "MIGRATED_VERIFIED"
    assert rec["schemas"][0]["tables"][0]["target_count"] == 5
    assert "COUNT_DRIFT" not in rec["totals"]


def test_counts_off_never_compares(reconcile, tmp_path):
    # The default job path is unchanged: no COUNT(*) issued, no comparison.
    _verified_orders(tmp_path)
    spark = _orders_spark(0)
    rec = reconcile.reconcile(spark, manifest=_manifest("ORDERS"),
                              target_catalog="lake", reports=tmp_path,
                              counts=False)
    assert rec["schemas"][0]["tables"][0]["verdict"] == "MIGRATED_VERIFIED"
    assert not any("count(*)" in s.lower() for s in spark.statements)


def test_a_report_without_a_verified_count_is_not_drift(reconcile, tmp_path):
    # A copy report written before counts were recorded has nothing to
    # compare against: no crash, no false alarm.
    _seeded_copy_report(tmp_path, "ORDERS", {"status": "verified"})
    rec = reconcile.reconcile(_orders_spark(0), manifest=_manifest("ORDERS"),
                              target_catalog="lake", reports=tmp_path,
                              counts=True)
    assert rec["schemas"][0]["tables"][0]["verdict"] == "MIGRATED_VERIFIED"


def test_an_unreadable_live_count_is_not_drift(reconcile, tmp_path):
    """Could not look is not the same as wrong."""
    class NoCount(_CatalogSpark):
        def sql(self, statement):
            if "count(*)" in statement.lower():
                raise RuntimeError("denied")
            return super().sql(statement)

    _verified_orders(tmp_path)
    spark = NoCount({"`lake`.`SALES`.`ORDERS`": [("A", "string")]})
    rec = reconcile.reconcile(spark, manifest=_manifest("ORDERS"),
                              target_catalog="lake", reports=tmp_path,
                              counts=True)
    row = rec["schemas"][0]["tables"][0]
    assert row["target_count"] is None
    assert "denied" in row["count_error"]
    assert row["verdict"] == "MIGRATED_VERIFIED"


def test_a_failed_create_whose_table_exists_anyway_is_still_a_problem(
        reconcile, tmp_path):
    # The CREATE raised, yet something by that name is there: nobody has
    # checked its layout. Re-running 01 settles it (it re-checks `failed`).
    (tmp_path / "structure_report_sales.json").write_text(json.dumps(
        {"schema": "SALES", "target": "lake.SALES",
         "objects": {"BAD": {"status": "failed", "reason": "boom"}}}),
        encoding="utf-8")
    spark = _CatalogSpark({"`lake`.`SALES`.`BAD`": [("A", "string")]})
    rec = reconcile.reconcile(spark, manifest=_manifest("BAD"),
                              target_catalog="lake", reports=tmp_path,
                              counts=False)
    assert rec["schemas"][0]["tables"][0]["verdict"] == "STRUCTURE_FAILED"


def test_the_report_says_plainly_when_nothing_is_wrong(reconcile):
    md = reconcile.render({"target_catalog": "lake", "generated_at": "now",
                           "totals": {"MIGRATED_VERIFIED": 5,
                                      "NOT_MIGRATED": 995},
                           "schemas": []})
    assert "No table is in a problem state" in md
    assert "not a failure" in md


def test_the_report_leads_with_the_count_that_needs_attention(reconcile):
    md = reconcile.render({"target_catalog": "lake", "generated_at": "now",
                           "totals": {"MISSING_DESPITE_REPORT": 2,
                                      "MIGRATED_VERIFIED": 1},
                           "schemas": []})
    assert "2 object(s) need attention" in md


def test_views_under_an_unreadable_target_are_not_counted_as_tables(
        reconcile, monkeypatch, tmp_path, capsys):
    """With SHOW TABLES failing, every manifest view is TARGET_UNREADABLE
    beside the tables -- a problem verdict, rightly: nobody could look. The
    headline then counted them as tables, so 1 table + 2 views read
    '3 table(s) need attention'. The count was right; the noun was not."""
    class NoList(_CatalogSpark):
        def sql(self, statement):
            if statement.lower().startswith("show tables"):
                raise RuntimeError("denied")
            return super().sql(statement)

    rec = reconcile.reconcile(NoList(), manifest=_manifest("ORDERS",
                                                           views=["V_A", "V_B"]),
                              target_catalog="lake", reports=tmp_path,
                              counts=False)
    assert rec["totals"] == {"TARGET_UNREADABLE": 3}
    md = reconcile.render(rec)
    assert "3 object(s) need attention" in md
    assert "table(s) need attention" not in md

    reports = _write_estate(tmp_path / "reports", {"SALES": ["ORDERS"]},
                            views={"SALES": ["V_A", "V_B"]})
    _inject_spark(monkeypatch, NoList())
    rc = _load("03_reconcile").main(["--target-catalog", "lake",
                                     "--reports-dir", str(reports)])
    assert rc == 1
    out = capsys.readouterr().out
    assert "3 object(s) in a PROBLEM state" in out
    assert "table(s) in a PROBLEM state" not in out

# --- the source config as it actually arrives on the mount -----------------

@pytest.fixture(scope="module")
def source_helpers():
    return _load("snowmig_source")


def test_the_one_migration_config_is_read_as_uploaded(source_helpers, tmp_path):
    """`provision --source-config` uploads the migration config VERBATIM, and
    that file nests the connection under `snowflake:`. Reading the top level
    for `account` found only the envelope keys, so the cluster reported every
    required field missing at once — which reads like a dead credential, not a
    config one level too deep."""
    cfg = tmp_path / "snowmig-config.yaml"
    cfg.write_text(json.dumps({
        "snowflake": {"account": "ACC", "warehouse": "WH", "database": "DB",
                      "user": "u", "auth": "password", "password": "p"},
        "aidp": {"datalake_ocid": "ocid1.aidataplatform.oc1..x"},
    }), encoding="utf-8")
    loaded = source_helpers.load_source_config(cfg)
    assert loaded["account"] == "ACC"
    assert loaded["auth"] == "password"
    assert "aidp" not in loaded


def test_a_flat_source_config_still_loads(source_helpers, tmp_path):
    """The loader predates the envelope and JSON is the documented fallback
    for a cluster with no PyYAML, so the flat shape stays supported."""
    cfg = tmp_path / "source.json"
    cfg.write_text(json.dumps({"account": "ACC", "warehouse": "WH",
                               "database": "DB", "user": "u",
                               "auth": "password"}), encoding="utf-8")
    assert source_helpers.load_source_config(cfg)["account"] == "ACC"


def test_every_spark_statement_the_stages_emit_parses_as_spark_sql(
        copy_schema, reconcile, monkeypatch, tmp_path):
    """The stages run on a cluster nobody can reach from the suite, so the
    strongest offline check on the SQL they build is a Spark-dialect parser.
    A statement that does not parse is wrong for certain."""
    sqlglot = pytest.importorskip("sqlglot", reason="dev-only SQL parse check")
    # structure: CREATE SCHEMA, DESCRIBE, CREATE TABLE ... USING DELTA
    spark = _CatalogSpark()
    _structure_run(monkeypatch, tmp_path, spark)
    statements = list(spark.statements)
    # copy: DESCRIBE both sides, COUNT(*), INSERT, the decimal SUMs
    typed = _typed([("ORDER_ID", "decimal(38,0)"), ("AMOUNT", "decimal(18,2)"),
                    ("NOTE", "string")])
    _copy_typed(copy_schema, typed)
    typed.counts = {_SRC: 3, _TGT: 3}
    _copy_typed(copy_schema, typed, mode="overwrite")
    statements += typed.statements
    # reconcile: SHOW TABLES, COUNT(*)
    _verified_orders(tmp_path)
    live = _orders_spark(5)
    reconcile.reconcile(live, manifest=_manifest("ORDERS"),
                        target_catalog="lake", reports=tmp_path, counts=True)
    statements += live.statements
    kinds = {s.split()[0].upper() for s in statements}
    assert {"CREATE", "DESCRIBE", "SELECT", "INSERT", "SHOW"} <= kinds, kinds
    for statement in statements:
        assert sqlglot.parse_one(statement, read="spark") is not None, statement


def test_the_committed_notebooks_match_their_sources():
    """The `.ipynb` under data-migration-scripts/ are generated from
    engine/dataplane and committed; the AIDP job runs the notebook, so a fix
    that lands only in the `.py` never reaches the live run. Regenerate with
    `snowmig.py build-notebooks`."""
    sys.path.insert(0, str(SCRIPTS.parent))
    from target.stage_notebooks import STAGES, build_stage_notebook

    shipped = SCRIPTS.parents[1] / "data-migration-scripts"

    def code_cells(nb):
        return ["".join(c["source"]).replace("\r\n", "\n")
                for c in nb["cells"] if c["cell_type"] == "code"]

    for stage in STAGES:
        committed = json.loads((shipped / stage.notebook_name)
                               .read_text(encoding="utf-8"))
        assert code_cells(committed) == code_cells(build_stage_notebook(stage)), (
            f"{stage.notebook_name} is stale against engine/dataplane/"
            f"{stage.source}; run `snowmig.py build-notebooks`")


def test_the_committed_notebooks_use_lf_line_endings():
    """The committed `.ipynb` are LF like the rest of the tree (the repo sets
    no autocrlf). `write_stage_notebooks` writes in text mode, so a
    `build-notebooks` on Windows rewrites every line as CRLF: a whole-file
    diff that buries the real change, and a tree that is no longer LF. The
    content test above cannot see it -- it compares parsed cells -- so the
    bytes are pinned here."""
    sys.path.insert(0, str(SCRIPTS.parent))
    from target.stage_notebooks import STAGES

    shipped = SCRIPTS.parents[1] / "data-migration-scripts"
    for stage in STAGES:
        data = (shipped / stage.notebook_name).read_bytes()
        crlf = data.count(b"\r\n")
        assert crlf == 0, (
            f"{stage.notebook_name} has {crlf} CRLF line ending(s); the tree "
            f"is LF. Rewrite it with LF endings after `build-notebooks`")

def test_the_structure_stage_does_not_ship_a_default_that_cannot_work():
    """`manifest` mode reads types from the discovery manifest, but a manifest
    built in `connector` mode carries SNOWFLAKE types and Delta rejects them
    verbatim. Shipping `source-mode: connector` beside `mode: manifest` meant
    the default pair refused every table on a first run."""
    import sys
    sys.path.insert(0, str(SCRIPTS.parent))
    from target.stage_notebooks import STAGES

    stage = next(s for s in STAGES if s.key == "structure")
    assert stage.params["mode"] == "ddl-plan", stage.params
    if stage.params.get("source-mode") == "connector":
        assert stage.params["mode"] != "manifest", (
            "connector-built manifests carry Snowflake types; this pair "
            "cannot create a Delta table")


# --- discovery: --schemas is a predicate, and a scoped run merges -------------
#
# Two defects with one root: `--schemas` was applied AFTER an unfiltered
# INFORMATION_SCHEMA fetch. So it could not narrow a query that hits
# Snowflake's result cap, and in connector mode the filtered result was
# ASSIGNED over the loaded manifest -- following DISCOVERY.md's own advice
# ("re-run with --force --schemas <name>") deleted every other schema from
# discovery_manifest.json, in both modes.

def _rel(schema, name, rows=1):
    return {"TABLE_SCHEMA": schema, "TABLE_NAME": name, "TABLE_TYPE": "BASE TABLE",
            "ROW_COUNT": rows, "BYTES": 10 * rows}


def _colrow(schema, name):
    return {"TABLE_SCHEMA": schema, "TABLE_NAME": name, "COLUMN_NAME": "ID",
            "ORDINAL_POSITION": 1, "DATA_TYPE": "NUMBER", "IS_NULLABLE": "NO",
            "NUMERIC_PRECISION": 38, "NUMERIC_SCALE": 0,
            "CHARACTER_MAXIMUM_LENGTH": None}


def test_wanted_schemas_are_pushed_down_not_filtered_client_side(discover):
    source = _FakeSource(tables=[_rel("SALES", "ORDERS"), _rel("HR", "EMP")],
                         columns=[_colrow("SALES", "ORDERS"), _colrow("HR", "EMP")])
    discover.discover_via_connector(source, wanted=["SALES"], exclude=set())
    assert len(source.queries) == 2
    for sql in source.queries:
        assert "where TABLE_SCHEMA in ('SALES')" in sql, sql
    # And an unscoped run stays unfiltered: the two-query fast path.
    source = _FakeSource(tables=[_rel("SALES", "ORDERS")],
                         columns=[_colrow("SALES", "ORDERS")])
    discover.discover_via_connector(source, wanted=None, exclude=set())
    assert not any("where TABLE_SCHEMA" in sql for sql in source.queries)


def test_schema_name_with_apostrophe_is_escaped_in_the_predicate(discover):
    source = _FakeSource(tables=[], columns=[])
    discover.discover_via_connector(source, wanted=["O'BRIEN", "HR"], exclude=set())
    assert "where TABLE_SCHEMA in ('O''BRIEN', 'HR')" in source.queries[0]


def test_discovery_summary_says_other_schemas_are_kept(discover):
    md = discover.render_summary({"source": {"mode": "connector"},
                                  "schemas": [], "generated_at": "now"})
    assert "kept" in md
    assert "--force --schemas" not in md, \
        "the old advice, followed literally, truncated the manifest"


# main() end to end, with pyspark and SnowflakeSource stubbed.

class _Estate:
    """Mutable estate the fakes answer from, so a test can change a row count
    or drop a schema between runs."""
    tables = [_rel("HR", "EMP", rows=3), _rel("SALES", "ORDERS", rows=5),
              _rel("FIN", "LEDGER", rows=7)]

    @classmethod
    def columns(cls):
        return [_colrow(t["TABLE_SCHEMA"], t["TABLE_NAME"]) for t in cls.tables]

    @classmethod
    def schemas(cls):
        return sorted({t["TABLE_SCHEMA"] for t in cls.tables})


class _DiscoverySource:  # discovery-only double; the copy/structure tests use _MainSource above
    """Stands in for SnowflakeSource inside main(): honours the schema
    predicate the way Snowflake would."""

    def __init__(self, spark, *, mode, config=None, external_catalog=None,
                 session_schema=None):
        self.spark, self.mode = spark, mode
        self.external_catalog = external_catalog or "ext"
        self.session_schema = session_schema or "PUBLIC"
        self.queries: list[str] = []

    def pushdown(self, sql, schema=None):
        import re
        self.queries.append(sql)
        rows = _Estate.columns() if "COLUMNS" in sql else list(_Estate.tables)
        m = re.search(r"where TABLE_SCHEMA in \((.*?)\)", sql)
        if m:
            wanted = {s.strip()[1:-1].replace("''", "'")
                      for s in m.group(1).split(",")}
            rows = [r for r in rows if r["TABLE_SCHEMA"] in wanted]
        return _FakeDF(rows)

    def describe(self):
        return {"mode": self.mode, "database": "DB", "host": "h", "user": "u",
                "warehouse": "w", "role": "r", "auth": "KeyPair",
                "external_catalog": self.external_catalog,
                "session_schema": self.session_schema}

    def database(self):
        return "DB"


class _DiscoverySpark(_FakeSpark):  # discovery-only double; the structure/copy tests use _CatalogSpark above
    """Answers SHOW SCHEMAS / SHOW TABLES / SHOW VIEWS / DESCRIBE against the
    same estate, for external-catalog mode."""

    def sql(self, statement):
        low = statement.lower()
        if low.startswith("show schemas"):
            return _FakeDF([{"namespace": s} for s in _Estate.schemas()])
        if low.startswith("show tables"):
            schema = statement.rsplit("`", 2)[-2]
            return _FakeDF([{"tableName": t["TABLE_NAME"]} for t in _Estate.tables
                            if t["TABLE_SCHEMA"] == schema])
        if low.startswith("show views"):
            return _FakeDF([])
        return super().sql(statement)


def _stub_pyspark(monkeypatch, spark):
    import types
    pyspark, sql = types.ModuleType("pyspark"), types.ModuleType("pyspark.sql")

    class _Builder:
        @staticmethod
        def getOrCreate():
            return spark

    class SparkSession:
        builder = _Builder()

    sql.SparkSession = SparkSession
    pyspark.sql = sql
    monkeypatch.setitem(sys.modules, "pyspark", pyspark)
    monkeypatch.setitem(sys.modules, "pyspark.sql", sql)


def _manifest_schemas(reports):
    data = json.loads((reports / "discovery_manifest.json").read_text(encoding="utf-8"))
    return {s["name"]: s for s in data["schemas"]}


@pytest.fixture
def estate(monkeypatch, discover):
    monkeypatch.setattr(_Estate, "tables", [_rel("HR", "EMP", rows=3),
                                            _rel("SALES", "ORDERS", rows=5),
                                            _rel("FIN", "LEDGER", rows=7)])
    monkeypatch.setattr(discover, "SnowflakeSource", _DiscoverySource)
    _stub_pyspark(monkeypatch, _DiscoverySpark())
    return _Estate


def test_a_scoped_connector_rerun_keeps_the_other_schemas(discover, estate, tmp_path):
    base = ["--source-mode", "connector", "--reports-dir", str(tmp_path)]
    assert discover.main(base) == 0
    assert sorted(_manifest_schemas(tmp_path)) == ["FIN", "HR", "SALES"]

    estate.tables[1] = _rel("SALES", "ORDERS", rows=500)       # SALES changed
    assert discover.main(base + ["--force", "--schemas", "SALES"]) == 0
    got = _manifest_schemas(tmp_path)
    assert sorted(got) == ["FIN", "HR", "SALES"], "the other schemas are kept"
    assert got["SALES"]["tables"][0]["source_rows"] == 500, "SALES was refreshed"
    assert got["HR"]["tables"][0]["source_rows"] == 3

    estate.tables[1] = _rel("SALES", "ORDERS", rows=501)
    assert discover.main(base + ["--schemas", "SALES"]) == 0   # no --force
    got = _manifest_schemas(tmp_path)
    assert sorted(got) == ["FIN", "HR", "SALES"]
    assert got["SALES"]["tables"][0]["source_rows"] == 501


def test_force_without_schemas_rediscovers_the_whole_estate(discover, estate, tmp_path):
    base = ["--source-mode", "connector", "--reports-dir", str(tmp_path)]
    assert discover.main(base) == 0
    del estate.tables[2]                                         # FIN is gone
    assert discover.main(base + ["--force"]) == 0
    assert sorted(_manifest_schemas(tmp_path)) == ["HR", "SALES"], \
        "a full re-discovery is authoritative and drops what no longer exists"


# A scoped re-run whose named schema returns NOTHING. After a grant
# revocation INFORMATION_SCHEMA simply returns zero rows for SALES -- no
# error -- and the scoped merge dropped every entry named in --schemas and
# added only what came back. Following DISCOVERY.md's own "re-run with
# --schemas <name>" advice therefore deleted SALES and exited 0; reconcile
# went from STRUCTURE_ONLY_COPY_FAILED / exit 1 to exit 0 with SALES absent,
# its failed copy still in the target. A lower-case typo ('sales') exited 0
# too. The run could not look, and it recorded the schema as gone.

def test_a_scoped_rerun_that_sees_nothing_keeps_the_schema_and_fails(
        discover, estate, tmp_path, capsys):
    base = ["--source-mode", "connector", "--reports-dir", str(tmp_path)]
    assert discover.main(base) == 0
    del estate.tables[1]                          # SALES: no longer visible
    capsys.readouterr()
    assert discover.main(base + ["--schemas", "SALES"]) == 1
    out = capsys.readouterr().out
    got = _manifest_schemas(tmp_path)
    assert sorted(got) == ["FIN", "HR", "SALES"], "SALES must not vanish"
    assert got["SALES"]["tables"][0]["name"] == "ORDERS", \
        "the previous discovery of it is kept"
    assert any("returned no rows" in e["error"] and "misspelled" in e["error"]
               for e in got["SALES"]["errors"]), got["SALES"]["errors"]
    assert "SALES" in out and "--force" in out
    # A second failed look does not pile up a second copy of the error.
    assert discover.main(base + ["--schemas", "SALES"]) == 1
    assert len(_manifest_schemas(tmp_path)["SALES"]["errors"]) == 1


def test_force_drops_a_named_schema_that_returns_nothing(discover, estate,
                                                         tmp_path):
    base = ["--source-mode", "connector", "--reports-dir", str(tmp_path)]
    assert discover.main(base) == 0
    del estate.tables[1]
    assert discover.main(base + ["--force", "--schemas", "SALES"]) == 0
    assert sorted(_manifest_schemas(tmp_path)) == ["FIN", "HR"], \
        "--force keeps the deliberate-drop semantics"


def test_a_misspelled_schema_is_a_failure_not_a_silent_success(
        discover, estate, tmp_path, capsys):
    base = ["--source-mode", "connector", "--reports-dir", str(tmp_path)]
    assert discover.main(base) == 0
    capsys.readouterr()
    assert discover.main(base + ["--schemas", "sales"]) == 1
    out = capsys.readouterr().out
    assert "case-sensitive" in out and "SALES" in out, out
    got = _manifest_schemas(tmp_path)
    assert sorted(got) == ["FIN", "HR", "SALES"], "no phantom `sales` entry"
    assert got["SALES"]["errors"] == []


def test_a_scoped_external_catalog_force_keeps_the_other_schemas(discover, estate,
                                                                 tmp_path):
    base = ["--source-mode", "external-catalog", "--source-catalog", "ext",
            "--reports-dir", str(tmp_path)]
    assert discover.main(base) == 0
    assert sorted(_manifest_schemas(tmp_path)) == ["FIN", "HR", "SALES"]
    assert discover.main(base + ["--force", "--schemas", "SALES"]) == 0
    assert sorted(_manifest_schemas(tmp_path)) == ["FIN", "HR", "SALES"]


def test_a_manifest_for_a_different_source_is_refused_even_with_force(discover, estate,
                                                                       tmp_path):
    # The identity guard used to be skipped under --force, which let a --force
    # run silently overwrite another database's manifest.
    (tmp_path / "discovery_manifest.json").write_text(json.dumps(
        {"schemas": [], "source_identity": "OTHER_DB"}), encoding="utf-8")
    rc = discover.main(["--source-mode", "connector", "--reports-dir",
                        str(tmp_path), "--force"])
    assert rc == 1
    data = json.loads((tmp_path / "discovery_manifest.json").read_text(encoding="utf-8"))
    assert data["source_identity"] == "OTHER_DB", "left untouched"


def test_zero_schemas_message_points_at_the_traceback_too(discover, estate, tmp_path,
                                                          monkeypatch, capsys):
    class Broken(_DiscoverySource):
        def pushdown(self, sql, schema=None):
            raise RuntimeError("SQL compilation error: Information schema query "
                               "returned too much data. Please repeat query with "
                               "more selective predicates.")

    monkeypatch.setattr(discover, "SnowflakeSource", Broken)
    rc = discover.main(["--source-mode", "connector", "--reports-dir", str(tmp_path)])
    out = capsys.readouterr().out
    assert rc == 1
    assert "DISCOVERY FAILED" in out and "too much data" in out
    assert "DISCOVERY FAILED above" in out, \
        "the hint must not point only at the credentials"


def test_the_committed_discovery_notebook_matches_its_source():
    """The AIDP job runs the .ipynb, not the .py. A fix to the source that is
    not regenerated into the notebook ships the old behaviour to the cluster
    while every test here passes against the new one."""
    sys.path.insert(0, str(SCRIPTS.parent))
    from target.stage_notebooks import STAGES, build_stage_notebook
    stage = next(s for s in STAGES if s.source == "00_discover_snowflake.py")
    committed = SCRIPTS.parents[1] / "data-migration-scripts" / stage.notebook_name
    generated = build_stage_notebook(stage)
    assert json.loads(committed.read_text(encoding="utf-8")) == generated, \
        "regenerate with `snowmig.py build-notebooks`"


# --- reconcile: a schema this target has not created yet is pending --------
#
# Seen live: schema R3 migrated and verified,
# and reconcile still exited 1 with 22 objects TARGET_UNREADABLE. They were
# the manifest's other schemas, never created in this target yet. `SHOW
# TABLES IN lake.core` raised SCHEMA_NOT_FOUND, and _live_tables read ANY
# exception as "could not look". A schema-by-schema migration -- the
# documented way to run it -- therefore failed reconcile after every schema
# but the last, which is how a real problem signal gets ignored.

class _NoSchemaSpark(_FakeSpark):
    def sql(self, statement):
        if statement.lower().startswith("show tables"):
            raise RuntimeError(
                "[SCHEMA_NOT_FOUND] The schema `lake`.`sales` cannot be found.")
        return super().sql(statement)


def test_a_schema_not_created_yet_is_not_migrated_not_unreadable(reconcile):
    rec = reconcile.reconcile(_NoSchemaSpark(),
                              manifest=_manifest("ORDERS", views=("V",)),
                              target_catalog="lake",
                              reports=pathlib.Path("/nonexistent"),
                              counts=False)
    s = rec["schemas"][0]
    assert s["target_readable"] is True
    assert s["tables"][0]["verdict"] == "NOT_MIGRATED"
    assert s["views"][0]["verdict"] == "VIEW_NOT_CREATED_BY_THIS_PATH"
    assert not set(rec["totals"]) & set(reconcile.PROBLEM_VERDICTS)


def test_a_missing_schema_a_report_says_was_created_is_still_a_problem(
        reconcile, tmp_path):
    """Absent is only pending when nothing claims otherwise."""
    (tmp_path / "structure_report_sales.json").write_text(json.dumps(
        {"schema": "SALES", "target": "lake.SALES",
         "objects": {"ORDERS": {"status": "created"}}}), encoding="utf-8")
    rec = reconcile.reconcile(_NoSchemaSpark(), manifest=_manifest("ORDERS"),
                              target_catalog="lake", reports=tmp_path,
                              counts=False)
    assert rec["schemas"][0]["tables"][0]["verdict"] == "MISSING_DESPITE_REPORT"


def test_reconcile_and_the_copy_agree_on_what_absent_looks_like(
        reconcile, copy_schema):
    """Two scripts, one rule: the markers that make a DESCRIBE "absent" in
    the copy make a SHOW TABLES "absent" here."""
    assert reconcile._NOT_FOUND == copy_schema._NOT_FOUND
