"""S10 creates the objects the approved plan names -- all of them.

Found on a live run: a database planned into a new catalog with
--bronze-catalog-prefix had target keys <catalog>.<db>_<schema>.<table>, but
the structure workflow took only the TYPES from ddl_plan.json and the NAMES
from the manifest, so it created <catalog>.<schema>.<table> -- hundreds of
tables the plan never approved -- and it iterated tables only, so the views
were never created at all.

In ddl-plan mode the plan is the authority for names too: target schema and
table come from each statement's target_fqn, views are created from the
plan's own CREATE VIEW SQL after every table exists, and a statement aimed at
a catalog other than --target-catalog is refused.
"""
import importlib.util
import json
import pathlib
import re
import sys
import types

import pytest

SCRIPTS = pathlib.Path(__file__).resolve().parents[1] / "dataplane"


class _DF:
    def __init__(self, rows=None):
        self._rows = rows or []

    def collect(self):
        return self._rows


class _Spark:
    """Enough of Spark to exercise S10's create-then-verify path.

    The stage DESCRIBEs before creating (absent table -> create) and again
    afterwards (the read-back that must match the plan), so a fake that
    always answers "no columns" reads as a table that exists and is empty,
    which is TYPE DRIFT. This one remembers what it created.
    """

    def __init__(self):
        self.statements = []
        self.tables: dict[str, list[tuple[str, str]]] = {}

    def sql(self, statement):
        s = " ".join(statement.split())
        self.statements.append(s)
        if s.upper().startswith("DESCRIBE "):
            fqn = s.split(None, 1)[1].strip()
            if fqn not in self.tables:
                raise RuntimeError(f"Table or view not found: {fqn}")
            return _DF([{"col_name": n, "data_type": ty}
                        for n, ty in self.tables[fqn]])
        m = re.match(r"(?is)CREATE TABLE IF NOT EXISTS (\S+) \((.*?)\) "
                     r"USING DELTA", s)
        if m:
            cols = []
            # Split on top-level commas only: DECIMAL(38,0) carries one.
            depth, part, parts = 0, "", []
            for ch in m.group(2):
                if ch == "(":
                    depth += 1
                elif ch == ")":
                    depth -= 1
                if ch == "," and depth == 0:
                    parts.append(part); part = ""
                else:
                    part += ch
            parts.append(part)
            for raw in parts:
                bits = raw.strip().split(None, 1)
                if bits:
                    cols.append((bits[0].strip("`"),
                                 bits[1].strip() if len(bits) > 1 else ""))
            self.tables[m.group(1)] = cols
        return _DF()


@pytest.fixture
def run(tmp_path, monkeypatch):
    sys.path.insert(0, str(SCRIPTS))
    spec = importlib.util.spec_from_file_location(
        "snowmig_script_structure_names", SCRIPTS / "01_create_structure.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    mod.DEFAULT_OUTPUT_DIR = ""     # no step files outside tmp_path
    spark = _Spark()
    fake = types.ModuleType("pyspark.sql")
    fake.SparkSession = types.SimpleNamespace(
        builder=types.SimpleNamespace(getOrCreate=lambda: spark))
    monkeypatch.setitem(sys.modules, "pyspark", types.ModuleType("pyspark"))
    monkeypatch.setitem(sys.modules, "pyspark.sql", fake)

    reports = tmp_path / "reports"
    reports.mkdir()
    (tmp_path / "plan").mkdir()
    (reports / "discovery_manifest.json").write_text(json.dumps({"schemas": [
        {"name": "ANALYTICS", "tables": [],
         "views": [{"name": "VW_ORDERS", "columns": []}]},
        {"name": "COMMERCE", "tables": [{"name": "ORDERS", "columns": []},
                                        {"name": "CARTS", "columns": []}],
         "views": []}]}))

    def go(plan, *extra, catalog="target_cat"):
        (tmp_path / "plan" / "ddl_plan.json").write_text(json.dumps(plan))
        code = mod.main(["--target-catalog", catalog, "--reports-dir",
                         str(reports), *extra])
        return code, spark.statements, reports
    return go


P = "target_cat.src_db_commerce"


def _plan():
    return {"statements": [
        {"source_identifier": "DB.COMMERCE.ORDERS", "object_type": "TABLE",
         "target_fqn": f"{P}.orders",
         "expected_columns": [{"name": "ID", "type": "DECIMAL(38,0)"}]},
        {"source_identifier": "DB.COMMERCE.CARTS", "object_type": "TABLE",
         "target_fqn": f"{P}.carts",
         "expected_columns": [{"name": "ID", "type": "DECIMAL(38,0)"}]},
        {"source_identifier": "DB.ANALYTICS.VW_ORDERS", "object_type": "VIEW",
         "target_fqn": "target_cat.src_db_analytics.vw_orders",
         "sql": ("CREATE VIEW IF NOT EXISTS `target_cat`."
                 "`src_db_analytics`.`vw_orders` AS\n"
                 f"SELECT o.ID FROM {P}.orders o"),
         "expected_columns": [{"name": "ID", "type": "DECIMAL(38,0)"}]}]}


def test_tables_land_under_the_plans_target_names(run):
    code, stmts, _ = run(_plan())
    assert code == 0
    tables = [s for s in stmts if s.startswith("CREATE TABLE")]
    assert any("`target_cat`.`src_db_commerce`.`orders`" in s
               for s in tables), tables
    assert not any("`commerce`" in s.lower().replace("src_db_commerce", "")
                   for s in stmts), "no schema named after the SOURCE schema"


def test_the_plans_schemas_are_created(run):
    _, stmts, _ = run(_plan())
    schemas = [s for s in stmts if s.startswith("CREATE SCHEMA")]
    assert "CREATE SCHEMA IF NOT EXISTS `target_cat`.`src_db_commerce`" in schemas
    assert "CREATE SCHEMA IF NOT EXISTS `target_cat`.`src_db_analytics`" in schemas


def test_views_are_created_from_the_plan_after_every_table(run):
    _, stmts, _ = run(_plan())
    views = [i for i, s in enumerate(stmts) if s.startswith("CREATE VIEW")]
    tables = [i for i, s in enumerate(stmts) if s.startswith("CREATE TABLE")]
    assert len(views) == 1 and views[0] > max(tables), \
        "ANALYTICS sorts first but its view reads COMMERCE tables"
    assert f"FROM {P}.orders o" in stmts[views[0]]


def test_the_report_records_views_and_the_target_it_used(run):
    _, _, reports = run(_plan())
    rep = json.loads((reports / "structure_report_analytics.json").read_text())
    # Under `views`, not `objects`: `objects` is the table map the copy
    # takes its scope from, and a view recorded there was copied into.
    assert rep["views"]["VW_ORDERS"]["status"] == "created"
    assert "VW_ORDERS" not in rep["objects"]
    assert rep["target"] == "target_cat.src_db_analytics"
    com = json.loads((reports / "structure_report_commerce.json").read_text())
    assert com["objects"]["ORDERS"]["target_fqn"] == f"{P}.orders"


def test_a_plan_aimed_at_another_catalog_is_refused(run):
    code, stmts, _ = run(_plan(), catalog="some_other_catalog")
    assert code == 1
    assert not any(s.startswith("CREATE TABLE") for s in stmts)


def test_a_view_that_fails_is_a_failure_not_a_silent_skip(run):
    plan = _plan()
    plan["statements"][2]["sql"] = None
    code, _, reports = run(plan)
    rep = json.loads((reports / "structure_report_analytics.json").read_text())
    assert rep["views"]["VW_ORDERS"]["status"] == "failed"
    assert code == 1
