"""OPTIMIZE must follow the execution-only writer redirect, never production."""
import sys
from pathlib import Path
from types import ModuleType, SimpleNamespace

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
import job_migrate as jm


@pytest.fixture
def kernel(monkeypatch):
    monkeypatch.setattr(jm, "_REDIRECT_TABLE_PREFIX", "default.test_sandbox")
    monkeypatch.setattr(jm, "_REDIRECT_BUCKET", None)
    monkeypatch.setattr(jm, "_REDIRECT_NAMESPACE", None)
    jm.clear_write_redirect_map()
    tables, calls = set(), []

    class Writer:
        def saveAsTable(self, name, **kwargs):
            tables.add(name)

        def insertInto(self, name, **kwargs):
            tables.add(name)

    class Reader:
        def table(self, name):
            return name

    modules = {name: ModuleType(name) for name in
               ("pyspark", "pyspark.sql", "pyspark.sql.readwriter")}
    modules["pyspark"].sql = modules["pyspark.sql"]
    modules["pyspark.sql.readwriter"].DataFrameWriter = Writer
    modules["pyspark.sql.readwriter"].DataFrameReader = Reader
    for name, module in modules.items():
        monkeypatch.setitem(sys.modules, name, module)
    spark = SimpleNamespace(
        catalog=SimpleNamespace(tableExists=lambda name: name in tables),
        table=lambda name: name,
        sql=lambda query, *a, **kw: calls.append((query, a, kw)),
    )

    def createTable(df, table_name, database_name, **kwargs):
        Writer().saveAsTable(database_name + "." + table_name)

    ns = {"spark": spark, "createTable": createTable, "df": object()}
    exec(jm.get_aidp_topandas_safety_cell(), ns)
    yield ns, tables, calls, Writer
    jm.clear_write_redirect_map()


@pytest.mark.parametrize("literal", [False, True])
def test_incident_write_then_optimize_through_execution_pipeline(kernel, literal):
    ns, tables, calls, _ = kernel
    target = "'scores_day'" if literal else 'f"scores_{day}"'
    source = (
        "day = 'day'\n"
        f"createTable(df, {target}, 'model_db', write_mode='overwrite', write_format='delta')\n"
        'spark.sql(f"optimize model_db.scores_{day}")'
    )
    execution_code = jm._prepare_cell_redirects(source)
    exec(execution_code, ns)
    assert len(tables) == 1
    assert calls == [("optimize " + next(iter(tables)), (), {})]
    # The runtime bootstrap redirects execution, not the delivered notebook.
    assert 'spark.sql(f"optimize model_db.scores_{day}")' in source


@pytest.mark.parametrize("target", ["model_db.scores", "`model_db`.`scores`",
                                    "default.model_db.scores", "scores"])
@pytest.mark.parametrize("prefix", ["OPTIMIZE ", "  optimize\n", "-- maintenance\nOPTIMIZE "])
def test_resolved_target_and_suffix_preserved(kernel, target, prefix):
    ns, tables, calls, writer = kernel
    writer().saveAsTable(target)
    suffix = " WHERE day = '2026-01-01' ZORDER BY (id);"
    ns["spark"].sql(prefix + target + suffix, args={"x": 1})
    assert calls == [(prefix + next(iter(tables)) + suffix, (), {"args": {"x": 1}})]


@pytest.mark.parametrize("catalog_error", [False, True])
def test_missing_sandbox_never_falls_back_to_production(kernel, catalog_error):
    ns, tables, calls, _ = kernel
    tables.add("model_db.scores")
    if catalog_error:
        def broken_catalog(name):
            raise RuntimeError("catalog unavailable")
        ns["spark"].catalog.tableExists = broken_catalog
    with pytest.raises(RuntimeError, match="OPTIMIZE.*model_db.scores"):
        ns["spark"].sql("OPTIMIZE model_db.scores")
    assert not calls


def test_existing_sandbox_and_bootstrap_are_idempotent(kernel):
    ns, tables, calls, _ = kernel
    tables.add("test_sandbox.scores")
    query = "OPTIMIZE `default`.`test_sandbox`.`scores`"
    # Catalog accepts the same qualified form Spark accepts.
    tables.add("`default`.`test_sandbox`.`scores`")
    exec(jm.get_aidp_topandas_safety_cell(), ns)
    ns["spark"].sql(query)
    assert calls == [(query, (), {})]


@pytest.mark.parametrize("query", [
    "SELECT 'OPTIMIZE model_db.scores' AS example",
    "SELECT 1 -- OPTIMIZE model_db.scores",
    "INSERT INTO model_db.scores VALUES (1)",
    "CREATE TABLE model_db.scores (id INT)",
])
def test_unrelated_sql_unchanged(kernel, query):
    ns, tables, calls, _ = kernel
    tables.add("test_sandbox.model_db_scores")
    ns["spark"].sql(query)
    assert calls == [(query, (), {})]


def test_existing_read_behavior_unchanged(kernel):
    ns, tables, calls, _ = kernel
    ns["spark"].sql("SELECT * FROM model_db.scores")
    tables.add("test_sandbox.model_db_scores")
    ns["spark"].sql("SELECT * FROM model_db.scores")
    assert [query for query, _, _ in calls] == [
        "SELECT * FROM model_db.scores", "SELECT * FROM test_sandbox.model_db_scores"]


@pytest.mark.parametrize("query", ["OPTIMIZE delta.`oci://production/path`",
                                  "OPTIMIZE IDENTIFIER(:target)",
                                  "OPTIMIZE `unsupported.name.with.dots`"])
def test_unsupported_optimize_target_fails_closed(kernel, query):
    ns, _, calls, _ = kernel
    with pytest.raises(RuntimeError, match="OPTIMIZE"):
        ns["spark"].sql(query)
    assert not calls
