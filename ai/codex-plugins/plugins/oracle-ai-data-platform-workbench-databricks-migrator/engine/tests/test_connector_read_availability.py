"""Connector read inventory, Scylla mirror naming and requirement-plan contracts."""
import json
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
import check_data_availability_for_workflow as checker
from data_check_extract import extract_from_notebook
from build_proactive_data_migration_plan import build_plan, write_excel


PROVIDER = "org.apache.spark.sql.cassandra"


def notebook(*sources):
    return {"cells": [{"cell_type": "code", "source": source} for source in sources]}


@pytest.mark.parametrize("expression", [
    f'spark.read.format("{PROVIDER}").options(keyspace=ks, table=t).load()',
    f'spark.read.format("{PROVIDER}").option("keyspace",ks).option("table",t).load()',
    f'spark.read.format("{PROVIDER}").options(**opts).load()',
    'reader.load()',
    f'spark.read.load(format="{PROVIDER}", keyspace=ks, table=t)',
    f'for t in names:\n    spark.read.format("{PROVIDER}").options(keyspace=ks, table=t).load()',
    f'def read(t):\n    return spark.read.format("{PROVIDER}").options(keyspace=ks, table=t).load()',
])
def test_option_load_is_never_silently_omitted(expression):
    result = extract_from_notebook(notebook(expression))
    assert len(result["unresolved_loads"]) == 1
    assert result["unresolved_scopes"][0]["kind"] == "UNRESOLVED_READ"


def test_inventory_preserves_distinct_same_line_sites_and_ignores_comments():
    result = extract_from_notebook(notebook('# reader.load()\na.load(); b.load()'))
    assert len({site["site_id"] for site in result["unresolved_loads"]}) == 2


def test_python_magic_preserves_physical_line_provenance():
    result = extract_from_notebook(notebook('%python\nreader.load()'))
    assert result["unresolved_loads"][0]["line_number"] == 2
    assert result["unresolved_loads"][0]["site_id"] == "1:2:0"


def test_pending_keyword_load_has_one_finding_instead_of_duplicate_legacy_warning():
    result = extract_from_notebook(notebook('spark.read.load(path=unknown, format="parquet")'))
    assert len(result["unresolved_scopes"]) == 1
    assert not result["unresolved"]


@pytest.mark.parametrize("source_format,source_object,expected", [
    (PROVIDER, "ds_offers_data.offer_keys_mapping", "scylla_ds_offers_data.offer_keys_mapping"),
    (PROVIDER, "KS_1.Table_2", "scylla_KS_1.Table_2"),
    ("jdbc", "sales.orders", None),
    ("cassandra", "ks.orders", None),
    (PROVIDER, "ds_offers_data", None),
    (PROVIDER, "ks.orders.archive", None),
])
def test_only_cassandra_keyspace_table_reads_get_a_mirror_name(source_format, source_object, expected):
    read = {"source_connector": source_format, "source_object": source_object}
    assert checker._external_target_table(read) == expected


def test_scylla_mirror_rule_matches_the_migrator():
    import agent_migrate
    import job_migrate
    prefix, provider = checker.SCYLLA_MIRROR_SCHEMA_PREFIX, checker.CASSANDRA_SOURCE_FORMAT
    rewrite = f'spark.read.table(s"{prefix}$ks.$t")'
    for prompt in (job_migrate.CELL_MIGRATE_PROMPT, agent_migrate.FIX_PROMPT):
        text = " ".join(prompt.split())
        assert f"schema named `{prefix}<keyspace>`" in text
        assert f'spark.read.format("{provider}")' in text
        assert rewrite in text
    scala = (f'val df = spark.read.format("{provider}")'
             '.options(Map("keyspace" -> ks, "table" -> t)).load()')
    assert rewrite in job_migrate._preprocess_scala_cell_source(scala)


def test_unresolved_excel_keeps_structured_reason_and_source(tmp_path):
    from openpyxl import load_workbook
    plan = build_plan({"notebooks": {"/Workspace/n.ipynb": {
        "unresolved_scopes": [{"kind": "UNRESOLVED_READ", "reason": "unknown table",
                               "cell_number": 2, "expression": "reader.load()"}]}}}, "123")
    path = tmp_path / "requirements.xlsx"
    write_excel(path, plan)
    book = load_workbook(path)
    values = list(book["Unresolved References"].values)
    assert "unknown table" in json.dumps(values)
    assert "reader.load()" in json.dumps(values)


def test_unresolved_excel_locates_an_ordinary_filtered_read(tmp_path):
    from openpyxl import load_workbook
    nb = notebook('x = 1', 'df = spark.table("sales.orders").filter("day = " + unknown_day)')
    scopes = extract_from_notebook(nb)["unresolved_scopes"]
    assert scopes and "reference" not in scopes[0]
    plan = build_plan({"notebooks": {"n": {"unresolved_scopes": scopes}}}, "123")
    path = tmp_path / "requirements.xlsx"
    write_excel(path, plan)
    rows = list(load_workbook(path)["Unresolved References"].values)
    row = dict(zip(rows[0], rows[1]))
    assert row["category"] == "UNRESOLVED_SCOPE"
    assert (row["cell_number"], row["line_number"]) == (2, 1)


def test_model_exclusion_keeps_its_category_and_explanation(tmp_path):
    from openpyxl import load_workbook
    # Free-text model reasons must not impersonate another exclusion category.
    plan = build_plan({"notebooks": {"n": {"excluded_reads": [
        {"site_id": "1:1:0", "cell_number": 1, "line_number": 1, "expression": "load()",
         "reason": "WORKFLOW_GENERATED_OUTPUT"}]}}}, "123")
    assert [(row["reason"], row["analysis_reason"]) for row in plan["exclusions"]] == [
        ("NOT_AN_INPUT_READ", "WORKFLOW_GENERATED_OUTPUT")]
    path = tmp_path / "exclusions.xlsx"
    write_excel(path, plan)
    book = load_workbook(path)
    assert book["Excluded Workflow Outputs"].max_row == 1
    rows = list(book["Excluded Read Sites"].values)
    assert dict(zip(rows[0], rows[1]))["analysis_reason"] == "WORKFLOW_GENERATED_OUTPUT"
    book.close()


def test_external_read_is_not_excluded_as_same_named_spark_output():
    report = {"notebooks": {"n": {
        "write_tables": ["ks.input"],
        "read_tables": [{"table": "ks.input", "source_connector": PROVIDER,
                         "source_object": "ks.input", "exists": "unverified"}],
    }}}
    assert len(build_plan(report, "123")["requirements"]) == 1


def test_saved_cassandra_report_keeps_its_mirror_requirement():
    report = {"notebooks": {"n": {
        "write_tables": ["ks.input"],
        "read_tables": [{"table": "ks.input", "source_connector": PROVIDER,
                         "source_object": "ks.input", "target_object": "scylla_ks.input",
                         "exists": False}],
    }}}
    plan = build_plan(report, "123")
    assert [(row["object_name"], row["status"], row["source_connector"])
            for row in plan["requirements"]] == [
        ("scylla_ks.input", "PROACTIVE_REQUIRED", PROVIDER)]
    assert not [row for row in plan["exclusions"]
                if row["reason"] == "WORKFLOW_GENERATED_OUTPUT"]
