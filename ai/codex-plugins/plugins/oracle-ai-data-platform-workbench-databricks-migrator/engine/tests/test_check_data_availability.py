"""Test suite for `scripts/data_check_extract.py`.

Covers every historical scanner defect (regression suite) plus general
correctness of the state-machine comment stripper, URL detection, tables,
imports, and variable-vs-literal argument classification.

Runs offline — no cluster, no network.

Usage:
    pytest tests/test_check_data_availability.py -v
    # or standalone:
    python3 tests/test_check_data_availability.py
"""
import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scripts"))
from data_check_extract import (
    strip_comments,
    extract_from_source,
    extract_from_notebook,
    _arg_is_literal_string,
)


# ═══════════════════════ strip_comments ═════════════════════════════
#
# Historical bug #1: `//` inside URLs was stripped, killing `s3://foo`.
# Historical bug #6: triple-quoted string bodies were deleted, losing URLs
# inside `spark.sql(\"\"\"...\"\"\")`.
# Historical bug #7: `#` strip was not quote-aware.

def test_strip_preserves_s3_url_python():
    src = 'x = "s3://examplecorp-dp-unit-rds/rds-and-kafka/example_device_service/"'
    out = strip_comments(src, lang="python")
    assert out == src, f"URL corrupted: {out!r}"


def test_strip_preserves_s3a_url_python():
    src = 'x = "s3a://bucket/path/to/data"'
    assert strip_comments(src, lang="python") == src


def test_strip_preserves_oci_url_python():
    src = 'p = "oci://oci-example-dp-rds@exmplnspace2/rds-and-kafka/example_device_service/"'
    assert strip_comments(src, lang="python") == src


def test_strip_preserves_dbfs_url_python():
    src = 'x = "dbfs://FileStore/jars/foo.jar"'
    assert strip_comments(src, lang="python") == src


def test_strip_preserves_url_in_scala_cell():
    # Even in a Scala cell, `//` inside a string literal must not be stripped.
    src = 'val p = "s3://bucket/path"'
    assert strip_comments(src, lang="scala") == src


def test_strip_scala_line_comment_removed_outside_string():
    src = 'val x = 42 // trailing comment'
    out = strip_comments(src, lang="scala")
    assert "// trailing comment" not in out
    assert "val x = 42" in out


def test_strip_scala_url_then_comment():
    # URL preserved, comment after string removed.
    src = 'val p = "s3://bucket/path" // read from data lake'
    out = strip_comments(src, lang="scala")
    assert '"s3://bucket/path"' in out
    assert "read from data lake" not in out


def test_strip_python_integer_division_NOT_stripped():
    # `//` in Python is integer division, not a comment.
    src = 'total_days = seconds // (24 * 60 * 60)'
    out = strip_comments(src, lang="python")
    assert "// (24" in out, f"Python `//` division destroyed: {out!r}"


def test_strip_python_hash_line_comment():
    src = 'x = 1  # a comment\ny = 2'
    out = strip_comments(src, lang="python")
    assert "# a comment" not in out
    assert "x = 1" in out
    assert "y = 2" in out


def test_strip_hash_inside_string_preserved():
    # Historical bug #7: `token = "abc#def"` used to become `token = "abc`.
    src = 'token = "abc#def"'
    out = strip_comments(src, lang="python")
    assert out == src


def test_strip_hash_inside_string_with_url():
    src = 'endpoint = "https://api.example.com/#/path"'
    out = strip_comments(src, lang="python")
    assert out == src


def test_strip_preserves_triple_quoted_body_with_url():
    # Historical bug #6: URLs inside triple-quoted SQL literals were lost.
    src = '''spark.sql("""SELECT * FROM my.tbl WHERE p='s3://bucket/data/'""")'''
    out = strip_comments(src, lang="python")
    assert "s3://bucket/data/" in out
    assert "SELECT" in out


def test_strip_preserves_triple_quoted_docstring():
    src = 'def f():\n    """docstring with #hash and //slashes and \'quotes\'"""\n    return 1'
    out = strip_comments(src, lang="python")
    assert '"""docstring with #hash' in out
    assert "return 1" in out


def test_strip_scala_block_comment_outside_string():
    src = 'val x = /* block comment */ 42'
    out = strip_comments(src, lang="scala")
    assert "block comment" not in out
    assert "val x =" in out
    assert "42" in out


def test_strip_block_comment_inside_string_preserved():
    src = 'val x = "keep /* this */ intact"'
    out = strip_comments(src, lang="scala")
    assert "/* this */" in out


def test_strip_escaped_quote_in_string():
    # \" inside a double-quoted string doesn't close the string.
    src = r'x = "he said \"hi\" and left"'
    out = strip_comments(src, lang="python")
    assert out == src


def test_strip_empty_and_whitespace():
    assert strip_comments("", lang="python") == ""
    assert strip_comments("   \n\n  ", lang="python") == "   \n\n  "


def test_strip_sql_dash_dash_comment():
    src = "SELECT * FROM t -- this is a comment\nWHERE x=1"
    out = strip_comments(src, lang="sql")
    assert "this is a comment" not in out
    assert "WHERE x=1" in out


# ═══════════════════════ URL literal capture ═══════════════════════════

def test_extract_captures_s3_url():
    out = extract_from_source('x = "s3://examplecorp-dp-unit-rds/rds-and-kafka/"')
    assert "s3://examplecorp-dp-unit-rds/rds-and-kafka/" in out["paths"]


def test_extract_captures_s3a_url():
    out = extract_from_source('x = "s3a://bucket/path/"')
    assert "s3a://bucket/path/" in out["paths"]


def test_extract_captures_oci_url():
    out = extract_from_source('x = "oci://oci-example-dp-rds@exmplnspace2/rds/"')
    assert "oci://oci-example-dp-rds@exmplnspace2/rds/" in out["paths"]


def test_extract_captures_dbfs_url():
    out = extract_from_source('x = "dbfs:/FileStore/jars/foo.jar"')
    assert any("dbfs:/FileStore/jars" in p for p in out["paths"])


def test_extract_captures_mnt_path():
    out = extract_from_source('x = "/mnt/data/foo"')
    assert any(p.startswith("/mnt/data") for p in out["paths"])


def test_extract_captures_volumes_path():
    out = extract_from_source('x = "/Volumes/main/schema/vol/foo"')
    assert any(p.startswith("/Volumes/main") for p in out["paths"])


# ═══════════════════════ f-string URL (unresolved) ══════════════════════

def test_extract_fstring_url_variable_reported_unresolved():
    src = 'x = f"s3://{bucket}/data/{prefix}"'
    out = extract_from_source(src)
    assert any("{bucket}" in u for u in out["unresolved"])


def test_extract_fstring_r_prefix_url_reported():
    # Historical bug #8: `fr"..."` misclassified.
    src = 'x = fr"s3://{bucket}/data/"'
    out = extract_from_source(src)
    assert any("{bucket}" in u for u in out["unresolved"])


def test_extract_rf_prefix_url_reported():
    src = 'x = rf"s3://{bucket}/data/"'
    out = extract_from_source(src)
    assert any("{bucket}" in u for u in out["unresolved"])


def test_extract_capital_F_fstring_reported():
    src = 'x = F"s3://{bucket}/data/"'
    out = extract_from_source(src)
    assert any("{bucket}" in u for u in out["unresolved"])


def test_extract_does_not_report_dynamic_partition_prefix_as_concrete_path():
    src = (
        'load_paths = [f"s3://examplecorp-sa-events-data/event-data/'
        'customer_device_location_event/data/load_date={x}/" for x in dates]'
    )
    out = extract_from_source(src)
    assert out["paths"] == []
    assert any("load_date={x}" in value for value in out["unresolved"])


def test_extract_does_not_report_dynamic_bucket_key_prefix_as_concrete_path():
    src = (
        'path = f"s3://examplecorp-store-alpha/user-{first_char}/'
        'upi-scratch-card/recent/{userId}.snappy.avro"'
    )
    out = extract_from_source(src)
    assert out["paths"] == []
    assert any("user-{first_char}" in value for value in out["unresolved"])


def test_extract_does_not_report_concatenated_url_prefix_as_concrete_path():
    src = (
        'currentPathForUserId = "s3://examplecorp-store-alpha/user-" + '
        'userId[0] + "/upi/recent/" + userId + ".snappy.avro"\n'
        'df = spark.read.format("avro").load(currentPathForUserId)'
    )
    out = extract_from_source(src)
    assert out["paths"] == []
    assert any("currentPathForUserId" in value for value in out["unresolved"])


# ═══════════════════════ spark.read variable-based (Bug #2) ═════════════

def test_extract_spark_read_load_var():
    src = 'df = spark.read.load(CUST_DEVICE_PATH)'
    out = extract_from_source(src)
    reads = out["spark_reads"]
    assert len(reads) == 1 and reads[0]["api"] == "load"
    assert reads[0]["arg_is_literal"] is False
    assert any("spark.read.load(CUST_DEVICE_PATH)" in u for u in out["unresolved"])


def test_extract_spark_read_parquet_var():
    src = 'df = spark.read.parquet(MIGRATED_DEVICES_PATH)'
    out = extract_from_source(src)
    assert any(r["api"] == "parquet" and not r["arg_is_literal"] for r in out["spark_reads"])
    assert any("spark.read.parquet(MIGRATED_DEVICES_PATH)" in u for u in out["unresolved"])


def test_extract_spark_read_json_var():
    src = 'df = spark.read.json(path)'
    out = extract_from_source(src)
    assert any(r["api"] == "json" and not r["arg_is_literal"] for r in out["spark_reads"])


def test_extract_spark_read_csv_var():
    src = 'df = spark.read.csv(path_var)'
    out = extract_from_source(src)
    assert any(r["api"] == "csv" and not r["arg_is_literal"] for r in out["spark_reads"])


def test_extract_spark_read_orc_var():
    out = extract_from_source('df = spark.read.orc(p)')
    assert any(r["api"] == "orc" and not r["arg_is_literal"] for r in out["spark_reads"])


def test_extract_spark_read_text_var():
    out = extract_from_source('df = spark.read.text(p)')
    assert any(r["api"] == "text" and not r["arg_is_literal"] for r in out["spark_reads"])


def test_extract_spark_read_table_var():
    out = extract_from_source('df = spark.read.table(tbl_name)')
    reads = out["spark_reads"]
    assert any(r["api"] == "table" for r in reads)
    # Variable arg → unresolved
    assert any("spark.read.table" in u for u in out["unresolved"])


def test_extract_spark_read_format_chained_load():
    # Historical bug #4: `.format(...).load(var)` chain didn't match.
    src = 'df = spark.read.format("org.apache.hudi").load(cust_device_path)'
    out = extract_from_source(src)
    reads = out["spark_reads"]
    assert any(r["api"] == "load" and not r["arg_is_literal"] for r in reads)
    assert any("cust_device_path" in u for u in out["unresolved"])


def test_extract_spark_read_format_option_chained_load():
    src = 'df = spark.read.format("parquet").option("k","v").option("k2","v2").load(path)'
    out = extract_from_source(src)
    assert any(r["api"] == "load" and not r["arg_is_literal"] for r in out["spark_reads"])


def test_extract_spark_read_delta_var():
    src = 'df = spark.read.delta(delta_path)'
    out = extract_from_source(src)
    assert any(r["api"] == "delta" and not r["arg_is_literal"] for r in out["spark_reads"])


def test_extract_spark_read_literal_string_not_unresolved():
    src = 'df = spark.read.parquet("s3://bucket/path")'
    out = extract_from_source(src)
    reads = out["spark_reads"]
    assert any(r["api"] == "parquet" and r["arg_is_literal"] for r in reads)
    # arg_is_literal=True ⇒ should NOT be added to unresolved
    assert not any("spark.read.parquet" in u for u in out["unresolved"])


# ═══════════════════════ .write variable-based (Bug #5) ═════════════════

def test_extract_write_mode_parquet_var():
    # Historical bug #5: `.write.mode("overwrite").parquet(path)` — the
    # dominant PySpark write pattern — didn't match at all.
    src = 'df.write.mode("overwrite").parquet(output_path)'
    out = extract_from_source(src)
    writes = out["spark_writes"]
    assert any(w["api"] == "parquet" and not w["arg_is_literal"] for w in writes)
    assert any("output_path" in u for u in out["unresolved"])


def test_extract_write_partitionBy_parquet_var():
    src = 'df.write.mode("append").partitionBy("dt").parquet(path)'
    out = extract_from_source(src)
    assert any(w["api"] == "parquet" and not w["arg_is_literal"] for w in out["spark_writes"])


def test_extract_write_format_delta_save_var():
    src = 'df.write.format("delta").mode("overwrite").save(delta_path)'
    out = extract_from_source(src)
    assert any(w["api"] == "save" and not w["arg_is_literal"] for w in out["spark_writes"])


def test_extract_write_saveAsTable_var():
    src = 'df.write.mode("overwrite").saveAsTable(table_name)'
    out = extract_from_source(src)
    assert any(w["api"] == "saveAsTable" and not w["arg_is_literal"] for w in out["spark_writes"])


def test_extract_write_json_var():
    out = extract_from_source('df.write.json(path)')
    assert any(w["api"] == "json" and not w["arg_is_literal"] for w in out["spark_writes"])


def test_extract_write_insertInto_var():
    out = extract_from_source('df.write.insertInto(tbl)')
    assert any(w["api"] == "insertInto" and not w["arg_is_literal"] for w in out["spark_writes"])


def test_extract_write_literal_not_unresolved():
    src = 'df.write.parquet("/mnt/output/foo")'
    out = extract_from_source(src)
    assert any(w["api"] == "parquet" and w["arg_is_literal"] for w in out["spark_writes"])


# ═══════════════════════ boto3.client('s3') (Bug #3) ════════════════════

def test_extract_boto3_client_s3_double_quote():
    src = 'client = boto3.client("s3")'
    out = extract_from_source(src)
    assert len(out["boto3_s3"]) == 1
    assert "boto3.client" in out["boto3_s3"][0]


def test_extract_boto3_client_s3_single_quote():
    src = "client = boto3.client('s3')"
    out = extract_from_source(src)
    assert len(out["boto3_s3"]) == 1


def test_extract_boto3_resource_s3():
    src = 'res = boto3.resource("s3")'
    out = extract_from_source(src)
    assert len(out["boto3_s3"]) == 1


def test_extract_boto3_s3_with_kwargs():
    src = 'client = boto3.client("s3", region_name="us-east-1")'
    out = extract_from_source(src)
    assert len(out["boto3_s3"]) == 1


def test_extract_boto3_non_s3_ignored():
    src = 'client = boto3.client("dynamodb")'
    out = extract_from_source(src)
    assert not out["boto3_s3"]


# ═══════════════════════ _arg_is_literal_string ═════════════════════════

def test_arg_literal_plain_double():
    assert _arg_is_literal_string('"foo"') is True


def test_arg_literal_plain_single():
    assert _arg_is_literal_string("'foo'") is True


def test_arg_literal_f_prefix():
    assert _arg_is_literal_string('f"foo"') is True
    assert _arg_is_literal_string("f'foo'") is True


def test_arg_literal_capital_F():
    assert _arg_is_literal_string('F"foo"') is True


def test_arg_literal_r_prefix():
    assert _arg_is_literal_string('r"foo"') is True
    assert _arg_is_literal_string('R"foo"') is True


def test_arg_literal_b_prefix():
    assert _arg_is_literal_string('b"foo"') is True
    assert _arg_is_literal_string('B"foo"') is True


def test_arg_literal_fr_prefix():
    # Historical bug #9: fr / rf / Fr / Rf misclassified.
    assert _arg_is_literal_string('fr"foo"') is True
    assert _arg_is_literal_string('rf"foo"') is True
    assert _arg_is_literal_string('Fr"foo"') is True
    assert _arg_is_literal_string('Rf"foo"') is True


def test_arg_literal_br_prefix():
    assert _arg_is_literal_string('br"foo"') is True
    assert _arg_is_literal_string('rb"foo"') is True


def test_arg_variable_not_literal():
    assert _arg_is_literal_string('CUST_DEVICE_PATH') is False
    assert _arg_is_literal_string('my_var') is False
    assert _arg_is_literal_string('a + b') is False
    assert _arg_is_literal_string('') is False


# ═══════════════════════ tables (regression, unchanged behaviour) ═══════

def test_extract_table_from_sql_from():
    src = 'df = spark.sql("SELECT * FROM db.tbl WHERE 1=1")'
    out = extract_from_source(src)
    assert "db.tbl" in out["tables"]


def test_extract_table_from_spark_table_literal():
    src = 'df = spark.table("schema.mytable")'
    out = extract_from_source(src)
    assert "schema.mytable" in out["tables"]


def test_extract_table_saveAsTable_literal():
    src = 'df.write.saveAsTable("out_schema.out_table")'
    out = extract_from_source(src)
    assert "out_schema.out_table" in out["tables"]


def test_extract_table_insertInto_literal():
    src = 'df.write.insertInto("db.tbl")'
    out = extract_from_source(src)
    assert "db.tbl" in out["tables"]


def test_extract_table_ignores_python_import_from():
    src = 'from db.tbl import x'
    out = extract_from_source(src)
    assert "db.tbl" not in out["tables"]


def test_extract_table_ignores_dotted_lhs_lookup():
    src = 'np.array([1,2,3])\nspark.conf.set(...)'
    out = extract_from_source(src)
    assert not out["tables"]


# ═══════════════════════ extract_from_notebook aggregation ══════════════

def _mk_nb(cells, default_lang="python"):
    return {
        "metadata": {"language_info": {"name": default_lang}},
        "cells": [{"cell_type": "code", "source": [c]} for c in cells],
    }


def test_nb_tuple_keyed_formatting_does_not_block_later_data_reads():
    # Phase 1 failed on this valid report-card setting before reaching reads.
    nb = _mk_nb([
        """CARD_MERGE = {
    ('Month', ''): [('Month', '')],
    ('Band', ''): [('Month', ''), ('Band', '')],
    ('Allocation Date', ''): [('Month', ''), ('Band', '')],
    ('Allocation BPS', 'Total'): [('Month', ''), ('Band', '')],
}""",
        'input_path = dbutils.widgets.get("input_path")',
        'spark.read.parquet(input_path).count()',
    ])
    expected = "s3://example-bucket/input/daily"

    out = extract_from_notebook(nb, {"input_path": expected})

    assert not out["analysis_errors"]
    assert any(ref["object_name"] == expected for ref in out["read_references"])


@pytest.mark.parametrize("key", ["('daily', '')", "(('daily', ''), 1)", "('daily',)", "()"])
@pytest.mark.parametrize("lookup", ["inputs[key][0]", "inputs.get(key)[0]"])
def test_nb_resolves_tuple_keyed_dictionary_reads_across_cells(key, lookup):
    nb = _mk_nb([
        f"key = {key}",
        'inputs = {key: [dbutils.widgets.get("input_path")]}',
        f"input_path = {lookup}",
        'spark.read.parquet(input_path).count()',
    ])
    expected = "s3://example-bucket/input/daily"

    out = extract_from_notebook(nb, {"input_path": expected})

    assert any(ref["object_name"] == expected for ref in out["read_references"])
    assert not out["unresolved_scopes"]


@pytest.mark.parametrize("sequence", ["(missing_path,)", "[missing_path]"])
def test_nb_unknown_sequence_element_keeps_data_read_unresolved(sequence):
    nb = _mk_nb([
        f"inputs = {sequence}",
        'input_path = inputs[0]',
        'spark.read.parquet(input_path).count()',
    ])

    out = extract_from_notebook(nb)

    assert not out["read_references"]
    assert any(ref["argument_expression"] == "input_path" for ref in out["unresolved_scopes"])


@pytest.mark.parametrize("prefixes, sequence", [([], '[input_path]'), ((), '(input_path,)')])
def test_nb_sequence_type_matches_initial_symbols(prefixes, sequence):
    nb = _mk_nb([
        'input_path = dbutils.widgets.get("input_path")',
        f"inputs = prefixes + {sequence}",
        'spark.read.parquet(inputs[0]).count()',
    ])
    expected = "s3://example-bucket/input/daily"

    out = extract_from_notebook(
        nb, {"input_path": expected}, initial_symbols={"prefixes": prefixes},
    )

    assert any(ref["object_name"] == expected for ref in out["read_references"])
    assert not out["unresolved_scopes"]


def test_nb_deduplicates_paths_across_cells():
    nb = _mk_nb([
        'p1 = "s3://bucket/a"',
        'p2 = "s3://bucket/a"',  # duplicate
    ])
    out = extract_from_notebook(nb)
    assert out["paths"] == ["s3://bucket/a"]


def test_nb_captures_variable_read_across_cells():
    nb = _mk_nb([
        'CUST_PATH = "s3://x/y"',
        'df = spark.read.parquet(CUST_PATH)',
    ])
    out = extract_from_notebook(nb)
    assert "s3://x/y" in out["paths"]
    assert any("CUST_PATH" in u for u in out["unresolved"])


def test_nb_captures_workspace_open_from_aidp_widget_parameter():
    nb = _mk_nb([
        'exrisk_scaling_root = aidputils.widgets.get("model_scaling_files_path").strip()',
        'with open(f"{exrisk_scaling_root}/scaling_files/Feature_List.csv") as f:\n    payload = f.read()',
    ])
    expected = (
        "/Workspace/Users/example/EXRISK/DL V2/artifacts/"
        "scaling_files/Feature_List.csv"
    )
    out = extract_from_notebook(nb, {
        "model_scaling_files_path":
            "/Workspace/Users/example/EXRISK/DL V2/artifacts",
    })
    assert expected in out["paths"]
    ref = next(r for r in out["read_references"] if r["object_name"] == expected)
    assert ref["api"] == "open"
    assert ref["usage"] == "read"


def test_nb_captures_absolute_workspace_pandas_read():
    nb = _mk_nb([
        'df = pd.read_csv("/Workspace/project/config/Feature_List.csv")',
    ])
    out = extract_from_notebook(nb)
    ref = next(
        r for r in out["read_references"]
        if r["object_name"] == "/Workspace/project/config/Feature_List.csv"
    )
    assert ref["api"] == "pd.read_csv"


def test_nb_does_not_classify_write_only_open_as_read_dependency():
    nb = _mk_nb([
        'with open("/Workspace/project/output.json", "w") as f:\n    f.write("{}")',
    ])
    out = extract_from_notebook(nb)
    assert "/Workspace/project/output.json" not in out["paths"]


def test_nb_captures_mlflow_client_artifacts_from_runtime_run_id():
    nb = _mk_nb([
        "from mlflow.tracking import MlflowClient",
        "client = MlflowClient()",
        'mlflow_run_id = aidputils.widgets.get("mlflow_run_id").strip()',
        '''model_folder_path = client.download_artifacts(
    mlflow_run_id, "model_files/model_folder_path.csv", artifact_dst
)
client.download_artifacts(mlflow_run_id, "scaling_files", artifact_dst)''',
    ])
    run_id = "00000000000000000000000000000abc"
    out = extract_from_notebook(nb, {"mlflow_run_id": run_id})
    by_uri = {
        artifact["artifact_uri"]: artifact
        for artifact in out["mlflow_artifacts"]
    }
    file_uri = f"runs:/{run_id}/model_files/model_folder_path.csv"
    directory_uri = f"runs:/{run_id}/scaling_files"
    assert by_uri[file_uri]["object_type"] == "FILE"
    assert by_uri[file_uri]["artifact_path"] == "model_files/model_folder_path.csv"
    assert by_uri[directory_uri]["object_type"] == "DIRECTORY"
    assert by_uri[directory_uri]["run_id"] == run_id


def test_nb_captures_mlflow_artifact_uri_and_model_loader():
    nb = _mk_nb([
        'payload = mlflow.artifacts.load_dict("runs:/run-123/config/model.json")',
        'model = mlflow.pyfunc.load_model("models:/credit-risk@champion")',
    ])
    out = extract_from_notebook(nb)
    by_uri = {
        artifact["artifact_uri"]: artifact
        for artifact in out["mlflow_artifacts"]
    }
    assert by_uri["runs:/run-123/config/model.json"]["object_type"] == "FILE"
    assert by_uri["models:/credit-risk@champion"]["object_type"] == "DIRECTORY"


def test_nb_reports_unresolved_mlflow_artifact_parameters():
    nb = _mk_nb([
        "from mlflow.tracking import MlflowClient",
        "client = MlflowClient()",
        'client.download_artifacts(dynamic_run_id, "scaling_files", artifact_dst)',
    ])
    out = extract_from_notebook(nb)
    assert not out["mlflow_artifacts"]
    assert len(out["unresolved_mlflow_artifacts"]) == 1
    assert out["unresolved_mlflow_artifacts"][0]["run_id_expression"] == "dynamic_run_id"


def test_nb_does_not_track_none_mlflow_run_id_from_f_string():
    nb = _mk_nb([
        "latest_run_id = None",
        'mlflow.artifacts.download_artifacts(f"runs:/{latest_run_id}/model_files")',
    ])
    out = extract_from_notebook(nb)
    assert not out["mlflow_artifacts"]
    assert len(out["unresolved_mlflow_artifacts"]) == 1
    assert out["unresolved_mlflow_artifacts"][0]["artifact_uri"] == "runs:/None/model_files"


def test_nb_resolves_runtime_month_filter_to_partition_scope():
    nb = _mk_nb([
        """from datetime import datetime
from dateutil.relativedelta import relativedelta
today = datetime.today()
current_month_start = today.replace(day=1)
msd = current_month_start.strftime(\"%Y-%m-%d\")""",
        """agents_path = \"s3://bucket/agents\"
agents = spark.read.format(\"hudi\").load(agents_path).filter(col(\"month_start_date\") == msd)""",
    ])
    out = extract_from_notebook(nb, {"execution_date_ist": "2026_08_10"})
    ref = next(r for r in out["read_references"] if r["object_name"] == "s3://bucket/agents")
    assert ref["filters"] == ["month_start_date = '2026-08-01'"]
    assert ref["partition_column"] == "month_start_date"
    assert ref["partition_value"] == "2026-08-01"
    assert ref["source_format"] == "hudi"
    assert ref["filter_resolution"]["runtime_parameter"] == "execution_date_ist"


def test_known_false_dbfs_guard_does_not_corrupt_aidp_volume_path():
    correct_path = (
        "/Volumes/default/default/dbfs/FileStore/user4/exrisk_revamp/"
        "ace_ios_plus_repeat_scoring_tof_base_v2.csv"
    )
    nb = _mk_nb([
        'decision_path = aidputils.widgets.get("decision_path").strip()',
        '''if decision_path.startswith("/dbfs/"):
    decision_path = decision_path.replace("/dbfs/", "dbfs:/")
base = spark.read.csv(decision_path, header=True)''',
    ])

    out = extract_from_notebook(nb, {"decision_path": correct_path})

    paths = {
        ref["object_name"]
        for ref in out["read_references"]
        if ref["object_type"] == "path"
    }
    assert correct_path in paths
    assert not any("defaultdbfs:" in path for path in paths)
    assert out["resolved_symbols"]["decision_path"] == correct_path


def test_nb_resolves_contains_runtime_month_filter_to_partition_scope():
    nb = _mk_nb([
        """from datetime import datetime
today = datetime.today()
msd = today.replace(day=1).strftime("%Y-%m-%d")""",
        """base_path = "s3://bucket/data/{2025,2026}/*/*.parquet"
df = spark.read.parquet(base_path).filter(col("month_start_date").contains(msd))""",
    ])
    out = extract_from_notebook(nb, {"execution_date_ist": "2026_08_10"})
    ref = next(
        r for r in out["read_references"]
        if r["object_name"] == "s3://bucket/data/{2025,2026}/*/*.parquet"
    )
    assert ref["filters"] == ["month_start_date LIKE '%2026-08-01%'"]
    assert ref["partition_column"] == "month_start_date"
    assert ref["partition_value"] == "2026-08-01"


def test_nb_captures_table_date_filter_across_cells():
    nb = _mk_nb([
        'execution_date_ist = getArgument("execution_date_ist")',
        'base = spark.read.table("collections.daily_base")',
        'scoped = base.where(F.col("run_date") == execution_date_ist)',
    ])
    out = extract_from_notebook(nb, {"execution_date_ist": "2026-08-10"})
    ref = next(r for r in out["read_references"] if r["object_name"] == "collections.daily_base")
    assert ref["filters"] == ["run_date = '2026-08-10'"]
    assert ref["partition_column"] == "run_date"
    assert ref["partition_value"] == "2026-08-10"


def test_terminal_chain_records_runtime_resolved_table_read():
    nb = _mk_nb([
        'env = dbutils.widgets.getArgument("env")',
        'spark.read.table(f"collections.{env}").count()',
    ])

    out = extract_from_notebook(nb, {"env": "prod"})

    refs = [
        ref for ref in out["read_references"]
        if ref.get("object_name") == "collections.prod"
    ]
    assert len(refs) == 1
    assert not refs[0].get("filters")
    assert not out["unresolved_scopes"]


def test_terminal_chain_records_runtime_resolved_path_read():
    nb = _mk_nb([
        'env = dbutils.widgets.getArgument("env")',
        'path = f"s3://example-bucket/{env}/input"',
        'spark.read.parquet(path).count()',
    ])

    out = extract_from_notebook(nb, {"env": "prod"})

    assert any(
        ref.get("object_name") == "s3://example-bucket/prod/input"
        for ref in out["read_references"]
    )


def test_terminal_chain_preserves_resolved_filter_scope():
    nb = _mk_nb([
        'run_date = dbutils.widgets.getArgument("run_date")',
        '''spark.read.table("collections.daily").filter(
    F.col("run_date") == run_date
).count()''',
    ])

    out = extract_from_notebook(nb, {"run_date": "2026-08-10"})

    ref = next(
        item for item in out["read_references"]
        if item.get("object_name") == "collections.daily"
    )
    assert ref["filters"] == ["run_date = '2026-08-10'"]
    assert ref["migration_filters"] == ["run_date = '2026-08-10'"]


def test_unresolved_filter_through_lineage_remains_unresolved():
    nb = _mk_nb([
        '''df = spark.table("collections.daily").filter(
    F.col("run_date") == run_date
).select("customer_id")''',
    ])

    out = extract_from_notebook(nb)

    assert not any(
        ref.get("object_name") == "collections.daily"
        for ref in out["read_references"]
    )
    unresolved = next(
        ref for ref in out["unresolved_scopes"]
        if ref.get("object_name") == "collections.daily"
    )
    assert unresolved["filter_resolution"]["status"] == "unresolved"
    assert unresolved["filters"] == ["F.col('run_date') == run_date"]


def test_dual_use_source_with_terminal_consumer_requires_full_migration():
    notebook_path = "/Workspace/root.ipynb"
    nb = _mk_nb([
        'table_name = "collections.daily"',
        'df = spark.read.table(table_name)',
        'scoped = df.filter(F.col("run_date") == "2026-08-10")',
        'df.count()',
    ])

    extracted = extract_from_notebook(nb)
    classified = asyncio.run(classify_refs_on_cluster(
        None, [notebook_path], {notebook_path: extracted},
    ))

    entries = classified[notebook_path]["tables"]
    source = next(entry for entry in entries if entry["table"] == "collections.daily")
    assert not source.get("filters")
    assert source["observed_filters"] == ["run_date = '2026-08-10'"]


def test_dual_use_source_with_write_consumer_requires_full_migration():
    notebook_path = "/Workspace/root.ipynb"
    nb = _mk_nb([
        'table_name = "collections.daily"',
        'df = spark.read.table(table_name)',
        'scoped = df.filter(F.col("run_date") == "2026-08-10")',
        'df.write.mode("overwrite").saveAsTable("collections.output")',
    ])

    extracted = extract_from_notebook(nb)
    classified = asyncio.run(classify_refs_on_cluster(
        None, [notebook_path], {notebook_path: extracted},
    ))

    source = next(
        entry for entry in classified[notebook_path]["tables"]
        if entry["table"] == "collections.daily"
    )
    assert not source.get("filters")
    assert source["observed_filters"] == ["run_date = '2026-08-10'"]


def _classify_dual_use(final_statement):
    notebook_path = "/Workspace/root.ipynb"
    nb = _mk_nb([
        'df = spark.read.table("collections.daily")',
        'scoped = df.filter(F.col("run_date") == "2026-08-10")',
        final_statement,
    ])
    extracted = extract_from_notebook(nb)
    classified = asyncio.run(classify_refs_on_cluster(
        None, [notebook_path], {notebook_path: extracted},
    ))
    return next(
        entry for entry in classified[notebook_path]["tables"]
        if entry["table"] == "collections.daily"
    )


def test_dual_use_print_wrapped_consumer_requires_full_migration():
    source = _classify_dual_use('print(df.count())')
    assert not source.get("filters")
    assert source["observed_filters"] == ["run_date = '2026-08-10'"]


def test_dual_use_display_function_consumer_requires_full_migration():
    source = _classify_dual_use('display(df)')
    assert not source.get("filters")
    assert source["observed_filters"] == ["run_date = '2026-08-10'"]


def test_dual_use_condition_consumer_requires_full_migration():
    source = _classify_dual_use('if df.count() > 0:\n    print("rows")')
    assert not source.get("filters")
    assert source["observed_filters"] == ["run_date = '2026-08-10'"]


def test_dual_use_bare_expression_consumer_requires_full_migration():
    source = _classify_dual_use('df')
    assert not source.get("filters")
    assert source["observed_filters"] == ["run_date = '2026-08-10'"]


def test_metadata_only_consumer_keeps_partition_scope():
    source = _classify_dual_use('df.printSchema()')
    assert source.get("filters") == ["run_date = '2026-08-10'"]


def test_lineage_preserving_orderby_keeps_partition_scope():
    notebook_path = "/Workspace/root.ipynb"
    nb = _mk_nb([
        'df = spark.read.table("collections.daily")',
        'ordered = df.orderBy("run_date").fillna(0)',
        'scoped = ordered.filter(F.col("run_date") == "2026-08-10")',
        'scoped.count()',
    ])
    extracted = extract_from_notebook(nb)
    classified = asyncio.run(classify_refs_on_cluster(
        None, [notebook_path], {notebook_path: extracted},
    ))
    entries = classified[notebook_path]["tables"]
    source = next(e for e in entries if e["table"] == "collections.daily")
    assert source.get("filters") == ["run_date = '2026-08-10'"]


def test_full_consumer_marker_never_reaches_unresolved_scopes():
    nb = _mk_nb([
        'table_name = getArgument("tbl")',
        'df = spark.read.table(f"collections.{table_name}")',
        'df.count()',
        'print(df.count())',
    ])
    out = extract_from_notebook(nb)
    rows = out.get("unresolved_scopes", [])
    assert all("_full_consumer" not in row for row in rows)
    keys = [
        (row.get("object_type"), row.get("argument_expression"),
         tuple(row.get("filters") or []))
        for row in rows
    ]
    assert len(keys) == len(set(keys))


def test_column_comparison_only_filter_is_classified_as_full_requirement():
    refs = {"/Workspace/root.ipynb": {
        "cells_source": "", "paths": [], "tables": ["collections.daily"],
        "read_references": [
            {"object_type": "table", "object_name": "collections.daily",
             "usage": "read", "cell_number": 2,
             "filters": ["created_at <= updated_at"],
             "migration_filters": []},
        ],
        "write_references": [],
    }}
    got = asyncio.run(classify_refs_on_cluster(None, list(refs), refs))
    entry = got["/Workspace/root.ipynb"]["tables"][0]
    assert not entry.get("filters")
    assert entry["observed_filters"] == ["created_at <= updated_at"]


def test_structural_conjunction_single_string_is_full_requirement():
    refs = {"/Workspace/root.ipynb": {
        "cells_source": "", "paths": [], "tables": ["collections.daily"],
        "read_references": [
            {"object_type": "table", "object_name": "collections.daily",
             "usage": "read", "cell_number": 2,
             "filters": [
                 "(customer_id IS NOT NULL) AND (created_at <= updated_at)"
             ],
             "migration_filters": []},
        ],
        "write_references": [],
    }}
    got = asyncio.run(classify_refs_on_cluster(None, list(refs), refs))
    entry = got["/Workspace/root.ipynb"]["tables"][0]
    assert not entry.get("filters")
    assert entry["observed_filters"] == [
        "(customer_id IS NOT NULL) AND (created_at <= updated_at)"
    ]


def test_static_null_and_column_comparison_do_not_demote_date_scope():
    nb = _mk_nb([
        'run_date = getArgument("execution_date_ist")',
        '''df = spark.read.table("collections.daily").filter(
    (F.col("run_date") == run_date)
    & F.col("customer_id").isNotNull()
    & (F.col("created_at") <= F.col("updated_at"))
)''',
    ])
    out = extract_from_notebook(nb, {"execution_date_ist": "2026-08-10"})
    ref = next(
        item for item in out["read_references"]
        if item.get("object_name") == "collections.daily"
    )
    assert ref["migration_filters"]
    assert "run_date = '2026-08-10'" in ref["migration_filters"][0]
    assert "customer_id IS NOT NULL" in ref["migration_filters"][0]
    assert "created_at <= updated_at" in ref["migration_filters"][0]
    assert ref["partition_value"] == "2026-08-10"
    assert ref["filter_resolution"]["status"] == "resolved"
    assert not out["unresolved_scopes"]


def test_absent_widget_uses_explicit_call_site_default():
    nb = _mk_nb([
        'run_date = dbutils.widgets.getArgument("run_date", "2026-08-10")',
        'df = spark.read.table("collections.daily").filter(col("run_date") == run_date)',
    ])
    out = extract_from_notebook(nb)
    ref = next(
        item for item in out["read_references"]
        if item.get("object_name") == "collections.daily"
    )
    assert ref["partition_value"] == "2026-08-10"
    assert ref["filter_resolution"]["status"] == "resolved"


def test_missing_widget_parameter_stays_unresolved():
    nb = _mk_nb([
        'run_date = dbutils.widgets.get("run_date")',
        'path = f"s3://bucket/input/run_date={run_date}"',
        'df = spark.read.parquet(path)',
    ])
    out = extract_from_notebook(nb, {"run_date": ""})
    assert not any("run_date=" in str(path) for path in out["paths"])
    assert any(
        ref.get("argument_expression") == "path"
        for ref in out["unresolved_scopes"]
    )


def test_missing_runtime_date_does_not_use_wall_clock():
    nb = _mk_nb([
        "from datetime import datetime",
        "run_date = datetime.now().strftime('%Y-%m-%d')",
        'df = spark.read.table("collections.daily").filter(col("run_date") == run_date)',
    ])
    out = extract_from_notebook(nb)
    assert out["runtime_anchor"] is None
    assert any(
        ref.get("object_name") == "collections.daily"
        and not ref.get("partition_value")
        for ref in out["unresolved_scopes"]
    )


def test_syntax_error_is_reported_instead_of_silently_skipped():
    out = extract_from_notebook(_mk_nb(["if True print('broken')"]))
    assert out["analysis_errors"][0]["type"] == "SYNTAX_ERROR"
    assert out["analysis_errors"][0]["cell_number"] == 1


def test_notebook_run_captures_literal_parameters_and_flags_dynamic_path():
    out = extract_from_notebook(_mk_nb([
        'dbutils.notebook.run("./child", 3600, {"run_date": "2026-08-10"})',
        'dbutils.notebook.run(dynamic_child, 3600, {"run_date": "2026-08-11"})',
    ]))
    assert out["notebook_runs"][0]["path"] == "./child"
    assert out["notebook_runs"][0]["parameters"] == {
        "run_date": "2026-08-10"
    }
    assert out["not_analyzed"][0]["kind"] == "notebook_run"
    assert out["not_analyzed"][0]["reason"] == "dynamic notebook path"


def test_triggered_child_job_is_reported_as_not_analyzed():
    out = extract_from_notebook(_mk_nb([
        "result = run_job_and_wait(child_job_id, params)",
    ]))
    assert out["not_analyzed"] == [{
        "kind": "child_job",
        "cell_number": 1,
        "line_number": 1,
        "expression": "run_job_and_wait(child_job_id, params)",
        "reason": "triggered child workflow is not in this notebook manifest",
    }]


def test_nb_captures_date_range_and_dynamic_path():
    nb = _mk_nb([
        'execution_date_ist = getArgument("execution_date_ist")',
        'base = "s3://bucket/geo-coding-output"',
        'dated = execution_date_ist.replace("-", "_")',
        'path = f"{base}/cfg/{dated}/run-1"',
        'df = spark.read.json(path).filter((col("created_at") >= "2026-08-01") & (col("created_at") <= execution_date_ist))',
    ])
    out = extract_from_notebook(nb, {"execution_date_ist": "2026-08-10"})
    ref = next(r for r in out["read_references"] if r["object_name"].endswith("/2026_08_10/run-1"))
    assert ref["start_date"] == "2026-08-01"
    assert ref["end_date"] == "2026-08-10"


def test_nb_recognizes_custom_create_table_before_read():
    nb = _mk_nb([
        'createTable(df, "derived_table", "collections", "overwrite")',
        'result = spark.read.table("collections.derived_table")',
    ])
    out = extract_from_notebook(nb)
    assert any(
        r["object_name"] == "collections.derived_table" and r["usage"] == "write_create"
        for r in out["write_references"]
    )


def test_nb_captures_custom_create_table_append_mode():
    nb = _mk_nb([
        'createTable(df, "history", "collections", "append")',
    ])
    out = extract_from_notebook(nb)
    ref = next(r for r in out["write_references"] if r["object_name"] == "collections.history")
    assert ref["write_mode"] == "append"


def test_same_cell_read_before_overwrite_is_not_hidden():
    refs = {"/Workspace/root.ipynb": {
        "cells_source": "",
        "paths": [],
        "tables": ["collections.history"],
        "read_references": [
            {"object_type": "table", "object_name": "collections.history",
             "usage": "read", "cell_number": 10,
             "filters": ["run_date != '2026-08-10'"]},
        ],
        "write_references": [
            {"object_type": "table", "object_name": "collections.history",
             "usage": "write_create", "cell_number": 10, "write_mode": "overwrite"},
        ],
    }}
    got = asyncio.run(classify_refs_on_cluster(None, list(refs), refs))
    entries = got["/Workspace/root.ipynb"]["tables"]
    assert any(entry["usage"] == "read" for entry in entries)
    assert any(entry["usage"] == "write_create" for entry in entries)


def test_same_cell_write_before_read_is_classified_as_generated_output():
    refs = {"/Workspace/root.ipynb": {
        "cells_source": "",
        "paths": [],
        "tables": ["collections.output"],
        "read_references": [
            {"object_type": "table", "object_name": "collections.output",
             "usage": "read", "cell_number": 10, "line_number": 2},
        ],
        "write_references": [
            {"object_type": "table", "object_name": "collections.output",
             "usage": "write_create", "cell_number": 10, "line_number": 1,
             "write_mode": "overwrite"},
        ],
    }}
    got = asyncio.run(classify_refs_on_cluster(None, list(refs), refs))
    entries = got["/Workspace/root.ipynb"]["tables"]
    assert not any(entry["usage"] == "read" for entry in entries)
    assert any(entry["usage"] == "write_create" for entry in entries)


def test_classifier_prefers_prewrite_history_read_over_postwrite_scoped_read():
    refs = {"/Workspace/root.ipynb": {
        "cells_source": "",
        "paths": [],
        "tables": ["collections.history"],
        "read_references": [
            {"object_type": "table", "object_name": "collections.history",
             "usage": "read", "cell_number": 10, "line_number": 1,
             "filters": ["run_date != '2026-08-10'"], "migration_filters": []},
            {"object_type": "table", "object_name": "collections.history",
             "usage": "read", "cell_number": 12, "line_number": 1,
             "filters": ["run_date = '2026-08-10'"],
             "migration_filters": ["run_date = '2026-08-10'"]},
        ],
        "write_references": [
            {"object_type": "table", "object_name": "collections.history",
             "usage": "write_create", "cell_number": 11, "line_number": 1,
             "write_mode": "overwrite"},
        ],
    }}
    got = asyncio.run(classify_refs_on_cluster(None, list(refs), refs))
    read = next(
        entry for entry in got["/Workspace/root.ipynb"]["tables"]
        if entry["usage"] == "read"
    )
    assert read["filters"] == ["run_date != '2026-08-10'"]


def test_runtime_underscore_date_becomes_partition_value():
    nb = _mk_nb([
        'run_date = getArgument("execution_date_ist")',
        'df = spark.read.table("collections.daily").filter(col("run_date") == run_date)',
    ])
    out = extract_from_notebook(nb, {"execution_date_ist": "2026_08_10"})
    ref = next(r for r in out["read_references"] if r["object_name"] == "collections.daily")
    assert ref["partition_column"] == "run_date"
    assert ref["partition_value"] == "2026_08_10"


def test_unresolved_path_still_records_resolved_filter_scope():
    nb = _mk_nb([
        """from datetime import datetime
today = datetime.today()
msd = today.replace(day=1).strftime("%Y-%m-%d")""",
        'df = spark.read.format("hudi").load(external_path).filter(col("month_start_date") == msd)',
    ])
    out = extract_from_notebook(nb, {"execution_date_ist": "2026_08_10"})
    ref = next(r for r in out["unresolved_scopes"] if r["argument_expression"] == "external_path")
    assert ref["object_name"] is None
    assert ref["filters"] == ["month_start_date = '2026-08-01'"]
    assert ref["partition_column"] == "month_start_date"
    assert ref["partition_value"] == "2026-08-01"


def test_classification_selects_migration_filter_and_keeps_observed_filters():
    refs = {"/Workspace/root.ipynb": {
        "cells_source": "",
        "paths": [],
        "tables": ["collections.daily"],
        "read_references": [
            {"object_type": "table", "object_name": "collections.daily", "usage": "read",
             "cell_number": 2, "filters": ["active = TRUE"]},
            {"object_type": "table", "object_name": "collections.daily", "usage": "read",
             "cell_number": 3, "filters": ["active = TRUE", "run_date = '2026-08-10'"],
             "migration_filters": ["run_date = '2026-08-10'"],
             "partition_column": "run_date", "partition_value": "2026-08-10"},
        ],
        "write_references": [],
    }}
    got = asyncio.run(classify_refs_on_cluster(None, list(refs), refs))
    entry = got["/Workspace/root.ipynb"]["tables"][0]
    assert entry["filters"] == ["run_date = '2026-08-10'"]
    assert "active = TRUE" in entry["observed_filters"]


def test_classification_preserves_every_distinct_migration_scope():
    refs = {"/Workspace/root.ipynb": {
        "cells_source": "", "paths": [], "tables": ["collections.daily"],
        "read_references": [
            {"object_type": "table", "object_name": "collections.daily",
             "usage": "read", "cell_number": 2,
             "filters": ["run_date = '2026-08-10'"],
             "migration_filters": ["run_date = '2026-08-10'"]},
            {"object_type": "table", "object_name": "collections.daily",
             "usage": "read", "cell_number": 3,
             "filters": ["run_date = '2026-08-11'"],
             "migration_filters": ["run_date = '2026-08-11'"]},
        ],
        "write_references": [],
    }}
    got = asyncio.run(classify_refs_on_cluster(None, list(refs), refs))
    assert {
        tuple(entry["filters"])
        for entry in got["/Workspace/root.ipynb"]["tables"]
    } == {
        ("run_date = '2026-08-10'",),
        ("run_date = '2026-08-11'",),
    }


def test_independent_full_read_wins_over_filtered_read_of_same_object():
    refs = {"/Workspace/root.ipynb": {
        "cells_source": "", "paths": [], "tables": ["collections.daily"],
        "read_references": [
            {"object_type": "table", "object_name": "collections.daily",
             "usage": "read", "cell_number": 2, "filters": []},
            {"object_type": "table", "object_name": "collections.daily",
             "usage": "read", "cell_number": 3,
             "filters": ["run_date = '2026-08-10'"],
             "migration_filters": ["run_date = '2026-08-10'"]},
        ],
        "write_references": [],
    }}
    got = asyncio.run(classify_refs_on_cluster(None, list(refs), refs))
    entries = got["/Workspace/root.ipynb"]["tables"]
    assert len(entries) == 1
    assert not entries[0].get("filters")
    assert entries[0]["observed_filters"] == ["run_date = '2026-08-10'"]


def test_structural_only_filter_is_classified_as_full_requirement():
    refs = {"/Workspace/root.ipynb": {
        "cells_source": "", "paths": [], "tables": ["collections.daily"],
        "read_references": [
            {"object_type": "table", "object_name": "collections.daily",
             "usage": "read", "cell_number": 2,
             "filters": ["customer_id IS NOT NULL"],
             "migration_filters": []},
        ],
        "write_references": [],
    }}

    got = asyncio.run(classify_refs_on_cluster(None, list(refs), refs))

    entry = got["/Workspace/root.ipynb"]["tables"][0]
    assert not entry.get("filters")
    assert entry["observed_filters"] == ["customer_id IS NOT NULL"]


def test_report_marks_missing_scope_with_base_present():
    notebook = "/Workspace/root.ipynb"
    filters = ["run_date = '2026-08-10'"]
    refs = {notebook: {
        "tables": [{"table": "collections.daily", "usage": "read", "filters": filters,
                    "partition_column": "run_date", "partition_value": "2026-08-10"}],
        "paths": [], "unresolved": [], "python_imports": [], "scala_imports": [],
    }}
    scope = {_scope_key(notebook, "table", "collections.daily", filters): {
        "base_exists": True, "scope_exists": False, "predicate": "(run_date = '2026-08-10')"}}
    report = generate_report(refs, {"collections.daily": {"exists": True}}, {}, {},
                             {"python": {}, "scala": {}}, "job", scope_results=scope)
    entry = report["notebooks"][notebook]["read_tables"][0]
    assert entry["base_exists"] is True
    assert entry["exists"] is False
    assert entry["scope_exists"] is False


def test_nb_scala_cell_lang_via_magic():
    nb = _mk_nb([
        '%scala\nval x = "s3://foo/bar" // this comment must be stripped',
    ])
    out = extract_from_notebook(nb)
    assert "s3://foo/bar" in out["paths"]
    assert "this comment must be stripped" not in out["cells_source"]


def test_nb_python_default_lang_slash_slash_preserved():
    nb = _mk_nb([
        'total = seconds // (24 * 3600)',
    ])
    out = extract_from_notebook(nb)
    assert "// (24 * 3600)" in out["cells_source"]


def test_nb_ignores_markdown_cells():
    nb = {
        "metadata": {"language_info": {"name": "python"}},
        "cells": [
            {"cell_type": "markdown", "source": ['# Header\ns3://ignored/']},
            {"cell_type": "code", "source": ['x = "s3://real/path/"']},
        ],
    }
    out = extract_from_notebook(nb)
    assert "s3://real/path/" in out["paths"]
    assert "s3://ignored/" not in out["paths"]


# ═══════════════════════ real-corpus regression (AutoRefit v19) ═══════════
# Sourced from /tmp/scanner_missed.json — every one of these was silently
# dropped by the historical scanner. If any of these regress, revert.

CORPUS_URLS = [
    "oci://oci-example-data-lake@exmplnspace2/raw-data/sales_analytics/example_flows_data_a/",
    "oci://oci-example-aggregator-prod@exmplnspace1/datalake_store/ml_attrs_set_a/",
    "oci://oci-ml-data-store-unit-bau@exmplnspace1/datalake_store/ml_attrs_set_b/",
    "oci://oci-ml-data-store-unit-batch@exmplnspace2/datalake_store/ml_attrs_set_c/",
    "s3a://example-datascience-temporary-bucket/intermediate_tables/",
    "oci://oci-example-datascience-temporary-bucket@exmplnspace1/intermediate_tables/",
    "oci://oci-example-workspace-bucket@exmplnspace1/intermediate_data/",
    "s3://examplecorp-underwriting-teamx-data/event-data/teamx-data-reference-link-events/data/p_origination",
    "s3a://examplecorp-underwriting-teamx-data/event-data/",
    "s3a://example-datascience-temporary-bucket/db_tables/default_user/x/user6/appEmbeddingsO2/",
    "s3://examplecorp-dp-unit-rds/rds-and-kafka/sales_analytics/location_features",
    "s3://examplecorp-underwriting-event-data-bau/event-data/example_flows_data_b/data/",
    "s3://user14-v2-bucket/user14_v2_dump/user14dump/",
    "s3://examplecorp-dp-unit-rds/rds-and-kafka/example_device_service/example_device_temp/",  # THE Task 02 blocker
]


def test_corpus_all_urls_captured():
    """Every URL that the AutoRefit v19 scan showed as MISSED must now surface
    in `paths`. If this test fails, we've regressed on real customer data."""
    misses = []
    for url in CORPUS_URLS:
        src = f'x = "{url}"'
        out = extract_from_source(strip_comments(src, lang="python"))
        if url not in out["paths"]:
            misses.append(url)
    assert not misses, f"regressed on {len(misses)} corpus URLs:\n" + "\n".join(misses)


def test_corpus_task02_specific_case():
    """The exact code from HelperFunctions Cell 26 that broke v19 Task 02."""
    src = '''def get_all_device_summary(start_date, end_date, flow_type = "O2"):

    cust_device_path = "s3://examplecorp-dp-unit-rds/rds-and-kafka/example_device_service/example_device_temp/"
    cust_device_table = spark.read.format('org.apache.hudi').load(cust_device_path)

    cd = spark.read.table("example_feature_backup.datascience_serviceable_base").filter(F.col("flow_type") == flow_type)
    return cd
'''
    src_stripped = strip_comments(src, lang="python")
    out = extract_from_source(src_stripped)

    # URL preserved AND captured
    assert "s3://examplecorp-dp-unit-rds/rds-and-kafka/example_device_service/example_device_temp/" in out["paths"], \
        f"THE Task 02 blocker URL is still missed! paths={out['paths']}"

    # Variable-based Hudi read captured as unresolved
    assert any("cust_device_path" in u for u in out["unresolved"]), \
        f"spark.read.format('hudi').load(cust_device_path) missed. unresolved={out['unresolved']}"

    # Literal-arg table still captured (regression check)
    assert "example_feature_backup.datascience_serviceable_base" in out["tables"]


def test_corpus_variable_read_forms():
    """Real spark.read/write forms from the AutoRefit corpus that must be
    surfaced as unresolved."""
    cases = [
        'spark.read.parquet(s3_path)',
        'spark.read.parquet(s3_path_nfs)',
        'spark.read.parquet(s3_path_ntl)',
        'spark.read.load(CUST_DEVICE_PATH)',
        'spark.read.parquet(MIGRATED_DEVICES_PATH)',
        '.write.json(path)',
    ]
    for src in cases:
        out = extract_from_source(src)
        expected_pattern = src.replace("spark.read.", "spark.read.").split("(")[0]
        assert any(expected_pattern in u for u in out["unresolved"]), \
            f"corpus case missed: {src!r} → unresolved={out['unresolved']}"


def test_corpus_boto3_s3_variants():
    for src in [
        'client = boto3.client("s3")',
        's3 = boto3.client("s3", region_name="ap-south-1")',
        'res = boto3.resource("s3")',
    ]:
        out = extract_from_source(src)
        assert out["boto3_s3"], f"boto3 s3 missed: {src!r}"


# ═══════════════════════ orchestrator: Class.forName candidates ═════════════
#
# `_scala_class_forname_candidates(cls)` is the fix for the extractor's
# `spark._jvm.<chain>` regex which greedily captures method invocations
# alongside class names. Empirical cluster probe (16:20 UTC 2026-07-27)
# confirmed the trimmed-fallback candidates resolve on the live HELPER
# cluster. These tests lock the algorithm.
from check_data_availability_for_workflow import _scala_class_forname_candidates as _cands
import check_data_availability_for_workflow as data_checker
from check_data_availability_for_workflow import (
    _aidp_control_plane_base,
    attach_data_migration_requirements,
    build_library_probe_plan,
    build_notebook_cluster_map,
    build_notebook_runtime_parameters,
    check_oci_paths,
    check_libraries,
    check_mlflow_artifacts,
    check_tables_describe,
    classify_refs_on_cluster,
    generate_report,
    merge_library_results_by_notebook,
    probe_libraries_on_job_clusters,
    load_bucket_ns_mapping_text,
    _safe_scope_predicate,
    _scope_key,
)
import asyncio


def test_library_probe_uses_spark_context_classloader_before_class_for_name():
    class Session:
        async def execute(self, code, timeout):
            assert "getContextClassLoader" in code
            assert "_context_loader.loadClass(cand)" in code
            assert "java.lang.Class.forName(cand)" in code
            assert "\n    loaded = False\n" in code
            assert "\nloaded = False\n" not in code
            return {"outputs": [{
                "type": "stream",
                "text": '{"python": {}, "scala": {}}',
            }]}

    result = asyncio.run(check_libraries(
        Session(), [], ["com.example.dp.UdfRegistrar"]
    ))
    assert result == {"python": {}, "scala": {}}


def test_task_parameters_are_scoped_per_notebook_and_job_values_win():
    tasks = [
        {
            "task_key": "scoring",
            "notebook_path": "/Workspace/scoring.ipynb",
            "base_parameters": {
                "model_scaling_files_path": "/Workspace/model-artifacts",
                "shared": "task",
            },
        },
        {
            "task_key": "prep",
            "notebook_path": "/Workspace/prep.ipynb",
            "base_parameters": {"input_path": "/Volumes/input"},
        },
    ]
    resolved = build_notebook_runtime_parameters(tasks, {"shared": "job"})
    assert resolved["/Workspace/scoring.ipynb"] == {
        "model_scaling_files_path": "/Workspace/model-artifacts",
        "shared": "job",
    }
    assert resolved["/Workspace/prep.ipynb"] == {
        "input_path": "/Volumes/input",
        "shared": "job",
    }


def test_literal_notebook_run_parameters_are_bound_to_child_scan(monkeypatch):
    parent = "/Workspace/root.ipynb"
    child = "/Workspace/child.ipynb"
    notebooks = {
        parent: _mk_nb([
            'dbutils.notebook.run("./child", 3600, '
            '{"run_date": "2026-08-10"})'
        ]),
        child: _mk_nb([
            'run_date = dbutils.widgets.get("run_date")',
            'path = f"s3://bucket/input/run_date={run_date}"',
            "df = spark.read.parquet(path)",
        ]),
    }
    monkeypatch.setattr(data_checker, "get_oci_signer", lambda _profile: ({}, object()))
    monkeypatch.setattr(
        data_checker, "_download_notebook_json",
        lambda _signer, _base, _lake, _workspace, path: notebooks[path],
    )

    refs = asyncio.run(data_checker.extract_data_refs(
        None, [parent, child], aidp_base="https://example.invalid",
        lake_ocid="ocid1.aidataplatform.oc1.iad.example",
        workspace_id="workspace",
    ))

    assert any(
        ref.get("object_name") == "s3://bucket/input/run_date=2026-08-10"
        for ref in refs[child]["read_references"]
    )


def test_unmatched_literal_notebook_run_is_explicitly_not_analyzed(monkeypatch):
    parent = "/Workspace/root.ipynb"
    notebook = _mk_nb(['dbutils.notebook.run("./outside", 3600, {})'])
    monkeypatch.setattr(data_checker, "get_oci_signer", lambda _profile: ({}, object()))
    monkeypatch.setattr(
        data_checker, "_download_notebook_json", lambda *_args: notebook,
    )
    refs = asyncio.run(data_checker.extract_data_refs(
        None, [parent], aidp_base="https://example.invalid",
        lake_ocid="ocid1.aidataplatform.oc1.iad.example",
        workspace_id="workspace",
    ))
    gap = refs[parent]["not_analyzed"][0]
    assert gap["kind"] == "notebook_run"
    assert gap["path"] == "./outside"
    assert "outside the manifest" in gap["reason"]


def test_notebook_download_failure_is_explicitly_not_analyzed(monkeypatch):
    notebook = "/Workspace/missing.ipynb"
    monkeypatch.setattr(data_checker, "get_oci_signer", lambda _profile: ({}, object()))
    monkeypatch.setattr(
        data_checker, "_download_notebook_json",
        lambda *_args: (_ for _ in ()).throw(RuntimeError("HTTP 404")),
    )
    refs = asyncio.run(data_checker.extract_data_refs(
        None, [notebook], aidp_base="https://example.invalid",
        lake_ocid="ocid1.aidataplatform.oc1.iad.example",
        workspace_id="workspace",
    ))
    assert refs[notebook]["unresolved"][0]["kind"] == "NOT_ANALYZED"
    assert refs[notebook]["analysis_errors"][0]["type"] == "NOTEBOOK_DOWNLOAD_ERROR"


def test_resolved_csv_path_is_not_classified_as_a_table():
    notebook_path = "/Workspace/scoring.ipynb"
    csv_path = "/Volumes/default/default/dbfs/FileStore/input.csv"
    notebook = _mk_nb([
        'decision_path = dbutils.widgets.get("decision_path")\n'
        'df = spark.read.table(decision_path)'
    ])
    extracted = extract_from_notebook(
        notebook, {"decision_path": csv_path})
    assert any(
        ref["object_type"] == "path" and ref["object_name"] == csv_path
        for ref in extracted["read_references"]
    )

    classified = asyncio.run(classify_refs_on_cluster(
        None, [notebook_path], {notebook_path: extracted}))
    assert not any(
        entry["table"] == csv_path
        for entry in classified[notebook_path]["tables"]
    )
    assert any(
        entry["path"] == csv_path
        for entry in classified[notebook_path]["paths"]
    )


def test_temp_view_read_is_not_an_external_table_dependency():
    notebook_path = "/Workspace/ledger.ipynb"
    notebook = _mk_nb([
        '%scala\nvarsDf.createOrReplaceTempView("varsDf")',
        'varsDf = spark.read.table("varsDf")',
    ])
    extracted = extract_from_notebook(notebook)
    assert extracted["temporary_views"] == [{
        "name": "varsDf",
        "cell_number": 1,
        "api": "createOrReplaceTempView",
    }]

    classified = asyncio.run(classify_refs_on_cluster(
        None, [notebook_path], {notebook_path: extracted}))
    assert not any(
        entry["table"] == "varsDf"
        for entry in classified[notebook_path]["tables"]
    )


def test_library_probe_plan_uses_each_tasks_job_cluster_for_dependencies():
    shared = "/Workspace/shared.ipynb"
    tasks = [
        {
            "task_key": "score",
            "notebook_path": "/Workspace/score.ipynb",
            "cluster_id": "scoring-cluster",
            "run_deps": [{"path": shared}],
        },
        {
            "task_key": "embed",
            "notebook_path": "/Workspace/embed.ipynb",
            "cluster": {"clusterKey": "embedding-cluster"},
            "run_deps": [{"path": shared}],
        },
    ]
    refs = {
        "/Workspace/score.ipynb": {
            "python_imports": ["xgboost"], "scala_imports": [],
        },
        "/Workspace/embed.ipynb": {
            "python_imports": ["torch"], "scala_imports": [],
        },
        shared: {
            "python_imports": [],
            "scala_imports": ["com.example.datascience.HelperLib"],
        },
    }

    notebook_clusters = build_notebook_cluster_map(tasks, "control-cluster")
    assert notebook_clusters[shared] == [
        "scoring-cluster", "embedding-cluster",
    ]
    plan = build_library_probe_plan(refs, notebook_clusters)
    assert plan["scoring-cluster"]["python"] == ["xgboost"]
    assert plan["embedding-cluster"]["python"] == ["torch"]
    assert plan["scoring-cluster"]["scala"] == [
        "com.example.datascience.HelperLib"
    ]
    assert plan["embedding-cluster"]["scala"] == [
        "com.example.datascience.HelperLib"
    ]
    assert "control-cluster" not in plan


def test_library_probe_plan_omits_clusters_with_no_imports():
    refs = {"/Workspace/no-imports.ipynb": {
        "python_imports": [], "scala_imports": [],
    }}
    assert build_library_probe_plan(
        refs, {"/Workspace/no-imports.ipynb": ["stopped-cluster"]}
    ) == {}


def test_stopped_job_cluster_probe_is_unverified_and_session_is_closed():
    created = []

    class ProbeSession:
        def __init__(self, **_kwargs):
            self.closed = False
            created.append(self)

        async def connect(self):
            raise RuntimeError("cluster is STOPPED")

        async def close(self):
            self.closed = True

    results = asyncio.run(probe_libraries_on_job_clusters(
        object(),
        {"stopped-cluster": {
            "python": ["xgboost"], "scala": ["com.example.Class"],
            "notebooks": ["/Workspace/job.ipynb"],
        }},
        control_cluster_id="control-cluster",
        lake_ocid="lake", workspace_id="workspace",
        oci_profile="DEFAULT", job_name="job",
        session_factory=ProbeSession,
    ))
    assert results["stopped-cluster"]["python"]["xgboost"]["unverified"] is True
    assert results["stopped-cluster"]["scala"]["com.example.Class"]["unverified"] is True
    assert created[0].closed is True


def test_control_cluster_library_probe_preserves_open_circuit_reason():
    reason = "replacement session connect failed: HTTP 500"
    results = asyncio.run(probe_libraries_on_job_clusters(
        None,
        {"control-cluster": {
            "python": ["pandas"], "scala": ["com.example.Class"],
            "notebooks": ["/Workspace/job.ipynb"],
        }},
        control_cluster_id="control-cluster",
        lake_ocid="lake", workspace_id="workspace",
        oci_profile="DEFAULT", job_name="job",
        control_unavailable_reason=reason,
    ))
    python_result = results["control-cluster"]["python"]["pandas"]
    scala_result = results["control-cluster"]["scala"]["com.example.Class"]
    assert python_result["unverified"] is True
    assert scala_result["unverified"] is True
    assert reason in python_result["error"]
    assert "NoneType" not in python_result["error"]


def test_main_closes_control_session_when_body_raises(monkeypatch):
    class Session:
        closed = False

        async def close(self):
            self.closed = True

    session = Session()

    async def fail_after_connect():
        data_checker._ACTIVE_CONTROL_SESSION = session
        raise RuntimeError("probe failed")

    monkeypatch.setattr(data_checker, "_main_impl", fail_after_connect)
    with pytest.raises(RuntimeError, match="probe failed"):
        asyncio.run(data_checker.main())
    assert session.closed is True


def test_cluster_library_results_are_scoped_back_to_notebook():
    notebook = "/Workspace/score.ipynb"
    refs = {notebook: {
        "python_imports": ["xgboost"], "scala_imports": [],
    }}
    merged = merge_library_results_by_notebook(
        refs,
        {notebook: ["scoring-cluster"]},
        {"scoring-cluster": {
            "python": {"xgboost": {
                "available": False, "error": "not installed",
            }},
            "scala": {},
        }},
    )
    info = merged["by_notebook"][notebook]["python"]["xgboost"]
    assert info["available"] is False
    assert info["checked_clusters"] == ["scoring-cluster"]
    assert "scoring-cluster: not installed" == info["error"]


def test_verified_missing_library_wins_over_other_unverified_cluster():
    notebook = "/Workspace/score.ipynb"
    refs = {notebook: {"python_imports": ["xgboost"], "scala_imports": []}}

    merged = merge_library_results_by_notebook(
        refs,
        {notebook: ["unreachable-cluster", "verified-cluster"]},
        {
            "unreachable-cluster": {
                "python": {"xgboost": {
                    "available": False,
                    "unverified": True,
                    "error": "cluster unreachable",
                }},
                "scala": {},
            },
            "verified-cluster": {
                "python": {"xgboost": {
                    "available": False,
                    "error": "No module named xgboost",
                }},
                "scala": {},
            },
        },
    )

    info = merged["by_notebook"][notebook]["python"]["xgboost"]
    assert info["available"] is False
    assert info.get("unverified") is not True
    assert "No module named xgboost" in info["error"]


def test_missing_aidp_widget_workspace_file_reaches_tracker_contract():
    notebook_path = "/Workspace/scoring.ipynb"
    missing_path = "/Workspace/model-artifacts/scaling_files/Feature_List.csv"
    notebook = _mk_nb([
        'root = aidputils.widgets.get("model_scaling_files_path").strip()',
        'with open(f"{root}/scaling_files/Feature_List.csv") as f:\n    payload = f.read()',
    ])
    extracted = extract_from_notebook(
        notebook, {"model_scaling_files_path": "/Workspace/model-artifacts"})
    classified = asyncio.run(classify_refs_on_cluster(
        None, [notebook_path], {notebook_path: extracted}))
    report = generate_report(
        classified, {}, {}, {missing_path: {"exists": False}},
        {"python": {}, "scala": {}}, "job")
    attach_data_migration_requirements(report, "900000000000010")

    assert report["summary"]["paths_missing"] == 1
    assert any(
        row["object_type"] == "FILE" and row["object_name"] == missing_path
        for row in report["data_migration"]["requirements"]
    )


def test_mlflow_artifacts_reach_tracker_without_oci_mapping():
    notebook_path = "/Workspace/scoring.ipynb"
    run_id = "00000000000000000000000000000abc"
    file_uri = f"runs:/{run_id}/model_files/model_folder_path.csv"
    directory_uri = f"runs:/{run_id}/scaling_files"
    refs = {notebook_path: {
        "tables": [], "paths": [], "unresolved": [],
        "unresolved_scopes": [], "unresolved_mlflow_artifacts": [],
        "python_imports": [], "scala_imports": [],
        "mlflow_artifacts": [
            {"artifact_uri": file_uri, "run_id": run_id,
             "artifact_path": "model_files/model_folder_path.csv",
             "object_type": "FILE", "api": "client.download_artifacts",
             "cell_number": 11, "line_number": 7},
            {"artifact_uri": directory_uri, "run_id": run_id,
             "artifact_path": "scaling_files", "object_type": "DIRECTORY",
             "api": "client.download_artifacts", "cell_number": 11,
             "line_number": 10},
        ],
    }}
    report = generate_report(
        refs, {}, {}, {}, {"python": {}, "scala": {}}, "job",
        mlflow_results={
            file_uri: {"exists": False},
            directory_uri: {"exists": True, "items": 4},
        },
    )
    attach_data_migration_requirements(report, "900000000000010")
    requirements = {
        row["object_name"]: row
        for row in report["data_migration"]["requirements"]
    }
    assert requirements[file_uri]["object_type"] == "FILE"
    assert requirements[directory_uri]["object_type"] == "DIRECTORY"
    assert requirements[file_uri]["status"] == "PROACTIVE_REQUIRED"
    assert requirements[directory_uri]["status"] == "PROACTIVE_REQUIRED"
    assert report["summary"]["mlflow_artifacts_found"] == 1
    assert report["summary"]["mlflow_artifacts_missing"] == 1


def test_check_mlflow_artifacts_parses_cluster_result():
    class Session:
        async def execute(self, code, timeout):
            assert "MLFLOW_TRACKING_URI" in code
            assert "mlflow-tracking-uri" in code
            return {"outputs": [{
                "type": "stream",
                "text": '{"runs:/run-1/a.csv":{"exists":true,"items":1}}',
            }]}

    result = asyncio.run(check_mlflow_artifacts(Session(), [{
        "artifact_uri": "runs:/run-1/a.csv",
        "run_id": "run-1",
        "artifact_path": "a.csv",
        "object_type": "FILE",
    }]))
    assert result["runs:/run-1/a.csv"] == {"exists": True, "items": 1}


def test_safe_scope_predicate_accepts_resolved_date_contains_like():
    assert _safe_scope_predicate(["month_start_date LIKE '%2026-08-01%'"]) == (
        "(month_start_date LIKE '%2026-08-01%')"
    )


def test_data_check_report_embeds_versioned_migration_requirements():
    report = {
        "job_name": "example",
        "generated_at": "2026-08-25T00:00:00",
        "summary": {},
        "notebooks": {
            "/Workspace/root.ipynb": {
                "read_tables": [{"table": "source.monthly", "exists": False}],
                "read_paths": [],
                "write_tables": [],
                "write_paths": [],
                "write_references": [],
                "unresolved": [],
                "unresolved_scopes": [],
            }
        },
    }
    combined = attach_data_migration_requirements(report, "123")
    assert combined is report
    assert combined["databricks_job_id"] == "123"
    contract = combined["data_migration"]
    assert contract["schema_version"] == "1.0"
    assert contract["document_type"] == "data_migration_requirements"
    assert contract["loadable"] is True
    assert contract["summary"]["requirements"] == 1
    assert contract["requirements"][0]["object_name"] == "source.monthly"


def test_data_check_skips_tracker_plan_for_build_dag_zero_job_id():
    report = {"summary": {}, "notebooks": {}}

    combined = attach_data_migration_requirements(report, 0)

    assert combined is report
    assert "data_migration" not in combined
    assert "no source Databricks job identifier" in combined["warnings"][0]


def test_unknown_date_delta_arguments_do_not_crash_extraction():
    notebook = _mk_nb([
        "from datetime import timedelta\n"
        "from dateutil.relativedelta import relativedelta\n"
        "month_delta = relativedelta(months=months_back)\n"
        "day_delta = timedelta(days=days_back)\n"
    ])

    extracted = extract_from_notebook(notebook)

    assert extracted["paths"] == []
    assert extracted["tables"] == []


def test_aidp_control_plane_base_uses_configured_region():
    assert _aidp_control_plane_base("us-phoenix-1") == (
        "https://aidp.us-phoenix-1.oci.oraclecloud.com/20240831"
    )


def test_workspace_bucket_mapping_is_parsed_as_csv():
    import check_data_availability_for_workflow as workflow_check

    load_bucket_ns_mapping_text(
        "s3_bucket,oci_bucket,oci_namespace\nsource,target,namespace1\n",
        "/Workspace/config/buckets.csv",
    )

    assert workflow_check._BUCKET_NS == {"target": "namespace1"}


def test_invalid_cluster_probe_output_fails_loudly():
    class Session:
        async def execute(self, code, timeout):
            return {"outputs": [{"type": "stream", "text": "not-json"}]}

    with pytest.raises(RuntimeError, match="Table availability check"):
        asyncio.run(check_tables_describe(Session(), ["schema.table"]))


@pytest.mark.parametrize("path_count", [1, 44, 150])
def test_oci_path_probe_uses_bounded_five_path_batches(path_count):
    paths = [
        f"oci://bucket@namespace/path-{index}"
        for index in range(path_count)
    ]
    observed_batches = []

    class Session:
        async def execute(self, code, timeout):
            import ast
            import json
            assert timeout == 180
            tree = ast.parse(code)
            specs = next(ast.literal_eval(n.value) for n in tree.body
                         if isinstance(n, ast.Assign)
                         and any(isinstance(t, ast.Name) and t.id == "path_specs"
                                 for t in n.targets))
            observed_batches.append([spec["path"] for spec in specs])
            result = {spec["path"]: {"exists": True} for spec in specs}
            return {"outputs": [{"type": "stream", "text": json.dumps(result)}]}

    result = asyncio.run(check_oci_paths(Session(), paths))
    assert list(result) == paths
    assert [path for batch in observed_batches for path in batch] == paths
    assert all(1 <= len(batch) <= 5 for batch in observed_batches)


def test_empty_oci_path_probe_does_not_execute():
    class Session:
        async def execute(self, code, timeout):
            pytest.fail("Empty path batches must not execute on the cluster")

    assert asyncio.run(check_oci_paths(Session(), [])) == {}


def test_oci_probe_error_marks_the_batch_unverified():
    class Session:
        async def execute(self, code, timeout):
            assert timeout == 180
            return {"status": "error", "outputs": [
                {"type": "error", "ename": "TimeoutError", "evalue": "timed out"}
            ]}

    paths = [f"oci://bucket@namespace/path-{i}" for i in range(5)]
    with pytest.raises(RuntimeError, match="TimeoutError"):
        asyncio.run(check_oci_paths(Session(), paths))


@pytest.mark.parametrize(
    "output",
    ["", "not-json", "{}", '{"oci://bucket@namespace/first": {"exists": true}}'],
)
def test_oci_probe_rejects_absent_or_partial_batch_results(output):
    class Session:
        async def execute(self, code, timeout):
            assert timeout == 180
            return {"outputs": [{"type": "stream", "text": output}]}

    paths = [
        "oci://bucket@namespace/first",
        "oci://bucket@namespace/second",
    ]
    with pytest.raises(RuntimeError, match="UNVERIFIED"):
        asyncio.run(check_oci_paths(Session(), paths))


def test_oci_probe_error_cannot_be_hidden_by_json_output():
    class Session:
        async def execute(self, code, timeout):
            return {"status": "error", "outputs": [
                {"type": "stream", "text": '{"oci://bucket@namespace/path": {"exists": true}}'},
                {"type": "error", "ename": "ConnectionDied", "evalue": "connection lost"},
            ]}

    with pytest.raises(RuntimeError, match="UNVERIFIED"):
        asyncio.run(check_oci_paths(Session(), ["oci://bucket@namespace/path"]))


def test_probe_failures_are_reported_unverified_not_missing():
    refs = {
        "/Workspace/job.ipynb": {
            "tables": [{"table": "schema.table", "usage": "read"}],
            "paths": [{
                "path": "oci://bucket@namespace/path", "usage": "read"
            }],
        }
    }
    unverified = {
        "exists": "unverified",
        "probe_status": "UNVERIFIED",
        "error": "probe timed out twice",
    }
    report = generate_report(
        refs,
        {"schema.table": unverified},
        {"oci://bucket@namespace/path": unverified},
        {}, {"python": {}, "scala": {}}, "job",
    )

    assert report["summary"]["tables_unverified"] == 1
    assert report["summary"]["paths_unverified"] == 1
    assert report["summary"]["tables_missing"] == 0
    assert report["summary"]["paths_missing"] == 0


def test_scope_predicate_rejects_sql_function_calls():
    assert _safe_scope_predicate([
        "reflect('java.lang.System','getProperty','user.dir') = 'x'"
    ]) == ""
    assert _safe_scope_predicate(["type IN ('train', 'test')"]) == (
        "(type IN ('train', 'test'))"
    )


def test_scope_predicate_accepts_compound_boolean_grouping():
    predicate = _safe_scope_predicate([
        "(event_date >= '2024-01-01') AND (customer_id IS NOT NULL)"
    ])

    assert predicate == (
        "((event_date >= '2024-01-01') AND (customer_id IS NOT NULL))"
    )


def test_scala_cands_bare_class_two_forms():
    """A bare fully-qualified class name gets [cls, cls$] — no trim needed."""
    got = _cands("com.example.datascience.helperlib.utils.Utils")
    assert got == [
        "com.example.datascience.helperlib.utils.Utils",
        "com.example.datascience.helperlib.utils.Utils$",
        "com.example.datascience.helperlib.utils",
        "com.example.datascience.helperlib.utils$",
    ], got


def test_scala_cands_method_chain_appends_trim():
    """Method chain from HELPER bug — Class.forName on the full string fails
    but the trimmed parent resolves. The candidate list must include both."""
    got = _cands("com.example.datascience.helperlib.utils.Utils.getDateStrFromEpoch")
    assert "com.example.datascience.helperlib.utils.Utils.getDateStrFromEpoch" in got
    assert "com.example.datascience.helperlib.utils.Utils" in got, got
    # Order matters: try full first, trim second.
    assert got.index("com.example.datascience.helperlib.utils.Utils.getDateStrFromEpoch") < \
           got.index("com.example.datascience.helperlib.utils.Utils"), got


def test_scala_cands_ledger_utils_variant():
    """Second real bug from HELPER — ledger.utils.Utils.getFeatureStatus."""
    got = _cands("com.example.datascience.example.utils.Utils.getFeatureStatus")
    assert "com.example.datascience.example.utils.Utils" in got


def test_scala_cands_scala_object_method_chain():
    """`spark._jvm.org.apache.spark.sql.functions.col` — `functions` is a
    Scala object, `.col` is a method. Trim resolves `functions$`."""
    got = _cands("org.apache.spark.sql.functions.col")
    assert "org.apache.spark.sql.functions$" in got, got


def test_scala_cands_single_dot_no_trim():
    """`x.y` — trim leaves `x`, which has no dots → skip trim to avoid
    emitting garbage single-segment candidates."""
    got = _cands("com.Foo")
    # `com` alone is not a valid FQN → must not appear
    assert "com" not in got
    assert "com$" not in got
    # bare class candidates still there
    assert got == ["com.Foo", "com.Foo$"], got


def test_scala_cands_no_dots():
    """Unqualified name (theoretical — extractor should never emit this,
    but be robust): return [cls, cls$] only."""
    got = _cands("Foo")
    assert got == ["Foo", "Foo$"], got


def test_scala_cands_all_returned_are_strings():
    """Defensive: every candidate is a str, no None / empty."""
    got = _cands("com.example.datascience.helperlib.utils.Utils.getDateStrFromEpoch")
    assert all(isinstance(x, str) and x for x in got), got


# ═══════════════════════ ═══════════════════════ ═══════════════════════
if __name__ == "__main__":
    fns = [(name, fn) for name, fn in globals().items()
           if name.startswith("test_") and callable(fn)]
    failures = []
    for name, fn in fns:
        try:
            fn()
            print(f"  ✓ {name}")
        except AssertionError as e:
            failures.append((name, str(e)))
            print(f"  ✗ {name}\n      {e}")
    print("=" * 60)
    if failures:
        print(f"✗ {len(failures)} FAILURE(S) out of {len(fns)}")
        sys.exit(1)
    print(f"✓ ALL {len(fns)} TESTS PASSED")
