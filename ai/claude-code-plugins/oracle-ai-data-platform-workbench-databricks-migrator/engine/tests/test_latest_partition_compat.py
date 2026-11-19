"""Behavioral tests for the SaveTableUtils compatibility conversion."""

import ast
from pathlib import Path
import sys
from types import SimpleNamespace
from unittest.mock import Mock
from urllib.parse import quote, unquote

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
from notebook_compat import rewrite_latest_partition_helper
import job_migrate as jm


# Independent fixture matching the exported dependency, including both branches.
ORIGINAL_HELPER = '''\
def readIntermediateTable(
    table_name, database_name="intermediate_tables", timestamp=None
):
    """Read an explicit partition, or the latest partition by default."""
    try:
        if timestamp:
            timestamp = str(int(timestamp))
            df = spark.sql(
                """
      select * from {database_name}.{table_name}
      where partitioning_timestamp = "{timestamp}"
      """.format(
                    timestamp=timestamp,
                    database_name=database_name,
                    table_name=table_name,
                )
            )
            df = df.drop("partitioning_timestamp")
            return df
        else:
            df = spark.sql(
                """
      select * from {database_name}.{table_name}
      where partitioning_timestamp in (select max(partitioning_timestamp) from {database_name}.{table_name})
      """.format(
                    timestamp=timestamp,
                    database_name=database_name,
                    table_name=table_name,
                )
            )
            df = df.drop("partitioning_timestamp")
            return df
    except Exception as e:
        print("Error while reading intermediate table", e)
        raise e
'''


def load_rewritten(spark, source=ORIGINAL_HELPER):
    rewritten, changed = rewrite_latest_partition_helper(source)
    assert changed
    namespace = {"spark": spark}
    exec(compile(rewritten, "SaveTableUtils.ipynb", "exec"), namespace)
    return namespace["readIntermediateTable"]


class Frame:
    """Small dataframe model exercising aggregation, filtering, and projection."""

    def __init__(self, rows):
        self.rows = rows

    @property
    def schema(self):
        values = [r.get("partitioning_timestamp") for r in self.rows
                  if r.get("partitioning_timestamp") is not None]
        kind = "long" if values and isinstance(values[0], int) else "string"
        return {"partitioning_timestamp": SimpleNamespace(dataType=SimpleNamespace(typeName=lambda: kind))}

    def isEmpty(self):
        return not self.rows

    def inputFiles(self):
        return ["part-000"] if self.rows else []

    def agg(self, expressions):
        raise AssertionError("Latest partition must not scan table rows")

    def __getitem__(self, column):
        class Column:
            def __eq__(self, value):
                return lambda row: row[column] == value
        return Column()

    def filter(self, condition):
        return Frame([row for row in self.rows if condition(row)])

    def limit(self, count):
        return Frame(self.rows[:count])

    def drop(self, column):
        return Frame([{k: v for k, v in row.items() if k != column} for row in self.rows])


class FileIterator:
    def __init__(self, names, root):
        self.root = root
        self.names = iter(names)
        self.current = next(self.names, None)

    def hasNext(self):
        return self.current is not None

    def next(self):
        name = self.current
        self.current = next(self.names, None)
        return SimpleNamespace(getPath=lambda: SimpleNamespace(
            getName=lambda: name.rsplit("/", 1)[-1], toString=lambda: self.root + "/" + name))


def storage_entry(name):
    path = SimpleNamespace(getName=lambda: name, toString=lambda: "file:/table/" + name)
    return SimpleNamespace(getPath=lambda: path, isDirectory=lambda: True)


def mock_spark(rows):
    spark = Mock()
    spark.table.return_value = Frame(rows)
    specs = {"partitioning_timestamp=" + ("__HIVE_DEFAULT_PARTITION__" if r["partitioning_timestamp"] is None
             else quote(str(r["partitioning_timestamp"]), safe="")) for r in rows}
    spark.sql.return_value.collect.return_value = [("Provider", "parquet"), ("Location", "file:/table")]
    fs = spark._jvm.org.apache.hadoop.fs.Path.return_value.getFileSystem.return_value
    fs.listStatus.return_value = [storage_entry(spec) for spec in sorted(specs)]
    fs.listFiles.side_effect = lambda path, recursive: FileIterator(["part-000.parquet"], path.toString())
    def read_partition(*paths):
        values = {unquote(path.rsplit("=", 1)[1]) for path in paths}
        selected = [r for r in rows if str(r["partitioning_timestamp"]) in values]
        return Frame(selected).drop("partitioning_timestamp")
    spark.read.schema.return_value.parquet.side_effect = read_partition
    return spark


@pytest.mark.parametrize("partitions,expected", [
    ([10, 20, 20, None], [1, 2]),
    (["10", "20", "20", None], [1, 2]),
    ([None, None], []),
    ([], []),
])
def test_latest_partition_results(partitions, expected):
    rows = [{"value": i, "partitioning_timestamp": p} for i, p in enumerate(partitions)]
    spark = mock_spark(rows)
    result = load_rewritten(spark)("events", database_name="intermediate_tables")
    assert result.rows == [{"value": i} for i in expected]
    spark.table.assert_called_once_with("intermediate_tables.events")
    spark.sql.assert_called_once_with("DESCRIBE TABLE EXTENDED `intermediate_tables`.`events`")
    spark.conf.set.assert_not_called()


def test_explicit_timestamp_branch_is_preserved():
    spark = Mock()
    spark.sql.return_value = Frame([{"value": "kept", "partitioning_timestamp": "123"}])
    result = load_rewritten(spark)("events", timestamp=123.9)
    assert result.rows == [{"value": "kept"}]
    spark.table.assert_not_called()
    spark.conf.set.assert_not_called()
    query = " ".join(spark.sql.call_args.args[0].split())
    assert query == 'select * from intermediate_tables.events where partitioning_timestamp = "123"'


@pytest.mark.parametrize("quote", ["'", '"'])
def test_model_changed_timestamp_quotes_do_not_block_latest_repair(quote):
    source = ORIGINAL_HELPER.replace('= "{timestamp}"', f'= {quote}{{timestamp}}{quote}')
    rewritten, changed = rewrite_latest_partition_helper(source)
    assert changed
    original_function = ast.parse(source).body[0]
    repaired_function = ast.parse(rewritten).body[0]
    # The explicit branch is preserved exactly in the parsed executable code.
    original_branch = original_function.body[1].body[0].body
    repaired_branch = repaired_function.body[1].body[0].body
    assert ast.dump(ast.Module(body=original_branch, type_ignores=[])) == ast.dump(
        ast.Module(body=repaired_branch, type_ignores=[]))
    spark = mock_spark([
        {"value": 1, "partitioning_timestamp": 10},
        {"value": 2, "partitioning_timestamp": 20},
    ])
    helper = load_rewritten(spark, source)
    assert helper("events").rows == [{"value": 2}]
    spark.sql.return_value = Frame([{"value": 1, "partitioning_timestamp": 10}])
    assert helper("events", timestamp=10).rows == [{"value": 1}]
    assert f'= {quote}10{quote}' in spark.sql.call_args.args[0]


def test_exceptions_propagate():
    spark = Mock()
    spark.table.side_effect = RuntimeError("read denied")
    with pytest.raises(RuntimeError, match="read denied"):
        load_rewritten(spark)("events")


def test_metadata_failure_is_not_hidden():
    spark = mock_spark([])
    spark.sql.side_effect = RuntimeError("metadata unavailable")
    with pytest.raises(RuntimeError, match="metadata unavailable"):
        load_rewritten(spark)("events")


@pytest.mark.parametrize("old_body_name", ["_PREVIOUS_STORAGE_PARTITION_BODY", "_PREVIOUS_SPARK_INDEX_BODY"])
def test_previous_repair_is_upgraded_without_changing_custom_helpers(old_body_name):
    import notebook_compat
    old_body = getattr(notebook_compat, old_body_name)
    tree = ast.parse(ORIGINAL_HELPER)
    tree.body[0].body[1].body[0].orelse = ast.parse(old_body).body
    previous = ast.unparse(tree)
    rewritten, changed = rewrite_latest_partition_helper(previous)
    assert changed
    spark = mock_spark([{"value": 1, "partitioning_timestamp": 20}])
    namespace = {"spark": spark}
    exec(compile(rewritten, "previous repair", "exec"), namespace)
    assert namespace["readIntermediateTable"]("events").rows == [{"value": 1}]
    spark.conf.set.assert_not_called()
    assert rewrite_latest_partition_helper(rewritten) == (rewritten, False)
    custom = previous.replace("df.limit(0)", "df.limit(5)")
    assert rewrite_latest_partition_helper(custom) == (custom, False)


def test_preserves_other_source_and_is_idempotent():
    prefix = '# existing notebook comment\nlabel = "café"\n'
    suffix = '\n# caller remains unchanged\nresult = readIntermediateTable("events")\n'
    rewritten, changed = rewrite_latest_partition_helper(prefix + ORIGINAL_HELPER + suffix)
    assert changed
    assert rewritten.startswith(prefix)
    assert rewritten.endswith(suffix)
    assert ast.get_docstring(ast.parse(rewritten).body[1]) == ast.get_docstring(ast.parse(ORIGINAL_HELPER).body[0])
    assert rewrite_latest_partition_helper(rewritten) == (rewritten, False)


@pytest.mark.parametrize("source", [
    ORIGINAL_HELPER.replace('df = df.drop("partitioning_timestamp")',
                            'df = df.drop("partitioning_timestamp").limit(10)'),
    ORIGINAL_HELPER.replace('timestamp = str(int(timestamp))', 'timestamp = str(timestamp)'),
    ORIGINAL_HELPER.replace('database_name="intermediate_tables"', 'database_name="custom"'),
    '@custom_decorator\n' + ORIGINAL_HELPER,
    'class Utils:\n' + '\n'.join('    ' + line for line in ORIGINAL_HELPER.splitlines()),
    'def readIntermediateTable(table_name):\n    return custom_reader(table_name)\n',
    '%python\n' + ORIGINAL_HELPER,
    'readIntermediateTable(',
])
def test_unrecognized_implementations_are_unchanged(source):
    assert rewrite_latest_partition_helper(source) == (source, False)


def test_sql_layout_and_case_do_not_prevent_matching():
    source = ORIGINAL_HELPER.replace('select * from', 'SELECT * FROM')
    assert rewrite_latest_partition_helper(source)[1]


@pytest.mark.parametrize("newline", ["\n", "\r\n"])
def test_line_endings_and_missing_final_newline(newline):
    source = ORIGINAL_HELPER.rstrip("\n").replace("\n", newline)
    rewritten, changed = rewrite_latest_partition_helper(source)
    assert changed
    assert not rewritten.endswith("\n")
    if newline == "\r\n":
        assert "\n" not in rewritten.replace("\r\n", "")
    compile(rewritten, "SaveTableUtils.ipynb", "exec")


@pytest.mark.parametrize("engine", ["scripts"])
def test_engine_wires_conversion_before_saving_and_code_only_branch(engine):
    """The same source must be delivered in code-only and execution modes."""
    source = (Path(__file__).resolve().parents[1] / engine / "job_migrate.py").read_text()
    tree = ast.parse(source)
    process = next(n for n in tree.body if isinstance(n, ast.AsyncFunctionDef)
                   and n.name == "process_notebook")
    finalizer = next(n for n in tree.body if isinstance(n, ast.FunctionDef)
                     and n.name == "_finalize_migrated_cell")
    hook = next(n for n in ast.walk(finalizer) if isinstance(n, ast.If)
                and ast.unparse(n.test) == "not DIRECT_EXECUTE")
    assert any(isinstance(n, ast.Call) and isinstance(n.func, ast.Name)
               and n.func.id == "rewrite_latest_partition_helper" for n in ast.walk(hook))
    calls = [n for n in ast.walk(process) if isinstance(n, ast.Call)
             and isinstance(n.func, ast.Name) and n.func.id == "_finalize_migrated_cell"]
    assert len(calls) == 2  # Normal execution and stopped-validation code-only continuation.
    for target in ("migrated_cell['source']", "_co_cell['source']"):
        save = next(n for n in ast.walk(process) if isinstance(n, ast.Assign)
                    and ast.unparse(n.targets[0]) == target)
        assert any(call.lineno < save.lineno for call in calls)



@pytest.mark.parametrize("direct", [False, True])
@pytest.mark.parametrize("quote", ["'", '"'])
def test_child_execution_receives_repaired_source_and_opens_save_gate(direct, quote):
    """Exercise the engine's real child preparation block, without cloud I/O."""
    source = (Path(__file__).resolve().parents[1] / "scripts/job_migrate.py").read_text()
    tree = ast.parse(source)
    child = next(n for n in tree.body if isinstance(n, ast.AsyncFunctionDef)
                 and n.name == "_inline_child_notebook")
    hook = next(n for n in ast.walk(child) if isinstance(n, ast.If)
                and ast.unparse(n.test) == "not DIRECT_EXECUTE"
                and any(isinstance(c, ast.Call) and isinstance(c.func, ast.Name)
                        and c.func.id == "rewrite_latest_partition_helper" for c in ast.walk(n)))
    current = next(n for n in ast.walk(child) if isinstance(n, ast.Assign)
                   and ast.unparse(n.targets[0]) == "current_code"
                   and ast.unparse(n.value) == "child_source")
    assert hook.lineno < current.lineno
    fixture = ORIGINAL_HELPER.replace('= "{timestamp}"', f'= {quote}{{timestamp}}{quote}')
    namespace = {
        "child_source": fixture, "DIRECT_EXECUTE": direct,
        "rewrite_latest_partition_helper": rewrite_latest_partition_helper,
        "cells_fixed": 0, "child_source_rewritten": False,
        "log": Mock(), "child_name": "SaveTableUtils.ipynb", "ci": 4,
    }
    exec(compile(ast.Module(body=[hook, current], type_ignores=[]), "child preparation", "exec"), namespace)
    if direct:
        assert namespace["current_code"] == fixture
        assert namespace["cells_fixed"] == 0
        namespace["log"].log.assert_not_called()
    else:
        assert "_aidp_latest_partition" in namespace["current_code"]
        assert namespace["current_code"] == namespace["child_source"]
        assert namespace["cells_fixed"] > 0 or namespace["child_source_rewritten"]
        namespace["log"].log.assert_called_once()


@pytest.mark.parametrize("values,expected", [(["9", "10"], "9"), ([9, 10], 10), (["a/b", "a=z"], "a=z")])
def test_metadata_ordering_and_escaped_values(values, expected):
    spark = mock_spark([{"value": v, "partitioning_timestamp": v} for v in values])
    assert load_rewritten(spark)("events").rows == [{"value": expected}]


def test_metadata_and_table_read_use_same_runtime_resolver():
    spark = mock_spark([{"value": 1, "partitioning_timestamp": "20"}])
    source, _ = rewrite_latest_partition_helper(ORIGINAL_HELPER)
    resolve = Mock(return_value="sandbox.intermediate_tables_events")
    ns = {"spark": spark, "_aidp_rr_resolve": resolve}
    exec(source, ns)
    assert ns["readIntermediateTable"]("events").rows == [{"value": 1}]
    resolve.assert_called_once_with("intermediate_tables.events")
    spark.table.assert_called_once_with("sandbox.intermediate_tables_events")
    spark.sql.assert_called_once_with("DESCRIBE TABLE EXTENDED `sandbox`.`intermediate_tables_events`")


def test_unexpected_partition_spec_fails_without_scanning():
    spark = mock_spark([])
    fs = spark._jvm.org.apache.hadoop.fs.Path.return_value.getFileSystem.return_value
    fs.listStatus.return_value = [storage_entry("other=20")]
    with pytest.raises(ValueError, match="requires partitioning"):
        load_rewritten(spark)("events")



def test_empty_latest_directory_is_ignored():
    spark = mock_spark([{"value": 1, "partitioning_timestamp": "10"}])
    fs = spark._jvm.org.apache.hadoop.fs.Path.return_value.getFileSystem.return_value
    fs.listStatus.return_value.append(storage_entry("partitioning_timestamp=20"))
    assert load_rewritten(spark)("events").rows == [{"value": 1}]
    assert [call.args for call in spark.read.schema.return_value.parquet.call_args_list] == [
        ("file:/table/partitioning_timestamp=20",), ("file:/table/partitioning_timestamp=10",)]
    fs.listFiles.assert_not_called()


def test_storage_error_is_not_hidden():
    spark = mock_spark([])
    fs = spark._jvm.org.apache.hadoop.fs.Path.return_value.getFileSystem.return_value
    fs.listStatus.side_effect = RuntimeError("storage access denied")
    with pytest.raises(RuntimeError, match="storage access denied"):
        load_rewritten(spark)("events")


def test_unsupported_provider_is_not_treated_as_empty():
    spark = mock_spark([])
    spark.sql.return_value.collect.return_value = [("Provider", "delta"), ("Location", "file:/table")]
    with pytest.raises(ValueError, match="requires a Parquet table"):
        load_rewritten(spark)("events")



def test_selected_partition_is_read_directly_with_catalog_data_schema():
    spark = mock_spark([{"value": 1, "partitioning_timestamp": "10"},
                        {"value": 2, "partitioning_timestamp": "20"}])
    assert load_rewritten(spark)("events").rows == [{"value": 2}]
    spark.read.schema.assert_called_once()
    spark.read.schema.return_value.parquet.assert_called_once_with("file:/table/partitioning_timestamp=20")
    spark.conf.set.assert_not_called()


def test_hidden_parent_directory_is_not_evidence_of_a_complete_partition():
    spark = mock_spark([{"value": 1, "partitioning_timestamp": "10"}])
    fs = spark._jvm.org.apache.hadoop.fs.Path.return_value.getFileSystem.return_value
    fs.listStatus.return_value.append(storage_entry("partitioning_timestamp=20"))
    # The file index returns no visible files for the newer partition.
    assert load_rewritten(spark)("events").rows == [{"value": 1}]
    fs.listFiles.assert_not_called()
    assert spark.read.schema.return_value.parquet.call_count == 2


@pytest.mark.parametrize("filename", ["part-000", "part-000.bin", "part-000.PARQUET"])
def test_visible_file_qualification_does_not_guess_format_from_suffix(filename):
    spark = mock_spark([{"value": 1, "partitioning_timestamp": "20"}])
    fs = spark._jvm.org.apache.hadoop.fs.Path.return_value.getFileSystem.return_value
    frame = Frame([{"value": 1}])
    frame.inputFiles = lambda: [filename]
    spark.read.schema.return_value.parquet.side_effect = lambda *paths: frame
    assert load_rewritten(spark)("events").rows == [{"value": 1}]
    fs.listFiles.assert_not_called()


def test_equivalent_integer_directories_are_read_together_without_scanning_older_values():
    spark = mock_spark([{"value": 42, "partitioning_timestamp": 1},
                        {"value": 99, "partitioning_timestamp": 1}])
    fs = spark._jvm.org.apache.hadoop.fs.Path.return_value.getFileSystem.return_value
    fs.listStatus.return_value = [storage_entry("partitioning_timestamp=" + v) for v in ("0", "01", "1")]
    helper = load_rewritten(spark)
    helper("events")
    paths = spark.read.schema.return_value.parquet.call_args.args
    assert set(paths) == {"file:/table/partitioning_timestamp=01", "file:/table/partitioning_timestamp=1"}
    fs.listFiles.assert_not_called()
    assert spark.read.schema.return_value.parquet.call_count == 1


def test_visible_extensionless_read_failure_propagates_without_trying_older_partition():
    spark = mock_spark([{"value": 1, "partitioning_timestamp": "10"},
                        {"value": 2, "partitioning_timestamp": "20"}])
    fs = spark._jvm.org.apache.hadoop.fs.Path.return_value.getFileSystem.return_value
    fs.listFiles.side_effect = lambda path, recursive: FileIterator(["part-000"], path.toString())
    spark.read.schema.return_value.parquet.side_effect = RuntimeError("invalid Parquet footer")
    with pytest.raises(RuntimeError, match="invalid Parquet footer"):
        load_rewritten(spark)("events")
    fs.listFiles.assert_not_called()


def test_file_index_failure_is_not_treated_as_an_empty_partition():
    spark = mock_spark([{"value": 1, "partitioning_timestamp": "10"},
                        {"value": 2, "partitioning_timestamp": "20"}])
    frame = Frame([])
    frame.inputFiles = Mock(side_effect=RuntimeError("file index unavailable"))
    spark.read.schema.return_value.parquet.side_effect = lambda *paths: frame
    with pytest.raises(RuntimeError, match="file index unavailable"):
        load_rewritten(spark)("events")
    assert spark.read.schema.return_value.parquet.call_count == 1


def test_large_partition_inventory_does_not_walk_older_partitions():
    spark = mock_spark([{"value": 1, "partitioning_timestamp": 9999}])
    fs = spark._jvm.org.apache.hadoop.fs.Path.return_value.getFileSystem.return_value
    fs.listStatus.return_value = [storage_entry("partitioning_timestamp=" + str(i)) for i in range(10000)]
    assert load_rewritten(spark)("events").rows == [{"value": 1}]
    fs.listStatus.assert_called_once()
    fs.listFiles.assert_not_called()
    spark.read.schema.return_value.parquet.assert_called_once_with("file:/table/partitioning_timestamp=9999")


@pytest.mark.parametrize("mode", ["execute", "code_only", "after_gap"])
@pytest.mark.parametrize("direct", [False, True])
def test_real_cell_loop_saves_helper_repair_in_every_finalizer_path(monkeypatch, tmp_path, mode, direct):
    import asyncio
    from test_attempt_loop_terminal_result import _SequenceKernel, _error
    from test_conditional_exit_below_execute import _patch_common, _saved_sources
    from test_eval_verdict_gate import _notebook

    sources = (["BLOCKED_LOOKUP = missing_input()\n"] if mode == "after_gap" else []) + [ORIGINAL_HELPER]
    _patch_common(monkeypatch, tmp_path, _notebook(*sources))
    monkeypatch.setattr(jm, "DIRECT_EXECUTE", direct)
    monkeypatch.setattr(jm, "MAX_FIX_ATTEMPTS", 2)
    monkeypatch.setattr(jm, "_cell_history", [])
    monkeypatch.setattr(jm, "_current_cell_notes", [])

    async def fix(code, *args, **kwargs):
        jm._current_cell_notes.append("DATA_UNAVAIL: required catalog tables unavailable")
        return code

    monkeypatch.setattr(jm, "call_fix", fix)
    kernel = _SequenceKernel({"BLOCKED_LOOKUP": [_error("catalog lookup failed")]})
    result = asyncio.run(jm.process_notebook(
        "/Workspace/test_cond/source.ipynb", kernel, "test_partition_finalizer", "01_task", {},
        run_all=mode != "code_only"))
    saved, _ = _saved_sources(kernel)
    assert ("from itertools import groupby" in saved) is (not direct)
    assert 'where partitioning_timestamp' in saved  # Original source is retained.
    if mode == "after_gap":
        assert result["status"] != "PASS"
        executed = "\n".join(c for c in kernel.executed if "base64.b64decode" not in c)
        if not direct:
            assert "def readIntermediateTable" not in executed
    elif mode == "code_only":
        assert result["status"] == "CONVERTED_NOT_VALIDATED", result
    else:
        assert result["status"] == "PASS", result


def test_zero_row_file_does_not_hide_an_older_partition():
    spark = mock_spark([{"value": 42, "partitioning_timestamp": "10"}])
    fs = spark._jvm.org.apache.hadoop.fs.Path.return_value.getFileSystem.return_value
    fs.listStatus.return_value.append(storage_entry("partitioning_timestamp=20"))
    read = spark.read.schema.return_value.parquet.side_effect
    def read_with_empty_file(*paths):
        result = read(*paths)
        result.inputFiles = lambda: ["part-000.parquet"]
        return result
    spark.read.schema.return_value.parquet.side_effect = read_with_empty_file
    assert load_rewritten(spark)("events").rows == [{"value": 42}]
    assert spark.read.schema.return_value.parquet.call_count == 2


def test_row_presence_failure_propagates_without_selecting_older_rows():
    spark = mock_spark([{"value": 42, "partitioning_timestamp": "10"}])
    frame = Frame([{"value": 42}])
    frame.isEmpty = Mock(side_effect=RuntimeError("row probe failed"))
    spark.read.schema.return_value.parquet.side_effect = lambda *paths: frame
    with pytest.raises(RuntimeError, match="row probe failed"):
        load_rewritten(spark)("events")
    assert spark.read.schema.return_value.parquet.call_count == 1


# Historical test inputs without a verified persisted artifact are not rewrite targets.
_UNEVIDENCED_REPAIR_BODIES = [
    '''\
# Oracle tool modification: avoid a self-subquery when reading the latest partition.
df = spark.table(f"{database_name}.{table_name}")
_aidp_latest_partition = df.agg({"partitioning_timestamp": "max"}).first()[0]
if _aidp_latest_partition is None:
    df = df.limit(0)
else:
    df = df.filter(df["partitioning_timestamp"] == _aidp_latest_partition)
df = df.drop("partitioning_timestamp")
return df
''',
    '''\
# Oracle tool modification: use the non-vectorized Parquet reader for this session.
# Keep this disabled for the returned DataFrame's lazy execution as well.
spark.conf.set("spark.sql.parquet.enableVectorizedReader", "false")
print("Oracle tool modification: readIntermediateTable disabled vectorized Parquet reads for this Spark session")
# Oracle tool modification: avoid a self-subquery when reading the latest partition.
df = spark.table(f"{database_name}.{table_name}")
_aidp_latest_partition = df.agg({"partitioning_timestamp": "max"}).first()[0]
if _aidp_latest_partition is None:
    df = df.limit(0)
else:
    df = df.filter(df["partitioning_timestamp"] == _aidp_latest_partition)
df = df.drop("partitioning_timestamp")
return df
''',
    '''\
# Oracle tool modification: select the latest registered partition without scanning table data.
from urllib.parse import unquote
_aidp_table = f"{database_name}.{table_name}"
# Use the existing runtime resolver for BOTH metadata and data reads.
_aidp_resolve = globals().get("_aidp_rr_resolve")
if callable(_aidp_resolve):
    _aidp_table = _aidp_resolve(_aidp_table)
_aidp_quoted_table = ".".join("`" + part.replace("`", "``") + "`" for part in _aidp_table.split("."))
df = spark.table(_aidp_table)
_aidp_type = df.schema["partitioning_timestamp"].dataType.typeName()
if _aidp_type not in ("string", "byte", "short", "integer", "long"):
    raise TypeError("readIntermediateTable metadata lookup requires a string or integer partitioning_timestamp")
_aidp_values = []
for _aidp_row in spark.sql(f"SHOW PARTITIONS {_aidp_quoted_table}").collect():
    _aidp_spec = dict(part.split("=", 1) for part in _aidp_row[0].split("/"))
    if "partitioning_timestamp" not in _aidp_spec:
        raise ValueError("readIntermediateTable requires partitioning by partitioning_timestamp")
    _aidp_value = unquote(_aidp_spec["partitioning_timestamp"])
    if _aidp_value != "__HIVE_DEFAULT_PARTITION__":
        _aidp_values.append(_aidp_value if _aidp_type == "string" else int(_aidp_value))
_aidp_latest_partition = max(_aidp_values, default=None)
print(f"Oracle tool modification: readIntermediateTable selected partition {_aidp_latest_partition!r} from catalog metadata for {_aidp_table}")
if _aidp_latest_partition is None:
    df = df.limit(0)
else:
    df = df.filter(df["partitioning_timestamp"] == _aidp_latest_partition)
return df.drop("partitioning_timestamp")
''',
]

@pytest.mark.parametrize("old_body", _UNEVIDENCED_REPAIR_BODIES)
def test_unevidenced_repairs_are_left_for_the_model(old_body):
    tree = ast.parse(ORIGINAL_HELPER)
    tree.body[0].body[1].body[0].orelse = ast.parse(old_body).body
    source = ast.unparse(tree)
    assert rewrite_latest_partition_helper(source) == (source, False)


@pytest.mark.parametrize("newline", ["\n", "\r\n"])
def test_original_comment_preserves_less_indented_sql_lines(newline):
    source = ORIGINAL_HELPER.replace("\n", newline)
    branch = ast.parse(source).body[0].body[1].body[0].orelse
    lines = source.splitlines()
    original = lines[branch[0].lineno - 1:branch[-1].end_lineno]
    rewritten, changed = rewrite_latest_partition_helper(source)
    assert changed
    marker = "            # Oracle tool modification: original latest-partition branch preserved below."
    saved = rewritten.splitlines()
    start = saved.index(marker) + 1
    comments = saved[start:start + len(original)]
    assert [line.removeprefix("            # ").lstrip() for line in comments] == [
        line.lstrip() for line in original]
    assert any(line == "            # where partitioning_timestamp in (select max(partitioning_timestamp) from {database_name}.{table_name})" for line in comments)
    assert '            # """.format(' in comments
    compile(rewritten, "preserved helper", "exec")


def test_previous_spark_index_fixture_is_frozen():
    import hashlib
    import notebook_compat
    assert hashlib.sha256(notebook_compat._PREVIOUS_SPARK_INDEX_BODY.encode()).hexdigest() == '5ff86deeb3fa48b28d8738ce41e21baacb5a49a36ec736b5fc0a63f66cc3ae99'
