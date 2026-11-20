"""Conservative, deterministic compatibility fixes for notebook source.

These transforms change delivered notebook code, unlike execution-only table
redirects. Unrecognized implementations must be left untouched.
"""

from __future__ import annotations

import ast
import copy
import re
import textwrap


# Match the complete known implementation, not just its function name or SQL.
# In particular, do not replace customized filtering or timestamp handling.
_LATEST_PARTITION_HELPER = '''\
def readIntermediateTable(table_name, database_name="intermediate_tables", timestamp=None):
    try:
        if timestamp:
            timestamp = str(int(timestamp))
            df = spark.sql(
                """select * from {database_name}.{table_name}
                where partitioning_timestamp = "{timestamp}"
                """.format(
                    timestamp=timestamp, database_name=database_name, table_name=table_name
                )
            )
            df = df.drop("partitioning_timestamp")
            return df
        else:
            df = spark.sql(
                """select * from {database_name}.{table_name}
                where partitioning_timestamp in
                (select max(partitioning_timestamp) from {database_name}.{table_name})""".format(
                    timestamp=timestamp, database_name=database_name, table_name=table_name
                )
            )
            df = df.drop("partitioning_timestamp")
            return df
    except Exception as e:
        print("Error while reading intermediate table", e)
        raise e
'''

_PREVIOUS_STORAGE_PARTITION_BODY = '''\
# Oracle tool modification: choose the latest nonempty partition from storage metadata.
from urllib.parse import unquote
_aidp_table = f"{database_name}.{table_name}"
_aidp_resolve = globals().get("_aidp_rr_resolve")
if callable(_aidp_resolve):
    _aidp_table = _aidp_resolve(_aidp_table)
_aidp_quoted_table = ".".join("`" + part.replace("`", "``") + "`" for part in _aidp_table.split("."))
df = spark.table(_aidp_table)
_aidp_type = df.schema["partitioning_timestamp"].dataType.typeName()
if _aidp_type not in ("string", "byte", "short", "integer", "long"):
    raise TypeError("readIntermediateTable metadata lookup requires a string or integer partitioning_timestamp")
_aidp_details = {row[0]: row[1] for row in spark.sql(f"DESCRIBE TABLE EXTENDED {_aidp_quoted_table}").collect()}
if _aidp_details.get("Provider", "").lower() != "parquet" or not _aidp_details.get("Location"):
    raise ValueError("readIntermediateTable requires a Parquet table with a catalog storage location")
_aidp_root = spark._jvm.org.apache.hadoop.fs.Path(_aidp_details["Location"])
_aidp_fs = _aidp_root.getFileSystem(spark._jsc.hadoopConfiguration())
_aidp_candidates = []
for _aidp_entry in _aidp_fs.listStatus(_aidp_root):
    _aidp_name = _aidp_entry.getPath().getName()
    if _aidp_entry.isDirectory() and _aidp_name.startswith("partitioning_timestamp="):
        _aidp_value = unquote(_aidp_name.split("=", 1)[1])
        if _aidp_value != "__HIVE_DEFAULT_PARTITION__":
            _aidp_candidates.append((_aidp_value if _aidp_type == "string" else int(_aidp_value), _aidp_entry.getPath()))
    elif not _aidp_name.startswith(("_", ".")):
        raise ValueError("readIntermediateTable requires partitioning only by partitioning_timestamp")
_aidp_latest_partition = None
_aidp_latest_path = None
for _aidp_value, _aidp_path in sorted(_aidp_candidates, key=lambda item: item[0], reverse=True):
    _aidp_files = _aidp_fs.listFiles(_aidp_path, True)
    while _aidp_files.hasNext():
        _aidp_file_path = _aidp_files.next().getPath().toString()
        _aidp_relative_file = _aidp_file_path[len(_aidp_path.toString().rstrip("/")) + 1:]
        # Spark ignores hidden directories as well as hidden filenames.
        if (all(not part.startswith(("_", ".")) for part in _aidp_relative_file.split("/"))
                and _aidp_relative_file.endswith(".parquet")):
            _aidp_latest_partition = _aidp_value
            _aidp_latest_path = _aidp_path.toString()
            break
    if _aidp_latest_partition is not None:
        break
print(f"Oracle tool modification: readIntermediateTable selected partition {_aidp_latest_partition!r} from storage metadata for {_aidp_table}")
if _aidp_latest_path is None:
    return df.limit(0).drop("partitioning_timestamp")
# The AIDP catalog reader mis-types the partition during pruning. Preserve
# the catalog data schema and read only the selected physical partition.
return spark.read.schema(df.drop("partitioning_timestamp").schema).parquet(_aidp_latest_path)
'''


_LATEST_PARTITION_BODY = '''\
# Oracle tool modification: choose the latest nonempty partition from storage metadata.
from urllib.parse import unquote
from itertools import groupby
_aidp_table = f"{database_name}.{table_name}"
_aidp_resolve = globals().get("_aidp_rr_resolve")
if callable(_aidp_resolve):
    _aidp_table = _aidp_resolve(_aidp_table)
_aidp_quoted_table = ".".join("`" + part.replace("`", "``") + "`" for part in _aidp_table.split("."))
df = spark.table(_aidp_table)
_aidp_type = df.schema["partitioning_timestamp"].dataType.typeName()
if _aidp_type not in ("string", "byte", "short", "integer", "long"):
    raise TypeError("readIntermediateTable metadata lookup requires a string or integer partitioning_timestamp")
_aidp_details = {row[0]: row[1] for row in spark.sql(f"DESCRIBE TABLE EXTENDED {_aidp_quoted_table}").collect()}
if _aidp_details.get("Provider", "").lower() != "parquet" or not _aidp_details.get("Location"):
    raise ValueError("readIntermediateTable requires a Parquet table with a catalog storage location")
_aidp_root = spark._jvm.org.apache.hadoop.fs.Path(_aidp_details["Location"])
_aidp_fs = _aidp_root.getFileSystem(spark._jsc.hadoopConfiguration())
_aidp_candidates = []
for _aidp_entry in _aidp_fs.listStatus(_aidp_root):
    _aidp_name = _aidp_entry.getPath().getName()
    if _aidp_entry.isDirectory() and _aidp_name.startswith("partitioning_timestamp="):
        _aidp_value = unquote(_aidp_name.split("=", 1)[1])
        if _aidp_value != "__HIVE_DEFAULT_PARTITION__":
            _aidp_candidates.append((_aidp_value if _aidp_type == "string" else int(_aidp_value), _aidp_entry.getPath()))
    elif not _aidp_name.startswith(("_", ".")):
        raise ValueError("readIntermediateTable requires partitioning only by partitioning_timestamp")
# Reuse Spark's file index instead of doing a separate recursive filesystem walk.
# This preserves its hidden-file rules and includes files with any filename suffix.
_aidp_schema = df.drop("partitioning_timestamp").schema
for _aidp_latest_partition, _aidp_group in groupby(
        sorted(_aidp_candidates, key=lambda item: item[0], reverse=True), key=lambda item: item[0]):
    # All physical spellings of the same typed value belong to one partition.
    _aidp_paths = [item[1].toString() for item in _aidp_group]
    _aidp_selected = spark.read.schema(_aidp_schema).parquet(*_aidp_paths)
    if _aidp_selected.inputFiles() and not _aidp_selected.isEmpty():
        print(f"Oracle tool modification: readIntermediateTable selected partition {_aidp_latest_partition!r} from storage metadata for {_aidp_table}")
        return _aidp_selected
print(f"Oracle tool modification: readIntermediateTable found no data files for {_aidp_table}")
return df.limit(0).drop("partitioning_timestamp")
'''


# Recognition-only fixture for notebooks saved before row-presence validation.
_PREVIOUS_SPARK_INDEX_BODY = '''\
# Oracle tool modification: choose the latest nonempty partition from storage metadata.
from urllib.parse import unquote
from itertools import groupby
_aidp_table = f"{database_name}.{table_name}"
_aidp_resolve = globals().get("_aidp_rr_resolve")
if callable(_aidp_resolve):
    _aidp_table = _aidp_resolve(_aidp_table)
_aidp_quoted_table = ".".join("`" + part.replace("`", "``") + "`" for part in _aidp_table.split("."))
df = spark.table(_aidp_table)
_aidp_type = df.schema["partitioning_timestamp"].dataType.typeName()
if _aidp_type not in ("string", "byte", "short", "integer", "long"):
    raise TypeError("readIntermediateTable metadata lookup requires a string or integer partitioning_timestamp")
_aidp_details = {row[0]: row[1] for row in spark.sql(f"DESCRIBE TABLE EXTENDED {_aidp_quoted_table}").collect()}
if _aidp_details.get("Provider", "").lower() != "parquet" or not _aidp_details.get("Location"):
    raise ValueError("readIntermediateTable requires a Parquet table with a catalog storage location")
_aidp_root = spark._jvm.org.apache.hadoop.fs.Path(_aidp_details["Location"])
_aidp_fs = _aidp_root.getFileSystem(spark._jsc.hadoopConfiguration())
_aidp_candidates = []
for _aidp_entry in _aidp_fs.listStatus(_aidp_root):
    _aidp_name = _aidp_entry.getPath().getName()
    if _aidp_entry.isDirectory() and _aidp_name.startswith("partitioning_timestamp="):
        _aidp_value = unquote(_aidp_name.split("=", 1)[1])
        if _aidp_value != "__HIVE_DEFAULT_PARTITION__":
            _aidp_candidates.append((_aidp_value if _aidp_type == "string" else int(_aidp_value), _aidp_entry.getPath()))
    elif not _aidp_name.startswith(("_", ".")):
        raise ValueError("readIntermediateTable requires partitioning only by partitioning_timestamp")
# Reuse Spark's file index instead of doing a separate recursive filesystem walk.
# This preserves its hidden-file rules and includes files with any filename suffix.
_aidp_schema = df.drop("partitioning_timestamp").schema
for _aidp_latest_partition, _aidp_group in groupby(
        sorted(_aidp_candidates, key=lambda item: item[0], reverse=True), key=lambda item: item[0]):
    # All physical spellings of the same typed value belong to one partition.
    _aidp_paths = [item[1].toString() for item in _aidp_group]
    _aidp_selected = spark.read.schema(_aidp_schema).parquet(*_aidp_paths)
    if _aidp_selected.inputFiles():
        print(f"Oracle tool modification: readIntermediateTable selected partition {_aidp_latest_partition!r} from storage metadata for {_aidp_table}")
        return _aidp_selected
print(f"Oracle tool modification: readIntermediateTable found no data files for {_aidp_table}")
return df.limit(0).drop("partitioning_timestamp")
'''


def _helper_shape(node: ast.FunctionDef) -> str:
    """Ignore documentation and SQL layout, preserving all executable structure."""
    node = copy.deepcopy(node)
    if ast.get_docstring(node) is not None:
        node.body = node.body[1:]
    for child in ast.walk(node):
        if isinstance(child, ast.Constant) and isinstance(child.value, str):
            if child.value.lstrip().lower().startswith("select * from "):
                child.value = re.sub(r"\s+", " ", child.value).strip().lower()
                # The model changes the explicit timestamp's SQL quotes during
                # migration. That untouched branch must not prevent repairing
                # the latest-partition branch.
                if child.value == (
                    "select * from {database_name}.{table_name} "
                    "where partitioning_timestamp = '{timestamp}'"
                ):
                    child.value = child.value.replace("'{timestamp}'", '\"{timestamp}\"')
    return ast.dump(node, include_attributes=False)


_original_helper = ast.parse(_LATEST_PARTITION_HELPER).body[0]
_KNOWN_HELPER_SHAPES = {_helper_shape(_original_helper)}
for _old_body in (_PREVIOUS_STORAGE_PARTITION_BODY, _PREVIOUS_SPARK_INDEX_BODY):
    _previous_helper = copy.deepcopy(_original_helper)
    _previous_helper.body[0].body[0].orelse = ast.parse(_old_body).body
    _KNOWN_HELPER_SHAPES.add(_helper_shape(_previous_helper))


def rewrite_latest_partition_helper(source: str) -> tuple[str, bool]:
    """Repair the known SaveTableUtils helper, preserving other source verbatim.

    Only top-level Python functions matching the complete known AST qualify.
    Replace just the latest-partition branch; the explicit timestamp branch,
    docstring, exception handling, and surrounding cells remain unchanged.
    Upgrade the previous known repair too. Magics, non-Python cells, and
    functions already containing the current repair are no-ops.
    """
    if "readIntermediateTable" not in source:
        return source, False
    try:
        tree = ast.parse(source)
    except (SyntaxError, ValueError):
        return source, False

    lines = source.splitlines(keepends=True)
    edits = []
    for node in tree.body:
        if not isinstance(node, ast.FunctionDef) or node.name != "readIntermediateTable":
            continue
        if _helper_shape(node) not in _KNOWN_HELPER_SHAPES:
            continue
        body = node.body[1:] if ast.get_docstring(node) is not None else node.body
        branch = body[0].body[0].orelse
        first, last = branch[0], branch[-1]
        # Preserve indentation, including tabs, rather than using AST byte offsets.
        original_line = lines[first.lineno - 1]
        indent = original_line[:len(original_line) - len(original_line.lstrip())]
        newline = "\r\n" if original_line.endswith("\r\n") else "\n"
        original_branch = "".join(lines[first.lineno - 1:last.end_lineno])
        preserved = indent + "# Oracle tool modification: original latest-partition branch preserved below.\n"
        preserved += "".join(indent + "# " + (line[len(indent):] if line.startswith(indent) else line.lstrip()) + "\n"
                             for line in original_branch.splitlines())
        replacement = (preserved + textwrap.indent(_LATEST_PARTITION_BODY, indent)).replace("\n", newline)
        if not lines[last.end_lineno - 1].endswith("\n"):
            replacement = replacement.rstrip("\r\n")
        edits.append((first.lineno - 1, last.end_lineno, replacement))

    for start, end, replacement in reversed(edits):
        lines[start:end] = [replacement]
    return "".join(lines), bool(edits)
