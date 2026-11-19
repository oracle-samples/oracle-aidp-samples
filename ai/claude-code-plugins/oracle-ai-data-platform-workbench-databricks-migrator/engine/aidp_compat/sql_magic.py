"""
AIDP SQL Magic Handler
======================
Handles Databricks %sql magic commands by converting them to spark.sql() calls.
Addresses the 930 dbutils issues + many spark_compat issues.

Usage:
    # In migrated notebooks, replace:
    #   %sql SELECT * FROM table
    # With:
    #   sql("SELECT * FROM table")

    # Or use the cell transformer to auto-convert
"""

import re


def sql(query: str, spark=None):
    """Execute a SQL query and display results.

    Drop-in replacement for Databricks %sql magic.
    """
    if spark is None:
        try:
            from pyspark.sql import SparkSession
            spark = SparkSession.builder.getOrCreate()
        except Exception:
            raise RuntimeError("SparkSession not available")

    result = spark.sql(query)

    # Auto-display in Jupyter
    try:
        from IPython import get_ipython
        if get_ipython() is not None:
            from aidp_compat.display import display
            display(result)
            return result
    except ImportError:
        pass

    result.show(100, truncate=False)
    return result


def transform_sql_cells(notebook_json: dict) -> dict:
    """Transform %sql cells in a notebook to spark.sql() calls.

    This processes a notebook JSON and converts:
    - %sql ... -> spark.sql("...").show()
    - %python ... -> (strip the magic, leave code)
    - %sh ... -> subprocess.run(...)
    - %pip install ... -> subprocess.check_call([...])
    """
    import copy
    nb = copy.deepcopy(notebook_json)

    for cell in nb.get("cells", []):
        if cell.get("cell_type") != "code":
            continue

        source = "".join(cell.get("source", []))
        lines = source.split("\n")

        if not lines:
            continue

        first_line = lines[0].strip()

        # %sql magic
        if first_line.startswith("%sql"):
            sql_query = first_line[4:].strip()
            if len(lines) > 1:
                sql_query += "\n" + "\n".join(lines[1:])
            sql_query = sql_query.strip()

            # Escape quotes in the SQL
            escaped = sql_query.replace('\\', '\\\\').replace('"', '\\"')
            new_source = f'spark.sql("""{sql_query}""").show(100, truncate=False)'
            cell["source"] = [new_source]

        # %python magic (just strip the magic)
        elif first_line.startswith("%python"):
            remaining = first_line[7:].strip()
            if remaining:
                lines[0] = remaining
            else:
                lines = lines[1:]
            cell["source"] = ["\n".join(lines)]

        # %sh magic
        elif first_line.startswith("%sh"):
            cmd = first_line[3:].strip()
            if len(lines) > 1:
                cmd += "\n" + "\n".join(lines[1:])
            cmd = cmd.strip()
            escaped = cmd.replace("'", "\\'")
            new_source = f"import subprocess\nsubprocess.run('{escaped}', shell=True, check=True)"
            cell["source"] = [new_source]

        # %pip magic
        elif first_line.startswith("%pip"):
            pip_args = first_line[4:].strip()
            new_source = f"import subprocess, sys\nsubprocess.check_call([sys.executable, '-m', 'pip', {', '.join(repr(a) for a in pip_args.split())}])"
            cell["source"] = [new_source]

        # %scala / %r - not supported
        elif first_line.startswith("%scala") or first_line.startswith("%r"):
            cell["source"] = [
                f"# WARNING: {first_line.split()[0]} magic is not supported on AIDP\n"
                f"# Original code:\n"
                f"# " + "\n# ".join(lines)
            ]

    return nb
