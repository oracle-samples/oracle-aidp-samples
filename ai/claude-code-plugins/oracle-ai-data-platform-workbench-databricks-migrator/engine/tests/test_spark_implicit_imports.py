"""Regression coverage for deterministic bare Spark-function imports."""

import os
import sys


sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scripts"))

import job_migrate as jm


def test_adds_bare_spark_sql_function_imports():
    source = '''def read_paths(paths):
    return (spark.read.parquet(*paths)
        .withColumn("dt", to_date(regexp_extract(input_file_name(), r"dt=(\\d+)", 1)))
    )
'''

    result = jm._add_missing_imports(source)

    assert "from pyspark.sql.functions import to_date" in result
    assert "from pyspark.sql.functions import regexp_extract" in result
    assert "from pyspark.sql.functions import input_file_name" in result


def test_respects_grouped_existing_spark_function_imports():
    source = '''from pyspark.sql.functions import input_file_name, regexp_extract, to_date

def read_paths(paths):
    return to_date(regexp_extract(input_file_name(), r"dt=(\\d+)", 1))
'''

    result = jm._add_missing_imports(source)

    assert result == source


def test_does_not_import_f_qualified_spark_functions():
    source = "result = F.to_date(F.regexp_extract(F.input_file_name(), 'x', 1))\n"

    result = jm._add_missing_imports(source)

    assert result == source


def test_does_not_shadow_function_defined_in_same_cell():
    source = '''def to_date(value):
    return f"customer:{value}"

result = to_date("2025-11-24")
'''

    result = jm._add_missing_imports(source)

    assert result == source


def test_does_not_shadow_function_defined_in_notebook_or_run_dependency():
    source = 'result = regexp_replace("customer-value")\n'
    symbol_scope = {"defined": {"regexp_replace"}}

    result = jm._add_missing_imports(source, symbol_scope)

    assert result == source


def test_string_literal_mentions_do_not_trigger_imports():
    source = '''query = "SELECT to_date(created_at) FROM source"
message = 'regexp_replace is documented here'
'''

    result = jm._add_missing_imports(source)

    assert result == source


# ── datetime-shadow regression (incident 2026-09-25) ──────────────────────────
# _add_missing_imports runs more than once per cell (dep code-only pass, then the
# parent %run inline pass). The datetime/date/timedelta rules inject a
# `from datetime import <name>` line; on a LATER pass the `datetime` token inside
# that injected line (or a customer's own `from datetime import date, timedelta`)
# must NOT be treated as usage and re-trigger the datetime rule — that injected
# `from datetime import datetime` into a cell that keeps `datetime` as the MODULE
# with no local `import datetime`, rebinding it to the class kernel-wide and
# breaking every module-style `datetime.datetime.*` downstream
# (Example_Escalations_V0 Staleness → 00_Parameters cell 4:
# `AttributeError: type object 'datetime.datetime' has no attribute 'datetime'`).


def test_injected_timedelta_import_does_not_self_trigger_datetime_class():
    # Models Data_Utils cell 19: timedelta usage + module-style datetime.datetime.*,
    # NO local `import datetime`. Pass 1 injects `from datetime import timedelta`;
    # subsequent passes must not inject `from datetime import datetime`.
    source = (
        "### PYTHON UTILS\n"
        "def add_to_date(start_date, days):\n"
        "    if isinstance(start_date, str):\n"
        "        start_date = datetime.datetime.strptime(start_date, '%Y-%m-%d')\n"
        "    return (start_date + timedelta(days=days)).strftime('%Y-%m-%d')\n"
    )

    first = jm._add_missing_imports(source)
    assert "from datetime import timedelta" in first
    assert "from datetime import datetime" not in first

    second = jm._add_missing_imports(first)
    assert "from datetime import datetime" not in second

    # Idempotent from here — re-application is a no-op.
    assert jm._add_missing_imports(second) == second


def test_injected_date_import_does_not_self_trigger_datetime_class():
    source = "d = date(2024, 1, 1)\n"  # bare `date` class usage

    first = jm._add_missing_imports(source)
    assert "from datetime import date" in first
    assert "from datetime import datetime" not in first

    second = jm._add_missing_imports(first)
    assert "from datetime import datetime" not in second


def test_customer_from_datetime_import_line_does_not_trigger_datetime_class():
    # The customer keeps datetime the MODULE and monkeypatches class methods onto
    # it; their own `from datetime import date, timedelta` line must not be read as
    # `datetime` usage.
    source = (
        "from datetime import date, timedelta\n"
        "import datetime\n"
        "datetime.strptime = datetime.datetime.strptime\n"
    )

    result = jm._add_missing_imports(source)
    assert "from datetime import datetime" not in result
    assert jm._add_missing_imports(result) == result


def test_module_style_datetime_with_import_is_not_shadowed():
    source = "import datetime\nprint(datetime.datetime.now())\n"

    result = jm._add_missing_imports(source)
    assert "from datetime import datetime" not in result


def test_bare_datetime_class_usage_still_imports():
    # The rule's INTENDED purpose (Databricks pre-imports the datetime CLASS):
    # genuine bare-class usage must still get the import injected.
    assert "from datetime import datetime" in jm._add_missing_imports(
        "d = datetime(2024, 1, 1)\n"
    )
    assert "from datetime import datetime" in jm._add_missing_imports(
        "if isinstance(x, datetime):\n    pass\n"
    )


def test_compound_import_line_keeps_trailing_class_usage_visible():
    # A compound statement must keep its trailing real usage visible to the scan:
    # the import portion is stripped only up to the `;`.
    source = "from datetime import timedelta; d = datetime(2024, 1, 1)\n"

    result = jm._add_missing_imports(source)
    assert "from datetime import datetime" in result


def test_multiline_parenthesized_import_is_fully_masked():
    # Parenthesized multi-line import: names on continuation lines
    # (`datetime as dt`, `timedelta`) must not trigger the datetime-class shadow.
    source = (
        "from datetime import (\n"
        "    datetime as dt,\n"
        "    timedelta,\n"
        ")\n"
        "import datetime\n"
        "print(datetime.datetime.now(), timedelta(days=1))\n"
    )

    result = jm._add_missing_imports(source)
    assert "from datetime import datetime" not in result
    assert jm._add_missing_imports(result) == result  # idempotent


def test_backslash_continued_import_is_fully_masked():
    source = (
        "from datetime import date, \\\n"
        "    timedelta\n"
        "import datetime\n"
        "print(datetime.datetime.now())\n"
    )

    result = jm._add_missing_imports(source)
    assert "from datetime import datetime" not in result


def test_import_after_semicolon_is_masked():
    """PR#153 review (BLOCKING): an import after a `;` (compound statement) is valid
    production Python; it must be masked so the `datetime` token inside it doesn't
    self-trigger the class shadow. `x = 1; from datetime import timedelta` + module-style
    use -> no `from datetime import datetime` injected. Fails without the `;`/`:` anchor."""
    source = "x = 1; from datetime import timedelta\nnow = datetime.datetime.now()\n"
    result = jm._add_missing_imports(source)
    assert "from datetime import datetime" not in result
    assert "from datetime import datetime" not in jm._add_missing_imports(result)  # idempotent


def test_import_in_one_line_suite_is_masked():
    """One-line suite form: `if c: from datetime import timedelta` — the import after `:`
    must also be masked."""
    source = "if True: from datetime import timedelta\nnow = datetime.datetime.now()\n"
    assert "from datetime import datetime" not in jm._add_missing_imports(source)


def test_bare_class_usage_after_semicolon_still_injects():
    """Only IMPORTS after `;`/`:` are masked — a real bare-class use after `;` is NOT
    masked, so the intended injection still fires."""
    assert "from datetime import datetime" in jm._add_missing_imports("x = 1; d = datetime(2024, 1, 1)\n")
