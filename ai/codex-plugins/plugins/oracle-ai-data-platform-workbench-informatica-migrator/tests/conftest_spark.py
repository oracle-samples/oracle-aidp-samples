"""A real local Spark session, and a helper that runs converted expressions.

Everything else in this suite asserts the *string* a converter emits. That
catches a changed translation; it cannot catch a translation that is wrong,
and it cannot catch one that does not run at all.

Both happened. ``LTRIM(NAME, 'xy')`` was emitted as
``F.ltrim(F.col('NAME'), F.lit('xy'))``, reviewed against the reference,
judged correct and pinned by a string assertion -- and it is a runtime
``TypeError``, because PySpark's ``ltrim`` takes one argument. No amount of
more careful reading would have found that. Running it finds it instantly.

pyspark is a TEST-ONLY dependency. The shipped package must import without
it, because generated notebooks run on the cluster and ``infa_compat``
defers every pyspark import into a function body to keep that true. Nothing
here changes that: if pyspark is absent these tests skip.
"""
from __future__ import annotations

import os
import sys
from datetime import datetime

import pytest

pyspark = pytest.importorskip(
    "pyspark", reason="executable expression tests need pyspark (test-only)"
)

from pyspark.sql import SparkSession, Window  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402

from infa2aidp.converters.expression_converter import ExpressionConverter  # noqa: E402


@pytest.fixture(scope="session")
def spark():
    """One local session for the whole run -- startup is seconds, not ms.

    ANSI is pinned off to match what generated notebooks do. Informatica
    evaluates permissively (a failed cast or a divide by zero yields NULL),
    and a harness running under different settings than the code it is
    checking would verify the wrong thing.
    """
    # Python workers must start with this interpreter: with PYSPARK_PYTHON
    # unset, Spark launches "python" from PATH -- on Windows the Store stub
    # -- and every UDF/transform stage fails "worker failed to connect back".
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    s = (
        SparkSession.builder.master("local[1]")
        .appName("infa2aidp-expression-tests")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.sql.ansi.enabled", "false")
        .config("spark.sql.storeAssignmentPolicy", "LEGACY")
        .getOrCreate()
    )
    s.sparkContext.setLogLevel("ERROR")
    yield s
    s.stop()


def _namespace() -> dict:
    """The names a generated notebook has in scope when it runs a converted
    expression.

    ``F`` and ``Window`` come from the notebook's own imports.
    ``_SESSION_START_TIME`` is defined in its setup cell
    (``notebook_generator``) because Informatica's ``SESSSTARTTIME`` is
    constant for a whole session, which ``F.current_timestamp()`` is not.
    Evaluating with a smaller namespace than the notebook has would report
    a NameError the notebook would never hit.
    """
    return {"F": F, "Window": Window, "_SESSION_START_TIME": datetime(2026, 1, 1, 3, 30)}


def run_expr(spark, infa_expr: str, rows: list[tuple], schema: str) -> list:
    """Convert an Informatica expression, run it, return the column values.

    ``eval`` of the emitted string is the point, not a shortcut: it is
    exactly what a generated notebook does with that text, so a form that
    does not evaluate here would not have run there either.
    """
    code = ExpressionConverter().convert(infa_expr)
    column = eval(code, _namespace())  # noqa: S307
    df = spark.createDataFrame(rows, schema)
    return [r[0] for r in df.select(column.alias("out")).collect()]


def run_agg(spark, infa_expr: str, rows: list[tuple], schema: str) -> list:
    """Same, for an aggregate expression.

    Aggregates cannot be evaluated through ``select`` -- they need a
    grouping context, which is how the generator uses them inside an
    Aggregator. Running them through ``run_expr`` would fail for the wrong
    reason and look like a defect in the conversion.
    """
    code = ExpressionConverter().convert(infa_expr)
    column = eval(code, _namespace())  # noqa: S307
    df = spark.createDataFrame(rows, schema)
    return [r[0] for r in df.agg(column.alias("out")).collect()]
