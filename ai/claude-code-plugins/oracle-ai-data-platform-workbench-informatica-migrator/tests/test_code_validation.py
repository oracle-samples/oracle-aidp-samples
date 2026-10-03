"""Tests for infa2aidp.generators.code_validation.unresolved_names.

This is the static name-resolution check added after the offline
corpus surfaced a real bug: two generated notebooks (orders_transform.xml,
incremental_cdc.json) ended with "df_final = df" where `df` was never
assigned anywhere in the script -- a guaranteed NameError at runtime that
`ast.parse()` cannot catch, because the code is syntactically valid Python
either way.

Two kinds of coverage here:
1. Unit tests directly against `unresolved_names()` -- the exact bug
   shape, a clean equivalent, and the edge cases (comprehension targets,
   `except ... as e:`, the `from pyspark.sql.types import *` wildcard
   import) that a naive implementation gets wrong.
2. An end-to-end test that runs the real migrator over every fixture in
   `tests/fixtures/corpus/` and asserts every generated notebook resolves
   cleanly -- this is the regression guard for the notebook_generator.py
   fix (df_final now reads the variable the pipeline actually last wrote,
   instead of assuming a hardcoded name).
"""
from __future__ import annotations

import os
import unittest
from pathlib import Path

from infa2aidp.generators.code_validation import unresolved_names

CORPUS_DIR = os.path.join(os.path.dirname(__file__), "fixtures", "corpus")


class TestUnresolvedNames(unittest.TestCase):
    def test_the_original_bug_is_caught(self) -> None:
        """df_final = df where df was never assigned anywhere."""
        code = (
            "df_source = 1\n"
            "df_source = df_source\n"
            "df_final = df\n"
        )
        self.assertEqual(unresolved_names(code), ["df"])

    def test_the_fixed_version_is_clean(self) -> None:
        code = (
            "df_source = 1\n"
            "df_source = df_source\n"
            "df_final = df_source\n"
        )
        self.assertEqual(unresolved_names(code), [])

    def test_assignment_order_matters(self) -> None:
        """Using a name before it's ever assigned, even if it IS assigned
        later in the script, is still a real bug -- order matters."""
        code = "y = x\nx = 1\n"
        self.assertEqual(unresolved_names(code), ["x"])

    def test_imports_are_bound(self) -> None:
        code = (
            "from pyspark.sql import SparkSession, functions as F, Window\n"
            "spark = SparkSession.builder.getOrCreate()\n"
            "df = spark.table('T')\n"
            "df = df.withColumn('x', F.lit(1))\n"
        )
        self.assertEqual(unresolved_names(code), [])

    def test_pyspark_sql_types_wildcard_import_is_known(self) -> None:
        """from pyspark.sql.types import * -- the one wildcard import the
        generated setup cell always uses -- must not produce false
        positives for the types it provides (DecimalType, StringType, ...)."""
        code = (
            "from pyspark.sql.types import *\n"
            "x = DecimalType(10, 2)\n"
            "y = StringType()\n"
        )
        self.assertEqual(unresolved_names(code), [])

    def test_unknown_wildcard_import_is_not_trusted(self) -> None:
        """An unrecognized `import *` must NOT be assumed to cover
        whatever name is used later -- prefer a false positive over
        silently trusting an unknown source of names."""
        code = (
            "from some_unknown_module import *\n"
            "x = something_from_that_module\n"
        )
        self.assertEqual(unresolved_names(code), ["something_from_that_module"])

    def test_comprehension_target_is_scoped_correctly(self) -> None:
        """The comprehension loop variable must not be flagged as
        unresolved, and must not leak out as a name usable after the
        comprehension either."""
        code = (
            "key_columns = ['A', 'B']\n"
            "null_counts = [c for c in key_columns]\n"
        )
        self.assertEqual(unresolved_names(code), [])

    def test_except_handler_binds_the_exception_name(self) -> None:
        code = (
            "try:\n"
            "    x = 1\n"
            "except Exception as e:\n"
            "    print(e)\n"
            "    raise\n"
        )
        self.assertEqual(unresolved_names(code), [])

    def test_bindings_inside_a_try_body_are_visible_later_in_the_same_body(self) -> None:
        """A real shape from the generated write cell: an import and an
        assignment inside the SAME try block, used later in that same
        block -- must not be flagged just because it's nested."""
        code = (
            "spark = 1\n"
            "df_final = 2\n"
            "try:\n"
            "    from delta.tables import DeltaTable\n"
            "    target_table = DeltaTable.forName(spark, 'T')\n"
            "    target_table.alias('t').merge(df_final, 'x').execute()\n"
            "except Exception as e:\n"
            "    print(e)\n"
            "    raise\n"
        )
        self.assertEqual(unresolved_names(code), [])

    def test_if_branches_checked_independently_not_merged_during_check(self) -> None:
        """A name bound only in the if-branch must not hide a real bug in
        the else-branch (checked against the same pre-branch state)."""
        code = (
            "cond = True\n"
            "if cond:\n"
            "    only_in_if = 1\n"
            "else:\n"
            "    y = only_in_if\n"  # never bound on this branch -> bug
        )
        self.assertEqual(unresolved_names(code), ["only_in_if"])

    def test_if_branch_bindings_are_available_after_the_if(self) -> None:
        code = (
            "cond = True\n"
            "if cond:\n"
            "    z = 1\n"
            "else:\n"
            "    z = 2\n"
            "print(z)\n"
        )
        self.assertEqual(unresolved_names(code), [])


class TestCorpusFixturesResolveCleanly(unittest.TestCase):
    """Every notebook generated from the committed offline corpus must
    have zero unresolved names -- this is the regression guard for the
    df_final fix. Before that fix, orders_transform.xml and
    incremental_cdc.json both failed this."""

    @classmethod
    def setUpClass(cls):
        import tempfile
        from infa2aidp.migrator import run_migration

        cls.tmpdir = tempfile.mkdtemp(prefix="infa2aidp_corpus_test_")
        cls.out_dir = Path(cls.tmpdir) / "out"
        input_files = sorted(
            str(p) for p in Path(CORPUS_DIR).rglob("*")
            if p.is_file() and p.suffix.lower() in (".xml", ".json")
        )
        assert input_files, f"no corpus fixtures found under {CORPUS_DIR}"
        cls.result = run_migration(input_files, str(cls.out_dir), use_llm=False)
        cls.notebooks = list(cls.out_dir.rglob("*.ipynb"))

    @classmethod
    def tearDownClass(cls):
        import shutil
        shutil.rmtree(cls.tmpdir, ignore_errors=True)

    def test_corpus_produced_notebooks(self) -> None:
        self.assertTrue(self.notebooks, "corpus migration produced no notebooks")

    def test_every_corpus_notebook_resolves_cleanly(self) -> None:
        import json as _json

        failures = []
        for nb in self.notebooks:
            data = _json.loads(nb.read_text(encoding="utf-8"))
            code = "\n".join(
                "".join(c["source"]) if isinstance(c["source"], list) else c["source"]
                for c in data["cells"] if c["cell_type"] == "code"
            )
            bad = unresolved_names(code)
            if bad:
                failures.append(f"{nb.name}: unresolved {bad}")
        self.assertEqual(
            failures, [],
            "one or more corpus notebooks reference a name before it's "
            "ever assigned (guaranteed NameError at runtime):\n"
            + "\n".join(failures)
        )


if __name__ == "__main__":
    unittest.main()


# ---------------------------------------------------------------------------
# A dropped join leaves an abandoned input DataFrame
# ---------------------------------------------------------------------------

from infa2aidp.generators.code_validation import abandoned_dataframes  # noqa: E402


DROPPED_JOIN = '''
from pyspark.sql import SparkSession, functions as F
spark = SparkSession.builder.getOrCreate()
df_source = spark.table("default.infa_e2e.employees")
df_source_1 = spark.table("default.infa_e2e.departments")
df_source = df_source.filter((F.col('STATUS') == F.lit('A')))
# REVIEW REQUIRED: Joiner 'JNR_EmpDept' master/detail side could not be
# determined -- join skipped, resolve manually before running this notebook.
df_in_jnr_empdept_sq_departments = df_source_1.withColumnRenamed(
    "DEPARTMENT_ID", "DEPARTMENT_ID_D")
df = df_source
df = df.groupBy(F.col("DEPARTMENT_NAME")).agg(
    F.count(F.col('EMPLOYEE_ID')).alias("EMPLOYEE_COUNT"))
'''


def test_a_dropped_join_is_caught_as_an_abandoned_input():
    """The exact shape that failed on a live AIDP cluster.

    m_EmployeeSummary generated this: both sources read, the departments
    side prepared into its own variable, the Joiner skipped with a
    REVIEW REQUIRED marker, and then a groupBy on DEPARTMENT_NAME -- a
    column that only exists on the abandoned side. Spark raised
    "cannot resolve DEPARTMENT_NAME". It was counted in
    `notebooks=12 error=0` and reported 100% auto-convertible.
    """
    assert abandoned_dataframes(DROPPED_JOIN) == ["df_in_jnr_empdept_sq_departments"]


def test_unresolved_names_cannot_see_a_dropped_join():
    """Why a second check was needed, not a wider first one.

    The abandoned variable IS assigned, so nothing is read before
    assignment; and DEPARTMENT_NAME lives inside a string, not a Python
    identifier. unresolved_names passes on code that cannot run.
    """
    assert unresolved_names(DROPPED_JOIN) == []


def test_an_unconsumed_router_group_is_not_reported():
    """An unconsumed Router group is legitimate, not a broken dataflow.

    In data_quality_route.xml only the `valid` group has outgoing
    connectors. A first version of this check reported every unused df*
    name and flagged three correct Router groups -- so the scope is
    df_in_* only, the generator's per-consumer input copies.
    """
    router = '''
from pyspark.sql import SparkSession, functions as F
spark = SparkSession.builder.getOrCreate()
df = spark.table("t")
df_valid = df.filter(F.col("OK"))
df_invalid_currency = df.filter(~F.col("OK"))
df_negative_amount = df.filter(F.col("AMT") < 0)
df_final = df_valid
'''
    assert abandoned_dataframes(router) == []


def test_a_consumed_input_copy_is_not_reported():
    consumed = '''
from pyspark.sql import SparkSession, functions as F
spark = SparkSession.builder.getOrCreate()
df_source = spark.table("e")
df_source_1 = spark.table("d")
df_in_jnr_x = df_source_1.withColumnRenamed("A", "A_D")
df = df_source.join(F.broadcast(df_in_jnr_x), on="A_D", how="inner")
'''
    assert abandoned_dataframes(consumed) == []
