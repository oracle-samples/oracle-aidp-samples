"""Fix (2026-08-12) — submit_code prose-leak guard (`_looks_like_code`).

Root cause: in the migration extraction path, `submit_code` sometimes does not
fire and `call_opus_with_tools` returns the model's raw text, which
`_extract_code_from_response` passes through unchanged. A prose preamble like
"Producing the final migrated code." was then persisted AS the cell's code,
silently destroying the real definition (observed: `SaveTableUtils_2` Cell 0's
2903-char `createTable` overwritten by a 34-char preamble → downstream
`NameError: name 'createTable' is not defined`).

`_looks_like_code` gates the migration output: if it isn't plausibly code, the
caller retries final-code delivery and fails if it remains unusable.

These tests lock:
  - the exact failing preamble is rejected,
  - real code (Python / Scala / SQL / magics / comments / top-level await) is
    accepted — no false-rejects that would drop valid migrations,
  - empty/whitespace/prose is rejected.
"""

from __future__ import annotations

import pathlib
import sys
import unittest

REPO = pathlib.Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO / "scripts"))

from job_migrate import _looks_like_code  # noqa: E402


class TestLooksLikeCodeRejectsProse(unittest.TestCase):
    def test_the_exact_bug_string(self):
        # The 34-char preamble that wiped createTable.
        self.assertFalse(_looks_like_code("Producing the final migrated code."))

    def test_other_preambles(self):
        for s in [
            "Here is the migrated code:",
            "Here is the final migrated code.",
            "The cell reads a table and writes it back.",
            "I have migrated the cell.",
            "Sure, migrating now.",
            "Producing final migrated code",   # no period
        ]:
            self.assertFalse(_looks_like_code(s), s)

    def test_empty_and_whitespace(self):
        for s in ["", "   ", "\n\n", None]:
            self.assertFalse(_looks_like_code(s), repr(s))


class TestLooksLikeCodeAcceptsCode(unittest.TestCase):
    def test_python_function_def(self):
        self.assertTrue(_looks_like_code(
            "def createTable(df, table_name):\n    return df"))

    def test_plain_assignment_compiles(self):
        # No code-opener token, but compiles → accepted.
        self.assertTrue(_looks_like_code(
            "df2 = df.filter(col('x') > 1).groupBy('a').count()"))

    def test_leading_blank_lines(self):
        self.assertTrue(_looks_like_code("\n\n   def f():\n    pass"))

    def test_magics(self):
        for s in ["%scala\nval x = 1", "%sql\nSELECT 1", "%python\nx=1",
                  "%pyspark\nx=1", "%pip install foo", "%sh ls"]:
            self.assertTrue(_looks_like_code(s), s)

    def test_scala_openers_no_magic(self):
        for s in ["val x = 1", "var y = 2", "object Foo { }",
                  "import org.apache.spark.sql.functions._"]:
            self.assertTrue(_looks_like_code(s), s)

    def test_sql_openers(self):
        for s in ["SELECT * FROM t", "select 1", "CREATE TABLE t AS SELECT 1",
                  "insert into t values (1)", "MERGE INTO t ..."]:
            self.assertTrue(_looks_like_code(s), s)

    def test_comment_only_cell(self):
        self.assertTrue(_looks_like_code("# just a comment, still a valid cell"))

    def test_imports_and_prints(self):
        for s in ["import pandas as pd", "from x import y", "print(df.count())",
                  "spark.read.table('t')"]:
            self.assertTrue(_looks_like_code(s), s)

    def test_top_level_await_notebook_cell(self):
        # Notebook cells may use top-level await — must NOT be false-rejected.
        self.assertTrue(_looks_like_code("await some_async()"))

    def test_multiline_block(self):
        code = (
            "import pandas as pd\n"
            "df = spark.read.table('a.b')\n"
            "if df.count() > 0:\n"
            "    print('ok')\n"
        )
        self.assertTrue(_looks_like_code(code))


if __name__ == "__main__":
    unittest.main()
