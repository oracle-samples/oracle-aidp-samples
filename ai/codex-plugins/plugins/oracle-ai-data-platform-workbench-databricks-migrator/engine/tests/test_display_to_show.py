"""Fix (2026-08-12) — deterministic `.display()` → `.show()` rewrite.

Databricks' `DataFrame.display()` method does not exist on AIDP/OSS Spark 3.5
(`AttributeError: 'DataFrame' object has no attribute 'display'`). For a Spark
DataFrame `.show()` is the correct equivalent, so we rewrite the zero-arg method
form up-front in the deterministic swap chain — letting the dominant Spark-DF
case pass on first execution instead of failing and burning a reactive Opus fix.

These tests lock the scope/safety properties of `_rewrite_display_to_show`:
  - rewrites ONLY the zero-arg *method* form `.display()`,
  - NEVER touches the function form `display(df)`, `displayHTML(...)`, or the
    argument form `.display(<args>)`,
  - skips full-line comments,
  - leaves lookalikes (`.mydisplay()`, `.display_name`) alone,
  - is idempotent,
  - and is wired into `_apply_field_learned_swaps` (parent + child paths).
"""

from __future__ import annotations

import pathlib
import sys
import unittest

REPO = pathlib.Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO / "scripts"))

from job_migrate import _rewrite_display_to_show, _apply_field_learned_swaps  # noqa: E402


class TestDisplayToShow(unittest.TestCase):
    def test_simple_method_call(self):
        self.assertEqual(_rewrite_display_to_show("df.display()"), "df.show()")

    def test_chained_expression(self):
        code = "df.filter(col('x') > 1).groupBy('a').count().display()"
        self.assertEqual(
            _rewrite_display_to_show(code),
            "df.filter(col('x') > 1).groupBy('a').count().show()",
        )

    def test_multiline_parenthesised(self):
        code = "(\n    df\n    .groupBy('a')\n    .count()\n).display()"
        out = _rewrite_display_to_show(code)
        self.assertTrue(out.endswith(").show()"))
        self.assertNotIn(".display()", out)

    def test_function_form_untouched(self):
        # display(df) is AIDP-native (aidp_compat) — must NOT be rewritten.
        for code in ["display(df)", "display(spark.table('t'))", "x = display(df)"]:
            self.assertEqual(_rewrite_display_to_show(code), code)

    def test_displayHTML_untouched(self):
        code = "displayHTML('<b>hello</b>')"
        self.assertEqual(_rewrite_display_to_show(code), code)

    def test_argument_form_untouched(self):
        # Only the zero-arg method form is deterministic; `.display(n)` (non-empty
        # args) is left for the reactive path.
        for code in ["df.display(5)", "df.display(10, False)", "df.display(n=20)"]:
            self.assertEqual(_rewrite_display_to_show(code), code)

    def test_spacing_variants(self):
        self.assertEqual(_rewrite_display_to_show("df.display( )"), "df.show()")
        self.assertEqual(_rewrite_display_to_show("df.display ()"), "df.show()")
        self.assertEqual(_rewrite_display_to_show("df.display(  )"), "df.show()")

    def test_lookalikes_untouched(self):
        for code in ["df.mydisplay()", "obj.display_name", "mydisplay()",
                     "displayer()"]:
            self.assertEqual(_rewrite_display_to_show(code), code)

    def test_full_line_comment_skipped(self):
        for code in ["# df.display()", "    # legacy: result.display()"]:
            self.assertEqual(_rewrite_display_to_show(code), code)

    def test_multiple_occurrences(self):
        code = "a.display()\nb.display()"
        self.assertEqual(_rewrite_display_to_show(code), "a.show()\nb.show()")

    def test_no_display_untouched_byte_identical(self):
        code = "x = spark.read.table('t')\nx.count()\nprint(x.columns)"
        self.assertEqual(_rewrite_display_to_show(code), code)

    def test_idempotent(self):
        once = _rewrite_display_to_show("df.display()")
        twice = _rewrite_display_to_show(once)
        self.assertEqual(once, twice)
        self.assertEqual(twice, "df.show()")

    def test_wired_into_field_learned_swaps(self):
        # Integration: the swap must run via _apply_field_learned_swaps, which is
        # invoked on BOTH the parent path and the child-inline path.
        out = _apply_field_learned_swaps("df.display()")
        self.assertIn(".show()", out)
        self.assertNotIn(".display()", out)


if __name__ == "__main__":
    unittest.main()
