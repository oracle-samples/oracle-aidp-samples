"""The T-SQL corpus ratchet: four objects, and exactly what each produces.

T9 was filed against this file for "weak, stale or tautological
assertions". Read against the tree, four were provable and are fixed here;
each carries the proof beside it. The rest hold.

  * `assertFalse(result.needs_manual_review)` beside `assertEqual(flags, 0)`
    -- `needs_manual_review` IS `flags > 0` (translate/types.py:36), so the
    second line restates the first and cannot fail on its own. Same for
    `assertTrue(needs_manual_review)` beside the GAPPY flag list.
  * `test_no_other_rule_runs_on_a_procedure` asserted `changes == 0`, which
    the exact finding list one test above already implies: SQ01 is a `flag`
    and `changes` counts `rewrite` findings. Measured on the tree: running
    all of `RULES` over the old PROCEDURE fixture by hand, with the gate
    bypassed and an item supplied, gave findings=[] and byte-identical
    text -- so the test passed with or without the gate it exists to
    guard. The fixture now reads from `dbo.claim`, which SQ11 rewrites,
    and the assertion is on the text.
  * `test_placeholder_rule_is_gone` asserted that `SQ02_NO_RULES_YET` is
    not emitted. That string appears nowhere in `fabric_aidp/`; no change
    to the module's behaviour can make the assertion fail. Replaced with
    the live form of the same worry.
  * `test_translation_is_deterministic` called a pure function twice in one
    process and compared the two. Within a process that cannot differ;
    what varies is set/dict ordering ACROSS processes, which two calls
    side by side never see. It now compares against a recorded output.

Two counted-not-valued assertions were strengthened to the exact ordered
list of (rule, severity) -- a set of rule ids hides a duplicate, and a
flag-only set hides every rewrite regression.

Left as they are, with reasons: `test_coverage_is_declared` asserts a
constant against a literal and is duplicated in test_translators_core.py,
but the id is a contract with the report and a change detector is the
right shape for it; `test_every_rule_is_registered_once` and the
output-shape assertions all fail on a real regression.
"""
import unittest

from fabric_aidp.translate import tsql_to_spark_sql as tsql

CLEAN_TABLE = """CREATE TABLE AcmeDW.dbo.claim (
    [claim id] BIGINT NOT NULL,
    policy_no NVARCHAR(50) NOT NULL,
    opened DATETIME2(7),
    is_open BIT
)"""

CLEAN_VIEW = """CREATE VIEW dbo.v_open_claims AS
SELECT TOP 100
       [claim id],
       ISNULL(agent, 'unassigned') AS agent,
       DATEDIFF(day, opened, GETDATE()) AS age_days,
       CHARINDEX('-', policy_no) AS dash_at,
       IIF(is_open = 1, 'open', 'closed') AS status
FROM AcmeDW.dbo.claim
ORDER BY opened"""

GAPPY_TABLE = """CREATE TABLE dbo.payment (
    id BIGINT IDENTITY(1,1) NOT NULL,
    amount MONEY,
    CONSTRAINT pk_payment PRIMARY KEY (id) NOT ENFORCED
)"""

# `FROM dbo.claim` is load-bearing: without it no rule in the set touches
# this object even with the procedural gate bypassed, so
# `test_no_other_rule_runs_on_a_procedure` had nothing to detect. Measured.
PROCEDURE = """CREATE PROCEDURE dbo.sp_load AS
BEGIN
    DECLARE @n INT;
    SET @n = 1;
    SELECT @n FROM dbo.claim;
END"""

CLEAN_VIEW_TRANSLATED = (
    "CREATE VIEW dbo.v_open_claims AS\n"
    "SELECT\n"
    "       `claim id`,\n"
    "       coalesce(agent, 'unassigned') AS agent,\n"
    "       datediff(current_timestamp(), opened) AS age_days,\n"
    "       locate('-', policy_no) AS dash_at,\n"
    "       if(is_open = 1, 'open', 'closed') AS status\n"
    "FROM default.AcmeDW.claim\n"
    "ORDER BY opened LIMIT 100")


class CleanObjectTests(unittest.TestCase):
    def test_clean_table_reaches_pass(self):
        result = tsql.translate(CLEAN_TABLE, kind="table")
        self.assertEqual(result.flags, 0, [str(f) for f in result.findings])
        # `assertFalse(result.needs_manual_review)` stood here and is
        # `flags > 0` restated. The exact list says what the line above
        # cannot: that PASS here means translated, not left alone, and it
        # catches a duplicate that a set of rule ids would hide.
        self.assertEqual([(f.rule, f.severity) for f in result.findings],
                         [("SQ10_BRACKET_IDENT", "rewrite"),
                          ("SQ11_THREE_PART_NAME", "rewrite"),
                          ("SQ60_TYPE", "rewrite"),
                          ("SQ60_TYPE", "rewrite"),
                          ("SQ60_TYPE", "rewrite"),
                          # The corpus table's first column is `[claim id]`,
                          # the exact name SQ77 was written for: it translated
                          # to valid Spark and graded PASS here while failing
                          # to create on Delta. Still a PASS -- the finding is
                          # a rewrite and `flags` above is still 0 -- but the
                          # list has to say the table now leaves with column
                          # mapping on it.
                          ("SQ77_DELTA_COLUMN_MAPPING", "rewrite")])

    def test_clean_table_output_is_spark_shaped(self):
        out = tsql.translate(CLEAN_TABLE, kind="table").translated_sql
        self.assertIn("`claim id` BIGINT NOT NULL", out)
        self.assertIn("policy_no STRING NOT NULL", out)
        self.assertIn("opened TIMESTAMP", out)
        self.assertIn("is_open BOOLEAN", out)
        self.assertIn("default.AcmeDW.claim", out)
        self.assertNotIn("[", out)
        self.assertNotIn("NVARCHAR", out)

    def test_clean_view_reaches_pass(self):
        result = tsql.translate(CLEAN_VIEW, kind="view")
        self.assertEqual(result.flags, 0, [str(f) for f in result.findings])

    def test_clean_view_applies_every_expected_rewrite(self):
        out = tsql.translate(CLEAN_VIEW, kind="view").translated_sql
        self.assertIn("datediff(current_timestamp(), opened)", out)
        self.assertIn("locate('-', policy_no)", out)
        self.assertIn("coalesce(agent, 'unassigned')", out)
        self.assertIn("if(is_open = 1, 'open', 'closed')", out)
        self.assertIn("LIMIT 100", out)
        self.assertNotIn("TOP", out)
        self.assertNotIn("GETDATE", out)

    def test_limit_lands_after_order_by(self):
        out = tsql.translate(CLEAN_VIEW, kind="view").translated_sql
        self.assertLess(out.index("ORDER BY"), out.index("LIMIT"))

    def test_the_select_list_keeps_the_layout_it_arrived_with(self):
        r"""SQ50 used to finish with a document-wide
        `re.sub(r"(\bSELECT(?:\s+DISTINCT)?)\s{2,}", r"\1 ", ...)`, which
        pulled `[claim id]` up onto the SELECT line here -- and did the same
        to every multi-line object in an estate, TOP or no TOP. T6."""
        out = tsql.translate(CLEAN_VIEW, kind="view").translated_sql
        self.assertIn("SELECT\n       `claim id`", out)


class GappyObjectTests(unittest.TestCase):
    def test_exactly_the_expected_flags_and_no_others(self):
        result = tsql.translate(GAPPY_TABLE, kind="table")
        # A `sorted({...})` of flag-severity rule ids stood here. It dropped
        # duplicates -- a second IDENTITY column was invisible to it -- and
        # ignored every `rewrite` finding, so a rewrite regression on this
        # object was not covered by the file that exists to ratchet it.
        self.assertEqual([(f.rule, f.severity) for f in result.findings],
                         [("SQ60_MONEY", "flag"),
                          ("SQ70_TABLE_CONSTRAINT", "flag"),
                          ("SQ70_IDENTITY", "flag")])

    def test_removed_clauses_are_gone_from_the_output(self):
        out = tsql.translate(GAPPY_TABLE, kind="table").translated_sql
        self.assertNotIn("IDENTITY", out)
        self.assertNotIn("PRIMARY KEY", out)
        self.assertIn("amount MONEY", out)

    def test_review_is_required_and_the_sql_changed_anyway(self):
        """`assertTrue(needs_manual_review)` alone restates the flag list
        above, since `needs_manual_review` is `flags > 0`. What it does not
        say, and what a reader of the report needs: SQ70's removals are
        `flag` severity and still EDIT the SQL, so `changes` -- which counts
        only `rewrite` findings -- reads 0 for an object whose text the tool
        rewrote. `changes` is not "was this file modified"."""
        result = tsql.translate(GAPPY_TABLE, kind="table")
        self.assertTrue(result.needs_manual_review)
        self.assertEqual(result.changes, 0)
        self.assertNotEqual(result.translated_sql, GAPPY_TABLE)


class ProcedureTests(unittest.TestCase):
    def test_procedure_is_flagged_whole_and_untouched(self):
        result = tsql.translate(PROCEDURE, kind="procedure")
        self.assertEqual([f.rule for f in result.findings], ["SQ01_PROCEDURAL"])
        self.assertEqual(result.translated_sql, PROCEDURE)

    def test_no_other_rule_runs_on_a_procedure(self):
        """`self.assertEqual(result.changes, 0)` stood here, and the test
        above already implies it: SQ01 is a `flag`, and `changes` counts
        `rewrite` findings, so it could not have been anything else.

        This asks what the name promises. `dbo.claim` in the body is a
        two-part name `rule_two_part_names` rewrites whenever it is told
        the owning item -- measured, with the gate bypassed and
        item="AcmeDW", RULES turns it into `default.AcmeDW.claim` and
        records SQ11_TWO_PART_NAME. With the gate in place the object must
        come back byte-identical and carry SQ01 alone.
        """
        result = tsql.translate(PROCEDURE, kind="procedure", item="AcmeDW")
        self.assertEqual(result.translated_sql, PROCEDURE)
        self.assertEqual([f.rule for f in result.findings], ["SQ01_PROCEDURAL"])
        self.assertNotIn("default.AcmeDW.claim", result.translated_sql)


class CoverageTests(unittest.TestCase):
    def test_no_finding_is_a_placeholder(self):
        """`test_placeholder_rule_is_gone` stood here, asserting that
        `SQ02_NO_RULES_YET` is not among the rules fired. That string is in
        no file under `fabric_aidp/`, so no change to the module's
        behaviour could make the assertion fail -- it was a guard against a
        rule id that cannot be emitted.

        The live form of the same worry: a rule registered with a stub
        message. Every finding has to say what happened, because the detail
        is what reaches the operator -- the rule id is an index, not an
        explanation. A `flag` has to say more than a `rewrite`: a rewrite
        that reads `BIT -> BOOLEAN` (14 characters, and the whole truth) is
        done, while a flag is work handed to a human and has to say what
        the work is. 40 is a floor, not a measurement: the shortest flag
        detail these four objects produce is 147 characters, SQ70_IDENTITY
        (measured).
        """
        for source in (CLEAN_TABLE, CLEAN_VIEW, GAPPY_TABLE, PROCEDURE):
            for finding in tsql.translate(source).findings:
                with self.subTest(rule=finding.rule):
                    self.assertIn(finding.severity, ("rewrite", "flag"))
                    self.assertNotEqual(finding.detail.strip(), finding.rule)
                    self.assertIn(" ", finding.detail.strip())
                    if finding.severity == "flag":
                        self.assertGreater(len(finding.detail), 40,
                                           finding.detail)

    def test_coverage_is_declared(self):
        self.assertEqual(tsql.RULESET_COVERAGE, "tsql-v1")

    def test_every_rule_is_registered_once(self):
        names = [rule.__name__ for rule in tsql.RULES]
        self.assertEqual(len(names), len(set(names)))

    def test_translation_is_deterministic(self):
        """Two calls in one process were compared here. A pure function
        cannot disagree with itself inside one interpreter; what varies is
        set and dict iteration order ACROSS processes, under hash
        randomisation, which two calls side by side never see. Comparing
        against a recorded output does see it -- and pins the exact text,
        which the two-call form never asserted at all."""
        for _ in range(2):
            result = tsql.translate(CLEAN_VIEW, kind="view")
            self.assertEqual(result.translated_sql, CLEAN_VIEW_TRANSLATED)
            self.assertEqual([f.rule for f in result.findings],
                             ["SQ10_BRACKET_IDENT", "SQ11_THREE_PART_NAME",
                              "SQ20_DATEDIFF", "SQ21_CHARINDEX",
                              "SQ30_ISNULL", "SQ30_GETDATE",
                              "SQ50_TOP", "SQ83_IIF"])


if __name__ == "__main__":
    unittest.main()
