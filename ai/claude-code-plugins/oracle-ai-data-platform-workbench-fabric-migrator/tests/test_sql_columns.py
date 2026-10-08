"""Which columns a statement references -- the half the catalog could not do.

Which TABLES a statement references is answerable because a table name sits
in a grammatically marked position: after FROM, JOIN, UPDATE. A column does
not. An identifier in a select list may be a column, a function name, a date
part, an alias, a type name or a keyword, and telling them apart is the whole
job.

The bar was set by the one non-trivial named-column view in the bundled
estate, which is adversarial by accident:

    SELECT TOP 100
           [claim id],
           ISNULL(policy_no, 'unknown') AS policy_no,
           DATEDIFF(day, opened, GETDATE()) AS age_days,
           IIF(is_open = 1, 'open', 'closed') AS status
    FROM dbo.claim
    ORDER BY opened

`dbo.claim` declares exactly `claim id, policy_no, opened, is_open`, so the
right answer is "every reference is declared". A naive extractor reports
`day` -- a false positive on one of only two checkable statements in the
whole project. That view is the first test in this file.

`None` means "not a shape I can read columns out of with certainty". It is
never "no columns referenced"; `()` is that. Two shapes below returned `None`
only after a measured false positive forced them to.
"""
import pathlib
import unittest

from fabric_aidp.translate.sql_text import referenced_columns

CLAIM_VIEW = """SELECT TOP 100
       [claim id],
       ISNULL(policy_no, 'unknown') AS policy_no,
       DATEDIFF(day, opened, GETDATE()) AS age_days,
       IIF(is_open = 1, 'open', 'closed') AS status
FROM dbo.claim
ORDER BY opened"""

CLAIM_DECLARES = {"claim id", "policy_no", "opened", "is_open"}


def _folded(columns):
    return {c.strip("[]").strip('"').strip("`").casefold() for c in columns}


class TheAdversarialViewTests(unittest.TestCase):
    def test_it_finds_exactly_the_four_real_columns(self):
        table, columns = referenced_columns(CLAIM_VIEW)
        self.assertEqual(table, "dbo.claim")
        self.assertEqual(_folded(columns), CLAIM_DECLARES)

    def test_it_does_not_report_the_date_part_as_a_column(self):
        """`DATEDIFF(day, opened, GETDATE())` references `opened`, and
        nothing called `day`. This is the false positive the whole design is
        shaped around."""
        _, columns = referenced_columns(CLAIM_VIEW)
        self.assertNotIn("day", _folded(columns))

    def test_it_does_not_report_the_function_names(self):
        _, columns = referenced_columns(CLAIM_VIEW)
        for name in ("isnull", "datediff", "getdate", "iif"):
            with self.subTest(name=name):
                self.assertNotIn(name, _folded(columns))

    def test_it_does_not_report_the_aliases(self):
        """`AS age_days` and `AS status` name the output, not an input. Only
        `policy_no` survives, because it is also a real reference."""
        _, columns = referenced_columns(CLAIM_VIEW)
        for alias in ("age_days", "status"):
            with self.subTest(alias=alias):
                self.assertNotIn(alias, _folded(columns))

    def test_it_does_not_report_top_or_the_row_count(self):
        _, columns = referenced_columns(CLAIM_VIEW)
        self.assertNotIn("top", _folded(columns))
        self.assertNotIn("100", _folded(columns))

    def test_it_reads_the_order_by_clause(self):
        """`ORDER BY opened` is a column reference and would have been missed
        by scanning only the select list."""
        self.assertIn("opened", _folded(referenced_columns(CLAIM_VIEW)[1]))


class DatePartsAreExcludedOnlyWhereTheyAreOneTests(unittest.TestCase):
    """A blanket exclusion would make every column named `year`, `month`,
    `day` or `hour` permanently uncheckable -- a blind spot wider than the
    false positive it avoids. So the exclusion is positional.
    """

    def test_a_bracketed_day_is_a_column(self):
        _, columns = referenced_columns("SELECT [day] FROM dbo.t")
        self.assertEqual(_folded(columns), {"day"})

    def test_a_bare_day_outside_a_date_call_is_a_column(self):
        _, columns = referenced_columns(
            "SELECT DATEDIFF(day, a, b) AS n, day FROM dbo.t")
        self.assertEqual(_folded(columns), {"a", "b", "day"})

    def test_only_the_first_argument_is_skipped(self):
        """`DATEADD(day, n, opened)` -- `n` and `opened` are columns."""
        _, columns = referenced_columns("SELECT DATEADD(day, n, opened) FROM dbo.t")
        self.assertEqual(_folded(columns), {"n", "opened"})


class RefusedShapesTests(unittest.TestCase):
    """Each of these returns None because answering would mean guessing."""

    def test_a_comma_join_is_refused(self):
        """MEASURED before this was a whitelist: `SELECT a FROM dbo.t, dbo.u`
        answered `('dbo.t', ('a', 'u'))` -- it accepted a two-table query AND
        reported the second TABLE as a column of the first. A comma join
        carries no JOIN keyword, so the unsafe-shape scan never sees it.
        """
        self.assertIsNone(referenced_columns("SELECT a FROM dbo.t, dbo.u"))

    def test_a_table_alias_is_refused(self):
        for sql in ("SELECT a FROM dbo.t x", "SELECT a FROM dbo.t AS x"):
            with self.subTest(sql=sql):
                self.assertIsNone(referenced_columns(sql))

    def test_more_than_one_table_in_scope_is_refused(self):
        """A column undeclared on one may be perfectly declared on the other.

        Refused by the TAIL guard, not by a keyword list: whatever follows
        the FROM target has to be the end of the statement or a clause
        keyword, and `JOIN dbo.u ...` is neither.
        """
        for sql in ("SELECT a FROM dbo.t JOIN dbo.u ON 1=1",
                    "SELECT a FROM dbo.t CROSS JOIN dbo.u",
                    "SELECT a FROM dbo.t OUTER APPLY (SELECT 1) x",
                    "SELECT a FROM dbo.t PIVOT (SUM(v) FOR k IN ([x])) p"):
            with self.subTest(sql=sql):
                self.assertIsNone(referenced_columns(sql))

    def test_a_second_select_anywhere_is_refused(self):
        """A CTE, a subquery and a UNION all reach this: the columns belong
        to different scopes and this cannot say which. Refused by the COUNT
        guard -- a T-SQL CTE always contains a SELECT of its own, which is
        why no separate CTE check is needed."""
        for sql in ("WITH c AS (SELECT 1 AS x) SELECT x FROM c",
                    "SELECT a FROM (SELECT b FROM dbo.t) d",
                    "SELECT a FROM dbo.t UNION SELECT b FROM dbo.u",
                    "SELECT a FROM dbo.t WHERE EXISTS (SELECT 1 FROM dbo.u)"):
            with self.subTest(sql=sql):
                self.assertIsNone(referenced_columns(sql))

    def test_a_statement_with_no_select_is_refused(self):
        """Refused because there is no select list to read, which is also
        what covers UPDATE, DELETE and MERGE."""
        for sql in ("INSERT INTO dbo.t (a) VALUES (1)",
                    "UPDATE dbo.t SET a = 1",
                    "DELETE FROM dbo.t WHERE a = 1",
                    "MERGE INTO dbo.t USING dbo.u ON 1=1"):
            with self.subTest(sql=sql):
                self.assertIsNone(referenced_columns(sql))

    def test_an_insert_select_is_answered_against_the_selects_table(self):
        """A keyword blacklist used to refuse this, and refusing it was a net
        loss: `c` and `d` really do come from `dbo.u`, so the answer below is
        correct and the refusal threw it away. The target's own `(a, b)` list
        sits before the SELECT and is not scanned -- a miss, not a false
        positive, and the return shape carries one table by design.
        """
        self.assertEqual(
            referenced_columns("INSERT INTO dbo.t (a, b) SELECT c, d FROM dbo.u"),
            ("dbo.u", ("c", "d")))

    def test_no_from_is_refused(self):
        self.assertIsNone(referenced_columns("SELECT 1"))

    def test_junk_is_refused_rather_than_crashing(self):
        for value in (None, "", "   ", 42, [], "SELECT", "FROM"):
            with self.subTest(value=value):
                self.assertIsNone(referenced_columns(value))


class TheStarIsNotARefusalTests(unittest.TestCase):
    """`*` is not an identifier, so it is never collected, and every column
    it expands to is declared by construction. Refusing it would call an
    answerable statement unanswerable -- and would stop checking
    `SELECT t.*, misspelt FROM t`, which is the case worth catching.
    """

    def test_select_star_is_clean_not_unknown(self):
        self.assertEqual(referenced_columns("SELECT * FROM dbo.claim"),
                         ("dbo.claim", ()))

    def test_count_star_is_clean(self):
        self.assertEqual(referenced_columns("SELECT COUNT(*) FROM dbo.t"),
                         ("dbo.t", ()))

    def test_a_star_beside_a_named_column_still_checks_the_column(self):
        _, columns = referenced_columns("SELECT t.*, misspelt FROM dbo.t")
        self.assertEqual(_folded(columns), {"misspelt"})


class TailClausesTests(unittest.TestCase):
    def test_where_group_having_and_order_are_scanned(self):
        _, columns = referenced_columns(
            "SELECT a FROM dbo.t WHERE b = 1 GROUP BY c HAVING COUNT(d) > 1 "
            "ORDER BY e")
        self.assertEqual(_folded(columns), {"a", "b", "c", "d", "e"})

    def test_option_and_for_are_allowed_but_not_read(self):
        """MEASURED after the whitelist first admitted them and then scanned
        them: `OPTION (RECOMPILE)` reported `RECOMPILE` as a column, and
        `FOR JSON PATH` reported `FOR`, `JSON` and `PATH`."""
        for sql, expected in (
                ("SELECT a FROM dbo.t OPTION (RECOMPILE)", {"a"}),
                ("SELECT a FROM dbo.t FOR JSON PATH", {"a"}),
                ("SELECT a FROM dbo.t WHERE b=1 OPTION (RECOMPILE)", {"a", "b"})):
            with self.subTest(sql=sql):
                self.assertEqual(_folded(referenced_columns(sql)[1]), expected)


class MaskingTests(unittest.TestCase):
    """The scan reads the masked view, so a name in a literal or a comment is
    not a reference."""

    def test_a_column_name_inside_a_literal_is_not_a_reference(self):
        self.assertEqual(referenced_columns("SELECT 'opened' FROM dbo.t"),
                         ("dbo.t", ()))

    def test_a_column_name_inside_a_comment_is_not_a_reference(self):
        _, columns = referenced_columns("SELECT a -- opened\nFROM dbo.t")
        self.assertEqual(_folded(columns), {"a"})

    def test_a_variable_is_not_a_column(self):
        self.assertEqual(referenced_columns("SELECT @n FROM dbo.t"),
                         ("dbo.t", ()))

    def test_a_qualified_reference_yields_the_column_not_the_qualifier(self):
        _, columns = referenced_columns("SELECT dbo.t.a FROM dbo.t")
        self.assertEqual(_folded(columns), {"a"})

    def test_duplicates_collapse_on_the_folded_name(self):
        _, columns = referenced_columns("SELECT a, A FROM dbo.t WHERE a = 1")
        self.assertEqual(columns, ("a",))

    def test_a_cast_target_type_is_not_a_column(self):
        """`CAST(x AS int)` -- `int` is in an AS position, which is already
        the alias rule, which is why type names are NOT in the keyword list."""
        _, columns = referenced_columns("SELECT CAST(x AS int) FROM dbo.t")
        self.assertEqual(_folded(columns), {"x"})


class NoFalsePositiveAnywhereWeShipTests(unittest.TestCase):
    """Swept over every `.sql` this repository ships.

    MEASURED: 22 files, 20 refused, 2 analysable, both clean -- so the rule
    emits nothing across the entire project. That is the correct answer and
    it is also the reason this class exists: a rule that fires nowhere has no
    evidence from the corpus that it works, so what the corpus CAN prove is
    that it invents nothing. If a future fixture makes it fire, this test
    fails and the firing has to be looked at on purpose.
    """

    @classmethod
    def setUpClass(cls):
        root = pathlib.Path(__file__).resolve().parent.parent
        cls.results = []
        for base in (root / "fabric_aidp" / "fixtures" / "demo-workspace",
                     root / "tests" / "fixtures" / "real" / "corpora"):
            for path in sorted(base.rglob("*.sql")):
                cls.results.append(
                    (path.name,
                     referenced_columns(path.read_text(errors="replace"))))

    def test_it_scanned_something(self):
        self.assertGreater(len(self.results), 15)

    def test_the_only_analysable_statements_are_the_two_views(self):
        analysable = sorted(n for n, r in self.results if r is not None)
        self.assertEqual(analysable, ["v_agent_names.sql", "v_open_claims.sql"])

    def test_and_both_reference_only_columns_their_table_declares(self):
        expected = {"v_agent_names.sql": {"id", "name"},
                    "v_open_claims.sql": CLAIM_DECLARES}
        for name, result in self.results:
            if result is None:
                continue
            with self.subTest(name=name):
                self.assertEqual(_folded(result[1]), expected[name])


if __name__ == "__main__":
    unittest.main()
