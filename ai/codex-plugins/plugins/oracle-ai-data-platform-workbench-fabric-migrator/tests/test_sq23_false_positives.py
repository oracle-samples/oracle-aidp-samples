"""SQ23 must not call a word a column when it is not one.

Each statement below raised SQ23_COLUMN_NOT_DECLARED for the quoted word on
5378e97, against a table whose column list was known, so an otherwise clean
view read REVIEW. The rule's own docstring names noise as its risk; these are
the measured cases. True positives are pinned alongside so a fix that simply
silences the rule cannot pass.
"""
import unittest

from fabric_aidp.inventory.catalog import _columns_from_ddl
from fabric_aidp.translate import tsql_to_spark_sql as sq

CATALOG = {"tables": {
    "acmedw.dbo.claim": {
        "tier": "warehouse_ddl", "name": "AcmeDW.dbo.claim", "owner": "AcmeDW",
        "columns": [{"name": n, "type": "INT"} for n in (
            "claim id", "policy_no", "opened", "is_open", "ClaimID", "total",
            "claim_amt")]},
}}

# statement -> the word it used to report
FALSE_POSITIVES = {
    "SELECT policy_no p FROM dbo.claim": "p",
    "SELECT ROW_NUMBER() OVER (ORDER BY opened) rn FROM dbo.claim": "rn",
    "SELECT policy_no AS p FROM dbo.claim ORDER BY p": "p",
    "SELECT p = policy_no FROM dbo.claim": "p",
    "SELECT policy_no FROM dbo.claim WHERE policy_no = N'x'": "N",
    "SELECT CONVERT(int, claim_amt) AS c FROM dbo.claim": "int",
    "SELECT TRY_CONVERT(DATE, opened) AS d FROM dbo.claim": "DATE",
    "SELECT CAST(policy_no AS VARCHAR(MAX)) AS v FROM dbo.claim": "MAX",
    "SELECT CURRENT_TIMESTAMP AS t FROM dbo.claim": "CURRENT_TIMESTAMP",
    "SELECT opened AT TIME ZONE 'UTC' AS o FROM dbo.claim": "ZONE",
    "SELECT policy_no FROM dbo.claim WHERE policy_no = 'x' COLLATE Latin1_General_CI_AS":
        "Latin1_General_CI_AS",
    "SELECT STRING_AGG(policy_no, ',') WITHIN GROUP (ORDER BY opened) AS s FROM dbo.claim":
        "WITHIN",
    "SELECT policy_no FROM dbo.claim ORDER BY policy_no OFFSET 0 ROWS FETCH FIRST 5 ROWS ONLY":
        "FIRST",
    "SELECT 1e5 AS a, 0x1F AS b FROM dbo.claim": "e5",
    "SELECT policy_no FROM dbo.claim GROUP BY GROUPING SETS ((policy_no))": "GROUPING",
    "SELECT policy_no FROM dbo.claim GROUP BY policy_no WITH ROLLUP": "ROLLUP",
    "SELECT NEXT VALUE FOR dbo.seq AS n FROM dbo.claim": "VALUE",
    "SELECT PARSE(policy_no AS int USING 'en-US') AS p FROM dbo.claim": "USING",
    "SELECT {d '2020-01-01'} AS d FROM dbo.claim": "d",
    "SELECT policy_no, IDENTITY(INT, 1, 1) AS id INTO dbo.newt FROM dbo.claim": "INT",
    "SELECT * INTO dbo.x FROM dbo.claim": "x",
}


def _sq23(sql):
    result = sq.translate(sql, kind="view", item="AcmeDW", table_catalog=CATALOG)
    return [f for f in result.findings if f.rule == "SQ23_COLUMN_NOT_DECLARED"]


class NotAColumnTests(unittest.TestCase):

    def test_none_of_these_words_is_reported_as_a_column(self):
        for sql, word in FALSE_POSITIVES.items():
            with self.subTest(sql=sql):
                self.assertEqual(_sq23(sql), [], f"{word!r} was reported")

    def test_a_real_undeclared_column_is_still_flagged(self):
        for sql in ("SELECT nosuch FROM dbo.claim",
                    "SELECT policy_no p, nosuch FROM dbo.claim",
                    "SELECT CONVERT(int, nosuch) AS c FROM dbo.claim",
                    "SELECT policy_no FROM dbo.claim ORDER BY nosuch"):
            with self.subTest(sql=sql):
                found = _sq23(sql)
                self.assertEqual(len(found), 1)
                self.assertIn("nosuch", found[0].detail)

    def test_an_alias_named_like_its_column_still_checks_the_column(self):
        self.assertEqual(_sq23("SELECT coalesce(policy_no, 'x') AS policy_no FROM dbo.claim"), [])
        self.assertEqual(len(_sq23("SELECT coalesce(nosuch, 'x') AS nosuch FROM dbo.claim")), 1)


class DdlColumnListTests(unittest.TestCase):
    """A wrong list makes SQ23 flag real columns; an unknowable one must be
    [] ("unknown"), never a guess. Each `was` is what 5378e97 returned."""

    def _names(self, sql):
        return [c["name"] for c in _columns_from_ddl(sql)]

    def test_a_comment_is_not_read_as_a_column(self):
        self.assertEqual(self._names("-- note (legacy INT)\nCREATE TABLE dbo.t (a INT, b INT)"),
                         ["a", "b"])                        # was ['legacy']
        self.assertEqual(self._names("CREATE TABLE dbo.t (a INT, -- first, then b\n b INT)"),
                         ["a", "b"])                        # was ['a', 'then']

    def test_a_double_quoted_identifier_is_a_column(self):
        self.assertEqual(self._names('CREATE TABLE dbo.t ("q c" INT, b INT)'),
                         ["q c", "b"])                      # was ['b']

    def test_a_ctas_has_no_column_list_to_read(self):
        self.assertEqual(self._names(
            "CREATE TABLE dbo.ctas AS SELECT CAST(pid AS INT) AS a2, pname FROM dbo.pol"),
            [])                                             # was ['pid']

    def test_a_file_that_alters_the_table_is_unknown(self):
        self.assertEqual(self._names(
            "CREATE TABLE dbo.claim (a INT)\nGO\nALTER TABLE dbo.claim ADD closed BIT"),
            [])                                             # was ['a'], missing `closed`

    def test_a_literal_default_does_not_split_the_list(self):
        self.assertEqual(self._names(
            "CREATE TABLE dbo.t (a NVARCHAR(9) DEFAULT ('x,y'), b INT)"), ["a", "b"])


if __name__ == "__main__":
    unittest.main()
