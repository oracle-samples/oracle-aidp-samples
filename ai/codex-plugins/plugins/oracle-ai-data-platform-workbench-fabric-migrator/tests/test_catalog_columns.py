"""Something reads the catalog's `columns` list now.

`_columns_from_ddl` has populated it since the catalog landed and nothing
consumed it. Making it produce findings needed an answer to "what is a
mismatch?", because a notebook naming a column the warehouse does not declare
is either a defect or a gap in the catalog, and picking wrong makes the report
noisy enough to be ignored.

The answer taken, and the measurement it rests on. On the bundled estate:

    tier                tables   with a column list
    warehouse_ddl           19                   17
    notebook_inferred        3                    0
    shortcut                 2                    0
    columns declared       103

A column list exists only for tables whose CREATE TABLE this tool read. For a
shortcut it never will -- the shortcut points at storage this tool does not
open -- and for a notebook-inferred table the write proves the table exists
and says nothing about its shape. So "column not declared" would mean *we do
not know* far more often than *this is wrong*.

Hence: answer only where the column list is present, and refuse to answer
where it is not. `None` is that refusal, and it is not the same value as
"clean" -- which is the single thing this file exists to hold still.
"""
import unittest

from fabric_aidp.inventory.catalog import (
    _normalise_column, declared_columns, undeclared_columns,
)
from fabric_aidp.translate import tsql_to_spark_sql as sq

DDL = {"tables": {
    "acmedw.dbo.claim": {
        "tier": "warehouse_ddl", "name": "AcmeDW.dbo.claim", "owner": "AcmeDW",
        # The four the bundled `claim.sql` really declares, plus one dotted
        # name for `_normalise_column`. The first draft of this fixture left
        # out `opened` and `is_open`, and `test_the_real_demo_view_is_silent`
        # duly failed -- correctly, because against THAT fixture the view does
        # name undeclared columns. Fixture, not code: the shape a test hands
        # the function has to be the shape the function really sees.
        "columns": [{"name": "claim id", "type": "bigint"},
                    {"name": "policy_no", "type": "nvarchar"},
                    {"name": "opened", "type": "datetime2"},
                    {"name": "is_open", "type": "bit"},
                    {"name": "Customer.Name", "type": "varchar"}],
    },
    "saleslake.claims_raw": {
        "tier": "shortcut", "name": "SalesLake.claims_raw", "owner": "SalesLake",
        "target": "s3://acme/claims",
    },
    "saleslake.claims_agg": {
        "tier": "notebook_inferred", "name": "SalesLake.claims_agg",
        "owner": "SalesLake", "notebook": "02_Build_Aggregates",
    },
}}


class RefusalIsNotAPassTests(unittest.TestCase):
    """The distinction the whole design turns on.

    Returning `()` where the columns are unknown would be the pre-#32
    warehouse defect exactly: the catalog was asked whether a table existed,
    said "I have no idea", and the answer was read as yes. Here it would mean
    every column reference to a shortcut silently passing -- which reads as
    verified and is not.
    """

    def test_a_shortcut_refuses_rather_than_passing(self):
        self.assertIsNone(
            undeclared_columns(DDL, "claims_raw", ["anything_at_all"]))

    def test_an_inferred_table_refuses_rather_than_passing(self):
        """The write proves the table exists. It says nothing about shape."""
        self.assertIsNone(
            undeclared_columns(DDL, "claims_agg", ["anything_at_all"]))

    def test_an_unknown_table_refuses(self):
        self.assertIsNone(
            undeclared_columns(DDL, "dbo.nowhere_at_all", ["id"]))

    def test_an_empty_but_present_catalog_refuses(self):
        """Same shape as `names_no_tables`: a truthy dict naming nothing is
        not the statement that a column does not exist."""
        self.assertIsNone(
            undeclared_columns({"summary": {}, "tables": {}}, "dbo.claim", ["id"]))

    def test_no_catalog_refuses(self):
        self.assertIsNone(undeclared_columns(None, "dbo.claim", ["id"]))

    def test_refusal_and_clean_are_different_values(self):
        """Written as one assertion because every caller has to branch on it,
        and `if not answer:` treats them alike."""
        refused = undeclared_columns(DDL, "claims_raw", ["zzz"])
        clean = undeclared_columns(DDL, "dbo.claim", ["policy_no"])
        self.assertIsNone(refused)
        self.assertEqual(clean, ())
        self.assertIsNot(refused, clean)
        self.assertNotEqual(refused, clean)


class WhereTheColumnsAreKnownTests(unittest.TestCase):
    def test_every_column_declared_is_clean(self):
        self.assertEqual(
            undeclared_columns(DDL, "dbo.claim", ["claim id", "policy_no"]), ())

    def test_an_undeclared_column_is_reported(self):
        self.assertEqual(
            undeclared_columns(DDL, "dbo.claim", ["claim id", "nope"]), ("nope",))

    def test_it_reports_the_spelling_the_caller_passed(self):
        """A finding has to quote the text that is in the file, so the display
        casing survives even though the comparison does not use it."""
        self.assertEqual(
            undeclared_columns(DDL, "dbo.claim", ["[Not_A_Column]"]),
            ("[Not_A_Column]",))

    def test_the_comparison_folds_case_because_aidp_does(self):
        """Measured on the cluster: `spark.sql.caseSensitive=false`. Comparing
        raw would report every capitalised reference as undeclared."""
        self.assertEqual(
            undeclared_columns(DDL, "dbo.claim", ["CLAIM ID", "Policy_No"]), ())

    def test_a_bracketed_name_matches_its_unbracketed_declaration(self):
        self.assertEqual(
            undeclared_columns(DDL, "dbo.claim", ["[claim id]"]), ())

    def test_duplicates_collapse_on_the_folded_name(self):
        """`SELECT id, ID` is one undeclared column, not two."""
        self.assertEqual(
            undeclared_columns(DDL, "dbo.claim", ["id", "ID", "Id"]), ("id",))

    def test_order_is_preserved(self):
        self.assertEqual(
            undeclared_columns(DDL, "dbo.claim", ["zeta", "alpha"]),
            ("zeta", "alpha"))

    def test_no_referenced_columns_is_clean_not_a_refusal(self):
        """`SELECT *` names no columns. There is nothing undeclared about
        that, and it is not an absence of knowledge either."""
        self.assertEqual(undeclared_columns(DDL, "dbo.claim", []), ())


class DeclaredColumnsTests(unittest.TestCase):
    def test_an_entry_with_no_columns_declares_none(self):
        for entry in ({"columns": []}, {}, None, {"columns": "id"}):
            with self.subTest(entry=entry):
                self.assertIsNone(declared_columns(entry))

    def test_it_never_returns_an_empty_set(self):
        """An empty set would compare false-y like None and equal to no other
        answer -- a table with zero columns is not a thing Fabric makes."""
        self.assertIsNone(declared_columns({"columns": []}))

    def test_columns_that_all_normalise_away_are_none_too(self):
        """The case the early return does NOT cover, and my first pass at
        this file missed it: `columns` is a non-empty list, so the length
        guard lets it through, and every entry folds to "". Bitten by
        deleting the `or None` -- which the empty-list test above cannot
        catch, because it never reaches that line.
        """
        for columns in ([{"name": ""}], [""], [{"name": "[]"}], [{"type": "int"}]):
            with self.subTest(columns=columns):
                self.assertIsNone(declared_columns({"columns": columns}))

    def test_and_a_table_whose_columns_all_normalise_away_refuses(self):
        catalog = {"tables": {"a.dbo.t": {"tier": "warehouse_ddl",
                                          "name": "A.dbo.t", "owner": "A",
                                          "columns": [{"name": ""}]}}}
        self.assertIsNone(undeclared_columns(catalog, "dbo.t", ["id"]))

    def test_it_folds_and_strips(self):
        self.assertEqual(
            declared_columns({"columns": [{"name": "[Claim ID]"}]}),
            frozenset({"claim id"}))

    def test_a_bare_string_column_is_accepted(self):
        """`--tables-csv` builds dicts, but a hand-written plan or a test may
        carry plain names, and refusing them would be a refusal to answer
        about a catalog that does in fact declare columns."""
        self.assertEqual(declared_columns({"columns": ["id", "name"]}),
                         frozenset({"id", "name"}))


class ColumnNamesAreNotTableNamesTests(unittest.TestCase):
    """`_normalise` splits on `.` to separate a table name's slots. A column
    name may legally contain one: `Table.ExpandRecordColumn` names its output
    `Customer.Name` by default, and the M corpus already produces
    `Data.Column1..18`. Running a column through `_normalise` cuts `[a.b]`
    into `a` and `b]` and compares neither.
    """

    def test_a_dotted_column_name_survives(self):
        self.assertEqual(_normalise_column("[Customer.Name]"), "customer.name")

    def test_and_it_matches_its_declaration(self):
        self.assertEqual(
            undeclared_columns(DDL, "dbo.claim", ["[Customer.Name]"]), ())

    def test_the_table_normaliser_would_have_mangled_it(self):
        """Pins the reason this function exists, so deleting it as a duplicate
        of `_normalise` fails rather than silently changing the answer."""
        from fabric_aidp.inventory.catalog import _normalise
        self.assertNotEqual(_normalise("[Customer.Name]"),
                            _normalise_column("[Customer.Name]"))

    def test_quoted_and_backticked_names_strip_too(self):
        for spelling in ('"id"', "`id`", "[id]", " id "):
            with self.subTest(spelling=spelling):
                self.assertEqual(_normalise_column(spelling), "id")

    def test_a_non_string_is_empty_not_a_crash(self):
        for value in (None, 42, [], {}):
            with self.subTest(value=value):
                self.assertEqual(_normalise_column(value), "")


class TheWarehouseRuleTests(unittest.TestCase):
    """SQ23, which is the thing that finally reads the `columns` list.

    Rewrites nothing and never refuses a rewrite. A column reference is not a
    name this tool can correct -- only a person knows whether the query is
    wrong or the catalog is stale -- so the value is in naming the column and
    the table and letting them decide.
    """

    @staticmethod
    def _sq23(sql, catalog=DDL, item="AcmeDW"):
        result = sq.translate(sql, kind="view", item=item, table_catalog=catalog)
        return [f for f in result.findings if f.rule == "SQ23_COLUMN_NOT_DECLARED"]

    def test_an_undeclared_column_is_flagged(self):
        found = self._sq23("SELECT policy_number FROM dbo.claim")
        self.assertEqual(len(found), 1)
        self.assertEqual(found[0].severity, "flag")

    def test_the_detail_names_the_column_and_the_table(self):
        detail = self._sq23("SELECT policy_number FROM dbo.claim")[0].detail
        self.assertIn("policy_number", detail)
        self.assertIn("dbo.claim", detail)

    def test_the_detail_gives_both_readings_rather_than_picking(self):
        """Either the query is wrong or the catalog is stale, and the export
        cannot say which. A detail that asserted one would be guessing."""
        detail = self._sq23("SELECT policy_number FROM dbo.claim")[0].detail
        self.assertIn("UNRESOLVED_COLUMN", detail)
        self.assertIn("stale", detail)

    def test_it_reports_every_undeclared_column_in_one_finding(self):
        found = self._sq23("SELECT policy_number, nope FROM dbo.claim")
        self.assertEqual(len(found), 1)
        for name in ("policy_number", "nope"):
            with self.subTest(name=name):
                self.assertIn(name, found[0].detail)

    def test_it_rewrites_nothing(self):
        """The name rewrite still happens; this rule is not in its way."""
        result = sq.translate("SELECT nope FROM dbo.claim", kind="view",
                              item="AcmeDW", table_catalog=DDL)
        self.assertEqual(result.translated_sql.strip(),
                         "SELECT nope FROM default.AcmeDW.claim")

    def test_the_real_demo_view_is_silent(self):
        """The one non-trivial named-column view in the bundled estate. Every
        column it names is declared, and `DATEDIFF(day, ...)` must not be read
        as a column called `day` -- the false positive this rule was shaped
        around."""
        self.assertEqual(self._sq23(
            "SELECT TOP 100 [claim id], ISNULL(policy_no, 'unknown') AS policy_no, "
            "DATEDIFF(day, opened, GETDATE()) AS age_days, "
            "IIF(is_open = 1, 'open', 'closed') AS status "
            "FROM dbo.claim ORDER BY opened"), [])

    def test_a_shortcut_gets_no_finding_and_is_not_graded_clean(self):
        """The case the whole design turns on: the catalog has no column list
        for a shortcut, so every reference to one is unknowable. Silence, not
        a pass -- and specifically NOT a finding either."""
        self.assertEqual(self._sq23("SELECT anything FROM claims_raw"), [])

    def test_an_inferred_table_gets_no_finding(self):
        self.assertEqual(self._sq23("SELECT anything FROM claims_agg"), [])

    def test_no_catalog_is_silent(self):
        """`translate()` is called with no catalog by `%%tsql` cells and by
        most of this suite. Treating that as "the column does not exist" would
        flag every reference in the estate."""
        self.assertEqual(self._sq23("SELECT nope FROM dbo.claim", catalog=None), [])

    def test_an_empty_but_present_catalog_is_silent(self):
        self.assertEqual(
            self._sq23("SELECT nope FROM dbo.claim",
                       catalog={"summary": {}, "tables": {}}), [])

    def test_an_unanalysable_statement_is_silent(self):
        """More than one table in scope: an undeclared column on one may be
        declared on the other."""
        self.assertEqual(
            self._sq23("SELECT nope FROM dbo.claim JOIN dbo.x ON 1=1"), [])

    def test_it_runs_before_the_name_rewrite(self):
        """Ordering, pinned. By the time SQ11 has run, `dbo.claim` is
        `default.AcmeDW.claim` and resolves differently -- so this rule sitting
        after it would silently stop finding anything."""
        from fabric_aidp.translate.tsql_to_spark_sql import (
            RULES, rule_two_part_names, rule_undeclared_columns)
        self.assertLess(RULES.index(rule_undeclared_columns),
                        RULES.index(rule_two_part_names))


if __name__ == "__main__":
    unittest.main()
