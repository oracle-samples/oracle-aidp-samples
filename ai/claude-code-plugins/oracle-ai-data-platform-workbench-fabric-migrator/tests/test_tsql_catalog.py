"""The warehouse T-SQL path consulting the resolved table catalog (SQ19).

Until this landed it consulted nothing. Every table reference it recognised
by *shape* got a confident three-part AIDP name and a `rewrite` finding, so a
Warehouse view over a shortcut -- whose data is in S3 and not in the lakehouse
at all -- or over a table nothing in the export declares came out as Spark SQL
that will run, read nothing, and grade PASS. The notebook path had refused
both since NB11/NB12 landed. Measured side by side on the demo's resolved
catalog, one reference through each translator:

    reference             notebook path          warehouse path
    dbo.nowhere_at_all    NB12_TABLE_UNKNOWN     SQ11 -> default.AcmeDW....
    claims_raw_s3         NB11_TABLE_IS_SHORTCUT SQ11, unchanged, PASS
    dbo.claim             NB10 rewrite           SQ11 rewrite, agreeing

`AgreementTests` at the bottom is the one that must not be deleted: it asserts
the two translators give the same verdict on the same reference, which is the
property that decays silently.
"""
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from fabric_aidp.inventory.catalog import (build_catalog, entry_owner,
                                           load_supplied_catalog,
                                           recorded_item)
from fabric_aidp.translate import fabric_notebook_to_spark as nb
from fabric_aidp.translate import tsql_to_spark_sql as tsql

# One catalog, the four tiers that matter, in the shape `inventory.catalog`
# builds. `dbo.currentdate` is declared by two Warehouses on purpose: that is
# the ambiguous case, and it is real -- the bundled estate has exactly this
# pair.
CATALOG = {
    "summary": {},
    "shortcuts": {},
    "tables": {
        "acmedw.dbo.claim": {
            "tier": "warehouse_ddl", "owner": "AcmeDW", "warehouse": "AcmeDW",
            "name": "dbo.claim",
            "columns": [{"name": "id", "type": "BIGINT"}]},
        "saleslake.claims_raw_s3": {
            "tier": "shortcut", "owner": "SalesLake", "lakehouse": "SalesLake",
            "name": "claims_raw_s3",
            "target": "https://acme-raw.s3.us-east-1.amazonaws.com/claims"},
        # Owned by the Warehouse being translated, so `InferredTableTests`
        # is about the tier and not about where the name lands. Where the
        # name lands is a separate question and the next entry is it.
        "acmedw.dbo.claims_agg": {
            "tier": "notebook_inferred", "owner": "AcmeDW",
            "warehouse": "AcmeDW", "name": "dbo.claims_agg",
            "created_by": "02_Build_Aggregates"},
        # Inferred, and owned by a Lakehouse that is not the caller's
        # binding: the cross-binding case, which is the only shape where a
        # recorded item and a binding come apart. `owning_item` used to
        # discard the Lakehouse here and name the table under whichever
        # item happened to be reading it.
        "saleslake.dbo.claims_daily": {
            "tier": "notebook_inferred", "owner": "SalesLake",
            "name": "dbo.claims_daily", "created_by": "01_Ingest_Claims"},
        # A `--tables-csv` row, in the shape `build_catalog`'s tier 3
        # actually builds it: `owner` is "" because `_add` is called
        # without one and `load_supplied_catalog` parses no column for one,
        # so the item the operator wrote is in `name` and nowhere else.
        "saleslake.dbo.ledger_csv": {
            "tier": "supplied", "owner": "",
            "name": "SalesLake.dbo.ledger_csv",
            "columns": [{"name": "id", "type": "BIGINT"}]},
        # The same, in a schema Fabric does not drop, so the schema slot of
        # a three-part supplied name is visible in the emitted name.
        "saleslake.sales.rate_card": {
            "tier": "supplied", "owner": "",
            "name": "SalesLake.sales.rate_card"},
        # Declared by a Warehouse that is not the one being translated: the
        # case where the folder the `.sql` came out of and the catalog
        # disagree about where the table is.
        "otherdw.dbo.ledger": {
            "tier": "warehouse_ddl", "owner": "OtherDW", "warehouse": "OtherDW",
            "name": "dbo.ledger"},
        "wareone.dbo.currentdate": {
            "tier": "warehouse_ddl", "owner": "WareOne", "warehouse": "WareOne",
            "name": "dbo.currentdate"},
        "waretwo.dbo.currentdate": {
            "tier": "warehouse_ddl", "owner": "WareTwo", "warehouse": "WareTwo",
            "name": "dbo.currentdate"},
    },
}


def _t(sql, *, item="AcmeDW", catalog=CATALOG, kind="view"):
    return tsql.translate(sql, kind=kind, item=item, table_catalog=catalog)


def _rules(result):
    return [f.rule for f in result.findings]


def _detail(result, rule):
    return next(f.detail for f in result.findings if f.rule == rule)


class UnknownTableTests(unittest.TestCase):
    def test_a_table_no_tier_knows_is_left_exactly_as_written(self):
        out = _t("CREATE VIEW dbo.v AS SELECT * FROM dbo.nowhere_at_all")
        self.assertIn("FROM dbo.nowhere_at_all", out.translated_sql)
        self.assertNotIn("default.AcmeDW.nowhere_at_all", out.translated_sql)

    def test_and_is_flagged_rather_than_silently_skipped(self):
        out = _t("CREATE VIEW dbo.v AS SELECT * FROM dbo.nowhere_at_all")
        self.assertIn("SQ19_TABLE_UNKNOWN", _rules(out))
        self.assertTrue(out.needs_manual_review)

    def test_the_detail_says_where_it_looked(self):
        out = _t("CREATE VIEW dbo.v AS SELECT * FROM dbo.nowhere_at_all")
        detail = _detail(out, "SQ19_TABLE_UNKNOWN")
        self.assertIn("warehouse DDL, shortcuts", detail)
        self.assertIn("left exactly as written", detail)

    def test_a_three_part_reference_is_refused_the_same_way(self):
        out = _t("CREATE VIEW dbo.v AS SELECT * FROM Other.dbo.nothing")
        self.assertIn("FROM Other.dbo.nothing", out.translated_sql)
        self.assertIn("SQ19_TABLE_UNKNOWN", _rules(out))
        self.assertNotIn("SQ11_THREE_PART_NAME", _rules(out))


class ShortcutTests(unittest.TestCase):
    """A shortcut's data is not in the lakehouse, so no three-part name
    reaches it. This is the case the evidence for issue #1 led with."""

    def test_a_shortcut_is_not_rewritten(self):
        out = _t("CREATE VIEW dbo.v AS SELECT * FROM SalesLake.claims_raw_s3")
        self.assertIn("FROM SalesLake.claims_raw_s3", out.translated_sql)
        self.assertIn("SQ19_TABLE_IS_SHORTCUT", _rules(out))
        self.assertTrue(out.needs_manual_review)

    def test_the_detail_names_the_target_a_reader_has_to_migrate(self):
        out = _t("CREATE VIEW dbo.v AS SELECT * FROM SalesLake.claims_raw_s3")
        self.assertIn("acme-raw.s3.us-east-1.amazonaws.com",
                      _detail(out, "SQ19_TABLE_IS_SHORTCUT"))


class AmbiguousTests(unittest.TestCase):
    """`resolve` returns None for "nothing matched" and for "several did",
    and calling both unknown sends the reader looking for a table the export
    does contain -- twice. The notebook path learned that as NB20."""

    def test_two_warehouses_declaring_the_same_name_is_not_unknown(self):
        out = _t("CREATE VIEW dbo.v AS SELECT * FROM dbo.currentdate",
                 item="Unrelated")
        self.assertIn("SQ19_TABLE_AMBIGUOUS", _rules(out))
        self.assertNotIn("SQ19_TABLE_UNKNOWN", _rules(out))

    def test_the_detail_names_both_candidates(self):
        out = _t("CREATE VIEW dbo.v AS SELECT * FROM dbo.currentdate",
                 item="Unrelated")
        self.assertIn("2 catalog entries", _detail(out, "SQ19_TABLE_AMBIGUOUS"))

    def test_the_owning_item_disambiguates_it(self):
        """The Warehouse an object lives in is the hint that picks between
        two candidates, and the runner always has it."""
        out = _t("CREATE VIEW dbo.v AS SELECT * FROM dbo.currentdate",
                 item="WareTwo")
        self.assertNotIn("SQ19_TABLE_AMBIGUOUS", _rules(out))
        self.assertIn("default.WareTwo.currentdate", out.translated_sql)


class InferredTableTests(unittest.TestCase):
    def test_a_table_known_only_from_a_notebook_write_is_still_rewritten(self):
        out = _t("CREATE VIEW dbo.v AS SELECT * FROM dbo.claims_agg")
        self.assertIn("default.AcmeDW.claims_agg", out.translated_sql)
        self.assertIn("SQ19_TABLE_INFERRED", _rules(out))
        self.assertFalse(out.needs_manual_review)

    def test_the_detail_names_the_notebook_that_must_run_first(self):
        out = _t("CREATE VIEW dbo.v AS SELECT * FROM dbo.claims_agg")
        self.assertIn("02_Build_Aggregates",
                      _detail(out, "SQ19_TABLE_INFERRED"))

    def test_it_is_info_so_one_rewrite_is_not_counted_twice(self):
        """NB14 is `rewrite` on the notebook path because it is the only
        finding for that name. Here SQ11 already emits the rewrite for the
        same substitution, so a second one would double the change count."""
        out = _t("CREATE VIEW dbo.v AS SELECT * FROM dbo.claims_agg")
        inferred = next(f for f in out.findings
                        if f.rule == "SQ19_TABLE_INFERRED")
        self.assertEqual(inferred.severity, "info")
        self.assertEqual(
            len([f for f in out.findings if f.rule == "SQ11_TWO_PART_NAME"
                 and "claims_agg" in f.detail]), 1)


class ObjectBeingCreatedTests(unittest.TestCase):
    """The warehouse_ddl tier is built out of these CREATE statements, so
    asking the catalog about the object a statement defines would have the
    tool refuse to read its own input."""

    def test_a_create_table_target_is_never_asked_about(self):
        out = _t("CREATE TABLE dbo.brand_new (id INT)", kind="table")
        self.assertIn("default.AcmeDW.brand_new", out.translated_sql)
        self.assertEqual([r for r in _rules(out) if r.startswith("SQ19")], [])

    def test_nor_is_a_create_view_target(self):
        out = _t("CREATE VIEW dbo.v_brand_new AS SELECT 1")
        self.assertIn("default.AcmeDW.v_brand_new", out.translated_sql)
        self.assertEqual([r for r in _rules(out) if r.startswith("SQ19")], [])

    def test_select_into_creates_its_target_so_that_is_exempt_too(self):
        out = _t("SELECT a INTO dbo.fresh FROM dbo.claim", kind="other")
        self.assertIn("default.AcmeDW.fresh", out.translated_sql)
        self.assertEqual([r for r in _rules(out) if r.startswith("SQ19")], [])

    def test_but_insert_into_references_a_table_that_must_exist(self):
        out = _t("INSERT INTO dbo.nowhere_at_all SELECT 1", kind="other")
        self.assertIn("SQ19_TABLE_UNKNOWN", _rules(out))
        self.assertIn("INSERT INTO dbo.nowhere_at_all", out.translated_sql)

    def test_and_so_does_a_drop(self):
        out = _t("DROP TABLE dbo.nowhere_at_all", kind="other")
        self.assertIn("SQ19_TABLE_UNKNOWN", _rules(out))


class NoCatalogTests(unittest.TestCase):
    """Every caller must decide what to do with no catalog before asking.
    Treating "nothing was supplied" as "this table does not exist" would
    flag every reference in the estate -- and `%%tsql` cells and most of
    the suite call `translate` without one."""

    def test_without_a_catalog_the_rules_behave_exactly_as_before(self):
        sql = "CREATE VIEW dbo.v AS SELECT * FROM dbo.nowhere_at_all"
        out = tsql.translate(sql, kind="view", item="AcmeDW")
        self.assertIn("default.AcmeDW.nowhere_at_all", out.translated_sql)
        self.assertEqual([r for r in _rules(out) if r.startswith("SQ19")], [])

    def test_a_system_schema_is_skipped_before_the_catalog_is_asked(self):
        """`sys.objects` is not a table the catalog should know about; a
        SQ19_TABLE_UNKNOWN on one would read as a migration gap.

        No CREATE in the statement, so the only name here is the one under
        test -- with one, the view being defined would supply an SQ19 of its
        own and this would pass for the wrong reason.
        """
        out = _t("SELECT * FROM sys.objects", kind="other")
        self.assertEqual([r for r in _rules(out) if r.startswith("SQ19")], [])

    def test_with_no_owning_item_the_two_part_rule_asks_nothing(self):
        """There is no warehouse analogue of the notebook's NB13 ("the
        catalog entry records no owning item") and none was added: this rule
        returns before the catalog is consulted when `item` is empty, and a
        three-part name carries its own item, so the state is unreachable."""
        out = _t("CREATE VIEW dbo.v AS SELECT * FROM dbo.nowhere_at_all",
                 item=None)
        self.assertEqual([r for r in _rules(out) if r.startswith("SQ19")], [])
        self.assertEqual([r for r in _rules(out) if r.startswith("SQ11")], [])


class KnownTableTests(unittest.TestCase):
    def test_a_declared_table_is_still_rewritten_with_no_extra_finding(self):
        out = _t("CREATE VIEW dbo.v AS SELECT * FROM dbo.claim")
        self.assertIn("default.AcmeDW.claim", out.translated_sql)
        self.assertEqual([r for r in _rules(out) if r.startswith("SQ19")], [])
        self.assertFalse(out.needs_manual_review)


class OwningItemTests(unittest.TestCase):
    """Asking the catalog whether a table exists and then naming it
    somewhere else is how one table gets two names.

    `item` is the folder the `.sql` came out of. For the object the file
    *defines* that is right by construction, but a read can name a table
    another Warehouse declares, and the catalog records which. These rules
    used to build every name from `item` regardless, which produced a
    confident three-part name for a table that is not there -- with no
    SQ19, because the catalog had been asked "does it exist", had said yes,
    and was then ignored about where.
    """

    def test_a_read_of_another_warehouses_table_uses_that_warehouse(self):
        out = _t("CREATE VIEW dbo.v AS SELECT * FROM dbo.ledger")
        self.assertIn("default.OtherDW.ledger", out.translated_sql)
        self.assertNotIn("default.AcmeDW.ledger", out.translated_sql)

    def test_the_finding_names_the_item_that_was_used(self):
        """It said "the Fabric item 'AcmeDW' owning this object", naming an
        item the rewrite did not use."""
        out = _t("CREATE VIEW dbo.v AS SELECT * FROM dbo.ledger")
        detail = next(f.detail for f in out.findings
                      if f.rule == "SQ11_TWO_PART_NAME"
                      and "dbo.ledger" in f.detail)
        self.assertIn("'OtherDW'", detail)
        self.assertNotIn("'AcmeDW'", detail)

    def test_the_entrys_warehouse_beats_a_three_part_name_too(self):
        """A three-part T-SQL name writes the database itself, and the
        catalog still outranks it -- exactly as `_resolved_table_name` on
        the notebook side treats a written item as a hint."""
        out = _t("CREATE VIEW dbo.v AS SELECT * FROM AcmeDW.dbo.ledger")
        self.assertIn("default.OtherDW.ledger", out.translated_sql)

    def test_the_object_this_file_defines_still_belongs_to_its_folder(self):
        """A CREATE target never reaches the catalog, so `item` stands --
        which is right: the folder is what says where the object is made."""
        out = _t("CREATE VIEW dbo.ledger AS SELECT 1")
        self.assertIn("CREATE VIEW default.AcmeDW.ledger", out.translated_sql)

    def test_with_no_catalog_the_folders_item_is_all_there_is(self):
        out = tsql.translate("CREATE VIEW dbo.v AS SELECT * FROM dbo.ledger",
                             kind="view", item="AcmeDW")
        self.assertIn("default.AcmeDW.ledger", out.translated_sql)

    def test_an_entry_recording_no_owning_item_at_all_leaves_the_binding(self):
        """"No recorded item" is a real state, not a gap: `build_catalog`
        leaves the owner empty for a table written by a notebook with no
        binding rather than guess one, and the bundled estate's
        `orphan_output` is one. The caller's binding is right for those."""
        catalog = {"summary": {}, "shortcuts": {}, "tables": {
            "orphan_output": {"tier": "notebook_inferred", "owner": "",
                              "name": "orphan_output",
                              "created_by": "05_Unbound_Writer"}}}
        out = _t("CREATE VIEW dbo.v AS SELECT * FROM dbo.orphan_output",
                 catalog=catalog)
        self.assertIn("default.AcmeDW.orphan_output", out.translated_sql)

    def test_an_inferred_tables_recorded_lakehouse_beats_the_binding(self):
        """The last `major` on issue #1. This named the table under whoever
        was reading it, so the same table got `default.SalesLake.
        claims_daily` from its writer and `default.AcmeDW.claims_daily`
        from an `AcmeDW` reader -- and only one of those is written by
        anything. The tier infers the table's *existence* from a
        `saveAsTable`; the owner is the writing notebook's attached
        lakehouse, which is where that write lands, so the location is
        exactly as certain as the existence and the binding is not
        evidence of anything."""
        out = _t("CREATE VIEW dbo.v AS SELECT * FROM dbo.claims_daily")
        self.assertIn("default.SalesLake.claims_daily", out.translated_sql)
        self.assertNotIn("default.AcmeDW.claims_daily", out.translated_sql)

    def test_and_the_inferred_finding_still_names_the_notebook_to_run_first(self):
        """The promise the tier exists to make, about the name it now
        emits: before this it promised `01_Ingest_Claims` writes
        `default.AcmeDW.claims_daily`, which it does not."""
        out = _t("CREATE VIEW dbo.v AS SELECT * FROM dbo.claims_daily")
        self.assertIn("01_Ingest_Claims", _detail(out, "SQ19_TABLE_INFERRED"))
        detail = next(f.detail for f in out.findings
                      if f.rule == "SQ11_TWO_PART_NAME"
                      and "dbo.claims_daily" in f.detail)
        self.assertIn("default.SalesLake.claims_daily", detail)
        self.assertIn("'SalesLake'", detail)

    def test_the_item_an_operator_wrote_in_the_csv_is_where_the_table_goes(self):
        """A supplied entry has no `owner`: `load_supplied_catalog` reads
        `table`, `column` and `type` and nothing else, and tier 3 calls
        `_add` with no owner. So a three-part name in the `table` column is
        the only way to say where a supplied table lives, and it was read
        to *resolve* the reference and then discarded to *name* it -- which
        makes the half of `--tables-csv` that says where do nothing."""
        out = _t("CREATE VIEW dbo.v AS SELECT * FROM dbo.ledger_csv")
        self.assertIn("default.SalesLake.ledger_csv", out.translated_sql)
        self.assertNotIn("default.AcmeDW.ledger_csv", out.translated_sql)

    def test_a_bare_reference_reads_the_schema_from_the_middle_slot(self):
        """The schema fallback only runs for a reference that names none,
        which in T-SQL is unreachable -- the two-part rule needs two parts
        -- so this goes through the notebook path, which resolves bare
        names. `SalesLake.sales.rate_card` is three-part and the schema is
        the *middle* slot; reading `[0]` for it, which is what the two-part
        tiers needed, gave `default.SalesLake_SalesLake.rate_card`."""
        out = nb.translate(
            AgreementTests.HEAD + "%%sql\nSELECT * FROM rate_card\n",
            namespace="ns", default_lakehouse="AcmeDW", catalog=CATALOG)
        self.assertIn("default.SalesLake_sales.rate_card", out.translated_sql)

    def test_a_shortcut_is_refused_before_any_item_is_chosen(self):
        """The tier `owning_item` deliberately leaves out. A shortcut's
        data is outside the lakehouse and no three-part name reaches it, so
        the answer for one is "there is no name" rather than "this
        lakehouse" -- and the refusal happens first, so the reference keeps
        the spelling the author used whatever this function would say."""
        out = _t("CREATE VIEW dbo.v AS SELECT * FROM dbo.claims_raw_s3")
        self.assertIn("FROM dbo.claims_raw_s3", out.translated_sql)
        self.assertNotIn("default.SalesLake.claims_raw_s3", out.translated_sql)
        self.assertIn("SQ19_TABLE_IS_SHORTCUT", _rules(out))


class AgreementTests(unittest.TestCase):
    """The same reference, the same catalog, one through each translator.

    This is the property the batch bought and the one that decays silently:
    two implementations of "is this table known" drift, and this codebase has
    paid for that twice. Both paths now go through
    `inventory.catalog.classify_reference`.
    """

    HEAD = ("# Fabric notebook source\n\n# METADATA ********************\n\n"
            "# META {\n# META   \"dependencies\": {\n"
            "# META     \"lakehouse\": {\n"
            "# META       \"default_lakehouse_name\": \"SalesLake\"\n"
            "# META     }\n# META   }\n# META }\n\n"
            "# CELL ********************\n\n")

    def _both(self, reference):
        warehouse = _t(f"CREATE VIEW dbo.v AS SELECT * FROM {reference}")
        notebook = nb.translate(
            self.HEAD + f"%%sql\nSELECT * FROM {reference}\n",
            namespace="ns", default_lakehouse="SalesLake", catalog=CATALOG)
        return warehouse, notebook

    def test_an_unknown_table_is_refused_by_both(self):
        warehouse, notebook = self._both("dbo.nowhere_at_all")
        self.assertIn("SQ19_TABLE_UNKNOWN", _rules(warehouse))
        self.assertIn("NB12_TABLE_UNKNOWN", _rules(notebook))
        self.assertTrue(warehouse.needs_manual_review)
        self.assertTrue(notebook.needs_manual_review)

    def test_a_shortcut_is_refused_by_both(self):
        warehouse, notebook = self._both("SalesLake.claims_raw_s3")
        self.assertIn("SQ19_TABLE_IS_SHORTCUT", _rules(warehouse))
        self.assertIn("NB11_TABLE_IS_SHORTCUT", _rules(notebook))

    def test_an_ambiguous_reference_is_refused_by_both(self):
        warehouse = _t("CREATE VIEW dbo.v AS SELECT * FROM dbo.currentdate",
                       item="Unrelated")
        notebook = nb.translate(
            self.HEAD + "%%sql\nSELECT * FROM dbo.currentdate\n",
            namespace="ns", default_lakehouse="Unrelated", catalog=CATALOG)
        self.assertIn("SQ19_TABLE_AMBIGUOUS", _rules(warehouse))
        self.assertIn("NB20_TABLE_AMBIGUOUS", _rules(notebook))

    def test_a_known_table_is_rewritten_to_the_same_name_by_both(self):
        warehouse, notebook = self._both("dbo.claim")
        self.assertIn("default.AcmeDW.claim", warehouse.translated_sql)
        self.assertIn("default.AcmeDW.claim", notebook.translated_sql)
        self.assertFalse(warehouse.needs_manual_review)
        self.assertFalse(notebook.needs_manual_review)

    def test_the_cross_database_spelling_of_a_shortcut_is_caught_by_both(self):
        """`SalesLake.dbo.claims_raw_s3` is what a Fabric Warehouse writes
        to read a Lakehouse table, and it used to resolve to nothing.

        Both translators answered "not found" -- wrong, and wrong
        identically, because the cause was in neither of them:
        `inventory.catalog` read a two-part entry key
        (`saleslake.claims_raw_s3`) right-aligned, which put the item name
        in the *schema* slot, so the entry claimed a schema called
        `saleslake` and any reference naming `dbo` was rejected as a
        different table. See `_entry_slots`. This test was the pin on that
        gap; it now asserts the answer.
        """
        warehouse, notebook = self._both("SalesLake.dbo.claims_raw_s3")
        self.assertIn("SQ19_TABLE_IS_SHORTCUT", _rules(warehouse))
        self.assertIn("NB11_TABLE_IS_SHORTCUT", _rules(notebook))
        self.assertNotIn("SQ19_TABLE_UNKNOWN", _rules(warehouse))
        self.assertNotIn("NB12_TABLE_UNKNOWN", _rules(notebook))

    def test_the_schema_qualified_spelling_of_a_shortcut_is_caught_by_both(self):
        """`dbo.claims_raw_s3` -- the other spelling Fabric's SQL analytics
        endpoint makes real, and the other one that resolved to nothing."""
        warehouse, notebook = self._both("dbo.claims_raw_s3")
        self.assertIn("SQ19_TABLE_IS_SHORTCUT", _rules(warehouse))
        self.assertIn("NB11_TABLE_IS_SHORTCUT", _rules(notebook))

    def test_a_table_another_warehouse_declares_gets_one_name_from_both(self):
        """The invariant `tests/test_one_name.py` and the README's "one
        name for one table" exist to hold, and the one place it still
        failed after the catalog reached the warehouse path: the notebook
        said `default.OtherDW.ledger` and the warehouse said
        `default.AcmeDW.ledger`, for a reference both had just resolved to
        the same catalog entry."""
        warehouse = _t("CREATE VIEW dbo.v AS SELECT * FROM dbo.ledger")
        notebook = nb.translate(
            self.HEAD + "%%sql\nSELECT * FROM dbo.ledger\n",
            namespace="ns", default_lakehouse="AcmeDW", catalog=CATALOG)
        self.assertIn("default.OtherDW.ledger", warehouse.translated_sql)
        self.assertIn("default.OtherDW.ledger", notebook.translated_sql)

    def test_an_inferred_table_gets_its_lakehouses_name_from_both(self):
        """The same claim one tier down, and the one issue #1's last
        `major` was about. Both translators agreed before this landed --
        they agreed on the *binding*, so the one-name invariant held per
        reference and broke across readers: `01_Ingest_Claims`, bound
        `SalesLake`, wrote `default.SalesLake.claims_daily`, and any
        `AcmeDW` reader of the same table was sent to
        `default.AcmeDW.claims_daily`. Two names, one table, and the second
        one is written by nothing."""
        warehouse = _t("CREATE VIEW dbo.v AS SELECT * FROM dbo.claims_daily")
        notebook = nb.translate(
            self.HEAD + "%%sql\nSELECT * FROM dbo.claims_daily\n",
            namespace="ns", default_lakehouse="AcmeDW", catalog=CATALOG)
        self.assertIn("default.SalesLake.claims_daily", warehouse.translated_sql)
        self.assertIn("default.SalesLake.claims_daily", notebook.translated_sql)
        self.assertNotIn("default.AcmeDW.claims_daily", warehouse.translated_sql)
        self.assertNotIn("default.AcmeDW.claims_daily", notebook.translated_sql)

    def test_a_supplied_rows_own_item_is_honoured_by_both(self):
        """Tier 3, where the item is in the `table` column of the CSV and
        in no `owner` field, because there is no column for one."""
        warehouse = _t("CREATE VIEW dbo.v AS SELECT * FROM dbo.ledger_csv")
        notebook = nb.translate(
            self.HEAD + "%%sql\nSELECT * FROM dbo.ledger_csv\n",
            namespace="ns", default_lakehouse="AcmeDW", catalog=CATALOG)
        self.assertIn("default.SalesLake.ledger_csv", warehouse.translated_sql)
        self.assertIn("default.SalesLake.ledger_csv", notebook.translated_sql)


class SuppliedCsvTests(unittest.TestCase):
    """The tier-3 shape end to end, from a file rather than a dict.

    `AgreementTests` and `OwningItemTests` assert against `CATALOG`, which
    is hand-written, and the reason this class exists is that the claim
    being made is about what `load_supplied_catalog` and `build_catalog`
    *produce*: that a supplied entry's `owner` is always "" and the item an
    operator wrote is only ever in `name`. A hand-built dict could assert
    that shape and be wrong about it -- which is the mistake
    `tests/test_one_name.py`'s docstring records being made four times.
    """

    def _catalog(self, rows):
        with TemporaryDirectory() as tmp:
            path = Path(tmp) / "tables.csv"
            path.write_text(rows, encoding="utf-8")
            return build_catalog({}, supplied=load_supplied_catalog(path))

    def test_a_supplied_entry_records_no_owner_field_at_all(self):
        catalog = self._catalog("table,column,type\n"
                                "SalesLake.dbo.ledger,id,BIGINT\n")
        entry = catalog["tables"]["saleslake.dbo.ledger"]
        self.assertEqual(entry["tier"], "supplied")
        self.assertEqual(entry["owner"], "")
        self.assertEqual(entry_owner(entry), "")
        self.assertEqual(entry["name"], "SalesLake.dbo.ledger")

    def test_so_the_item_comes_off_the_name_and_reaches_the_sql(self):
        """`entry_owner` is "" here, so a rule written against that field
        alone would leave this tier exactly as broken as it was."""
        catalog = self._catalog("table,column,type\n"
                                "SalesLake.dbo.ledger,id,BIGINT\n")
        entry = catalog["tables"]["saleslake.dbo.ledger"]
        self.assertEqual(recorded_item(entry), "SalesLake")
        out = tsql.translate("CREATE VIEW dbo.v AS SELECT * FROM dbo.ledger",
                             kind="view", item="AcmeDW", table_catalog=catalog)
        self.assertIn("default.SalesLake.ledger", out.translated_sql)

    def test_a_row_that_names_no_item_still_falls_back_to_the_binding(self):
        """Two-part in the CSV says nothing about the owning item, and the
        caller's binding is the only answer there is."""
        catalog = self._catalog("table,column,type\ndbo.ledger,id,BIGINT\n")
        entry = catalog["tables"]["dbo.ledger"]
        self.assertEqual(recorded_item(entry), "")
        out = tsql.translate("CREATE VIEW dbo.v AS SELECT * FROM dbo.ledger",
                             kind="view", item="AcmeDW", table_catalog=catalog)
        self.assertIn("default.AcmeDW.ledger", out.translated_sql)


class EmptyCatalogDeclinesToAnswerTests(unittest.TestCase):
    """SQ19's guard tested `not table_catalog`, and a plan whose resolution
    found nothing carries `{"summary": {}, "tables": {}}` -- a truthy dict
    naming nothing. MEASURED on 03f019b:

        classify_reference({"tables": {}, "shortcuts": []}, "dbo.claim", "W")
          ->  ('unknown', None, [])

    so the guard let it through and every read in the estate collected
    SQ19_TABLE_UNKNOWN: "we resolved nothing" reported, once per reference,
    as "we resolved this and it is absent".
    """

    EMPTY = {"summary": {}, "tables": {}}
    SQL = "CREATE VIEW dbo.v AS SELECT * FROM dbo.nowhere_at_all"

    def test_no_sq19_finding_on_an_empty_catalog(self):
        self.assertNotIn("SQ19_TABLE_UNKNOWN",
                         _rules(_t(self.SQL, catalog=self.EMPTY)))

    def test_it_behaves_exactly_as_no_catalog_does(self):
        empty = _t(self.SQL, catalog=self.EMPTY)
        absent = _t(self.SQL, catalog=None)
        self.assertEqual(_rules(empty), _rules(absent))
        self.assertEqual(empty.translated_sql, absent.translated_sql)

    def test_the_name_is_rewritten_on_shape_as_it_always_was(self):
        result = _t(self.SQL, catalog=self.EMPTY)
        self.assertIn("default.AcmeDW.nowhere_at_all", result.translated_sql)
        self.assertIn("SQ11_TWO_PART_NAME", _rules(result))

    def test_a_shortcut_reference_is_no_longer_claimed_either(self):
        """The other SQ19 branches go quiet too, and they must: with nothing
        resolved this run cannot know a reference is a shortcut any more than
        it can know a table is absent."""
        result = _t("CREATE VIEW dbo.v AS SELECT * FROM SalesLake.claims_raw_s3",
                    catalog=self.EMPTY)
        self.assertEqual(
            [r for r in _rules(result) if r.startswith("SQ19")], [])

    def test_the_real_catalog_still_answers_all_four_ways(self):
        """The guard against switching the check off. One reference per
        verdict, through the populated catalog at the top of this file."""
        for sql, rule in (
                ("CREATE VIEW dbo.v AS SELECT * FROM dbo.nowhere_at_all",
                 "SQ19_TABLE_UNKNOWN"),
                ("CREATE VIEW dbo.v AS SELECT * FROM SalesLake.claims_raw_s3",
                 "SQ19_TABLE_IS_SHORTCUT"),
                ("CREATE VIEW dbo.v AS SELECT * FROM dbo.currentdate",
                 "SQ19_TABLE_AMBIGUOUS")):
            with self.subTest(rule=rule):
                self.assertIn(rule, _rules(_t(sql)))


class WrittenDatabaseOverriddenTests(unittest.TestCase):
    """`FROM AcmeDW.dbo.ledger`, with `dbo.ledger` declared only by
    OtherDW, came out as `default.OtherDW.ledger` with SQ11 alone -- another
    warehouse's table, graded PASS."""

    def test_an_overridden_written_database_is_flagged(self):
        out = _t("CREATE VIEW dbo.v AS SELECT * FROM AcmeDW.dbo.ledger")
        self.assertIn("default.OtherDW.ledger", out.translated_sql)
        found = [f for f in out.findings if f.rule == "SQ25_DATABASE_OVERRIDDEN"]
        self.assertEqual(len(found), 1)
        self.assertEqual(found[0].severity, "flag")
        self.assertIn("'AcmeDW'", found[0].detail)
        self.assertIn("'OtherDW'", found[0].detail)

    def test_naming_the_owner_is_not_flagged(self):
        out = _t("CREATE VIEW dbo.v AS SELECT * FROM OtherDW.dbo.ledger")
        self.assertNotIn("SQ25_DATABASE_OVERRIDDEN", _rules(out))

if __name__ == "__main__":
    unittest.main()
