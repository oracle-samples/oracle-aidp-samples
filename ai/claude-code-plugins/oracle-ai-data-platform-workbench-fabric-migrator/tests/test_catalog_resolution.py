import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from fabric_aidp.inventory import catalog as cat


def _sources(*, warehouses=None, lakehouses=None, notebooks=None) -> dict:
    return {
        "warehouse": {"items": {"warehouses": warehouses or []}},
        "lakehouse": {"items": {"lakehouses": lakehouses or []}},
        "notebook": {"items": {"notebooks": notebooks or []}},
    }


WAREHOUSE = [{"name": "AcmeDW", "objects": [
    {"kind": "table", "schema": "dbo", "name": "claim",
     "sql": "CREATE TABLE dbo.claim (claim_id BIGINT, amount DECIMAL(10,2))"},
    {"kind": "view", "schema": "dbo", "name": "v_claims",
     "sql": "CREATE VIEW dbo.v_claims AS SELECT 1"},
    {"kind": "procedure", "schema": "dbo", "name": "sp_load", "sql": "CREATE PROCEDURE ..."},
]}]
LAKEHOUSE = [{"name": "Sales", "tracking": {"shortcuts": "tracked"}, "shortcuts": [
    {"name": "claims_raw", "section": "Tables", "target_type": "AmazonS3",
     "target": "s3://acme/claims", "external": True},
    {"name": "files_only", "section": "Files", "target_type": "AmazonS3",
     "target": "s3://acme/f", "external": True},
]}]
NOTEBOOKS = [{"name": "Build_Aggregates", "content":
              'df.write.saveAsTable("claims_agg")\n'}]
TWO_WAREHOUSES = [
    {"name": "WareOne", "objects": [
        {"kind": "table", "schema": "dbo", "name": "CurrentDate",
         "sql": "CREATE TABLE dbo.CurrentDate (d DATE)"}]},
    {"name": "WareTwo", "objects": [
        {"kind": "table", "schema": "dbo", "name": "CurrentDate",
         "sql": "CREATE TABLE dbo.CurrentDate (d DATE)"}]},
]


class WrittenTableTests(unittest.TestCase):
    def test_save_as_table_literal(self):
        self.assertEqual(cat.extract_written_tables('df.write.saveAsTable("agg")'), {"agg"})

    def test_single_quotes_and_whitespace(self):
        self.assertEqual(cat.extract_written_tables("df.write.saveAsTable( 'agg' )"), {"agg"})

    def test_create_table_in_spark_sql(self):
        src = 'spark.sql("CREATE TABLE IF NOT EXISTS mart.agg AS SELECT 1")'
        self.assertEqual(cat.extract_written_tables(src), {"mart.agg"})

    def test_create_or_replace_table(self):
        src = 'spark.sql("CREATE OR REPLACE TABLE agg AS SELECT 1")'
        self.assertEqual(cat.extract_written_tables(src), {"agg"})

    def test_variable_target_is_not_inferred(self):
        self.assertEqual(cat.extract_written_tables("df.write.saveAsTable(name)"), set())

    def test_fstring_target_is_not_inferred(self):
        self.assertEqual(cat.extract_written_tables('df.write.saveAsTable(f"a_{x}")'), set())

    def test_non_string_input(self):
        self.assertEqual(cat.extract_written_tables(None), set())

    def test_a_commented_out_save_is_not_a_write(self):
        src = ('# df.write.saveAsTable("commented_out_table")\n'
               'real = df.write.saveAsTable("really_written")\n')
        self.assertEqual(cat.extract_written_tables(src), {"really_written"})

    def test_a_save_named_in_prose_is_not_a_write(self):
        src = 'note = "we saveAsTable(\'in_a_string\') sometimes"\n'
        self.assertEqual(cat.extract_written_tables(src), set())

    def test_a_commented_out_create_table_is_not_a_write(self):
        """Measured on the bundled estate: edkreuk_FMD_FRAMEWORK_005 has
        `#Check if Target exist, ... if not create table and exit`, and the
        catalog gained a table called `and`."""
        src = ("#Check if Target exist, if not create table and exit\n"
               "x = 1\n")
        self.assertEqual(cat.extract_written_tables(src), set())

    def test_a_create_table_inside_a_sql_string_is_still_a_write(self):
        src = 'spark.sql("CREATE TABLE mart.agg AS SELECT 1")\n'
        self.assertEqual(cat.extract_written_tables(src), {"mart.agg"})

    def test_a_create_table_in_an_fstring_is_not_a_write(self):
        src = 'spark.sql(f"CREATE TABLE mart.{name} AS SELECT 1")\n'
        self.assertEqual(cat.extract_written_tables(src), set())

    def test_a_cell_that_will_not_tokenize_contributes_nothing(self):
        src = 'x = "unterminated\ndf.write.saveAsTable("agg")\n'
        self.assertEqual(cat.extract_written_tables(src), set())


class NotebookCellTests(unittest.TestCase):
    """`extract_written_tables` is handed a whole Fabric notebook file, and
    that format carries non-Python cells as `# MAGIC ` comments. Masking
    Python comments over the raw text would have thrown every `%%sql` cell
    away, so the source is split into cells first and each is read in its
    own language."""

    HEADER = "# Fabric notebook source\n"
    CELL = "\n# CELL ********************\n"

    def _notebook(self, *cells):
        return self.HEADER + "".join(self.CELL + c + "\n" for c in cells)

    def test_a_magic_sql_cell_still_contributes_its_create(self):
        nb = self._notebook("# MAGIC %%sql\n"
                            "# MAGIC CREATE TABLE mart.agg AS SELECT 1")
        self.assertEqual(cat.extract_written_tables(nb), {"mart.agg"})

    def test_a_sql_comment_in_a_magic_sql_cell_is_not_a_write(self):
        nb = self._notebook("# MAGIC %%sql\n"
                            "# MAGIC -- CREATE TABLE mart.ghost AS SELECT 1\n"
                            "# MAGIC SELECT 1")
        self.assertEqual(cat.extract_written_tables(nb), set())

    def test_a_python_comment_in_a_python_cell_is_not_a_write(self):
        nb = self._notebook('# df.write.saveAsTable("ghost")\n'
                            'df.write.saveAsTable("real")')
        self.assertEqual(cat.extract_written_tables(nb), {"real"})

    def test_a_markdown_magic_cell_contributes_nothing(self):
        nb = self._notebook("# MAGIC %%markdown\n"
                            "# MAGIC we saveAsTable(\"prose\") here")
        self.assertEqual(cat.extract_written_tables(nb), set())


class BuildTests(unittest.TestCase):
    def test_warehouse_tables_and_views_are_tier_one(self):
        c = cat.build_catalog(_sources(warehouses=WAREHOUSE))
        self.assertEqual(c["tables"]["acmedw.dbo.claim"]["tier"],
                         cat.TIER_WAREHOUSE_DDL)
        self.assertEqual(c["tables"]["acmedw.dbo.v_claims"]["tier"],
                         cat.TIER_WAREHOUSE_DDL)

    def test_procedures_are_not_tables(self):
        c = cat.build_catalog(_sources(warehouses=WAREHOUSE))
        self.assertNotIn("sp_load", [e["name"].rsplit(".", 1)[-1]
                                     for e in c["tables"].values()])

    def test_warehouse_columns_are_captured(self):
        c = cat.build_catalog(_sources(warehouses=WAREHOUSE))
        columns = [col["name"]
                   for col in c["tables"]["acmedw.dbo.claim"]["columns"]]
        self.assertEqual(columns, ["claim_id", "amount"])

    def test_table_section_shortcuts_are_tier_two(self):
        c = cat.build_catalog(_sources(lakehouses=LAKEHOUSE))
        self.assertEqual(c["tables"]["sales.claims_raw"]["tier"], cat.TIER_SHORTCUT)
        self.assertEqual(c["tables"]["sales.claims_raw"]["target"],
                         "s3://acme/claims")

    def test_files_section_shortcuts_are_not_tables(self):
        c = cat.build_catalog(_sources(lakehouses=LAKEHOUSE))
        self.assertNotIn("files_only", [e["name"] for e in c["tables"].values()])

    def test_untracked_lakehouse_contributes_nothing(self):
        untracked = [{"name": "Sales", "tracking": {"shortcuts": "not_tracked"},
                      "shortcuts": []}]
        self.assertEqual(cat.build_catalog(_sources(lakehouses=untracked))["tables"], {})

    def test_notebook_writes_are_tier_four_and_name_their_creator(self):
        c = cat.build_catalog(_sources(notebooks=NOTEBOOKS))
        entry = c["tables"]["claims_agg"]
        self.assertEqual(entry["tier"], cat.TIER_NOTEBOOK_INFERRED)
        self.assertEqual(entry["created_by"], "Build_Aggregates")

    def test_tier_four_entries_never_claim_columns(self):
        c = cat.build_catalog(_sources(notebooks=NOTEBOOKS))
        self.assertNotIn("columns", c["tables"]["claims_agg"])

    def test_supplied_catalog_outranks_notebook_inference(self):
        supplied = {"claims_agg": {"columns": [{"name": "k", "type": "STRING"}]}}
        c = cat.build_catalog(_sources(notebooks=NOTEBOOKS), supplied=supplied)
        self.assertEqual(c["tables"]["claims_agg"]["tier"], cat.TIER_SUPPLIED)

    def test_warehouse_ddl_outranks_everything(self):
        notebooks = [{"name": "N", "content": 'df.write.saveAsTable("dbo.claim")'}]
        c = cat.build_catalog(_sources(warehouses=WAREHOUSE, notebooks=notebooks))
        self.assertEqual(c["tables"]["acmedw.dbo.claim"]["tier"],
                         cat.TIER_WAREHOUSE_DDL)

    def test_summary_counts_each_tier(self):
        c = cat.build_catalog(_sources(warehouses=WAREHOUSE, lakehouses=LAKEHOUSE,
                                       notebooks=NOTEBOOKS))
        self.assertEqual(c["summary"][cat.TIER_WAREHOUSE_DDL], 2)
        self.assertEqual(c["summary"][cat.TIER_SHORTCUT], 1)
        self.assertEqual(c["summary"][cat.TIER_NOTEBOOK_INFERRED], 1)
        self.assertEqual(c["summary"]["unresolved"], 0)

    def test_two_items_owning_one_table_name_do_not_shadow_each_other(self):
        """Both `WareSecondary` and `WareSecondary_2` in the bundled estate
        hold a `dbo.CurrentDate`. Keyed by bare table name, one of them
        simply was not in the catalog."""
        c = cat.build_catalog(_sources(warehouses=TWO_WAREHOUSES))
        self.assertEqual(c["summary"][cat.TIER_WAREHOUSE_DDL], 2)
        self.assertEqual(sorted(e["owner"] for e in c["tables"].values()),
                         ["WareOne", "WareTwo"])

    def test_a_notebook_write_is_owned_by_the_notebooks_default_lakehouse(self):
        notebooks = [{"name": "N", "default_lakehouse": "SalesLake",
                      "content": 'df.write.saveAsTable("agg")'}]
        c = cat.build_catalog(_sources(notebooks=notebooks))
        entry = c["tables"]["saleslake.agg"]
        self.assertEqual(entry["owner"], "SalesLake")
        self.assertEqual(entry["tier"], cat.TIER_NOTEBOOK_INFERRED)

    def test_an_ownerless_entry_is_still_outranked(self):
        """Precedence has to survive the new key: a notebook write of a
        table the warehouse already declares is the same table, not a
        second one, even though only one side records an owner."""
        notebooks = [{"name": "N", "content": 'df.write.saveAsTable("dbo.claim")'}]
        c = cat.build_catalog(_sources(warehouses=WAREHOUSE, notebooks=notebooks))
        self.assertEqual([e["tier"] for e in c["tables"].values()
                          if e["name"] == "dbo.claim"], [cat.TIER_WAREHOUSE_DDL])

    def test_empty_sources_yield_an_empty_catalog(self):
        c = cat.build_catalog(_sources())
        self.assertEqual(c["tables"], {})
        self.assertEqual(c["summary"]["unresolved"], 0)


class ResolveTests(unittest.TestCase):
    def setUp(self):
        self.catalog = cat.build_catalog(
            _sources(warehouses=WAREHOUSE, lakehouses=LAKEHOUSE, notebooks=NOTEBOOKS))

    def test_exact_qualified_match(self):
        self.assertEqual(cat.resolve(self.catalog, "dbo.claim")["tier"],
                         cat.TIER_WAREHOUSE_DDL)

    def test_bare_name_matches_a_qualified_entry(self):
        self.assertEqual(cat.resolve(self.catalog, "claim")["tier"],
                         cat.TIER_WAREHOUSE_DDL)

    def test_qualified_name_matches_a_bare_entry(self):
        self.assertEqual(cat.resolve(self.catalog, "Sales.claims_raw")["tier"],
                         cat.TIER_SHORTCUT)

    def test_matching_is_case_insensitive(self):
        self.assertIsNotNone(cat.resolve(self.catalog, "DBO.CLAIM"))

    def test_backticks_and_brackets_are_stripped(self):
        self.assertIsNotNone(cat.resolve(self.catalog, "`dbo`.`claim`"))
        self.assertIsNotNone(cat.resolve(self.catalog, "[dbo].[claim]"))

    def test_unknown_name_resolves_to_none(self):
        self.assertIsNone(cat.resolve(self.catalog, "nope"))

    def test_an_unknown_schema_does_not_resolve_to_another_schemas_table(self):
        """`other.claim` names a schema this export says nothing about.

        Matching on the last name part alone resolved it to `dbo.claim` --
        a different table, in a different schema, which then got a
        confident AIDP name and no flag.
        """
        self.assertIsNone(cat.resolve(self.catalog, "other.claim"))

    def test_an_ambiguous_bare_name_does_not_resolve(self):
        supplied = {"sales.claim": {}}
        c = cat.build_catalog(_sources(warehouses=WAREHOUSE), supplied=supplied)
        self.assertIsNone(cat.resolve(c, "claim"))
        self.assertEqual(len(cat.candidates(c, "claim")), 2)

    def test_an_ambiguous_bare_name_reports_its_candidates(self):
        supplied = {"sales.claim": {}}
        c = cat.build_catalog(_sources(warehouses=WAREHOUSE), supplied=supplied)
        self.assertEqual(
            sorted(e["name"] for e in cat.candidates(c, "claim")),
            ["dbo.claim", "sales.claim"])

    def test_a_reference_qualified_by_its_owning_item_resolves(self):
        self.assertEqual(cat.resolve(self.catalog, "AcmeDW.dbo.claim")["tier"],
                         cat.TIER_WAREHOUSE_DDL)

    def test_a_contradicting_schema_does_not_resolve_even_three_parts_deep(self):
        self.assertIsNone(cat.resolve(self.catalog, "AcmeDW.other.claim"))

    def test_the_item_slot_is_a_hint_not_a_constraint(self):
        """A reference that names the wrong item still reaches the one
        entry: the catalog's recorded owner is authoritative over a
        notebook's binding, which is what `_owning_item` already decides.
        The item slot only chooses *between* candidates -- see
        tests/test_notebook_table_names.py::test_the_entrys_warehouse_still_wins,
        which this would otherwise silently reverse."""
        self.assertEqual(cat.resolve(self.catalog, "OtherDW.dbo.claim")["name"],
                         "dbo.claim")

    def test_a_four_part_name_matches_nothing(self):
        self.assertEqual(cat.candidates(self.catalog, "srv.AcmeDW.dbo.claim"), [])


class LakehouseTableHasNoSchemaTests(unittest.TestCase):
    """A Lakehouse table has no schema in Fabric, and the catalog has to
    say so rather than invent one.

    Its entry key is `<item>.<table>` -- two parts -- and reading that key
    right-aligned put the *item* in the schema slot, so the entry claimed
    to live in a schema named after its own lakehouse. Every reference that
    named a real schema was then rejected as a different table, in both
    translators. The two spellings below are the ones that matter: Fabric's
    SQL analytics endpoint exposes Lakehouse tables under `dbo`, and a
    Warehouse cross-database query is three-part `[Lakehouse].[dbo].[table]`.
    Both came back "not found". See `_entry_slots`.
    """

    def setUp(self):
        self.catalog = cat.build_catalog(
            _sources(warehouses=WAREHOUSE, lakehouses=LAKEHOUSE))

    def test_the_bare_name_still_resolves(self):
        self.assertEqual(cat.resolve(self.catalog, "claims_raw")["tier"],
                         cat.TIER_SHORTCUT)

    def test_the_item_qualified_name_still_resolves(self):
        self.assertEqual(cat.resolve(self.catalog, "Sales.claims_raw")["tier"],
                         cat.TIER_SHORTCUT)

    def test_the_sql_endpoints_dbo_spelling_resolves(self):
        self.assertEqual(cat.resolve(self.catalog, "dbo.claims_raw")["tier"],
                         cat.TIER_SHORTCUT)

    def test_the_three_part_cross_database_spelling_resolves(self):
        self.assertEqual(
            cat.resolve(self.catalog, "Sales.dbo.claims_raw")["tier"],
            cat.TIER_SHORTCUT)

    def test_a_warehouse_entry_still_holds_its_schema_against_a_wrong_one(self):
        """The looseness is only for entries that genuinely have no schema.
        `dbo.claim` is declared with one, so `other.claim` is still a
        different table -- the case the slot comparison was added for."""
        self.assertIsNone(cat.resolve(self.catalog, "other.claim"))

    def test_an_item_whose_display_name_contains_a_dot_stays_one_atom(self):
        """`_add` promises "nothing parses the key back apart"; slicing the
        key gave `toolbox` as the item for `gbrueckl_Fabric.Toolbox`."""
        dotted = [{"name": "gbrueckl_Fabric.Toolbox", "objects": [
            {"kind": "table", "schema": "dbo", "name": "t",
             "sql": "CREATE TABLE dbo.t (id INT)"}]}]
        c = cat.build_catalog(_sources(warehouses=dotted))
        key, entry = next(iter(c["tables"].items()))
        self.assertEqual(cat._entry_slots(key, entry),
                         ("gbrueckl_fabric.toolbox", "dbo", "t"))
        self.assertIsNotNone(cat.resolve(c, "dbo.t"))

    def test_an_entry_with_no_name_of_its_own_falls_back_to_the_key(self):
        """A catalog assembled by hand in a test, or written by a release
        from before `_add` recorded `name`."""
        self.assertEqual(cat._entry_slots("acmedw.dbo.claim", {}),
                         ("acmedw", "dbo", "claim"))


class OwnerTests(unittest.TestCase):
    def setUp(self):
        self.catalog = cat.build_catalog(_sources(warehouses=TWO_WAREHOUSES))

    def test_a_reference_naming_its_item_picks_that_item(self):
        self.assertEqual(
            cat.resolve(self.catalog, "WareTwo.dbo.CurrentDate")["owner"],
            "WareTwo")

    def test_a_reference_naming_no_item_is_ambiguous(self):
        self.assertIsNone(cat.resolve(self.catalog, "dbo.CurrentDate"))
        self.assertEqual(len(cat.candidates(self.catalog, "dbo.CurrentDate")), 2)

    def test_a_caller_supplied_owner_breaks_the_tie(self):
        self.assertEqual(
            cat.resolve(self.catalog, "dbo.CurrentDate", owner="WareOne")["owner"],
            "WareOne")

    def test_an_owner_that_matches_nothing_leaves_it_ambiguous(self):
        self.assertIsNone(
            cat.resolve(self.catalog, "dbo.CurrentDate", owner="Elsewhere"))

    def test_an_owner_never_rejects_the_only_candidate(self):
        c = cat.build_catalog(_sources(warehouses=WAREHOUSE))
        self.assertIsNotNone(cat.resolve(c, "dbo.claim", owner="Elsewhere"))

    def test_candidates_of_an_unknown_name_is_empty(self):
        self.assertEqual(cat.candidates(self.catalog, "nope"), [])

    def test_empty_name_resolves_to_none(self):
        self.assertIsNone(cat.resolve(self.catalog, ""))


class SuppliedCatalogTests(unittest.TestCase):
    def _load(self, text):
        with TemporaryDirectory() as t:
            p = Path(t) / "tables.csv"
            p.write_text(text, encoding="utf-8")
            return cat.load_supplied_catalog(p)

    def test_name_only_csv(self):
        self.assertIn("dbo.claim", self._load("table\ndbo.claim\n"))

    def test_name_and_columns(self):
        loaded = self._load("table,column,type\ndbo.claim,id,BIGINT\ndbo.claim,amt,DOUBLE\n")
        self.assertEqual([c["name"] for c in loaded["dbo.claim"]["columns"]], ["id", "amt"])

    def test_blank_rows_are_skipped(self):
        self.assertEqual(len(self._load("table\n\ndbo.claim\n\n")), 1)

    def test_missing_file_raises(self):
        with self.assertRaises(ValueError):
            cat.load_supplied_catalog(Path("/nonexistent/tables.csv"))

    def test_header_without_a_table_column_raises(self):
        with self.assertRaises(ValueError):
            self._load("thing,other\na,b\n")


class SuppliedCatalogHeaderTests(unittest.TestCase):
    """`Table,Lakehouse` is what a person writes and what Excel produces.

    It loaded 0 rows, with no error and no warning: the header *check*
    folded case, the *read* used `row["table"]`, which is
    `csv.DictReader`'s literal key. So `--tables-csv` did nothing and
    every table fell through to unknown. A semicolon export has always
    raised with a clear message -- same mistake, opposite behaviour, and
    silence is the wrong half.
    """

    def _load(self, text):
        with TemporaryDirectory() as t:
            p = Path(t) / "tables.csv"
            p.write_text(text, encoding="utf-8")
            return cat.load_supplied_catalog(p)

    def test_a_capitalised_header_is_read(self):
        self.assertIn("claims", self._load("Table,Lakehouse\nclaims,Sales\n"))

    def test_an_upper_case_header_is_read(self):
        self.assertIn("claims", self._load("TABLE,LAKEHOUSE\nclaims,Sales\n"))

    def test_spaces_around_a_header_name_are_ignored(self):
        self.assertIn("claims", self._load("table , lakehouse\nclaims,Sales\n"))

    def test_a_capitalised_column_header_is_read_too(self):
        loaded = self._load("Table,Column,Type\ndbo.claim,id,BIGINT\n")
        self.assertEqual(loaded["dbo.claim"]["columns"],
                         [{"name": "id", "type": "BIGINT"}])

    def test_a_byte_order_mark_still_does_not_break_the_header(self):
        """The BOM half was fixed before this one; it stays fixed."""
        self.assertIn("claims",
                      self._load("\ufefftable,lakehouse\nclaims,Sales\n"))

    def test_a_file_that_names_no_tables_raises(self):
        with self.assertRaises(ValueError):
            self._load("table,lakehouse\n")

    def test_a_file_whose_table_column_is_all_blank_raises(self):
        with self.assertRaises(ValueError):
            self._load("table,lakehouse\n,Sales\n")

    def test_the_refusal_says_what_it_saw(self):
        with self.assertRaises(ValueError) as caught:
            self._load("table,lakehouse\n,Sales\n")
        message = str(caught.exception)
        self.assertIn("named no tables", message)
        self.assertIn("1 data row", message)
        self.assertIn("table", message)

    def test_a_semicolon_export_still_raises_the_older_message(self):
        """The behaviour the silent case is being made to match."""
        with self.assertRaises(ValueError) as caught:
            self._load("table;lakehouse\nclaims;Sales\n")
        self.assertIn("must have a 'table' column", str(caught.exception))



class ExtractReadTablesTests(unittest.TestCase):
    """The other half of `extract_written_tables`, and the half missing.

    The writes were already found, so a notebook that writes
    `agg_claims` and one that reads it were two nodes with no edge
    between them and the plan emitted the reader first whenever the
    names sorted that way.
    """

    def test_spark_table(self):
        self.assertEqual(cat.extract_read_tables('df = spark.table("agg")'),
                         {"agg"})

    def test_spark_read_table(self):
        self.assertEqual(
            cat.extract_read_tables('df = spark.read.table("dbo.agg")'),
            {"dbo.agg"})

    def test_a_session_bound_to_another_name(self):
        self.assertEqual(cat.extract_read_tables('sess.table("agg")'), {"agg"})

    def test_sql_handed_to_spark_sql(self):
        self.assertEqual(
            cat.extract_read_tables('spark.sql("SELECT * FROM agg JOIN dim ON 1=1")'),
            {"agg", "dim"})

    def test_a_variable_target_is_not_a_read(self):
        self.assertEqual(cat.extract_read_tables("spark.table(name)"), set())

    def test_a_commented_out_read_is_not_a_read(self):
        self.assertEqual(cat.extract_read_tables('# spark.table("agg")'), set())

    def test_a_temp_view_this_notebook_registers_is_not_a_read(self):
        source = ('df.createOrReplaceTempView("tmp")\n'
                  'other = spark.table("tmp")\n')
        self.assertEqual(cat.extract_read_tables(source), set())

    def test_a_sql_temp_view_is_not_a_read_either(self):
        source = ('spark.sql("CREATE OR REPLACE TEMP VIEW tmp AS SELECT 1")\n'
                  'spark.sql("SELECT * FROM tmp")\n')
        self.assertEqual(cat.extract_read_tables(source), set())

    def test_a_write_is_not_a_read(self):
        self.assertEqual(
            cat.extract_read_tables('df.write.saveAsTable("agg")'), set())

    def test_a_sql_cell_is_read_as_sql(self):
        source = ("# Fabric notebook source\n\n"
                  "# CELL ********************\n\n"
                  "# MAGIC %%sql\n"
                  "# MAGIC SELECT * FROM agg\n")
        self.assertEqual(cat.extract_read_tables(source), {"agg"})

    def test_non_string_input_is_empty(self):
        self.assertEqual(cat.extract_read_tables(None), set())


class TempViewNameTests(unittest.TestCase):
    """One definition of "this name is not a catalog table", shared with
    the notebook translator, which imports it from here."""

    def test_the_python_spellings(self):
        for call in ("createTempView", "createOrReplaceTempView",
                     "createGlobalTempView", "createOrReplaceGlobalTempView"):
            with self.subTest(call=call):
                self.assertEqual(cat.temp_view_names('df.%s("v")' % call),
                                 frozenset({"v"}))

    def test_the_sql_spelling(self):
        self.assertEqual(
            cat.temp_view_names("CREATE OR REPLACE TEMPORARY VIEW v AS SELECT 1"),
            frozenset({"v"}))

    def test_a_dotted_name_is_not_a_temp_view(self):
        self.assertEqual(cat.temp_view_names('df.createTempView("a.b")'),
                         frozenset())


class ClassifyReferenceTests(unittest.TestCase):
    """The one answer both translators ask for.

    `resolve` plus `candidates` plus a tier test is the whole of "is this
    table known", and it was written out longhand inside the notebook
    translator. The warehouse T-SQL translator needed the identical decision
    and a second copy would have drifted -- this codebase has paid for that
    twice, over a rule id claimed by two branches and over two places
    building a lakehouse bucket name. Only the wording stays per-translator.
    """

    def setUp(self):
        self.catalog = cat.build_catalog(
            _sources(warehouses=WAREHOUSE, lakehouses=LAKEHOUSE,
                     notebooks=NOTEBOOKS))

    def test_a_declared_table_is_known(self):
        verdict, entry, _ = cat.classify_reference(self.catalog, "dbo.claim")
        self.assertEqual(verdict, cat.REF_KNOWN)
        self.assertEqual(entry["name"], "dbo.claim")

    def test_a_shortcut_is_told_apart_from_an_ordinary_table(self):
        verdict, entry, _ = cat.classify_reference(self.catalog, "claims_raw")
        self.assertEqual(verdict, cat.REF_SHORTCUT)
        self.assertEqual(entry["target"], "s3://acme/claims")

    def test_a_table_only_a_notebook_write_implies_is_inferred(self):
        verdict, entry, _ = cat.classify_reference(self.catalog, "claims_agg")
        self.assertEqual(verdict, cat.REF_INFERRED)
        self.assertEqual(entry["created_by"], "Build_Aggregates")

    def test_nothing_matching_is_unknown(self):
        verdict, entry, matches = cat.classify_reference(self.catalog, "nope")
        self.assertEqual(verdict, cat.REF_UNKNOWN)
        self.assertIsNone(entry)
        self.assertEqual(matches, [])

    def test_several_matching_is_ambiguous_and_names_them(self):
        """Not `unknown`: that sends a reader looking for a table the
        export does contain, twice."""
        supplied = {"sales.claim": {}}
        c = cat.build_catalog(_sources(warehouses=WAREHOUSE), supplied=supplied)
        verdict, entry, matches = cat.classify_reference(c, "claim")
        self.assertEqual(verdict, cat.REF_AMBIGUOUS)
        self.assertIsNone(entry)
        self.assertEqual(sorted(e["name"] for e in matches),
                         ["dbo.claim", "sales.claim"])

    def test_the_owner_hint_chooses_between_candidates(self):
        c = cat.build_catalog(_sources(warehouses=TWO_WAREHOUSES))
        self.assertEqual(
            cat.classify_reference(c, "dbo.CurrentDate")[0], cat.REF_AMBIGUOUS)
        verdict, entry, _ = cat.classify_reference(
            c, "dbo.CurrentDate", owner="WareTwo")
        self.assertEqual(verdict, cat.REF_KNOWN)
        self.assertEqual(cat.entry_owner(entry), "waretwo")

    def test_an_empty_catalog_calls_everything_unknown(self):
        """Which is why every caller has to decide what to do with no
        catalog *before* asking: one that skips the check turns "nothing was
        supplied" into "this table does not exist" for the whole estate.

        This is `classify_reference`'s contract and it is deliberately
        unchanged. The fix for that trap is `names_no_tables` below, in the
        callers, not a fourth verdict here: the function answers the question
        it was asked, and "should I be asking at all" is the caller's.
        """
        self.assertEqual(
            cat.classify_reference({"tables": {}}, "dbo.claim")[0],
            cat.REF_UNKNOWN)


class NamesNoTablesTests(unittest.TestCase):
    """The check every caller of `classify_reference` has to make first.

    They were all testing `not catalog`, which is right for `None` and for
    `{}` and wrong for the shape a plan actually carries. MEASURED on
    03f019b:

        classify_reference({"tables": {}, "shortcuts": []}, "dbo.claim", "W")
          ->  ('unknown', None, [])

    -- and `{"tables": {}, "shortcuts": []}` is truthy, so the guard let it
    through and every reference in the estate collected its own
    SQ19_TABLE_UNKNOWN / NB12_TABLE_UNKNOWN.
    """

    def test_the_shape_a_plan_carries_when_nothing_resolved(self):
        """The one the old `not catalog` test missed."""
        self.assertTrue(cat.names_no_tables({"summary": {}, "tables": {}}))
        self.assertTrue(cat.names_no_tables({"tables": {}, "shortcuts": []}))

    def test_none_and_empty_are_still_empty(self):
        for value in (None, {}, "", 0, []):
            with self.subTest(value=value):
                self.assertTrue(cat.names_no_tables(value))

    def test_a_malformed_tables_field_names_no_tables(self):
        for tables in (None, [], "claim", 3):
            with self.subTest(tables=tables):
                self.assertTrue(cat.names_no_tables({"tables": tables}))

    def test_one_table_is_not_empty(self):
        self.assertFalse(cat.names_no_tables(
            {"tables": {"dbo.claim": {"tier": cat.TIER_WAREHOUSE_DDL}}}))

    def test_a_built_catalog_over_a_real_export_is_not_empty(self):
        """Guards the definition against the thing it is used for: if a
        catalog this module builds from a warehouse could be called empty,
        the guard would switch every check off."""
        built = cat.build_catalog(_sources(warehouses=TWO_WAREHOUSES))
        self.assertFalse(cat.names_no_tables(built))

    def test_a_shortcut_only_catalog_is_not_empty(self):
        """`tables` is the whole of the test, so this has to hold: a
        shortcut is recorded in `tables` under its own tier as well as in
        the `shortcuts` map."""
        catalog = {"tables": {"saleslake.raw": {
            "tier": cat.TIER_SHORTCUT, "owner": "SalesLake",
            "name": "raw", "target": "s3://b/k"}}, "shortcuts": {}}
        self.assertFalse(cat.names_no_tables(catalog))
        self.assertEqual(
            cat.classify_reference(catalog, "raw")[0], cat.REF_SHORTCUT)


if __name__ == "__main__":
    unittest.main()
