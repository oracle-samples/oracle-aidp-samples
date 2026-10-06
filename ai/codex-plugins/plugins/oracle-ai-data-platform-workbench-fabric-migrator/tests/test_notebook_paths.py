"""Notebook path rewriting: what a OneLake location becomes on OCI.

One file for the four defects in the `notebook-paths` batch, because they
are one decision taken four times -- what a string has to be before this
tool is allowed to point it somewhere else.
"""
import unittest

from fabric_aidp.translate import fabric_notebook_to_spark as nb2spark
from fabric_aidp.translate import onelake_to_oci as o

NS = "ns"
HEADER = "# Fabric notebook source\n\n# CELL ********************\n\n"
CATALOG = {"tables": {
    "claims_raw": {"tier": "shortcut", "name": "claims_raw",
                   "target": "s3://acme/claims", "lakehouse": "Sales"},
}}


def _translate(body, **kw):
    kw.setdefault("namespace", NS)
    kw.setdefault("default_lakehouse", "Sales")
    kw.setdefault("catalog", CATALOG)
    return nb2spark.translate(HEADER + body + "\n", **kw)


def _body(result) -> str:
    return result.translated_sql.split("# CELL ********************")[1].strip()


def _rules(result) -> list:
    return [f.rule for f in result.findings]


class ItemTypeAndWorkspaceTests(unittest.TestCase):
    """D1. `abfss://ws@onelake.../lh.Lakehouse/Tables/claim` came out as
    `oci://lh@ns/Tables/claim`: the item type and the workspace were both
    dropped. A Fabric display name is unique only inside one workspace and
    one item type, so on their own two genuinely different items -- a
    Lakehouse and a Warehouse both called `Sales`, or the same name in two
    workspaces -- collided on one bucket with nothing said about it. This
    is the defect `shortcut_to_oci` fixed for Azure containers in PR #9,
    and the name is built the same way: `<authority>_<name>`, `_`.
    """

    def test_the_item_type_stays_in_the_bucket_name(self):
        lakehouse = o.map_onelake_path(
            "abfss://ws@onelake.dfs.fabric.microsoft.com/Sales.Lakehouse/Tables/claim",
            namespace=NS)
        warehouse = o.map_onelake_path(
            "abfss://ws@onelake.dfs.fabric.microsoft.com/Sales.Warehouse/Tables/claim",
            namespace=NS)
        self.assertNotEqual(lakehouse.mapped, warehouse.mapped)
        self.assertEqual(lakehouse.mapped, "oci://ws_Sales_Lakehouse@ns/Tables/claim")
        self.assertEqual(warehouse.mapped, "oci://ws_Sales_Warehouse@ns/Tables/claim")

    def test_two_workspaces_do_not_share_one_bucket(self):
        first = o.map_onelake_path(
            "abfss://wsa@onelake.dfs.fabric.microsoft.com/Sales.Lakehouse/Files/x",
            namespace=NS)
        second = o.map_onelake_path(
            "abfss://wsb@onelake.dfs.fabric.microsoft.com/Sales.Lakehouse/Files/x",
            namespace=NS)
        self.assertNotEqual(first.mapped, second.mapped)

    def test_the_join_is_refused_when_it_would_not_split_back(self):
        """`shortcut_to_oci` picks `_` because no name it joins can contain
        one. A Fabric workspace display name can, so the same separator is
        only injective while the workspace has none -- and where it does
        not hold the mapping says so instead of emitting a name that two
        different items could both produce."""
        result = o.map_onelake_path(
            "abfss://ws_a@onelake.dfs.fabric.microsoft.com/b_c.Lakehouse/Files/x",
            namespace=NS)
        self.assertIsNone(result.mapped)
        self.assertIn("_", result.reason)

    def test_a_relative_path_is_not_qualified_because_nothing_names_a_workspace(self):
        """The FUSE and relative forms resolve against the notebook's
        binding, and a Fabric export records that binding's workspace
        nowhere -- only a GUID, usually empty. Nothing is dropped there, so
        nothing is added."""
        result = o.map_onelake_path("Files/raw/x.csv", namespace=NS,
                                    default_lakehouse="Sales")
        self.assertEqual(result.mapped, "oci://Sales@ns/Files/raw/x.csv")

    def test_the_two_spellings_of_one_lakehouse_are_reported(self):
        """The cost of the line above: a notebook bound to `Sales` that
        also spells it out as `abfss://AcmeWS@.../Sales.Lakehouse/...`
        can get two bucket names for one lakehouse. That is visible and a
        human can merge it; the collision it replaces was invisible."""
        result = _translate(QUALIFIED)
        self.assertIn("NB25_LAKEHOUSE_TWO_BUCKETS", _rules(result))


QUALIFIED = ('df = spark.read.parquet("abfss://AcmeWS@onelake.dfs.fabric'
             '.microsoft.com/Sales.Lakehouse/Files/raw")')
QUALIFIED_BUCKET = "oci://AcmeWS_Sales_Lakehouse@ns"
PLAIN_BUCKET = "oci://Sales@ns"


def _finding(result, rule):
    return next(f for f in result.findings if f.rule == rule)


class TwoBucketSeverityTests(unittest.TestCase):
    """NB25 graded every notebook the same way, and the notebooks differ.

    A qualified path naming the bound lakehouse is only a heads-up while
    it is the *only* spelling in the notebook: one bucket is emitted and
    there is nothing to reconcile. Add a `Files/...` or
    `/lakehouse/default/...` path for the same lakehouse and the notebook
    emits two bucket names for one file -- one of those buckets will not
    exist, so one of the two reads fails at run time. Both graded `info`,
    so the second shape came out PASS.

    The severity cannot be decided from one literal: it depends on what
    the whole notebook emitted. So the bucket names are collected as the
    rewrites happen and NB25 is raised once, at the end.

    A third shape was silent, and that silence used to be pinned here as
    `test_relative_paths_alone_say_nothing`. The qualified-only notebook
    already says "any other artifact in this estate that does reach the
    same lakehouse that way is on the other bucket" -- so the warning
    existed, and only one of the two ends of the split carried it. Which
    end a reviewer happened to open decided whether they heard about it.
    The relative-only notebook now carries the mirror: same rule, same
    `info`, same single bucket emitted, no grade moved.
    """

    def test_one_spelling_is_a_heads_up(self):
        result = _translate(QUALIFIED)
        finding = _finding(result, "NB25_LAKEHOUSE_TWO_BUCKETS")
        self.assertEqual(finding.severity, "info")
        self.assertEqual(result.flags, 0)

    def test_the_heads_up_does_not_assert_a_path_that_is_not_there(self):
        """It used to say `Files/...` in the same notebook maps to bucket
        Sales, in the present tense, about a notebook containing no such
        path."""
        detail = _finding(result := _translate(QUALIFIED),
                          "NB25_LAKEHOUSE_TWO_BUCKETS").detail
        self.assertNotIn(PLAIN_BUCKET[len("oci://"):].join(("emits ", "")), detail)
        self.assertIn("would", detail)
        self.assertIn("AcmeWS_Sales_Lakehouse", detail)
        self.assertNotIn("oci://Sales@ns/Files/raw", _body(result))

    def test_a_relative_path_beside_it_is_a_flag(self):
        result = _translate(
            QUALIFIED + '\nother = spark.read.parquet("Files/raw/c")')
        self.assertIn(QUALIFIED_BUCKET + "/Files/raw", _body(result))
        self.assertIn(PLAIN_BUCKET + "/Files/raw/c", _body(result))
        finding = _finding(result, "NB25_LAKEHOUSE_TWO_BUCKETS")
        self.assertEqual(finding.severity, "flag")

    def test_the_flag_names_both_buckets_and_both_spellings(self):
        result = _translate(
            QUALIFIED + '\nother = spark.read.parquet("Files/raw/c")')
        detail = _finding(result, "NB25_LAKEHOUSE_TWO_BUCKETS").detail
        self.assertIn("AcmeWS_Sales_Lakehouse", detail)
        self.assertIn("'Sales'", detail)
        self.assertIn("Files/raw/c", detail)
        self.assertIn("abfss://AcmeWS@", detail)

    def test_a_fuse_path_beside_it_is_a_flag_too(self):
        result = _translate(
            QUALIFIED + '\nother = spark.read.parquet("/lakehouse/default/Files/raw/c")')
        self.assertIn(PLAIN_BUCKET + "/Files/raw/c", _body(result))
        self.assertEqual(
            _finding(result, "NB25_LAKEHOUSE_TWO_BUCKETS").severity, "flag")

    def test_it_is_raised_once_for_the_notebook_not_once_per_path(self):
        result = _translate(QUALIFIED + "\n" + QUALIFIED.replace("raw", "curated"))
        self.assertEqual(_rules(result).count("NB25_LAKEHOUSE_TWO_BUCKETS"), 1)

    def test_relative_paths_alone_carry_the_mirror_of_the_heads_up(self):
        """This asserted silence, and the silence was the defect.

        MEASURED before the change, one lakehouse `Sales` across two
        notebooks:

            notebook bound + `abfss://AcmeWS@.../Sales.Lakehouse/...`
                -> bucket AcmeWS_Sales_Lakehouse, NB25 info
            notebook bound + `Files/raw/c`
                -> bucket Sales,                  NO FINDING AT ALL

        Only one of those two buckets exists after the migration, so one
        of the two notebooks fails at run time -- and the one that stayed
        quiet is as likely to be the one a reviewer opens. `info`, not
        `flag`: this notebook emits one bucket and both rewrites are
        applied, so nothing *in it* is unresolved, exactly as for the
        qualified-only shape it mirrors.
        """
        result = _translate('df = spark.read.parquet("Files/raw/c")')
        finding = _finding(result, "NB25_LAKEHOUSE_TWO_BUCKETS")
        self.assertEqual(finding.severity, "info")
        self.assertEqual(result.flags, 0)

    def test_the_mirror_names_this_bucket_and_the_shape_of_the_other(self):
        detail = _finding(_translate('df = spark.read.parquet("Files/raw/c")'),
                          "NB25_LAKEHOUSE_TWO_BUCKETS").detail
        self.assertIn("'Sales'", detail)            # the bucket it did emit
        self.assertIn("Files/raw/c", detail)        # the spelling that got it
        self.assertIn("<workspace>_Sales_Lakehouse", detail)  # the other one
        self.assertNotIn("AcmeWS", detail)          # which is NOT knowable here

    def test_the_mirror_does_not_claim_to_know_which_bucket_is_right(self):
        """The workspace *name* of a bound lakehouse is the one fact that
        would settle it, and a Git export records it nowhere -- so the
        detail has to say so rather than pick."""
        detail = _finding(_translate('df = spark.read.parquet("Files/raw/c")'),
                          "NB25_LAKEHOUSE_TWO_BUCKETS").detail
        self.assertIn("cannot be decided from a Git export", detail)

    def test_a_fuse_path_alone_carries_it_too(self):
        result = _translate(
            'df = spark.read.parquet("/lakehouse/default/Files/raw/c")')
        self.assertEqual(
            _finding(result, "NB25_LAKEHOUSE_TWO_BUCKETS").severity, "info")

    def test_it_is_raised_once_for_several_relative_paths(self):
        result = _translate('a = spark.read.parquet("Files/raw/c")\n'
                            'b = spark.read.parquet("Files/raw/d")')
        self.assertEqual(_rules(result).count("NB25_LAKEHOUSE_TWO_BUCKETS"), 1)

    def test_a_notebook_reaching_no_lakehouse_file_still_says_nothing(self):
        """The mirror is about a bucket that WAS emitted. A notebook with
        no lakehouse path at all emits none, so there is no second
        spelling to warn about and the rule must stay quiet."""
        result = _translate('df = spark.table("claims_raw")')
        self.assertNotIn("NB25_LAKEHOUSE_TWO_BUCKETS", _rules(result))

    def test_an_unbound_notebook_says_nothing(self):
        """With no binding a relative path resolves to no item, so there
        is no lakehouse to have two buckets for."""
        result = _translate('df = spark.read.parquet("Files/raw/c")',
                            default_lakehouse=None)
        self.assertNotIn("NB25_LAKEHOUSE_TWO_BUCKETS", _rules(result))

    def test_two_workspaces_naming_one_item_name_are_two_items(self):
        """`wsa/Sales.Lakehouse` and `wsb/Sales.Lakehouse` are different
        lakehouses that share a display name -- two buckets is the right
        answer and the D1 fix exists to produce it, so this must not be
        reported as one lakehouse spelled twice."""
        result = _translate(
            'a = spark.read.parquet("abfss://wsa@onelake.dfs.fabric.microsoft'
            '.com/Sales.Lakehouse/Files/x")\n'
            'b = spark.read.parquet("abfss://wsb@onelake.dfs.fabric.microsoft'
            '.com/Sales.Lakehouse/Files/x")')
        self.assertNotIn(
            "flag", [f.severity for f in result.findings
                     if f.rule == "NB25_LAKEHOUSE_TWO_BUCKETS"])


class RelativePathEvidenceTests(unittest.TestCase):
    """D2. `p = "Files/raw/x.csv"` became `p = "oci://Sales@ns/Files/raw/x.csv"`.

    Neither `Files/raw/x.csv` nor `Tables/claim` is a OneLake path on its
    own -- both are ordinary strings that happen to start with a word
    Fabric also uses for a folder, and the rule rewrote a relative path, a
    dictionary key, a label or a log message on the strength of that
    prefix. The tokenizer-backed mask already answers *string* versus
    *code*; what it cannot answer is whether a string is a path. The
    enclosing call can, and where nothing does this now flags.

    The absolute forms are untouched: `abfss://...` and
    `/lakehouse/default/...` can only be paths, so nothing has to say so.
    """

    def test_a_bare_files_string_is_not_rewritten(self):
        result = _translate('p = "Files/raw/x.csv"')
        self.assertIn('p = "Files/raw/x.csv"', _body(result))
        self.assertIn("NB26_RELATIVE_PATH_UNVERIFIED", _rules(result))
        self.assertNotIn("NB01_ONELAKE_PATH", _rules(result))

    def test_a_bare_tables_string_is_not_rewritten(self):
        result = _translate('p = "Tables/claim"')
        self.assertIn('p = "Tables/claim"', _body(result))
        self.assertIn("NB26_RELATIVE_PATH_UNVERIFIED", _rules(result))

    def test_a_dictionary_value_is_not_a_path(self):
        result = _translate('target = {"path": "Tables/" + schema}')
        self.assertIn('"Tables/"', _body(result))
        self.assertIn("NB26_RELATIVE_PATH_UNVERIFIED", _rules(result))

    def test_the_refusal_names_the_construct(self):
        detail = next(f.detail for f in _translate('p = "Files/raw/x.csv"').findings
                      if f.rule == "NB26_RELATIVE_PATH_UNVERIFIED")
        self.assertIn("Files/raw/x.csv", detail)
        self.assertIn("spark.read", detail)

    def test_a_spark_reader_is_evidence(self):
        result = _translate('df = spark.read.parquet("Files/raw/x.csv")')
        self.assertIn("oci://Sales@ns/Files/raw/x.csv", _body(result))
        self.assertIn("NB01_ONELAKE_PATH", _rules(result))

    def test_a_spark_writer_is_evidence(self):
        result = _translate('df.write.mode("overwrite").save("Files/out")')
        self.assertIn("oci://Sales@ns/Files/out", _body(result))

    def test_the_fabric_filesystem_utility_is_evidence(self):
        result = _translate('mssparkutils.fs.ls("Files/raw")')
        self.assertIn("oci://Sales@ns/Files/raw", _body(result))

    def test_a_fuse_path_needs_no_evidence(self):
        """`/lakehouse/default/...` is a OneLake location under every
        reading, so the prefix is not what is being trusted."""
        result = _translate('p = "/lakehouse/default/Files/x.csv"')
        self.assertIn("oci://Sales@ns/Files/x.csv", _body(result))
        self.assertNotIn("NB26_RELATIVE_PATH_UNVERIFIED", _rules(result))

    def test_an_abfss_uri_needs_no_evidence(self):
        result = _translate(
            'p = "abfss://ws@onelake.dfs.fabric.microsoft.com/lh.Lakehouse/Files/x"')
        self.assertIn("oci://ws_lh_Lakehouse@ns/Files/x", _body(result))
        self.assertNotIn("NB26_RELATIVE_PATH_UNVERIFIED", _rules(result))

    def test_the_same_hole_in_sql_closes_with_it(self):
        """A `%%sql` cell and a `.sql()` string go through one rule, so a
        label that happens to read `Tables/claim` was rewritten there too."""
        result = _translate("""spark.sql("SELECT 'Tables/claim' AS label")""")
        self.assertIn("'Tables/claim'", _body(result))
        self.assertIn("NB26_RELATIVE_PATH_UNVERIFIED", _rules(result))

    def test_a_sql_location_clause_is_evidence(self):
        result = _translate(
            """spark.sql("CREATE TABLE t LOCATION 'Files/raw'")""")
        self.assertIn("oci://Sales@ns/Files/raw", _body(result))


# The same shortcut twice: once the way an older inventory recorded it, in
# `tables` at the shortcut tier, and once the way `build_catalog` records
# it now, in `shortcuts`, which is the only place a `Files` section one can
# appear at all.
MAPPABLE = {
    "tables": {
        "claims_raw": {"tier": "shortcut", "name": "claims_raw",
                       "section": "Tables", "lakehouse": "Sales",
                       "target_type": "AmazonS3",
                       "target": "https://acme-raw.s3.us-east-1.amazonaws.com/claims"},
    },
    "shortcuts": [
        {"name": "landing", "section": "Files", "lakehouse": "Sales",
         "target_type": "AdlsGen2",
         "target": "https://acmestore.dfs.core.windows.net/landing"},
    ],
}


TWO_SCHEMAS = {"shortcuts": [
    {"name": "ext", "table_name": "sales.ext", "schema": "sales", "section": "Tables",
     "lakehouse": "Sales", "target_type": "AmazonS3",
     "target": "https://acme.s3.us-east-1.amazonaws.com/sales"},
    {"name": "ext", "table_name": "hr.ext", "schema": "hr", "section": "Tables",
     "lakehouse": "Sales", "target_type": "AmazonS3",
     "target": "https://other.s3.us-east-1.amazonaws.com/hr"},
    {"name": "drop", "schema": "raw", "section": "Files", "lakehouse": "Sales",
     "target_type": "AmazonS3", "target": "https://landing.s3.us-east-1.amazonaws.com/raw"},
    {"name": "drop", "schema": "archive", "section": "Files", "lakehouse": "Sales",
     "target_type": "AmazonS3", "target": "https://cold.s3.us-east-1.amazonaws.com/old"},
]}


class ShortcutUnderSchemaTests(unittest.TestCase):
    """Two shortcuts with one name in two folders are two objects. Before,
    `Tables/hr/ext` resolved to the `Tables/sales/ext` shortcut -- whichever
    came first -- as a clean NB27 rewrite to the wrong bucket."""

    def test_each_schema_reaches_its_own_shortcut(self):
        for path, bucket in (("Tables/hr/ext/p.parquet", "oci://other@ns/hr/p.parquet"),
                             ("Tables/sales/ext/p.parquet", "oci://acme@ns/sales/p.parquet")):
            with self.subTest(path=path):
                result = _translate(f'df = spark.read.parquet("{path}")', catalog=TWO_SCHEMAS)
                self.assertIn(bucket, _body(result))

    def test_the_order_of_the_list_does_not_decide(self):
        flipped = {"shortcuts": list(reversed(TWO_SCHEMAS["shortcuts"]))}
        result = _translate('df = spark.read.parquet("Tables/hr/ext/p.parquet")', catalog=flipped)
        self.assertIn("oci://other@ns/hr/p.parquet", _body(result))

    def test_a_files_subfolder_shortcut_is_told_apart_too(self):
        result = _translate('df = spark.read.csv("Files/archive/drop/x.csv")', catalog=TWO_SCHEMAS)
        self.assertIn("oci://cold@ns/old/x.csv", _body(result))

    def test_a_schema_only_in_table_name_still_counts(self):
        glued = {"shortcuts": [{k: v for k, v in s.items() if k != "schema"}
                               for s in TWO_SCHEMAS["shortcuts"][:2]]}
        result = _translate('df = spark.read.parquet("Tables/hr/ext/p.parquet")', catalog=glued)
        self.assertIn("oci://other@ns/hr/p.parquet", _body(result))


class ShortcutPathTests(unittest.TestCase):
    """D3. A read of `Files/claims_raw/part.parquet` where `claims_raw` is a
    shortcut came out as `oci://Sales@ns/Files/claims_raw/part.parquet` --
    a rewrite, graded PASS, pointing at a bucket the data is not in. A
    shortcut's bytes live at its own target, which `shortcut_to_oci`
    already knows how to name, and the catalog carries the shortcut list.
    """

    def test_a_path_under_a_shortcut_is_not_sent_to_the_lakehouse_bucket(self):
        result = _translate(
            'df = spark.read.parquet("Files/claims_raw/part.parquet")')
        self.assertNotIn("oci://Sales@ns/Files/claims_raw", _body(result))
        self.assertNotIn("NB01_ONELAKE_PATH", _rules(result))

    def test_an_unmappable_shortcut_is_refused_by_name(self):
        """The repro catalog records no target type, so `map_target` cannot
        name the bucket. Refusing says which shortcut and why."""
        result = _translate(
            'df = spark.read.parquet("Files/claims_raw/part.parquet")')
        detail = next(f.detail for f in result.findings
                      if f.rule == "NB28_PATH_UNDER_SHORTCUT")
        self.assertIn("claims_raw", detail)
        self.assertIn("Files/claims_raw/part.parquet", detail)

    def test_a_mappable_shortcut_resolves_to_its_own_target(self):
        result = _translate(
            'df = spark.read.parquet("Tables/claims_raw/part.parquet")',
            catalog=MAPPABLE)
        self.assertIn("oci://acme-raw@ns/claims/part.parquet", _body(result))
        self.assertIn("NB27_PATH_VIA_SHORTCUT", _rules(result))

    def test_a_files_section_shortcut_resolves_too(self):
        """A `Files` shortcut never reaches the `tables` half of the
        catalog -- it is not a table -- and it is the commonest thing a
        `Files/<name>/...` path points at."""
        result = _translate('df = spark.read.parquet("Files/landing/x.csv")',
                            catalog=MAPPABLE)
        self.assertIn("oci://acmestore_landing@ns/x.csv", _body(result))
        self.assertIn("NB27_PATH_VIA_SHORTCUT", _rules(result))

    def test_the_abfss_spelling_gets_the_same_answer(self):
        result = _translate(
            'df = spark.read.parquet("abfss://ws@onelake.dfs.fabric.microsoft'
            '.com/Sales.Lakehouse/Files/landing/x.csv")', catalog=MAPPABLE)
        self.assertIn("oci://acmestore_landing@ns/x.csv", _body(result))

    def test_the_section_is_honoured_when_the_catalog_records_one(self):
        """`claims_raw` is a `Tables` shortcut, so a real folder called
        `claims_raw` under `Files` is a different object and the lakehouse
        bucket is the right answer for it."""
        result = _translate('df = spark.read.parquet("Files/claims_raw/x")',
                            catalog=MAPPABLE)
        self.assertIn("oci://Sales@ns/Files/claims_raw/x", _body(result))

    def test_an_ordinary_folder_is_unaffected(self):
        result = _translate('df = spark.read.parquet("Files/raw/x.parquet")')
        self.assertIn("oci://Sales@ns/Files/raw/x.parquet", _body(result))
        self.assertIn("NB01_ONELAKE_PATH", _rules(result))

    def test_the_sql_carrier_gets_the_same_answer(self):
        result = _translate(
            '''spark.sql("SELECT * FROM delta.`Tables/claims_raw`")''',
            catalog=MAPPABLE)
        self.assertIn("oci://acme-raw@ns/claims", _body(result))


TABLES = {"tables": {"sales.claim": {"tier": "supplied", "name": "sales.claim"},
                     "ledger": {"tier": "supplied", "name": "ledger"}}}


class SchemaEnabledTablesTests(unittest.TestCase):
    """D4, and the NB20 verdict with it.

    `Tables/dbo/claim` came out as `oci://Sales@ns/Tables/dbo/claim`,
    treating the schema of a schema-enabled Lakehouse as a folder. NB20
    already held the rule that a `Tables/` location is a table and is
    reached through the AIDP catalog -- but NB20 never ran: `_rewrite_paths`
    rewrote the literal to an oci:// URI earlier in the same per-line loop,
    so `rule_tables_path`'s pattern, which asks for the FUSE spelling,
    could not match afterwards. Verified on this tree before the fix:

      spark.read.load("/lakehouse/default/Tables/claim")
        -> spark.read.load("oci://Sales@ns/Tables/claim"), NB01 only

    while the same rule called on its own returned
    `spark.table("default.Sales.claim")` with NB20. The rule was right and
    unreachable, so it is made reachable rather than rewritten: it runs
    before the path rule, and its pattern now takes every OneLake spelling
    and the schema segment.
    """

    def test_nb20_is_reachable_at_all(self):
        result = _translate('df = spark.read.load("/lakehouse/default/Tables/claim")')
        self.assertIn('spark.table("default.Sales.claim")', _body(result))
        self.assertIn("NB20_TABLES_PATH", _rules(result))

    def test_the_schema_segment_is_a_schema_and_not_a_folder(self):
        result = _translate('df = spark.read.parquet("Tables/dbo/claim")')
        self.assertIn('spark.table("default.Sales.claim")', _body(result))
        self.assertNotIn("oci://Sales@ns/Tables/dbo", _body(result))

    def test_a_non_default_schema_keeps_its_name(self):
        result = _translate('df = spark.read.parquet("Tables/sales/claim")')
        self.assertIn('spark.table("default.Sales_sales.claim")', _body(result))

    def test_the_relative_spelling_reaches_it(self):
        result = _translate('df = spark.read.load("Tables/claim")')
        self.assertIn('spark.table("default.Sales.claim")', _body(result))

    def test_the_abfss_spelling_reaches_it_and_uses_the_uris_own_item(self):
        result = _translate(
            'df = spark.read.load("abfss://ws@onelake.dfs.fabric.microsoft.com'
            '/lh.Lakehouse/Tables/claim")')
        self.assertIn('spark.table("default.lh.claim")', _body(result))

    def test_a_read_inside_a_known_tables_storage_stays_a_path(self):
        """`Tables/<a>/<b>` is the schema layout only when `<a>` is not
        itself a table. The catalog is what can tell them apart, and where
        it knows `<a>` the two-segment form is a file under that table."""
        result = _translate('df = spark.read.parquet("Tables/ledger/part-0.parquet")',
                            catalog=TABLES)
        self.assertIn("oci://Sales@ns/Tables/ledger/part-0.parquet", _body(result))

    def test_delta_metadata_is_never_read_as_a_table(self):
        result = _translate('df = spark.read.parquet("Tables/claim/_delta_log")')
        self.assertIn("oci://Sales@ns/Tables/claim/_delta_log", _body(result))
        self.assertNotIn("NB20_TABLES_PATH", _rules(result))

    def test_a_shortcut_still_wins_over_the_table_reading(self):
        """A `Tables/` shortcut is a table whose data is elsewhere, so D3's
        answer is the right one and this rule must not get there first."""
        result = _translate('df = spark.read.parquet("Tables/claims_raw/part.parquet")',
                            catalog=MAPPABLE)
        self.assertIn("oci://acme-raw@ns/claims/part.parquet", _body(result))

    def test_a_files_path_is_left_to_the_path_rule(self):
        result = _translate('df = spark.read.parquet("Files/raw/x.parquet")')
        self.assertIn("oci://Sales@ns/Files/raw/x.parquet", _body(result))

    def test_an_unbound_notebook_is_still_flagged(self):
        result = _translate('df = spark.read.load("/lakehouse/default/Tables/claim")',
                            default_lakehouse=None)
        self.assertIn("/lakehouse/default/Tables/claim", _body(result))
        self.assertIn("NB20_TABLES_PATH_UNBOUND", _rules(result))


if __name__ == "__main__":
    unittest.main()
