import unittest

from fabric_aidp.translate import fabric_notebook_to_spark as nb2spark

# A `warehouse_ddl` entry always carries the Warehouse that declared the
# table -- inventory/catalog.py writes it on every one -- so a fixture
# without that key tests a shape that cannot occur, and hid a real bug.
# `dbo.claim` here is a *lakehouse* table known from a supplied CSV: nothing
# in the export says which item owns it, so the notebook's own default
# lakehouse is the answer. `dbo.ledger` is the Warehouse case.
CATALOG = {"tables": {
    "dbo.claim": {"tier": "supplied", "name": "dbo.claim"},
    "dbo.ledger": {"tier": "warehouse_ddl", "name": "dbo.ledger",
                   "warehouse": "AcmeDW"},
    "postgres_air.rate": {"tier": "warehouse_ddl", "name": "postgres_air.rate",
                          "warehouse": "AcmeDW"},
    "claims_raw": {"tier": "shortcut", "name": "claims_raw",
                   "target": "s3://acme/claims", "lakehouse": "Sales"},
    "claims_agg": {"tier": "notebook_inferred", "name": "claims_agg",
                   "created_by": "Build_Aggregates"},
    "sales.orders": {"tier": "supplied", "name": "sales.orders"},
}}


def _run(source, rule_name="rule_table_refs", **kw):
    findings = []
    kw.setdefault("default_lakehouse", "Sales")
    kw.setdefault("catalog", CATALOG)
    out = getattr(nb2spark, rule_name)(source, findings, **kw)
    return out, findings


class TableRefTests(unittest.TestCase):
    def test_schema_qualified_name_becomes_three_part(self):
        """`dbo.claim` names the same table `claim` does, inside the
        notebook's own default lakehouse -- not a two-part opaque name under
        `default`. `dbo` is Fabric's default schema and is dropped, exactly
        as it is everywhere else this rule applies."""
        out, findings = _run('df = spark.table("dbo.claim")')
        self.assertEqual(out, 'df = spark.table("default.Sales.claim")')
        self.assertEqual(findings[0].severity, "rewrite")

    def test_read_table_form(self):
        out, _ = _run('spark.read.table("dbo.claim")')
        self.assertIn("default.Sales.claim", out)

    def test_save_as_table_form(self):
        out, _ = _run('df.write.saveAsTable("dbo.claim")')
        self.assertIn("default.Sales.claim", out)

    def test_single_quotes(self):
        out, _ = _run("spark.table('dbo.claim')")
        self.assertEqual(out, "spark.table('default.Sales.claim')")

    def test_already_three_part_is_untouched(self):
        source = 'spark.table("default.dbo.claim")'
        self.assertEqual(_run(source), (source, []))

    def test_shortcut_name_is_flagged_not_rewritten(self):
        source = 'spark.table("claims_raw")'
        out, findings = _run(source)
        self.assertEqual(out, source)
        self.assertEqual(findings[0].severity, "flag")
        self.assertIn("s3://acme/claims", findings[0].detail)

    def test_inferred_table_is_rewritten_and_counted_as_a_rewrite(self):
        """It reported `info`, and `info` counts for nothing: `changes`
        counts `rewrite` findings alone and verify prints the count only
        when it is non-zero, so a notebook whose only findings were NB14
        showed no change count on an artifact it had edited. The name is
        rewritten here -- same substitution NB10 makes -- so the severity
        has to say so. What is less certain about it stays in the detail:
        the table's existence is inferred from another notebook's write,
        and that notebook is named and said to run first."""
        out, findings = _run('spark.table("claims_agg")')
        self.assertIn("default.Sales.claims_agg", out)
        self.assertEqual(findings[0].severity, "rewrite")
        self.assertIn("Build_Aggregates", findings[0].detail)
        self.assertIn("must run first", findings[0].detail)

    def test_unknown_table_is_flagged(self):
        source = 'spark.table("nope")'
        out, findings = _run(source)
        self.assertEqual(out, source)
        self.assertEqual(findings[0].severity, "flag")
        self.assertIn("not found", findings[0].detail.lower())

    def test_no_default_lakehouse_flags_a_bare_name(self):
        source = 'spark.table("claims_agg")'
        out, findings = _run(source, default_lakehouse=None)
        self.assertEqual(out, source)
        self.assertEqual(findings[0].severity, "flag")
        self.assertIn("default lakehouse", findings[0].detail.lower())

    def test_qualified_name_is_flagged_without_a_default_lakehouse(self):
        """A schema-qualified name has nothing to resolve the item against
        without a default lakehouse binding, so it must not silently gain a
        `default.` prefix -- that produced `default.dbo.claim`, a name that
        looks resolved and is not."""
        source = 'spark.table("dbo.claim")'
        out, findings = _run(source, default_lakehouse=None)
        self.assertEqual(out, source)
        self.assertEqual(findings[0].severity, "flag")
        self.assertIn("default lakehouse", findings[0].detail.lower())

    def test_variable_argument_is_left_alone(self):
        source = "spark.table(name)"
        self.assertEqual(_run(source), (source, []))

    def test_no_catalog_means_no_rewrites(self):
        source = 'spark.table("dbo.claim")'
        self.assertEqual(_run(source, catalog=None), (source, []))


class WarehouseOwnedTableTests(unittest.TestCase):
    """The catalog entry knows which Fabric item declared the table. The
    notebook translator threw that away and used the notebook's default
    lakehouse instead, so the one demo table `dbo.claim` -- declared in
    Warehouse `AcmeDW` -- was `default.SalesLake.claim` from notebooks,
    `default.AcmeDW.claim` in the plan and `dbo.claim` in the artifact that
    creates it. Three names for one table, all grading PASS."""

    def test_the_entrys_warehouse_wins_over_the_default_lakehouse(self):
        out, findings = _run('spark.table("dbo.ledger")')
        self.assertEqual(out, 'spark.table("default.AcmeDW.ledger")')
        self.assertEqual(findings[0].severity, "rewrite")

    def test_a_bare_reference_resolves_to_the_same_name(self):
        out, _ = _run('spark.table("ledger")')
        self.assertEqual(out, 'spark.table("default.AcmeDW.ledger")')

    def test_a_write_lands_in_the_warehouse_too(self):
        out, _ = _run('df.write.saveAsTable("dbo.ledger")')
        self.assertEqual(out, 'df.write.saveAsTable("default.AcmeDW.ledger")')

    def test_a_sql_clause_resolves_to_the_same_name(self):
        out, _ = _run("SELECT * FROM dbo.ledger", "rule_sql_table_refs")
        self.assertEqual(out, "SELECT * FROM default.AcmeDW.ledger")

    def test_a_non_default_schema_comes_off_the_entry(self):
        """A bare `rate` against an entry recorded as `postgres_air.rate`
        must keep the schema, or the notebook and the DDL disagree in the one
        case where the schema is not dropped."""
        out, _ = _run('spark.table("rate")')
        self.assertEqual(out, 'spark.table("default.AcmeDW_postgres_air.rate")')

    def test_it_resolves_with_no_default_lakehouse_at_all(self):
        """NB13 used to claim the item 'cannot be determined' whenever the
        notebook had no lakehouse binding, even where the entry determines
        it outright."""
        out, findings = _run('spark.table("dbo.ledger")', default_lakehouse=None)
        self.assertEqual(out, 'spark.table("default.AcmeDW.ledger")')
        self.assertNotIn("NB13_NO_DEFAULT_LAKEHOUSE", [f.rule for f in findings])


class SqlCellStringLiteralTests(unittest.TestCase):
    """A `%%sql` cell is Spark dialect, where `"..."` is a string literal --
    not the identifier T-SQL reads it as. The shared masking module was taught
    T-SQL's reading for the warehouse translator, and this path inherited it,
    so the contents of a user's string became eligible for rewriting.
    Measured before the dialect split:

        in:  SELECT "text FROM dbo.claim" AS c FROM dbo.claim
        out: SELECT "text FROM default.Sales.claim" AS c
                    FROM default.Sales.claim
             [NB10_TABLE_REF, NB10_TABLE_REF]

    Two findings, reading as two legitimate rewrites rather than one rewrite
    and one corrupted label. Losing a qualification is bad; editing text the
    user wrote is worse.
    """

    def test_a_table_name_inside_a_string_literal_is_not_rewritten(self):
        out, findings = _run('SELECT "text FROM dbo.claim" AS c FROM dbo.claim',
                             "rule_sql_table_refs")
        self.assertEqual(
            out, 'SELECT "text FROM dbo.claim" AS c FROM default.Sales.claim')
        self.assertEqual([f.rule for f in findings], ["NB10_TABLE_REF"])

    def test_a_plain_string_literal_is_left_alone(self):
        out, _ = _run('SELECT "hello" AS greeting FROM dbo.claim',
                      "rule_sql_table_refs")
        self.assertEqual(out,
                         'SELECT "hello" AS greeting FROM default.Sales.claim')

    def test_a_like_pattern_is_left_alone(self):
        out, _ = _run('SELECT c FROM dbo.claim WHERE c LIKE "%dbo.claim%"',
                      "rule_sql_table_refs")
        self.assertEqual(
            out,
            'SELECT c FROM default.Sales.claim WHERE c LIKE "%dbo.claim%"')

    def test_single_quoted_literals_are_still_protected(self):
        out, _ = _run("SELECT 'text FROM dbo.claim' AS c FROM dbo.claim",
                      "rule_sql_table_refs")
        self.assertEqual(
            out, "SELECT 'text FROM dbo.claim' AS c FROM default.Sales.claim")

    def test_a_tsql_cell_still_gets_the_tsql_reading(self):
        # `%%tsql` is translated by the T-SQL module, where `"..."` is an
        # identifier. The two dialects coexist in one notebook.
        source = ("# Fabric notebook source\n\n# CELL ********************\n\n"
                  '%%tsql\nSELECT "my col" FROM AcmeDW.dbo.ledger\n')
        result = nb2spark.translate(source, namespace="ns",
                                    default_lakehouse="Sales", catalog=CATALOG)
        self.assertIn("`my col`", result.translated_sql)


class AidpCatalogTests(unittest.TestCase):
    """`plan --catalog myc` reached the plan, the warehouse SQL and the
    dataflows. The notebook translator had no AIDP-catalog parameter at all,
    so it kept emitting `default`."""

    def test_the_catalog_reaches_a_python_call(self):
        out, _ = _run('spark.table("dbo.claim")', aidp_catalog="myc")
        self.assertEqual(out, 'spark.table("myc.Sales.claim")')

    def test_the_catalog_reaches_a_sql_clause(self):
        out, _ = _run("SELECT * FROM dbo.claim", "rule_sql_table_refs",
                      aidp_catalog="myc")
        self.assertEqual(out, "SELECT * FROM myc.Sales.claim")

    def test_the_catalog_reaches_a_tables_path(self):
        out, _ = _run('spark.read.load("/lakehouse/default/Tables/claim")',
                      "rule_tables_path", aidp_catalog="myc")
        self.assertIn('spark.table("myc.Sales.claim")', out)

    def test_the_catalog_reaches_a_tsql_cell(self):
        source = ("# Fabric notebook source\n\n# CELL ********************\n\n"
                  "%%tsql\nSELECT * FROM AcmeDW.dbo.claim\n")
        result = nb2spark.translate(source, namespace="ns",
                                    default_lakehouse="Sales", catalog=CATALOG,
                                    aidp_catalog="myc")
        self.assertIn("myc.AcmeDW.claim", result.translated_sql)
        self.assertNotIn("default.AcmeDW.claim", result.translated_sql)

    def test_the_catalog_reaches_translate(self):
        source = ("# Fabric notebook source\n\n# CELL ********************\n\n"
                  'df = spark.table("dbo.claim")\n')
        result = nb2spark.translate(source, namespace="ns",
                                    default_lakehouse="Sales", catalog=CATALOG,
                                    aidp_catalog="myc")
        self.assertIn("myc.Sales.claim", result.translated_sql)
        self.assertNotIn("default.Sales.claim", result.translated_sql)


class TranslateIntegrationTests(unittest.TestCase):
    HEADER = "# Fabric notebook source\n\n# CELL ********************\n\n"

    def test_catalog_flows_through_translate(self):
        result = nb2spark.translate(
            self.HEADER + 'df = spark.table("dbo.claim")\n',
            namespace="ns", default_lakehouse="Sales", catalog=CATALOG)
        self.assertIn("default.Sales.claim", result.translated_sql)

    def test_translate_without_a_catalog_still_works(self):
        result = nb2spark.translate(
            self.HEADER + 'df = spark.table("dbo.claim")\n', namespace="ns")
        self.assertIn('spark.table("dbo.claim")', result.translated_sql)



class TablesPathTests(unittest.TestCase):
    def test_tables_path_becomes_a_three_part_name(self):
        out, findings = _run('df = spark.read.load("/lakehouse/default/Tables/claim")',
                             "rule_tables_path")
        self.assertIn('spark.table("default.Sales.claim")', out)
        self.assertEqual(findings[0].severity, "rewrite")

    def test_files_path_is_left_for_the_path_rule(self):
        source = 'p = "/lakehouse/default/Files/raw.csv"'
        self.assertEqual(_run(source, "rule_tables_path"), (source, []))

    def test_no_default_lakehouse_flags(self):
        source = 'spark.read.load("/lakehouse/default/Tables/claim")'
        out, findings = _run(source, "rule_tables_path", default_lakehouse=None)
        self.assertEqual(out, source)
        self.assertEqual(findings[0].severity, "flag")


class MagicTests(unittest.TestCase):
    def test_configure_is_flagged(self):
        out, findings = _run('%%configure\n{"driverMemory": "8g"}', "rule_magics")
        self.assertIn("%%configure", out)
        self.assertEqual(findings[0].severity, "flag")

    def test_pyspark_magic_is_removed(self):
        out, findings = _run("%%pyspark\nx = 1", "rule_magics")
        self.assertNotIn("%%pyspark", out)
        self.assertIn("x = 1", out)
        self.assertEqual(findings[0].severity, "rewrite")

    def test_non_python_language_magic_is_flagged(self):
        out, findings = _run("%%csharp\nvar x = 1;", "rule_magics")
        self.assertIn("%%csharp", out)
        self.assertEqual(findings[0].severity, "flag")

    def test_run_magic_is_flagged(self):
        out, findings = _run("%run Common_Utils", "rule_magics")
        self.assertIn("%run Common_Utils", out)
        self.assertEqual(findings[0].severity, "flag")
        self.assertIn("Common_Utils", findings[0].detail)

    def test_plain_code_is_untouched(self):
        source = "x = 1"
        self.assertEqual(_run(source, "rule_magics"), (source, []))

    def test_percent_inside_a_string_is_not_a_magic(self):
        source = 'fmt = "%%configure"'
        self.assertEqual(_run(source, "rule_magics"), (source, []))


class SempyTests(unittest.TestCase):
    """NB29. Semantic Link is Fabric-only and its import fails on AIDP, so a
    notebook carrying one dies on that cell and never reaches the rest. Both
    of the shapes below came back unchanged with zero findings, so the
    notebook graded PASS -- the same class of miss as notebookutils, and
    flagged the same way: left in place, named, handed to a human."""

    HEADER = "# Fabric notebook source\n\n# CELL ********************\n\n"

    def _translate(self, body):
        return nb2spark.translate(self.HEADER + body + "\n", namespace="ns",
                                  default_lakehouse="Sales", catalog=CATALOG)

    def _rules(self, body):
        return [f.rule for f in self._translate(body).findings]

    def test_the_dotted_import_is_flagged(self):
        result = self._translate("import sempy.fabric as fabric\n"
                                 "df = fabric.list_workspaces()")
        self.assertEqual([f.rule for f in result.findings], ["NB30_SEMPY"])
        self.assertEqual(result.findings[0].severity, "flag")

    def test_the_from_import_is_flagged(self):
        self.assertEqual(
            self._rules('from sempy.fabric import evaluate_dax\n'
                        'evaluate_dax("Model", "EVALUATE x")'),
            ["NB30_SEMPY"])

    def test_the_bare_import_is_flagged(self):
        self.assertEqual(self._rules("import sempy"), ["NB30_SEMPY"])

    def test_the_statement_is_left_exactly_as_written(self):
        """There is no AIDP equivalent to rewrite to, and commenting the
        import out would only move the failure to a NameError below it."""
        body = "import sempy.fabric as fabric"
        self.assertIn(body, self._translate(body).translated_sql)

    def test_the_notebook_no_longer_grades_pass(self):
        self.assertEqual(self._translate("import sempy").flags, 1)

    def test_sempy_in_a_comment_is_prose(self):
        self.assertEqual(self._rules("# we used to import sempy here"), [])

    def test_sempy_in_a_string_is_prose(self):
        self.assertEqual(self._rules('note = "import sempy.fabric"'), [])

    def test_a_module_that_merely_starts_with_sempy_is_not_flagged(self):
        self.assertEqual(self._rules("import sempyx"), [])


class SqlCellTests(unittest.TestCase):
    HEADER = "# Fabric notebook source\n\n# CELL ********************\n\n"

    def _translate(self, body):
        return nb2spark.translate(self.HEADER + body + "\n", namespace="ns",
                                  default_lakehouse="Sales", catalog=CATALOG)

    def test_sql_cell_table_reference_is_resolved(self):
        out = self._translate("%%sql\nSELECT * FROM dbo.claim").translated_sql
        self.assertIn("default.Sales.claim", out)

    def test_sql_cell_is_not_run_through_the_tsql_rules(self):
        """`%%sql` in a Fabric notebook is Spark SQL, not T-SQL.

        Measured on this rule set: applying them turns `arr[0]` into
        ``arr`0` `` (Spark rejects it) and rewrites `lh.silver.orders` to
        `default.silver.orders` -- a different table, which Spark accepts.
        See TsqlInSqlCellTests for what happens instead.
        """
        result = self._translate("%%sql\nSELECT datediff(b, a) FROM dbo.claim")
        self.assertIn("datediff(b, a)", result.translated_sql)
        self.assertFalse(any(f.rule.startswith("SQ") for f in result.findings))

    def test_sql_cell_magic_line_survives(self):
        out = self._translate("%%sql\nSELECT 1").translated_sql
        self.assertIn("%%sql", out)


class MagicCellTests(unittest.TestCase):
    """Fabric writes a non-Python cell as Python comments, every body line
    behind `# MAGIC `. Read literally the cell is inert commentary, so its
    code reached the output untouched and unflagged."""

    STAR = "*" * 20

    def _nb(self, body, language=None):
        meta = ""
        if language:
            meta = (f'\n\n# METADATA {self.STAR}\n\n# META {{\n'
                    f'# META   "language": "{language}"\n# META }}\n')
        return (f"# Fabric notebook source\n\n# CELL {self.STAR}\n\n"
                f"{body}{meta}\n")

    def _t(self, body, language=None):
        return nb2spark.translate(self._nb(body, language), namespace="ns")

    def test_magic_sql_cell_is_unwrapped(self):
        result = self._t("# MAGIC %%sql\n# MAGIC SELECT 1", "sparksql")
        self.assertIn("%%sql", result.translated_sql)
        self.assertNotIn("# MAGIC", result.translated_sql)
        self.assertIn("NB22_MAGIC_CELL", [f.rule for f in result.findings])

    def test_magic_python_cell_loses_its_language_line(self):
        result = self._t("# MAGIC %%pyspark\n# MAGIC df = spark.table('t')", "python")
        self.assertIn("df = spark.table('t')", result.translated_sql)
        self.assertNotIn("%%pyspark", result.translated_sql)

    def test_a_language_we_do_not_translate_is_refused_by_name(self):
        result = self._t('# MAGIC %%spark\n# MAGIC val d = 1', "scala")
        rules = [f.rule for f in result.findings]
        self.assertIn("NB23_MAGIC_LANGUAGE", rules)
        self.assertIn("scala", str(result.findings))

    def test_an_ordinary_python_cell_is_untouched(self):
        result = self._t("df = spark.table('t')", "python")
        self.assertNotIn("NB22_MAGIC_CELL", [f.rule for f in result.findings])

    def test_parameters_cell_is_a_code_cell(self):
        source = (f"# Fabric notebook source\n\n"
                  f"# PARAMETERS CELL {self.STAR}\n\nimport notebookutils\n")
        result = nb2spark.translate(source, namespace="ns")
        self.assertIn("NB05_UTILS_IMPORT", [f.rule for f in result.findings])


class MagicCellMagicLineTests(unittest.TestCase):
    """Fabric records a `%%configure` or `%%html` cell with
    `"language": "python"` in its METADATA, so it reaches the Python branch
    of the unwrapper -- which deleted every `%%` line in the cell.

    Deleting is right for `%%pyspark`/`%%python`, a redundant language
    declaration, and wrong for everything else, and only the name tells them
    apart. `rule_magics` is the one place that knows which is which."""

    STAR = "*" * 20

    def _t(self, body, language):
        meta = (f'\n\n# METADATA {self.STAR}\n\n'
                f'# META {{"language": "{language}"}}\n')
        return nb2spark.translate(
            f"# Fabric notebook source\n\n# CELL {self.STAR}\n\n{body}{meta}",
            namespace="ns")

    def _out(self, body, language="python"):
        text = self._t(body, language).translated_sql
        return text.split(f"# CELL {self.STAR}")[1].split("# METADATA")[0].strip()

    def test_configure_survives_the_unwrap(self):
        """The cluster sizing was deleted outright, leaving a dict literal
        that evaluates and does nothing."""
        self.assertEqual(self._out('# MAGIC %%configure\n'
                                   '# MAGIC {"driverMemory": "8g"}'),
                         '%%configure\n{"driverMemory": "8g"}')

    def test_configure_is_flagged_now_that_the_line_reaches_the_rule(self):
        """NB21_MAGIC_CONFIGURE exists for exactly this cell and never
        fired: the line it matches had already been deleted."""
        result = self._t('# MAGIC %%configure\n# MAGIC {"driverMemory": "8g"}',
                         "python")
        self.assertIn("NB21_MAGIC_CONFIGURE", [f.rule for f in result.findings])

    def test_html_survives_the_unwrap(self):
        """`<b>hi</b>` on its own is a Python SyntaxError, so deleting the
        magic turned a cell that was harmless into one that is fatal.
        `%%html` is an IPython cell magic and the published artifact is an
        .ipynb, so keeping it is what makes the cell run."""
        self.assertEqual(self._out("# MAGIC %%html\n# MAGIC <b>hi</b>"),
                         "%%html\n<b>hi</b>")

    def test_an_ipython_line_magic_survives(self):
        self.assertEqual(self._out("# MAGIC %%time\n# MAGIC x = 1"),
                         "%%time\nx = 1")

    def test_the_redundant_language_magic_is_still_removed(self):
        result = self._t("# MAGIC %%pyspark\n# MAGIC df = spark.table('t')",
                         "python")
        self.assertNotIn("%%pyspark", result.translated_sql)
        self.assertIn("NB21_MAGIC_REDUNDANT", [f.rule for f in result.findings])


class TsqlInSqlCellTests(unittest.TestCase):
    """T-SQL inside a `%%sql` cell is reported, never rewritten.

    Rewriting needs to know the dialect, and knowing it needs a live Spark to
    parse against, which this tool deliberately does not have. So: flag what
    is recognisably T-SQL, leave the text alone, and translate only an
    explicit `%%tsql` cell.
    """

    STAR = "*" * 20

    def _t(self, body):
        return nb2spark.translate(
            f"# Fabric notebook source\n\n# CELL {self.STAR}\n\n{body}\n",
            namespace="ns")

    def test_tsql_in_a_sql_cell_is_flagged_not_rewritten(self):
        result = self._t("%%sql\nSELECT TOP 10 [region] FROM [dbo].[sales]")
        self.assertIn("TOP 10", result.translated_sql)
        self.assertIn("NB24_TSQL_IN_SQL_CELL", [f.rule for f in result.findings])

    def test_array_indexing_is_not_mistaken_for_a_bracketed_identifier(self):
        result = self._t("%%sql\nSELECT arr[0] FROM t")
        self.assertIn("arr[0]", result.translated_sql)
        self.assertEqual([f.rule for f in result.findings], [])

    def test_a_three_part_spark_name_is_left_alone(self):
        result = self._t("%%sql\nSELECT * FROM lh.silver.orders")
        self.assertIn("lh.silver.orders", result.translated_sql)
        self.assertEqual([f.rule for f in result.findings], [])

    def test_getdate_is_flagged(self):
        result = self._t("%%sql\nSELECT GETDATE()")
        self.assertIn("NB24_TSQL_IN_SQL_CELL", [f.rule for f in result.findings])

    def test_an_explicit_tsql_cell_is_translated(self):
        result = self._t("%%tsql\nSELECT TOP 10 [region] FROM [dbo].[sales]")
        self.assertIn("LIMIT 10", result.translated_sql)
        self.assertIn("`region`", result.translated_sql)
        self.assertNotIn("NB24_TSQL_IN_SQL_CELL", [f.rule for f in result.findings])


class SqlNotebookTests(unittest.TestCase):
    """Fabric's T-SQL notebook is committed as `notebook-content.sql`. Its
    cells are bare SQL with no magic line, so the routing test -- "does the
    first line start with %%sql" -- sent every one of them down the Python
    path. Nothing matched, the tokenizer accepted the SQL (tokenizing is not
    parsing, so there was no NB07 either), and the whole notebook came back
    byte-identical with zero findings, grading PASS."""

    STAR = "*" * 20
    CATALOG = {"tables": {
        "dbo.claim": {"tier": "warehouse_ddl", "name": "dbo.claim",
                      "warehouse": "AcmeDW", "owner": "AcmeDW"},
    }}

    def _t(self, body, comment="--", language="tsql"):
        meta = ""
        if language:
            meta = (f'\n\n{comment} METADATA {self.STAR}\n\n'
                    f'{comment} META {{"language": "{language}"}}\n')
        return nb2spark.translate(
            f"{comment} Fabric notebook source\n\n"
            f"{comment} CELL {self.STAR}\n\n{body}{meta}",
            namespace="ns", default_lakehouse="Sales", catalog=self.CATALOG)

    def _rules(self, result):
        return [f.rule for f in result.findings]

    def test_a_sql_notebook_is_no_longer_translated_as_python(self):
        result = self._t("SELECT TOP 5 GETDATE() AS now, * FROM dbo.claim")
        self.assertNotEqual(result.findings, [])

    def test_its_tsql_is_translated(self):
        result = self._t("SELECT TOP 5 GETDATE() AS now, * FROM dbo.claim")
        self.assertIn("LIMIT 5", result.translated_sql)
        self.assertIn("current_timestamp()", result.translated_sql)

    def test_its_table_names_are_resolved(self):
        result = self._t("SELECT TOP 5 * FROM dbo.claim")
        self.assertIn("default.AcmeDW.claim", result.translated_sql)
        self.assertIn("NB10_TABLE_REF", self._rules(result))

    def test_the_first_line_of_the_query_is_not_held_back(self):
        """The `%%sql` line is kept out of the translator and re-attached
        unchanged. A T-SQL notebook has no such line, so holding the first
        line back would take a line of the query out of the translation and
        leave it untranslated in the middle of the result."""
        result = self._t("SELECT ISNULL(amount, 0) AS amount\nFROM dbo.claim")
        self.assertIn("coalesce(amount, 0)", result.translated_sql)
        self.assertIn("SQ30_ISNULL", self._rules(result))

    def test_a_python_notebook_is_unaffected(self):
        result = self._t('df = spark.table("dbo.claim")', comment="#",
                         language="python")
        self.assertIn("default.AcmeDW.claim", result.translated_sql)

    def test_a_cell_language_of_sparksql_routes_to_sql_without_a_magic(self):
        """The same miss in the `.py` format: a cell recorded as `sparksql`
        with no `%%sql` line in its body."""
        result = self._t("SELECT TOP 5 * FROM dbo.claim", comment="#",
                         language="sparksql")
        self.assertIn("NB24_TSQL_IN_SQL_CELL", self._rules(result))
        self.assertIn("TOP 5", result.translated_sql)


class TsqlCellTableNameTests(unittest.TestCase):
    """A `%%tsql` cell goes through NB15 first and the T-SQL rules after, and
    the second pass overrode the first. Where NB15 *declined* a name -- NB12
    for one no catalog tier knows, NB20 for one matching two entries, both
    of which report "left exactly as written" -- SQ11's three-part rule
    rewrote it anyway, from the name's shape and nothing else. The finding
    and the artifact beside it then disagreed, and the emitted name was a
    confident answer to a question the catalog says is ambiguous.

    The same statement spelt `%%sql` left it alone, so one notebook gave two
    answers depending on the cell magic."""

    STAR = "*" * 20
    # `dbo.thing` in two Warehouses: a reference naming neither is ambiguous.
    CATALOG = {"tables": {
        "w1.dbo.thing": {"tier": "warehouse_ddl", "warehouse": "W1",
                         "name": "dbo.thing", "owner": "W1"},
        "w2.dbo.thing": {"tier": "warehouse_ddl", "warehouse": "W2",
                         "name": "dbo.thing", "owner": "W2"},
        "dbo.ledger": {"tier": "warehouse_ddl", "warehouse": "AcmeDW",
                       "name": "dbo.ledger", "owner": "AcmeDW"},
    }}

    def _t(self, body, catalog=None):
        return nb2spark.translate(
            f"# Fabric notebook source\n\n# CELL {self.STAR}\n\n{body}\n",
            namespace="ns", default_lakehouse="Sales",
            catalog=self.CATALOG if catalog is None else catalog)

    def _rules(self, result):
        return [f.rule for f in result.findings]

    def test_an_ambiguous_name_really_is_left_as_written(self):
        result = self._t("%%tsql\nSELECT * FROM W3.dbo.thing")
        self.assertIn("W3.dbo.thing", result.translated_sql)
        self.assertNotIn("default.W3.thing", result.translated_sql)

    def test_an_ambiguous_name_is_not_also_reported_as_a_rewrite(self):
        result = self._t("%%tsql\nSELECT * FROM W3.dbo.thing")
        self.assertEqual(self._rules(result), ["NB20_TABLE_AMBIGUOUS"])

    def test_an_unknown_three_part_name_is_left_as_written(self):
        result = self._t("%%tsql\nSELECT * FROM Other.dbo.nowhere")
        self.assertIn("Other.dbo.nowhere", result.translated_sql)
        self.assertEqual(self._rules(result), ["NB12_TABLE_UNKNOWN"])

    def test_the_sql_and_tsql_spellings_of_one_cell_now_agree(self):
        body = "SELECT * FROM W3.dbo.thing"
        self.assertEqual(self._t("%%sql\n" + body).translated_sql,
                         self._t("%%tsql\n" + body).translated_sql
                         .replace("%%tsql", "%%sql"))

    def test_a_name_nb15_does_resolve_is_still_rewritten(self):
        result = self._t("%%tsql\nSELECT TOP 5 * FROM dbo.ledger")
        self.assertIn("default.AcmeDW.ledger", result.translated_sql)
        self.assertIn("LIMIT 5", result.translated_sql)

    def test_the_sq15_column_reading_still_runs(self):
        """Only the object-position rewrite is switched off; the rest of
        SQ11 -- reading `a.b.c` outside an object slot as a column -- is
        what keeps `dbo.t.c` resolvable after the FROM is rewritten."""
        result = self._t("%%tsql\nSELECT dbo.t.c FROM dbo.ledger")
        self.assertIn("t.c", result.translated_sql)
        self.assertIn("SQ15_QUALIFIED_COLUMN", self._rules(result))

    def test_the_linked_server_flag_still_runs(self):
        result = self._t("%%tsql\nSELECT * FROM srv.db.dbo.tbl")
        self.assertIn("SQ11_LINKED_SERVER", self._rules(result))


class NotebookNamingTests(unittest.TestCase):
    STAR = "*" * 20

    def _t(self, body, **kw):
        source = (f"# Fabric notebook source\n\n# CELL {self.STAR}\n\n"
                  f"{body}\n")
        kw.setdefault("namespace", "myns")
        return nb2spark.translate(source, **kw).translated_sql

    def test_a_table_gets_the_lakehouse_as_its_schema(self):
        out = self._t('spark.table("claim")', default_lakehouse="SalesLake",
                      catalog=CATALOG)
        self.assertIn('spark.table("default.SalesLake.claim")', out)

    def test_the_oci_namespace_never_appears_in_a_table_name(self):
        """--namespace is for oci:// paths. It is not a SQL catalog."""
        out = self._t('spark.table("claim")', default_lakehouse="SalesLake",
                      catalog=CATALOG)
        self.assertNotIn("myns.SalesLake", out)


if __name__ == "__main__":
    unittest.main()
