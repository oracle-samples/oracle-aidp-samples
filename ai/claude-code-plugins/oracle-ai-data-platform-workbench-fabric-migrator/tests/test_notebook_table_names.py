"""Table-name resolution in notebooks: one table, one name, whatever the API.

Four defects, all of which graded PASS because the translator emitted no
finding at all:

  D1  `spark.table("dbo.claim")` resolved and `spark.sql("... FROM dbo.claim")`
      did not, so one notebook read one table under two names
  D2  a three- or four-part name returned early, skipping NB10/NB11/NB12
  D3  `USE other` / `setCurrentDatabase("other")` were ignored and later bare
      names were pinned to the default lakehouse regardless
  D4  a OneLake URI inside a SQL string was neither rewritten nor flagged
"""
import unittest

from fabric_aidp.translate import fabric_notebook_to_spark as nb2spark

# Same shape as tests/test_notebook_rules.py: a `warehouse_ddl` entry always
# carries the Warehouse that declared the table, a `supplied` one does not
# say which item owns it, so the notebook's own default lakehouse answers.
CATALOG = {"tables": {
    "dbo.claim": {"tier": "supplied", "name": "dbo.claim"},
    "dbo.ledger": {"tier": "warehouse_ddl", "name": "dbo.ledger",
                   "warehouse": "AcmeDW"},
    "claims_raw": {"tier": "shortcut", "name": "claims_raw",
                   "target": "s3://acme/claims", "lakehouse": "Sales"},
    "claims_agg": {"tier": "notebook_inferred", "name": "claims_agg",
                   "created_by": "Build_Aggregates"},
    # A table under a schema that is not Fabric's default, so the
    # non-default-schema naming rule can be exercised against a catalog
    # that agrees with the reference. It used to be exercised with
    # `lh.sales.claim` against an entry recorded as `dbo.claim`, which
    # only resolved because resolution matched on the last name part and
    # ignored the contradicting schema.
    "sales.invoice": {"tier": "supplied", "name": "sales.invoice"},
}}

HEADER = "# Fabric notebook source\n\n# CELL ********************\n\n"
CELL = "\n\n# CELL ********************\n\n"


def _run(source, rule_name="rule_sql_call_strings", **kw):
    findings = []
    kw.setdefault("default_lakehouse", "Sales")
    kw.setdefault("catalog", CATALOG)
    out = getattr(nb2spark, rule_name)(source, findings, **kw)
    return out, findings


def _translate(*cells, **kw):
    kw.setdefault("namespace", "ns")
    kw.setdefault("default_lakehouse", "Sales")
    kw.setdefault("catalog", CATALOG)
    return nb2spark.translate(HEADER + CELL.join(cells) + "\n", **kw)


def _body(result):
    """The code of the translated notebook, cell markers stripped."""
    return "\n".join(line for line in result.translated_sql.split("\n")
                     if not line.startswith("# "))


def _rules(result_or_findings):
    findings = getattr(result_or_findings, "findings", result_or_findings)
    return [f.rule for f in findings]


class SqlStringTests(unittest.TestCase):
    """D1: a SQL string argument is SQL, and goes through the SQL rules."""

    def test_spark_sql_resolves_the_same_table_the_python_call_does(self):
        result = _translate('spark.table("dbo.claim")',
                            'spark.sql("SELECT * FROM dbo.claim")')
        body = _body(result)
        self.assertIn('spark.table("default.Sales.claim")', body)
        self.assertIn('spark.sql("SELECT * FROM default.Sales.claim")', body)
        self.assertEqual(_rules(result), ["NB10_TABLE_REF", "NB10_TABLE_REF"])

    def test_one_notebook_gives_one_table_one_name(self):
        """The defect in one assertion: three APIs, one name."""
        result = _translate('spark.table("dbo.claim")',
                            'spark.read.table("dbo.claim")',
                            'spark.sql("SELECT * FROM dbo.claim")')
        body = _body(result)
        self.assertNotIn('"dbo.claim"', body)
        self.assertEqual(body.count("default.Sales.claim"), 3)

    def test_a_triple_quoted_multi_line_query_is_resolved(self):
        """Real notebooks write SQL over several lines. The Python rules run
        one line at a time and a multi-line literal belongs to no line, so
        this pass has to run over the whole cell."""
        result = _translate('df = spark.sql("""\n'
                            '    SELECT *\n'
                            '    FROM dbo.claim\n'
                            '""")')
        self.assertIn("FROM default.Sales.claim", _body(result))

    def test_a_session_bound_to_another_name(self):
        out, findings = _run('ss.sql("SELECT * FROM dbo.claim")')
        self.assertEqual(out, 'ss.sql("SELECT * FROM default.Sales.claim")')
        self.assertEqual(_rules(findings), ["NB10_TABLE_REF"])

    def test_an_attribute_chain_receiver(self):
        out, _ = _run('self.spark.sql("SELECT * FROM dbo.claim")')
        self.assertIn("default.Sales.claim", out)

    def test_read_sql_is_not_a_spark_sql_call(self):
        source = 'pd.read_sql("SELECT * FROM dbo.claim", conn)'
        self.assertEqual(_run(source), (source, []))

    def test_a_shortcut_inside_a_query_is_flagged_not_rewritten(self):
        source = 'spark.sql("SELECT * FROM claims_raw")'
        out, findings = _run(source)
        self.assertEqual(out, source)
        self.assertEqual(_rules(findings), ["NB11_TABLE_IS_SHORTCUT"])

    def test_an_unknown_table_inside_a_query_is_flagged(self):
        source = 'spark.sql("SELECT * FROM nope")'
        out, findings = _run(source)
        self.assertEqual(out, source)
        self.assertEqual(_rules(findings), ["NB12_TABLE_UNKNOWN"])

    def test_a_join_is_resolved_too(self):
        out, _ = _run('spark.sql("SELECT * FROM dbo.claim '
                      'JOIN dbo.ledger ON 1=1")')
        self.assertIn("default.Sales.claim", out)
        self.assertIn("default.AcmeDW.ledger", out)


class SqlStringMaskingTests(unittest.TestCase):
    """D1's crux: a table name in a SQL string is code, one in any other
    string is not, and the two are the same Python literal to a regex."""

    def test_a_table_name_in_an_ordinary_string_is_left_alone(self):
        source = 'note = "SELECT * FROM dbo.claim"'
        self.assertEqual(_run(source), (source, []))

    def test_a_spark_sql_call_quoted_inside_prose_is_not_a_call(self):
        source = 'doc = "we call spark.sql(\'SELECT * FROM dbo.claim\') here"'
        self.assertEqual(_run(source), (source, []))

    def test_a_spark_sql_call_in_a_comment_is_not_a_call(self):
        source = '# spark.sql("SELECT * FROM dbo.claim")'
        self.assertEqual(_run(source), (source, []))

    def test_a_double_quoted_run_inside_the_query_is_a_string_literal(self):
        """Spark dialect: `"..."` is a string literal, so its body is the
        user's text and must not be rewritten. Reading it as T-SQL does --
        an identifier, body visible -- corrupts the text instead."""
        out, findings = _run(
            'spark.sql(\'SELECT "text FROM dbo.claim" AS c FROM dbo.claim\')')
        self.assertEqual(
            out,
            'spark.sql(\'SELECT "text FROM dbo.claim" AS c '
            'FROM default.Sales.claim\')')
        self.assertEqual(_rules(findings), ["NB10_TABLE_REF"])

    def test_a_sql_comment_inside_the_query_is_left_alone(self):
        out, findings = _run('spark.sql("""SELECT 1 -- FROM dbo.claim\n'
                             'FROM dbo.claim""")')
        self.assertIn("-- FROM dbo.claim", out)
        self.assertIn("\nFROM default.Sales.claim", out)
        self.assertEqual(_rules(findings), ["NB10_TABLE_REF"])

    def test_an_escaped_newline_ends_the_sql_comment(self):
        """`\\n` inside a single-quoted body is two source characters, and
        the SQL passes read them as two ordinary characters, so the `--`
        comment it follows ran to the end of the string and the FROM after
        it never happened. Measured before the fix: the whole query read as
        comment text, nothing rewritten, no finding.

        This file used to assert that behaviour as deliberate, on the
        grounds that fixing it meant decoding the literal, rewriting the
        decoded string, and re-escaping it on the way back. It does not:
        `python_text.sql_scan_text` decodes the whitespace escapes
        *length-preservingly*, so the scan sees a line break while the
        rewrite still goes back into the original source at the original
        offsets with `\\n` still spelt `\\n`. The output text below is
        where that shows.
        """
        out, findings = _run('spark.sql("SELECT 1 -- x\\nFROM dbo.claim")')
        self.assertEqual(
            out, 'spark.sql("SELECT 1 -- x\\nFROM default.Sales.claim")')
        self.assertEqual(_rules(findings), ["NB10_TABLE_REF"])

    def test_an_escaped_newline_separates_two_sql_tokens(self):
        """Not only comments, and this is the bigger half. `*\\nFROM` puts
        `n` hard against `FROM`, so `\\bFROM` cannot match at all and the
        commonest one-line spelling of a multi-line query resolved
        nothing."""
        out, findings = _run('spark.sql("SELECT *\\nFROM dbo.claim")')
        self.assertEqual(
            out, 'spark.sql("SELECT *\\nFROM default.Sales.claim")')
        self.assertEqual(_rules(findings), ["NB10_TABLE_REF"])

    def test_an_escaped_tab_does_the_same(self):
        out, _ = _run('spark.sql("SELECT *\\tFROM dbo.claim")')
        self.assertIn("default.Sales.claim", out)

    def test_a_doubled_backslash_is_not_a_line_break(self):
        """`"a\\\\nb"` is one backslash and the letter n. The escape scan
        matches a backslash pair whole, so it cannot be read as a newline
        and the `n` stays where it is."""
        out, _ = _run('spark.sql("SELECT a\\\\nb FROM dbo.claim")')
        self.assertEqual(
            out, 'spark.sql("SELECT a\\\\nb FROM default.Sales.claim")')

    def test_a_raw_string_keeps_its_escapes_undecoded(self):
        """In `r"..."` those two characters really are two characters, and
        whatever Spark makes of them is not a line break. Decoding here
        would be inventing SQL the cluster never sees."""
        source = r'spark.sql(r"SELECT 1 -- x\nFROM dbo.claim")'
        self.assertEqual(_run(source), (source, []))

    def test_an_fstring_query_is_left_alone_and_said_so(self):
        """NB32. Not translating it is right -- a partial rewrite, `dbo` in
        `dbo.{name}`, would leave one query naming its tables under two
        conventions. Saying nothing was not: a reviewer reading the report
        had no way to learn that this notebook holds a query the tool never
        looked at. Measured before: `unchanged, findings: []`."""
        source = 'spark.sql(f"SELECT * FROM dbo.{name}")'
        out, findings = _run(source)
        self.assertEqual(out, source)
        self.assertEqual(_rules(findings), ["NB32_SQL_BUILT_AT_RUNTIME"])
        self.assertEqual([f.severity for f in findings], ["flag"])

    def test_the_finding_names_the_query_it_could_not_read(self):
        """A Finding carries no line number, so two of these in one
        notebook have to be told apart by their own text."""
        _out, findings = _run('spark.sql(f"SELECT * FROM dbo.{name}")')
        self.assertIn("SELECT * FROM dbo.{name}", findings[0].detail)

    def test_an_fstring_with_no_field_is_an_ordinary_query(self):
        """Its SQL is fully determined, so flagging it would say its names
        are built at run time when none of them is."""
        out, findings = _run('spark.sql(f"SELECT * FROM dbo.claim")')
        self.assertEqual(out, 'spark.sql(f"SELECT * FROM default.Sales.claim")')
        self.assertEqual(_rules(findings), ["NB10_TABLE_REF"])

    def test_a_bytes_argument_is_neither_translated_nor_flagged(self):
        """`.sql()` raises on one; it is not SQL text this tool declined to
        read."""
        source = 'spark.sql(b"SELECT * FROM dbo.claim")'
        self.assertEqual(_run(source), (source, []))

    def test_a_raw_string_query_is_still_resolved(self):
        out, _ = _run(r'spark.sql(r"SELECT * FROM dbo.claim")')
        self.assertIn("default.Sales.claim", out)


class CatalogApiTests(unittest.TestCase):
    """D1: `spark.catalog.<x>("<table>")` names a table, so it gets the
    same name the other APIs do."""

    def test_list_columns(self):
        out, findings = _run('spark.catalog.listColumns("dbo.claim")',
                             "rule_table_refs")
        self.assertEqual(out, 'spark.catalog.listColumns("default.Sales.claim")')
        self.assertEqual(_rules(findings), ["NB10_TABLE_REF"])

    def test_cache_and_refresh(self):
        for call in ("cacheTable", "uncacheTable", "isCached", "refreshTable",
                     "recoverPartitions", "tableExists", "getTable"):
            with self.subTest(call=call):
                out, _ = _run(f'spark.catalog.{call}("dbo.claim")',
                              "rule_table_refs")
                self.assertIn("default.Sales.claim", out)

    def test_set_current_database_is_not_a_table_name(self):
        source = 'spark.catalog.setCurrentDatabase("dbo.claim")'
        out, _ = _run(source, "rule_table_refs")
        self.assertEqual(out, source)

    def test_drop_temp_view_is_not_a_table_name(self):
        source = 'spark.catalog.dropTempView("dbo.claim")'
        out, _ = _run(source, "rule_table_refs")
        self.assertEqual(out, source)


class TempViewTests(unittest.TestCase):
    """D1's own guard: once SQL strings are rewritten, a bare name in one may
    be a temp view registered by the notebook rather than a catalog table.
    Rewriting it points the query at a different object."""

    def test_a_temp_view_target_is_not_rewritten(self):
        result = _translate('df.createOrReplaceTempView("claim")')
        self.assertIn('createOrReplaceTempView("claim")', _body(result))

    def test_a_query_against_a_registered_temp_view_is_left_as_written(self):
        result = _translate('df.createOrReplaceTempView("claim")',
                            'spark.sql("SELECT * FROM claim")')
        body = _body(result)
        self.assertIn('spark.sql("SELECT * FROM claim")', body)
        self.assertIn("NB16_TEMP_VIEW", _rules(result))
        self.assertEqual(result.flags, 0)

    def test_a_python_read_of_a_registered_temp_view_is_left_as_written(self):
        result = _translate('df.createOrReplaceTempView("claim")',
                            'spark.table("claim")')
        self.assertIn('spark.table("claim")', _body(result))

    def test_a_temp_view_declared_in_sql_counts_too(self):
        result = _translate('spark.sql("CREATE OR REPLACE TEMPORARY VIEW '
                            'claim AS SELECT 1")',
                            'spark.sql("SELECT * FROM claim")')
        body = _body(result)
        self.assertIn("FROM claim", body)
        self.assertNotIn("default.Sales.claim", body)

    def test_a_qualified_name_is_never_a_temp_view(self):
        """A temp view name is a single unqualified identifier, so
        `dbo.claim` cannot be one even when a view named `claim` exists."""
        result = _translate('df.createOrReplaceTempView("claim")',
                            'spark.sql("SELECT * FROM dbo.claim")')
        self.assertIn("default.Sales.claim", _body(result))


class NotATableAfterFromTests(unittest.TestCase):
    """Two things follow the keyword FROM without being tables, and both were
    resolved against the catalog, missed, and reported NB12_TABLE_UNKNOWN --
    a flag on something that is not a table, which grades an otherwise clean
    notebook REVIEW. In every case the real table in the same statement
    resolved correctly, so the flag was purely additional.

    The T-SQL two-part rule already guards the function half; the notebook
    rule anchors on FROM the same way and had no such guard."""

    def test_a_cte_name_is_not_resolved_as_a_table(self):
        result = _translate("%%sql\nWITH recent AS (SELECT * FROM dbo.claim)\n"
                            "SELECT * FROM recent")
        self.assertEqual(_rules(result), ["NB10_TABLE_REF"])
        self.assertIn("FROM recent", _body(result))

    def test_the_real_table_inside_the_cte_still_resolves(self):
        result = _translate("%%sql\nWITH recent AS (SELECT * FROM dbo.claim)\n"
                            "SELECT * FROM recent")
        self.assertIn("default.Sales.claim", _body(result))

    def test_every_name_in_a_multi_entry_with_clause_is_skipped(self):
        result = _translate("%%sql\nWITH a AS (SELECT 1), b AS (SELECT 2)\n"
                            "SELECT * FROM a JOIN b ON TRUE")
        self.assertEqual(_rules(result), [])

    def test_extract_reads_a_column_not_a_table(self):
        result = _translate("%%sql\nSELECT EXTRACT(year FROM opened) "
                            "FROM dbo.claim")
        self.assertEqual(_rules(result), ["NB10_TABLE_REF"])

    def test_trim_reads_a_column_not_a_table(self):
        result = _translate("%%sql\nSELECT TRIM(BOTH ' ' FROM name) "
                            "FROM dbo.claim")
        self.assertEqual(_rules(result), ["NB10_TABLE_REF"])

    def test_substring_reads_a_column_not_a_table(self):
        """`SUBSTRING(name FROM 1 FOR 2)` never flagged, because `1` is not
        an identifier and the pattern needs one. The column form does."""
        result = _translate("%%sql\nSELECT SUBSTRING(name FROM start_at) "
                            "FROM dbo.claim")
        self.assertEqual(_rules(result), ["NB10_TABLE_REF"])

    def test_a_table_inside_a_derived_table_is_still_a_table(self):
        """The guard must not read the paren of `FROM (SELECT ...)` as a
        function call."""
        result = _translate("%%sql\nSELECT * FROM (SELECT * FROM dbo.claim) x")
        self.assertEqual(_rules(result), ["NB10_TABLE_REF"])

    def test_a_genuinely_unknown_table_is_still_flagged(self):
        result = _translate("%%sql\nSELECT * FROM nowhere_at_all")
        self.assertEqual(_rules(result), ["NB12_TABLE_UNKNOWN"])

    def test_a_cte_in_one_cell_does_not_silence_the_next(self):
        """The `WITH` scan is per body, so a CTE named after a real table
        cannot suppress a reference in another cell."""
        result = _translate("%%sql\nWITH claim AS (SELECT 1) SELECT * FROM claim",
                            "%%sql\nSELECT * FROM dbo.claim")
        self.assertEqual(_rules(result), ["NB10_TABLE_REF"])


class ThreePartNameTests(unittest.TestCase):
    """D2: in Fabric a three-part name is `item.schema.table`, and the
    naming rule has a defined answer for it. The resolver returned early on
    anything with three parts, so NB10, NB11 and NB12 were all skipped and
    the reference shipped as written, naming nothing on AIDP."""

    def test_a_fabric_three_part_name_resolves(self):
        out, findings = _run('spark.table("lh.dbo.claim")', "rule_table_refs")
        self.assertEqual(out, 'spark.table("default.lh.claim")')
        self.assertEqual(_rules(findings), ["NB10_TABLE_REF"])

    def test_a_non_default_schema_is_kept_as_a_suffix(self):
        """The rule in docs/.../2026-09-28-table-naming-design.md: the item
        becomes the schema, and a Fabric schema that is not the default is
        suffixed onto it."""
        out, _ = _run('spark.table("lh.sales.invoice")', "rule_table_refs")
        self.assertEqual(out, 'spark.table("default.lh_sales.invoice")')

    def test_a_name_already_in_aidp_form_is_left_alone(self):
        source = 'spark.table("default.dbo.claim")'
        self.assertEqual(_run(source, "rule_table_refs"), (source, []))

    def test_a_custom_aidp_catalog_is_recognised_as_already_resolved(self):
        source = 'spark.table("myc.Sales.claim")'
        self.assertEqual(
            _run(source, "rule_table_refs", aidp_catalog="myc"), (source, []))

    def test_the_entrys_warehouse_still_wins(self):
        """One table, one name: `dbo.ledger` is declared in Warehouse
        `AcmeDW`, so a reference that puts it somewhere else must not mint a
        second name for it."""
        out, _ = _run('spark.table("Sales.dbo.ledger")', "rule_table_refs")
        self.assertEqual(out, 'spark.table("default.AcmeDW.ledger")')

    def test_a_sql_clause_resolves_it_the_same_way(self):
        out, findings = _run("SELECT * FROM lh.dbo.claim", "rule_sql_table_refs")
        self.assertEqual(out, "SELECT * FROM default.lh.claim")
        self.assertEqual(_rules(findings), ["NB10_TABLE_REF"])

    def test_an_unknown_three_part_name_is_flagged(self):
        source = 'spark.table("lh.dbo.nope")'
        out, findings = _run(source, "rule_table_refs")
        self.assertEqual(out, source)
        self.assertEqual(_rules(findings), ["NB12_TABLE_UNKNOWN"])

    def test_a_three_part_shortcut_is_flagged(self):
        source = 'spark.table("Sales.dbo.claims_raw")'
        out, findings = _run(source, "rule_table_refs")
        self.assertEqual(out, source)
        self.assertEqual(_rules(findings), ["NB11_TABLE_IS_SHORTCUT"])

    def test_it_reaches_a_sql_string_too(self):
        result = _translate('spark.sql("SELECT * FROM lh.dbo.claim")')
        self.assertIn("default.lh.claim", _body(result))


class ContradictedAndAmbiguousNameTests(unittest.TestCase):
    """The catalog answered a reference it had no entry for.

    `other.claim` names a schema this export says nothing about; resolution
    matched on the last name part, so it came back as `dbo.claim` -- a
    different table, in a different schema -- and was rewritten with no
    flag. A bare name matching two entries picked whichever sorted first.
    """

    def test_a_reference_to_an_unknown_schema_is_flagged_not_rewritten(self):
        source = 'spark.table("other.claim")'
        out, findings = _run(source, "rule_table_refs")
        self.assertEqual(out, source)
        self.assertEqual(_rules(findings), ["NB12_TABLE_UNKNOWN"])

    def test_an_ambiguous_bare_name_is_flagged_by_name(self):
        catalog = {"tables": {
            "dbo.claim": {"tier": "warehouse_ddl", "name": "dbo.claim",
                          "warehouse": "AcmeDW"},
            "sales.claim": {"tier": "warehouse_ddl", "name": "sales.claim",
                            "warehouse": "OtherDW"},
        }}
        source = 'spark.table("claim")'
        out, findings = _run(source, "rule_table_refs", catalog=catalog,
                             default_lakehouse=None)
        self.assertEqual(out, source)
        self.assertEqual(_rules(findings), ["NB20_TABLE_AMBIGUOUS"])
        self.assertEqual(findings[0].severity, "flag")
        self.assertIn("dbo.claim", findings[0].detail)
        self.assertIn("sales.claim", findings[0].detail)

    def test_the_notebooks_own_binding_breaks_the_tie(self):
        """Two Lakehouses each holding a `claim`: the notebook is attached
        to one of them, and that is which one a bare name means."""
        catalog = {"tables": {
            "salesl.claim": {"tier": "notebook_inferred", "name": "claim",
                             "owner": "SalesL", "created_by": "A"},
            "otherl.claim": {"tier": "notebook_inferred", "name": "claim",
                             "owner": "OtherL", "created_by": "B"},
        }}
        out, findings = _run('spark.table("claim")', "rule_table_refs",
                             catalog=catalog, default_lakehouse="OtherL")
        self.assertEqual(out, 'spark.table("default.OtherL.claim")')
        self.assertEqual(_rules(findings), ["NB14_TABLE_INFERRED"])

    def test_an_ambiguous_name_resolves_once_the_owner_is_written(self):
        catalog = {"tables": {
            "dbo.claim": {"tier": "warehouse_ddl", "name": "dbo.claim",
                          "warehouse": "AcmeDW"},
            "sales.claim": {"tier": "warehouse_ddl", "name": "sales.claim",
                            "warehouse": "OtherDW"},
        }}
        out, findings = _run('spark.table("OtherDW.sales.claim")',
                             "rule_table_refs", catalog=catalog,
                             default_lakehouse=None)
        self.assertEqual(out, 'spark.table("default.OtherDW_sales.claim")')
        self.assertEqual(_rules(findings), ["NB10_TABLE_REF"])


class FourPartNameTests(unittest.TestCase):
    """D2: a four-part name is ambiguous and has no AIDP answer either way,
    so the only honest thing is to refuse it by name rather than pass it
    through unmentioned.

    The *refusal* is the settled part. The message was not: it asserted
    T-SQL's linked-server form, `server.database.schema.object`, as though
    that were the only reading. A Fabric item's display name may contain a
    dot, so `my.item.dbo.t` is a three-part reference into an item called
    `my.item` at least as readily -- and the two readings want opposite
    treatment, one resolved and one refused. Nothing in the reference says
    which, which is the reason to refuse and is now what the finding says.
    """

    def test_a_four_part_name_is_refused_by_name(self):
        source = 'spark.table("a.b.c.d")'
        out, findings = _run(source, "rule_table_refs")
        self.assertEqual(out, source)
        self.assertEqual(_rules(findings), ["NB17_FOUR_PART_NAME"])
        self.assertEqual(findings[0].severity, "flag")
        self.assertIn("a.b.c.d", findings[0].detail)
        self.assertIn("linked server", findings[0].detail.lower())

    def test_the_finding_does_not_assert_the_linked_server_reading(self):
        """Both readings, and the word that says neither is established."""
        _out, findings = _run('spark.table("my.item.dbo.t")',
                              "rule_table_refs")
        detail = findings[0].detail
        self.assertIn("ambiguous", detail)
        self.assertIn("display name contains a dot", detail)
        self.assertNotIn("which is a linked server reference", detail)

    def test_the_finding_names_the_item_the_other_reading_implies(self):
        """So a reader who knows the item can act on it without working the
        split out for themselves."""
        _out, findings = _run('spark.table("my.item.dbo.t")',
                              "rule_table_refs")
        self.assertIn("'my.item'", findings[0].detail)

    def test_a_four_part_name_in_sql_is_refused_whole(self):
        """The clause pattern used to stop at three parts, so `a.b.c` was
        read as the name and `.d` left dangling behind it. Resolving that
        prefix once three-part names resolve would have produced
        `default.a_b.c.d` -- a name built out of half a reference."""
        source = "SELECT * FROM a.b.c.d"
        out, findings = _run(source, "rule_sql_table_refs")
        self.assertEqual(out, source)
        self.assertEqual(_rules(findings), ["NB17_FOUR_PART_NAME"])

    def test_five_parts_is_refused_as_well(self):
        source = 'spark.table("a.b.c.d.e")'
        out, findings = _run(source, "rule_table_refs")
        self.assertEqual(out, source)
        self.assertEqual(_rules(findings), ["NB17_FOUR_PART_NAME"])


SQL_CELL_META = ('\n\n# METADATA ********************\n\n'
                 '# META {\n# META   "language": "sparksql"\n# META }')


class CurrentDatabaseTests(unittest.TestCase):
    """D3: the notebook says the current database is somewhere else and the
    translator resolved bare names against the default lakehouse anyway.

    The decision, recorded here because it is the interesting half: a switch
    naming an item the export knows -- the default lakehouse, or any
    Lakehouse or Warehouse a catalog entry records -- is *followed*, and
    bare names resolve into that item. A switch naming anything else is
    *refused*: nothing in the export says whether the name is a Fabric item
    or a schema inside one, and those two produce different AIDP names, so
    resolving would mint a confident wrong one. What is not allowed is the
    third option, which is what the code did: ignore the switch and keep
    pinning bare names to the default lakehouse.
    """

    def test_set_current_database_stops_a_bare_name_being_pinned(self):
        result = _translate('spark.catalog.setCurrentDatabase("other")\n'
                            'spark.table("claim")')
        body = _body(result)
        self.assertIn('spark.table("claim")', body)
        self.assertNotIn("default.Sales.claim", body)
        self.assertEqual(_rules(result),
                         ["NB18_CURRENT_DATABASE",
                          "NB19_NAME_AFTER_DATABASE_SWITCH"])

    def test_the_refusal_names_the_database(self):
        result = _translate('spark.catalog.setCurrentDatabase("other")\n'
                            'spark.table("claim")')
        for finding in result.findings:
            with self.subTest(rule=finding.rule):
                self.assertEqual(finding.severity, "flag")
                self.assertIn("other", finding.detail)

    def test_a_use_statement_in_a_sql_cell(self):
        result = _translate("%%sql\nUSE other;\nSELECT * FROM claim"
                            + SQL_CELL_META)
        body = _body(result)
        self.assertIn("FROM claim", body)
        self.assertNotIn("default.Sales.claim", body)
        self.assertIn("NB18_CURRENT_DATABASE", _rules(result))

    def test_use_catalog_counts_too(self):
        result = _translate("%%sql\nUSE CATALOG other\nSELECT * FROM claim"
                            + SQL_CELL_META)
        self.assertIn("NB18_CURRENT_DATABASE", _rules(result))

    def test_a_use_inside_a_sql_string(self):
        result = _translate('spark.sql("USE other")',
                            'spark.sql("SELECT * FROM claim")')
        body = _body(result)
        self.assertIn("FROM claim", body)
        self.assertNotIn("default.Sales.claim", body)
        self.assertIn("NB18_CURRENT_DATABASE", _rules(result))

    def test_a_switch_in_one_cell_reaches_the_next(self):
        result = _translate('spark.catalog.setCurrentDatabase("other")',
                            'spark.table("claim")')
        self.assertIn('spark.table("claim")', _body(result))

    def test_a_qualified_name_is_unaffected(self):
        """`USE` sets the database a *bare* name resolves in. A two-part
        name carries its own, so refusing it would be refusing something
        the switch does not touch."""
        result = _translate('spark.catalog.setCurrentDatabase("other")\n'
                            'spark.table("dbo.claim")')
        self.assertIn("default.Sales.claim", _body(result))
        self.assertNotIn("NB19_NAME_AFTER_DATABASE_SWITCH", _rules(result))

    def test_a_switch_to_the_default_lakehouse_is_followed(self):
        result = _translate('spark.catalog.setCurrentDatabase("Sales")\n'
                            'spark.table("claim")')
        self.assertIn("default.Sales.claim", _body(result))
        self.assertEqual(_rules(result), ["NB10_TABLE_REF"])

    def test_a_switch_to_a_known_item_is_followed(self):
        """`AcmeDW` is the Warehouse a catalog entry records, so the export
        does say what it is and the bare name can be resolved into it."""
        result = _translate('spark.catalog.setCurrentDatabase("AcmeDW")\n'
                            'spark.table("claim")')
        self.assertIn("default.AcmeDW.claim", _body(result))
        self.assertEqual(_rules(result), ["NB10_TABLE_REF"])

    def test_a_temp_view_is_unaffected_by_a_switch(self):
        """A temp view lives in the session, not in a database."""
        result = _translate('df.createOrReplaceTempView("claim")',
                            'spark.catalog.setCurrentDatabase("other")\n'
                            'spark.table("claim")')
        self.assertIn('spark.table("claim")', _body(result))
        self.assertIn("NB16_TEMP_VIEW", _rules(result))

    def test_prose_that_starts_with_the_word_use_is_not_a_switch(self):
        """Three cells in the bundled corpus open a comment with
        `# Use first load when no data exists yet`."""
        result = _translate("# Use first load when no data exists yet\n"
                            'spark.table("claim")')
        self.assertIn("default.Sales.claim", _body(result))
        self.assertNotIn("NB18_CURRENT_DATABASE", _rules(result))

    def test_no_switch_leaves_everything_as_it_was(self):
        result = _translate('spark.table("claim")')
        self.assertIn("default.Sales.claim", _body(result))
        self.assertEqual(_rules(result), ["NB10_TABLE_REF"])


class KnownItemTests(unittest.TestCase):
    """Which items count as "named in this export".

    `_known_items` asked each catalog entry for `warehouse` and
    `lakehouse`, the two fields that predate `owner`, and `resolve` asks
    `catalog.entry_owner`, which prefers `owner`. They therefore disagreed
    about exactly one tier: a notebook-inferred entry records the writing
    notebook's lakehouse in `owner` and in neither of the other two.

    Measured on this tree, a catalog built from one notebook bound to
    `Bronze` that writes `events`:

      before  known_items = {}
              NB18_CURRENT_DATABASE flag, whose text reads "...is not a
                Lakehouse or Warehouse named anywhere in this export" --
                which is false, the export names it
              NB19 flag, `events` left bare
      after   NB14_TABLE_INFERRED, 'events' -> 'default.Bronze.events'

    The writer already spells that table `default.Bronze.events` from its
    own `saveAsTable`, so the old answer was two names for one table on
    top of two findings that should not exist.
    """

    @staticmethod
    def _catalog():
        from fabric_aidp.inventory.catalog import build_catalog
        return build_catalog({"notebook": {"items": {"notebooks": [
            {"name": "Writer", "default_lakehouse": "Bronze",
             "writes": ["events"]},
        ]}}})

    def _run(self, *cells):
        return _translate(*cells, catalog=self._catalog())

    def test_a_lakehouse_known_only_from_a_write_is_a_known_item(self):
        self.assertEqual(sorted(nb2spark._known_items(self._catalog())),
                         ["bronze"])

    def test_a_switch_to_it_is_followed(self):
        result = self._run('spark.sql("USE Bronze")\nspark.table("events")')
        self.assertIn('spark.table("default.Bronze.events")', _body(result))
        self.assertEqual(_rules(result), ["NB14_TABLE_INFERRED"])

    def test_a_switch_to_something_the_export_does_not_name_still_refuses(self):
        result = self._run('spark.sql("USE Nowhere")\nspark.table("events")')
        self.assertEqual(_rules(result),
                         ["NB18_CURRENT_DATABASE",
                          "NB19_NAME_AFTER_DATABASE_SWITCH"])

    def test_the_hand_written_shape_still_works(self):
        """A catalog assembled in a test, or a plan from an older release,
        carries `warehouse`/`lakehouse` and no `owner`. `entry_owner` reads
        those too, which is why the switch loses nothing."""
        hand = {"tables": {"dbo.ledger": {
            "tier": "warehouse_ddl", "name": "dbo.ledger",
            "warehouse": "AcmeDW"}}}
        self.assertEqual(sorted(nb2spark._known_items(hand)), ["acmedw"])


class ATablesFieldThatIsNotADictTests(unittest.TestCase):
    """F5. `_known_items` read `catalog["tables"].values()` unguarded.

    MEASURED on 2a223bd, `nb2spark._known_items(catalog)`:

        {'tables': 'dbo.claim'} -> AttributeError: 'str' object has no
                                   attribute 'values'
        {'tables': ['a']}       -> AttributeError: 'list' ...
        {'tables': 42}          -> AttributeError: 'int' ...
        {'tables': {...}}       -> frozenset({'acmedw'})

    and end to end, through `migrate` with that dict as the plan's
    `resolved_catalog` and one notebook doing `spark.table("dbo.claim")`:

        tables='dbo.claim'   status=error  rules=[]
                             error="'str' object has no attribute 'values'"
        tables={}            status=ok     rules=[]

    -- the #35 shape: an uncaught exception becomes a status `error` row
    with no rule attached, so the report cannot say what went wrong. It
    happens in `translate`'s scope construction, before the first rule, so
    the whole notebook is lost rather than one cell.

    A nit, because no plan this tool writes produces those shapes. What
    settles it as a *fix* rather than a new tolerance is that the answer is
    not invented: `names_no_tables` is `not isinstance(tables, dict) or not
    tables`, so those three shapes are already "this catalog resolves
    nothing" to every other reader -- including the two SQL rules in this
    same file, which ask `names_no_tables` and return early, and the T-SQL
    translator, which returns normally for all three. `_known_items` was
    the one reader that disagreed.

    And it is not silent either: `migrate` computes
    `catalog_resolved_nothing` from `names_no_tables` off the same plan,
    which is True for all three -- measured, `catalog_resolved_nothing:
    true` in report.json and one "names no tables" line in report.md,
    identical to `{"tables": {}}`. That is the scope at which a catalog
    naming nothing is worth saying, so an assertion here would be a second
    and contradictory answer to a question already answered once.
    """

    NOT_DICTS = ("dbo.claim", ["dbo.claim"], 42, 0.5, True, object())

    def test_the_reproduction_returns_the_empty_set(self):
        for tables in self.NOT_DICTS:
            with self.subTest(tables=tables):
                self.assertEqual(nb2spark._known_items({"tables": tables}),
                                 frozenset())

    def test_it_answers_exactly_what_names_no_tables_says(self):
        """The invariant, and the reason `frozenset()` is not invented: one
        predicate decides, and it is the one every other reader asks."""
        from fabric_aidp.inventory.catalog import names_no_tables
        for tables in self.NOT_DICTS + (None, [], {}):
            with self.subTest(tables=tables):
                catalog = {"tables": tables}
                self.assertTrue(names_no_tables(catalog))
                self.assertEqual(nb2spark._known_items(catalog), frozenset())

    def test_a_whole_notebook_no_longer_fails_on_one(self):
        """The cost was not a wrong answer, it was no answer: the raise
        happens in `translate`'s scope construction, so every cell went
        with it."""
        for tables in self.NOT_DICTS:
            with self.subTest(tables=tables):
                result = _translate('df = spark.table("dbo.claim")',
                                    catalog={"tables": tables})
                self.assertEqual(_rules(result), [])
                self.assertIn("dbo.claim", _body(result))

    def test_it_behaves_exactly_as_an_empty_catalog_does(self):
        """The three shapes and `{"tables": {}}` are one state, so the
        artifact and the findings must be identical."""
        empty = _translate('df = spark.table("dbo.claim")',
                           catalog={"tables": {}})
        for tables in self.NOT_DICTS:
            with self.subTest(tables=tables):
                result = _translate('df = spark.table("dbo.claim")',
                                    catalog={"tables": tables})
                self.assertEqual(_rules(result), _rules(empty))
                self.assertEqual(_body(result), _body(empty))

    def test_a_catalog_that_is_not_a_dict_at_all_is_the_same(self):
        """`names_no_tables` guards the outer object too, so this inherits
        it rather than needing its own branch."""
        for catalog in ("nonsense", [], 7):
            with self.subTest(catalog=catalog):
                self.assertEqual(nb2spark._known_items(catalog), frozenset())

    def test_a_real_catalog_still_names_its_items(self):
        """The other direction, so the guard cannot have switched the
        function off. The measured `frozenset({'acmedw'})` above."""
        hand = {"tables": {"dbo.ledger": {
            "tier": "warehouse_ddl", "name": "dbo.ledger",
            "warehouse": "AcmeDW"}}}
        self.assertEqual(sorted(nb2spark._known_items(hand)), ["acmedw"])

    def test_a_switch_into_a_known_item_is_still_followed(self):
        """And the behaviour `_known_items` exists for, end to end."""
        from fabric_aidp.inventory.catalog import build_catalog
        catalog = build_catalog({"notebook": {"items": {"notebooks": [
            {"name": "Writer", "default_lakehouse": "Bronze",
             "writes": ["events"]}]}}})
        result = _translate('spark.sql("USE Bronze")\nspark.table("events")',
                            catalog=catalog)
        self.assertIn('spark.table("default.Bronze.events")', _body(result))
        self.assertEqual(_rules(result), ["NB14_TABLE_INFERRED"])


ABFSS = "abfss://ws@onelake.dfs.fabric.microsoft.com/lh.Lakehouse/Tables/claim"
OCI = "oci://ws_lh_Lakehouse@ns/Tables/claim"


class SqlOneLakePathTests(unittest.TestCase):
    """D4: the same URI Python source has rewritten since NB01 shipped was
    invisible inside SQL. It is not a second path -- once D1 made a SQL
    string reachable by the SQL rules, the rule had only to be added to the
    one place both SQL entry points meet, and the `%%sql` cell, which had
    the same hole, was fixed by the same line."""

    def test_a_onelake_uri_in_a_sql_string_is_rewritten(self):
        result = _translate('spark.sql("SELECT * FROM delta.`%s`")' % ABFSS)
        self.assertIn(OCI, _body(result))
        self.assertIn("NB01_ONELAKE_PATH", _rules(result))

    def test_the_datasource_prefix_is_not_reported_as_an_unknown_table(self):
        """`delta.`<path>`` is Spark's read-by-path syntax, so `delta` is a
        format name and not a table. The clause pattern stopped at the
        backquote and reported it as a table that does not exist."""
        result = _translate('spark.sql("SELECT * FROM delta.`%s`")' % ABFSS)
        self.assertNotIn("NB12_TABLE_UNKNOWN", _rules(result))

    def test_the_same_hole_in_a_sql_cell_closes_with_it(self):
        result = _translate("%%sql\nSELECT * FROM delta.`" + ABFSS + "`"
                            + SQL_CELL_META)
        self.assertIn(OCI, _body(result))
        self.assertIn("NB01_ONELAKE_PATH", _rules(result))

    def test_a_location_clause_literal_is_rewritten(self):
        result = _translate("%%sql\nCREATE TABLE t LOCATION '" + ABFSS + "'"
                            + SQL_CELL_META)
        self.assertIn(OCI, _body(result))

    def test_a_path_that_cannot_be_mapped_is_flagged(self):
        uri = ("abfss://ws@onelake.dfs.fabric.microsoft.com/"
               "11111111-2222-3333-4444-555555555555/Tables/claim")
        result = _translate('spark.sql("SELECT * FROM delta.`%s`")' % uri)
        self.assertIn(uri, _body(result))
        self.assertIn("NB02_ONELAKE_UNMAPPED", _rules(result))

    def test_a_path_that_is_not_onelake_is_left_alone(self):
        source = 'spark.sql("SELECT * FROM delta.`/mnt/raw/claim`")'
        result = _translate(source)
        self.assertIn("/mnt/raw/claim", _body(result))
        self.assertEqual(_rules(result), [])

    def test_a_uri_inside_a_sql_comment_is_left_alone(self):
        result = _translate("%%sql\n-- see " + ABFSS + "\nSELECT 1"
                            + SQL_CELL_META)
        self.assertIn(ABFSS, _body(result))
        self.assertEqual(_rules(result), [])

    def test_a_uri_in_ordinary_python_prose_is_still_left_alone(self):
        """The Python path rule already refuses a run that is not a whole
        literal; this must not become a way around that."""
        result = _translate('note = "the data was at %s"' % ABFSS)
        self.assertIn(ABFSS, _body(result))


class EmptyCatalogIsNotAnAbsentTableTests(unittest.TestCase):
    """The notebook half of the empty-catalog defect.

    Both SQL rules here guarded on `not catalog`, which is right for `None`
    and for `{}` and wrong for the shape a plan carries when its resolution
    found nothing: `{"summary": {}, "tables": {}}`, a truthy dict naming
    nothing. MEASURED on 03f019b, that dict reached `classify_reference`,
    every reference came back `('unknown', None, [])`, and every table
    reference in the estate collected its own NB12_TABLE_UNKNOWN.

    "This run resolved nothing" is not "this table does not exist". The
    run says the first once, in `migrate`; these rules now say nothing.
    """

    EMPTY = {"summary": {}, "tables": {}}
    CELLS = ('df = spark.table("nowhere")',
             'df = spark.sql("SELECT * FROM dbo.nowhere")')

    def test_an_empty_catalog_raises_no_finding_at_all(self):
        for cell in self.CELLS:
            with self.subTest(cell=cell):
                result = _translate(cell, catalog=self.EMPTY)
                self.assertEqual(_rules(result), [])

    def test_it_behaves_exactly_as_no_catalog_does(self):
        """The invariant: a plan that resolved nothing and a plan with no
        catalog at all are the same state, and were two different ones."""
        for cell in self.CELLS:
            with self.subTest(cell=cell):
                empty = _translate(cell, catalog=self.EMPTY)
                absent = _translate(cell, catalog=None)
                self.assertEqual(_rules(empty), _rules(absent))
                self.assertEqual(_body(empty), _body(absent))

    def test_the_name_is_left_as_written(self):
        """With nothing to consult there is nothing to rewrite it to, so it
        is neither rewritten nor flagged -- the behaviour a notebook with no
        catalog supplied has always had."""
        for cell in self.CELLS:
            with self.subTest(cell=cell):
                self.assertIn("nowhere",
                              _body(_translate(cell, catalog=self.EMPTY)))

    def test_a_missing_or_empty_tables_field_is_treated_the_same(self):
        """`{"tables": None}`, `{"tables": []}` and a `tables` that is not a
        collection at all are one state.

        Rewritten deliberately. This used to exclude a string, a list and a
        number, and to record why: "`translate`'s own `_known_items` has
        assumed a dict there since before this change -- MEASURED on
        03f019b, `{"tables": "dbo.claim"}` raises AttributeError from
        `_known_items`, unchanged by this fix -- and no plan this tool
        writes produces that shape." The observation was true and the
        exclusion was the wrong conclusion: it left the one reader in the
        file that disagreed with `names_no_tables` disagreeing, and it
        disagreed by raising before any rule ran, which `migrate` turns into
        a status `error` row with no rule on it. `_known_items` asks
        `names_no_tables` now, so the shapes are in the loop with the two
        that were always here. See
        `ATablesFieldThatIsNotADictTests` for the reproduction.
        """
        for tables in (None, [], "dbo.claim", ["dbo.claim"], 42):
            with self.subTest(tables=tables):
                result = _translate(self.CELLS[0],
                                    catalog={"tables": tables})
                self.assertEqual(_rules(result), [])

    def test_a_real_catalog_still_reports_an_unknown_table(self):
        """The other direction, so the fix cannot have switched the check
        off: one declared table, and a reference to something else."""
        for cell in self.CELLS:
            with self.subTest(cell=cell):
                result = _translate(cell)
                self.assertIn("NB12_TABLE_UNKNOWN", _rules(result))

    def test_a_real_catalog_still_resolves_a_table_it_knows(self):
        result = _translate('df = spark.table("dbo.claim")')
        self.assertIn("NB10_TABLE_REF", _rules(result))
        self.assertIn("default.Sales.claim", _body(result))


class WrittenItemOverriddenTests(unittest.TestCase):
    """Owner-wins over a written item is deliberate
    (`test_the_entrys_warehouse_still_wins`); doing it silently is not.
    `spark.table("Sales.dbo.ledger")` became `default.AcmeDW.ledger` with
    only an NB10 rewrite, so the redirect graded PASS."""

    def test_an_overridden_written_item_is_flagged_and_still_resolved(self):
        out, findings = _run('spark.table("Sales.dbo.ledger")', "rule_table_refs")
        self.assertEqual(out, 'spark.table("default.AcmeDW.ledger")')
        detail = next(f.detail for f in findings if f.rule == "NB40_ITEM_OVERRIDDEN")
        self.assertIn("'Sales'", detail)
        self.assertIn("'AcmeDW'", detail)
        self.assertEqual(next(f.severity for f in findings if f.rule == "NB40_ITEM_OVERRIDDEN"), "flag")

    def test_the_owners_own_name_is_not_flagged(self):
        _, findings = _run('spark.table("AcmeDW.dbo.ledger")', "rule_table_refs")
        self.assertNotIn("NB40_ITEM_OVERRIDDEN", _rules(findings))

    def test_a_two_part_name_is_not_an_explicit_item(self):
        _, findings = _run('spark.table("dbo.ledger")', "rule_table_refs")
        self.assertNotIn("NB40_ITEM_OVERRIDDEN", _rules(findings))

if __name__ == "__main__":
    unittest.main()
