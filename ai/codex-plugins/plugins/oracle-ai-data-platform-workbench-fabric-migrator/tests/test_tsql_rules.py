import re
import unittest

from fabric_aidp.translate import tsql_to_spark_sql as tsql


def _run(sql, rule_name):
    """Run a single named rule and return (sql, findings)."""
    findings = []
    out = getattr(tsql, rule_name)(sql, findings)
    return out, findings


class BracketIdentifierTests(unittest.TestCase):
    def test_simple_bracket_becomes_backtick(self):
        out, findings = _run("SELECT [id] FROM [dbo].[claim]", "rule_bracket_identifiers")
        self.assertEqual(out, "SELECT `id` FROM `dbo`.`claim`")
        self.assertTrue(all(f.severity == "rewrite" for f in findings))

    def test_identifier_with_spaces(self):
        out, _ = _run("SELECT [claim id] FROM t", "rule_bracket_identifiers")
        self.assertEqual(out, "SELECT `claim id` FROM t")

    def test_embedded_backtick_is_doubled(self):
        out, _ = _run("SELECT [we`ird] FROM t", "rule_bracket_identifiers")
        self.assertEqual(out, "SELECT `we``ird` FROM t")

    def test_brackets_inside_a_string_literal_are_untouched(self):
        sql = "SELECT '[not an ident]' AS x"
        out, findings = _run(sql, "rule_bracket_identifiers")
        self.assertEqual(out, sql)
        self.assertEqual(findings, [])

    def test_brackets_inside_a_comment_are_untouched(self):
        sql = "SELECT 1 -- [nope]\nFROM t"
        self.assertEqual(_run(sql, "rule_bracket_identifiers")[0], sql)

    def test_no_brackets_is_a_no_op(self):
        self.assertEqual(_run("SELECT a FROM t", "rule_bracket_identifiers"),
                         ("SELECT a FROM t", []))

    def test_one_finding_per_identifier(self):
        _, findings = _run("SELECT [a], [b] FROM t", "rule_bracket_identifiers")
        self.assertEqual(len(findings), 2)


class ThreePartNameTests(unittest.TestCase):
    """SQ11 used to rewrite the database part to `default` and keep the
    schema, on the premise that AIDP has one catalog. A live instance has
    five, and the first part of a T-SQL name is the database -- the Fabric
    item -- so `SalesLake.dbo.claim` became `default.dbo.claim`: a different
    table, silently, under PASS."""

    def _run(self, sql, **kw):
        findings = []
        return tsql.rule_three_part_names(sql, findings, **kw), findings

    def test_the_fabric_item_becomes_the_schema(self):
        out, _ = self._run("SELECT * FROM SalesLake.dbo.claim")
        self.assertEqual(out, "SELECT * FROM default.SalesLake.claim")

    def test_a_non_default_schema_is_kept(self):
        out, _ = self._run("SELECT * FROM AcmeDW.postgres_air.t")
        self.assertEqual(out, "SELECT * FROM default.AcmeDW_postgres_air.t")

    def test_backticked_three_part_name(self):
        """Only the part that needs quoting keeps it. Quoting all three was
        safe and unreadable; leaving all three bare made `Sales Lake` a
        syntax error. See naming.quote_part."""
        out, _ = self._run("SELECT * FROM [Sales Lake].[dbo].[claim]")
        self.assertEqual(out, "SELECT * FROM default.`Sales Lake`.claim")

    def test_the_catalog_is_configurable(self):
        out, _ = self._run("SELECT * FROM SalesLake.dbo.claim", catalog="myc")
        self.assertEqual(out, "SELECT * FROM myc.SalesLake.claim")

    def test_rewriting_twice_changes_nothing(self):
        once, _ = self._run("SELECT * FROM SalesLake.dbo.claim")
        twice, _ = self._run(once)
        self.assertEqual(once, twice)

    def test_more_than_three_parts_is_flagged_not_rewritten(self):
        out, findings = self._run("SELECT * FROM a.b.c.d")
        self.assertEqual(out, "SELECT * FROM a.b.c.d")
        self.assertIn("SQ11_LINKED_SERVER", [f.rule for f in findings])

    def test_a_two_part_name_is_left_alone(self):
        """By the three-part rule. `rule_two_part_names` handles it, and only
        when it is told which Fabric item owns the object."""
        out, findings = self._run("SELECT * FROM dbo.claim")
        self.assertEqual(out, "SELECT * FROM dbo.claim")
        self.assertEqual(findings, [])

    def test_the_finding_names_the_catalog(self):
        _, findings = self._run("SELECT * FROM SalesLake.dbo.claim")
        detail = next(f.detail for f in findings
                      if f.rule == "SQ11_THREE_PART_NAME")
        self.assertIn("default.SalesLake.claim", detail)

    def test_decimal_literal_is_not_a_name(self):
        sql = "SELECT 1.5 FROM t"
        self.assertEqual(self._run(sql), (sql, []))

    def test_name_inside_a_literal_is_untouched(self):
        sql = "SELECT 'AcmeDW.dbo.claim' AS s"
        self.assertEqual(self._run(sql), (sql, []))


class TwoPartNameTests(unittest.TestCase):
    """DacFx writes two-part DDL -- 9 of the 9 real corpus warehouse files
    and 42 of their 44 objects are `CREATE TABLE schema.object`, the
    Warehouse name living in the folder. The three-part rule therefore fired
    zero times across the whole demo, and the plan said
    `default.AcmeDW.claim` while the artifact creating it said
    ``CREATE TABLE `dbo`.`claim` ``."""

    def _run(self, sql, **kw):
        findings = []
        return tsql.rule_two_part_names(sql, findings, **kw), findings

    def test_the_create_target_is_qualified_with_the_item(self):
        out, _ = self._run("CREATE TABLE dbo.claim (id BIGINT)", item="AcmeDW")
        self.assertEqual(out, "CREATE TABLE default.AcmeDW.claim (id BIGINT)")

    def test_a_backticked_target_is_qualified_too(self):
        """SQ10 runs first, so what reaches this rule is backticked, not
        bracketed -- which is the shape 42 of the 44 real objects arrive in."""
        out, _ = self._run("CREATE TABLE `dbo`.`claim` (id BIGINT)",
                           item="AcmeDW")
        self.assertEqual(out, "CREATE TABLE default.AcmeDW.claim (id BIGINT)")

    def test_a_non_default_schema_is_kept_as_a_suffix(self):
        out, _ = self._run("CREATE TABLE postgres_air.t (id BIGINT)",
                           item="AcmeDW")
        self.assertEqual(out, "CREATE TABLE default.AcmeDW_postgres_air.t (id BIGINT)")

    def test_a_view_body_reference_is_qualified(self):
        out, _ = self._run("CREATE VIEW dbo.v AS SELECT id FROM dbo.agent",
                           item="AcmeDW")
        self.assertEqual(
            out, "CREATE VIEW default.AcmeDW.v AS SELECT id FROM default.AcmeDW.agent")

    def test_no_item_leaves_the_name_alone(self):
        """A guessed item is worse than an unqualified name: the first reads
        as a fact."""
        sql = "CREATE TABLE dbo.claim (id BIGINT)"
        self.assertEqual(self._run(sql), (sql, []))
        self.assertEqual(self._run(sql, item=""), (sql, []))

    def test_an_alias_qualified_column_is_not_a_table(self):
        """The whole reason this rule is anchored to a keyword. `t.col` in a
        select list is a column, and qualifying it would be nonsense Spark
        would accept."""
        out, _ = self._run("SELECT t.col, t.other FROM dbo.claim AS t",
                           item="AcmeDW")
        self.assertEqual(out, "SELECT t.col, t.other FROM default.AcmeDW.claim AS t")

    def test_a_three_part_name_is_not_touched_here(self):
        sql = "SELECT * FROM default.AcmeDW.claim"
        self.assertEqual(self._run(sql, item="AcmeDW"), (sql, []))

    def test_rewriting_twice_changes_nothing(self):
        once, _ = self._run("CREATE TABLE dbo.claim (id BIGINT)", item="AcmeDW")
        twice, _ = self._run(once, item="AcmeDW")
        self.assertEqual(once, twice)

    def test_the_catalog_is_configurable(self):
        out, _ = self._run("CREATE TABLE dbo.claim (id BIGINT)",
                           item="AcmeDW", catalog="myc")
        self.assertEqual(out, "CREATE TABLE myc.AcmeDW.claim (id BIGINT)")

    def test_an_item_needing_quotes_gets_them(self):
        out, _ = self._run(
            "CREATE TABLE dbo.synthetic_orders (id BIGINT)",
            item="fabric-data-engineering-ws_on-prem-warehouse-test-wh")
        self.assertIn(
            "default.`fabric-data-engineering-ws_on-prem-warehouse-test-wh`"
            ".synthetic_orders", out)

    def test_a_system_schema_is_left_alone(self):
        sql = "SELECT * FROM sys.tables"
        self.assertEqual(self._run(sql, item="AcmeDW"), (sql, []))

    def test_a_name_inside_a_literal_is_untouched(self):
        sql = "SELECT 'FROM dbo.claim' AS s"
        self.assertEqual(self._run(sql, item="AcmeDW"), (sql, []))

    def test_a_procedure_target_is_left_alone(self):
        """What an AIDP name for a stored procedure should be is open; SQ01
        flags the object whole in any case."""
        sql = "CREATE PROCEDURE dbo.sp_load AS SELECT 1"
        self.assertEqual(self._run(sql, item="AcmeDW"), (sql, []))

    def test_the_finding_names_both_ends(self):
        _, findings = self._run("CREATE TABLE dbo.claim (id BIGINT)",
                                item="AcmeDW")
        detail = next(f.detail for f in findings if f.rule == "SQ11_TWO_PART_NAME")
        self.assertIn("dbo.claim", detail)
        self.assertIn("default.AcmeDW.claim", detail)


class OnePartNameTests(unittest.TestCase):
    """A bare one-part name reached no rule at all. MEASURED before SQ11's
    one-part rule, `translate(sql, kind="table", item="W")`:

        SELECT * FROM claim  ->  SELECT * FROM claim   findings: []

    Not rewritten, not flagged, nothing -- while the notebook path reported
    NB12_TABLE_UNKNOWN for the identical reference. A one-part name is
    ordinary in a DacFx export, so this was silence on a common shape.
    """

    def _run(self, sql, **kw):
        findings = []
        return tsql.rule_one_part_names(sql, findings, **kw), findings

    def _rules(self, sql, **kw):
        return [f.rule for f in self._run(sql, **kw)[1]]

    # -- it fires where a bare name can only be an object ------------------

    def test_a_bare_name_after_from_is_flagged(self):
        out, findings = self._run("SELECT * FROM claim", item="AcmeDW")
        self.assertEqual(out, "SELECT * FROM claim")
        self.assertEqual([f.rule for f in findings], ["SQ11_ONE_PART_NAME"])

    def test_it_never_rewrites(self):
        """The decision this rule records. T-SQL resolves a bare name against
        the caller's default schema and a DacFx export records no caller;
        `aidp_schema` drops Fabric's default schema, so qualifying a bare
        name as `dbo` emits the very name `dbo.claim` would and names a
        different table wherever that assumption is wrong."""
        for sql in ("SELECT * FROM claim", "CREATE TABLE claim (id INT)",
                    "TRUNCATE TABLE claim", "INSERT INTO claim VALUES (1)"):
            with self.subTest(sql=sql):
                out, findings = self._run(sql, item="AcmeDW")
                self.assertEqual(out, sql)
                self.assertEqual([f.severity for f in findings], ["flag"])

    def test_every_object_keyword_position_is_covered(self):
        for sql in ("CREATE TABLE claim (id INT)",
                    "CREATE OR ALTER VIEW v_claim AS SELECT 1",
                    "ALTER TABLE claim ADD COLUMN a INT",
                    "DROP TABLE IF EXISTS claim",
                    "TRUNCATE TABLE claim",
                    "INSERT INTO claim VALUES (1)",
                    "SELECT a INTO staging FROM dbo.t",
                    "SELECT * FROM claim",
                    "SELECT * FROM dbo.t JOIN payment ON 1 = 1"):
            with self.subTest(sql=sql):
                self.assertIn("SQ11_ONE_PART_NAME",
                              self._rules(sql, item="AcmeDW"))

    def test_both_names_in_one_statement_are_flagged(self):
        rules = self._rules("SELECT * FROM claim AS c JOIN payment p "
                            "ON c.id = p.id", item="AcmeDW")
        self.assertEqual(rules, ["SQ11_ONE_PART_NAME"] * 2)

    # -- and not where it cannot ------------------------------------------

    def test_a_two_part_name_is_not_this_rules_business(self):
        sql = "SELECT * FROM dbo.claim"
        self.assertEqual(self._run(sql, item="AcmeDW"), (sql, []))

    def test_an_already_qualified_three_part_name_is_invisible(self):
        """The two-part rule runs first and leaves this shape behind, so the
        one-part pattern must not read `claim` out of the tail of it."""
        sql = "SELECT * FROM default.AcmeDW.claim"
        self.assertEqual(self._run(sql, item="AcmeDW"), (sql, []))

    def test_a_cte_the_statement_declares_is_not_a_table(self):
        """`WITH recent AS (...) SELECT * FROM recent`. A CTE names a result
        set that lives for one statement, so the reference is already
        correct -- and flagging it grades an otherwise clean object REVIEW,
        which is the cost `cte_names` exists to stop."""
        sql = ("WITH recent AS (SELECT * FROM dbo.claim) "
               "SELECT * FROM recent")
        self.assertEqual(self._run(sql, item="AcmeDW"), (sql, []))

    def test_a_table_valued_function_is_not_a_table(self):
        for sql in ("SELECT * FROM STRING_SPLIT('a,b', ',')",
                    "SELECT * FROM OPENJSON(@j)"):
            with self.subTest(sql=sql):
                self.assertEqual(self._run(sql, item="AcmeDW"), (sql, []))

    def test_a_from_taking_call_is_not_a_table_position(self):
        for sql in ("SELECT TRIM(BOTH ' ' FROM name) FROM dbo.t",
                    "SELECT EXTRACT(year FROM d) FROM dbo.t"):
            with self.subTest(sql=sql):
                self.assertEqual(self._run(sql, item="AcmeDW"), (sql, []))

    def test_a_temp_table_or_variable_is_not_a_bare_object(self):
        """`#tmp` has SQ50 and `@tv` is not an object at all. Neither starts
        an identifier part, so `_IDENT_PART` excludes both by shape."""
        for sql in ("SELECT * FROM #tmp", "SELECT * FROM @tv"):
            with self.subTest(sql=sql):
                self.assertEqual(self._run(sql, item="AcmeDW"), (sql, []))

    def test_a_derived_table_is_not_a_name(self):
        sql = "SELECT * FROM (SELECT 1 AS a) d"
        self.assertEqual(self._run(sql, item="AcmeDW"), (sql, []))

    def test_a_name_inside_a_literal_or_comment_is_untouched(self):
        for sql in ("SELECT 'FROM claim' AS s FROM dbo.t",
                    "-- FROM claim\nSELECT * FROM dbo.t"):
            with self.subTest(sql=sql):
                self.assertEqual(self._run(sql, item="AcmeDW"), (sql, []))

    # -- UPDATE is excluded, and this is the measurement that says why -----

    def test_update_is_not_an_object_position_here(self):
        """MEASURED: with UPDATE in the keyword list these three produced a
        false positive each -- an alias, and two keywords. The two keywords
        could be excluded by a word list the way `_NOT_AN_INSERT_TARGET`
        excludes the INSERT-position ones; the alias cannot, because it is an
        arbitrary identifier the same statement declares further along. So
        UPDATE is out, and the cost is the deliberate miss below."""
        for sql, what in (
                ("UPDATE c SET c.a = 1 FROM dbo.claim AS c", "an alias"),
                ("UPDATE STATISTICS dbo.claim", "a keyword"),
                ("UPDATE TOP (1) dbo.claim SET a = 1", "a keyword")):
            with self.subTest(what=what, sql=sql):
                self.assertEqual(self._run(sql, item="AcmeDW"), (sql, []))

    def test_a_genuinely_bare_update_target_is_the_accepted_miss(self):
        """The price of the line above, written down so it is a decision and
        not a surprise. A bare UPDATE only reaches the rules at all in a
        non-procedural object: `translate` raises SQ01_PROCEDURAL and returns
        before any rule runs for the rest."""
        sql = "UPDATE claim SET a = 1"
        self.assertEqual(self._run(sql, item="AcmeDW"), (sql, []))

    # -- gating ------------------------------------------------------------

    def test_no_item_says_nothing(self):
        """Gated exactly as the two-part rule is. A `%%tsql` cell passes no
        item, and there NB15 has already put the catalog over every table
        name in the body -- bare ones included -- so firing here would report
        one reference twice."""
        sql = "SELECT * FROM claim"
        self.assertEqual(self._run(sql), (sql, []))
        self.assertEqual(self._run(sql, item=""), (sql, []))

    def test_a_tsql_cell_gets_no_finding_from_this_rule(self):
        result = tsql.translate("SELECT * FROM claim", kind="other",
                                resolve_table_names=False)
        self.assertNotIn("SQ11_ONE_PART_NAME",
                         [f.rule for f in result.findings])

    # -- the finding itself ------------------------------------------------

    def test_the_finding_names_the_name_and_the_reason(self):
        _, findings = self._run("SELECT * FROM claim", item="AcmeDW")
        detail = findings[0].detail
        self.assertIn("'claim'", detail)
        self.assertIn("default schema", detail)
        self.assertIn("AcmeDW", detail)

    def test_the_whole_translator_reports_it(self):
        result = tsql.translate("SELECT * FROM claim", kind="table", item="W")
        self.assertEqual(result.translated_sql, "SELECT * FROM claim")
        self.assertEqual([f.rule for f in result.findings],
                         ["SQ11_ONE_PART_NAME"])
        self.assertEqual(result.changes, 0)
        self.assertEqual(result.flags, 1)

    def test_the_demo_warehouse_sql_carries_none_of_this_shape(self):
        """Why the demo does not move for this rule, asserted rather than
        asserted-in-prose. DacFx writes two-part DDL, so the false-positive
        surface measured on those files was zero matches of any kind -- there
        was nothing there to measure it against, which is the honest reason
        the adversarial battery above exists instead."""
        from pathlib import Path
        root = Path(__file__).resolve().parents[1]
        files = sorted(root.glob("fabric_aidp/fixtures/demo-workspace/"
                                 "*.Warehouse/*.sql"))
        self.assertGreater(len(files), 0)
        for path in files:
            sql = path.read_text(encoding="utf-8-sig")
            with self.subTest(path=path.name):
                self.assertEqual(
                    self._rules(sql, item="AcmeDW"), [],
                    "%s now carries a one-part object name; the demo "
                    "movement for SQ11_ONE_PART_NAME has to be re-measured"
                    % path.name)


class DatediffTests(unittest.TestCase):
    def test_day_swaps_operands_and_drops_the_datepart(self):
        out, findings = _run("SELECT DATEDIFF(day, start_dt, end_dt) FROM t",
                             "rule_datediff")
        self.assertEqual(out, "SELECT datediff(end_dt, start_dt) FROM t")
        self.assertEqual(findings[0].severity, "rewrite")
        self.assertIn("operand", findings[0].detail.lower())

    def test_dd_and_d_abbreviations(self):
        for part in ("dd", "d", "DAY", "Dd"):
            with self.subTest(part=part):
                out, _ = _run(f"SELECT DATEDIFF({part}, a, b)", "rule_datediff")
                self.assertEqual(out, "SELECT datediff(b, a)")

    def test_expression_arguments_are_preserved_verbatim(self):
        out, _ = _run("SELECT DATEDIFF(day, MIN(a), CAST(b AS DATE))", "rule_datediff")
        self.assertEqual(out, "SELECT datediff(CAST(b AS DATE), MIN(a))")

    def test_month_is_flagged_not_rewritten(self):
        sql = "SELECT DATEDIFF(month, a, b)"
        out, findings = _run(sql, "rule_datediff")
        self.assertEqual(out, sql)
        self.assertEqual(findings[0].severity, "flag")
        self.assertIn("month", findings[0].detail.lower())

    def test_hour_is_flagged(self):
        _, findings = _run("SELECT DATEDIFF(hour, a, b)", "rule_datediff")
        self.assertEqual(findings[0].severity, "flag")

    def test_wrong_arity_is_flagged(self):
        sql = "SELECT DATEDIFF(day, a)"
        out, findings = _run(sql, "rule_datediff")
        self.assertEqual(out, sql)
        self.assertEqual(findings[0].severity, "flag")

    def test_inside_a_literal_is_untouched(self):
        sql = "SELECT 'DATEDIFF(day, a, b)' AS s"
        self.assertEqual(_run(sql, "rule_datediff"), (sql, []))

    def test_two_calls_are_both_handled(self):
        out, findings = _run("SELECT DATEDIFF(day,a,b), DATEDIFF(day,c,d)",
                             "rule_datediff")
        self.assertEqual(out, "SELECT datediff(b, a), datediff(d, c)")
        self.assertEqual(len(findings), 2)


class CharindexTests(unittest.TestCase):
    def test_two_arguments_keep_their_order(self):
        out, findings = _run("SELECT CHARINDEX('x', name) FROM t", "rule_charindex")
        self.assertEqual(out, "SELECT locate('x', name) FROM t")
        self.assertEqual(findings[0].severity, "rewrite")

    def test_three_argument_form_is_supported(self):
        out, _ = _run("SELECT CHARINDEX('x', name, 3)", "rule_charindex")
        self.assertEqual(out, "SELECT locate('x', name, 3)")

    def test_finding_explains_why_not_instr(self):
        _, findings = _run("SELECT CHARINDEX('x', name)", "rule_charindex")
        self.assertIn("instr", findings[0].detail)

    def test_wrong_arity_is_flagged(self):
        sql = "SELECT CHARINDEX('x')"
        out, findings = _run(sql, "rule_charindex")
        self.assertEqual(out, sql)
        self.assertEqual(findings[0].severity, "flag")

    def test_inside_a_comment_is_untouched(self):
        sql = "SELECT 1 -- CHARINDEX('x', y)\nFROM t"
        self.assertEqual(_run(sql, "rule_charindex"), (sql, []))



class ScalarRenameTests(unittest.TestCase):
    def test_isnull_becomes_coalesce(self):
        out, findings = _run("SELECT ISNULL(a, 0) FROM t", "rule_scalar_renames")
        self.assertEqual(out, "SELECT coalesce(a, 0) FROM t")
        self.assertEqual(findings[0].severity, "rewrite")

    def test_isnull_with_three_arguments_is_flagged(self):
        sql = "SELECT ISNULL(a, b, c)"
        out, findings = _run(sql, "rule_scalar_renames")
        self.assertEqual(out, sql)
        self.assertEqual(findings[0].severity, "flag")

    def test_getdate_becomes_current_timestamp(self):
        out, _ = _run("SELECT GETDATE()", "rule_scalar_renames")
        self.assertEqual(out, "SELECT current_timestamp()")

    def test_sysdatetime_becomes_current_timestamp(self):
        out, _ = _run("SELECT SYSDATETIME()", "rule_scalar_renames")
        self.assertEqual(out, "SELECT current_timestamp()")

    def test_getutcdate_is_flagged_not_rewritten(self):
        sql = "SELECT GETUTCDATE()"
        out, findings = _run(sql, "rule_scalar_renames")
        self.assertEqual(out, sql)
        self.assertEqual(findings[0].severity, "flag")
        self.assertIn("UTC", findings[0].detail)

    def test_len_is_flagged_and_names_the_replacement(self):
        sql = "SELECT LEN(name) FROM t"
        out, findings = _run(sql, "rule_scalar_renames")
        self.assertEqual(out, sql)
        self.assertEqual(findings[0].severity, "flag")
        self.assertIn("length(rtrim(", findings[0].detail)
        self.assertIn("trailing", findings[0].detail.lower())

    def test_space_becomes_repeat(self):
        out, _ = _run("SELECT SPACE(4)", "rule_scalar_renames")
        self.assertEqual(out, "SELECT repeat(' ', 4)")

    def test_square_becomes_power(self):
        out, _ = _run("SELECT SQUARE(x)", "rule_scalar_renames")
        self.assertEqual(out, "SELECT power(x, 2)")

    def test_case_insensitive_matching(self):
        out, _ = _run("SELECT isnull(a, 0)", "rule_scalar_renames")
        self.assertEqual(out, "SELECT coalesce(a, 0)")

    def test_a_column_named_like_a_function_is_untouched(self):
        sql = "SELECT t.isnull FROM t"
        self.assertEqual(_run(sql, "rule_scalar_renames"), (sql, []))

    def test_inside_a_literal_is_untouched(self):
        sql = "SELECT 'GETDATE()' AS s"
        self.assertEqual(_run(sql, "rule_scalar_renames"), (sql, []))

    def test_nested_arguments_are_preserved(self):
        out, _ = _run("SELECT ISNULL(MAX(a), 0)", "rule_scalar_renames")
        self.assertEqual(out, "SELECT coalesce(MAX(a), 0)")



class StringConcatTests(unittest.TestCase):
    def test_literal_plus_column(self):
        out, findings = _run("SELECT 'Mr ' + name FROM t", "rule_string_concat")
        self.assertEqual(out, "SELECT concat('Mr ', name) FROM t")
        self.assertEqual(findings[0].severity, "rewrite")

    def test_column_plus_literal(self):
        out, _ = _run("SELECT name + '!' FROM t", "rule_string_concat")
        self.assertEqual(out, "SELECT concat(name, '!') FROM t")

    def test_three_term_run_becomes_one_concat(self):
        out, findings = _run("SELECT first + ' ' + last FROM t", "rule_string_concat")
        self.assertEqual(out, "SELECT concat(first, ' ', last) FROM t")
        self.assertEqual(len(findings), 1)

    def test_dotted_identifier_term(self):
        out, _ = _run("SELECT t.a + '-' + t.b", "rule_string_concat")
        self.assertEqual(out, "SELECT concat(t.a, '-', t.b)")

    def test_function_call_term(self):
        out, _ = _run("SELECT UPPER(a) + '-' + b", "rule_string_concat")
        self.assertEqual(out, "SELECT concat(UPPER(a), '-', b)")

    def test_backticked_identifier_term(self):
        out, _ = _run("SELECT `claim id` + '!'", "rule_string_concat")
        self.assertEqual(out, "SELECT concat(`claim id`, '!')")

    def test_backticked_identifier_on_the_right(self):
        out, _ = _run("SELECT '!' + `claim id`", "rule_string_concat")
        self.assertEqual(out, "SELECT concat('!', `claim id`)")

    def test_bracketed_identifier_with_a_space(self):
        out, _ = _run("SELECT [claim id] + '!'", "rule_string_concat")
        self.assertEqual(out, "SELECT concat([claim id], '!')")

    def test_numeric_addition_is_left_alone(self):
        sql = "SELECT a + b FROM t"
        self.assertEqual(_run(sql, "rule_string_concat"), (sql, []))

    def test_numeric_literals_are_left_alone(self):
        sql = "SELECT 1 + 2"
        self.assertEqual(_run(sql, "rule_string_concat"), (sql, []))

    def test_plus_inside_a_literal_is_untouched(self):
        sql = "SELECT 'a + b' AS s"
        self.assertEqual(_run(sql, "rule_string_concat"), (sql, []))

    def test_plus_inside_a_comment_is_untouched(self):
        sql = "SELECT 1 -- 'a' + b\nFROM t"
        self.assertEqual(_run(sql, "rule_string_concat"), (sql, []))

    def test_two_separate_runs_each_become_a_concat(self):
        out, findings = _run("SELECT 'a' + b, 'c' + d", "rule_string_concat")
        self.assertEqual(out, "SELECT concat('a', b), concat('c', d)")
        self.assertEqual(len(findings), 2)

    def test_finding_explains_the_silent_failure(self):
        _, findings = _run("SELECT 'a' + b", "rule_string_concat")
        detail = findings[0].detail.lower()
        self.assertIn("null", detail)

    def test_escaped_quote_inside_a_term(self):
        out, _ = _run("SELECT 'it''s ' + name", "rule_string_concat")
        self.assertEqual(out, "SELECT concat('it''s ', name)")

    def test_parenthesised_term(self):
        out, _ = _run("SELECT (a) + '!'", "rule_string_concat")
        self.assertEqual(out, "SELECT concat((a), '!')")



class TopTests(unittest.TestCase):
    def test_top_becomes_limit_at_the_end(self):
        out, findings = _run("SELECT TOP 10 a FROM t", "rule_top")
        self.assertEqual(out, "SELECT a FROM t LIMIT 10")
        self.assertEqual(findings[0].severity, "rewrite")

    def test_parenthesised_count(self):
        out, _ = _run("SELECT TOP (10) a FROM t", "rule_top")
        self.assertEqual(out, "SELECT a FROM t LIMIT 10")

    def test_limit_goes_after_order_by(self):
        out, _ = _run("SELECT TOP 5 a FROM t ORDER BY a", "rule_top")
        self.assertEqual(out, "SELECT a FROM t ORDER BY a LIMIT 5")

    def test_trailing_semicolon_is_preserved(self):
        out, _ = _run("SELECT TOP 5 a FROM t;", "rule_top")
        self.assertEqual(out, "SELECT a FROM t LIMIT 5;")

    def test_distinct_is_preserved(self):
        out, _ = _run("SELECT DISTINCT TOP 5 a FROM t", "rule_top")
        self.assertEqual(out, "SELECT DISTINCT a FROM t LIMIT 5")

    def test_top_in_a_subquery_is_flagged_not_moved(self):
        sql = "SELECT * FROM (SELECT TOP 5 a FROM t) x"
        out, findings = _run(sql, "rule_top")
        self.assertEqual(out, sql)
        self.assertEqual(findings[0].severity, "flag")
        self.assertIn("subquery", findings[0].detail.lower())

    def test_percent_is_flagged(self):
        sql = "SELECT TOP 10 PERCENT a FROM t"
        out, findings = _run(sql, "rule_top")
        self.assertEqual(out, sql)
        self.assertEqual(findings[0].severity, "flag")
        self.assertIn("PERCENT", findings[0].detail)

    def test_with_ties_is_flagged(self):
        sql = "SELECT TOP 10 WITH TIES a FROM t"
        out, findings = _run(sql, "rule_top")
        self.assertEqual(out, sql)
        self.assertEqual(findings[0].severity, "flag")

    def test_variable_count_is_flagged(self):
        sql = "SELECT TOP @n a FROM t"
        out, findings = _run(sql, "rule_top")
        self.assertEqual(out, sql)
        self.assertEqual(findings[0].severity, "flag")

    def test_no_top_is_a_no_op(self):
        sql = "SELECT a FROM t"
        self.assertEqual(_run(sql, "rule_top"), (sql, []))

    def test_top_inside_a_literal_is_untouched(self):
        sql = "SELECT 'TOP 5' AS s"
        self.assertEqual(_run(sql, "rule_top"), (sql, []))


class SelectIntoTests(unittest.TestCase):
    def test_select_into_becomes_ctas(self):
        out, findings = _run("SELECT a, b INTO new_t FROM t", "rule_select_into")
        self.assertEqual(out, "CREATE TABLE new_t AS SELECT a, b FROM t")
        self.assertEqual(findings[0].severity, "rewrite")

    def test_qualified_target_name(self):
        out, _ = _run("SELECT a INTO dbo.new_t FROM t", "rule_select_into")
        self.assertEqual(out, "CREATE TABLE dbo.new_t AS SELECT a FROM t")

    def test_bracketed_target_name(self):
        out, _ = _run("SELECT a INTO [new t] FROM t", "rule_select_into")
        self.assertEqual(out, "CREATE TABLE [new t] AS SELECT a FROM t")

    def test_temp_table_target_is_left_as_written(self):
        """No CTAS: a `CREATE TABLE #tmp AS ...` would create a permanent
        table where the original created a session-scoped one. The finding
        belongs to `rule_temp_table`, which names the target and says why;
        SQ51_TEMP_TABLE was reported here too and could never fire, because
        `#tmp` tripped the procedural gate before RULES ran."""
        sql = "SELECT a INTO #tmp FROM t"
        self.assertEqual(_run(sql, "rule_select_into"), (sql, []))
        _, findings = _run(sql, "rule_temp_table")
        self.assertEqual([f.rule for f in findings], ["SQ51_TEMP_TABLE"])
        self.assertIn("#tmp", findings[0].detail)

    def test_insert_into_is_not_touched(self):
        sql = "INSERT INTO t (a) VALUES (1)"
        self.assertEqual(_run(sql, "rule_select_into"), (sql, []))

    def test_into_inside_a_subquery_is_flagged(self):
        sql = "SELECT * FROM (SELECT a INTO x FROM t) y"
        out, findings = _run(sql, "rule_select_into")
        self.assertEqual(out, sql)
        self.assertEqual(findings[0].severity, "flag")

    def test_no_into_is_a_no_op(self):
        sql = "SELECT a FROM t"
        self.assertEqual(_run(sql, "rule_select_into"), (sql, []))


class DataTypeTests(unittest.TestCase):
    def _ddl(self, column_sql):
        return f"CREATE TABLE dbo.t (\n    {column_sql}\n)"

    def test_nvarchar_becomes_string_and_drops_length(self):
        out, findings = _run(self._ddl("name NVARCHAR(100)"), "rule_data_types")
        self.assertIn("name STRING", out)
        self.assertNotIn("100", out)
        self.assertEqual(findings[0].severity, "rewrite")

    def test_varchar_max(self):
        out, _ = _run(self._ddl("body VARCHAR(MAX)"), "rule_data_types")
        self.assertIn("body STRING", out)

    def test_bit_becomes_boolean(self):
        out, _ = _run(self._ddl("is_open BIT"), "rule_data_types")
        self.assertIn("is_open BOOLEAN", out)

    def test_datetime2_becomes_timestamp(self):
        out, _ = _run(self._ddl("created DATETIME2(7)"), "rule_data_types")
        self.assertIn("created TIMESTAMP", out)

    def test_numeric_keeps_precision(self):
        out, _ = _run(self._ddl("amount NUMERIC(10,2)"), "rule_data_types")
        self.assertIn("amount DECIMAL(10,2)", out)

    def test_float_and_real(self):
        out, _ = _run(self._ddl("a FLOAT,\n    b REAL"), "rule_data_types")
        self.assertIn("a DOUBLE", out)
        self.assertIn("b FLOAT", out)

    def test_unchanged_types_produce_no_finding(self):
        sql = self._ddl("id BIGINT,\n    d DATE")
        self.assertEqual(_run(sql, "rule_data_types"), (sql, []))

    def test_money_is_flagged_not_mapped(self):
        out, findings = _run(self._ddl("amount MONEY"), "rule_data_types")
        self.assertIn("amount MONEY", out)
        self.assertEqual(findings[0].severity, "flag")
        self.assertIn("rounding", findings[0].detail.lower())

    def test_uniqueidentifier_is_flagged(self):
        _, findings = _run(self._ddl("id UNIQUEIDENTIFIER"), "rule_data_types")
        self.assertEqual(findings[0].severity, "flag")

    def test_cast_target_is_rewritten(self):
        out, _ = _run("SELECT CAST(a AS NVARCHAR(50)) FROM t", "rule_data_types")
        self.assertEqual(out, "SELECT CAST(a AS STRING) FROM t")

    def test_try_cast_target_is_rewritten(self):
        out, _ = _run("SELECT TRY_CAST(a AS BIT) FROM t", "rule_data_types")
        self.assertEqual(out, "SELECT TRY_CAST(a AS BOOLEAN) FROM t")

    def test_a_column_named_like_a_type_is_untouched(self):
        sql = "SELECT text, date FROM t"
        self.assertEqual(_run(sql, "rule_data_types"), (sql, []))

    def test_a_column_actually_named_text_still_gets_its_type_mapped(self):
        out, _ = _run(self._ddl("text NVARCHAR(10)"), "rule_data_types")
        self.assertIn("text STRING", out)

    def test_type_inside_a_literal_is_untouched(self):
        sql = "SELECT 'NVARCHAR(10)' AS s"
        self.assertEqual(_run(sql, "rule_data_types"), (sql, []))

    def test_several_columns_each_get_a_finding(self):
        _, findings = _run(self._ddl("a NVARCHAR(1),\n    b BIT,\n    c FLOAT"),
                           "rule_data_types")
        self.assertEqual(len(findings), 3)


class ConstraintTests(unittest.TestCase):
    def test_table_level_primary_key_is_removed_and_flagged(self):
        sql = "CREATE TABLE t (\n    id BIGINT,\n    PRIMARY KEY (id)\n)"
        out, findings = _run(sql, "rule_constraints")
        self.assertNotIn("PRIMARY KEY", out)
        self.assertIn("id BIGINT", out)
        self.assertEqual(findings[0].severity, "flag")

    def test_named_constraint_is_removed(self):
        sql = ("CREATE TABLE t (\n    id BIGINT,\n"
               "    CONSTRAINT pk_t PRIMARY KEY NONCLUSTERED (id) NOT ENFORCED\n)")
        out, _ = _run(sql, "rule_constraints")
        self.assertNotIn("CONSTRAINT", out)
        self.assertNotIn("pk_t", out)

    def test_foreign_key_is_removed(self):
        sql = ("CREATE TABLE t (\n    a BIGINT,\n"
               "    FOREIGN KEY (a) REFERENCES other(id)\n)")
        out, findings = _run(sql, "rule_constraints")
        self.assertNotIn("FOREIGN KEY", out)
        self.assertIn("foreign key", findings[0].detail.lower())

    def test_check_constraint_with_nested_parens_is_one_item(self):
        sql = "CREATE TABLE t (\n    a INT,\n    CHECK (a IN (1, 2))\n)"
        out, findings = _run(sql, "rule_constraints")
        self.assertNotIn("CHECK", out)
        self.assertIn("a INT", out)
        self.assertEqual(len(findings), 1)

    def test_remaining_columns_stay_comma_separated_and_valid(self):
        sql = ("CREATE TABLE t (\n    a INT,\n    PRIMARY KEY (a),\n    b INT\n)")
        out, _ = _run(sql, "rule_constraints")
        self.assertIn("a INT", out)
        self.assertIn("b INT", out)
        self.assertNotIn(",,", out.replace(" ", "").replace("\n", ""))
        self.assertNotIn("(,", out.replace(" ", "").replace("\n", ""))
        self.assertNotIn(",)", out.replace(" ", "").replace("\n", ""))

    def test_inline_primary_key_is_removed(self):
        sql = "CREATE TABLE t (\n    id BIGINT NOT NULL PRIMARY KEY\n)"
        out, findings = _run(sql, "rule_constraints")
        self.assertIn("id BIGINT NOT NULL", out)
        self.assertNotIn("PRIMARY KEY", out)
        self.assertEqual(findings[0].severity, "flag")

    def test_identity_is_removed_and_flagged(self):
        sql = "CREATE TABLE t (\n    id BIGINT IDENTITY(1,1) NOT NULL\n)"
        out, findings = _run(sql, "rule_constraints")
        self.assertNotIn("IDENTITY", out)
        self.assertIn("id BIGINT", out)
        self.assertIn("NOT NULL", out)
        self.assertTrue(any("IDENTITY" in f.detail for f in findings))

    def test_default_is_kept_but_flagged(self):
        sql = "CREATE TABLE t (\n    a INT DEFAULT 0\n)"
        out, findings = _run(sql, "rule_constraints")
        self.assertIn("DEFAULT 0", out)
        self.assertEqual(findings[0].severity, "flag")

    def test_not_null_is_kept_silently(self):
        sql = "CREATE TABLE t (\n    a INT NOT NULL\n)"
        self.assertEqual(_run(sql, "rule_constraints"), (sql, []))

    def test_plain_table_is_untouched(self):
        sql = "CREATE TABLE t (\n    a INT,\n    b STRING\n)"
        self.assertEqual(_run(sql, "rule_constraints"), (sql, []))

    def test_non_create_table_sql_is_untouched(self):
        sql = "SELECT a FROM t WHERE b = 1"
        self.assertEqual(_run(sql, "rule_constraints"), (sql, []))

    def test_every_removal_says_it_was_removed(self):
        sql = "CREATE TABLE t (\n    id BIGINT IDENTITY(1,1),\n    PRIMARY KEY (id)\n)"
        _, findings = _run(sql, "rule_constraints")
        self.assertTrue(all("removed" in f.detail.lower() for f in findings))


class ConvertTests(unittest.TestCase):
    def test_two_argument_convert_becomes_cast_with_operands_swapped(self):
        out, findings = _run("SELECT CONVERT(INT, a) FROM t", "rule_convert")
        self.assertEqual(out, "SELECT CAST(a AS INT) FROM t")
        self.assertIn("operand", findings[0].detail.lower())

    def test_convert_with_a_style_code_is_flagged(self):
        sql = "SELECT CONVERT(VARCHAR, d, 120) FROM t"
        out, findings = _run(sql, "rule_convert")
        self.assertEqual(out, sql)
        self.assertEqual(findings[0].severity, "flag")
        self.assertIn("style", findings[0].detail.lower())

    def test_nested_expression_is_preserved(self):
        out, _ = _run("SELECT CONVERT(INT, MAX(a))", "rule_convert")
        self.assertEqual(out, "SELECT CAST(MAX(a) AS INT)")


class DateaddTests(unittest.TestCase):
    def test_day_adds_an_interval_that_keeps_the_type(self):
        # date_add() returns DATE, so a datetime lost its time of day and
        # `>= DATEADD(day, -7, GETDATE())` started at midnight a week ago.
        out, findings = _run("SELECT DATEADD(day, 7, d) FROM t", "rule_dateadd")
        self.assertEqual(out, "SELECT (d + make_interval(0, 0, 0, 7)) FROM t")
        self.assertEqual(findings[0].severity, "rewrite")

    def test_abbreviations(self):
        for part in ("dd", "d", "DAY"):
            with self.subTest(part=part):
                out, _ = _run(f"SELECT DATEADD({part}, 1, d)", "rule_dateadd")
                self.assertEqual(out, "SELECT (d + make_interval(0, 0, 0, 1))")

    def test_a_string_literal_date_becomes_a_timestamp(self):
        # T-SQL types a string-literal date argument as datetime.
        out, _ = _run("SELECT DATEADD(day, 1, '2024-03-10 15:30')", "rule_dateadd")
        self.assertEqual(out, "SELECT timestampadd(DAY, 1, '2024-03-10 15:30')")
        for literal in ("('2024-03-10 15:30')", "n'2024-03-10 15:30'"):
            with self.subTest(literal=literal):
                out, _ = _run(f"SELECT DATEADD(day, 1, {literal})", "rule_dateadd")
                self.assertTrue(out.startswith("SELECT timestampadd(DAY, 1, "), out)

    def test_month_is_flagged(self):
        sql = "SELECT DATEADD(month, 1, d)"
        out, findings = _run(sql, "rule_dateadd")
        self.assertEqual(out, sql)
        self.assertEqual(findings[0].severity, "flag")

    def test_negative_offset(self):
        out, _ = _run("SELECT DATEADD(day, -7, d)", "rule_dateadd")
        self.assertEqual(out, "SELECT (d + make_interval(0, 0, 0, -7))")


class IifTests(unittest.TestCase):
    def test_iif_becomes_if(self):
        out, findings = _run("SELECT IIF(a > 1, 'y', 'n')", "rule_iif")
        self.assertEqual(out, "SELECT if(a > 1, 'y', 'n')")
        self.assertEqual(findings[0].severity, "rewrite")

    def test_wrong_arity_is_flagged(self):
        sql = "SELECT IIF(a, b)"
        out, findings = _run(sql, "rule_iif")
        self.assertEqual(out, sql)
        self.assertEqual(findings[0].severity, "flag")


class FlagOnlyTests(unittest.TestCase):
    def _flags(self, sql):
        _, findings = _run(sql, "rule_flag_only")
        return [f.rule for f in findings]

    def test_string_agg_is_flagged(self):
        self.assertIn("SQ80_STRING_AGG", self._flags("SELECT STRING_AGG(a, ',')"))

    def test_merge_is_flagged(self):
        self.assertIn("SQ80_MERGE", self._flags("MERGE t USING s ON t.id = s.id"))

    def test_output_clause_is_flagged(self):
        self.assertIn("SQ80_OUTPUT", self._flags("DELETE t OUTPUT deleted.id"))

    def test_division_is_flagged(self):
        # T-SQL 7 / 2 = 3; Spark 3.5. It used to pass through as PASS.
        self.assertIn("SQ80_DIVISION", self._flags("SELECT SUM(qty) / COUNT(*) FROM t"))

    def test_division_inside_comments_and_strings_is_not(self):
        self.assertNotIn("SQ80_DIVISION",
                         self._flags("SELECT 'a/b' AS p /* x */ -- y/z\nFROM t"))

    def test_a_slash_or_avg_inside_a_quoted_identifier_is_not_flagged(self):
        # [Sales/Units] is a column name; bracket rewriting turns it into a
        # backtick identifier before this rule looks.
        from fabric_aidp.translate import tsql_to_spark_sql as t
        rules = [f.rule for f in t.translate("SELECT [Sales/Units], [avg(x)] FROM t").findings]
        self.assertNotIn("SQ80_DIVISION", rules)
        self.assertNotIn("SQ80_AVG", rules)

    def test_avg_is_flagged(self):
        # T-SQL AVG(int) returns int; Spark avg returns double.
        self.assertIn("SQ80_AVG", self._flags("SELECT AVG(qty) FROM t"))

    def test_nolock_hint_is_flagged(self):
        self.assertIn("SQ80_HINT", self._flags("SELECT a FROM t WITH (NOLOCK)"))

    def test_option_clause_is_flagged(self):
        self.assertIn("SQ80_HINT", self._flags("SELECT a FROM t OPTION (RECOMPILE)"))

    def test_datepart_is_flagged(self):
        self.assertIn("SQ80_DATEPART", self._flags("SELECT DATEPART(yy, d)"))

    def test_stuff_and_isnumeric_are_flagged(self):
        self.assertIn("SQ80_STUFF", self._flags("SELECT STUFF(a, 1, 2, 'x')"))
        self.assertIn("SQ80_ISNUMERIC", self._flags("SELECT ISNUMERIC(a)"))

    def test_clean_sql_produces_no_flags(self):
        self.assertEqual(self._flags("SELECT a, b FROM t WHERE c = 1"), [])

    def test_keyword_inside_a_literal_is_not_flagged(self):
        self.assertEqual(self._flags("SELECT 'MERGE' AS s"), [])

    def test_each_construct_is_flagged_once(self):
        self.assertEqual(self._flags("SELECT STRING_AGG(a,','), STRING_AGG(b,',')"),
                         ["SQ80_STRING_AGG"])


class GoBatchTests(unittest.TestCase):
    """SQ02. Ported back from the sibling migrator after a real tenant run."""

    def test_go_separator_is_removed(self):
        out, findings = _run("CREATE VIEW v AS SELECT 1;\nGO\nSELECT * FROM v;\nGO",
                             "rule_go_batch")
        self.assertNotIn("GO", out)
        self.assertIn("CREATE VIEW", out)
        self.assertEqual([f.rule for f in findings], ["SQ02_GO_BATCH"])

    def test_the_word_go_inside_an_identifier_is_left_alone(self):
        sql = "SELECT [go_live_date], [category] FROM dbo.[GO_LOG];"
        out, findings = _run(sql, "rule_go_batch")
        self.assertEqual(out, sql)
        self.assertEqual(findings, [])

    def test_go_inside_a_string_literal_is_left_alone(self):
        sql = "SELECT 'GO' AS x;"
        out, findings = _run(sql, "rule_go_batch")
        self.assertEqual(out, sql)
        self.assertEqual(findings, [])

    def test_go_must_be_alone_on_its_line(self):
        sql = "SELECT 1; GO SELECT 2;"
        out, _ = _run(sql, "rule_go_batch")
        self.assertEqual(out, sql)


class SchemabindingTests(unittest.TestCase):
    """SQ74. Ported back from the sibling migrator."""

    def test_schemabinding_is_stripped_and_the_loss_is_flagged(self):
        out, findings = _run("CREATE VIEW [v]\nWITH SCHEMABINDING\nAS SELECT 1 AS x;",
                             "rule_schemabinding")
        self.assertNotIn("SCHEMABINDING", out.upper())
        self.assertEqual([(f.rule, f.severity) for f in findings],
                         [("SQ74_SCHEMABINDING", "flag")])

    def test_view_metadata_variant_is_also_stripped(self):
        out, _ = _run("CREATE VIEW v WITH SCHEMABINDING, VIEW_METADATA AS SELECT 1;",
                      "rule_schemabinding")
        self.assertNotIn("SCHEMABINDING", out.upper())

    def test_a_view_without_the_clause_is_untouched(self):
        sql = "CREATE VIEW v AS SELECT 1;"
        out, findings = _run(sql, "rule_schemabinding")
        self.assertEqual(out, sql)
        self.assertEqual(findings, [])


class AlterTableAddConstraintTests(unittest.TestCase):
    """SQ75. The worst of the three: this one emitted invalid SQL silently.

    SQ70 only looks inside a `CREATE TABLE (...)` body, so Fabric's separate
    `ALTER TABLE ... ADD CONSTRAINT` statements reached no rule -- then SQ73
    stripped the constraint name and SQ70's closing document-wide
    `_INLINE_PK_RE.sub` stripped "primary key", leaving wreckage that no
    engine accepts and no finding to say so.
    """

    CLAUSES = [
        "CONSTRAINT PK_t primary key NONCLUSTERED ([a])",
        "CONSTRAINT FK_t FOREIGN KEY ([a]) REFERENCES [u]([b])",
        "CONSTRAINT UQ_t UNIQUE ([a])",
        "PRIMARY KEY ([a])",
    ]

    def test_the_whole_statement_goes_and_says_so(self):
        for clause in self.CLAUSES:
            with self.subTest(clause=clause):
                out, findings = _run(f"ALTER TABLE [dbo].[t] ADD {clause};",
                                     "rule_alter_add_constraint")
                self.assertEqual(out.strip(), "")
                self.assertEqual([f.rule for f in findings],
                                 ["SQ75_ALTER_ADD_CONSTRAINT"])

    def test_the_finding_is_a_flag_not_a_silent_rewrite(self):
        _, findings = _run("ALTER TABLE [t] ADD PRIMARY KEY ([a]);",
                           "rule_alter_add_constraint")
        self.assertEqual([f.severity for f in findings], ["flag"])

    def test_nothing_half_stripped_is_left_behind_end_to_end(self):
        """The exact wreckage this rule exists to prevent."""
        result = tsql.translate(
            "CREATE TABLE [t] (a INT);\nGO\n"
            "ALTER TABLE [dbo].[t] ADD CONSTRAINT PK_t "
            "primary key NONCLUSTERED ([a]);", kind="table")
        out = result.translated_sql.upper()
        self.assertNotIn("NONCLUSTERED", out)
        self.assertNotIn("ADD CONSTRAINT", out)
        self.assertIn("CREATE TABLE", out)
        self.assertTrue(any(f.rule == "SQ75_ALTER_ADD_CONSTRAINT"
                            for f in result.findings))

    def test_adding_a_column_is_untouched(self):
        """Spark supports ALTER TABLE ADD COLUMN; only constraints go."""
        for stmt in ("ALTER TABLE [t] ADD [c] INT;",
                     "ALTER TABLE [t] ADD [c] INT, [d] VARCHAR(10);"):
            with self.subTest(stmt=stmt):
                out, findings = _run(stmt, "rule_alter_add_constraint")
                self.assertEqual(out, stmt)
                self.assertEqual(findings, [])


class ScopedRuleRegressionTests(unittest.TestCase):
    """Nine defects the sibling project found scoping these rules, plus one
    found here. Every one produced output that looks migrated and is wrong --
    either invalid SQL, or valid SQL with different results.

    The common cause in most of them is a document-wide regex substitution
    reaching text it never examined.
    """

    def _t(self, sql, kind="table"):
        return tsql.translate(sql, kind=kind)

    # --- LIMIT placement -------------------------------------------------
    def test_limit_lands_on_the_statement_that_had_the_top(self):
        out = self._t("SELECT TOP 5 a FROM t1;\nSELECT b FROM t2;").translated_sql
        first, second = [ln for ln in out.splitlines() if ln.strip()][:2]
        self.assertIn("LIMIT 5", first)
        self.assertNotIn("LIMIT", second)

    def test_top_over_a_set_operation_is_flagged_not_capped(self):
        result = self._t("SELECT TOP 5 a FROM t1 UNION SELECT a FROM t2;")
        self.assertIn("TOP 5", result.translated_sql)
        self.assertNotIn("LIMIT", result.translated_sql)
        self.assertIn("SQ50_TOP_SET_OPERATION", [f.rule for f in result.findings])

    def test_a_single_statement_still_gets_its_limit(self):
        self.assertIn("LIMIT 3", self._t("SELECT TOP 3 a FROM t;").translated_sql)

    # --- edits must stay inside the CREATE TABLE body --------------------
    def test_is_unique_inside_a_literal_survives(self):
        out = self._t("CREATE TABLE t (a INT); "
                      "SELECT 'this is unique' AS n FROM t;").translated_sql
        self.assertIn("'this is unique'", out)

    def test_a_column_called_identity_no_keeps_its_name(self):
        out = self._t("CREATE TABLE t (id INT IDENTITY(1,1), identity_no INT);").translated_sql
        self.assertIn("identity_no", out)
        self.assertNotIn("IDENTITY(1,1)", out)

    def test_a_later_statement_is_not_edited_by_the_table_rule(self):
        out = self._t("CREATE TABLE t (a INT IDENTITY(1,1)); "
                      "SELECT b FROM u WHERE c = 1;").translated_sql
        self.assertIn("SELECT b FROM u WHERE c = 1;", out)

    # --- nullability and defaults ---------------------------------------
    def test_default_null_keeps_its_null(self):
        out = self._t("CREATE TABLE t (mgr INT DEFAULT NULL);").translated_sql
        self.assertNotIn("DEFAULT,", out)
        self.assertNotIn("DEFAULT)", out)

    # --- constraint names -------------------------------------------------
    def test_quoted_table_level_constraint_name_is_recognised(self):
        out = self._t("CREATE TABLE t (a INT, CONSTRAINT [my pk] PRIMARY KEY (a));").translated_sql
        self.assertNotIn("CONSTRAINT", out.upper())

    def test_orphaned_inline_constraint_name_is_removed(self):
        """Found here, not ported: rule_constraints cuts PRIMARY KEY first, so
        SQ73's keyword lookahead can no longer match and the name is stranded
        against the closing paren -- invalid in every engine."""
        for sql in ("CREATE TABLE t (a INT CONSTRAINT [my pk] PRIMARY KEY);",
                    "CREATE TABLE t (a INT CONSTRAINT [my pk] PRIMARY KEY, b INT);"):
            with self.subTest(sql=sql):
                out = self._t(sql).translated_sql
                self.assertNotIn("CONSTRAINT", out.upper())
                self.assertNotIn("`my pk`", out)

    def test_a_named_default_still_keeps_its_clause(self):
        out = self._t("CREATE TABLE t (a INT CONSTRAINT df_a DEFAULT 0);").translated_sql
        self.assertIn("DEFAULT 0", out)
        self.assertNotIn("CONSTRAINT", out.upper())

    def test_a_column_whose_name_starts_with_constraint_is_untouched(self):
        out = self._t("CREATE TABLE t (constraint_id INT, b INT);").translated_sql
        self.assertIn("constraint_id", out)

    # --- ALTER TABLE ADD DEFAULT -----------------------------------------
    def test_alter_table_add_default_goes_whole(self):
        result = self._t("ALTER TABLE [t] ADD CONSTRAINT [DF_a] DEFAULT (0) FOR [a];")
        self.assertEqual(result.translated_sql.strip(), "")
        self.assertIn("SQ75_ALTER_ADD_CONSTRAINT", [f.rule for f in result.findings])

    # --- literals in SELECT INTO -----------------------------------------
    def test_select_into_keeps_whitespace_inside_a_literal(self):
        out = self._t("SELECT 'a   b' AS x INTO newt FROM t;", kind="other").translated_sql
        self.assertIn("'a   b'", out)
        self.assertIn("CREATE TABLE newt AS", out)

    # --- types -------------------------------------------------------------
    def test_time_column_is_flagged(self):
        result = self._t("CREATE TABLE t (a TIME);")
        self.assertTrue(any("TIME" in f.detail.upper() for f in result.findings))


class TwoPartNameFromInsideACallTests(unittest.TestCase):
    """T-SQL spells one of its functions `TRIM(chars FROM string)`.

    The two-part rule anchors on the keyword FROM, so it read that FROM as a
    table source and rewrote the *column* after it: `d.name` became
    `default.AcmeDW_d.name`, a reference to a schema that does not exist,
    emitted as a `rewrite` with no flag -- so it graded PASS. Depth alone
    cannot fix it, because a FROM inside parentheses is legitimate in a
    subquery; the discriminator is whether the enclosing paren belongs to a
    function call.
    """

    def _run(self, sql, item="AcmeDW"):
        findings = []
        return tsql.rule_two_part_names(sql, findings, item=item), findings

    def test_trim_does_not_make_its_column_a_table(self):
        out, findings = self._run(
            "SELECT TRIM(BOTH ' ' FROM d.name) FROM dbo.t AS d")
        self.assertIn("TRIM(BOTH ' ' FROM d.name)", out)
        self.assertIn("default.AcmeDW.t", out)
        self.assertEqual(len(findings), 1)

    def test_a_from_inside_a_subquery_is_still_a_table(self):
        """The paren is not a call -- it is a derived table, and the name
        after its FROM is real."""
        out, _ = self._run("SELECT * FROM (SELECT x FROM dbo.t) AS a")
        self.assertEqual(out, "SELECT * FROM (SELECT x FROM default.AcmeDW.t) AS a")

    def test_a_from_inside_a_call_inside_a_subquery_is_still_not_a_table(self):
        out, _ = self._run(
            "SELECT * FROM (SELECT TRIM(' ' FROM q.c) FROM dbo.t AS q) AS a")
        self.assertIn("TRIM(' ' FROM q.c)", out)
        self.assertIn("default.AcmeDW.t", out)

    def test_a_plain_two_part_name_is_still_qualified(self):
        out, _ = self._run("SELECT * FROM dbo.t")
        self.assertEqual(out, "SELECT * FROM default.AcmeDW.t")

    def test_a_join_inside_a_subquery_still_works(self):
        out, _ = self._run("SELECT * FROM (SELECT 1) a JOIN dbo.u ON 1=1")
        self.assertIn("JOIN default.AcmeDW.u", out)

    def test_the_whole_thing_through_translate(self):
        result = tsql.translate(
            "SELECT TRIM(BOTH ' ' FROM d.name) FROM dbo.t AS d",
            kind="view", item="AcmeDW")
        self.assertIn("TRIM(BOTH ' ' FROM d.name)", result.translated_sql)
        self.assertNotIn("AcmeDW_d", result.translated_sql)


class DeltaColumnMappingTests(unittest.TestCase):
    """AIDP creates Delta tables, and Delta refuses a column name holding
    ` ,;{}()=`, tab or newline unless the table maps columns by name.

    Measured on a live workspace: the Acme `claim` table, whose first column
    is `[claim id]`, was translated to valid Spark DDL, graded OK, and failed
    to create with DELTA_INVALID_CHARACTERS_IN_COLUMN_NAMES; the view and the
    notebooks reading it failed after it. With the properties this rule adds
    the table created, took inserts, and served the view."""

    MAPPING = "TBLPROPERTIES ('delta.columnMapping.mode' = 'name'"

    def _translate(self, sql):
        result = tsql.translate(sql, kind="table", item="AcmeDW")
        return result.translated_sql, [f for f in result.findings
                                       if f.rule.startswith("SQ77")]

    def test_a_spaced_column_name_gets_column_mapping(self):
        out, found = self._translate(
            "CREATE TABLE [dbo].[claim] (\n    [claim id] BIGINT NOT NULL,\n"
            "    policy_no NVARCHAR(50) NOT NULL\n)")
        self.assertIn("`claim id` BIGINT NOT NULL", out)
        self.assertTrue(out.rstrip().endswith(
            ") TBLPROPERTIES ('delta.columnMapping.mode' = 'name', "
            "'delta.minReaderVersion' = '2', 'delta.minWriterVersion' = '5')"), out)
        self.assertEqual([(f.rule, f.severity) for f in found],
                         [("SQ77_DELTA_COLUMN_MAPPING", "rewrite")])
        self.assertIn("`claim id`", found[0].detail)

    def test_every_character_delta_names_is_caught(self):
        for name in ("a b", "a,b", "a;b", "a{b", "a}b", "a(b", "a)b", "a=b"):
            with self.subTest(name=name):
                out, _ = self._translate(f"CREATE TABLE dbo.t ([{name}] INT, ok INT)")
                self.assertIn(self.MAPPING, out)

    def test_names_delta_accepts_are_left_alone(self):
        out, found = self._translate(
            "CREATE TABLE dbo.t ([sales_eur] INT, [Sales$] INT, [2024] INT, id INT)")
        self.assertNotIn("TBLPROPERTIES", out)
        self.assertEqual(found, [])

    def test_each_statement_is_judged_on_its_own_columns(self):
        out, _ = self._translate("CREATE TABLE dbo.a ([x y] INT);\nCREATE TABLE dbo.b (z INT);")
        first, second = out.split(";\n")
        self.assertIn(self.MAPPING, first)
        self.assertNotIn("TBLPROPERTIES", second)

    def test_a_removed_table_constraint_does_not_hide_the_column(self):
        out, _ = self._translate(
            "CREATE TABLE dbo.t ([claim id] BIGINT NOT NULL, "
            "CONSTRAINT pk PRIMARY KEY NONCLUSTERED ([claim id]) NOT ENFORCED)")
        self.assertIn(self.MAPPING, out)

    def test_select_into_puts_the_properties_before_as(self):
        out, found = self._translate("SELECT [claim id] INTO dbo.t2 FROM dbo.claim")
        self.assertRegex(out, r"^CREATE TABLE default\.AcmeDW\.t2 TBLPROPERTIES "
                              r"\(.*\) AS SELECT `claim id` FROM default\.AcmeDW\.claim$")
        self.assertEqual(found[0].rule, "SQ77_DELTA_COLUMN_MAPPING")

    def test_a_spaced_table_name_in_a_ctas_is_not_a_column(self):
        out, found = self._translate("SELECT a INTO dbo.t3 FROM [Sales Lake].dbo.x")
        self.assertNotIn("TBLPROPERTIES", out)
        self.assertEqual(found, [])

    def test_an_explicit_using_or_tblproperties_is_flagged_not_merged(self):
        for tail in ("USING PARQUET", "TBLPROPERTIES ('a' = 'b')"):
            with self.subTest(tail=tail):
                sql = f"CREATE TABLE dbo.t ([x y] INT) {tail}"
                out, found = self._translate(sql)
                self.assertEqual(out.count("TBLPROPERTIES"), tail.count("TBLPROPERTIES"))
                self.assertEqual([(f.rule, f.severity) for f in found],
                                 [("SQ77_DELTA_COLUMN_NAME", "flag")])


class DeltaColumnMappingScanBoundaryTests(unittest.TestCase):
    """What in a CTAS is a column of the table being created, and what is not.

    The rule used to read every quoted name in the statement. That is wrong in
    both directions, and both were measured on the merged tree before this:

      SELECT a INTO dbo.t FROM [my table]
        -> ... TBLPROPERTIES (...) AS SELECT a FROM `my table`
           SQ77_DELTA_COLUMN_MAPPING "column name(s) `my table`"

    `my table` is a table. The created table's only column is `a`, it needed
    no mapping, and it left with reader 2 / writer 5 anyway.

      SELECT c.[claim id] INTO dbo.t FROM dbo.claim AS c
        -> CREATE TABLE default.W.t AS SELECT c.`claim id` FROM ...
           no SQ77 finding at all

    The same over-broad filter skipped every name next to a `.`, so the one
    construct this rule exists for -- an output column called `claim id` --
    went out unmapped and would have failed to create with
    DELTA_INVALID_CHARACTERS_IN_COLUMN_NAMES.
    """

    MAPPING = "TBLPROPERTIES ('delta.columnMapping.mode' = 'name'"

    def _translate(self, sql):
        result = tsql.translate(sql, kind="table", item="W")
        return result.translated_sql, [f for f in result.findings
                                       if f.rule.startswith("SQ77")]

    def test_a_one_part_quoted_table_name_is_not_a_column(self):
        """Her own test covers `dbo.[my table]`; the one-part name is the
        gap, and it is the shape a Warehouse export actually holds."""
        out, found = self._translate("SELECT a INTO dbo.t FROM [my table]")
        self.assertEqual(out, "CREATE TABLE default.W.t AS SELECT a FROM `my table`")
        self.assertEqual(found, [])

    def test_a_qualified_output_column_is_still_a_column(self):
        out, found = self._translate(
            "SELECT c.[claim id] INTO dbo.t FROM dbo.claim AS c")
        self.assertIn(self.MAPPING, out)
        self.assertEqual([(f.rule, f.severity) for f in found],
                         [("SQ77_DELTA_COLUMN_MAPPING", "rewrite")])
        self.assertIn("`claim id`", found[0].detail)
        self.assertNotIn("`c`", found[0].detail)

    def test_a_spaced_qualifier_names_only_the_column_after_it(self):
        _out, found = self._translate(
            "SELECT [my db].[claim id] INTO dbo.t FROM dbo.claim")
        self.assertIn("`claim id`", found[0].detail)
        self.assertNotIn("my db", found[0].detail)

    def test_a_name_outside_the_select_list_is_not_a_column(self):
        for clause in ("WHERE [odd col] = 1",
                       "FROM (SELECT [inner col] FROM [odd tbl]) z"):
            with self.subTest(clause=clause):
                sql = ("SELECT a INTO dbo.t FROM dbo.x " + clause
                       if clause.startswith("WHERE")
                       else "SELECT a INTO dbo.t " + clause)
                out, found = self._translate(sql)
                self.assertNotIn("TBLPROPERTIES", out)
                self.assertEqual(found, [])

    def test_a_cte_contributes_its_final_select_and_not_its_body(self):
        _out, found = self._translate(
            "CREATE TABLE dbo.t AS WITH c AS (SELECT [a b] FROM dbo.x) "
            "SELECT [claim id] FROM c")
        self.assertIn("`claim id`", found[0].detail)
        self.assertNotIn("a b", found[0].detail)

    def test_a_parenthesised_ctas_body_is_still_read(self):
        out, found = self._translate(
            "CREATE TABLE dbo.t AS (SELECT [claim id] FROM dbo.x)")
        self.assertIn(self.MAPPING, out)
        self.assertEqual(found[0].rule, "SQ77_DELTA_COLUMN_MAPPING")

    def test_a_select_list_with_no_from_is_read(self):
        out, _found = self._translate("CREATE TABLE dbo.t AS SELECT 1 AS [claim id]")
        self.assertIn(self.MAPPING, out)

    def test_a_star_asks_for_nothing(self):
        """Not a silent miss to fix here: a `*` names columns of a table this
        translator cannot see, which is what a star has always meant to it.
        Pinned so that narrowing the scan is not read as having handled it."""
        out, found = self._translate("SELECT * INTO dbo.t FROM [my table]")
        self.assertNotIn("TBLPROPERTIES", out)
        self.assertEqual(found, [])


class DeltaColumnMappingTableClauseTests(unittest.TestCase):
    """`USING`/`TBLPROPERTIES` is a table clause only where a table clause goes.

    Measured on the merged tree before this, with the search running across
    the whole statement:

      SELECT [claim id] INTO dbo.t FROM dbo.a JOIN dbo.b USING (k)
        -> SQ77_DELTA_COLUMN_NAME (flag) "... this table already declares
           USING or TBLPROPERTIES"

    It declares neither. The `USING` is the join's, the reason given is
    false, and the table that needed mapping did not get it.
    """

    MAPPING = "TBLPROPERTIES ('delta.columnMapping.mode' = 'name'"

    def _translate(self, sql):
        result = tsql.translate(sql, kind="table", item="W")
        return result.translated_sql, [f for f in result.findings
                                       if f.rule.startswith("SQ77")]

    def test_a_joins_using_is_not_a_table_clause(self):
        out, found = self._translate(
            "SELECT [claim id] INTO dbo.t FROM dbo.a JOIN dbo.b USING (k)")
        self.assertIn(self.MAPPING, out)
        self.assertEqual([(f.rule, f.severity) for f in found],
                         [("SQ77_DELTA_COLUMN_MAPPING", "rewrite")])

    def test_a_ctas_that_really_declares_using_is_still_flagged(self):
        """The clause a CTAS can carry sits between the name and `AS`, which
        is the only place the anchored search now looks."""
        out, found = self._translate(
            "CREATE TABLE dbo.t USING delta AS SELECT [claim id] FROM dbo.claim")
        self.assertNotIn("delta.columnMapping.mode", out)
        self.assertEqual([(f.rule, f.severity) for f in found],
                         [("SQ77_DELTA_COLUMN_NAME", "flag")])

    def test_a_column_list_table_clause_is_still_flagged(self):
        """The anchored search for the other branch starts past the `)`, so a
        `USING` inside the column list -- a column named `[using]` -- is not
        one either."""
        out, found = self._translate("CREATE TABLE dbo.t ([x y] INT, [using] INT)")
        self.assertIn(self.MAPPING, out)
        self.assertEqual([(f.rule, f.severity) for f in found],
                         [("SQ77_DELTA_COLUMN_MAPPING", "rewrite")])


class DeltaColumnMappingRuleIdTests(unittest.TestCase):
    """SQ76 was taken while this branch was out.

    `rule_alter_add_column` landed on main as SQ76_ALTER_ADD_COLUMN /
    SQ76_ALTER_ADD_COMPUTED. The merge is clean textually -- the two rules
    never touch the same lines -- and silently gives one number to two
    unrelated rules. Measured on the merged tree before the renumber, one
    document reported SQ76_ALTER_ADD_COLUMN and SQ76_DELTA_COLUMN_MAPPING
    side by side.
    """

    def test_the_two_rules_that_share_a_document_do_not_share_a_number(self):
        result = tsql.translate(
            "ALTER TABLE dbo.t ADD c money;\nCREATE TABLE dbo.u ([claim id] INT)",
            kind="table", item="W")
        fired = [f.rule for f in result.findings]
        self.assertIn("SQ76_ALTER_ADD_COLUMN", fired)
        self.assertIn("SQ77_DELTA_COLUMN_MAPPING", fired)

    def test_no_delta_finding_is_still_numbered_sq76(self):
        result = tsql.translate("CREATE TABLE dbo.t ([claim id] INT)",
                                kind="table", item="W")
        for finding in result.findings:
            self.assertNotIn("SQ76_DELTA", finding.rule)

    def test_every_sq_number_belongs_to_exactly_one_rule(self):
        """The invariant, read off the source rather than off one document.

        A rule number is a family: SQ70 is the constraint rules, SQ11 the
        name rules. Two functions emitting the same number is not a style
        problem -- the README coverage figure, the fixture tables and every
        triage conversation key on the number, and a reader who greps SQ76
        for "the ALTER ADD rule" finds Delta column mapping as well.

        This is the guard that was missing when the merge that needed it
        happened: the two rules never touch the same lines, so git had
        nothing to report.
        """
        import ast
        import inspect

        tree = ast.parse(inspect.getsource(tsql))
        owners = {}
        for node in tree.body:
            if not isinstance(node, ast.FunctionDef):
                continue
            if not node.name.startswith("rule_"):
                continue
            for child in ast.walk(node):
                if not isinstance(child, ast.Constant):
                    continue
                if not isinstance(child.value, str):
                    continue
                if not re.fullmatch(r"SQ\d\d_[A-Z0-9_]+", child.value):
                    continue
                owners.setdefault(child.value[:4], set()).add(node.name)

        self.assertIn("SQ77", owners, "SQ77 ids are not emitted by any rule")
        shared = {number: sorted(names)
                  for number, names in owners.items() if len(names) > 1}
        # Pinned rather than asserted empty, because three numbers are shared
        # on purpose and each is one concern split over several functions:
        #   SQ11 object-name resolution, at all three name arities. The
        #        one-part function is the newest and is the odd one of the
        #        three: it only ever flags, because a bare name's schema is
        #        not in a DacFx export -- see `rule_one_part_names`.
        #   SQ14 session settings -- SQ10's rule is where QUOTED_IDENTIFIER
        #        OFF is detected, and it raises the SQ14 finding from there;
        #   SQ51 SELECT ... INTO and the temp tables it turns up next to;
        #   SQ52 T-SQL's optional INTO written in, on INSERT and on MERGE --
        #        one concern, two statements that have it.
        # SQ76 held two: `rule_alter_add_column`'s ADD COLUMNS rewrite and
        # this branch's Delta mapping, which have nothing to do with each
        # other. Adding an entry here is the thing to argue about.
        self.assertEqual(
            shared,
            {"SQ11": ["rule_one_part_names", "rule_three_part_names",
                      "rule_two_part_names"],
             "SQ14": ["rule_bracket_identifiers", "rule_session_settings"],
             "SQ51": ["rule_select_into", "rule_temp_table"],
             "SQ52": ["rule_insert_without_into",
                      "rule_merge_without_into"]},
            "the set of rule numbers emitted by more than one rule function "
            "changed; a new entry means two concerns share a number")


if __name__ == "__main__":
    unittest.main()
