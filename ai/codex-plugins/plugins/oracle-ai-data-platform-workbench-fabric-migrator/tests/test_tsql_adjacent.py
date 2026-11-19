"""Five T-SQL defects filed by the #44 sweep and left out of it.

Every expectation here was measured on 2a223bd, before its fix, and the
measured "before" is quoted next to the case it belongs to. Spark verdicts
were taken on pyspark 3.5.0 -- the version the cluster runs -- with
JAVA_HOME=openjdk@21, master local[1], session defaults
(spark.sql.ansi.enabled false); a statement is reported PARSED when it gets
past the parser and fails on TABLE_OR_VIEW_NOT_FOUND for the probe's empty
catalog.

  F1  SQ15 read the three-part target of `SET IDENTITY_INSERT` as a
      qualified *column* and dropped the database from it, so the statement
      named a different object -- silently, beside a SQ14 flag whose own
      text says the name was left as written
  F2  SQ50 appended a trailing `LIMIT n` for a `TOP (n)` on MERGE, UPDATE
      and DELETE, and Spark takes no LIMIT on any of the three: the rewrite
      turned a statement that parsed into PARSE_SYNTAX_ERROR. Filed as a
      MERGE defect; UPDATE and DELETE were measured here and have it too
  F3  SQ80_MERGE matched the word `MERGE` and not the statement, so five
      shapes that are not MERGE statements -- all of them a column called
      `merge` -- pushed an otherwise clean object to REVIEW
  F4  a MERGE's `USING` source reached no object position at all, so the
      target was qualified and the source was not -- and a *three-part*
      source was read as a column and lost its database, which is F1's
      defect in a second place and was not in the filing
"""
import unittest

from fabric_aidp.translate import tsql_to_spark_sql as tsql


def _t(sql, **kw):
    kw.setdefault("kind", "view")
    kw.setdefault("item", "AcmeDW")
    return tsql.translate(sql, **kw)


def _rules(result):
    return [f.rule for f in result.findings]


class IdentityInsertTargetIsNotAColumnTests(unittest.TestCase):
    """F1. SQ15 rewrote `SET IDENTITY_INSERT mydb.dbo.t ON` to name `dbo.t`.

    MEASURED on 2a223bd, `translate(sql, kind="view", item="AcmeDW")`:

        SET IDENTITY_INSERT mydb.dbo.t ON
          -> SET IDENTITY_INSERT dbo.t ON
             ['SQ14_IDENTITY_INSERT', 'SQ15_QUALIFIED_COLUMN']

    A silent rewrite to a *different object*. The only other finding is
    SQ14's, which is about the statement being unsupported and whose text
    reads "left exactly as written, name included" -- so the artifact and
    the finding beside it contradicted each other.

    SQ15 is the rule that moved, and the evidence is in the two rules' own
    stated contracts. SQ14 says it leaves the name alone and says why: a
    rewritten table name inside a statement Spark rejects reads as
    translated work. SQ15 says `a.b.c` is `schema.table.column` "outside an
    object position", and derives "object position" from
    `_OBJECT_KEYWORDS`, whose own comment defines it as the positions in
    which a dotted name *cannot be anything else*. That is not a list of
    every object position, so "absent from it" never meant "a column" --
    and T-SQL's grammar for IDENTITY_INSERT takes `database.schema.table`
    and nothing else. Reading a name SQ14 owns was the error.

    The two- and one-part targets were already right on 2a223bd
    (`SET IDENTITY_INSERT dbo.t ON` and `SET IDENTITY_INSERT t ON` both came
    back unchanged with `['SQ14_IDENTITY_INSERT']`), which is why only the
    three-part branch had to change and why SQ14 needed no edit at all.

    On pyspark 3.5.0 the statement is rejected either way --
    `SET IDENTITY_INSERT dbo.t ON` is INVALID_SET_SYNTAX -- so no reading of
    the name makes it run. That is the point: the name is the only part of
    the line a human resolving the flag can still trust, and it was wrong.
    """

    def test_the_reproduction_keeps_all_three_parts(self):
        result = _t("SET IDENTITY_INSERT mydb.dbo.t ON")
        self.assertEqual(result.translated_sql,
                         "SET IDENTITY_INSERT mydb.dbo.t ON")
        self.assertEqual(_rules(result), ["SQ14_IDENTITY_INSERT"])

    def test_sq15_does_not_read_the_target_as_a_column(self):
        for state in ("ON", "OFF"):
            with self.subTest(state=state):
                result = _t("SET IDENTITY_INSERT mydb.dbo.t %s" % state)
                self.assertNotIn("SQ15_QUALIFIED_COLUMN", _rules(result))
                self.assertIn("mydb.dbo.t", result.translated_sql)

    def test_no_second_finding_is_raised_about_the_name(self):
        """SQ14's flag already quotes the target, so a second finding saying
        "and the name was left alone" would repeat it. The span is skipped,
        not re-reported."""
        result = _t("SET IDENTITY_INSERT mydb.dbo.t ON")
        self.assertEqual(_rules(result), ["SQ14_IDENTITY_INSERT"])
        self.assertIn("mydb.dbo.t",
                      next(f.detail for f in result.findings))

    def test_the_bracketed_spelling_keeps_its_database_too(self):
        """MEASURED on 2a223bd: `SET IDENTITY_INSERT [mydb].[dbo].[t] ON`
        came back ``SET IDENTITY_INSERT `dbo`.`t` ON`` -- the same loss,
        reached through SQ10's bracket conversion instead of the bare
        spelling. SQ10 still converts the brackets, which is right and is
        not what was wrong."""
        result = _t("SET IDENTITY_INSERT [mydb].[dbo].[t] ON")
        self.assertEqual(result.translated_sql,
                         "SET IDENTITY_INSERT `mydb`.`dbo`.`t` ON")
        self.assertNotIn("SQ15_QUALIFIED_COLUMN", _rules(result))

    def test_the_one_and_two_part_targets_are_still_left_as_written(self):
        """The behaviour that was already correct, pinned so the fix cannot
        have been a wider change than it claims."""
        for target in ("t", "dbo.t"):
            with self.subTest(target=target):
                result = _t("SET IDENTITY_INSERT %s ON" % target)
                self.assertEqual(result.translated_sql,
                                 "SET IDENTITY_INSERT %s ON" % target)
                self.assertEqual(_rules(result), ["SQ14_IDENTITY_INSERT"])

    def test_a_following_statement_is_still_qualified(self):
        """The skip is a span, not a document. The INSERT that the
        IDENTITY_INSERT exists to permit still gets its name."""
        result = _t("SET IDENTITY_INSERT mydb.dbo.t ON;\n"
                    "INSERT INTO dbo.t (id) VALUES (1)")
        self.assertIn("SET IDENTITY_INSERT mydb.dbo.t ON",
                      result.translated_sql)
        self.assertIn("INSERT INTO default.AcmeDW.t", result.translated_sql)
        self.assertEqual(_rules(result),
                         ["SQ14_IDENTITY_INSERT", "SQ11_TWO_PART_NAME"])

    def test_a_three_part_column_reference_elsewhere_is_still_a_column(self):
        """The other direction, so the skip cannot have switched SQ15 off:
        the reading SQ15 exists for still happens everywhere else."""
        result = _t("SELECT dbo.t.c FROM dbo.t")
        self.assertIn("SQ15_QUALIFIED_COLUMN", _rules(result))
        self.assertIn("t.c", result.translated_sql)

    def test_a_set_that_does_not_head_a_statement_claims_no_span(self):
        """`_identity_insert_targets` asks `_heads_a_statement`, the same
        test SQ14 makes. Where SQ14 does not claim the statement, SQ15 is
        not silenced by a word that merely looks like one."""
        result = _t("UPDATE dbo.t SET IDENTITY_INSERT = 1")
        self.assertNotIn("SQ14_IDENTITY_INSERT", _rules(result))

    def test_a_four_part_target_is_still_a_linked_server(self):
        """MEASURED on 2a223bd and unchanged: `SET IDENTITY_INSERT a.b.c.d
        ON` gets SQ11_LINKED_SERVER and no SQ14, because
        `_SET_IDENTITY_INSERT_RE` allows at most three parts and T-SQL
        permits no more there either. Recorded because it is the one
        IDENTITY_INSERT shape SQ14 does not claim, so this fix could not
        cover it and does not pretend to."""
        result = _t("SET IDENTITY_INSERT a.b.c.d ON")
        self.assertEqual(_rules(result), ["SQ11_LINKED_SERVER"])
        self.assertEqual(result.translated_sql, "SET IDENTITY_INSERT a.b.c.d ON")


class TopOnAStatementThatTakesNoLimitTests(unittest.TestCase):
    """F2. SQ50 appended `LIMIT n` to statements Spark accepts no LIMIT on.

    MEASURED on 2a223bd, `translate(sql, kind="view", item="AcmeDW")`:

        MERGE TOP (10) INTO dbo.t USING s ON 1=1
          -> MERGE INTO default.AcmeDW.t USING s ON 1=1 LIMIT 10
             ['SQ11_TWO_PART_NAME', 'SQ50_TOP', 'SQ80_MERGE']
        UPDATE TOP (10) dbo.t SET a = 1
          -> UPDATE dbo.t SET a = 1 LIMIT 10          ['SQ50_TOP']
        DELETE TOP (10) FROM dbo.t
          -> DELETE FROM default.AcmeDW.t LIMIT 10
             ['SQ11_TWO_PART_NAME', 'SQ50_TOP']

    All three reported SQ50_TOP as a `rewrite` -- a change the rule claims
    to have made correctly -- and all three do not parse. MEASURED on
    pyspark 3.5.0, JAVA_HOME=openjdk@21, master local[1], session defaults
    (spark.sql.ansi.enabled false), against a session with no tables, so
    TABLE_OR_VIEW_NOT_FOUND means the statement PARSED:

        MERGE INTO t USING s ON t.a=s.a WHEN MATCHED THEN UPDATE SET t.b=s.b
                                             PARSED
        ... the same + ` LIMIT 10`           PARSE_SYNTAX_ERROR
                                               "Syntax error at or near
                                               'LIMIT'" (line 1, pos 69)
        DELETE FROM t WHERE a=1              PARSED
        DELETE FROM t WHERE a=1 LIMIT 10     PARSE_SYNTAX_ERROR at 'LIMIT'
        UPDATE t SET b=1                     PARSED
        UPDATE t SET b=1 LIMIT 10            PARSE_SYNTAX_ERROR at 'LIMIT'

    LIMIT is a clause of a query and none of the three is a query. SELECT
    and INSERT are, and both accept it on the same session --
    `INSERT INTO t SELECT a FROM s LIMIT 10` and
    `INSERT INTO t VALUES (1) LIMIT 10` both PARSED -- so those keep the
    rewrite, which is what SQ50 exists for.

    Only MERGE was filed. UPDATE and DELETE were measured while fixing it
    and are the same defect in the same rule, so they are fixed with it
    rather than left to be re-found: the sweep's note that SQ80_MERGE
    "flags the statement so it does reach a human" is true of MERGE and
    false of the other two, which came back with flags=0 and graded PASS
    holding SQL that cannot parse.
    """

    def test_the_merge_reproduction_no_longer_appends_a_limit(self):
        result = _t("MERGE TOP (10) INTO dbo.t USING s ON 1=1")
        self.assertNotIn("LIMIT", result.translated_sql)
        self.assertEqual(
            result.translated_sql,
            "MERGE TOP (10) INTO default.AcmeDW.t USING s ON 1=1")
        self.assertIn("SQ50_TOP_NO_LIMIT", _rules(result))
        self.assertNotIn("SQ50_TOP", _rules(result))

    def test_update_and_delete_have_the_same_defect_and_the_same_fix(self):
        for sql in ("UPDATE TOP (10) dbo.t SET a = 1",
                    "DELETE TOP (10) FROM dbo.t",
                    "DELETE TOP (10) FROM dbo.t WHERE a = 1"):
            with self.subTest(sql=sql):
                result = _t(sql)
                self.assertNotIn("LIMIT", result.translated_sql)
                self.assertIn("SQ50_TOP_NO_LIMIT", _rules(result))

    def test_it_is_a_flag_and_not_a_rewrite(self):
        """The whole point: the old finding said `rewrite`, which is a claim
        that the statement was translated. Nothing here can be."""
        result = _t("MERGE TOP (10) INTO dbo.t USING s ON 1=1")
        finding = next(f for f in result.findings
                       if f.rule == "SQ50_TOP_NO_LIMIT")
        self.assertEqual(finding.severity, "flag")
        self.assertGreaterEqual(result.flags, 1)

    def test_the_top_is_left_where_it_was_rather_than_moved(self):
        """Refused, not relocated. Moving the cap to the MERGE's source
        would change which rows are written, and this rule has no
        measurement saying which rows the author meant."""
        result = _t("MERGE TOP (10) INTO dbo.t USING s ON 1=1")
        self.assertIn("MERGE TOP (10) INTO", result.translated_sql)

    def test_the_finding_names_the_verb_and_the_way_out(self):
        result = _t("DELETE TOP (10) FROM dbo.t")
        detail = next(f.detail for f in result.findings
                      if f.rule == "SQ50_TOP_NO_LIMIT")
        self.assertIn("DELETE", detail)
        self.assertIn("PARSE_SYNTAX_ERROR", detail)
        self.assertIn("3.5.0", detail)
        self.assertIn("LIMIT 10", detail)

    def test_select_and_insert_still_get_the_rewrite(self):
        """The other direction. Spark accepts a LIMIT on both, measured
        above, so switching the rule off for them would be over-refusal."""
        for sql, expected in (
                ("SELECT TOP (10) a FROM dbo.t",
                 "SELECT a FROM default.AcmeDW.t LIMIT 10"),
                ("INSERT TOP (10) INTO dbo.t SELECT a FROM dbo.s",
                 "INSERT INTO default.AcmeDW.t SELECT a FROM "
                 "default.AcmeDW.s LIMIT 10")):
            with self.subTest(sql=sql):
                result = _t(sql)
                self.assertEqual(result.translated_sql, expected)
                self.assertIn("SQ50_TOP", _rules(result))
                self.assertNotIn("SQ50_TOP_NO_LIMIT", _rules(result))

    def test_a_top_in_a_subquery_of_one_still_belongs_to_the_subquery(self):
        """The verb that owns the TOP is the nearest one at its own depth,
        so a capped source inside a MERGE is still the SELECT's TOP -- and
        still SQ50_TOP_SUBQUERY, which is the finding it had before."""
        result = _t("MERGE INTO dbo.t USING (SELECT TOP (5) a FROM dbo.s) "
                    "src ON 1=1")
        self.assertIn("SQ50_TOP_SUBQUERY", _rules(result))
        self.assertNotIn("SQ50_TOP_NO_LIMIT", _rules(result))
        self.assertNotIn("LIMIT", result.translated_sql)

    def test_the_verb_is_read_per_statement_not_per_file(self):
        """Two statements, one file: the SELECT keeps its LIMIT and the
        DELETE beside it does not acquire one."""
        result = _t("SELECT TOP (3) a FROM dbo.t; DELETE TOP (2) FROM dbo.u")
        self.assertEqual(
            result.translated_sql,
            "SELECT a FROM default.AcmeDW.t LIMIT 3; "
            "DELETE TOP (2) FROM default.AcmeDW.u")
        self.assertEqual(sorted(r for r in _rules(result)
                                if r.startswith("SQ50")),
                         ["SQ50_TOP", "SQ50_TOP_NO_LIMIT"])

    def test_the_older_refusals_still_win_where_they_applied(self):
        """PERCENT and a non-literal count are refused before the verb is
        consulted: both were already flags, and re-deciding them here would
        replace a specific reason with a general one."""
        for sql, rule in (
                ("MERGE TOP (10) PERCENT INTO dbo.t USING s ON 1=1",
                 "SQ50_TOP_UNSUPPORTED"),
                ("MERGE TOP (@n) INTO dbo.t USING s ON 1=1",
                 "SQ50_TOP_NON_LITERAL")):
            with self.subTest(sql=sql):
                result = _t(sql)
                self.assertIn(rule, _rules(result))
                self.assertNotIn("SQ50_TOP_NO_LIMIT", _rules(result))


class MergeFlagIsAboutTheStatementNotTheWordTests(unittest.TestCase):
    """F3. SQ80_MERGE fired on a column named `merge`.

    Its pattern was `\\bMERGE\\s+(?!JOIN\\b)`, which reads the word.
    MEASURED on 2a223bd, `translate(sql, kind="view", item="AcmeDW")` -- all
    five carried SQ80_MERGE and none is a MERGE statement:

        SELECT merge FROM dbo.claim        a column of that name
        SELECT t.merge FROM dbo.claim t    qualified by its alias
        SELECT a AS merge FROM dbo.claim   an output column named that
        CREATE TABLE dbo.t (merge INT)     a column being declared
        UPDATE dbo.t SET merge = 1         a column being assigned

    The first is the filed reproduction:

        SELECT merge FROM dbo.claim
          -> SELECT merge FROM default.AcmeDW.claim
             ['SQ11_TWO_PART_NAME', 'SQ80_MERGE']

    The rewrite is right in every one; only the finding is wrong -- and the
    finding's own sentence, about OUTPUT and WHEN NOT MATCHED BY SOURCE,
    describes clauses that are not in the SQL. A flag pushes the object to
    REVIEW, so a clean view or notebook needed a human because of a name.
    Same family as #27, where NB12 hit CTE names and `EXTRACT/TRIM ...
    FROM`.

    The test is now the MERGE's own structure --
    `MERGE [TOP] [INTO] <target> [[AS] <alias>] USING` -- and not
    `_heads_a_statement`, which is what SQ52 uses for the same word.
    `_heads_a_statement`'s own docstring accepts a miss on
    `WITH c AS (...) MERGE ...`, which T-SQL allows, and that is affordable
    for SQ52 (a rewrite) and not for SQ80_MERGE: SQ80_MERGE is the flag that
    sends the statement to a human, and losing it would grade a real MERGE
    clean. MEASURED on 2a223bd, both CTE spellings carried SQ80_MERGE, and
    both still do.
    """

    NOT_STATEMENTS = (
        "SELECT merge FROM dbo.claim",
        "SELECT t.merge FROM dbo.claim t",
        "SELECT a AS merge FROM dbo.claim",
        "CREATE TABLE dbo.t (merge INT)",
        "UPDATE dbo.t SET merge = 1",
        "SELECT merge FROM dbo.t WHERE merge = 1",
        "SELECT COUNT(merge) FROM dbo.t",
    )

    STATEMENTS = (
        "MERGE INTO dbo.t USING s ON 1=1",
        "MERGE dbo.t USING s ON 1=1",
        "MERGE INTO dbo.t AS tgt USING dbo.s AS src ON tgt.a=src.a",
        "MERGE INTO dbo.t tgt USING dbo.s src ON 1=1",
        "MERGE mydb.dbo.t USING dbo.s ON 1=1",
        "MERGE TOP (10) INTO dbo.t USING s ON 1=1",
        "MERGE\n  INTO dbo.t\n  USING dbo.s\n  ON 1=1",
        "merge into dbo.t using dbo.s on 1=1",
        "SELECT 1; MERGE INTO dbo.t USING s ON 1=1",
        # T-SQL allows a CTE in front of a MERGE. `_heads_a_statement` would
        # lose both of these; the structural test keeps them.
        "WITH c AS (SELECT 1 a) MERGE INTO dbo.t USING c ON 1=1",
        "WITH c AS (SELECT 1 a) MERGE dbo.t USING c ON 1=1",
    )

    def test_the_reproduction_is_no_longer_flagged(self):
        result = _t("SELECT merge FROM dbo.claim")
        self.assertEqual(_rules(result), ["SQ11_TWO_PART_NAME"])

    def test_the_rewrite_was_always_right_and_is_unchanged(self):
        """The finding was the only thing wrong, so the artifact must come
        out of the fix byte for byte as it went in."""
        result = _t("SELECT merge FROM dbo.claim")
        self.assertEqual(result.translated_sql,
                         "SELECT merge FROM default.AcmeDW.claim")

    def test_none_of_the_five_measured_shapes_is_a_merge(self):
        for sql in self.NOT_STATEMENTS:
            with self.subTest(sql=sql):
                self.assertNotIn("SQ80_MERGE", _rules(_t(sql)))

    def test_every_real_merge_statement_is_still_flagged(self):
        for sql in self.STATEMENTS:
            with self.subTest(sql=sql):
                self.assertIn("SQ80_MERGE", _rules(_t(sql)))

    def test_a_cte_led_merge_keeps_the_flag_it_already_had(self):
        """Called out on its own because it is the one case the obvious fix
        -- `_heads_a_statement`, which SQ52 uses for this word -- would have
        lost, and losing it means a MERGE graded clean."""
        for sql in ("WITH c AS (SELECT 1 a) MERGE INTO dbo.t USING c ON 1=1",
                    "WITH c AS (SELECT 1 a) MERGE dbo.t USING c ON 1=1"):
            with self.subTest(sql=sql):
                self.assertIn("SQ80_MERGE", _rules(_t(sql)))

    def test_the_join_hint_is_still_the_join_hint(self):
        """`INNER MERGE JOIN` was excluded by a `(?!JOIN\\b)` the new shape
        makes unnecessary. It must still be excluded, and still reported by
        the rule whose sentence is about it."""
        result = _t("SELECT a FROM t INNER MERGE JOIN u ON t.a=u.a")
        self.assertNotIn("SQ80_MERGE", _rules(result))
        self.assertIn("SQ80_JOIN_HINT", _rules(result))

    def test_a_quoted_column_of_that_name_is_still_not_one(self):
        """`[merge]` was already safe -- the rule reads a view with
        identifier bodies blanked -- and is pinned so the new pattern cannot
        have started reading them."""
        self.assertNotIn("SQ80_MERGE", _rules(_t("SELECT [merge] FROM dbo.claim")))

    def test_a_literal_or_a_comment_is_not_a_statement(self):
        for sql in ("SELECT 'MERGE INTO x USING y' AS s FROM dbo.t",
                    "-- MERGE INTO x USING y\nSELECT merge FROM dbo.t"):
            with self.subTest(sql=sql):
                self.assertNotIn("SQ80_MERGE", _rules(_t(sql)))

    def test_a_table_named_merge_is_not_a_merge_either(self):
        result = _t("SELECT * FROM dbo.merge")
        self.assertNotIn("SQ80_MERGE", _rules(result))
        self.assertEqual(result.translated_sql,
                         "SELECT * FROM default.AcmeDW.merge")


class MergeUsingSourceIsAnObjectPositionTests(unittest.TestCase):
    """F4. The MERGE source kept its two-part name; a three-part one lost it.

    MEASURED on 2a223bd, `translate(sql, kind="view", item="AcmeDW")`:

        MERGE INTO dbo.t USING dbo.s ON 1=1
          -> MERGE INTO default.AcmeDW.t USING dbo.s ON 1=1
             ['SQ11_TWO_PART_NAME', 'SQ80_MERGE']

    One statement naming one table the plan promises and one it does not.
    `USING` is not in `_OBJECT_KEYWORDS`, so the source was the one name in
    a MERGE the rules could not see -- the other half of the hole SQ52's
    MERGE case closed for the target.

    And the three-part source is worse than the filing said. MEASURED on
    2a223bd:

        MERGE INTO dbo.t USING mydb.dbo.s AS src ON 1=1
          -> MERGE INTO default.AcmeDW.t USING dbo.s AS src ON 1=1
             ['SQ15_QUALIFIED_COLUMN', 'SQ11_TWO_PART_NAME', 'SQ80_MERGE']

    -- the database dropped and the source silently renamed, which is F1's
    defect reached through a different position. The filing recorded only
    the two-part case.

    The fix is the MERGE's own structure and not a keyword, which is what
    the filing asked for and why it was right to stop at
    `_OBJECT_KEYWORDS`. `_MERGE_USING_RE` matches
    `MERGE [TOP] [INTO] <target> [[AS] <alias>] USING <source>` whole, so
    the `USING` of `JOIN b USING (c)` and of `CREATE TABLE t (...) USING
    parquet` is not reachable: neither has a MERGE in front of it. Both were
    MEASURED on 2a223bd as left alone, and both still are.

    What the source is qualified *to* was never in doubt: `USING dbo.s` is
    the same read position as `FROM dbo.s`, which this rule has always
    qualified, and every shape comes out with FROM's answer exactly --
    measured below.
    """

    def test_the_reproduction_qualifies_both_names(self):
        result = _t("MERGE INTO dbo.t USING dbo.s ON 1=1")
        self.assertEqual(
            result.translated_sql,
            "MERGE INTO default.AcmeDW.t USING default.AcmeDW.s ON 1=1")
        self.assertEqual(_rules(result), ["SQ11_TWO_PART_NAME",
                                          "SQ11_TWO_PART_NAME", "SQ80_MERGE"])

    def test_a_three_part_source_keeps_its_database(self):
        """The half the filing did not have: SQ15 read it as a column."""
        result = _t("MERGE INTO dbo.t USING mydb.dbo.s AS src ON 1=1")
        self.assertNotIn("SQ15_QUALIFIED_COLUMN", _rules(result))
        self.assertIn("SQ11_THREE_PART_NAME", _rules(result))
        self.assertIn("USING default.mydb.s AS src", result.translated_sql)

    def test_the_source_gets_exactly_the_name_the_from_position_gets(self):
        """The invariant, and the reason the rewrite needed no new decision:
        `USING x` and `FROM x` are one read position and must not be two
        answers. Every shape, including the ones this rule refuses."""
        for name in ("dbo.s", "mydb.dbo.s", "mydb.sales.s", "sys.objects",
                     "a.b.c.d"):
            with self.subTest(name=name):
                from_side = _t("SELECT * FROM %s" % name).translated_sql
                expected = from_side[len("SELECT * FROM "):]
                using = _t("MERGE INTO dbo.t USING %s ON 1=1" % name)
                self.assertIn("USING %s ON" % expected, using.translated_sql)

    def test_every_spelling_of_the_head_finds_the_source(self):
        for sql in ("MERGE INTO dbo.t USING dbo.s ON 1=1",
                    "MERGE dbo.t USING dbo.s ON 1=1",
                    "MERGE INTO dbo.t AS tgt USING dbo.s AS src ON 1=1",
                    "MERGE INTO dbo.t tgt USING dbo.s src ON 1=1",
                    "MERGE TOP (10) INTO dbo.t USING dbo.s ON 1=1",
                    "MERGE INTO [dbo].[t] USING [dbo].[s] ON 1=1",
                    "MERGE INTO dbo.t USING\n  dbo.s\n  ON 1=1",
                    "merge into dbo.t using dbo.s on 1=1"):
            with self.subTest(sql=sql):
                self.assertIn("default.AcmeDW.s", _t(sql).translated_sql)

    def test_the_two_usings_that_are_not_merge_sources_are_untouched(self):
        """Why `USING` could not simply be added to `_OBJECT_KEYWORDS`, and
        the check that anchoring on the MERGE did not do it by the back
        door. MEASURED on 2a223bd as leaving both alone."""
        for sql, expected in (
                ("SELECT * FROM a JOIN b USING (c)",
                 "SELECT * FROM a JOIN b USING (c)"),
                ("CREATE TABLE dbo.t (a INT) USING parquet",
                 "CREATE TABLE default.AcmeDW.t (a INT) USING parquet")):
            with self.subTest(sql=sql):
                self.assertEqual(_t(sql).translated_sql, expected)

    def test_a_derived_table_source_is_not_read_as_a_name(self):
        """`USING (SELECT ...)` has no name in the slot, and the rules
        inside the subquery already reach its own. Unchanged from 2a223bd."""
        result = _t("MERGE INTO dbo.t USING (SELECT a FROM dbo.s) AS src "
                    "ON 1=1")
        self.assertEqual(
            result.translated_sql,
            "MERGE INTO default.AcmeDW.t USING (SELECT a FROM "
            "default.AcmeDW.s) AS src ON 1=1")

    def test_a_bare_source_is_still_left_alone(self):
        """A one-part source is as often a CTE name as a table --
        `WITH c AS (...) MERGE INTO t USING c` -- so `_ONE_PART_KEYWORDS`
        does not gain USING either. Unchanged from 2a223bd."""
        result = _t("WITH c AS (SELECT 1 a) MERGE INTO dbo.t USING c ON 1=1")
        self.assertIn("USING c ON", result.translated_sql)

    def test_a_four_part_source_is_still_a_linked_server(self):
        result = _t("MERGE INTO dbo.t USING a.b.c.d ON 1=1")
        self.assertIn("SQ11_LINKED_SERVER", _rules(result))
        self.assertIn("USING a.b.c.d ON", result.translated_sql)

    def test_the_catalog_still_decides_where_the_source_lives(self):
        """The source is a *read*, so the recorded owner outranks this
        object's folder -- the same rule the FROM position follows."""
        catalog = {"tables": {"dbo.s": {"tier": "warehouse_ddl",
                                        "name": "dbo.s",
                                        "owner": "OtherDW"}}}
        result = _t("MERGE INTO dbo.t USING dbo.s ON 1=1",
                    table_catalog=catalog)
        self.assertIn("USING default.OtherDW.s ON", result.translated_sql)

    def test_a_source_no_catalog_tier_knows_is_left_as_written(self):
        catalog = {"tables": {"dbo.s": {"tier": "warehouse_ddl",
                                        "name": "dbo.s", "owner": "AcmeDW"}}}
        result = _t("MERGE INTO dbo.s USING dbo.nowhere ON 1=1",
                    table_catalog=catalog)
        self.assertIn("USING dbo.nowhere ON", result.translated_sql)
        self.assertIn("SQ19_TABLE_UNKNOWN", _rules(result))

    def test_the_same_name_in_both_slots_does_not_overlap(self):
        """`apply_replacements` raises on two rewrites over one span, and an
        AssertionError out of a rule is a FAIL row with no rule on it (#33).
        `_object_positions` deduplicates on the name span for that reason,
        and a self-merge is the shape that gets closest to it."""
        result = _t("MERGE INTO dbo.t USING dbo.t ON 1=1")
        self.assertEqual(
            result.translated_sql,
            "MERGE INTO default.AcmeDW.t USING default.AcmeDW.t ON 1=1")

    def test_two_merges_in_one_file_each_find_their_own_source(self):
        result = _t("MERGE INTO dbo.t USING dbo.s ON 1=1; "
                    "MERGE INTO dbo.u USING dbo.v ON 1=1")
        self.assertEqual(
            result.translated_sql,
            "MERGE INTO default.AcmeDW.t USING default.AcmeDW.s ON 1=1; "
            "MERGE INTO default.AcmeDW.u USING default.AcmeDW.v ON 1=1")

    def test_a_merge_inside_a_literal_names_nothing(self):
        result = _t("MERGE INTO dbo.t USING dbo.s "
                    "ON s.a = 'MERGE INTO x USING y'")
        self.assertIn("'MERGE INTO x USING y'", result.translated_sql)
        self.assertEqual(
            sorted(r for r in _rules(result) if r.startswith("SQ11")),
            ["SQ11_TWO_PART_NAME", "SQ11_TWO_PART_NAME"])


if __name__ == "__main__":
    unittest.main()
