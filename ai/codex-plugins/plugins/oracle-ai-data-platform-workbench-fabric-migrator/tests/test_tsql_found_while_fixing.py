"""T-SQL defects filed while fixing other T-SQL defects (issue #1).

Each case below was measured on the tree before its fix, and the measured
"before" is quoted next to it. Spark verdicts were taken on pyspark 4.2.0
with JAVA_HOME=openjdk@21, session defaults, tables created `USING parquet`;
where a verdict depends on a non-default setting, the setting is named.

  F1  `rule_select_into` bailed out for the WHOLE document when an `INSERT`
      appeared anywhere in it, so one unrelated row load left an unrewritten
      `SELECT ... INTO` in the artifact with no finding against it
  F2  `INSERT dbo.t VALUES (...)` -- T-SQL's `INTO`-less INSERT -- left the
      target unqualified and unflagged, and Spark rejects the form outright
  F3  `ALTER TABLE t ADD c <type>` was neither rewritten to Spark's
      `ADD COLUMNS (...)` nor type-scanned, so `money` and `timestamp` passed
      with flags=0 in the one place SQ60 does not look
  F4  `SET QUOTED_IDENTIFIER ON` reached the artifact verbatim, and Spark
      rejects it with INVALID_SET_SYNTAX, taking the whole object down
  F5  SQ10 UNWRAPPED `[timestamp]` to a bare `timestamp` when the bracketed
      word happened to be a type name, whatever position it stood in, so a
      user's quoting of a column name was thrown away
  F6  SQ60's finding for a length-bearing column type was a bare arrow,
      `NVARCHAR(50) -> STRING`, and said nothing about the length it dropped
  F8  a T-SQL PIVOT's `IN ([a],[b])` list holds VALUES; SQ10 backticked them
      into column references and reported two successful rewrites
"""
import unittest

from fabric_aidp.translate import tsql_to_spark_sql as tsql


def _t(sql, **kw):
    kw.setdefault("kind", "table")
    kw.setdefault("item", "W")
    return tsql.translate(sql, **kw)


def _rules(result):
    return [f.rule for f in result.findings]


class SelectIntoIsScopedToItsStatementTests(unittest.TestCase):
    """F1. One `INSERT` anywhere disarmed SQ51 for the whole document.

    Measured before the fix:

        INSERT INTO dbo.log VALUES (1);
        SELECT a INTO dbo.u FROM dbo.t
        -> INSERT INTO default.W.log VALUES (1);
           SELECT a INTO default.W.u FROM default.W.t
           findings: SQ11 x3, flags=0, PASS

    and `SELECT a INTO u FROM t` on Spark 4.2.0 is
    `PARSE_SYNTAX_ERROR at or near 'u'`. A warehouse `.sql` holding a table
    load and a staging SELECT INTO is an ordinary file, and it graded clean
    with unrunnable SQL in the artifact.

    The guard existed because `_INTO_RE` also matches the `INTO` of
    `INSERT INTO`. That is a question about one statement, and
    `_statement_bounds` already knew where each statement was.
    """

    def test_the_reproduction(self):
        result = _t("INSERT INTO dbo.log VALUES (1);\n"
                    "SELECT a INTO dbo.u FROM dbo.t")
        self.assertEqual(
            result.translated_sql,
            "INSERT INTO default.W.log VALUES (1);\n"
            "CREATE TABLE default.W.u AS SELECT a FROM default.W.t")
        self.assertIn("SQ51_SELECT_INTO", _rules(result))

    def test_the_insert_after_the_into_disarmed_it_too(self):
        result = _t("SELECT a INTO dbo.u FROM dbo.t;\n"
                    "INSERT INTO dbo.log VALUES (1)")
        self.assertEqual(
            result.translated_sql,
            "CREATE TABLE default.W.u AS SELECT a FROM default.W.t;\n"
            "INSERT INTO default.W.log VALUES (1)")

    def test_an_inserts_own_into_is_still_not_a_ctas(self):
        """The thing the guard was protecting, kept."""
        for sql in ("INSERT INTO dbo.t SELECT a FROM dbo.s",
                    "INSERT INTO dbo.t (a) VALUES (1)",
                    "INSERT INTO dbo.t (a) SELECT a FROM dbo.s "
                    "WHERE a IN (SELECT b FROM dbo.z)"):
            with self.subTest(sql=sql):
                result = _t(sql)
                self.assertNotIn("SQ51_SELECT_INTO", _rules(result))
                self.assertNotIn("CREATE TABLE", result.translated_sql)

    def test_the_into_of_an_insert_top_is_not_a_ctas_either(self):
        """`INSERT TOP (n) INTO t` puts a clause between the two words, so a
        rule that only looked at the token in front of `INTO` would take this
        one for a CTAS."""
        result = _t("INSERT TOP (5) INTO dbo.t SELECT a FROM dbo.s")
        self.assertNotIn("SQ51_SELECT_INTO", _rules(result))

    def test_an_output_clauses_into_is_not_a_ctas(self):
        """Found while fixing F1, and not reachable by any INSERT check: the
        document has no INSERT in it at all. Measured before:

            SELECT a FROM dbo.x;
            DELETE FROM dbo.t OUTPUT deleted.id INTO dbo.audit
            -> SELECT a FROM default.W.x;
               CREATE TABLE default.W.audit AS
                 DELETE FROM default.W.t OUTPUT deleted.id
               SQ51_SELECT_INTO, reported as a rewrite

        A DELETE wrapped in a CTAS. Spark 4.2.0 rejects it either way; what
        is wrong is that the tool called it a successful rewrite.
        """
        for verb, statement in (
                ("DELETE",
                 "DELETE FROM dbo.t OUTPUT deleted.id INTO dbo.audit"),
                ("UPDATE",
                 "UPDATE dbo.t SET a = 1 OUTPUT inserted.a INTO dbo.audit")):
            with self.subTest(verb=verb):
                result = _t("SELECT a FROM dbo.x;\n" + statement)
                self.assertNotIn("SQ51_SELECT_INTO", _rules(result))
                self.assertNotIn("CREATE TABLE", result.translated_sql)

    def test_a_cte_before_the_select_still_reads_as_a_ctas(self):
        result = _t("WITH c AS (SELECT 1 AS x) SELECT x INTO dbo.u FROM c")
        self.assertEqual(
            result.translated_sql,
            "CREATE TABLE default.W.u AS "
            "WITH c AS (SELECT 1 AS x) SELECT x FROM c")

    def test_a_subquery_into_is_still_refused(self):
        result = _t("SELECT * FROM (SELECT c INTO x FROM dbo.w) y")
        self.assertIn("SQ51_INTO_SUBQUERY", _rules(result))

    def test_the_single_statement_case_is_exactly_as_before(self):
        result = _t("SELECT a INTO dbo.u FROM dbo.t")
        self.assertEqual(result.translated_sql,
                         "CREATE TABLE default.W.u AS SELECT a FROM default.W.t")
        self.assertEqual(result.flags, 0)


class InsertWithoutIntoTests(unittest.TestCase):
    """F2. T-SQL's `INTO` is optional on INSERT, and without it the target was
    invisible to SQ11 and unrunnable on Spark. Measured before:

        INSERT dbo.t VALUES (1)      -> INSERT dbo.t VALUES (1)            []
        INSERT INTO dbo.t VALUES (1) -> INSERT INTO default.W.t VALUES (1) SQ11

    Spark 4.2.0: `INSERT t VALUES (1, 2, 'x')` is PARSE_SYNTAX_ERROR at or
    near 't'; the same statement with INTO is ACCEPTED. So the INTO-less form
    both wrote to the wrong catalog and could not run, with zero findings.
    """

    def test_the_reproduction(self):
        result = _t("INSERT dbo.t VALUES (1)")
        self.assertEqual(result.translated_sql,
                         "INSERT INTO default.W.t VALUES (1)")
        self.assertIn("SQ52_INSERT_NO_INTO", _rules(result))
        self.assertIn("SQ11_TWO_PART_NAME", _rules(result))

    def test_the_form_with_a_column_list_and_a_select(self):
        result = _t("INSERT dbo.t (a, b) SELECT a, b FROM dbo.s")
        self.assertEqual(
            result.translated_sql,
            "INSERT INTO default.W.t (a, b) SELECT a, b FROM default.W.s")

    def test_a_bracketed_target_is_still_backticked_and_qualified(self):
        result = _t("INSERT [dbo].[my table] VALUES (1)")
        self.assertEqual(result.translated_sql,
                         "INSERT INTO default.W.`my table` VALUES (1)")

    def test_a_statement_that_already_has_into_is_untouched(self):
        result = _t("INSERT INTO dbo.t VALUES (1)")
        self.assertEqual(result.translated_sql,
                         "INSERT INTO default.W.t VALUES (1)")
        self.assertNotIn("SQ52_INSERT_NO_INTO", _rules(result))

    def test_bulk_insert_is_left_alone(self):
        """`BULK INSERT t FROM 'file'` does not take INTO. It has no Spark
        form either, and inventing `BULK INSERT INTO` would turn something a
        reader recognises into something nobody wrote."""
        sql = "BULK INSERT dbo.t FROM '/tmp/x.csv'"
        result = _t(sql)
        self.assertEqual(result.translated_sql, sql)
        self.assertNotIn("SQ52_INSERT_NO_INTO", _rules(result))

    def test_a_merge_insert_clause_has_no_target_to_qualify(self):
        for tail in ("INSERT (a) VALUES (src.a)", "INSERT VALUES (src.a)"):
            with self.subTest(tail=tail):
                sql = ("MERGE dbo.t AS tgt USING dbo.s AS src "
                       "ON tgt.id = src.id WHEN NOT MATCHED THEN " + tail)
                self.assertNotIn("SQ52_INSERT_NO_INTO", _rules(_t(sql)))

    def test_the_word_insert_in_a_column_name_or_a_literal_is_not_the_keyword(self):
        for sql in ("SELECT [insert count] FROM dbo.t",
                    "SELECT 'INSERT foo VALUES' AS s FROM dbo.t"):
            with self.subTest(sql=sql):
                self.assertNotIn("SQ52_INSERT_NO_INTO", _rules(_t(sql)))


class MergeWithoutIntoTests(unittest.TestCase):
    """The same defect as F2 above, in the other statement whose INTO T-SQL
    makes optional. MEASURED on 03f019b:

        MERGE dbo.t AS tgt USING dbo.s AS src ON tgt.id=src.id
          WHEN MATCHED THEN UPDATE SET tgt.a=src.a;
            -> the target `dbo.t` stays two-part     findings: ['SQ80_MERGE']

        MERGE INTO dbo.t AS tgt USING ... (the same statement)
            -> MERGE INTO default.W.t ...   SQ11_TWO_PART_NAME, SQ80_MERGE

    One statement, two spellings, and only one of them got the name the plan
    promises. SQ80_MERGE flags the statement either way, so this always
    reached a human -- which is why it was filed a minor. What it did not do
    is reach the human with the right name: a reader who resolves the flag by
    hand keeps whatever the artifact says, and the artifact said `dbo.t`.

    MEASURED on pyspark 4.2.0, JAVA_HOME=openjdk@21, local[1], session
    defaults (spark.version 4.2.0, ansi.enabled true):

        MERGE t USING u ON t.a=u.a WHEN MATCHED THEN UPDATE SET t.b=u.b
          PARSE_SYNTAX_ERROR "Syntax error at or near 't'" (line 1, pos 6)
        MERGE INTO t USING u ON t.a=u.a WHEN MATCHED THEN UPDATE SET t.b=u.b
          TABLE_OR_VIEW_NOT_FOUND

    The second failure is the probe having no table `t` -- which is the point:
    that statement PARSED. Spark requires INTO and T-SQL does not, so writing
    the keyword in is both the syntax fix and what lets SQ11 see the name.
    """

    RULE = "SQ52_MERGE_NO_INTO"
    TAIL = ("USING dbo.s AS src ON tgt.id = src.id "
            "WHEN MATCHED THEN UPDATE SET tgt.a = src.a")
    # The same tail as SQ11 now writes it. `USING dbo.s` used to survive
    # this rule unqualified, and the assertion below was written around it.
    TRANSLATED_TAIL = ("USING default.W.s AS src ON tgt.id = src.id "
                       "WHEN MATCHED THEN UPDATE SET tgt.a = src.a")

    def test_the_reproduction(self):
        """Rewritten deliberately, and the old assertion was wrong.

        It said the translated statement was
        `MERGE INTO default.W.t AS tgt ` + TAIL -- the tail verbatim, so
        `USING dbo.s AS src` unqualified. That was this class's own defect
        in the other name: `USING` is not in `_OBJECT_KEYWORDS` either, so
        the source was the second name in a MERGE the rules could not see,
        and the sentence above -- "a reader who resolves the flag by hand
        keeps whatever the artifact says" -- was as true of `dbo.s` as of
        `dbo.t`. MEASURED on 2a223bd, with this rule already fixed:

            MERGE INTO dbo.t USING dbo.s ON 1=1
              -> MERGE INTO default.W.t USING dbo.s ON 1=1

        `_MERGE_USING_RE` now anchors that position on the MERGE's own
        structure, so the tail is qualified too. The assertion is kept, as
        an equality over the whole statement, against the tail SQ11 now
        writes; nothing about SQ52 changed.
        """
        result = _t("MERGE dbo.t AS tgt " + self.TAIL)
        self.assertEqual(result.translated_sql,
                         "MERGE INTO default.W.t AS tgt "
                         + self.TRANSLATED_TAIL)
        self.assertIn(self.RULE, _rules(result))
        self.assertIn("SQ11_TWO_PART_NAME", _rules(result))

    def test_the_two_spellings_now_agree(self):
        """The whole of the defect, asserted as the equality it should
        always have been."""
        without = _t("MERGE dbo.t AS tgt " + self.TAIL).translated_sql
        with_into = _t("MERGE INTO dbo.t AS tgt " + self.TAIL).translated_sql
        self.assertEqual(without, with_into)

    def test_a_statement_that_already_has_into_is_untouched(self):
        result = _t("MERGE INTO dbo.t AS tgt " + self.TAIL)
        self.assertNotIn(self.RULE, _rules(result))
        self.assertIn("MERGE INTO default.W.t", result.translated_sql)

    def test_sq80_still_flags_the_statement(self):
        """The rewrite is the name, not the semantics. T-SQL MERGE and Delta
        MERGE INTO differ in their clause set, and that is still a human's to
        finish."""
        result = _t("MERGE dbo.t AS tgt " + self.TAIL)
        self.assertIn("SQ80_MERGE", _rules(result))
        self.assertGreater(result.flags, 0)

    def test_a_bare_target_is_found_and_left_for_the_one_part_rule(self):
        result = _t("MERGE t AS tgt " + self.TAIL)
        self.assertIn(self.RULE, _rules(result))
        self.assertIn("MERGE INTO t AS tgt", result.translated_sql)
        self.assertIn("SQ11_ONE_PART_NAME", _rules(result))

    def test_a_bracketed_target_is_backticked_and_qualified(self):
        result = _t("MERGE [dbo].[my table] AS tgt " + self.TAIL)
        self.assertIn("MERGE INTO default.W.`my table` AS tgt",
                      result.translated_sql)

    def test_a_target_with_no_alias(self):
        result = _t("MERGE dbo.t USING dbo.s ON 1 = 1 "
                    "WHEN MATCHED THEN DELETE")
        self.assertIn("MERGE INTO default.W.t USING", result.translated_sql)

    def test_the_top_clause_does_not_hide_the_target(self):
        """T-SQL allows `MERGE TOP (n) [PERCENT] [INTO] target`, so the
        target is not the first thing after the keyword."""
        for head in ("MERGE TOP (10) ", "MERGE TOP (10) PERCENT "):
            with self.subTest(head=head):
                result = _t(head + "dbo.t USING dbo.s ON 1 = 1 "
                            "WHEN MATCHED THEN DELETE")
                self.assertIn(self.RULE, _rules(result))
                self.assertIn("INTO default.W.t", result.translated_sql)

    # -- and the readings of the word MERGE that are not this statement ----

    def test_a_join_hint_is_not_a_merge_statement(self):
        """`INNER MERGE JOIN` names a physical strategy. SQ80_JOIN_HINT
        reports it; its MERGE never begins a statement, and `JOIN` is in
        `_NOT_A_MERGE_TARGET` as well."""
        sql = "SELECT t.a FROM dbo.t INNER MERGE JOIN dbo.u ON t.a = u.a"
        result = _t(sql)
        self.assertNotIn(self.RULE, _rules(result))
        self.assertNotIn("INTO", result.translated_sql)
        self.assertIn("SQ80_JOIN_HINT", _rules(result))

    def test_a_column_called_merge_is_not_the_keyword(self):
        """`SELECT merge FROM dbo.t`. Without the statement-head test the
        `FROM` behind it was read as the target and `INTO` written in front
        of it, producing `SELECT merge INTO FROM ...`."""
        result = _t("SELECT merge FROM dbo.t")
        self.assertNotIn(self.RULE, _rules(result))
        self.assertEqual(result.translated_sql,
                         "SELECT merge FROM default.W.t")

    def test_the_word_merge_in_a_literal_or_a_comment_is_not_the_keyword(self):
        for sql in ("SELECT 'MERGE dbo.t USING' AS s FROM dbo.t",
                    "-- MERGE dbo.t USING\nSELECT a FROM dbo.t"):
            with self.subTest(sql=sql):
                self.assertNotIn(self.RULE, _rules(_t(sql)))

    def test_rewriting_twice_changes_nothing(self):
        once = _t("MERGE dbo.t AS tgt " + self.TAIL).translated_sql
        self.assertEqual(_t(once).translated_sql, once)


class AlterTableAddColumnTests(unittest.TestCase):
    """F3. `ALTER TABLE t ADD c <type>` reached the artifact verbatim.

    Measured before:

        ALTER TABLE dbo.t ADD c timestamp
          -> ALTER TABLE default.W.t ADD c timestamp   SQ11 only, flags=0
        ALTER TABLE dbo.t ADD c money
          -> ALTER TABLE default.W.t ADD c money       SQ11 only, flags=0

    Two defects. The syntax, on Spark 4.2.0:

        ALTER TABLE t ADD c timestamp            PARSE_SYNTAX_ERROR at 'c'
        ALTER TABLE t ADD COLUMNS (c TIMESTAMP)  ACCEPTED

    and the types, because `_column_list_bodies` knew only about CREATE
    TABLE, so `money` (UNSUPPORTED_DATATYPE on Spark) and `timestamp`
    (T-SQL's rowversion, not a datetime) both passed with flags=0 -- the
    exact pair SQ60 catches inside a CREATE TABLE body.
    """

    def test_the_syntax_is_rewritten(self):
        result = _t("ALTER TABLE dbo.t ADD c timestamp")
        self.assertEqual(result.translated_sql,
                         "ALTER TABLE default.W.t ADD COLUMNS (c timestamp)")
        self.assertIn("SQ76_ALTER_ADD_COLUMN", _rules(result))

    def test_the_types_are_now_scanned(self):
        for sql, rule in (("ALTER TABLE dbo.t ADD c money", "SQ60_MONEY"),
                          ("ALTER TABLE dbo.t ADD c timestamp",
                           "SQ60_TYPE_UNKNOWN")):
            with self.subTest(sql=sql):
                result = _t(sql)
                self.assertIn(rule, _rules(result))
                self.assertGreater(result.flags, 0)

    def test_a_mappable_type_is_mapped_in_an_alter_too(self):
        result = _t("ALTER TABLE dbo.t ADD c NVARCHAR(50)")
        self.assertEqual(result.translated_sql,
                         "ALTER TABLE default.W.t ADD COLUMNS (c STRING)")
        self.assertIn("SQ60_TYPE", _rules(result))

    def test_several_columns_and_a_trailing_semicolon(self):
        result = _t("ALTER TABLE dbo.t ADD a INT, b VARCHAR(10);")
        self.assertEqual(
            result.translated_sql,
            "ALTER TABLE default.W.t ADD COLUMNS (a INT, b STRING);")

    def test_the_nullability_and_constraint_rules_reach_it_too(self):
        """They all read `_column_list_bodies`, so pointing that at the ALTER
        body is what makes the three agree. `ADD COLUMNS (c INT NULL)` is a
        PARSE_SYNTAX_ERROR at 'NULL' on Spark 4.2.0, measured."""
        result = _t("ALTER TABLE dbo.t ADD c INT NULL")
        self.assertEqual(result.translated_sql,
                         "ALTER TABLE default.W.t ADD COLUMNS (c INT)")
        self.assertIn("SQ72_COLUMN_NULL", _rules(result))

        identity = _t("ALTER TABLE dbo.t ADD c INT IDENTITY(1,1)")
        self.assertEqual(identity.translated_sql,
                         "ALTER TABLE default.W.t ADD COLUMNS (c INT)")
        self.assertIn("SQ70_IDENTITY", _rules(identity))

    def test_a_bracketed_column_name_is_still_backticked(self):
        result = _t("ALTER TABLE dbo.t ADD [my col] INT")
        self.assertEqual(
            result.translated_sql,
            "ALTER TABLE default.W.t ADD COLUMNS (`my col` INT)")

    def test_add_constraint_still_goes_to_sq75_whole(self):
        for sql in ("ALTER TABLE dbo.t ADD CONSTRAINT pk PRIMARY KEY (id)",
                    "ALTER TABLE dbo.t ADD PRIMARY KEY (id)"):
            with self.subTest(sql=sql):
                result = _t(sql)
                self.assertIn("SQ75_ALTER_ADD_CONSTRAINT", _rules(result))
                self.assertNotIn("SQ76_ALTER_ADD_COLUMN", _rules(result))

    def test_a_statement_already_in_sparks_form_is_untouched(self):
        result = _t("ALTER TABLE dbo.t ADD COLUMNS (c INT)")
        self.assertEqual(result.translated_sql,
                         "ALTER TABLE default.W.t ADD COLUMNS (c INT)")
        self.assertNotIn("SQ76_ALTER_ADD_COLUMN", _rules(result))

    def test_a_computed_column_is_refused_not_wrapped(self):
        """Spark has no computed columns, so `ADD COLUMNS (c AS a + b)` does
        not parse. Wrapping it and calling that a rewrite would report a
        success for SQL that cannot run."""
        sql = "ALTER TABLE dbo.t ADD c AS (a + b)"
        result = _t(sql)
        self.assertEqual(result.translated_sql,
                         "ALTER TABLE default.W.t ADD c AS (a + b)")
        self.assertIn("SQ76_ALTER_ADD_COMPUTED", _rules(result))
        self.assertNotIn("SQ76_ALTER_ADD_COLUMN", _rules(result))

    def test_a_create_table_in_the_same_file_is_still_cleaned(self):
        result = _t("CREATE TABLE dbo.u (id INT);\n"
                    "ALTER TABLE dbo.u ADD c money")
        self.assertEqual(
            result.translated_sql,
            "CREATE TABLE default.W.u (id INT);\n"
            "ALTER TABLE default.W.u ADD COLUMNS (c money)")
        self.assertIn("SQ60_MONEY", _rules(result))


class SessionSettingTests(unittest.TestCase):
    """F4. `SET QUOTED_IDENTIFIER ON` reached the artifact verbatim.

    Measured before:

        SET QUOTED_IDENTIFIER ON;
        SELECT a FROM dbo.t   -> unchanged apart from SQ11; SQ11 only

    and on Spark 4.2.0 every T-SQL session setting is REJECTED with
    INVALID_SET_SYNTAX ("Expected format is 'SET', 'SET key', or
    'SET key=value'") -- QUOTED_IDENTIFIER ON and OFF, ANSI_NULLS ON,
    NOCOUNT ON, XACT_ABORT ON, ANSI_PADDING ON -- while
    `SET spark.sql.shuffle.partitions=8` is ACCEPTED.

    The choice made here: ON is dropped, everything else is flagged. ON is
    T-SQL's default and is the reading this translator already applies to
    every double-quoted name, so the line asserts what was assumed and
    removing it cannot change a character of the object's meaning. Refusing
    an object over a statement that says "behave normally" would be
    over-refusal; guessing the same for ARITHABORT would not be safe.
    """

    def test_the_reproduction(self):
        result = _t("SET QUOTED_IDENTIFIER ON;\nSELECT a FROM dbo.t",
                    kind="view")
        self.assertEqual(result.translated_sql, "SELECT a FROM default.W.t")
        self.assertIn("SQ14_QUOTED_IDENTIFIER_ON", _rules(result))
        self.assertEqual(result.flags, 0)

    def test_the_finding_says_why_dropping_it_is_safe(self):
        detail = next(f.detail for f in
                      _t("SET QUOTED_IDENTIFIER ON;\nSELECT a FROM dbo.t",
                         kind="view").findings
                      if f.rule == "SQ14_QUOTED_IDENTIFIER_ON")
        self.assertIn("default", detail)
        self.assertIn("INVALID_SET_SYNTAX", detail)

    def test_the_whole_dacfx_header_across_gos(self):
        """DacFx writes two of these, separated by GO. Dropping one and
        leaving the other unflagged would still not run."""
        result = _t("SET ANSI_NULLS ON\nGO\nSET QUOTED_IDENTIFIER ON\nGO\n"
                    "SELECT a FROM dbo.t", kind="view")
        self.assertEqual(result.translated_sql,
                         "SET ANSI_NULLS ON;\nSELECT a FROM default.W.t")
        self.assertIn("SQ14_SESSION_SETTING", _rules(result))
        self.assertGreater(result.flags, 0)

    def test_another_setting_is_flagged_and_left_as_written(self):
        result = _t("SET NOCOUNT ON;\nSELECT a FROM dbo.t", kind="view")
        self.assertEqual(result.translated_sql,
                         "SET NOCOUNT ON;\nSELECT a FROM default.W.t")
        self.assertIn("SQ14_SESSION_SETTING", _rules(result))

    def test_quoted_identifier_off_keeps_its_own_finding_and_is_not_doubled(self):
        result = _t('SET QUOTED_IDENTIFIER OFF;\nSELECT "abc" AS x FROM dbo.t',
                    kind="view")
        self.assertIn("SQ14_QUOTED_IDENTIFIER_OFF", _rules(result))
        self.assertNotIn("SQ14_SESSION_SETTING", _rules(result))
        self.assertNotIn("SQ14_QUOTED_IDENTIFIER_ON", _rules(result))
        self.assertIn("SET QUOTED_IDENTIFIER OFF", result.translated_sql)

    def test_identity_insert_is_not_this_construct(self):
        """`SET IDENTITY_INSERT dbo.t ON` puts a table name where the
        pattern needs ON|OFF, so it is not read as a session setting. It has
        its own id and its own reason -- see IdentityInsertTests below --
        because this rule's sentence is about a bare setting and the two
        cannot share a decision about dropping."""
        sql = "SET IDENTITY_INSERT dbo.t ON;\nSELECT a FROM dbo.t"
        self.assertNotIn("SQ14_SESSION_SETTING", _rules(_t(sql, kind="view")))

    def test_an_object_with_no_set_statement_is_untouched(self):
        result = _t("SELECT a FROM dbo.t", kind="view")
        self.assertEqual(result.translated_sql, "SELECT a FROM default.W.t")
        self.assertNotIn("SQ14_QUOTED_IDENTIFIER_ON", _rules(result))
        self.assertNotIn("SQ14_SESSION_SETTING", _rules(result))


class IdentityInsertTests(unittest.TestCase):
    """`SET IDENTITY_INSERT dbo.t ON` passed completely unflagged.

    MEASURED on 03f019b, `translate(sql, kind="table", item="W")`:

        SET IDENTITY_INSERT dbo.t ON;
        SELECT 1              ->  unchanged                 findings: []

    It is the one session setting that carries an object name, and that is
    exactly why: the table sits where `_SET_OPTION_RE` needs ON|OFF, so
    SQ14's pattern could not see it and nothing else looks for it.

    Spark rejects it in the same class as the settings SQ14 already covers.
    MEASURED on pyspark 4.2.0, JAVA_HOME=openjdk@21, local[1], session
    defaults (spark.version 4.2.0, ansi.enabled true):

        SET IDENTITY_INSERT dbo.t ON        INVALID_SET_SYNTAX
        SET IDENTITY_INSERT t ON            INVALID_SET_SYNTAX
        SET QUOTED_IDENTIFIER ON            INVALID_SET_SYNTAX
        SET spark.sql.shuffle.partitions=8  ACCEPTED

    The README records that `SET QUOTED_IDENTIFIER` is one of the claims
    re-run on 3.5.0 with `ansi.enabled` false and found identical, and this
    is the same parser path rejecting the same shape for the same stated
    reason, so the class is not an artefact of the local version.

    Flagged, never dropped. QUOTED_IDENTIFIER ON is dropped because it
    asserts the reading the translator already applies; this one is a
    *permission*, and removing it changes what a following INSERT is allowed
    to write.
    """

    RULE = "SQ14_IDENTITY_INSERT"

    def test_the_reproduction(self):
        sql = "SET IDENTITY_INSERT dbo.t ON;\nSELECT 1"
        result = _t(sql, kind="table")
        self.assertEqual(_rules(result), [self.RULE])
        self.assertEqual(result.flags, 1)

    def test_the_statement_is_left_exactly_as_written(self):
        """Including the table name. `_OBJECT_KEYWORDS` does not reach it,
        and a rewritten name inside a statement Spark rejects would make the
        line read as translated work."""
        sql = "SET IDENTITY_INSERT dbo.t ON;\nSELECT 1"
        self.assertEqual(_t(sql, kind="table").translated_sql, sql)

    def test_it_is_not_dropped_the_way_quoted_identifier_on_is(self):
        dropped = _t("SET QUOTED_IDENTIFIER ON;\nSELECT 1", kind="table")
        kept = _t("SET IDENTITY_INSERT dbo.t ON;\nSELECT 1", kind="table")
        self.assertNotIn("SET QUOTED_IDENTIFIER", dropped.translated_sql)
        self.assertIn("SET IDENTITY_INSERT", kept.translated_sql)
        self.assertEqual(dropped.flags, 0)
        self.assertEqual(kept.flags, 1)

    def test_off_is_flagged_too(self):
        """Same rejection, and the pair is what a load script writes: ON
        before the INSERT and OFF after it. Flagging one would leave half a
        statement pair in the artifact."""
        result = _t("SET IDENTITY_INSERT dbo.t OFF;\nSELECT 1", kind="table")
        self.assertEqual(_rules(result), [self.RULE])

    def test_one_and_three_part_targets_are_both_found(self):
        for target in ("t", "dbo.t", "db.dbo.t"):
            with self.subTest(target=target):
                result = _t("SET IDENTITY_INSERT %s ON;\nSELECT 1" % target,
                            kind="table")
                self.assertIn(self.RULE, _rules(result))

    def test_the_finding_names_the_table_and_both_readings(self):
        detail = next(f.detail for f in
                      _t("SET IDENTITY_INSERT dbo.t ON;\nSELECT 1",
                         kind="table").findings if f.rule == self.RULE)
        self.assertIn("dbo.t", detail)
        self.assertIn("INVALID_SET_SYNTAX", detail)
        # Why it is not dropped, and the premise SQ70_IDENTITY has moved.
        self.assertIn("permission", detail)
        self.assertIn("SQ70_IDENTITY", detail)
        self.assertIn("GENERATED ALWAYS AS IDENTITY", detail)

    def test_a_set_that_does_not_head_a_statement_is_not_one(self):
        """`UPDATE t SET IDENTITY_INSERT = 1` assigns a column that happens
        to be called that. A session setting always begins a statement."""
        result = _t("UPDATE dbo.t SET IDENTITY_INSERT = 1", kind="table")
        self.assertNotIn(self.RULE, _rules(result))

    def test_a_literal_or_a_comment_is_not_a_statement(self):
        for sql in ("SELECT 'SET IDENTITY_INSERT dbo.t ON' AS s",
                    "-- SET IDENTITY_INSERT dbo.t ON\nSELECT 1"):
            with self.subTest(sql=sql):
                self.assertNotIn(self.RULE, _rules(_t(sql, kind="table")))

    def test_it_does_not_double_report_as_a_session_setting(self):
        result = _t("SET IDENTITY_INSERT dbo.t ON;\nSELECT 1", kind="table")
        self.assertNotIn("SQ14_SESSION_SETTING", _rules(result))
        self.assertNotIn("SQ14_QUOTED_IDENTIFIER_ON", _rules(result))

    def test_it_sits_beside_the_other_settings_in_a_dacfx_header(self):
        result = _t("SET ANSI_NULLS ON\nGO\nSET IDENTITY_INSERT dbo.t ON\nGO\n"
                    "SELECT 1", kind="table")
        self.assertEqual(
            sorted(r for r in set(_rules(result)) if r.startswith("SQ14")),
            ["SQ14_IDENTITY_INSERT", "SQ14_SESSION_SETTING"])


class BracketedTypeNameInAColumnPositionTests(unittest.TestCase):
    """F5. SQ10's type branch tested the WORD and not the position, so a
    column whose name happened to be a type name lost its quoting.

    Measured before:

        SELECT [timestamp], [int] FROM dbo.t
          -> SELECT timestamp, int FROM default.W.t
             SQ10_BRACKET_TYPE x2, both reported as successful rewrites
        SELECT t.[date] FROM dbo.t  -> SELECT t.date FROM default.W.t
        CREATE TABLE t([int] INT)   -> CREATE TABLE t(int INT)

    Measured on pyspark 4.2.0 (JAVA_HOME=openjdk@21, local[1]) against a
    view that really has a column of each name, over all 37 names in
    `_KNOWN_TYPE_NAMES`:

        ansi.enabled=true, enforceReservedKeywords=false (both defaults)
          SELECT <name> FROM x     ACCEPTED for all 37
        enforceReservedKeywords=true
          SELECT time FROM x       PARSE_SYNTAX_ERROR at or near 'time'
          the other 36 -- int, timestamp, date among them -- ACCEPTED
        ansi.enabled=false + enforceReservedKeywords=true
          SELECT time FROM x       ACCEPTED
        `` `time` ``               ACCEPTED in every combination

    One name of thirty-seven under one non-default setting: narrower than
    the finding claimed, and not nothing. The backtick is right under every
    configuration, so the rule emits one instead of deciding.
    """

    def test_the_reproduction(self):
        result = _t("SELECT [timestamp], [int] FROM dbo.t")
        self.assertEqual(result.translated_sql,
                         "SELECT `timestamp`, `int` FROM default.W.t")
        self.assertNotIn("SQ10_BRACKET_TYPE", _rules(result))
        self.assertEqual(_rules(result).count("SQ10_BRACKET_IDENT"), 2)

    def test_a_qualified_column_and_an_order_by(self):
        self.assertEqual(_t("SELECT t.[date] FROM dbo.t").translated_sql,
                         "SELECT t.`date` FROM default.W.t")
        self.assertEqual(
            _t("SELECT a FROM dbo.t ORDER BY [date]").translated_sql,
            "SELECT a FROM default.W.t ORDER BY `date`")

    def test_a_column_named_for_a_type_keeps_its_quoting(self):
        """The name slot of a column definition, which is the clearest case:
        the rule that exists to quote identifiers was unquoting one."""
        self.assertEqual(_t("CREATE TABLE t([int] INT)").translated_sql,
                         "CREATE TABLE t(`int` INT)")

    def test_a_double_quoted_one_too(self):
        self.assertEqual(_t('SELECT "int" FROM dbo.t').translated_sql,
                         "SELECT `int` FROM default.W.t")

    def test_the_type_slot_of_a_column_definition_still_unwraps(self):
        """What the position-blind test was there for. `` `int` `` as a type
        is UNSUPPORTED_DATATYPE on Spark."""
        for sql, expected in (
                ("CREATE TABLE CUSTOMER([CustomerID] [int] NOT NULL)",
                 "CREATE TABLE CUSTOMER(`CustomerID` int NOT NULL)"),
                ("CREATE TABLE t([a] [nvarchar](50) NOT NULL)",
                 "CREATE TABLE t(`a` STRING NOT NULL)"),
                ("CREATE TABLE t([a] [numeric](7,2))",
                 "CREATE TABLE t(`a` DECIMAL(7,2))")):
            with self.subTest(sql=sql):
                self.assertEqual(_t(sql).translated_sql, expected)

    def test_a_cast_and_a_convert_type_slot_still_unwrap(self):
        self.assertEqual(_t("SELECT CAST(a AS [int]) FROM dbo.t").translated_sql,
                         "SELECT CAST(a AS int) FROM default.W.t")
        self.assertEqual(
            _t("SELECT CONVERT([varchar](10), a) FROM dbo.t").translated_sql,
            "SELECT substring(CAST(a AS STRING), 1, 10) FROM default.W.t")
        self.assertEqual(
            _t("SELECT TRY_CONVERT([varchar](10), a) FROM dbo.t").translated_sql,
            "SELECT substring(try_cast(a AS STRING), 1, 10) FROM default.W.t")

    def test_an_escaped_bracket_in_a_column_name_no_longer_truncates_the_body(self):
        """Found while writing `_type_positions`, which needs the body
        bounds. `_closing_index` counts brackets, and `[a]]b]` holds one it
        must not count, so `_column_list_bodies` reported the body as `[a]]`
        and every column rule stopped four characters in. Measured before:

            CREATE TABLE dbo.t ([a]]b] [nvarchar](50) NOT NULL)
              -> CREATE TABLE default.W.t (`a]b` nvarchar(50) NOT NULL)
                 no SQ60 finding -- unrunnable DDL, reported clean
            CREATE TABLE dbo.t ([a]]b] money NOT NULL)
              -> ... `a]b` money ...   no SQ60 finding either
        """
        result = _t("CREATE TABLE dbo.t ([a]]b] [nvarchar](50) NOT NULL)")
        self.assertEqual(result.translated_sql,
                         "CREATE TABLE default.W.t (`a]b` STRING NOT NULL)")
        self.assertIn("SQ60_TYPE", _rules(result))
        self.assertIn("SQ60_MONEY",
                      _rules(_t("CREATE TABLE dbo.t ([a]]b] money NOT NULL)")))

    def test_a_constraint_name_is_not_read_as_a_type(self):
        """The reason the position test is "the second token of the item"
        and not "a quoted token after an identifier"."""
        self.assertEqual(
            _t("CREATE TABLE t([a] INT CONSTRAINT [date] DEFAULT 0)"
               ).translated_sql,
            "CREATE TABLE t(`a` INT DEFAULT 0)")

    def test_the_alter_add_type_slot_unwraps_too(self):
        """SQ76 has not run yet when SQ10 does, so `_type_positions` has to
        know the un-parenthesised T-SQL shape as well."""
        self.assertEqual(_t("ALTER TABLE dbo.t ADD [c] [int]").translated_sql,
                         "ALTER TABLE default.W.t ADD COLUMNS (`c` int)")


class ColumnTypeLengthLossTests(unittest.TestCase):
    """F6, the half of it that stands. The cast half is closed -- measured:

        CAST(a AS NVARCHAR(50))    -> substring(CAST(a AS STRING), 1, 50)
        CONVERT(VARCHAR(10), a)    -> substring(CAST(a AS STRING), 1, 10)
        TRY_CONVERT(VARCHAR(10),a) -> substring(try_cast(a AS STRING), 1, 10)

    The column half is not. `CREATE TABLE dbo.t (n NVARCHAR(50))` becomes
    `(n STRING)` and the finding read, in full, `NVARCHAR(50) -> STRING`:
    two type names and nothing about the difference between them.

    The decision made here is STRING with the cost written into the finding,
    and the reason is in the finding too, because Spark 4 DOES have
    VARCHAR(n) and a reader will ask. Measured on pyspark 4.2.0
    (JAVA_HOME=openjdk@21, local[1], `USING parquet`):

        CREATE TABLE v1 (n VARCHAR(5)) USING parquet    ACCEPTED
        DESCRIBE TABLE v1                               n  varchar(5)
        INSERT INTO v1 VALUES ('abcdefghij')            EXCEED_LIMIT_LENGTH
        the same with legacy.charVarcharAsString=true   ACCEPTED, stored whole
        CREATE TABLE v2 (n VARCHAR(max))                PARSE_SYNTAX_ERROR
        SELECT CAST('abcdefghij' AS VARCHAR(5))         'abcdefghij', string
        CHAR(5) column given 'ab'                       reads back '[ab   ]'

    One position out of four, under one default setting. Emitting VARCHAR(n)
    for a column while SQ61 emits STRING for a cast would make the two
    disagree, and `nvarchar(50)` and `nvarchar(max)` would become two
    different Spark types for one T-SQL concept.
    """

    def test_the_detail_is_no_longer_a_bare_arrow(self):
        detail = next(f.detail for f in
                      _t("CREATE TABLE dbo.t (n NVARCHAR(50))").findings
                      if f.rule == "SQ60_TYPE")
        self.assertNotEqual(detail, "NVARCHAR(50) -> STRING")
        self.assertTrue(detail.startswith("NVARCHAR(50) -> STRING;"))

    def test_the_detail_says_what_the_loss_costs(self):
        detail = next(f.detail for f in
                      _t("CREATE TABLE dbo.t (n NVARCHAR(50))").findings
                      if f.rule == "SQ60_TYPE")
        for phrase in ("length is dropped", "refuses a longer value",
                       "used to fail now succeeds"):
            with self.subTest(phrase=phrase):
                self.assertIn(phrase, detail)

    def test_the_detail_says_why_it_is_still_string(self):
        """Spark 4 has VARCHAR(n); the finding has to answer that, and with
        the measurement rather than an assertion."""
        detail = next(f.detail for f in
                      _t("CREATE TABLE dbo.t (n NVARCHAR(50))").findings
                      if f.rule == "SQ60_TYPE")
        for phrase in ("EXCEED_LIMIT_LENGTH",
                       "spark.sql.legacy.charVarcharAsString",
                       "varchar(max)"):
            with self.subTest(phrase=phrase):
                self.assertIn(phrase, detail)

    def test_a_fixed_length_type_also_names_the_padding(self):
        for sql in ("CREATE TABLE dbo.t (c CHAR(3))",
                    "CREATE TABLE dbo.t (c NCHAR(3))"):
            with self.subTest(sql=sql):
                detail = next(f.detail for f in _t(sql).findings
                              if f.rule == "SQ60_TYPE")
                self.assertIn("blank padding", detail)

    def test_max_keeps_the_bare_arrow_because_nothing_is_lost(self):
        """Neither `nvarchar(max)` nor STRING has a maximum, so there is no
        cost to describe and inventing one would be noise."""
        for sql, expected in (
                ("CREATE TABLE dbo.t (n NVARCHAR(MAX))",
                 "NVARCHAR(MAX) -> STRING"),
                ("CREATE TABLE dbo.t (n VARCHAR(max))",
                 "VARCHAR(max) -> STRING")):
            with self.subTest(sql=sql):
                detail = next(f.detail for f in _t(sql).findings
                              if f.rule == "SQ60_TYPE")
                self.assertEqual(detail, expected)

    def test_a_type_with_no_length_is_untouched(self):
        for sql, expected in (("CREATE TABLE dbo.t (n TEXT)",
                               "TEXT -> STRING"),
                              ("CREATE TABLE dbo.t (d DATETIME)",
                               "DATETIME -> TIMESTAMP")):
            with self.subTest(sql=sql):
                detail = next(f.detail for f in _t(sql).findings
                              if f.rule == "SQ60_TYPE")
                self.assertEqual(detail, expected)

    def test_the_emitted_sql_is_unchanged(self):
        """A detail change, not a mapping change. STRING stays STRING."""
        self.assertEqual(_t("CREATE TABLE dbo.t (n NVARCHAR(50))").translated_sql,
                         "CREATE TABLE default.W.t (n STRING)")

    def test_the_cast_half_of_the_finding_is_closed(self):
        """REFUTED, and pinned so it stays refuted."""
        for sql, expected in (
                ("SELECT CAST(a AS NVARCHAR(50)) FROM dbo.t",
                 "SELECT substring(CAST(a AS STRING), 1, 50) FROM default.W.t"),
                ("SELECT CONVERT(VARCHAR(10), a) FROM dbo.t",
                 "SELECT substring(CAST(a AS STRING), 1, 10) FROM default.W.t"),
                ("SELECT TRY_CONVERT(VARCHAR(10), a) FROM dbo.t",
                 "SELECT substring(try_cast(a AS STRING), 1, 10) "
                 "FROM default.W.t")):
            with self.subTest(sql=sql):
                result = _t(sql)
                self.assertEqual(result.translated_sql, expected)
                self.assertIn("SQ61_CAST_LENGTH", _rules(result))


class PivotValueListTests(unittest.TestCase):
    """F8. A T-SQL PIVOT's `IN ([a],[b])` list holds VALUES, and SQ10
    backticked them into column references and called it two rewrites.

    Measured before:

        SELECT * FROM dbo.t PIVOT (SUM(v) FOR k IN ([a],[b])) p
          -> SELECT * FROM default.W.t PIVOT (SUM(v) FOR k IN (`a`,`b`)) p
             SQ10_BRACKET_IDENT x2, both `rewrite`

    Measured on Spark 4.2.0 (pyspark 4.2.0, JAVA_HOME=openjdk@21, local[1]),
    one session:

        PIVOT   (SUM(v) FOR k IN (`a`,`b`))  REJECTED UNRESOLVED_COLUMN
        PIVOT   (SUM(v) FOR k IN ('a','b'))  ACCEPTED
        UNPIVOT (v FOR k IN (`a`,`b`))       ACCEPTED
        UNPIVOT (v FOR k IN ('a','b'))       REJECTED PARSE_SYNTAX_ERROR

    The last two are why the exception is scoped to PIVOT and not to "an IN
    list": UNPIVOT's really does hold column names, in both dialects, and a
    broader exception would break it the other way round.
    """

    def test_the_reproduction(self):
        result = _t("SELECT * FROM dbo.t PIVOT (SUM(v) FOR k IN ([a],[b])) p",
                    kind="view")
        self.assertEqual(
            result.translated_sql,
            "SELECT * FROM default.W.t PIVOT (SUM(v) FOR k IN ('a','b')) p")
        self.assertEqual(_rules(result).count("SQ53_PIVOT_VALUE"), 2)
        self.assertNotIn("SQ10_BRACKET_IDENT", _rules(result))

    def test_an_unpivot_list_is_still_backquoted(self):
        result = _t("SELECT * FROM dbo.t UNPIVOT (v FOR k IN ([a],[b])) p",
                    kind="view")
        self.assertEqual(
            result.translated_sql,
            "SELECT * FROM default.W.t UNPIVOT (v FOR k IN (`a`,`b`)) p")
        self.assertNotIn("SQ53_PIVOT_VALUE", _rules(result))

    def test_the_pivot_column_itself_is_still_an_identifier(self):
        """Only the IN list holds values. The column being pivoted FOR is a
        column, and so is anything inside the aggregate."""
        result = _t("SELECT * FROM dbo.t "
                    "PIVOT (SUM([my val]) FOR [my key] IN ([a b],[c])) p",
                    kind="view")
        self.assertEqual(
            result.translated_sql,
            "SELECT * FROM default.W.t "
            "PIVOT (SUM(`my val`) FOR `my key` IN ('a b','c')) p")

    def test_an_in_predicate_inside_the_aggregate_is_not_the_value_list(self):
        """The value list is the IN after the FOR, at the PIVOT's own depth.
        "The first IN in the group" measured wrong here."""
        result = _t("SELECT * FROM dbo.t PIVOT "
                    "(SUM(CASE WHEN x IN (1,2) THEN v END) FOR k IN ([a])) p",
                    kind="view")
        self.assertEqual(
            result.translated_sql,
            "SELECT * FROM default.W.t PIVOT "
            "(SUM(CASE WHEN x IN (1,2) THEN v END) FOR k IN ('a')) p")

    def test_an_ordinary_in_predicate_elsewhere_is_untouched(self):
        result = _t("SELECT [a] FROM dbo.t WHERE k IN (1,2)", kind="view")
        self.assertEqual(result.translated_sql,
                         "SELECT `a` FROM default.W.t WHERE k IN (1,2)")
        self.assertNotIn("SQ53_PIVOT_VALUE", _rules(result))

    def test_a_double_quoted_value_too(self):
        """QUOTED_IDENTIFIER ON makes `"a"` an identifier in T-SQL, so DacFx
        can write the value list either way."""
        self.assertEqual(
            _t('SELECT * FROM dbo.t PIVOT (SUM(v) FOR k IN ("a","b")) p',
               kind="view").translated_sql,
            "SELECT * FROM default.W.t PIVOT (SUM(v) FOR k IN ('a','b')) p")

    def test_two_pivots_in_one_statement(self):
        result = _t("SELECT * FROM (SELECT k,v FROM dbo.t) s "
                    "PIVOT (SUM(v) FOR k IN ([a])) p "
                    "PIVOT (MAX(x) FOR y IN ([z])) q", kind="view")
        self.assertEqual(_rules(result).count("SQ53_PIVOT_VALUE"), 2)
        self.assertIn("IN ('a')", result.translated_sql)
        self.assertIn("IN ('z')", result.translated_sql)

    def test_an_apostrophe_in_a_value_is_escaped_for_spark(self):
        """`[it''s]` is the T-SQL identifier `it''s` -- inside brackets only
        `]]` is an escape -- and the literal has to come out spelled for
        Spark's lexer, which SQ16 does on the last pass."""
        out = _t("SELECT * FROM dbo.t PIVOT (SUM(v) FOR k IN ([it''s])) p",
                 kind="view").translated_sql
        self.assertIn(r"IN ('it\'\'s')", out)


class UnreadableWarehouseFilesTests(unittest.TestCase):
    """F7. Closed by #25, re-measured here, and pinned so it stays closed.

    The finding was that three undecodable `.sql` files produced three
    `<unreadable: ...>` objects and `unreadable_items: []`. Measured on this
    tree with three `.sql` files holding a 0xE9 byte in a Warehouse item:

        unreadable_items                                 []
        warehouse.summary.unreadable_object_count        3
        each object's `read_error`   "'utf-8' codec can't decode byte 0xe9 ..."
        the plan                     three assets, one per file
        summarize()                  "warehouse  warehouse_count=1,
                                      unreadable_object_count=3"

    So the count, the per-object reason and the per-file asset all exist.
    `unreadable_items: []` is the CORRECT answer -- no item directory was
    unreadable -- and what remains is that the name reads like "files that
    could not be read". That is documented rather than renamed: the key is
    in every manifest already written and `planner._unreadable_assets` looks
    it up by name, so renaming it would drop those assets out of a plan
    built from an existing inventory.json without a word. See
    `RunbookTests.test_it_tells_the_two_unreadable_counts_apart`.
    """

    @classmethod
    def setUpClass(cls):
        from pathlib import Path
        from tempfile import TemporaryDirectory

        from fabric_aidp.inventory.manifest import ALL_SOURCES, build_manifest
        from fabric_aidp.plan.planner import build_plan

        cls._tmp = TemporaryDirectory()
        item = Path(cls._tmp.name) / "W.Warehouse"
        item.mkdir(parents=True)
        (item / ".platform").write_bytes(
            b'{"metadata": {"type": "Warehouse", "displayName": "W"}}')
        for stem in ("a", "b", "c"):
            # 0xE9 is not valid UTF-8 and not valid UTF-16 with a BOM, so
            # `_detect_encoding` cannot rescue it.
            (item / (stem + ".sql")).write_bytes(
                b"CREATE TABLE dbo.t\xe9 (a INT)")
        cls.manifest = build_manifest(Path(cls._tmp.name), ALL_SOURCES)
        cls.plan = build_plan(cls.manifest, oci_namespace="ns")

    @classmethod
    def tearDownClass(cls):
        cls._tmp.cleanup()

    def test_the_files_are_counted(self):
        self.assertEqual(
            self.manifest["sources"]["warehouse"]["summary"][
                "unreadable_object_count"], 3)

    def test_each_object_carries_its_own_reason(self):
        objects = (self.manifest["sources"]["warehouse"]["items"]
                   ["warehouses"][0]["objects"])
        self.assertEqual(len(objects), 3)
        for obj in objects:
            with self.subTest(name=obj["name"]):
                self.assertIn("codec", obj["read_error"])

    def test_each_file_is_still_its_own_asset_in_the_plan(self):
        ids = {a["id"] for a in self.plan["assets"]}
        self.assertEqual(
            sorted(i for i in ids if i.startswith("warehouse.")),
            ["warehouse.W.other.a.sql", "warehouse.W.other.b.sql",
             "warehouse.W.other.c.sql"])

    def test_unreadable_items_is_empty_and_that_is_correct(self):
        """No item DIRECTORY was unreadable -- `W.Warehouse` read fine. The
        empty list is the right answer to the question the key asks, and the
        wrong answer to the question its name suggests."""
        self.assertEqual(self.manifest["unreadable_items"], [])

    def test_the_summary_line_an_operator_reads_shows_the_count(self):
        from fabric_aidp.inventory.manifest import summarize

        self.assertIn("unreadable_object_count=3", summarize(self.manifest))


class TruncatedSqlFileTests(unittest.TestCase):
    """Verify-or-refute item 1: "inventory does not use `unterminated_span`,
    so a truncated `.sql` still classifies as ("other","","")".

    CONFIRMED, and the filed shape is the mild half of it. Measured before,
    with `classify_sql_object`:

        /* header                          -> ('other', '', '')
        CREATE TABLE dbo.address (id INT)     the CREATE is inside the comment

        CREATE TABLE dbo.[claim (id INT)   -> ('table', 'dbo', 'dbo')
        CREATE TABLE dbo.[policy (id INT)  -> ('table', 'dbo', 'dbo')

        CREATE TABLE dbo.claim (id INT, n VARCHAR(10) DEFAULT 'abc
                                           -> ('table', 'dbo', 'claim')

    The second pair is the one that costs something, and it is worse than
    ("other","",""): the unterminated `[` swallows the name, the schema is
    read as the name, two objects collapse onto one asset id, and

        build_plan(...) -> ValueError: duplicate asset id(s):
                           warehouse.W.table.dbo.dbo

    refuses the WHOLE workspace over two malformed files -- the same end
    `_name_from_path` was written to prevent for the undecodable case. The
    third is the filed case and is REFUTED: a break after the header does
    not make the header wrong, and that classification is correct.
    """

    @classmethod
    def setUpClass(cls):
        from pathlib import Path
        from tempfile import TemporaryDirectory

        from fabric_aidp.inventory.manifest import ALL_SOURCES, build_manifest
        from fabric_aidp.plan.planner import build_plan

        cls._tmp = TemporaryDirectory()
        item = Path(cls._tmp.name) / "W.Warehouse"
        item.mkdir(parents=True)
        (item / ".platform").write_text(
            '{"metadata": {"type": "Warehouse", "displayName": "W"}}',
            encoding="utf-8")
        for stem, body in (
                ("claim", "CREATE TABLE dbo.[claim (id INT)\n"),
                ("policy", "CREATE TABLE dbo.[policy (id INT)\n"),
                ("address", "/* header\nCREATE TABLE dbo.address (id INT)\n")):
            (item / (stem + ".sql")).write_text(body, encoding="utf-8")
        cls.manifest = build_manifest(Path(cls._tmp.name), ALL_SOURCES)
        cls.plan = build_plan(cls.manifest, oci_namespace="ns")
        cls.objects = (cls.manifest["sources"]["warehouse"]["items"]
                       ["warehouses"][0]["objects"])

    @classmethod
    def tearDownClass(cls):
        cls._tmp.cleanup()

    def test_a_break_inside_the_name_is_no_longer_classified(self):
        from fabric_aidp.inventory import warehouse as wh

        for sql in ("CREATE TABLE dbo.[claim (id INT)",
                    'CREATE TABLE dbo."claim (id INT)'):
            with self.subTest(sql=sql):
                self.assertEqual(wh.classify_sql_object(sql), ("other", "", ""))

    def test_a_break_after_the_header_still_classifies(self):
        """REFUTED half. The header is whole, so the answer is right and
        throwing it away would lose real information."""
        from fabric_aidp.inventory import warehouse as wh

        self.assertEqual(
            wh.classify_sql_object(
                "CREATE TABLE dbo.claim (id INT, n VARCHAR(10) DEFAULT 'abc"),
            ("table", "dbo", "claim"))
        self.assertEqual(
            wh.classify_sql_object("CREATE VIEW dbo.v AS SELECT 'x"),
            ("view", "dbo", "v"))

    def test_an_apostrophe_in_a_line_comment_is_not_a_break(self):
        from fabric_aidp.inventory import warehouse as wh

        self.assertEqual(
            wh.classify_sql_object(
                "-- note 'oops\nCREATE TABLE dbo.claim (id INT)"),
            ("table", "dbo", "claim"))

    def test_the_malformation_is_counted_and_named_per_object(self):
        self.assertEqual(
            self.manifest["sources"]["warehouse"]["summary"][
                "unterminated_object_count"], 3)
        by_file = {o["file"]: o for o in self.objects}
        self.assertIn("an identifier opened at offset 17",
                      by_file["claim.sql"]["unterminated"])
        self.assertIn("a block comment opened at offset 0",
                      by_file["address.sql"]["unterminated"])

    def test_a_clean_object_carries_an_empty_unterminated(self):
        from fabric_aidp.inventory import warehouse as wh

        out = wh.scan(_one_warehouse({"t.sql": "CREATE TABLE dbo.t (id INT)"}))
        obj = out["items"]["warehouses"][0]["objects"][0]
        self.assertEqual(obj["unterminated"], "")
        self.assertEqual(out["summary"]["unterminated_object_count"], 0)

    def test_the_plan_no_longer_refuses_the_whole_workspace(self):
        self.assertEqual(
            sorted(a["id"] for a in self.plan["assets"]),
            ["warehouse.W.other.address.sql", "warehouse.W.other.claim.sql",
             "warehouse.W.other.policy.sql"])

    def test_migrate_still_flags_each_one(self):
        from pathlib import Path
        from tempfile import TemporaryDirectory

        from fabric_aidp.migrate.runner import migrate

        with TemporaryDirectory() as out:
            report = migrate(self.plan, out_dir=Path(out))
        fired = {row["asset_id"]: [f["rule"] for f in (row["findings"] or [])]
                 for row in report["results"]}
        self.assertEqual(fired["warehouse.W.other.claim.sql"],
                         ["SQ01_UNTERMINATED_IDENTIFIER"])
        self.assertEqual(fired["warehouse.W.other.address.sql"],
                         ["SQ01_UNTERMINATED_COMMENT"])
        self.assertTrue(all(row["status"] == "needs_manual_review"
                            for row in report["results"]))


def _passes_used(texts):
    """The deepest pass count `_to_fixed_point` reaches over `texts`.

    The loop is re-run here rather than counted inside the module: an
    instrument left in the shipped function would be a cost paid on every
    object forever, to record a number nobody reads at run time.
    """
    deepest = 0
    original = tsql._to_fixed_point

    def counting(sql, findings, collect, max_passes=8):
        nonlocal deepest
        before = len(findings)
        used, reported, result = 0, set(), sql

        def take(items):
            edits = []
            for item in items:
                if item[0] == item[1]:
                    finding = item[3]
                    if finding is not None:
                        key = (finding.rule, finding.detail)
                        if key not in reported:
                            reported.add(key)
                    continue
                edits.append(item)
            return edits

        for index in range(max_passes):
            items = tsql._outermost(take(collect(result)))
            if not items:
                break
            used = index + 1
            result = tsql.apply_replacements(
                result, [(a, b, c) for a, b, c, _f in items])
        deepest = max(deepest, used)
        del findings[before:]
        return original(sql, findings, collect, max_passes)

    tsql._to_fixed_point = counting
    try:
        for text in texts:
            for kind in ("table", "view", "other"):
                tsql.translate(text, kind=kind, item="W")
    finally:
        tsql._to_fixed_point = original
    return deepest


class PassBudgetIsASafetyValveTests(unittest.TestCase):
    """Verify-or-refute item 2: "the 8-pass fixed-point budget is arbitrary".

    CONFIRMED, and left arbitrary on purpose. The instruction was to measure
    the depth the corpora reach and write that number down, or to say
    plainly that 8 is a safety valve -- and the measurement does not justify
    8, so `_to_fixed_point`'s docstring now says both: here are the numbers,
    and no, they do not add up to 8.

    Measured with the loop instrumented to record passes actually used:

        a full migrate of the 106-asset demo estate
          228 calls -- 224 used 0 passes, 4 used 1.   Deepest: 1
        the 9 vendored real Warehouse objects
          0 passes on every one
        the whole test suite, 10775 calls
          10448 x 0, 298 x 1, 23 x 2, 4 x 3, and 2 that exhaust the budget
          -- those two being ten nested ISNULLs written to make SQ03 fire

    These two assertions re-take the first two, so the numbers in the
    comment cannot go stale without something going red.
    """

    def test_the_bundled_estate_reaches_one_pass(self):
        from fabric_aidp.fixtures import demo_workspace_path

        texts = [path.read_text(encoding="utf-8-sig")
                 for path in sorted(demo_workspace_path().rglob("*.sql"))]
        self.assertTrue(texts, "the bundled workspace has no .sql files")
        self.assertEqual(
            _passes_used(texts), 1,
            "the bundled estate's fixed-point depth moved; re-measure and "
            "update the numbers in _to_fixed_point's docstring")

    def test_the_vendored_real_warehouse_objects_reach_none(self):
        from pathlib import Path

        corpus = Path(__file__).resolve().parent / "fixtures" / "real" / \
            "corpora" / "warehouse"
        texts = [path.read_text(encoding="utf-8-sig")
                 for path in sorted(corpus.glob("*.sql"))]
        self.assertTrue(texts, "the vendored warehouse corpus is missing")
        self.assertEqual(
            _passes_used(texts), 0,
            "the vendored corpus's fixed-point depth moved; re-measure and "
            "update the numbers in _to_fixed_point's docstring")

    def test_the_measured_depth_is_nowhere_near_the_budget(self):
        """The point of the two numbers above: the budget is not close to
        binding on anything observed, so it cannot have been chosen from
        them."""
        import inspect

        self.assertEqual(
            inspect.signature(tsql._to_fixed_point)
            .parameters["max_passes"].default, 8)

    def test_the_docstring_does_not_claim_the_number_is_measured(self):
        """The one thing the finding asked for that prose can get wrong. A
        future edit that turns "safety valve" into "because the corpus
        reaches 3" would be the invented justification this was filed
        against."""
        doc = tsql._to_fixed_point.__doc__ or ""
        self.assertIn("SAFETY VALVE, NOT A MEASURED BOUND", doc)
        self.assertIn("does not follow", doc)


def _one_warehouse(files):
    """A throwaway W.Warehouse item list holding `files`, for `warehouse.scan`."""
    import tempfile
    from pathlib import Path

    from fabric_aidp.inventory.git_workspace import discover_items

    root = Path(tempfile.mkdtemp())
    item = root / "W.Warehouse"
    item.mkdir()
    (item / ".platform").write_text(
        '{"metadata": {"type": "Warehouse", "displayName": "W"}}',
        encoding="utf-8")
    for name, body in files.items():
        (item / name).write_text(body, encoding="utf-8")
    return discover_items(root)


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
