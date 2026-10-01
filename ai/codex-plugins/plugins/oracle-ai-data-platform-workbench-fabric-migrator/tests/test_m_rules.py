import ast
import unittest

from fabric_aidp.translate import m_runtime, m_to_pyspark as m
from tests.test_m_runtime import spark_available


def _step(name, fn, args=(), nav=(), inputs=(), raw=""):
    return {"name": name, "fn": fn, "args": list(args), "nav": list(nav),
            "inputs": list(inputs), "raw": raw}


SOURCE = [
    _step("Pattern", "Lakehouse.Contents", ["[]"]),
    _step("Nav1", "Pattern", nav=['{[lakehouseId = "lh-1"]}', "[Data]"]),
    _step("Nav2", "Nav1", nav=['{[Id = "claim", ItemKind = "Table"]}', "[Data]"]),
]
CATALOG = {"lh-1": "sales"}
# What `{"b", Int64.Type}` emits: rounded half to even, then cast.
# `Int64.Type` is not a cast; it goes through the generated `_m_int64`,
# which rounds half to even and returns NULL outside bigint's range. See
# `_m_int64` in m_runtime.py for the measurements behind both.
INT64_B = '_m_int64(F.col("b"))'


def _query(*steps, **kwargs):
    steps = list(SOURCE) + list(steps)
    return dict({"name": "Q", "attrs": None, "steps": steps,
                 "final": steps[-1]["name"]}, **kwargs)


def _translate(*steps, **kwargs):
    lakehouses = kwargs.pop("lakehouses", CATALOG)
    return m.translate_query(_query(*steps, **kwargs), lakehouses=lakehouses)


class SanitizeTests(unittest.TestCase):
    def test_quoted_name_with_spaces_becomes_an_identifier(self):
        self.assertEqual(m.sanitize('#"Promoted headers"'), "promoted_headers")

    def test_leading_digit_is_prefixed(self):
        self.assertEqual(m.sanitize('#"1st step"'), "step_1st_step")

    def test_python_keyword_is_suffixed(self):
        self.assertEqual(m.sanitize("class"), "class_")

    def test_punctuation_only_name_still_yields_an_identifier(self):
        self.assertTrue(m.sanitize('#"-----PARAMETERS-----"').isidentifier())

    def test_isalnum_but_not_identifier_characters_are_legal(self):
        # `\W` keeps `³` and `½` (str.isalnum() is True) but Python rejects them.
        for name in ('#"Größe³"', '#"Anteil ½"', '#"m²"', '#"Ⅻ total"'):
            self.assertTrue(m.sanitize(name).isidentifier(), name)

    def test_names_python_would_normalize_together_collide_here(self):
        # Python NFKC-normalizes identifiers; _unique_names must see the clash.
        self.assertEqual(m.sanitize('#"ﬁle"'), m.sanitize("file"))
        self.assertEqual(m.sanitize('#"x³"'), "x3")

    def test_non_ascii_letters_are_kept(self):
        self.assertEqual(m.sanitize('#"Kundenübersicht"'), "kundenübersicht")


class ClassifyTests(unittest.TestCase):
    def test_destination_helper_is_a_helper(self):
        self.assertEqual(m.classify({"name": "T_DataDestination", "steps": SOURCE}),
                         "helper")

    def test_stepless_query_is_a_parameter(self):
        self.assertEqual(m.classify({"name": "d", "steps": []}), "parameter")

    def test_ordinary_query_is_a_pipeline(self):
        self.assertEqual(m.classify({"name": "T", "steps": SOURCE}), "pipeline")

    def test_helper_emits_nothing(self):
        result = m.translate_query({"name": "T_DataDestination", "attrs": None,
                                    "steps": SOURCE, "final": "Nav2"})
        self.assertEqual(result.translated_sql, "")
        self.assertEqual([f.rule for f in result.findings], ["M13_SUPPRESS_HELPER"])


# `parse.js` gives a non-`let` member `steps: []` and writes down why in
# `note`. These are the three shapes the corpus actually holds.
def _member(name, raw, kind="RecursivePrimaryExpression", attrs=None):
    return {"name": name, "attrs": attrs, "steps": [], "raw": raw,
            "note": "unsupported member kind %s" % kind}


_DEFAULT_SETTINGS = (
    '[DefaultOutputDestinationSettings = [DestinationDefinition = '
    '[Kind = "Reference", QueryName = "DefaultDestination"]]]')


class UnreadMemberTests(unittest.TestCase):
    """A non-`let` member was called a parameter and vanished.

    `classify` was "no steps, therefore a parameter", and only a `pipeline`
    becomes a plan asset -- so a complete query written without `let` left
    no file, no asset and no finding anywhere. `parse.js` had already
    recorded the member's AST kind in `note`; nothing read it.
    """

    def test_a_query_written_without_let_is_not_a_parameter(self):
        # The pure case: a table-producing expression, no `let`.
        self.assertEqual(
            m.classify(_member("RealPipeline",
                               'Table.FromRows({{1,"a"}}, {"id","name"})')),
            "unread_member")

    def test_a_custom_m_function_is_not_a_parameter(self):
        self.assertEqual(
            m.classify(_member("fnClean", "(tbl as table) as table => let x = 1 in x",
                               kind="FunctionExpression")),
            "unread_member")

    def test_a_scalar_this_tool_can_read_is_still_a_parameter(self):
        # `#date(2026, 1, 1)` is a RecursivePrimaryExpression, exactly like
        # `Table.FromRows(...)` above -- the AST kind cannot tell them apart,
        # so the verdict comes from m_expr reading the value or failing to.
        self.assertEqual(m.classify(_member("load_cutoff", "#date(2026, 1, 1)")),
                         "parameter")

    def test_fabrics_own_parameter_marker_is_believed(self):
        # `meta [...]` is refused by m_expr, so the marker is the only proof.
        self.assertEqual(
            m.classify(_member(
                "data_inicial",
                '#date(2024, 11, 1) meta [IsParameterQuery=true, Type="Date"]',
                kind="MetadataExpression")),
            "parameter")

    def test_the_sections_default_destination_member_is_a_write_target(self):
        """10 of the 39 corpus files carry one and none is named
        `*_DataDestination`, so the suffix rule missed every one."""
        query = _member("DefaultDestination",
                        'Lakehouse.Contents(null){[lakehouseId = "lh-1"]}[Data]')
        self.assertEqual(m.classify(query), "unread_member")
        self.assertEqual(m.classify(query, _DEFAULT_SETTINGS), "helper")

    def test_an_unread_member_is_reported_rather_than_silently_dropped(self):
        result = m.translate_query(
            _member("Absence", 'Table.Group(AbsenceSource, {"LeaveTypeId"}, {})'))
        self.assertEqual(result.translated_sql, "")
        self.assertEqual([f.rule for f in result.findings], ["M19_UNREAD_MEMBER"])
        detail = result.findings[0].detail
        # The finding has to carry the two things a human needs: what the
        # member is, and enough of its text to find it in the mashup.
        self.assertIn("RecursivePrimaryExpression", detail)
        self.assertIn("Table.Group", detail)

    def test_an_unread_member_is_a_flag_not_a_rewrite(self):
        """A helper and a parameter emit nothing because there is nothing to
        emit; this emits nothing because the member was not read. Grading the
        two the same is what made the loss invisible."""
        result = m.translate_query(_member("X", "Table.FromRows({{1}})"))
        self.assertEqual([f.severity for f in result.findings], ["flag"])
        self.assertTrue(result.flags)


class SourceTests(unittest.TestCase):
    def test_resolved_lakehouse_emits_a_three_part_name_and_no_flag(self):
        # Assert on the rule, not on result.flags: these fixtures declare no
        # destination, so M12 contributes a flag of its own.
        result = _translate()
        self.assertIn('spark.table("default.sales.claim")', result.translated_sql)
        source = [f for f in result.findings if f.rule == "M10_SOURCE_LAKEHOUSE"]
        self.assertEqual([f.severity for f in source], ["rewrite"])

    def test_unresolved_lakehouse_flags_and_falls_back_to_two_parts(self):
        result = _translate(lakehouses={})
        self.assertIn('spark.table("default.claim")', result.translated_sql)
        source = [f for f in result.findings if f.rule == "M10_SOURCE_LAKEHOUSE"]
        self.assertEqual([f.severity for f in source], ["flag"])

    def test_the_whole_navigation_chain_produces_exactly_one_read(self):
        self.assertEqual(_translate().translated_sql.count("spark.table("), 1)

    def test_csv_over_http_is_read_and_flagged(self):
        result = m.translate_query({
            "name": "Q", "attrs": None, "final": "Src",
            "steps": [_step("Src", "Csv.Document",
                            ['Web.Contents("https://h/x.csv")', '[Delimiter = ";"]'])]})
        self.assertIn("spark.read", result.translated_sql)
        self.assertIn("'sep', ';'".replace("'", '"').replace('"sep"', '"sep"'),
                      result.translated_sql.replace("'", '"'))
        self.assertTrue(any(f.rule == "M11_SOURCE_CSV" and f.severity == "flag"
                            for f in result.findings))

    def test_tab_delimiter_is_decoded(self):
        # `Delimiter = "#(tab)"` is how M says tab-separated. Six literal
        # characters reached Spark as the separator.
        step = {"name": "Source", "fn": "Csv.Document", "args": [
            'Web.Contents("https://example.com/a.tsv")',
            '[Delimiter = "#(tab)", Encoding = 65001]']}
        findings = []
        emitted = m._rule_csv_web(step, findings)
        self.assertIn('.option("sep", \'\\t\')', emitted)

    def test_unrecognised_delimiter_escape_blocks_not_untranslatable(self):
        # decode_escapes raises Untranslatable, named for m_expr callers; this
        # module's refusal type is Blocked. Left unconverted, the wrong
        # exception type would leak out of _rule_csv_web -- and without this
        # test, a later edit that dropped the conversion would leave the
        # suite green.
        step = {"name": "Source", "fn": "Csv.Document", "args": [
            'Web.Contents("https://example.com/a.csv")',
            '[Delimiter = "#(bogus)"]']}
        with self.assertRaises(m.Blocked) as context:
            m._rule_csv_web(step, [])
        self.assertIn("#(bogus)", str(context.exception))

    def test_addcolumn_malformed_escape_blocks_not_valueerror(self):
        # The critical bug this guards: chr() raises a bare ValueError above
        # 0x10FFFF, which is not Untranslatable and would crash
        # translate_query outright -- for any step's expression, not just a
        # CSV delimiter -- instead of naming and blocking the one step.
        step = {"name": "Added", "fn": "Table.AddColumn",
                "args": ["Nav2", '"total"', 'each "#(FFFFFFFF)"'],
                "inputs": ["Nav2"]}
        with self.assertRaises(m.Blocked) as context:
            m._rule_add_column(step, "nav2", {}, [], set())
        self.assertIn("#(FFFFFFFF)", str(context.exception))


class CatalogShapeTests(unittest.TestCase):
    """A Git export cannot resolve a lakehouse GUID: the Lakehouse item's
    logicalId is a different identifier from the lakehouseId M navigates by.
    So a wrong-shaped catalog must degrade to a flag, never splice junk into
    a table name."""

    def test_table_catalog_shape_does_not_leak_into_the_table_name(self):
        wrong = {"summary": {"warehouse_ddl": 1}, "tables": {"claim": {}}}
        result = _translate(lakehouses=wrong)
        self.assertIn('spark.table("default.claim")', result.translated_sql)
        self.assertNotIn("warehouse_ddl", result.translated_sql)

    def test_non_string_catalog_value_degrades_to_a_flag(self):
        result = _translate(lakehouses={"lh-1": {"name": "sales"}})
        source = [f for f in result.findings if f.rule == "M10_SOURCE_LAKEHOUSE"]
        self.assertEqual([f.severity for f in source], ["flag"])

    def test_none_catalog_is_accepted(self):
        result = _translate(lakehouses=None)
        self.assertIn('spark.table("default.claim")', result.translated_sql)


# The navigation chain, and then the `Csv.Document` the corpus always puts
# over it -- 031.pq's `#"Imported CSV" = Csv.Document(#"Navigation 3")`, and
# every lakehouse file read in the corpora vendored here (2 of 2). Without
# the document function the chain's value is the `[Content]` binary rather
# than a table and `_read_lakehouse` refuses the query, so a test about the
# *URI* has to carry it to reach the reader at all.
FILE_SOURCE = [
    _step("Src", "Lakehouse.Contents", ["[]"]),
    _step("N1", "Src", nav=['{[lakehouseId = "lh-1"]}', "[Data]"]),
    _step("N2", "N1", nav=['{[Id = "Files", ItemKind = "Folder"]}', "[Data]"]),
    _step("N3", "N2", nav=['{[Name = "DimPort.csv"]}', "[Content]"]),
]
FILE_SOURCE_CSV = FILE_SOURCE + [
    _step("Imported", "Csv.Document", ["N3"], inputs=["N3"]),
]


class LakehouseFileTests(unittest.TestCase):
    """Verified against a live AIDP cluster: Spark resolves a relative path
    against the executor's working directory, not the notebook's. A bare
    relative path is therefore never correct."""

    def _run(self, lakehouses=None):
        return m.translate_query(
            {"name": "Q", "attrs": None, "steps": FILE_SOURCE_CSV,
             "final": "Imported"},
            lakehouses=lakehouses, namespace="ns")

    def test_the_same_chain_with_nothing_parsing_it_is_refused(self):
        """The other half of this path, and the reason `FILE_SOURCE_CSV`
        exists. With no document function over the navigation the query's
        value is the `[Content]` binary, and emitting a CSV reader for it
        asserted a format nothing in the M states."""
        result = m.translate_query(
            {"name": "Q", "attrs": None, "steps": FILE_SOURCE, "final": "N3"},
            lakehouses={"lh-1": "sales"}, namespace="ns")
        self.assertEqual(result.translated_sql, "")
        self.assertIn("M90_UNSUPPORTED_STEP",
                      [f.rule for f in result.findings])

    def test_file_read_emits_an_oci_uri_not_a_relative_path(self):
        sql = self._run().translated_sql
        self.assertIn("oci://", sql)
        self.assertNotIn('.csv("Files/', sql)
        self.assertNotIn(".csv('Files/", sql)

    def test_unresolved_lakehouse_uses_the_guid_as_the_bucket(self):
        self.assertIn("oci://lh-1@ns/Files/DimPort.csv", self._run().translated_sql)

    def test_resolved_lakehouse_uses_its_name_as_the_bucket(self):
        self.assertIn("oci://sales@ns/Files/DimPort.csv",
                      self._run(lakehouses={"lh-1": "sales"}).translated_sql)

    def test_file_read_is_still_flagged_for_a_human(self):
        findings = [f for f in self._run().findings
                    if f.rule == "M14_SOURCE_LAKEHOUSE_FILE"]
        self.assertEqual([f.severity for f in findings], ["flag"])


class StepRuleTests(unittest.TestCase):
    def test_add_column(self):
        result = _translate(_step("Added", "Table.AddColumn",
                                  ["Nav2", '"total"', "each [a] + [b]"], inputs=["Nav2"]))
        self.assertIn('_m_add_column(nav2, "total",', result.translated_sql)

    def test_transform_column_types(self):
        result = _translate(_step("Typed", "Table.TransformColumnTypes",
                                  ["Nav2", '{{"a", type text}, {"b", Int64.Type}}'],
                                  inputs=["Nav2"]))
        # `type text` no longer casts: Spark's cast writes '3.0' where M
        # writes '3'. The text columns hoist into one helper call instead.
        self.assertIn('_m_text_columns(nav2.withColumn("b", ' + INT64_B + '), "a")',
                      result.translated_sql)
        self.assertIn(INT64_B, result.translated_sql)

    def test_int64_rounds_half_to_even_before_the_cast(self):
        """`Int64.Type` is `Int64.From`, which rounds; Spark's cast truncates.

        Power Query's Int64.From rounds with RoundingMode.ToEven by default.
        The plain `F.col(c).cast("bigint")` this used to emit truncates
        toward zero. Measured live on an AIDP cluster (Spark 3.5.0, ANSI
        off):

            doubles [1.6, 2.5, -1.5]        cast("bigint") -> [1, 2, -1]
            strings ["1.5", "2.5", "-1.5"]  cast("bigint") -> [1, 2, -1]

        Power Query answers [2, 2, -2] for both. `F.bround` is half-to-even
        -- it is what Number.Round translates to -- and routing through
        decimal(38,18) reads a text column's digits exactly instead of as
        the nearest double. No Spark here, so the shape is pinned;
        IntegerRoundingExecutionTests runs it where Spark exists.
        """
        step = {"name": "Changed", "fn": "Table.TransformColumnTypes", "args": [
            "Source", '{{"amount", Int64.Type}}']}
        helpers = set()
        emitted = m._rule_transform_types(step, "source", [], helpers)
        self.assertEqual(
            emitted, 'source.withColumn("amount", _m_int64(F.col("amount")))')
        self.assertEqual(helpers, {"_m_int64"})
        self.assertNotIn('F.col("amount").cast("bigint")', emitted)
        # The rounding this test is named for, and the range guard, are in
        # the helper the emission calls -- so assert on the helper's body,
        # not only on the call site, or the two can drift apart silently.
        body = m_runtime.HELPERS["_m_int64"]
        self.assertIn('F.bround(column.cast("decimal(38,18)"), 0)', body)
        self.assertIn("9223372036854775807", body)

    def test_only_int64_is_rounded(self):
        # A cast to double, date or decimal does not truncate a fraction
        # away, so those keep the plain cast. Currency.Type's decimal(19,4)
        # is its own question (M's currency rounding) and not this one.
        step = {"name": "Changed", "fn": "Table.TransformColumnTypes", "args": [
            "Source", '{{"x", type number}, {"d", type date}}']}
        helpers = set()
        emitted = m._rule_transform_types(step, "source", [], helpers)
        self.assertNotIn("_m_int64", emitted)
        self.assertEqual(helpers, set())
        self.assertIn('F.col("x").cast("double")', emitted)
        self.assertIn('F.col("d").cast("date")', emitted)

    def test_type_text_columns_hoist_into_one_call(self):
        step = {"name": "Changed", "fn": "Table.TransformColumnTypes", "args": [
            "Source",
            '{{"a", Int64.Type}, {"b", type text}, {"c", type text}}']}
        findings, helpers = [], set()
        emitted = m._rule_transform_types(step, "source", findings, helpers)
        self.assertEqual(
            emitted,
            '_m_text_columns(source.withColumn("a", _m_int64(F.col("a")))'
            ', "b", "c")')
        self.assertEqual(helpers, {"_m_text_columns", "_m_int64"})

    def test_a_column_listed_twice_is_blocked_not_reordered(self):
        step = {"name": "Changed", "fn": "Table.TransformColumnTypes", "args": [
            "Source", '{{"a", type text}, {"a", Int64.Type}}']}
        with self.assertRaises(m.Blocked):
            m._rule_transform_types(step, "source", [], set())

    def test_a_case_only_collision_is_also_blocked(self):
        # M column names are case-sensitive; Spark's default session is not.
        # Before this guard casefolded, this pair passed it and the hoist
        # inverted the outcome: "bigint" before this plan, "string" after.
        step = {"name": "Changed", "fn": "Table.TransformColumnTypes", "args": [
            "Source", '{{"A", type text}, {"a", Int64.Type}}']}
        with self.assertRaises(m.Blocked):
            m._rule_transform_types(step, "source", [], set())

    def test_value_changing_type_maps_are_gone(self):
        for m_type in ("type time", "type duration", "type any"):
            with self.subTest(m_type=m_type):
                self.assertNotIn(m_type, m.M_TYPES)

    def test_type_text_is_not_in_m_types(self):
        # _rule_transform_types handles `type text` itself and `continue`s
        # before M_TYPES is consulted, so an entry there is unreachable dead
        # code -- and the exact wrong mapping (a cast) this work removes.
        self.assertNotIn("type text", m.M_TYPES)

    def test_rendering_text_raises_a_flag_and_carries_the_prelude(self):
        # A date column stops the generated job. Say so in the report too.
        result = _translate(_step("Changed", "Table.TransformColumnTypes",
                                  ["Nav2", '{{"a", type text}, {"b", Int64.Type}}'],
                                  inputs=["Nav2"]))
        self.assertIn("M28_TEXT_RENDERING", [f.rule for f in result.findings])
        self.assertIn("def _m_text(", result.translated_sql)
        self.assertIn(
            'changed = _m_text_columns(nav2.withColumn("b", '
            + INT64_B + '), "a")',
            result.translated_sql)

    def test_text_from_in_an_expression_reads_the_frame_it_transforms(self):
        # No corpus query reaches _m_text through Table.AddColumn today, but
        # this is the path Task 7's date family uses. Only the helper it asks
        # for is spliced in -- not _m_text_columns.
        result = _translate(_step("Added", "Table.AddColumn",
                                  ["Nav2", '"txt"', "each Text.From([n])"],
                                  inputs=["Nav2"]))
        self.assertIn('added = _m_add_column(nav2, "txt", _m_text(nav2, F.col(\'n\')))',
                      result.translated_sql)
        self.assertNotIn("_m_text_columns", result.translated_sql)
        self.assertIn("M28_TEXT_RENDERING", [f.rule for f in result.findings])

    def test_a_query_needing_no_helper_gains_no_prelude(self):
        # prelude_for(set()) is "", so these files stay byte-identical.
        # `type number` is a plain cast; this used to say `Int64.Type`,
        # which now carries `_m_int64` and so proves the opposite.
        result = _translate(_step("Changed", "Table.TransformColumnTypes",
                                  ["Nav2", '{{"b", type number}}'],
                                  inputs=["Nav2"]))
        self.assertNotIn("_m_", result.translated_sql)
        self.assertNotIn("M28_TEXT_RENDERING",
                         [f.rule for f in result.findings])

    def test_int64_carries_its_helper_into_the_generated_file(self):
        """The complement: a query whose only conversion is `Int64.Type`
        must ship `_m_int64`, or the generated file calls a name it does
        not define and dies on the first row."""
        result = _translate(_step("Changed", "Table.TransformColumnTypes",
                                  ["Nav2", '{{"b", Int64.Type}}'],
                                  inputs=["Nav2"]))
        self.assertIn("def _m_int64(", result.translated_sql)
        self.assertIn("_m_int64(F.col(\"b\"))", result.translated_sql)
        # The whole file has to be a file, not just a fragment that reads
        # well: the `ast.parse` gate is what catches a broken splice.
        ast.parse(result.translated_sql)

    def test_the_helper_casts_to_the_type_the_table_maps(self):
        """`M_TYPES["Int64.Type"]` is still `bigint` and is still the truth:
        `_m_int64` does the cast, so nothing else appends one. If the table
        and the helper disagree, the table is a lie about the emission."""
        self.assertEqual(m.M_TYPES["Int64.Type"], "bigint")
        self.assertIn('.cast("%s")' % m.M_TYPES["Int64.Type"],
                      m_runtime.HELPERS["_m_int64"])

    def test_unmapped_m_type_blocks(self):
        result = _translate(_step("Typed", "Table.TransformColumnTypes",
                                  ["Nav2", '{{"a", Nonsense.Type}}'], inputs=["Nav2"]))
        self.assertEqual(result.translated_sql, "")
        self.assertIn("Nonsense.Type", str(result.findings))

    def test_select_rows(self):
        result = _translate(_step("Kept", "Table.SelectRows",
                                  ["Nav2", "each [a] > 1"], inputs=["Nav2"]))
        self.assertIn(".filter(", result.translated_sql)

    def test_a_quoted_column_name_reaches_the_frame_unquoted(self):
        """`[#"order-id"]` is the column `order-id`, in every expression.

        The whole query used to emit and grade a rewrite while referring to
        a column called `#"order-id"`, which no frame has. The filter and
        the added column are both here because the two took separate paths
        into the expression parser and both were wrong.
        """
        result = _translate(
            _step("Kept", "Table.SelectRows",
                  ["Nav2", 'each [#"order-id"] > 0'], inputs=["Nav2"]),
            _step("Added", "Table.AddColumn",
                  ["Kept", '"doubled"', 'each [#"order-id"] * 2'],
                  inputs=["Kept"]))
        self.assertIn("F.col('order-id')", result.translated_sql)
        self.assertNotIn('#"order-id"', result.translated_sql)

    def test_a_nested_table_call_is_blocked_not_dropped(self):
        # The inner SelectRows used to vanish: only the rename was emitted,
        # applied to the unfiltered frame, with no finding at all.
        result = _translate(_step(
            "Kept", "Table.RenameColumns",
            ['Table.SelectRows(Nav2, each [status] <> "closed")', '{{"x", "y"}}']))
        self.assertEqual(result.translated_sql, "")
        self.assertIn("not a step of this query", str(result.findings))

    def test_a_quoted_or_parenthesised_reference_is_the_same_step(self):
        # #"Nav2" and (Nav2) name step Nav2; blocking them was a regression.
        for arg in ('#"Nav2"', "(Nav2)", '((#"Nav2"))'):
            with self.subTest(arg=arg):
                result = _translate(_step("Kept", "Table.SelectRows", [arg, "each [a] > 1"]))
                self.assertIn("kept = nav2.filter(", result.translated_sql)

    def test_parentheses_around_an_expression_are_not_a_reference(self):
        result = _translate(_step("Kept", "Table.SelectRows", ["(Nav2) & (Nav2)", "each true"]))
        self.assertEqual(result.translated_sql, "")

    def test_combine_of_a_list_still_resolves_its_frames(self):
        result = _translate(
            _step("Kept", "Table.SelectRows", ["Nav2", "each [a] > 1"], inputs=["Nav2"]),
            _step("Both", "Table.Combine", ["{Nav2, Kept}"]))
        self.assertIn("nav2.unionByName(kept", result.translated_sql)

    def test_select_columns(self):
        result = _translate(_step("Cols", "Table.SelectColumns",
                                  ["Nav2", '{"a", "b"}'], inputs=["Nav2"]))
        self.assertIn('.select("a", "b")', result.translated_sql)

    def test_remove_columns(self):
        # Not a bare `.drop("a")`: measured on Spark 4.2.0 that ignores a
        # column the frame does not have, where M's Table.RemoveColumns
        # raises. `_m_drop` does the check in the generated file, which is
        # the only place the schema exists. See
        # tests/test_m_pipeline_structure.py's MissingColumnTests.
        result = _translate(_step("Dropped", "Table.RemoveColumns",
                                  ["Nav2", '{"a"}'], inputs=["Nav2"]))
        self.assertIn('_m_drop(nav2, "a")', result.translated_sql)

    def test_rename_columns(self):
        # Likewise not a bare `.withColumnRenamed("a", "b")`.
        result = _translate(_step("Renamed", "Table.RenameColumns",
                                  ["Nav2", '{{"a", "b"}}'], inputs=["Nav2"]))
        self.assertIn('_m_rename(nav2, ("a", "b"))', result.translated_sql)

    def test_promote_headers_is_folded_into_the_reader(self):
        # Over a *file* read, which really does become a
        # `spark.read.option("header", True)...csv(...)` there is to fold
        # into. This used to run over the lakehouse *table* fixture, where
        # nothing was folded and the finding said otherwise; that case is
        # refused now. See tests/test_m_pipeline_structure.py's
        # PassThroughFindingTests.
        # Over the `Csv.Document`, not over the navigation's `[Content]`:
        # promoting headers on a binary is not a shape M permits, and the
        # corpus writes file -> Csv.Document -> Table.PromoteHeaders.
        query = {"name": "Q", "attrs": None, "final": "Promoted",
                 "steps": FILE_SOURCE_CSV + [
                     _step("Promoted", "Table.PromoteHeaders",
                           ["Imported"], inputs=["Imported"])]}
        result = m.translate_query(query, lakehouses=CATALOG, namespace="ns")
        self.assertNotIn("PromoteHeaders", result.translated_sql)
        self.assertTrue(any(f.rule == "M26_PROMOTE_HEADERS" for f in result.findings))

    def test_scalar_binding_feeds_the_expression_scope(self):
        result = _translate(
            _step("startOfWeek", "IdentifierExpression", raw="Day.Monday"),
            _step("Added", "Table.AddColumn",
                  ["Nav2", '"dow"', "each Date.DayOfWeek([d], startOfWeek)"],
                  inputs=["Nav2"]))
        self.assertIn('_m_add_column(nav2, "dow"', result.translated_sql)
        self.assertNotIn("startOfWeek =", result.translated_sql)


class BlockingTests(unittest.TestCase):
    def test_out_of_scope_connector_is_named_as_a_connector(self):
        result = m.translate_query({"name": "Q", "attrs": None, "final": "Src",
                                    "steps": [_step("Src", "Excel.Workbook", ["f"])]})
        self.assertEqual(result.translated_sql, "")
        rules = [f.rule for f in result.findings]
        self.assertIn("M91_UNSUPPORTED_CONNECTOR", rules)
        self.assertIn("Excel.Workbook", str(result.findings))

    def test_unknown_table_function_is_a_step_not_a_connector(self):
        result = _translate(_step("X", "Table.Pivot", ["Nav2"], inputs=["Nav2"]))
        self.assertEqual(result.translated_sql, "")
        self.assertIn("M90_UNSUPPORTED_STEP", [f.rule for f in result.findings])

    def test_untranslatable_expression_blocks_the_whole_query(self):
        # A PySpark file missing a filter runs, and is wrong. Refuse instead.
        result = _translate(_step("Kept", "Table.SelectRows",
                                  ["Nav2", "each Mystery([a])"], inputs=["Nav2"]))
        self.assertEqual(result.translated_sql, "")
        self.assertIn("Mystery", str(result.findings))

    def test_a_blocked_query_still_reports_what_it_managed(self):
        result = _translate(_step("X", "Table.Pivot", ["Nav2"], inputs=["Nav2"]))
        self.assertTrue(any(f.rule == "M10_SOURCE_LAKEHOUSE" for f in result.findings))


ATTRS = ('[DataDestinations = {[Definition = [Kind = "Reference", '
         'QueryName = "H", IsNewTarget = true], '
         'Settings = [UpdateMethod = [Kind = "Append"]]]}]')
HELPER = {"name": "H", "attrs": None, "steps": SOURCE, "final": "Nav2"}


class DestinationTests(unittest.TestCase):
    def test_declared_destination_becomes_a_save(self):
        """Through `aidp_table`, like every read in the same file. It used to
        emit the bare `saveAsTable("claim")` -- so the one write in a script
        whose reads were all fully qualified landed in the session default."""
        query = _query(attrs=ATTRS)
        result = m.translate_query(query, queries_by_name={"H": HELPER}, lakehouses=CATALOG)
        self.assertIn('.write.mode("append").saveAsTable("default.sales.claim")',
                      result.translated_sql)

    def test_the_write_and_the_read_name_the_same_table(self):
        query = _query(attrs=ATTRS)
        result = m.translate_query(query, queries_by_name={"H": HELPER},
                                   lakehouses=CATALOG)
        self.assertIn('spark.table("default.sales.claim")', result.translated_sql)
        self.assertIn('saveAsTable("default.sales.claim")', result.translated_sql)

    def test_the_catalog_reaches_the_write(self):
        query = _query(attrs=ATTRS)
        result = m.translate_query(query, queries_by_name={"H": HELPER},
                                   lakehouses=CATALOG, catalog="myc")
        self.assertIn('saveAsTable("myc.sales.claim")', result.translated_sql)

    def test_an_unresolved_destination_lakehouse_is_flagged(self):
        """A write to the wrong place is not recoverable, so an unqualified
        destination is a flag, not a silent two-part name."""
        query = _query(attrs=ATTRS)
        result = m.translate_query(query, queries_by_name={"H": HELPER},
                                   lakehouses={})
        self.assertIn('saveAsTable("default.claim")', result.translated_sql)
        self.assertTrue(any(f.rule == "M12_DESTINATION" and f.severity == "flag"
                            for f in result.findings))

    def test_absent_destination_leaves_a_dataframe_and_flags(self):
        result = _translate()
        self.assertIn("result =", result.translated_sql)
        self.assertTrue(any(f.rule == "M12_DESTINATION" and f.severity == "flag"
                            for f in result.findings))

    def test_unsupported_destination_connector_blocks(self):
        helper = {"name": "H", "attrs": None, "final": "S",
                  "steps": [_step("S", "FabricSql.Contents", ["null"])]}
        result = m.translate_query(_query(attrs=ATTRS),
                                   queries_by_name={"H": helper}, lakehouses=CATALOG)
        self.assertEqual(result.translated_sql, "")
        self.assertIn("FabricSql.Contents", str(result.findings))


# 023.pq line 1 (and 015, 020, 031, 035), verbatim in shape.
SECTION_DEFAULT = (
    '[DefaultOutputDestinationSettings = [DestinationDefinition = '
    '[Kind = "Reference", QueryName = "DefaultDestination", IsNewTarget = true], '
    'UpdateMethod = [Kind = "Replace"], DestinationTypeSettings = [Kind = "Table"]], '
    'StagingDefinition = [Kind = "FastCopy"]]')
DEFAULT_DESTINATION = {
    "name": "DefaultDestination", "attrs": None, "steps": [],
    "raw": ('Lakehouse.Contents([EnableFolding = false])'
            '{[workspaceId = "ws-1"]}[Data]{[lakehouseId = "lh-1"]}[Data]'),
    "note": "unsupported member kind RecursivePrimaryExpression"}
BIND = "[BindToDefaultDestination = true]"


class BindToDefaultDestinationTests(unittest.TestCase):
    """Nine corpus queries said only `[BindToDefaultDestination = true]`, and
    all nine computed a frame and threw it away."""

    def _translate(self, attrs=BIND, section=SECTION_DEFAULT, members=None,
                   lakehouses=CATALOG):
        members = {"DefaultDestination": DEFAULT_DESTINATION} \
            if members is None else members
        return m.translate_query(_query(attrs=attrs), queries_by_name=members,
                                 lakehouses=lakehouses, section_attrs=section)

    def test_a_bound_query_writes_instead_of_dropping_its_result(self):
        result = self._translate()
        self.assertIn('.write.mode("overwrite").saveAsTable("default.sales.Q")',
                      result.translated_sql)
        self.assertNotIn("result =", result.translated_sql)

    def test_the_derivation_is_named_in_a_finding(self):
        result = self._translate()
        detail = next(f.detail for f in result.findings
                      if f.rule == "M17_DEFAULT_DESTINATION")
        self.assertIn("BindToDefaultDestination", detail)
        self.assertIn("default.sales.Q", detail)

    def test_it_is_flagged_because_the_table_name_is_derived_not_declared(self):
        # The export never writes the table name down for a bound query, so
        # the write is emitted and marked for a human rather than graded clean.
        self.assertTrue(any(f.rule == "M17_DEFAULT_DESTINATION"
                            and f.severity == "flag"
                            for f in self._translate().findings))

    def test_no_document_attribute_blocks_rather_than_writing_nowhere(self):
        result = self._translate(section="")
        self.assertEqual(result.translated_sql, "")
        self.assertIn("BindToDefaultDestination", str(result.findings))

    def test_an_absent_default_destination_query_blocks(self):
        result = self._translate(members={})
        self.assertEqual(result.translated_sql, "")
        self.assertIn("DefaultDestination", str(result.findings))

    def test_a_blocked_bind_is_never_a_silent_pass_through(self):
        result = self._translate(section="")
        self.assertNotIn("result =", result.translated_sql)


def _mapped_attrs(pairs, dynamic="false"):
    records = ", ".join('[SourceColumnName = "%s", DestinationColumnName = "%s"]'
                        % pair for pair in pairs)
    return ('[DataDestinations = {[Definition = [Kind = "Reference", '
            'QueryName = "H", IsNewTarget = false], '
            'Settings = [Kind = "Manual", AllowCreation = false, ColumnSettings = '
            '[Mappings = {%s}], DynamicSchema = %s, '
            'UpdateMethod = [Kind = "Replace"], TypeSettings = [Kind = "Table"]]]}]'
            % (records, dynamic))


class DestinationColumnMappingTests(unittest.TestCase):
    """A `DataDestinations` entry can say which source columns go to which
    target columns. Writing the whole frame instead gives the target the
    wrong shape -- extra columns, or columns under the wrong names."""

    def _translate(self, pairs, dynamic="false"):
        return m.translate_query(_query(attrs=_mapped_attrs(pairs, dynamic)),
                                 queries_by_name={"H": HELPER},
                                 lakehouses=CATALOG)

    def test_the_mapping_is_applied_before_the_write(self):
        body = self._translate([("a", "a"), ("b", "b")]).translated_sql
        self.assertIn('select(F.col("a").alias("a"), F.col("b").alias("b"))', body)

    def test_a_renamed_column_lands_under_its_destination_name(self):
        # 033.pq: SourceColumnName = "Date" -> DestinationColumnName =
        # "Message_Date". The only rename in 136 corpus mappings, and the
        # one case where writing the frame unchanged puts the data in a
        # column the destination does not have.
        body = self._translate([("Date", "Message_Date")]).translated_sql
        self.assertIn('F.col("Date").alias("Message_Date")', body)

    def test_a_column_outside_the_mapping_does_not_reach_the_write(self):
        body = self._translate([("a", "a")]).translated_sql
        write = next(line for line in body.splitlines() if "saveAsTable" in line)
        self.assertTrue(write.startswith("result."), write)
        self.assertIn('result = ', body)
        self.assertNotIn('"b"', body.split("result = ")[1])

    def test_the_mapping_is_reported(self):
        detail = next(f.detail for f in self._translate([("Date", "Message_Date")]).findings
                      if f.rule == "M18_DESTINATION_COLUMN_MAP")
        self.assertIn("1 column", detail)
        self.assertIn("Message_Date", detail)

    def test_a_dynamic_schema_mapping_is_flagged_as_a_snapshot(self):
        result = self._translate([("a", "a")], dynamic="true")
        self.assertTrue(any(f.rule == "M18_DESTINATION_COLUMN_MAP"
                            and f.severity == "flag" for f in result.findings))

    def test_a_fixed_schema_mapping_is_a_rewrite_not_a_flag(self):
        result = self._translate([("a", "a")], dynamic="false")
        self.assertTrue(any(f.rule == "M18_DESTINATION_COLUMN_MAP"
                            and f.severity == "rewrite" for f in result.findings))

    def test_column_settings_we_cannot_read_block_rather_than_write_the_frame(self):
        result = self._translate([])
        self.assertEqual(result.translated_sql, "")
        self.assertIn("ColumnSettings", str(result.findings))

    def test_two_sources_into_one_destination_column_block(self):
        result = self._translate([("a", "x"), ("b", "x")])
        self.assertEqual(result.translated_sql, "")
        self.assertIn("x", str(result.findings))

    def test_a_destination_with_no_mapping_still_writes_the_whole_frame(self):
        result = m.translate_query(_query(attrs=ATTRS), queries_by_name={"H": HELPER},
                                   lakehouses=CATALOG)
        self.assertNotIn("M18_DESTINATION_COLUMN_MAP",
                         [f.rule for f in result.findings])
        self.assertNotIn(".select(F.col(", result.translated_sql)


class CompileGateTests(unittest.TestCase):
    def test_emitted_output_is_valid_python(self):
        result = _translate(_step("Added", "Table.AddColumn",
                                  ["Nav2", '"total"', "each [a] & [b]"], inputs=["Nav2"]))
        ast.parse(result.translated_sql)

    def test_output_carries_a_provenance_header(self):
        result = m.translate_query(_query(), lakehouses=CATALOG, source="mashup.pq")
        self.assertIn("mashup.pq", result.translated_sql)
        self.assertIn("Not execution-verified", result.translated_sql)

    def test_duplicate_sanitized_names_do_not_collide(self):
        result = _translate(
            _step('#"Added custom"', "Table.AddColumn",
                  ["Nav2", '"a"', "each 1"], inputs=["Nav2"]),
            _step('#"Added  custom"', "Table.AddColumn",
                  ['#"Added custom"', '"b"', "each 2"], inputs=['#"Added custom"']))
        ast.parse(result.translated_sql)
        self.assertIn("added_custom_2", result.translated_sql)

    def test_the_gate_refuses_rather_than_raising_out_of_translate_query(self):
        """It used to `raise AssertionError`, and nothing above catches one.

        `migrate/runner.py`'s blanket `except Exception` turned it into a
        result row with `status: "error"` and a bare `str(exc)` -- no rule,
        no severity, no finding -- which `verify` grades FAIL. So one column
        name took the whole migration to FAIL and said only "invalid
        syntax".
        """
        saved = m.HEADER
        m.HEADER = "this is not python (((\n"
        try:
            result = m.translate_query(_query(), lakehouses=CATALOG)
        finally:
            m.HEADER = saved
        self.assertEqual(result.translated_sql, "")
        self.assertIn("M31_UNCOMPILABLE_OUTPUT",
                      [f.rule for f in result.findings])

    def test_the_gate_has_its_own_rule_id_and_does_not_borrow_m90s(self):
        """M90 is "the Dataflow does something with no mapping" and the
        reader's next step is to translate it by hand. This is "the tool
        built Python it cannot parse" and the next step is to report it."""
        saved = m.HEADER
        m.HEADER = "this is not python (((\n"
        try:
            rules = [f.rule for f in
                     m.translate_query(_query(), lakehouses=CATALOG).findings]
        finally:
            m.HEADER = saved
        self.assertNotIn("M90_UNSUPPORTED_STEP", rules)


class QuoteInAColumnNameTests(unittest.TestCase):
    '''A column name holding a `"` produced PySpark that would not parse.

    M spells such a name `{"he said ""hi"""}`, and every one of the five
    rules that put a name into generated source spliced it in as `"%s"`.
    Measured before the fix, each raising AssertionError out of
    `translate_query`:

        Table.SelectColumns        invalid syntax
        Table.RemoveColumns        invalid syntax
        Table.RenameColumns        unterminated string literal
        Table.AddColumn            invalid syntax
        Table.TransformColumnTypes unmatched ')'
    '''

    QUOTED = 'he said ""hi""'      # M source for: he said "hi"
    NAME = 'he said "hi"'

    CASES = (
        ("Table.SelectColumns",
         _step("Kept", "Table.SelectColumns", ["Nav2", '{"%s"}' % QUOTED],
               inputs=["Nav2"])),
        ("Table.RemoveColumns",
         _step("Dropped", "Table.RemoveColumns", ["Nav2", '{"%s"}' % QUOTED],
               inputs=["Nav2"])),
        ("Table.RenameColumns",
         _step("Ren", "Table.RenameColumns", ["Nav2", '{{"%s", "x"}}' % QUOTED],
               inputs=["Nav2"])),
        ("Table.AddColumn",
         _step("Add", "Table.AddColumn", ["Nav2", '"%s"' % QUOTED, "each 1"],
               inputs=["Nav2"])),
        ("Table.TransformColumnTypes",
         _step("Ty", "Table.TransformColumnTypes",
               ["Nav2", '{{"%s", Int64.Type}}' % QUOTED], inputs=["Nav2"])),
    )

    def test_every_rule_that_emits_a_name_produces_python_that_parses(self):
        for label, step in self.CASES:
            with self.subTest(rule=label):
                result = _translate(step)
                self.assertTrue(result.translated_sql, label)
                ast.parse(result.translated_sql)

    def test_the_emitted_literal_means_the_column_m_named(self):
        """Parsing is necessary and not sufficient: `.select("he said hi")`
        would parse too, and read a different column."""
        result = _translate(self.CASES[0][1])
        call = next(
            node for node in ast.walk(ast.parse(result.translated_sql))
            if isinstance(node, ast.Call)
            and isinstance(node.func, ast.Attribute)
            and node.func.attr == "select")
        self.assertEqual([arg.value for arg in call.args], [self.NAME])

    def test_a_backslash_in_a_name_is_escaped_too(self):
        # `"c:\new"` would parse -- and hold a newline where M has `\n`.
        result = _translate(_step("Kept", "Table.SelectColumns",
                                  ["Nav2", r'{"c:\new"}'], inputs=["Nav2"]))
        call = next(
            node for node in ast.walk(ast.parse(result.translated_sql))
            if isinstance(node, ast.Call)
            and isinstance(node.func, ast.Attribute)
            and node.func.attr == "select")
        self.assertEqual([arg.value for arg in call.args], [r"c:\new"])

    def test_an_ordinary_name_is_still_written_the_readable_way(self):
        """Every existing emission has to be byte-identical: `repr` would
        turn every `"a"` in this repo's fixtures into `'a'` for no gain."""
        self.assertEqual(m.py_name("order-id"), '"order-id"')
        self.assertEqual(m.py_name("line total"), '"line total"')
        self.assertEqual(m.py_name("it's"), '"it\'s"')


class DataflowNamingTests(unittest.TestCase):
    def _steps(self):
        return [_step("P", "Lakehouse.Contents", ["[]"]),
                _step("N", "P", nav=['{[lakehouseId = "lh-1"]}', "[Data]"]),
                _step("T", "N",
                      nav=['{[Id = "claim", ItemKind = "Table"]}', "[Data]"])]

    def _run(self, **kw):
        query = {"name": "Q", "attrs": None, "steps": self._steps(),
                 "final": "T"}
        return m.translate_query(query, **kw).translated_sql

    def test_a_resolved_lakehouse_becomes_the_schema(self):
        out = self._run(lakehouses={"lh-1": "SalesLake"})
        self.assertIn('spark.table("default.SalesLake.claim")', out)

    def test_the_catalog_is_configurable(self):
        out = self._run(lakehouses={"lh-1": "SalesLake"}, catalog="myc")
        self.assertIn('spark.table("myc.SalesLake.claim")', out)

    def test_the_oci_namespace_is_not_used_as_a_catalog(self):
        """It was. `--namespace myns` produced `myns.SalesLake.claim`."""
        out = self._run(lakehouses={"lh-1": "SalesLake"}, namespace="myns")
        self.assertNotIn("myns.SalesLake", out)

    def test_an_unresolved_lakehouse_still_yields_two_parts(self):
        out = self._run(lakehouses={})
        self.assertIn('spark.table("default.claim")', out)


class DottedColumnNameTests(unittest.TestCase):
    '''A column reference is parsed; `drop` and `toDF` take a name verbatim.

    MEASURED on pyspark 4.2.0, a frame with a flat column literally called
    `a.b`: `df.select("a.b")` and `F.col("a.b")` both raise
    UNRESOLVED_COLUMN -- Spark reads the dot as a nested-field access --
    while `df.drop("a.b")` succeeds. `F.col("`a.b`")` and
    `df.select("`a.b`")` succeed.

    Not exotic: `Table.ExpandRecordColumn` names its output
    `Customer.Name` by default, so this is the ordinary way Power Query
    produces a column name with punctuation in it.

    Every emitted form below was executed against pyspark 4.2.0 on a frame
    with columns `Customer.Name`, `n`, `plain`.
    '''

    def test_select_quotes_the_dotted_name(self):
        result = _translate(_step("Kept", "Table.SelectColumns",
                                  ["Nav2", '{"Customer.Name", "plain"}'],
                                  inputs=["Nav2"]))
        self.assertIn('nav2.select("`Customer.Name`", "plain")',
                      result.translated_sql)

    def test_an_each_expression_quotes_the_dotted_name(self):
        result = _translate(_step(
            "Add", "Table.AddColumn",
            ["Nav2", '"up"', 'each Text.Upper([#"Customer.Name"])'],
            inputs=["Nav2"]))
        self.assertIn("F.col('`Customer.Name`')", result.translated_sql)

    def test_a_cast_quotes_the_reference_and_not_the_name(self):
        """`withColumn`'s first argument is a name and its second contains
        a reference, so the same column is spelled two ways on one line.
        Quoting the first would create a column whose name really does
        start with a backtick.

        `Int64.Type` goes through `_m_int64`, so the reference is inside a
        helper call; the point here is which of the two spellings is
        back-quoted, and it is still only the one Spark parses. A plain
        cast is pinned below on `type number`, whose emission has no helper
        call to read past.
        """
        result = _translate(_step(
            "Ty", "Table.TransformColumnTypes",
            ["Nav2", '{{"Customer.Name", Int64.Type}}'], inputs=["Nav2"]))
        self.assertIn('.withColumn("Customer.Name", '
                      '_m_int64(F.col("`Customer.Name`")))',
                      result.translated_sql)

    def test_a_plain_cast_quotes_the_reference_and_not_the_name(self):
        """The same rule on a type that is cast directly, so the assertion
        reads the `withColumn`/`F.col` pair with nothing between them."""
        result = _translate(_step(
            "Ty", "Table.TransformColumnTypes",
            ["Nav2", '{{"Customer.Name", type number}}'], inputs=["Nav2"]))
        self.assertIn('.withColumn("Customer.Name", '
                      'F.col("`Customer.Name`").cast("double"))',
                      result.translated_sql)

    def test_drop_and_rename_take_the_name_verbatim(self):
        """Measured: `df.drop("a.b")` succeeds and `toDF` is positional, so
        back-quoting either would look for a column that does not exist."""
        dropped = _translate(_step("D", "Table.RemoveColumns",
                                   ["Nav2", '{"Customer.Name"}'],
                                   inputs=["Nav2"])).translated_sql
        self.assertIn('_m_drop(nav2, "Customer.Name")', dropped)
        renamed = _translate(_step("R", "Table.RenameColumns",
                                   ["Nav2", '{{"Customer.Name", "x"}}'],
                                   inputs=["Nav2"])).translated_sql
        self.assertIn('_m_rename(nav2, ("Customer.Name", "x"))', renamed)

    def test_a_hyphen_is_still_emitted_unquoted(self):
        """The refuted half of the filed finding, pinned so it stays
        refuted: `F.col("order-id")` and `df.select("order-id")` both
        resolve on Spark 4.2.0, so quoting them buys nothing and would
        rewrite every generated file here."""
        result = _translate(_step("Kept", "Table.SelectColumns",
                                  ["Nav2", '{"order-id", "line total"}'],
                                  inputs=["Nav2"]))
        self.assertIn('nav2.select("order-id", "line total")',
                      result.translated_sql)
        self.assertNotIn("`order-id`", result.translated_sql)


class UnaddressableTableNameTests(unittest.TestCase):
    '''A name part Spark's own catalog will not accept.

    MEASURED on pyspark 4.2.0, built-in Hive-compatible catalog:
    `saveAsTable("pdb.`my-table`")` fails INVALID_SCHEMA_OR_RELATION_NAME
    even back-quoted, and `CREATE DATABASE `my-lake`` likewise; a read
    parses and finds nothing.

    `info`, not `flag`. That rule is Hive's, and the built-in catalog
    cannot address the three-part names this tool emits at all --
    `spark.table("default.SalesLake.claim")` fails
    REQUIRES_SINGLE_PART_NAMESPACE on the same session. pyspark 4.2.0
    ships no v2 catalog implementation, so the rule that would actually
    apply could not be measured here.
    '''

    def _steps(self, table):
        return [_step("P", "Lakehouse.Contents", ["[]"]),
                _step("N", "P", nav=['{[lakehouseId = "lh-1"]}', "[Data]"]),
                _step("T", "N",
                      nav=['{[Id = "%s", ItemKind = "Table"]}' % table,
                           "[Data]"])]

    def _findings(self, table, lakehouse="SalesLake"):
        query = {"name": "Q", "attrs": None, "steps": self._steps(table),
                 "final": "T"}
        return m.translate_query(query,
                                 lakehouses={"lh-1": lakehouse}).findings

    def test_a_plain_name_says_nothing(self):
        self.assertNotIn("M34_NAME_NOT_ADDRESSABLE",
                         [f.rule for f in self._findings("claim")])

    def test_a_hyphenated_table_is_recorded(self):
        findings = self._findings("my-table")
        self.assertIn("M34_NAME_NOT_ADDRESSABLE", [f.rule for f in findings])

    def test_it_is_a_flag_now_that_the_cluster_has_answered(self):
        """It was `info` while only Spark's Hive-compatible catalog had been
        measured and the AIDP catalog had not, which was the honest severity
        for a rule that might not apply to the target at all. The cluster
        answered on 2026-09-29: `saveAsTable` of a back-quoted hyphenated
        name is rejected on fabricTest (Spark 3.5.0). A write that cannot
        land is not an `info`. The cost is real and was paid deliberately --
        corpus `clean` fell 2 -> 1, and `MIN_CLEAN` was lowered with the
        reason recorded beside it."""
        finding = next(f for f in self._findings("my-table")
                       if f.rule == "M34_NAME_NOT_ADDRESSABLE")
        self.assertEqual(finding.severity, "flag")

    def test_the_note_says_the_cluster_was_measured(self):
        """The old wording said the AIDP catalog was NOT measured. It has
        been. A finding that still said otherwise would send a reader to run
        a probe that has already been run."""
        detail = next(f.detail for f in self._findings("my-table")
                      if f.rule == "M34_NAME_NOT_ADDRESSABLE")
        self.assertIn("Measured on the AIDP cluster", detail)
        self.assertNotIn("NOT measured against the AIDP catalog", detail)

    def test_the_lakehouse_part_counts_too(self):
        detail = next(f.detail for f in self._findings("claim", "my-lake")
                      if f.rule == "M34_NAME_NOT_ADDRESSABLE")
        self.assertIn("my-lake", detail)

    def test_the_write_is_named_as_the_write(self):
        query = {"name": "Q",
                 "attrs": '[DataDestinations = {[Definition = [Kind = '
                          '"Reference", QueryName = "H"]]}]',
                 "steps": self._steps("claim"), "final": "T"}
        helper = {"name": "H", "attrs": None,
                  "steps": self._steps("my-table"), "final": "T"}
        findings = m.translate_query(query, queries_by_name={"H": helper},
                                     lakehouses={"lh-1": "SalesLake"}).findings
        detail = next(f.detail for f in findings
                      if f.rule == "M34_NAME_NOT_ADDRESSABLE")
        self.assertTrue(detail.startswith("this write"), detail[:40])



@unittest.skipUnless(spark_available(), "pyspark and a JVM not available")
class IntegerRoundingExecutionTests(unittest.TestCase):
    """Runs the emitted Int64.Type conversion where Spark exists.

    The expected values are Power Query's (Int64.From, RoundingMode.ToEven,
    and an error outside bigint's range). The rounding inputs are the ones
    measured live on AIDP (Spark 3.5.0, ANSI off), where the old plain cast
    returned [1, 2, -1] for both.

    ANSI is left at whatever the installed Spark defaults to, and that is
    deliberate. AIDP is Spark 3.5, where ANSI is off; Spark 4 defaults it
    on. MEASURED on 4.2.0, the guarded expression answers identically under
    both for every row asserted here -- the one row that differs, 1e25, has
    its own test below that reads the conf and asserts the property the two
    share.

    An earlier draft set `spark.sql.ansi.enabled=false` on the builder. It
    did not stay in this class: measured, it left
    `HelperExecutionTests.test_m_text_writes_what_m_writes_for_every_
    decimal_shape` failing on `decimal(38,25) 1e-25` when the two classes
    ran in the same process, and passing when either ran alone. A test
    class that changes a session conf changes it for whoever gets that
    session next.
    """

    @classmethod
    def setUpClass(cls):
        from pyspark.sql import SparkSession, functions as F, types as T
        cls.F = F
        cls.spark = (SparkSession.builder.master("local[1]")
                     .appName("fabric-aidp int64 rounding").getOrCreate())
        cls.spark.sparkContext.setLogLevel("FATAL")
        # The prelude exactly as a generated notebook sees it: assembled by
        # prelude_for, F and T in the namespace, no import of this tool. So
        # `_m_int64` here is the generated function and not a copy of it,
        # and the emission is eval'd against the file it ships with.
        cls.generated = {"F": F, "T": T}
        exec(compile(m_runtime.prelude_for(["_m_int64"]), "<prelude>", "exec"),
             cls.generated)

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def _convert(self, values, schema=None):
        step = {"name": "Changed", "fn": "Table.TransformColumnTypes", "args": [
            "Source", '{{"amount", Int64.Type}}']}
        helpers = set()
        emitted = m._rule_transform_types(step, "source", [], helpers)
        self.assertEqual(helpers, {"_m_int64"})
        rows = [(v,) for v in values]
        source = (self.spark.createDataFrame(rows, ["amount"])
                  if schema is None
                  else self.spark.createDataFrame(rows, "amount string")
                  .select(self.F.col("amount").cast(schema).alias("amount")))
        namespace = dict(self.generated, source=source)
        return [row[0] for row in eval(emitted, namespace).collect()]

    def _expect_not_a_number(self, values, error, schema=None):
        """Assert NULLs with ANSI off and `error` with ANSI on.

        Two rows below reach the `decimal(38,18)` intermediate rather than
        the range test, and that cast is one of the ones ANSI changes from
        "NULL" to "raise". Both answers say the same thing -- the value did
        not become a number -- and which one appears is the session's, not
        this tool's, so the assertion is on the shared property and the
        raise is identified by error class rather than accepted as any
        exception at all.
        """
        if self.spark.conf.get("spark.sql.ansi.enabled") == "true":
            with self.assertRaises(Exception) as caught:
                self._convert(values, schema=schema)
            self.assertIn(error, str(caught.exception))
        else:
            self.assertEqual(self._convert(values, schema=schema),
                             [None] * len(values))

    def test_doubles_round_half_to_even(self):
        self.assertEqual(self._convert([1.6, 2.5, -1.5]), [2, 2, -2])

    def test_text_rounds_half_to_even(self):
        self.assertEqual(self._convert(["1.5", "2.5", "-1.5"]), [2, 2, -2])

    def test_integers_are_unchanged(self):
        self.assertEqual(self._convert([7, -7, 9223372036854775807]),
                         [7, -7, 9223372036854775807])

    def test_both_bounds_of_the_range_survive_untouched(self):
        """The guard is inclusive. An off-by-one here would NULL the two
        values most likely to be a deliberate sentinel in real data."""
        self.assertEqual(
            self._convert([9223372036854775807, -9223372036854775808]),
            [9223372036854775807, -9223372036854775808])

    def test_above_the_range_is_null_and_not_a_wrapped_negative(self):
        """MEASURED before the guard, on 3.5.0 and again on 4.2.0:

            9.3e18  ->  -9146744073709551616
            1e19    ->  -8446744073709551616

        A sign-flipped nineteen-digit number is a plausible id. The plain
        cast this replaced saturated to the maximum, which is also a
        plausible id. M raises; NULL is the only one of the three that
        cannot be read as data.
        """
        self.assertEqual(self._convert([9.3e18, 1e19]), [None, None])

    def test_a_value_too_wide_for_the_intermediate_is_never_a_number(self):
        """1e25 needs 26 integer digits where `decimal(38,18)` holds 20, so
        the intermediate overflows before the guard is consulted at all.

        MEASURED on 4.2.0: ANSI off gives NULL, ANSI on raises
        NUMERIC_VALUE_OUT_OF_RANGE. AIDP is 3.5 with ANSI off, so NULL is
        the case that ships -- but this class must run on whatever Spark is
        installed without changing a conf other classes share, so the
        assertion is the property both answers have, and the raise is
        checked by error class rather than by "something went wrong".
        """
        self._expect_not_a_number([1e25], "NUMERIC_VALUE_OUT_OF_RANGE")

    def test_below_the_range_is_null_and_not_a_wrapped_positive(self):
        """The wrap goes both ways -- measured -9.3e18 -> 9146744073709551616
        and -1e19 -> 8446744073709551616, a negative becoming positive."""
        self.assertEqual(self._convert([-9.3e18, -1e19]), [None, None])

    def test_one_past_the_maximum_is_null_and_not_the_minimum(self):
        """The worst measured row. Text "9223372036854775808" is the
        maximum plus one; unguarded it produced -9223372036854775808, the
        minimum -- the largest possible error, from the smallest possible
        overshoot. The plain cast gave NULL here, so on a text column the
        rounding fix turned no answer into a wrong one.
        """
        self.assertEqual(self._convert(["9223372036854775808",
                                        "9300000000000000000"]), [None, None])

    def test_rounding_up_onto_the_boundary_is_null(self):
        """A decimal column inside the range whose ROUNDED value is not.
        Measured: decimal(38,2) 9223372036854775807.50 rounds half to even
        to ...808 and wrapped to -9223372036854775808; ...49 rounds down
        and is fine. The guard has to test the rounded value, not the
        input, and this is the row that proves which one it tests.
        """
        self.assertEqual(
            self._convert(["9223372036854775807.49",
                           "9223372036854775807.50"], schema="decimal(38,2)"),
            [9223372036854775807, None])

    def test_a_value_that_is_not_a_number_is_still_not_one(self):
        """Unchanged by the guard, and asserted so the guard cannot be read
        as the thing that introduced it: MEASURED on 4.2.0, the plain
        `cast("bigint")` this replaced answers the same way for "abc" --
        NULL with ANSI off, CAST_INVALID_INPUT with ANSI on.
        """
        self._expect_not_a_number(["abc"], "CAST_INVALID_INPUT")
        # A real NULL is a NULL either way: there is no cast to fail. The
        # schema is given because a one-row frame of `None` has no type to
        # infer, not because the type matters here.
        self.assertEqual(self._convert([None], schema="string"), [None])


if __name__ == "__main__":
    unittest.main()
