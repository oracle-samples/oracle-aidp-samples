"""Dataflow pipeline structure: the steps, the names, and what the findings say.

Seven defects, all in the shape of a query rather than in one rule's
arithmetic: a step dropped on the floor, a variable shadowed, or a finding
that describes something other than what happened.

Two kinds of test live here. The dict fixtures need no Node -- they are the
same `_step`/`_query` shapes `tests/test_m_rules.py` uses -- so they run
everywhere. The `M*Through*Parser` classes drive real Power Query text
through the Node parser and are skipped without it, the same way
`tests/test_m_corpus.py` skips.
"""
import ast
import builtins
import unittest

from fabric_aidp.translate import m_parser
from fabric_aidp.translate import m_to_pyspark as m

requires_node = unittest.skipUnless(
    m_parser.parser_available(), "Node + mparse not installed")


def _step(name, fn, args=(), nav=(), inputs=(), raw=""):
    return {"name": name, "fn": fn, "args": list(args), "nav": list(nav),
            "inputs": list(inputs), "raw": raw}


SOURCE = [
    _step("Pattern", "Lakehouse.Contents", ["[]"]),
    _step("Nav1", "Pattern", nav=['{[lakehouseId = "lh-1"]}', "[Data]"]),
    _step("Nav2", "Nav1", nav=['{[Id = "claim", ItemKind = "Table"]}', "[Data]"]),
]
CATALOG = {"lh-1": "sales"}

# The M prologue every parser-driven case below starts from: a lakehouse
# navigation chain ending in the table `claim`, bound to the step `Nav2`.
LAKEHOUSE_M = ('Source = Lakehouse.Contents([]), '
               'Nav1 = Source{[lakehouseId="lh-1"]}[Data], '
               'Nav2 = Nav1{[Id="claim", ItemKind="Table"]}[Data]')


def _query(*steps, **kwargs):
    steps = list(SOURCE) + list(steps)
    return dict({"name": "Q", "attrs": None, "steps": steps,
                 "final": steps[-1]["name"]}, **kwargs)


def _translate(*steps, **kwargs):
    lakehouses = kwargs.pop("lakehouses", CATALOG)
    return m.translate_query(_query(*steps, **kwargs), lakehouses=lakehouses)


def translate_m(body, **kwargs):
    """`let <body> in ...` -> the TranslationResult for its single query."""
    kwargs.setdefault("lakehouses", CATALOG)
    parsed = m_parser.parse_text(body)
    queries = parsed["queries"]
    by_name = {q["name"]: q for q in queries}
    return m.translate_query(queries[0], queries_by_name=by_name,
                             source="mashup.pq", **kwargs)


def undefined_names(code):
    """Names an emitted file reads without ever binding.

    `result = b` compiles -- `ast.parse` is happy with it -- and then raises
    NameError on the cluster. The generated-file gate cannot see that, so
    this walks the module the way a reader would, top-level statement by
    top-level statement: a name is defined once an import, an assignment, a
    `def` or a parameter has bound it, or if it is a builtin.

    An assignment's right-hand side is checked *before* its targets are
    bound, so `x = x + 1` with no earlier `x` is reported; everything else
    is checked against its own bindings too, which is what lets a `def`'s
    body see its parameters.
    """
    tree = ast.parse(code)

    def reads(node):
        # A comprehension's target lives in its own scope, so it is never a
        # module-level read: `tuple(t for t in xs if t)` binds and reads `t`
        # inside the genexp. Subtracting every comprehension target inside
        # `node` is approximate -- it would also excuse a same-named read
        # outside the comprehension -- but it errs towards silence rather
        # than towards a false alarm on the generated prelude, which does
        # exactly this.
        local = {child.id
                 for generator in ast.walk(node)
                 if isinstance(generator, ast.comprehension)
                 for child in ast.walk(generator.target)
                 if isinstance(child, ast.Name)}
        return [child.id for child in ast.walk(node)
                if isinstance(child, ast.Name)
                and isinstance(child.ctx, ast.Load) and child.id not in local]

    def binds(node):
        names = set()
        for child in ast.walk(node):
            if isinstance(child, ast.Name) and isinstance(child.ctx, ast.Store):
                names.add(child.id)
            elif isinstance(child, ast.arg):
                names.add(child.arg)
            elif isinstance(child, (ast.FunctionDef, ast.AsyncFunctionDef,
                                    ast.ClassDef)):
                names.add(child.name)
            elif isinstance(child, ast.alias):
                names.add((child.asname or child.name).split(".")[0])
        return names

    defined, missing = set(dir(builtins)), []
    for node in tree.body:
        if isinstance(node, ast.Assign):
            missing += [n for n in reads(node.value) if n not in defined]
            defined |= binds(node)
        else:
            visible = defined | binds(node)
            missing += [n for n in reads(node) if n not in visible]
            defined |= binds(node)
    return sorted(set(missing))


class UndefinedNameHelperTests(unittest.TestCase):
    """`undefined_names` is load-bearing for the assertions below, so it gets
    its own tests rather than being trusted."""

    def test_a_read_before_any_binding_is_reported(self):
        self.assertEqual(undefined_names("result = b\n"), ["b"])

    def test_an_assignment_defines_the_name_for_later_lines(self):
        self.assertEqual(undefined_names("b = 1\nresult = b\n"), [])

    def test_an_import_defines_the_name(self):
        self.assertEqual(
            undefined_names("from pyspark.sql import functions as F\nx = F.col('a')\n"),
            [])

    def test_a_builtin_is_not_reported(self):
        self.assertEqual(undefined_names("x = len('a')\n"), [])

    def test_a_function_parameter_is_not_reported(self):
        self.assertEqual(undefined_names("def f(df):\n    return df\n"), [])

    def test_a_comprehension_target_is_not_reported(self):
        # The generated prelude opens with exactly this shape.
        self.assertEqual(
            undefined_names("xs = ()\ny = tuple(t for t in xs if t)\n"), [])

    def test_a_self_reference_with_no_earlier_binding_is_reported(self):
        self.assertEqual(undefined_names("x = x + 1\n"), ["x"])


# --------------------------------------------------------------------------
# D1 -- `let ... in <expr>` returns the expression, not the last step


class FinalExpressionTests(unittest.TestCase):
    """M's `let ... in <expr>` returns the *expression*. Folding the last
    step in its place puts an unrelated value under the query's name, with
    no finding -- the single worst outcome this tool can produce."""

    def test_an_in_expression_that_is_not_a_step_is_refused(self):
        result = _translate(
            _step("Kept", "Table.SelectRows", ["Nav2", "each [a] > 1"],
                  inputs=["Nav2"]),
            final="Table.Buffer(Kept)")
        self.assertEqual(result.translated_sql, "")
        self.assertIn("M90_UNSUPPORTED_STEP", [f.rule for f in result.findings])
        self.assertIn("Table.Buffer(Kept)", str(result.findings))

    def test_the_last_step_is_not_substituted_for_the_in_expression(self):
        result = _translate(
            _step("Kept", "Table.SelectRows", ["Nav2", "each [a] > 1"],
                  inputs=["Nav2"]),
            final="Kept & Kept")
        self.assertNotIn("result = kept", result.translated_sql)
        self.assertEqual(result.translated_sql, "")

    def test_a_bare_step_reference_still_folds(self):
        result = _translate(
            _step("Kept", "Table.SelectRows", ["Nav2", "each [a] > 1"],
                  inputs=["Nav2"]),
            final="Kept")
        self.assertIn("result = kept", result.translated_sql)

    def test_a_quoted_or_parenthesised_reference_still_folds(self):
        for final in ('#"Kept"', "(Kept)", '((#"Kept"))'):
            with self.subTest(final=final):
                result = _translate(
                    _step("Kept", "Table.SelectRows", ["Nav2", "each [a] > 1"],
                          inputs=["Nav2"]),
                    final=final)
                self.assertIn("result = kept", result.translated_sql)

    def test_an_in_clause_naming_an_earlier_step_wins_over_the_last_one(self):
        result = _translate(
            _step("Kept", "Table.SelectRows", ["Nav2", "each [a] > 1"],
                  inputs=["Nav2"]),
            _step("Later", "Table.SelectColumns", ["Kept", '{"a"}'],
                  inputs=["Kept"]),
            final="Kept")
        self.assertIn("result = kept", result.translated_sql)
        self.assertNotIn("result = later", result.translated_sql)

    def test_a_query_with_no_recorded_in_clause_is_refused(self):
        query = _query(_step("Kept", "Table.SelectRows", ["Nav2", "each [a] > 1"],
                             inputs=["Nav2"]))
        query.pop("final")
        result = m.translate_query(query, lakehouses=CATALOG)
        self.assertEqual(result.translated_sql, "")
        self.assertIn("M90_UNSUPPORTED_STEP", [f.rule for f in result.findings])

    def test_the_refusal_still_reports_what_it_managed(self):
        result = _translate(
            _step("Kept", "Table.SelectRows", ["Nav2", "each [a] > 1"],
                  inputs=["Nav2"]),
            final="Kept & Kept")
        self.assertIn("M10_SOURCE_LAKEHOUSE", [f.rule for f in result.findings])


@requires_node
class FinalExpressionThroughParserTests(unittest.TestCase):
    def test_the_reported_shape_no_longer_emits_an_unrelated_value(self):
        # `let A = 1, B = 2 in A + B` emitted `result = b`: the `in` clause
        # discarded, replaced by the last binding, and `b` never assigned --
        # so the file raised NameError even before being wrong.
        result = translate_m("section S;\nshared Q = let A = 1, B = 2 in A + B;")
        self.assertEqual(result.translated_sql, "")
        self.assertIn("A + B", str(result.findings))

    def test_no_emitted_dataflow_reads_a_name_it_never_binds(self):
        result = translate_m(
            "let %s, Out = Table.SelectColumns(Nav2, {\"a\"}) in Out" % LAKEHOUSE_M)
        self.assertTrue(result.translated_sql)
        self.assertEqual(undefined_names(result.translated_sql), [])

    def test_a_real_in_expression_over_frames_is_refused_by_name(self):
        result = translate_m(
            'let %s, A = Table.SelectRows(Nav2, each true), '
            'B = Table.SelectColumns(Nav2, {"a"}) in Table.Combine({A, B})'
            % LAKEHOUSE_M)
        self.assertEqual(result.translated_sql, "")
        self.assertIn("Table.Combine", str(result.findings))


# --------------------------------------------------------------------------
# D2 -- a step named `Spark` used to overwrite the SparkSession


class ReservedNameTests(unittest.TestCase):
    """`sanitize()` lowercases, so `Spark`, `SPARK` and `#"spark"` all become
    the identifier the session is bound to. The step's assignment then
    replaced the session, and the next `spark.read` in the same file hit a
    DataFrame."""

    def test_a_step_named_spark_does_not_take_the_session_variable(self):
        result = _translate(_step("Spark", "Table.SelectRows",
                                  ["Nav2", "each [a] > 1"], inputs=["Nav2"]))
        self.assertIn("spark = SparkSession.builder.getOrCreate()",
                      result.translated_sql)
        self.assertNotIn("spark = nav2", result.translated_sql)

    def test_every_casing_of_spark_is_kept_off_the_session(self):
        for name in ("Spark", "SPARK", '#"spark"', '#"Spark "'):
            with self.subTest(name=name):
                result = _translate(_step(name, "Table.SelectRows",
                                          ["Nav2", "each [a] > 1"],
                                          inputs=["Nav2"]))
                self.assertNotIn("spark = nav2", result.translated_sql)

    def test_a_later_spark_read_still_reads_from_the_session(self):
        # The session is destroyed, not merely shadowed: the CSV step below
        # emitted `web = spark.read...` against the DataFrame `spark` had
        # become, which is an AttributeError on the cluster.
        result = _translate(
            _step("Spark", "Table.SelectRows", ["Nav2", "each [a] > 1"],
                  inputs=["Nav2"]),
            _step("Web", "Csv.Document", ['Web.Contents("https://h/x.csv")']),
            final="Web")
        self.assertIn("spark.read", result.translated_sql)
        session_line = "spark = SparkSession.builder.getOrCreate()"
        body = result.translated_sql
        self.assertEqual(
            [line for line in body.splitlines() if line.startswith("spark = ")],
            [session_line])

    def test_no_step_is_ever_assigned_a_reserved_name(self):
        for name in ("Spark", "SPARK", '#"spark"', "Result", '#"result"',
                     "F", "SparkSession", "T", '#"_m_text"',
                     '#"_M_TIMESTAMP_TYPES"'):
            with self.subTest(name=name):
                allocated = m._unique_names([_step(name, "Table.Buffer")])
                self.assertNotIn(allocated[name], m.RESERVED)

    def test_the_renamed_step_is_still_the_one_later_steps_read(self):
        result = _translate(
            _step("Spark", "Table.SelectRows", ["Nav2", "each [a] > 1"],
                  inputs=["Nav2"]),
            _step("Out", "Table.SelectColumns", ["Spark", '{"a"}'],
                  inputs=["Spark"]),
            final="Out")
        self.assertEqual(undefined_names(result.translated_sql), [])
        self.assertIn(".filter(", result.translated_sql)
        self.assertIn(".select(", result.translated_sql)


class ReservedSetTests(unittest.TestCase):
    """RESERVED is read out of the emitted header, prelude and session with
    `ast`, not typed out, so that a new import or a new generated helper
    cannot quietly fall outside it."""

    def test_every_generated_helper_is_reserved(self):
        from fabric_aidp.translate import m_runtime
        self.assertTrue(set(m_runtime.HELPERS) <= m.RESERVED,
                        sorted(set(m_runtime.HELPERS) - m.RESERVED))

    def test_the_header_imports_and_the_session_are_reserved(self):
        for name in ("SparkSession", "F", "spark", "result", "T",
                     "_M_TIMESTAMP_TYPES"):
            with self.subTest(name=name):
                self.assertIn(name, m.RESERVED)

    def test_a_real_generated_file_binds_nothing_outside_reserved_or_steps(self):
        # The strongest form: generate a file that pulls in the helper
        # prelude, read every module-level binding back out, and account for
        # each one as either a step variable or a reserved name.
        query = _query(_step("Changed", "Table.TransformColumnTypes",
                             ["Nav2", '{{"a", type text}, {"b", Int64.Type}}'],
                             inputs=["Nav2"]))
        result = m.translate_query(query, lakehouses=CATALOG)
        self.assertIn("def _m_text(", result.translated_sql)
        steps = set(m._unique_names(query["steps"]).values())
        bound = m._module_level_names(result.translated_sql)
        self.assertEqual(sorted(bound - steps - set(m.RESERVED)), [])


# --------------------------------------------------------------------------
# D3 -- RemoveColumns / RenameColumns on a column the table does not have


class _StubFrame:
    """The smallest thing `_m_drop` / `_m_rename` can be driven against.

    The helpers' guard is pure Python -- a membership test against
    `df.columns` -- so it is provable without Spark, and this project does
    not have pyspark as a dependency. What the guard *protects against* was
    measured on Spark 4.2.0 and is recorded in each helper's docstring.
    """

    def __init__(self, columns):
        self.columns = list(columns)
        self.dropped, self.renamed = [], []

    def drop(self, *names):
        self.dropped.extend(names)
        return _StubFrame([c for c in self.columns if c not in names])

    def toDF(self, *names):
        """Spark's positional rename, which is what `_m_rename` uses.

        Positional, so it models the one property the `withColumnRenamed`
        chain it replaced did not have: every pair applies to the original
        column list at once. `self.renamed` records the call so a test can
        assert nothing was applied when a check refused.
        """
        self.renamed.append(tuple(names))
        return _StubFrame(names)

    def withColumnRenamed(self, old, new):
        """Kept although `_m_rename` no longer calls it.

        Spark has this method, so the stub has it: without it, reverting
        `_m_rename` to the chain it used to be makes the tests below error
        on a missing attribute instead of failing on the wrong answer, and
        the wrong answer is the whole point. Reverting the helper must
        produce columns ['a', 'a'] here, exactly as it does on Spark 4.2.0.
        """
        self.renamed.append((old, new))
        return _StubFrame([new if c == old else c for c in self.columns])

    def withColumn(self, name, column):
        # Spark's default session: a case-insensitive match is REPLACED.
        kept = [c for c in self.columns if c.casefold() != name.casefold()]
        return _StubFrame(kept + [name])


def run_helper(name, *args, **kwargs):
    """Execute one generated helper body on its own, with no Spark.

    `prelude_for` would prepend PREAMBLE, which imports pyspark; these two
    helpers read nothing but the frame they are handed, so their bodies exec
    standalone.
    """
    from fabric_aidp.translate.m_runtime import HELPERS
    namespace = {}
    exec(compile(HELPERS[name], "<%s>" % name, "exec"), namespace)
    return namespace[name](*args, **kwargs)


class MissingColumnTests(unittest.TestCase):
    """Measured on Spark 4.2.0: `df.drop('nope')` returns the frame unchanged
    and `df.withColumnRenamed('nope', 'x')` likewise, both without error --
    while `df.select('nope')` raises. M's `Table.RemoveColumns` and
    `Table.RenameColumns` raise. So a typo, or a column an upstream change
    renamed, turned a hard M error into a table that is quietly a different
    shape, under a PASS.

    The schema is not knowable at migration time, so the check moves into
    the generated file, where it is -- the same reason `m_runtime` exists at
    all. Flagging instead was the alternative: it would put every drop and
    every rename into REVIEW while still shipping a job that runs and is
    wrong. This closes the divergence rather than disclosing it.
    """

    def test_remove_columns_emits_the_checked_form(self):
        result = _translate(_step("Dropped", "Table.RemoveColumns",
                                  ["Nav2", '{"a", "b"}'], inputs=["Nav2"]))
        self.assertIn('_m_drop(nav2, "a", "b")', result.translated_sql)
        self.assertNotIn(".drop(", result.translated_sql.split("def _m_drop")[0])

    def test_rename_columns_emits_the_checked_form(self):
        result = _translate(_step("Renamed", "Table.RenameColumns",
                                  ["Nav2", '{{"a", "b"}, {"c", "d"}}'],
                                  inputs=["Nav2"]))
        self.assertIn('_m_rename(nav2, ("a", "b"), ("c", "d"))',
                      result.translated_sql)

    def test_the_helper_definitions_travel_with_the_file(self):
        for step, marker in (
                (_step("Dropped", "Table.RemoveColumns", ["Nav2", '{"a"}'],
                       inputs=["Nav2"]), "def _m_drop("),
                (_step("Renamed", "Table.RenameColumns", ["Nav2", '{{"a", "b"}}'],
                       inputs=["Nav2"]), "def _m_rename(")):
            with self.subTest(marker=marker):
                result = _translate(step)
                self.assertIn(marker, result.translated_sql)
                ast.parse(result.translated_sql)
                self.assertEqual(undefined_names(result.translated_sql), [])

    def test_a_query_with_neither_still_carries_no_prelude(self):
        result = _translate(_step("Cols", "Table.SelectColumns",
                                  ["Nav2", '{"a"}'], inputs=["Nav2"]))
        self.assertNotIn("_m_", result.translated_sql)

    def test_drop_raises_on_a_column_the_frame_does_not_have(self):
        with self.assertRaises(ValueError) as context:
            run_helper("_m_drop", _StubFrame(["a", "b"]), "nope")
        self.assertIn("nope", str(context.exception))

    def test_drop_removes_the_columns_when_they_are_all_there(self):
        frame = _StubFrame(["a", "b", "c"])
        self.assertEqual(run_helper("_m_drop", frame, "a", "c").columns, ["b"])

    def test_drop_is_case_sensitive_the_way_m_is(self):
        # Spark's own resolution is case-insensitive by default, so
        # `df.drop('ID')` would quietly take `id`. M column names are
        # case-sensitive and M raises, so the guard compares exactly.
        with self.assertRaises(ValueError):
            run_helper("_m_drop", _StubFrame(["id"]), "ID")

    def test_rename_raises_on_a_column_the_frame_does_not_have(self):
        with self.assertRaises(ValueError) as context:
            run_helper("_m_rename", _StubFrame(["a"]), ("nope", "x"))
        self.assertIn("nope", str(context.exception))

    def test_rename_checks_every_pair_before_renaming_any(self):
        # Half a rename is worse than none: it would leave a frame neither M
        # nor Spark would produce.
        frame = _StubFrame(["a", "b"])
        with self.assertRaises(ValueError):
            run_helper("_m_rename", frame, ("a", "x"), ("nope", "y"))
        self.assertEqual(frame.renamed, [])

    def test_rename_applies_every_pair_when_they_are_all_there(self):
        frame = _StubFrame(["a", "c"])
        self.assertEqual(
            run_helper("_m_rename", frame, ("a", "b"), ("c", "d")).columns,
            ["b", "d"])

    def test_rename_leaves_the_columns_it_was_not_given_alone(self):
        frame = _StubFrame(["a", "keep", "c"])
        self.assertEqual(
            run_helper("_m_rename", frame, ("c", "d")).columns,
            ["a", "keep", "d"])


class SimultaneousRenameTests(unittest.TestCase):
    """`Table.RenameColumns` is simultaneous in M; the chain was sequential.

    MEASURED on Spark 4.2.0, frame [(1, 2)] with columns a, b, renaming
    {{"a","b"},{"b","a"}}:

        withColumnRenamed('a','b').withColumnRenamed('b','a')
                                     -> columns ['a', 'a'], Row(a=1, a=2)
        df.withColumnsRenamed({'a':'b','b':'a'})
                                     -> columns ['a', 'a'], Row(a=1, a=2)
        df.toDF('b', 'a')            -> columns ['b', 'a'], Row(b=1, a=2)
        M's answer                   -> columns ['b', 'a'], values (1, 2)

    Two columns with the same name, and neither holding the name M gives
    it. Spark's own bulk API is not the fix -- it gives the identical wrong
    answer -- so the rendering is a positional rename instead.
    """

    def test_a_swap_gives_ms_answer_and_not_two_columns_of_one_name(self):
        frame = _StubFrame(["a", "b"])
        self.assertEqual(
            run_helper("_m_rename", frame, ("a", "b"), ("b", "a")).columns,
            ["b", "a"])

    def test_the_rename_is_positional_so_no_name_is_ever_parsed(self):
        """`F.col("a.b")` raises UNRESOLVED_COLUMN on a frame whose column
        is literally called `a.b` -- Spark reads the dot as a nested field.
        Measured on Spark 4.2.0. So a select of aliased columns would fix
        the swap and break every dotted name; `toDF` does neither."""
        frame = _StubFrame(["a.b", "c"])
        result = run_helper("_m_rename", frame, ("a.b", "x"))
        self.assertEqual(result.columns, ["x", "c"])
        self.assertEqual(frame.renamed, [("x", "c")])

    def test_renaming_one_column_onto_another_that_exists_is_refused(self):
        # M raises; the positional rename would silently produce ['b', 'b'].
        with self.assertRaises(ValueError) as context:
            run_helper("_m_rename", _StubFrame(["a", "b"]), ("a", "b"))
        self.assertIn("two columns named", str(context.exception))

    def test_renaming_the_same_column_twice_is_refused(self):
        with self.assertRaises(ValueError) as context:
            run_helper("_m_rename", _StubFrame(["a", "z"]),
                       ("a", "b"), ("a", "c"))
        self.assertIn("more than once", str(context.exception))

    def test_a_chain_of_with_column_renamed_is_not_emitted(self):
        """The mechanism, not just the result: a generated file that still
        loops over withColumnRenamed has the sequential semantics whatever
        the stub above says.

        Read off the code with the docstring removed, because the docstring
        names both wrong APIs on purpose -- it records the measurement.
        """
        import ast

        from fabric_aidp.translate.m_runtime import HELPERS
        tree = ast.parse(HELPERS["_m_rename"])
        function = tree.body[0]
        if ast.get_docstring(function) is not None:
            function.body = function.body[1:]
        code = ast.dump(function)
        self.assertNotIn("withColumnRenamed", code)
        self.assertNotIn("withColumnsRenamed", code)
        self.assertIn("toDF", code)

    def test_the_findings_say_the_check_is_there(self):
        for step, rule in (
                (_step("Dropped", "Table.RemoveColumns", ["Nav2", '{"a"}'],
                       inputs=["Nav2"]), "M25_REMOVE_COLUMNS"),
                (_step("Renamed", "Table.RenameColumns", ["Nav2", '{{"a", "b"}}'],
                       inputs=["Nav2"]), "M23_RENAME_COLUMNS")):
            with self.subTest(rule=rule):
                findings = [f for f in _translate(step).findings
                            if f.rule == rule]
                self.assertEqual([f.severity for f in findings], ["rewrite"])
                self.assertIn("exist", findings[0].detail)


# --------------------------------------------------------------------------
# M-12 -- Table.AddColumn over a column that differs only in case


class AddColumnCaseCollisionTests(unittest.TestCase):
    """Measured on the AIDP cluster (Spark 3.5.0, spark.sql.caseSensitive
    false, the default): a frame with column x = [1, 3], then
    `df.withColumn("X", F.col("x") * 2)`, came back with columns ['X'] and
    rows [(2,), (6,)] -- x and its values replaced. Power Query is
    case-sensitive and keeps both x and X; an AddColumn with an exact
    existing name is an error in M. The probe export's Q17
    (`Table.AddColumn(SrcT, "X", each [x] * 2)`) emitted exactly that
    withColumn under a PASS.

    A column the expression reads is known to exist, so that collision is
    refused at migration time; any other is checked in the generated file.
    """

    def test_a_case_only_collision_with_a_read_column_is_refused(self):
        result = _translate(_step("Added", "Table.AddColumn",
                                  ["Nav2", '"X"', "each [x] * 2"],
                                  inputs=["Nav2"]))
        self.assertEqual(result.translated_sql, "")
        blocked = [f for f in result.findings if f.rule == "M90_UNSUPPORTED_STEP"]
        self.assertEqual(len(blocked), 1)
        self.assertIn("'X'", blocked[0].detail)
        self.assertIn("'x'", blocked[0].detail)

    def test_an_exact_collision_with_a_read_column_is_refused(self):
        result = _translate(_step("Added", "Table.AddColumn",
                                  ["Nav2", '"x"', "each [x] + 1"],
                                  inputs=["Nav2"]))
        self.assertIn("M90_UNSUPPORTED_STEP", [f.rule for f in result.findings])

    def test_a_distinct_name_emits_the_checked_form(self):
        result = _translate(_step("Added", "Table.AddColumn",
                                  ["Nav2", '"y"', "each [x] * 2"],
                                  inputs=["Nav2"]))
        self.assertIn('added = _m_add_column(nav2, "y", ', result.translated_sql)
        self.assertIn("def _m_add_column(", result.translated_sql)
        self.assertNotIn(".withColumn(",
                         result.translated_sql.split("spark = ")[-1])
        ast.parse(result.translated_sql)
        self.assertEqual(undefined_names(result.translated_sql), [])
        m20 = [f for f in result.findings if f.rule == "M20_ADD_COLUMN"]
        self.assertEqual([f.severity for f in m20], ["rewrite"])
        self.assertIn("exist", m20[0].detail)

    def test_the_stub_shows_what_the_bare_withcolumn_did(self):
        # The measured Spark behaviour, reproduced: no guard, x is gone.
        self.assertEqual(_StubFrame(["x"]).withColumn("X", None).columns, ["X"])

    def test_the_helper_raises_on_a_case_only_collision(self):
        with self.assertRaises(ValueError) as context:
            run_helper("_m_add_column", _StubFrame(["id", "x"]), "X", None)
        self.assertIn("'x'", str(context.exception))

    def test_the_helper_raises_on_an_exact_collision(self):
        with self.assertRaises(ValueError):
            run_helper("_m_add_column", _StubFrame(["x"]), "x", None)

    def test_the_helper_adds_when_no_column_collides(self):
        frame = _StubFrame(["x"])
        self.assertEqual(run_helper("_m_add_column", frame, "y", None).columns,
                         ["x", "y"])

    @requires_node
    def test_the_probe_shape_through_the_parser(self):
        result = translate_m(
            'section S; shared Q = let %s, Added = Table.AddColumn(Nav2, "X", '
            'each [x] * 2) in Added;' % LAKEHOUSE_M)
        self.assertEqual(result.translated_sql, "")
        self.assertIn("M90_UNSUPPORTED_STEP", [f.rule for f in result.findings])


# --------------------------------------------------------------------------
# D4 -- a library function reported as an unsupported connector


class RefusalReasonTests(unittest.TestCase):
    """`Text.Upper` is a function, not a connector. The refusal was right and
    the reason was wrong, and a wrong reason sends the reader looking for a
    data source that does not exist."""

    def test_a_text_function_is_a_step_not_a_connector(self):
        result = _translate(_step("Out", "Text.Upper", ["Nav2"], inputs=["Nav2"]))
        self.assertEqual(result.translated_sql, "")
        rules = [f.rule for f in result.findings]
        self.assertIn("M90_UNSUPPORTED_STEP", rules)
        self.assertNotIn("M91_UNSUPPORTED_CONNECTOR", rules)
        self.assertIn("Text.Upper", str(result.findings))

    def test_every_value_namespace_refuses_as_a_step(self):
        for function in ("Text.Upper", "Date.From", "DateTime.LocalNow",
                         "Number.Round", "List.Sum", "Record.Field",
                         "Duration.Days", "Value.NativeQuery", "Type.Is",
                         "Splitter.SplitTextByDelimiter", "Binary.Decompress",
                         "Table.Pivot"):
            with self.subTest(function=function):
                result = _translate(_step("Out", function, ["Nav2"],
                                          inputs=["Nav2"]))
                self.assertIn("M90_UNSUPPORTED_STEP",
                              [f.rule for f in result.findings])

    def test_a_real_connector_is_still_named_as_a_connector(self):
        # Every one of these appears in the reference corpus as an actual
        # data source, and each must keep its M91 verdict: this change only
        # narrows M91, it never widens it.
        for function in ("Excel.Workbook", "Sql.Database", "SharePoint.Files",
                         "Snowflake.Databases", "Databricks.Catalogs",
                         "GoogleSheets.Contents", "AzureStorage.Blobs",
                         "PowerPlatform.Dataflows", "FabricSql.Contents"):
            with self.subTest(function=function):
                result = m.translate_query(
                    {"name": "Q", "attrs": None, "final": "Src",
                     "steps": [_step("Src", function, ["x"])]})
                self.assertIn("M91_UNSUPPORTED_CONNECTOR",
                              [f.rule for f in result.findings])

    def test_a_namespaceless_function_is_still_a_step(self):
        result = _translate(_step("Out", "#date", ["Nav2"], inputs=["Nav2"]))
        self.assertIn("M90_UNSUPPORTED_STEP", [f.rule for f in result.findings])

    def test_the_discriminator_is_the_namespace_not_the_input(self):
        # Both of these read step Nav2. The verdict still differs, because a
        # connector is recognised by its namespace, not by whether the call
        # happens to take an earlier step as an argument.
        text = _translate(_step("Out", "Text.Upper", ["Nav2"], inputs=["Nav2"]))
        workbook = _translate(_step("Out", "Excel.Workbook", ["Nav2"],
                                    inputs=["Nav2"]))
        self.assertIn("M90_UNSUPPORTED_STEP", [f.rule for f in text.findings])
        self.assertIn("M91_UNSUPPORTED_CONNECTOR",
                      [f.rule for f in workbook.findings])


@requires_node
class RefusalReasonThroughParserTests(unittest.TestCase):
    def test_the_reported_shape_is_a_step_refusal(self):
        result = translate_m("let %s, Out = Text.Upper(Nav2) in Out" % LAKEHOUSE_M)
        self.assertEqual(result.translated_sql, "")
        self.assertEqual(
            [f.rule for f in result.findings if f.rule.startswith("M9")],
            ["M90_UNSUPPORTED_STEP"])


# --------------------------------------------------------------------------
# D5 -- "folded into the reader" when there was no reader

# A lakehouse *file* read: this one really does become a reader,
# `spark.read.option("header", True)...csv(...)`, so a header promotion
# folded into it is folded into something.
FILE_SOURCE = [
    _step("Src", "Lakehouse.Contents", ["[]"]),
    _step("N1", "Src", nav=['{[lakehouseId = "lh-1"]}', "[Data]"]),
    _step("N2", "N1", nav=['{[Id = "Files", ItemKind = "Folder"]}', "[Data]"]),
    _step("N3", "N2", nav=['{[Name = "DimPort.csv"]}', "[Content]"]),
]


def translate_over_file(*steps, **kwargs):
    all_steps = list(FILE_SOURCE) + list(steps)
    query = dict({"name": "Q", "attrs": None, "steps": all_steps,
                  "final": all_steps[-1]["name"]}, **kwargs)
    return m.translate_query(query, lakehouses=CATALOG, namespace="ns")


def translate_over_csv_file(*steps, **kwargs):
    """`translate_over_file` with the `Csv.Document` in between, as `Imported`.

    A step handed the navigation's `[Content]` directly is handed a *binary*,
    and `Table.Buffer`/`Table.PromoteHeaders` over a binary is a type error
    in M as much as it is untranslatable here -- the corpus writes
    file -> Csv.Document -> <step>, which is 031.pq's shape and 009.pq's. So
    a test about what a pass-through says over a real reader has to put the
    document function where M puts it; without one `_read_lakehouse` refuses
    the query rather than inventing a CSV reader for the binary.
    """
    return translate_over_file(
        _step("Imported", "Csv.Document", ["N3"], inputs=["N3"]),
        *steps, **kwargs)


class PassThroughFindingTests(unittest.TestCase):
    """`Table.Buffer` and `Table.PromoteHeaders` both reported "<fn> folded
    into the reader". Over a lakehouse table there is no reader, and nothing
    was folded -- the finding described an operation that did not happen."""

    def test_buffer_over_a_lakehouse_table_says_what_happened(self):
        result = _translate(_step("Out", "Table.Buffer", ["Nav2"],
                                  inputs=["Nav2"]))
        buffer_findings = [f for f in result.findings if f.rule == "M29_BUFFER"]
        self.assertEqual(len(buffer_findings), 1)
        self.assertNotIn("folded", buffer_findings[0].detail)
        self.assertIn("passed through unchanged", buffer_findings[0].detail)
        self.assertNotIn("M11_SOURCE_CSV", [f.rule for f in result.findings])

    def test_buffer_is_never_a_csv_source(self):
        # It reported under M11_SOURCE_CSV, which is not merely the wrong
        # wording -- it is the wrong rule. Table.Buffer reads nothing.
        result = _translate(_step("Out", "Table.Buffer", ["Nav2"],
                                  inputs=["Nav2"]))
        self.assertIn("out = nav2", result.translated_sql)
        self.assertIn("M29_BUFFER", result.translated_sql)
        self.assertNotIn("M11_SOURCE_CSV", result.translated_sql)

    def test_buffer_over_a_real_reader_still_says_the_same_thing(self):
        # Buffer never folds into anything: it is a materialisation hint
        # whichever frame it is handed.
        result = translate_over_csv_file(
            _step("Out", "Table.Buffer", ["Imported"], inputs=["Imported"]))
        detail = [f.detail for f in result.findings if f.rule == "M29_BUFFER"]
        self.assertEqual(len(detail), 1)
        self.assertNotIn("folded", detail[0])

    def test_promote_headers_over_a_real_reader_is_folded(self):
        result = translate_over_csv_file(
            _step("Promoted", "Table.PromoteHeaders", ["Imported"],
                  inputs=["Imported"]))
        promoted = [f for f in result.findings if f.rule == "M26_PROMOTE_HEADERS"]
        self.assertEqual(len(promoted), 1)
        self.assertIn("folded into the reader", promoted[0].detail)
        self.assertIn("promoted = imported", result.translated_sql)

    def test_promote_headers_over_a_lakehouse_table_is_refused(self):
        # There is no reader to fold into, and on a frame that already has
        # column names `Table.PromoteHeaders` is a real operation -- it
        # takes the first row and makes it the header. Passing the frame
        # through drops that silently.
        result = _translate(_step("Promoted", "Table.PromoteHeaders", ["Nav2"],
                                  inputs=["Nav2"]))
        self.assertEqual(result.translated_sql, "")
        self.assertIn("M90_UNSUPPORTED_STEP", [f.rule for f in result.findings])
        self.assertIn("Table.PromoteHeaders", str(result.findings))
        self.assertNotIn("M26_PROMOTE_HEADERS", [f.rule for f in result.findings])

    def test_csv_document_over_a_real_reader_is_folded(self):
        result = translate_over_file(_step("Imported", "Csv.Document", ["N3"],
                                           inputs=["N3"]))
        csv = [f for f in result.findings if f.rule == "M11_SOURCE_CSV"]
        self.assertEqual([f.detail.count("folded into the reader") for f in csv],
                         [1])

    def test_csv_document_over_something_that_is_not_a_reader_is_refused(self):
        result = _translate(_step("Imported", "Csv.Document", ["Nav2"],
                                  inputs=["Nav2"]))
        self.assertEqual(result.translated_sql, "")
        self.assertIn("M90_UNSUPPORTED_STEP", [f.rule for f in result.findings])

    def test_reader_provenance_travels_through_a_chain(self):
        # The corpus shape: file read -> Csv.Document -> Table.PromoteHeaders.
        # A step that returns its input unchanged aliases it, so the header
        # promotion at the end really is folded into the reader at the start.
        result = translate_over_file(
            _step("Imported", "Csv.Document", ["N3"], inputs=["N3"]),
            _step("Promoted", "Table.PromoteHeaders", ["Imported"],
                  inputs=["Imported"]))
        self.assertTrue(result.translated_sql)
        rules = [f.rule for f in result.findings]
        self.assertIn("M11_SOURCE_CSV", rules)
        self.assertIn("M26_PROMOTE_HEADERS", rules)

    def test_a_buffer_in_the_chain_does_not_break_the_provenance(self):
        result = translate_over_csv_file(
            _step("Held", "Table.Buffer", ["Imported"], inputs=["Imported"]),
            _step("Promoted", "Table.PromoteHeaders", ["Held"], inputs=["Held"]))
        self.assertTrue(result.translated_sql)
        self.assertIn("M26_PROMOTE_HEADERS", [f.rule for f in result.findings])

    def test_a_web_csv_read_is_a_reader_too(self):
        result = m.translate_query({
            "name": "Q", "attrs": None, "final": "Promoted",
            "steps": [
                _step("Src", "Csv.Document",
                      ['Web.Contents("https://h/x.csv")']),
                _step("Promoted", "Table.PromoteHeaders", ["Src"],
                      inputs=["Src"])]})
        self.assertTrue(result.translated_sql)
        self.assertIn("M26_PROMOTE_HEADERS", [f.rule for f in result.findings])


@requires_node
class PassThroughFindingThroughParserTests(unittest.TestCase):
    def test_the_reported_buffer_shape_no_longer_claims_a_fold(self):
        result = translate_m("let %s, Out = Table.Buffer(Nav2) in Out"
                             % LAKEHOUSE_M)
        self.assertIn("out = nav2", result.translated_sql)
        self.assertNotIn("folded into the reader", str(result.findings))

    def test_the_reported_promote_headers_shape_is_refused(self):
        result = translate_m("let %s, Out = Table.PromoteHeaders(Nav2) in Out"
                             % LAKEHOUSE_M)
        self.assertEqual(result.translated_sql, "")
        self.assertNotIn("folded into the reader", str(result.findings))


# --------------------------------------------------------------------------
# D6 -- step-name deduplication collides and silently skips a step


class NameAllocationTests(unittest.TestCase):
    """CONFIRMED, in two shapes.

    `_unique_names` numbered duplicates with a counter per sanitized base
    and never checked whether the numbered name it produced was already
    taken. Three steps named `#"a b"`, `#"a_b"` and `#"a_b_2"` all sanitize
    into that one family: the first takes `a_b`, the second is renamed to
    `a_b_2`, and the third's base *is* `a_b_2`, with a counter of its own
    still at zero. Both got `a_b_2`.

    `translate_query` then skipped any step whose variable already appeared
    on the left of an emitted line, so the third step -- a real
    `Table.RemoveColumns` -- produced no line, no rule and no finding, and
    `result` pointed at the second step's frame.

    The second shape is two steps with the identical raw name. That is not
    valid M (a `let` cannot bind a name twice), but the Power Query parser
    accepts it and `_unique_names` is keyed by raw name, so the two
    collapsed into one mapping entry and the first step vanished.
    """

    def test_three_names_in_one_sanitized_family_get_three_variables(self):
        allocated = m._unique_names([_step('#"a b"', "Table.Buffer"),
                                     _step('#"a_b"', "Table.Buffer"),
                                     _step('#"a_b_2"', "Table.Buffer")])
        self.assertEqual(len(set(allocated.values())), 3, allocated)

    def test_the_reported_shape_emits_every_step(self):
        result = _translate(
            _step('#"a b"', "Table.SelectRows", ["Nav2", "each [a] > 1"],
                  inputs=["Nav2"]),
            _step('#"a_b"', "Table.SelectColumns", ['#"a b"', '{"x"}'],
                  inputs=['#"a b"']),
            _step('#"a_b_2"', "Table.RemoveColumns", ['#"a_b"', '{"y"}'],
                  inputs=['#"a_b"']))
        rules = [f.rule for f in result.findings]
        self.assertIn("M22_SELECT_ROWS", rules)
        self.assertIn("M24_SELECT_COLUMNS", rules)
        self.assertIn("M25_REMOVE_COLUMNS", rules)
        self.assertIn("_m_drop(", result.translated_sql)
        self.assertEqual(undefined_names(result.translated_sql), [])

    def test_the_result_is_the_last_step_not_the_one_that_shadowed_it(self):
        result = _translate(
            _step('#"a b"', "Table.SelectRows", ["Nav2", "each [a] > 1"],
                  inputs=["Nav2"]),
            _step('#"a_b"', "Table.SelectColumns", ['#"a b"', '{"x"}'],
                  inputs=['#"a b"']),
            _step('#"a_b_2"', "Table.RemoveColumns", ['#"a_b"', '{"y"}'],
                  inputs=['#"a_b"']))
        lines = result.translated_sql.strip().splitlines()
        # The call site, not the helper definition that also mentions the name.
        drop_line = [line for line in lines if " = _m_drop(" in line][0]
        self.assertEqual(lines[-1], "result = %s" % drop_line.split(" =")[0])

    def test_no_two_steps_ever_share_a_variable(self):
        # The property, stated once, over every family that collides here:
        # a sanitize collision, a numbered-name collision, and a collision
        # with a reserved name and its numbered form.
        for names in (['#"a b"', '#"a_b"', '#"a_b_2"'],
                      ["X", '#"x"', '#"x 2"', "x_2", '#"x_2_2"'],
                      ["Spark", '#"spark 2"', '#"spark"'],
                      ['#"Added custom"', '#"Added  custom"', "Added_custom_2"]):
            with self.subTest(names=names):
                allocated = m._unique_names([_step(n, "Table.Buffer")
                                             for n in names])
                self.assertEqual(len(set(allocated.values())), len(names),
                                 allocated)

    def test_every_allocated_variable_is_a_python_identifier(self):
        allocated = m._unique_names([_step(n, "Table.Buffer") for n in
                                     ('#"a b"', '#"a_b"', "class", '#"1st"')])
        for variable in allocated.values():
            self.assertTrue(variable.isidentifier(), variable)

    def test_the_existing_duplicate_numbering_is_unchanged(self):
        allocated = m._unique_names([_step('#"Added custom"', "Table.Buffer"),
                                     _step('#"Added  custom"', "Table.Buffer")])
        self.assertEqual(list(allocated.values()),
                         ["added_custom", "added_custom_2"])

    def test_two_steps_with_the_identical_name_are_refused(self):
        result = _translate(
            _step("A", "Table.SelectRows", ["Nav2", "each [a] > 1"],
                  inputs=["Nav2"]),
            _step("A", "Table.SelectColumns", ["Nav2", '{"x"}'],
                  inputs=["Nav2"]),
            final="A")
        self.assertEqual(result.translated_sql, "")
        self.assertIn("M90_UNSUPPORTED_STEP", [f.rule for f in result.findings])
        self.assertIn("twice", str(result.findings))

    def test_a_step_named_like_a_letter_of_the_previous_line_still_emits(self):
        # The `emitted` set must stay a set of step *names*. Holding the
        # previous step's emitted expression there instead turns the skip
        # into a substring test: a step called `l` matched inside
        # `nav2.filter(F.lit(True))` and was dropped, leaving `result = l`
        # reading a name the file never binds. Caught by the invariant
        # below, not by any single rule's test.
        for name in ("l", "a", "F", "i", "t", "e", "r"):
            with self.subTest(name=name):
                result = _translate(
                    _step("Pre", "Table.SelectRows", ["Nav2", "each [a] > 1"],
                          inputs=["Nav2"]),
                    _step(name, "Table.SelectColumns", ["Pre", '{"x"}'],
                          inputs=["Pre"]),
                    final=name)
                self.assertIn("M24_SELECT_COLUMNS",
                              [f.rule for f in result.findings])
                self.assertEqual(undefined_names(result.translated_sql), [])

    def test_the_source_step_is_still_emitted_exactly_once(self):
        # The skip that hid the third step was `does a line already start
        # with this variable?`, and it was there to stop the lakehouse fold
        # re-emitting its own last step. That job is now done by an explicit
        # set of step names, so this has to keep working.
        self.assertEqual(_translate().translated_sql.count("spark.table("), 1)


@requires_node
class NameAllocationThroughParserTests(unittest.TestCase):
    def test_the_reported_shape_no_longer_drops_the_third_step(self):
        result = translate_m(
            'let %s, #"a b" = Table.SelectRows(Nav2, each true), '
            '#"a_b" = Table.SelectColumns(#"a b", {"x"}), '
            '#"a_b_2" = Table.RemoveColumns(#"a_b", {"y"}) in #"a_b_2"'
            % LAKEHOUSE_M)
        self.assertIn("M25_REMOVE_COLUMNS", [f.rule for f in result.findings])
        self.assertIn("_m_drop(", result.translated_sql)

    def test_a_duplicated_step_name_is_refused_rather_than_half_emitted(self):
        result = translate_m(
            'let %s, A = Table.SelectRows(Nav2, each true), '
            'A = Table.SelectColumns(Nav2, {"x"}) in A' % LAKEHOUSE_M)
        self.assertEqual(result.translated_sql, "")
        self.assertIn("twice", str(result.findings))


# --------------------------------------------------------------------------
# D7 -- Table.Combine input detection by substring regex


def combine_steps(*extra, **kwargs):
    """Two frames to union, plus whatever the case under test adds."""
    return list(extra)


class CombineInputTests(unittest.TestCase):
    """CONFIRMED, in two shapes -- and the reporter's own shape REFUTED.

    `_rule_combine` collected a frame for every step name that appeared, as
    a whole word, anywhere in the step's argument *text*. The word
    boundaries do their job, so `Other` and `OtherLong` never confused it
    (refuted). What they cannot do is tell a table reference from a name
    that happens to appear somewhere else in the same text:

      Table.Combine({A, B}, {"Zed"})  -- `Zed` is a *column* name in the
          optional second argument. With a step also called `Zed`, the
          regex matched inside the string literal and unioned a third
          frame M does not union.

      Table.Combine({A, Table.SelectRows(B, each [q] > 1)})  -- the regex
          found `B` inside the nested call, unioned the *unfiltered* `b`,
          and dropped the filter with no finding. This is the same defect
          `_input_var` was fixed for, which `Table.Combine` bypasses
          because it resolves its own inputs.
    """

    A = _step("A", "Table.SelectRows", ["Nav2", "each [a] > 1"], inputs=["Nav2"])
    B = _step("B", "Table.SelectColumns", ["Nav2", '{"x"}'], inputs=["Nav2"])

    def test_prefix_names_were_never_the_problem(self):
        # REFUTED, kept as the evidence: `\b` boundaries already handled it.
        result = _translate(
            _step("Other", "Table.SelectRows", ["Nav2", "each [a] > 1"],
                  inputs=["Nav2"]),
            _step("OtherLong", "Table.SelectColumns", ["Nav2", '{"x"}'],
                  inputs=["Nav2"]),
            _step("Out", "Table.Combine", ["{Other, OtherLong}"]))
        self.assertIn("out = other.unionByName(otherlong, "
                      "allowMissingColumns=True)", result.translated_sql)

    def test_a_name_inside_the_optional_column_list_is_not_a_frame(self):
        result = _translate(
            self.A, self.B,
            _step("Zed", "Table.SelectColumns", ["Nav2", '{"y"}'],
                  inputs=["Nav2"]),
            _step("Out", "Table.Combine", ["{A, B}", '{"Zed"}']))
        self.assertEqual(result.translated_sql, "")
        self.assertNotIn("M27_COMBINE", [f.rule for f in result.findings])

    def test_the_optional_column_list_is_refused_rather_than_ignored(self):
        # unionByName(allowMissingColumns=True) keeps every column; M's
        # second argument keeps exactly the ones it names. Dropping it
        # silently would be the same class of bug one layer down.
        result = _translate(self.A, self.B,
                            _step("Out", "Table.Combine", ["{A, B}", '{"x"}']))
        self.assertEqual(result.translated_sql, "")
        self.assertIn("Table.Combine", str(result.findings))

    def test_a_nested_call_in_the_list_is_refused_not_unwrapped(self):
        result = _translate(
            self.A, self.B,
            _step("Out", "Table.Combine",
                  ["{A, Table.SelectRows(B, each [q] > 1)}"]))
        self.assertEqual(result.translated_sql, "")
        self.assertNotIn("unionByName", result.translated_sql)
        self.assertIn("M90_UNSUPPORTED_STEP", [f.rule for f in result.findings])
        self.assertIn("Table.SelectRows(B", str(result.findings))

    def test_the_union_order_comes_from_the_list_not_the_step_order(self):
        result = _translate(self.A, self.B,
                            _step("Out", "Table.Combine", ["{B, A}"]))
        self.assertIn("out = b.unionByName(a, allowMissingColumns=True)",
                      result.translated_sql)

    def test_a_frame_listed_twice_is_unioned_twice(self):
        # M appends the table to itself; deduplicating the list halved the
        # row count.
        result = _translate(self.A,
                            _step("Out", "Table.Combine", ["{A, A}"]))
        self.assertIn("out = a.unionByName(a, allowMissingColumns=True)",
                      result.translated_sql)

    def test_a_computed_list_is_refused_by_name(self):
        # The only Table.Combine shape in the reference corpus that is not a
        # literal list: Table.Combine(List.Transform(years, fxGenerate)).
        result = _translate(
            self.A,
            _step("Out", "Table.Combine", ["List.Transform(A, fxGenerate)"]))
        self.assertEqual(result.translated_sql, "")
        self.assertIn("List.Transform", str(result.findings))

    def test_a_string_in_the_list_is_not_a_step_reference(self):
        result = _translate(self.A, self.B,
                            _step("Out", "Table.Combine", ['{A, "B"}']))
        self.assertEqual(result.translated_sql, "")

    def test_quoted_and_parenthesised_references_still_resolve(self):
        for listed in ('{#"A", B}', "{(A), B}", '{((#"A")), B}'):
            with self.subTest(listed=listed):
                result = _translate(self.A, self.B,
                                    _step("Out", "Table.Combine", [listed]))
                self.assertIn("out = a.unionByName(b, allowMissingColumns=True)",
                              result.translated_sql)

    def test_a_one_table_list_is_refused_by_count(self):
        result = _translate(self.A, _step("Out", "Table.Combine", ["{A}"]))
        self.assertEqual(result.translated_sql, "")
        self.assertIn("Table.Combine", str(result.findings))

    def test_three_frames_chain(self):
        result = _translate(
            self.A, self.B,
            _step("C", "Table.SelectColumns", ["Nav2", '{"z"}'], inputs=["Nav2"]),
            _step("Out", "Table.Combine", ["{A, B, C}"]))
        self.assertIn("out = a.unionByName(b, allowMissingColumns=True)"
                      ".unionByName(c, allowMissingColumns=True)",
                      result.translated_sql)


class MListItemTests(unittest.TestCase):
    """The splitter `_rule_combine` leans on. A comma inside a nested list,
    a call or a string is not a separator."""

    def test_a_flat_list_splits(self):
        self.assertEqual(m._m_list_items("{A, B}"), ["A", "B"])

    def test_a_nested_list_keeps_its_commas(self):
        self.assertEqual(m._m_list_items("{A, {B, C}}"), ["A", "{B, C}"])

    def test_a_call_keeps_its_commas(self):
        self.assertEqual(m._m_list_items("{A, f(B, C)}"), ["A", "f(B, C)"])

    def test_a_record_keeps_its_commas(self):
        self.assertEqual(m._m_list_items('{A, [x = 1, y = 2]}'),
                         ["A", "[x = 1, y = 2]"])

    def test_a_string_keeps_its_commas_and_its_braces(self):
        self.assertEqual(m._m_list_items('{A, "b, c}"}'), ["A", '"b, c}"'])

    def test_a_doubled_quote_inside_a_string_is_not_a_terminator(self):
        self.assertEqual(m._m_list_items('{"a""b, c", D}'), ['"a""b, c"', "D"])

    def test_an_empty_list_has_no_items(self):
        self.assertEqual(m._m_list_items("{}"), [])

    def test_something_that_is_not_a_list_is_none(self):
        for text in ("List.Transform(x, f)", "A", "", None, "{A", "A}",
                     '{"unterminated}'):
            with self.subTest(text=text):
                self.assertIsNone(m._m_list_items(text))


@requires_node
class CombineInputThroughParserTests(unittest.TestCase):
    def test_the_column_list_shape_no_longer_unions_a_third_frame(self):
        result = translate_m(
            'let %s, A = Table.SelectRows(Nav2, each true), '
            'B = Table.SelectColumns(Nav2, {"x"}), '
            'Zed = Table.SelectColumns(Nav2, {"y"}), '
            'Out = Table.Combine({A, B}, {"Zed"}) in Out' % LAKEHOUSE_M)
        self.assertNotIn("zed", result.translated_sql)
        self.assertEqual(result.translated_sql, "")

    def test_the_nested_call_shape_no_longer_drops_the_filter(self):
        result = translate_m(
            'let %s, A = Table.SelectRows(Nav2, each true), '
            'B = Table.SelectColumns(Nav2, {"x"}), '
            'Out = Table.Combine({A, Table.SelectRows(B, each [q] > 1)}) '
            'in Out' % LAKEHOUSE_M)
        self.assertEqual(result.translated_sql, "")

    def test_the_plain_shape_still_unions(self):
        result = translate_m(
            'let %s, A = Table.SelectRows(Nav2, each true), '
            'B = Table.SelectColumns(Nav2, {"x"}), '
            'Out = Table.Combine({A, B}) in Out' % LAKEHOUSE_M)
        self.assertIn("out = a.unionByName(b, allowMissingColumns=True)",
                      result.translated_sql)


if __name__ == "__main__":
    unittest.main()
