import ast
import importlib.util
import os
import shutil
import unittest
from pathlib import Path

from fabric_aidp.translate import m_runtime
from fabric_aidp.translate.m_runtime import HELPERS, REQUIRES, prelude_for
from fabric_aidp.translate.m_to_pyspark import sanitize


def spark_available() -> bool:
    """True where a helper body can actually be run, not just parsed.

    pyspark is not a dependency of this project and must not become one: the
    suite has to pass with no Spark, no Node and no network. So the execution
    tests below skip where Spark is missing, exactly as tests/test_m_corpus.py
    skips on Node and on the corpus directory.

    The JVM is part of the check. pyspark imports perfectly well without one
    and then fails inside SparkSession.getOrCreate, which would surface as an
    error instead of a skip -- the same "this machine has no Spark" case,
    reported as if the code were broken.
    """
    if importlib.util.find_spec("pyspark") is None:
        return False
    java_home = os.environ.get("JAVA_HOME")
    if java_home and (Path(java_home) / "bin" / "java").is_file():
        return True
    return shutil.which("java") is not None


class PreludeTests(unittest.TestCase):
    def test_nothing_needed_emits_nothing(self):
        self.assertEqual(prelude_for([]), "")

    def test_helper_pulls_in_what_it_calls(self):
        source = prelude_for(["_m_text_columns"])
        self.assertIn("def _m_text_columns(", source)
        self.assertIn("def _m_text(", source)
        self.assertIn("_M_TIMESTAMP_TYPES", source)

    def test_every_helper_parses_on_its_own(self):
        for name in HELPERS:
            with self.subTest(helper=name):
                ast.parse(prelude_for([name]))

    def test_all_together_define_exactly_their_names(self):
        tree = ast.parse(prelude_for(list(HELPERS)))
        defined = {node.name for node in tree.body
                   if isinstance(node, ast.FunctionDef)}
        self.assertEqual(defined, set(HELPERS))

    def test_dependency_is_defined_before_its_caller(self):
        source = prelude_for(["_m_add_days"])
        self.assertLess(source.index("def _m_is_timestamp("),
                        source.index("def _m_add_days("))

    def test_unknown_helper_is_an_error_not_a_silent_omission(self):
        with self.assertRaises(KeyError):
            prelude_for(["_m_nonsense"])

    def test_module_imports_without_a_syntax_warning(self):
        # The helper sources contain regex backslashes. A non-raw outer
        # literal makes those invalid escape sequences: a warning now, a
        # SyntaxError in a later Python.
        import importlib
        import warnings

        import fabric_aidp.translate.m_runtime as module
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            importlib.reload(module)
        self.assertEqual(
            [str(w.message) for w in caught
             if issubclass(w.category, SyntaxWarning)], [])

    def test_no_step_name_can_collide_with_a_helper(self):
        # sanitize() strips leading underscores, so a step called "_m_text"
        # becomes "m_text" and cannot shadow the helper.
        for name in HELPERS:
            self.assertTrue(name.startswith("_m_"), name)
            self.assertNotEqual(sanitize(name), name)


def _defined_at_module_level(source):
    """Every name the assembled prelude binds at module level."""
    names = set()
    for node in ast.parse(source).body:
        if isinstance(node, ast.FunctionDef):
            names.add(node.name)
        elif isinstance(node, ast.Assign):
            names.update(target.id for target in node.targets
                         if isinstance(target, ast.Name))
        elif isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name):
            names.add(node.target.id)
    return names


def _generated_names_read(source):
    """Every `_m_`/`_M_` name the prelude reads.

    Loads rather than calls only: a helper that took another helper's name
    without calling it would need the same dependency edge, and the failure
    -- NameError on the cluster -- is identical.

    No helper has a local starting with `_m_`, so a bare Name walk needs no
    scope analysis. If one ever does, this reports a missing dependency that
    is not missing, which is a loud false alarm rather than a silent pass.
    """
    return {node.id for node in ast.walk(ast.parse(source))
            if isinstance(node, ast.Name) and isinstance(node.ctx, ast.Load)
            and node.id.startswith(("_m_", "_M_"))}


class HelperWiringTests(unittest.TestCase):
    """The third edge of the helper-wiring contract.

    tests/test_m_expr.py's test_every_entry_emits_the_name_it_registers closes
    two of the three: the emitted call name equals the registered helper name,
    and the registered name is in HELPERS. Nothing closed *helper body ->
    REQUIRES*.

    Measured: deleting `"_m_end_of_month": ("_m_is_timestamp",),` from REQUIRES
    produces a prelude that ast.parse accepts, calls an undefined
    _m_is_timestamp, and leaves the whole suite green -- exactly the
    NameError-on-the-cluster the other two edges were closed to prevent. With
    this test the same deletion fails, naming both helpers.
    """

    def test_every_helper_prelude_defines_everything_that_prelude_reads(self):
        # prelude_for() walks REQUIRES transitively, so asking it for one
        # helper and then resolving the result is the transitive-closure
        # check: anything the body reads and the closure does not define is
        # a missing REQUIRES edge.
        for name in HELPERS:
            with self.subTest(helper=name):
                source = prelude_for([name])
                missing = _generated_names_read(source) - _defined_at_module_level(source)
                self.assertEqual(
                    missing, set(),
                    "prelude_for([%r]) calls %s but never defines it -- add it "
                    "to REQUIRES[%r]" % (name, ", ".join(sorted(missing)), name))

    def test_requires_only_names_real_helpers(self):
        # A typo on either side of REQUIRES is the same bug seen later:
        # prelude_for raises KeyError for an unknown dependency, and an
        # unknown key is an edge that silently never fires.
        for caller, dependencies in REQUIRES.items():
            with self.subTest(caller=caller):
                self.assertIn(caller, HELPERS)
                for dependency in dependencies:
                    self.assertIn(dependency, HELPERS)



def _bound_by(source):
    """Module-level definitions plus imported names -- what `source` binds."""
    names = _defined_at_module_level(source)
    for node in ast.parse(source).body:
        if isinstance(node, ast.ImportFrom):
            names.update(alias.asname or alias.name for alias in node.names)
    return names


def _names_read(source):
    return {node.id for node in ast.walk(ast.parse(source))
            if isinstance(node, ast.Name) and isinstance(node.ctx, ast.Load)}


class PreambleWiringTests(unittest.TestCase):
    """PREAMBLE goes only where a helper reads what it binds.

    It used to be emitted whenever ANY helper was, so a file that needed
    only `_m_add_column`, `_m_drop`, `_m_rename` or `_m_int64` -- none of
    which reads `T` or `_M_TIMESTAMP_TYPES` -- carried an import and a
    tuple nothing in it used.
    """

    def test_the_declaration_matches_what_each_body_reads(self):
        # Read from the bodies, so a helper that starts reading `T` without
        # being declared fails here rather than NameError-ing on a cluster,
        # and a declaration left behind by a rewrite fails too.
        bound = _bound_by(m_runtime.PREAMBLE)
        self.assertEqual(bound, {"T", "_M_TIMESTAMP_TYPES"})
        for name, body in HELPERS.items():
            with self.subTest(helper=name):
                self.assertEqual(name in m_runtime.USES_PREAMBLE,
                                 bool(_names_read(body) & bound))

    def test_a_helper_that_reads_nothing_of_it_gets_no_preamble(self):
        for names in (["_m_add_column"], ["_m_drop"], ["_m_rename"],
                      ["_m_int64"],
                      ["_m_add_column", "_m_drop", "_m_rename", "_m_int64"]):
            with self.subTest(names=names):
                source = prelude_for(names)
                ast.parse(source)
                self.assertNotIn("types as T", source)
                self.assertNotIn("_M_TIMESTAMP_TYPES", source)
                self.assertTrue(source.startswith("\n\ndef "), source[:40])

    def test_a_helper_that_reads_it_still_gets_it_directly_or_transitively(self):
        # _m_add_days reads neither name itself; _m_is_timestamp, which it
        # calls, does. So the check runs on the closure, not the request.
        for names in (["_m_text"], ["_m_number"], ["_m_is_timestamp"],
                      ["_m_add_days"], ["_m_text_columns"],
                      ["_m_drop", "_m_start_of_week"]):
            with self.subTest(names=names):
                source = prelude_for(names)
                ast.parse(source)
                self.assertTrue(source.startswith(m_runtime.PREAMBLE))

    def test_every_helper_alone_defines_everything_it_reads_from_the_preamble(self):
        bound = _bound_by(m_runtime.PREAMBLE)
        for name in HELPERS:
            with self.subTest(helper=name):
                source = prelude_for([name])
                read = _names_read(source) & bound
                self.assertEqual(read - _bound_by(source), set())


# value -> what Power Query writes for it. Every row was executed on Spark
# 4.2.0; nothing here is inferred. The first three are a deliberate
# divergence -- Spark's spelling, because M's could not be verified here --
# and are recorded as such in _m_text's own docstring.
#
# The pre-fix helper aborted the job on the first three and on two of the
# decimal shapes below ([INVALID_ARRAY_INDEX] under Spark 4's default ANSI),
# rendered 1e300 as '1E+30', put [1e-5, 1e-4) on the fixed-point side, and
# dropped digits from 0.00012345678901234567. All measured, all under a PASS,
# because nothing in this suite could run a helper body.
M_TEXT_DOUBLES = [
    (float("nan"), "NaN"),
    (float("inf"), "Infinity"),
    (float("-inf"), "-Infinity"),
    (3.0, "3"),
    (1e7, "10000000"),
    (123456789.123, "123456789.123"),
    (0.0, "0"),
    (-2.5, "-2.5"),
    (1e-4, "0.0001"),          # exponent -4: .NET "G" is still fixed-point
    (1e-5, "1E-05"),           # exponent -5: "G" goes scientific here
    (5e-5, "5E-05"),
    (9.99e-5, "9.99E-05"),
    (1e15, "1E+15"),
    (1e14, "100000000000000"),
    (1e300, "1E+300"),         # lpad truncated this to '1E+30'
    (1e100, "1E+100"),
    (1e-100, "1E-100"),
    (9.99e99, "9.99E+99"),
    (1.0 / 3.0, "0.3333333333333333"),
    (0.00012345678901234567, "0.00012345678901234567"),
]

# Spark type, SQL literal, what M writes. decimal(19,4) is what M_TYPES maps
# Currency.Type to; decimal(38,0) is an ordinary surrogate key.
#
# The first four rows are all this table held when the decimal arm was first
# written, and all four have 16 integer digits or fewer. The arm round-tripped
# through decimal(38,20), which holds 18 -- so the band stopped three digits
# short of the failure and the arm shipped aborting the job on anything wider:
# [NUMERIC_VALUE_OUT_OF_RANGE] on decimal(38,0) holding 9223372036854775807,
# a value a plain cast renders correctly. The wide and high-scale rows below
# are the ones that would have caught it, and the reason this table now goes
# past what any corpus query happens to use.
M_TEXT_DECIMALS = [
    ("decimal(38,0)", "1234567890123456", "1234567890123456"),
    ("decimal(38,2)", "1000000000000000.00", "1000000000000000"),
    ("decimal(38,10)", "0.0000010000", "0.000001"),
    ("decimal(19,4)", "12345.6789", "12345.6789"),
    # 19 integer digits: bigint's ceiling, Oracle NUMBER, numeric(38,0).
    ("decimal(38,0)", "9223372036854775807", "9223372036854775807"),
    ("decimal(38,0)", "12345678901234567890", "12345678901234567890"),
    ("decimal(38,2)", "12345678901234567890.12", "12345678901234567890.12"),
    # 18 integer digits: the last width the old intermediate survived.
    ("decimal(38,0)", "999999999999999999", "999999999999999999"),
    # The dot-guard: trailing zeros go only after a decimal point, or this
    # renders '1'.
    ("decimal(38,0)", "1000", "1000"),
    # High scale: the old intermediate silently flattened this to '0'.
    ("decimal(38,25)", "0.0000000000000000000000001",
     "0.0000000000000000000000001"),
    ("decimal(38,24)", "1.234567890123456789012345",
     "1.234567890123456789012345"),
    ("decimal(38,2)", "0.00", "0"),
    ("decimal(19,4)", "-12345.6789", "-12345.6789"),
]

# 2024-03-15 is a Friday, so Sunday-based and Monday-based weeks differ.
DATE_SQL = "SELECT DATE'2024-03-15' AS v"
TIMESTAMP_SQL = "SELECT TIMESTAMP'2024-03-15 13:45:30' AS v"

# helper, extra arguments, answer on a date, answer on a timestamp. Measured.
# The timestamp column is the point of every one of these: Spark's own
# date_add/add_months/last_day/trunc return a date and drop the time of day,
# which M keeps.
DATE_HELPERS = [
    ("_m_add_days", ("F.lit(1)",), "2024-03-16", "2024-03-16 13:45:30"),
    ("_m_add_months", ("F.lit(1)",), "2024-04-15", "2024-04-15 13:45:30"),
    ("_m_start_of_month", (), "2024-03-01", "2024-03-01 00:00:00"),
    ("_m_end_of_month", (), "2024-03-31", "2024-03-31 23:59:59.999999"),
    ("_m_start_of_year", (), "2024-01-01", "2024-01-01 00:00:00"),
    ("_m_end_of_year", (), "2024-12-31", "2024-12-31 23:59:59.999999"),
    # M's default first day of the week is Sunday: 2024-03-10, not 2024-03-11.
    ("_m_start_of_week", (), "2024-03-10", "2024-03-10 00:00:00"),
]


@unittest.skipUnless(spark_available(), "pyspark and a JVM not available")
class HelperExecutionTests(unittest.TestCase):
    """The only tests that run a helper body.

    Everything above stops at ast.parse, which accepts a call to a name that
    does not exist and cannot see a value at all. That gap is how _m_text
    shipped a version that aborted the job on NaN or either infinity,
    truncated every exponent of 100 or more, returned NULL for a decimal
    outside a narrow band, cut over to scientific notation a decade too low,
    and dropped significant digits near 1e-4 -- five defects, all reachable,
    all under a green suite.

    The prelude is exec'd exactly as a generated notebook would see it:
    assembled by prelude_for, with F and T in the namespace and no import of
    this project anywhere in it.
    """

    @classmethod
    def setUpClass(cls):
        from pyspark.sql import SparkSession, functions as F, types as T
        cls.F, cls.T = F, T
        cls.spark = (SparkSession.builder.master("local[1]")
                     .appName("fabric-aidp helper execution").getOrCreate())
        cls.spark.sparkContext.setLogLevel("FATAL")
        cls.generated = {"F": F, "T": T}
        exec(compile(prelude_for(list(HELPERS)), "<prelude>", "exec"),
             cls.generated)

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_the_prelude_defines_every_helper_and_nothing_imports_this_tool(self):
        # A generated notebook runs on a cluster that has never heard of
        # fabric-aidp, so the prelude has to stand alone.
        for name in HELPERS:
            self.assertTrue(callable(self.generated.get(name)), name)
        self.assertNotIn("fabric_aidp", prelude_for(list(HELPERS)))

    def test_m_text_writes_what_m_writes_for_every_measured_double(self):
        F, T = self.F, self.T
        schema = T.StructType([T.StructField("i", T.IntegerType(), False),
                               T.StructField("v", T.DoubleType(), True)])
        df = self.spark.createDataFrame(
            [(i, value) for i, (value, _) in enumerate(M_TEXT_DOUBLES)], schema)
        rendered = {row["i"]: row["t"] for row in df.select(
            "i", self.generated["_m_text"](df, F.col("v")).alias("t")).collect()}
        for index, (value, expected) in enumerate(M_TEXT_DOUBLES):
            with self.subTest(value=repr(value)):
                self.assertEqual(rendered[index], expected)

    def test_m_text_writes_what_m_writes_for_every_decimal_shape(self):
        for spark_type, literal, expected in M_TEXT_DECIMALS:
            with self.subTest(column="%s %s" % (spark_type, literal)):
                df = self.spark.sql(
                    "SELECT CAST(%s AS %s) AS v" % (literal, spark_type))
                rendered = df.select(
                    self.generated["_m_text"](df, self.F.col("v"))).collect()[0][0]
                self.assertEqual(rendered, expected)

    def test_m_text_writes_zero_for_negative_zero(self):
        # Recorded, not endorsed: the zero branch wins, which is .NET
        # Framework's answer. .NET Core 3.0 and later write "-0", and there is
        # no Power BI here to say which one M follows. Asserting it means a
        # change to the zero branch has to face the choice.
        T = self.T
        df = self.spark.createDataFrame(
            [(-0.0,)], T.StructType([T.StructField("v", T.DoubleType())]))
        self.assertEqual(df.select(
            self.generated["_m_text"](df, self.F.col("v"))).collect()[0][0], "0")

    def test_m_text_keeps_null_as_null(self):
        for spark_type in ("double", "decimal(19,4)"):
            with self.subTest(type=spark_type):
                df = self.spark.sql("SELECT CAST(NULL AS %s) AS v" % spark_type)
                self.assertIsNone(df.select(
                    self.generated["_m_text"](df, self.F.col("v"))).collect()[0][0])

    def test_m_text_leaves_a_non_float_column_to_the_plain_cast(self):
        # Spark's cast already writes what M writes for these; the rendering
        # work exists only because a double's cast does not.
        for literal, expected in (("CAST(42 AS bigint)", "42"),
                                  ("'already text'", "already text"),
                                  ("true", "true")):
            with self.subTest(literal=literal):
                df = self.spark.sql("SELECT %s AS v" % literal)
                self.assertEqual(df.select(
                    self.generated["_m_text"](df, self.F.col("v"))).collect()[0][0],
                    expected)

    def test_m_text_refuses_a_date_and_a_timestamp(self):
        for sql in (DATE_SQL, TIMESTAMP_SQL):
            with self.subTest(sql=sql):
                df = self.spark.sql(sql)
                with self.assertRaises(TypeError) as caught:
                    self.generated["_m_text"](df, self.F.col("v"))
                self.assertIn("culture-dependent", str(caught.exception))

    def test_m_number_casts_a_number_and_refuses_what_a_cast_would_mangle(self):
        for literal, expected in (("CAST(42 AS bigint)", 42.0),
                                  ("CAST(12345.6789 AS decimal(19,4))", 12345.6789),
                                  ("CAST(2.5 AS double)", 2.5),
                                  ("true", 1.0)):
            with self.subTest(literal=literal):
                df = self.spark.sql("SELECT %s AS v" % literal)
                self.assertEqual(df.select(
                    self.generated["_m_number"](df, self.F.col("v"))).collect()[0][0],
                    expected)
        # The timestamp is the one that matters: cast('double') answers
        # 1710510330.0 -- epoch seconds -- where M answers ~45366.57, an OLE
        # automation serial. Measured; the cast is not an error, it is a
        # plausible wrong number.
        for sql in (DATE_SQL, TIMESTAMP_SQL, "SELECT '1,5' AS v"):
            with self.subTest(sql=sql):
                df = self.spark.sql(sql)
                with self.assertRaises(TypeError):
                    self.generated["_m_number"](df, self.F.col("v"))

    def test_m_text_columns_renders_each_named_column(self):
        df = self.spark.sql("SELECT CAST(1e7 AS double) AS a, "
                            "CAST(3.0 AS double) AS b, CAST(2.5 AS double) AS c")
        row = self.generated["_m_text_columns"](df, "a", "b").collect()[0]
        self.assertEqual((row["a"], row["b"]), ("10000000", "3"))
        # The column it was not given keeps its type, rather than everything
        # in the frame being swept into text.
        self.assertEqual(row["c"], 2.5)

    def test_m_drop_raises_on_a_missing_column_where_spark_would_not(self):
        # The measurement this helper exists for, re-run here: the bare
        # `.drop` is silent, `_m_drop` is not.
        df = self.spark.sql("SELECT 1 AS a, 2 AS b")
        self.assertEqual(df.drop("nope").columns, ["a", "b"])
        with self.assertRaises(ValueError):
            self.generated["_m_drop"](df, "nope")
        self.assertEqual(self.generated["_m_drop"](df, "a").columns, ["b"])

    def test_m_rename_raises_on_a_missing_column_where_spark_would_not(self):
        df = self.spark.sql("SELECT 1 AS a, 2 AS b")
        self.assertEqual(df.withColumnRenamed("nope", "x").columns, ["a", "b"])
        with self.assertRaises(ValueError):
            self.generated["_m_rename"](df, ("nope", "x"))
        self.assertEqual(
            self.generated["_m_rename"](df, ("a", "x")).columns, ["x", "b"])

    def test_m_drop_is_case_sensitive_where_sparks_own_resolution_is_not(self):
        # Spark resolves column names case-insensitively by default, so the
        # bare drop takes `id` when asked for `ID`. M is case-sensitive.
        df = self.spark.sql("SELECT 1 AS id")
        self.assertEqual(df.drop("ID").columns, [])
        with self.assertRaises(ValueError):
            self.generated["_m_drop"](df, "ID")

    def test_is_timestamp_separates_the_two_families(self):
        self.assertFalse(self.generated["_m_is_timestamp"](
            self.spark.sql(DATE_SQL), self.F.col("v")))
        self.assertTrue(self.generated["_m_is_timestamp"](
            self.spark.sql(TIMESTAMP_SQL), self.F.col("v")))

    def test_every_date_helper_answers_what_it_was_measured_to_answer(self):
        for name, extra, on_date, on_timestamp in DATE_HELPERS:
            for sql, expected in ((DATE_SQL, on_date),
                                  (TIMESTAMP_SQL, on_timestamp)):
                with self.subTest(helper=name, sql=sql):
                    df = self.spark.sql(sql)
                    arguments = [eval(a, {"F": self.F}) for a in extra]
                    column = self.generated[name](df, self.F.col("v"), *arguments)
                    self.assertEqual(
                        str(df.select(column).collect()[0][0]), expected)

    def test_start_of_week_takes_monday_when_m_asked_for_monday(self):
        for sql, expected in ((DATE_SQL, "2024-03-11"),
                              (TIMESTAMP_SQL, "2024-03-11 00:00:00")):
            with self.subTest(sql=sql):
                df = self.spark.sql(sql)
                column = self.generated["_m_start_of_week"](
                    df, self.F.col("v"), monday=True)
                self.assertEqual(
                    str(df.select(column).collect()[0][0]), expected)
