"""Converted expressions, executed against real Spark and checked by RESULT.

Every other expression test in this suite asserts the emitted string. This
file asserts what the emitted code actually computes, which is a different
question and the one that matters.

Each test states the Informatica behaviour it is checking, so a reader can
challenge the expectation rather than only the code.
"""
from __future__ import annotations

import pytest

from pyspark.sql.functions import col as F_col

from tests.conftest_spark import run_agg, run_expr, spark  # noqa: F401


# ── NULL handling ──────────────────────────────────────────────────────

def test_concat_treats_null_as_empty_string(spark):
    """Informatica CONCAT treats NULL as ''; Spark's concat returns NULL if
    any argument is NULL. A full name built from first/middle/last must not
    blank out for a row with no middle name."""
    out = run_expr(spark, "CONCAT(A, B)",
                   [("John", None), ("Jane", "Doe")], "A string, B string")
    assert out == ["John", "JaneDoe"]


def test_decode_matches_null_to_null(spark):
    """DECODE(x, NULL, 'missing') fires when x IS NULL. Emitting
    `x == NULL` makes that branch unreachable."""
    out = run_expr(spark, "DECODE(S, NULL, 'MISSING', 'A', 'Active')",
                   [(None,), ("A",), ("Z",)], "S string")
    assert out == ["MISSING", "Active", None]


# ── The defect string assertions could not see ─────────────────────────

def test_ltrim_with_a_trim_set_runs_and_trims_a_character_set(spark):
    """The case that proved the point.

    This was emitted as F.ltrim(col, F.lit('xy')) -- reviewed, judged
    correct, pinned by a string assertion, and a runtime TypeError because
    PySpark's ltrim takes one argument. It also has to treat 'xy' as a SET
    of characters, not a substring.
    """
    out = run_expr(spark, "LTRIM(S, 'xy')",
                   [("xyxyhello",), ("yxworld",), ("hello",)], "S string")
    assert out == ["hello", "world", "hello"]


def test_rtrim_with_a_trim_set_runs_and_trims_a_character_set(spark):
    out = run_expr(spark, "RTRIM(S, 'xy')",
                   [("helloxyxy",), ("worldyx",), ("hello",)], "S string")
    assert out == ["hello", "world", "hello"]


def test_replacechr_replaces_each_character_not_the_substring(spark):
    """REPLACECHR(0, S, 'aeiou', '*') replaces every vowel independently.
    regexp_replace would look for the literal substring 'aeiou'."""
    out = run_expr(spark, "REPLACECHR(0, S, 'aeiou', '*')",
                   [("banana",), ("xyz",)], "S string")
    assert out == ["b*n*n*", "xyz"]


# ── Values that were quietly truncated ─────────────────────────────────

def test_date_diff_in_months_keeps_the_fraction(spark):
    """Informatica returns 2.5 months, not 2. Every unit branch used to end
    in .cast('int')."""
    from datetime import datetime

    out = run_expr(
        spark, "DATE_DIFF(D1, D2, 'MM')",
        [(datetime(2026, 3, 16), datetime(2026, 1, 1))],
        "D1 timestamp, D2 timestamp",
    )
    assert out[0] != int(out[0]), f"fraction lost: {out[0]}"
    assert 2.4 < out[0] < 2.6, out[0]


def test_to_integer_truncates_only_when_the_flag_says_so(spark):
    """Informatica's flag means TRUNCATE, and the default is to ROUND.

    Written the other way round first, from a misreading: the flag was
    taken to mean "round". Executing it against the documented values is
    what settled the direction -- TO_INTEGER('2.6') is 3 and
    TO_INTEGER('2.6', TRUE) is 2.
    """
    rows, schema = [("2.6",)], "S string"
    assert run_expr(spark, "TO_INTEGER(S)", rows, schema) == [3]
    assert run_expr(spark, "TO_INTEGER(S, TRUE)", rows, schema) == [2]


def test_to_decimal_keeps_the_requested_scale(spark):
    """TO_DECIMAL(x, 2) must not truncate to an integer."""
    from decimal import Decimal
    out = run_expr(spark, "TO_DECIMAL(S, 2)", [("123.456",)], "S string")
    assert out == [Decimal("123.46")], out


# ── Arguments that were silently dropped ───────────────────────────────

def test_instr_start_position_is_honoured(spark):
    """INSTR(s, sub, start) must not search from position 1."""
    out = run_expr(spark, "INSTR(S, 'a', 3)", [("abcabc",)], "S string")
    assert out == [4], out


def test_instr_finds_the_nth_occurrence(spark):
    out = run_expr(spark, "INSTR(S, '.', 1, 2)",
                   [("a.b.c.d",), ("a.b",), (None,)], "S string")
    assert out == [4, 0, None], out


def test_instr_negative_start_searches_backward_from_the_end(spark):
    rows, schema = [("abcabca",), ("xyz",)], "S string"
    assert run_expr(spark, "INSTR(S, 'a', -1)", rows, schema) == [7, 0]
    assert run_expr(spark, "INSTR(S, 'a', -1, 2)", rows, schema) == [4, 0]
    assert run_expr(spark, "INSTR(S, 'a', -2)", rows, schema) == [4, 0]


def test_instr_start_zero_is_treated_as_one(spark):
    assert run_expr(spark, "INSTR(S, 'a', 0)", [("bca",)], "S string") == [3]


def test_is_date_uses_the_format_it_was_given(spark):
    """A value valid only under the stated format must validate, and one
    valid under a different format must not."""
    rows, schema = [("12/25/2026",), ("2026-12-25",)], "S string"
    assert run_expr(spark, "IS_DATE(S, 'MM/DD/YYYY')", rows, schema) == [True, False]


# ── Permissive evaluation, matching Informatica ────────────────────────

def test_divide_by_zero_yields_null_rather_than_failing(spark):
    """Informatica returns NULL. The generated notebook pins ANSI off so
    Spark does too -- this asserts the pin actually achieves that."""
    out = run_expr(spark, "A / B", [(10.0, 0.0), (10.0, 2.0)], "A double, B double")
    assert out == [None, 5.0]


def test_a_failed_cast_yields_null_rather_than_failing(spark):
    out = run_expr(spark, "TO_INTEGER(S)", [("not a number",)], "S string")
    assert out == [None]


# ── The eleven that convert silently and were never executed ──────────

def test_date_compare_distinguishes_times_on_the_same_day(spark):
    """Informatica compares full timestamps. A translation built on
    datediff would compare dates only and call these equal."""
    from datetime import datetime
    rows = [(datetime(2026, 1, 1, 9, 0), datetime(2026, 1, 1, 17, 0)),
            (datetime(2026, 1, 1, 17, 0), datetime(2026, 1, 1, 9, 0)),
            (datetime(2026, 1, 1, 9, 0), datetime(2026, 1, 1, 9, 0))]
    out = run_expr(spark, "DATE_COMPARE(D1, D2)", rows, "D1 timestamp, D2 timestamp")
    assert out == [-1, 1, 0], out


def test_initcap_matches_informatica_on_punctuated_names(spark):
    """Informatica capitalises after ANY non-alphanumeric character.

    Spark's initcap splits on whitespace only, so it is not used: the
    converter builds Informatica's rule from Spark 3.5 array functions.
    This used to be a flagged divergence -- and the flag made the
    Expression converter replace the whole port with NULL, so every name
    in a mapping that INITCAPped them was lost (found by the golden
    execution harness). Includes Informatica's documented quirk: the
    apostrophe is a delimiter, so "mcdonald's" becomes "Mcdonald'S".
    """
    from infa2aidp.converters.transformation_converter import TransformationConverter

    out = run_expr(spark, "INITCAP(S)",
                   [("o'brien-smith",), ("mary jane",), ("ANN LEE",), ("x1y2 z",),
                    ("mcdonald's",), ("",), (None,)], "S string")
    assert out == ["O'Brien-Smith", "Mary Jane", "Ann Lee", "X1y2 Z", "Mcdonald'S", "", None], out

    code, review = TransformationConverter()._convert_or_flag("INITCAP(NAME)")
    assert review is None and "F.initcap" not in code, (code, review)


def test_is_number_accepts_surrounding_spaces(spark):
    """Informatica IS_NUMBER tolerates leading and trailing spaces."""
    rows = [(" 42 ",), ("42",), ("abc",), (None,)]
    out = run_expr(spark, "IS_NUMBER(S)", rows, "S string")
    assert out == [True, True, False, None], out


def test_is_spaces_returns_null_for_null(spark):
    """Informatica returns NULL for NULL input, not FALSE. Collapsing that
    to FALSE shifts every three-valued comparison downstream."""
    out = run_expr(spark, "IS_SPACES(S)", [("   ",), ("x",), (None,)], "S string")
    assert out == [True, False, None], out


def test_md5_returns_a_32_char_hex_digest(spark):
    out = run_expr(spark, "MD5(S)", [("hello",)], "S string")
    assert out == ["5d41402abc4b2a76b9719d911017c592"], out


def test_mod_returns_null_on_a_zero_denominator(spark):
    """Informatica returns NULL; Spark throws under ANSI, which the
    generated notebook pins off."""
    out = run_expr(spark, "MOD(A, B)", [(10, 3), (10, 0), (-10, 3)], "A int, B int")
    assert out == [1, None, -1], out


def test_percentile_converts_0_to_100_into_a_fraction(spark):
    """Informatica takes 0-100; Spark takes 0-1. PERCENTILE(x, 90) asking
    for the 90th must not become the 90th *fraction*."""
    rows = [(float(i),) for i in range(1, 101)]
    out = run_expr(spark, "PERCENTILE(A, 90)", rows, "A double")
    assert 89.0 <= out[0] <= 91.0, out


def test_reg_match_is_a_full_match_not_a_search(spark):
    """Informatica REG_MATCH matches the WHOLE value; an unanchored rlike
    would report a partial match as true."""
    out = run_expr(spark, "REG_MATCH(S, '[0-9]+')",
                   [("12345",), ("abc123",), ("abc",)], "S string")
    assert out == [True, False, False], out


def test_soundex_encodes_a_name(spark):
    out = run_expr(spark, "SOUNDEX(S)", [("Robert",), ("Rupert",)], "S string")
    assert out[0] == out[1], f"Robert/Rupert should share a code: {out}"


def test_to_decimal_rounds_to_the_requested_scale(spark):
    from decimal import Decimal
    out = run_expr(spark, "TO_DECIMAL(S, 2)", [("1.005",), ("2.344",)], "S string")
    assert out == [Decimal("1.01"), Decimal("2.34")], out


def test_to_float_yields_null_on_invalid_input(spark):
    """Informatica returns NULL; Spark throws under ANSI."""
    out = run_expr(spark, "TO_FLOAT(S)", [("1.5",), ("not a number",)], "S string")
    assert out == [1.5, None], out


# ── Pinned by a string assertion, never executed until now ────────────

def test_add_to_date_adds_the_requested_unit(spark):
    """One Informatica function covers every unit; Spark splits it across
    add_months, date_add and interval arithmetic. Executing it checks the
    emitted interval form actually evaluates."""
    from datetime import datetime
    base = [(datetime(2026, 1, 31, 9, 0),)]
    month = run_expr(spark, "ADD_TO_DATE(D, 'MM', 1)", base, "D timestamp")
    assert month[0].month == 2, month
    day = run_expr(spark, "ADD_TO_DATE(D, 'DD', 1)", base, "D timestamp")
    assert day[0].day == 1 and day[0].month == 2, day
    hour = run_expr(spark, "ADD_TO_DATE(D, 'HH', 5)", base, "D timestamp")
    assert hour[0].hour == 14, hour
    quarter = run_expr(spark, "ADD_TO_DATE(D, 'Q', 1)", base, "D timestamp")
    assert quarter[0].month == 4, quarter


def test_add_to_date_accepts_a_column_amount(spark):
    """The amount may be a port rather than a literal, and only a literal
    needs lifting into a Column -- wrapping an existing Column again is an
    error of its own."""
    from datetime import datetime
    out = run_expr(spark, "ADD_TO_DATE(D, 'DD', N)",
                   [(datetime(2026, 1, 1), 5)], "D timestamp, N int")
    assert out[0].day == 6, out


def test_to_date_parses_with_the_translated_format(spark):
    out = run_expr(spark, "TO_DATE(S, 'MM/DD/YYYY')",
                   [("12/25/2026",), ("not a date",)], "S string")
    assert out[0].year == 2026 and out[0].month == 12 and out[0].day == 25, out
    assert out[1] is None, "an unparseable value must be NULL, not an error"


def test_to_bigint_rounds_by_default_and_holds_large_values(spark):
    """TO_BIGINT is 64-bit, so a value past 2^31 must survive."""
    out = run_expr(spark, "TO_BIGINT(S)", [("2.6",), ("3000000000",)], "S string")
    assert out == [3, 3000000000], out


def test_substr_is_one_based(spark):
    """Informatica SUBSTR counts from 1, as does Spark's substring."""
    out = run_expr(spark, "SUBSTR(S, 2, 3)", [("abcdef",)], "S string")
    assert out == ["bcd"], out


def test_length_counts_characters_including_multibyte(spark):
    out = run_expr(spark, "LENGTH(S)", [("abc",), ("héllo",), (None,)], "S string")
    assert out == [3, 5, None], out


def test_iif_evaluates_both_branches_correctly(spark):
    out = run_expr(spark, "IIF(A > 1, 'y', 'n')", [(0,), (2,)], "A int")
    assert out == ["n", "y"], out


def test_iif_omitted_value2_follows_the_type_of_value1(spark):
    rows, schema = [(0,), (2,)], "A int"
    assert run_expr(spark, "IIF(A > 1, 'y')", rows, schema) == ["", "y"]
    assert run_expr(spark, "IIF(A > 1, 7)", rows, schema) == [0, 7]


def test_round_date_follows_the_informatica_cutovers(spark):
    """MM rounds up from day 16, DD from 12:00, HH from :30, MI from :30s."""
    from datetime import datetime as dt
    rows = [(dt(2026, 3, 15, 23, 0),), (dt(2026, 3, 16, 0, 0),)]
    schema = "D timestamp"
    assert run_expr(spark, "ROUND(D, 'MM')", rows, schema) == [
        dt(2026, 3, 1), dt(2026, 4, 1)]
    assert run_expr(spark, "ROUND(D, 'DD')", rows, schema) == [
        dt(2026, 3, 16), dt(2026, 3, 16)]
    hm = [(dt(2026, 3, 15, 10, 29, 59),), (dt(2026, 3, 15, 10, 30, 30),)]
    assert run_expr(spark, "ROUND(D, 'HH')", hm, schema) == [
        dt(2026, 3, 15, 10), dt(2026, 3, 15, 11)]
    assert run_expr(spark, "ROUND(D, 'MI')", hm, schema) == [
        dt(2026, 3, 15, 10, 30), dt(2026, 3, 15, 10, 31)]


def test_in_matches_any_of_the_listed_values(spark):
    out = run_expr(spark, "IN(S, 'a', 'b')",
                   [("a",), ("b",), ("c",), (None,)], "S string")
    assert out == [True, True, False, None], out


def test_replacestr_treats_the_search_as_a_literal_not_a_regex(spark):
    """Informatica REPLACESTR replaces a literal substring. If the search
    string reached regexp_replace unescaped, '.' would match every
    character and blank the whole value."""
    out = run_expr(spark, "REPLACESTR(0, S, '.', '-')",
                   [("a.b.c",), ("abc",)], "S string")
    assert out == ["a-b-c", "abc"], out


def test_sessstarttime_is_constant_and_in_scope(spark):
    """Informatica's SESSSTARTTIME is fixed for the whole session, so the
    generator emits a name its setup cell defines rather than
    current_timestamp(). This checks the emitted code evaluates in the
    namespace a notebook actually provides."""
    out = run_expr(spark, "SESSSTARTTIME", [(1,), (2,)], "A int")
    assert out[0] == out[1], f"not constant across rows: {out}"


def test_sysdate_evaluates(spark):
    out = run_expr(spark, "SYSDATE", [(1,)], "A int")
    assert out[0] is not None


# ── Aggregates: need a grouping context, not select ───────────────────

def test_median_is_the_exact_middle_value(spark):
    """percentile (exact) rather than percentile_approx, which returns a
    different number and would not reconcile against Informatica."""
    rows = [(float(i),) for i in [1, 2, 3, 4, 100]]
    out = run_agg(spark, "MEDIAN(A)", rows, "A double")
    assert out == [3.0], out


def test_first_and_last_evaluate_as_aggregates(spark):
    """Both are nondeterministic without an ordering -- flagged at the
    Aggregator, see test_aggregator_order_semantics.py. This only checks
    the emitted form runs."""
    rows = [(1.0,), (2.0,), (3.0,)]
    assert run_agg(spark, "FIRST(A)", rows, "A double")[0] in (1.0, 2.0, 3.0)
    assert run_agg(spark, "LAST(A)", rows, "A double")[0] in (1.0, 2.0, 3.0)


# ── Refused rather than approximated ─────────────────────────────────

@pytest.mark.parametrize("expr", ["CUME(A)", "MOVINGAVG(A, 3)", "MOVINGSUM(A, 3)"])
def test_running_and_moving_aggregates_are_refused(expr):
    """Each needs an explicit ordering the export does not carry, so there
    is no faithful translation. Refusing produces a REVIEW REQUIRED
    marker; approximating would produce a plausible wrong number."""
    from infa2aidp.converters.expression_converter import (
        ExpressionConverter,
        UnconvertibleExpression,
    )
    with pytest.raises(UnconvertibleExpression):
        ExpressionConverter().convert(expr)


# ── Previously refused, now mapped (item 2) ──────────────────────────

def test_greatest_propagates_null_where_spark_would_ignore_it(spark):
    """Informatica returns NULL if ANY argument is NULL. Spark's greatest
    IGNORES nulls and returns the largest of the rest -- a value where
    Informatica returns nothing, which is a wrong answer rather than an
    error."""
    out = run_expr(spark, "GREATEST(A, B)",
                   [(1, 2), (None, 2), (3, None)], "A int, B int")
    assert out == [2, None, None], out


def test_least_propagates_null_too(spark):
    out = run_expr(spark, "LEAST(A, B)",
                   [(1, 2), (None, 2)], "A int, B int")
    assert out == [1, None], out


def test_sha256_returns_a_64_char_hex_digest(spark):
    out = run_expr(spark, "SHA256(S)", [("hello",)], "S string")
    assert out == [
        "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824"
    ], out


def test_reg_replace_replaces_every_match(spark):
    out = run_expr(spark, "REG_REPLACE(S, 'a', 'X')", [("abca",)], "S string")
    assert out == ["XbcX"], out


def test_reg_extract_returns_the_requested_group(spark):
    """The pattern must reach regexp_extract as a plain string.

    Emitted as F.lit(...) it raised "Column is not iterable" at run time --
    the same trap as locate(). regexp_replace accepts either, which is why
    only this one needed unwrapping.
    """
    out = run_expr(spark, "REG_EXTRACT(S, '([0-9]+)', 1)",
                   [("abc123",), ("abc",)], "S string")
    assert out == ["123", ""], out


def test_reg_extract_refuses_a_non_literal_pattern():
    """Spark takes the pattern as a plain string, so it cannot come from a
    column. Refusing beats emitting something that raises."""
    from infa2aidp.converters.expression_converter import (
        ExpressionConverter,
        UnconvertibleExpression,
    )
    with pytest.raises(UnconvertibleExpression, match="not a literal"):
        ExpressionConverter().convert("REG_EXTRACT(S, P, 1)")


def test_indexof_is_one_based_and_zero_when_absent(spark):
    """Informatica INDEXOF returns a 1-based position, or 0 if not found.
    array_position has exactly those two properties."""
    out = run_expr(spark, "INDEXOF(S, 'a', 'b', 'c')",
                   [("a",), ("b",), ("z",)], "S string")
    assert out == [1, 2, 0], out


def test_systimestamp_and_uuid_and_rand_evaluate(spark):
    assert run_expr(spark, "SYSTIMESTAMP()", [(1,)], "A int")[0] is not None
    uid = run_expr(spark, "UUID_STRING()", [(1,)], "A int")[0]
    assert len(uid) == 36 and uid.count("-") == 4, uid
    assert 0.0 <= run_expr(spark, "RAND(42)", [(1,)], "A int")[0] <= 1.0


def test_rand_refuses_a_non_literal_seed():
    from infa2aidp.converters.expression_converter import (
        ExpressionConverter,
        UnconvertibleExpression,
    )
    with pytest.raises(UnconvertibleExpression, match="not a literal"):
        ExpressionConverter().convert("RAND(N)")


# ── Still refused, because the semantics genuinely differ ────────────

@pytest.mark.parametrize("expr,why", [
    ("ABORT('bad')", "stops the session"),
    ("ERROR('bad')", "skips the row"),
])
def test_abort_and_error_are_refused_with_the_reason(expr, why):
    """F.raise_error exists, but it fails the Spark TASK -- which the
    cluster retries and then fails the whole job. Informatica's ERROR skips
    one row into the reject file and carries on; ABORT rolls back the
    session. Emitting raise_error would turn a row-level reject into an
    aborted run."""
    from infa2aidp.converters.expression_converter import (
        ExpressionConverter,
        UnconvertibleExpression,
    )
    with pytest.raises(UnconvertibleExpression) as exc:
        ExpressionConverter().convert(expr)
    assert why in str(exc.value)


@pytest.mark.parametrize("fn", ["AES_ENCRYPT", "AES_DECRYPT",
                                "AES_GCM_ENCRYPT", "AES_GCM_DECRYPT"])
def test_aes_functions_are_refused_rather_than_mapped(fn):
    """Spark has aes_encrypt/aes_decrypt, so this could be mapped -- and
    must not be. Key derivation, mode and padding differ, so ciphertext is
    not interchangeable: decrypting data Informatica wrote would produce
    garbage or fail, and both look like a working pipeline until someone
    reads the output."""
    from infa2aidp.converters.expression_converter import (
        ExpressionConverter,
        UnconvertibleExpression,
    )
    with pytest.raises(UnconvertibleExpression, match="not interchangeable"):
        ExpressionConverter().convert(f"{fn}(S, 'key')")


# ── The ANSI pins become load-bearing on Spark 4 ─────────────────────

def test_the_generated_notebook_pins_ansi_off(spark):
    """Spark 4 defaults spark.sql.ansi.enabled to TRUE.

    Verified on Spark 4.2: with ANSI on, 10/0 raises and casting 'x' to int
    raises. Informatica yields NULL for both and logs a row error. So every
    migrated mapping that relied on permissive evaluation would abort on a
    Spark 4 cluster rather than produce the rows it used to.

    The setup cell pins the flag rather than inheriting it, which is what
    makes AIDP's move from Spark 3.5 to 4 a non-event for generated code.
    This test asserts the pin exists; the two below assert it is necessary.
    """
    from infa2aidp.generators.notebook_generator import NotebookGenerator

    setup = NotebookGenerator._setup_cell()
    assert 'spark.conf.set("spark.sql.ansi.enabled", "false")' in setup
    assert 'spark.conf.set("spark.sql.storeAssignmentPolicy", "LEGACY")' in setup


def test_divide_by_zero_raises_when_ansi_is_on(spark):
    """Proof the pin is load-bearing, not decoration."""
    df = spark.createDataFrame([(10.0, 0.0)], "a double, b double")
    spark.conf.set("spark.sql.ansi.enabled", "true")
    try:
        with pytest.raises(Exception):
            df.select((F_col("a") / F_col("b")).alias("o")).collect()
    finally:
        spark.conf.set("spark.sql.ansi.enabled", "false")


def test_a_failed_cast_raises_when_ansi_is_on(spark):
    df = spark.createDataFrame([("x",)], "c string")
    spark.conf.set("spark.sql.ansi.enabled", "true")
    try:
        with pytest.raises(Exception):
            df.select(F_col("c").cast("int").alias("o")).collect()
    finally:
        spark.conf.set("spark.sql.ansi.enabled", "false")
