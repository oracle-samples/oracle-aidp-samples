"""Informatica functions whose Spark equivalent is not the obvious one.

Every case here is a *silent* mismatch: the obvious translation compiles,
runs, and returns the wrong answer. They are grouped by that property
rather than by function category, because it is the property that makes
them worth a dedicated test file.

The defects fixed alongside these tests were all found by reading a
conversion reference against generated output. None was caught by the
existing suite -- the CONCAT mapping had been wrong since it was written
and 747 tests passed over it, because no test asserted what CONCAT should
emit, only that it emitted something.

Note on verification: these assertions pin the *generated code*, not its
runtime behaviour. pyspark is not a dependency of this repo, so nothing
here executes Spark. Where a claim about Spark semantics is load-bearing
it is stated in the docstring so a reader can challenge it.
"""
from __future__ import annotations

import pytest

from infa2aidp.converters.expression_converter import (
    ExpressionConverter,
    UnconvertibleExpression,
)


@pytest.fixture
def conv():
    return ExpressionConverter()


# ── NULL handling: the largest class of silent mismatch ────────────────

def test_concat_treats_null_as_empty_string(conv):
    """Informatica CONCAT treats NULL as ''; Spark's concat returns NULL
    if ANY argument is NULL.

    A mapping building FULL_NAME from FIRST/MIDDLE/LAST would return NULL
    for every row without a middle name. concat_ws with an empty separator
    skips NULLs, which matches Informatica.
    """
    out = conv.convert("CONCAT(FIRST_NAME, LAST_NAME)")
    assert "concat_ws('', " in out
    assert "F.concat(" not in out, "F.concat propagates NULL -- wrong for Informatica"


def test_decode_matches_null_to_null(conv):
    """DECODE(x, NULL, 'missing') fires when x IS NULL.

    Emitting `x == F.lit(None)` produces a branch that can never be taken,
    because SQL's `x = NULL` is never true -- so the row silently falls
    through to the default.
    """
    out = conv.convert("DECODE(STATUS, NULL, 'MISSING', 'A', 'Active')")
    assert ".isNull()" in out
    assert "== F.lit(None)" not in out, "a NULL branch compared with == can never fire"


def test_decode_non_null_branches_still_use_equality(conv):
    out = conv.convert("DECODE(STATUS, 'A', 'Active', 'B', 'Blocked')")
    assert out.count("==") == 2
    assert ".isNull()" not in out


# ── Arguments silently dropped ─────────────────────────────────────────

def test_instr_two_arg_reverses_the_argument_order(conv):
    """Informatica INSTR(string, search); Spark locate(search, string)."""
    out = conv.convert("INSTR(NAME, 'a')")
    assert out == "F.locate('a', F.col('NAME'))"


def test_instr_start_position_is_carried_through(conv):
    """Spark's locate takes a 1-based pos, same as Informatica's start."""
    out = conv.convert("INSTR(NAME, 'a', 3)")
    assert out == "F.locate('a', F.col('NAME'), 3)"


def test_instr_nth_occurrence_is_converted_not_collapsed_to_the_first(conv):
    """The defect this replaces: INSTR(s, x, 1, 2) asked for the SECOND
    occurrence and silently got the position of the first. The runtime
    result is pinned in test_expression_execution.py."""
    out = conv.convert("INSTR(NAME, 'a', 1, 2)")
    assert not out.startswith("F.locate(")
    assert "F.split(" in out


def test_instr_occurrence_of_one_is_the_default_and_converts(conv):
    out = conv.convert("INSTR(NAME, 'a', 1, 1)")
    assert out == "F.locate('a', F.col('NAME'), 1)"


def test_instr_case_insensitive_flag_refuses(conv):
    with pytest.raises(UnconvertibleExpression, match="comparison_type"):
        conv.convert("INSTR(NAME, 'a', 1, 1, 0)")


# ── Already correct: pinned so they cannot regress ─────────────────────

def test_to_date_translates_the_format_language(conv):
    """Informatica and Spark format strings are different languages.
    MM/DD/YYYY HH24:MI:SS -> MM/dd/yyyy HH:mm:ss. DD and YYYY and MI are
    each wrong if passed through verbatim.
    """
    out = conv.convert("TO_DATE(DT, 'MM/DD/YYYY HH24:MI:SS')")
    assert "'MM/dd/yyyy HH:mm:ss'" in out


def test_iif_without_an_else_defaults_by_the_type_of_value1(conv):
    """Informatica's omitted value2 is 0 for a numeric value1, an empty
    string for a string value1, and NULL otherwise."""
    assert conv.convert("IIF(AMT > 10, 'BIG')").endswith(".otherwise(F.lit(''))")
    assert conv.convert("IIF(AMT > 10, 5)").endswith(".otherwise(F.lit(0))")
    assert conv.convert("IIF(AMT > 10, OTHER)").endswith(".otherwise(F.lit(None))")


def test_ltrim_and_rtrim_trim_a_character_set(conv):
    """Informatica LTRIM(string, trim_set) treats the second argument as a
    SET OF CHARACTERS, not a substring. Spark's F.ltrim/F.rtrim take ONE
    argument, so the two-argument form used to reach the notebook as
    F.rtrim(col, F.lit('xy')) and fail at run time with a TypeError. A
    literal trim set is a regex character class anchored at the right end.
    """
    assert conv.convert("LTRIM(NAME, 'xy')") == "F.regexp_replace(F.col('NAME'), '^[xy]+', '')"
    assert conv.convert("RTRIM(NAME, 'xy')") == "F.regexp_replace(F.col('NAME'), '[xy]+$', '')"
    # regex metacharacters in the set are escaped
    assert conv.convert("RTRIM(NAME, ']')") == "F.regexp_replace(F.col('NAME'), '[\\\\]]+$', '')"
    # the one-argument form is still the plain Spark call
    assert conv.convert("RTRIM(NAME)") == "F.rtrim(F.col('NAME'))"
    with pytest.raises(UnconvertibleExpression):
        conv.convert("RTRIM(NAME, OTHER_COL)")


def test_replacechr_uses_character_set_semantics_not_substring(conv):
    """REPLACECHR(0, NAME, 'aeiou', '*') replaces each vowel independently.
    regexp_replace would look for the literal substring 'aeiou'.
    """
    out = conv.convert("REPLACECHR(0, NAME, 'aeiou', '*')")
    assert "translate" in out
    assert "regexp_replace" not in out


def test_add_to_date_does_not_treat_every_unit_as_days(conv):
    """'W', 'Q' and 'J' each silently became date_add(..., N) once."""
    week = conv.convert("ADD_TO_DATE(DT, 'W', 2)")
    assert "date_add" not in week or "7" in week, week


# ── Pass 3: results that are the right shape and the wrong value ───────

def test_date_diff_returns_a_fractional_difference(conv):
    """Informatica DATE_DIFF returns 2.5 months, not 2.

    Every unit branch used to end in .cast('int'). A mapping computing
    tenure in months or an age in years silently lost the remainder, and
    every downstream average was wrong by up to a whole unit.
    """
    assert ".cast('int')" not in conv.convert("DATE_DIFF(D1,D2,'MM')")
    assert ".cast('int')" not in conv.convert("DATE_DIFF(D1,D2,'YYYY')")
    assert ".cast('int')" not in conv.convert("DATE_DIFF(D1,D2,'DD')")


def test_date_diff_days_is_not_whole_days(conv):
    """datediff() is whole days by construction, so the fractional part
    needs the timestamp difference instead."""
    out = conv.convert("DATE_DIFF(D1,D2,'DD')")
    assert "unix_timestamp" in out and "86400" in out
    assert "F.datediff(" not in out


def test_date_diff_still_distinguishes_its_units(conv):
    """The fractional fix must not undo the unit-resolution fix."""
    assert "months_between" in conv.convert("DATE_DIFF(D1,D2,'MM')")
    assert "/ 12" in conv.convert("DATE_DIFF(D1,D2,'YYYY')")
    assert "/ 3" in conv.convert("DATE_DIFF(D1,D2,'Q')")
    assert "604800" in conv.convert("DATE_DIFF(D1,D2,'W')")


def test_is_date_honours_its_format_argument(conv):
    """IS_DATE(value, format) validates against THAT format.

    Ignoring the second argument validated against Spark's default parser:
    a string valid only under the stated format was reported invalid, and
    one valid under the default but not the stated format was reported
    valid. Both silent.
    """
    out = conv.convert("IS_DATE(S, 'MM/DD/YYYY')")
    assert "'MM/dd/yyyy'" in out, out
    assert "to_timestamp" in out


def test_to_integer_is_32_bit_and_to_bigint_is_64(conv):
    """Emitting 'long' for both let a value past 2^31 that Informatica
    would reject flow through silently."""
    assert conv.convert("TO_INTEGER(S)").endswith(".cast('int')")
    assert conv.convert("TO_BIGINT(S)").endswith(".cast('long')")
    assert "'long'" not in conv.convert("TO_INTEGER(S)")


def test_to_integer_rounds_by_default_and_truncates_on_true(conv):
    """Transformation Language Reference, TO_INTEGER(value [, flag]):
    "Truncates the decimal portion when TRUE or a number other than 0.
    Rounds to the nearest integer if flag is FALSE or 0 or is omitted."
    So TO_INTEGER('2.6') is 3 and TO_INTEGER('2.6', TRUE) is 2. A bare
    cast (the original emission) always truncated -- wrong for the
    omitted-flag form, which is how the function is almost always written.
    """
    assert conv.convert("TO_INTEGER(S)") == "F.round(F.col('S').cast('double')).cast('int')"
    assert conv.convert("TO_INTEGER(S, FALSE)") == "F.round(F.col('S').cast('double')).cast('int')"
    assert conv.convert("TO_INTEGER(S, 0)") == "F.round(F.col('S').cast('double')).cast('int')"
    assert conv.convert("TO_INTEGER(S, TRUE)") == "F.col('S').cast('double').cast('int')"
    assert conv.convert("TO_INTEGER(S, 1)") == "F.col('S').cast('double').cast('int')"
    assert conv.convert("TO_BIGINT(S, TRUE)") == "F.col('S').cast('double').cast('long')"


def test_median_is_exact_not_approximate(conv):
    """percentile_approx and percentile return different numbers, and a
    reconciliation against Informatica compares exact values."""
    out = conv.convert("MEDIAN(AMT)")
    assert "F.percentile(" in out
    assert "percentile_approx" not in out


def test_a_non_literal_boolean_flag_is_refused_not_guessed(conv):
    """A flag whose value is only known at runtime must not pick a branch:
    round-vs-truncate changes every row's value, so the expression is a
    review item rather than a silently chosen behaviour."""
    with pytest.raises(UnconvertibleExpression):
        conv.convert("TO_INTEGER(S, SOME_PORT)")
