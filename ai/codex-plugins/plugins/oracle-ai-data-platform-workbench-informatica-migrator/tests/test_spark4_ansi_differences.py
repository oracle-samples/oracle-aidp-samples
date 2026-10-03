"""The Spark 3.5 vs Spark 4 inventory, as a test instead of a one-off script.

``references/spark-4-behaviour-differences.md`` lists 14 expressions that
return a value under the settings Spark 3.5 ships and raise under the
settings Spark 4 ships. That inventory was produced by a script run once, so
the document was accurate on the day it was written and silently decayed
after -- an expression the converter started emitting differently, or a new
ANSI-sensitive form, would not show up anywhere.

This file is that probe, run every time the suite runs.

Method: evaluate each emitted expression twice on the same rows -- once with
``spark.sql.ansi.enabled=false`` (3.5's default, which generated notebooks
pin) and once with it ``true`` (Spark 4's default) -- and compare. The edge
values are deliberate: a zero divisor, a non-numeric string, a value that
overflows an INT, an invalid date, and NULLs.

**A new difference appearing is a finding, not a failure to paper over.**
Add it here and to the reference document, and decide whether the generated
notebook still covers it. One disappearing means the converter changed and
the document is stale.
"""
from __future__ import annotations

import pytest

from tests.conftest_spark import run_expr, spark  # noqa: F401

# Expressions that differ between the two settings, with the input that
# provokes it. Each entry: (informatica_expr, rows, schema).
#
# Kept in the same four groups as the reference document so the two can be
# read side by side.
ANSI_SENSITIVE: dict[str, tuple[str, list[tuple], str]] = {
    # Division and modulo by zero
    "A / B": ("A / B", [(10, 2), (10, 0)], "A int, B int"),
    "ROUND(A / B, 2)": ("ROUND(A / B, 2)", [(10, 2), (10, 0)], "A int, B int"),
    "ABS(A / B)": ("ABS(A / B)", [(10, 2), (10, 0)], "A int, B int"),
    "MOD(A, B)": ("MOD(A, B)", [(10, 2), (10, 0)], "A int, B int"),
    # Malformed value cast to a number
    "TO_FLOAT(S)": ("TO_FLOAT(S)", [("1.5",), ("abc",)], "S string"),
    "TO_DECIMAL(S)": ("TO_DECIMAL(S)", [("1.5",), ("abc",)], "S string"),
    "TO_DECIMAL(S, 2)": ("TO_DECIMAL(S, 2)", [("1.5",), ("abc",)], "S string"),
    "IS_NUMBER(S)": ("IS_NUMBER(S)", [("1.5",), ("abc",)], "S string"),
    # Numeric overflow on cast
    "TO_INTEGER(S)": ("TO_INTEGER(S)", [("1",), ("99999999999999999999",)], "S string"),
    "TO_INTEGER(S, TRUE)": ("TO_INTEGER(S, TRUE)", [("1",), ("99999999999999999999",)], "S string"),
    "TO_BIGINT(S)": ("TO_BIGINT(S)", [("1",), ("99999999999999999999",)], "S string"),
    # Date parsing
    "TO_DATE(S, 'MM/DD/YYYY')": (
        "TO_DATE(S, 'MM/DD/YYYY')", [("01/15/2024",), ("13/45/2024",)], "S string"),
    "IS_DATE(S, 'MM/DD/YYYY')": (
        "IS_DATE(S, 'MM/DD/YYYY')", [("01/15/2024",), ("13/45/2024",)], "S string"),
}

# TO_CHAR on a malformed number is the fourteenth. Kept separate because it
# reaches ANSI through a cast nested in a string conversion rather than
# directly, which is the form a reader is most likely to think is safe.
ANSI_SENSITIVE["TO_CHAR(TO_FLOAT(S))"] = (
    "TO_CHAR(TO_FLOAT(S))", [("1.5",), ("abc",)], "S string")

# Expressions whose result is a NEW value on every call, so "differs between
# two runs" says nothing about ANSI. Excluded from the count by nature, not
# by convenience -- the reference document excludes the same three.
NONDETERMINISTIC = ("SYSDATE", "SYSTIMESTAMP", "UUID_STRING")

# Expressions that must behave IDENTICALLY under both settings. A sample
# across the families the converter emits -- if ANSI starts affecting one of
# these, the pins are load-bearing somewhere new and the inventory is wrong.
ANSI_NEUTRAL: dict[str, tuple[str, list[tuple], str]] = {
    "CONCAT(A, B)": ("CONCAT(A, B)", [("John", None), ("Jane", "Doe")], "A string, B string"),
    "UPPER(S)": ("UPPER(S)", [("abc",), (None,)], "S string"),
    "LENGTH(S)": ("LENGTH(S)", [("abc",), (None,)], "S string"),
    "SUBSTR(S, 1, 2)": ("SUBSTR(S, 1, 2)", [("abcdef",), (None,)], "S string"),
    "ISNULL(S)": ("ISNULL(S)", [("abc",), (None,)], "S string"),
    "IIF(A > 5, 'big', 'small')": ("IIF(A > 5, 'big', 'small')", [(10,), (1,)], "A int"),
    "NVL(S, 'x')": ("NVL(S, 'x')", [("abc",), (None,)], "S string"),
    "TRIM(S)": ("TRIM(S)", [("  abc  ",), (None,)], "S string"),
    "ABS(A)": ("ABS(A)", [(-5,), (None,)], "A int"),
    "ROUND(A, 1)": ("ROUND(A, 1)", [(1.25,), (None,)], "A double"),
    "DECODE(S, 'A', 'Active', 'other')": (
        "DECODE(S, 'A', 'Active', 'other')", [("A",), (None,)], "S string"),
    "LTRIM(S)": ("LTRIM(S)", [("  abc",), (None,)], "S string"),
    "INITCAP(S)": ("INITCAP(S)", [("john doe",), (None,)], "S string"),
    "REG_MATCH(S, '[0-9]+')": ("REG_MATCH(S, '[0-9]+')", [("123",), ("abc",)], "S string"),
    "MD5(S)": ("MD5(S)", [("abc",), (None,)], "S string"),
}


def _outcome(spark, infa_expr, rows, schema, ansi: bool):
    """Evaluate under one ANSI setting. Returns ('ok', values) or ('raise', type)."""
    spark.conf.set("spark.sql.ansi.enabled", "true" if ansi else "false")
    try:
        return ("ok", run_expr(spark, infa_expr, rows, schema))
    except Exception as e:                                   # noqa: BLE001
        # The error CLASS is what matters, not its message: Spark's text
        # carries config hints and query fragments that change between
        # releases and would make this test fail for no reason.
        return ("raise", type(e).__name__)
    finally:
        spark.conf.set("spark.sql.ansi.enabled", "false")


@pytest.mark.parametrize("label", sorted(ANSI_SENSITIVE))
def test_documented_difference_still_differs(spark, label):
    """Each documented expression must still return on 3.5 and raise on 4.

    This is the direction that matters operationally: on Spark 4 these are
    aborted jobs, not wrong numbers, and the generated notebook's ANSI pin
    is the only thing standing between a migrated mapping and a failed run.
    """
    expr, rows, schema = ANSI_SENSITIVE[label]
    permissive = _outcome(spark, expr, rows, schema, ansi=False)
    strict = _outcome(spark, expr, rows, schema, ansi=True)

    assert permissive[0] == "ok", (
        f"{label} raises even with ANSI off ({permissive[1]}). Generated "
        f"notebooks pin ANSI off, so this would abort a migrated run."
    )
    assert strict[0] == "raise", (
        f"{label} no longer raises under ANSI. Spark may have changed its "
        f"behaviour, or the converter now emits a different form. Re-measure "
        f"and update references/spark-4-behaviour-differences.md -- the "
        f"inventory there claims this one differs."
    )


@pytest.mark.parametrize("label", sorted(ANSI_NEUTRAL))
def test_ansi_neutral_expression_is_unaffected(spark, label):
    """These must agree under both settings, value for value.

    If one starts differing, the ANSI pin has become load-bearing for a
    family the inventory does not cover.
    """
    expr, rows, schema = ANSI_NEUTRAL[label]
    permissive = _outcome(spark, expr, rows, schema, ansi=False)
    strict = _outcome(spark, expr, rows, schema, ansi=True)

    assert permissive == strict, (
        f"{label} differs between ANSI off and ANSI on: "
        f"{permissive} vs {strict}. It was believed ANSI-neutral; it is not. "
        f"Add it to references/spark-4-behaviour-differences.md."
    )


def test_the_documented_count_matches_this_probe():
    """The reference document states a number. Keep it honest."""
    import re
    from pathlib import Path

    doc = (Path(__file__).resolve().parents[1]
           / "references" / "spark-4-behaviour-differences.md").read_text()
    m = re.search(r"leaving \*\*(\d+) real differences\*\*", doc)
    assert m, "reference document no longer states a difference count"
    assert int(m.group(1)) == len(ANSI_SENSITIVE), (
        f"document says {m.group(1)} differences, this probe covers "
        f"{len(ANSI_SENSITIVE)}"
    )


def test_nondeterministic_expressions_are_excluded_deliberately():
    """Guard the exclusion itself.

    These three differ between any two calls, ANSI or not, so counting them
    as ANSI differences would inflate the inventory. Asserting they are
    absent stops someone 'fixing' the count by adding them.
    """
    for name in NONDETERMINISTIC:
        assert not any(name in k for k in ANSI_SENSITIVE), (
            f"{name} is nondeterministic -- it cannot evidence an ANSI "
            f"difference and must not be counted as one"
        )
