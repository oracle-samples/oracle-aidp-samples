"""Every PySpark function the converter emits must exist on AIDP's Spark.

AIDP compute clusters run **Spark 3.5.0** (verified against live clusters via
the AIDP API, 2026-09-23). The executable expression tests, however, run
whatever pyspark is installed locally -- currently 4.x, because Spark 3.5
cannot start on Java 25: it calls ``Subject.getSubject``, which newer JDKs
removed. Spark 3.5 supports Java 8/11/17.

That leaves a gap: a function added in Spark 4.0 would pass the local
execution tests and fail on AIDP. This file closes it without needing 3.5
installed, by reading each function's own ``versionadded`` annotation.

It is an API-surface check, not a behaviour check. Two functions the
converter emits -- ``make_interval`` and ``percentile`` -- land exactly on
3.5.0, so the margin is one release. Behavioural differences between 3.5
and 4.x are a separate risk, and the reason the generated notebook pins
``spark.sql.ansi.enabled`` rather than inheriting it.

``versionadded`` also cannot see a THIRD category: a function that exists
in 3.5 whose Python signature *widened* in 4.x to accept a Column where 3.5
requires a plain value. ``array_position`` and ``substring`` both did, and
both produced ``PySparkTypeError: Column is not iterable`` on the cluster
while passing here on 4.x. The guard at the bottom of this file reports an
emitted call that passes a Column where 3.5 needs a literal. It is a shape
assertion: it cannot prove 3.5 compatibility and is no substitute for
running on 3.5 -- it stops a regression back to a known-bad form from
passing silently on a 4.x laptop. Two tests prove it fires on the bad forms
and stays quiet on the good ones.
"""
from __future__ import annotations

import inspect
import re

import pytest

pytest.importorskip("pyspark", reason="needs pyspark to read versionadded metadata")

import pyspark.sql.functions as F  # noqa: E402

from infa2aidp.converters.expression_converter import (  # noqa: E402
    ExpressionConverter,
    UnconvertibleExpression,
)

# The OLDEST Spark an AIDP cluster may be running, not the newest.
#
# Verified 3.5.0 on live clusters; Spark 4 support arrives shortly. During
# the overlap a generated notebook may land on either, so it must be
# emittable for the older one -- a function added in 4.0 would work on the
# new clusters and fail on the old. Raise this floor only when no 3.5
# cluster remains, and expect that to be a deliberate decision rather than
# a consequence of upgrading a laptop.
# Imported, not redeclared: the generated notebook writes this same
# floor into its setup cell and the deployer refuses a cluster below
# it, so three places have to agree.
from infa2aidp.spark_target import TARGET_SPARK  # noqa: E402

# Every distinct Informatica expression the converter is asked about
# elsewhere in this suite, so this check covers what we actually emit.
PROBE_EXPRESSIONS = [
    "CONCAT(A, B)", "DECODE(S, NULL, 'M', 'A', 'x')", "INSTR(S, 'a')",
    "INSTR(S, 'a', 3)", "IIF(A > 1, 'y', 'n')", "IN(S, 'a', 'b')",
    "LENGTH(S)", "LTRIM(S, 'xy')", "RTRIM(S, 'xy')", "SUBSTR(S, 2, 3)",
    "REPLACECHR(0, S, 'ab', 'x')", "REPLACESTR(0, S, 'ab', 'x')",
    "IS_DATE(S, 'MM/DD/YYYY')", "IS_NUMBER(S)", "IS_SPACES(S)",
    "TO_DATE(S, 'MM/DD/YYYY')", "TO_BIGINT(S)", "TO_INTEGER(S, TRUE)",
    "TO_DECIMAL(S)", "TO_DECIMAL(S, 2)", "TO_FLOAT(S)",
    "DATE_DIFF(D1, D2, 'MM')", "DATE_DIFF(D1, D2, 'DD')",
    "DATE_COMPARE(D1, D2)", "ADD_TO_DATE(D, 'MM', 1)",
    "ADD_TO_DATE(D, 'HH', 1)", "ADD_TO_DATE(D, 'Q', 1)",
    "TRUNC(D, 'MM')", "SYSDATE", "SESSSTARTTIME", "SYSTIMESTAMP()",
    "GREATEST(A, B)", "LEAST(A, B)", "SHA256(S)", "MD5(S)", "SOUNDEX(S)",
    "INITCAP(S)", "UUID_STRING()", "RAND(42)", "INDEXOF(S, 'a', 'b')",
    "REG_MATCH(S, 'p')", "REG_REPLACE(S, 'a', 'b')",
    "REG_EXTRACT(S, '(a)', 1)", "MEDIAN(A)", "PERCENTILE(A, 90)",
    "FIRST(A)", "LAST(A)", "MOD(A, B)", "A / B", "NVL(A, B)",
    "UPPER(S)", "LOWER(S)", "ABS(A)", "ROUND(A, 2)",
]

_FUNC_CALL = re.compile(r"\bF\.([a-zA-Z_][a-zA-Z0-9_]*)\s*\(")


def _version_added(name: str) -> tuple[int, ...] | None:
    fn = getattr(F, name, None)
    if fn is None:
        return None
    doc = inspect.getdoc(fn) or ""
    m = re.search(r"versionadded::\s*([0-9]+(?:\.[0-9]+)*)", doc)
    if not m:
        return ()
    return tuple(int(p) for p in m.group(1).split("."))


def _emitted_function_names() -> set[str]:
    conv = ExpressionConverter()
    names: set[str] = set()
    for expr in PROBE_EXPRESSIONS:
        try:
            code = conv.convert(expr)
        except UnconvertibleExpression:
            continue  # a refusal emits no code
        names.update(_FUNC_CALL.findall(code))
    return names


def test_the_probe_list_actually_produces_code():
    """A guard on the guard: if every probe refused, this file would pass
    while checking nothing."""
    names = _emitted_function_names()
    assert len(names) >= 20, f"only {len(names)} functions emitted: {sorted(names)}"


def test_every_emitted_function_exists_in_pyspark():
    missing = sorted(n for n in _emitted_function_names() if getattr(F, n, None) is None)
    assert not missing, f"emitted but not a pyspark.sql.functions member: {missing}"


def test_no_emitted_function_is_newer_than_the_aidp_spark_version():
    """The check this file exists for.

    A function added in Spark 4.0 would run locally and fail on AIDP, and
    nothing else in the suite would notice.
    """
    too_new = {}
    for name in sorted(_emitted_function_names()):
        added = _version_added(name)
        if added and added > TARGET_SPARK:
            too_new[name] = ".".join(str(p) for p in added)
    assert not too_new, (
        f"emitted functions newer than AIDP's Spark "
        f"{'.'.join(str(p) for p in TARGET_SPARK)}: {too_new}"
    )


# ---------------------------------------------------------------------------
# Signature widening: the category versionadded cannot see
# ---------------------------------------------------------------------------

_COLUMN_ARG = re.compile(r"F\.(?:col|lit|when|locate|coalesce|expr)\(")

# Functions whose Python signature ACCEPTS a Column in pyspark 4.x but
# requires a plain Python value in 3.5. The function exists in both, so
# `versionadded` is silent, and on a 4.x laptop the 4-only form passes the
# execution tests and then raises on AIDP:
#
#   F.array_position(col, <Column>)  -> PySparkTypeError: Column is not iterable
#   F.substring(col, <Column>, n)    -> PySparkTypeError: Column is not iterable
#
# Both were found by running on the cluster, not here. This is a
# shape assertion, and it is worth being precise about what that buys: it
# cannot prove 3.5 compatibility, and it is not a substitute for running on
# 3.5. What it does is stop a regression BACK to a known-bad form from
# passing silently on a 4.x local install.
_WIDENED_IN_4 = {
    "array_position": "use a CASE chain, or pass a Python literal",
    "substring": "use Column.substr(start, length) when either bound is computed",
}


def _emitted_snippets() -> list[tuple[str, str]]:
    """(informatica_expression, emitted_pyspark) over the probe list."""
    from infa2aidp.converters.expression_converter import ExpressionConverter

    conv = ExpressionConverter()
    out = []
    for expr in PROBE_EXPRESSIONS:
        try:
            out.append((expr, conv.convert(expr)))
        except Exception:
            continue
    return out


@pytest.mark.parametrize("fn,advice", sorted(_WIDENED_IN_4.items()))
def test_no_emitted_call_passes_a_column_where_spark_35_needs_a_literal(fn, advice):
    offenders = []
    for expr, code in _emitted_snippets():
        for m in re.finditer(rf"F\.{fn}\(", code):
            tail = code[m.end():]
            # crude but sufficient: the argument list of this call, to its
            # matching close paren
            depth, arg = 1, []
            for ch in tail:
                if ch == "(":
                    depth += 1
                elif ch == ")":
                    depth -= 1
                    if depth == 0:
                        break
                arg.append(ch)
            args = "".join(arg)
            # first argument is legitimately a Column; look past it
            after_first = args.split(",", 1)[1] if "," in args else ""
            if _COLUMN_ARG.search(after_first):
                offenders.append((expr, code[:120]))
    assert not offenders, (
        f"F.{fn}() is passed a Column after its first argument. pyspark 4 "
        f"accepts that and Spark 3.5 on AIDP raises "
        f"'Column is not iterable'. {advice}. Offending: {offenders}"
    )


def test_the_widening_guard_actually_fires_on_the_known_bad_form(monkeypatch):
    """Proof the check above is not vacuous.

    Both bad forms are the ones that actually raised on AIDP. If this test
    ever passes while the guard reports nothing, the guard has stopped
    detecting and the parametrised test above is measuring nothing.
    """
    bad = [
        ("INDEXOF(C,'x')",
         "F.array_position(F.col('C'), F.lit('x'))"),
        ("SUBSTR(E, INSTR(E,'@')+1)",
         "F.substring(F.col('E'), F.locate('@', F.col('E')) + 1, 2147483647)"),
    ]
    monkeypatch.setattr(
        "tests.test_spark_api_compatibility._emitted_snippets", lambda: bad
    )
    fired = []
    for fn, advice in sorted(_WIDENED_IN_4.items()):
        try:
            test_no_emitted_call_passes_a_column_where_spark_35_needs_a_literal(fn, advice)
        except AssertionError:
            fired.append(fn)
    assert sorted(fired) == ["array_position", "substring"], (
        f"guard did not fire on the known-bad forms; it detected {fired}"
    )


def test_a_literal_second_argument_is_not_flagged(monkeypatch):
    """The safe forms must not be reported, or the guard is noise."""
    good = [
        ("SUBSTR(NAME,1,3)", "F.substring(F.col('NAME'), 1, 3)"),
        ("INDEXOF(C,'x')", "F.when((F.col('C')).isNull(), F.lit(None))"
                           ".when(F.col('C') == F.lit('x'), F.lit(1)).otherwise(F.lit(0))"),
    ]
    monkeypatch.setattr(
        "tests.test_spark_api_compatibility._emitted_snippets", lambda: good
    )
    for fn, advice in sorted(_WIDENED_IN_4.items()):
        test_no_emitted_call_passes_a_column_where_spark_35_needs_a_literal(fn, advice)
