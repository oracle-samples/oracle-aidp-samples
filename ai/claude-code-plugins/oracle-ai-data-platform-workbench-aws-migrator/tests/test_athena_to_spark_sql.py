"""Tests for the Athena(Presto)→Spark SQL translator. Runs standalone:

    python3 -m tests.test_athena_to_spark_sql

Also pytest-discoverable (functions named test_*).
"""
from __future__ import annotations

from aws_aidp.translate.athena_to_spark_sql import translate


def test_cardinality_requires_null_semantics_review():
    r = translate("SELECT cardinality(a) FROM t")
    assert r.translated_sql == "SELECT cardinality(a) FROM t"
    assert any(f.rule == "cardinality_null_semantics" for f in r.findings)


def test_unnest_bare_column():
    r = translate("SELECT x FROM t CROSS JOIN UNNEST(arr) AS u(x)")
    assert "LATERAL VIEW explode(arr) u AS x" in r.translated_sql
    assert "CROSS JOIN UNNEST" not in r.translated_sql
    assert r.flags == 0


def test_unnest_with_function_arg():
    """Regression: UNNEST(split(col, ';')) — the array expr has nested parens.
    Found live: previously fell through unrewritten AND unflagged (false OK)."""
    src = ("SELECT signal FROM claims c "
           "CROSS JOIN UNNEST(split(c.fraud_signals, ';')) AS t(signal)")
    r = translate(src)
    assert "LATERAL VIEW explode(split(c.fraud_signals, ';')) t AS signal" in r.translated_sql
    assert "CROSS JOIN UNNEST" not in r.translated_sql
    assert r.flags == 0


def test_unnest_with_ordinality_is_flagged():
    src = "SELECT x, i FROM t CROSS JOIN UNNEST(arr) WITH ORDINALITY AS u(x, i)"
    r = translate(src)
    assert r.flags >= 1
    assert any(f.rule == "unnest_with_ordinality" for f in r.findings)


def test_unhandled_unnest_is_flagged_not_silent():
    """Safety net: a multi-column UNNEST form we don't rewrite must be FLAGGED,
    never passed through as a clean translation."""
    src = "SELECT a, b FROM t CROSS JOIN UNNEST(arr1, arr2) AS u(a, b)"
    r = translate(src)
    assert r.flags >= 1
    assert any(f.rule == "unnest_unhandled" for f in r.findings)
    assert r.needs_manual_review


def test_histogram_is_flagged():
    r = translate("SELECT histogram(bucket) FROM t")
    assert r.flags >= 1
    assert any(f.rule == "histogram" for f in r.findings)


def test_clean_query_unchanged_no_flags():
    src = "SELECT a, b FROM t WHERE a > 1"
    r = translate(src)
    assert r.translated_sql == src
    assert r.changes == 0 and r.flags == 0


def _run_all():
    fns = [v for k, v in sorted(globals().items()) if k.startswith("test_") and callable(v)]
    for fn in fns:
        fn()
        print(f"  ok  {fn.__name__}")
    print(f"\n{len(fns)} passed")


if __name__ == "__main__":
    _run_all()
