"""Regression guard for the case-insensitive column-compare rewrite (PR #52 / M1).

_rewrite_case_insensitive_col_compare handles the AIDP metastore lowercasing the
Spark catalog columns, by rewriting the exact single-diff idiom
    <tgt> = set(<EXPECTED-names>) - set(<df>.columns)
(and its list-comprehension form) to a case-insensitive compare.

It MUST fire on that idiom, but MUST NOT fire on:
  - CHAINED diffs  set(a) - set(b) - set(df.columns)  (the lazy group backtracks
    and mis-captures, emitting semantically wrong code);
  - pandas columns-vs-columns  set(df1.columns) - set(df2.columns)  (pandas is
    case-SENSITIVE; converting it would silently change a valid program).

Run: python -m pytest tests/test_ci_col_compare_guard.py -v
"""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scripts"))
import job_migrate as jm


def _fire(code):
    return jm._rewrite_case_insensitive_col_compare(code)


# ── must fire (safe single-diff idiom) ──────────────────────────────────────
def test_safe_setdiff_fires():
    out, changed = _fire("missing = set(prod_var) - set(df.columns)")
    assert changed
    assert "_c.lower() not in" in out and "_cc.lower()" in out


def test_safe_listcomp_fires():
    out, changed = _fire("m = [c for c in prod_var if c not in df.columns]")
    assert changed
    assert ".lower() not in" in out


def test_safe_setdiff_idempotent():
    out1, changed1 = _fire("missing = set(prod_var) - set(df.columns)")
    assert changed1
    _, changed2 = _fire(out1)          # second pass must be a no-op
    assert not changed2


# ── must NOT fire (false-positive guards) ───────────────────────────────────
def test_chained_diff_not_rewritten():
    code = "x = set(a) - set(b) - set(df.columns)"
    out, changed = _fire(code)
    assert not changed
    assert out == code                 # left byte-for-byte untouched


def test_pandas_columns_vs_columns_setdiff_not_rewritten():
    code = "d = set(df1.columns) - set(df2.columns)"
    out, changed = _fire(code)
    assert not changed
    assert out == code


def test_pandas_columns_vs_columns_listcomp_not_rewritten():
    code = "d = [c for c in df1.columns if c not in df2.columns]"
    out, changed = _fire(code)
    assert not changed
    assert out == code


def test_plain_setdiff_without_columns_is_noop():
    _, changed = _fire("x = set(a) - set(b)")
    assert not changed
