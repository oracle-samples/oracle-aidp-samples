"""Tests for PandasBackend."""

import pandas as pd
import pytest

from qualifire.backends.pandas_backend import PandasBackend


@pytest.fixture
def backend():
    df = pd.DataFrame({
        "id": [1, 2, 3, 4, 5],
        "amount": [10.0, 20.0, 30.0, None, 50.0],
        "category": ["A", "B", "A", "B", "C"],
    })
    return PandasBackend(tables={"sales": df})


class TestPandasBackend:
    def test_table_exists(self, backend):
        assert backend.table_exists("sales")
        assert not backend.table_exists("missing")

    def test_get_table_metadata(self, backend):
        meta = backend.get_table_metadata("sales")
        assert meta["table"] == "sales"
        cols = {c["name"] for c in meta["columns"]}
        assert cols == {"id", "amount", "category"}

    def test_get_column_profile(self, backend):
        profile = backend.get_column_profile("sales", "amount")
        assert profile["total_count"] == 5
        assert profile["null_count"] == 1
        assert profile["non_null_count"] == 4
        assert profile["min_value"] == 10.0
        assert profile["max_value"] == 50.0

    def test_get_column_profile_with_filter(self, backend):
        profile = backend.get_column_profile("sales", "amount", "category == 'A'")
        assert profile["total_count"] == 2

    def test_sample_records(self, backend):
        sample = backend.sample_records("sales", 3)
        assert len(sample) == 3

    def test_sample_records_clamps(self, backend):
        """Sampling more than available returns all."""
        sample = backend.sample_records("sales", 100)
        assert len(sample) == 5

    def test_get_delta_history_returns_none(self, backend):
        assert backend.get_delta_history("sales") is None

    def test_write_df_overwrite(self, backend):
        new_df = pd.DataFrame({"id": [99]})
        backend.write_df(new_df, "sales", {"mode": "overwrite"})
        assert len(backend._tables["sales"]) == 1

    def test_write_df_append(self, backend):
        new_df = pd.DataFrame({"id": [6, 7], "amount": [60.0, 70.0], "category": ["D", "E"]})
        backend.write_df(new_df, "sales", {"mode": "append"})
        assert len(backend._tables["sales"]) == 7

    def test_write_df_new_table(self, backend):
        new_df = pd.DataFrame({"x": [1]})
        backend.write_df(new_df, "new_table")
        assert backend.table_exists("new_table")

    def test_drop_table(self, backend):
        backend.drop_table("sales")
        assert not backend.table_exists("sales")

    def test_drop_nonexistent_table(self, backend):
        backend.drop_table("missing")  # should not raise

    def test_register_table(self, backend):
        df = pd.DataFrame({"a": [1]})
        backend.register_table("custom", df)
        assert backend.table_exists("custom")


# ---------------------------------------------------------------------------
# SQL → pandas-query translator (R3 review fix)
# ---------------------------------------------------------------------------


class TestSqlToPandasQuery:
    """The sampler emits SQL-style filter strings; the production
    PandasBackend must translate ``=``/``AND``/``OR``/``NOT`` so
    ``df.query`` succeeds. Covers the breakage flagged in the R3
    adversarial review where the test backend rewrote `=` but
    production didn't."""

    def test_bare_equal_becomes_double(self):
        from qualifire.backends.pandas_backend import _sql_to_pandas_query
        assert _sql_to_pandas_query("col = 'x'") == "col == 'x'"

    def test_existing_double_equal_preserved(self):
        from qualifire.backends.pandas_backend import _sql_to_pandas_query
        assert _sql_to_pandas_query("col == 'x'") == "col == 'x'"

    def test_inequality_operators_preserved(self):
        from qualifire.backends.pandas_backend import _sql_to_pandas_query
        for op in ("!=", "<=", ">="):
            assert _sql_to_pandas_query(f"col {op} 1") == f"col {op} 1"

    def test_quoted_equals_round_trips_unchanged(self):
        """A literal containing ``=`` (e.g. ``'a=b'``) must survive."""
        from qualifire.backends.pandas_backend import _sql_to_pandas_query
        assert (
            _sql_to_pandas_query("name = 'a=b'")
            == "name == 'a=b'"
        )

    def test_uppercase_and_or_not_lowered(self):
        from qualifire.backends.pandas_backend import _sql_to_pandas_query
        assert (
            _sql_to_pandas_query("a = 1 AND b = 2 OR NOT c = 3")
            == "a == 1 and b == 2 or not c == 3"
        )

    def test_keywords_inside_quoted_literals_preserved(self):
        """A literal containing ``AND`` / ``OR`` / ``NOT`` must NOT
        be lowercased — the value would silently change. R4 review
        fix."""
        from qualifire.backends.pandas_backend import _sql_to_pandas_query
        assert (
            _sql_to_pandas_query("msg = 'a AND b'")
            == "msg == 'a AND b'"
        )
        assert (
            _sql_to_pandas_query("name = 'or' AND status = 'NOT yet'")
            == "name == 'or' and status == 'NOT yet'"
        )

    def test_double_quoted_literals_are_quote_protected(self):
        """``"value"`` literals also opt out of rewriting — operators
        sometimes write filters in pandas-query syntax directly, and
        the walker must not corrupt their string contents.
        R5 review fix."""
        from qualifire.backends.pandas_backend import _sql_to_pandas_query
        assert (
            _sql_to_pandas_query('msg = "a=b AND c"')
            == 'msg == "a=b AND c"'
        )
        # Mixed: outside-quote operator gets rewritten; inside-quote
        # text doesn't.
        assert (
            _sql_to_pandas_query('a = 1 AND b = "x AND y"')
            == 'a == 1 and b == "x AND y"'
        )

    def test_backslash_escaped_quote_does_not_exit_quote_state(self):
        """Backslash-escaped quotes (``'it\\'s'``, ``"a\\"b"``)
        stay inside the literal — the walker must not flip out of
        quote mode at the escaped quote, otherwise the trailing
        text gets rewritten as if it were outside a string. R5
        review fix."""
        from qualifire.backends.pandas_backend import _sql_to_pandas_query
        assert (
            _sql_to_pandas_query("msg = 'it\\'s AND a=b'")
            == "msg == 'it\\'s AND a=b'"
        )
        assert (
            _sql_to_pandas_query('msg = "a\\"b AND c=d"')
            == 'msg == "a\\"b AND c=d"'
        )

    def test_sql_doubled_quote_escape_round_trips(self):
        """SQL standard ``''`` inside a single-quoted literal works
        naturally — the consecutive pair flips out then back in.
        Documented in the docstring; locked here so future walker
        tweaks don't break it."""
        from qualifire.backends.pandas_backend import _sql_to_pandas_query
        assert (
            _sql_to_pandas_query("name = 'it''s'")
            == "name == 'it''s'"
        )

    def test_none_passes_through(self):
        from qualifire.backends.pandas_backend import _sql_to_pandas_query
        assert _sql_to_pandas_query(None) is None

    def test_pandas_backend_sample_records_accepts_sql_eq(self):
        """End-to-end: production PandasBackend takes SQL-style
        ``col = 'x'`` and returns the matching rows. Before the fix
        this raised ``ValueError: cannot assign without a target``."""
        import pandas as pd
        from qualifire.backends.pandas_backend import PandasBackend

        df = pd.DataFrame({"region": ["us", "uk", "us"], "amount": [1, 2, 3]})
        backend = PandasBackend(tables={"t": df})

        out = backend.sample_records("t", n=10, filter_expr="region = 'us'")
        assert len(out) == 2
        assert set(out["region"]) == {"us"}
