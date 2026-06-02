"""Pandas backend implementation using an in-memory table registry."""

from __future__ import annotations

import logging
import re
from typing import Any

import pandas as pd

logger = logging.getLogger(__name__)


# Translate SQL-style filter expressions to pandas-query syntax.
# Sampler / aggregation collectors emit SQL fragments (``col = 'x'
# AND status = 'active'``) but ``df.query`` needs ``col == 'x' and
# status == 'active'``. Both rewrites are quote-aware so values
# containing ``=``, ``AND``, ``OR``, ``NOT`` (e.g. ``name = 'a=b'``,
# ``msg = 'a AND b'``) round-trip unchanged.
_KEYWORD_RE = re.compile(r"\b(AND|OR|NOT)\b")
_KEYWORD_LOWER = {"AND": "and", "OR": "or", "NOT": "not"}


def _sql_to_pandas_query(filter_expr: str | None) -> str | None:
    """Rewrite a SQL-style ``WHERE`` fragment into pandas-query form.

    Handles the cases the collectors actually emit:

    * SQL ``=`` → pandas ``==`` (preserves ``==`` / ``!=`` / ``<=``
      / ``>=``).
    * SQL ``AND`` / ``OR`` / ``NOT`` keywords → lowercase pandas
      keywords.

    Both rewrites skip characters inside string literals so values
    containing ``=`` / ``AND`` / ``OR`` / ``NOT`` round-trip
    unchanged. Quote handling covers:

    * Single-quoted literals (SQL standard): ``'value'``. SQL
      doubled-quote escape ``'it''s'`` works naturally — the
      consecutive quote pair flips state out then immediately back
      in, so the inner ``''`` boundary is fine.
    * Double-quoted literals: ``"value"``. Used by some operator
      filters and by pandas ``df.query`` itself, so we treat them
      as opaque.
    * Backslash-escaped quotes inside either form (``'it\\'s'``,
      ``"a\\"b"``): the backslash + quote pair is skipped so the
      walker doesn't mis-detect a quote exit.

    Returns ``None`` when the input is ``None``.
    """
    if filter_expr is None:
        return None

    out: list[str] = []
    buf: list[str] = []  # outside-quote run, batched for keyword regex

    def _flush_buf() -> None:
        if not buf:
            return
        chunk = "".join(buf)
        chunk = _KEYWORD_RE.sub(lambda m: _KEYWORD_LOWER[m.group(0)], chunk)
        out.append(chunk)
        buf.clear()

    i = 0
    quote_char: str | None = None
    while i < len(filter_expr):
        ch = filter_expr[i]
        if quote_char is not None:
            # Inside a string literal — passthrough, with one escape
            # rule: a backslash before the quote char skips the pair
            # so ``\'`` doesn't read as quote exit.
            if ch == "\\" and i + 1 < len(filter_expr):
                out.append(ch)
                out.append(filter_expr[i + 1])
                i += 2
                continue
            out.append(ch)
            if ch == quote_char:
                quote_char = None
            i += 1
            continue
        if ch in ("'", '"'):
            _flush_buf()
            out.append(ch)
            quote_char = ch
            i += 1
            continue
        if ch == "=":
            prev = filter_expr[i - 1] if i > 0 else ""
            nxt = filter_expr[i + 1] if i + 1 < len(filter_expr) else ""
            # Skip ``==``, ``!=``, ``<=``, ``>=``.
            if prev in "=!<>" or nxt == "=":
                buf.append(ch)
            else:
                buf.append("==")
            i += 1
            continue
        buf.append(ch)
        i += 1
    _flush_buf()

    return "".join(out)


class PandasBackend:
    """Backend wrapping Pandas DataFrames with SQL via pandasql (optional) or manual ops.

    Tables are registered in an in-memory dict. SQL execution requires pandasql.

    Args:
        tables: Optional initial mapping of table names to DataFrames.
    """

    def __init__(self, tables: dict[str, pd.DataFrame] | None = None):
        self._tables: dict[str, pd.DataFrame] = dict(tables or {})

    def register_table(self, name: str, df: pd.DataFrame) -> None:
        self._tables[name] = df

    def execute_sql(self, sql: str) -> pd.DataFrame:
        try:
            import pandasql
        except ImportError:
            raise ImportError(
                "pandasql is required for SQL execution with PandasBackend. "
                "Install it with: pip install pandasql"
            )
        return pandasql.sqldf(sql, self._tables)

    def get_table_metadata(self, table: str) -> dict[str, Any]:
        df = self._tables[table]
        columns = [
            {"name": col, "type": str(df[col].dtype), "nullable": df[col].isna().any()}
            for col in df.columns
        ]
        return {"table": table, "columns": columns, "partitions": []}

    def get_column_profile(
        self, table: str, column: str, filter_expr: str | None = None
    ) -> dict[str, Any]:
        df = self._tables[table]
        if filter_expr:
            df = df.query(_sql_to_pandas_query(filter_expr))
        col = df[column]
        total = len(df)
        non_null = col.count()
        return {
            "total_count": total,
            "non_null_count": int(non_null),
            "null_count": total - int(non_null),
            "null_pct": round((total - int(non_null)) * 100.0 / max(total, 1), 4),
            "min_value": col.min() if non_null > 0 else None,
            "max_value": col.max() if non_null > 0 else None,
            "mean_value": col.mean() if non_null > 0 else None,
            "stddev_value": col.std() if non_null > 0 else None,
            "approx_distinct": int(col.nunique()),
        }

    def get_aggregations(
        self, table: str, expressions: list[str], filter_expr: str | None = None
    ) -> dict[str, Any]:
        # For Pandas, we delegate to SQL via pandasql
        select_clause = ", ".join(expressions)
        where = f" WHERE {filter_expr}" if filter_expr else ""
        sql = f"SELECT {select_clause} FROM {table}{where}"
        result = self.execute_sql(sql)
        return result.iloc[0].to_dict()

    def sample_records(
        self, table: str, n: int, filter_expr: str | None = None
    ) -> pd.DataFrame:
        df = self._tables[table]
        if filter_expr:
            df = df.query(_sql_to_pandas_query(filter_expr))
        return df.sample(n=min(n, len(df)), random_state=42)

    def get_delta_history(self, table: str) -> None:
        return None  # Pandas doesn't support Delta

    def write_df(self, df: Any, table: str, options: dict[str, Any] | None = None) -> None:
        options = options or {}
        mode = options.get("mode", "append")
        if mode == "overwrite" or table not in self._tables:
            self._tables[table] = df
        else:
            self._tables[table] = pd.concat([self._tables[table], df], ignore_index=True)

    def drop_table(self, table: str) -> None:
        self._tables.pop(table, None)

    def drop_temp_view(self, name: str) -> None:
        """Pandas conflates temp views and permanent tables in the
        in-memory dict; mirror ``drop_table`` so the engine's
        backend-agnostic temp-view cleanup works."""
        self._tables.pop(name, None)

    def table_exists(self, table: str) -> bool:
        return table in self._tables
