"""NULL-safe equality predicates used across storage backends.

The history-read SELECTs all need to filter by columns whose value
may be SQL NULL — ``dimension_value`` is the canonical example
(NULL for non-dimensioned rows). No single SQL operator works
across SQLite (``IS``), Spark (``<=>``), and the targeted JDBC
dialects (Oracle / PostgreSQL / MySQL / DB2 lack a portable
null-safe operator), so two helpers live here:

* :func:`spark_dim_predicate` — uses Spark's ``<=>`` operator.
* :func:`jdbc_dim_predicate` — switches between ``IS NULL`` and
  ``= 'value'`` based on the input.

SQLite uses its native ``IS`` directly in the SELECT (see
``sqlite_storage.py``); the helper isn't needed there because
``IS`` doubles as both null-safe equality and ordinary equality.
"""

from __future__ import annotations


def spark_dim_predicate(column: str, value: str | None) -> str:
    """Render a NULL-safe equality predicate for ``column`` in Spark
    SQL. ``<=>`` (NULL-safe equal-to) handles
    ``column IS NULL`` and ``column = 'x'`` uniformly.

    For ``value=None`` we emit ``IS NULL`` rather than ``<=> NULL``
    because the SQL string is already simple and IS NULL is more
    portable across Spark + Delta + external catalog SQL execution
    surfaces.
    """
    if value is None:
        return f"{column} IS NULL"
    safe = value.replace("'", "''")
    return f"{column} <=> '{safe}'"


def jdbc_dim_predicate(column: str, value: str | None) -> str:
    """Render a NULL-safe equality predicate for ``column`` portable
    across Oracle / PostgreSQL / MySQL / DB2.

    None of the targeted dialects share a single null-safe equality
    operator (Postgres has ``IS NOT DISTINCT FROM``, MySQL has
    ``<=>``, Oracle has neither), so we generate the predicate from
    Python: ``IS NULL`` for ``None`` and a literal-string equality
    otherwise.

    Apostrophe escaping uses the standard SQL doubled-quote form
    (``'`` → ``''``), which is portable across the targeted
    dialects. Backslash-escapes (``\\'``) and other dialect-specific
    quoting conventions are not generated — operators using
    non-standard SQL quoting modes need to switch their database
    config.
    """
    if value is None:
        return f"{column} IS NULL"
    safe = value.replace("'", "''")
    return f"{column} = '{safe}'"
