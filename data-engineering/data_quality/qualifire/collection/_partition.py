"""Shared partition_ts plumbing for collectors.

All four collectors that produce :class:`CollectionResult` rows
(``aggregation``, ``metrics``, ``custom_query``, ``profiling``) need to
stamp ``cr.partition_ts`` when the dataset declared a
``partition_ts`` expression. The pattern is identical:

1. Render the Jinja expression once at collect time.
2. Add ``(<expr>) AS qf_partition_ts`` to the SELECT (alongside the
   user's metric expressions).
3. Pop the synthetic column out of each row.
4. Coerce the raw cell value into a ``datetime``.

This module is the single source of truth so the per-collector
modules don't repeat themselves and so any future change (alias
rename, coercion rule change) lands in one place.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any

# Reserved column alias used to surface the dataset's partition_ts
# expression into each result row. Collectors strip this column from
# the metrics dict so it doesn't appear as a user metric.
PARTITION_ALIAS = "qf_partition_ts"


def coerce_partition_ts(value: Any) -> datetime | None:
    """Coerce a raw cell value (datetime, date, ISO string, ...) into datetime.

    Spark hands back ``datetime.datetime`` / ``datetime.date`` objects;
    other backends may return ISO strings. Normalise so downstream
    code always sees a ``datetime | None``.
    """
    if value is None:
        return None
    if isinstance(value, datetime):
        return value
    # ``datetime.date`` (not datetime) — promote to datetime at midnight.
    from datetime import date as _date
    if isinstance(value, _date):
        return datetime(value.year, value.month, value.day)
    if isinstance(value, str):
        try:
            return datetime.fromisoformat(value)
        except ValueError:
            try:
                return datetime.fromisoformat(value.replace(" ", "T"))
            except ValueError:
                return None
    return None
