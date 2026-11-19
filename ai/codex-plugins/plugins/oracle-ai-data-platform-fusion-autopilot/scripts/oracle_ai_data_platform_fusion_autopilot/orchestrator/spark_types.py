"""Spark SQL type-name normalisation — the single source of truth.

Factored out of :mod:`orchestrator.sql_runner` (which carries heavy Spark
imports) so that design-time, Spark-free validators — notably the
producer/consumer column-contract gate (AIDPF-2045) in
:mod:`orchestrator.content_pack_validators` — can share the *exact* type
comparison used by the runtime materialised-schema gate (AIDPF-4070) without
importing the runtime module.

``sql_runner`` re-imports :data:`_SPARK_TYPE_SYNONYMS` and
:func:`_normalise_spark_type` under their original names, so the 4070 gate is
unchanged and the two gates provably agree.
"""

from __future__ import annotations

_SPARK_TYPE_SYNONYMS = {
    "int": "integer",
    "long": "bigint",
    "double": "double",
    "string": "string",
    "boolean": "boolean",
    "timestamp": "timestamp",
    "date": "date",
}


def _normalise_spark_type(type_str: str) -> str:
    """Map common Spark type synonyms to a canonical form for comparison."""
    t = type_str.strip().lower()
    return _SPARK_TYPE_SYNONYMS.get(t, t)


__all__ = ["_SPARK_TYPE_SYNONYMS", "_normalise_spark_type"]
