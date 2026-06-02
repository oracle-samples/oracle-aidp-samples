"""Backend-agnostic factory for SystemTableStorage instances.

Thin wrapper around :func:`qualifire.storage.factory.open_storage` —
the same factory :class:`qualifire.api.Qualifire` uses internally.
Notebook callers can supply ``jdbc_config`` as either a
:class:`~qualifire.core.config.JDBCConfig` or a flat dict (the YAML
shape) and the wrapper normalizes before delegating.
"""

from __future__ import annotations

from typing import Any


def _spark_session(app_name: str = "qualifire-reporting") -> Any:
    """Return an active :class:`pyspark.sql.SparkSession`, building a
    local one if none exists yet."""
    from pyspark.sql import SparkSession

    return SparkSession.builder.appName(app_name).getOrCreate()


def make_storage(
    backend: str,
    system_table: str,
    jdbc_config: Any = None,
    *,
    spark: Any = None,
) -> Any:
    """Open a SystemTableStorage handle for the given backend.

    Args:
        backend: One of ``"sqlite"``, ``"jdbc"``, ``"external_catalog"``,
            ``"delta"``.
        system_table: Backend-specific identifier for the history
            table — a filesystem path for sqlite, a fully-qualified
            ``catalog.schema.table`` for external_catalog/delta, or
            the remote DB table name for jdbc.
        jdbc_config: Required when ``backend == "jdbc"``. Accepts
            either a :class:`~qualifire.core.config.JDBCConfig` (the
            programmatic shape) or a flat dict (the YAML shape, e.g.
            ``{"url": "...", "user": "...", "properties": {...}}``).
            ``None`` for non-JDBC backends.
        spark: Optional pre-built SparkSession to reuse for the
            Spark-backed storages. When omitted, the factory builds /
            attaches to a local one via ``getOrCreate``. Ignored for
            sqlite (the sqlite storage is pure-Python).

    Returns:
        An initialized storage instance.

    Raises:
        ValueError: ``backend`` is unknown, JDBC was requested
            without a ``url``, or a Spark backend was requested
            without a SparkSession we can build.
    """
    from qualifire.core.config import JDBCConfig
    from qualifire.storage.factory import open_storage

    jdbc: JDBCConfig | None
    if jdbc_config is None:
        jdbc = None
    elif isinstance(jdbc_config, JDBCConfig):
        jdbc = jdbc_config
    elif isinstance(jdbc_config, dict):
        # Accept the YAML / notebook flat-dict shape. Empty / missing
        # values pass through as None — the factory's url check raises
        # the user-facing migration error.
        jdbc = JDBCConfig(**jdbc_config)
    else:
        raise ValueError(
            f"jdbc_config must be JDBCConfig, dict, or None — got "
            f"{type(jdbc_config).__name__}"
        )

    # Sqlite has no Spark dependency; passing ``spark=`` to a sqlite
    # call is a likely caller mistake (e.g. a notebook author who
    # supplied a session expecting it would be used) — surface it
    # rather than silently ignoring.
    if backend == "sqlite" and spark is not None:
        raise ValueError(
            "make_storage(backend='sqlite', spark=...) was called with a "
            "SparkSession, but sqlite is pure Python and never uses one. "
            "Drop the spark= argument or pick a Spark-backed backend "
            "('external_catalog', 'delta', 'jdbc')."
        )
    needs_spark = backend in ("external_catalog", "delta", "jdbc")
    spark_session = spark or (_spark_session() if needs_spark else None)
    return open_storage(backend, system_table, jdbc=jdbc, spark=spark_session)
