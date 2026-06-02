"""Shared SystemTableStorage factory.

Single source of truth for "open a storage handle for backend X".
Both :class:`qualifire.api.Qualifire` (full engine path) and
:func:`qualifire.reporting.make_storage` (notebook / ad-hoc path)
call into here so the per-backend wiring lives in exactly one place.
"""

from __future__ import annotations

from typing import Any

from qualifire.core.config import JDBCConfig


def open_storage(
    backend: str,
    system_table: str,
    *,
    jdbc: JDBCConfig | None = None,
    spark: Any = None,
) -> Any:
    """Open and initialize a SystemTableStorage for ``backend``.

    Args:
        backend: One of ``"sqlite"``, ``"external_catalog"``,
            ``"delta"``, ``"jdbc"``.
        system_table: Backend-specific identifier (filesystem path
            for sqlite, fully-qualified table name for the others,
            remote DB table name for jdbc).
        jdbc: Required for ``backend="jdbc"``. Must carry a non-empty
            ``url``; ``user``/``password``/``driver`` collapse into
            connection properties via ``properties.setdefault(...)`` so
            an explicit entry in ``properties`` wins.
        spark: Required for ``"external_catalog"``, ``"delta"``,
            ``"jdbc"``. Ignored for sqlite. The Spark-backed paths
            don't auto-create a session here — the caller is
            responsible (Qualifire pulls it from
            ``backend.spark``; ``make_storage`` builds / attaches via
            ``getOrCreate``).

    Returns:
        An initialized storage instance (``SQLiteStorage`` /
        ``DeltaStorage`` / ``ExternalCatalogStorage`` / ``JDBCStorage``).

    Raises:
        ValueError: ``backend`` is unknown, JDBC config is missing or
            invalid, or a Spark backend was requested without a
            SparkSession.
    """
    if backend == "sqlite":
        from qualifire.storage.sqlite_storage import SQLiteStorage

        storage = SQLiteStorage(db_path=system_table)
    elif backend == "external_catalog":
        from qualifire.storage.external_catalog import ExternalCatalogStorage

        if spark is None:
            raise ValueError(
                "system_table_backend='external_catalog' requires a "
                "SparkSession — pass spark=... or use a Spark-capable backend."
            )
        storage = ExternalCatalogStorage(spark, system_table)
    elif backend == "delta":
        from qualifire.storage.delta_storage import DeltaStorage

        if spark is None:
            raise ValueError(
                "system_table_backend='delta' requires a SparkSession — "
                "pass spark=... or use a Spark-capable backend."
            )
        storage = DeltaStorage(spark, system_table)
    elif backend == "jdbc":
        from qualifire.storage.jdbc_storage import JDBCStorage

        if jdbc is None or not jdbc.url:
            raise ValueError(
                "system_table_backend='jdbc' requires a jdbc connection "
                "config with a 'url' (plus optional "
                "user/password/driver/properties). Pass "
                "Qualifire(..., jdbc=JDBCConfig(url=..., ...)) "
                "programmatically, or provide a top-level 'jdbc:' "
                "block with 'url:' in the YAML config."
            )
        if spark is None:
            raise ValueError(
                "system_table_backend='jdbc' requires a Spark-capable "
                "backend / SparkSession. JDBCStorage uses Spark's JDBC "
                "DataFrame reader/writer; pure-Python JDBC is not "
                "supported. Pass spark=... or switch to a non-JDBC "
                "backend ('sqlite' for dev/test)."
            )
        props = dict(jdbc.properties)
        if jdbc.user is not None:
            props.setdefault("user", jdbc.user)
        if jdbc.password is not None:
            props.setdefault("password", jdbc.password)
        if jdbc.driver is not None:
            props.setdefault("driver", jdbc.driver)
        storage = JDBCStorage(
            spark=spark,
            table=system_table,
            jdbc_url=jdbc.url,
            connection_properties=props,
        )
    else:
        raise ValueError(
            f"Unknown system_table_backend: {backend!r}. "
            "Supported values: 'external_catalog', 'delta', 'sqlite', 'jdbc'."
        )

    storage.initialize()
    return storage
