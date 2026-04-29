"""Helpers for AIDP's built-in `aidataplatform` Spark format handler.

The `aidataplatform` format is registered on every AIDP cluster and dispatches
on the `type` option to a connector implementation. The shape is identical
across many connector types — host/port/user/password/schema/table — only
`type` and a small handful of type-specific keys differ.

Connector types known to be supported by the format handler (from the official
oracle-aidp-samples repo):

* ``ORACLE_DB``       — Oracle DB on Compute / on-prem / Base DB
* ``ORACLE_EXADATA``  — Exadata Cloud Service
* ``ORACLE_ALH``      — Autonomous Data Warehouse / Lakehouse
* ``ORACLE_ATP``      — Autonomous Transaction Processing
* ``POSTGRESQL``      — PostgreSQL
* ``MYSQL``           — MySQL
* ``MYSQL_HEATWAVE``  — OCI MySQL HeatWave
* ``SQLSERVER``       — Microsoft SQL Server
* ``KAFKA``           — Kafka via the format-handler shape
* ``FUSION_BICC``     — Fusion BICC bulk extracts
* ``GENERIC_REST``    — any REST API with a manifest

This module exposes one canonical builder, ``aidataplatform_options()``, that
takes the common keys and lets caller-specific extras flow through. Skills
that wrap a particular ``type`` should compose this helper rather than
re-declaring the option dict.
"""

from __future__ import annotations

from typing import Optional


AIDP_FORMAT = "aidataplatform"


def aidataplatform_options(
    *,
    type: str,
    host: Optional[str] = None,
    port: Optional[int] = None,
    database_name: Optional[str] = None,
    user: Optional[str] = None,
    password: Optional[str] = None,
    schema: Optional[str] = None,
    table: Optional[str] = None,
    extra: Optional[dict] = None,
) -> dict:
    """Build an option dict for ``spark.read.format("aidataplatform").options(**...)``.

    Args:
        type: Connector type — one of the constants documented at the module
            top. Required.
        host: DB host or service-FQDN.
        port: DB port. Coerced to ``str`` because Spark options are stringly
            typed.
        database_name: DB/schema name where the type uses one (SQLSERVER,
            ORACLE_DB, ORACLE_EXADATA on write).
        user: ``user.name`` option value.
        password: ``password`` option value.
        schema: Logical schema (different from ``database_name`` — e.g.
            Oracle SCHEMA, Postgres schema).
        table: Source/target table.
        extra: Any additional ``aidataplatform`` options (e.g. wallet.content,
            tns, ssl.enabled, manifest.url, fusion.external.storage,
            datastore, write.mode, catalog.id, pushdown.sql).

    Returns:
        A dict ready to pass to ``.options(**opts)``. Keys absent in the
        arguments are absent in the result.
    """
    if not type:
        raise ValueError("aidataplatform_options requires a `type`")
    opts: dict = {"type": type}
    if host is not None:
        opts["host"] = host
    if port is not None:
        opts["port"] = str(port)
    if database_name is not None:
        opts["database.name"] = database_name
    if user is not None:
        opts["user.name"] = user
    if password is not None:
        opts["password"] = password
    if schema is not None:
        opts["schema"] = schema
    if table is not None:
        opts["table"] = table
    if extra:
        opts.update(extra)
    return opts
