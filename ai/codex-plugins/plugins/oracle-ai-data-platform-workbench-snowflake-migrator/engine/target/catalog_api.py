"""Create AIDP structure through the catalog REST API. Pure body construction.

Why this exists: `POST /workspaces/<ws>/sql/execute` returns 404 -- verified
against a live DataLake -- so the SQL transport cannot create anything. Schema,
table and view CRUD is GA on the catalog API, and for a STRUCTURE-ONLY clone it
is the better transport anyway:

  * it needs NO Spark cluster, so nothing has to be started and nothing is
    billed for the clone
  * there is no session, so the "batch DDL is silently discarded when the
    session closes" behaviour the SQL deploy path was built around cannot
    happen
  * columns are posted as a FIELD LIST rather than rendered into CREATE TABLE
    text, so the structure is data rather than a string to be re-parsed

Two shapes were read off a real table in the target environment rather than
taken from documentation:

  * `fieldPrecision` and `fieldScale` are STRINGS, not numbers
  * `schemaKey` must be fully qualified `<catalog>.<schema>`; a bare schema
    returns 400 InvalidParameter

Body construction is pure and unit-tested. Nothing here performs I/O.
"""
from __future__ import annotations

import re

__all__ = ["UnmappableFieldType", "InvalidCatalogSpec", "field_from_spark_type",
           "build_schema_body", "build_table_body", "build_view_body",
           "build_catalog_body", "MANAGED_FORMAT", "CATALOG_TYPES",
           "normalize_catalog_type", "PROPERTIES_THIS_BODY_CANNOT_CARRY"]

# What this transport CANNOT express, named once so every caller reports the
# same thing rather than silently sending less than the plan.
#
# A field entry is `fieldName` / `fieldType` (+ `fieldPrecision` /
# `fieldScale` for decimal) / `fieldDescription`. There is no nullability
# field: the shape was read off a real table in the target environment, and
# inventing a key here would be a guess posted to a create that returns 202
# and then fails silently. So `NOT NULL` -- which IS in the reviewed SQL and
# IS applied by the in-AIDP structure notebook -- does not travel on this
# path, and that is reported per object instead of dropped.
PROPERTIES_THIS_BODY_CANNOT_CARRY = {
    "not_null": (
        "the catalog API table body has no nullability field (a field is "
        "fieldName/fieldType/fieldPrecision/fieldScale/fieldDescription), so "
        "columns the plan declares NOT NULL are created NULLABLE on this "
        "transport. The plugin sends only the fields the API defines. Create "
        "the table with the in-AIDP structure notebook (`snowmig provision` "
        "+ 01_create_structure), which emits the reviewed CREATE TABLE "
        "verbatim, if the constraint has to hold."),
    # The Delta clauses `ddl` carries into the reviewed CREATE TABLE
    # (statement `delta_features`, rule R13 in DDL_PLAN.md). The table body
    # is fields + description only: there is no clustering or table-property
    # field, so on this path they are not applied.
    "cluster_by": (
        "the catalog API table body has no clustering field, so the table "
        "was created WITHOUT liquid clustering. Apply it afterwards with "
        "ALTER TABLE ... CLUSTER BY, or create the table with the in-AIDP "
        "structure notebook (`snowmig provision` + 01_create_structure), "
        "which emits the reviewed CREATE TABLE verbatim (rule R13)."),
    "tblproperties": (
        "the catalog API table body has no table-property field, so the "
        "table was created WITHOUT it. Apply it afterwards with ALTER TABLE "
        "... SET TBLPROPERTIES, or create the table with the in-AIDP "
        "structure notebook (`snowmig provision` + 01_create_structure), "
        "which emits the reviewed CREATE TABLE verbatim (rule R13)."),
}

# What AIDP calls its two catalog shapes, LIVE-VERIFIED 2026-09-18 by reading
# the catalogType of every catalog on a real DataLake: INTERNAL and EXTERNAL,
# and nothing else. INTERNAL holds managed Delta tables the migrator writes to
# directly; EXTERNAL is a registered, read-only pointer at a live source and
# holds no managed data of its own.
CATALOG_TYPES = ("EXTERNAL", "INTERNAL")

# The runbook, the skills and the CLI all say "STANDARD" for the managed
# shape, but the API has never accepted it -- POSTing catalogType=STANDARD is
# a flat `400 InvalidParameter: Invalid CatalogType: STANDARD`. The word is
# kept as an ACCEPTED ALIAS so the documented vocabulary keeps working, and
# it is translated here, once, so no caller can put it on the wire.
_CATALOG_TYPE_ALIASES = {"STANDARD": "INTERNAL"}


def normalize_catalog_type(catalog_type: str | None) -> str:
    """The wire value for a catalog type, resolving the STANDARD alias.

    Unknown values pass through untouched so the caller -- not this helper --
    owns the refusal and its message.
    """
    value = str(catalog_type or "").strip().upper()
    return _CATALOG_TYPE_ALIASES.get(value, value)

# Managed Delta, always. The environment's own tables report CSV in places;
# this plugin creates Delta and says so explicitly rather than inheriting a
# catalog default.
MANAGED_FORMAT = "DELTA"

# Spark type -> catalog fieldType. Lower-case, matching what the API returns.
_SIMPLE = {
    "STRING": "string",
    "BOOLEAN": "boolean",
    "DATE": "date",
    "DOUBLE": "double",
    "FLOAT": "float",
    "BINARY": "binary",
    "TIMESTAMP": "timestamp",
    "TIMESTAMP_NTZ": "timestamp_ntz",
    "INT": "int",
    "INTEGER": "int",
    "BIGINT": "bigint",
    "SMALLINT": "smallint",
    "TINYINT": "tinyint",
}

_DECIMAL = re.compile(r"^DECIMAL\s*\(\s*(\d+)\s*(?:,\s*(\d+)\s*)?\)$", re.I)

# Verified live: the catalog API accepts timestamp, date, boolean, binary,
# double, bigint, int and float -- but NOT timestamp_ntz. A POST carrying it
# returns 202 Accepted and the asynchronous create then fails SILENTLY: the
# table never appears and no error is reported anywhere.
_SILENTLY_REJECTED = {
    "TIMESTAMP_NTZ": (
        "the catalog API accepts `timestamp` but not `timestamp_ntz` as a "
        "field type, so this column cannot be sent as planned. Snowflake "
        "TIMESTAMP_NTZ is mapped to Spark TIMESTAMP_NTZ on purpose, because "
        "bare TIMESTAMP is session-timezone-dependent and the wrong choice "
        "shifts every timestamp. Downgrading is therefore a DECISION: pass "
        "timestamp_ntz_as_timestamp=True (CLI: --timestamp-ntz timestamp) to "
        "accept the timezone semantics change."),
}


class InvalidCatalogSpec(ValueError):
    """A catalog body was asked for without the fields that make it valid."""


class UnmappableFieldType(ValueError):
    """A Spark type with no catalog field equivalent. Refused, never guessed.

    A field type invented here would create a table whose columns silently
    differ from the plan, and the structure probe would then report a mismatch
    it could not explain.
    """


def field_from_spark_type(name: str, spark_type: str | None,
                          description: str | None = None, *,
                          timestamp_ntz_as_timestamp: bool = False) -> dict:
    """One `tableFields` entry for a column."""
    if not name:
        raise ValueError("column name is required")
    if not spark_type:
        raise UnmappableFieldType(f"{name}: no target type was resolved")

    text = " ".join(str(spark_type).strip().upper().split())
    field = {"fieldName": name, "fieldDescription": description}

    if text == "TIMESTAMP_NTZ":
        if not timestamp_ntz_as_timestamp:
            raise UnmappableFieldType(f"{name}: {_SILENTLY_REJECTED[text]}")
        field["fieldType"] = "timestamp"
        field["fieldDescription"] = (
            (description + " | " if description else "")
            + "was TIMESTAMP_NTZ in Snowflake; stored as timestamp because the "
              "catalog API has no timestamp_ntz. TIMEZONE SEMANTICS DIFFER: "
              "Spark timestamp is session-timezone-dependent.")
        return field

    match = _DECIMAL.match(text)
    if match:
        precision, scale = match.group(1), match.group(2) or "0"
        # Strings on purpose: that is what the API returns for these.
        field.update({"fieldType": "decimal", "fieldPrecision": precision,
                      "fieldScale": scale})
        return field

    if text in _SIMPLE:
        field["fieldType"] = _SIMPLE[text]
        return field

    raise UnmappableFieldType(
        f"{name}: Spark type {spark_type!r} has no catalog field equivalent. "
        f"Refusing to guess one -- a wrong field type would create a table "
        f"that differs from the plan.")


def _fields(columns: list[dict], *,
            timestamp_ntz_as_timestamp: bool = False) -> list[dict]:
    return [field_from_spark_type(
                c.get("name"), c.get("type"), c.get("description"),
                timestamp_ntz_as_timestamp=timestamp_ntz_as_timestamp)
            for c in columns]


def build_schema_body(catalog: str, schema: str,
                      description: str = "") -> dict:
    if not catalog or not schema:
        raise ValueError("catalog and schema are both required")
    return {"displayName": schema, "catalogName": catalog,
            "description": description}


def build_table_body(catalog: str, schema: str, table: str,
                     columns: list[dict], description: str = "", *,
                     timestamp_ntz_as_timestamp: bool = False) -> dict:
    """A managed Delta table with zero rows. Structure only, always."""
    if not (catalog and schema and table):
        raise ValueError("catalog, schema and table are all required")
    if not columns:
        raise ValueError(
            f"{table}: refusing to create a table with no columns")
    return {
        "displayName": table,
        "catalogKey": catalog,
        # Fully qualified: a bare schema returns 400 InvalidParameter.
        "schemaKey": f"{catalog}.{schema}",
        "description": description,
        "tableType": "MANAGED",
        "managedTableDefinition": {"managedTableDataFormat": MANAGED_FORMAT},
        "tableFields": _fields(
            columns, timestamp_ntz_as_timestamp=timestamp_ntz_as_timestamp),
        "partitionKeys": [],
    }


def build_catalog_body(display_name: str, *, catalog_type: str = "EXTERNAL",
                       source_type: str | None = None, description: str = "",
                       connection: dict | None = None,
                       properties: dict | None = None) -> dict:
    """A `CreateCatalogDetails` body. EXTERNAL/SNOWFLAKE is the default shape.

    LIVE-VERIFIED NESTING (2026-09-16): the connection map rides inside
    `connectionDetails.connectionProperties` -- a flat `connectionDetails`
    is rejected with 400 InvalidParameter
    ("connectionDetails.connectionProperties must not be null"), observed
    against a real deployment and matching the documented
    `CreateCatalogDetails` schema. The KEY NAMES inside the map are still the
    connector-inferred ones; `testConnection` (see provision_api) is the
    documented way to prove them. This function never invents connection
    fields itself: it passes through exactly what
    `build_snowflake_connection_details` (or the caller) built.

    STANDARD catalogs hold managed Delta tables the migrator writes into
    directly and carry no `connectionDetails` or `sourceType` -- they are the
    higher-blast-radius shape (real storage, not a read-only pointer), which
    is why callers must ask for one explicitly rather than getting it by
    default. See `catalog_provision.ensure_catalog` for that gate.
    """
    if not display_name or not str(display_name).strip():
        raise InvalidCatalogSpec("display_name is required")
    catalog_type = normalize_catalog_type(catalog_type)
    if catalog_type not in CATALOG_TYPES:
        raise InvalidCatalogSpec(
            f"catalog_type must be one of {CATALOG_TYPES}, got {catalog_type!r}")

    body: dict = {"displayName": display_name, "description": description,
                  "catalogType": catalog_type, "properties": properties or {}}

    if catalog_type == "EXTERNAL":
        if not source_type:
            raise InvalidCatalogSpec(
                "an EXTERNAL catalog needs source_type (e.g. 'SNOWFLAKE')")
        if not connection:
            raise InvalidCatalogSpec(
                f"an EXTERNAL {source_type} catalog needs connectionDetails; "
                f"build one with build_snowflake_connection_details() from a "
                f"YAML/JSON connection config rather than passing fields "
                f"inline")
        body["sourceType"] = source_type
        # The map is NESTED under connectionProperties -- flat is a live 400.
        body["connectionDetails"] = {"connectionProperties": dict(connection)}

    return body


def build_view_body(catalog: str, schema: str, view: str, view_text: str,
                    columns: list[dict], description: str = "", *,
                    timestamp_ntz_as_timestamp: bool = False) -> dict:
    if not (catalog and schema and view):
        raise ValueError("catalog, schema and view are all required")
    if not (view_text or "").strip():
        raise ValueError(f"{view}: a view needs its defining SQL")
    return {
        "displayName": view,
        "catalogKey": catalog,
        "schemaKey": f"{catalog}.{schema}",
        "description": description,
        "viewText": view_text,
        "viewFields": _fields(
            columns,
            timestamp_ntz_as_timestamp=timestamp_ntz_as_timestamp)
        if columns else [],
    }
