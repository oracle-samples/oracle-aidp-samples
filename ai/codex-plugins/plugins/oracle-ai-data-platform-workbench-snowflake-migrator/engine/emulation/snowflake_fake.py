"""An emulated Snowflake estate for dev mode. Zero network, zero credentials.

One database, SNOWDEMO, small but deliberately awkward — every object exists
to teach one real migration lesson:

  SALES.ORDERS        clusters, tracks changes, overrides retention, churns
                      >1M rewritten rows: every maintenance signal fires.
  SALES.CUSTOMERS     carries a masking policy on EMAIL: the security stage
                      must report the exposure.
  SALES.EVENTS_RAW    a VARIANT column: blocked at assess unless the operator
                      opts into string.
  SALES.LEGACY_AUDIT  fine here — the EMULATED AIDP silently drops its create,
                      demonstrating the poisoned-name diagnosis.
  ANALYTICS.ORDER_SUMMARY_VW   translatable view (IFF, ::) — and the emulated
                               AIDP re-derives one column type, demonstrating
                               derived-type drift.
  ANALYTICS.TOP_CUSTOMERS_VW   QUALIFY: refused, never guessed.
  ANALYTICS.CUSTOMER_360_VW    SECURE view: unsupported object.
  TASK_LOAD_ORDERS             populates a migrating table — the blast-radius
                               lesson: the clone succeeds and then goes stale.

`demo_run_sql(sql, params)` answers exactly the read-only queries the real
extractors issue, in the row shapes Snowflake returns. An unrecognised query
raises — the emulation must never silently answer a question it was not built
for, because that is how a fake starts lying.

A second, larger estate -- ENTERPRISE, database SNOWENT -- lives at the end of
this module (`enterprise_run_sql`). It holds what a trial account cannot:
external and Iceberg tables, hybrid and event tables, shares with consumers,
Snowpark Container Services, a native app, replication and failover groups.
"""
from __future__ import annotations

__all__ = ["DEMO_DB", "demo_run_sql", "ENTERPRISE_DB", "enterprise_run_sql"]

DEMO_DB = "SNOWDEMO"

_SESSION = {"U": "DEMO_MIGRATOR", "A": "DEMO_ACCOUNT", "R": "EMULATED_REGION",
            "ROLE": "MIGRATION_READER", "WH": "WH_ETL", "V": "emulated"}

_SCHEMAS = ("SALES", "ANALYTICS")

_ORDER_SUMMARY_SQL = (
    "CREATE OR REPLACE VIEW SNOWDEMO.ANALYTICS.ORDER_SUMMARY_VW AS\n"
    "select o.CUSTOMER_ID::string as CUSTOMER,\n"
    "       iff(o.AMOUNT > 100, 'BIG', 'SMALL') as BUCKET,\n"
    "       sum(o.AMOUNT) as TOTAL_AMOUNT,\n"
    "       count(*) as ORDER_COUNT\n"
    "from SNOWDEMO.SALES.ORDERS o\n"
    "group by 1, 2")

_TOP_CUSTOMERS_SQL = (
    "CREATE OR REPLACE VIEW SNOWDEMO.ANALYTICS.TOP_CUSTOMERS_VW AS\n"
    "select CUSTOMER_ID, sum(AMOUNT) as TOTAL\n"
    "from SNOWDEMO.SALES.ORDERS\n"
    "group by 1\n"
    "qualify row_number() over (order by TOTAL desc) <= 10")

_CUSTOMER_360_SQL = (
    "CREATE OR REPLACE SECURE VIEW SNOWDEMO.ANALYTICS.CUSTOMER_360_VW AS\n"
    "select c.CUSTOMER_ID, c.FULL_NAME, count(o.ORDER_ID) as ORDERS\n"
    "from SNOWDEMO.SALES.CUSTOMERS c\n"
    "join SNOWDEMO.SALES.ORDERS o on o.CUSTOMER_ID = c.CUSTOMER_ID\n"
    "group by 1, 2")

_VIEW_SQL = {"ORDER_SUMMARY_VW": _ORDER_SUMMARY_SQL,
             "TOP_CUSTOMERS_VW": _TOP_CUSTOMERS_SQL,
             "CUSTOMER_360_VW": _CUSTOMER_360_SQL}

_SHOW_TABLES = {
    "SALES": [
        {"name": "ORDERS", "rows": 1_250_000, "bytes": 402_653_184,
         "comment": "order fact table", "owner": "SYSADMIN",
         "cluster_by": "LINEAR(ORDER_DATE)", "automatic_clustering": "ON",
         "change_tracking": "ON", "retention_time": "7",
         "search_optimization": "OFF"},
        {"name": "CUSTOMERS", "rows": 240_000, "bytes": 58_720_256,
         "comment": "customer dimension", "owner": "SYSADMIN",
         "cluster_by": "", "automatic_clustering": "OFF",
         "change_tracking": "OFF", "retention_time": "1",
         "search_optimization": "OFF"},
        {"name": "EVENTS_RAW", "rows": 9_800_000, "bytes": 5_368_709_120,
         "comment": "raw event payloads (VARIANT)", "owner": "SYSADMIN",
         "cluster_by": "", "automatic_clustering": "OFF",
         "change_tracking": "OFF", "retention_time": "1",
         "search_optimization": "OFF"},
        {"name": "LEGACY_AUDIT", "rows": 3_100_000, "bytes": 1_073_741_824,
         "comment": "append-only audit trail", "owner": "SYSADMIN",
         "cluster_by": "", "automatic_clustering": "OFF",
         "change_tracking": "OFF", "retention_time": "1",
         "search_optimization": "OFF"},
    ],
    "ANALYTICS": [],
}

_SHOW_VIEWS = {
    "SALES": [],
    "ANALYTICS": [
        {"name": "ORDER_SUMMARY_VW", "text": _ORDER_SUMMARY_SQL,
         "is_secure": "false", "comment": "translatable: IFF and :: rewrite"},
        {"name": "TOP_CUSTOMERS_VW", "text": _TOP_CUSTOMERS_SQL,
         "is_secure": "false", "comment": "QUALIFY has no exact Spark rewrite"},
        {"name": "CUSTOMER_360_VW", "text": _CUSTOMER_360_SQL,
         "is_secure": "true", "comment": "secure view"},
    ],
}


def _col(table, pos, name, data_type, *, precision=None, scale=None,
         char_length=None, nullable="YES", comment=None):
    return {"TABLE_SCHEMA": None, "TABLE_NAME": table, "ORDINAL_POSITION": pos,
            "COLUMN_NAME": name, "DATA_TYPE": data_type,
            "IS_NULLABLE": nullable, "NUMERIC_PRECISION": precision,
            "NUMERIC_SCALE": scale, "CHARACTER_MAXIMUM_LENGTH": char_length,
            "DATETIME_PRECISION": 9 if data_type.startswith("TIMESTAMP") else None,
            "COMMENT": comment}


_COLUMNS = {
    "SALES": [
        _col("ORDERS", 1, "ORDER_ID", "NUMBER", precision=38, scale=0, nullable="NO"),
        _col("ORDERS", 2, "CUSTOMER_ID", "NUMBER", precision=38, scale=0),
        _col("ORDERS", 3, "AMOUNT", "NUMBER", precision=18, scale=2),
        _col("ORDERS", 4, "STATUS", "TEXT", char_length=20),
        _col("ORDERS", 5, "CREATED_AT", "TIMESTAMP_NTZ"),
        _col("ORDERS", 6, "IS_PRIORITY", "BOOLEAN"),
        _col("ORDERS", 7, "ORDER_DATE", "DATE"),
        _col("CUSTOMERS", 1, "CUSTOMER_ID", "NUMBER", precision=38, scale=0, nullable="NO"),
        _col("CUSTOMERS", 2, "FULL_NAME", "TEXT", char_length=200),
        _col("CUSTOMERS", 3, "EMAIL", "TEXT", char_length=320,
             comment="masked by MASK_EMAIL in Snowflake"),
        _col("CUSTOMERS", 4, "SEGMENT", "TEXT", char_length=40),
        _col("EVENTS_RAW", 1, "EVENT_ID", "NUMBER", precision=38, scale=0),
        _col("EVENTS_RAW", 2, "PAYLOAD", "VARIANT"),
        _col("EVENTS_RAW", 3, "INGESTED_AT", "TIMESTAMP_NTZ"),
        _col("LEGACY_AUDIT", 1, "AUDIT_ID", "NUMBER", precision=38, scale=0),
        _col("LEGACY_AUDIT", 2, "NOTE", "TEXT", char_length=4000),
    ],
    "ANALYTICS": [
        _col("ORDER_SUMMARY_VW", 1, "CUSTOMER", "TEXT", char_length=39),
        _col("ORDER_SUMMARY_VW", 2, "BUCKET", "TEXT", char_length=5),
        _col("ORDER_SUMMARY_VW", 3, "TOTAL_AMOUNT", "NUMBER", precision=30, scale=2),
        _col("ORDER_SUMMARY_VW", 4, "ORDER_COUNT", "NUMBER", precision=18, scale=0),
        _col("TOP_CUSTOMERS_VW", 1, "CUSTOMER_ID", "NUMBER", precision=38, scale=0),
        _col("TOP_CUSTOMERS_VW", 2, "TOTAL", "NUMBER", precision=30, scale=2),
        _col("CUSTOMER_360_VW", 1, "CUSTOMER_ID", "NUMBER", precision=38, scale=0),
        _col("CUSTOMER_360_VW", 2, "FULL_NAME", "TEXT", char_length=200),
        _col("CUSTOMER_360_VW", 3, "ORDERS", "NUMBER", precision=18, scale=0),
    ],
}

_DEPENDENCY_ROWS = [
    {"REFERENCING": "SNOWDEMO.ANALYTICS.ORDER_SUMMARY_VW",
     "REFERENCED": "SNOWDEMO.SALES.ORDERS",
     "REFERENCING_TYPE": "VIEW", "REFERENCED_TYPE": "TABLE"},
    {"REFERENCING": "SNOWDEMO.ANALYTICS.TOP_CUSTOMERS_VW",
     "REFERENCED": "SNOWDEMO.SALES.ORDERS",
     "REFERENCING_TYPE": "VIEW", "REFERENCED_TYPE": "TABLE"},
    {"REFERENCING": "SNOWDEMO.ANALYTICS.CUSTOMER_360_VW",
     "REFERENCED": "SNOWDEMO.SALES.CUSTOMERS",
     "REFERENCING_TYPE": "VIEW", "REFERENCED_TYPE": "TABLE"},
    {"REFERENCING": "SNOWDEMO.ANALYTICS.CUSTOMER_360_VW",
     "REFERENCED": "SNOWDEMO.SALES.ORDERS",
     "REFERENCING_TYPE": "VIEW", "REFERENCED_TYPE": "TABLE"},
]

_WAREHOUSES = [
    {"name": "WH_ETL", "size": "Medium", "state": "SUSPENDED", "type": "STANDARD",
     "min_cluster_count": 1, "max_cluster_count": 3, "auto_suspend": 300,
     "auto_resume": "true", "owner": "SYSADMIN", "comment": "nightly loads"},
    {"name": "WH_BI", "size": "X-Small", "state": "SUSPENDED", "type": "STANDARD",
     "min_cluster_count": 1, "max_cluster_count": 1, "auto_suspend": 60,
     "auto_resume": "true", "owner": "SYSADMIN", "comment": "dashboards"},
]

_METERING = [
    {"WAREHOUSE_NAME": "WH_ETL", "CREDITS": 412.5, "DAYS": 30},
    {"WAREHOUSE_NAME": "WH_BI", "CREDITS": 36.2, "DAYS": 30},
]

_POLICY_REFS = [
    {"REF_DATABASE_NAME": "SNOWDEMO", "REF_SCHEMA_NAME": "SALES",
     "REF_ENTITY_NAME": "CUSTOMERS", "REF_COLUMN_NAME": "EMAIL",
     "POLICY_KIND": "MASKING_POLICY", "POLICY_NAME": "MASK_EMAIL"},
]

_GRANTS = [
    {"NAME": "ORDERS", "TABLE_SCHEMA": "SALES", "DATABASE_NAME": "SNOWDEMO",
     "GRANTED_ON": "TABLE",
     "PRIVILEGE": "SELECT", "GRANTEE_NAME": "ANALYST_ROLE", "GRANTS": 1},
    {"NAME": "CUSTOMERS", "TABLE_SCHEMA": "SALES", "DATABASE_NAME": "SNOWDEMO",
     "GRANTED_ON": "TABLE",
     "PRIVILEGE": "SELECT", "GRANTEE_NAME": "ANALYST_ROLE", "GRANTS": 1},
    # Grants the old three-class read never saw: on a schema, and on a
    # warehouse (which has no database at all).
    {"NAME": "SALES", "TABLE_SCHEMA": None, "DATABASE_NAME": "SNOWDEMO",
     "GRANTED_ON": "SCHEMA",
     "PRIVILEGE": "USAGE", "GRANTEE_NAME": "ANALYST_ROLE", "GRANTS": 1},
    {"NAME": "ANALYTICS_WH", "TABLE_SCHEMA": None, "DATABASE_NAME": None,
     "GRANTED_ON": "WAREHOUSE",
     "PRIVILEGE": "USAGE", "GRANTEE_NAME": "ANALYST_ROLE", "GRANTS": 1},
]

# One tag attachment, so the demo's SECURITY.md teaches that a classification
# does not travel: PII on the e-mail column of a migrated table.
_TAG_REFS = [
    {"TAG_DATABASE": "SNOWDEMO", "TAG_SCHEMA": "SALES", "TAG_NAME": "PII",
     "TAG_VALUE": "EMAIL", "OBJECT_DATABASE": "SNOWDEMO",
     "OBJECT_SCHEMA": "SALES", "OBJECT_NAME": "CUSTOMERS",
     "COLUMN_NAME": "EMAIL", "DOMAIN": "COLUMN", "LEVEL": "COLUMN"},
    # A TABLE-level tag comes back once per column of the table, which is one
    # finding and not four -- the shape that fooled the first reading of a
    # real estate.
    *[{"TAG_DATABASE": "SNOWDEMO", "TAG_SCHEMA": "SALES",
       "TAG_NAME": "SENSITIVITY", "TAG_VALUE": "RESTRICTED",
       "OBJECT_DATABASE": "SNOWDEMO", "OBJECT_SCHEMA": "SALES",
       "OBJECT_NAME": "CUSTOMERS", "COLUMN_NAME": col, "DOMAIN": "TABLE",
       "LEVEL": "TABLE"}
      for col in ("CUSTOMER_ID", "EMAIL", "SIGNUP_TS")],
]

_CLUSTERING_HISTORY = [
    {"DATABASE_NAME": "SNOWDEMO", "SCHEMA_NAME": "SALES",
     "TABLE_NAME": "ORDERS", "EVENTS": 14, "CREDITS": 3.7,
     "BYTES": 9_663_676_416, "ROWS_RECLUSTERED": 41_000_000},
]

_DML_HISTORY = [
    {"DATABASE_NAME": "SNOWDEMO", "SCHEMA_NAME": "SALES",
     "TABLE_NAME": "ORDERS", "ROWS_ADDED": 2_400_000,
     "ROWS_REMOVED": 780_000, "ROWS_UPDATED": 420_000, "WINDOWS": 30},
]

_PROCEDURES = [
    {"PROCEDURE_NAME": "REFRESH_ORDERS", "PROCEDURE_SCHEMA": "SALES",
     "PROCEDURE_LANGUAGE": "SQL", "ARGUMENT_SIGNATURE": "()",
     "PROCEDURE_OWNER": "SYSADMIN"},
]

_FUNCTIONS = [
    {"FUNCTION_NAME": "CLEAN_EMAIL", "FUNCTION_SCHEMA": "SALES",
     "FUNCTION_LANGUAGE": "JAVASCRIPT", "ARGUMENT_SIGNATURE": "(S VARCHAR)",
     "FUNCTION_OWNER": "SYSADMIN"},
]

_TASKS = [
    {"name": "TASK_LOAD_ORDERS", "schema_name": "SALES", "state": "started"},
]

_STREAMS = [
    {"name": "ORDERS_STREAM", "schema_name": "SALES", "mode": "DEFAULT"},
]

_TAGS = [
    {"name": "PII", "database_name": "SNOWDEMO", "schema_name": "SALES",
     "kind": "TAG"},
]


def _names_object(flat: str, row: dict, prefix: str = "") -> bool:
    """Is this per-object statement asking about the object in `row`?

    The statement carries the object as a quoted three-part literal, so the
    name is matched inside it rather than anywhere in the SQL.
    """
    name = str(row.get(f"{prefix}ENTITY_NAME")
               or row.get("OBJECT_NAME") or "")
    schema = str(row.get(f"{prefix}SCHEMA_NAME")
                 or row.get("OBJECT_SCHEMA") or "")
    return f'"{schema}"."{name}"'.lower() in flat


def _schema_in(flat: str) -> str:
    """The schema name out of `... in schema "SNOWDEMO"."<schema>" ...`."""
    for schema in _SCHEMAS:
        if f'"{schema.lower()}"' in flat or f".{schema.lower()}" in flat:
            return schema
    raise ValueError(f"emulation: no known schema in: {flat[:160]}")


def demo_run_sql(sql: str, params: dict | None = None) -> list[dict]:
    """Answer one read-only query against the emulated estate.

    Raises on anything unrecognised: a fake that improvises answers is worse
    than no fake, because the pipeline would then report an estate nobody
    defined.
    """
    flat = " ".join(sql.split()).lower()
    p = params or {}

    if "current_user()" in flat:
        return [dict(_SESSION)]
    if flat.startswith("show databases"):
        return [{"name": DEMO_DB}]
    if "show schemas in database" in flat:
        return [{"name": s} for s in _SCHEMAS]
    if "show tables in schema" in flat:
        return [dict(r) for r in _SHOW_TABLES[_schema_in(flat)]]
    if "show views in schema" in flat:
        return [dict(r) for r in _SHOW_VIEWS[_schema_in(flat)]]
    if "information_schema.columns" in flat:
        schema = p.get("schema")
        return [{**c, "TABLE_SCHEMA": schema}
                for c in _COLUMNS.get(schema, [])]
    if "get_ddl" in flat:
        name = str(p.get("f", "")).replace('"', "").rsplit(".", 1)[-1]
        return [{"D": _VIEW_SQL[name]}]

    # --- census -----------------------------------------------------------
    if "information_schema.procedures" in flat:
        return [dict(r) for r in _PROCEDURES]
    if "information_schema.functions" in flat:
        return [dict(r) for r in _FUNCTIONS]
    if ("information_schema.sequences" in flat
            or "information_schema.stages" in flat
            or "information_schema.file_formats" in flat
            or "information_schema.pipes" in flat):
        return []
    if "show tasks in database" in flat:
        return [dict(r) for r in _TASKS]
    if "show streams in database" in flat:
        return [dict(r) for r in _STREAMS]
    if ("show materialized views in database" in flat
            or "show dynamic tables in database" in flat):
        return []
    # One alert, so CENSUS.md teaches the lesson it can now teach: an alert
    # that watched a migrated table stops firing at cutover, unannounced.
    if "show alerts in database" in flat:
        return [{"name": "LOW_STOCK_ALERT", "database_name": DEMO_DB,
                 "schema_name": "SALES", "state": "started",
                 "condition": "select 1 from SNOWDEMO.SALES.ORDERS"}]
    if ("show secrets in database" in flat
            or "show network rules in database" in flat
            or "show streamlits in database" in flat
            or "show notebooks in database" in flat
            or "show services in database" in flat):
        return []
    # Constraints: one primary key, so DDL_PLAN.md's R20 names a real one.
    if "show primary keys in database" in flat:
        return [{"database_name": DEMO_DB, "schema_name": "SALES",
                 "table_name": "ORDERS", "column_name": "ORDER_ID",
                 "key_sequence": 1, "constraint_name": "ORDERS_PK",
                 "rely": "false"}]
    if ("show unique keys in database" in flat
            or "show imported keys in database" in flat):
        return []
    # Account-scoped reads, issued once per run. One outbound share: a live
    # contract with a consumer account, which finds out at cutover.
    if flat.startswith("show shares"):
        return [{"name": "SNOWDEMO_SALES_SHARE", "kind": "OUTBOUND",
                 "database_name": DEMO_DB, "to": "PARTNER_ACCOUNT",
                 "owner": "ACCOUNTADMIN"}]
    if (flat.startswith("show roles") or flat.startswith("show network policies")
            or flat.startswith("show applications")
            or flat.startswith("show compute pools")
            or flat.startswith("show replication groups")):
        return []

    # --- lineage / compute / security / maintenance ------------------------
    if "object_dependencies" in flat:
        return [dict(r) for r in _DEPENDENCY_ROWS]
    if flat.startswith("show warehouses"):
        return [dict(r) for r in _WAREHOUSES]
    if "warehouse_metering_history" in flat:
        return [dict(r) for r in _METERING]
    if "show masking policies" in flat:
        return [{"name": "MASK_EMAIL", "database_name": DEMO_DB,
                 "schema_name": "SALES", "kind": "MASKING_POLICY"}]
    if "show row access policies" in flat:
        return []
    # Enumerated, and empty: the report may say so because it asked.
    if "show aggregation policies" in flat or "show projection policies" in flat:
        return []
    if "show tags" in flat:
        return [dict(r) for r in _TAGS]
    # The per-object reads: <db>.INFORMATION_SCHEMA.POLICY_REFERENCES and
    # TAG_REFERENCES_ALL_COLUMNS take one object and answer for that object
    # only. They are what the security stage trusts, because unlike the
    # ACCOUNT_USAGE views they carry no ~2 h lag.
    if "tag_references_all_columns" in flat:
        return [dict(r) for r in _TAG_REFS if _names_object(flat, r)]
    if "information_schema.policy_references" in flat:
        return [dict(r) for r in _POLICY_REFS
                if _names_object(flat, r, prefix="REF_")]
    if "tag_references" in flat:
        return [dict(r) for r in _TAG_REFS]
    if "policy_references" in flat:
        return [dict(r) for r in _POLICY_REFS]
    if "grants_to_roles" in flat:
        return [dict(r) for r in _GRANTS]
    if "show parameters like 'max_data_extension_time_in_days'" in flat:
        return [{"value": "14", "level": "ACCOUNT"}]
    if "show parameters like 'data_retention_time_in_days'" in flat:
        # Account, database and schema all agree on 1 day; ORDERS' effective 7
        # then reads as a table-level override, which is the signal.
        level = ("ACCOUNT" if "in account" in flat else "")
        return [{"value": "1", "level": level}]
    if "automatic_clustering_history" in flat:
        return [dict(r) for r in _CLUSTERING_HISTORY]
    if "table_dml_history" in flat:
        return [dict(r) for r in _DML_HISTORY]

    # --- smoke -------------------------------------------------------------
    if "information_schema.tables" in flat and flat.startswith("select count"):
        return [{"N": len(_SHOW_TABLES["SALES"])}]

    raise ValueError(f"the emulated Snowflake has no answer for: {flat[:200]}")


# ===========================================================================
# ENTERPRISE estate: what a trial account cannot hold.
# ===========================================================================
#
# One database, SNOWENT, in three schemas. Every object teaches the lesson a
# trial could never run:
#
#   SALES.ORDERS            clustered, change-tracked, SEARCH OPTIMIZATION on,
#                           a ROW ACCESS policy on REGION, in an outbound share
#   SALES.CUSTOMERS         MASKING policy + a PII tag on EMAIL, a table-level
#                           SENSITIVITY tag, in the same outbound share
#   SALES.CUSTOMER_360_SV   SECURE view (shared -- Snowflake shares only secure
#                           views)
#   SALES.ORDER_TOTALS_MV   materialized view with a clustering key
#   LAKE.EXT_CLICKS         external table, PARQUET, on an S3 stage
#   LAKE.EXT_PARTNER_FEED   external table, CSV, on the same stage
#   LAKE.ICE_EVENTS         Iceberg table, Snowflake-managed, external volume
#   LAKE.ICE_GLUE_ORDERS    Iceberg table, externally managed (Glue catalog
#                           integration)
#   OPS.HYB_SESSIONS        hybrid (Unistore) table, with its required PK
#   OPS.APP_EVENTS          event table, Snowflake's fixed event schema
#   OPS.SCORE_LEAD          external function through an API integration
#   OPS.LEAD_SCORER         Snowpark Container Service on compute pool
#                           ENT_SCORING_POOL
#   account                 a native app, an outbound share to two consumer
#                           accounts, an inbound share, a replication group
#                           and a failover group
#
# Shape sources, stated per statement so nobody mistakes one for the other:
#   LIVE    the field NAMES a live trial returned on 2026-09-29, pinned by
#           tests/fixtures/snowflake_live_field_names.json (SHOW TABLES,
#           SHOW MATERIALIZED VIEWS, SHOW STREAMS)
#   DOC     Snowflake's documented output for a statement a trial cannot
#           answer (SHOW EXTERNAL TABLES, SHOW ICEBERG TABLES, DESCRIBE
#           EXTERNAL VOLUME / CATALOG INTEGRATION / SHARE, SHOW SHARES,
#           SHOW REPLICATION GROUPS, SHOW SERVICES, SHOW COMPUTE POOLS,
#           SHOW APPLICATIONS). Not live-verified: re-capture on an
#           enterprise account before trusting a field this estate invents.
# Value types are the Python connector's: SHOW numbers (rows, bytes) as int,
# flags and retention as text.
#
# Every identifier is a fake: EMUORG / EMU_* accounts, emu-* buckets, the
# all-zero AWS account. Nothing here names a real system.

import re as _re  # noqa: E402

ENTERPRISE_DB = "SNOWENT"

_ENT_SESSION = {"U": "ENT_MIGRATOR", "A": "EMU_ENT", "R": "EMULATED_REGION",
                "ROLE": "MIGRATION_READER", "WH": "WH_ENT_ETL", "V": "emulated",
                "SECONDARY_ROLES": '{"roles":"","value":""}'}

_ENT_SCHEMAS = ("SALES", "LAKE", "OPS")

_ENT_CREATED = "2026-09-01 09:00:00.000000-07:00"


def _ent_table(schema, name, *, kind="TABLE", comment="", cluster_by="",
               rows=0, bytes_=0, retention="1", automatic_clustering="OFF",
               change_tracking="OFF", search_optimization="OFF",
               search_optimization_progress=None,
               search_optimization_bytes=None, is_external="N",
               is_event="N", is_hybrid="N", is_iceberg="N", is_dynamic="N"):
    """One SHOW TABLES row in the LIVE field order (fixture-pinned)."""
    return {
        "created_on": _ENT_CREATED, "name": name,
        "database_name": ENTERPRISE_DB, "schema_name": schema, "kind": kind,
        "comment": comment, "cluster_by": cluster_by, "rows": rows,
        "bytes": bytes_, "owner": "SYSADMIN", "retention_time": retention,
        "automatic_clustering": automatic_clustering,
        "change_tracking": change_tracking,
        "search_optimization": search_optimization,
        "search_optimization_progress": search_optimization_progress,
        "search_optimization_bytes": search_optimization_bytes,
        "is_external": is_external, "enable_schema_evolution": "N",
        "owner_role_type": "ROLE", "is_event": is_event,
        "is_hybrid": is_hybrid, "is_iceberg": is_iceberg,
        "is_dynamic": is_dynamic, "is_immutable": "N",
        "is_interactive": "N", "row_timestamp": "OFF", "error_logging": "OFF",
    }


_ENT_SHOW_TABLES = {
    "SALES": [
        _ent_table("SALES", "CUSTOMERS", comment="customer dimension",
                   rows=240_000, bytes_=58_720_256),
        _ent_table("SALES", "ORDERS", comment="order fact",
                   cluster_by="LINEAR(ORDER_DATE)", rows=1_250_000,
                   bytes_=402_653_184, automatic_clustering="ON",
                   change_tracking="ON", search_optimization="ON",
                   search_optimization_progress="100",
                   search_optimization_bytes=73_400_320),
    ],
    # External tables are listed by SHOW TABLES with is_external = Y (the
    # planner's assumption, and Snowflake's documented flag); SHOW EXTERNAL
    # TABLES below carries their location. `rows` is 0: Snowflake maintains
    # no row count for an external table.
    "LAKE": [
        _ent_table("LAKE", "EXT_CLICKS", comment="clickstream, parquet on S3",
                   is_external="Y"),
        _ent_table("LAKE", "EXT_PARTNER_FEED", comment="partner feed, csv on S3",
                   is_external="Y"),
        _ent_table("LAKE", "ICE_EVENTS", comment="Snowflake-managed Iceberg",
                   rows=5_400_000, bytes_=734_003_200, is_iceberg="Y"),
        _ent_table("LAKE", "ICE_GLUE_ORDERS",
                   comment="Iceberg, externally managed in Glue",
                   rows=880_000, bytes_=96_468_992, is_iceberg="Y"),
    ],
    "OPS": [
        _ent_table("OPS", "APP_EVENTS", comment="account event table",
                   rows=12_000_000, bytes_=2_147_483_648, is_event="Y"),
        _ent_table("OPS", "HYB_SESSIONS", comment="OLTP session store",
                   rows=64_000, bytes_=16_777_216, is_hybrid="Y"),
    ],
}

_ENT_SV_SQL = (
    "CREATE OR REPLACE SECURE VIEW SNOWENT.SALES.CUSTOMER_360_SV AS\n"
    "select c.CUSTOMER_ID, c.FULL_NAME, count(o.ORDER_ID) as ORDERS,\n"
    "       sum(o.AMOUNT) as TOTAL\n"
    "from SNOWENT.SALES.CUSTOMERS c\n"
    "join SNOWENT.SALES.ORDERS o on o.CUSTOMER_ID = c.CUSTOMER_ID\n"
    "group by c.CUSTOMER_ID, c.FULL_NAME")

_ENT_MV_SQL = (
    "CREATE OR REPLACE MATERIALIZED VIEW SNOWENT.SALES.ORDER_TOTALS_MV\n"
    "  CLUSTER BY (CUSTOMER_ID) AS\n"
    "select CUSTOMER_ID, sum(AMOUNT) as TOTAL\n"
    "from SNOWENT.SALES.ORDERS group by CUSTOMER_ID")

_ENT_VIEW_SQL = {"CUSTOMER_360_SV": _ENT_SV_SQL, "ORDER_TOTALS_MV": _ENT_MV_SQL}


def _ent_view(name, text, *, secure, materialized, comment=""):
    # DOC: SHOW VIEWS field set. A secure view's `text` is shown to its owner
    # role only; this estate models a migration role that can read it, which
    # `--secure-views as-view` needs.
    return {"created_on": _ENT_CREATED, "name": name, "reserved": "",
            "database_name": ENTERPRISE_DB, "schema_name": "SALES",
            "owner": "SYSADMIN", "comment": comment, "text": text,
            "is_secure": "true" if secure else "false",
            "is_materialized": "true" if materialized else "false",
            "owner_role_type": "ROLE", "change_tracking": "OFF"}


_ENT_SHOW_VIEWS = {
    "SALES": [
        _ent_view("CUSTOMER_360_SV", _ENT_SV_SQL, secure=True,
                  materialized=False, comment="shared with partners"),
        _ent_view("ORDER_TOTALS_MV", _ENT_MV_SQL, secure=False,
                  materialized=True, comment="clustered MV"),
    ],
    "LAKE": [],
    "OPS": [],
}


def _ent_col(table, pos, name, data_type, *, precision=None, scale=None,
             char_length=None, nullable="YES", comment=None, dt_precision=None):
    """One INFORMATION_SCHEMA.COLUMNS row, every column the inventory reads."""
    if dt_precision is None and (data_type.startswith("TIMESTAMP")
                                 or data_type == "TIME"):
        dt_precision = 9
    return {"TABLE_SCHEMA": None, "TABLE_NAME": table, "ORDINAL_POSITION": pos,
            "COLUMN_NAME": name, "DATA_TYPE": data_type,
            "IS_NULLABLE": nullable, "NUMERIC_PRECISION": precision,
            "NUMERIC_SCALE": scale, "CHARACTER_MAXIMUM_LENGTH": char_length,
            "DATETIME_PRECISION": dt_precision, "COMMENT": comment,
            "COLLATION_NAME": None, "COLUMN_DEFAULT": None,
            "IDENTITY_START": None, "IDENTITY_INCREMENT": None}


_TEXT = 16_777_216

_ENT_COLUMNS = {
    "SALES": [
        _ent_col("CUSTOMERS", 1, "CUSTOMER_ID", "NUMBER", precision=38, scale=0, nullable="NO"),
        _ent_col("CUSTOMERS", 2, "FULL_NAME", "TEXT", char_length=200),
        _ent_col("CUSTOMERS", 3, "EMAIL", "TEXT", char_length=320),
        _ent_col("CUSTOMERS", 4, "SEGMENT", "TEXT", char_length=40),
        _ent_col("CUSTOMERS", 5, "SIGNUP_DATE", "DATE"),
        _ent_col("ORDERS", 1, "ORDER_ID", "NUMBER", precision=38, scale=0, nullable="NO"),
        _ent_col("ORDERS", 2, "CUSTOMER_ID", "NUMBER", precision=38, scale=0),
        _ent_col("ORDERS", 3, "REGION", "TEXT", char_length=16),
        _ent_col("ORDERS", 4, "AMOUNT", "NUMBER", precision=18, scale=2),
        _ent_col("ORDERS", 5, "ORDER_DATE", "DATE"),
        _ent_col("ORDERS", 6, "UPDATED_AT", "TIMESTAMP_LTZ"),
        _ent_col("CUSTOMER_360_SV", 1, "CUSTOMER_ID", "NUMBER", precision=38, scale=0),
        _ent_col("CUSTOMER_360_SV", 2, "FULL_NAME", "TEXT", char_length=200),
        _ent_col("CUSTOMER_360_SV", 3, "ORDERS", "NUMBER", precision=18, scale=0),
        _ent_col("CUSTOMER_360_SV", 4, "TOTAL", "NUMBER", precision=38, scale=2),
        _ent_col("ORDER_TOTALS_MV", 1, "CUSTOMER_ID", "NUMBER", precision=38, scale=0),
        _ent_col("ORDER_TOTALS_MV", 2, "TOTAL", "NUMBER", precision=38, scale=2),
    ],
    "LAKE": [
        # An external table's columns: VALUE (the whole record as VARIANT)
        # and the virtual columns defined as expressions over it.
        _ent_col("EXT_CLICKS", 1, "VALUE", "VARIANT", nullable="NO"),
        _ent_col("EXT_CLICKS", 2, "CLICK_ID", "NUMBER", precision=38, scale=0),
        _ent_col("EXT_CLICKS", 3, "URL", "TEXT", char_length=_TEXT),
        _ent_col("EXT_CLICKS", 4, "CLICK_DATE", "DATE"),
        _ent_col("EXT_PARTNER_FEED", 1, "VALUE", "VARIANT", nullable="NO"),
        _ent_col("EXT_PARTNER_FEED", 2, "PARTNER_ID", "TEXT", char_length=_TEXT),
        _ent_col("EXT_PARTNER_FEED", 3, "AMOUNT", "NUMBER", precision=18, scale=2),
        _ent_col("ICE_EVENTS", 1, "EVENT_ID", "NUMBER", precision=19, scale=0),
        _ent_col("ICE_EVENTS", 2, "EVENT_TYPE", "TEXT", char_length=_TEXT),
        _ent_col("ICE_EVENTS", 3, "EVENT_TS", "TIMESTAMP_NTZ", dt_precision=6),
        _ent_col("ICE_GLUE_ORDERS", 1, "ORDER_ID", "NUMBER", precision=19, scale=0),
        _ent_col("ICE_GLUE_ORDERS", 2, "AMOUNT", "NUMBER", precision=18, scale=2),
    ],
    "OPS": [
        # Snowflake's fixed event-table schema, in its documented order.
        *[_ent_col("APP_EVENTS", i, name, dtype,
                   char_length=_TEXT if dtype == "TEXT" else None)
          for i, (name, dtype) in enumerate((
              ("TIMESTAMP", "TIMESTAMP_NTZ"), ("START_TIMESTAMP", "TIMESTAMP_NTZ"),
              ("OBSERVED_TIMESTAMP", "TIMESTAMP_NTZ"), ("TRACE", "OBJECT"),
              ("RESOURCE", "OBJECT"), ("RESOURCE_ATTRIBUTES", "OBJECT"),
              ("SCOPE", "OBJECT"), ("SCOPE_ATTRIBUTES", "OBJECT"),
              ("RECORD_TYPE", "TEXT"), ("RECORD", "OBJECT"),
              ("RECORD_ATTRIBUTES", "OBJECT"), ("VALUE", "VARIANT"),
              ("EXEMPLARS", "ARRAY")), 1)],
        _ent_col("HYB_SESSIONS", 1, "SESSION_ID", "TEXT", char_length=36, nullable="NO"),
        _ent_col("HYB_SESSIONS", 2, "USER_ID", "NUMBER", precision=38, scale=0),
        _ent_col("HYB_SESSIONS", 3, "STARTED_AT", "TIMESTAMP_LTZ"),
    ],
}

# DOC: SHOW EXTERNAL TABLES IN SCHEMA.
_ENT_EXTERNAL_TABLES = [
    {"created_on": _ENT_CREATED, "name": "EXT_CLICKS",
     "database_name": ENTERPRISE_DB, "schema_name": "LAKE", "invalid": "false",
     "invalid_reason": None, "owner": "SYSADMIN",
     "comment": "clickstream, parquet on S3",
     "stage": "@SNOWENT.LAKE.S3_LAKE_STAGE",
     "location": "s3://emu-ent-lake/raw/clicks/",
     "file_format_name": "SNOWENT.LAKE.FF_PARQUET",
     "file_format_type": "PARQUET", "cloud": "AWS", "region": "us-east-1",
     "notification_channel": "arn:aws:sqs:us-east-1:000000000000:emulated",
     "last_refreshed_on": _ENT_CREATED, "table_format": None,
     "last_refresh_details": "", "owner_role_type": "ROLE"},
    {"created_on": _ENT_CREATED, "name": "EXT_PARTNER_FEED",
     "database_name": ENTERPRISE_DB, "schema_name": "LAKE", "invalid": "false",
     "invalid_reason": None, "owner": "SYSADMIN",
     "comment": "partner feed, csv on S3",
     "stage": "@SNOWENT.LAKE.S3_LAKE_STAGE",
     "location": "s3://emu-ent-lake/raw/partner/",
     "file_format_name": "", "file_format_type": "CSV", "cloud": "AWS",
     "region": "us-east-1", "notification_channel": None,
     "last_refreshed_on": _ENT_CREATED, "table_format": None,
     "last_refresh_details": "", "owner_role_type": "ROLE"},
]

# DOC: SHOW ICEBERG TABLES IN SCHEMA.
_ENT_ICEBERG_TABLES = [
    {"created_on": _ENT_CREATED, "name": "ICE_EVENTS",
     "database_name": ENTERPRISE_DB, "schema_name": "LAKE",
     "owner": "SYSADMIN", "catalog_name": "SNOWFLAKE",
     "iceberg_table_type": "MANAGED", "catalog_table_name": "",
     "catalog_namespace": "", "external_volume_name": "EV_ENT_LAKE",
     "base_location": "ice_events/", "invalid": "false",
     "invalid_reason": None, "owner_role_type": "ROLE",
     "last_polled_at": None, "auto_refresh_status": None},
    {"created_on": _ENT_CREATED, "name": "ICE_GLUE_ORDERS",
     "database_name": ENTERPRISE_DB, "schema_name": "LAKE",
     "owner": "SYSADMIN", "catalog_name": "GLUE_ENT_CATALOG",
     "iceberg_table_type": "UNMANAGED", "catalog_table_name": "orders",
     "catalog_namespace": "sales_db", "external_volume_name": "EV_ENT_LAKE",
     "base_location": "", "invalid": "false", "invalid_reason": None,
     "owner_role_type": "ROLE", "last_polled_at": _ENT_CREATED,
     "auto_refresh_status": '{"executionState":"RUNNING"}'},
]

# DOC: DESCRIBE EXTERNAL VOLUME -- one row per property; each storage
# location is a JSON document in property_value, ACTIVE names the one in use.
_ENT_EXTERNAL_VOLUMES = {
    "EV_ENT_LAKE": [
        {"parent_property": "", "property": "ALLOW_WRITES",
         "property_type": "Boolean", "property_value": "true",
         "property_default": "true"},
        {"parent_property": "STORAGE_LOCATIONS",
         "property": "STORAGE_LOCATION_1", "property_type": "String",
         "property_value": (
             '{"NAME":"emu-s3-iceberg","STORAGE_PROVIDER":"S3",'
             '"STORAGE_BASE_URL":"s3://emu-ent-iceberg/warehouse/",'
             '"STORAGE_ALLOWED_LOCATIONS":["s3://emu-ent-iceberg/warehouse/*"],'
             '"STORAGE_REGION":"us-east-1","PRIVILEGES_VERIFIED":true,'
             '"STORAGE_AWS_ROLE_ARN":"arn:aws:iam::000000000000:role/emulated",'
             '"STORAGE_AWS_EXTERNAL_ID":"EMULATED_EXTERNAL_ID",'
             '"ENCRYPTION_TYPE":"NONE"}'),
         "property_default": ""},
        {"parent_property": "STORAGE_LOCATIONS", "property": "ACTIVE",
         "property_type": "String", "property_value": "emu-s3-iceberg",
         "property_default": ""},
    ],
}

_ENT_VOLUME_BASE_URL = {"EV_ENT_LAKE": "s3://emu-ent-iceberg/warehouse/"}

# DOC: DESCRIBE CATALOG INTEGRATION.
_ENT_CATALOG_INTEGRATIONS = {
    "GLUE_ENT_CATALOG": [
        {"property": "ENABLED", "property_type": "Boolean",
         "property_value": "true", "property_default": "false"},
        {"property": "CATALOG_SOURCE", "property_type": "String",
         "property_value": "GLUE", "property_default": ""},
        {"property": "TABLE_FORMAT", "property_type": "String",
         "property_value": "ICEBERG", "property_default": ""},
        {"property": "CATALOG_NAMESPACE", "property_type": "String",
         "property_value": "sales_db", "property_default": ""},
        {"property": "GLUE_AWS_ROLE_ARN", "property_type": "String",
         "property_value": "arn:aws:iam::000000000000:role/emulated",
         "property_default": ""},
        {"property": "GLUE_CATALOG_ID", "property_type": "String",
         "property_value": "000000000000", "property_default": ""},
        {"property": "GLUE_REGION", "property_type": "String",
         "property_value": "us-east-1", "property_default": ""},
    ],
}

# DOC: SHOW SHARES. `to` lists consumer accounts as ORG.ACCOUNT, comma
# separated; an inbound share names its provider in owner_account.
_ENT_SHARES = [
    {"created_on": _ENT_CREATED, "kind": "OUTBOUND",
     "owner_account": "EMUORG.EMU_ENT", "name": "ENT_PARTNER_SHARE",
     "database_name": ENTERPRISE_DB,
     "to": "EMUORG.PARTNER_A, EMUORG.PARTNER_B", "owner": "ACCOUNTADMIN",
     "comment": "orders and the customer 360 for two partners",
     "listing_global_name": "", "secure_objects_only": "true"},
    {"created_on": _ENT_CREATED, "kind": "INBOUND",
     "owner_account": "EMUPROV.WEATHER", "name": "WEATHER_SHARE",
     "database_name": "WEATHER_DB", "to": "", "owner": "",
     "comment": "provider weather feed", "listing_global_name": "",
     "secure_objects_only": ""},
]

# DOC: DESCRIBE SHARE -- one row per object granted to the share.
_ENT_SHARE_OBJECTS = {
    "ENT_PARTNER_SHARE": [
        {"kind": "DATABASE", "name": ENTERPRISE_DB, "shared_on": _ENT_CREATED},
        {"kind": "SCHEMA", "name": "SNOWENT.SALES", "shared_on": _ENT_CREATED},
        {"kind": "TABLE", "name": "SNOWENT.SALES.ORDERS",
         "shared_on": _ENT_CREATED},
        {"kind": "TABLE", "name": "SNOWENT.SALES.CUSTOMERS",
         "shared_on": _ENT_CREATED},
        {"kind": "VIEW", "name": "SNOWENT.SALES.CUSTOMER_360_SV",
         "shared_on": _ENT_CREATED},
    ],
}

# DOC: SHOW REPLICATION GROUPS lists replication AND failover groups, told
# apart by `type`; SHOW FAILOVER GROUPS lists only the failover groups.
def _ent_group(name, type_, object_types):
    return {"snowflake_region": "EMULATED_REGION", "created_on": _ENT_CREATED,
            "account_name": "EMU_ENT", "name": name, "type": type_,
            "comment": "", "is_primary": "true",
            "primary": f"EMUORG.EMU_ENT.{name}", "object_types": object_types,
            "allowed_integration_types": "",
            "allowed_accounts": "EMUORG.EMU_ENT_DR",
            "organization_name": "EMUORG", "replication_schedule": "10 MINUTE",
            "secondary_state": "", "next_scheduled_refresh": "",
            "owner": "ACCOUNTADMIN",
            "is_listing_auto_fulfillment_group": "false"}


_ENT_GROUPS = [
    _ent_group("ENT_FG", "FAILOVER", "DATABASES, ROLES, WAREHOUSES"),
    _ent_group("ENT_RG", "REPLICATION", "DATABASES"),
]

# DOC: SHOW SERVICES IN DATABASE / SHOW COMPUTE POOLS / SHOW APPLICATIONS.
_ENT_SERVICES = [
    {"name": "LEAD_SCORER", "status": "RUNNING",
     "database_name": ENTERPRISE_DB, "schema_name": "OPS",
     "owner": "SYSADMIN", "compute_pool": "ENT_SCORING_POOL",
     "current_instances": 1, "target_instances": 1, "min_instances": 1,
     "max_instances": 2, "auto_resume": "true",
     "external_access_integrations": "[]", "created_on": _ENT_CREATED,
     "comment": "container that scores leads", "owner_role_type": "ROLE",
     "query_warehouse": "WH_ENT_ETL", "is_job": "false"},
]

_ENT_COMPUTE_POOLS = [
    {"name": "ENT_SCORING_POOL", "state": "ACTIVE", "min_nodes": 1,
     "max_nodes": 2, "instance_family": "CPU_X64_S", "num_services": 1,
     "num_jobs": 0, "auto_suspend_secs": 3600, "auto_resume": "true",
     "active_nodes": 1, "idle_nodes": 0, "created_on": _ENT_CREATED,
     "owner": "SYSADMIN", "comment": "", "is_exclusive": "false",
     "application": None},
]

_ENT_APPLICATIONS = [
    {"created_on": _ENT_CREATED, "name": "ENT_DQ_APP", "is_default": "N",
     "is_current": "N", "source_type": "LISTING", "source": "EMU_DQ_LISTING",
     "owner": "ACCOUNTADMIN", "comment": "third-party data-quality app",
     "version": "V1_0", "label": "", "patch": 3, "options": "",
     "retention_time": "1"},
]

# LIVE field set (fixture-pinned).
_ENT_MATERIALIZED_VIEWS = [
    {"created_on": _ENT_CREATED, "name": "ORDER_TOTALS_MV", "reserved": "",
     "database_name": ENTERPRISE_DB, "schema_name": "SALES",
     "cluster_by": "LINEAR(CUSTOMER_ID)", "rows": 180_000, "bytes": 4_194_304,
     "source_database_name": ENTERPRISE_DB, "source_schema_name": "SALES",
     "source_table_name": "ORDERS", "refreshed_on": _ENT_CREATED,
     "compacted_on": _ENT_CREATED, "owner": "SYSADMIN", "invalid": "false",
     "invalid_reason": None, "behind_by": "0s", "comment": "clustered MV",
     "text": _ENT_MV_SQL, "is_secure": "false", "automatic_clustering": "ON",
     "owner_role_type": "ROLE"},
]

_ENT_STREAMS = [
    {"created_on": _ENT_CREATED, "name": "STR_ORDERS",
     "database_name": ENTERPRISE_DB, "schema_name": "SALES",
     "owner": "SYSADMIN", "comment": "", "table_name": "SNOWENT.SALES.ORDERS",
     "source_type": "Table", "base_tables": "SNOWENT.SALES.ORDERS",
     "type": "DELTA", "stale": "false", "mode": "DEFAULT",
     "stale_after": "2026-10-15 09:00:00.000000-07:00",
     "invalid_reason": "N/A", "owner_role_type": "ROLE"},
]

_ENT_FUNCTIONS = [
    {"FUNCTION_NAME": "NORMALIZE_URL", "FUNCTION_SCHEMA": "LAKE",
     "FUNCTION_LANGUAGE": "SQL", "ARGUMENT_SIGNATURE": "(U VARCHAR)",
     "FUNCTION_OWNER": "SYSADMIN", "DATA_TYPE": "VARCHAR",
     "IS_EXTERNAL": "NO", "API_INTEGRATION": None},
    {"FUNCTION_NAME": "SCORE_LEAD", "FUNCTION_SCHEMA": "OPS",
     "FUNCTION_LANGUAGE": "EXTERNAL", "ARGUMENT_SIGNATURE": "(PAYLOAD VARIANT)",
     "FUNCTION_OWNER": "SYSADMIN", "DATA_TYPE": "VARIANT",
     "IS_EXTERNAL": "YES", "API_INTEGRATION": "ENT_SCORING_API"},
]

_ENT_STAGES = [
    {"STAGE_NAME": "S3_LAKE_STAGE", "STAGE_SCHEMA": "LAKE",
     "STAGE_TYPE": "External Named", "STAGE_URL": "s3://emu-ent-lake/raw/",
     "STAGE_REGION": "us-east-1"},
]

_ENT_FILE_FORMATS = [
    {"FILE_FORMAT_NAME": "FF_PARQUET", "FILE_FORMAT_SCHEMA": "LAKE"},
]

_ENT_DEPENDENCIES = [
    {"REFERENCING": "SNOWENT.SALES.CUSTOMER_360_SV",
     "REFERENCED": "SNOWENT.SALES.CUSTOMERS",
     "REFERENCING_TYPE": "VIEW", "REFERENCED_TYPE": "TABLE"},
    {"REFERENCING": "SNOWENT.SALES.CUSTOMER_360_SV",
     "REFERENCED": "SNOWENT.SALES.ORDERS",
     "REFERENCING_TYPE": "VIEW", "REFERENCED_TYPE": "TABLE"},
    {"REFERENCING": "SNOWENT.SALES.ORDER_TOTALS_MV",
     "REFERENCED": "SNOWENT.SALES.ORDERS",
     "REFERENCING_TYPE": "MATERIALIZED VIEW", "REFERENCED_TYPE": "TABLE"},
]

_ENT_POLICY_REFS = [
    {"REF_DATABASE_NAME": ENTERPRISE_DB, "REF_SCHEMA_NAME": "SALES",
     "REF_ENTITY_NAME": "CUSTOMERS", "REF_COLUMN_NAME": "EMAIL",
     "POLICY_KIND": "MASKING_POLICY", "POLICY_NAME": "MASK_EMAIL"},
    # A row access policy is attached to the TABLE; its argument column is
    # REF_ARG_COLUMN_NAMES, so REF_COLUMN_NAME is null.
    {"REF_DATABASE_NAME": ENTERPRISE_DB, "REF_SCHEMA_NAME": "SALES",
     "REF_ENTITY_NAME": "ORDERS", "REF_COLUMN_NAME": None,
     "REF_ARG_COLUMN_NAMES": '[ "REGION" ]',
     "POLICY_KIND": "ROW_ACCESS_POLICY", "POLICY_NAME": "RAP_REGION"},
]

_ENT_TAG_REFS = [
    {"TAG_DATABASE": ENTERPRISE_DB, "TAG_SCHEMA": "SALES", "TAG_NAME": "PII",
     "TAG_VALUE": "EMAIL", "OBJECT_DATABASE": ENTERPRISE_DB,
     "OBJECT_SCHEMA": "SALES", "OBJECT_NAME": "CUSTOMERS",
     "COLUMN_NAME": "EMAIL", "DOMAIN": "COLUMN", "LEVEL": "COLUMN"},
    # A table-level tag comes back once per column.
    *[{"TAG_DATABASE": ENTERPRISE_DB, "TAG_SCHEMA": "SALES",
       "TAG_NAME": "SENSITIVITY", "TAG_VALUE": "RESTRICTED",
       "OBJECT_DATABASE": ENTERPRISE_DB, "OBJECT_SCHEMA": "SALES",
       "OBJECT_NAME": "CUSTOMERS", "COLUMN_NAME": col, "DOMAIN": "TABLE",
       "LEVEL": "TABLE"}
      for col in ("CUSTOMER_ID", "FULL_NAME", "EMAIL", "SEGMENT", "SIGNUP_DATE")],
    *[{"TAG_DATABASE": ENTERPRISE_DB, "TAG_SCHEMA": "LAKE",
       "TAG_NAME": "COST_CENTER", "TAG_VALUE": "MARKETING",
       "OBJECT_DATABASE": ENTERPRISE_DB, "OBJECT_SCHEMA": "LAKE",
       "OBJECT_NAME": "EXT_CLICKS", "COLUMN_NAME": col, "DOMAIN": "TABLE",
       "LEVEL": "TABLE"}
      for col in ("VALUE", "CLICK_ID", "URL", "CLICK_DATE")],
]

_ENT_TAGS = [
    {"name": "PII", "database_name": ENTERPRISE_DB, "schema_name": "SALES",
     "kind": "TAG"},
    {"name": "SENSITIVITY", "database_name": ENTERPRISE_DB,
     "schema_name": "SALES", "kind": "TAG"},
    {"name": "COST_CENTER", "database_name": ENTERPRISE_DB,
     "schema_name": "LAKE", "kind": "TAG"},
]

_ENT_GRANTS = [
    {"NAME": "ORDERS", "TABLE_SCHEMA": "SALES", "DATABASE_NAME": ENTERPRISE_DB,
     "GRANTED_ON": "TABLE", "PRIVILEGE": "SELECT",
     "GRANTEE_NAME": "ANALYST_ROLE", "GRANTS": 1},
    {"NAME": "CUSTOMER_360_SV", "TABLE_SCHEMA": "SALES",
     "DATABASE_NAME": ENTERPRISE_DB, "GRANTED_ON": "VIEW",
     "PRIVILEGE": "SELECT", "GRANTEE_NAME": "PARTNER_READER", "GRANTS": 1},
    {"NAME": "EXT_CLICKS", "TABLE_SCHEMA": "LAKE",
     "DATABASE_NAME": ENTERPRISE_DB, "GRANTED_ON": "EXTERNAL_TABLE",
     "PRIVILEGE": "SELECT", "GRANTEE_NAME": "ANALYST_ROLE", "GRANTS": 1},
    {"NAME": "ENT_SCORING_API", "TABLE_SCHEMA": None, "DATABASE_NAME": None,
     "GRANTED_ON": "INTEGRATION", "PRIVILEGE": "USAGE",
     "GRANTEE_NAME": "SYSADMIN", "GRANTS": 1},
]

_ENT_PRIMARY_KEYS = [
    {"database_name": ENTERPRISE_DB, "schema_name": "SALES",
     "table_name": "ORDERS", "column_name": "ORDER_ID", "key_sequence": 1,
     "constraint_name": "ORDERS_PK", "rely": "false"},
    # A hybrid table REQUIRES a primary key, and enforces it.
    {"database_name": ENTERPRISE_DB, "schema_name": "OPS",
     "table_name": "HYB_SESSIONS", "column_name": "SESSION_ID",
     "key_sequence": 1, "constraint_name": "HYB_SESSIONS_PK", "rely": "false"},
]

_ENT_WAREHOUSES = [
    {"name": "WH_ENT_ETL", "size": "Large", "state": "SUSPENDED",
     "type": "STANDARD", "min_cluster_count": 1, "max_cluster_count": 4,
     "auto_suspend": 300, "auto_resume": "true", "owner": "SYSADMIN",
     "comment": "nightly loads"},
    {"name": "WH_ENT_BI", "size": "Small", "state": "SUSPENDED",
     "type": "STANDARD", "min_cluster_count": 1, "max_cluster_count": 2,
     "auto_suspend": 60, "auto_resume": "true", "owner": "SYSADMIN",
     "comment": "dashboards"},
]

_ENT_METERING = [
    {"WAREHOUSE_NAME": "WH_ENT_ETL", "CREDITS": 1_840.0, "DAYS": 30},
    {"WAREHOUSE_NAME": "WH_ENT_BI", "CREDITS": 212.4, "DAYS": 30},
]


def _ent_schema_in(flat: str) -> str:
    """The schema named in `... in schema "SNOWENT"."<schema>" ...`."""
    m = _re.search(r'in schema "snowent"\."([a-z_]+)"', flat)
    if m and m.group(1).upper() in _ENT_SCHEMAS:
        return m.group(1).upper()
    raise ValueError(f"the emulated enterprise Snowflake has no such schema: "
                     f"{flat[:160]}")


def _ent_quoted_name(flat: str, verb: str) -> str:
    """The object after `<verb>` -- quoted or bare -- upper-cased."""
    rest = flat.split(verb, 1)[1].strip()
    return rest.split()[0].strip('"').upper() if rest else ""


def _ent_describe_type(col: dict) -> str:
    """DESCRIBE TABLE's `type` for one fixture column, spelled as Snowflake
    spells it (live 2026-09-29: NUMBER(38,0), VARCHAR(n), TIMESTAMP_NTZ(9),
    TIME(3); plain OBJECT / VARIANT / ARRAY carry no element types)."""
    dtype = col["DATA_TYPE"]
    if dtype == "NUMBER":
        return f'NUMBER({col["NUMERIC_PRECISION"]},{col["NUMERIC_SCALE"]})'
    if dtype == "TEXT":
        return f'VARCHAR({col["CHARACTER_MAXIMUM_LENGTH"] or _TEXT})'
    if dtype.startswith("TIMESTAMP") or dtype == "TIME":
        return f'{dtype}({col["DATETIME_PRECISION"]})'
    return dtype


def _ent_describe_table(sql: str) -> list[dict]:
    """DESCRIBE TABLE over the same column fixtures INFORMATION_SCHEMA
    answers from, so the two reads cannot disagree."""
    parts = [p.strip('"').upper() for p in
             sql.split(None, 2)[2].strip().split(".")]
    schema, table = parts[-2], parts[-1]
    return [{"name": c["COLUMN_NAME"], "type": _ent_describe_type(c),
             "kind": "COLUMN", "null?": "Y" if c["IS_NULLABLE"] == "YES" else "N",
             "default": None, "comment": c["COMMENT"]}
            for c in _ENT_COLUMNS.get(schema, [])
            if c["TABLE_NAME"].upper() == table]


def enterprise_run_sql(sql: str, params: dict | None = None) -> list[dict]:
    """Answer one read-only query against the ENTERPRISE estate.

    Every statement first passes the SAME read-only guard the live connection
    applies (invariant I1), so a write is refused exactly as it would be in
    production, never answered. Anything unrecognised raises: a fake that
    improvises answers is worse than no fake.
    """
    from snowflake_source.conn import assert_read_only
    assert_read_only(sql)
    flat = " ".join(sql.split()).lower()
    p = params or {}

    if "current_user()" in flat:
        return [dict(_ENT_SESSION)]
    if flat.startswith("show databases"):
        return [{"name": ENTERPRISE_DB}]
    if "show schemas in database" in flat:
        return [{"name": s} for s in _ENT_SCHEMAS]
    if "show tables in schema" in flat:
        return [dict(r) for r in _ENT_SHOW_TABLES[_ent_schema_in(flat)]]
    if "show views in schema" in flat:
        return [dict(r) for r in _ENT_SHOW_VIEWS[_ent_schema_in(flat)]]
    if "show external tables in database" in flat:
        return [dict(r) for r in _ENT_EXTERNAL_TABLES]
    if "show iceberg tables in database" in flat:
        return [dict(r) for r in _ENT_ICEBERG_TABLES]
    if "system$get_iceberg_table_information" in flat:
        # DOC: the root metadata file of a Snowflake-managed Iceberg table.
        name = flat.split("system$get_iceberg_table_information(", 1)[1]
        name = name.split(")", 1)[0].replace("'", "").replace('"', "")
        table = name.rsplit(".", 1)[-1].upper()
        row = next(r for r in _ENT_ICEBERG_TABLES if r["name"] == table
                   and r["catalog_name"] == "SNOWFLAKE")
        base = _ENT_VOLUME_BASE_URL[row["external_volume_name"]]
        return [{"INFO": (
            '{"metadataLocation":"' + base + row["base_location"]
            + 'metadata/00003-0f0e0d0c-0000-4000-8000-000000000003.metadata.json",'
            '"status":"success"}')}]
    if "show external tables in schema" in flat:
        schema = _ent_schema_in(flat)
        return [dict(r) for r in _ENT_EXTERNAL_TABLES
                if r["schema_name"] == schema]
    if "show iceberg tables in schema" in flat:
        schema = _ent_schema_in(flat)
        return [dict(r) for r in _ENT_ICEBERG_TABLES
                if r["schema_name"] == schema]
    if flat.startswith(("describe external volume", "desc external volume")):
        name = _ent_quoted_name(flat, "external volume")
        return [dict(r) for r in _ENT_EXTERNAL_VOLUMES[name]]
    if flat.startswith(("describe catalog integration",
                        "desc catalog integration")):
        name = _ent_quoted_name(flat, "catalog integration")
        return [dict(r) for r in _ENT_CATALOG_INTEGRATIONS[name]]
    if flat.startswith("show catalog integrations"):
        return [{"name": n, "type": "CATALOG", "category": "CATALOG",
                 "enabled": "true", "comment": "", "created_on": _ENT_CREATED}
                for n in _ENT_CATALOG_INTEGRATIONS]
    if flat.startswith(("describe table", "desc table")):
        return _ent_describe_table(" ".join(sql.split()))
    if flat.startswith(("describe share", "desc share")):
        name = _ent_quoted_name(flat, "share")
        return [dict(r) for r in _ENT_SHARE_OBJECTS[name]]
    if "information_schema.columns" in flat:
        schema = p.get("schema")
        return [{**c, "TABLE_SCHEMA": schema}
                for c in _ENT_COLUMNS.get(schema, [])]
    if "get_ddl" in flat:
        name = str(p.get("f", "")).replace('"', "").rsplit(".", 1)[-1]
        return [{"D": _ENT_VIEW_SQL[name]}]

    # --- census ------------------------------------------------------------
    if "information_schema.functions" in flat:
        return [dict(r) for r in _ENT_FUNCTIONS]
    if "information_schema.stages" in flat:
        return [dict(r) for r in _ENT_STAGES]
    if "information_schema.file_formats" in flat:
        return [dict(r) for r in _ENT_FILE_FORMATS]
    if ("information_schema.procedures" in flat
            or "information_schema.sequences" in flat
            or "information_schema.pipes" in flat):
        return []
    if "show materialized views in database" in flat:
        return [dict(r) for r in _ENT_MATERIALIZED_VIEWS]
    if "show streams in database" in flat:
        return [dict(r) for r in _ENT_STREAMS]
    if "show services in database" in flat:
        return [dict(r) for r in _ENT_SERVICES]
    if any(f"show {k} in database" in flat for k in (
            "tasks", "dynamic tables", "alerts", "secrets", "network rules",
            "streamlits", "notebooks")):
        return []
    if "show primary keys in database" in flat:
        return [dict(r) for r in _ENT_PRIMARY_KEYS]
    if ("show unique keys in database" in flat
            or "show imported keys in database" in flat):
        return []
    # Account-scoped.
    if flat.startswith("show shares"):
        return [dict(r) for r in _ENT_SHARES]
    if flat.startswith("show replication groups"):
        return [dict(r) for r in _ENT_GROUPS]
    if flat.startswith("show failover groups"):
        return [dict(r) for r in _ENT_GROUPS if r["type"] == "FAILOVER"]
    if flat.startswith("show compute pools"):
        return [dict(r) for r in _ENT_COMPUTE_POOLS]
    if flat.startswith("show applications"):
        return [dict(r) for r in _ENT_APPLICATIONS]
    if flat.startswith("show roles") or flat.startswith("show network policies"):
        return []

    # --- lineage / compute / security / maintenance -------------------------
    if "object_dependencies" in flat:
        return [dict(r) for r in _ENT_DEPENDENCIES]
    if flat.startswith("show warehouses"):
        return [dict(r) for r in _ENT_WAREHOUSES]
    if "warehouse_metering_history" in flat:
        return [dict(r) for r in _ENT_METERING]
    if "show masking policies" in flat:
        return [{"name": "MASK_EMAIL", "database_name": ENTERPRISE_DB,
                 "schema_name": "SALES", "kind": "MASKING_POLICY"}]
    if "show row access policies" in flat:
        return [{"name": "RAP_REGION", "database_name": ENTERPRISE_DB,
                 "schema_name": "SALES", "kind": "ROW_ACCESS_POLICY"}]
    if "show aggregation policies" in flat or "show projection policies" in flat:
        return []
    if "show tags" in flat:
        return [dict(r) for r in _ENT_TAGS]
    if "tag_references_all_columns" in flat:
        return [dict(r) for r in _ENT_TAG_REFS if _names_object(flat, r)]
    if "information_schema.policy_references" in flat:
        return [dict(r) for r in _ENT_POLICY_REFS
                if _names_object(flat, r, prefix="REF_")]
    if "tag_references" in flat:
        return [dict(r) for r in _ENT_TAG_REFS]
    if "policy_references" in flat:
        return [dict(r) for r in _ENT_POLICY_REFS]
    if "grants_to_roles" in flat:
        return [dict(r) for r in _ENT_GRANTS]
    if "show parameters like 'max_data_extension_time_in_days'" in flat:
        return [{"value": "14", "level": "ACCOUNT"}]
    if "show parameters like 'data_retention_time_in_days'" in flat:
        level = "ACCOUNT" if "in account" in flat else ""
        return [{"value": "1", "level": level}]
    if "automatic_clustering_history" in flat:
        return [{"DATABASE_NAME": ENTERPRISE_DB, "SCHEMA_NAME": "SALES",
                 "TABLE_NAME": "ORDERS", "EVENTS": 22, "CREDITS": 6.1,
                 "BYTES": 12_884_901_888, "ROWS_RECLUSTERED": 55_000_000}]
    if "table_dml_history" in flat:
        return [{"DATABASE_NAME": ENTERPRISE_DB, "SCHEMA_NAME": "SALES",
                 "TABLE_NAME": "ORDERS", "ROWS_ADDED": 3_100_000,
                 "ROWS_REMOVED": 900_000, "ROWS_UPDATED": 610_000,
                 "WINDOWS": 30}]

    # --- smoke --------------------------------------------------------------
    if "information_schema.tables" in flat and flat.startswith("select count"):
        return [{"N": sum(len(v) for v in _ENT_SHOW_TABLES.values())}]

    raise ValueError(
        f"the emulated enterprise Snowflake has no answer for: {flat[:200]}")
