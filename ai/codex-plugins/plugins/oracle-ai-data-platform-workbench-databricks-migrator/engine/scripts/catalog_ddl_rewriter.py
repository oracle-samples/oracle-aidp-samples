"""Catalog DDL Rewriter — pure functions, zero I/O.

Takes a Databricks Unity Catalog or Hive Metastore table descriptor (the dict
returned by `/api/2.1/unity-catalog/tables/<full_name>?include_delta_metadata=true`)
and produces an AIDP-compatible `CREATE TABLE` SQL statement.

Each transformation rule has a stable rule_id used for logging / skipping.
The rewriter NEVER performs I/O — all bucket mapping and catalog mapping is
passed in as data. Fully unit-testable.

Rules implemented (subset of the 18 from the design spec; rest stub out as
warnings since the sample fixture corpus doesn't exercise them):

  R01  3-part name -> 2-part (schema-qualified)
  R02  s3:// LOCATION -> oci:// LOCATION via bucket_map
  R03  /Volumes/ LOCATION -> /Volumes/<aidp_cat>/...
  R09  USING <override> if --target-using is set; otherwise preserve source format (default)
  R10  drop HMS-internal TBLPROPERTIES (transient_lastDdlTime, numFiles, ...)
  R11  drop spark.sql.* TBLPROPERTIES
  R13  drop unsupported delta.feature.* / delta.enable* / delta.parquet.*
  R14  drop delta.minReader/minWriter version (AIDP isn't Delta)
  R16  flag CREATE MATERIALIZED VIEW as UnsupportedDDL
  R17  flag CREATE STREAMING TABLE as UnsupportedDDL

Each rule emits a RuleApplication record so callers can build a transparent
audit log of what was changed.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional


class UnsupportedDDL(Exception):
    """Raised when the source DDL uses a feature that AIDP can't replay safely."""
    def __init__(self, rule_id: str, msg: str):
        super().__init__(f"[{rule_id}] {msg}")
        self.rule_id = rule_id


@dataclass
class RuleApplication:
    """One rule firing during rewrite — used for the audit report."""
    rule_id: str
    object_name: str
    before: str
    after: str
    severity: str = "info"  # info | warn | error


@dataclass
class RewriteResult:
    """Output of rewrite_table_ddl()."""
    aidp_full_name: str             # e.g. "samples_tpch.orders"
    create_table_sql: str           # the SQL to execute on AIDP
    applied_rules: list[RuleApplication] = field(default_factory=list)
    dropped_properties: list[str] = field(default_factory=list)


# ---------- property classification ----------

# Properties that are HMS-internal stats and have no meaning on AIDP.
HMS_INTERNAL_PROP_KEYS = {
    "transient_lastDdlTime", "numFiles", "numRows", "numPartitions",
    "rawDataSize", "totalSize", "COLUMN_STATS_ACCURATE",
    "serialization.format", "EXTERNAL",
    # Liquid clustering — single-key Delta property, no dot prefix
    "clusteringColumns",
}

# Property prefixes that are never meaningful on AIDP regardless of target
# format (engine-config bleed, delta-sharing view internals, UC/DLT markers).
_NON_FORMAT_DROP_PREFIXES = (
    "spark.sql.",
    "view.query.",             # delta-sharing internal view query keys
    "unity.",                  # UC-internal markers
    "pipelines.",              # DLT/streaming-table metadata
)

# When the target is NOT Delta, every `delta.*` property must also be dropped:
# a non-Delta table can only be harmed by inheriting Delta-runtime properties
# (delta.columnMapping.mode in particular silently corrupts column resolution
# on parquet). Per code review 2026-06-05.
DROPPED_PROP_PREFIXES = _NON_FORMAT_DROP_PREFIXES + ("delta.",)

def is_dropped_property(key: str) -> bool:
    """Return True if a TBLPROPERTIES key should be dropped during rewrite.

    ALL `delta.*` properties are dropped regardless of target format. On a fresh
    (empty) table they are unnecessary: protocol/feature/runtime-metadata keys
    (delta.feature.*, delta.minReaderVersion, delta.lastCommitTimestamp, …) are
    Delta-managed and are rejected as CREATE-time TBLPROPERTIES with
    [DELTA_UNKNOWN_CONFIGURATION] (verified on AIDP us-phoenix-1); the rest are
    re-settable tuning the target Delta re-derives via its own defaults. A
    Delta target still gets `USING delta` — just a clean property set.
    Non-Delta custom properties (e.g. business tags) are preserved.
    """
    if key in HMS_INTERNAL_PROP_KEYS:
        return True
    for prefix in DROPPED_PROP_PREFIXES:
        if key.startswith(prefix):
            return True
    return False


# ---------- name / path rewriting ----------

def rewrite_full_name(
    dbx_full_name: str,
    catalog_map: dict[str, str],
    flatten_strategy: str = "catalog-remap",
    default_catalog: str = "default",
) -> str:
    """Map a Databricks UC/HMS name to an AIDP name.

    catalog_map: { 'main': 'default', ... } — per-catalog target overrides.
    default_catalog: AIDP catalog used for any source catalog not in catalog_map
        (and for 2-part HMS names, which resolve in the default catalog on AIDP).
    flatten_strategy:
      - "catalog-remap" (default): AIDP supports 3-level naming, so preserve the
        schema + table names and only remap the catalog →
        `main.sample_schema.t` -> `default.sample_schema.t`. Keeps notebook
        references stable (no schema renaming).
      - "schema-prefix": `samples.tpch.orders` -> `samples_tpch.orders`
        (2-level flatten — preserves origin catalog in the schema name).
      - "schema-only":   `samples.tpch.orders` -> `tpch.orders`
        (lossy — collisions across catalogs).
    """
    parts = dbx_full_name.split(".")
    if len(parts) == 2:
        # HMS-style db.table — no source catalog. On AIDP a 2-level name resolves
        # in the default catalog; qualify it explicitly for catalog-remap.
        db, tbl = parts
        if flatten_strategy == "catalog-remap":
            return f"{default_catalog}.{db}.{tbl}"
        return dbx_full_name
    if len(parts) != 3:
        raise ValueError(f"Expected 2- or 3-part name, got: {dbx_full_name!r}")

    cat, sch, tbl = parts
    if flatten_strategy == "catalog-remap":
        cat_mapped = catalog_map.get(cat, default_catalog)
        return f"{cat_mapped}.{sch}.{tbl}"
    cat_mapped = catalog_map.get(cat, cat)
    if flatten_strategy == "schema-prefix":
        return f"{cat_mapped}_{sch}.{tbl}"
    elif flatten_strategy == "schema-only":
        return f"{sch}.{tbl}"
    else:
        raise ValueError(f"Unknown flatten_strategy: {flatten_strategy!r}")


def rewrite_location(
    dbx_location: Optional[str],
    bucket_map: dict[str, tuple[str, str]],
    aidp_catalog_name: Optional[str] = None,
) -> Optional[str]:
    """Rewrite source LOCATION URI to AIDP-compatible URI.

    bucket_map: { 's3_bucket_name': ('oci_bucket', 'oci_namespace'), ... }
    aidp_catalog_name: used for /Volumes/ rewrites

    Returns None if input is None (caller drops the LOCATION clause).
    Returns the rewritten URI otherwise. Unmapped buckets pass through unchanged
    with a flag (caller should warn).
    """
    if not dbx_location:
        return None

    # s3:// or s3a://
    if dbx_location.startswith(("s3://", "s3a://")):
        # s3://bucket/path -> oci://oci_bucket@oci_ns/path
        scheme_end = dbx_location.find("://") + 3
        rest = dbx_location[scheme_end:]
        slash = rest.find("/")
        if slash < 0:
            return dbx_location  # malformed, pass through
        bucket = rest[:slash]
        path = rest[slash + 1:]
        if bucket in bucket_map:
            oci_bucket, oci_ns = bucket_map[bucket]
            return f"oci://{oci_bucket}@{oci_ns}/{path}"
        return dbx_location  # unmapped — caller decides

    # /Volumes/<catalog>/<schema>/<volume>/<path>
    if dbx_location.startswith("/Volumes/"):
        parts = dbx_location.split("/", 5)  # ['', 'Volumes', cat, sch, vol, rest]
        if len(parts) >= 5 and aidp_catalog_name:
            parts[2] = aidp_catalog_name
            return "/".join(parts)
        return dbx_location

    # dbfs:/ — legacy DBFS, must be mapped via bucket_map
    if dbx_location.startswith(("dbfs:/", "/dbfs/")):
        # Not handled by this rewriter — needs separate dbfs_map lookup.
        # Pass through; caller should warn.
        return dbx_location

    return dbx_location


# ---------- main rewrite entry point ----------

def _q(s: str) -> str:
    """Backtick-quote a SQL identifier."""
    return f"`{s}`"


def _format_col(col: dict) -> str:
    """Format one column dict into a CREATE TABLE column line."""
    name = col["name"]
    type_text = col["type_text"]
    nullable = col.get("nullable", True)
    comment = (col.get("comment") or "").strip()

    parts = [f"  {_q(name)} {type_text}"]
    if not nullable:
        parts.append("NOT NULL")
    if comment:
        # Escape single quotes in SQL string literal
        escaped = comment.replace("'", "''")
        parts.append(f"COMMENT '{escaped}'")
    return " ".join(parts)


def rewrite_table_ddl(
    dbx_table: dict,
    catalog_map: dict[str, str],
    bucket_map: dict[str, tuple[str, str]],
    aidp_target_catalog: Optional[str] = None,
    target_using: Optional[str] = None,
    flatten_strategy: str = "catalog-remap",
    location_strategy: str = "auto",
    default_catalog: str = "default",
) -> RewriteResult:
    """Build an AIDP CREATE TABLE statement from a Databricks UC/HMS table dict.

    dbx_table: the JSON dict returned by Databricks UC REST API table-get
        (or a synthesized equivalent for HMS).
    catalog_map: source-catalog -> target-catalog name mapping.
    bucket_map: source-S3-bucket -> (oci-bucket, oci-namespace).
    aidp_target_catalog: name of the AIDP catalog (or None).
    target_using: explicit AIDP storage format OVERRIDE. Default None = preserve
        the source format (Delta stays Delta — AIDP supports Delta). Set e.g.
        'parquet'/'iceberg' only to deliberately convert.
    flatten_strategy: see rewrite_full_name() (default 'catalog-remap').
    default_catalog: AIDP catalog for unmapped source catalogs / 2-part names.
    location_strategy:
      - "auto" (default): table-type-aware. EXTERNAL tables preserve + rewrite
        their LOCATION (S3->OCI via bucket_map) so they stay external; MANAGED
        tables omit LOCATION so they stay managed. This is the faithful default.
      - "drop":     force-omit LOCATION for all tables (e.g. cross-account
        Delta-Sharing managed tables whose S3 we can't read).
      - "preserve": force-include the rewritten LOCATION for all tables.

    Raises UnsupportedDDL for table types AIDP can't replay safely
    (materialized views, streaming tables, share-based virtual tables).
    """
    applied: list[RuleApplication] = []
    dropped_props: list[str] = []

    full_name = dbx_table.get("full_name") or f"{dbx_table.get('schema_name','default')}.{dbx_table['name']}"
    table_type = dbx_table.get("table_type", "MANAGED")
    fmt = (dbx_table.get("data_source_format") or "DELTA").upper()
    securable_kind = dbx_table.get("securable_kind", "")

    # ── R16/R17 unsupported types ──
    if table_type == "MATERIALIZED_VIEW":
        raise UnsupportedDDL("R16_MV_UNSUPPORTED", f"AIDP doesn't support MATERIALIZED VIEW: {full_name}")
    if table_type == "STREAMING_TABLE":
        raise UnsupportedDDL("R17_STREAMING_UNSUPPORTED", f"AIDP doesn't support STREAMING TABLE: {full_name}")
    if table_type == "VIEW":
        # Views with view_definition need separate handling — for v1 we skip.
        # The sample fixtures don't include views, so this is a deferred path.
        raise UnsupportedDDL("R15_VIEW_DEFERRED", f"VIEW migration not yet implemented: {full_name}")

    # ── R01 rewrite name ──
    aidp_full = rewrite_full_name(full_name, catalog_map, flatten_strategy, default_catalog)
    applied.append(RuleApplication(
        rule_id="R01_NAME_FLATTEN",
        object_name=full_name,
        before=full_name,
        after=aidp_full,
    ))

    # ── columns ──
    cols = dbx_table.get("columns") or []
    if not cols:
        raise UnsupportedDDL(
            "NO_COLUMNS_VISIBLE",
            f"No columns returned by UC API for {full_name} — likely a Delta-shared "
            f"table or insufficient privileges. Cannot reconstruct DDL."
        )
    # Sort by position (UC may return out-of-order)
    cols = sorted(cols, key=lambda c: c.get("position", 0))
    col_lines = ",\n".join(_format_col(c) for c in cols)

    # ── partitioning ──
    # Partition order is defined by partition_index, NOT by column position
    # (per code review 2026-06-05). A position-ordered partition list emits
    # PARTITIONED BY (...) in the wrong order, changing the physical layout
    # on disk and breaking partition-predicate pushdown.
    _partition_cols = [c for c in cols if c.get("partition_index") is not None]
    _partition_cols.sort(key=lambda c: c["partition_index"])
    partition_cols = [c["name"] for c in _partition_cols]

    # ── R09 USING — preserve source format unless an explicit override is given ──
    src_fmt = fmt.lower()
    if target_using and target_using.lower() != src_fmt:
        using_clause = f"USING {target_using}"
        applied.append(RuleApplication(
            rule_id="R09_USING_OVERRIDE",
            object_name=full_name,
            before=f"USING {src_fmt}",
            after=using_clause,
        ))
        effective_fmt = target_using.lower()
    else:
        using_clause = f"USING {src_fmt}"
        effective_fmt = src_fmt
    target_is_delta = effective_fmt == "delta"

    # ── R02/R03 LOCATION (table-type-aware by default) ──
    # "auto": EXTERNAL tables keep their (rewritten) LOCATION so they stay
    # external; MANAGED tables omit LOCATION so they stay managed. An explicit
    # "preserve"/"drop" overrides this per-run.
    location_clause = ""
    raw_loc = dbx_table.get("storage_location")
    if location_strategy == "auto":
        effective_loc_strategy = "preserve" if table_type == "EXTERNAL" else "drop"
    else:
        effective_loc_strategy = location_strategy
    if effective_loc_strategy == "preserve" and raw_loc:
        rewritten = rewrite_location(raw_loc, bucket_map, aidp_target_catalog)
        if rewritten and rewritten != raw_loc:
            applied.append(RuleApplication(
                rule_id="R02_LOCATION_S3_TO_OCI",
                object_name=full_name,
                before=raw_loc,
                after=rewritten,
            ))
        if rewritten and rewritten == raw_loc and raw_loc.startswith(("s3://", "s3a://")):
            # Unmapped bucket
            applied.append(RuleApplication(
                rule_id="R02_BUCKET_UNMAPPED",
                object_name=full_name,
                before=raw_loc,
                after=raw_loc,
                severity="warn",
            ))
        if rewritten:
            location_clause = f"LOCATION '{rewritten}'"
    elif effective_loc_strategy == "drop":
        if raw_loc:
            applied.append(RuleApplication(
                rule_id="R02_LOCATION_DROPPED",
                object_name=full_name,
                before=raw_loc,
                after="(dropped — AIDP will choose default location)",
                severity="info",
            ))

    # ── R10/R11/R13/R14 TBLPROPERTIES ──
    raw_props = dbx_table.get("properties") or {}
    # ── TBLPROPERTIES ──
    # Delta target → emit NO properties at all. A fresh Delta table needs none,
    # and carrying any source property over risks failures on later fresh-data
    # creation / data sync (several are also Delta-managed and non-settable —
    # see is_dropped_property). Non-Delta target → keep custom properties and
    # drop only delta.*/engine/HMS-internal junk.
    kept_props: dict[str, str] = {}
    if target_is_delta:
        dropped_props.extend(sorted(raw_props.keys()))
    else:
        for k, v in raw_props.items():
            if is_dropped_property(k):
                dropped_props.append(k)
            else:
                kept_props[k] = str(v)

    tbl_props_clause = ""
    if kept_props:
        parts = []
        for k, v in sorted(kept_props.items()):
            v_esc = v.replace("'", "''")
            parts.append(f"  '{k}' = '{v_esc}'")
        # Python <3.12 forbids backslashes inside f-string {} braces (PEP 701).
        # Pull the separator into a local to stay portable down to 3.10.
        _sep = ",\n"
        tbl_props_clause = "TBLPROPERTIES (\n" + _sep.join(parts) + "\n)"

    # ── partitioning clause ──
    part_clause = ""
    if partition_cols:
        cols_str = ", ".join(_q(c) for c in partition_cols)
        part_clause = f"PARTITIONED BY ({cols_str})"

    # ── table comment ──
    tbl_comment = (dbx_table.get("comment") or "").strip()
    comment_clause = ""
    if tbl_comment:
        comment_clause = f"COMMENT '{tbl_comment.replace(chr(39), chr(39) * 2)}'"

    # ── assemble ──
    # aidp_full is the rewritten name (3-level catalog.schema.table under the
    # default catalog-remap strategy, or 2-level for schema-prefix/-only).
    sql_parts = [f"CREATE TABLE IF NOT EXISTS {aidp_full} (\n{col_lines}\n)"]
    sql_parts.append(using_clause)
    if part_clause:
        sql_parts.append(part_clause)
    if comment_clause:
        sql_parts.append(comment_clause)
    if location_clause:
        sql_parts.append(location_clause)
    if tbl_props_clause:
        sql_parts.append(tbl_props_clause)

    create_sql = "\n".join(sql_parts)

    return RewriteResult(
        aidp_full_name=aidp_full,
        create_table_sql=create_sql,
        applied_rules=applied,
        dropped_properties=dropped_props,
    )


def schema_create_sql(aidp_schema_name: str, comment: str = "") -> str:
    """Helper: build CREATE SCHEMA IF NOT EXISTS statement.

    NOTE: AIDP-Spark Hive metastore silently no-ops CREATE SCHEMA when the
    COMMENT clause contains certain characters (verified: colons in ISO
    timestamps cause the schema to not persist across sessions even though
    the call returns status=ok). Workaround: never emit COMMENT on CREATE
    SCHEMA. Comment can be added post-create via ALTER SCHEMA SET PROPERTIES
    (`'comment' = '...'`) if needed.

    aidp_schema_name may be catalog-qualified ("catalog.schema") under the
    catalog-remap strategy or bare ("schema"). Each dotted part is quoted
    independently so the dot is a namespace separator, not part of one name.
    """
    quoted = ".".join(f"`{p}`" for p in aidp_schema_name.split("."))
    return f"CREATE SCHEMA IF NOT EXISTS {quoted}"
