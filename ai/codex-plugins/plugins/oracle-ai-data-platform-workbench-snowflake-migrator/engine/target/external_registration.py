"""External and Iceberg tables: an AIDP registration over OCI Object Storage.

An external table's rows are files in the customer's bucket; an Iceberg
table is an open table format there already. Neither is copied: reading
every byte through a warehouse to re-write what already sits in object
storage is the wrong move. The planner files both under `register_in_place`
(plan/build.py) and this module writes the registration.

What it generates, and what it does not:

  * For an EXTERNAL table, ONE `CREATE TABLE IF NOT EXISTS <target> USING
    PARQUET|CSV|JSON|ORC|AVRO|DELTA LOCATION
    'oci://<bucket>@<namespace>/<path>'`, with the placeholders left for the
    operator. A placeholder cannot be satisfied (`<bucket>` is not a valid
    bucket name), so a statement run unfilled gives a table that reads
    nothing -- never one that quietly reads the wrong location.
  * For an ICEBERG table, never `CREATE TABLE ... USING ICEBERG LOCATION`:
    by Iceberg's semantics that does not adopt existing metadata -- it
    creates a new, empty table there (Hive/REST catalog) or is rejected
    (Hadoop/path catalog), so filled in it would register a table that reads
    0 rows. The form that adopts existing snapshots is `CALL
    <iceberg_catalog>.system.register_table(table => ..., metadata_file =>
    'oci://.../metadata/<rewritten root metadata file>')`, and the file it
    names exists only after the metadata's absolute paths are rewritten. So
    an Iceberg entry carries that CALL with the rewrite as its explicit
    prerequisite, is marked `needs_metadata_rewrite`, and is NOT counted as
    registrable as generated (`registrable` counts external tables only;
    `after_rewrite` counts these).
  * The SOURCE location only as where the files come FROM. No statement
    ever points at `s3://`, Azure or GCS: AIDP's tables live under
    `oci://` (live, probe 1, 2026-09-29) and an S3 path would need cloud
    credentials this plugin never configures. Moving the files is a stated
    PREREQUISITE -- this plugin moves no bytes.
  * Nothing is executed. The statements run on an AIDP cluster, by the
    operator, after the move. That an AIDP catalog accepts an external
    `USING ... LOCATION 'oci://...'` table, and how Iceberg metadata is
    registered there, are NOT live-verified; the report says so.

Reads (Snowflake, read-only, bounded -- never a read per external table):
`SHOW EXTERNAL TABLES IN DATABASE` once per database in scope (also the
safety net for an external table SHOW TABLES did not list), `SHOW ICEBERG
TABLES IN DATABASE` once per database holding an Iceberg candidate,
`DESCRIBE EXTERNAL VOLUME` once per volume, `DESCRIBE CATALOG INTEGRATION`
once per external catalog, and `SYSTEM$GET_ICEBERG_TABLE_INFORMATION` once
per Snowflake-managed Iceberg table (the exact root metadata file). A read
that fails is named, and the tables it would have described are listed as
not registrable with the error -- never dropped.
"""
from __future__ import annotations

import datetime
import json
from typing import Callable

from snowflake_source.dialect import lexer
from snowflake_source.extract.catalog import show_paged
from target.ddl import quote_backtick, quote_spark_string

__all__ = ["REGISTER_CATEGORY", "build_external_registration",
           "render_external_registration"]

REGISTER_CATEGORY = "register_in_place"

OCI_LOCATION = "oci://<bucket>@<namespace>/"

# Snowflake file_format_type -> Spark data source. XML has no built-in Spark
# 3.5 source, so it is not registrable here.
_SPARK_SOURCE = {"PARQUET": "PARQUET", "CSV": "CSV", "JSON": "JSON",
                 "ORC": "ORC", "AVRO": "AVRO"}

_CSV_OPTIONS = "OPTIONS (header '<true|false>', sep '<delimiter>')"

PREREQUISITES = (
    "**Move the files to OCI Object Storage first.** This plugin moves no "
    "bytes. The source locations below are S3, Azure or GCS paths; no "
    "statement points at them, because an AIDP table is registered over "
    "`oci://` and the cluster holds no credential for the source cloud. "
    "Until the files are in the OCI bucket, each statement registers a "
    "table over an empty location. The copy tool (rclone, OCI Data "
    "Transfer, a one-off Spark job) is the operator's choice.",
    "**Keep each table's layout.** The suggested OCI path repeats the source "
    "key prefix, so partition directories (`key=value/`) survive the move "
    "and Spark discovers them the same way.",
    "**Grant the AIDP workspace identity read on the bucket** -- the path "
    "Object Storage reads are authorised by -- before the first query.",
    "**Fill every placeholder** (`<bucket>`, `<namespace>`, and where shown "
    "`<path>` and the CSV options). A placeholder cannot be satisfied -- "
    "`<bucket>` is not a valid bucket name -- so a statement run unfilled "
    "gives a table that reads nothing, never one over the wrong location.",
    "**Run the statements on an AIDP cluster** (a notebook cell, in the "
    "target catalog). `snowmig` executes none of them.",
    "**Cut over deliberately.** The Snowflake object keeps reading the old "
    "location, and whatever writes the source bucket keeps writing there: "
    "decide which side is the system of record before anyone writes to the "
    "OCI copy.",
)

LIVE_CHECKS = (
    "That an AIDP catalog accepts an external (unmanaged) table "
    "`CREATE TABLE ... USING PARQUET LOCATION 'oci://...'` and reads it "
    "back: run one statement on a cluster over a moved Parquet prefix and "
    "`SELECT count(*)` it against the Snowflake external table.",
    "Iceberg: which Iceberg catalog the workspace uses (AIDP documents an "
    "Iceberg Hadoop catalog on `oci://`, `spark.sql.catalog.<name>.type = "
    "hadoop`) and that its `system.register_table` procedure adopts a moved, "
    "rewritten table: register one and `SELECT count(*)` it against the "
    "Snowflake table.",
    "Iceberg path rewrite: that the Iceberg version on the cluster has "
    "`rewrite_table_path` (Iceberg 1.8+), or plan to re-write the table.",
)

_REWRITTEN_ROOT = "<rewritten root metadata file>"

ICEBERG_OPTIONS = (
    "**Rewrite, then register.** Copy the files, rewrite the metadata's "
    "absolute paths to the OCI prefix (Iceberg `rewrite_table_path`, 1.8+; "
    "its output names the rewritten latest metadata file), then run the "
    "`register_table` CALL below with `<rewritten root metadata file>` "
    "filled in. `register_table` adopts the existing snapshots.",
    "**Or re-write the table** on AIDP from its data (a fresh copy through a "
    "Spark job), which this plan does not generate for it.",
    "**Never `CREATE TABLE ... USING ICEBERG LOCATION`** over the moved "
    "directory: it does not adopt the existing metadata -- it makes a new, "
    "empty table there, or a path catalog rejects it -- so it reads 0 rows.",
)


def _now() -> str:
    return datetime.datetime.now(datetime.timezone.utc).isoformat()


def _is_set(value) -> bool:
    return str(value if value is not None else "").strip().lower() in (
        "true", "y", "yes", "1")


def _key_prefix(url: str | None) -> str | None:
    """The object key prefix of a cloud URL, bucket and host removed.

    s3://bucket/a/b/ and gcs://bucket/a/b/ -> a/b/; an Azure URL
    azure://<account>.blob.core.windows.net/<container>/a/b/ -> a/b/.
    """
    if not url or "://" not in url:
        return None
    scheme, rest = url.split("://", 1)
    parts = rest.split("/")
    drop = 2 if scheme.lower() == "azure" else 1
    key = "/".join(parts[drop:])
    return key


def _split_ident(rec: dict) -> tuple[str, str, str]:
    ident = rec["source_identifier"]
    db, schema = rec.get("source_database"), rec.get("source_schema")
    if db and schema and ident.startswith(f"{db}.{schema}."):
        return db, schema, ident[len(db) + len(schema) + 2:]
    db, schema, name = ident.split(".", 2)
    return db, schema, name


def _target_sql(target: str) -> str:
    return ".".join(quote_backtick(p) for p in target.split(".", 2))


def _virtual_columns(rec: dict) -> list[str]:
    return [c["COLUMN_NAME"] for c in rec.get("columns") or []
            if c.get("COLUMN_NAME") != "VALUE"]


def _external_entry(rec: dict, row: dict | None, target: str) -> dict:
    entry = _base(rec, "external table", target)
    if row is None:
        return _unregistrable(entry, "SHOW EXTERNAL TABLES did not list it, so "
                                     "its location and file format are unknown")
    location = row.get("location") or None
    fmt = str(row.get("file_format_type") or "").upper()
    table_format = str(row.get("table_format") or "").upper()
    entry.update({"move_from": location, "cloud": row.get("cloud"),
                  "region": row.get("region"), "stage": row.get("stage"),
                  "file_format_type": fmt or None,
                  "file_format_name": row.get("file_format_name") or None,
                  "table_format": table_format or None})
    prefix = _key_prefix(location)
    if prefix is None:
        return _unregistrable(entry, f"no readable location ({location!r})")
    source = "DELTA" if table_format == "DELTA" else _SPARK_SOURCE.get(fmt)
    if source is None:
        return _unregistrable(
            entry, f"file format {fmt or 'unknown'} has no built-in Spark "
                   f"data source to register it with")
    oci = OCI_LOCATION + prefix
    stmt = f"CREATE TABLE IF NOT EXISTS {_target_sql(target)} USING {source}"
    if source == "CSV":
        stmt += f" {_CSV_OPTIONS}"
    stmt += " LOCATION " + quote_spark_string(oci)
    entry.update({"oci_path": oci, "statement": stmt, "registrable": True})
    notes = entry["notes"]
    virtual = _virtual_columns(rec)
    if virtual:
        notes.append(
            "The schema comes from the files, not from Snowflake: the virtual "
            "columns " + ", ".join(virtual) + " are expressions over VALUE in "
            "Snowflake and are not carried. Where they rename or cast a field, "
            "define a view over the registered table.")
    if source == "CSV":
        notes.append(
            "The Snowflake FILE_FORMAT options (delimiter, header, NULL_IF, "
            "escape, compression) were not read, so the statement carries "
            "placeholders for them instead of a guess.")
    if source == "AVRO":
        notes.append("AVRO needs the spark-avro package on the cluster; not "
                     "verified on AIDP.")
    if row.get("notification_channel"):
        notes.append(
            "Snowflake refreshed this table from bucket event notifications; "
            "nothing does on AIDP. New files are seen after `REFRESH TABLE` "
            "on the target.")
    notes.append("Partition columns defined in Snowflake (PARTITION BY) are "
                 "not read; Spark discovers only `key=value/` directories.")
    return entry


def _iceberg_entry(rec: dict, row: dict | None, target: str, *,
                   volumes: dict, catalogs: dict, metadata: dict) -> dict:
    entry = _base(rec, "Iceberg table", target)
    if row is None:
        return _unregistrable(entry, "SHOW ICEBERG TABLES did not list it, so "
                                     "its volume and catalog are unknown")
    catalog = row.get("catalog_name") or None
    entry.update({"catalog": catalog,
                  "iceberg_table_type": row.get("iceberg_table_type"),
                  "external_volume": row.get("external_volume_name") or None,
                  "base_location": row.get("base_location") or None})
    vol = volumes.get(entry["external_volume"]) or {}
    entry["storage_provider"] = vol.get("STORAGE_PROVIDER")
    notes = entry["notes"]
    managed = str(catalog or "").upper() == "SNOWFLAKE"
    if managed:
        entry["catalog_source"] = "SNOWFLAKE"
        meta = metadata.get(rec["source_identifier"]) or {}
        root = meta.get("metadataLocation")
        if root and "/metadata/" in root:
            entry["metadata_location"] = root
            entry["move_from"] = root.split("/metadata/", 1)[0] + "/"
        elif vol.get("STORAGE_BASE_URL") and entry["base_location"]:
            entry["move_from"] = (vol["STORAGE_BASE_URL"].rstrip("/") + "/"
                                  + entry["base_location"].lstrip("/"))
            notes.append("The location is derived from the volume's "
                         "STORAGE_BASE_URL and base_location, not confirmed "
                         "from the root metadata file: check the directory in "
                         "the bucket before moving it.")
        prefix = _key_prefix(entry["move_from"])
        if prefix is None:
            return _unregistrable(entry, "its location could not be read "
                                         "from the external volume")
        oci = OCI_LOCATION + prefix
    else:
        entry["catalog_source"] = (catalogs.get(catalog) or {}).get(
            "CATALOG_SOURCE")
        entry["move_from"] = None
        oci = OCI_LOCATION + "<path>"
        where = entry["catalog_source"] or "an external catalog"
        notes.append(
            f"Catalogued in {where} (catalog integration {catalog}), not in "
            f"Snowflake: the table's location and metadata live there, and "
            f"Snowflake reads them. Registering a moved copy on AIDP forks "
            f"the table -- writers through {where} keep writing the source "
            f"bucket. Take `<path>` from that catalog's table location.")
    metadata_file = oci.rstrip("/") + "/metadata/" + _REWRITTEN_ROOT
    entry.update({"oci_path": oci, "registrable": False,
                  "needs_metadata_rewrite": True,
                  "statement": _register_table_call(target, metadata_file)})
    notes.append(
        "Iceberg metadata records ABSOLUTE file paths: every manifest names "
        "its data files by their source URL, so a byte-copy to OCI is not yet "
        "a readable table. Rewrite the paths after the copy (Iceberg "
        "`rewrite_table_path`, 1.8+) or re-write the table; the CALL names "
        "the REWRITTEN root metadata file, which exists only after the "
        "rewrite.")
    notes.append(
        f"Registered in the Iceberg catalog `<iceberg_catalog>` as "
        f"`{_iceberg_name(target)}` (the plan's target `{target}` without its "
        f"catalog part). Whether the AIDP catalog itself can hold it is a "
        f"live check.")
    if entry.get("metadata_location"):
        notes.append(f'Root metadata file at the source: '
                     f'`{entry["metadata_location"]}` -- the rewrite '
                     f'produces its OCI counterpart.')
    return entry


def _iceberg_name(target: str) -> str:
    parts = target.split(".", 2)
    return ".".join(parts[1:]) if len(parts) == 3 else target


def _register_table_call(target: str, metadata_file: str) -> str:
    """`register_table` adopts existing snapshots; CREATE TABLE ... LOCATION
    would make a new, empty table. `<iceberg_catalog>` is unquoted, so an
    unfilled CALL is a parse error, not a call against some catalog."""
    # Spark escapes with a backslash: a doubled quote reads as two adjacent
    # literals, so `db.o'brien` registered as `db.obrien`.
    return (f"CALL <iceberg_catalog>.system.register_table("
            f"table => {quote_spark_string(_iceberg_name(target))}, "
            f"metadata_file => {quote_spark_string(metadata_file)})")


def _base(rec: dict, kind: str, target: str) -> dict:
    return {"source_identifier": rec["source_identifier"], "kind": kind,
            "target": target, "move_from": None, "oci_path": None,
            "statement": None, "registrable": False,
            "needs_metadata_rewrite": False, "notes": [], "error": None}


def _unregistrable(entry: dict, why: str) -> dict:
    entry.update({"registrable": False, "needs_metadata_rewrite": False,
                  "statement": None, "error": why})
    return entry


def _describe(run_sql, what: str, name: str, notes: list[str]) -> list[dict] | None:
    try:
        return list(run_sql(f"describe {what} {lexer.quote_ident(name)}"))
    except Exception as exc:
        notes.append(f"DESCRIBE {what.upper()} {name}: {str(exc)[:200]}")
        return None


def _volume(rows: list[dict] | None) -> dict:
    """The ACTIVE storage location of a DESCRIBE EXTERNAL VOLUME answer."""
    if not rows:
        return {}
    active = next((r.get("property_value") for r in rows
                   if r.get("property") == "ACTIVE"), None)
    locations = []
    for r in rows:
        if str(r.get("property") or "").startswith("STORAGE_LOCATION_"):
            try:
                locations.append(json.loads(r.get("property_value") or "{}"))
            except ValueError:
                continue
    for loc in locations:
        if loc.get("NAME") == active:
            return loc
    return locations[0] if len(locations) == 1 else {}


def build_external_registration(run_sql: Callable[..., list[dict]],
                                inventory: dict, plan: dict) -> dict:
    notes: list[str] = []
    by_id = {r["source_identifier"]: r for r in inventory.get("inventory") or []}
    targets = plan.get("target_names") or {}
    candidates = [by_id[c["source_identifier"]]
                  for c in plan.get("cannot_migrate") or []
                  if c.get("category") == REGISTER_CATEGORY
                  and c["source_identifier"] in by_id]
    externals = [r for r in candidates
                 if _is_set((r.get("source_metadata") or {}).get("is_external"))]
    icebergs = [r for r in candidates
                if _is_set((r.get("source_metadata") or {}).get("is_iceberg"))]

    ext_rows: dict[str, dict] = {}
    ext_failed: dict[str, str] = {}
    not_in_inventory: list[str] = []
    for db in inventory.get("databases_in_scope") or []:
        try:
            rows, cap = show_paged(
                run_sql, f"show external tables in database {lexer.qualify(db)}")
        except Exception as exc:
            ext_failed[db] = str(exc)[:200]
            notes.append(f"SHOW EXTERNAL TABLES in {db}: {ext_failed[db]}")
            continue
        if cap:
            notes.append(f"SHOW EXTERNAL TABLES in {db}: {cap}")
        for row in rows:
            ident = f'{row.get("database_name") or db}.{row.get("schema_name")}.{row.get("name")}'
            ext_rows[ident] = row
            if ident not in by_id:
                not_in_inventory.append(ident)

    ice_rows: dict[str, dict] = {}
    ice_failed: dict[str, str] = {}
    for db in sorted({_split_ident(r)[0] for r in icebergs}):
        try:
            rows, cap = show_paged(
                run_sql, f"show iceberg tables in database {lexer.qualify(db)}")
        except Exception as exc:
            ice_failed[db] = str(exc)[:200]
            notes.append(f"SHOW ICEBERG TABLES in {db}: {ice_failed[db]}")
            continue
        if cap:
            notes.append(f"SHOW ICEBERG TABLES in {db}: {cap}")
        for row in rows:
            ice_rows[f'{row.get("database_name") or db}.{row.get("schema_name")}.{row.get("name")}'] = row

    volumes = {}
    catalogs = {}
    metadata = {}
    listed = [ice_rows[r["source_identifier"]] for r in icebergs
              if r["source_identifier"] in ice_rows]
    for name in sorted({r.get("external_volume_name") for r in listed} - {None, ""}):
        volumes[name] = _volume(_describe(run_sql, "external volume", name, notes))
    for name in sorted({r.get("catalog_name") for r in listed} - {None, "", "SNOWFLAKE"}):
        rows = _describe(run_sql, "catalog integration", name, notes) or []
        catalogs[name] = {r.get("property"): r.get("property_value") for r in rows}
    for rec in icebergs:
        row = ice_rows.get(rec["source_identifier"])
        if not row or str(row.get("catalog_name") or "").upper() != "SNOWFLAKE":
            continue
        db, schema, name = _split_ident(rec)
        literal = lexer.sql_literal(lexer.qualify(db, schema, name))
        try:
            info = run_sql(
                f"select system$get_iceberg_table_information('{literal}') INFO")
            metadata[rec["source_identifier"]] = json.loads(info[0]["INFO"])
        except Exception as exc:
            notes.append(f"SYSTEM$GET_ICEBERG_TABLE_INFORMATION "
                         f'{rec["source_identifier"]}: {str(exc)[:200]}')

    tables = []
    for rec in externals:
        ident = rec["source_identifier"]
        target = targets.get(ident) or ident.lower()
        entry = _external_entry(rec, ext_rows.get(ident), target)
        failed = ext_failed.get(_split_ident(rec)[0])
        if failed and ident not in ext_rows:
            entry = _unregistrable(entry, f"SHOW EXTERNAL TABLES failed: {failed}")
        tables.append(entry)
    for rec in icebergs:
        ident = rec["source_identifier"]
        target = targets.get(ident) or ident.lower()
        entry = _iceberg_entry(rec, ice_rows.get(ident), target,
                               volumes=volumes, catalogs=catalogs,
                               metadata=metadata)
        failed = ice_failed.get(_split_ident(rec)[0])
        if failed and ident not in ice_rows:
            entry = _unregistrable(entry, f"SHOW ICEBERG TABLES failed: {failed}")
        tables.append(entry)
    tables.sort(key=lambda e: e["source_identifier"])

    return {
        "generated_at": _now(),
        "category": REGISTER_CATEGORY,
        "executed": False,
        "live_verified": False,
        "tables": tables,
        # Only statements that register a readable table once filled in:
        # an Iceberg CALL waits on the metadata rewrite, counted apart.
        "registrable": sum(1 for t in tables if t["registrable"]),
        "after_rewrite": sum(1 for t in tables if t["needs_metadata_rewrite"]),
        "not_in_inventory": sorted(not_in_inventory),
        "prerequisites": list(PREREQUISITES),
        "live_checks": list(LIVE_CHECKS),
        "unreadable": notes,
    }


def render_external_registration(reg: dict) -> str:
    tables = reg.get("tables") or []
    out = ["# External and Iceberg tables — register in place over OCI "
           "Object Storage", "",
           "> **Generated, not executed, and NOT live-verified on AIDP.** "
           "These tables are not copied: their files are registered where "
           "they will sit, as an AIDP table over OCI Object Storage. "
           "`snowmig` runs none of the statements below and moves no bytes.",
           ""]
    if not tables and not reg.get("not_in_inventory"):
        out += ["No external or Iceberg table was planned `register_in_place`, "
                "and `SHOW EXTERNAL TABLES` listed none in scope: nothing to "
                "register.", ""]
        if reg.get("unreadable"):
            out += ["## Could not be read", ""]
            out += [f"- {n}" for n in reg["unreadable"]] + [""]
        return "\n".join(out)

    out += ["## Prerequisites — before any statement below", ""]
    out += [f"{i}. {p}" for i, p in enumerate(reg.get("prerequisites") or [], 1)]
    out += ["", "## Placeholders", "",
            "| Placeholder | Fill with |", "|---|---|",
            "| `<bucket>` | the OCI Object Storage bucket the files were moved to |",
            "| `<namespace>` | that tenancy's Object Storage namespace |",
            "| `<path>` | the table's directory, where the source catalog holds "
            "it (externally catalogued Iceberg only) |",
            "| `<iceberg_catalog>` | the Spark Iceberg catalog on the AIDP "
            "cluster (Iceberg only) |",
            "| `<rewritten root metadata file>` | the latest metadata file the "
            "path rewrite produced (Iceberg only; exists only after it) |",
            "| `<true\\|false>`, `<delimiter>` | the CSV header flag and field "
            "separator of the Snowflake file format |", ""]

    ok = [t for t in tables if t["registrable"]]
    out += [f"## Tables to register — {len(ok)} of {len(tables)}", ""]
    for t in ok:
        fmt = t.get("file_format_type") or t.get("iceberg_table_type") or ""
        out += [f'### `{t["source_identifier"]}` — {t["kind"]}'
                + (f", {fmt}" if fmt else ""), "",
                "| | |", "|---|---|",
                f'| Move files from | '
                + (f'`{t["move_from"]}`' if t.get("move_from")
                   else "*held by the external catalog, not by Snowflake*")
                + (f' ({t["cloud"]} {t.get("region") or ""})'.rstrip()
                   if t.get("cloud") else "") + " |",
                f'| Register at | `{t["oci_path"]}` |',
                f'| Target | `{t["target"]}` |', "",
                "```sql", t["statement"], "```", ""]
        out += [f"- {n}" for n in t.get("notes") or []] + [""]

    rewrite = [t for t in tables if t.get("needs_metadata_rewrite")]
    if rewrite:
        out += [f"## Iceberg tables — not registrable as generated: the "
                f"metadata rewrite comes first ({len(rewrite)})", "",
                "An Iceberg table is adopted only by `register_table` over its "
                "root metadata file, and after the move that file must be the "
                "REWRITTEN one. `CREATE TABLE ... USING ICEBERG LOCATION` would "
                "not adopt it: it makes a new, empty table (or a path catalog "
                "rejects it), which reads 0 rows. The ways forward:", ""]
        out += [f"{i}. {o}" for i, o in enumerate(ICEBERG_OPTIONS, 1)] + [""]
        for t in rewrite:
            fmt = t.get("iceberg_table_type") or ""
            out += [f'### `{t["source_identifier"]}` — {t["kind"]}'
                    + (f", {fmt}" if fmt else ""), "",
                    "| | |", "|---|---|",
                    f'| Move files from | '
                    + (f'`{t["move_from"]}`' if t.get("move_from")
                       else "*held by the external catalog, not by Snowflake*")
                    + " |",
                    f'| Table directory on OCI | `{t["oci_path"]}` |',
                    f'| Target | `{t["target"]}` |', "",
                    "After the rewrite:", "",
                    "```sql", t["statement"], "```", ""]
            out += [f"- {n}" for n in t.get("notes") or []] + [""]

    bad = [t for t in tables if not t["registrable"]
           and not t.get("needs_metadata_rewrite")]
    if bad:
        out += ["## Not registrable as generated", ""]
        out += [f'- `{t["source_identifier"]}` ({t["kind"]}) — {t["error"]}'
                for t in bad] + [""]
    if reg.get("not_in_inventory"):
        out += ["## Listed by SHOW EXTERNAL TABLES but not in the inventory", "",
                "SHOW TABLES did not return these, so `assess` never saw them "
                "and the plan says nothing about them. Re-run `assess` or "
                "register them by hand from the row above.", ""]
        out += [f"- `{i}`" for i in reg["not_in_inventory"]] + [""]
    if reg.get("unreadable"):
        out += ["## Could not be read", ""]
        out += [f"- {n}" for n in reg["unreadable"]] + [""]
    out += ["## Live checks before relying on this", ""]
    out += [f"- {c}" for c in reg.get("live_checks") or []] + [""]
    return "\n".join(out)
