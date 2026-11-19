#!/usr/bin/env python3
"""Create target schemas and EMPTY Delta tables for one schema (or all).

Runs on AIDP compute. Three structure sources, chosen with --mode:

  ddl-plan (default)  types come from `plan/ddl_plan.json`, which the
                      migrator's own type mapper produced -- it refuses what
                      it cannot map exactly instead of guessing, and the plan
                      was reviewed and signed off before this ran. Costs NO
                      source read per table, which is what makes a large
                      estate feasible: each CTAS is its own Snowflake round
                      trip.
  ctas                CREATE TABLE ... USING DELTA AS SELECT * ... WHERE 1=0.
                      Spark derives the types through the connector, so the
                      copy cannot hit a type the table cannot hold -- but the
                      mapping is the connector's, not an audited one, and it
                      pays a source read per table.
  manifest            types come from discovery_manifest.json verbatim. Only
                      valid when the manifest carries SPARK types, i.e. it
                      was built in external-catalog mode (DESCRIBE); a
                      connector-mode manifest carries SNOWFLAKE types and the
                      whole run is refused before anything is created. Some
                      of those types Delta accepts verbatim with another
                      meaning -- FLOAT is 64-bit in Snowflake and 32-bit in
                      Spark -- so no per-type check can tell them apart.

Safety: CREATE TABLE IF NOT EXISTS everywhere; nothing is ever dropped or
replaced here. The target catalog must be INTERNAL — this script REFUSES to
address the source catalog as its target, and AIDP refuses DDL on external
catalogs anyway (documented), so the failure would be loud, not silent.

The CREATE returning is not the claim: IF NOT EXISTS is a silent no-op on a
table that is already there, so every table is DESCRIBEd afterwards and
compared with the plan, column by column and in order.

In --mode ddl-plan the plan's `NOT NULL`, column COMMENTs, table COMMENT and
Delta features (`delta_features`: a liquid `CLUSTER BY`, and TBLPROPERTIES
for retention and the change data feed -- live-verified on AIDP Delta 3.1)
are applied, not just its names and types: they are in the CREATE TABLE the
reviewer approved, and each table is read back against that full shape.
Nullability is read from the table's schema
(DESCRIBE does not report it); when that read fails the table's record says
the property is UNCHECKED rather than counting it as applied.

Writes `structure_report_<schema>.json` per schema, one status per table:
  created          it was not there before, and it reads back as planned
  already_existed  it was there before, and it matches the plan (in
                   --mode ctas there is no plan: the layout is NOT
                   compared, and the record's reason says so)
  type_drift       it was there with a layout the plan did not produce; it
                   is left as found, listed with the differing columns, and
                   counted as a problem (exit 1) -- the copy is a positional
                   INSERT, so a mismatched layout would land rows in the
                   wrong columns with matching counts
  not_in_plan      the approved plan carries no columns for it; NOT created
  failed           the CREATE raised; the error is the reason
Resumable: `created` and `already_existed` are skipped on a re-run with the
same --mode (--force re-checks them); one recorded under another --mode is
re-checked, because that mode's layout is not this one's -- a table --mode
manifest created is not thereby what the ddl plan approved. `type_drift`,
`failed` and `not_in_plan` are looked at again every run, so fixing the
table or the plan is enough.

--parallel N (default 8; --mode ctas: 1) creates and reads back N tables at
once; each table's record is decided by the same create-and-read-back
whatever N is, and the report lists them in the manifest's order. Views never run in
parallel (below).

Views are recorded under a separate `views` key, never in `objects` (the
table map the copy scope reads). In --mode ddl-plan each planned view is
created from the plan's own CREATE VIEW SQL after every table exists and
recorded `created`, `failed` (a failure: exit 1) or `dry_run`; a manifest
view the plan does not carry is `not_in_plan`. --mode ctas and manifest
create tables only and record their views `not_created_by_this_path`.

A dynamic table or materialized view the plan migrates as a TABLE SNAPSHOT
(a TABLE statement with `snapshot_of`) is a table here, whatever list the
manifest files it under: in-AIDP discovery puts a materialized view under
`views` (its TABLE_TYPE is not BASE TABLE), and live 2026-09-29 this stage
built the manifest's `tables` only, so the planned CREATE TABLE never ran
and the view over it failed TABLE_OR_VIEW_NOT_FOUND. In --mode ddl-plan
every such statement for the schema is created exactly as a table is (plan
columns, Delta features, the read-back and type-drift check) -- also when
the manifest does not list the source at all -- and recorded in `objects`
with `kind` TABLE and its `snapshot_of`, never among the views.
"""
from __future__ import annotations

import argparse
import datetime
import hashlib
import json
import pathlib
import re
import sys
import time
from concurrent.futures import ThreadPoolExecutor

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
from snowmig_source import (  # noqa: E402
    SOURCE_MODES, SnowflakeSource, SourceConfigError, load_source_config, q,
    read_plan_json, read_report_json, write_step_output)

# /Workspace is the live-verified mount of the workspace tree on cluster
# filesystems (probed 2026-09-16 on a real cluster).
DEFAULT_REPORTS_DIR = "/Workspace/backup-snowflake-migration/reports"
# The step's own values also go here, next to the accumulated run report
# the CLI publishes after every stage. `--output-dir ''` switches it off.
DEFAULT_OUTPUT_DIR = "/Workspace/report/output"
MANIFEST_NAME = "discovery_manifest.json"


def three(*parts: str) -> str:
    return ".".join(q(p) for p in parts)


# Tables created at once. Live 2026-09-29: CREATE TABLE ~4 s a table
# serially on a warm cluster, ~2 s a table at 8 threads.
DEFAULT_PARALLEL = 8
# Tables recorded per batch: the report on disk is rewritten after each.
STRUCTURE_CHUNK = 50
# Seconds between report writes in the view phase (and always at its end).
REPORT_WRITE_INTERVAL = 15.0


def effective_parallel(requested: int | None, mode: str) -> int:
    """Tables created at once: the flag when given; else 1 for --mode ctas
    (each CTAS reads the whole source table through the connector, so eight
    at once multiplies the warehouse load) and DEFAULT_PARALLEL otherwise."""
    if requested is not None:
        return requested
    return 1 if mode == "ctas" else DEFAULT_PARALLEL


def _in_parallel(fn, items: list, parallel: int) -> list:
    """`[fn(item)]` in order, `parallel` at a time (1 = one by one)."""
    workers = max(1, min(parallel, len(items)))
    if workers == 1:
        return [fn(item) for item in items]
    with ThreadPoolExecutor(max_workers=workers,
                            thread_name_prefix="snowmig-structure") as pool:
        return list(pool.map(fn, items))


def _view_name(schema: str, table: str) -> str:
    """A temp view name unique to (schema, table), exact case included.

    Spark resolves view names case-insensitively, so `Orders` and `ORDERS`
    shared one name -- and with tables created in parallel, one table's
    CTAS would read the other's source.
    """
    digest = hashlib.sha1(f"{schema}\x00{table}".encode("utf-8")).hexdigest()
    readable = re.sub(r"[^a-z0-9_]", "_", f"{schema}_{table}".lower())[:80]
    return f"snowmig_src_{readable}_{digest[:12]}"


def log(msg: str) -> None:
    print(f"[structure] {msg}", flush=True)


def fail(msg: str) -> int:
    """Report a refusal on BOTH streams and return 1.

    A notebook task captures stdout only: live, a script that exited 1 via a
    stderr-only message produced a job failure with NO explanation anywhere.
    """
    print(f"ERROR: {msg}", flush=True)
    print(f"error: {msg}", file=sys.stderr)
    return 1


def _report_path(reports: pathlib.Path, schema: str) -> pathlib.Path:
    return reports / f"structure_report_{schema.lower()}.json"


# Live 2026-09-29: the /Workspace mount served a just-written report
# incompletely (JSONDecodeError on a file that was valid a minute later).
_read_report_json = read_report_json


def _load_report(path: pathlib.Path, schema: str, target: str) -> dict:
    """The prior report for this schema, but ONLY if it targeted the same place.

    Resumability is keyed by SOURCE schema, so a report written while
    targeting one destination would otherwise let a run against a DIFFERENT
    destination skip every create as "already created" -- observed live, with
    an empty target schema reported as done. A changed target starts a fresh
    record and says so.
    """
    if not path.exists():
        return {"schema": schema, "objects": {}, "target": target}
    prior = _read_report_json(path)
    if prior.get("target") and prior["target"] != target:
        log(f"{schema}: the previous report targeted {prior['target']}, not "
            f"{target} — starting a fresh record for this target (the old "
            f"one is kept at {path.name}.{prior['target'].replace('.', '_')})")
        path.with_suffix(
            f".{prior['target'].replace('.', '_')}.json").write_text(
                json.dumps(prior, indent=2), encoding="utf-8")
        return {"schema": schema, "objects": {}, "target": target}
    return prior


class TypeDrift(Exception):
    """The table was already there with a layout the plan did not produce."""


def _describe_columns(spark, fqn: str) -> list[tuple[str, str]] | None:
    """(name, type) pairs from DESCRIBE, or None when the table is not there.

    Columns end at the first blank or `#` row (Delta's metadata section).
    """
    try:
        rows = spark.sql(f"DESCRIBE {fqn}").collect()
    except Exception:
        return None
    out: list[tuple[str, str]] = []
    for row in rows:
        name = str(row["col_name"] or "").strip()
        if not name or name.startswith("#"):
            break
        out.append((name, str(row["data_type"] or "")))
    return out


def _describe_comments(spark, fqn: str) -> dict[str, str] | None:
    """{column: comment} from DESCRIBE's third column, upper-cased keys.

    None when the read-back carries no comment column at all -- a comment
    that was NOT LOOKED AT must not read as a comment that is missing.
    DESCRIBE reports comments; it does NOT report nullability, which is why
    that is read from the table's schema instead.
    """
    try:
        rows = spark.sql(f"DESCRIBE {fqn}").collect()
    except Exception:
        return None
    out: dict[str, str] = {}
    carried = False
    for row in rows:
        name = str(row["col_name"] or "").strip()
        if not name or name.startswith("#"):
            break
        try:
            value = row["comment"]
            carried = True
        except Exception:
            value = None
        out[name.upper()] = str(value or "")
    return out if carried else None


def _nullability(spark, fqn: str) -> dict[str, bool] | None:
    """{column: nullable} from the table's schema, or None when unreadable.

    DESCRIBE has no nullability column, so the read-back for `NOT NULL` is
    the StructType. None means NOT CHECKED -- reported as such rather than
    passed off as a match, because "we did not look" and "it is right" are
    the two answers this whole stage exists to keep apart.
    """
    try:
        fields = spark.table(fqn).schema.fields
        return {str(f.name).upper(): bool(f.nullable) for f in fields}
    except Exception:
        return None


def _norm_type(value: str) -> str:
    """Compare types ignoring case and internal spacing only."""
    return "".join(str(value).split()).upper()


def _compare_columns(expected: list[dict],
                     actual: list[tuple[str, str]],
                     nullable: dict[str, bool] | None = None,
                     comments: dict[str, str] | None = None) -> str | None:
    """None if the structures match, else a one-line description of the diff.

    Same rule as the control-plane deploy: names and types, in order. A
    same-count layout in another order is a diff -- the copy is a positional
    INSERT, so that is the case that lands rows in the wrong columns with
    matching counts.

    `NOT NULL` and column COMMENTs are part of the approved DDL, so they are
    compared too when the read-back supplies them: a table created with the
    reviewed SQL and reported "verified" against a name-and-type-only
    comparison is how the reviewed artifact and the applied one came apart in
    the first place. `nullable=None` means the schema could not be read; the
    caller says so rather than counting it as a match.
    """
    want = [(str(c.get("name", "")).upper(), _norm_type(c.get("type", "")))
            for c in expected]
    got = [(n.upper(), _norm_type(ty)) for n, ty in actual]
    if want != got:
        if len(want) != len(got):
            return (f"column count differs: planned {len(want)}, found {len(got)} "
                    f"(planned {[n for n, _ in want]}, found {[n for n, _ in got]})")
        diffs = [f"position {i + 1}: planned {w[0]} {w[1]}, found {g[0]} {g[1]}"
                 for i, (w, g) in enumerate(zip(want, got)) if w != g]
        return "; ".join(diffs)

    property_diffs: list[str] = []
    for col in expected:
        name = str(col.get("name", "")).upper()
        if nullable is not None and col.get("nullable") is False \
                and nullable.get(name, True):
            property_diffs.append(
                f"{name}: planned NOT NULL, found nullable")
        if comments is not None:
            planned = str(col.get("description") or "")
            found = str(comments.get(name) or "")
            if planned and planned != found:
                property_diffs.append(
                    f"{name}: planned comment {planned!r}, found {found!r}")
    return "; ".join(property_diffs) or None


def create_table_ctas(source: SnowflakeSource, schema: str, name: str,
                      target_catalog: str, target_schema: str) -> str:
    """Empty table whose columns Spark derives from the SOURCE read.

    The source is addressed through `SnowflakeSource`, so this works in
    connector mode (a temp view over the connector read) as well as against
    an external catalog's three-part name. Returns `created`, or
    `already_existed` when the table was there before this run -- CTAS has
    no plan to compare that layout with, so it is reported, not checked.
    """
    fqn = three(target_catalog, target_schema, name)
    if _describe_columns(source.spark, fqn) is not None:
        return "already_existed"
    view = _view_name(schema, name)
    ref = source.register_temp_view(schema, name, view)
    try:
        source.spark.sql(
            f"CREATE TABLE IF NOT EXISTS {fqn} "
            f"USING DELTA AS SELECT * FROM {ref} WHERE 1=0")
    finally:
        source.drop_temp_view(view)
    return "created"


def lit(value: str) -> str:
    """A single-quoted Spark string literal, escaped the way Spark expects.

    Backslash, not doubling: Spark reads `'it\\'\\'s'` as two adjacent
    literals and concatenates them, so a doubled quote silently eats the
    apostrophe. Identical to `target.ddl.quote_spark_string`, which this
    stage cannot import (it is uploaded as a single standalone file).
    """
    escaped = str(value).replace("\\", "\\\\").replace("'", "\\'")
    return "'" + escaped + "'"


def _column_sql(col: dict) -> str:
    """One column of the CREATE TABLE, from one `expected_columns` entry.

    The SAME rules as `target.ddl.render_column_sql`, which wrote the SQL the
    operator approved in DDL_PLAN.md -- a parity test in the engine's suite
    holds the two together. This stage used to render `name type` only, so
    the `NOT NULL` and the COMMENT in the approved SQL were dropped here and
    the comparison below then agreed with itself.
    """
    piece = f'{q(col["name"])} {col["type"]}'
    if col.get("nullable") is False:
        piece += " NOT NULL"
    if col.get("description"):
        piece += " COMMENT " + lit(col["description"])
    return piece


def _cluster_by_sql(features: dict | None) -> str:
    """`CLUSTER BY (...)` from a plan statement's `delta_features`, or ''.

    The SAME rendering as `target.ddl.render_cluster_by`, which wrote the SQL
    the operator approved; a parity test holds the two together.
    """
    # Bare names, as the plan wrote them: AIDP's Delta CLUSTER BY keeps
    # backticks as part of the name (live 2026-09-29).
    keys = (features or {}).get("cluster_by") or []
    return ("CLUSTER BY (" + ", ".join(keys) + ")") if keys else ""


def _tblproperties_sql(features: dict | None) -> str:
    """`TBLPROPERTIES (...)`, as `target.ddl.render_tblproperties`."""
    props = (features or {}).get("tblproperties") or {}
    return ("TBLPROPERTIES (" + ", ".join(
        f"{lit(k)} = {lit(v)}" for k, v in props.items()) + ")") if props else ""


def _table_properties(spark, fqn: str) -> dict[str, str] | None:
    """{key: value} from SHOW TBLPROPERTIES, or None when unreadable."""
    try:
        rows = spark.sql(f"SHOW TBLPROPERTIES {fqn}").collect()
    except Exception:
        return None
    out = {}
    for row in rows:
        try:
            out[str(row["key"])] = str(row["value"])
        except Exception:
            continue
    return out


def _check_features(spark, fqn: str, features: dict | None, existed: bool,
                    notes: list | None) -> None:
    """Say what of the plan's Delta features is on the table, and what is not.

    Properties are read back (SHOW TBLPROPERTIES); a missing or different
    one is a note, never a silent pass. Clustering is not visible to
    DESCRIBE, so it is recorded as applied-in-the-CREATE rather than
    verified. A table that was already there did not get this run's CREATE
    at all, and is not claimed to carry anything it was not seen to carry.
    """
    if notes is None or not features:
        return
    if existed:
        notes.append(
            "the table was already there, so this run's CREATE (with its "
            "CLUSTER BY / TBLPROPERTIES) did not apply to it; the properties "
            "below are what it carries, and clustering is UNCHECKED")
    elif features.get("cluster_by"):
        notes.append(
            f"CLUSTER BY ({', '.join(features['cluster_by'])}) was in the "
            f"CREATE; DESCRIBE does not show clustering, so it is not read "
            f"back here")
    wanted = features.get("tblproperties") or {}
    if not wanted:
        return
    found = _table_properties(spark, fqn)
    if found is None:
        notes.append("TBLPROPERTIES could not be read back, so "
                     + ", ".join(wanted) + " are UNCHECKED")
        return
    for key, value in wanted.items():
        if key not in found:
            notes.append(f"planned {key} = {value}, not found on the table")
        elif str(found[key]).lower() != str(value).lower():
            notes.append(f"planned {key} = {value}, found {found[key]}")


def features_from_ddl_plan(ddl_plan: dict) -> dict[tuple[str, str], dict]:
    """{(source_schema, table): delta_features} for every planned TABLE that
    carries any. Absent from an older plan, which creates as before."""
    out: dict[tuple[str, str], dict] = {}
    for stmt in ddl_plan.get("statements") or []:
        parts = str(stmt.get("source_identifier") or "").split(".")
        if len(parts) != 3 or not stmt.get("delta_features"):
            continue
        if str(stmt.get("object_type") or "TABLE").upper() == "VIEW":
            continue
        out[(parts[1], parts[2])] = stmt["delta_features"]
    return out


def _existing_tables(spark, catalog: str, schema: str) -> set[str] | None:
    """Lower-cased table names in the target schema, from one SHOW TABLES;
    None when it could not be read (then every table is DESCRIBEd, as
    before)."""
    try:
        rows = spark.sql(f"SHOW TABLES IN {q(catalog)}.{q(schema)}").collect()
    except Exception:
        return None
    out = set()
    for r in rows:
        d = {k.lower(): v for k, v in r.asDict().items()}
        out.add(str(d.get("tablename") or d.get("name") or "").lower())
    return out


class _Flusher:
    """Writes a schema's report at most every FLUSH_SECONDS, and on now().

    The whole report used to be rewritten to /Workspace after EVERY table --
    the 90 a reduced plan leaves out included. A resume still finds all but
    the last few seconds' records, and the end of every schema is written.
    """

    FLUSH_SECONDS = 5.0

    def __init__(self, path: pathlib.Path, report: dict):
        self.path, self.report = path, report
        self.last = time.monotonic()

    def now(self) -> None:
        self.report["updated_at"] = datetime.datetime.now(
            datetime.timezone.utc).isoformat()
        self.path.write_text(json.dumps(self.report, indent=2),
                             encoding="utf-8")
        self.last = time.monotonic()

    def maybe(self) -> None:
        if time.monotonic() - self.last >= self.FLUSH_SECONDS:
            self.now()


def create_table_from_columns(spark, columns: list[dict],
                              target_catalog: str, target_schema: str,
                              name: str, description: str = "",
                              notes: list | None = None,
                              features: dict | None = None,
                              exists: bool | None = None) -> str:
    """CREATE TABLE from an explicit column list, then READ IT BACK.

    Types are used verbatim, and so are the plan's `nullable` and
    `description` -- the properties the approved SQL shows. `CREATE TABLE IF
    NOT EXISTS` is a silent no-op on a table that is already there, so the
    CREATE returning is not the claim: the table is DESCRIBEd afterwards and
    compared with the plan, nullability included.
    Returns `created` (it was not there before and now matches),
    `already_existed` (it was there and matches), or raises TypeDrift when
    what is there differs from the plan -- the table is left as found.
    `notes` collects what could NOT be checked, so an unverified property is
    never reported as a verified one.
    """
    if not columns:
        raise ValueError("no column list for this table; rediscover it or "
                         "use --mode ctas")
    fqn = three(target_catalog, target_schema, name)
    # `exists=False` comes from the schema's own listing: the table is known
    # to be absent, so the DESCRIBE that would fail for it is skipped. The
    # CREATE is still read back below, whatever the listing said.
    before = None if exists is False else _describe_columns(spark, fqn)
    if before is None:
        cols = ", ".join(_column_sql(c) for c in columns)
        cluster, properties = _cluster_by_sql(features), _tblproperties_sql(features)
        spark.sql(f"CREATE TABLE IF NOT EXISTS {fqn} ({cols}) USING DELTA"
                  + (f" {cluster}" if cluster else "")
                  + (f" COMMENT {lit(description)}" if description else "")
                  + (f" {properties}" if properties else ""))
        after = _describe_columns(spark, fqn)
        if after is None:
            raise RuntimeError("CREATE TABLE returned but the table does not "
                               "DESCRIBE afterwards; NOT created")
    else:
        after = before

    # Both extra read-backs are skipped when the plan asks for nothing they
    # would check: a table with no NOT NULL and no comments costs exactly
    # what it cost before.
    wants_not_null = any(c.get("nullable") is False for c in columns)
    wants_comments = any(c.get("description") for c in columns)
    nullable = _nullability(spark, fqn) if wants_not_null else None
    comments = _describe_comments(spark, fqn) if wants_comments else None
    # Not a match and not a failure: it was not looked at. Said out loud,
    # because a property reported as applied when nobody checked is the
    # defect this stage is guarding against.
    if notes is not None:
        if wants_not_null and nullable is None:
            notes.append(
                "NOT NULL was requested but could not be verified: the "
                "table's schema could not be read back, so nullability is "
                "UNCHECKED on this table")
        if wants_comments and comments is None:
            notes.append(
                "column COMMENTs were requested but DESCRIBE carried no "
                "comment column, so they are UNCHECKED on this table")
    diff = _compare_columns(columns, after, nullable, comments)
    if diff is None:
        _check_features(spark, fqn, features, before is not None, notes)
        return "created" if before is None else "already_existed"
    if before is None:
        raise TypeDrift(f"created by this run, but it reads back differently "
                        f"from the plan -- {diff}")
    raise TypeDrift(f"already there with a layout the plan did not produce -- "
                    f"{diff}. CREATE TABLE IF NOT EXISTS left it as found; "
                    f"NOT created from the plan")


# Types Snowflake reports but Spark/Delta does not accept verbatim. Their
# presence in a manifest means it was built in CONNECTOR mode, where the
# manifest records SOURCE types on purpose -- translating them here would
# duplicate (and inevitably diverge from) the migrator's own type mapper,
# which refuses ambiguous cases rather than guessing.
_SNOWFLAKE_ONLY_TYPES = ("NUMBER", "TEXT", "VARIANT", "OBJECT", "GEOGRAPHY",
                         "GEOMETRY", "TIMESTAMP_LTZ", "TIMESTAMP_TZ")


def _looks_like_snowflake_types(columns: list[dict]) -> bool:
    return any(str(c.get("type", "")).upper().startswith(t)
               for c in columns for t in _SNOWFLAKE_ONLY_TYPES)


def _snowflake_typed_manifest(manifest: dict, schemas: list[str]) -> str | None:
    """Why this manifest's types are SNOWFLAKE types, or None.

    Decided for the manifest, not per table. The prefix list above only
    catches types Delta rejects; FLOAT, DATE and BOOLEAN pass it, and FLOAT
    is then created 32-bit where Snowflake's is a double: READINGS(READING
    FLOAT) was recorded `created` and the copy narrowed every value to ~7
    digits. Discovery records which mode wrote the manifest, and the
    connector's raw `data_type` field is never written by DESCRIBE.
    """
    mode = (manifest.get("source") or {}).get("mode")
    if mode and mode != "external-catalog":
        return f"it was built in {mode} mode"
    by_name = {s.get("name"): s for s in manifest.get("schemas") or []}
    for schema in schemas:
        for table in (by_name.get(schema) or {}).get("tables") or []:
            if any("data_type" in c for c in table.get("columns") or []):
                return (f"{schema}.{table['name']} carries the connector's "
                        f"raw `data_type` field")
    return None


def columns_from_ddl_plan(ddl_plan: dict) -> dict[tuple[str, str], list[dict]]:
    """{(source_schema, table): [{name, type}]} from the engine's ddl_plan.

    The engine translated these types with its full discipline (it blocks a
    table it cannot map exactly), and the plan was the artifact the user
    signed off, so applying it needs no source read and no cluster-side
    translation.
    """
    out: dict[tuple[str, str], list[dict]] = {}
    for stmt in ddl_plan.get("statements") or []:
        ident = str(stmt.get("source_identifier") or "")
        parts = ident.split(".")
        if len(parts) != 3 or not stmt.get("expected_columns"):
            continue
        if str(stmt.get("object_type") or "TABLE").upper() == "VIEW":
            continue                    # views are not created by this path
        out[(parts[1], parts[2])] = stmt["expected_columns"]
    return out


def targets_from_ddl_plan(ddl_plan: dict
                          ) -> dict[tuple[str, str], tuple[str, str]]:
    """{(source_schema, table): (target_schema, target_name)} from the plan.

    The plan's `target_fqn` IS the approved name. Deriving it again from the
    source schema silently discards `--bronze-catalog-prefix` and
    `--bronze-schema-style`, and creates an object the reviewer never saw.

    A statement without a three-part `target_fqn` contributes nothing: this
    map is only ever used to place an object the plan actually named.
    """
    out: dict[tuple[str, str], tuple[str, str]] = {}
    for stmt in ddl_plan.get("statements") or []:
        source = str(stmt.get("source_identifier") or "").split(".")
        target = str(stmt.get("target_fqn") or "").split(".")
        if len(source) != 3 or len(target) != 3:
            continue
        if str(stmt.get("object_type") or "TABLE").upper() == "VIEW":
            continue
        out[(source[1], source[2])] = (target[1], target[2])
    return out


def catalogs_from_ddl_plan(ddl_plan: dict) -> set:
    """Every catalog the plan targets. More than one, or one that is not
    the catalog this run was given, is the operator's to see."""
    out = set()
    for stmt in ddl_plan.get("statements") or []:
        target = str(stmt.get("target_fqn") or "").split(".")
        if len(target) == 3:
            out.add(target[0])
    return out


def descriptions_from_ddl_plan(ddl_plan: dict) -> dict[tuple[str, str], str]:
    """{(source_schema, table): table COMMENT} from the engine's ddl_plan.

    The source table's COMMENT is in the approved CREATE TABLE, so it is
    applied here too rather than being the one property the reviewer sees
    and the target never gets.
    """
    out: dict[tuple[str, str], str] = {}
    for stmt in ddl_plan.get("statements") or []:
        parts = str(stmt.get("source_identifier") or "").split(".")
        if len(parts) != 3 or not stmt.get("description"):
            continue
        if str(stmt.get("object_type") or "TABLE").upper() == "VIEW":
            continue
        out[(parts[1], parts[2])] = str(stmt["description"])
    return out



def planned_view_facts(ddl_plan: dict) -> dict[tuple[str, str], dict]:
    """{(source_schema, view): plan facts} for every VIEW the plan carries,
    in the plan's statement order (wave order: dependencies first).

    `targets_from_ddl_plan` is tables only, because it places tables. A view
    needs its own CREATE VIEW SQL and the target the plan approved, so it is
    collected separately rather than by widening that map.
    """
    out: dict[tuple[str, str], dict] = {}
    for stmt in ddl_plan.get("statements") or []:
        parts = str(stmt.get("source_identifier") or "").split(".")
        target = str(stmt.get("target_fqn") or "").split(".")
        if len(parts) != 3 or len(target) != 3:
            continue
        if str(stmt.get("object_type") or "TABLE").upper() != "VIEW":
            continue
        out[(parts[1], parts[2])] = {
            "catalog": target[0], "schema": target[1], "name": target[2],
            "target_fqn": ".".join(target), "sql": stmt.get("sql")}
    return out


def planned_schema_targets(ddl_plan: dict) -> dict[str, set]:
    """{source_schema: {target_schema}} over EVERY statement.

    Tables and views both: a schema the plan carries only views for still
    has to be created under the name the plan approved, not the source one.
    """
    out: dict[str, set] = {}
    for stmt in ddl_plan.get("statements") or []:
        parts = str(stmt.get("source_identifier") or "").split(".")
        target = str(stmt.get("target_fqn") or "").split(".")
        if len(parts) == 3 and len(target) == 3:
            out.setdefault(parts[1], set()).add(target[1])
    return out

def create_planned_views(spark, planned: dict, schemas: list[str],
                         reports: pathlib.Path, target_catalog: str, *,
                         dry_run: bool, force: bool) -> int:
    """Every planned view, from the plan's own CREATE VIEW SQL, AFTER every
    table exists: a view in one schema reads tables in others (ANALYTICS
    sorts first and reads COMMERCE). Returns the failure count.

    The outcome goes under the report's `views`, never `objects`: `objects`
    is the TABLE map the copy takes its default scope from, and a view
    recorded there was copied into (live, INSERT INTO a view).

    In the PLAN's order, never sorted: the ddl stage emits statements in
    wave order, so a view follows every view it reads. Sorted, A_SUMMARY
    went before the B_DETAIL it reads and failed TABLE_OR_VIEW_NOT_FOUND,
    one re-run per level of the chain.
    """
    failures = 0
    # One read per schema, kept in memory: re-reading the file this loop has
    # just written is what failed live on the /Workspace mount.
    loaded: dict[str, dict] = {}
    # Written every REPORT_WRITE_INTERVAL seconds and once at the end -- in a
    # `finally`, so a view created before a crash is still on disk -- rather
    # than after every view: a 20,000-table schema's report is several MB.
    # A view the report missed is re-issued by the next run, and CREATE VIEW
    # IF NOT EXISTS makes that a no-op.
    dirty: set[str] = set()
    last_write = time.monotonic()

    def flush() -> None:
        for s in sorted(dirty):
            loaded[s]["updated_at"] = datetime.datetime.now(
                datetime.timezone.utc).isoformat()
            _report_path(reports, s).write_text(
                json.dumps(loaded[s], indent=2), encoding="utf-8")
        dirty.clear()

    try:
        for (schema, name), fact in planned.items():
            if schema not in schemas:
                continue
            path = _report_path(reports, schema)
            target = f'{target_catalog}.{fact["schema"]}'
            if schema not in loaded:
                loaded[schema] = _load_report(path, schema, target)
            report = loaded[schema]
            report["target"] = target
            views = report.setdefault("views", {})
            if views.get(name, {}).get("status") == "created" and not force:
                log(f"skip view {schema}.{name}: already created")
                continue
            try:
                if not fact["sql"]:
                    raise ValueError("the approved plan carries no CREATE VIEW "
                                     "SQL for this view")
                if dry_run:
                    log(f'DRY RUN: would create view {fact["target_fqn"]}')
                    status = "dry_run"
                else:
                    spark.sql(fact["sql"])
                    status = "created"
                views[name] = {"status": status, "in_plan": True,
                               "target_fqn": fact["target_fqn"]}
                log(f"view {schema}.{name} -> {fact['target_fqn']}: {status}")
            except Exception as exc:
                failures += 1
                views[name] = {"status": "failed", "in_plan": True,
                               "target_fqn": fact["target_fqn"],
                               "reason": str(exc)[:400]}
                log(f"view {schema}.{name}: FAILED — {str(exc)[:200]}")
            dirty.add(schema)
            if time.monotonic() - last_write >= REPORT_WRITE_INTERVAL:
                flush()
                last_write = time.monotonic()
    finally:
        flush()
    return failures


def snapshots_from_ddl_plan(ddl_plan: dict) -> dict[tuple[str, str], str]:
    """{(source_schema, name): snapshot_of} for every TABLE statement that
    migrates a dynamic table or materialized view as a table snapshot.

    The plan's statement decides that it is a table; the manifest may file
    the source under `views` (a materialized view) or not list it at all.
    """
    out: dict[tuple[str, str], str] = {}
    for stmt in ddl_plan.get("statements") or []:
        parts = str(stmt.get("source_identifier") or "").split(".")
        if len(parts) != 3 or not stmt.get("snapshot_of"):
            continue
        if str(stmt.get("object_type") or "TABLE").upper() == "VIEW":
            continue
        out[(parts[1], parts[2])] = str(stmt["snapshot_of"])
    return out


def views_from_ddl_plan(ddl_plan: dict) -> set[tuple[str, str]]:
    """{(source_schema, view)} for every VIEW statement in the plan."""
    out: set[tuple[str, str]] = set()
    for stmt in ddl_plan.get("statements") or []:
        parts = str(stmt.get("source_identifier") or "").split(".")
        if len(parts) == 3 and str(stmt.get("object_type") or "").upper() == "VIEW":
            out.add((parts[1], parts[2]))
    return out


# In --mode ddl-plan the plan's views are created from its own CREATE VIEW
# SQL (create_planned_views). --mode ctas and --mode manifest have no plan
# and create TABLES only, so their views are the catalog path's job
# (`snowmig deploy --execute`). Every manifest view is still LISTED, so a
# view can never be absent from every report with exit 0.
VIEW_NOT_CREATED = ("views are not created by --mode ctas or --mode "
                    "manifest (tables only); create them with --mode "
                    "ddl-plan or `snowmig deploy --execute` and verify "
                    "them against the source")
VIEW_NOT_IN_PLAN = ("the approved ddl_plan carries no CREATE VIEW for this "
                    "view -- the engine either blocked it or it was outside "
                    "the plan's scope. NOT created.")


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--source-mode", choices=list(SOURCE_MODES),
                    default="connector",
                    help="how to READ the source when --mode ctas derives "
                         "types from it (see snowmig_source.py)")
    ap.add_argument("--source-config",
                    help="JSON/YAML connection config (connector mode)")
    ap.add_argument("--source-catalog",
                    help="the registered EXTERNAL catalog "
                         "(external-catalog mode)")
    ap.add_argument("--target-catalog", required=True)
    ap.add_argument("--schema", action="append", default=None,
                    help="repeatable; default: every schema in the manifest")
    ap.add_argument("--target-schema", default=None,
                    help="override the target schema name (single --schema "
                         "runs only); default mirrors the source")
    ap.add_argument("--mode", choices=("ddl-plan", "ctas", "manifest"),
                    default="ddl-plan")
    ap.add_argument("--ddl-plan",
                    help="path to ddl_plan.json (default: ../plan/"
                         "ddl_plan.json next to --reports-dir)")
    ap.add_argument("--output-dir", default=DEFAULT_OUTPUT_DIR,
                    help="where this step saves its values (report/output in "
                         "the workspace); '' to skip")
    ap.add_argument("--reports-dir", default=DEFAULT_REPORTS_DIR)
    ap.add_argument("--dry-run", action="store_true",
                    help="print every statement; execute nothing")
    ap.add_argument("--force", action="store_true",
                    help="re-check tables the report already records as "
                         "created or already_existed")
    ap.add_argument("--parallel", type=int, default=None,
                    help=f"tables created at once (default "
                         f"{DEFAULT_PARALLEL}; --mode ctas defaults to 1, "
                         f"since each CTAS is a full Snowflake read; 1 = one "
                         f"after another). Views are always created after "
                         f"every table, one at a time, in the plan's order")
    args = ap.parse_args(argv)
    args.parallel = effective_parallel(args.parallel, args.mode)

    if args.parallel < 1:
        return fail(f"error: --parallel {args.parallel}: at least 1 (1 creates "
                    f"the tables one after another)")

    if args.source_catalog and \
            args.source_catalog.lower() == args.target_catalog.lower():
        return fail("error: source and target catalog are the same. The source is "
              "the read-only EXTERNAL catalog; the target must be an INTERNAL "
              "one.")
    if args.target_schema and len(args.schema or []) != 1:
        return fail("error: --target-schema needs exactly one --schema")

    reports = pathlib.Path(args.reports_dir)
    manifest = json.loads((reports / MANIFEST_NAME).read_text(encoding="utf-8"))
    by_name = {s["name"]: s for s in manifest["schemas"]}
    schemas = args.schema or sorted(by_name)

    if args.mode == "manifest":
        why = _snowflake_typed_manifest(manifest, schemas)
        if why:
            return fail(
                f"error: --mode manifest needs a manifest of SPARK types, and "
                f"this one records SNOWFLAKE types ({why}; 00_discover's "
                f"default connector mode writes them on purpose). Delta "
                f"rejects some of them and accepts others with a different "
                f"meaning -- FLOAT would be created 32-bit -- so nothing was "
                f"created. Use --mode ddl-plan (engine-translated types) or "
                f"--mode ctas.")

    from pyspark.sql import SparkSession
    spark = SparkSession.builder.getOrCreate()

    planned_columns: dict = {}
    planned_descriptions: dict = {}
    planned_features: dict = {}
    view_facts: dict = {}
    planned_schemas: dict = {}
    planned_views: set | None = None
    planned_targets: dict = {}
    planned_snapshots: dict = {}
    if args.mode == "ddl-plan":
        ddl_path = (pathlib.Path(args.ddl_plan) if args.ddl_plan
                    else reports.parent / "plan" / "ddl_plan.json")
        if not ddl_path.is_file():
            return fail(f"--mode ddl-plan needs ddl_plan.json; {ddl_path} is "
                        f"not there. Run the migrator's `ddl` stage and let "
                        f"`provision` upload it, or pass --ddl-plan")
        try:
            ddl_plan = read_plan_json(ddl_path)
        except ValueError as exc:
            return fail(str(exc))
        planned_columns = columns_from_ddl_plan(ddl_plan)
        planned_descriptions = descriptions_from_ddl_plan(ddl_plan)
        planned_features = features_from_ddl_plan(ddl_plan)
        planned_views = views_from_ddl_plan(ddl_plan)
        planned_targets = targets_from_ddl_plan(ddl_plan)
        planned_snapshots = snapshots_from_ddl_plan(ddl_plan)
        view_facts = planned_view_facts(ddl_plan)
        planned_schemas = planned_schema_targets(ddl_plan)
        # A plan for another catalog is a different migration. Creating its
        # tables here under this run's catalog would be the same silent
        # substitution this map exists to stop.
        plan_catalogs = catalogs_from_ddl_plan(ddl_plan)
        stray = {c for c in plan_catalogs
                 if c.lower() != str(args.target_catalog).lower()}
        if stray:
            return fail(
                f"error: the approved plan targets catalog(s) "
                f"{', '.join(sorted(stray))}, and this run was given "
                f"--target-catalog {args.target_catalog}. Creating the "
                f"plan's tables somewhere it does not name would be exactly "
                f"the substitution the plan exists to prevent. Re-run `plan "
                f"--bronze-catalog-prefix {args.target_catalog}` and `ddl` "
                f"so the plan names this catalog, or point this run at the "
                f"one the plan names.")
        log(f"ddl plan: {len(planned_columns)} table(s) with engine-"
            f"translated types, from {ddl_path}"
            + (f"; {len(view_facts)} view(s) it carries are created from "
               f"its own CREATE VIEW SQL after every table" if view_facts
               else ""))

    source = None
    if args.mode == "ctas":
        try:
            config = (load_source_config(args.source_config)
                      if args.source_config else None)
            source = SnowflakeSource(spark, mode=args.source_mode,
                                     config=config,
                                     external_catalog=args.source_catalog)
        except SourceConfigError as exc:
            return fail(f"error: {exc}")

    failures = 0
    # Per-schema status counts, for the run's step output.
    summary: dict[str, dict] = {}
    # Run-wide, not per schema: a canary plan scoped to one schema
    # legitimately leaves every other schema all-`not_in_plan`.
    created_total = 0
    not_in_plan_total = 0
    for schema in schemas:
        record = by_name.get(schema)
        if record is None:
            return fail(f"error: schema {schema!r} is not in the manifest; run "
                  f"00_discover first")
        # The approved plan decides the target namespace. Only where the
        # plan is silent (ctas/manifest mode, or a table it does not carry)
        # does the source schema name stand in.
        planned_for_schema = {
            tgt_schema for (src_schema, _t), (tgt_schema, _n)
            in planned_targets.items() if src_schema == schema}
        # A schema the plan carries only VIEWS for still has an approved
        # target name; without this it would be created as the source name.
        planned_for_schema |= planned_schemas.get(schema, set())
        if args.target_schema:
            target_schema = args.target_schema
            if planned_for_schema and target_schema not in planned_for_schema:
                return fail(
                    f"error: --target-schema {target_schema!r} contradicts "
                    f"the approved plan, which puts {schema} in "
                    f"{', '.join(sorted(planned_for_schema))}. The plan is "
                    f"the reviewed artifact; change it, or drop the flag.")
        elif len(planned_for_schema) == 1:
            target_schema = next(iter(planned_for_schema))
        elif len(planned_for_schema) > 1:
            return fail(
                f"error: the approved plan puts source schema {schema} in "
                f"more than one target schema "
                f"({', '.join(sorted(planned_for_schema))}); this stage "
                f"creates one schema per run. Pass --target-schema to say "
                f"which.")
        else:
            target_schema = schema
        path = _report_path(reports, schema)
        target = f"{args.target_catalog}.{target_schema}"
        report = _load_report(path, schema, target)
        # Which mode wrote the statuses loaded above, for records that
        # predate the per-object `mode`.
        prior_mode = report.get("mode")
        report["target"] = target
        report["mode"] = args.mode

        schema_sql = (f"CREATE SCHEMA IF NOT EXISTS "
                      f"{q(args.target_catalog)}.{q(target_schema)}")
        if args.dry_run:
            log(f"DRY RUN: {schema_sql}")
        else:
            spark.sql(schema_sql)

        # Which target tables are already there, from ONE listing for the
        # schema: a table known to be absent skips its before-DESCRIBE, the
        # call that fails -- slowly -- for every table a first run creates.
        # None (listing unreadable, or a dry run) keeps the per-table look.
        existing = (None if args.dry_run else
                    _existing_tables(spark, args.target_catalog,
                                     target_schema))
        flush = _Flusher(path, report)

        def build(table: dict, schema=schema, target_schema=target_schema,
                  existing=existing) -> tuple[dict, str]:
            """One table's create-and-read-back: `(record, kind)`, kind one
            of created / not_in_plan / failure / other. Runs in a worker
            thread; it never raises, and it never touches the report --
            the main thread records every outcome in the manifest's order."""
            name = table["name"]
            started = time.monotonic()
            notes: list[str] = []
            try:
                if args.dry_run:
                    log(f"DRY RUN: would create "
                        f"{args.target_catalog}.{target_schema}.{name} "
                        f"({args.mode})")
                    status = "dry_run"
                elif args.mode == "ctas":
                    status = create_table_ctas(source, schema, name,
                                               args.target_catalog,
                                               target_schema)
                elif args.mode == "ddl-plan":
                    columns = planned_columns.get((schema, name))
                    if not columns:
                        # Counted, not logged one line each: live, a 4-table
                        # plan over a 1000-table estate printed 996 such
                        # lines and buried the four that mattered. The
                        # schema's summary line carries the count, and its
                        # report lists every name.
                        return ({"status": "not_in_plan",
                                 "reason": "the approved ddl_plan carries no "
                                           "columns for this table -- the "
                                           "engine either blocked it or it "
                                           "was outside the plan's scope. "
                                           "NOT created."}, "not_in_plan")
                    # The plan names the target TABLE as well as the
                    # schema: a source `ORDERS` planned as `orders` has to
                    # land as `orders`, or the copy addresses a table that
                    # is not there.
                    tgt_name = (planned_targets.get((schema, name))
                                or (target_schema, name))[1]
                    status = create_table_from_columns(
                        spark, columns, args.target_catalog, target_schema,
                        tgt_name,
                        description=planned_descriptions.get((schema, name),
                                                             ""),
                        notes=notes,
                        features=planned_features.get((schema, name)),
                        exists=(None if existing is None
                                else tgt_name.lower() in existing))
                else:
                    columns = table.get("columns") or []
                    if _looks_like_snowflake_types(columns):
                        raise ValueError(
                            "this manifest carries SNOWFLAKE types (it was "
                            "built in connector mode), which Delta will not "
                            "accept verbatim. Use --mode ddl-plan (engine-"
                            "translated types) or --mode ctas.")
                    status = create_table_from_columns(
                        spark, columns, args.target_catalog, target_schema,
                        name, notes=notes,
                        exists=(None if existing is None
                                else name.lower() in existing))
                tgt_name = (planned_targets.get((schema, name))
                            or (target_schema, name))[1]
                entry = {"status": status, "mode": args.mode,
                         "target_fqn": f"{args.target_catalog}."
                                       f"{target_schema}.{tgt_name}"}
                if notes:
                    # Properties that could NOT be read back. Recorded next
                    # to the status so "created" never implies "and every
                    # property was checked".
                    entry["unverified_properties"] = notes
                if args.mode == "ctas" and status == "already_existed":
                    # CTAS has no plan to compare the layout with: the
                    # table was there before this run and nobody has
                    # checked it. Said here, so the copy scope and the
                    # reconcile report carry it rather than a bare pass.
                    entry["reason"] = (
                        "there before this run; --mode ctas has no plan "
                        "to compare its layout with, so the layout was "
                        "NOT compared")
                elapsed = time.monotonic() - started
                entry["elapsed_s"] = round(elapsed, 1)
                # The elapsed time is what shows a create's real cost: the
                # report had none.
                log(f"{schema}.{name}: {status} ({elapsed:.1f}s)")
                return entry, ("created" if status in ("created",
                                                       "already_existed")
                               else "other")
            except TypeDrift as exc:
                # A problem state, not a failure of THIS run: the table is
                # there, it is not what the plan says, and a positional copy
                # into it would land rows in the wrong columns with matching
                # counts. Re-checked on every run until it matches.
                log(f"{schema}.{name}: TYPE DRIFT — {str(exc)[:200]}")
                return ({"status": "type_drift", "reason": str(exc)[:400]},
                        "failure")
            except Exception as exc:
                log(f"{schema}.{name}: FAILED — {str(exc)[:200]}")
                return ({"status": "failed", "reason": str(exc)[:400]},
                        "failure")

        # The plan's table snapshots are tables, wherever the manifest put
        # them: a materialized view is under its `views`, and one the
        # manifest does not list at all is still in the approved plan.
        snapshots = {n: label for (s, n), label in planned_snapshots.items()
                     if s == schema}
        listed = {t["name"] for t in record["tables"]}
        tables = list(record["tables"]) + [
            {"name": n, "columns": []} for n in sorted(snapshots)
            if n not in listed]
        if snapshots:
            log(f"{schema}: {len(snapshots)} table snapshot(s) in the plan "
                f"created as tables ({', '.join(sorted(snapshots)[:5])}"
                f"{', ...' if len(snapshots) > 5 else ''})")

        work = []
        for table in tables:
            name = table["name"]
            prior = report["objects"].get(name, {})
            done = prior.get("status") in ("created", "already_existed")
            # Every report this stage writes names its mode; one that does
            # not was not written by it, and is taken as this run's.
            recorded_by = prior.get("mode", prior_mode) or args.mode
            if done and not args.force and recorded_by == args.mode:
                log(f"skip {schema}.{name}: already {prior['status']}")
                created_total += 1
                continue
            if done and not args.force:
                # Another mode's `created` checked that mode's layout, not
                # this one's: a FLOAT table --mode manifest made was skipped
                # here as done, with the plan saying DOUBLE.
                log(f"re-check {schema}.{name}: recorded {prior['status']} "
                    f"by --mode {recorded_by}, not {args.mode}")
            work.append(table)

        # `--parallel` tables at a time, a chunk at a time, so the report on
        # disk never lags far behind what was created. A dry run creates
        # nothing and stays one at a time.
        parallel = 1 if args.dry_run else args.parallel
        for start in range(0, len(work), STRUCTURE_CHUNK):
            chunk = work[start:start + STRUCTURE_CHUNK]
            for table, (entry, kind) in zip(
                    chunk, _in_parallel(build, chunk, parallel)):
                if table["name"] in snapshots:
                    # Said on the record, so the copy and the reconcile
                    # take it for the table it is, not the view the
                    # manifest lists.
                    entry = {**entry, "kind": "TABLE",
                             "snapshot_of": snapshots[table["name"]]}
                report["objects"][table["name"]] = entry
                if kind == "created":
                    created_total += 1
                elif kind == "not_in_plan":
                    not_in_plan_total += 1
                elif kind == "failure":
                    failures += 1
            # At most every FLUSH_SECONDS, checked once per chunk, and in
            # full at the end of the schema. Live, the scale run's 1,000-
            # table schema rewrote this growing file to /Workspace after
            # every table -- the 950 outside the plan included -- and that,
            # not the CREATE, was the per-table cost. Creates are
            # idempotent, so a crash loses nothing a re-run does not redo.
            flush.maybe()
        flush.now()

        # Views: kept apart from `objects` so the table tally, the resume
        # logic and the copy scope stay table-only. In --mode ddl-plan the
        # planned ones are created after every table (create_planned_views
        # records each outcome here); every other manifest view is listed
        # with why it was not created.
        prior_views = dict(report.get("views") or {})
        # A report written while views were still recorded in `objects`:
        # moved, or the copy keeps taking them for tables.
        for stale in [n for n, o in report["objects"].items()
                      if str(o.get("kind") or "").upper() == "VIEW"]:
            prior_views.setdefault(stale, report["objects"].pop(stale))
        views = [v["name"] for v in record.get("views") or []
                 if v["name"] not in snapshots]
        views += [n for (s, n) in sorted(view_facts)
                  if s == schema and n not in views]
        if views or prior_views:
            report["views"] = {}
            unplanned = []
            for view in views:
                if args.mode != "ddl-plan":
                    entry = {"status": "not_created_by_this_path",
                             "reason": VIEW_NOT_CREATED}
                elif (schema, view) in view_facts:
                    # Its outcome is written when it is created, below; a
                    # `created` one from an earlier run is kept for resume.
                    entry = dict(prior_views.get(view)
                                 or {"status": "not_attempted"})
                    entry["in_plan"] = True
                else:
                    entry = {"status": "not_in_plan", "reason": VIEW_NOT_IN_PLAN,
                             "in_plan": (schema, view) in (planned_views
                                                           or set())}
                    unplanned.append(view)
                report["views"][view] = entry
            path.write_text(json.dumps(report, indent=2), encoding="utf-8")
            if args.mode != "ddl-plan" and views:
                log(f"{schema}: {len(views)} view(s) in the manifest are NOT "
                    f"created by --mode {args.mode} ({', '.join(views[:5])}"
                    f"{', ...' if len(views) > 5 else ''}); use --mode "
                    f"ddl-plan or `snowmig deploy --execute` for views")
            elif unplanned:
                log(f"{schema}: {len(unplanned)} view(s) in the manifest are "
                    f"not in the approved plan and are NOT created "
                    f"({', '.join(unplanned[:5])}"
                    f"{', ...' if len(unplanned) > 5 else ''})")

        counts = {}
        for obj in report["objects"].values():
            counts[obj["status"]] = counts.get(obj["status"], 0) + 1
        log(f"{schema}: {counts} -> {path}")
        summary[schema] = counts

    if args.mode == "ddl-plan" and view_facts:
        failures += create_planned_views(
            spark, view_facts, schemas, reports, args.target_catalog,
            dry_run=args.dry_run, force=args.force)

    log(f"run: created or already there {created_total}, not in plan "
        f"{not_in_plan_total}, failed or drifted {failures}")
    error = None
    if not failures and args.mode == "ddl-plan" and not args.dry_run \
            and not created_total and not_in_plan_total:
        # Every per-table record above is right; the RUN still did nothing.
        # Exit 0 here gave three SUCCESS jobs (structure, copy, reconcile)
        # for a plan that never overlapped the requested schema.
        error = (f"error: created 0 table(s); {not_in_plan_total} were not "
                 f"in the approved plan ({ddl_path}). The plan and the "
                 f"requested schema(s) do not overlap -- is this the "
                 f"ddl_plan.json for THIS estate and wave? Nothing was "
                 f"created, so 02_copy_schema has nothing to copy.")

    # Written by EVERY run that got this far, failed ones included: it used
    # to be written only on success, so a failed re-run left the previous
    # run's `failures: 0` in report/output beside this run's exit 1.
    write_step_output(args.output_dir, "S10_structure.json", {
        "step": "S10", "stage": "structure", "mode": args.mode,
        "target_catalog": args.target_catalog, "schemas": summary,
        "views": len(view_facts), "failures": failures,
        "outcome": ("failed" if failures else
                    "created_nothing" if error else "ok"),
        **({"error": error} if error else {})})
    if error:
        return fail(error)
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
