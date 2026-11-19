"""Spark/Delta DDL generation. Pure functions, zero I/O.

Every transformation is attributable to a named rule, and every dropped property
is recorded. A migration is reviewable only if each change to a schema can be
traced to the rule that made it, so the audit trail is part of the output rather
than a debugging aid.

Two AIDP-specific behaviours are encoded here rather than rediscovered:
  * CREATE SCHEMA ... COMMENT silently fails to persist -- specifically when the
    comment contains ISO-timestamp colons -- so COMMENT is never emitted on a
    schema create.
  * USING DELTA is always explicit, so the managed-table format does not depend
    on a cluster default.

THE SQL AND THE COLUMN LIST ARE RENDERED FROM ONE SPEC. `expected_columns` is
what both execution paths apply -- the catalog-API body is built from it and
the in-AIDP structure notebook renders `CREATE TABLE` from it -- so it carries
every property the reviewed SQL shows, and the SQL is written from the same
dicts (`_column_spec` -> `render_column_sql`). It used to carry name and type
alone, which meant the `NOT NULL` and the column `COMMENT` an operator
approved in DDL_PLAN.md were dropped by BOTH appliers and the structure
verification then compared the same reduced shape and reported "verified".
A property one path cannot apply is named per object in `rules_applied`, which
is where DDL_PLAN.md lists them -- never silently reduced.
"""
from __future__ import annotations

import re
from dataclasses import asdict, dataclass, field

from snowflake_source.dialect import lexer
from snowflake_source.dialect.types import copy_expressions, source_type_key
from snowflake_source.dialect.views import (  # noqa: F401  (re-exported)
    detect_unsupported_constructs, extract_view_body, extract_view_columns,
    translate_view_body,
)

__all__ = ["RuleApplication", "RewriteResult", "UnsupportedDDL",
           "SCRUBBED_PROPERTIES", "DEFERRED_EQUIVALENT_PROPERTIES", "build_create_schema", "quote_backtick", "quote_spark_string", "build_create_table",
           "build_create_view", "build_ddl_payload",
           "TARGET_REJECTED_COLUMN_TYPES", "unsupported_target_types",
           "render_column_sql", "uncarried_column_facts",
           "describe_constraints", "classify_table_properties",
           "render_cluster_by", "render_tblproperties", "copy_spec"]

# COLUMN TYPES THE TARGET REJECTS AT `CREATE TABLE`, LIVE-VERIFIED.
#
# Not a style list and not a portability opinion: each entry is a type the
# AIDP metastore refused on a real run, paired with the remedy that cleared
# that refusal on the same estate. They are caught HERE -- in an offline stage, in under a
# second -- because the alternative is finding out from a job run, five to six
# minutes of cluster startup later, with the failure buried in a Java
# traceback partway down a thousand-line log.
#
# `TIMESTAMP_NTZ` is genuinely supported by Delta and by Spark 3.4+; it is the
# HIVE METASTORE behind the catalog that refuses it, with
# `InvalidObjectException: Invalid column type: timestamp_ntz`. So the type is
# not "wrong" -- it simply cannot be declared to this catalog, which is why
# the remedy is a documented mapping flag rather than a fix to the mapper.
TARGET_REJECTED_COLUMN_TYPES: dict[str, str] = {
    "TIMESTAMP_NTZ":
        "the AIDP Hive metastore refuses it with `InvalidObjectException: "
        "Invalid column type: timestamp_ntz`. "
        "Re-run `ddl --timestamp-ntz timestamp` (offline, re-mapped from "
        "inventory.json, no Snowflake re-read), or `assess`/`ingest` with "
        "`--timestamp-ntz timestamp` if INVENTORY.md should show the "
        "downgraded type too. That is a "
        "SEMANTIC DOWNGRADE, not a rename: Spark TIMESTAMP is an instant read "
        "through the session timezone, while TIMESTAMP_NTZ is wall-clock with "
        "no zone, so the same value can read back differently under a "
        "different session. The mapper records it as a warning for that "
        "reason -- decide it, do not inherit it.",
}


def unsupported_target_types(statements: list[dict]) -> list[dict]:
    """Column types in `statements` that the target will refuse.

    Reads what this plan would actually DECLARE, after every mapping flag has
    been applied: the statement's expected_columns, which build_ddl_plan fills
    from the same target_type the SQL was written from, or -- for a caller that
    hands over raw SQL -- the emitted text itself.
    """
    found: list[dict] = []
    for stmt in statements:
        sql = stmt.get("sql") or ""
        # Views carry their SOURCE column types in expected_columns and declare
        # none in their SQL, so they are out regardless of the path taken.
        if "CREATE TABLE" not in sql.upper():
            continue
        columns = stmt.get("expected_columns") or []
        for type_name, remedy in TARGET_REJECTED_COLUMN_TYPES.items():
            if columns:
                hits = [c["name"] for c in columns
                        if str(c.get("type") or "").upper() == type_name]
            else:
                # Anchored on the backtick-quoted column the emitter writes, so
                # a type NAMED in a comment or a rule note is not a false
                # positive. Any character may appear inside the backticks --
                # `(\w+)` missed every name with a space, hyphen or dot, and
                # truncated one with an embedded (doubled) backtick.
                hits = [h.replace("``", "`") for h in re.findall(
                    r"`((?:[^`]|``)+)`\s+" + re.escape(type_name) + r"\b", sql)]
            if hits:
                found.append({"target_fqn": stmt.get("target_fqn"),
                              "source_identifier": stmt.get(
                                  "source_identifier"),
                              "type": type_name, "columns": hits,
                              "remedy": remedy})
    return found

# Snowflake table PROPERTIES that genuinely have NO AIDP equivalent. A value
# here was a deliberate source-side setting, so dropping it is a decision worth
# reporting -- but it is a decision with nowhere to go.
SCRUBBED_PROPERTIES = (
    "is_iceberg", "is_dynamic", "is_secure",
    "max_data_extension_time_in_days",
    # MAINTENANCE.md flags it ("no equivalent; point-lookup performance ...
    # will regress"); left out of this list, DDL_PLAN.md and SUMMARY.md
    # scored the same table "no properties dropped".
    "search_optimization",
)

# Properties that DO have an AIDP equivalent. They were once reported as
# "dropped, no Delta equivalent", which is false for every one of them.
#
# Since probe 1 (live 2026-09-29, AIDP Delta 3.1.0: CLUSTER BY, the retention
# TBLPROPERTIES and delta.enableChangeDataFeed all created, and table_changes
# read the feed back) each is CARRIED into the CREATE TABLE where Delta can
# express the source's value -- see `classify_table_properties`. What cannot
# be carried (an expression clustering key, a key Delta cannot cluster on) is
# still DEFERRED with its reason and this equivalent. No OPTIMIZE / VACUUM is
# emitted: scheduling them is the customer's (references/maintenance-and-
# layout.md).
DEFERRED_EQUIVALENT_PROPERTIES = {
    "cluster_by": (
        "Delta liquid clustering (`CLUSTER BY`) or `OPTIMIZE … ZORDER BY`. "
        "Neither is automatic: Snowflake reclusters in the background, AIDP "
        "needs a scheduled job"),
    "retention_time": (
        "`delta.deletedFileRetentionDuration` + `delta.logRetentionDuration`, "
        "which bound how far `VERSION AS OF` / `TIMESTAMP AS OF` can reach"),
    "data_retention_time_in_days": (
        "`delta.deletedFileRetentionDuration` + `delta.logRetentionDuration` "
        "(same setting as retention_time)"),
    "change_tracking": (
        "Delta Change Data Feed (`delta.enableChangeDataFeed`)"),
}

# Observational SHOW metadata. Never emitted either, but it was never a property
# to preserve, so listing it as "dropped" is misleading noise in the report.
_INFORMATIONAL_METADATA = ("rows", "bytes", "created_on", "owner", "comment")

# Values that mean "this property is not set" and so are not worth reporting.
_UNSET = (None, "", "false", "FALSE", "N", "OFF", "null", "NULL")


@dataclass(frozen=True)
class RuleApplication:
    rule_id: str
    detail: str


@dataclass
class RewriteResult:
    source_identifier: str
    target_fqn: str
    sql: str | None
    rules_applied: list[RuleApplication] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    omitted_properties: list[str] = field(default_factory=list)
    blocked: bool = False
    blocked_reason: str | None = None
    # The source object's COMMENT, carried so the transports can apply it.
    # Empty string, never None: it is passed straight to the catalog API's
    # `description`, which is a string field.
    description: str = ""
    # The columns this statement intends to create, in order. Carried so that
    # deployment can verify the STRUCTURE that arrived rather than only that
    # something with the right name exists.
    expected_columns: list[dict] = field(default_factory=list)
    # Source settings with a real AIDP equivalent that this version does not
    # apply. Distinct from omitted_properties, which have nowhere to go.
    deferred_properties: list[dict] = field(default_factory=list)
    # How the copy reads and converts each column: see
    # `copy_spec`. Tables only; a view is not copied.
    copy_columns: list[dict] = field(default_factory=list)
    # Source settings carried INTO this CREATE TABLE, and the Delta clauses
    # that carry them: {"cluster_by": [...], "tblproperties": {...}}. The
    # structure notebook applies the same dict.
    carried_properties: list[dict] = field(default_factory=list)
    delta_features: dict = field(default_factory=dict)


class UnsupportedDDL(Exception):
    def __init__(self, rule_id: str, message: str):
        self.rule_id = rule_id
        super().__init__(f"{rule_id}: {message}")


def quote_backtick(identifier: str) -> str:
    """A backtick-quoted Spark identifier, with embedded backticks doubled."""
    return "`" + identifier.replace("`", "``") + "`"


def quote_spark_string(value: str) -> str:
    """A single-quoted Spark string literal, escaped the way Spark expects.

    Spark escapes with a BACKSLASH. Doubling the quote -- correct in Snowflake
    and in standard SQL -- is not an escape here: Spark reads `\'it\'\'s\'` as
    two adjacent literals and concatenates them, so a comment of "Customer's
    orders" silently became "Customers orders". Backslash first, so an escape
    we add is not itself re-escaped.
    """
    escaped = str(value).replace("\\", "\\\\").replace("'", "\\'")
    return "'" + escaped + "'"


_q = quote_backtick


def _qualify(fqn: str) -> str:
    parts = fqn.split(".")
    if len(parts) != 3:
        raise ValueError(f"target_fqn must be three-part catalog.schema.table: {fqn!r}")
    return ".".join(_q(p) for p in parts)


def build_create_schema(catalog: str, schema: str) -> str:
    # R14: no COMMENT here, ever. See module docstring.
    return f"CREATE SCHEMA IF NOT EXISTS {_q(catalog)}.{_q(schema)}"


def _column_spec(c: dict) -> dict:
    """The one description of a planned column, for SQL and for both appliers.

    `nullable` and `description` are the properties the reviewed SQL shows;
    they are part of the spec so no applier can render less than the plan
    without the comparison catching it.
    """
    return {"name": c["COLUMN_NAME"], "type": c["target_type"],
            "nullable": str(c.get("IS_NULLABLE", "YES")).upper() != "NO",
            "description": c.get("COMMENT") or None}


def render_column_sql(spec: dict) -> str:
    """One column of a CREATE TABLE, from a spec entry.

    The in-AIDP structure notebook renders the same spec with the same rules
    (`_column_sql` in dataplane/01_create_structure.py); a parity test holds
    the two together, because the defect this fixes was exactly the two
    drifting apart.
    """
    piece = f'{_q(spec["name"])} {spec["type"]}'
    if not spec.get("nullable", True):
        piece += " NOT NULL"
    if spec.get("description"):
        piece += " COMMENT " + quote_spark_string(spec["description"])
    return piece


# Column facts Snowflake holds that the AIDP target REFUSES.
#
# Live 2026-09-29 (probe 1, AIDP Spark 3.5.0 / Delta 3.1.0): a column DEFAULT
# failed with "[UNSUPPORTED_FEATURE.TABLE_OPERATION] ... does not support
# column default value", and both `GENERATED BY DEFAULT AS IDENTITY` and
# `GENERATED ALWAYS AS IDENTITY` failed with [PARSE_SYNTAX_ERROR]. The catalog
# API body has no field for either (`build_table_body`, catalog_api.py). So
# they are CAPTURED and WARNED, never emitted: after cutover an insert
# Snowflake would have populated arrives NULL or fails, and that has to be a
# decision somebody made rather than something they discover.
_DEFAULT_REFUSED = (
    "AIDP's Delta 3.1 refuses a column default: CREATE TABLE fails with "
    "\"does not support column default value\" (live-verified 2026-09-29)")
_IDENTITY_REFUSED = (
    "AIDP's Delta 3.1 refuses GENERATED ... AS IDENTITY with "
    "[PARSE_SYNTAX_ERROR] (live-verified 2026-09-29)")
_PK_REFUSED = (
    "AIDP's Delta 3.1 refuses PRIMARY KEY in CREATE TABLE with "
    "[PARSE_SYNTAX_ERROR] (live-verified 2026-09-29)")


def _column_fact_pairs(record: dict) -> list[tuple[str, str, str]]:
    """`(kind, column, sentence)` for every DEFAULT / IDENTITY on a record."""
    out: list[tuple[str, str, str]] = []
    for c in sorted(record.get("columns") or [],
                    key=lambda c: c.get("ORDINAL_POSITION") or 0):
        name = c.get("COLUMN_NAME")
        default = c.get("COLUMN_DEFAULT")
        start, step = c.get("IDENTITY_START"), c.get("IDENTITY_INCREMENT")
        if default not in (None, ""):
            out.append(("DEFAULT", str(name),
                f"{name}: column DEFAULT {default} is NOT carried to the "
                f"target: {_DEFAULT_REFUSED}. After cutover an INSERT that "
                f"omits this column arrives NULL (or fails, if the column is "
                f"NOT NULL) where Snowflake would have supplied the default; "
                f"the writer has to supply it."))
        if start not in (None, "") or step not in (None, ""):
            out.append(("IDENTITY", str(name),
                f"{name}: IDENTITY / AUTOINCREMENT (start {start}, increment "
                f"{step}) is NOT carried to the target: {_IDENTITY_REFUSED}. "
                f"After cutover this key stops generating and every insert "
                f"must supply it (a MAX(key)+ROW_NUMBER() in the writer, or "
                f"a key generated upstream)."))
    return out


def uncarried_column_facts(record: dict) -> list[str]:
    """`COLUMN: sentence` warnings for DEFAULT / IDENTITY on one record.

    Shared with plan/build.py so DDL_PLAN.md and PLANNED_OBJECTS.md say the
    same thing about the same column.
    """
    return [text for _, _, text in _column_fact_pairs(record)]


def describe_constraints(constraints: list[dict] | None) -> str:
    """The constraints of one object, as one line for the R20 audit detail."""
    parts = []
    for c in constraints or []:
        text = f'{c.get("constraint_type")} ({", ".join(c.get("columns") or [])})'
        if c.get("references"):
            text += (f' -> {c["references"]}'
                     f'({", ".join(c.get("referenced_columns") or [])})')
        parts.append(text)
    return "; ".join(parts)


# Delta 3.1 liquid clustering: at most four keys, each needing min/max
# statistics -- collected on the first 32 columns by default
# (delta.dataSkippingNumIndexedCols) and only for these types.
_CLUSTER_MAX_KEYS = 4
_STATS_COLUMNS = 32
_CLUSTERABLE = re.compile(
    r"^(DECIMAL\(\d+,\d+\)|DOUBLE|FLOAT|INT|INTEGER|BIGINT|SMALLINT|TINYINT"
    r"|STRING|DATE|TIMESTAMP|TIMESTAMP_NTZ)$")
# Delta's own defaults: 7 days of removed files, 30 days of log.
_DELTA_FILE_DAYS = 7
_DELTA_LOG_DAYS = 30
_CDF = "delta.enableChangeDataFeed"
_FILE_RETENTION = "delta.deletedFileRetentionDuration"
_LOG_RETENTION = "delta.logRetentionDuration"


def _split_keys(text: str) -> list[str]:
    """Top-level comma split of a cluster_by list, quote- and paren-aware."""
    parts, depth, quote, cur = [], 0, None, ""
    for ch in text:
        if quote:
            quote = None if ch == quote else quote
        elif ch in "\"'":
            quote = ch
        elif ch == "(":
            depth += 1
        elif ch == ")":
            depth -= 1
        elif ch == "," and depth == 0:
            parts.append(cur)
            cur = ""
            continue
        cur += ch
    parts.append(cur)
    return [p.strip() for p in parts if p.strip()]


def _cluster_by(value, columns: list[dict]) -> tuple[list[str] | None, str]:
    """(target key columns, why not) for SHOW TABLES' `cluster_by`.

    `LINEAR(SEGMENT)` is the live shape. A key is carried only when it is a
    plain column Delta can cluster on; anything else returns None and the
    reason, and the setting stays a deferred decision.
    """
    text = str(value).strip()
    m = re.match(r"(?is)^LINEAR\s*\((.*)\)$", text)
    inner = m.group(1) if m else (text[1:-1] if text.startswith("(")
                                  and text.endswith(")") else text)
    keys = _split_keys(inner)
    if not keys:
        return None, "no key could be read from it"
    ordered = [c["COLUMN_NAME"] for c in columns]
    by_name = {c["COLUMN_NAME"]: c for c in columns}
    out = []
    for key in keys:
        if re.fullmatch(r'"(?:[^"]|"")+"', key):
            name = key[1:-1].replace('""', '"')
        elif re.fullmatch(r"[A-Za-z_][A-Za-z0-9_$]*", key):
            # Unquoted folds to upper case in Snowflake, as the column did.
            name = key.upper() if key.upper() in by_name else key
        else:
            return None, (f"key {key} is an expression; liquid clustering "
                          f"takes plain columns (a generated column or "
                          f"ZORDER BY is the design decision here)")
        if name not in by_name:
            return None, f"key {key} is not a column of this table"
        if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", name):
            # Live 2026-09-29 (Delta 3.1.0): CLUSTER BY (`SEGMENT`) failed
            # with PARTITION_WITH_NESTED_COLUMN_IS_UNSUPPORTED -- the
            # clustering parser keeps backticks as part of the name. So the
            # clause is written bare, and a name that cannot be is not
            # carried rather than emitted in a form AIDP rejects.
            return None, (f"key {name} cannot be written bare, and AIDP's "
                          f"Delta CLUSTER BY does not accept a quoted column "
                          f"name (live-verified)")
        target = " ".join(str(by_name[name]["target_type"]).upper().split())
        if not _CLUSTERABLE.match(target.replace(" ", "")):
            return None, (f"key {name} is {target} on the target, which "
                          f"Delta cannot cluster on (no min/max statistics)")
        if ordered.index(name) >= _STATS_COLUMNS:
            return None, (f"key {name} is column {ordered.index(name) + 1}; "
                          f"Delta collects clustering statistics on the "
                          f"first {_STATS_COLUMNS} columns only")
        out.append(name)
    if len(out) > _CLUSTER_MAX_KEYS:
        return None, (f"{len(out)} keys; Delta liquid clustering takes at "
                      f"most {_CLUSTER_MAX_KEYS}")
    return out, ""


def _retention_days(value) -> int | None:
    try:
        return int(str(value).strip())
    except (TypeError, ValueError):
        return None


def classify_table_properties(record: dict, streams_on=()) -> dict:
    """Every source table setting, sorted by what `ddl` can do with it.

    Returns {"features", "carried", "deferred", "omitted"}: the Delta
    clauses to emit, the settings they carry (each with how), the ones with
    an equivalent this table cannot take (each with why), and the ones with
    no equivalent at all. Shared with plan/build.py so SUMMARY.md scores the
    same settings DDL_PLAN.md lists.
    """
    columns = sorted(record.get("columns") or [],
                     key=lambda c: c.get("ORDINAL_POSITION") or 0)
    features: dict = {}
    props: dict[str, str] = {}
    carried, deferred, omitted = [], [], []
    retention_seen = False
    for prop, value in (record.get("source_metadata") or {}).items():
        if prop in _INFORMATIONAL_METADATA or value in _UNSET:
            continue
        if prop == "cluster_by":
            keys, why = _cluster_by(value, columns)
            if keys:
                features["cluster_by"] = keys
                carried.append({
                    "property": prop, "value": value,
                    "carried_as": "CLUSTER BY (" + ", ".join(keys) + ") -- "
                                  "liquid clustering. Delta does not "
                                  "recluster in the background: a scheduled "
                                  "OPTIMIZE does it"})
            else:
                deferred.append({"property": prop, "value": value,
                                 "aidp_equivalent":
                                     DEFERRED_EQUIVALENT_PROPERTIES[prop],
                                 "reason": why})
        elif prop in ("retention_time", "data_retention_time_in_days"):
            days = _retention_days(value)
            if days is None or retention_seen:
                if days is None:
                    deferred.append({"property": prop, "value": value,
                                     "aidp_equivalent":
                                         DEFERRED_EQUIVALENT_PROPERTIES[prop],
                                     "reason": "the value is not a number "
                                               "of days"})
                continue
            retention_seen = True
            raised = []
            if days > _DELTA_FILE_DAYS:
                props[_FILE_RETENTION] = f"interval {days} days"
                raised.append(_FILE_RETENTION)
            if days > _DELTA_LOG_DAYS:
                props[_LOG_RETENTION] = f"interval {days} days"
                raised.append(_LOG_RETENTION)
            how = (f"{' and '.join(raised)} = 'interval {days} days'"
                   if raised else "nothing emitted")
            carried.append({
                "property": prop, "value": value,
                "carried_as": how + (
                    f"; Delta's defaults ({_DELTA_FILE_DAYS} days of removed "
                    f"files, {_DELTA_LOG_DAYS} of log) already reach at least "
                    f"{days} day(s) back"
                    + (" for the rest" if raised else
                       ", and a shorter setting would only shrink recovery "
                       "and trip VACUUM's retention check")
                    if days <= _DELTA_LOG_DAYS else "")
                + ". On Delta the reach holds only until VACUUM runs, which "
                  "nothing schedules for you"})
        elif prop == "change_tracking":
            props[_CDF] = "true"
            carried.append({"property": prop, "value": value,
                            "carried_as": f"{_CDF} = true (Change Data Feed)"})
        elif prop in SCRUBBED_PROPERTIES:
            omitted.append(f"{prop}={value}")
    streams = sorted(set(streams_on or ()))
    if streams:
        props[_CDF] = "true"
        carried.append({
            "property": "stream", "value": ", ".join(streams),
            "carried_as": f"{_CDF} = true: a Snowflake stream reads this "
                          f"table. Stream offsets do not transfer -- a "
                          f"consumer starts from the migrated data, and the "
                          f"initial copy itself appears in the feed as "
                          f"inserts"})
    if props:
        features["tblproperties"] = dict(sorted(props.items()))
    return {"features": features, "carried": carried, "deferred": deferred,
            "omitted": omitted}


def render_cluster_by(features: dict) -> str:
    """`CLUSTER BY (...)` for a statement's delta_features, or ''.

    01_create_structure renders the same clause from the same dict
    (`_cluster_by_sql`); a parity test holds them together.
    """
    # Bare names: AIDP's Delta CLUSTER BY keeps backticks as part of the name
    # (live-verified); _cluster_by carries only keys that can be written so.
    keys = (features or {}).get("cluster_by") or []
    return ("CLUSTER BY (" + ", ".join(keys) + ")") if keys else ""


def render_tblproperties(features: dict) -> str:
    """`TBLPROPERTIES (...)` for a statement's delta_features, or ''."""
    props = (features or {}).get("tblproperties") or {}
    return ("TBLPROPERTIES (" + ", ".join(
        f"{quote_spark_string(k)} = {quote_spark_string(v)}"
        for k, v in props.items()) + ")") if props else ""


def copy_spec(columns: list[dict], *, geospatial: str | None = None
              ) -> list[dict]:
    """The per-column copy spec, one entry per column in order.

    `{name, source_type, target_type, read_expr, convert_expr}`: the copy
    stage selects every `read_expr AS "<name>"` in ONE qualified pushdown
    and inserts `convert_expr AS `<name>`` by name. The expressions come
    from the same module as the type mapping (`copy_expressions`), so the
    read is always the one the mapped `target_type` was decided for;
    `geospatial` is the inventory's recorded mode, which decides WKT versus
    GeoJSON. `source_type` is the type those expressions were decided FOR:
    the copy compares it with the live source and does not run a
    conversion planned for a type the column no longer has.
    """
    out = []
    for c in columns:
        read, convert = copy_expressions(
            c.get("DATA_TYPE"), c["target_type"], name=c["COLUMN_NAME"],
            type_detail=c.get("type_detail"),
            datetime_precision=c.get("DATETIME_PRECISION"),
            geospatial=geospatial)
        out.append({"name": c["COLUMN_NAME"],
                    "source_type": source_type_key(
                        c.get("DATA_TYPE"), c.get("NUMERIC_PRECISION"),
                        c.get("NUMERIC_SCALE")),
                    "target_type": c["target_type"],
                    "read_expr": read, "convert_expr": convert})
    return out


def build_create_table(record: dict, target_fqn: str, *,
                       geospatial: str | None = None,
                       streams_on=()) -> RewriteResult:
    qualified = _qualify(target_fqn)
    res = RewriteResult(record["source_identifier"], target_fqn, None)
    res.rules_applied.append(RuleApplication(
        "R01_TARGET_NAME", f'{record["source_identifier"]} -> {target_fqn}'))
    res.rules_applied.append(RuleApplication(
        "R02_QUOTE_BACKTICK", "Snowflake double-quote identifiers -> Spark backticks"))

    if record.get("compatibility_status") == "blocked":
        res.blocked = True
        res.blocked_reason = "; ".join(record.get("blocked_reasons") or ["unspecified"])
        return res

    columns = sorted(record.get("columns") or [],
                     key=lambda c: c.get("ORDINAL_POSITION") or 0)
    if not columns:
        res.blocked = True
        # A failed INFORMATION_SCHEMA.COLUMNS read leaves the list empty too,
        # and guessing "privilege" for it sends the operator after grants
        # when the read timed out. The extractor records the failure; say it.
        error = record.get("columns_read_error")
        res.blocked_reason = (
            f"the column list could not be read from the source "
            f"(INFORMATION_SCHEMA.COLUMNS failed: {error}), so no structure "
            f"can be emitted" if error else
            "table has no columns visible to this role "
            "(Delta-shared or insufficient privilege)")
        return res

    unmapped = [c["COLUMN_NAME"] + ": " + str(c.get("DATA_TYPE"))
                for c in columns if not c.get("target_type")]
    if unmapped:
        res.blocked = True
        res.blocked_reason = "unmapped column types: " + "; ".join(unmapped)
        return res

    specs = [_column_spec(c) for c in columns]
    lines = ["  " + render_column_sql(s) for s in specs]
    for c in columns:
        res.rules_applied.append(RuleApplication(
            "R03_TYPE_MAP",
            f'{c["COLUMN_NAME"]}: {c.get("DATA_TYPE")} -> {c["target_type"]}'))
    res.copy_columns = copy_spec(columns, geospatial=geospatial)
    exact = [c for c in res.copy_columns
             if c["read_expr"] != '"' + c["name"].replace('"', '""') + '"'
             or c["convert_expr"] != _q(c["name"])]
    if exact:
        res.rules_applied.append(RuleApplication(
            "R04_EXACT_READ",
            "read from Snowflake as text and converted on AIDP, because the "
            "connector's native read loses digits, fractions or offsets, or "
            "cannot open the type at all (observed live 2026-09-29): "
            + "; ".join(f'{c["read_expr"]} -> {c["convert_expr"]}'
                        for c in exact)))

    settings = classify_table_properties(record, streams_on)
    res.delta_features = settings["features"]
    res.carried_properties = settings["carried"]
    res.deferred_properties = settings["deferred"]
    res.omitted_properties = settings["omitted"]
    if res.carried_properties:
        res.rules_applied.append(RuleApplication(
            "R12_DELTA_FEATURES_CARRIED",
            "source settings carried into this CREATE TABLE (Delta 3.1 on "
            "AIDP accepts each, live-verified 2026-09-29): "
            + "; ".join(f'{c["property"]}={c["value"]} -> {c["carried_as"]}'
                        for c in res.carried_properties)))
    if res.delta_features:
        res.rules_applied.append(RuleApplication(
            "R13_DELTA_FEATURES_CATALOG_API_GAP",
            "the in-AIDP structure notebook (01_create_structure) applies "
            "the CLUSTER BY / TBLPROPERTIES above and reads the properties "
            "back. The catalog-API transport (`snowmig deploy --execute`) "
            "CANNOT: its table body has no clustering or table-property "
            "field, so on that path they are deferred -- apply them "
            "afterwards with ALTER TABLE ... CLUSTER BY / SET TBLPROPERTIES, "
            "or create the table with the structure notebook"))
    if res.omitted_properties:
        res.rules_applied.append(RuleApplication(
            "R10_PROP_SCRUB",
            "dropped Snowflake properties with no AIDP equivalent: "
            + ", ".join(res.omitted_properties)))
    if res.deferred_properties:
        res.rules_applied.append(RuleApplication(
            "R11_MAINTENANCE_DEFERRED",
            "source settings with an AIDP equivalent that this table cannot "
            "take as it stands, carried into the maintenance decision "
            "instead: "
            + "; ".join(f'{d["property"]}={d["value"]}'
                        + (f' ({d["reason"]})' if d.get("reason") else "")
                        for d in res.deferred_properties)))

    if record.get("constraints"):
        res.rules_applied.append(RuleApplication(
            "R20_CONSTRAINTS_NOT_EMITTED",
            "Snowflake PK/FK/UNIQUE are unenforced metadata, and "
            + _PK_REFUSED
            + ", so they are captured in the inventory and not emitted as "
              "DDL: "
            + describe_constraints(record["constraints"])
            + ". Delta does enforce NOT NULL (carried above) and CHECK; "
              "Snowflake has no CHECK constraint, so there is nothing of that "
              "class to carry over."))

    # NOT NULL is in the SQL above and the structure-notebook path applies it.
    # The catalog-API path cannot: the body has no nullability field (see
    # catalog_api.build_table_body), so it is named here -- per object, next
    # to the rules DDL_PLAN.md prints -- rather than quietly dropped.
    not_null = [s["name"] for s in specs if not s["nullable"]]
    if not_null:
        res.rules_applied.append(RuleApplication(
            "R21_NOT_NULL_CATALOG_API_GAP",
            f"{len(not_null)} column(s) are NOT NULL in the source and in "
            f"this SQL ({', '.join(not_null)}). The in-AIDP structure "
            f"notebook applies them. The catalog-API transport (`snowmig "
            f"deploy --execute`) does not carry them: its table body has no "
            f"nullability field, so on that path those columns are created "
            f"NULLABLE. Create "
            f"this table with the structure notebook if the constraint "
            f"matters, or re-apply it afterwards."))

    description = str((record.get("source_metadata") or {}).get("comment")
                      or "")
    if description:
        res.description = description
        res.rules_applied.append(RuleApplication(
            "R24_TABLE_COMMENT_CARRIED",
            "the source table COMMENT is emitted on the CREATE TABLE and "
            "sent as the catalog API's `description`. A schema COMMENT is "
            "still never emitted (R14)."))

    facts = _column_fact_pairs(record)
    res.warnings.extend(text for _, _, text in facts
                        if text not in res.warnings)
    defaults = [name for kind, name, _ in facts if kind == "DEFAULT"]
    identities = [name for kind, name, _ in facts if kind == "IDENTITY"]
    if defaults:
        res.rules_applied.append(RuleApplication(
            "R22_COLUMN_DEFAULT_NOT_EMITTED",
            f"column DEFAULT is read from the source and NOT emitted for "
            f"{', '.join(defaults)}: {_DEFAULT_REFUSED}. Inserts that omit "
            f"these columns after cutover arrive NULL."))
    if identities:
        res.rules_applied.append(RuleApplication(
            "R23_IDENTITY_NOT_EMITTED",
            f"IDENTITY / AUTOINCREMENT is read from the source and NOT "
            f"emitted for {', '.join(identities)}: {_IDENTITY_REFUSED}. The "
            f"key stops generating at cutover; every insert must supply it."))
    if record.get("column_facts_unknown"):
        res.warnings.append(
            "column DEFAULT / IDENTITY are UNKNOWN for this table, not absent: "
            "it was planned from a manifest whose discovery did not read them.")

    res.rules_applied.append(RuleApplication(
        "R30_USING_DELTA", "explicit USING DELTA so format is not cluster-default"))
    cluster = render_cluster_by(res.delta_features)
    properties = render_tblproperties(res.delta_features)
    res.sql = (f"CREATE TABLE IF NOT EXISTS {qualified} (\n"
               + ",\n".join(lines) + "\n)\nUSING DELTA"
               + (f"\n{cluster}" if cluster else "")
               + (f"\nCOMMENT {quote_spark_string(description)}"
                  if description else "")
               + (f"\n{properties}" if properties else ""))
    res.expected_columns = specs

    for c in columns:
        for w in record.get("warnings") or []:
            if w.startswith(c["COLUMN_NAME"] + ":") and w not in res.warnings:
                res.warnings.append(w)
    return res



# A one- or two-part name can only be read as a table where nothing else can
# appear: an item of a FROM list, or after JOIN (see _relation_spans).
# Strings and comments are blanked first, so text that merely looks like a
# reference is never rewritten. The quote may be a double quote (as written
# in Snowflake) or a backtick: the dialect pass runs BEFORE this one and has
# already rewritten quoted identifiers to Spark backticks, so matching only
# `"` misses them.


def _code_mask(sql: str) -> str:
    """`sql` with string and comment text blanked, offsets preserved."""
    return "".join(
        "".join("\n" if c == "\n" else " " for c in text)
        if kind in ("string", "comment") else text
        for kind, text in lexer.segments(sql))


# One part of a relation name as a view body may spell it: unquoted, or
# quoted with Snowflake's double quote or (after the dialect pass) Spark's
# backtick.
_REL_PART = r'[A-Za-z_][\w$]*|"(?:[^"]|"")+"|`(?:[^`]|``)+`'
_REL_NAME = re.compile(rf"(?:{_REL_PART})(?:\s*\.\s*(?:{_REL_PART})){{0,2}}")
_REL_PARTS = re.compile(_REL_PART)
_REL_KEYWORD = re.compile(r"\b(FROM|JOIN)\b", re.IGNORECASE)
_REL_ALIAS = re.compile(rf"\s*(?:AS\s+)?({_REL_PART})", re.IGNORECASE)
_WORD_BEFORE = re.compile(r"([A-Za-z_][\w$]*)\s*$")
_WORD_AFTER = re.compile(r"\s*([A-Za-z_][\w$]*)")
# Functions whose argument list carries a FROM that is not a FROM clause:
# `EXTRACT(YEAR FROM o.D)`, `TRIM(BOTH ' ' FROM c.X)`, `SUBSTRING(s FROM 2)`.
_FROM_IN_ARGS = {"EXTRACT", "TRIM", "SUBSTRING", "SUBSTR", "OVERLAY"}
# Words that end a FROM item, so they are never read as its alias -- or open
# one that is not a named relation (TABLE(...), VALUES, LATERAL).
_NOT_A_RELATION = {
    "TABLE", "VALUES", "UNNEST", "IDENTIFIER", "LATERAL", "SELECT", "FROM",
    "WHERE", "JOIN", "INNER", "LEFT", "RIGHT", "FULL", "OUTER", "CROSS",
    "NATURAL", "ON", "USING", "GROUP", "ORDER", "HAVING", "QUALIFY", "LIMIT",
    "OFFSET", "FETCH", "UNION", "EXCEPT", "MINUS", "INTERSECT", "WINDOW",
    "AT", "BEFORE", "CHANGES", "SAMPLE", "TABLESAMPLE", "PIVOT", "UNPIVOT",
    "MATCH_RECOGNIZE", "ASOF"}


def _relation_spans(sql: str) -> list[tuple[int, int, list[tuple[str, str]]]]:
    """Every named relation a view body reads: (start, end, parts).

    `parts` is [(quote, name)] -- quote is `"`, a backtick or "" for an
    unquoted part, and name is the part without its quotes. The same reading
    as the lineage scanner (extract/dependencies.py), so the plan's rewrite
    and its warnings agree with the edges its waves were built from:
      * every item of a FROM list is read, not only the first -- `from
        ORDERS o, CUSTOMERS c` has two, and the second used to stay bare;
      * a FROM that is not a FROM clause is skipped: `IS DISTINCT FROM x`,
        `EXTRACT(YEAR FROM x)` / `TRIM(... FROM x)` / `SUBSTRING(s FROM n)`,
        and NTH_VALUE's `FROM FIRST` / `FROM LAST`; their operand is a
        column, which read as a table "not part of this migration";
      * a subquery is skipped (its own FROMs are found by the scan), and a
        name followed by `(` is a table function, not a relation.
    A CTE name is returned like any other; callers decide with `_is_cte`.
    """
    try:
        code = lexer.code_only(sql)     # keywords/parens/commas: code only
        mask = _code_mask(sql)          # names: identifiers kept, as written
    except lexer.UnterminatedLiteral:
        return []

    keywords = list(_REL_KEYWORD.finditer(code))
    starts = {kw.start() for kw in keywords}
    enclosing: dict[int, int | None] = {}
    opener: dict[int, int] = {}                 # `)` index -> its `(`
    stack: list[int] = []
    for i, ch in enumerate(code):
        if i in starts:
            enclosing[i] = stack[-1] if stack else None
        if ch == "(":
            stack.append(i)
        elif ch == ")" and stack:
            opener[i] = stack.pop()

    def word_before(i: int) -> str:
        m = _WORD_BEFORE.search(code[max(0, i - 200):i])
        return m.group(1).upper() if m else ""

    def not_a_clause(kw: re.Match) -> bool:
        if kw.group(1).upper() != "FROM":
            return False
        if word_before(kw.start()) == "DISTINCT":
            return True
        opened = enclosing.get(kw.start())
        if opened is not None and word_before(opened) in _FROM_IN_ARGS:
            return True
        before = code[:kw.start()].rstrip()
        after = _WORD_AFTER.match(code, kw.end())
        return (before.endswith(")") and after is not None
                and after.group(1).upper() in ("FIRST", "LAST")
                and word_before(opener.get(len(before) - 1, 0)) == "NTH_VALUE")

    spans: list[tuple[int, int, list[tuple[str, str]]]] = []

    def skip_blank(pos: int) -> int:
        while pos < len(mask) and mask[pos].isspace():
            pos += 1
        return pos

    def take(pos: int) -> int | None:
        """Read one FROM item at `pos`; the index after it (and its alias),
        or None when what stands there is not a relation."""
        pos = skip_blank(pos)
        if pos >= len(mask):
            return None
        if code[pos] == "(":
            depth = 0
            for j in range(pos, len(code)):
                if code[j] == "(":
                    depth += 1
                elif code[j] == ")":
                    depth -= 1
                    if depth == 0:
                        pos = j + 1
                        break
            else:
                return None
        else:
            m = _REL_NAME.match(mask, pos)
            if m is None:
                return None
            raw = _REL_PARTS.findall(m.group(0))
            if len(raw) == 1 and raw[0][0] not in '"`' \
                    and raw[0].upper() in _NOT_A_RELATION:
                return None
            if mask[skip_blank(m.end()):skip_blank(m.end()) + 1] == "(":
                return None                         # a table function
            parts = [(p[0], p[1:-1].replace(p[0] * 2, p[0]))
                     if p[0] in '"`' else ("", p) for p in raw]
            spans.append((m.start(), m.end(), parts))
            pos = m.end()
        alias = _REL_ALIAS.match(mask, pos)
        if alias and alias.group(1).upper() not in _NOT_A_RELATION:
            pos = alias.end()
        return pos

    for kw in keywords:
        if not_a_clause(kw):
            continue
        pos = take(kw.end())
        # Only a FROM carries a comma list; a JOIN names one relation.
        while pos is not None and kw.group(1).upper() == "FROM":
            pos = skip_blank(pos)
            if pos >= len(mask) or code[pos] != ",":
                break
            pos = take(pos + 1)
    # In text order: a subquery's own FROMs are found after the outer list's
    # later items, and callers apply edits back to front.
    return sorted(spans)


def _rewrite_positional_refs(sql: str, name_map: dict[str, str], db: str,
                             schema: str) -> tuple[str, list[str], list[str]]:
    """Qualify the one- and two-part references Snowflake would resolve.

    Snowflake resolves `SCHEMA.NAME` against the view's own DATABASE and a
    bare `NAME` against the view's own SCHEMA, so both targets are known,
    not guessed. Nothing crosses that boundary: a bare ORDERS in ANALYTICS
    never becomes COMMERCE.ORDERS. Returns (sql, rewrites, unresolved).
    """
    # Keyed by the source's EXACT spelling: a quoted part keeps its case in
    # Snowflake, so "Orders" and ORDERS are two objects. Upper-casing the
    # keys while looking a quoted part up in its exact case meant a quoted
    # in-migration name never matched, and was blamed on the scope.
    two_part: dict[tuple[str, str], str] = {}
    bare: dict[str, str] = {}
    for src, tgt in name_map.items():
        parts = src.split(".", 2)
        if len(parts) != 3 or parts[0].upper() != db.upper():
            continue
        two_part[(parts[1], parts[2])] = tgt
        if parts[1].upper() == schema.upper():
            bare[parts[2]] = tgt

    def key(part: tuple[str, str]) -> str:
        # A quoted part keeps its exact case; unquoted folds to upper, as in
        # Snowflake.
        quote, name = part
        return name if quote else name.upper()

    edits, rewrites, unresolved = [], [], []
    # A CTE is not a table. This pass runs after the 3-part rewrite, which
    # already leaves CTEs alone; without the same check here a `WITH ORDERS
    # AS (...)` would be re-pointed at the base table and the view would
    # silently lose the CTE's filter.
    ctes = lexer.cte_scopes(sql)
    for start, end, parts in _relation_spans(sql):
        if len(parts) > 2:
            continue
        if len(parts) == 1 and _is_cte(sql, start, end, ctes):
            continue
        written = sql[start:end]
        target = (two_part.get((key(parts[0]), key(parts[1])))
                  if len(parts) == 2 else bare.get(key(parts[0])))
        if target is None:
            unresolved.append(written)
            continue
        replacement = ".".join(p if re.match(r"^[a-z_][a-z0-9_]*$", p)
                               else _q(p) for p in target.split("."))
        edits.append((start, end, replacement))
        rewrites.append(f"{written} -> {target}")
    for start, end, text in reversed(edits):
        sql = sql[:start] + text + sql[end:]
    return sql, rewrites, list(dict.fromkeys(unresolved))

_PLAIN_PART = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _ref_part_pattern(part: str) -> str:
    # One part of a 3-part name as it may appear in a body: unquoted (Snowflake
    # folds it, so case-insensitive), or quoted -- Spark backticks after the
    # dialect pass, Snowflake double quotes before it -- which is exact-case,
    # because "orders" and ORDERS are different objects in Snowflake.
    backticked = re.escape("`" + part.replace("`", "``") + "`")
    double_quoted = re.escape('"' + part.replace('"', '""') + '"')
    return f"(?:(?-i:{backticked}|{double_quoted})|{re.escape(part)})"


def _context_refs(name_map: dict[str, str], db: str, schema: str
                  ) -> dict[str, str]:
    """The one- and two-part forms of every mapped object in this view's own
    schema, which is where Snowflake resolves an unqualified reference.

    Only for the view's own database and schema: a bare `ORDERS` in a view
    in SALES cannot mean `MARKETING.ORDERS`, and guessing across schemas is
    how a view silently reads from the wrong table.
    """
    out: dict[str, str] = {}
    if not db or not schema:
        return out
    prefix = f"{db}.{schema}.".upper()
    for src, tgt in name_map.items():
        if not src.upper().startswith(prefix):
            continue
        name = src.split(".", 2)[2]
        out[f"{schema}.{name}"] = tgt
        out[name] = tgt
    return out


def _rewrite_view_refs(body: str, name_map: dict[str, str],
                       positional: dict[str, str] | None = None
                       ) -> tuple[str, list[str]]:
    """Rewrite whole 3-part object references in `body` per `name_map`.

    Matches run over code and identifier segments only. A 3-part name inside a
    string literal is DATA the view returns, and one inside a comment is prose;
    a plain re.sub over the body rewrote both. Each part must be whole: word
    boundaries stop `DB.S.ORDERS` from hitting `DB.S.ORDERS_ARCHIVE`, and a hit
    is blanked before shorter names are tried so nothing matches inside it.
    Returns the rewritten body and the `src -> tgt` pairs that actually hit.

    A bare name after FROM or JOIN that is one of the view's own CTEs, where
    that CTE is in scope, is the CTE and is left alone: rewriting `from
    ORDERS` to the base table when ORDERS is `with ORDERS as (... where
    STATUS = 'OPEN')` silently dropped the CTE's filter.
    """
    ctes = lexer.cte_scopes(body)
    mask = "".join(
        "".join("\n" if c == "\n" else " " for c in text)
        if kind in ("string", "comment") else text
        for kind, text in lexer.segments(body))
    relations = {(start, end) for start, end, _ in _relation_spans(body)}
    edits: list[tuple[int, int, str]] = []
    changed: list[str] = []
    # Only a name whose last part occurs in the body can match. The map is
    # every planned object: live, one regex per entry per view took `ddl`
    # hours on a 50,500-object plan. Exact for an ASCII body and name -- a
    # hit holds the last part verbatim, or case-folded when unquoted; a
    # non-ASCII body, or a part holding a quote, is still tried.
    folded = mask.lower() if mask.isascii() else None

    def may_occur(src: str) -> bool:
        last = src.rsplit(".", 1)[-1]
        if folded is None or not last.isascii() or '"' in last or "`" in last:
            return True
        return last.lower() in folded

    # Longest first, and the fully-qualified map before the positional one:
    # a three-part hit is unambiguous and blanks the span before any shorter
    # form is tried against it.
    ordered = [(s, t, False)
               for s, t in sorted(((s, t) for s, t in name_map.items()
                                   if may_occur(s)),
                                  key=lambda kv: -len(kv[0]))]
    ordered += [(s, t, True)
                for s, t in sorted(((s, t) for s, t
                                    in (positional or {}).items()
                                    if may_occur(s)),
                                   key=lambda kv: -len(kv[0]))]
    for src, tgt, after_keyword in ordered:
        if src == tgt:
            continue
        ref = (r"\s*\.\s*".join(_ref_part_pattern(p) for p in src.split("."))
               + r'(?![\w`"$])')
        pattern = r'(?<![\w`"$.])' + ref
        # A one- or two-part name only where a relation stands: a whole item
        # of a FROM list or a JOIN (see _relation_spans), never the column
        # in `EXTRACT(YEAR FROM o.D)` or `IS DISTINCT FROM c.S`.
        hits = [(m.span(), "")
                for m in re.finditer(pattern, mask, re.IGNORECASE)
                if not after_keyword
                or (m.span() in relations
                    and not ("." not in src
                             and _is_cte(body, m.start(), m.end(), ctes)))]
        if not hits:
            continue
        # A target part the Spark parser would not read as one word (a hyphen
        # from a prefixed catalog name) is backticked; a plain one is emitted
        # as-is, so the common case stays byte-identical to the planned name.
        replacement = ".".join(p if _PLAIN_PART.match(p) else _q(p)
                               for p in tgt.split("."))
        for (start, end), keep in hits:
            edits.append((start, end, keep + replacement))
            mask = mask[:start] + " " * (end - start) + mask[end:]
        changed.append(f"{src} -> {tgt}")
    out = body
    for start, end, replacement in sorted(edits, reverse=True):
        out = out[:start] + replacement + out[end:]
    return out, changed


def _is_cte(sql: str, start: int, end: int,
            ctes: list[tuple[str, int, int]]) -> bool:
    """Whether the bare name at sql[start:end] is a CTE visible there."""
    text = sql[start:end]
    key = _cte_key(text)
    return any(name == key and lo <= start < hi for name, lo, hi in ctes)


def _cte_key(text: str) -> str:
    # Compared as lexer.cte_scopes records it: unquoted folds to upper case,
    # a quoted name (double quote or, after translation, backtick) is exact.
    if text[:1] in ('"', "`"):
        return text[1:-1].replace(text[0] * 2, text[0])
    return text.upper()


def _unqualified_refs(sql: str) -> set[str]:
    """Bare names still sitting where only a table can go -- other than the
    view's own CTE names, which the target resolves from the WITH clause.

    Every item of a FROM list counts (`from ORDERS o, CUSTOMERS c`), and a
    FROM inside an expression (`EXTRACT(YEAR FROM D)`) does not.
    """
    ctes = lexer.cte_scopes(sql)
    out = set()
    for start, end, parts in _relation_spans(sql):
        if len(parts) != 1 or parts[0][0]:
            continue
        if _is_cte(sql, start, end, ctes):
            continue
        out.add(sql[start:end])
    return out


def build_create_view(record: dict, target_fqn: str,
                      name_map: dict[str, str] | None = None) -> RewriteResult:
    """Generate CREATE VIEW, or block with the reason it cannot be migrated."""
    res = RewriteResult(record["source_identifier"], target_fqn, None)
    res.rules_applied.append(RuleApplication(
        "R01_TARGET_NAME", f'{record["source_identifier"]} -> {target_fqn}'))

    meta = record.get("source_metadata") or {}
    if str(meta.get("is_secure", "")).lower() in ("true", "y", "yes"):
        res.blocked = True
        res.blocked_reason = ("Snowflake secure view: its definition and row "
                              "visibility rules have no Delta equivalent")
        return res
    if str(meta.get("is_materialized", "")).lower() in ("true", "y", "yes"):
        res.blocked = True
        res.blocked_reason = ("Snowflake materialized view: no AIDP equivalent; "
                              "rebuild as a table plus a refresh job")
        return res

    ddl = record.get("view_ddl_get_ddl") or record.get("view_text_show")
    if not ddl:
        res.blocked = True
        res.blocked_reason = ("no view SQL was captured during extraction; "
                             "GET_DDL and SHOW VIEWS both returned nothing")
        return res

    try:
        body = extract_view_body(ddl)
        view_columns = extract_view_columns(ddl)
        # Inside the guard on purpose: a translator that cannot read one view
        # blocks THAT view with the reason, it does not abort the stage.
        translated = translate_view_body(body)
    except ValueError as exc:
        res.blocked = True
        res.blocked_reason = str(exc)
        return res

    if translated.unsupported:
        res.blocked = True
        res.blocked_reason = "Snowflake-only SQL: " + "; ".join(
            f'{u["construct"]} ({u["detail"]})' for u in translated.unsupported)
        return res

    for applied in translated.applied:
        res.rules_applied.append(RuleApplication(
            applied["rule_id"], f'{applied["construct"]}: {applied["detail"]}'))
    # The type mapper's notes on a `::TIMESTAMP` cast travel with the view,
    # the same way a column's mapping warning travels with a table. (They are
    # also a caveat on the T02 application, so R43 below is not "exact".)
    res.warnings.extend(translated.warnings)

    positional = _context_refs(name_map or {},
                               str(record.get("source_database") or ""),
                               str(record.get("source_schema") or ""))
    rewritten, changed = _rewrite_view_refs(translated.sql, name_map or {},
                                            positional)

    # Anything still unqualified after FROM or JOIN is a reference the target
    # has no way to resolve. On the catalog-API transport that is a bare 500
    # with no detail, so it is named here instead of discovered there.
    leftover = _unqualified_refs(rewritten)
    if leftover:
        res.warnings.append(
            "unqualified object reference(s) remain in the view SQL: "
            + ", ".join(sorted(leftover))
            + ". They are not part of this migration, so there is no target "
              "name to qualify them with. A view whose body the target cannot "
              "resolve is rejected (on the catalog-API path, as an HTTP 500), "
              "so create this view only once those objects exist and are "
              "named in full.")
        res.rules_applied.append(RuleApplication(
            "R42_VIEW_REFS_UNRESOLVED",
            "left unqualified, no target name known: "
            + ", ".join(sorted(leftover))))

    rewritten, positional, unresolved = _rewrite_positional_refs(
        rewritten, name_map or {}, str(record.get("source_database") or ""),
        str(record.get("source_schema") or ""))
    changed += positional
    if unresolved:
        res.rules_applied.append(RuleApplication(
            # R45, not R44: R44 is the view's column list, below.
            "R45_VIEW_REFS_UNRESOLVED",
            "left as written, outside the migration and with no target name: "
            + ", ".join(unresolved)))
        res.warnings.append(
            "unresolved view reference(s) " + ", ".join(unresolved)
            + ": not part of this migration, so there is no target name to "
            "qualify them with. The CREATE VIEW will fail on the target until "
            "they exist there.")

    if changed:
        res.rules_applied.append(RuleApplication(
            "R41_VIEW_REFS_REWRITTEN",
            "rewrote object references: " + ", ".join(changed)))
    else:
        res.rules_applied.append(RuleApplication(
            "R40_VIEW_REFS_IDENTITY",
            "bronze mirrors the source 1:1, so object references are unchanged"))

    if translated.applied:
        n = len(translated.applied)
        # A rule that is exact only under a condition says so here, next to
        # the count, rather than letting the plan call the whole view exact.
        caveats = [f'{a["rule_id"]}: {a["caveat"]}'
                   for a in translated.applied if a.get("caveat")]
        if caveats:
            res.rules_applied.append(RuleApplication(
                "R43_VIEW_DIALECT_TRANSLATED",
                f"{n} dialect rule(s) applied; NOT all exact: "
                + "; ".join(caveats)))
            res.warnings.append(
                f"View SQL was dialect-translated by {n} rule(s), not all exact: "
                + "; ".join(caveats)
                + ". Confirm the operand types against the source before "
                "relying on it.")
        else:
            res.rules_applied.append(RuleApplication(
                "R43_VIEW_DIALECT_TRANSLATED",
                f"{n} dialect rule(s) applied; every one is an exact rewrite"))
            res.warnings.append(
                f"View SQL was dialect-translated by {n} exact rule(s). Verify "
                "its result against the source before relying on it.")
    else:
        # This is what was checked, no more: the rule table matched nothing.
        # A function outside the table (GREATEST/LEAST null handling, SPLIT's
        # regex separator, ZEROIFNULL) ships as written and is not parsed here.
        res.rules_applied.append(RuleApplication(
            "R42_VIEW_PORTABLE_SQL",
            "no known Snowflake-only construct matched; functions not in the "
            "rule table are carried verbatim and may fail or differ at Spark "
            "parse time"))
        res.warnings.append(
            "View SQL was carried over unchanged: no known Snowflake-only "
            "construct matched, and functions not in the rule table were not "
            "checked. Verify its result against the source before relying on it.")
    # The view's COMMENT is emitted here AND sent as the catalog API's
    # `description`, so the reviewed statement and the applied object carry
    # the same documentation rather than one of them quietly carrying less.
    res.description = str(meta.get("comment") or "")
    # The header column list RENAMES the body's output columns, so it is
    # carried. Dropped, `V(CUSTOMER, TOTAL) as select CUST_ID, SUM(AMT)` was
    # created with columns CUST_ID and SUM(AMT) while the plan and the
    # catalog API's viewFields said CUSTOMER and TOTAL. It is carried by
    # aliasing the body, never as a view column list: live 2026-09-29 on
    # AIDP (Spark 3.5 + Hive metastore) `CREATE VIEW v (`CUSTOMER`, ...) AS`
    # succeeded and every read of the view then failed
    # INCOMPATIBLE_VIEW_SCHEMA_CHANGE, while `SELECT * FROM (<body>) AS
    # named_columns(...)` read -- and that is the viewText the catalog API
    # already got, so both transports create the same query.
    if view_columns:
        res.rules_applied.append(RuleApplication(
            "R44_VIEW_COLUMN_LIST",
            "the source view's column list is carried: "
            + ", ".join(view_columns)
            + ", as SELECT * FROM (<body>) AS named_columns(<list>) -- not "
              "as a view column list, which AIDP creates and then cannot "
              "read (INCOMPATIBLE_VIEW_SCHEMA_CHANGE, live 2026-09-29). The "
              "catalog API's viewText is the same query"))
        query, head = _named_columns(rewritten, view_columns), " AS "
    else:
        query, head = rewritten, " AS\n"
    res.sql = (f"CREATE VIEW IF NOT EXISTS {_qualify(target_fqn)}"
               + (f" COMMENT {quote_spark_string(res.description)}"
                  if res.description else "")
               + head + query)
    # Same four-key spec as a table's, so one shape travels the whole plan.
    # A view's column types are re-derived by the target from the SQL (see
    # catalog_deploy), which is why nothing here is compared as strictly.
    res.expected_columns = [
        _column_spec(c)
        for c in sorted(record.get("columns") or [],
                        key=lambda c: c.get("ORDINAL_POSITION") or 0)
        if c.get("target_type")]
    return res


def _named_columns(body: str, columns: list[str]) -> str:
    """`body` as a query whose output columns are named `columns`.

    The derived-table alias is the form AIDP reads back; a view column list
    is not (build_create_view, R44).
    """
    return (f"SELECT * FROM (\n{body}\n) AS named_columns("
            + ", ".join(_q(c) for c in columns) + ")")


def _view_text(sql: str | None) -> str:
    """The SELECT of a generated CREATE VIEW, for the catalog API.

    The API takes the query alone. build_create_view already writes a view's
    column list into the query (`SELECT * FROM (<body>) AS
    named_columns(<list>)`), so this is that same query. A CREATE VIEW that
    still carries a header list (an older plan) has its body wrapped the
    same way, so its output columns carry the list's names.

    Comments are removed. Live, the API refused a view carrying one --
    "inline SQL comments are not allowed" -- in any style, where Spark SQL
    takes it. A comment carries no meaning; the reviewed CREATE VIEW keeps
    it, and the lexer leaves a `--` inside a literal alone.
    """
    try:
        body = lexer.strip_comments(extract_view_body(sql or "")).strip()
        columns = extract_view_columns(sql or "")
    except ValueError:
        return ""
    if not columns:
        return body
    return _named_columns(body, columns)


def _streams_by_table(census: dict | None) -> dict[str, list[str]]:
    """{table: [streams reading it]} from the census STREAM rows.

    SHOW STREAMS' `table_name` is the fully qualified base table, recorded
    by the census as `on_table`. Matched exactly, and again with quotes
    stripped, to the inventory's `DB.SCHEMA.TABLE` identifier.
    """
    out: dict[str, list[str]] = {}
    for obj in (census or {}).get("objects") or []:
        if obj.get("kind") != "STREAM" or not obj.get("on_table"):
            continue
        table = str(obj["on_table"])
        for key in {table, table.replace('"', "")}:
            out.setdefault(key, []).append(str(obj.get("source_identifier")))
    return out


def build_ddl_payload(inventory: dict, plan: dict) -> dict:
    """The whole `ddl` stage as a pure function: inventory + plan -> payload.

    Emits in wave order, so a view always follows the tables it reads. Shared
    by the CLI stage and the emulated (demo) pipeline, so the two cannot
    drift apart.

    A member of a dependency cycle is NOT emitted. PLANNED_OBJECTS.md lists
    those objects as excluded from the ordering pending a human decision, and
    the ddl stage used to re-add every un-waved clone target -- exactly the
    cycle members -- so the two artifacts contradicted each other and deploy
    attempted views whose dependency did not exist.
    """
    by_id = {r["source_identifier"]: r for r in inventory["inventory"]}
    name_map = plan.get("target_names", {})
    cycles = [list(c) for c in plan.get("cycles", [])]
    # Stuck, but in no cycle: it depends on one. Named as such -- it used to
    # be folded into the cycle and told it was in "dependency cycle with"
    # objects it only reads.
    behind = dict(plan.get("blocked_behind_cycle") or {})
    in_cycle = {n for c in cycles for n in c} | set(behind)
    ordered = [i for wave in plan.get("waves", []) for i in wave]
    # A set: `not in` a 50,000-entry list, once per clone target, is
    # 2.5 billion comparisons at estate scale.
    waved = set(ordered)
    ordered += [i for i in plan.get("clone_targets", [])
                if i not in waved and i not in in_cycle]

    streams = _streams_by_table(inventory.get("census") or plan.get("census"))
    # A dynamic table or materialized view the plan carries as a TABLE
    # SNAPSHOT (plan.build.snapshot_kind) is created as a table, whatever
    # SHOW listed it under: its rows are copied, its query is the generated
    # refresh job's (target/generated_jobs.py), never a CREATE VIEW.
    snapshots = {c["source_identifier"]: c["snapshot_of"]
                 for c in plan.get("can_migrate") or []
                 if c.get("snapshot_of")}
    statements, blocked = [], []
    for ident in sorted(i for i in plan.get("clone_targets", []) if i in in_cycle):
        rec = by_id.get(ident) or {}
        if ident in behind:
            reason = ("not emitted: not in a cycle, but it depends on the "
                      "dependency cycle " + ", ".join(behind[ident])
                      + ", which cannot be ordered; PLANNED_OBJECTS.md lists "
                      "it as blocked behind that cycle, and no edge was "
                      "broken to force an order")
        else:
            others = sorted(n for c in cycles if ident in c
                            for n in c if n != ident)
            reason = ("not emitted: dependency cycle with "
                      + (", ".join(others) or "itself")
                      + "; PLANNED_OBJECTS.md lists it under Dependency "
                      "cycles for a human decision, and no edge was broken "
                      "to force an order")
        blocked.append({"source_identifier": ident,
                        "object_type": rec.get("object_type"),
                        "reason": reason})
    # `plan --secure-views as-view`: planned as plain views, and said so on
    # the statement. The sentence is the plan's own, so the two agree.
    secure_ids = set(plan.get("secure_views_as_views") or [])
    as_views = {c["source_identifier"]: c.get("kind_warning") or ""
                for c in plan.get("can_migrate") or []
                if c["source_identifier"] in secure_ids}
    for ident in ordered:
        rec = by_id.get(ident)
        if rec is None:
            continue
        object_type = ("TABLE" if ident in snapshots
                       else rec.get("object_type"))
        if object_type == "VIEW" and ident in as_views:
            meta = {**(rec.get("source_metadata") or {}), "is_secure": "false"}
            res = build_create_view({**rec, "source_metadata": meta},
                                    name_map[ident], name_map)
            if not res.blocked:
                res.rules_applied.append(RuleApplication(
                    "R60_SECURE_VIEW_AS_PLAIN",
                    "Snowflake SECURE view created as a plain view at the "
                    "operator's request (plan --secure-views as-view); "
                    "SECURE has no AIDP equivalent and is dropped"))
                res.warnings.insert(0, as_views[ident])
        elif object_type == "VIEW":
            res = build_create_view(rec, name_map[ident], name_map)
        else:
            res = build_create_table(
                rec, name_map[ident],
                geospatial=inventory.get("geospatial_mode"),
                streams_on=streams.get(ident, ()))
        if res.blocked:
            blocked.append({"source_identifier": ident,
                            "object_type": object_type,
                            "reason": res.blocked_reason})
            continue
        statements.append({
            "source_identifier": res.source_identifier,
            "object_type": object_type,
            **({"snapshot_of": snapshots[ident]} if ident in snapshots
               else {}),
            "target_fqn": res.target_fqn, "sql": res.sql,
            # The source COMMENT, for the transports that can apply it.
            "description": res.description,
            "rules_applied": [asdict(r) for r in res.rules_applied],
            "warnings": res.warnings,
            "omitted_properties": res.omitted_properties,
            # Deployment verifies the structure against this, not just the name.
            "expected_columns": res.expected_columns,
            # Source settings with an AIDP equivalent that this version does
            # not apply. Reported, never silently invented.
            "deferred_properties": res.deferred_properties,
            # How the copy reads and converts each column. Tables only
            # -- the STATEMENT's kind, not the source's: a materialized view
            # planned as a table snapshot is a VIEW in the inventory, and
            # asking that left its copy to read every column bare.
            **({"columns": res.copy_columns,
                # The settings carried into the CREATE TABLE, and the Delta
                # clauses the structure notebook applies for them.
                "carried_properties": res.carried_properties,
                "delta_features": res.delta_features}
               if object_type != "VIEW" else {}),
            # The catalog API takes a view's body as a field, not as CREATE
            # VIEW text, so it is carried separately.
            **({"view_text": _view_text(res.sql)}
               if object_type == "VIEW" else {})})

    return {"statements": statements, "blocked": blocked,
            # Checked on the way out so no caller can forget to ask: a plan
            # that cannot be created is worth knowing about before it is
            # handed to a workflow.
            "target_rejected": unsupported_target_types(statements),
            "bronze_catalog_prefix": plan.get("bronze_catalog_prefix")}
