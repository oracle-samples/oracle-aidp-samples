"""How the migration scripts READ Snowflake from inside AIDP. Two modes.

LIVE-VERIFIED 2026-09-16 on a real cluster (Spark 3.5, AIDP 4.x):

  connector (default)  spark.read.format("aidataplatform") with
                       type=SNOWFLAKE. Talked to the account, read a table
                       (87 rows x 9 cols) and ran a pushdown query
                       (`current_user()` = the service user). Needs NO extra
                       cluster library — the format is built in — and needs
                       NO successful catalog crawl.

  external-catalog     three-part names `catalog.schema.table` against a
                       registered EXTERNAL catalog. Cheaper (no per-read
                       Snowflake login) but it can only see what the CRAWLER
                       has already discovered, and on the validated
                       deployment the crawler failed with
                       "CONNECTOR_0067 ... Login has timed out" while the
                       connector above worked with the same credentials.

So: connector mode is the default because it is the one proven end to end,
and external-catalog mode is kept for a deployment whose crawl succeeds.

The option NAMES are the live-verified raw ones, which differ from the
Python helper's keyword names — `user.name` (not `user`), `database.name`,
`authentication.method` ∈ {Basic, KeyPair}, `private.key.content`. A wrong
name fails loud (`DATA_ACCESS_LAYER_0001 - Required spark option ... was not
provided`), which is how these were established.

Credentials come from a CONFIG FILE, never from arguments: the same rule the
control plane follows. Point --source-config at a JSON file shaped like
snowmig-config.example.yaml (JSON, on the workspace mount).
"""
from __future__ import annotations

import gzip
import hashlib
import json
import pathlib
import re

__all__ = ["SOURCE_MODES", "SourceConfigError", "SnowflakeSource",
           "write_step_output", "read_plan_json",
           "load_source_config"]

SOURCE_MODES = ("connector", "external-catalog")

AIDP_FORMAT = "aidataplatform"


# The verbs this transport may send. Deliberately narrower than the
# control-plane transport's list: WITH is absent, because following a CTE to
# the statement it prefixes needs the engine's lexer, which does not exist on
# a cluster. A read that needs a CTE can be written as a subquery.
PUSHDOWN_READ_VERBS = ("SELECT", "SHOW", "DESCRIBE", "DESC", "EXPLAIN")


class SourceWriteRefused(PermissionError):
    """A statement that is not a read was handed to the pushdown transport."""


def _code_only(sql: str) -> str:
    """`sql` with string literals and comments blanked, length preserved.

    A `;` inside a literal is data, not a statement boundary, and `--` or
    `//` inside one is not a comment. Blanking rather than deleting keeps offsets, so the
    scan cannot be confused about where anything starts.

    Lexed as SNOWFLAKE lexes: a backslash escapes only inside a '...'
    string; a "..." identifier ends at the first quote that is not doubled.
    Honouring a backslash there too made `"a\\"; delete from T` one
    identifier to this scan and two statements to Snowflake -- the guard
    failed open.
    """
    out = []
    i, n = 0, len(sql)
    while i < n:
        c = sql[i]
        two = sql[i:i + 2]
        if c in ("'", '"'):
            out.append(" ")
            i += 1
            closed = False
            while i < n:
                if c == "'" and sql[i] == "\\" and i + 1 < n:  # escape
                    out.append("  ")
                    i += 2
                    continue
                if sql[i] == c:
                    if sql[i:i + 2] == c * 2:           # doubled = literal
                        out.append("  ")
                        i += 2
                        continue
                    out.append(" ")
                    i += 1
                    closed = True
                    break
                out.append("\n" if sql[i] == "\n" else " ")
                i += 1
            if not closed:
                raise SourceWriteRefused(
                    f"an unclosed {c} literal; refused (fails closed)")
            continue
        if two == "$$":
            out.append("  ")
            i += 2
            while i < n and sql[i:i + 2] != "$$":
                out.append("\n" if sql[i] == "\n" else " ")
                i += 1
            if i >= n:
                raise SourceWriteRefused(
                    "an unclosed $$ string; refused (fails closed)")
            out.append("  ")
            i += 2
            continue
        # `//` is a line comment in Snowflake exactly as `--` is. Missing it
        # let an apostrophe in `// it's` open a phantom literal that hid the
        # `;` on the next line -- two statements read as one.
        if two in ("--", "//"):
            while i < n and sql[i] != "\n":
                out.append(" ")
                i += 1
            continue
        if two == "/*":
            # Snowflake ends a block comment at the FIRST `*/` (no nesting),
            # as the engine's lexer does. Counting depth made
            # `select 1 /* /* */ ; delete from t` one statement here and
            # two to Snowflake. An unclosed comment is refused.
            i += 2
            out.append("  ")
            while i < n and sql[i:i + 2] != "*/":
                out.append("\n" if sql[i] == "\n" else " ")
                i += 1
            if i >= n:
                raise SourceWriteRefused(
                    "an unclosed /* comment; refused (fails closed)")
            out.append("  ")
            i += 2
            continue
        out.append(c)
        i += 1
    return "".join(out)


def assert_pushdown_read_only(sql: str) -> None:
    """Refuse anything that is not a single read. Fails closed.

    The cluster-side counterpart of the control plane's `assert_read_only`:
    the credential the notebook holds may well be able to write, and the
    only thing standing between a migration and a modified SOURCE is this
    check.
    """
    code = _code_only(sql or "")
    if "->>" in code:
        # Snowflake's flow operator chains another statement into the same
        # request: `select 1 ->> delete from t` would pass a leading-verb
        # check. Nothing this plugin generates uses it.
        raise SourceWriteRefused(
            "the ->> flow operator chains statements; refused")
    statements = [s for s in code.split(";") if s.strip()]
    if not statements:
        raise SourceWriteRefused(
            f"empty statement refused; this transport is read-only against "
            f"Snowflake (allowed: {', '.join(PUSHDOWN_READ_VERBS)})")
    if len(statements) > 1:
        raise SourceWriteRefused(
            f"{len(statements)} statements in one pushdown; refused. A "
            f"second statement is how a write rides along behind a read.")
    verb = statements[0].split()[0].upper() if statements[0].split() else ""
    if verb == "WITH":
        raise SourceWriteRefused(
            "a CTE is refused by this transport: deciding whether `WITH ... "
            "INSERT` is a read needs the engine's scanner, which does not "
            "run on the cluster. Write the CTE as a subquery.")
    if verb not in PUSHDOWN_READ_VERBS:
        raise SourceWriteRefused(
            f"{verb or 'unrecognised statement'} refused: this transport is "
            f"read-only against Snowflake, whatever the credential allows "
            f"(allowed: {', '.join(PUSHDOWN_READ_VERBS)}).")


class SourceConfigError(ValueError):
    """The source config is missing, unreadable or incomplete."""


def q(identifier: str) -> str:
    """Backtick-quote one Spark identifier."""
    return "`" + str(identifier).replace("`", "``") + "`"


def _sql_ident(identifier: str) -> str:
    """Double-quote one SNOWFLAKE identifier (the pushdown runs there)."""
    return '"' + str(identifier).replace('"', '""') + '"'


# An identifier Snowflake reads WITHOUT quotes, and so resolves upper-cased.
_UNQUOTED_IDENT = re.compile(r"^[A-Za-z_][A-Za-z0-9_$]*$")


def _database_name(name: str) -> str:
    """The name Snowflake resolves the config's database to, unquoted.

    `database: snowmig_db` is an unquoted identifier to the connector, so it
    names SNOWMIG_DB. `database: '"MyDb"'` -- written with its double quotes,
    as Snowflake SQL would -- names the mixed-case MyDb exactly. Any other
    value that is not a plain identifier can only have been meant verbatim.
    Every statement that names the database goes through here, so no two
    of them can address different databases.
    """
    text = str(name).strip()
    if len(text) >= 2 and text[0] == text[-1] == '"':
        return text[1:-1].replace('""', '"')
    if _UNQUOTED_IDENT.match(text):
        return text.upper()
    return text


def _sql_database(name: str) -> str:
    """The config's database, quoted as the name Snowflake resolves it to."""
    return _sql_ident(_database_name(name))


# The copy expressions for a column whose live type is not the planned one,
# used only under `mapping.source_type_drift: convert`. A mirror of
# snowflake_source/dialect/types.copy_expressions -- this module is inlined
# into every notebook and cannot import the engine -- held to it by a parity
# test. The TIME precision is the live read's default (all nine digits);
# GEOGRAPHY reads as GeoJSON, as a plan without a recorded mode does.
_DRIFT_NUMERIC = {"NUMBER", "DECIMAL", "NUMERIC", "INT", "INTEGER", "BIGINT",
                  "SMALLINT", "TINYINT", "BYTEINT"}
_DRIFT_FLOATS = {"FLOAT", "FLOAT4", "FLOAT8", "DOUBLE", "REAL",
                 "DOUBLE PRECISION"}
_DRIFT_ZONED = ("TIMESTAMP_TZ", "TIMESTAMP_LTZ", "TIMESTAMP")
_DRIFT_JSON = ("VARIANT", "OBJECT", "ARRAY", "MAP")
_DRIFT_GEO = ("GEOGRAPHY", "GEOMETRY")


def live_copy_expressions(data_type: str, target_type: str, *,
                          name: str) -> tuple[str, str]:
    """(read_expr, convert_expr) for `name` read as its LIVE `data_type`
    and converted to the EXISTING target column's `target_type`."""
    col = '"' + str(name).replace('"', '""') + '"'
    out = "`" + str(name).replace("`", "``") + "`"
    key = str(data_type or "").strip().upper().split("(")[0].strip()
    target = str(target_type or "")
    upper = target.upper()
    if upper.startswith(("ARRAY<", "MAP<", "STRUCT<")):
        read = f"{col}::ARRAY::VARCHAR" if key == "VECTOR" \
            else f"{col}::VARIANT::VARCHAR"
        schema = target if "STRUCT<" in upper else target.lower()
        literal = "'" + schema.replace("\\", "\\\\").replace("'", "\\'") + "'"
        return read, f"from_json({out}, {literal})"
    if key in _DRIFT_NUMERIC:
        return f"{col}::VARCHAR", f"CAST({out} AS {target})"
    if key in _DRIFT_FLOATS:
        return f"TO_VARCHAR({col}, 'TME')", f"CAST({out} AS {target})"
    if key == "TIME":
        return f"TO_VARCHAR({col}, 'HH24:MI:SS.FF9')", out
    if key == "TIMESTAMP_NTZ":
        return (f"TO_VARCHAR({col}, 'YYYY-MM-DD HH24:MI:SS.FF9')",
                f"CAST({out} AS {target})")
    if key in _DRIFT_ZONED:
        return (f"TO_VARCHAR({col}, 'YYYY-MM-DD\"T\"HH24:MI:SS.FF9TZH:TZM')",
                f"CAST({out} AS {target})")
    if key in _DRIFT_GEO:
        return f"ST_ASGEOJSON({col})::VARCHAR", out
    if key in _DRIFT_JSON and upper == "STRING":
        return f"TO_JSON({col}::VARIANT)", out
    return col, out


def _sql_literal(value: str) -> str:
    """Escape a string literal for Snowflake SQL.

    The backslash first: Snowflake reads it as an escape inside '...', so a
    table named `a\\` made `'a\\'` an unterminated literal that swallowed the
    SQL after it.
    """
    return str(value).replace("\\", "\\\\").replace("'", "''")


# The /Workspace mount can serve a report that was just written before its
# bytes are all there: an incomplete read is retried briefly, then fails
# loudly, naming the file. Shared by every stage that re-reads a report.
REPORT_READ_TRIES = 5
REPORT_READ_WAIT = 2.0


def read_report_json(path: pathlib.Path) -> dict:
    """A report's JSON, retried briefly when the read comes back incomplete;
    still invalid after that is a loud failure that names the file."""
    import time
    last = None
    for attempt in range(REPORT_READ_TRIES):
        try:
            return json.loads(pathlib.Path(path).read_text(encoding="utf-8"))
        except json.JSONDecodeError as exc:
            last = exc
            if attempt + 1 < REPORT_READ_TRIES:
                time.sleep(REPORT_READ_WAIT)
    raise ValueError(f"{pathlib.Path(path).name} is not valid JSON after "
                     f"{REPORT_READ_TRIES} reads ({last}); it was not "
                     f"overwritten -- inspect it before re-running")


def load_source_config(path: str | pathlib.Path) -> dict:
    """Read the Snowflake connection config (JSON) from the workspace."""
    p = pathlib.Path(path).expanduser()
    try:
        text = p.read_text(encoding="utf-8")
    except OSError as exc:
        raise SourceConfigError(
            f"source config not readable at {p}: {exc.strerror}") from exc
    if p.suffix.lower() in (".yaml", ".yml"):
        try:
            import yaml
        except ImportError as exc:
            raise SourceConfigError(
                f"{p} is YAML but PyYAML is not on the cluster; write the "
                f"config as JSON instead") from exc
        try:
            data = yaml.safe_load(text) or {}
        except yaml.YAMLError as exc:
            # The parser's message quotes the offending line, which can be
            # the password line: only its position is reported.
            mark = getattr(exc, "problem_mark", None)
            where = f" at line {mark.line + 1}" if mark is not None else ""
            raise SourceConfigError(
                f"{p} is not valid YAML{where}; the line is withheld, "
                f"since it may hold the credential") from None
    else:
        try:
            data = json.loads(text) if text.strip() else {}
        except json.JSONDecodeError as exc:
            raise SourceConfigError(
                f"{p} is not valid JSON at line {exc.lineno}; the line is "
                f"withheld, since it may hold the credential") from None
    if not isinstance(data, dict):
        raise SourceConfigError(f"{p}: expected a mapping at the top level")
    # THE MIGRATION CONFIG IS ONE FILE FOR BOTH ENDS: the Snowflake connection
    # nested under `snowflake:`, the AIDP coordinates under `aidp:`. That is
    # the file `provision --source-config` reads, and it uploads the
    # `snowflake:` block as JSON (`plan/<stem>.json`) -- so the shape that
    # reaches the mount is the nested one, and reading the top level for
    # `account` found nothing but the envelope key. The failure surfaced on
    # the cluster, as every required field missing at once, which reads
    # like a broken credential rather than a config one level too deep.
    nested = data.get("snowflake")
    if isinstance(nested, dict):
        return dict(nested)
    return data


class SnowflakeSource:
    """Read-only access to the Snowflake source, in either mode.

    Nothing here can write: every method issues a read, and the connector is
    read-only in AIDP 4.0 by Oracle's own statement.
    """

    def __init__(self, spark, *, mode: str = "connector",
                 config: dict | None = None,
                 external_catalog: str | None = None,
                 session_schema: str | None = None):
        if mode not in SOURCE_MODES:
            raise SourceConfigError(
                f"unknown source mode {mode!r}; expected one of "
                f"{list(SOURCE_MODES)}")
        self.spark = spark
        self.mode = mode
        self.external_catalog = external_catalog
        self._options: dict[str, str] = {}
        # A REAL schema, used only to scope a pushdown session. The connector
        # validates this option against the schemas it can see and rejects
        # INFORMATION_SCHEMA itself with DATA_ACCESS_LAYER_0031 -- but a
        # pushdown query scoped to a real schema may reference
        # INFORMATION_SCHEMA freely (both established live).
        self.session_schema = session_schema or (config or {}).get("schema")

        if mode == "external-catalog":
            if not external_catalog:
                raise SourceConfigError(
                    "external-catalog mode needs --source-catalog")
            return

        cfg = config or {}
        missing = [k for k in ("account", "warehouse", "database", "user",
                               "auth") if not cfg.get(k)]
        if missing:
            raise SourceConfigError(
                "source config is missing required field(s): "
                + ", ".join(sorted(missing)))
        account = str(cfg["account"]).strip()
        # The connector wants the account/server URL host, which is what the
        # Snowflake console calls "Account/Server URL".
        host = str(cfg.get("host")
                   or f"{account}.snowflakecomputing.com").strip()
        opts = {
            "type": "SNOWFLAKE",
            "host": host,
            "port": str(cfg.get("port") or 443),
            "database.name": str(cfg["database"]).strip(),
            "user.name": str(cfg["user"]).strip(),
            "warehouse": str(cfg["warehouse"]).strip(),
        }
        if cfg.get("role"):
            opts["role"] = str(cfg["role"]).strip()

        # A secret may be inline in the one config file, or in a file the
        # config points at. On a cluster the inline form is usually the only
        # one available, since the workspace mount carries the config but not
        # the operator's home directory.
        def secret(inline, path_field):
            if cfg.get(inline):
                return str(cfg[inline])
            if cfg.get(path_field):
                return pathlib.Path(
                    str(cfg[path_field])).expanduser().read_text(encoding="utf-8").strip()
            return None

        auth = str(cfg["auth"]).strip().lower()
        if auth == "keypair":
            key = secret("private_key", "key_path")
            if not key:
                raise SourceConfigError(
                    "auth: keypair needs `private_key` (inline) or `key_path`")
            opts["authentication.method"] = "KeyPair"
            opts["private.key.content"] = key
            passphrase = secret("key_passphrase", "key_passphrase_path")
            if passphrase:
                opts["private.key.pass.phrase"] = passphrase
        elif auth == "password":
            password = secret("password", "password_path")
            if not password:
                raise SourceConfigError(
                    "auth: password needs `password` (inline) or "
                    "`password_path`")
            opts["authentication.method"] = "Basic"
            opts["password"] = password
        else:
            raise SourceConfigError(
                f"auth {auth!r} is not supported by the AIDP Snowflake "
                f"connector; use keypair (preferred) or password")
        self._options = opts

    # -- describing the estate ------------------------------------------

    def database(self) -> str | None:
        return self._options.get("database.name")

    def qualified(self, schema: str, table: str) -> str:
        """`"DB"."SCHEMA"."TABLE"` for a pushdown, the database from the config.

        LIVE 2026-09-29: the pushdown session has NO current schema, whatever
        the connector's `schema` option says, so an unqualified `"T"` fails
        with "Object does not exist" -- the root cause of the batched count's
        CONNECTOR_0099. Every table a pushdown names goes through here.
        Schema and table keep their exact case: they come from
        INFORMATION_SCHEMA, where the case is the name.
        """
        database = self.database()
        if not database:
            raise SourceConfigError(
                "a qualified pushdown needs the source database; add "
                "`database:` to the source config")
        return (f"{_sql_database(database)}.{_sql_ident(schema)}."
                f"{_sql_ident(table)}")

    def pushdown(self, sql: str, *, schema: str | None = None):
        """Run `sql` IN SNOWFLAKE and return a DataFrame.

        Refuses anything that is not a single read statement, whatever the
        credential allows -- see `assert_pushdown_read_only`.

        Connector mode only. The `schema` option must name a REAL schema:
        the connector rejects INFORMATION_SCHEMA there with
        DATA_ACCESS_LAYER_0031, while a query scoped to a real schema may
        reference INFORMATION_SCHEMA freely. Both established live.
        """
        # Before the mode check and before anything reaches Spark: the
        # refusal must not depend on configuration being right.
        assert_pushdown_read_only(sql)
        if self.mode != "connector":
            raise SourceConfigError(
                "pushdown is connector-mode only; external-catalog mode has "
                "no Snowflake session to push into")
        scope = schema or self.session_schema
        if not scope:
            raise SourceConfigError(
                "connector pushdown needs a REAL schema to scope the "
                "session: add `schema:` to the source config or pass "
                "--session-schema. INFORMATION_SCHEMA is not accepted there.")
        return (self.spark.read.format(AIDP_FORMAT)
                .options(**self._options)
                .option("schema", scope)
                .option("pushdown.sql", sql)
                .load())

    def source_counts(self, schema: str, tables: list[str], *,
                      chunk: int = 50) -> dict[str, int]:
        """`{table: COUNT(*)}` for many tables in ONE round trip per chunk.

        Connector mode opens a Snowflake session per read, and the copy needs
        a source count twice per table (before, and again after, to catch a
        source that moved during the copy). Per-table counting therefore cost
        more session setup than the copy itself on a live run. A single
        UNION ALL answers a whole schema instead; it is chunked because a
        statement with thousands of branches is its own problem.

        Every branch names its table "DB"."SCHEMA"."TABLE" (see `qualified`):
        unqualified, the live pushdown session refused every chunk with
        CONNECTOR_0099 and the copy fell back to a count per table.
        Qualified, 50 tables answered in one query in 25 s (2026-09-29).

        External-catalog mode has no session to amortise, so it falls back to
        a count per table -- correct either way, and the caller does not care.
        """
        out: dict[str, int] = {}
        if self.mode != "connector":
            for table in tables:
                out[table] = self.read_table(schema, table).count()
            return out
        for start in range(0, len(tables), chunk):
            batch = tables[start:start + chunk]
            sql = " union all ".join(
                # The literal is the table's own name, so one query can carry
                # many counts and still say which is which. Both the literal
                # and the identifier are escaped: one apostrophe in a table
                # name would otherwise break the whole chunk.
                f"select '{_sql_literal(t)}' as SNOWMIG_TABLE, "
                f"count(*) as SNOWMIG_N from {self.qualified(schema, t)}"
                for t in batch)
            for row in self.pushdown(sql, schema=schema).collect():
                data = row.asDict()
                out[str(data["SNOWMIG_TABLE"])] = int(data["SNOWMIG_N"])
        return out

    def live_columns(self, schema: str, tables: list[str], *,
                     chunk: int = 200) -> dict[str, dict[str, str]]:
        """`{table: {column: type}}` for many tables, from INFORMATION_SCHEMA.

        The copy compares the LIVE source's columns with the target's before
        it moves a row. The connector's table read answered that from its
        own metadata lookup, at 126-241 s per table (2026-09-29); one
        INFORMATION_SCHEMA.COLUMNS query answers a chunk of tables instead.
        Types are what the copy's pre-flight compares: NUMBER(p,s) becomes
        `decimal(p,s)` (the DECIMAL check), everything else is Snowflake's
        own type name lower-cased. A table the query does not list is ABSENT
        from the result, never an empty list -- the caller says so.
        Connector mode only.
        """
        out: dict[str, dict[str, str]] = {}
        database = self.database()
        if not database:
            raise SourceConfigError(
                "reading INFORMATION_SCHEMA needs the source database; add "
                "`database:` to the source config")
        for start in range(0, len(tables), chunk):
            batch = tables[start:start + chunk]
            names = ", ".join(f"'{_sql_literal(t)}'" for t in batch)
            sql = ("select TABLE_NAME, COLUMN_NAME, DATA_TYPE, "
                   "NUMERIC_PRECISION, NUMERIC_SCALE, ORDINAL_POSITION "
                   f"from {_sql_database(database)}.INFORMATION_SCHEMA.COLUMNS "
                   f"where TABLE_SCHEMA = '{_sql_literal(schema)}' "
                   f"and TABLE_NAME in ({names}) "
                   "order by TABLE_NAME, ORDINAL_POSITION")
            rows = sorted((r.asDict() for r in
                           self.pushdown(sql, schema=schema).collect()),
                          key=lambda d: (str(d["TABLE_NAME"]),
                                         int(d["ORDINAL_POSITION"] or 0)))
            for data in rows:
                kind = str(data.get("DATA_TYPE") or "").strip()
                if kind.upper() in ("NUMBER", "DECIMAL", "NUMERIC") and \
                        data.get("NUMERIC_PRECISION") is not None:
                    kind = (f"decimal({int(data['NUMERIC_PRECISION'])},"
                            f"{int(data.get('NUMERIC_SCALE') or 0)})")
                out.setdefault(str(data["TABLE_NAME"]), {})[
                    str(data["COLUMN_NAME"])] = kind.lower()
        return out

    def read_columns(self, schema: str, table: str,
                     select: list[tuple[str, str]]):
        """ONE qualified pushdown: `SELECT <expr> AS "<name>", ... FROM
        "DB"."SCHEMA"."TABLE"`.

        `select` is `[(name, read_expr)]`: a Snowflake expression over the
        quoted source column, unaliased. Each is aliased to the column's
        exact name, so the Spark side sees the source's names. Live
        2026-09-29 this cost ~8.5 s a table where the connector's table read
        cost 126-241 s, and the exact reads it carries (`::VARCHAR`,
        `TO_VARCHAR(.., FF9)`, `::ARRAY::VARCHAR`) are what keep NUMBER,
        TIME/TIMESTAMP fractions and VECTOR/MAP/OBJECT columns intact.
        The whole statement goes through the read-only guard, as every
        pushdown does.
        """
        if not select:
            raise SourceConfigError(f"no columns to read from {schema}.{table}")
        items = ", ".join(f"{expr} as {_sql_ident(name)}"
                          for name, expr in select)
        return self.pushdown(
            f"select {items} from {self.qualified(schema, table)}",
            schema=schema)

    def source_sums(self, schema: str, table: str,
                    columns: list[str]) -> dict[str, str | None]:
        """`{column: exact total as text}` summed IN SNOWFLAKE, one pushdown.

        `select sum("C")::VARCHAR as "C", ... from "DB"."SCHEMA"."TABLE"`:
        Snowflake adds its own NUMBERs exactly, and `::VARCHAR` carries the
        total past the connector's typing, which cut NUMBER to ten
        significant digits live (2026-09-29). None is SQL NULL -- no rows,
        or only NULLs.
        """
        if not columns:
            return {}
        items = ", ".join(f"sum({_sql_ident(c)})::VARCHAR as {_sql_ident(c)}"
                          for c in columns)
        row = self.pushdown(
            f"select {items} from {self.qualified(schema, table)}",
            schema=schema).collect()[0].asDict()
        return {c: (None if row.get(c) is None else str(row[c]))
                for c in columns}

    def register_columns_view(self, schema: str, table: str, view: str,
                              select: list[tuple[str, str]]) -> str:
        """`read_columns` registered as a session temp view; its name."""
        self.read_columns(schema, table, select).createOrReplaceTempView(view)
        return q(view)

    def read_table(self, schema: str, table: str):
        """A DataFrame over one source table."""
        if self.mode == "external-catalog":
            return self.spark.table(
                f"{q(self.external_catalog)}.{q(schema)}.{q(table)}")
        return (self.spark.read.format(AIDP_FORMAT)
                .options(**self._options)
                .option("schema", schema)
                .option("table", table)
                .load())

    def register_temp_view(self, schema: str, table: str, view: str) -> str:
        """Expose a source table to SQL as a temp view, and return its name.

        INSERT ... SELECT needs the source addressable in SQL. In
        external-catalog mode the three-part name already is; in connector
        mode the DataFrame is registered as a session-local temp view, which
        is dropped by the caller.
        """
        if self.mode == "external-catalog":
            return f"{q(self.external_catalog)}.{q(schema)}.{q(table)}"
        self.read_table(schema, table).createOrReplaceTempView(view)
        return q(view)

    def drop_temp_view(self, view: str) -> None:
        if self.mode == "connector":
            self.spark.catalog.dropTempView(view)

    def describe(self) -> dict:
        """What this source is, for the report header. Never the credential."""
        out = {"mode": self.mode}
        if self.mode == "external-catalog":
            out["external_catalog"] = self.external_catalog
        else:
            out.update(session_schema=self.session_schema,
                       host=self._options.get("host"),
                       database=self._options.get("database.name"),
                       user=self._options.get("user.name"),
                       warehouse=self._options.get("warehouse"),
                       role=self._options.get("role"),
                       auth=self._options.get("authentication.method"))
        return out


# What `provision` puts under a plan file's plain name when the file itself
# was too large to upload and went up gzipped beside it (see
# target/provisioning.py COMPRESS_OVER_BYTES).
PLAN_POINTER_KEY = "snowmig_compressed_to"


def read_plan_json(path) -> dict:
    """A plan file `provision` pushed: plain JSON, or a pointer to its gzip
    copy in the same folder, which is read and checked against the digest.

    Raises ValueError naming the file when what is there is not a plan.
    Live 2026-09-29, a failed overwrite upload left ddl_plan.json EMPTY on
    the workspace, and the stage died on a bare JSONDecodeError at char 0.
    """
    path = pathlib.Path(path)
    raw = path.read_bytes()
    try:
        doc = json.loads(raw.decode("utf-8"))
    except ValueError as exc:
        raise ValueError(
            f"{path} is not valid JSON ({len(raw):,} bytes: {exc}). A failed "
            f"upload leaves the file empty or partial; re-run `provision "
            f"--execute` and check that its upload of this file succeeded"
        ) from None
    if not (isinstance(doc, dict) and doc.get(PLAN_POINTER_KEY)):
        return doc
    name = str(doc[PLAN_POINTER_KEY])
    if pathlib.PurePosixPath(name).name != name or "\\" in name:
        raise ValueError(f"{path}: the pointer names {name!r}, which is not "
                         f"a file beside it")
    target = path.with_name(name)
    try:
        data = gzip.decompress(target.read_bytes())
    except (OSError, EOFError) as exc:
        raise ValueError(f"{target} (the plan {path.name} points at) could "
                         f"not be read: {exc}; re-run `provision --execute`"
                         ) from None
    if doc.get("sha256") and hashlib.sha256(data).hexdigest() != doc["sha256"]:
        raise ValueError(f"{target} does not match the digest its pointer "
                         f"{path.name} records: the two were not pushed "
                         f"together; re-run `provision --execute`")
    return json.loads(data.decode("utf-8"))


def write_step_output(output_dir: str | None, name: str,
                      payload: dict) -> str | None:
    """Save one runbook step's values under the workspace's report/output.

    `name` carries the step (S06_discover.json, S10_structure.json, ...).
    An empty output_dir switches it off. Never raises: the step's own
    reports are already written, and a copy that cannot be made must not
    turn a successful stage into a failed one.
    """
    if not output_dir:
        return None
    import datetime as _dt
    import json as _json
    try:
        folder = pathlib.Path(output_dir)
        folder.mkdir(parents=True, exist_ok=True)
        path = folder / name
        path.write_text(_json.dumps(
            {**payload, "written_at": _dt.datetime.now(
                _dt.timezone.utc).isoformat()}, indent=2, default=str))
        print(f"[output] {name} -> {path}", flush=True)
        return str(path)
    except OSError as exc:
        print(f"[output] {name} NOT written to {output_dir}: {exc}", flush=True)
        return None
