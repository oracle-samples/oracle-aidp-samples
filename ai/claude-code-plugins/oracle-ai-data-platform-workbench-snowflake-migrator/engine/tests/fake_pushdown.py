"""A connector pushdown session and a Delta target, faked from live facts.

Used by the copy tests that drive the REAL `SnowflakeSource` (so the SQL it
builds is the SQL under test) over a fake `spark`. What the fake does is
what AIDP Spark 3.5.0 + the AIDP Snowflake connector did on 2026-09-29:

* a pushdown table reference that is not "DB"."SCHEMA"."TABLE" is refused
  ("Object does not exist"): the pushdown session has no current schema;
* a column read BARE (`"N"`) gets the connector's typing: NUMBER is cut to
  ten significant digits (0.1234567890123... -> 0.1234567890000...), TIME
  and TIMESTAMP lose their fraction, and VECTOR / MAP / structured OBJECT
  cannot be opened at all ("Type:50003 is not a valid Types.java value");
* the exact reads -- `"N"::VARCHAR`, `TO_VARCHAR("T", '...FF9')`,
  `"V"::ARRAY::VARCHAR`, `"M"::VARIANT::VARCHAR` -- return exact text;
* Spark's `CAST(`N` AS DECIMAL(p,s))`, `CAST(`F` AS DOUBLE)` and
  `from_json(`V`, '<type>')` turn that text into typed values;
* a decimal SUM keeps precision 38 at the column's scale on both engines:
  past that, Snowflake's SUM errors ("Number out of representable range";
  `sum_overflow="wide"` returns the wide total instead) and non-ANSI Spark's
  SUM(DECIMAL(38,s)) is NULL -- documented behaviour of both, not yet
  reproduced on the live estate.

The connector's own table read (`option("table", ...)`) is recorded as a
`table_reads` entry: live it cost 126-241 s per table, and the copy is not
supposed to use it any more.
"""
from __future__ import annotations

import decimal
import json
import re
import threading

_IDENT = r'"(?:[^"]|"")*"'
_QUALIFIED = re.compile(rf'({_IDENT})\.({_IDENT})\.({_IDENT})')


def _unq(ident: str) -> str:
    return ident[1:-1].replace('""', '"')


class Rows:
    def __init__(self, rows, columns=None, spark=None):
        self._rows = [dict(r) for r in rows]
        self._columns = columns
        self._spark = spark

    def collect(self):
        class Row(dict):
            def asDict(self):
                return dict(self)
        return [Row(r) for r in self._rows]

    def count(self):
        return len(self._rows)

    def createOrReplaceTempView(self, name):
        with self._spark.lock:
            self._spark.views[name.casefold()] = (list(self._columns or []),
                                                  self._rows)


def _fits_38(total: decimal.Decimal, scale: int) -> bool:
    """Whether `total` fits DECIMAL(38, scale): 38 - scale integer digits."""
    digits = total.adjusted() + 1 if total != 0 else 0
    return digits <= 38 - scale


def _truncate(value: decimal.Decimal, digits: int = 10) -> decimal.Decimal:
    """The connector's NUMBER: ten significant digits, the rest zeroed."""
    if value == 0:
        return value
    with decimal.localcontext() as ctx:
        ctx.prec = 80
        exp = value.adjusted() - digits + 1
        quantum = decimal.Decimal(1).scaleb(exp)
        cut = value.quantize(quantum, rounding=decimal.ROUND_DOWN)
        return cut.quantize(value) if value.as_tuple().exponent < 0 else cut


class _Read:
    """`spark.read.format("aidataplatform")...load()` for one statement."""

    def __init__(self, spark):
        self._spark = spark
        self._opts: dict[str, str] = {}

    def format(self, *_a):
        return self

    def options(self, **kw):
        self._opts.update(kw)
        return self

    def option(self, key, value=None):
        self._opts[key] = value
        return self

    def load(self):
        spark = self._spark
        if "table" in self._opts and "pushdown.sql" not in self._opts:
            with spark.lock:
                spark.table_reads.append((self._opts.get("schema"),
                                          self._opts["table"]))
            raise AssertionError("the connector's table read was used")
        sql = self._opts["pushdown.sql"]
        with spark.lock:
            spark.pushdowns.append(sql)
        return spark.snowflake.run(sql, spark)


class FakeSnowflake:
    """The source estate: {(db, schema, table): {"columns": [(name, type,
    precision, scale)], "rows": [{name: value}]}}. Values are held exact
    (Decimal, str for TIME/TIMESTAMP, list/dict for VECTOR/MAP/OBJECT)."""

    def __init__(self, tables, *, fail_info_schema=None,
                 sum_overflow="error"):
        self.tables = tables
        self.fail_info_schema = fail_info_schema
        self.sum_overflow = sum_overflow

    def _table(self, ref):
        m = _QUALIFIED.fullmatch(ref.strip())
        if not m:
            raise RuntimeError(
                "SQL compilation error: Object '" + ref.strip() +
                "' does not exist or not authorized.")
        key = tuple(_unq(p) for p in m.groups())
        if key not in self.tables:
            raise RuntimeError(f"Object '{'.'.join(key)}' does not exist")
        return self.tables[key]

    def run(self, sql, spark):
        if "INFORMATION_SCHEMA.COLUMNS" in sql:
            return self._columns(sql, spark)
        if " union all " in sql or "SNOWMIG_TABLE" in sql:
            return self._counts(sql, spark)
        if "SNOWMIG_SUM" in sql or re.search(r"\bsum\(", sql, re.I):
            return self._sums(sql, spark)
        return self._select(sql, spark)

    # -- INFORMATION_SCHEMA.COLUMNS -------------------------------------
    def _columns(self, sql, spark):
        if self.fail_info_schema:
            raise RuntimeError(self.fail_info_schema)
        m = re.search(r'from\s+(' + _IDENT + r')\.INFORMATION_SCHEMA\.COLUMNS',
                      sql, re.I)
        assert m, f"INFORMATION_SCHEMA must be database-qualified: {sql}"
        db = _unq(m.group(1))
        schema = re.search(r"TABLE_SCHEMA\s*=\s*'((?:[^']|'')*)'",
                           sql).group(1).replace("''", "'")
        names = [n.replace("''", "'") for n in re.findall(
            r"'((?:[^']|'')*)'", sql.split("TABLE_NAME in", 1)[1])] \
            if "TABLE_NAME in" in sql else None
        rows = []
        for (d, s, t), spec in self.tables.items():
            if d != db or s != schema or (names is not None
                                          and t not in names):
                continue
            for i, (name, typ, p, sc) in enumerate(spec["columns"], 1):
                rows.append({"TABLE_NAME": t, "COLUMN_NAME": name,
                             "DATA_TYPE": typ, "NUMERIC_PRECISION": p,
                             "NUMERIC_SCALE": sc, "ORDINAL_POSITION": i})
        return Rows(rows)

    # -- batched counts ---------------------------------------------------
    def _counts(self, sql, spark):
        rows = []
        for branch in re.split(r"\s+union all\s+", sql, flags=re.I):
            m = re.search(r"select '((?:[^']|'')*)' as SNOWMIG_TABLE, "
                          r"count\(\*\) as SNOWMIG_N from (.+)$", branch.strip())
            assert m, branch
            table = self._table(m.group(2))
            rows.append({"SNOWMIG_TABLE": m.group(1).replace("''", "'"),
                         "SNOWMIG_N": len(table["rows"])})
        return Rows(rows)

    # -- SUM pushdown (exact, Snowflake-side) -----------------------------
    def _sums(self, sql, spark):
        m = re.match(r"select (.*) from (.+)$", sql.strip(), re.S)
        table = self._table(m.group(2))
        out = {}
        for sm in re.finditer(r'sum\((' + _IDENT + r')\)::VARCHAR as ('
                              + _IDENT + r')', m.group(1)):
            col, alias = _unq(sm.group(1)), _unq(sm.group(2))
            values = [r[col] for r in table["rows"] if r[col] is not None]
            if not values:
                out[alias] = None
                continue
            scale = next(sc for n, _t, _p, sc in table["columns"] if n == col)
            with decimal.localcontext() as ctx:
                ctx.prec = 80           # exact, then held to NUMBER(38,s)
                total = sum(values, decimal.Decimal(0))
                if not _fits_38(total, scale or 0) and \
                        self.sum_overflow == "error":
                    raise RuntimeError(
                        "100046 (22003): Number out of representable range: "
                        "type FIXEDSB16{nullable=true}, value " + str(total))
                out[alias] = str(total.quantize(decimal.Decimal(1).scaleb(
                    -(scale or 0))))
        return Rows([out])

    # -- the column read ----------------------------------------------------
    def _select(self, sql, spark):
        m = re.match(r"select (.*) from (.+)$", sql.strip(), re.S)
        assert m, sql
        table = self._table(m.group(2))
        types = {n: t for n, t, _p, _s in table["columns"]}
        items = [(e, _unq('"' + a + '"')) for e, a in re.findall(
            r'(.*?) as "((?:[^"]|"")*)"(?:, |$)', m.group(1))]
        assert items, sql
        rows = []
        for row in table["rows"]:
            rows.append({alias: self._eval(expr, row, types)
                         for expr, alias in items})
        return Rows(rows, columns=[a for _e, a in items], spark=spark)

    @staticmethod
    def _eval(expr, row, types):
        bare = re.fullmatch(_IDENT, expr)
        if bare:
            name = _unq(expr)
            value, typ = row[name], types[name].upper()
            if typ in ("VECTOR", "MAP") or typ == "OBJECT" and \
                    isinstance(value, dict) and value.get("__structured__"):
                raise RuntimeError(
                    "IllegalArgumentException: Type:50003 is not a valid "
                    "Types.java value.")
            if typ == "NUMBER" and value is not None:
                return _truncate(value)
            if typ.startswith(("TIME", "TIMESTAMP")) and value is not None:
                return value.split(".")[0]
            return value
        m = re.fullmatch(rf'({_IDENT})::VARCHAR', expr)
        if m:
            value = row[_unq(m.group(1))]
            return None if value is None else str(value)
        m = re.fullmatch(rf"TO_VARCHAR\(({_IDENT}), '[^']*'\)", expr)
        if m:
            value = row[_unq(m.group(1))]
            return None if value is None else str(value)
        m = re.fullmatch(rf'({_IDENT})::(?:ARRAY|VARIANT)::VARCHAR', expr)
        if m:
            value = row[_unq(m.group(1))]
            if isinstance(value, dict):
                value = {k: v for k, v in value.items()
                         if k != "__structured__"}
            return None if value is None else json.dumps(value, default=str)
        raise AssertionError(f"the fake cannot evaluate read_expr {expr!r}")


class FakeLakeSpark:
    """Spark with a Delta target catalog, temp views, and a pushdown read.

    `lake` is {backticked fqn: [(column, spark type)]}; rows land in
    `rows[fqn]`. INSERT evaluates the select list's convert expressions.
    """

    def __init__(self, snowflake, lake, *, describe_raises=None):
        self.snowflake = snowflake
        self.lake = {k: list(v) for k, v in lake.items()}
        self.rows: dict[str, list[dict]] = {k: [] for k in lake}
        self.views: dict[str, tuple[list, list]] = {}
        self.statements: list[str] = []
        self.pushdowns: list[str] = []
        self.table_reads: list = []
        self.describe_raises = describe_raises or {}
        self.lock = threading.Lock()
        spark = self

        class _Catalog:
            """pyspark's `spark.catalog`: only what the source calls."""
            @staticmethod
            def dropTempView(name):
                with spark.lock:
                    spark.views.pop(name.casefold(), None)
        self.catalog = _Catalog()

    @property
    def read(self):
        return _Read(self)

    def _relation(self, ref):
        ref = ref.strip()
        if ref.startswith("`") and ref.count("`") == 2:
            with self.lock:
                cols, rows = self.views[ref.strip("`").casefold()]
            return cols, rows
        return [n for n, _t in self.lake[ref]], self.rows[ref]

    def sql(self, statement):
        flat = " ".join(statement.split())
        with self.lock:
            self.statements.append(flat)
        low = flat.lower()
        if low.startswith("describe"):
            fqn = flat.split(None, 1)[1].strip()
            if fqn in self.describe_raises:
                raise RuntimeError(self.describe_raises[fqn])
            if fqn not in self.lake:
                raise RuntimeError(f"[TABLE_OR_VIEW_NOT_FOUND] {fqn}")
            return Rows([{"col_name": n, "data_type": t}
                         for n, t in self.lake[fqn]])
        if low.startswith("select count(*)"):
            _cols, rows = self._relation(flat.rsplit(" FROM ", 1)[1])
            return Rows([{"n": len(rows)}])
        if low.startswith("select") and "union all" in low:
            out = []
            for branch in re.split(r" UNION ALL ", flat):
                m = re.match(r"SELECT (\d+) AS `i`, COUNT\(\*\) AS `n` "
                             r"FROM (.+)$", branch)
                assert m, branch
                _c, rows = self._relation(m.group(2))
                out.append({"i": int(m.group(1)), "n": len(rows)})
            return Rows(out)
        if low.startswith("select") and "sum(" in low:
            ref = flat.rsplit(" FROM ", 1)[1]
            _cols, rows = self._relation(ref)
            out = {}
            for col, scale, alias, ncol, nalias in re.findall(
                    r"CAST\(SUM\(CAST\(`([^`]+)` AS DECIMAL\(38,(\d+)\)\)\) "
                    r"AS STRING\) AS `([^`]+)`|COUNT\(`([^`]+)`\) AS `([^`]+)`",
                    flat):
                if ncol:
                    out[nalias] = sum(1 for r in rows
                                      if r.get(ncol) is not None)
                    continue
                vals = [decimal.Decimal(str(r[col])) for r in rows
                        if r.get(col) is not None]
                with decimal.localcontext() as ctx:
                    ctx.prec = 80
                    total = sum(vals, decimal.Decimal(0))
                    # Non-ANSI Spark: a DECIMAL(38,s) SUM that overflows is
                    # NULL, not an error and not a wider number.
                    out[alias] = (None if not vals
                                  or not _fits_38(total, int(scale))
                                  else str(total.quantize(
                                      decimal.Decimal(1).scaleb(-int(scale)))))
            return Rows([out])
        if low.startswith("insert"):
            m = re.match(r"INSERT (INTO|OVERWRITE) (\S+) SELECT (.*) "
                         r"FROM (`[^`]+`)$", flat)
            assert m, flat
            kind, tgt, select, src = m.groups()
            items = re.findall(r"(.*?) AS `((?:[^`]|``)*)`(?:, |$)", select)
            assert items, flat
            _cols, src_rows = self._relation(src)
            landed = [{t: self._convert(e, r) for e, t in items}
                      for r in src_rows]
            with self.lock:
                if kind == "OVERWRITE":
                    self.rows[tgt] = []
                self.rows[tgt].extend(landed)
            return Rows([])
        raise AssertionError(f"unexpected statement: {flat}")

    @staticmethod
    def _convert(expr, row):
        def get(name):
            return next(v for k, v in row.items()
                        if k.casefold() == name.casefold())
        m = re.fullmatch(r"`((?:[^`]|``)*)`", expr)
        if m:
            return get(m.group(1))
        m = re.fullmatch(r"CAST\(`([^`]+)` AS DECIMAL\((\d+),(\d+)\)\)", expr,
                         re.IGNORECASE)
        if m:
            v = get(m.group(1))
            return None if v is None else decimal.Decimal(str(v))
        m = re.fullmatch(r"CAST\(`([^`]+)` AS DOUBLE\)", expr, re.IGNORECASE)
        if m:
            v = get(m.group(1))
            return None if v is None else float(v)
        m = re.fullmatch(r"from_json\(`([^`]+)`, '[^']*'\)", expr)
        if m:
            v = get(m.group(1))
            return None if v is None else json.loads(v)
        raise AssertionError(f"the fake cannot evaluate convert_expr {expr!r}")


def source_for(spark, database="SNOWMIG_DB", schema="TYPES"):
    """The REAL SnowflakeSource over the fake read (connector mode)."""
    from dataplane.snowmig_source import SnowflakeSource
    return SnowflakeSource(spark, config={
        "account": "acct", "warehouse": "WH", "database": database,
        "user": "svc", "auth": "password", "password": "p",
        "schema": schema})
