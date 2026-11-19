"""The element types INFORMATION_SCHEMA hides must be READ, cheaply and
read-only, before anything can map them.

Live 2026-09-29, a trial account. INFORMATION_SCHEMA.COLUMNS answers
`VECTOR`, `MAP`, `OBJECT` and `ARRAY` and stops there: a `VECTOR(FLOAT, 4)`
and a `VECTOR(INT, 3)`, a `MAP(VARCHAR, NUMBER(38,0))`, an
`OBJECT(X NUMBER(38,0), Y VARCHAR)` and a plain semi-structured OBJECT all
look alike. `DESCRIBE TABLE` spells the full type in its `type` column --
the shapes below are the exact rows it returned. Without them the mapper
has two choices, both bad: block every such table, or carry a typed vector
as untyped JSON text and never say it could have done better.

Cost is the other half. A DESCRIBE per table is a round trip per table, so
it is issued ONLY for a table holding a column whose detail the mapper
needs, and a table of NUMBER and VARCHAR costs exactly what it cost before.

Inside AIDP there is no DESCRIBE: the connector's pushdown takes a SELECT.
GET_DDL is a SELECT, and its CREATE TABLE text carries the same type
spelling, so discovery batches it -- one qualified pushdown per 50 tables,
the shape the live 50-table UNION ALL count proved (25 s, cross-database).
Unqualified names fail there ("Object does not exist": the pushdown session
has no current schema), so every name is fully qualified. Where the read
cannot happen, the column says so -- "unread" is never "plain".
"""
import importlib.util
import pathlib
import sys

import pytest

from fake_sql import FakeSql
from snowflake_source.conn import assert_read_only
from snowflake_source.dialect.types import needs_type_detail
from snowflake_source.extract.catalog import build_inventory
from snowflake_source.extract.manifest import inventory_from_manifest
from test_catalog import SESSION

SCRIPTS = pathlib.Path(__file__).resolve().parents[1] / "dataplane"


def _is_col(table, name, pos, data_type, **extra):
    base = {"TABLE_SCHEMA": "TYPES", "TABLE_NAME": table,
            "ORDINAL_POSITION": pos, "COLUMN_NAME": name,
            "DATA_TYPE": data_type, "IS_NULLABLE": "YES",
            "NUMERIC_PRECISION": None, "NUMERIC_SCALE": None,
            "CHARACTER_MAXIMUM_LENGTH": None, "DATETIME_PRECISION": None,
            "COMMENT": None}
    base.update(extra)
    return base


def _describe_row(name, type_):
    # The exact row shape DESCRIBE TABLE returned live (snowflake_shapes.json).
    return {"name": name, "type": type_, "kind": "COLUMN", "null?": "Y",
            "default": None, "primary key": "N", "unique key": "N",
            "check": None, "expression": None, "comment": None,
            "policy name": None, "privacy domain": None,
            "write default": None}


def _responses(columns, tables, describe=None, **over):
    r = {"current_user()": SESSION,
         "show schemas": [{"name": "TYPES"}],
         "information_schema.columns": columns,
         "show tables": [{"name": t, "rows": 1} for t in tables],
         "show views": [],
         "table_constraints": [], "show primary keys": [],
         "show unique keys": [], "show imported keys": []}
    if describe is not None:
        r["describe table"] = describe
    r.update(over)
    return r


def _column(inv, table, name):
    rec = next(r for r in inv["inventory"]
               if r["source_identifier"].endswith("." + table))
    return next(c for c in rec["columns"] if c["COLUMN_NAME"] == name)


# ------------------------------------------------ which columns need it

@pytest.mark.parametrize("data_type", ["VECTOR", "MAP", "OBJECT", "ARRAY",
                                       "GEOGRAPHY", "GEOMETRY"])
def test_the_hidden_element_types_need_detail(data_type):
    assert needs_type_detail(data_type, None)


@pytest.mark.parametrize("data_type", ["NUMBER", "TEXT", "VARIANT", "BOOLEAN",
                                       "DATE", "FLOAT", "BINARY"])
def test_a_fully_described_type_needs_none(data_type):
    # VARIANT has no element type to find: it is untyped by definition.
    assert not needs_type_detail(data_type, None)


@pytest.mark.parametrize("data_type", ["TIME", "TIMESTAMP_NTZ",
                                       "TIMESTAMP_TZ", "TIMESTAMP_LTZ"])
def test_a_time_type_needs_detail_only_when_its_precision_is_missing(data_type):
    # INFORMATION_SCHEMA carries DATETIME_PRECISION, which is all DESCRIBE's
    # `TIME(3)` adds. Asking for it anyway would DESCRIBE nearly every table
    # of a real estate -- almost all of them hold a timestamp.
    assert not needs_type_detail(data_type, 9)
    assert needs_type_detail(data_type, None)


# ------------------------------------------------------- laptop `assess`

def test_a_vector_column_carries_the_type_describe_spells():
    sql = FakeSql(_responses(
        [_is_col("T_VECTOR_F", "V", 1, "VECTOR")], ["T_VECTOR_F"],
        describe=[_describe_row("V", "VECTOR(FLOAT, 4)")]))
    inv = build_inventory(sql, ["SNOWMIG_COVERAGE"])
    assert _column(inv, "T_VECTOR_F", "V")["type_detail"] == "VECTOR(FLOAT, 4)"
    describes = [c for c in sql.calls if c.lower().startswith("describe")]
    assert describes == ['describe table "SNOWMIG_COVERAGE"."TYPES"."T_VECTOR_F"']


def test_every_structured_shape_the_trial_returned_is_carried():
    shapes = {"T_MAP": ("M", "MAP", "MAP(VARCHAR(16777216), NUMBER(38,0))"),
              "T_OBJECT_S": ("O", "OBJECT",
                             "OBJECT(X NUMBER(38,0), Y VARCHAR(16777216))"),
              "T_ARRAY_NUM": ("A", "ARRAY", "ARRAY(NUMBER(38,0))"),
              "T_ARRAY_PLAIN": ("A", "ARRAY", "ARRAY"),
              "T_GEOGRAPHY": ("G", "GEOGRAPHY", "GEOGRAPHY")}

    class PerTable(FakeSql):
        def __call__(self, sql, params=None):
            if sql.lower().startswith("describe table"):
                self.calls.append(sql)
                table = sql.rsplit(".", 1)[1].strip('"')
                name, _, detail = shapes[table]
                return [_describe_row(name, detail)]
            return super().__call__(sql, params)

    columns = [_is_col(t, n, 1, dt) for t, (n, dt, _) in shapes.items()]
    inv = build_inventory(PerTable(_responses(columns, list(shapes))),
                          ["DB"])
    for table, (name, _, detail) in shapes.items():
        assert _column(inv, table, name)["type_detail"] == detail


def test_a_table_of_plain_types_costs_no_describe():
    # The bound: NUMBER / TEXT / VARIANT / a timestamp with its precision
    # are fully described by INFORMATION_SCHEMA already.
    sql = FakeSql(_responses(
        [_is_col("ORDERS", "ID", 1, "NUMBER", NUMERIC_PRECISION=38,
                 NUMERIC_SCALE=0),
         _is_col("ORDERS", "BODY", 2, "VARIANT"),
         _is_col("ORDERS", "AT", 3, "TIMESTAMP_NTZ", DATETIME_PRECISION=9)],
        ["ORDERS"]))
    inv = build_inventory(sql, ["DB"])
    assert not [c for c in sql.calls if c.lower().startswith("describe")]
    assert "type_detail" not in _column(inv, "ORDERS", "ID")


def test_a_view_is_never_described():
    # Views are not copied; their column types are the target's to derive.
    sql = FakeSql(_responses(
        [_is_col("V1", "V", 1, "VECTOR")], [],
        **{"show views": [{"name": "V1", "text": "select 1"}],
           "get_ddl": [{"D": "create view V1 as select 1"}]}))
    build_inventory(sql, ["DB"])
    assert not [c for c in sql.calls if c.lower().startswith("describe")]


def test_the_describe_passes_the_read_only_guard():
    # I1: every statement this plugin sends Snowflake is a read.
    sql = FakeSql(_responses(
        [_is_col('we"ird', "V", 1, "MAP")], ['we"ird'],
        describe=[_describe_row("V", "MAP(VARCHAR(16777216), VARCHAR(16777216))")]))
    build_inventory(sql, ["DB"])
    describe = next(c for c in sql.calls if c.lower().startswith("describe"))
    assert describe == 'describe table "DB"."TYPES"."we""ird"'
    assert_read_only(describe)


def test_a_failed_describe_is_recorded_on_the_column_and_in_the_notes():
    class Refusing(FakeSql):
        def __call__(self, sql, params=None):
            if sql.lower().startswith("describe table"):
                raise RuntimeError("SQL access control error")
            return super().__call__(sql, params)

    inv = build_inventory(Refusing(_responses(
        [_is_col("T_MAP", "M", 1, "MAP"),
         _is_col("T_MAP", "ID", 2, "NUMBER", NUMERIC_PRECISION=38,
                 NUMERIC_SCALE=0)], ["T_MAP"])), ["DB"])
    m = _column(inv, "T_MAP", "M")
    assert "type_detail" not in m
    assert "access control" in m["type_detail_unread"]
    # A column that never needed the read is not marked.
    assert "type_detail_unread" not in _column(inv, "T_MAP", "ID")
    assert any("DB.TYPES.T_MAP" in n and "DESCRIBE" in n
               for n in inv["extraction_notes"]), inv["extraction_notes"]


def test_a_column_describe_did_not_list_is_unread_not_plain():
    inv = build_inventory(FakeSql(_responses(
        [_is_col("T", "O", 1, "OBJECT")], ["T"],
        describe=[_describe_row("OTHER", "NUMBER(38,0)")])), ["DB"])
    assert "no row for this column" in _column(inv, "T", "O")["type_detail_unread"]


# ------------------------------------------------- in-AIDP discovery (S6)

@pytest.fixture(scope="module")
def discover():
    sys.path.insert(0, str(SCRIPTS))
    spec = importlib.util.spec_from_file_location(
        "snowmig_script_00_type_detail", SCRIPTS / "00_discover_snowflake.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class _DF:
    def __init__(self, rows):
        self._rows = rows

    def collect(self):
        class Row(dict):
            def asDict(self):
                return dict(self)
        return [Row(r) for r in self._rows]


# GET_DDL('TABLE', ...) text. NOT captured live: snowflake_shapes.json holds
# DESCRIBE / SHOW output but no GET_DDL text, so these are constructed from
# the documented syntax. Quoted names, comments holding commas and
# parentheses, defaults and a table constraint are all legal there, so the
# reader must survive them. The clustering clause is written in BOTH
# positions because which one GET_DDL uses is unverified: T_MIXED puts it
# after the column list; T_CLUSTERED* put it BEFORE, the order
# `CREATE TABLE t CLUSTER BY (k) (cols...)` that sqlglot's Snowflake parser
# accepts and the one GET_DDL most likely emits for a clustered table.
_DDLS = {
    "T_VECTOR_F": "create or replace TABLE T_VECTOR_F (\n\tV VECTOR(FLOAT, 4)\n);",
    "T_MIXED": (
        'create or replace TABLE T_MIXED (\n'
        '\tID NUMBER(38,0) NOT NULL autoincrement start 1 increment 1 noorder,\n'
        '\t"lower, col" MAP(VARCHAR(16777216), NUMBER(38,0)) COMMENT \'a, (tricky) one\',\n'
        '\tO OBJECT(X NUMBER(38,0), Y VARCHAR(16777216)) DEFAULT NULL,\n'
        '\tSTATUS VARCHAR(16777216) DEFAULT \'NEW\',\n'
        '\tprimary key (ID)\n'
        ') cluster by (ID) COMMENT=\'t(1)\';'),
    "T_CLUSTERED": (
        'create or replace TABLE T_CLUSTERED cluster by (ID)(\n'
        '\tID NUMBER(38,0),\n'
        '\tV VECTOR(FLOAT, 4)\n'
        ');'),
    "T_CLUSTERED_EXPR": (
        'create or replace TABLE T_CLUSTERED_EXPR cluster by LINEAR(ID, '
        'to_date(TS))(\n'
        '\tID NUMBER(38,0),\n'
        '\tTS TIMESTAMP_NTZ(9),\n'
        '\tM MAP(VARCHAR(16777216), NUMBER(38,0))\n'
        ');'),
}


class _Source:
    """INFORMATION_SCHEMA plus GET_DDL, as the connector's pushdown answers."""

    def __init__(self, tables, columns, *, database="SNOWMIG_COVERAGE",
                 fail_ddl=False):
        self._tables, self._columns = tables, columns
        self._database, self._fail = database, fail_ddl
        self.queries: list[str] = []

    def database(self):
        return self._database

    def pushdown(self, sql, schema=None):
        self.queries.append(sql)
        if "get_ddl" in sql.lower():
            if self._fail:
                raise RuntimeError("CONNECTOR_0099 - something upstream")
            rows = []
            for branch in sql.split(" union all "):
                table = branch.split("SNOWMIG_TABLE")[0].split("'")[-2]
                schema_ = branch.split("'")[1]
                rows.append({"SNOWMIG_SCHEMA": schema_,
                             "SNOWMIG_TABLE": table,
                             "SNOWMIG_DDL": _DDLS[table]})
            return _DF(rows)
        return _DF(self._columns if "COLUMNS" in sql else self._tables)


def _rel(name):
    return {"TABLE_SCHEMA": "TYPES", "TABLE_NAME": name,
            "TABLE_TYPE": "BASE TABLE", "ROW_COUNT": 1, "BYTES": 10}


def _manifest_col(schemas, table, name):
    entry = next(t for s in schemas for t in s["tables"] if t["name"] == table)
    return next(c for c in entry["columns"] if c["name"] == name)


def test_discovery_reads_the_detail_through_one_qualified_get_ddl(discover):
    source = _Source(
        [_rel("T_VECTOR_F"), _rel("T_MIXED"), _rel("PLAIN")],
        [_is_col("T_VECTOR_F", "V", 1, "VECTOR"),
         _is_col("T_MIXED", "ID", 1, "NUMBER", NUMERIC_PRECISION=38,
                 NUMERIC_SCALE=0),
         _is_col("T_MIXED", "lower, col", 2, "MAP"),
         _is_col("T_MIXED", "O", 3, "OBJECT"),
         _is_col("T_MIXED", "STATUS", 4, "TEXT"),
         _is_col("PLAIN", "N", 1, "NUMBER", NUMERIC_PRECISION=38,
                 NUMERIC_SCALE=0)])
    schemas = discover.discover_via_connector(source, wanted=None,
                                              exclude=set())
    ddl_reads = [q for q in source.queries if "get_ddl" in q.lower()]
    assert len(ddl_reads) == 1, "batched: one round trip for both tables"
    assert "'\"SNOWMIG_COVERAGE\".\"TYPES\".\"T_MIXED\"'" in ddl_reads[0]
    assert "PLAIN" not in ddl_reads[0], "a table of plain types is not read"
    # I1 on the cluster side: the pushdown transport's own guard passes it.
    sys.modules["snowmig_source"].assert_pushdown_read_only(ddl_reads[0])

    assert _manifest_col(schemas, "T_VECTOR_F", "V")["type_detail"] == \
        "VECTOR(FLOAT, 4)"
    assert _manifest_col(schemas, "T_MIXED", "lower, col")["type_detail"] == \
        "MAP(VARCHAR(16777216), NUMBER(38,0))"
    assert _manifest_col(schemas, "T_MIXED", "O")["type_detail"] == \
        "OBJECT(X NUMBER(38,0), Y VARCHAR(16777216))"
    assert "type_detail" not in _manifest_col(schemas, "T_MIXED", "ID")


def test_a_failed_get_ddl_marks_the_columns_unread_and_discovery_goes_on(
        discover):
    source = _Source([_rel("T_VECTOR_F")],
                     [_is_col("T_VECTOR_F", "V", 1, "VECTOR")], fail_ddl=True)
    schemas = discover.discover_via_connector(source, wanted=None,
                                              exclude=set())
    col = _manifest_col(schemas, "T_VECTOR_F", "V")
    assert "type_detail" not in col
    assert "CONNECTOR_0099" in col["type_detail_unread"]


def test_with_no_database_to_qualify_with_nothing_is_guessed(discover):
    # An unqualified GET_DDL fails live, and a guessed database reads the
    # wrong table. Neither: the column says why it has no detail.
    source = _Source([_rel("T_VECTOR_F")],
                     [_is_col("T_VECTOR_F", "V", 1, "VECTOR")], database=None)
    schemas = discover.discover_via_connector(source, wanted=None,
                                              exclude=set())
    assert not [q for q in source.queries if "get_ddl" in q.lower()]
    assert "database" in _manifest_col(
        schemas, "T_VECTOR_F", "V")["type_detail_unread"]


def test_the_ddl_column_reader_handles_quotes_comments_and_constraints(
        discover):
    got = discover._ddl_column_types(_DDLS["T_MIXED"])
    assert got == {"ID": "NUMBER(38,0)",
                   "lower, col": "MAP(VARCHAR(16777216), NUMBER(38,0))",
                   "O": "OBJECT(X NUMBER(38,0), Y VARCHAR(16777216))",
                   "STATUS": "VARCHAR(16777216)"}


# Without this, the first "(" -- the clustering key -- was read as the
# column list and the result was {}: every structured column of a clustered
# table became type_detail_unread, and its VECTOR / MAP was blocked on the
# in-AIDP path although GET_DDL had returned the full type.

def test_a_clustering_clause_before_the_columns_is_not_the_column_list(
        discover):
    assert discover._ddl_column_types(_DDLS["T_CLUSTERED"]) == {
        "ID": "NUMBER(38,0)", "V": "VECTOR(FLOAT, 4)"}


def test_an_expression_clustering_key_before_the_columns_is_skipped(
        discover):
    assert discover._ddl_column_types(_DDLS["T_CLUSTERED_EXPR"]) == {
        "ID": "NUMBER(38,0)", "TS": "TIMESTAMP_NTZ(9)",
        "M": "MAP(VARCHAR(16777216), NUMBER(38,0))"}


def test_a_clustered_tables_vector_gets_its_detail_in_discovery(discover):
    source = _Source([_rel("T_CLUSTERED")],
                     [_is_col("T_CLUSTERED", "ID", 1, "NUMBER",
                              NUMERIC_PRECISION=38, NUMERIC_SCALE=0),
                      _is_col("T_CLUSTERED", "V", 2, "VECTOR")])
    schemas = discover.discover_via_connector(source, wanted=None,
                                              exclude=set())
    col = _manifest_col(schemas, "T_CLUSTERED", "V")
    assert col["type_detail"] == "VECTOR(FLOAT, 4)"
    assert "type_detail_unread" not in col


def test_the_two_needs_detail_rules_agree(discover):
    # The dataplane script is uploaded alone and cannot import the engine,
    # so it carries its own copy of the rule. Held together here.
    for dt in ("VECTOR", "MAP", "OBJECT", "ARRAY", "GEOGRAPHY", "GEOMETRY",
               "NUMBER", "TEXT", "VARIANT", "TIME", "TIMESTAMP_TZ", "DATE"):
        for precision in (None, 3):
            assert discover._needs_type_detail(dt, precision) == \
                needs_type_detail(dt, precision), (dt, precision)


# ------------------------------------------------------------ the bridge

def test_the_bridge_carries_the_detail_and_the_unread_reason():
    manifest = {"schemas": [{"name": "TYPES", "errors": [], "views": [],
                             "tables": [{"name": "T", "columns": [
        {"name": "V", "data_type": "VECTOR", "nullable": True,
         "ordinal_position": 1, "type_detail": "VECTOR(INT, 3)"},
        {"name": "M", "data_type": "MAP", "nullable": True,
         "ordinal_position": 2, "type_detail_unread": "GET_DDL failed"}]}]}]}
    inv = inventory_from_manifest(manifest, database="DB")
    cols = {c["COLUMN_NAME"]: c for c in inv["inventory"][0]["columns"]}
    assert cols["V"]["type_detail"] == "VECTOR(INT, 3)"
    assert cols["M"]["type_detail_unread"] == "GET_DDL failed"
