"""Every name the pushdown sends to Snowflake is DATABASE.SCHEMA.TABLE.

Live 2026-09-29 (AIDP Spark 3.5.0, the AIDP Snowflake connector): the
batched source count -- one UNION ALL of `select 'T' ..., count(*) from "T"`
per chunk -- failed with CONNECTOR_0099 on every run, and the copy fell back
to one count per table, which is one Snowflake session per table. The root
cause is the pushdown session: it has NO current schema, whatever the
connector's `schema` option says, so an unqualified `"T"` is "Object does
not exist". The same UNION ALL with every branch written
"DB"."SCHEMA"."TABLE" answered 3 tables in 8.9 s, and 50 tables -- across
databases -- in 25.4 s.

The database is the one the source config names (the connector's
`database.name`). A name typed unquoted there is the upper-cased name to
Snowflake, so it is rendered that way inside the quotes; anything that is
not a plain identifier is quoted verbatim.

The fake read below behaves as the live pushdown session does: any table
reference that is not three-part is refused with Snowflake's own wording.
"""
import re

import pytest

from dataplane.snowmig_source import SnowflakeSource


_REF = re.compile(r'as SNOWMIG_N from\s+((?:"(?:[^"]|"")*"|[A-Za-z_][\w$]*)'
                  r'(?:\.(?:"(?:[^"]|"")*"|[A-Za-z_][\w$]*))*)',
                  re.IGNORECASE)


def _parts(ref):
    return re.findall(r'"((?:[^"]|"")*)"|([A-Za-z_][\w$]*)', ref)


class _Rows:
    def __init__(self, rows):
        self._rows = rows

    def collect(self):
        class Row(dict):
            def asDict(self):
                return dict(self)
        return [Row(r) for r in self._rows]


class _NoCurrentSchemaRead:
    """`spark.read` for the connector: the pushdown SQL is evaluated the way
    the live session does -- counts per fully qualified table, and an
    unqualified reference refused."""

    def __init__(self, counts):
        self.counts = counts          # {(db, schema, table): n}
        self.sql: list[str] = []
        self._pending = None

    def format(self, *_a):
        return self

    def options(self, **_k):
        return self

    def option(self, key, value=None):
        if key == "pushdown.sql":
            self._pending = value
        return self

    def load(self):
        sql = self._pending
        self.sql.append(sql)
        rows = []
        for branch in re.split(r"\s+union all\s+", sql, flags=re.IGNORECASE):
            ref = _REF.search(branch).group(1)
            parts = [(a.replace('""', '"') if a else b.upper())
                     for a, b in _parts(ref)]
            if len(parts) != 3:
                raise RuntimeError(
                    "CONNECTOR_0099 - SQL compilation error: Object "
                    f"'{parts[-1]}' does not exist or not authorized.")
            label = re.search(r"select '((?:[^']|'')*)' as SNOWMIG_TABLE",
                              branch).group(1).replace("''", "'")
            rows.append({"SNOWMIG_TABLE": label,
                         "SNOWMIG_N": self.counts[tuple(parts)]})
        return _Rows(rows)


def _source(counts, database="SNOWMIG_DB"):
    class Spark:
        read = _NoCurrentSchemaRead(counts)
    return SnowflakeSource(Spark(), config={
        "account": "acct", "warehouse": "WH", "database": database,
        "user": "svc", "auth": "password", "password": "p", "schema": "TYPES"})


def test_batched_counts_name_every_table_with_its_database_and_schema():
    counts = {("SNOWMIG_DB", "TYPES", "T_NUM_NEG"): 1,
              ("SNOWMIG_DB", "TYPES", "T_TIME3"): 1,
              ("SNOWMIG_DB", "TYPES", "T_COLLATE"): 4}
    src = _source(counts)
    got = src.source_counts("TYPES", ["T_NUM_NEG", "T_TIME3", "T_COLLATE"])
    assert got == {"T_NUM_NEG": 1, "T_TIME3": 1, "T_COLLATE": 4}
    sql = src.spark.read.sql
    assert len(sql) == 1, "three counts, ONE round trip"
    assert '"SNOWMIG_DB"."TYPES"."T_NUM_NEG"' in sql[0]


def test_an_unqualified_count_is_what_the_live_session_refuses():
    """The premise, pinned: the fake refuses what the live session refused,
    so the test above passes only because the SQL is qualified."""
    read = _NoCurrentSchemaRead({})
    read.option("pushdown.sql", "select 'T' as SNOWMIG_TABLE, count(*) as "
                                'SNOWMIG_N from "T"')
    with pytest.raises(RuntimeError, match="does not exist"):
        read.load()


def test_a_lower_case_database_in_the_config_is_the_upper_case_name():
    """`database: snowmig_db` in the config is SNOWMIG_DB to Snowflake (an
    unquoted identifier); quoting it verbatim would name another database."""
    src = _source({("SNOWMIG_DB", "S", "T"): 2}, database="snowmig_db")
    assert src.source_counts("S", ["T"]) == {"T": 2}
    assert src.qualified("S", "T") == '"SNOWMIG_DB"."S"."T"'


def test_a_database_that_is_not_a_plain_identifier_is_quoted_verbatim():
    src = _source({}, database="my-db")
    assert src.qualified("S", 'a"b') == '"my-db"."S"."a""b"'


def test_schema_and_table_keep_their_exact_case():
    """They come from INFORMATION_SCHEMA, where the case IS the name: a
    table created as "orders_edge" is not ORDERS_EDGE."""
    src = _source({("SNOWMIG_DB", "Mixed", "orders_edge"): 5})
    assert src.source_counts("Mixed", ["orders_edge"]) == {"orders_edge": 5}


def test_counts_are_still_chunked_and_each_chunk_is_qualified():
    counts = {("SNOWMIG_DB", "S", f"T{i}"): i for i in range(5)}
    src = _source(counts)
    got = src.source_counts("S", [f"T{i}" for i in range(5)], chunk=2)
    assert got == {f"T{i}": i for i in range(5)}
    assert len(src.spark.read.sql) == 3
    for sql in src.spark.read.sql:
        assert sql.count('"SNOWMIG_DB"."S".') == sql.lower().count(" from ")


def test_the_batched_count_still_passes_the_read_only_guard():
    """I1: the qualified SQL is still ONE read; a crafted name cannot turn it
    into two statements (the guard runs before anything reaches Spark)."""
    from dataplane.snowmig_source import assert_pushdown_read_only
    src = _source({("SNOWMIG_DB", "S", 'x"; delete from T; --'): 0})
    src.source_counts("S", ['x"; delete from T; --'])
    assert_pushdown_read_only(src.spark.read.sql[0])
