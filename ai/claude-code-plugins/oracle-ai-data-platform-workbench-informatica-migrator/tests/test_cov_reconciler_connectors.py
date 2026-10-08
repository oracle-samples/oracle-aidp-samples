"""Reconciliation connectors: the SQL they issue and how they read results back.

No database and no JVM: the JDBC connector's local mode is driven through a
fake DB-API connection (the object ``jaydebeapi.connect`` would return), its
Spark mode and the AIDP connector through a fake SparkSession that records
every query. What is pinned is what a user of these connectors relies on --
that a WHERE fragment and a sample limit reach the database in a form the
dialect accepts, that result rows come back as plain dicts keyed by column,
and that NULL aggregates read as 0.0.
"""
from __future__ import annotations

import csv
import sys
import types

import pytest

from infa2aidp.reconciler.connectors import (
    AIDPConnector,
    FlatFileConnector,
    JDBCConnector,
    create_source_connector,
    create_target_connector,
)
from infa2aidp.reconciler.models import DataSourceType, SourceConfig, TargetConfig


# ---------------------------------------------------------------------------
# Fakes
# ---------------------------------------------------------------------------

class FakeCursor:
    def __init__(self, conn):
        self.conn = conn
        self.description = None
        self._rows = []
        self.closed = False

    def execute(self, sql):
        self.conn.executed.append(sql)
        cols, rows = self.conn.responder(sql)
        self.description = [(c, t) for c, t in cols]
        self._rows = rows

    def fetchall(self):
        return list(self._rows)

    def fetchone(self):
        return self._rows[0]

    def close(self):
        self.closed = True


class FakeConnection:
    def __init__(self, responder):
        self.responder = responder
        self.executed: list[str] = []
        self.closed = False

    def cursor(self):
        return FakeCursor(self)

    def close(self):
        self.closed = True


class FakeRow(dict):
    def asDict(self):
        return dict(self)


class FakeField:
    def __init__(self, name, dtype, nullable=True):
        self.name, self.dataType, self.nullable = name, dtype, nullable


class FakeDF:
    def __init__(self, rows=(), fields=()):
        self._rows = [FakeRow(r) for r in rows]
        self.schema = types.SimpleNamespace(fields=list(fields))
        self.limited_to = None

    def collect(self):
        return self._rows

    def limit(self, n):
        df = FakeDF(self._rows[:n], self.schema.fields)
        df.limited_to = n
        return df


class FakeReader:
    def __init__(self, spark):
        self.spark = spark
        self.options = {}

    def format(self, fmt):
        self.options["format"] = fmt
        return self

    def option(self, k, v):
        self.options[k] = v
        return self

    def load(self):
        self.spark.loads.append(dict(self.options))
        return self.spark.responder(self.options["dbtable"])


class FakeSpark:
    """Records spark.sql / spark.table / spark.read.format('jdbc') calls."""

    def __init__(self, responder):
        self.responder = responder
        self.sqls: list[str] = []
        self.tables: list[str] = []
        self.loads: list[dict] = []

    def sql(self, sql):
        self.sqls.append(sql)
        return self.responder(sql)

    def table(self, name):
        self.tables.append(name)
        return self.responder(name)

    @property
    def read(self):
        return FakeReader(self)


def _src(**kw):
    kw.setdefault("source_type", DataSourceType.ORACLE)
    kw.setdefault("host", "db")
    kw.setdefault("port", 1521)
    kw.setdefault("database", "ORCL")
    kw.setdefault("schema", "HR")
    kw.setdefault("username", "u")
    kw.setdefault("password", "p")
    return SourceConfig(**kw)


# ---------------------------------------------------------------------------
# Models used by the connectors
# ---------------------------------------------------------------------------

class TestConnectionModels:
    @pytest.mark.parametrize("stype,url,driver", [
        (DataSourceType.ORACLE, "jdbc:oracle:thin:@h:1/db", "oracle.jdbc.driver.OracleDriver"),
        (DataSourceType.SQL_SERVER, "jdbc:sqlserver://h:1;databaseName=db",
         "com.microsoft.sqlserver.jdbc.SQLServerDriver"),
        (DataSourceType.MYSQL, "jdbc:mysql://h:1/db", "com.mysql.cj.jdbc.Driver"),
        (DataSourceType.POSTGRESQL, "jdbc:postgresql://h:1/db", "org.postgresql.Driver"),
        (DataSourceType.FLAT_FILE, "", ""),
    ])
    def test_url_and_driver_per_source_type(self, stype, url, driver):
        cfg = SourceConfig(source_type=stype, host="h", port=1, database="db")
        assert cfg.get_jdbc_url() == url
        assert cfg.get_driver_class() == driver

    def test_explicit_url_and_driver_win(self):
        cfg = SourceConfig(jdbc_url="jdbc:x", driver_class="x.Driver")
        assert (cfg.get_jdbc_url(), cfg.get_driver_class()) == ("jdbc:x", "x.Driver")

    def test_qualified_names(self):
        assert SourceConfig(schema="HR", table_name="EMP").get_qualified_table() == "HR.EMP"
        assert SourceConfig(table_name="EMP").get_qualified_table() == "EMP"
        assert TargetConfig(catalog="c", schema="s", table_name="t").get_full_name() == "c.s.t"
        assert TargetConfig(schema="s", table_name="t").get_full_name() == "s.t"
        assert TargetConfig(catalog="c", table_name="t", full_table_name="x.y.z").get_full_name() == "x.y.z"


# ---------------------------------------------------------------------------
# JDBC, local (jaydebeapi) mode
# ---------------------------------------------------------------------------

class TestJDBCLocal:
    def _conn(self, cfg=None, responder=None):
        c = JDBCConnector(cfg or _src())
        fake = FakeConnection(responder or (lambda sql: ([("CNT", "NUMBER")], [(7,)])))
        c._connection = fake
        return c, fake

    def test_row_count_qualifies_table_and_adds_where(self):
        c, fake = self._conn()
        assert c.get_row_count("EMP", "dept = 10") == 7
        assert fake.executed == ["SELECT COUNT(*) FROM HR.EMP WHERE dept = 10"]

    def test_where_keyword_is_not_doubled_and_dotted_table_is_kept(self):
        c, fake = self._conn()
        c.get_row_count("OTHER.EMP", "  WHERE x = 1")
        assert fake.executed == ["SELECT COUNT(*) FROM OTHER.EMP WHERE x = 1"]

    def test_unqualified_without_schema(self):
        c, fake = self._conn(_src(schema=""))
        c.get_row_count("EMP")
        assert fake.executed == ["SELECT COUNT(*) FROM EMP"]

    def test_schema_reads_cursor_description(self):
        c, fake = self._conn(responder=lambda sql: ([("ID", "NUMBER"), ("NAME", None)], []))
        schema = c.get_schema("EMP")
        assert fake.executed == ["SELECT * FROM HR.EMP WHERE 1=0"]
        assert schema == [
            {"name": "ID", "type": "NUMBER", "nullable": True},
            {"name": "NAME", "type": "UNKNOWN", "nullable": True},
        ]

    def test_data_returns_dicts_and_oracle_limit_uses_rownum(self):
        c, fake = self._conn(responder=lambda sql: ([("ID", "N"), ("NAME", "S")], [(1, "a"), (2, "b")]))
        rows = c.get_data("EMP", ["ID", "NAME"], "ID > 0", limit=5)
        assert rows == [{"ID": 1, "NAME": "a"}, {"ID": 2, "NAME": "b"}]
        assert fake.executed == [
            "SELECT * FROM (SELECT ID, NAME FROM HR.EMP WHERE ID > 0) WHERE ROWNUM <= 5"
        ]

    def test_data_without_columns_selects_star(self):
        c, fake = self._conn(responder=lambda sql: ([("ID", "N")], []))
        c.get_data("EMP", [])
        assert fake.executed == ["SELECT * FROM HR.EMP"]

    @pytest.mark.parametrize("stype", [DataSourceType.MYSQL, DataSourceType.POSTGRESQL])
    def test_limit_dialects_that_accept_limit(self, stype):
        c, fake = self._conn(_src(source_type=stype, schema=""),
                             responder=lambda sql: ([("ID", "N")], []))
        c.get_data("T", ["ID"], limit=3)
        assert fake.executed == ["SELECT ID FROM T LIMIT 3"]

    def test_sql_server_sample_limit_is_valid_tsql(self):
        c, fake = self._conn(_src(source_type=DataSourceType.SQL_SERVER, schema="dbo"),
                             responder=lambda sql: ([("ID", "N")], []))
        c.get_data("T", ["ID"], limit=3)
        assert "LIMIT" not in fake.executed[0].upper()

    def test_aggregates_build_one_query_and_map_uppercase_aliases(self):
        def responder(sql):
            return ([("COUNT_AMT", "N"), ("DISTINCT_COUNT_AMT", "N"), ("SUM_AMT", "N")],
                    [(3, 2, None)])
        c, fake = self._conn(responder=responder)
        out = c.get_aggregates("EMP", ["AMT"], ["COUNT", "DISTINCT_COUNT", "SUM"])
        assert fake.executed == [
            "SELECT COUNT(AMT) AS count_amt, COUNT(DISTINCT AMT) AS distinct_count_amt, "
            "SUM(AMT) AS sum_amt FROM HR.EMP"
        ]
        assert out == [
            {"column": "AMT", "metric": "COUNT", "value": 3.0},
            {"column": "AMT", "metric": "DISTINCT_COUNT", "value": 2.0},
            {"column": "AMT", "metric": "SUM", "value": 0.0},   # NULL SUM reads as 0.0
        ]

    def test_aggregates_accept_lowercase_aliases(self):
        c, _ = self._conn(responder=lambda sql: ([("max_amt", "N")], [(9,)]))
        assert c.get_aggregates("EMP", ["AMT"], ["MAX"]) == [
            {"column": "AMT", "metric": "MAX", "value": 9.0}]

    def test_aggregates_with_no_row_returns_empty(self):
        c, _ = self._conn(responder=lambda sql: ([("max_amt", "N")], []))
        assert c.get_aggregates("EMP", ["AMT"], ["MAX"]) == []

    def test_close_closes_once_and_swallows_errors(self):
        c, fake = self._conn()
        c.close()
        assert fake.closed and c._connection is None
        c.close()  # idempotent

        class Boom:
            def close(self):
                raise RuntimeError("already gone")
        c._connection = Boom()
        c.close()
        assert c._connection is None

    def test_connection_is_opened_lazily_with_url_driver_and_credentials(self, monkeypatch):
        seen = []
        fake_mod = types.ModuleType("jaydebeapi")
        fake_mod.connect = lambda driver, url, creds: seen.append((driver, url, creds)) or "CONN"
        monkeypatch.setitem(sys.modules, "jaydebeapi", fake_mod)
        c = JDBCConnector(_src())
        assert c._get_local_connection() == "CONN"
        assert c._get_local_connection() == "CONN"   # cached
        assert seen == [("oracle.jdbc.driver.OracleDriver", "jdbc:oracle:thin:@db:1521/ORCL", ["u", "p"])]

    def test_missing_jaydebeapi_names_the_package(self, monkeypatch):
        monkeypatch.setitem(sys.modules, "jaydebeapi", None)
        with pytest.raises(ImportError, match="jaydebeapi"):
            JDBCConnector(_src())._get_local_connection()


# ---------------------------------------------------------------------------
# JDBC, Spark mode
# ---------------------------------------------------------------------------

class TestJDBCSpark:
    def _conn(self, responder):
        c = JDBCConnector(_src(), use_spark=True)
        c._spark = FakeSpark(responder)
        return c, c._spark

    def test_queries_are_wrapped_as_a_subquery_with_credentials(self):
        c, spark = self._conn(lambda q: FakeDF([{"cnt": 4}]))
        assert c.get_row_count("EMP", "a = 1") == 4
        load = spark.loads[0]
        assert load["format"] == "jdbc"
        assert load["dbtable"] == "(SELECT COUNT(*) AS cnt FROM HR.EMP WHERE a = 1) src_q"
        assert (load["url"], load["driver"], load["user"], load["password"]) == (
            "jdbc:oracle:thin:@db:1521/ORCL", "oracle.jdbc.driver.OracleDriver", "u", "p")

    def test_plain_table_name_is_passed_as_dbtable(self):
        c, spark = self._conn(lambda q: FakeDF())
        c._spark_jdbc_read("HR.EMP")
        assert spark.loads[0]["dbtable"] == "HR.EMP"

    def test_schema_uses_spark_field_types(self):
        fields = [FakeField("ID", "IntegerType()", False), FakeField("NAME", "StringType()")]
        c, _ = self._conn(lambda q: FakeDF(fields=fields))
        assert c.get_schema("EMP") == [
            {"name": "ID", "type": "IntegerType()", "nullable": False},
            {"name": "NAME", "type": "StringType()", "nullable": True},
        ]

    def test_data_limit_is_applied_on_the_dataframe(self):
        c, _ = self._conn(lambda q: FakeDF([{"ID": 1}, {"ID": 2}, {"ID": 3}]))
        assert c.get_data("EMP", ["ID"], limit=2) == [{"ID": 1}, {"ID": 2}]
        assert c.get_data("EMP", ["ID"]) == [{"ID": 1}, {"ID": 2}, {"ID": 3}]

    def test_aggregates_read_aliases_and_null_is_zero(self):
        c, _ = self._conn(lambda q: FakeDF([{"sum_amt": 12, "min_amt": None}]))
        assert c.get_aggregates("EMP", ["AMT"], ["SUM", "MIN"]) == [
            {"column": "AMT", "metric": "SUM", "value": 12.0},
            {"column": "AMT", "metric": "MIN", "value": 0.0},
        ]


# ---------------------------------------------------------------------------
# AIDP target connector
# ---------------------------------------------------------------------------

class TestAIDPConnector:
    def _conn(self, responder):
        c = AIDPConnector(TargetConfig(catalog="cat", schema="sch", table_name="orders"))
        c._spark = FakeSpark(responder)
        return c, c._spark

    def test_row_count_uses_configured_table_and_where(self):
        c, spark = self._conn(lambda q: FakeDF([{"cnt": 11}]))
        assert c.get_row_count(where="WHERE x = 1") == 11
        assert spark.sqls == ["SELECT COUNT(*) AS cnt FROM cat.sch.orders WHERE x = 1"]

    def test_an_explicit_qualified_table_overrides_config(self):
        c, spark = self._conn(lambda q: FakeDF([{"cnt": 0}]))
        c.get_row_count("other.tbl")
        c.get_row_count("bare")  # unqualified -> configured name
        assert spark.sqls == ["SELECT COUNT(*) AS cnt FROM other.tbl",
                              "SELECT COUNT(*) AS cnt FROM cat.sch.orders"]

    def test_schema(self):
        c, spark = self._conn(lambda n: FakeDF(fields=[FakeField("id", "LongType()")]))
        assert c.get_schema() == [{"name": "id", "type": "LongType()", "nullable": True}]
        assert spark.tables == ["cat.sch.orders"]

    def test_data_with_columns_where_and_limit(self):
        c, spark = self._conn(lambda q: FakeDF([{"id": 1}]))
        assert c.get_data(columns=["id", "v"], where="v > 0", limit=10) == [{"id": 1}]
        c.get_data()
        assert spark.sqls == [
            "SELECT id, v FROM cat.sch.orders WHERE v > 0 LIMIT 10",
            "SELECT * FROM cat.sch.orders",
        ]

    def test_aggregates(self):
        c, spark = self._conn(lambda q: FakeDF([{"avg_amt": 2.5, "distinct_count_amt": None}]))
        out = c.get_aggregates(columns=["amt"], metrics=["AVG", "DISTINCT_COUNT"])
        assert spark.sqls == [
            "SELECT AVG(amt) AS avg_amt, COUNT(DISTINCT amt) AS distinct_count_amt FROM cat.sch.orders"]
        assert out == [
            {"column": "amt", "metric": "AVG", "value": 2.5},
            {"column": "amt", "metric": "DISTINCT_COUNT", "value": 0.0},
        ]

    def test_time_travel_reads(self):
        c, spark = self._conn(lambda q: FakeDF([{"id": 1}]))
        assert c.get_data_at_version(3) == [{"id": 1}]
        assert c.get_data_at_timestamp("2026-01-01") == [{"id": 1}]
        assert spark.sqls == [
            "SELECT * FROM cat.sch.orders VERSION AS OF 3",
            "SELECT * FROM cat.sch.orders TIMESTAMP AS OF '2026-01-01'",
        ]


# ---------------------------------------------------------------------------
# Flat file source
# ---------------------------------------------------------------------------

def _write_csv(path, rows):
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0]))
        w.writeheader()
        w.writerows(rows)


class TestFlatFileConnector:
    ROWS = [
        {"ID": "1", "AMT": "10", "NAME": "a"},
        {"ID": "2", "AMT": "", "NAME": "b"},
        {"ID": "3", "AMT": "30", "NAME": "c"},
        {"ID": "4", "AMT": "10", "NAME": "d"},
    ]

    def test_csv_count_schema_and_projection(self, tmp_path):
        p = tmp_path / "src.csv"
        _write_csv(p, self.ROWS)
        c = FlatFileConnector(str(p))
        assert c.get_row_count() == 4
        assert c.get_schema() == [
            {"name": n, "type": "STRING", "nullable": True} for n in ("ID", "AMT", "NAME")]
        assert c.get_data(columns=["ID", "MISSING"], limit=2) == [
            {"ID": "1", "MISSING": None}, {"ID": "2", "MISSING": None}]
        assert len(c.get_data()) == 4

    def test_csv_aggregates_skip_blank_and_non_numeric(self, tmp_path):
        p = tmp_path / "src.csv"
        _write_csv(p, self.ROWS)
        out = FlatFileConnector(str(p)).get_aggregates(
            columns=["AMT", "NAME"],
            metrics=["COUNT", "SUM", "MIN", "MAX", "AVG", "DISTINCT_COUNT"])
        amt = {a["metric"]: a["value"] for a in out if a["column"] == "AMT"}
        assert amt == {"COUNT": 3.0, "SUM": 50.0, "MIN": 10.0, "MAX": 30.0,
                       "AVG": pytest.approx(50 / 3), "DISTINCT_COUNT": 2.0}
        name = {a["metric"]: a["value"] for a in out if a["column"] == "NAME"}
        assert set(name.values()) == {0.0}

    def test_empty_csv_has_no_schema(self, tmp_path):
        p = tmp_path / "empty.csv"
        p.write_text("ID,NAME\n", encoding="utf-8")
        assert FlatFileConnector(str(p)).get_schema() == []

    def test_file_is_read_once(self, tmp_path):
        p = tmp_path / "src.csv"
        _write_csv(p, self.ROWS)
        c = FlatFileConnector(str(p))
        c.get_row_count()
        p.unlink()
        assert c.get_row_count() == 4

    def test_parquet(self, tmp_path):
        pa = pytest.importorskip("pyarrow")
        import pyarrow.parquet as pq
        p = tmp_path / "src.parquet"
        pq.write_table(pa.table({"ID": [1, 2], "AMT": [1.5, None]}), str(p))
        c = FlatFileConnector(str(p), file_format="PARQUET")
        assert c.get_data() == [{"ID": 1, "AMT": 1.5}, {"ID": 2, "AMT": None}]
        sums = c.get_aggregates(columns=["AMT"], metrics=["SUM"])
        assert sums == [{"column": "AMT", "metric": "SUM", "value": 1.5}]

    def test_parquet_without_pyarrow_names_the_package(self, tmp_path, monkeypatch):
        monkeypatch.setitem(sys.modules, "pyarrow", None)
        monkeypatch.setitem(sys.modules, "pyarrow.parquet", None)
        with pytest.raises(ImportError, match="pyarrow"):
            FlatFileConnector(str(tmp_path / "x.parquet"), "parquet").get_data()

    def test_where_clause_is_not_silently_ignored(self, tmp_path):
        p = tmp_path / "src.csv"
        _write_csv(p, self.ROWS)
        c = FlatFileConnector(str(p))
        try:
            count = c.get_row_count(where="ID = '1'")
        except (NotImplementedError, ValueError):
            return  # refusing is an acceptable answer
        assert count == 1


class TestFactories:
    def test_flat_file_source_uses_table_name_as_path(self):
        c = create_source_connector(SourceConfig(source_type=DataSourceType.FLAT_FILE,
                                                 table_name="/data/x.csv"))
        assert isinstance(c, FlatFileConnector) and c.file_path == "/data/x.csv"

    def test_relational_source_is_jdbc_and_keeps_mode(self):
        c = create_source_connector(_src(), use_spark=True)
        assert isinstance(c, JDBCConnector) and c.use_spark

    def test_target_is_aidp(self):
        cfg = TargetConfig(full_table_name="a.b.c")
        c = create_target_connector(cfg)
        assert isinstance(c, AIDPConnector) and c.config is cfg
