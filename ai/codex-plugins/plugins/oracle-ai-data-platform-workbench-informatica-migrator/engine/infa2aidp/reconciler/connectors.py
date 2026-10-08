"""Source and target data connectors for reconciliation.

Supports two execution modes:
  - Local mode: generates Python code using jaydebeapi / oracledb.
  - Spark mode: generates PySpark code using spark.read.format("jdbc").

For AIDP targets, always uses Spark (spark.table / spark.sql).
"""

from __future__ import annotations

import csv
import io
from abc import ABC, abstractmethod
from typing import Any

from infa2aidp.reconciler.models import DataSourceType, SourceConfig, TargetConfig


class SourceConnector(ABC):
    """Base class for source data connectors."""

    @abstractmethod
    def get_row_count(self, table: str, where: str = "") -> int:
        ...

    @abstractmethod
    def get_schema(self, table: str) -> list[dict]:
        """Return list of {name, type, nullable} dicts."""
        ...

    @abstractmethod
    def get_data(
        self,
        table: str,
        columns: list[str],
        where: str = "",
        limit: int = 0,
    ) -> list[dict]:
        ...

    @abstractmethod
    def get_aggregates(
        self,
        table: str,
        columns: list[str],
        metrics: list[str],
    ) -> list[dict]:
        """Return list of {column, metric, value} dicts."""
        ...


# ---------------------------------------------------------------------------
# JDBC connector (Oracle, SQL Server, MySQL, PostgreSQL)
# ---------------------------------------------------------------------------

class JDBCConnector(SourceConnector):
    """Connects to relational sources via JDBC.

    Operates in two modes controlled by *use_spark*:
      - use_spark=False (local): uses jaydebeapi for direct queries.
      - use_spark=True: delegates reads to PySpark JDBC DataFrameReader.
    """

    def __init__(self, config: SourceConfig, *, use_spark: bool = False):
        self.config = config
        self.use_spark = use_spark
        self._connection = None
        self._spark = None

    # -- connection helpers --------------------------------------------------

    def _get_local_connection(self):
        """Lazy-init a jaydebeapi connection."""
        if self._connection is not None:
            return self._connection
        try:
            import jaydebeapi
        except ImportError as exc:
            raise ImportError(
                "jaydebeapi is required for local JDBC mode. "
                "Install it with: pip install jaydebeapi"
            ) from exc
        self._connection = jaydebeapi.connect(
            self.config.get_driver_class(),
            self.config.get_jdbc_url(),
            [self.config.username, self.config.password],
        )
        return self._connection

    def _get_spark(self):
        if self._spark is not None:
            return self._spark
        from pyspark.sql import SparkSession
        self._spark = SparkSession.builder.getOrCreate()
        return self._spark

    def _spark_jdbc_read(self, query_or_table: str):
        """Build a PySpark JDBC DataFrame."""
        spark = self._get_spark()
        reader = (
            spark.read.format("jdbc")
            .option("url", self.config.get_jdbc_url())
            .option("driver", self.config.get_driver_class())
            .option("user", self.config.username)
            .option("password", self.config.password)
        )
        if query_or_table.strip().upper().startswith("SELECT"):
            reader = reader.option("dbtable", f"({query_or_table}) src_q")
        else:
            reader = reader.option("dbtable", query_or_table)
        return reader.load()

    def _qualified_table(self, table: str) -> str:
        if "." in table:
            return table
        if self.config.schema:
            return f"{self.config.schema}.{table}"
        return table

    def _where_sql(self, where: str) -> str:
        if not where:
            return ""
        w = where.strip()
        if w.upper().startswith("WHERE"):
            return f" {w}"
        return f" WHERE {w}"

    # -- local mode helpers --------------------------------------------------

    def _local_query(self, sql: str) -> list[dict]:
        conn = self._get_local_connection()
        cursor = conn.cursor()
        cursor.execute(sql)
        columns = [desc[0] for desc in cursor.description]
        rows = cursor.fetchall()
        cursor.close()
        return [dict(zip(columns, row)) for row in rows]

    def _local_scalar(self, sql: str):
        conn = self._get_local_connection()
        cursor = conn.cursor()
        cursor.execute(sql)
        value = cursor.fetchone()[0]
        cursor.close()
        return value

    # -- public interface ----------------------------------------------------

    def get_row_count(self, table: str, where: str = "") -> int:
        qt = self._qualified_table(table)
        if self.use_spark:
            sql = f"SELECT COUNT(*) AS cnt FROM {qt}{self._where_sql(where)}"
            df = self._spark_jdbc_read(sql)
            return df.collect()[0]["cnt"]
        sql = f"SELECT COUNT(*) FROM {qt}{self._where_sql(where)}"
        return int(self._local_scalar(sql))

    def get_schema(self, table: str) -> list[dict]:
        qt = self._qualified_table(table)
        if self.use_spark:
            df = self._spark_jdbc_read(f"SELECT * FROM {qt} WHERE 1=0")
            return [
                {"name": f.name, "type": str(f.dataType), "nullable": f.nullable}
                for f in df.schema.fields
            ]
        # Local: use JDBC metadata
        conn = self._get_local_connection()
        cursor = conn.cursor()
        cursor.execute(f"SELECT * FROM {qt} WHERE 1=0")
        schema = []
        for desc in cursor.description:
            schema.append({
                "name": desc[0],
                "type": str(desc[1]) if desc[1] else "UNKNOWN",
                "nullable": True,  # JDBC metadata limited here
            })
        cursor.close()
        return schema

    def get_data(
        self,
        table: str,
        columns: list[str],
        where: str = "",
        limit: int = 0,
    ) -> list[dict]:
        qt = self._qualified_table(table)
        col_list = ", ".join(columns) if columns else "*"
        sql = f"SELECT {col_list} FROM {qt}{self._where_sql(where)}"
        if self.use_spark:
            df = self._spark_jdbc_read(sql)
            if limit > 0:
                df = df.limit(limit)
            return [row.asDict() for row in df.collect()]
        if limit > 0:
            # Use subquery wrapper for portability
            if self.config.source_type == DataSourceType.ORACLE:
                sql = f"SELECT * FROM ({sql}) WHERE ROWNUM <= {limit}"
            elif self.config.source_type == DataSourceType.SQL_SERVER:
                # T-SQL has no LIMIT.
                sql = f"SELECT TOP {limit} * FROM ({sql}) AS _sample"
            else:
                sql = f"{sql} LIMIT {limit}"
        return self._local_query(sql)

    def get_aggregates(
        self,
        table: str,
        columns: list[str],
        metrics: list[str],
    ) -> list[dict]:
        qt = self._qualified_table(table)
        results: list[dict] = []
        agg_exprs = []
        mapping: list[tuple[str, str, str]] = []  # (alias, column, metric)
        for col in columns:
            for metric in metrics:
                alias = f"{metric}_{col}".lower()
                if metric == "DISTINCT_COUNT":
                    agg_exprs.append(f"COUNT(DISTINCT {col}) AS {alias}")
                else:
                    agg_exprs.append(f"{metric}({col}) AS {alias}")
                mapping.append((alias, col, metric))

        sql = f"SELECT {', '.join(agg_exprs)} FROM {qt}"
        if self.use_spark:
            df = self._spark_jdbc_read(sql)
            row = df.collect()[0]
            for alias, col, metric in mapping:
                val = row[alias]
                results.append({
                    "column": col,
                    "metric": metric,
                    "value": float(val) if val is not None else 0.0,
                })
        else:
            row_data = self._local_query(sql)
            if row_data:
                row = row_data[0]
                for alias, col, metric in mapping:
                    key = alias.upper() if alias.upper() in row else alias
                    val = row.get(key, row.get(alias, 0))
                    results.append({
                        "column": col,
                        "metric": metric,
                        "value": float(val) if val is not None else 0.0,
                    })
        return results

    def close(self):
        if self._connection is not None:
            try:
                self._connection.close()
            except Exception:
                pass
            self._connection = None


# ---------------------------------------------------------------------------
# AIDP (Spark) connector for target tables
# ---------------------------------------------------------------------------

class AIDPConnector(SourceConnector):
    """Reads AIDP Delta/Parquet tables via Spark."""

    def __init__(self, config: TargetConfig):
        self.config = config
        self._spark = None

    def _get_spark(self):
        if self._spark is not None:
            return self._spark
        from pyspark.sql import SparkSession
        self._spark = SparkSession.builder.getOrCreate()
        return self._spark

    def _table_name(self, table: str | None = None) -> str:
        if table and "." in table:
            return table
        return self.config.get_full_name()

    def _where_sql(self, where: str) -> str:
        if not where:
            return ""
        w = where.strip()
        if w.upper().startswith("WHERE"):
            return f" {w}"
        return f" WHERE {w}"

    def get_row_count(self, table: str = "", where: str = "") -> int:
        spark = self._get_spark()
        tn = self._table_name(table)
        sql = f"SELECT COUNT(*) AS cnt FROM {tn}{self._where_sql(where)}"
        return spark.sql(sql).collect()[0]["cnt"]

    def get_schema(self, table: str = "") -> list[dict]:
        spark = self._get_spark()
        tn = self._table_name(table)
        df = spark.table(tn)
        return [
            {"name": f.name, "type": str(f.dataType), "nullable": f.nullable}
            for f in df.schema.fields
        ]

    def get_data(
        self,
        table: str = "",
        columns: list[str] | None = None,
        where: str = "",
        limit: int = 0,
    ) -> list[dict]:
        spark = self._get_spark()
        tn = self._table_name(table)
        col_list = ", ".join(columns) if columns else "*"
        sql = f"SELECT {col_list} FROM {tn}{self._where_sql(where)}"
        if limit > 0:
            sql += f" LIMIT {limit}"
        return [row.asDict() for row in spark.sql(sql).collect()]

    def get_aggregates(
        self,
        table: str = "",
        columns: list[str] | None = None,
        metrics: list[str] | None = None,
    ) -> list[dict]:
        spark = self._get_spark()
        tn = self._table_name(table)
        columns = columns or []
        metrics = metrics or []
        results: list[dict] = []
        agg_exprs = []
        mapping: list[tuple[str, str, str]] = []
        for col in columns:
            for metric in metrics:
                alias = f"{metric}_{col}".lower()
                if metric == "DISTINCT_COUNT":
                    agg_exprs.append(f"COUNT(DISTINCT {col}) AS {alias}")
                else:
                    agg_exprs.append(f"{metric}({col}) AS {alias}")
                mapping.append((alias, col, metric))
        sql = f"SELECT {', '.join(agg_exprs)} FROM {tn}"
        row = spark.sql(sql).collect()[0]
        for alias, col, metric in mapping:
            val = row[alias]
            results.append({
                "column": col,
                "metric": metric,
                "value": float(val) if val is not None else 0.0,
            })
        return results

    def get_data_at_version(self, version: int, table: str = "") -> list[dict]:
        """Read Delta table at a specific version (time travel)."""
        spark = self._get_spark()
        tn = self._table_name(table)
        sql = f"SELECT * FROM {tn} VERSION AS OF {version}"
        return [row.asDict() for row in spark.sql(sql).collect()]

    def get_data_at_timestamp(self, timestamp: str, table: str = "") -> list[dict]:
        """Read Delta table at a specific timestamp (time travel)."""
        spark = self._get_spark()
        tn = self._table_name(table)
        sql = f"SELECT * FROM {tn} TIMESTAMP AS OF '{timestamp}'"
        return [row.asDict() for row in spark.sql(sql).collect()]


# ---------------------------------------------------------------------------
# Flat file connector (CSV / Parquet)
# ---------------------------------------------------------------------------

class FlatFileConnector(SourceConnector):
    """Reads CSV or Parquet files for source comparison."""

    def __init__(self, file_path: str, file_format: str = "csv"):
        self.file_path = file_path
        self.file_format = file_format.lower()
        self._data: list[dict] | None = None

    def _load_csv(self) -> list[dict]:
        with open(self.file_path, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            return list(reader)

    def _load_parquet(self) -> list[dict]:
        try:
            import pyarrow.parquet as pq
        except ImportError as exc:
            raise ImportError(
                "pyarrow is required for Parquet files. "
                "Install with: pip install pyarrow"
            ) from exc
        table = pq.read_table(self.file_path)
        return table.to_pylist()

    def _load(self) -> list[dict]:
        if self._data is not None:
            return self._data
        if self.file_format == "parquet":
            self._data = self._load_parquet()
        else:
            self._data = self._load_csv()
        return self._data

    @staticmethod
    def _refuse_where(where: str) -> None:
        """A file has no SQL engine behind it: a filter cannot be applied,
        and ignoring it compared a filtered target with the whole file."""
        if where and where.strip():
            raise NotImplementedError(
                f"FlatFileConnector cannot apply a where clause ({where!r}); "
                f"filter the file first or reconcile without one"
            )

    def get_row_count(self, table: str = "", where: str = "") -> int:
        self._refuse_where(where)
        return len(self._load())

    def get_schema(self, table: str = "") -> list[dict]:
        data = self._load()
        if not data:
            return []
        return [
            {"name": col, "type": "STRING", "nullable": True}
            for col in data[0].keys()
        ]

    def get_data(
        self,
        table: str = "",
        columns: list[str] | None = None,
        where: str = "",
        limit: int = 0,
    ) -> list[dict]:
        self._refuse_where(where)
        data = self._load()
        if columns:
            data = [{k: row.get(k) for k in columns} for row in data]
        if limit > 0:
            data = data[:limit]
        return data

    def get_aggregates(
        self,
        table: str = "",
        columns: list[str] | None = None,
        metrics: list[str] | None = None,
    ) -> list[dict]:
        data = self._load()
        columns = columns or []
        metrics = metrics or []
        results: list[dict] = []
        for col in columns:
            values = []
            for row in data:
                v = row.get(col)
                if v is not None:
                    try:
                        values.append(float(v))
                    except (ValueError, TypeError):
                        pass
            for metric in metrics:
                val = 0.0
                if values:
                    if metric == "COUNT":
                        val = float(len(values))
                    elif metric == "SUM":
                        val = sum(values)
                    elif metric == "MIN":
                        val = min(values)
                    elif metric == "MAX":
                        val = max(values)
                    elif metric == "AVG":
                        val = sum(values) / len(values)
                    elif metric == "DISTINCT_COUNT":
                        val = float(len(set(values)))
                results.append({
                    "column": col,
                    "metric": metric,
                    "value": val,
                })
        return results


# ---------------------------------------------------------------------------
# Factory
# ---------------------------------------------------------------------------

def create_source_connector(
    config: SourceConfig,
    *,
    use_spark: bool = False,
) -> SourceConnector:
    """Build the right connector for a SourceConfig."""
    if config.source_type == DataSourceType.FLAT_FILE:
        return FlatFileConnector(config.table_name)  # table_name holds path
    return JDBCConnector(config, use_spark=use_spark)


def create_target_connector(config: TargetConfig) -> AIDPConnector:
    return AIDPConnector(config)
