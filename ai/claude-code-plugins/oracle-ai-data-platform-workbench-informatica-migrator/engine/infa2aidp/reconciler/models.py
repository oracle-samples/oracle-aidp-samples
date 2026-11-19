"""Data models for reconciliation configuration and results."""

from dataclasses import dataclass, field
from enum import Enum
from typing import Optional


class ReconcileType(Enum):
    ROW_COUNT = "row_count"
    SCHEMA = "schema"
    DATA = "data"
    AGGREGATE = "aggregate"
    ALL = "all"


class DataSourceType(Enum):
    ORACLE = "oracle"
    SQL_SERVER = "sql_server"
    MYSQL = "mysql"
    POSTGRESQL = "postgresql"
    FLAT_FILE = "flat_file"
    AIDP_TABLE = "aidp_table"


@dataclass
class SourceConfig:
    """Source database connection config."""

    source_type: DataSourceType = DataSourceType.ORACLE
    host: str = ""
    port: int = 1521
    database: str = ""
    schema: str = ""
    username: str = ""
    password: str = ""  # In practice, use secrets/env vars
    jdbc_url: str = ""
    driver_class: str = ""
    table_name: str = ""
    query: str = ""  # Optional custom query

    def get_jdbc_url(self) -> str:
        if self.jdbc_url:
            return self.jdbc_url
        builders = {
            DataSourceType.ORACLE: lambda: (
                f"jdbc:oracle:thin:@{self.host}:{self.port}/{self.database}"
            ),
            DataSourceType.SQL_SERVER: lambda: (
                f"jdbc:sqlserver://{self.host}:{self.port}"
                f";databaseName={self.database}"
            ),
            DataSourceType.MYSQL: lambda: (
                f"jdbc:mysql://{self.host}:{self.port}/{self.database}"
            ),
            DataSourceType.POSTGRESQL: lambda: (
                f"jdbc:postgresql://{self.host}:{self.port}/{self.database}"
            ),
        }
        builder = builders.get(self.source_type)
        return builder() if builder else ""

    def get_driver_class(self) -> str:
        if self.driver_class:
            return self.driver_class
        drivers = {
            DataSourceType.ORACLE: "oracle.jdbc.driver.OracleDriver",
            DataSourceType.SQL_SERVER: (
                "com.microsoft.sqlserver.jdbc.SQLServerDriver"
            ),
            DataSourceType.MYSQL: "com.mysql.cj.jdbc.Driver",
            DataSourceType.POSTGRESQL: "org.postgresql.Driver",
        }
        return drivers.get(self.source_type, "")

    def get_qualified_table(self) -> str:
        if self.schema:
            return f"{self.schema}.{self.table_name}"
        return self.table_name


@dataclass
class TargetConfig:
    """AIDP target table config."""

    catalog: str = ""
    schema: str = ""
    table_name: str = ""
    full_table_name: str = ""  # catalog.schema.table override

    def get_full_name(self) -> str:
        if self.full_table_name:
            return self.full_table_name
        parts = [p for p in [self.catalog, self.schema, self.table_name] if p]
        return ".".join(parts)


@dataclass
class ReconcileConfig:
    """Configuration for a reconciliation run."""

    name: str = ""
    reconcile_type: ReconcileType = ReconcileType.ALL
    source: SourceConfig = field(default_factory=SourceConfig)
    target: TargetConfig = field(default_factory=TargetConfig)
    key_columns: list = field(default_factory=list)
    compare_columns: list = field(default_factory=list)  # Empty = all
    ignore_columns: list = field(default_factory=list)
    tolerance: float = 0.0001
    date_tolerance_seconds: int = 1
    sample_size: int = 0  # 0 = full comparison
    where_clause: str = ""
    column_mapping: dict = field(default_factory=dict)  # {src_col: tgt_col}


# --- Result models ---


@dataclass
class ColumnDiff:
    column_name: str
    source_value: str
    target_value: str
    diff_type: str = ""  # VALUE_MISMATCH, NULL_MISMATCH, TYPE_MISMATCH


@dataclass
class RowDiff:
    key_values: dict = field(default_factory=dict)
    column_diffs: list = field(default_factory=list)  # list[ColumnDiff]
    diff_type: str = ""  # MISSING_IN_TARGET, MISSING_IN_SOURCE, VALUE_MISMATCH


@dataclass
class SchemaColumnDiff:
    column_name: str
    source_type: str = ""
    target_type: str = ""
    diff_type: str = ""  # MISSING_IN_TARGET, MISSING_IN_SOURCE, TYPE_MISMATCH, NULLABLE_MISMATCH


@dataclass
class AggregateResult:
    column_name: str
    metric: str  # COUNT, SUM, MIN, MAX, AVG, DISTINCT_COUNT
    source_value: float = 0.0
    target_value: float = 0.0
    difference: float = 0.0
    match: bool = True


@dataclass
class ReconcileResult:
    """Result of a reconciliation run."""

    config_name: str = ""
    reconcile_type: str = ""
    status: str = "PENDING"  # PENDING, RUNNING, PASSED, FAILED, ERROR

    # Row count
    source_row_count: int = 0
    target_row_count: int = 0
    row_count_match: bool = False

    # Schema comparison
    schema_diffs: list = field(default_factory=list)  # list[SchemaColumnDiff]
    schema_match: bool = False

    # Data comparison
    total_rows_compared: int = 0
    matching_rows: int = 0
    mismatched_rows: int = 0
    missing_in_target: int = 0
    missing_in_source: int = 0
    sample_diffs: list = field(default_factory=list)  # list[RowDiff]

    # Aggregate comparison
    aggregate_results: list = field(default_factory=list)  # list[AggregateResult]

    # Overall
    overall_match: bool = False
    match_percentage: float = 0.0
    error_message: str = ""
    duration_seconds: float = 0.0
    started_at: str = ""
    completed_at: str = ""
