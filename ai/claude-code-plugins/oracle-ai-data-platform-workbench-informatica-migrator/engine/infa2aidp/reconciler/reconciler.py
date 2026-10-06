"""Main reconciliation orchestrator.

DataReconciler runs the configured checks (row count, schema, aggregate,
data) in order, aggregates results, and optionally generates a standalone
AIDP notebook for running the reconciliation on-cluster.
"""

from __future__ import annotations

import os
import time
from datetime import datetime, timezone
from typing import Any

import yaml

from infa2aidp.reconciler.comparators import (
    AggregateComparator,
    DataComparator,
    RowCountComparator,
    SchemaComparator,
)
from infa2aidp.reconciler.connectors import (
    AIDPConnector,
    create_source_connector,
    create_target_connector,
)
from infa2aidp.reconciler.models import (
    DataSourceType,
    ReconcileConfig,
    ReconcileResult,
    ReconcileType,
    SourceConfig,
    TargetConfig,
)


class DataReconciler:
    """Orchestrates reconciliation runs."""

    def __init__(self, *, use_spark: bool = False):
        """
        Args:
            use_spark: If True, JDBC connectors use PySpark DataFrameReader
                       instead of jaydebeapi.  Set True when running on AIDP.
        """
        self.use_spark = use_spark

    # ------------------------------------------------------------------
    # Single reconciliation
    # ------------------------------------------------------------------

    def reconcile(self, config: ReconcileConfig) -> ReconcileResult:
        """Run reconciliation based on config type."""
        started = datetime.now(timezone.utc)
        t0 = time.monotonic()
        combined = ReconcileResult(
            config_name=config.name,
            reconcile_type=config.reconcile_type.value,
            status="RUNNING",
            started_at=started.isoformat(),
        )
        try:
            source = create_source_connector(
                config.source, use_spark=self.use_spark
            )
            target = create_target_connector(config.target)

            rtype = config.reconcile_type
            checks = self._checks_for_type(rtype)
            sub_results: list[ReconcileResult] = []

            if "row_count" in checks:
                sub_results.append(
                    RowCountComparator().compare(source, target, config)
                )
            if "schema" in checks:
                sub_results.append(
                    SchemaComparator().compare(source, target, config)
                )
            if "aggregate" in checks:
                sub_results.append(
                    AggregateComparator().compare(source, target, config)
                )
            if "data" in checks:
                sub_results.append(
                    DataComparator().compare(source, target, config)
                )

            self._merge_results(combined, sub_results)

        except Exception as exc:
            combined.status = "ERROR"
            combined.error_message = str(exc)

        combined.duration_seconds = round(time.monotonic() - t0, 3)
        combined.completed_at = datetime.now(timezone.utc).isoformat()
        return combined

    # ------------------------------------------------------------------
    # Batch
    # ------------------------------------------------------------------

    def reconcile_batch(
        self, configs: list[ReconcileConfig]
    ) -> list[ReconcileResult]:
        return [self.reconcile(cfg) for cfg in configs]

    # ------------------------------------------------------------------
    # YAML config loading
    # ------------------------------------------------------------------

    def load_config(self, config_path: str) -> list[ReconcileConfig]:
        """Load reconciliation configs from a YAML file.

        Supports ${ENV_VAR} expansion in string values.
        """
        with open(config_path, encoding="utf-8") as f:
            raw = yaml.safe_load(f)

        configs: list[ReconcileConfig] = []
        for entry in raw.get("reconciliations", []):
            src_raw = entry.get("source", {})
            tgt_raw = entry.get("target", {})
            source_type = DataSourceType(
                self._expand(src_raw.get("type", "oracle"))
            )
            source = SourceConfig(
                source_type=source_type,
                host=self._expand(src_raw.get("host", "")),
                port=int(src_raw.get("port", 1521)),
                database=self._expand(src_raw.get("database", "")),
                schema=self._expand(src_raw.get("schema", "")),
                table_name=self._expand(src_raw.get("table", "")),
                username=self._expand(src_raw.get("username", "")),
                password=self._expand(src_raw.get("password", "")),
                jdbc_url=self._expand(src_raw.get("jdbc_url", "")),
                driver_class=self._expand(src_raw.get("driver_class", "")),
                query=self._expand(src_raw.get("query", "")),
            )
            target = TargetConfig(
                catalog=self._expand(tgt_raw.get("catalog", "")),
                schema=self._expand(tgt_raw.get("schema", "")),
                table_name=self._expand(tgt_raw.get("table", "")),
                full_table_name=self._expand(
                    tgt_raw.get("full_table_name", "")
                ),
            )
            rtype = ReconcileType(entry.get("type", "all"))
            cfg = ReconcileConfig(
                name=entry.get("name", ""),
                reconcile_type=rtype,
                source=source,
                target=target,
                key_columns=entry.get("key_columns", []),
                compare_columns=entry.get("compare_columns", []),
                ignore_columns=entry.get("ignore_columns", []),
                tolerance=float(entry.get("tolerance", 0.0001)),
                date_tolerance_seconds=int(
                    entry.get("date_tolerance_seconds", 1)
                ),
                sample_size=int(entry.get("sample_size", 0)),
                where_clause=self._expand(entry.get("where_clause", "")),
                column_mapping=entry.get("column_mapping", {}),
            )
            configs.append(cfg)
        return configs

    # ------------------------------------------------------------------
    # Notebook generation
    # ------------------------------------------------------------------

    def generate_reconcile_notebook(self, config: ReconcileConfig) -> str:
        """Generate a PySpark notebook (Python source) that runs this
        reconciliation entirely on AIDP.

        Returns a string of Python code suitable for an AIDP notebook cell.
        """
        src = config.source
        tgt = config.target
        key_cols = config.key_columns
        ignore_cols = config.ignore_columns
        tolerance = config.tolerance
        where = config.where_clause

        where_clause = f' WHERE {where}' if where else ''
        src_table = src.get_qualified_table()
        tgt_table = tgt.get_full_name()
        key_csv = ", ".join(f'"{k}"' for k in key_cols)
        ignore_csv = ", ".join(f'"{c}"' for c in ignore_cols)

        nb = f'''# Reconciliation Notebook: {config.name}
# Auto-generated by infa2aidp reconciler
# Source: {src_table} ({src.source_type.value})
# Target: {tgt_table} (AIDP)

# --- Configuration ---
SOURCE_JDBC_URL = "{src.get_jdbc_url()}"
SOURCE_DRIVER = "{src.get_driver_class()}"
SOURCE_USER = dbutils.secrets.get(scope="reconciler", key="source_user")
SOURCE_PASS = dbutils.secrets.get(scope="reconciler", key="source_pass")
SOURCE_TABLE = "{src_table}"
TARGET_TABLE = "{tgt_table}"
KEY_COLUMNS = [{key_csv}]
IGNORE_COLUMNS = [{ignore_csv}]
TOLERANCE = {tolerance}

# --- Step 1: Read source via JDBC ---
source_df = (
    spark.read.format("jdbc")
    .option("url", SOURCE_JDBC_URL)
    .option("driver", SOURCE_DRIVER)
    .option("user", SOURCE_USER)
    .option("password", SOURCE_PASS)
    .option("dbtable", SOURCE_TABLE)
    .load()
)
{f'source_df = source_df.where("{where}")' if where else ''}

# --- Step 2: Read target ---
target_df = spark.table(TARGET_TABLE)
{f'target_df = target_df.where("{where}")' if where else ''}

# --- Step 3: Row count comparison ---
src_count = source_df.count()
tgt_count = target_df.count()
print(f"Source rows: {{src_count}}")
print(f"Target rows: {{tgt_count}}")
print(f"Row count match: {{src_count == tgt_count}}")

# --- Step 4: Schema comparison ---
src_cols = set(c.upper() for c in source_df.columns)
tgt_cols = set(c.upper() for c in target_df.columns)
ignore_upper = set(c.upper() for c in IGNORE_COLUMNS)
src_cols -= ignore_upper
tgt_cols -= ignore_upper
missing_in_target = src_cols - tgt_cols
missing_in_source = tgt_cols - src_cols
print(f"Missing in target: {{missing_in_target or 'None'}}")
print(f"Missing in source: {{missing_in_source or 'None'}}")

# --- Step 5: Data comparison ---
if KEY_COLUMNS:
    from pyspark.sql import functions as F

    # Drop ignored columns
    for col in IGNORE_COLUMNS:
        if col in source_df.columns:
            source_df = source_df.drop(col)
        if col in target_df.columns:
            target_df = target_df.drop(col)

    # Align column names to uppercase
    for c in source_df.columns:
        source_df = source_df.withColumnRenamed(c, c.upper())
    for c in target_df.columns:
        target_df = target_df.withColumnRenamed(c, c.upper())

    key_upper = [k.upper() for k in KEY_COLUMNS]
    common_cols = sorted(
        set(source_df.columns) & set(target_df.columns) - set(key_upper)
    )

    # Tag rows
    source_tagged = source_df.select(
        *key_upper, *common_cols
    ).withColumn("_src", F.lit(1))
    target_tagged = target_df.select(
        *key_upper, *common_cols
    ).withColumn("_tgt", F.lit(1))

    joined = source_tagged.join(target_tagged, key_upper, "full_outer")

    missing_tgt = joined.where("_src IS NOT NULL AND _tgt IS NULL").count()
    missing_src = joined.where("_src IS NULL AND _tgt IS NOT NULL").count()
    print(f"Rows missing in target: {{missing_tgt}}")
    print(f"Rows missing in source: {{missing_src}}")

    # Value comparison on matched rows, column by column, NULL-safe. (This
    # used to compare each column with itself and report nothing.)
    s_al, t_al = source_tagged.alias("s"), target_tagged.alias("t")
    matched = s_al.join(t_al, [F.col(f"s.`{{k}}`") == F.col(f"t.`{{k}}`") for k in key_upper], "inner")
    print(f"Matched rows for value comparison: {{matched.count()}}")
    value_mismatches = 0
    for col in common_cols:
        n = matched.where(~F.col(f"s.`{{col}}`").eqNullSafe(F.col(f"t.`{{col}}`"))).count()
        value_mismatches += n
        if n:
            print(f"Value mismatches in {{col}}: {{n}}")
    print(f"Value mismatches on matched rows (cells): {{value_mismatches}}")

    # --- Aggregate check ---
    from pyspark.sql.types import NumericType
    num_cols = [
        f.name for f in source_df.schema.fields
        if isinstance(f.dataType, NumericType) and f.name in common_cols
    ]
    for col in num_cols[:10]:  # Top 10 numeric cols
        src_sum = source_df.agg(F.sum(col)).collect()[0][0] or 0
        tgt_sum = target_df.agg(F.sum(col)).collect()[0][0] or 0
        diff = abs(float(src_sum) - float(tgt_sum))
        status = "PASS" if diff <= TOLERANCE else "FAIL"
        print(f"  {{col}} SUM: src={{src_sum}}, tgt={{tgt_sum}}, diff={{diff:.6f}} [{{status}}]")

print("\\n=== Reconciliation complete ===")
'''
        return nb

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _checks_for_type(rtype: ReconcileType) -> list[str]:
        if rtype == ReconcileType.ALL:
            return ["row_count", "schema", "aggregate", "data"]
        return [rtype.value]

    @staticmethod
    def _merge_results(
        combined: ReconcileResult,
        sub_results: list[ReconcileResult],
    ) -> None:
        """Merge sub-check results into the combined result."""
        if not sub_results:
            combined.status = "PASSED"
            combined.overall_match = True
            combined.match_percentage = 100.0
            return

        all_passed = True
        for sr in sub_results:
            if sr.status == "ERROR":
                combined.status = "ERROR"
                combined.error_message = sr.error_message
                combined.overall_match = False
                return
            if sr.status != "PASSED":
                all_passed = False

            # Merge fields by type
            if sr.reconcile_type == "row_count":
                combined.source_row_count = sr.source_row_count
                combined.target_row_count = sr.target_row_count
                combined.row_count_match = sr.row_count_match
            elif sr.reconcile_type == "schema":
                combined.schema_diffs = sr.schema_diffs
                combined.schema_match = sr.schema_match
            elif sr.reconcile_type == "data":
                combined.total_rows_compared = sr.total_rows_compared
                combined.matching_rows = sr.matching_rows
                combined.mismatched_rows = sr.mismatched_rows
                combined.missing_in_target = sr.missing_in_target
                combined.missing_in_source = sr.missing_in_source
                combined.sample_diffs = sr.sample_diffs
            elif sr.reconcile_type == "aggregate":
                combined.aggregate_results = sr.aggregate_results

        combined.overall_match = all_passed
        combined.status = "PASSED" if all_passed else "FAILED"

        # Compute overall match percentage as average of sub-checks
        pcts = [sr.match_percentage for sr in sub_results]
        combined.match_percentage = round(sum(pcts) / len(pcts), 2)

    @staticmethod
    def _expand(value: str) -> str:
        """Expand ${ENV_VAR} references in a string."""
        if not value or "${" not in value:
            return value
        result = value
        while "${" in result:
            start = result.index("${")
            end = result.index("}", start)
            var_name = result[start + 2 : end]
            var_value = os.environ.get(var_name, "")
            result = result[:start] + var_value + result[end + 1 :]
        return result
