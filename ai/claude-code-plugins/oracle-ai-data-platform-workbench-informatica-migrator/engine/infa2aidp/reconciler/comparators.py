"""Comparison logic for reconciliation checks.

Each comparator handles one reconcile type: row count, schema, data,
or aggregates. They work against the SourceConnector / AIDPConnector
abstraction so the same logic applies regardless of source database.
"""

from __future__ import annotations

import math
from typing import Any

from infa2aidp.reconciler.connectors import SourceConnector, AIDPConnector
from infa2aidp.reconciler.models import (
    AggregateResult,
    ColumnDiff,
    ReconcileConfig,
    ReconcileResult,
    RowDiff,
    SchemaColumnDiff,
)


# ---------------------------------------------------------------------------
# Oracle / relational type -> Spark type mapping for schema comparison
# ---------------------------------------------------------------------------

_TYPE_MAP: dict[str, str] = {
    # Oracle
    "NUMBER": "DecimalType",
    "FLOAT": "DoubleType",
    "BINARY_FLOAT": "FloatType",
    "BINARY_DOUBLE": "DoubleType",
    "VARCHAR2": "StringType",
    "NVARCHAR2": "StringType",
    "CHAR": "StringType",
    "NCHAR": "StringType",
    "CLOB": "StringType",
    "NCLOB": "StringType",
    "DATE": "TimestampType",
    "TIMESTAMP": "TimestampType",
    "BLOB": "BinaryType",
    "RAW": "BinaryType",
    "LONG": "StringType",
    "LONG RAW": "BinaryType",
    "XMLTYPE": "StringType",
    # SQL Server
    "INT": "IntegerType",
    "BIGINT": "LongType",
    "SMALLINT": "ShortType",
    "TINYINT": "ShortType",
    "BIT": "BooleanType",
    "DECIMAL": "DecimalType",
    "NUMERIC": "DecimalType",
    "MONEY": "DecimalType",
    "SMALLMONEY": "DecimalType",
    "REAL": "FloatType",
    "VARCHAR": "StringType",
    "NVARCHAR": "StringType",
    "TEXT": "StringType",
    "NTEXT": "StringType",
    "DATETIME": "TimestampType",
    "DATETIME2": "TimestampType",
    "SMALLDATETIME": "TimestampType",
    "DATETIMEOFFSET": "TimestampType",
    "VARBINARY": "BinaryType",
    "IMAGE": "BinaryType",
    "UNIQUEIDENTIFIER": "StringType",
    # MySQL
    "MEDIUMINT": "IntegerType",
    "DOUBLE": "DoubleType",
    "LONGTEXT": "StringType",
    "MEDIUMTEXT": "StringType",
    "TINYTEXT": "StringType",
    "BOOLEAN": "BooleanType",
    "TINYINT(1)": "BooleanType",
    # PostgreSQL
    "INTEGER": "IntegerType",
    "SERIAL": "IntegerType",
    "BIGSERIAL": "LongType",
    "DOUBLE PRECISION": "DoubleType",
    "CHARACTER VARYING": "StringType",
    "BYTEA": "BinaryType",
    "TIMESTAMPTZ": "TimestampType",
    "TIMESTAMP WITH TIME ZONE": "TimestampType",
    "BOOL": "BooleanType",
    "JSON": "StringType",
    "JSONB": "StringType",
    "UUID": "StringType",
}


def _normalize_type(raw: str) -> str:
    """Normalise a raw type string for comparison.

    Strips precision/scale and maps to Spark type names so we can
    compare across databases.
    """
    raw = raw.strip()
    upper = raw.upper()
    # Strip parenthesized precision, e.g. NUMBER(10,2) -> NUMBER
    base = upper.split("(")[0].strip()
    # Handle Oracle NUMBER with precision as integer
    if base == "NUMBER":
        # NUMBER(p, 0) or NUMBER(p) with p <= 10 -> IntegerType
        if "(" in upper:
            inner = upper.split("(")[1].rstrip(")")
            parts = [p.strip() for p in inner.split(",")]
            # NUMBER(*,0) / NUMBER(*) -- '*' is Oracle's max precision
            if parts and parts[0] in ("*", ""):
                return "DecimalType"
            try:
                prec = int(parts[0])
            except ValueError:
                return "DecimalType"
            if len(parts) == 1 or parts[1] == "0":
                if prec <= 10:
                    return "IntegerType"
                if prec <= 18:
                    return "LongType"
    # Direct lookup -- the full spelling first: MySQL TINYINT(1) is a
    # boolean, and its base name TINYINT is not.
    if upper.replace(" ", "") in _TYPE_MAP:
        return _TYPE_MAP[upper.replace(" ", "")]
    if base in _TYPE_MAP:
        return _TYPE_MAP[base]
    # Already a Spark type name -- str(f.dataType) gives "StringType()",
    # "DecimalType(10,2)", "IntegerType"; the old check compared the
    # upper-cased text against the mixed-case suffix "Type" and never
    # matched, so every Spark-side type was "incompatible" with everything
    # (and no Spark numeric column was ever recognised as numeric, which
    # made the aggregate reconciliation pass with nothing compared).
    if base.endswith("TYPE"):
        return base[:-4].capitalize() + "Type" if base[:-4].isalpha() else base
    return upper


def _types_compatible(src_type: str, tgt_type: str) -> bool:
    """Check if two types are compatible after normalisation."""
    ns = _normalize_type(src_type)
    nt = _normalize_type(tgt_type)
    if ns == nt:
        return True
    # Numeric family compatibility
    numeric = {"IntegerType", "LongType", "ShortType", "ByteType", "DecimalType",
               "FloatType", "DoubleType"}
    if ns in numeric and nt in numeric:
        return True
    # String family
    string = {"StringType"}
    if ns in string and nt in string:
        return True
    # Timestamp family
    ts = {"TimestampType", "DateType"}
    if ns in ts and nt in ts:
        return True
    return False


# ---------------------------------------------------------------------------
# Row count
# ---------------------------------------------------------------------------

class RowCountComparator:
    """Compare source vs target row counts."""

    def compare(
        self,
        source: SourceConnector,
        target: AIDPConnector,
        config: ReconcileConfig,
    ) -> ReconcileResult:
        result = ReconcileResult(
            config_name=config.name,
            reconcile_type="row_count",
            status="RUNNING",
        )
        src_table = config.source.table_name
        where = config.where_clause
        result.source_row_count = source.get_row_count(src_table, where)
        result.target_row_count = target.get_row_count(where=where)
        result.row_count_match = (
            result.source_row_count == result.target_row_count
        )
        result.overall_match = result.row_count_match
        if result.source_row_count > 0:
            result.match_percentage = (
                min(result.source_row_count, result.target_row_count)
                / max(result.source_row_count, result.target_row_count)
                * 100
            )
        else:
            result.match_percentage = (
                100.0 if result.target_row_count == 0 else 0.0
            )
        result.status = "PASSED" if result.row_count_match else "FAILED"
        return result


# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------

class SchemaComparator:
    """Compare column names, types, and nullability."""

    def compare(
        self,
        source: SourceConnector,
        target: AIDPConnector,
        config: ReconcileConfig,
    ) -> ReconcileResult:
        result = ReconcileResult(
            config_name=config.name,
            reconcile_type="schema",
            status="RUNNING",
        )
        src_schema = source.get_schema(config.source.table_name)
        tgt_schema = target.get_schema()

        # Build lookup dicts (case-insensitive)
        src_map = {s["name"].upper(): s for s in src_schema}
        tgt_map = {s["name"].upper(): s for s in tgt_schema}

        # Apply column mapping
        col_map = {
            k.upper(): v.upper() for k, v in config.column_mapping.items()
        }

        ignore = {c.upper() for c in config.ignore_columns}
        diffs: list[SchemaColumnDiff] = []

        for src_name_upper, src_col in src_map.items():
            if src_name_upper in ignore:
                continue
            tgt_name_upper = col_map.get(src_name_upper, src_name_upper)
            if tgt_name_upper in ignore:
                continue
            if tgt_name_upper not in tgt_map:
                diffs.append(SchemaColumnDiff(
                    column_name=src_col["name"],
                    source_type=src_col["type"],
                    diff_type="MISSING_IN_TARGET",
                ))
                continue
            tgt_col = tgt_map[tgt_name_upper]
            if not _types_compatible(src_col["type"], tgt_col["type"]):
                diffs.append(SchemaColumnDiff(
                    column_name=src_col["name"],
                    source_type=src_col["type"],
                    target_type=tgt_col["type"],
                    diff_type="TYPE_MISMATCH",
                ))

        # Check for columns in target but not in source
        reverse_map = {v: k for k, v in col_map.items()}
        for tgt_name_upper, tgt_col in tgt_map.items():
            if tgt_name_upper in ignore:
                continue
            src_name_upper = reverse_map.get(tgt_name_upper, tgt_name_upper)
            if src_name_upper not in src_map and tgt_name_upper not in src_map:
                diffs.append(SchemaColumnDiff(
                    column_name=tgt_col["name"],
                    target_type=tgt_col["type"],
                    diff_type="MISSING_IN_SOURCE",
                ))

        result.schema_diffs = diffs
        result.schema_match = len(diffs) == 0
        result.overall_match = result.schema_match
        result.match_percentage = 100.0 if result.schema_match else 0.0
        result.status = "PASSED" if result.schema_match else "FAILED"
        return result


# ---------------------------------------------------------------------------
# Data comparison
# ---------------------------------------------------------------------------

class DataComparator:
    """Full row-by-row data comparison using key columns."""

    MAX_SAMPLE_DIFFS = 50  # Cap stored diff rows

    def compare(
        self,
        source: SourceConnector,
        target: AIDPConnector,
        config: ReconcileConfig,
    ) -> ReconcileResult:
        result = ReconcileResult(
            config_name=config.name,
            reconcile_type="data",
            status="RUNNING",
        )
        if not config.key_columns:
            result.status = "ERROR"
            result.error_message = (
                "key_columns required for data comparison"
            )
            return result

        src_table = config.source.table_name
        where = config.where_clause
        limit = config.sample_size

        # Determine columns to compare
        all_cols = self._resolve_columns(source, target, config)
        key_cols = [c.upper() for c in config.key_columns]

        src_data = source.get_data(src_table, all_cols, where, limit)
        tgt_data = target.get_data(columns=all_cols, where=where, limit=limit)

        # Index by key. A key that occurs twice (a double-loaded row) must not
        # collapse into one dict entry and pass: each extra row is a diff.
        src_keyed, src_dups = self._index_by_key(src_data, key_cols)
        tgt_keyed, tgt_dups = self._index_by_key(tgt_data, key_cols)

        matching = 0
        mismatched = 0
        missing_in_target = 0
        missing_in_source = 0
        sample_diffs: list[RowDiff] = []

        compare_cols = [
            c for c in all_cols if c.upper() not in key_cols
        ]

        for key, src_row in src_keyed.items():
            if key not in tgt_keyed:
                missing_in_target += 1
                if len(sample_diffs) < self.MAX_SAMPLE_DIFFS:
                    sample_diffs.append(RowDiff(
                        key_values=dict(zip(key_cols, key)),
                        diff_type="MISSING_IN_TARGET",
                    ))
                continue
            tgt_row = tgt_keyed[key]
            row_diffs = self._compare_row(
                src_row, tgt_row, compare_cols, config
            )
            if row_diffs:
                mismatched += 1
                if len(sample_diffs) < self.MAX_SAMPLE_DIFFS:
                    sample_diffs.append(RowDiff(
                        key_values=dict(zip(key_cols, key)),
                        column_diffs=row_diffs,
                        diff_type="VALUE_MISMATCH",
                    ))
            else:
                matching += 1

        duplicates = 0
        for side, dups in (("SOURCE", src_dups), ("TARGET", tgt_dups)):
            for key, extra in dups.items():
                duplicates += extra
                if len(sample_diffs) < self.MAX_SAMPLE_DIFFS:
                    sample_diffs.append(RowDiff(
                        key_values=dict(zip(key_cols, key)),
                        diff_type=f"DUPLICATE_KEY_IN_{side}",
                    ))
        mismatched += duplicates

        for key in tgt_keyed:
            if key not in src_keyed:
                missing_in_source += 1
                if len(sample_diffs) < self.MAX_SAMPLE_DIFFS:
                    sample_diffs.append(RowDiff(
                        key_values=dict(zip(key_cols, key)),
                        diff_type="MISSING_IN_SOURCE",
                    ))

        total = len(src_keyed)
        result.total_rows_compared = total
        result.matching_rows = matching
        result.mismatched_rows = mismatched
        result.missing_in_target = missing_in_target
        result.missing_in_source = missing_in_source
        result.sample_diffs = sample_diffs
        result.overall_match = (
            mismatched == 0
            and missing_in_target == 0
            and missing_in_source == 0
        )
        # An empty source against a non-empty target matched nothing: 0%, as
        # the row-count check scores it -- not 100%.
        result.match_percentage = (
            (matching / total * 100) if total > 0 else (100.0 if not tgt_keyed else 0.0)
        )
        result.status = "PASSED" if result.overall_match else "FAILED"
        return result

    def _resolve_columns(
        self,
        source: SourceConnector,
        target: AIDPConnector,
        config: ReconcileConfig,
    ) -> list[str]:
        """Determine which columns to read and compare."""
        if config.compare_columns:
            cols = list(config.compare_columns)
        else:
            # Use target schema columns, exclude ignored
            tgt_schema = target.get_schema()
            cols = [s["name"] for s in tgt_schema]
        ignore = {c.upper() for c in config.ignore_columns}
        cols = [c for c in cols if c.upper() not in ignore]
        # Make sure key columns are included
        for kc in config.key_columns:
            if kc.upper() not in {c.upper() for c in cols}:
                cols.insert(0, kc)
        return cols

    @staticmethod
    def _index_by_key(
        data: list[dict], key_cols: list[str]
    ) -> tuple[dict[tuple, dict], dict[tuple, int]]:
        """``({key: row}, {key: extra rows beyond the first})``."""
        indexed: dict[tuple, dict] = {}
        dups: dict[tuple, int] = {}
        for row in data:
            # Case-insensitive column lookup
            norm = {k.upper(): v for k, v in row.items()}
            key = tuple(norm.get(k) for k in key_cols)
            if key in indexed:
                dups[key] = dups.get(key, 0) + 1
                continue
            indexed[key] = norm
        return indexed, dups

    def _compare_row(
        self,
        src_row: dict,
        tgt_row: dict,
        columns: list[str],
        config: ReconcileConfig,
    ) -> list[ColumnDiff]:
        diffs: list[ColumnDiff] = []
        col_map = {k.upper(): v.upper() for k, v in config.column_mapping.items()}
        for col in columns:
            src_col = col.upper()
            tgt_col = col_map.get(src_col, src_col)
            src_val = src_row.get(src_col)
            tgt_val = tgt_row.get(tgt_col)
            if not self._values_match(
                src_val, tgt_val, config.tolerance, config.date_tolerance_seconds
            ):
                diff_type = "VALUE_MISMATCH"
                if src_val is None or tgt_val is None:
                    diff_type = "NULL_MISMATCH"
                diffs.append(ColumnDiff(
                    column_name=col,
                    source_value=str(src_val),
                    target_value=str(tgt_val),
                    diff_type=diff_type,
                ))
        return diffs

    @staticmethod
    def _values_match(
        src: Any,
        tgt: Any,
        tolerance: float,
        date_tolerance_seconds: int,
    ) -> bool:
        # Both null
        if src is None and tgt is None:
            return True
        if src is None or tgt is None:
            return False
        import datetime as _dt
        from decimal import Decimal as _Dec
        # Date/time: within date_tolerance_seconds (it was passed and unused).
        if isinstance(src, (_dt.date, _dt.datetime)) and isinstance(tgt, (_dt.date, _dt.datetime)):
            a = src if isinstance(src, _dt.datetime) else _dt.datetime.combine(src, _dt.time())
            b = tgt if isinstance(tgt, _dt.datetime) else _dt.datetime.combine(tgt, _dt.time())
            return abs((a - b).total_seconds()) <= (date_tolerance_seconds or 0)
        # Numeric comparison with tolerance -- only when at least one side IS
        # a number. Two strings compare as text: '00123' is not '123'.
        numbers = (int, float, _Dec)
        if (isinstance(src, numbers) or isinstance(tgt, numbers)) and not isinstance(src, bool)                 and not isinstance(tgt, bool):
            try:
                src_f = float(src)
                tgt_f = float(tgt)
                if math.isnan(src_f) and math.isnan(tgt_f):
                    return True
                if src_f == tgt_f:          # also equal infinities (inf - inf is NaN)
                    return True
                return abs(src_f - tgt_f) <= tolerance
            except (ValueError, TypeError):
                pass
        # String comparison: surrounding whitespace ignored, case significant
        return str(src).strip() == str(tgt).strip()


# ---------------------------------------------------------------------------
# Aggregate comparison
# ---------------------------------------------------------------------------

class AggregateComparator:
    """Compare aggregate metrics (COUNT, SUM, MIN, MAX, AVG, DISTINCT_COUNT)."""

    DEFAULT_METRICS = ["COUNT", "SUM", "MIN", "MAX", "AVG", "DISTINCT_COUNT"]

    def compare(
        self,
        source: SourceConnector,
        target: AIDPConnector,
        config: ReconcileConfig,
    ) -> ReconcileResult:
        result = ReconcileResult(
            config_name=config.name,
            reconcile_type="aggregate",
            status="RUNNING",
        )
        # Pick numeric columns from target schema
        tgt_schema = target.get_schema()
        numeric_types = {
            "IntegerType", "LongType", "ShortType", "ByteType", "FloatType",
            "DoubleType", "DecimalType",
        }
        numeric_cols = [
            s["name"] for s in tgt_schema
            if _normalize_type(s["type"]) in numeric_types
        ]
        ignore = {c.upper() for c in config.ignore_columns}
        numeric_cols = [c for c in numeric_cols if c.upper() not in ignore]

        if not numeric_cols:
            result.status = "PASSED"
            result.overall_match = True
            result.match_percentage = 100.0
            return result

        metrics = self.DEFAULT_METRICS
        src_aggs = source.get_aggregates(
            config.source.table_name, numeric_cols, metrics
        )
        tgt_aggs = target.get_aggregates(
            columns=numeric_cols, metrics=metrics
        )

        # Index target aggs
        tgt_index = {
            (a["column"], a["metric"]): a["value"] for a in tgt_aggs
        }

        agg_results: list[AggregateResult] = []
        all_match = True
        for agg in src_aggs:
            col, metric = agg["column"], agg["metric"]
            src_val = agg["value"]
            tgt_val = tgt_index.get((col, metric), 0.0)
            diff = abs(src_val - tgt_val)
            match = diff <= config.tolerance
            if not match:
                all_match = False
            agg_results.append(AggregateResult(
                column_name=col,
                metric=metric,
                source_value=src_val,
                target_value=tgt_val,
                difference=diff,
                match=match,
            ))

        result.aggregate_results = agg_results
        result.overall_match = all_match
        matched = sum(1 for a in agg_results if a.match)
        total = len(agg_results) or 1
        result.match_percentage = matched / total * 100
        result.status = "PASSED" if all_match else "FAILED"
        return result
