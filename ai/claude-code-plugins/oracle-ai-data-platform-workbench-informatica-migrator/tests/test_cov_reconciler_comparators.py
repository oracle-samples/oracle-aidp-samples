"""Behaviour of the four reconciliation comparators, against in-memory fakes.

A reconciliation verdict is what a migration team signs off on, so the
thing pinned here is the verdict (status, match percentage, counts, diff
records) a comparator reaches for a given pair of source/target readings --
not how it reaches it. The fakes implement exactly the connector methods
the comparators call (get_row_count / get_schema / get_data /
get_aggregates) and record the arguments, so no database or Spark session
is involved.
"""
from __future__ import annotations

import math
from datetime import datetime, timedelta

import pytest

from infa2aidp.reconciler.comparators import (
    AggregateComparator,
    DataComparator,
    RowCountComparator,
    SchemaComparator,
    _normalize_type,
    _types_compatible,
)
from infa2aidp.reconciler.models import ReconcileConfig, SourceConfig, TargetConfig


class FakeSide:
    """One side of a reconciliation: canned readings plus a call log."""

    def __init__(self, *, count=0, schema=None, rows=None, aggs=None):
        self.count = count
        self.schema = schema or []
        self.rows = rows or []
        self.aggs = aggs or []
        self.calls: list[tuple] = []

    def get_row_count(self, table="", where=""):
        self.calls.append(("count", table, where))
        return self.count

    def get_schema(self, table=""):
        self.calls.append(("schema", table))
        return self.schema

    def get_data(self, table="", columns=None, where="", limit=0):
        self.calls.append(("data", table, list(columns or []), where, limit))
        return self.rows

    def get_aggregates(self, table="", columns=None, metrics=None):
        self.calls.append(("aggs", table, list(columns or []), list(metrics or [])))
        return self.aggs


def _cfg(**kw) -> ReconcileConfig:
    kw.setdefault("name", "orders")
    kw.setdefault("source", SourceConfig(table_name="SRC.ORDERS"))
    kw.setdefault("target", TargetConfig(full_table_name="cat.sch.orders"))
    return ReconcileConfig(**kw)


# ---------------------------------------------------------------------------
# Row count
# ---------------------------------------------------------------------------

class TestRowCountComparator:
    def test_equal_counts_pass_at_100_percent(self):
        r = RowCountComparator().compare(FakeSide(count=100), FakeSide(count=100), _cfg())
        assert r.status == "PASSED"
        assert r.row_count_match and r.overall_match
        assert r.match_percentage == 100.0
        assert (r.source_row_count, r.target_row_count) == (100, 100)
        assert r.reconcile_type == "row_count" and r.config_name == "orders"

    def test_target_short_by_ten_percent_fails_at_90(self):
        r = RowCountComparator().compare(FakeSide(count=100), FakeSide(count=90), _cfg())
        assert r.status == "FAILED"
        assert not r.overall_match
        assert r.match_percentage == pytest.approx(90.0)

    def test_target_over_by_ratio_is_symmetric(self):
        """A target with extra rows is scored by min/max, not by a >100% ratio."""
        r = RowCountComparator().compare(FakeSide(count=90), FakeSide(count=100), _cfg())
        assert r.status == "FAILED"
        assert r.match_percentage == pytest.approx(90.0)

    def test_both_empty_is_a_pass(self):
        r = RowCountComparator().compare(FakeSide(count=0), FakeSide(count=0), _cfg())
        assert r.status == "PASSED" and r.match_percentage == 100.0

    def test_empty_source_nonempty_target_fails_at_zero(self):
        r = RowCountComparator().compare(FakeSide(count=0), FakeSide(count=5), _cfg())
        assert r.status == "FAILED" and r.match_percentage == 0.0

    def test_where_clause_and_source_table_are_passed_to_both_sides(self):
        src, tgt = FakeSide(count=1), FakeSide(count=1)
        RowCountComparator().compare(src, tgt, _cfg(where_clause="region = 'EU'"))
        assert src.calls == [("count", "SRC.ORDERS", "region = 'EU'")]
        assert tgt.calls == [("count", "", "region = 'EU'")]


# ---------------------------------------------------------------------------
# Type normalisation / compatibility
# ---------------------------------------------------------------------------

class TestTypeNormalisation:
    @pytest.mark.parametrize("raw,expected", [
        ("NUMBER", "DecimalType"),
        ("NUMBER(10,2)", "DecimalType"),
        ("NUMBER(5)", "IntegerType"),
        ("NUMBER(10,0)", "IntegerType"),
        ("NUMBER(12)", "LongType"),
        ("NUMBER(18,0)", "LongType"),
        ("NUMBER(20)", "DecimalType"),
        ("NUMBER(*,0)", "DecimalType"),
        ("NUMBER(abc)", "DecimalType"),
        ("varchar2(100)", "StringType"),
        ("  VARCHAR2(30 CHAR) ", "StringType"),
        ("DATE", "TimestampType"),
        ("TIMESTAMP(6)", "TimestampType"),
        ("DOUBLE PRECISION", "DoubleType"),
        ("CHARACTER VARYING(20)", "StringType"),
        ("LONG RAW", "BinaryType"),
        ("BIT", "BooleanType"),
        ("UNIQUEIDENTIFIER", "StringType"),
        # Spark side: str(field.dataType) forms
        ("StringType()", "StringType"),
        ("IntegerType()", "IntegerType"),
        ("DecimalType(10,2)", "DecimalType"),
        ("LongType", "LongType"),
        ("TimestampType()", "TimestampType"),
        ("DateType()", "DateType"),
        # Unknown types come back upper-cased, not dropped
        ("GEOGRAPHY", "GEOGRAPHY"),
    ])
    def test_normalize(self, raw, expected):
        assert _normalize_type(raw) == expected

    @pytest.mark.parametrize("src,tgt", [
        ("VARCHAR2(50)", "StringType()"),
        ("NUMBER(10,2)", "DecimalType(10,2)"),
        ("NUMBER(5)", "LongType()"),       # numeric family widening
        ("INT", "DoubleType()"),
        ("DATE", "DateType()"),            # timestamp family
        ("DATETIME2", "TimestampType()"),
        ("CLOB", "StringType()"),
        ("BLOB", "BinaryType()"),
    ])
    def test_compatible_pairs(self, src, tgt):
        assert _types_compatible(src, tgt)

    @pytest.mark.parametrize("src,tgt", [
        ("VARCHAR2(50)", "IntegerType()"),
        ("DATE", "StringType()"),
        ("NUMBER", "StringType()"),
        ("BLOB", "StringType()"),
        ("BIT", "IntegerType()"),
    ])
    def test_incompatible_pairs(self, src, tgt):
        assert not _types_compatible(src, tgt)

    def test_mysql_tinyint1_is_boolean(self):
        assert _types_compatible("TINYINT(1)", "BooleanType()")

    def test_spark_bytetype_is_numeric(self):
        assert _types_compatible("TINYINT", "ByteType()")


# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------

def _col(name, typ):
    return {"name": name, "type": typ, "nullable": True}


class TestSchemaComparator:
    def test_matching_schema_case_insensitive_passes(self):
        src = FakeSide(schema=[_col("ID", "NUMBER(10)"), _col("NAME", "VARCHAR2(50)")])
        tgt = FakeSide(schema=[_col("id", "IntegerType()"), _col("name", "StringType()")])
        r = SchemaComparator().compare(src, tgt, _cfg())
        assert r.status == "PASSED" and r.schema_match and r.schema_diffs == []
        assert r.match_percentage == 100.0
        assert src.calls == [("schema", "SRC.ORDERS")]

    def test_missing_extra_and_mistyped_columns_are_each_reported(self):
        src = FakeSide(schema=[
            _col("ID", "NUMBER(10)"), _col("NAME", "VARCHAR2(50)"), _col("DROPPED", "DATE"),
        ])
        tgt = FakeSide(schema=[
            _col("ID", "IntegerType()"), _col("NAME", "IntegerType()"), _col("ADDED", "StringType()"),
        ])
        r = SchemaComparator().compare(src, tgt, _cfg())
        assert r.status == "FAILED" and not r.schema_match and r.match_percentage == 0.0
        kinds = {(d.column_name, d.diff_type) for d in r.schema_diffs}
        assert kinds == {
            ("NAME", "TYPE_MISMATCH"),
            ("DROPPED", "MISSING_IN_TARGET"),
            ("ADDED", "MISSING_IN_SOURCE"),
        }
        mistyped = next(d for d in r.schema_diffs if d.diff_type == "TYPE_MISMATCH")
        assert (mistyped.source_type, mistyped.target_type) == ("VARCHAR2(50)", "IntegerType()")

    def test_ignored_columns_are_not_compared_on_either_side(self):
        src = FakeSide(schema=[_col("ID", "INT"), _col("LOAD_TS", "DATE")])
        tgt = FakeSide(schema=[_col("ID", "IntegerType()"), _col("ETL_BATCH", "LongType()")])
        r = SchemaComparator().compare(src, tgt, _cfg(ignore_columns=["load_ts", "etl_batch"]))
        assert r.status == "PASSED", r.schema_diffs

    def test_column_mapping_renames_are_honoured_both_ways(self):
        src = FakeSide(schema=[_col("CUST_NO", "NUMBER(10)")])
        tgt = FakeSide(schema=[_col("customer_id", "IntegerType()")])
        r = SchemaComparator().compare(src, tgt, _cfg(column_mapping={"cust_no": "customer_id"}))
        assert r.status == "PASSED", r.schema_diffs

    def test_mapping_to_an_ignored_target_column_skips_it(self):
        src = FakeSide(schema=[_col("A", "INT")])
        tgt = FakeSide(schema=[_col("B", "IntegerType()")])
        r = SchemaComparator().compare(
            src, tgt, _cfg(column_mapping={"A": "B"}, ignore_columns=["B"]))
        assert r.status == "PASSED"


# ---------------------------------------------------------------------------
# Data
# ---------------------------------------------------------------------------

class TestDataComparator:
    def test_key_columns_are_required(self):
        src, tgt = FakeSide(), FakeSide()
        r = DataComparator().compare(src, tgt, _cfg())
        assert r.status == "ERROR"
        assert "key_columns" in r.error_message
        assert src.calls == [] and tgt.calls == []

    def test_identical_rows_pass(self):
        rows = [{"ID": 1, "AMT": 10.5, "NAME": "a"}, {"ID": 2, "AMT": None, "NAME": "b"}]
        tgt_rows = [{"id": 2, "amt": None, "name": "b"}, {"id": 1, "amt": 10.5, "name": "a"}]
        r = DataComparator().compare(
            FakeSide(rows=rows), FakeSide(rows=tgt_rows),
            _cfg(key_columns=["id"], compare_columns=["ID", "AMT", "NAME"]))
        assert r.status == "PASSED" and r.overall_match
        assert (r.total_rows_compared, r.matching_rows, r.mismatched_rows) == (2, 2, 0)
        assert r.match_percentage == 100.0
        assert r.sample_diffs == []

    def test_value_null_and_missing_rows_are_classified(self):
        src = [
            {"ID": 1, "V": "x"},     # matches
            {"ID": 2, "V": "x"},     # value mismatch
            {"ID": 3, "V": None},    # null vs value
            {"ID": 4, "V": "x"},     # missing in target
        ]
        tgt = [
            {"ID": 1, "V": "x"},
            {"ID": 2, "V": "y"},
            {"ID": 3, "V": "z"},
            {"ID": 5, "V": "x"},     # missing in source
        ]
        r = DataComparator().compare(
            FakeSide(rows=src), FakeSide(rows=tgt), _cfg(key_columns=["ID"], compare_columns=["V"]))
        assert r.status == "FAILED"
        assert (r.matching_rows, r.mismatched_rows) == (1, 2)
        assert (r.missing_in_target, r.missing_in_source) == (1, 1)
        assert r.total_rows_compared == 4
        assert r.match_percentage == pytest.approx(25.0)
        by_key = {d.key_values["ID"]: d for d in r.sample_diffs}
        assert by_key[2].diff_type == "VALUE_MISMATCH"
        assert by_key[2].column_diffs[0].diff_type == "VALUE_MISMATCH"
        assert (by_key[2].column_diffs[0].source_value, by_key[2].column_diffs[0].target_value) == ("x", "y")
        assert by_key[3].column_diffs[0].diff_type == "NULL_MISMATCH"
        assert by_key[4].diff_type == "MISSING_IN_TARGET" and by_key[4].column_diffs == []
        assert by_key[5].diff_type == "MISSING_IN_SOURCE"

    def test_sample_diffs_are_capped_but_counts_are_not(self):
        n = DataComparator.MAX_SAMPLE_DIFFS + 25
        src = [{"ID": i, "V": "a"} for i in range(n)]
        tgt = [{"ID": i, "V": "b"} for i in range(n)]
        r = DataComparator().compare(
            FakeSide(rows=src), FakeSide(rows=tgt), _cfg(key_columns=["ID"], compare_columns=["V"]))
        assert r.mismatched_rows == n
        assert len(r.sample_diffs) == DataComparator.MAX_SAMPLE_DIFFS

    def test_missing_rows_also_respect_the_cap(self):
        n = DataComparator.MAX_SAMPLE_DIFFS + 5
        src = [{"ID": i} for i in range(n)]
        tgt = [{"ID": -i - 1} for i in range(n)]
        r = DataComparator().compare(
            FakeSide(rows=src), FakeSide(rows=tgt), _cfg(key_columns=["ID"], compare_columns=["ID"]))
        assert (r.missing_in_target, r.missing_in_source) == (n, n)
        assert len(r.sample_diffs) == DataComparator.MAX_SAMPLE_DIFFS

    def test_numeric_tolerance_is_absolute(self):
        src = [{"ID": 1, "AMT": 100.0}, {"ID": 2, "AMT": 100.0}]
        tgt = [{"ID": 1, "AMT": 100.00005}, {"ID": 2, "AMT": "100.5"}]
        r = DataComparator().compare(
            FakeSide(rows=src), FakeSide(rows=tgt),
            _cfg(key_columns=["ID"], compare_columns=["AMT"], tolerance=0.0001))
        assert (r.matching_rows, r.mismatched_rows) == (1, 1)
        assert r.sample_diffs[0].key_values == {"ID": 2}

    def test_numeric_string_and_number_compare_by_value(self):
        """A CSV source reads every value as text; '42' must equal 42."""
        r = DataComparator().compare(
            FakeSide(rows=[{"ID": "1", "N": "42"}]), FakeSide(rows=[{"ID": "1", "N": 42}]),
            _cfg(key_columns=["ID"], compare_columns=["N"]))
        assert r.status == "PASSED"

    def test_nan_equals_nan_and_whitespace_is_trimmed(self):
        r = DataComparator().compare(
            FakeSide(rows=[{"ID": 1, "F": float("nan"), "S": "abc  "}]),
            FakeSide(rows=[{"ID": 1, "F": float("nan"), "S": "abc"}]),
            _cfg(key_columns=["ID"], compare_columns=["F", "S"]))
        assert r.status == "PASSED"

    def test_string_comparison_is_case_sensitive(self):
        r = DataComparator().compare(
            FakeSide(rows=[{"ID": 1, "S": "Abc"}]), FakeSide(rows=[{"ID": 1, "S": "abc"}]),
            _cfg(key_columns=["ID"], compare_columns=["S"]))
        assert r.status == "FAILED"

    def test_composite_key(self):
        src = [{"A": 1, "B": "x", "V": 1}, {"A": 1, "B": "y", "V": 2}]
        tgt = [{"A": 1, "B": "y", "V": 2}, {"A": 1, "B": "x", "V": 1}]
        r = DataComparator().compare(
            FakeSide(rows=src), FakeSide(rows=tgt), _cfg(key_columns=["A", "B"], compare_columns=["V"]))
        assert r.status == "PASSED" and r.matching_rows == 2

    def test_columns_default_to_target_schema_minus_ignored_plus_keys(self):
        tgt = FakeSide(schema=[_col("NAME", "StringType()"), _col("ETL_TS", "TimestampType()")])
        src = FakeSide()
        DataComparator().compare(
            src, tgt, _cfg(key_columns=["ID"], ignore_columns=["etl_ts"],
                           sample_size=10, where_clause="x > 1"))
        assert src.calls[-1] == ("data", "SRC.ORDERS", ["ID", "NAME"], "x > 1", 10)
        assert tgt.calls[-1] == ("data", "", ["ID", "NAME"], "x > 1", 10)

    def test_column_mapping_compares_source_column_against_renamed_target(self):
        r = DataComparator().compare(
            FakeSide(rows=[{"ID": 1, "OLD": "v"}]),
            FakeSide(rows=[{"ID": 1, "NEW": "v"}]),
            _cfg(key_columns=["ID"], compare_columns=["OLD"], column_mapping={"old": "new"}))
        assert r.status == "PASSED"

    def test_empty_on_both_sides_passes(self):
        r = DataComparator().compare(
            FakeSide(), FakeSide(), _cfg(key_columns=["ID"], compare_columns=["ID"]))
        assert r.status == "PASSED" and r.match_percentage == 100.0

    def test_date_tolerance_seconds_is_applied(self):
        t = datetime(2026, 1, 1, 12, 0, 0)
        r = DataComparator().compare(
            FakeSide(rows=[{"ID": 1, "TS": t}]),
            FakeSide(rows=[{"ID": 1, "TS": t + timedelta(milliseconds=500)}]),
            _cfg(key_columns=["ID"], compare_columns=["TS"], date_tolerance_seconds=1))
        assert r.status == "PASSED"

    def test_leading_zeros_in_text_are_significant(self):
        r = DataComparator().compare(
            FakeSide(rows=[{"ID": 1, "ZIP": "00123"}]),
            FakeSide(rows=[{"ID": 1, "ZIP": "123"}]),
            _cfg(key_columns=["ID"], compare_columns=["ZIP"]))
        assert r.status == "FAILED"

    def test_equal_infinities_match(self):
        r = DataComparator().compare(
            FakeSide(rows=[{"ID": 1, "F": math.inf}]),
            FakeSide(rows=[{"ID": 1, "F": math.inf}]),
            _cfg(key_columns=["ID"], compare_columns=["F"]))
        assert r.status == "PASSED"

    def test_duplicate_keys_in_target_do_not_pass(self):
        r = DataComparator().compare(
            FakeSide(rows=[{"ID": 1, "V": "a"}]),
            FakeSide(rows=[{"ID": 1, "V": "a"}, {"ID": 1, "V": "a"}]),
            _cfg(key_columns=["ID"], compare_columns=["V"]))
        assert r.status == "FAILED"

    def test_empty_source_with_target_rows_is_not_100_percent(self):
        r = DataComparator().compare(
            FakeSide(rows=[]), FakeSide(rows=[{"ID": 1}]),
            _cfg(key_columns=["ID"], compare_columns=["ID"]))
        assert r.status == "FAILED"
        assert r.match_percentage < 100.0


# ---------------------------------------------------------------------------
# Aggregates
# ---------------------------------------------------------------------------

def _aggs(values: dict) -> list[dict]:
    return [{"column": c, "metric": m, "value": v} for (c, m), v in values.items()]


class TestAggregateComparator:
    SCHEMA = [
        _col("AMT", "DecimalType(10,2)"), _col("QTY", "IntegerType()"),
        _col("NAME", "StringType()"), _col("ETL_ID", "LongType()"),
    ]

    def test_only_numeric_non_ignored_columns_are_aggregated(self):
        src = FakeSide(aggs=[])
        tgt = FakeSide(schema=self.SCHEMA, aggs=[])
        AggregateComparator().compare(src, tgt, _cfg(ignore_columns=["etl_id"]))
        assert src.calls == [("aggs", "SRC.ORDERS", ["AMT", "QTY"], AggregateComparator.DEFAULT_METRICS)]
        assert tgt.calls[-1] == ("aggs", "", ["AMT", "QTY"], AggregateComparator.DEFAULT_METRICS)

    def test_matching_aggregates_pass(self):
        vals = {("AMT", "SUM"): 1000.0, ("AMT", "MAX"): 99.5, ("QTY", "COUNT"): 10.0}
        r = AggregateComparator().compare(
            FakeSide(aggs=_aggs(vals)), FakeSide(schema=self.SCHEMA, aggs=_aggs(vals)), _cfg())
        assert r.status == "PASSED" and r.overall_match and r.match_percentage == 100.0
        assert len(r.aggregate_results) == 3
        assert all(a.match and a.difference == 0 for a in r.aggregate_results)

    def test_a_differing_metric_fails_and_is_scored(self):
        src = {("AMT", "SUM"): 1000.0, ("AMT", "MIN"): 1.0, ("QTY", "SUM"): 50.0, ("QTY", "MAX"): 9.0}
        tgt = dict(src)
        tgt[("AMT", "SUM")] = 999.0
        r = AggregateComparator().compare(
            FakeSide(aggs=_aggs(src)), FakeSide(schema=self.SCHEMA, aggs=_aggs(tgt)), _cfg())
        assert r.status == "FAILED"
        assert r.match_percentage == pytest.approx(75.0)
        bad = [a for a in r.aggregate_results if not a.match]
        assert len(bad) == 1
        assert (bad[0].column_name, bad[0].metric, bad[0].difference) == ("AMT", "SUM", 1.0)
        assert (bad[0].source_value, bad[0].target_value) == (1000.0, 999.0)

    def test_difference_within_tolerance_passes(self):
        r = AggregateComparator().compare(
            FakeSide(aggs=_aggs({("AMT", "AVG"): 10.0})),
            FakeSide(schema=self.SCHEMA, aggs=_aggs({("AMT", "AVG"): 10.004})),
            _cfg(tolerance=0.01))
        assert r.status == "PASSED"

    def test_no_numeric_columns_is_a_trivial_pass_without_querying(self):
        src = FakeSide()
        r = AggregateComparator().compare(
            src, FakeSide(schema=[_col("NAME", "StringType()")]), _cfg())
        assert r.status == "PASSED" and r.match_percentage == 100.0
        assert src.calls == []
