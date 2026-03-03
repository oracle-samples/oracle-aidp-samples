"""
native_rules.py
------------------------------------------------------------
Responsibility:
- Execute basic Spark-native data quality rules
- Fast, scalable, no LLM dependency

Rules implemented:
- NOT NULL
- RANGE
- UNIQUE
- PATTERN (regex)
- DUPLICATE
- COMPLETENESS
- TYPE CHECK
- VALUE SET (allowed values)

These rules should always be preferred
over LLM-based rules where possible.
------------------------------------------------------------
"""

from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, DoubleType, StringType, DateType, TimestampType


def run_not_null(df, rule: dict) -> bool:
    """
    Validate that a column contains no NULL values.

    Args:
        df: Spark DataFrame
        rule (dict): Rule definition containing 'column'

    Returns:
        bool: True if rule passes, False otherwise
    """
    null_count = df.filter(F.col(rule["column"]).isNull()).count()
    return null_count == 0


def run_range(df, rule: dict) -> bool:
    """
    Validate that column values lie within a numeric range.

    Args:
        df: Spark DataFrame
        rule (dict): Rule definition containing min/max

    Returns:
        bool: True if rule passes, False otherwise
    """
    c = rule["column"]
    out_of_range = df.filter(
        (F.col(c) < rule["min"]) | (F.col(c) > rule["max"])
    ).count()
    return out_of_range == 0


def run_unique(df, rule: dict) -> bool:
    """
    Validate that a column contains only unique values.

    Args:
        df: Spark DataFrame
        rule (dict): Rule definition containing 'column'

    Returns:
        bool: True if rule passes, False otherwise
    """
    c = rule["column"]
    total_count = df.count()
    distinct_count = df.select(c).distinct().count()
    return total_count == distinct_count


def run_pattern(df, rule: dict) -> bool:
    """
    Validate that column values match a regex pattern.

    Args:
        df: Spark DataFrame
        rule (dict): Rule definition containing 'column' and 'pattern'

    Returns:
        bool: True if rule passes, False otherwise
    """
    c = rule["column"]
    pattern = rule["pattern"]
    
    violations = df.filter(
        F.col(c).isNotNull() & ~F.col(c).rlike(pattern)
    ).count()
    
    return violations == 0


def run_duplicate(df, rule: dict) -> bool:
    """
    Check for duplicate rows based on specified columns.

    Args:
        df: Spark DataFrame
        rule (dict): Rule with 'columns' (list) or 'column' (single)

    Returns:
        bool: True if no duplicates found
    """
    columns = rule.get("columns", [rule["column"]])
    if isinstance(columns, str):
        columns = [columns]
    
    duplicates = (
        df.groupBy(*columns)
        .count()
        .filter(F.col("count") > 1)
        .count()
    )
    
    return duplicates == 0


def run_completeness(df, rule: dict) -> bool:
    """
    Validate that column completeness meets threshold.

    Args:
        df: Spark DataFrame
        rule (dict): Rule with 'column' and 'min_completeness_pct'

    Returns:
        bool: True if completeness >= threshold
    """
    c = rule["column"]
    min_pct = rule.get("min_completeness_pct", 100)
    
    total = df.count()
    non_null = df.filter(F.col(c).isNotNull()).count()
    
    if total == 0:
        return True
    
    completeness_pct = (non_null / total) * 100
    return completeness_pct >= min_pct


def run_type_check(df, rule: dict) -> bool:
    """
    Validate that column values match expected data type.

    Args:
        df: Spark DataFrame
        rule (dict): Rule with 'column' and 'expected_type'

    Returns:
        bool: True if all values match expected type
    """
    c = rule["column"]
    expected = rule["expected_type"].lower()
    
    # Get actual data type
    actual_type = dict(df.dtypes)[c].lower()
    
    # Type mapping
    type_matches = {
        "integer": ["int", "bigint", "smallint", "tinyint", "long"],
        "float": ["float", "double", "decimal"],
        "string": ["string", "varchar", "char"],
        "date": ["date"],
        "timestamp": ["timestamp"],
        "boolean": ["boolean", "bool"]
    }
    
    for key, vals in type_matches.items():
        if expected in vals and any(actual_type.startswith(v) for v in vals):
            return True
    
    return False


def run_value_set(df, rule: dict) -> bool:
    """
    Validate that column values are within allowed set.

    Args:
        df: Spark DataFrame
        rule (dict): Rule with 'column' and 'allowed_values' (list)

    Returns:
        bool: True if all values are in allowed set
    """
    c = rule["column"]
    allowed = rule["allowed_values"]
    
    violations = df.filter(
        F.col(c).isNotNull() & ~F.col(c).isin(allowed)
    ).count()
    
    return violations == 0


def run_string_length(df, rule: dict) -> bool:
    """
    Validate string length constraints.

    Args:
        df: Spark DataFrame
        rule (dict): Rule with 'column', optional 'min_length', 'max_length'

    Returns:
        bool: True if all strings meet length requirements
    """
    c = rule["column"]
    min_len = rule.get("min_length", 0)
    max_len = rule.get("max_length", float('inf'))
    
    violations = df.filter(
        F.col(c).isNotNull() & 
        ((F.length(c) < min_len) | (F.length(c) > max_len))
    ).count()
    
    return violations == 0
