"""
schema_checks.py
------------------------------------------------------------
Responsibility:
- Detect schema drift between expected and actual schema
- Prevent silent breaking of downstream reports
------------------------------------------------------------
"""

def detect_schema_drift(df, expected_schema: dict):
    """
    Compare actual DataFrame schema with expected schema.

    Args:
        df: Spark DataFrame
        expected_schema (dict): column -> datatype mapping

    Returns:
        list: List of schema drift tuples
    """
    actual = {f.name: f.dataType.simpleString() for f in df.schema.fields}
    drift = []

    for col, dtype in expected_schema.items():
        if col not in actual:
            drift.append((col, dtype, "MISSING"))
        elif actual[col] != dtype:
            drift.append((col, dtype, actual[col]))

    return drift
