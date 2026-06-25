"""
AIDP Glue Compat - Replacement for boto3 AWS Glue table location lookups.
Uses Spark catalog (DESCRIBE FORMATTED) instead of boto3/Glue API.
"""

import re as _re
_SAFE_IDENTIFIER = _re.compile(r'^[A-Za-z0-9_]+$')


def get_glue_table_s3_location(database: str, table: str, spark=None) -> str:
    """Get a catalog table's storage location via DESCRIBE FORMATTED.

    Drop-in replacement for boto3-based get_glue_table_s3_location() calls.
    Works on AIDP by querying the Spark catalog instead of AWS Glue.

    Args:
        database: Database/schema name
        table: Table name
        spark: SparkSession (optional â€” uses active session if not provided)

    Returns:
        Storage location string (e.g. 'oci://bucket@namespace/path/')

    Raises:
        ValueError: If the table is not found or has no Location field
    """
    if not _SAFE_IDENTIFIER.match(database):
        raise ValueError(f"Unsafe database identifier: {database!r}. Must match ^[A-Za-z0-9_]+$")
    if not _SAFE_IDENTIFIER.match(table):
        raise ValueError(f"Unsafe table identifier: {table!r}. Must match ^[A-Za-z0-9_]+$")

    from pyspark.sql import SparkSession
    if spark is None:
        spark = SparkSession.getActiveSession()
    if spark is None:
        raise RuntimeError("No active SparkSession â€” cannot look up table location")

    rows = spark.sql(f"DESCRIBE FORMATTED `{database}`.`{table}`").collect()
    for row in rows:
        if row[0].strip().lower() == "location":
            loc = row[1].strip()
            if loc and loc != "":
                return loc
    raise ValueError(
        f"Location not found for {database}.{table} â€” "
        f"table may not exist or may not have a storage location"
    )
