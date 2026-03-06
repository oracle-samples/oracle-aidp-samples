"""
cross_table_rules.py
------------------------------------------------------------
Responsibility:
- Execute cross-table data quality checks
- Foreign key integrity
- Orphan record detection

Best Practices:
- Pure Spark logic
- No LLM
- Scales to large datasets
------------------------------------------------------------
"""

from pyspark.sql import functions as F


def run_foreign_key_rule(
    source_df,
    target_df,
    rule: dict
) -> bool:
    """
    Validate foreign key relationship between two tables.

    Args:
        source_df: Spark DataFrame (child table)
        target_df: Spark DataFrame (parent table)
        rule (dict): FK rule definition

    Returns:
        bool: True if rule passes, False otherwise
    """
    src_col = rule["source"]["column"]
    tgt_col = rule["target"]["column"]

    # Optional NULL handling
    if not rule.get("allow_nulls", True):
        source_df = source_df.filter(F.col(src_col).isNotNull())

    # Find orphan records
    orphan_count = (
        source_df.alias("src")
        .join(
            target_df.select(tgt_col).alias("tgt"),
            F.col(f"src.{src_col}") == F.col(f"tgt.{tgt_col}"),
            how="left_anti"
        )
        .count()
    )

    return orphan_count == 0
