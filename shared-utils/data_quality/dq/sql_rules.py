"""
sql_rules.py
------------------------------------------------------------
Responsibility:
- Execute business-defined SQL data quality rules
- Enables business teams to express logic in SQL

Best Practices:
- SQL must return a count of violating records
- cnt = 0 implies PASS
------------------------------------------------------------
"""

def run_sql_rule(df, rule: dict, spark) -> bool:
    """
    Execute SQL-based data quality rule.

    Args:
        df: Spark DataFrame
        rule (dict): Rule containing SQL statement
        spark: SparkSession

    Returns:
        bool: True if rule passes, False otherwise
    """
    df.createOrReplaceTempView("dq_table")

    sql = rule["sql"].replace("${TABLE}", "dq_table")
    cnt = spark.sql(sql).first()["cnt"]

    return cnt == 0
