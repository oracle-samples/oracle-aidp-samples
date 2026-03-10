"""
orchestrator.py
------------------------------------------------------------
Runs DQ checks for a SINGLE table.
Called iteratively for multiple tables.
Includes cross-table DQ execution

------------------------------------------------------------
"""

from dq.native_rules import (
    run_not_null, run_range, run_unique, run_pattern, 
    run_duplicate, run_completeness, run_type_check, 
    run_value_set, run_string_length
)
from dq.sql_rules import run_sql_rule
from dq.llm_rules import run_llm_rule
from dq.cross_table_rules import run_foreign_key_rule


def run_dq_for_table(df, table_key, table_cfg, spark):
    results = []
    total_weight = 0
    failed_weight = 0

    for rule in table_cfg.get("rules", []):
        total_weight += rule["weight"]

        try:
            # Native rules (fast, no LLM)
            if rule["type"] == "not_null":
                status = "PASS" if run_not_null(df, rule) else "FAIL"

            elif rule["type"] == "range":
                status = "PASS" if run_range(df, rule) else "FAIL"

            elif rule["type"] == "unique":
                status = "PASS" if run_unique(df, rule) else "FAIL"

            elif rule["type"] == "pattern":
                status = "PASS" if run_pattern(df, rule) else "FAIL"

            elif rule["type"] == "duplicate":
                status = "PASS" if run_duplicate(df, rule) else "FAIL"

            elif rule["type"] == "completeness":
                status = "PASS" if run_completeness(df, rule) else "FAIL"

            elif rule["type"] == "type_check":
                status = "PASS" if run_type_check(df, rule) else "FAIL"

            elif rule["type"] == "value_set":
                status = "PASS" if run_value_set(df, rule) else "FAIL"

            elif rule["type"] == "string_length":
                status = "PASS" if run_string_length(df, rule) else "FAIL"

            # SQL rules
            elif rule["type"] == "sql":
                status = "PASS" if run_sql_rule(df, rule, spark) else "FAIL"

            # LLM rules (use sparingly)
            elif rule["type"] == "llm_semantic":
                status = run_llm_rule(df, rule, spark)

            else:
                status = "ERROR"
                print(f"⚠️  Unknown rule type: {rule['type']}")

        except Exception as e:
            status = "ERROR"
            print(f"❌ Error executing rule '{rule['name']}': {str(e)}")

        if status != "PASS":
            failed_weight += rule["weight"]

        results.append({
            "scope": "TABLE",
            "table": table_key,
            "rule": rule["name"],
            "status": status,
            "severity": rule["severity"],
            "weight": rule["weight"]
        })

    dq_score = round(100 - (failed_weight / total_weight) * 100, 2) if total_weight > 0 else 100
    return results, dq_score


def run_cross_table_rules(
    tables_cfg: dict,
    cross_rules: list,
    spark
):
    """
    Execute all cross-table rules.

    Returns:
        list: rule execution results
    """
    results = []

    # Load all tables once
    dfs = {
        t: spark.table(cfg["table_name"])
        for t, cfg in tables_cfg.items()
    }

    for rule in cross_rules:
        if rule["type"] == "foreign_key":
            src_table = rule["source"]["table"]
            tgt_table = rule["target"]["table"]

            passed = run_foreign_key_rule(
                dfs[src_table],
                dfs[tgt_table],
                rule
            )

            results.append({
                "scope": "CROSS_TABLE",
                "table": f"{src_table}->{tgt_table}",
                "rule": rule["name"],
                "status": "PASS" if passed else "FAIL",
                "severity": rule["severity"],
                "weight": rule["weight"]
            })

    return results
