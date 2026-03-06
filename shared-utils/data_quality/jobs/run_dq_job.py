"""
run_dq_job.py
------------------------------------------------------------
Executes comprehensive data quality checks:
1. Table-level DQ (nulls, ranges, patterns, uniqueness, etc.)
2. Cross-table DQ (foreign keys, referential integrity)
3. Schema validation
4. Volume checks
5. LLM-based semantic checks (optional)

Output:
- Formatted console report
- DataFrame with all results
- Summary statistics
------------------------------------------------------------
"""

from pyspark.sql import SparkSession
from dq.config_loader import load_dq_config
from dq.orchestrator import run_dq_for_table, run_cross_table_rules
from dq.schema_checks import detect_schema_drift
from dq.volume_checks import check_volume
from pyspark.sql import functions as F
import sys
from datetime import datetime

# Add path for local development
sys.path.append("/Workspace/oracle-aidp-samples/data_quality/")


def print_header(text, char="="):
    """Print formatted header."""
    width = 100
    print(f"\n{char * width}")
    print(f"{text.center(width)}")
    print(f"{char * width}")


def print_subheader(text):
    """Print formatted subheader."""
    print(f"\n{'─' * 100}")
    print(f"  {text}")
    print(f"{'─' * 100}")


def run_dq_checks(config_path: str, spark: SparkSession):
    """
    Main orchestrator for data quality checks.
    
    Args:
        config_path (str): Path to DQ configuration YAML
        spark (SparkSession): Active Spark session
        
    Returns:
        DataFrame: Complete results
    """
    
    print_header("🔍 DATA QUALITY ENGINE - AIDP", "=")
    print(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Load configuration
    print_subheader("📋 Loading Configuration")
    cfg = load_dq_config(config_path)
    tables_cfg = cfg.get("tables", {})
    cross_rules = cfg.get("cross_table_rules", [])
    
    print(f"   • Tables to validate: {len(tables_cfg)}")
    print(f"   • Cross-table rules: {len(cross_rules)}")
    
    all_results = []
    table_scores = {}
    
    # ---- Table-level checks ----
    print_header("📊 TABLE-LEVEL DATA QUALITY CHECKS")
    
    for table_key, table_cfg in tables_cfg.items():
        print_subheader(f"Table: {table_key}")
        
        table_name = table_cfg["table_name"]
        print(f"   Loading: {table_name}")
        
        try:
            df = spark.table(table_name)
            row_count = df.count()
            print(f"   ✓ Loaded {row_count:,} rows")
            
            # Schema checks
            if "schema" in table_cfg and "expected" in table_cfg["schema"]:
                print(f"   Checking schema drift...")
                drift = detect_schema_drift(df, table_cfg["schema"]["expected"])
                if drift:
                    print(f"   ⚠️  Schema drift detected: {len(drift)} issues")
                    for col, expected, actual in drift:
                        print(f"      - Column '{col}': Expected {expected}, Got {actual}")
                else:
                    print(f"   ✓ Schema validation passed")
            
            # Volume checks
            if "volume" in table_cfg and "previous_count" in table_cfg["volume"]:
                prev_count = table_cfg["volume"]["previous_count"]
                warn_pct = table_cfg["volume"].get("warn_pct", 20)
                fail_pct = table_cfg["volume"].get("fail_pct", 40)
                
                vol_status, diff_pct = check_volume(row_count, prev_count, warn_pct, fail_pct)
                print(f"   Volume check: {vol_status} ({diff_pct:.1f}% difference from baseline)")
            
            # Run data quality rules
            print(f"   Running {len(table_cfg.get('rules', []))} quality rules...")
            results, score = run_dq_for_table(df, table_key, table_cfg, spark)
            all_results.extend(results)
            table_scores[table_key] = score
            
            # Print rule results
            passed = sum(1 for r in results if r['status'] == 'PASS')
            failed = sum(1 for r in results if r['status'] == 'FAIL')
            errors = sum(1 for r in results if r['status'] == 'ERROR')
            
            print(f"\n   Results: ✓ {passed} passed | ✗ {failed} failed | ⚠️  {errors} errors")
            print(f"   📈 DQ Score: {score}% {'🎉' if score == 100 else '⚠️' if score >= 80 else '❌'}")
            
        except Exception as e:
            print(f"   ❌ Error processing table: {str(e)}")
    
    # ---- Cross-table checks ----
    if cross_rules:
        print_header("🔗 CROSS-TABLE DATA QUALITY CHECKS")
        print(f"   Running {len(cross_rules)} cross-table rules...")
        
        try:
            cross_results = run_cross_table_rules(tables_cfg, cross_rules, spark)
            all_results.extend(cross_results)
            
            passed = sum(1 for r in cross_results if r['status'] == 'PASS')
            failed = sum(1 for r in cross_results if r['status'] == 'FAIL')
            
            print(f"\n   Results: ✓ {passed} passed | ✗ {failed} failed")
            
        except Exception as e:
            print(f"   ❌ Error in cross-table checks: {str(e)}")
    
    # ---- Summary Report ----
    print_header("📋 SUMMARY REPORT")
    
    total_checks = len(all_results)
    passed_checks = sum(1 for r in all_results if r['status'] == 'PASS')
    failed_checks = sum(1 for r in all_results if r['status'] == 'FAIL')
    error_checks = sum(1 for r in all_results if r['status'] == 'ERROR')
    overall_pass_rate = (passed_checks / total_checks * 100) if total_checks > 0 else 0
    
    print(f"\n   Total Checks Executed: {total_checks}")
    print(f"   ✓ Passed: {passed_checks} ({passed_checks/total_checks*100:.1f}%)")
    print(f"   ✗ Failed: {failed_checks} ({failed_checks/total_checks*100:.1f}%)")
    print(f"   ⚠️  Errors: {error_checks} ({error_checks/total_checks*100:.1f}%)")
    print(f"\n   🎯 Overall Pass Rate: {overall_pass_rate:.2f}%")
    
    print("\n   Table Scores:")
    for table, score in table_scores.items():
        status_icon = "🎉" if score == 100 else "✓" if score >= 90 else "⚠️" if score >= 70 else "❌"
        print(f"      {status_icon} {table}: {score}%")
    
    # ---- Failed Checks Detail ----
    failed_results = [r for r in all_results if r['status'] == 'FAIL']
    if failed_results:
        print_header("❌ FAILED CHECKS DETAIL")
        for r in failed_results:
            print(f"\n   • {r['rule']}")
            print(f"     Table: {r['table']}")
            print(f"     Severity: {r['severity']}")
            print(f"     Status: {r['status']}")
    
    print_header("✅ DATA QUALITY CHECK COMPLETED", "=")
    print(f"Completed at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
    
    # Return results as DataFrame
    if all_results:
        results_df = spark.createDataFrame(all_results)
        return results_df
    else:
        return None


# ============================================================
# MAIN EXECUTION
# ============================================================
if __name__ == "__main__":
    
    # Initialize Spark
    spark = SparkSession.builder.appName("AIDP_DQ_ENGINE").getOrCreate()
    
    # Configuration path
    config_path = "/Workspace/oracle-aidp-samples/data_quality/config/dq_rules.yaml"
    
    # Run DQ checks
    results_df = run_dq_checks(config_path, spark)
    
    # Display detailed results
    if results_df:
        print("\n" + "=" * 100)
        print("DETAILED RESULTS TABLE".center(100))
        print("=" * 100 + "\n")
        results_df.show(truncate=False, n=1000)
        
        # Save results (optional)
        # results_df.write.mode("overwrite").saveAsTable("dq_results")
        # print("\n✓ Results saved to table: dq_results")
