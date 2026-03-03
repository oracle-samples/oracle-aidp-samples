"""
Single Function to Run All Data Quality Checks
===============================================
Just call run_all_dq_checks() - that's it!

Features:
- Single function call runs everything
- Enable/disable checks via config
- Automatic data generation (optional)
- Complete reporting
"""

import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent / "data_generator"))
sys.path.append(str(Path(__file__).parent))


def run_all_dq_checks(
    config_path: str = None,
    generate_sample_data: bool = False,
    output_format: str = "console",  # console, json, dataframe
    save_results: bool = False
):
    """
    🎯 SINGLE FUNCTION TO RUN ALL DATA QUALITY CHECKS
    
    Args:
        config_path (str): Path to DQ config YAML (optional, uses demo config if None)
        generate_sample_data (bool): Generate test data using data_generator
        output_format (str): 'console', 'json', 'dataframe', or 'all'
        save_results (bool): Save results to CSV file
        
    Returns:
        dict: Complete test results with summary
        
    Example:
        # Simple - uses demo config
        results = run_all_dq_checks()
        
        # With sample data generation
        results = run_all_dq_checks(generate_sample_data=True)
        
        # Custom config
        results = run_all_dq_checks(config_path="/path/to/config.yaml")
    """
    
    print("=" * 100)
    print("🔍 DATA QUALITY - ONE FUNCTION RUNS ALL CHECKS".center(100))
    print("=" * 100)
    
    results = {
        'success': False,
        'checks_run': 0,
        'checks_passed': 0,
        'checks_failed': 0,
        'pass_rate': 0,
        'details': []
    }
    
    try:
        # STEP 1: Generate sample data if requested
        if generate_sample_data:
            print("\n📊 Generating sample data...")
            _generate_test_data()
        
        # STEP 2: Determine which environment (Spark or Pandas)
        try:
            from pyspark.sql import SparkSession
            spark = SparkSession.builder.appName("DQ_Checks").getOrCreate()
            use_spark = True
            print("\n✓ Using PySpark environment")
        except:
            use_spark = False
            print("\n✓ Using Pandas environment (local testing)")
        
        # STEP 3: Run checks based on environment
        if use_spark:
            results = _run_spark_checks(config_path, spark, output_format, save_results)
        else:
            results = _run_pandas_checks(config_path, output_format, save_results)
        
        # STEP 4: Display results
        _display_results(results, output_format)
        
        results['success'] = True
        
    except Exception as e:
        print(f"\n❌ Error: {str(e)}")
        results['error'] = str(e)
    
    return results


def _generate_test_data():
    """Generate sample test data using data_generator"""
    from data_generator import MultiTableDataGenerator
    
    gen = MultiTableDataGenerator(seed=42)
    config = {
        'output_path': './test_output',
        'output_format': 'csv',
        'tables': [
            {
                'table_name': 'users',
                'rows_count': 100,
                'columns': [
                    {'name': 'user_id', 'type': 'integer', 'range': [1, 100], 'unique': True},
                    {'name': 'name', 'type': 'string', 'length': 10, 'prefix': 'User_'},
                    {'name': 'email', 'type': 'email', 'domains': ['example.com', 'test.com'], 'unique': True},
                    {'name': 'age', 'type': 'integer', 'range': [18, 80]},
                    {'name': 'created_date', 'type': 'date', 'format': '%Y-%m-%d'}
                ]
            },
            {
                'table_name': 'orders',
                'rows_count': 500,
                'columns': [
                    {'name': 'order_id', 'type': 'integer', 'range': [1, 500], 'unique': True},
                    {'name': 'user_id', 'type': 'reference', 'ref_table': 'users', 'ref_column': 'user_id'},
                    {'name': 'product', 'type': 'choice', 'values': ['Laptop', 'Phone', 'Tablet', 'Monitor', 'Keyboard']},
                    {'name': 'amount', 'type': 'float', 'range': [10.0, 5000.0], 'decimals': 2},
                    {'name': 'status', 'type': 'choice', 'values': ['pending', 'completed', 'cancelled', 'shipped']},
                    {'name': 'order_date', 'type': 'date', 'format': '%Y-%m-%d'}
                ]
            }
        ]
    }
    
    gen.generate_from_config(config)
    print("   ✓ Test data generated in ./test_output/")


def _run_spark_checks(config_path, spark, output_format, save_results):
    """Run checks in Spark environment"""
    from jobs.run_dq_job import run_dq_checks
    
    # Use demo config if none provided
    if config_path is None:
        config_path = str(Path(__file__).parent / "config" / "demo_dq_config.yaml")
    
    print(f"\n📋 Loading configuration: {config_path}")
    
    # Load data into Spark tables if CSV files exist
    import os
    if os.path.exists('./test_output/users.csv'):
        print("\n📥 Loading data into Spark tables...")
        users_df = spark.read.csv("./test_output/users.csv", header=True, inferSchema=True)
        users_df.write.mode("overwrite").saveAsTable("default.users")
        
        orders_df = spark.read.csv("./test_output/orders.csv", header=True, inferSchema=True)
        orders_df.write.mode("overwrite").saveAsTable("default.orders")
        print("   ✓ Tables created: default.users, default.orders")
    
    # Run DQ checks
    results_df = run_dq_checks(config_path, spark)
    
    # Convert to result dictionary
    results_list = results_df.collect() if results_df else []
    
    passed = sum(1 for r in results_list if r['status'] == 'PASS')
    failed = sum(1 for r in results_list if r['status'] == 'FAIL')
    
    if save_results and results_df:
        results_df.write.mode("overwrite").csv("./dq_results", header=True)
        print("\n✓ Results saved to: ./dq_results/")
    
    return {
        'checks_run': len(results_list),
        'checks_passed': passed,
        'checks_failed': failed,
        'pass_rate': (passed / len(results_list) * 100) if results_list else 0,
        'details': [r.asDict() for r in results_list],
        'dataframe': results_df
    }


def _run_pandas_checks(config_path, output_format, save_results):
    """Run checks in Pandas environment (for local testing)"""
    import pandas as pd
    import yaml
    
    # Use demo config if none provided
    if config_path is None:
        config_path = str(Path(__file__).parent / "config" / "demo_dq_config.yaml")
    
    print(f"\n📋 Loading configuration: {config_path}")
    
    # Load config
    with open(config_path, 'r') as f:
        config = yaml.safe_load(f)
    
    # Load data
    print("\n📥 Loading data...")
    users_df = pd.read_csv('./test_output/users.csv')
    orders_df = pd.read_csv('./test_output/orders.csv')
    print(f"   ✓ Users: {len(users_df)} rows")
    print(f"   ✓ Orders: {len(orders_df)} rows")
    
    # Run checks
    from test_dq_complete import (
        test_not_null_pandas, test_unique_pandas, test_range_pandas,
        test_pattern_pandas, test_foreign_key_pandas
    )
    
    all_checks = []
    
    # Process tables from config
    tables = config.get('tables', {})
    
    for table_name, table_config in tables.items():
        df = users_df if table_name == 'users' else orders_df
        rules = table_config.get('rules', [])
        
        for rule in rules:
            # Check if rule is enabled (default True)
            if not rule.get('enabled', True):
                continue
            
            check_type = rule['type']
            result = False
            
            try:
                if check_type == 'not_null':
                    result = test_not_null_pandas(df, rule['column'])
                elif check_type == 'unique':
                    result = test_unique_pandas(df, rule['column'])
                elif check_type == 'range':
                    result = test_range_pandas(df, rule['column'], rule['min'], rule['max'])
                elif check_type == 'pattern':
                    result = test_pattern_pandas(df, rule['column'], rule['pattern'])
                elif check_type == 'value_set':
                    result = df[rule['column']].isin(rule['allowed_values']).all()
                elif check_type == 'completeness':
                    completeness = (df[rule['column']].notna().sum() / len(df)) * 100
                    result = completeness >= rule.get('min_completeness_pct', 100)
            except Exception as e:
                print(f"   ⚠️  Error in {rule['name']}: {str(e)}")
                result = False
            
            all_checks.append({
                'table': table_name,
                'rule': rule['name'],
                'type': check_type,
                'status': 'PASS' if result else 'FAIL',
                'severity': rule.get('severity', 'MEDIUM'),
                'weight': rule.get('weight', 1)
            })
    
    # Cross-table checks
    cross_rules = config.get('cross_table_rules', [])
    for rule in cross_rules:
        if not rule.get('enabled', True):
            continue
        
        if rule['type'] == 'foreign_key':
            result = test_foreign_key_pandas(
                orders_df, users_df,
                rule['source']['column'],
                rule['target']['column']
            )
            
            all_checks.append({
                'table': f"{rule['source']['table']}->{rule['target']['table']}",
                'rule': rule['name'],
                'type': 'foreign_key',
                'status': 'PASS' if result else 'FAIL',
                'severity': rule.get('severity', 'CRITICAL'),
                'weight': rule.get('weight', 10)
            })
    
    passed = sum(1 for c in all_checks if c['status'] == 'PASS')
    failed = len(all_checks) - passed
    
    # Save results if requested
    if save_results:
        results_df = pd.DataFrame(all_checks)
        results_df.to_csv('./dq_results.csv', index=False)
        print("\n✓ Results saved to: ./dq_results.csv")
    
    return {
        'checks_run': len(all_checks),
        'checks_passed': passed,
        'checks_failed': failed,
        'pass_rate': (passed / len(all_checks) * 100) if all_checks else 0,
        'details': all_checks
    }


def _display_results(results, output_format):
    """Display results in requested format"""
    
    if output_format in ['console', 'all']:
        print("\n" + "=" * 100)
        print("📊 RESULTS SUMMARY".center(100))
        print("=" * 100)
        print(f"\n   Total Checks: {results['checks_run']}")
        print(f"   ✅ Passed: {results['checks_passed']}")
        print(f"   ❌ Failed: {results['checks_failed']}")
        print(f"   🎯 Pass Rate: {results['pass_rate']:.2f}%")
        
        if results['pass_rate'] == 100:
            print("\n   🎉 EXCELLENT! All checks passed!")
        elif results['pass_rate'] >= 80:
            print("\n   ⚠️  GOOD. Most checks passed, some need attention.")
        else:
            print("\n   ❌ CRITICAL. Multiple checks failed!")
        
        # Show failed checks
        failed_checks = [c for c in results['details'] if c['status'] == 'FAIL']
        if failed_checks:
            print("\n   Failed Checks:")
            for check in failed_checks:
                print(f"      ❌ {check['table']}.{check['rule']} ({check['type']})")
    
    if output_format in ['json', 'all']:
        import json
        print("\n" + "=" * 100)
        print("JSON OUTPUT".center(100))
        print("=" * 100)
        print(json.dumps(results, indent=2, default=str))


# ============================================================
# CONVENIENCE FUNCTIONS
# ============================================================

def quick_test():
    """Quick test with sample data generation"""
    return run_all_dq_checks(generate_sample_data=True, output_format="console")


def production_run(config_path: str):
    """Production run with custom config"""
    return run_all_dq_checks(
        config_path=config_path,
        generate_sample_data=False,
        output_format="all",
        save_results=True
    )


# ============================================================
# MAIN EXECUTION
# ============================================================
if __name__ == "__main__":
    print("""
╔═══════════════════════════════════════════════════════════════╗
║                                                               ║
║     DATA QUALITY TOOL - ONE FUNCTION RUNS ALL CHECKS         ║
║                                                               ║
║  Usage:                                                       ║
║    from run_all_checks import run_all_dq_checks              ║
║    results = run_all_dq_checks()                             ║
║                                                               ║
║  Or use convenience functions:                               ║
║    quick_test()          # Generate data + run all checks    ║
║    production_run(path)  # Run with custom config            ║
║                                                               ║
╚═══════════════════════════════════════════════════════════════╝
""")
    
    # Run quick test
    results = quick_test()
    
    print("\n" + "=" * 100)
    print("✅ DONE! Use the functions above in your code.".center(100))
    print("=" * 100)
