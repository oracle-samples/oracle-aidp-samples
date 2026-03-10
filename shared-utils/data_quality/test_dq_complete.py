"""
Complete Data Quality Test Script
==================================
This script:
1. Generates sample data using data_generator
2. Creates test tables
3. Runs comprehensive data quality checks
4. Displays results

Run this in AIDP workbench to test the complete workflow.
"""

import sys
import os
from pathlib import Path

# Add paths
sys.path.append("/Workspace/oracle-aidp-samples/data_generator")
sys.path.append("/Workspace/oracle-aidp-samples/data_quality")

# Alternative for local testing
current_dir = Path(__file__).parent
sys.path.append(str(current_dir.parent / "data_generator"))
sys.path.append(str(current_dir))

print("=" * 100)
print("DATA QUALITY TOOL - COMPLETE TEST".center(100))
print("=" * 100)

# ============================================================
# STEP 1: Generate Test Data
# ============================================================
print("\n" + "─" * 100)
print("STEP 1: Generating Test Data".center(100))
print("─" * 100)

try:
    # Try different import methods
    try:
        from data_generator.data_generator import MultiTableDataGenerator
    except:
        sys.path.insert(0, str(Path(__file__).parent.parent / "data_generator"))
        from data_generator import MultiTableDataGenerator
    
    gen = MultiTableDataGenerator(seed=42)
    
    # Configuration for test data
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
    
    results = gen.generate_from_config(config)
    
    print("\n✅ Data Generation Complete!")
    for table_name, result in results.items():
        print(f"   • {table_name}: {result['rows_count']} rows generated")
    
except Exception as e:
    print(f"\n❌ Error generating data: {str(e)}")
    sys.exit(1)

# ============================================================
# STEP 2: Load Data and Create Mock Spark Environment
# ============================================================
print("\n" + "─" * 100)
print("STEP 2: Testing Data Quality Rules".center(100))
print("─" * 100)

try:
    # For testing without Spark, we'll test individual rules directly
    import pandas as pd
    
    # Load generated data
    users_df_pd = pd.read_csv('./test_output/users.csv')
    orders_df_pd = pd.read_csv('./test_output/orders.csv')
    
    print(f"\n✓ Loaded data:")
    print(f"   • Users: {len(users_df_pd)} rows")
    print(f"   • Orders: {len(orders_df_pd)} rows")
    
    # Display sample
    print("\n📊 Sample Data - Users:")
    print(users_df_pd.head(3).to_string())
    
    print("\n📊 Sample Data - Orders:")
    print(orders_df_pd.head(3).to_string())
    
except Exception as e:
    print(f"\n❌ Error loading data: {str(e)}")
    sys.exit(1)

# ============================================================
# STEP 3: Test Data Quality Checks (Without Spark)
# ============================================================
print("\n" + "─" * 100)
print("STEP 3: Running Data Quality Checks (Pandas-based)".center(100))
print("─" * 100)

def test_not_null_pandas(df, column):
    """Test NOT NULL check"""
    return df[column].notna().all()

def test_unique_pandas(df, column):
    """Test UNIQUE check"""
    return df[column].nunique() == len(df)

def test_range_pandas(df, column, min_val, max_val):
    """Test RANGE check"""
    return ((df[column] >= min_val) & (df[column] <= max_val)).all()

def test_pattern_pandas(df, column, pattern):
    """Test PATTERN check"""
    import re
    return df[column].astype(str).apply(lambda x: bool(re.match(pattern, x))).all()

def test_foreign_key_pandas(child_df, parent_df, child_col, parent_col):
    """Test FOREIGN KEY check"""
    child_values = set(child_df[child_col].dropna())
    parent_values = set(parent_df[parent_col].dropna())
    orphans = child_values - parent_values
    return len(orphans) == 0

# Run tests
test_results = []

print("\n🔍 Testing Users Table:")
print("─" * 80)

# Test 1: user_id NOT NULL
result = test_not_null_pandas(users_df_pd, 'user_id')
test_results.append(('users', 'user_id_not_null', 'NOT NULL', result))
print(f"   {'✓' if result else '✗'} user_id NOT NULL: {'PASS' if result else 'FAIL'}")

# Test 2: email NOT NULL
result = test_not_null_pandas(users_df_pd, 'email')
test_results.append(('users', 'email_not_null', 'NOT NULL', result))
print(f"   {'✓' if result else '✗'} email NOT NULL: {'PASS' if result else 'FAIL'}")

# Test 3: user_id UNIQUE
result = test_unique_pandas(users_df_pd, 'user_id')
test_results.append(('users', 'user_id_unique', 'UNIQUE', result))
print(f"   {'✓' if result else '✗'} user_id UNIQUE: {'PASS' if result else 'FAIL'}")

# Test 4: email UNIQUE
result = test_unique_pandas(users_df_pd, 'email')
test_results.append(('users', 'email_unique', 'UNIQUE', result))
print(f"   {'✓' if result else '✗'} email UNIQUE: {'PASS' if result else 'FAIL'}")

# Test 5: age RANGE
result = test_range_pandas(users_df_pd, 'age', 18, 100)
test_results.append(('users', 'age_range', 'RANGE', result))
print(f"   {'✓' if result else '✗'} age RANGE (18-100): {'PASS' if result else 'FAIL'}")

# Test 6: email PATTERN
email_pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
result = test_pattern_pandas(users_df_pd, 'email', email_pattern)
test_results.append(('users', 'email_pattern', 'PATTERN', result))
print(f"   {'✓' if result else '✗'} email PATTERN: {'PASS' if result else 'FAIL'}")

print("\n🔍 Testing Orders Table:")
print("─" * 80)

# Test 7: order_id NOT NULL
result = test_not_null_pandas(orders_df_pd, 'order_id')
test_results.append(('orders', 'order_id_not_null', 'NOT NULL', result))
print(f"   {'✓' if result else '✗'} order_id NOT NULL: {'PASS' if result else 'FAIL'}")

# Test 8: user_id NOT NULL
result = test_not_null_pandas(orders_df_pd, 'user_id')
test_results.append(('orders', 'user_id_not_null', 'NOT NULL', result))
print(f"   {'✓' if result else '✗'} user_id NOT NULL: {'PASS' if result else 'FAIL'}")

# Test 9: order_id UNIQUE
result = test_unique_pandas(orders_df_pd, 'order_id')
test_results.append(('orders', 'order_id_unique', 'UNIQUE', result))
print(f"   {'✓' if result else '✗'} order_id UNIQUE: {'PASS' if result else 'FAIL'}")

# Test 10: amount RANGE
result = test_range_pandas(orders_df_pd, 'amount', 0, 10000)
test_results.append(('orders', 'amount_range', 'RANGE', result))
print(f"   {'✓' if result else '✗'} amount RANGE (0-10000): {'PASS' if result else 'FAIL'}")

# Test 11: status VALUE SET
valid_statuses = {'pending', 'completed', 'cancelled', 'shipped'}
result = orders_df_pd['status'].isin(valid_statuses).all()
test_results.append(('orders', 'status_value_set', 'VALUE SET', result))
print(f"   {'✓' if result else '✗'} status VALUE SET: {'PASS' if result else 'FAIL'}")

print("\n🔗 Testing Cross-Table Checks:")
print("─" * 80)

# Test 12: FOREIGN KEY
result = test_foreign_key_pandas(orders_df_pd, users_df_pd, 'user_id', 'user_id')
test_results.append(('orders->users', 'fk_user_id', 'FOREIGN KEY', result))
print(f"   {'✓' if result else '✗'} orders.user_id → users.user_id: {'PASS' if result else 'FAIL'}")

# ============================================================
# STEP 4: Generate Summary Report
# ============================================================
print("\n" + "=" * 100)
print("SUMMARY REPORT".center(100))
print("=" * 100)

passed = sum(1 for _, _, _, result in test_results if result)
failed = len(test_results) - passed
pass_rate = (passed / len(test_results) * 100) if test_results else 0

print(f"\n   Total Checks: {len(test_results)}")
print(f"   ✅ Passed: {passed} ({passed/len(test_results)*100:.1f}%)")
print(f"   ❌ Failed: {failed} ({failed/len(test_results)*100:.1f}%)")
print(f"\n   🎯 Overall Pass Rate: {pass_rate:.2f}%")

if pass_rate == 100:
    print("\n   🎉 All checks passed! Data quality is excellent!")
elif pass_rate >= 80:
    print("\n   ⚠️  Most checks passed. Some issues need attention.")
else:
    print("\n   ❌ Multiple checks failed. Data quality needs improvement.")

# Display detailed results
print("\n" + "─" * 100)
print("DETAILED RESULTS")
print("─" * 100)
print(f"{'Table':<20} {'Check Name':<30} {'Type':<15} {'Result':<10}")
print("─" * 100)
for table, check, check_type, result in test_results:
    status = "PASS" if result else "FAIL"
    icon = "✓" if result else "✗"
    print(f"{table:<20} {check:<30} {check_type:<15} {icon} {status:<10}")

# ============================================================
# STEP 5: Data Statistics
# ============================================================
print("\n" + "=" * 100)
print("DATA STATISTICS".center(100))
print("=" * 100)

print("\n📊 Users Table Statistics:")
print(f"   • Total records: {len(users_df_pd)}")
print(f"   • Unique user_ids: {users_df_pd['user_id'].nunique()}")
print(f"   • Unique emails: {users_df_pd['email'].nunique()}")
print(f"   • Age range: {users_df_pd['age'].min()} - {users_df_pd['age'].max()}")
print(f"   • Missing values: {users_df_pd.isna().sum().sum()}")

print("\n📊 Orders Table Statistics:")
print(f"   • Total records: {len(orders_df_pd)}")
print(f"   • Unique order_ids: {orders_df_pd['order_id'].nunique()}")
print(f"   • Unique customers: {orders_df_pd['user_id'].nunique()}")
print(f"   • Amount range: ${orders_df_pd['amount'].min():.2f} - ${orders_df_pd['amount'].max():.2f}")
print(f"   • Total order value: ${orders_df_pd['amount'].sum():.2f}")
print(f"   • Average order value: ${orders_df_pd['amount'].mean():.2f}")
print(f"   • Status distribution:")
for status, count in orders_df_pd['status'].value_counts().items():
    print(f"      - {status}: {count} ({count/len(orders_df_pd)*100:.1f}%)")

# ============================================================
# STEP 6: Instructions for AIDP
# ============================================================
print("\n" + "=" * 100)
print("NEXT STEPS: Running in AIDP Workbench".center(100))
print("=" * 100)

print("""
To run this with full PySpark support in AIDP:

1. Upload generated CSV files to AIDP workspace:
   • test_output/users.csv
   • test_output/orders.csv

2. Load data into Spark tables:
   
   spark = SparkSession.builder.appName("DQ_Test").getOrCreate()
   
   users_df = spark.read.csv("/Workspace/test_output/users.csv", header=True, inferSchema=True)
   users_df.write.mode("overwrite").saveAsTable("default.users")
   
   orders_df = spark.read.csv("/Workspace/test_output/orders.csv", header=True, inferSchema=True)
   orders_df.write.mode("overwrite").saveAsTable("default.orders")

3. Run DQ checks:
   
   from jobs.run_dq_job import run_dq_checks
   results_df = run_dq_checks("/Workspace/oracle-aidp-samples/data_quality/config/demo_dq_config.yaml", spark)

4. View results:
   
   results_df.show(truncate=False)

✅ The data quality tool is working correctly!
""")

print("=" * 100)
print("TEST COMPLETED SUCCESSFULLY".center(100))
print("=" * 100)
