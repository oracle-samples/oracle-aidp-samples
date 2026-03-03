# Data Quality Tool for Oracle AIDP

A comprehensive, scalable data quality framework built for Oracle AI Data Platform (AIDP) using PySpark. This tool provides extensive data quality checks including table-level validation, cross-table referential integrity, and AI-powered semantic validation.

## 🚀 Features

### Table-Level Checks
- ✅ **NOT NULL**: Validate required fields
- ✅ **UNIQUE**: Ensure uniqueness constraints
- ✅ **RANGE**: Validate numeric value ranges
- ✅ **PATTERN**: Regex pattern matching
- ✅ **DUPLICATE**: Detect duplicate rows
- ✅ **COMPLETENESS**: Data completeness percentage validation
- ✅ **TYPE CHECK**: Validate data types
- ✅ **VALUE SET**: Check against allowed values
- ✅ **STRING LENGTH**: Validate string length constraints
- ✅ **SQL RULES**: Custom SQL-based validation

### Cross-Table Checks
- 🔗 **FOREIGN KEY**: Validate referential integrity
- 🔗 **CROSS JOIN QUALITY**: Data quality across joined tables
- 🔗 **ORPHAN DETECTION**: Find orphaned records

### Advanced Features
- 🤖 **AI-POWERED VALIDATION**: Semantic validation using OCI GenAI models
- 📊 **SCHEMA DRIFT DETECTION**: Detect schema changes
- 📈 **VOLUME CHECKS**: Detect data volume anomalies
- 📋 **COMPREHENSIVE REPORTING**: Formatted console output and DataFrame results
- ⚡ **SCALABLE**: Built on PySpark for large datasets

## 📁 Project Structure

```
data_quality/
├── dq/                              # Core DQ modules
│   ├── config_loader.py             # Configuration loader
│   ├── orchestrator.py              # Main orchestration logic
│   ├── native_rules.py              # Native Spark-based rules
│   ├── cross_table_rules.py         # Cross-table validation
│   ├── llm_rules.py                 # AI-powered validation
│   ├── sql_rules.py                 # SQL-based rules
│   ├── schema_checks.py             # Schema validation
│   └── volume_checks.py             # Volume anomaly detection
├── jobs/
│   └── run_dq_job.py                # Main execution script
├── config/
│   ├── dq_rules.yaml                # Production configuration
│   └── demo_dq_config.yaml          # Demo configuration
├── data_quality_example.ipynb       # Complete tutorial notebook
└── README.md                        # This file
```

## 🔧 Installation

### Prerequisites
- Oracle AI Data Platform (AIDP) access
- PySpark environment
- Python 3.8+

### Dependencies
```bash
pip install pyyaml
```

## 🎯 Quick Start

### 1. Generate Sample Data

```python
from data_generator import MultiTableDataGenerator

gen = MultiTableDataGenerator(seed=42)
config = {
    'output_path': '/Workspace/dq_demo_data',
    'output_format': 'csv',
    'tables': [
        {
            'table_name': 'users',
            'rows_count': 100,
            'columns': [
                {'name': 'user_id', 'type': 'integer', 'range': [1, 100], 'unique': True},
                {'name': 'email', 'type': 'email', 'unique': True},
                {'name': 'age', 'type': 'integer', 'range': [18, 80]}
            ]
        }
    ]
}
results = gen.generate_from_config(config)
```

### 2. Load Data to Spark Tables

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("DQ_Demo").getOrCreate()

# Load and create tables
users_df = spark.read.csv("/Workspace/dq_demo_data/users.csv", header=True, inferSchema=True)
users_df.write.mode("overwrite").saveAsTable("default.users")
```

### 3. Run Data Quality Checks

```python
from jobs.run_dq_job import run_dq_checks

config_path = "/Workspace/oracle-aidp-samples/data_quality/config/demo_dq_config.yaml"
results_df = run_dq_checks(config_path, spark)
```

## 📝 Configuration Format

Create a YAML configuration file with your data quality rules:

```yaml
tables:
  users:
    table_name: default.users
    
    schema:
      expected:
        user_id: bigint
        email: string
        age: bigint
    
    volume:
      warn_pct: 20
      fail_pct: 40
    
    rules:
      # NOT NULL check
      - name: user_id_not_null
        type: not_null
        column: user_id
        severity: CRITICAL
        weight: 10
      
      # UNIQUE check
      - name: email_unique
        type: unique
        column: email
        severity: HIGH
        weight: 8
      
      # RANGE check
      - name: age_range
        type: range
        column: age
        min: 18
        max: 100
        severity: HIGH
        weight: 5
      
      # PATTERN check
      - name: email_pattern
        type: pattern
        column: email
        pattern: '^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$'
        severity: HIGH
        weight: 7
      
      # AI-powered semantic check
      - name: email_semantic
        type: llm_semantic
        column: email
        description: "valid professional email addresses"
        model: meta.llama-3.1-70b-instruct
        severity: MEDIUM
        weight: 5

  orders:
    table_name: default.orders
    rules:
      - name: order_id_not_null
        type: not_null
        column: order_id
        severity: CRITICAL
        weight: 10

# Cross-table validation
cross_table_rules:
  - name: orders_user_fk
    type: foreign_key
    severity: CRITICAL
    weight: 10
    source:
      table: orders
      column: user_id
    target:
      table: users
      column: user_id
    allow_nulls: false
```

## 📊 Check Types Reference

### 1. NOT NULL
Validates that a column contains no NULL values.

```yaml
- name: column_not_null
  type: not_null
  column: column_name
  severity: CRITICAL
  weight: 10
```

### 2. UNIQUE
Validates that all values in a column are unique.

```yaml
- name: column_unique
  type: unique
  column: column_name
  severity: HIGH
  weight: 8
```

### 3. RANGE
Validates that numeric values fall within specified range.

```yaml
- name: value_range
  type: range
  column: amount
  min: 0
  max: 10000
  severity: HIGH
  weight: 7
```

### 4. PATTERN
Validates values against a regex pattern.

```yaml
- name: email_format
  type: pattern
  column: email
  pattern: '^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$'
  severity: HIGH
  weight: 7
```

### 5. COMPLETENESS
Validates data completeness percentage.

```yaml
- name: field_completeness
  type: completeness
  column: name
  min_completeness_pct: 95
  severity: MEDIUM
  weight: 5
```

### 6. VALUE SET
Validates values against allowed set.

```yaml
- name: status_values
  type: value_set
  column: status
  allowed_values: ['active', 'inactive', 'pending']
  severity: HIGH
  weight: 7
```

### 7. STRING LENGTH
Validates string length constraints.

```yaml
- name: name_length
  type: string_length
  column: name
  min_length: 2
  max_length: 100
  severity: LOW
  weight: 3
```

### 8. DUPLICATE
Checks for duplicate rows based on columns.

```yaml
- name: no_duplicates
  type: duplicate
  column: id
  max_duplicates: 0
  severity: CRITICAL
  weight: 10
```

### 9. FOREIGN KEY
Validates referential integrity between tables.

```yaml
- name: fk_check
  type: foreign_key
  severity: CRITICAL
  weight: 10
  source:
    table: orders
    column: user_id
  target:
    table: users
    column: user_id
  allow_nulls: false
```

### 10. LLM SEMANTIC
AI-powered semantic validation using OCI GenAI.

```yaml
- name: ai_validation
  type: llm_semantic
  column: description
  description: "meaningful product descriptions"
  model: meta.llama-3.1-70b-instruct
  sample_size: 50
  severity: MEDIUM
  weight: 5
```

## 🔍 Output Format

### Console Output

```
====================================================================================================
                               🔍 DATA QUALITY ENGINE - AIDP                                
====================================================================================================

────────────────────────────────────────────────────────────────────────────────────────────────────
  📋 Loading Configuration
────────────────────────────────────────────────────────────────────────────────────────────────────
   • Tables to validate: 2
   • Cross-table rules: 1

====================================================================================================
                            📊 TABLE-LEVEL DATA QUALITY CHECKS                            
====================================================================================================

────────────────────────────────────────────────────────────────────────────────────────────────────
  Table: users
────────────────────────────────────────────────────────────────────────────────────────────────────
   Loading: default.users
   ✓ Loaded 100 rows
   ✓ Schema validation passed
   Running 8 quality rules...

   Results: ✓ 8 passed | ✗ 0 failed | ⚠️  0 errors
   📈 DQ Score: 100% 🎉

====================================================================================================
                               📋 SUMMARY REPORT                                
====================================================================================================

   Total Checks Executed: 17
   ✓ Passed: 17 (100.0%)
   ✗ Failed: 0 (0.0%)
   ⚠️  Errors: 0 (0.0%)

   🎯 Overall Pass Rate: 100.00%

   Table Scores:
      🎉 users: 100%
      🎉 orders: 100%
```

### DataFrame Results

Results are returned as a Spark DataFrame with columns:
- `scope`: TABLE or CROSS_TABLE
- `table`: Table name
- `rule`: Rule name
- `status`: PASS, FAIL, or ERROR
- `severity`: CRITICAL, HIGH, MEDIUM, or LOW
- `weight`: Weight for scoring

## 🎓 Tutorial Notebook

See `data_quality_example.ipynb` for a complete, step-by-step tutorial covering:
1. Data generation
2. Loading data to Spark
3. Running all check types
4. Analyzing results
5. Individual rule testing
6. AI-powered validation
7. Custom configuration

## 🔧 Advanced Usage

### Run as a Job

```python
from jobs.run_dq_job import run_dq_checks
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("DQ_Job").getOrCreate()
config_path = "/path/to/config.yaml"

results_df = run_dq_checks(config_path, spark)

# Save results
results_df.write.mode("overwrite").saveAsTable("dq_results")
```

### Programmatic Rule Testing

```python
from dq.native_rules import run_not_null, run_unique, run_range
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
df = spark.table("my_table")

# Test individual rules
if run_not_null(df, {"column": "id"}):
    print("✓ NOT NULL check passed")

if run_unique(df, {"column": "email"}):
    print("✓ UNIQUE check passed")

if run_range(df, {"column": "amount", "min": 0, "max": 10000}):
    print("✓ RANGE check passed")
```

### Custom SQL Rules

```yaml
rules:
  - name: custom_business_rule
    type: sql
    sql: "SELECT COUNT(*) as cnt FROM ${TABLE} WHERE amount < 0 OR amount > balance"
    severity: HIGH
    weight: 7
```

## 🤖 AI-Powered Validation

The tool integrates with OCI GenAI for semantic validation:

```python
from dq.llm_rules import run_llm_rule

rule = {
    "column": "product_description",
    "description": "meaningful and professional product descriptions",
    "model": "meta.llama-3.1-70b-instruct",
    "sample_size": 50
}

result = run_llm_rule(df, rule, spark)
print(f"AI Validation: {result}")  # PASS or FAIL
```

## 📈 Best Practices

1. **Start with Native Rules**: Use Spark-native rules for performance
2. **Use AI Sparingly**: LLM rules are powerful but costly - use for semantic validation only
3. **Weight Appropriately**: Assign higher weights to critical checks
4. **Monitor Trends**: Track DQ scores over time
5. **Automate**: Schedule DQ checks as part of your data pipeline
6. **Alert on Failures**: Set up alerts for critical check failures
7. **Document Rules**: Maintain clear descriptions in your configuration

## 🔄 Integration with Data Generator

This tool works seamlessly with the data generator:

```python
# Generate test data
from data_generator import MultiTableDataGenerator
gen = MultiTableDataGenerator(seed=42)
results = gen.generate_from_config('config.yaml')

# Load to Spark
spark.read.csv('/output/users.csv', header=True).write.saveAsTable('users')

# Run DQ checks
from jobs.run_dq_job import run_dq_checks
dq_results = run_dq_checks('dq_config.yaml', spark)
```

## 📊 Performance Considerations

- Native rules (not_null, range, etc.) are highly optimized
- Cross-table checks cache DataFrames for efficiency
- LLM rules sample data to reduce costs
- Use appropriate sample sizes for large tables
- Consider partitioning for very large datasets

## 🤝 Contributing

Contributions are welcome! Areas for enhancement:
- Additional check types
- More sophisticated AI validation
- Integration with alerting systems
- Dashboard visualizations
- Historical trend analysis

## 📄 License

See LICENSE.txt in the repository root.

## 🆘 Support

For issues or questions:
1. Check the tutorial notebook
2. Review configuration examples
3. Consult AIDP documentation
4. Raise an issue in the repository

## 🎯 Roadmap

- [ ] Web UI for configuration
- [ ] Real-time monitoring dashboard
- [ ] Automated remediation suggestions
- [ ] Integration with data catalogs
- [ ] Historical trend analysis
- [ ] ML-based anomaly detection

---

**Built for Oracle AI Data Platform (AIDP)**

Leverage the power of PySpark and AI for comprehensive data quality validation at scale.
