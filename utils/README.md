# Merge Generator Utility

A PySpark utility for performing partition-aware merge operations with configurable update behaviors. This tool simplifies the process of merging incremental data into base DataFrames, similar to Delta Lake MERGE operations.

## Overview

The `merge_generator.py` module provides the `get_merged_df()` function, which performs intelligent merges by:
- Updating existing records based on primary keys
- Inserting new records
- Supporting partition-aware filtering for performance optimization
- Handling column-level update policies
- Supporting soft deletes via deletion conditions
- Managing schema evolution with extra columns

## Installation

Install the required dependencies:

```bash
pip install -r requirements.txt
```

## Quick Start

```python
from pyspark.sql import SparkSession
from utils.merge_generator import get_merged_df

# Initialize Spark
spark = SparkSession.builder.appName("merge_example").getOrCreate()

# Create base DataFrame
base_data = [
    ("Alice", 30, 20, "North"),
    ("Bob", 25, 20, "South"),
    ("Charlie", 30, 10, "West")
]
base_df = spark.createDataFrame(base_data, ["name", "department_id", "category_id", "region"])

# Create incremental DataFrame with updates and new records
incremental_data = [
    ("Alice", 35, 20, "North"),  # Update Alice's department_id
    ("David", 28, 10, "West")    # Insert new record
]
incremental_df = spark.createDataFrame(incremental_data, ["name", "department_id", "category_id", "region"])

# Perform merge
merged_df = get_merged_df(
    base_df=base_df,
    incremental_df=incremental_df,
    primary_keys=["name", "region"],
    partition_columns=["category_id", "region"]
)

merged_df.show()
```

## Function Signature

```python
def get_merged_df(
    base_df: DataFrame,
    incremental_df: DataFrame,
    primary_keys: List[str],
    partition_columns: Optional[List[str]] = None,
    carry_base_values_when_column_missing_list: Optional[List[str]] = None,
    column_update_allow_list: Optional[List[str]] = None,
    column_update_deny_list: Optional[List[str]] = None,
    allow_extra_columns: bool = True,
    extra_columns_deny_list: Optional[List[str]] = None,
    deletion_condition: str = "false",
    set_dynamic_partition_mode: bool = True
) -> DataFrame
```

### Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `base_df` | DataFrame | Required | Base DataFrame containing existing data |
| `incremental_df` | DataFrame | Required | Incremental DataFrame with updates and new records |
| `primary_keys` | List[str] | Required | Column names that uniquely identify records |
| `partition_columns` | List[str] | `None` | Partition columns for optimized filtering |
| `carry_base_values_when_column_missing_list` | List[str] | `None` | Columns to preserve from base when missing in incremental |
| `column_update_allow_list` | List[str] | `["*"]` | Columns allowed to be updated (["*"] = all) |
| `column_update_deny_list` | List[str] | `None` | Columns that should never be updated |
| `allow_extra_columns` | bool | `True` | Whether to include new columns from incremental data |
| `extra_columns_deny_list` | List[str] | `None` | New columns to exclude from the merge |
| `deletion_condition` | str | `"false"` | SQL expression for filtering out deleted records |
| `set_dynamic_partition_mode` | bool | `True` | Whether to set Spark dynamic partition mode |

## Usage Examples

### 1. Basic Merge (Updates and Inserts)

```python
merged_df = get_merged_df(
    base_df=base_df,
    incremental_df=incremental_df,
    primary_keys=["name", "region"],
    partition_columns=["category_id", "region"]
)
```

**Equivalent Delta Lake SQL:**
```sql
MERGE INTO base_table AS base
USING incremental_table AS incremental
ON base.name = incremental.name AND base.region = incremental.region
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
```

### 2. Merge with Soft Deletes

```python
# Incremental data with status column indicating deletes
merged_df = get_merged_df(
    base_df=base_df,
    incremental_df=incremental_df,
    primary_keys=["name", "region"],
    partition_columns=["category_id", "region"],
    deletion_condition="incremental.status = 'delete'"
)
```

**Equivalent Delta Lake SQL:**
```sql
MERGE INTO base_table AS base
USING incremental_table AS incremental
ON base.name = incremental.name AND base.region = incremental.region
WHEN MATCHED AND incremental.status = 'delete' THEN DELETE
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
```

### 3. Selective Column Updates

Only update specific columns (e.g., only department_id):

```python
merged_df = get_merged_df(
    base_df=base_df,
    incremental_df=incremental_df,
    primary_keys=["name", "region"],
    partition_columns=["region"],
    column_update_allow_list=["department_id"]
)
```

**Equivalent Delta Lake SQL:**
```sql
MERGE INTO base_table AS base
USING incremental_table AS incremental
ON base.name = incremental.name AND base.region = incremental.region
WHEN MATCHED THEN UPDATE SET base.department_id = incremental.department_id
WHEN NOT MATCHED THEN INSERT *
```

### 4. Prevent Specific Column Updates

Update all columns except specific ones (e.g., preserve department_id):

```python
merged_df = get_merged_df(
    base_df=base_df,
    incremental_df=incremental_df,
    primary_keys=["name", "region"],
    partition_columns=["category_id", "region"],
    column_update_deny_list=["department_id"]
)
```

**Equivalent Delta Lake SQL:**
```sql
MERGE INTO base_table AS base
USING incremental_table AS incremental
ON base.name = incremental.name AND base.region = incremental.region
WHEN MATCHED THEN UPDATE SET
    base.name = incremental.name,
    base.category_id = incremental.category_id,
    base.region = incremental.region
    -- department_id is NOT updated
WHEN NOT MATCHED THEN INSERT *
```

### 5. Handling Missing Columns

When incremental data is missing columns, carry forward base values:

```python
# Incremental data missing 'department_id' column
merged_df = get_merged_df(
    base_df=base_df,
    incremental_df=incremental_df,
    primary_keys=["name"],
    partition_columns=["category_id", "region"],
    carry_base_values_when_column_missing_list=["department_id"]
)
```

**Equivalent Delta Lake SQL:**
```sql
MERGE INTO base_table AS base
USING incremental_table AS incremental
ON base.name = incremental.name
WHEN MATCHED THEN UPDATE SET
    base.category_id = incremental.category_id,
    base.region = incremental.region,
    base.department_id = base.department_id  -- Preserve original value
WHEN NOT MATCHED THEN INSERT (name, category_id, region, department_id)
    VALUES (incremental.name, incremental.category_id, incremental.region, NULL)
```

### 6. Schema Evolution (Extra Columns)

Allow or deny new columns from incremental data:

```python
# Allow extra columns
merged_df = get_merged_df(
    base_df=base_df,
    incremental_df=incremental_df,  # Has additional 'status' column
    primary_keys=["name", "region"],
    allow_extra_columns=True
)

# Deny specific extra columns
merged_df = get_merged_df(
    base_df=base_df,
    incremental_df=incremental_df,
    primary_keys=["name", "region"],
    extra_columns_deny_list=["status", "temp_field"]
)
```

**Equivalent Delta Lake SQL:**
```sql
-- For allowing new columns, you need to add them to the target table first
ALTER TABLE base_table ADD COLUMNS (status STRING);

MERGE INTO base_table AS base
USING incremental_table AS incremental
ON base.name = incremental.name AND base.region = incremental.region
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
```

### 7. Merge Without Partitions

For smaller datasets or when partition pruning is not needed:

```python
merged_df = get_merged_df(
    base_df=base_df,
    incremental_df=incremental_df,
    primary_keys=["name", "region"]
    # No partition_columns specified
)
```

**Equivalent Delta Lake SQL:**
```sql
MERGE INTO base_table AS base
USING incremental_table AS incremental
ON base.name = incremental.name AND base.region = incremental.region
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
```

## Performance Optimization

### Partition Filtering

The utility performs **partition-aware filtering** to optimize performance:

1. Extracts distinct partition values from incremental data
2. Filters base DataFrame to only include matching partitions
3. Performs merge only on relevant data

This is particularly useful for large datasets where incremental changes affect only a subset of partitions.

```python
# Only processes partitions [category_id=10, region='West'] and [category_id=20, region='North']
merged_df = get_merged_df(
    base_df=large_base_df,
    incremental_df=small_incremental_df,
    primary_keys=["name", "region"],
    partition_columns=["category_id", "region"]
)
```

### Dynamic Partition Mode

When `set_dynamic_partition_mode=True` (default), the function automatically sets:
```python
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "DYNAMIC")
```

This ensures only affected partitions are overwritten when writing to partitioned tables.

## Writing Merged Results to Delta Lake

```python
# Perform merge
merged_df = get_merged_df(
    base_df=base_df,
    incremental_df=incremental_df,
    primary_keys=["name", "region"],
    partition_columns=["category_id", "region"]
)

# Write back to Delta Lake with partition overwrite
merged_df.write \
    .format("delta") \
    .mode("overwrite") \
    .partitionBy("category_id", "region") \
    .save("/path/to/delta/table")
```

## Running Tests

### Prerequisites

Ensure all dependencies are installed:
```bash
pip install -r utils/requirements.txt
```

### Test Configuration

The test suite includes a `conftest.py` file that automatically configures the PyArrow timezone environment variable to suppress warnings. No additional configuration is needed.

### Run All Tests

```bash
# From project root
pytest utils/merge_generator_tests.py -v
```

Expected output:
```
============================= ... passed in ... ==============================
```

### Run Specific Test Categories

```bash
# Run only validation tests
pytest utils/merge_generator_tests.py -v -k "validation"

# Run only functionality tests
pytest utils/merge_generator_tests.py -v -k "merge"
```

### Run with Coverage

```bash
pip install pytest-cov
pytest utils/merge_generator_tests.py --cov=utils.merge_generator --cov-report=html
```

### Test Structure

The test suite includes:

**Functionality Tests (9 tests):**
- Updates and inserts
- Deletions with additional columns
- Column denylists
- Missing columns handling
- Selective updates
- Partition filtering

**Validation Tests (7 tests):**
- Empty/None primary keys
- Invalid primary keys
- Invalid partition columns
- Multiple validation errors

## Common Use Cases

### ETL Pipeline: Daily Incremental Updates

```python
# Load previous day's snapshot
base_df = spark.read.format("delta").load("/data/customer_table")

# Load today's changes from staging
incremental_df = spark.read.format("delta").load("/data/staging/customer_updates")

# Merge updates
merged_df = get_merged_df(
    base_df=base_df,
    incremental_df=incremental_df,
    primary_keys=["customer_id"],
    partition_columns=["region", "date"],
    deletion_condition="incremental.operation = 'DELETE'"
)

# Write back
merged_df.write.format("delta").mode("overwrite") \
    .partitionBy("region", "date") \
    .save("/data/customer_table")
```

### CDC (Change Data Capture) Processing

```python
# Process CDC feed with insert/update/delete operations
merged_df = get_merged_df(
    base_df=base_df,
    incremental_df=cdc_df,
    primary_keys=["id"],
    partition_columns=["partition_key"],
    deletion_condition="incremental.cdc_operation = 'D'"
)
```

### Slowly Changing Dimension (Type 1)

```python
# SCD Type 1: Overwrite dimension attributes
merged_df = get_merged_df(
    base_df=dimension_df,
    incremental_df=updates_df,
    primary_keys=["dimension_key"],
    column_update_allow_list=["*"]  # Update all attributes
)
```

## Error Handling

The function performs comprehensive input validation:

```python
from utils.merge_generator import get_merged_df

try:
    merged_df = get_merged_df(
        base_df=base_df,
        incremental_df=incremental_df,
        primary_keys=["invalid_key"],  # Key doesn't exist
        partition_columns=["category_id"]
    )
except ValueError as e:
    print(f"Validation error: {e}")
    # Output: Primary key 'invalid_key' not found in base_df columns
```

## Best Practices

1. **Always specify primary keys**: Ensure primary keys uniquely identify records
2. **Use partition columns**: For large datasets, specify partition columns for better performance
3. **Test with small datasets**: Validate merge logic on sample data before processing full datasets
4. **Monitor partition count**: Too many partitions can cause performance issues
5. **Handle schema evolution carefully**: Plan for extra columns in incremental data
6. **Use deletion conditions wisely**: Ensure deletion logic is accurate to avoid data loss

## Troubleshooting

### Issue: PyArrow timezone warning
**Warning message:**
```
UserWarning: 'PYARROW_IGNORE_TIMEZONE' environment variable was not set
```

**Solution:** Set the environment variable before importing PySpark:
```python
import os
os.environ['PYARROW_IGNORE_TIMEZONE'] = '1'
```

For tests, this is automatically handled by `conftest.py`.

### Issue: Merge is slow
- **Solution**: Ensure `partition_columns` are specified and match your data partitioning strategy
- Check if partition columns have high cardinality

### Issue: Unexpected NULL values
- **Solution**: Use `carry_base_values_when_column_missing_list` to preserve values when columns are missing

### Issue: Columns not updating
- **Solution**: Check `column_update_allow_list` and `column_update_deny_list` configurations

### Issue: Extra columns appearing
- **Solution**: Set `allow_extra_columns=False` or use `extra_columns_deny_list` to filter specific columns

## Contributing

When contributing to this utility:
1. Add tests for new functionality
2. Update this README with examples
3. Ensure all tests pass: `pytest utils/merge_generator_tests.py -v`
4. Follow the existing code style and type hints

## License

This utility is part of the Oracle AIDP Samples repository.
