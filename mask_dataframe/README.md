# DataFrame Masking with AI

A PySpark utility that automatically detects and masks sensitive data in DataFrames using an AI model (e.g. Grok-4).

## Overview

This application identifies columns containing personally identifiable information (PII) or other sensitive data and automatically masks them using PySpark's built-in masking functions.

## How It Works

The `mask_df` function:

1. **Analyzes each column** - Samples up to 1,000 non-null values from each column
2. **Detects sensitive data** - Sends column metadata to an AI model with a prompt asking if the column contains sensitive values
3. **Masks PII columns** - Converts sensitive columns to string type and applies PySpark's masking function
4. **Returns masked DataFrame** - Returns the original DataFrame with masked columns applied

## Usage

```python
from app import mask_df

# Apply masking to a DataFrame
masked_df = mask_df(df)  # df is an existing dataframe
masked_df.show()

# Read a table and apply maskign
masked_df = mask_df(spark.table("catalog.schema.table"))
masked_df.show()
```

### Parameters

- `df` (DataFrame): The PySpark DataFrame to process
- `model` (str, optional): The AI model to use for sensitivity detection. Default: `'xai.grok-4'`
- `column_values_to_fetch` (int, optional): The number of non-null column values to fetch in checking for sensitive data. Default: `1000`

### Returns

- A PySpark DataFrame with sensitive columns masked using PySpark's `F.mask()` function

## Requirements

- Run in AIDP (with spark 3.5.0 or higher)
- Access to Oracle's AI models (to determine if the column is sensitive)
- Spark SQL with `query_model` function available

## Example

```python
# Analyze and mask a table
result = mask_df(spark.table("catalog.schema.table"))
result.show()
```

The function will:
1. Examine each column in the table
2. Use AI model to identify which columns contain sensitive data based on column name and column values
3. Apply masking to identified sensitive columns
4. Return the masked dataframe

## Notes

- The function samples up to 1,000 non-null values per column for analysis
- Null values are excluded from the sample
- The masking function converts values to string type
- Columns identified as non-sensitive remain unchanged
