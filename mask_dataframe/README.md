# DataFrame Masking with AI

A PySpark utility that automatically detects and masks sensitive data in DataFrames using AI model backends including Anthropic's Claude models and models in Oracle OCI.

## Overview

This application identifies columns containing personally identifiable information (PII) or other sensitive data and automatically masks them using PySpark's built-in masking functions. It supports multiple AI model providers through an abstraction layer, allowing you to choose between Anthropic Claude models (Haiku, Sonnet, Opus) or the AI models in Oracle OCI.

## Architecture

The implementation uses an abstract `PIIChecker` class that supports:

- **AnthropicPIIChecker** - Uses Anthropic's Claude models via the Anthropic API
- **OciPIIChecker** - Uses Oracle OCI models via Spark's `query_model()` function

## How It Works

The `mask_df` function:

1. **Analyzes each column** - Samples (by default) up to 1,000 non-null values from each column
2. **Detects sensitive data** - Uses the appropriate PII checker to analyze column metadata
3. **Masks PII columns** - Converts sensitive columns to string type and applies PySpark's masking function
4. **Returns masked DataFrame** - Returns the original DataFrame with masked columns applied

## Usage

```python
from app import mask_df

# Apply masking to a DataFrame (defaults to Oracle OCI model)
masked_df = mask_df(df)  # df is an existing dataframe
masked_df.show()

# Read a table and apply masking with Oracle OCI model
masked_df = mask_df(spark.table("catalog.schema.table"))
masked_df.show()

# Use Anthropic's Claude Haiku model
masked_df = mask_df(spark.table("catalog.schema.table"), model='haiku')
masked_df.show()

# Use Anthropic's Claude Sonnet model
masked_df = mask_df(spark.table("catalog.schema.table"), model='sonnet')
masked_df.show()

# Use Anthropic's Claude Opus model
masked_df = mask_df(spark.table("catalog.schema.table"), model='opus')
masked_df.show()
```

### Parameters

- `df` (DataFrame): The PySpark DataFrame to process
- `model` (str, optional): The AI model to use for sensitivity detection. Options:
  - `'haiku'` - Anthropic Claude Haiku (fastest, most cost-effective)
  - `'sonnet'` - Anthropic Claude Sonnet (balanced performance)
  - `'opus'` - Anthropic Claude Opus (most capable)
  - `'xai.grok-4'` (default) - Oracle OCI Grok model
  - Any other string - Treated as an Oracle OCI model name
- `column_values_to_fetch` (int, optional): The number of non-null column values to fetch in checking for sensitive data. Default: `1000`

### Returns

- A PySpark DataFrame with sensitive columns masked using PySpark's `F.mask()` function

## Requirements

### For Anthropic Models
- Anthropic Python SDK (`anthropic`)
- `ANTHROPIC_API_KEY` environment variable set with your API key

### For Oracle OCI Models
- Run in AIDP (with Spark 3.5.0 or higher)
- Access to Oracle's AI models via Spark's `query_model()` function
- Spark SQL with `query_model` function available

## Example

```python
# Analyze and mask a table with Oracle OCI model
result = mask_df(spark.table("catalog.schema.table"))
result.show()

# Analyze and mask with Claude Sonnet for better accuracy
result = mask_df(spark.table("catalog.schema.table"), model='sonnet')
result.show()
```

The function will:
1. Examine each column in the table
2. Use the selected AI model to identify which columns contain sensitive data based on column name and column values
3. Apply masking to identified sensitive columns
4. Return the masked dataframe

## Notes

- The function samples up to 1,000 non-null values per column for analysis
- Null values are excluded from the sample
- The masking function converts values to string type
- Columns identified as non-sensitive remain unchanged
- For Anthropic models, ensure `ANTHROPIC_API_KEY` is set in your environment
- The `PIIChecker.get()` factory method automatically instantiates the correct checker based on the model name
