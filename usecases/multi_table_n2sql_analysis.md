# Supplier-Item Group Analysis

A PySpark application that performs automated AI-powered analysis of procurement data by supplier-item pairs using LLM (Grok-4).

## Overview

This script processes procurement data from a data warehouse, groups it by supplier and item, and generates business insights using an AI model. Each supplier-item combination receives a concise analysis covering pricing trends, contract risks, and negotiation recommendations.

## Features

- **Grouped Analysis**: Processes data by supplier-item pairs for focused insights
- **LLM Integration**: Uses Grok-4 AI model via `query_model()` for intelligent analysis
- **Incremental Processing**: Tracks completed analyses to avoid reprocessing
- **Data Capping**: Limits rows per group to manage LLM context size
- **Delta Lake Storage**: Stores results in Delta format for reliability
- **Error Handling**: Continues processing even if individual groups fail

## Prerequisites

- Apache Spark environment with PySpark
- Access to Delta Lake tables
- Grok-4 model access via `query_model()` function
- Required tables:
  - `default.adobe.test_data` (fact table)
  - `default.adobe.dim_supplier` (supplier dimension)
  - `default.adobe.dim_item` (item dimension)
  - `default.adobe.dim_time` (time dimension)

## Configuration

Edit the following parameters at the top of the script:

```python
# Table Configuration
TABLE_FACTS = "default.adobe.test_data"
TABLE_SUPPLIER = "default.adobe.dim_supplier"
TABLE_ITEM = "default.adobe.dim_item"
TABLE_TIME = "default.adobe.dim_time"
OUTPUT_TABLE = "default.adobe.supplier_item_group_analysis"

# Processing Parameters
MAX_ROWS_PER_GROUP = 500      # Maximum rows per supplier-item sent to LLM
SKIP_ALREADY_PROCESSED = True # Skip groups already analyzed
```

## Input Data Schema

The script expects the following key columns in the fact table:

- `supplier_key`, `item_key`, `time_key` (join keys)
- `Current_Contract_Price_USD`
- `Months_to_Contract_Expiry`
- `Current_Avg_Price_USD`
- `Annual_Spend_USD`
- `Best_Historical_Bid_USD`
- `Price_Gap_vs_BestBid`
- `Forecasted_Volume`
- `Revenue_at_Risk_USD`
- `Negotiation_Priority_Score`
- Payment terms and other metrics

## Output Schema

Results are saved to the output table with:

| Column | Type | Description |
|--------|------|-------------|
| Supplier_Key | STRING | Supplier identifier |
| Item_Key | STRING | Item identifier |
| Analysis | STRING | AI-generated analysis (5-7 bullet points) |
| run_timestamp | TIMESTAMP | When analysis was performed |

## Analysis Output

Each supplier-item pair receives a focused analysis covering:

- Price trend and volatility patterns
- Contract expiry risk assessment
- Price gap versus best historical bid
- Forecasted volume and spend implications
- Negotiation priority and actionable recommendations

## Usage

Run the script in your Spark environment:

```bash
spark-submit supplier_item_analysis.py
```

Or in a notebook environment:

```python
%run /path/to/supplier_item_analysis.py
```

## Processing Flow

1. **Initialize**: Creates output table if not exists
2. **Load Data**: Reads fact and dimension tables
3. **Join**: Combines facts with supplier, item, and time dimensions
4. **Group**: Identifies unique supplier-item combinations
5. **Filter**: Skips already-processed pairs (if enabled)
6. **Analyze**: For each group:
   - Fetches up to MAX_ROWS_PER_GROUP rows
   - Converts to CSV format
   - Sends to LLM for analysis
   - Saves results to output table
7. **Report**: Prints summary statistics

## Performance Considerations

- **Group Size**: Adjust `MAX_ROWS_PER_GROUP` based on:
  - LLM context window limits
  - Processing time requirements
  - Data granularity needs

- **Incremental Processing**: Enable `SKIP_ALREADY_PROCESSED` for:
  - Resuming interrupted runs
  - Processing only new supplier-item pairs
  - Avoiding duplicate LLM costs

- **Parallelization**: Current implementation processes groups sequentially. For large datasets, consider:
  - Partitioning groups and running parallel jobs
  - Using Spark's distributed processing capabilities
  - Implementing batch LLM calls if supported

## Error Handling

The script includes error handling for:

- Missing or empty groups (skipped with warning)
- Failed LLM calls (logged and counted)
- Table read errors (graceful degradation)

Failed groups are logged but don't stop overall processing.

## Monitoring

The script provides detailed logging:

```
[1/150] Processing Supplier=S001 Item=I042
  Rows to send: 248 (capped at 500)
  Saved analysis for S=S001 I=I042
```

Final summary includes:
- Total groups processed
- Groups skipped (already done)
- Groups failed (errors)

## Cost Considerations

Each supplier-item group triggers one LLM API call. For large datasets:

- Calculate total unique supplier-item pairs
- Estimate API costs based on your LLM pricing
- Consider filtering to high-priority pairs first
- Use incremental processing to spread costs over time

## Customization

### Modify Analysis Focus

Edit the prompt in `llm_analyze_group()` to change analysis focus:

```python
prompt = f"""
Your custom instructions here...
- Custom metric 1
- Custom metric 2
...
"""
```

### Change LLM Model

Update the model reference in `llm_analyze_group()`:

```python
expr("query_model('your.model.path', prompt) as analysis_text")
```

### Adjust Output Format

Modify the prompt to request different output formats (JSON, paragraphs, tables, etc.)

## Troubleshooting

**Issue**: "Table not found" errors
- Verify table paths match your environment
- Check database/schema permissions

**Issue**: LLM calls timing out
- Reduce `MAX_ROWS_PER_GROUP`
- Check model availability and quotas

**Issue**: Out of memory errors
- Process fewer groups per run
- Reduce `MAX_ROWS_PER_GROUP`
- Increase executor memory

**Issue**: Duplicate analyses
- Ensure `SKIP_ALREADY_PROCESSED = True`
- Verify output table is accessible
- Check for concurrent runs
