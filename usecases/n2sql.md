# Natural Language to SQL Agent (N2SQL)

An intelligent PySpark agent that converts natural language questions into SQL queries, executes them safely, and provides AI-powered analysis of the results.

## Overview

This tool allows non-technical users to query databases using plain English. It leverages Large Language Models (LLMs) to:
1. Generate SQL queries from natural language questions
2. Execute queries safely with built-in security controls
3. Summarize results in business-friendly language

## Features

- **Natural Language Interface**: Ask questions in plain English
- **Automatic SQL Generation**: LLM converts questions to optimized Spark SQL
- **Schema Introspection**: Automatically reads and understands table structure
- **Security First**: Built-in SQL injection protection and query sanitization
- **Smart Summarization**: AI-powered analysis of query results
- **Self-Service Analytics**: Enables business users to query data independently

## Requirements
- Pandas (for result summarization)


## Quick Start

```python
from n2sql_agent import ask_time_table

# Ask a question in natural language
ask_time_table("Show the top 5 Item_Key by Revenue_at_Risk_USD")

# More examples
ask_time_table("What is the average revenue by product category?")
ask_time_table("List customers with revenue greater than 100000")
ask_time_table("Show monthly trends in sales for the last 6 months")
```

## Configuration

### Basic Setup

```python
# Configure your table and security settings
TIME_TABLE = "default.adobe.test_data"  # Your table name
BLOCKLIST = ["INSERT","UPDATE","DELETE","DROP","ALTER","CREATE","TRUNCATE","MERGE"]
```

### Changing the LLM Model

To use a different model, update the `query_model` calls:

```python
expr("query_model('your.model.endpoint', prompt) as sql_text")
```

## Architecture

### Workflow

```
User Question
    ↓
Schema Introspection → LLM SQL Generation
    ↓
SQL Sanitization & Validation
    ↓
Query Execution
    ↓
LLM Result Summarization
    ↓
Business Insights
```

## Core Functions

### `build_schema_text(table_name: str) -> str`

Introspects the table schema and returns a formatted string describing columns and data types.

```python
schema = build_schema_text("default.adobe.test_data")
# Returns: "default.adobe.test_data(Item_Key:string, Revenue_at_Risk_USD:double, ...)"
```

### `llm_generate_sql(user_question: str, schema_text: str) -> str`

Generates a SQL query from a natural language question using an LLM.

**Parameters:**
- `user_question`: Plain English question
- `schema_text`: Table schema information

**Returns:** SQL query string

### `sanitize_sql(sql: str) -> str`

Validates and sanitizes generated SQL to prevent malicious queries.

**Security Checks:**
- Ensures query starts with SELECT
- Blocks DML/DDL operations (INSERT, UPDATE, DELETE, etc.)
- Enforces LIMIT clause (default 100 rows)

**Raises:** `ValueError` if query violates security rules

### `run_sql(sql: str) -> DataFrame`

Executes the sanitized SQL query and displays results.

### `llm_summarize_dataframe(df, max_rows=50) -> str`

Generates an AI-powered summary of query results.

**Parameters:**
- `df`: Spark DataFrame with query results
- `max_rows`: Maximum rows to analyze (default: 50)

**Returns:** Text summary with 5 bullet points

### `ask_time_table(question: str) -> DataFrame`

Main agent function that orchestrates the entire workflow.

**Parameters:**
- `question`: Natural language question

**Returns:** Spark DataFrame with query results

## Example Usage

### Example 1: Top Revenue Items

```python
ask_time_table("Show the top 5 Item_Key by Revenue_at_Risk_USD")
```

**Output:**
```
LLM Raw SQL:
SELECT Item_Key, Revenue_at_Risk_USD FROM default.adobe.test_data 
ORDER BY Revenue_at_Risk_USD DESC LIMIT 5

Running SQL:
[Query results displayed]

LLM Analysis:
• Top 5 items account for $2.3M in revenue at risk
• Item ABC-123 leads with $750K, 32% of total
• Significant drop-off after top 3 items (45% decrease)
• Revenue concentration suggests high dependency on key items
• Consider risk mitigation strategies for top performers
```

### Example 2: Aggregation Query

```python
ask_time_table("What is the average revenue by product category?")
```

### Example 3: Filtering

```python
ask_time_table("Show all items where revenue at risk exceeds $100,000")
```

## Security Features

### SQL Injection Protection

The agent includes multiple layers of security:

1. **Whitelist Approach**: Only SELECT statements allowed
2. **Blocklist Filtering**: Prevents destructive operations
3. **Automatic Limiting**: Enforces row limits to prevent resource exhaustion
4. **Query Validation**: Checks for malicious patterns

### Blocked Operations

The following SQL operations are explicitly blocked:
- `INSERT`, `UPDATE`, `DELETE`
- `DROP`, `ALTER`, `CREATE`
- `TRUNCATE`, `MERGE`

## Best Practices

### For Users

1. **Be Specific**: More detailed questions yield better SQL
   - ❌ "Show me data"
   - ✅ "Show the top 10 customers by total revenue in 2024"

2. **Use Business Terms**: The LLM understands domain language
   - "high-value customers"
   - "recent transactions"
   - "year-over-year growth"

3. **Start Simple**: Test with basic queries before complex analysis

### For Administrators

1. **Validate Schema**: Ensure table schemas are up-to-date
2. **Monitor Usage**: Track LLM costs and query patterns
3. **Update Blocklist**: Add domain-specific blocked terms as needed
4. **Set Appropriate Limits**: Adjust `LIMIT` values based on use case

## Customization

### Modifying SQL Generation Prompt

Edit the prompt in `llm_generate_sql()` to adjust behavior:

```python
prompt = f"""
You are a Spark SQL generator specialized in financial data.

Schema: {schema_text}

Additional Rules:
- Always use UPPER() for text comparisons
- Format currency fields with 2 decimal places
- Include data quality checks in WHERE clauses

User question: {user_question}
"""
```

### Customizing Summarization

Modify the prompt in `llm_summarize_dataframe()` for different output styles:

```python
prompt = f"""
Summarize in 3 bullet points for executive audience.
Focus on: revenue impact, risk factors, and recommendations.

CSV:
{table_text}
"""
```

## Troubleshooting

### Issue: "Only SELECT queries allowed"

**Cause**: LLM generated a non-SELECT query

**Solution**: Rephrase question to focus on data retrieval, not modification

### Issue: "Disallowed token: INSERT"

**Cause**: Generated SQL contains blocked operation

**Solution**: Security feature working correctly - rephrase question

### Issue: Empty or incorrect results

**Possible Causes:**
1. Schema mismatch - verify table name and columns
2. Ambiguous question - add more context
3. Data doesn't match query criteria

**Solutions:**
- Check schema with `build_schema_text()`
- Test with simpler questions first
- Verify data exists in table

### Issue: Summarization timeout

**Cause**: Large result set or slow LLM response

**Solutions:**
- Reduce `max_rows` parameter
- Use more specific queries to reduce result size
- Add time-based filters to questions

## Performance Considerations

- **Query Optimization**: LLM generates reasonably optimized SQL, but complex joins may need review
- **Row Limits**: Default 100-row limit prevents resource exhaustion
- **Summarization Cost**: Only analyzes up to 50 rows to control LLM costs
- **Caching**: Consider caching frequent queries for faster response

## Limitations

1. **Single Table**: Currently supports queries on one table (can be extended)
2. **Read-Only**: No data modification capabilities (by design)
3. **LLM Dependency**: Requires access to OCI AI Models
4. **Query Complexity**: Very complex multi-step analyses may require iteration
5. **Cost**: LLM usage incurs costs based on token consumption

## Extending the Agent

### Multi-Table Support

```python
TABLES = ["default.adobe.test_data", "default.adobe.customer_data"]
SCHEMA_TEXT = "\n".join([build_schema_text(t) for t in TABLES])
```

### Adding Query History

```python
query_history = []

def ask_time_table(question: str):
    # ... existing code ...
    query_history.append({
        "question": question,
        "sql": safe_sql,
        "timestamp": datetime.now()
    })
    # ... rest of function ...
```

### Custom Metrics Tracking

```python
def ask_time_table(question: str):
    start_time = time.time()
    # ... existing code ...
    execution_time = time.time() - start_time
    log_metrics(question, execution_time, df.count())
```

## Use Cases

- **Self-Service BI**: Enable business users to query data independently
- **Ad-Hoc Analysis**: Quick exploration without writing SQL
- **Executive Dashboards**: Natural language interface for leadership
- **Data Discovery**: Help users understand available data
- **Compliance Reporting**: Safe, auditable query generation