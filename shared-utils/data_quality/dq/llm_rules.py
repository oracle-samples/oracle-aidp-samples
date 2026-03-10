"""
llm_rules.py
------------------------------------------------------------
Responsibility:
- Execute semantic data quality checks using OCI GenAI
- Used only when rule logic cannot be expressed deterministically

AIDP Specific:
- Uses query_model() Spark SQL function
- No external SDK or API key required

Best Practices:
- Limit sample size to reduce costs
- Use clear, specific prompts
- Always request structured output (PASS/FAIL)
------------------------------------------------------------
"""

from pyspark.sql import functions as F
import json


def run_llm_rule(df, rule: dict, spark, model: str = None) -> str:
    """
    Execute LLM-based semantic data quality rule.

    Args:
        df: Spark DataFrame
        rule (dict): Rule with column and description
        spark: SparkSession
        model (str): OCI GenAI model name (optional, can be in rule)

    Returns:
        str: PASS or FAIL
    """
    col = rule["column"]
    
    # Get model from rule or use default
    if model is None:
        model = rule.get("model", "meta.llama-3.1-70b-instruct")
    
    # Sample limited values to reduce cost and latency
    sample_size = rule.get("sample_size", 50)
    samples = (
        df.select(col)
        .where(F.col(col).isNotNull())
        .limit(sample_size)
        .rdd.map(lambda r: str(r[0]))
        .collect()
    )
    
    if not samples:
        return "PASS"  # Empty column passes by default

    # Build clear, structured prompt
    prompt = f"""You are a data quality validator. Analyze the following data sample and determine if it passes the quality rule.

Column Name: {col}
Sample Size: {len(samples)}
Sample Values: {samples[:20]}  # Show first 20 for context

Quality Rule: {rule['description']}

Instructions:
1. Examine the sample values carefully
2. Check if they meet the quality rule criteria
3. Respond with ONLY one word: PASS or FAIL

Your response:"""

    try:
        # Execute query_model in AIDP
        result = (
            spark.range(1)
            .withColumn(
                "response",
                F.expr(f"query_model('{model}', '{_escape_prompt(prompt)}')")
            )
            .head()
            .response
        )
        
        # Parse response
        result_clean = result.strip().upper()
        
        # Extract PASS or FAIL from response
        if "PASS" in result_clean and "FAIL" not in result_clean:
            return "PASS"
        elif "FAIL" in result_clean:
            return "FAIL"
        else:
            print(f"⚠️  Ambiguous LLM response: {result}")
            return "FAIL"  # Conservative approach
            
    except Exception as e:
        print(f"❌ LLM rule execution error: {str(e)}")
        return "ERROR"


def run_llm_anomaly_detection(df, rule: dict, spark, model: str = None) -> str:
    """
    Use LLM to detect anomalies in data patterns.

    Args:
        df: Spark DataFrame
        rule (dict): Rule with column and expected pattern description
        spark: SparkSession
        model (str): OCI GenAI model name

    Returns:
        str: PASS or FAIL
    """
    col = rule["column"]
    
    if model is None:
        model = rule.get("model", "meta.llama-3.1-70b-instruct")
    
    # Get statistics
    stats = df.select(col).where(F.col(col).isNotNull()).describe().collect()
    samples = df.select(col).limit(30).rdd.map(lambda r: str(r[0])).collect()
    
    prompt = f"""Analyze this data for anomalies:

Column: {col}
Statistics: {[row.asDict() for row in stats]}
Sample Values: {samples}

Expected Pattern: {rule['description']}

Are there any anomalies or unexpected patterns?
Answer: PASS (no anomalies) or FAIL (anomalies detected)"""

    try:
        result = (
            spark.range(1)
            .withColumn(
                "response",
                F.expr(f"query_model('{model}', '{_escape_prompt(prompt)}')")
            )
            .head()
            .response.strip().upper()
        )
        
        return "PASS" if "PASS" in result else "FAIL"
        
    except Exception as e:
        print(f"❌ Anomaly detection error: {str(e)}")
        return "ERROR"


def run_llm_data_profiling(df, rule: dict, spark, model: str = None) -> dict:
    """
    Use LLM to generate insights about data quality.

    Args:
        df: Spark DataFrame
        rule (dict): Rule configuration
        spark: SparkSession
        model (str): OCI GenAI model name

    Returns:
        dict: Profiling insights
    """
    col = rule["column"]
    
    if model is None:
        model = rule.get("model", "meta.llama-3.1-70b-instruct")
    
    # Get sample and statistics
    samples = df.select(col).limit(50).rdd.map(lambda r: str(r[0])).collect()
    
    prompt = f"""Analyze this data column and provide quality insights:

Column: {col}
Sample Values: {samples}

Provide a brief analysis covering:
1. Data type and format
2. Any quality issues
3. Recommendations

Keep response under 200 words."""

    try:
        result = (
            spark.range(1)
            .withColumn(
                "response",
                F.expr(f"query_model('{model}', '{_escape_prompt(prompt)}')")
            )
            .head()
            .response
        )
        
        return {
            "column": col,
            "insights": result,
            "status": "SUCCESS"
        }
        
    except Exception as e:
        return {
            "column": col,
            "insights": f"Error: {str(e)}",
            "status": "ERROR"
        }


def _escape_prompt(prompt: str) -> str:
    """
    Escape prompt for Spark SQL execution.
    
    Args:
        prompt (str): Raw prompt text
        
    Returns:
        str: Escaped prompt
    """
    # Remove or escape problematic characters
    escaped = (
        prompt.replace("\\", "\\\\")  # Escape backslashes first
              .replace("'", "\\'")     # Escape single quotes
              .replace("\n", " ")      # Replace newlines with spaces
              .replace("\r", " ")      # Replace carriage returns
              .replace("\t", " ")      # Replace tabs
    )
    
    return escaped
