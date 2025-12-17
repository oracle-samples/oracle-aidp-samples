# LLM Model Output Parser

A PySpark utility that leverages Large Language Models (LLMs) to automatically parse and summarize statistical model outputs into clear, business-friendly insights.

## Overview

This tool reads raw model training output files (containing coefficients, metrics, statistical tests, etc.) and uses an LLM to extract and summarize the most important findings in an easily digestible format.

## Features

- **Automated Parsing**: Reads raw text output from statistical models
- **Intelligent Summarization**: Uses LLM to identify key metrics and findings
- **Business-Friendly Output**: Converts technical output into clear bullet points
- **PySpark Integration**: Leverages Spark's distributed computing capabilities
- **LLM-Powered**: Uses the `query_model` function with Grok-4 for analysis


## Usage

### Basic Example

```python
from llm_model_parser import llm_summarize_model_output

# Parse and summarize model output
summary = llm_summarize_model_output("/Workspace/output.txt")
print(summary)
```

### Input File Format

The input file should contain raw model output, such as:
- Regression coefficients
- Statistical significance values (p-values)
- Model performance metrics (AUC, accuracy, precision, recall)
- Confusion matrices
- Feature importance scores
- Any other model training diagnostics

### Output Format

The function returns a text summary containing 5-7 bullet points covering:
- **Model Quality**: Overall performance metrics (AUC, accuracy, etc.)
- **Strongest Predictors**: Variables with highest impact
- **Weak Predictors**: Variables with minimal or no significance
- **Interpretation**: Business implications and actionable insights

## Function Reference

### `llm_summarize_model_output(file_path: str) -> str`

Parses raw model output and generates an LLM-powered summary.

**Parameters:**
- `file_path` (str): Path to the raw model output text file

**Returns:**
- `str`: A formatted summary of the model outcomes

**Example:**
```python
summary = llm_summarize_model_output("/path/to/model_output.txt")
```

## Configuration

### Changing the LLM Model

To use a different LLM model, modify the `query_model` call:

```python
expr("query_model('your.model.endpoint', prompt) AS summary")
```

### Customizing the Prompt

Edit the prompt template in the `llm_summarize_model_output` function to adjust:
- Number of bullet points
- Focus areas (e.g., more emphasis on feature importance)
- Output style (technical vs. business-friendly)

## Example Output

```
=== Model Outcome Summary ===

• Model Performance: The model achieved an AUC of 0.87 and accuracy of 82%, indicating strong predictive power for the target variable.

• Top Predictors: Customer tenure (OR: 2.3, p<0.001) and contract type (OR: 1.8, p<0.01) are the strongest predictors of churn.

• Weak Signals: Device type and payment method showed no statistical significance (p>0.05) and can likely be excluded from future models.

• Model Quality: The confusion matrix shows 15% false positives, suggesting the model is slightly conservative in predictions.

• Business Insight: Focus retention efforts on customers with short tenure and month-to-month contracts for maximum impact.
```

## Limitations

- Requires access to OCI AI Models infrastructure
- LLM interpretation quality depends on the model's training and capabilities
- Processing time depends on file size and LLM response time
- Costs may apply based on LLM usage

## Best Practices

1. **Clean Input**: Ensure model output files are well-formatted and complete
2. **Validate Results**: Always review LLM summaries for accuracy
3. **Iterate Prompts**: Adjust the prompt template based on your specific use case
4. **Error Handling**: Add try-catch blocks for production use

## Troubleshooting

**Issue**: Empty or incomplete summaries
- **Solution**: Check that the input file contains actual model output and is readable

**Issue**: LLM response is too technical
- **Solution**: Adjust the prompt to emphasize "explain to a non-technical stakeholder"
