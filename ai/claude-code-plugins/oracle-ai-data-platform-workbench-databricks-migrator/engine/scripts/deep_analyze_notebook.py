#!/usr/bin/env python3
"""
Deep Notebook Analyzer - Uses Claude Opus (1M context) via the Anthropic API
to do a thorough, line-by-line analysis of each notebook for AIDP migration.

This is NOT a regex scan. Each notebook gets a full Claude analysis that:
1. Reads every cell, every line of code
2. Identifies ALL dependencies (Python packages, JARs, external services)
3. Flags ALL Databricks-specific code (dbutils, display, %run, DBFS paths, etc.)
4. Checks Spark API compatibility (3.5 vs 4.0 APIs)
5. Identifies AWS-specific code (S3, boto3, Glue, etc.)
6. Finds network dependencies (Kafka, Slack, HTTP APIs, JDBC connections)
7. Identifies mount point assumptions and storage path patterns
8. Checks for custom JAR dependencies and their Spark 3.5 compatibility
9. Produces a detailed migration report with specific line-by-line changes

Usage:
    python3 deep_analyze_notebook.py <notebook_path> [output_report_path]
    python3 deep_analyze_notebook.py --batch <notebook_list.json> [--concurrency 5]
"""

import anthropic
import json
import os
import sys
import time
import argparse
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
NOTEBOOKS_DIR = os.path.join(PROJECT_DIR, "notebooks")
REPORTS_DIR = os.path.join(PROJECT_DIR, "reports", "deep_analysis")

ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")

SYSTEM_PROMPT = """You are an expert Spark/Databricks migration engineer. You are analyzing a Jupyter notebook that was originally written for Databricks and needs to be migrated to OCI AIDP (AI Data Platform) which runs Apache Spark 3.5.

Your task is to perform a THOROUGH, LINE-BY-LINE analysis of every cell in this notebook. Be extremely detailed and specific.

For EACH code cell, you must identify:

1. **Databricks-specific code**: dbutils.fs, dbutils.secrets, dbutils.widgets, dbutils.notebook, dbutils.jobs, dbutils.library, dbutils.credentials, display(), %run, %sql, %pip, %sh, %scala, %r magic commands
2. **DBFS/Mount paths**: Any references to dbfs:/, /dbfs/, /mnt/, mount points
3. **AWS-specific code**: s3://, s3a://, boto3, botocore, AWS SDK calls, Glue references, Redshift, SQS, SNS, Lambda, IAM role assumptions
4. **Spark API compatibility**: APIs deprecated or changed between Spark 3.5 and 4.0 (registerTempTable, SQLContext, HiveContext, SparkContext directly, etc.)
5. **External dependencies**:
   - Python packages imported (pip packages)
   - JAR files referenced or loaded
   - JDBC connections and database endpoints
   - Kafka brokers and topics
   - HTTP API calls (Slack, webhooks, REST APIs)
   - MLflow tracking URIs
6. **Storage paths**: All file paths referenced (S3, DBFS, HDFS, local) and what they map to
7. **Spark configuration**: Any spark.conf.set() calls, especially Databricks-specific ones
8. **Delta Lake usage**: DeltaTable operations, merge/upsert, time travel, etc.
9. **Streaming**: Structured Streaming sources and sinks
10. **Notebook orchestration**: %run calls to other notebooks, dbutils.notebook.run()
11. **Custom UDFs**: Any user-defined functions that might have platform dependencies
12. **Security/credentials**: How secrets, credentials, tokens are accessed

For each issue found, provide:
- The exact cell number and line number
- The exact code snippet
- Why it's a problem on AIDP Spark 3.5
- The specific migration action needed (exact code replacement)
- Severity: CRITICAL (will fail), HIGH (likely fail), MEDIUM (may fail), LOW (cosmetic/best practice)

Also provide:
- A complete list of all Python imports/packages used
- A complete list of all JAR dependencies referenced
- A complete list of all external service connections
- A complete list of all file/storage paths referenced
- A dependency graph showing which other notebooks this one depends on (via %run or dbutils.notebook.run)

Return your analysis as a structured JSON object with the following schema:
{
  "notebook_path": "string",
  "analysis_timestamp": "ISO datetime",
  "summary": {
    "total_cells": number,
    "code_cells": number,
    "markdown_cells": number,
    "overall_migration_complexity": "CRITICAL|HIGH|MEDIUM|LOW|NONE",
    "estimated_effort_hours": number,
    "can_run_as_is": boolean,
    "brief_description": "What this notebook does in 1-2 sentences"
  },
  "issues": [
    {
      "cell_index": number,
      "line_number": number,
      "code_snippet": "the exact problematic code",
      "issue_type": "dbutils|dbfs_path|aws_specific|spark_compat|external_dep|storage_path|spark_config|delta_lake|streaming|notebook_orchestration|udf|security|jar_dependency|other",
      "severity": "CRITICAL|HIGH|MEDIUM|LOW",
      "description": "Detailed description of the issue",
      "migration_action": "Specific code change needed",
      "replacement_code": "The exact replacement code (if applicable)"
    }
  ],
  "dependencies": {
    "python_packages": ["list of all imported packages"],
    "jar_files": ["list of JAR files referenced"],
    "external_services": [{"service": "name", "endpoint": "url/host", "purpose": "what it's used for"}],
    "storage_paths": [{"original_path": "dbfs:/...", "path_type": "DBFS|S3|HDFS|LOCAL|OCI", "purpose": "what data"}],
    "notebook_dependencies": [{"path": "notebook path", "method": "%run|dbutils.notebook.run", "arguments": {}}],
    "database_connections": [{"type": "JDBC|Hive|Delta", "details": "connection string or table"}]
  },
  "spark_config": {
    "configs_set": [{"key": "spark.xxx", "value": "yyy", "is_databricks_specific": boolean}]
  },
  "migration_plan": {
    "pre_migration_steps": ["steps needed before code changes"],
    "code_changes": [{"description": "change needed", "priority": "1-5"}],
    "post_migration_steps": ["steps needed after code changes"],
    "testing_notes": ["what to test after migration"]
  }
}

Be exhaustive. Do not miss anything. Every single line matters."""


def analyze_single_notebook(notebook_path: str, relative_path: str, report_dir: str) -> dict:
    """Analyze a single notebook using Claude Opus."""

    # Read the notebook
    try:
        with open(notebook_path, 'r', encoding='utf-8', errors='replace') as f:
            notebook_content = f.read()
    except Exception as e:
        return {"error": f"Cannot read notebook: {e}", "path": relative_path}

    # Check size - if too large, we'll need to handle differently
    content_size = len(notebook_content)

    # Parse to get a readable version
    try:
        nb = json.loads(notebook_content)
        cells = nb.get("cells", [])

        # Build a readable representation
        readable_parts = []
        readable_parts.append(f"# Notebook: {relative_path}")
        readable_parts.append(f"# Kernel: {nb.get('metadata', {}).get('kernelspec', {}).get('display_name', 'unknown')}")
        readable_parts.append(f"# Total cells: {len(cells)}")
        readable_parts.append("")

        for i, cell in enumerate(cells):
            cell_type = cell.get("cell_type", "unknown")
            source = "".join(cell.get("source", []))
            readable_parts.append(f"--- Cell {i} [{cell_type}] ---")
            readable_parts.append(source)
            readable_parts.append("")

        readable_content = "\n".join(readable_parts)
    except json.JSONDecodeError:
        readable_content = notebook_content

    # Call Claude API
    client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)

    try:
        message = client.messages.create(
            model="claude-opus-4-8",
            max_tokens=16000,
            system=SYSTEM_PROMPT,
            messages=[
                {
                    "role": "user",
                    "content": f"Please perform a thorough, line-by-line analysis of this notebook for migration from Databricks to OCI AIDP (Spark 3.5). Return ONLY the JSON analysis object, no other text.\n\nNotebook path: {relative_path}\n\n```\n{readable_content}\n```"
                }
            ]
        )

        response_text = message.content[0].text

        # Try to parse as JSON - handle markdown code blocks
        # Strip leading/trailing whitespace
        cleaned = response_text.strip()

        # Remove markdown code blocks if present
        if cleaned.startswith("```"):
            lines = cleaned.split("\n")
            # Remove first line (```json or ```)
            lines = lines[1:]
            # Remove last line if it's ```)
            if lines and lines[-1].strip() == "```":
                lines = lines[:-1]
            cleaned = "\n".join(lines).strip()

        # Try extracting JSON from within the text
        json_start = cleaned.find("{")
        json_end = cleaned.rfind("}") + 1
        if json_start >= 0 and json_end > json_start:
            cleaned = cleaned[json_start:json_end]

        try:
            analysis = json.loads(cleaned)
        except json.JSONDecodeError:
            # If JSON parsing fails, save the raw response
            analysis = {
                "notebook_path": relative_path,
                "raw_analysis": response_text,
                "parse_error": "Could not parse as JSON"
            }

        # Add metadata
        analysis["notebook_path"] = relative_path
        analysis["analysis_timestamp"] = datetime.now().isoformat()
        analysis["notebook_size_bytes"] = content_size
        analysis["api_usage"] = {
            "input_tokens": message.usage.input_tokens,
            "output_tokens": message.usage.output_tokens,
            "model": "claude-opus-4-8"
        }

        return analysis

    except Exception as e:
        return {
            "notebook_path": relative_path,
            "error": str(e),
            "analysis_timestamp": datetime.now().isoformat()
        }


def save_report(analysis: dict, report_dir: str, relative_path: str):
    """Save the analysis report."""
    report_path = os.path.join(report_dir, relative_path.replace(".ipynb", "_deep_report.json"))
    os.makedirs(os.path.dirname(report_path), exist_ok=True)
    with open(report_path, 'w') as f:
        json.dump(analysis, f, indent=2)
    return report_path


def analyze_batch(notebook_list_path: str, concurrency: int = 3, start_index: int = 0, end_index: int = None):
    """Analyze a batch of notebooks."""
    os.makedirs(REPORTS_DIR, exist_ok=True)

    with open(notebook_list_path) as f:
        notebooks = json.load(f)

    if end_index:
        notebooks = notebooks[start_index:end_index]
    else:
        notebooks = notebooks[start_index:]

    print(f"Deep Analysis Pipeline")
    print(f"=" * 60)
    print(f"Notebooks to analyze: {len(notebooks)}")
    print(f"Concurrency: {concurrency}")
    print(f"Model: Claude Opus (1M context)")
    print(f"Started: {datetime.now().isoformat()}")
    print(f"=" * 60)

    results = []
    completed = 0
    failed = 0
    skipped = 0

    def process_notebook(nb_info):
        nonlocal completed, failed, skipped
        nb_path = nb_info["path"]
        local_path = os.path.join(NOTEBOOKS_DIR, nb_path)

        # Check if report already exists
        report_path = os.path.join(REPORTS_DIR, nb_path.replace(".ipynb", "_deep_report.json"))
        if os.path.exists(report_path):
            try:
                with open(report_path) as f:
                    existing = json.load(f)
                if "error" not in existing:
                    return {"path": nb_path, "status": "skipped", "report": report_path}
            except Exception:
                pass

        # Check if notebook exists locally
        if not os.path.exists(local_path):
            return {"path": nb_path, "status": "missing", "error": "Not downloaded"}

        # Analyze
        try:
            analysis = analyze_single_notebook(local_path, nb_path, REPORTS_DIR)
            report_file = save_report(analysis, REPORTS_DIR, nb_path)

            has_error = "error" in analysis
            issue_count = len(analysis.get("issues", []))
            complexity = analysis.get("summary", {}).get("overall_migration_complexity", "UNKNOWN")

            return {
                "path": nb_path,
                "status": "error" if has_error else "analyzed",
                "report": report_file,
                "issue_count": issue_count,
                "complexity": complexity,
                "error": analysis.get("error")
            }
        except Exception as e:
            return {"path": nb_path, "status": "error", "error": str(e)}

    # Process with controlled concurrency
    with ThreadPoolExecutor(max_workers=concurrency) as executor:
        futures = {executor.submit(process_notebook, nb): nb for nb in notebooks}

        for future in as_completed(futures):
            nb = futures[future]
            try:
                result = future.result()
                results.append(result)

                status = result["status"]
                if status == "analyzed":
                    completed += 1
                    complexity = result.get("complexity", "?")
                    issues = result.get("issue_count", 0)
                    print(f"  [{completed+failed+skipped}/{len(notebooks)}] ANALYZED [{complexity}] {issues} issues: {result['path']}")
                elif status == "skipped":
                    skipped += 1
                    print(f"  [{completed+failed+skipped}/{len(notebooks)}] SKIPPED: {result['path']}")
                elif status == "missing":
                    failed += 1
                    print(f"  [{completed+failed+skipped}/{len(notebooks)}] MISSING: {result['path']}")
                else:
                    failed += 1
                    print(f"  [{completed+failed+skipped}/{len(notebooks)}] FAILED: {result['path']}: {result.get('error', 'unknown')}")

            except Exception as e:
                failed += 1
                print(f"  [{completed+failed+skipped}/{len(notebooks)}] ERROR: {nb['path']}: {e}")
                results.append({"path": nb["path"], "status": "error", "error": str(e)})

    # Save batch results
    batch_report = {
        "timestamp": datetime.now().isoformat(),
        "total": len(notebooks),
        "analyzed": completed,
        "skipped": skipped,
        "failed": failed,
        "results": results
    }

    batch_report_path = os.path.join(REPORTS_DIR, "batch_analysis_report.json")
    with open(batch_report_path, 'w') as f:
        json.dump(batch_report, f, indent=2)

    print(f"\n{'=' * 60}")
    print(f"BATCH ANALYSIS COMPLETE")
    print(f"  Analyzed: {completed}")
    print(f"  Skipped:  {skipped}")
    print(f"  Failed:   {failed}")
    print(f"  Reports:  {REPORTS_DIR}")
    print(f"{'=' * 60}")

    return batch_report


def main():
    parser = argparse.ArgumentParser(description="Deep notebook analysis with Claude Opus")
    parser.add_argument("notebook", nargs="?", help="Single notebook path to analyze")
    parser.add_argument("--batch", help="Path to notebook_list.json for batch analysis")
    parser.add_argument("--concurrency", type=int, default=3, help="Number of concurrent analyses")
    parser.add_argument("--start", type=int, default=0, help="Start index in batch")
    parser.add_argument("--end", type=int, default=None, help="End index in batch")
    parser.add_argument("--output", help="Output report path (single notebook mode)")

    args = parser.parse_args()

    if args.batch:
        analyze_batch(args.batch, args.concurrency, args.start, args.end)
    elif args.notebook:
        local_path = os.path.join(NOTEBOOKS_DIR, args.notebook)
        if not os.path.exists(local_path):
            local_path = args.notebook

        print(f"Analyzing: {args.notebook}")
        analysis = analyze_single_notebook(local_path, args.notebook, REPORTS_DIR)

        output_path = args.output or os.path.join(REPORTS_DIR, args.notebook.replace(".ipynb", "_deep_report.json"))
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        with open(output_path, 'w') as f:
            json.dump(analysis, f, indent=2)

        print(f"Report saved to: {output_path}")

        # Print summary
        if "summary" in analysis:
            s = analysis["summary"]
            print(f"\nComplexity: {s.get('overall_migration_complexity', 'N/A')}")
            print(f"Issues: {len(analysis.get('issues', []))}")
            print(f"Can run as-is: {s.get('can_run_as_is', 'N/A')}")
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
