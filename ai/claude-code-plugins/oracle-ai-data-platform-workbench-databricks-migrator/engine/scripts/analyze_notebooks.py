#!/usr/bin/env python3
"""
Notebook Analyzer - Scans downloaded notebooks for Databricks dependencies.
Produces per-notebook reports and a consolidated migration report.
"""

import json
import os
import re
import sys
from collections import defaultdict
from datetime import datetime

PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
NOTEBOOKS_DIR = os.path.join(PROJECT_DIR, "notebooks")
REPORTS_DIR = os.path.join(PROJECT_DIR, "reports")

# Databricks-specific patterns to detect
DBUTILS_PATTERNS = {
    "dbutils.fs": {
        "pattern": r"dbutils\.fs\.\w+",
        "severity": "HIGH",
        "description": "DBFS filesystem operations (cp, ls, mv, rm, mkdirs, put, head, mount, etc.)",
        "migration": "Replace with OCI Object Storage SDK or HDFS-compatible operations"
    },
    "dbutils.secrets": {
        "pattern": r"dbutils\.secrets\.\w+",
        "severity": "HIGH",
        "description": "Databricks secret management",
        "migration": "Replace with OCI Vault secrets"
    },
    "dbutils.widgets": {
        "pattern": r"dbutils\.widgets\.\w+",
        "severity": "MEDIUM",
        "description": "Notebook parameterization widgets",
        "migration": "Replace with environment variables or notebook parameters"
    },
    "dbutils.notebook": {
        "pattern": r"dbutils\.notebook\.\w+",
        "severity": "HIGH",
        "description": "Notebook orchestration (run/exit)",
        "migration": "Replace with %run magic or Python imports; exit() for exit"
    },
    "dbutils.jobs": {
        "pattern": r"dbutils\.jobs\.\w+",
        "severity": "MEDIUM",
        "description": "Job task values",
        "migration": "Replace with AIDP job parameters or environment variables"
    },
    "dbutils.library": {
        "pattern": r"dbutils\.library\.\w+",
        "severity": "LOW",
        "description": "Library management (mostly deprecated)",
        "migration": "Use pip install or cluster library configuration"
    },
    "dbutils.credentials": {
        "pattern": r"dbutils\.credentials\.\w+",
        "severity": "HIGH",
        "description": "IAM role credential passthrough",
        "migration": "Replace with OCI API key auth (oci.config.from_file('/Workspace/<oci-config-workspace-path>', 'DEFAULT') + oci.signer.Signer). Do NOT use resource principal."
    },
    "dbutils.data": {
        "pattern": r"dbutils\.data\.\w+",
        "severity": "LOW",
        "description": "Data summarization utilities",
        "migration": "Use pandas describe() or custom summary functions"
    },
    "dbutils_generic": {
        "pattern": r"dbutils\.\w+",
        "severity": "MEDIUM",
        "description": "Other dbutils usage",
        "migration": "Needs manual review"
    },
}

# Additional Databricks-specific patterns
DATABRICKS_PATTERNS = {
    "spark_dbfs_path": {
        "pattern": r"dbfs:/|/dbfs/",
        "severity": "HIGH",
        "description": "DBFS path references",
        "migration": "Replace with OCI Object Storage paths (oci://bucket@namespace/path)"
    },
    "databricks_display": {
        "pattern": r"\bdisplay\s*\(",
        "severity": "LOW",
        "description": "Databricks display() function",
        "migration": "Replace with df.show() or df.toPandas() for visualization"
    },
    "mount_points": {
        "pattern": r"/mnt/\w+",
        "severity": "HIGH",
        "description": "DBFS mount point references",
        "migration": "Replace with direct OCI Object Storage paths"
    },
    "databricks_sql_magic": {
        "pattern": r"%sql\b",
        "severity": "LOW",
        "description": "Databricks SQL magic command",
        "migration": "Use spark.sql() instead or %%sql cell magic if supported"
    },
    "databricks_run_magic": {
        "pattern": r"%run\s+",
        "severity": "MEDIUM",
        "description": "Databricks %run notebook execution",
        "migration": "Convert to Python imports or %run if supported on AIDP"
    },
    "databricks_pip_magic": {
        "pattern": r"%pip\s+install",
        "severity": "LOW",
        "description": "Pip install via magic command",
        "migration": "Should work on AIDP, verify compatibility"
    },
    "s3_paths": {
        "pattern": r"s3[a]?://[\w\-\.]+/",
        "severity": "HIGH",
        "description": "AWS S3 path references",
        "migration": "Replace with OCI Object Storage paths"
    },
    "aws_imports": {
        "pattern": r"import\s+boto3|from\s+boto3|import\s+botocore|from\s+botocore",
        "severity": "HIGH",
        "description": "AWS SDK imports",
        "migration": "Replace with OCI SDK (oci package)"
    },
    "databricks_connect": {
        "pattern": r"databricks[_\-]connect|DatabricksSession",
        "severity": "HIGH",
        "description": "Databricks Connect",
        "migration": "Remove/replace with direct Spark session"
    },
    "delta_lake": {
        "pattern": r"delta\.|DeltaTable|\.format\(['\"]delta['\"]\)",
        "severity": "MEDIUM",
        "description": "Delta Lake usage",
        "migration": "Check AIDP Delta Lake support or convert to Parquet/Iceberg"
    },
    "unity_catalog": {
        "pattern": r"spark\.catalog\.|USE\s+CATALOG|CREATE\s+CATALOG",
        "severity": "MEDIUM",
        "description": "Unity Catalog references",
        "migration": "Replace with AIDP catalog system"
    },
    "mlflow_databricks": {
        "pattern": r"mlflow\.set_tracking_uri.*databricks|databricks://",
        "severity": "MEDIUM",
        "description": "MLflow with Databricks tracking",
        "migration": "Configure MLflow with AIDP-compatible tracking URI"
    },
    "spark_conf_databricks": {
        "pattern": r"spark\.databricks\.|spark\.conf\.set.*databricks",
        "severity": "MEDIUM",
        "description": "Databricks-specific Spark configuration",
        "migration": "Remove or replace with AIDP Spark configuration"
    },
    "autoloader": {
        "pattern": r"cloudFiles|Auto\s*Loader|readStream.*format.*cloudFiles",
        "severity": "HIGH",
        "description": "Databricks Auto Loader",
        "migration": "Replace with Spark Structured Streaming from OCI"
    },
    "photon": {
        "pattern": r"spark\.databricks\.photon|photon",
        "severity": "LOW",
        "description": "Photon engine references",
        "migration": "Remove - not available on AIDP"
    },
    "notebook_context": {
        "pattern": r"spark\.conf\.get.*notebook|getContext|notebookPath|currentRunId",
        "severity": "MEDIUM",
        "description": "Databricks notebook context",
        "migration": "Replace with AIDP-equivalent context or environment variables"
    },
    "kafka_producer": {
        "pattern": r"KafkaProducer|kafka\.producer|\.writeStream.*format.*kafka|from_kafka|kafka\.bootstrap",
        "severity": "HIGH",
        "description": "Kafka producer/consumer integration",
        "migration": "Verify Kafka connectivity from AIDP; may need OCI Streaming Service"
    },
    "slack_integration": {
        "pattern": r"slack_sdk|slackclient|WebhookClient|slack\.com/api|slack_token|SLACK_",
        "severity": "MEDIUM",
        "description": "Slack API integration",
        "migration": "Verify network access from AIDP to Slack; may need OCI Functions proxy"
    },
    "requests_http": {
        "pattern": r"import requests|from requests|urllib\.request|http\.client",
        "severity": "LOW",
        "description": "HTTP requests library usage",
        "migration": "Verify network access from AIDP cluster; may need proxy configuration"
    },
    "jdbc_connections": {
        "pattern": r"\.jdbc\(|\.format\(['\"]jdbc['\"]\)|jdbc:",
        "severity": "MEDIUM",
        "description": "JDBC database connections",
        "migration": "Update JDBC connection strings for OCI database endpoints"
    },
    "hudi_usage": {
        "pattern": r"hudi|\.format\(['\"]org\.apache\.hudi|hoodie\.",
        "severity": "MEDIUM",
        "description": "Apache Hudi usage",
        "migration": "Verify Hudi JAR availability on AIDP Spark 3.5; may need custom JARs"
    },
    "custom_jars": {
        "pattern": r"spark\.jars|addJar|spark\.driver\.extraClassPath|spark\.executor\.extraClassPath|--jars",
        "severity": "HIGH",
        "description": "Custom JAR dependencies",
        "migration": "Upload JARs to AIDP workspace and update paths"
    },
    "gpu_usage": {
        "pattern": r"torch\.|tensorflow|keras|cuda|gpu|rapids|cudf|cuml",
        "severity": "HIGH",
        "description": "GPU/ML framework usage",
        "migration": "Verify GPU availability on AIDP; may need specific instance shapes"
    },
    "pip_packages": {
        "pattern": r"%pip\s+install\s+[\w\-]+|pip\s+install\s+[\w\-]+|subprocess.*pip.*install",
        "severity": "MEDIUM",
        "description": "Package installation at runtime",
        "migration": "Pre-install packages in AIDP cluster config or use init scripts"
    },
}

# Spark 3.5 vs Spark 4.0 compatibility patterns
SPARK_COMPAT_PATTERNS = {
    "spark4_removed_apis": {
        "pattern": r"\.registerTempTable\(|SQLContext\(|HiveContext\(|sqlContext\.",
        "severity": "MEDIUM",
        "description": "Spark APIs deprecated/removed in 4.0 (registerTempTable, SQLContext, HiveContext)",
        "migration": "Use createOrReplaceTempView(), SparkSession instead. These work on 3.5 too."
    },
    "spark4_pandas_api": {
        "pattern": r"import pyspark\.pandas|from pyspark\.pandas|\.to_pandas_on_spark",
        "severity": "LOW",
        "description": "PySpark Pandas API (available since 3.2)",
        "migration": "Should work on Spark 3.5; verify version compatibility"
    },
    "spark4_connect": {
        "pattern": r"SparkSession\.builder\.remote\(|spark_connect|grpc://",
        "severity": "HIGH",
        "description": "Spark Connect (enhanced in 4.0)",
        "migration": "Not available on Spark 3.5; use traditional SparkSession"
    },
    "spark_datasource_v2": {
        "pattern": r"DataSourceV2|TableCatalog|SupportsCatalogOptions",
        "severity": "MEDIUM",
        "description": "DataSource V2 API usage",
        "migration": "V2 available in 3.5 but check specific implementations"
    },
    "spark_ansi_mode": {
        "pattern": r"spark\.sql\.ansi\.enabled",
        "severity": "LOW",
        "description": "ANSI SQL mode (default changed in 4.0)",
        "migration": "ANSI mode is opt-in on 3.5, default on 4.0. Verify SQL behavior."
    },
    "spark_session_creation": {
        "pattern": r"SparkSession\.builder",
        "severity": "INFO",
        "description": "SparkSession creation",
        "migration": "On AIDP, SparkSession is pre-configured as 'spark'. Remove manual creation."
    },
    "structured_streaming": {
        "pattern": r"\.readStream|\.writeStream|StreamingQuery|trigger\(",
        "severity": "MEDIUM",
        "description": "Spark Structured Streaming",
        "migration": "Verify streaming support on AIDP; check source/sink availability"
    },
    "udf_registration": {
        "pattern": r"@udf|@pandas_udf|spark\.udf\.register|F\.udf\(",
        "severity": "LOW",
        "description": "User Defined Functions",
        "migration": "UDFs should work on Spark 3.5; verify Python UDF availability"
    },
}

def extract_code_from_notebook(notebook_path):
    """Extract all code cells from a Jupyter notebook."""
    try:
        with open(notebook_path, 'r', encoding='utf-8', errors='replace') as f:
            nb = json.load(f)
    except (json.JSONDecodeError, UnicodeDecodeError) as e:
        return None, str(e)

    cells = nb.get("cells", [])
    code_cells = []
    all_source = []

    for i, cell in enumerate(cells):
        cell_type = cell.get("cell_type", "")
        source = "".join(cell.get("source", []))

        if cell_type == "code":
            code_cells.append({"index": i, "source": source})
            all_source.append(source)

    return {
        "cells": code_cells,
        "full_source": "\n".join(all_source),
        "total_cells": len(cells),
        "code_cells": len(code_cells),
        "kernel": nb.get("metadata", {}).get("kernelspec", {}).get("display_name", "unknown")
    }, None

def analyze_notebook(notebook_path, relative_path):
    """Analyze a single notebook for Databricks dependencies."""
    result = {
        "path": relative_path,
        "local_path": notebook_path,
        "timestamp": datetime.now().isoformat(),
        "issues": [],
        "summary": {},
        "severity_counts": {"HIGH": 0, "MEDIUM": 0, "LOW": 0, "INFO": 0},
        "dbutils_methods": [],
    }

    code_data, error = extract_code_from_notebook(notebook_path)
    if error:
        result["error"] = error
        return result

    result["notebook_info"] = {
        "total_cells": code_data["total_cells"],
        "code_cells": code_data["code_cells"],
        "kernel": code_data["kernel"]
    }

    full_source = code_data["full_source"]

    # Check all patterns
    all_patterns = {**DBUTILS_PATTERNS, **DATABRICKS_PATTERNS, **SPARK_COMPAT_PATTERNS}
    seen_matches = set()

    for pattern_name, pattern_info in all_patterns.items():
        matches = re.findall(pattern_info["pattern"], full_source, re.IGNORECASE)
        if matches:
            unique_matches = list(set(matches))

            # Find which cells contain matches
            affected_cells = []
            for cell in code_data["cells"]:
                cell_matches = re.findall(pattern_info["pattern"], cell["source"], re.IGNORECASE)
                if cell_matches:
                    # Get a snippet around the match
                    lines = cell["source"].split("\n")
                    match_lines = []
                    for j, line in enumerate(lines):
                        if re.search(pattern_info["pattern"], line, re.IGNORECASE):
                            match_lines.append({"line_num": j+1, "content": line.strip()[:200]})
                    affected_cells.append({
                        "cell_index": cell["index"],
                        "matches": list(set(cell_matches)),
                        "match_lines": match_lines[:5]  # Limit to first 5 matches per cell
                    })

            # Avoid double-counting dbutils_generic with specific dbutils patterns
            match_key = f"{pattern_name}:{','.join(sorted(unique_matches[:5]))}"
            if match_key not in seen_matches:
                seen_matches.add(match_key)
                issue = {
                    "pattern": pattern_name,
                    "severity": pattern_info["severity"],
                    "description": pattern_info["description"],
                    "migration_hint": pattern_info["migration"],
                    "match_count": len(matches),
                    "unique_matches": unique_matches[:20],
                    "affected_cells": affected_cells
                }
                result["issues"].append(issue)
                result["severity_counts"][pattern_info["severity"]] += 1

    # Extract specific dbutils method calls
    dbutils_calls = re.findall(r"dbutils\.(\w+)\.(\w+)\s*\(", full_source)
    result["dbutils_methods"] = list(set(f"dbutils.{m}.{f}" for m, f in dbutils_calls))

    # Summary
    result["summary"] = {
        "total_issues": len(result["issues"]),
        "has_dbutils": any("dbutils" in i["pattern"] for i in result["issues"]),
        "has_dbfs_paths": any(i["pattern"] in ("spark_dbfs_path", "mount_points") for i in result["issues"]),
        "has_s3_paths": any(i["pattern"] == "s3_paths" for i in result["issues"]),
        "has_aws_sdk": any(i["pattern"] == "aws_imports" for i in result["issues"]),
        "has_delta": any(i["pattern"] == "delta_lake" for i in result["issues"]),
        "migration_complexity": "NONE" if not result["issues"] else
                                "HIGH" if result["severity_counts"]["HIGH"] > 0 else
                                "MEDIUM" if result["severity_counts"]["MEDIUM"] > 0 else "LOW"
    }

    return result

def main():
    os.makedirs(REPORTS_DIR, exist_ok=True)

    # Find all downloaded notebooks
    notebooks = []
    for root, dirs, files in os.walk(NOTEBOOKS_DIR):
        for f in files:
            if f.endswith(".ipynb"):
                full_path = os.path.join(root, f)
                rel_path = os.path.relpath(full_path, NOTEBOOKS_DIR)
                notebooks.append((full_path, rel_path))

    if not notebooks:
        print("No notebooks found in", NOTEBOOKS_DIR)
        sys.exit(1)

    print(f"Analyzing {len(notebooks)} notebooks...")
    print("=" * 60)

    all_reports = []
    category_counts = defaultdict(int)
    severity_totals = {"HIGH": 0, "MEDIUM": 0, "LOW": 0, "INFO": 0}
    dbutils_usage = defaultdict(int)
    problematic_notebooks = []

    for i, (full_path, rel_path) in enumerate(sorted(notebooks)):
        print(f"  [{i+1}/{len(notebooks)}] {rel_path}...", end=" ", flush=True)
        report = analyze_notebook(full_path, rel_path)
        all_reports.append(report)

        # Save individual report
        report_path = os.path.join(REPORTS_DIR, "per_notebook", rel_path.replace(".ipynb", "_report.json"))
        os.makedirs(os.path.dirname(report_path), exist_ok=True)
        with open(report_path, 'w') as f:
            json.dump(report, f, indent=2)

        # Aggregate stats
        for issue in report["issues"]:
            category_counts[issue["pattern"]] += 1
            severity_totals[issue["severity"]] += 1

        for method in report.get("dbutils_methods", []):
            dbutils_usage[method] += 1

        if report["summary"].get("total_issues", 0) > 0:
            problematic_notebooks.append({
                "path": rel_path,
                "complexity": report["summary"]["migration_complexity"],
                "issue_count": report["summary"]["total_issues"],
                "high_severity": report["severity_counts"]["HIGH"],
                "dbutils_methods": report.get("dbutils_methods", [])
            })

        complexity = report["summary"].get("migration_complexity", "NONE")
        issue_count = report["summary"].get("total_issues", 0)
        print(f"[{complexity}] {issue_count} issues")

    # Sort problematic notebooks by severity
    problematic_notebooks.sort(key=lambda x: (-x["high_severity"], -x["issue_count"]))

    # Consolidated report
    consolidated = {
        "timestamp": datetime.now().isoformat(),
        "total_notebooks_analyzed": len(notebooks),
        "notebooks_with_issues": len(problematic_notebooks),
        "notebooks_clean": len(notebooks) - len(problematic_notebooks),
        "severity_totals": severity_totals,
        "issue_category_counts": dict(category_counts),
        "dbutils_method_usage": dict(dbutils_usage),
        "migration_complexity_distribution": {
            "HIGH": len([n for n in problematic_notebooks if n["complexity"] == "HIGH"]),
            "MEDIUM": len([n for n in problematic_notebooks if n["complexity"] == "MEDIUM"]),
            "LOW": len([n for n in problematic_notebooks if n["complexity"] == "LOW"]),
            "NONE": len(notebooks) - len(problematic_notebooks)
        },
        "problematic_notebooks": problematic_notebooks,
        "top_dbutils_methods": sorted(dbutils_usage.items(), key=lambda x: -x[1])[:30],
    }

    consolidated_path = os.path.join(REPORTS_DIR, "consolidated_analysis.json")
    with open(consolidated_path, 'w') as f:
        json.dump(consolidated, f, indent=2)

    # Print summary
    print(f"\n{'=' * 60}")
    print(f"ANALYSIS SUMMARY")
    print(f"{'=' * 60}")
    print(f"  Total notebooks analyzed: {len(notebooks)}")
    print(f"  Notebooks with issues:    {len(problematic_notebooks)}")
    print(f"  Clean notebooks:          {len(notebooks) - len(problematic_notebooks)}")
    print(f"\n  Migration Complexity:")
    for level in ("HIGH", "MEDIUM", "LOW", "NONE"):
        count = consolidated["migration_complexity_distribution"][level]
        print(f"    {level:8s}: {count}")
    print(f"\n  Issue Categories:")
    for cat, count in sorted(category_counts.items(), key=lambda x: -x[1]):
        print(f"    {cat:30s}: {count}")
    print(f"\n  Top dbutils methods:")
    for method, count in sorted(dbutils_usage.items(), key=lambda x: -x[1])[:15]:
        print(f"    {method:40s}: {count}")

    print(f"\nReports saved to: {REPORTS_DIR}")
    print(f"Consolidated report: {consolidated_path}")

if __name__ == "__main__":
    main()
