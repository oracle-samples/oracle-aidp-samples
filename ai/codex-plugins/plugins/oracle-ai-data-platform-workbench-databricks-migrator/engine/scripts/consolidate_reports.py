#!/usr/bin/env python3
"""
Consolidate all deep analysis reports into a single migration plan.
Produces:
1. A consolidated summary of all notebooks
2. A prioritized migration plan
3. Required infrastructure/config changes
4. Dependency map across all notebooks
"""

import json
import os
from collections import defaultdict
from datetime import datetime

PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DEEP_REPORTS_DIR = os.path.join(PROJECT_DIR, "reports", "deep_analysis")
REPORTS_DIR = os.path.join(PROJECT_DIR, "reports")


def load_all_reports():
    """Load all deep analysis JSON reports."""
    reports = []
    for root, dirs, files in os.walk(DEEP_REPORTS_DIR):
        for f in files:
            if f.endswith("_deep_report.json"):
                path = os.path.join(root, f)
                try:
                    with open(path) as fh:
                        data = json.load(fh)
                        if "error" not in data or "issues" in data:
                            reports.append(data)
                except Exception:
                    pass
    return reports


def consolidate(reports):
    """Build consolidated analysis."""
    # Aggregate statistics
    total = len(reports)
    complexity_dist = defaultdict(int)
    all_issues = []
    issue_type_counts = defaultdict(int)
    severity_counts = defaultdict(int)
    all_python_packages = defaultdict(int)
    all_jar_files = defaultdict(int)
    all_external_services = defaultdict(list)
    all_storage_paths = []
    all_notebook_deps = defaultdict(list)
    all_spark_configs = defaultdict(int)
    notebooks_by_complexity = defaultdict(list)
    total_effort_hours = 0
    can_run_as_is_count = 0

    for report in reports:
        nb_path = report.get("notebook_path", "unknown")
        summary = report.get("summary", {})
        complexity = summary.get("overall_migration_complexity", "UNKNOWN")
        complexity_dist[complexity] += 1
        notebooks_by_complexity[complexity].append(nb_path)

        effort = summary.get("estimated_effort_hours", 0)
        if isinstance(effort, (int, float)):
            total_effort_hours += effort

        if summary.get("can_run_as_is"):
            can_run_as_is_count += 1

        # Issues
        for issue in report.get("issues", []):
            issue_type_counts[issue.get("issue_type", "other")] += 1
            severity_counts[issue.get("severity", "UNKNOWN")] += 1
            all_issues.append({
                "notebook": nb_path,
                "cell": issue.get("cell_index"),
                "line": issue.get("line_number"),
                "type": issue.get("issue_type"),
                "severity": issue.get("severity"),
                "description": issue.get("description", "")[:200],
                "migration_action": issue.get("migration_action", "")[:200]
            })

        # Dependencies
        deps = report.get("dependencies", {})
        for pkg in deps.get("python_packages", []):
            if isinstance(pkg, str):
                all_python_packages[pkg] += 1
            elif isinstance(pkg, dict):
                all_python_packages[str(pkg.get("name", pkg))] += 1

        for jar in deps.get("jar_files", []):
            if isinstance(jar, str):
                all_jar_files[jar] += 1
            elif isinstance(jar, dict):
                all_jar_files[str(jar.get("name", jar.get("path", str(jar))))] += 1

        for svc in deps.get("external_services", []):
            if isinstance(svc, dict):
                svc_name = svc.get("service", "unknown")
            else:
                svc_name = str(svc)
            all_external_services[svc_name].append(nb_path)

        for sp in deps.get("storage_paths", []):
            sp["notebook"] = nb_path
            all_storage_paths.append(sp)

        for nd in deps.get("notebook_dependencies", []):
            target = nd.get("path", "unknown")
            all_notebook_deps[target].append(nb_path)

        for cfg in report.get("spark_config", {}).get("configs_set", []):
            key = cfg.get("key", "unknown")
            all_spark_configs[key] += 1

    # Build migration plan
    migration_plan = {
        "infrastructure_changes": [],
        "cluster_configuration": [],
        "package_installation": [],
        "jar_deployment": [],
        "credential_setup": [],
        "notebook_migration_order": []
    }

    # Infrastructure
    if any(s in all_external_services for s in ["Kafka", "kafka"]):
        migration_plan["infrastructure_changes"].append(
            "Configure Kafka connectivity from AIDP (may need OCI Streaming Service)"
        )
    if any("slack" in s.lower() for s in all_external_services):
        migration_plan["infrastructure_changes"].append(
            "Configure Slack webhook access from AIDP network"
        )
    if any("jdbc" in s.lower() for s in all_external_services):
        migration_plan["infrastructure_changes"].append(
            "Configure JDBC connectivity to databases from AIDP cluster"
        )

    # Packages
    standard_packages = {"os", "sys", "json", "re", "datetime", "time", "typing",
                         "collections", "functools", "itertools", "math", "pathlib",
                         "base64", "hashlib", "copy", "dataclasses", "abc", "io",
                         "csv", "string", "textwrap", "warnings", "logging", "traceback",
                         "uuid", "struct", "pprint"}
    external_packages = {p for p in all_python_packages if p.lower() not in standard_packages}
    migration_plan["package_installation"] = sorted(external_packages)

    # JARs
    migration_plan["jar_deployment"] = sorted(all_jar_files.keys())

    # Notebook ordering (most depended-on first)
    dep_count = {k: len(v) for k, v in all_notebook_deps.items()}
    migration_plan["notebook_migration_order"] = sorted(
        dep_count.items(), key=lambda x: -x[1]
    )[:50]  # Top 50 most depended-on

    # Build consolidated report
    consolidated = {
        "timestamp": datetime.now().isoformat(),
        "total_notebooks_analyzed": total,
        "can_run_as_is": can_run_as_is_count,
        "total_estimated_effort_hours": round(total_effort_hours, 1),
        "complexity_distribution": dict(complexity_dist),
        "severity_distribution": dict(severity_counts),
        "issue_type_distribution": dict(issue_type_counts),
        "total_unique_issues": len(all_issues),
        "python_packages_used": dict(sorted(all_python_packages.items(), key=lambda x: -x[1])),
        "jar_dependencies": dict(sorted(all_jar_files.items(), key=lambda x: -x[1])),
        "external_services": {k: len(v) for k, v in all_external_services.items()},
        "spark_configs_used": dict(sorted(all_spark_configs.items(), key=lambda x: -x[1])),
        "most_depended_notebooks": dict(sorted(dep_count.items(), key=lambda x: -x[1])[:20]),
        "notebooks_by_complexity": {k: sorted(v) for k, v in notebooks_by_complexity.items()},
        "migration_plan": migration_plan,
        "critical_issues": [i for i in all_issues if i["severity"] == "CRITICAL"][:100],
        "high_severity_issues": [i for i in all_issues if i["severity"] == "HIGH"][:100],
    }

    return consolidated


def main():
    print("Consolidating deep analysis reports...")
    reports = load_all_reports()
    print(f"Loaded {len(reports)} reports")

    if not reports:
        print("No reports found to consolidate.")
        return

    consolidated = consolidate(reports)

    output_path = os.path.join(REPORTS_DIR, "consolidated_deep_analysis.json")
    with open(output_path, 'w') as f:
        json.dump(consolidated, f, indent=2)

    print(f"\nConsolidated report saved to: {output_path}")
    print(f"\nSummary:")
    print(f"  Total notebooks: {consolidated['total_notebooks_analyzed']}")
    print(f"  Can run as-is: {consolidated['can_run_as_is']}")
    print(f"  Total effort (hours): {consolidated['total_estimated_effort_hours']}")
    print(f"\n  Complexity distribution:")
    for level, count in sorted(consolidated['complexity_distribution'].items()):
        print(f"    {level}: {count}")
    print(f"\n  Top issue types:")
    for itype, count in sorted(consolidated['issue_type_distribution'].items(), key=lambda x: -x[1])[:10]:
        print(f"    {itype}: {count}")
    print(f"\n  External packages needed ({len(consolidated['python_packages_used'])}):")
    for pkg, count in list(consolidated['python_packages_used'].items())[:15]:
        print(f"    {pkg}: used in {count} notebooks")


if __name__ == "__main__":
    main()
