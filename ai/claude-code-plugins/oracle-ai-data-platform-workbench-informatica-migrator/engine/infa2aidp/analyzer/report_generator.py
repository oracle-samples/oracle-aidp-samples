"""Generate analysis reports in multiple formats."""

import csv
import io
import json
import logging
from datetime import datetime

from infa2aidp.analyzer.models import (
    AnalysisReport,
    ComponentInventory,
    DependencyInfo,
    MappingComplexity,
)

logger = logging.getLogger(__name__)


class ReportGenerator:
    """Generate comprehensive analysis reports in Markdown, JSON, and CSV."""

    def generate_markdown(self, report: AnalysisReport, output_path: str) -> None:
        lines = []
        inv = report.inventory
        assessments = report.complexity_assessments
        deps = report.dependencies

        # Header
        lines.append("# Informatica to AIDP Migration Analysis Report")
        lines.append(f"Generated: {report.generated_at}")
        lines.append("")

        # Executive Summary
        lines.append("## Executive Summary")
        lines.append("")
        lines.append(f"- **Informatica Version**: {inv.informatica_version or 'Unknown'}")

        # Mapping breakdown
        folder_parts = []
        for folder, count in sorted(inv.folder_counts.items()):
            folder_parts.append(f"{folder}: {count}")
        folder_str = f" ({', '.join(folder_parts)})" if folder_parts else ""
        lines.append(f"- **Total Mappings**: {inv.total_mappings}{folder_str}")

        # Complexity distribution
        dist = _complexity_distribution(assessments)
        if dist:
            pct_parts = []
            for level in ("SIMPLE", "MEDIUM", "COMPLEX", "VERY_COMPLEX"):
                count = dist.get(level, 0)
                if inv.total_mappings > 0:
                    pct = round(100 * count / inv.total_mappings)
                    pct_parts.append(f"{pct}% {level.replace('_', ' ').title()}")
            lines.append(f"- **Migration Complexity**: {', '.join(pct_parts)}")

        total_effort = sum(a.estimated_effort_hours for a in assessments)
        lines.append(f"- **Estimated Total Effort**: {total_effort:.0f} hours")
        lines.append("")

        # Component Inventory
        lines.append("## Component Inventory")
        lines.append("")
        lines.append("| Component | Count |")
        lines.append("|-----------|------:|")
        lines.append(f"| Mappings | {inv.total_mappings} |")
        lines.append(f"| Sessions | {inv.total_sessions} |")
        lines.append(f"| Workflows | {inv.total_workflows} |")
        lines.append(f"| Sources | {inv.total_sources} |")
        lines.append(f"| Targets | {inv.total_targets} |")
        lines.append(f"| Transformations | {inv.total_transformations} |")
        lines.append("")

        if inv.folder_counts:
            lines.append("### Mapping Folders")
            lines.append("")
            lines.append("| Folder | Count |")
            lines.append("|--------|------:|")
            for folder, count in sorted(inv.folder_counts.items()):
                lines.append(f"| {folder} | {count} |")
            lines.append("")

        if inv.connection_types:
            lines.append("### Connection Types")
            lines.append("")
            lines.append("| Type | Count |")
            lines.append("|------|------:|")
            for ctype, count in sorted(inv.connection_types.items()):
                lines.append(f"| {ctype} | {count} |")
            lines.append("")

        # Transformation Analysis
        if inv.transformation_type_counts:
            lines.append("## Transformation Analysis")
            lines.append("")
            sorted_tx = sorted(
                inv.transformation_type_counts.items(),
                key=lambda x: x[1],
                reverse=True,
            )
            max_count = max(inv.transformation_type_counts.values()) if inv.transformation_type_counts else 1
            bar_width = 40

            lines.append("```")
            for tx_type, count in sorted_tx:
                bar_len = max(1, round(bar_width * count / max_count))
                bar = "#" * bar_len
                lines.append(f"  {tx_type:<25s} {bar} {count}")
            lines.append("```")
            lines.append("")

        # Complexity Assessment
        if assessments:
            lines.append("## Complexity Assessment")
            lines.append("")
            lines.append(
                "| Mapping | Level | Score | Effort (hrs) | Risk | Key Issues |"
            )
            lines.append(
                "|---------|-------|------:|-------------:|------|------------|"
            )
            for a in sorted(assessments, key=lambda x: x.complexity_score, reverse=True):
                issues_str = "; ".join(a.issues[:2]) if a.issues else "-"
                lines.append(
                    f"| {a.mapping_name} | {a.complexity_level} | {a.complexity_score} "
                    f"| {a.estimated_effort_hours} | {a.migration_risk} | {issues_str} |"
                )
            lines.append("")

        # Dependency Map
        if deps.mapping_dependencies:
            lines.append("## Dependency Map")
            lines.append("")
            lines.append("```mermaid")
            lines.append("graph LR")
            for mapping, depends_on in deps.mapping_dependencies.items():
                for dep in depends_on:
                    lines.append(f"    {_mermaid_id(dep)}[{dep}] --> {_mermaid_id(mapping)}[{mapping}]")
            lines.append("```")
            lines.append("")

        # Subject Area Breakdown
        if deps.subject_area_groups:
            lines.append("## Subject Area Breakdown")
            lines.append("")
            lines.append("| Subject Area | Mappings | Count |")
            lines.append("|--------------|----------|------:|")
            for area, mappings in sorted(deps.subject_area_groups.items()):
                names = ", ".join(mappings[:5])
                if len(mappings) > 5:
                    names += f" (+{len(mappings) - 5} more)"
                lines.append(f"| {area} | {names} | {len(mappings)} |")
            lines.append("")

        # Compatibility Issues
        if report.compatibility_issues:
            lines.append("## Compatibility Issues")
            lines.append("")
            lines.append("| Severity | Component | Issue | Suggestion |")
            lines.append("|----------|-----------|-------|------------|")
            sorted_issues = sorted(
                report.compatibility_issues,
                key=lambda x: {"ERROR": 0, "WARNING": 1, "INFO": 2}.get(x.severity, 3),
            )
            for issue in sorted_issues:
                lines.append(
                    f"| {issue.severity} | {issue.component} | "
                    f"{issue.description} | {issue.suggestion} |"
                )
            lines.append("")

        # Migration Recommendations
        lines.append("## Migration Recommendations")
        lines.append("")
        lines.extend(self._generate_recommendations(report))
        lines.append("")

        with open(output_path, "w", encoding="utf-8") as f:
            f.write("\n".join(lines) + "\n")

        logger.info("Markdown report written to %s", output_path)

    def generate_json(self, report: AnalysisReport, output_path: str) -> None:
        data = {
            "generated_at": report.generated_at,
            "inventory": _dataclass_to_dict(report.inventory),
            "complexity_assessments": [
                _dataclass_to_dict(a) for a in report.complexity_assessments
            ],
            "dependencies": _dataclass_to_dict(report.dependencies),
            "compatibility_issues": [
                _dataclass_to_dict(i) for i in report.compatibility_issues
            ],
            "summary": report.summary,
        }

        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, default=str)

        logger.info("JSON report written to %s", output_path)

    def generate_csv(self, report: AnalysisReport, output_path: str) -> None:
        fieldnames = [
            "mapping_name",
            "complexity_level",
            "complexity_score",
            "estimated_effort_hours",
            "migration_risk",
            "num_transformations",
            "num_sources",
            "num_targets",
            "num_lookups",
            "num_joins",
            "has_complex_expressions",
            "has_stored_procedures",
            "has_custom_transformations",
            "has_sql_overrides",
            "has_scd_logic",
            "issues",
        ]

        with open(output_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            for a in report.complexity_assessments:
                row = {k: getattr(a, k, "") for k in fieldnames}
                row["issues"] = "; ".join(a.issues)
                writer.writerow(row)

        logger.info("CSV report written to %s", output_path)

    def _generate_recommendations(self, report: AnalysisReport) -> list[str]:
        recs = []
        assessments = report.complexity_assessments

        # Priority 1: Start with simple mappings
        dist = _complexity_distribution(assessments)
        simple_count = dist.get("SIMPLE", 0)
        if simple_count > 0:
            recs.append(
                f"1. **Start with SIMPLE mappings** ({simple_count} mappings) "
                f"to build migration patterns and validate the pipeline."
            )

        # Priority 2: Address blockers
        critical = [a for a in assessments if a.migration_risk == "CRITICAL"]
        if critical:
            names = ", ".join(a.mapping_name for a in critical[:5])
            recs.append(
                f"2. **Investigate CRITICAL risk mappings** ({len(critical)} total: {names}). "
                f"These contain transformations with no direct PySpark equivalent."
            )

        # Priority 3: Shared lookups
        shared = report.dependencies.shared_lookups
        if shared:
            recs.append(
                f"3. **Centralize shared lookup tables** ({len(shared)} shared across mappings). "
                f"Convert to broadcast DataFrames or Delta tables for reuse."
            )

        # Priority 4: Stored procedures
        sp_mappings = [a for a in assessments if a.has_stored_procedures]
        if sp_mappings:
            recs.append(
                f"4. **Rewrite stored procedures as PySpark** ({len(sp_mappings)} mappings affected). "
                f"These require manual conversion."
            )

        if not recs:
            recs.append("1. No specific blockers detected. Proceed with standard migration approach.")

        return recs


def _complexity_distribution(assessments: list) -> dict[str, int]:
    dist: dict[str, int] = {}
    for a in assessments:
        dist[a.complexity_level] = dist.get(a.complexity_level, 0) + 1
    return dist


def _dataclass_to_dict(obj) -> dict:
    """Convert a dataclass to a dict, handling nested dataclasses."""
    import dataclasses
    if dataclasses.is_dataclass(obj):
        return {k: _dataclass_to_dict(v) for k, v in dataclasses.asdict(obj).items()}
    return obj


def _mermaid_id(name: str) -> str:
    import re
    return re.sub(r"[^a-zA-Z0-9_]", "_", name)
