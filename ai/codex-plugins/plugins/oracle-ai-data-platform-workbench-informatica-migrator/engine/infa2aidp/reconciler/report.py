"""Reconciliation report generation in Markdown, JSON, and CSV formats."""

from __future__ import annotations

import csv
import io
import json
from datetime import datetime, timezone
from typing import Any

from infa2aidp.reconciler.models import (
    AggregateResult,
    ColumnDiff,
    ReconcileResult,
    RowDiff,
    SchemaColumnDiff,
)


class ReconcileReport:
    """Generates human-readable and machine-readable reconciliation reports."""

    def __init__(self, results: list[ReconcileResult]):
        self.results = results
        self.generated_at = datetime.now(timezone.utc).isoformat()

    # ------------------------------------------------------------------
    # Markdown
    # ------------------------------------------------------------------

    def to_markdown(self) -> str:
        lines: list[str] = []
        lines.append("# Data Reconciliation Report")
        lines.append(f"Run: {self.generated_at}\n")

        # Summary table
        lines.append("## Summary")
        lines.append(
            "| Table | Type | Status | Match % | Source Rows | Target Rows |"
        )
        lines.append(
            "|-------|------|--------|---------|-------------|-------------|"
        )
        for r in self.results:
            status_icon = _status_icon(r.status)
            lines.append(
                f"| {r.config_name} | {r.reconcile_type.upper()} "
                f"| {status_icon} {r.status} | {r.match_percentage:.1f}% "
                f"| {r.source_row_count:,} | {r.target_row_count:,} |"
            )
        lines.append("")

        # Detail per result
        lines.append("## Detailed Results\n")
        for r in self.results:
            lines.append(f"### {r.config_name}")
            if r.error_message:
                lines.append(f"**Error:** {r.error_message}\n")
                continue

            # Row count
            if r.source_row_count or r.target_row_count:
                rc_status = "PASSED" if r.row_count_match else "FAILED"
                diff = abs(r.source_row_count - r.target_row_count)
                lines.append(
                    f"- **Row Count:** {rc_status} "
                    f"(source: {r.source_row_count:,}, "
                    f"target: {r.target_row_count:,}, diff: {diff:,})"
                )

            # Schema -- only when a schema comparison actually ran. The
            # default schema_diffs is [] (never None) and schema_match
            # defaults False, so every row-count-only run used to print
            # "Schema: FAILED" for a check that never happened.
            schema_ran = (
                r.schema_match
                or bool(r.schema_diffs)
                or str(r.reconcile_type).lower() in ("schema", "all", "full")
            )
            if schema_ran:
                s_status = "PASSED" if r.schema_match else "FAILED"
                lines.append(f"- **Schema:** {s_status}")
                for sd in r.schema_diffs[:20]:
                    lines.append(
                        f"  - {sd.column_name}: {sd.diff_type} "
                        f"(source={sd.source_type}, target={sd.target_type})"
                    )

            # Aggregates
            if r.aggregate_results:
                failed_aggs = [a for a in r.aggregate_results if not a.match]
                if failed_aggs:
                    lines.append(
                        f"- **Aggregates:** {len(failed_aggs)} mismatches"
                    )
                    for a in failed_aggs[:10]:
                        lines.append(
                            f"  - {a.column_name} {a.metric}: "
                            f"source={a.source_value}, "
                            f"target={a.target_value}, "
                            f"diff={a.difference:.6f}"
                        )
                else:
                    lines.append("- **Aggregates:** PASSED")

            # Data mismatches -- also when nothing on the source side was
            # compared: an empty source against a loaded target is exactly the
            # failure a reader needs the detail for.
            if (r.total_rows_compared > 0 or r.missing_in_source or r.missing_in_target
                    or r.mismatched_rows):
                lines.append(
                    f"- **Data:** {r.matching_rows:,} matching, "
                    f"{r.mismatched_rows:,} mismatched, "
                    f"{r.missing_in_target:,} missing in target, "
                    f"{r.missing_in_source:,} missing in source"
                )
                if r.sample_diffs:
                    lines.append("")
                    lines.append("#### Sample Mismatches")
                    lines.append("| Key | Column | Source | Target | Type |")
                    lines.append("|-----|--------|--------|--------|------|")
                    for rd in r.sample_diffs[:20]:
                        key_str = _format_key(rd.key_values)
                        if rd.column_diffs:
                            for cd in rd.column_diffs[:5]:
                                lines.append(
                                    f"| {key_str} | {cd.column_name} "
                                    f"| {cd.source_value} "
                                    f"| {cd.target_value} "
                                    f"| {cd.diff_type} |"
                                )
                        else:
                            lines.append(
                                f"| {key_str} | - | - | - | {rd.diff_type} |"
                            )

            lines.append(
                f"- **Duration:** {r.duration_seconds:.1f}s\n"
            )

        return "\n".join(lines)

    # ------------------------------------------------------------------
    # JSON
    # ------------------------------------------------------------------

    def to_json(self, indent: int = 2) -> str:
        return json.dumps(self._to_dict(), indent=indent, default=str)

    def _to_dict(self) -> dict:
        return {
            "generated_at": self.generated_at,
            "results": [self._result_to_dict(r) for r in self.results],
        }

    @staticmethod
    def _result_to_dict(r: ReconcileResult) -> dict:
        return {
            "config_name": r.config_name,
            "reconcile_type": r.reconcile_type,
            "status": r.status,
            "match_percentage": r.match_percentage,
            "source_row_count": r.source_row_count,
            "target_row_count": r.target_row_count,
            "row_count_match": r.row_count_match,
            "schema_match": r.schema_match,
            "schema_diffs": [
                {
                    "column": sd.column_name,
                    "source_type": sd.source_type,
                    "target_type": sd.target_type,
                    "diff_type": sd.diff_type,
                }
                for sd in (r.schema_diffs or [])
            ],
            "total_rows_compared": r.total_rows_compared,
            "matching_rows": r.matching_rows,
            "mismatched_rows": r.mismatched_rows,
            "missing_in_target": r.missing_in_target,
            "missing_in_source": r.missing_in_source,
            "aggregate_results": [
                {
                    "column": a.column_name,
                    "metric": a.metric,
                    "source_value": a.source_value,
                    "target_value": a.target_value,
                    "difference": a.difference,
                    "match": a.match,
                }
                for a in (r.aggregate_results or [])
            ],
            "sample_diffs": [
                {
                    "key": rd.key_values,
                    "diff_type": rd.diff_type,
                    "columns": [
                        {
                            "name": cd.column_name,
                            "source": cd.source_value,
                            "target": cd.target_value,
                            "type": cd.diff_type,
                        }
                        for cd in (rd.column_diffs or [])
                    ],
                }
                for rd in (r.sample_diffs or [])[:20]
            ],
            "error_message": r.error_message,
            "duration_seconds": r.duration_seconds,
            "started_at": r.started_at,
            "completed_at": r.completed_at,
        }

    # ------------------------------------------------------------------
    # CSV summary
    # ------------------------------------------------------------------

    def to_csv(self) -> str:
        buf = io.StringIO()
        writer = csv.writer(buf)
        writer.writerow([
            "config_name",
            "reconcile_type",
            "status",
            "match_percentage",
            "source_row_count",
            "target_row_count",
            "row_count_match",
            "schema_match",
            "total_rows_compared",
            "matching_rows",
            "mismatched_rows",
            "missing_in_target",
            "missing_in_source",
            "duration_seconds",
            "error_message",
        ])
        for r in self.results:
            writer.writerow([
                r.config_name,
                r.reconcile_type,
                r.status,
                f"{r.match_percentage:.2f}",
                r.source_row_count,
                r.target_row_count,
                r.row_count_match,
                r.schema_match,
                r.total_rows_compared,
                r.matching_rows,
                r.mismatched_rows,
                r.missing_in_target,
                r.missing_in_source,
                f"{r.duration_seconds:.3f}",
                r.error_message,
            ])
        return buf.getvalue()

    # ------------------------------------------------------------------
    # File output helpers
    # ------------------------------------------------------------------

    def save_markdown(self, path: str) -> None:
        with open(path, "w", encoding="utf-8") as f:
            f.write(self.to_markdown())

    def save_json(self, path: str) -> None:
        with open(path, "w", encoding="utf-8") as f:
            f.write(self.to_json())

    def save_csv(self, path: str) -> None:
        with open(path, "w", encoding="utf-8", newline="") as f:
            f.write(self.to_csv())

    def save_all(self, output_dir: str, prefix: str = "reconcile") -> dict[str, str]:
        """Write all three formats and return paths."""
        import os
        os.makedirs(output_dir, exist_ok=True)
        paths = {}
        for fmt, ext, method in [
            ("markdown", "md", self.save_markdown),
            ("json", "json", self.save_json),
            ("csv", "csv", self.save_csv),
        ]:
            path = os.path.join(output_dir, f"{prefix}.{ext}")
            method(path)
            paths[fmt] = path
        return paths


# ------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------

def _status_icon(status: str) -> str:
    return {
        "PASSED": "PASS",
        "FAILED": "FAIL",
        "ERROR": "ERR ",
        "RUNNING": "... ",
        "PENDING": "    ",
    }.get(status, "    ")


def _format_key(key_values: dict) -> str:
    if not key_values:
        return "-"
    return ", ".join(f"{k}={v}" for k, v in key_values.items())
