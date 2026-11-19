"""Spark optimization engine for generated AIDP notebooks.

Analyzes PySpark code (as text) and applies performance optimizations
specific to AIDP/Spark.
"""

from __future__ import annotations

import re
from pathlib import Path

from .models import OptimizationReport, OptimizationSuggestion
from .rules import (
    ALL_RULES,
    AQERule,
    BroadcastJoinRule,
    CacheReuseRule,
    ColumnPruningRule,
    DeltaOptimizeRule,
    OptimizationRule,
    ParallelismRule,
    RepartitionRule,
    UDFEliminationRule,
)


class SparkOptimizer:
    """Analyze generated PySpark notebooks and suggest/apply optimizations."""

    def __init__(self, rules: list[OptimizationRule] | None = None):
        if rules is not None:
            self.rules = rules
        else:
            self.rules = [cls() for cls in ALL_RULES]

    def optimize(
        self,
        notebook_code: str,
        context: dict | None = None,
    ) -> OptimizationReport:
        """Analyze notebook code and return optimization suggestions."""
        report = OptimizationReport()

        if context:
            report.notebook_name = context.get("notebook_name", "")

        for rule in self.rules:
            hits = rule.analyze(notebook_code, context)
            report.suggestions.extend(hits)

        report.compute_counts()
        report.estimated_overall_improvement = self._estimate_overall(report)
        return report

    def apply_to_file(self, path: str, raw: str) -> bool:
        """Apply the auto-applicable optimizations to a notebook file in
        place: cell by cell for an ``.ipynb`` (the JSON stays a notebook --
        writing the joined code back used to replace it with plain text),
        the whole text for a ``.py``. Returns whether anything changed."""
        import json

        if not path.endswith(".ipynb"):
            code, applied = self.apply_auto_optimizations(raw)
            if applied:
                with open(path, "w", encoding="utf-8") as f:
                    f.write(code)
            return bool(applied)
        nb = json.loads(raw)
        changed = False
        for cell in nb.get("cells", []):
            if cell.get("cell_type") != "code":
                continue
            src = "".join(cell.get("source", []))
            code, applied = self.apply_auto_optimizations(src)
            if applied:
                cell["source"] = code.splitlines(keepends=True)
                changed = True
        if changed:
            with open(path, "w", encoding="utf-8") as f:
                json.dump(nb, f, indent=1)
        return changed

    def apply_auto_optimizations(
        self,
        notebook_code: str,
        context: dict | None = None,
    ) -> tuple[str, list[OptimizationSuggestion]]:
        """Apply auto-applicable optimizations and return modified code + what was changed."""
        report = self.optimize(notebook_code, context)
        modified_code = notebook_code
        applied: list[OptimizationSuggestion] = []

        # Sort by line number descending so replacements don't shift offsets
        auto_suggestions = sorted(
            [s for s in report.suggestions if s.auto_applicable],
            key=lambda s: s.line_number,
            reverse=True,
        )

        for suggestion in auto_suggestions:
            if not suggestion.original_code or not suggestion.optimized_code:
                continue

            new_code = modified_code.replace(
                suggestion.original_code,
                suggestion.optimized_code,
                1,  # replace first occurrence only
            )

            if new_code != modified_code:
                modified_code = new_code
                applied.append(suggestion)

        return modified_code, applied

    def generate_report(
        self,
        reports: list[OptimizationReport],
        output_path: str | None = None,
    ) -> str:
        """Generate a markdown optimization report for all notebooks.

        Returns the markdown string. If output_path is given, also writes
        the report to that file.
        """
        total_suggestions = sum(r.total_suggestions for r in reports)
        total_auto = sum(r.auto_applicable for r in reports)
        total_manual = sum(r.manual_review for r in reports)

        lines = [
            "# Optimization Report",
            "",
            "## Summary",
            f"- Notebooks analyzed: {len(reports)}",
            f"- Total suggestions: {total_suggestions}",
            f"- Auto-applicable: {total_auto}",
            f"- Manual review needed: {total_manual}",
            f"- Estimated improvement: {self._aggregate_improvement(reports)}",
            "",
        ]

        for report in reports:
            if not report.suggestions:
                continue

            lines.append(f"## {report.notebook_name or 'Unnamed Notebook'}")
            lines.append("")
            lines.append("| # | Rule | Priority | Auto | Description |")
            lines.append("|---|------|----------|------|-------------|")

            for idx, s in enumerate(report.suggestions, 1):
                auto = "Yes" if s.auto_applicable else "No"
                desc = s.description.replace("|", "\\|")
                lines.append(
                    f"| {idx} | {s.rule_name} | {s.priority} | {auto} | {desc} |"
                )

            lines.append("")

            # Detail section with code suggestions
            lines.append("<details>")
            lines.append(f"<summary>Code suggestions for {report.notebook_name or 'notebook'}</summary>")
            lines.append("")

            for idx, s in enumerate(report.suggestions, 1):
                lines.append(f"### {idx}. {s.rule_name} — {s.applies_to}")
                lines.append("")
                if s.original_code:
                    lines.append("**Before:**")
                    lines.append("```python")
                    lines.append(s.original_code)
                    lines.append("```")
                    lines.append("")
                if s.optimized_code:
                    lines.append("**After:**")
                    lines.append("```python")
                    lines.append(s.optimized_code)
                    lines.append("```")
                    lines.append("")
                if s.estimated_improvement:
                    lines.append(f"*Estimated improvement: {s.estimated_improvement}*")
                    lines.append("")

            lines.append("</details>")
            lines.append("")

        md = "\n".join(lines)

        if output_path:
            Path(output_path).write_text(md, encoding="utf-8")

        return md

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _estimate_overall(report: OptimizationReport) -> str:
        """Produce a rough overall improvement estimate from suggestion priorities."""
        high_count = sum(1 for s in report.suggestions if s.priority in ("HIGH", "CRITICAL"))
        medium_count = sum(1 for s in report.suggestions if s.priority == "MEDIUM")

        if high_count >= 3:
            return "3-5x faster execution"
        elif high_count >= 1:
            return "2-3x faster execution"
        elif medium_count >= 2:
            return "1.5-2x faster execution"
        elif report.suggestions:
            return "Minor performance gains"
        return "No optimizations needed"

    @staticmethod
    def _aggregate_improvement(reports: list[OptimizationReport]) -> str:
        """Aggregate improvement estimate across multiple reports."""
        all_suggestions = [s for r in reports for s in r.suggestions]
        high_count = sum(1 for s in all_suggestions if s.priority in ("HIGH", "CRITICAL"))
        total = len(all_suggestions)

        if high_count >= 5:
            return "3-5x faster execution"
        elif high_count >= 2:
            return "2-3x faster execution"
        elif total >= 3:
            return "1.5-2x faster execution"
        elif total:
            return "Minor performance gains"
        return "No optimizations needed"
