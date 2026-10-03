"""Conversion confidence scoring for Informatica-to-AIDP migration.

Scores each transformation conversion on a HIGH / MEDIUM / LOW / MANUAL
scale so the migration report clearly shows what is auto-convertible and
what needs developer attention.
"""

import os
import re
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Optional

from infa2aidp.models import Mapping, Transformation, TransformationType


class ConversionConfidence(Enum):
    HIGH = "HIGH"       # Direct 1:1 mapping, no ambiguity
    MEDIUM = "MEDIUM"   # Converted with some assumptions
    LOW = "LOW"         # LLM-assisted, needs review
    MANUAL = "MANUAL"   # Cannot auto-convert, requires manual work


@dataclass
class ConversionScore:
    transformation_name: str
    transformation_type: str
    confidence: ConversionConfidence = ConversionConfidence.HIGH
    conversion_method: str = "rule-based"  # rule-based, llm-assisted, manual
    score: int = 100  # 0-100
    issues: list = field(default_factory=list)
    notes: list = field(default_factory=list)
    original_code: str = ""
    converted_code: str = ""


# -----------------------------------------------------------------------
# Classification sets (keyed by TransformationType enum values)
# -----------------------------------------------------------------------

_HIGH_CONFIDENCE_TYPES = {
    TransformationType.SOURCE_QUALIFIER,  # spark.read
    TransformationType.FILTER,            # .filter()
    TransformationType.SORTER,            # .orderBy()
    TransformationType.UNION,             # .unionByName()
}

_MEDIUM_CONFIDENCE_TYPES = {
    # A PowerCenter sequence is a persisted, restart-safe counter with
    # cache/cycle/reset semantics; the generated infa_compat.sequence()
    # call has never run on a live cluster. Not HIGH.
    TransformationType.SEQUENCE_GENERATOR,
    TransformationType.EXPRESSION,        # depends on complexity
    TransformationType.AGGREGATOR,        # .groupBy().agg()
    TransformationType.JOINER,            # .join()
    TransformationType.ROUTER,            # multiple .filter()
    TransformationType.RANK,              # Window functions
    TransformationType.UPDATE_STRATEGY,   # Delta MERGE
    TransformationType.NORMALIZER,        # explode()
}

_LOW_CONFIDENCE_TYPES = {
    TransformationType.LOOKUP,            # complex join logic
    TransformationType.SQL,               # arbitrary SQL
    TransformationType.TRANSACTION_CONTROL,
}

_MANUAL_REQUIRED_TYPES = {
    TransformationType.STORED_PROCEDURE,
    TransformationType.CUSTOM,
    TransformationType.JAVA,
    TransformationType.HTTP,
    TransformationType.XML_PARSER,
    TransformationType.XML_GENERATOR,
}

# Informatica functions that have straightforward PySpark equivalents
_SIMPLE_FUNCTIONS = {
    "NVL", "IIF", "TO_DATE", "TO_CHAR", "TO_INTEGER", "TO_DECIMAL",
    "TO_FLOAT", "LTRIM", "RTRIM", "TRIM", "UPPER", "LOWER", "SUBSTR",
    "LENGTH", "LPAD", "RPAD", "INSTR", "REPLACE_CHR", "CONCAT",
    "SYSDATE", "ADD_TO_DATE", "DATE_DIFF", "TRUNC", "ROUND", "ABS",
    "MOD", "POWER", "IS_SPACES", "IS_NUMBER", "REG_MATCH",
}

# Functions with no direct PySpark equivalent (need custom UDF or logic)
_UNSUPPORTED_FUNCTIONS = {
    "SETVARIABLE", "GETVAR", "ABORT", "ERROR", "LOOKUP",
    "MAX", "MIN",  # Informatica aggregate in expression context
    "MOVINGAVG", "MOVINGSUM", "CUME", "FIRST", "LAST",
    "GET_DATE_PART",
}

# Effort estimates (person-hours per transformation)
_EFFORT_HOURS = {
    ConversionConfidence.HIGH: 0.0,
    ConversionConfidence.MEDIUM: 0.5,
    ConversionConfidence.LOW: 2.0,
    ConversionConfidence.MANUAL: 8.0,
}


class ConfidenceScorer:
    """Score conversion confidence for each transformation."""

    # ------------------------------------------------------------------
    # Single transformation
    # ------------------------------------------------------------------

    def score_transformation(
        self,
        transformation: Transformation,
        converted_code: str = "",
        conversion_method: str = "rule-based",
    ) -> ConversionScore:
        """Score a single transformation conversion."""
        tx = transformation
        cs = ConversionScore(
            transformation_name=tx.name,
            transformation_type=tx.type.value,
            conversion_method=conversion_method,
        )

        # Start with base confidence from transformation type
        base = self._base_confidence(tx.type)
        cs.confidence = base
        cs.score = self._confidence_to_score(base)

        # Refine for Expression transformations
        if tx.type == TransformationType.EXPRESSION:
            self._score_expression(tx, cs)

        # Refine for Lookup transformations
        elif tx.type == TransformationType.LOOKUP:
            self._score_lookup(tx, cs)

        # Refine for Source Qualifier with SQL override
        elif tx.type == TransformationType.SOURCE_QUALIFIER and tx.sql_override:
            cs.confidence = ConversionConfidence.MEDIUM
            cs.score = 65
            cs.notes.append("SQL override present on Source Qualifier")
            if self._has_oracle_specific_sql(tx.sql_override):
                cs.confidence = ConversionConfidence.LOW
                cs.score = 40
                cs.issues.append("SQL override contains Oracle-specific syntax")

        # LLM-assisted conversions are capped at MEDIUM unless already MANUAL
        if conversion_method == "llm-assisted" and cs.confidence == ConversionConfidence.HIGH:
            cs.confidence = ConversionConfidence.MEDIUM
            cs.score = min(cs.score, 70)
            cs.notes.append("LLM-assisted conversion — verify output")

        if conversion_method == "manual":
            cs.confidence = ConversionConfidence.MANUAL
            cs.score = 0

        cs.converted_code = converted_code
        return cs

    # ------------------------------------------------------------------
    # Expression scoring
    # ------------------------------------------------------------------

    def _score_expression(self, tx: Transformation, cs: ConversionScore):
        """Dig deeper into Expression transformations."""
        expressions = self._collect_expressions(tx)
        if not expressions:
            # Pure pass-through
            cs.confidence = ConversionConfidence.HIGH
            cs.score = 100
            cs.notes.append("Pass-through expression")
            return

        combined = " ".join(expressions)
        upper = combined.upper()

        # Check for $$ parameters
        if "$$" in combined:
            cs.confidence = ConversionConfidence.MEDIUM
            cs.score = min(cs.score, 65)
            cs.issues.append("Contains $$ parameters — need parameter mapping")

        # Nesting depth
        max_depth = self._max_nesting_depth(combined)
        if max_depth >= 3:
            self._downgrade(cs, ConversionConfidence.MEDIUM, 60)
            cs.notes.append(f"Nested functions ({max_depth} levels deep)")

        # DECODE with many cases
        decode_matches = re.findall(r"DECODE\s*\(", upper)
        if decode_matches:
            # Count commas in the outermost DECODE to estimate cases
            for expr in expressions:
                if "DECODE" in expr.upper():
                    comma_count = expr.count(",")
                    if comma_count > 10:  # >5 case pairs
                        self._downgrade(cs, ConversionConfidence.MEDIUM, 55)
                        cs.notes.append(f"DECODE with many cases ({comma_count // 2}+)")

        # Unsupported functions
        found_unsupported = []
        for fn in _UNSUPPORTED_FUNCTIONS:
            if re.search(rf"\b{fn}\s*\(", upper):
                found_unsupported.append(fn)
        if found_unsupported:
            self._downgrade(cs, ConversionConfidence.LOW, 35)
            cs.issues.append(f"Unsupported functions: {', '.join(found_unsupported)}")

        # Multiple nested IIF with aggregates
        iif_count = len(re.findall(r"\bIIF\s*\(", upper))
        has_agg = bool(re.search(r"\b(SUM|AVG|COUNT|MAX|MIN)\s*\(", upper))
        if iif_count >= 3 and has_agg:
            self._downgrade(cs, ConversionConfidence.LOW, 30)
            cs.issues.append("Multiple nested IIF with aggregate functions")

        # If nothing triggered a downgrade, check if all functions are simple
        if cs.confidence == ConversionConfidence.MEDIUM:
            all_fns = set(re.findall(r"\b([A-Z_]+)\s*\(", upper))
            if all_fns and all_fns.issubset(_SIMPLE_FUNCTIONS):
                cs.confidence = ConversionConfidence.HIGH
                cs.score = 90
                cs.notes.append("All functions have direct PySpark equivalents")

    # ------------------------------------------------------------------
    # Lookup scoring
    # ------------------------------------------------------------------

    def _score_lookup(self, tx: Transformation, cs: ConversionScore):
        """Refine confidence for Lookup transformations."""
        # Unconnected lookups. "Lookup policy on multiple match" is a
        # TABLEATTRIBUTE on EVERY PowerCenter lookup, so testing its presence
        # (as this used to) scored every lookup LOW as "unconnected". A
        # lookup is unconnected when nothing in the mapping is wired into
        # it (it is called from expressions as :LKP.<name>(...)); the
        # explicit connection_type property is honoured when a parser sets it.
        is_unconnected = bool(tx.properties.get("unconnected")) or (
            str(tx.properties.get("connection_type", "")).lower() == "unconnected"
        )
        if is_unconnected:
            self._downgrade(cs, ConversionConfidence.LOW, 35)
            cs.issues.append("Unconnected lookup — complex join pattern")
            return

        # Lookup with SQL override
        if tx.lookup_sql:
            self._downgrade(cs, ConversionConfidence.LOW, 35)
            cs.issues.append("Lookup has SQL override")
            if self._has_oracle_specific_sql(tx.lookup_sql):
                cs.score = min(cs.score, 25)
                cs.issues.append("Lookup SQL contains Oracle-specific syntax")
            return

        # Complex condition
        if tx.lookup_condition and len(tx.lookup_condition) > 100:
            self._downgrade(cs, ConversionConfidence.LOW, 40)
            cs.issues.append("Complex lookup condition")
            return

        # Simple connected lookup
        cs.confidence = ConversionConfidence.MEDIUM
        cs.score = 70
        cs.notes.append("Simple connected lookup")

    # ------------------------------------------------------------------
    # Mapping-level scoring
    # ------------------------------------------------------------------

    def score_mapping(
        self,
        mapping: Mapping,
        conversion_results: Optional[dict] = None,
    ) -> list[ConversionScore]:
        """Score all transformations in a mapping."""
        results = conversion_results or {}
        scores = []
        for tx in mapping.transformations:
            code = results.get(tx.name, "")
            method = "llm-assisted" if "# LLM" in code else "rule-based"
            if "# TODO: Manual conversion required" in code:
                method = "manual"
            scores.append(self.score_transformation(tx, code, method))
        return scores

    # ------------------------------------------------------------------
    # Summary
    # ------------------------------------------------------------------

    def get_mapping_summary(self, scores: list[ConversionScore]) -> dict:
        """Summarize: % HIGH, % MEDIUM, % LOW, % MANUAL, overall conversion rate."""
        total = len(scores)
        if total == 0:
            return {
                "total": 0,
                "high": 0, "medium": 0, "low": 0, "manual": 0,
                "high_pct": 0.0, "medium_pct": 0.0,
                "low_pct": 0.0, "manual_pct": 0.0,
                "auto_convertible_pct": 0.0,
                "avg_score": 0.0,
                "estimated_effort_hours": 0.0,
            }

        counts = {c: 0 for c in ConversionConfidence}
        total_score = 0
        effort = 0.0
        for s in scores:
            counts[s.confidence] += 1
            total_score += s.score
            effort += _EFFORT_HOURS.get(s.confidence, 0)

        pct = lambda n: round(n / total * 100, 1)
        auto = counts[ConversionConfidence.HIGH] + counts[ConversionConfidence.MEDIUM]

        return {
            "total": total,
            "high": counts[ConversionConfidence.HIGH],
            "medium": counts[ConversionConfidence.MEDIUM],
            "low": counts[ConversionConfidence.LOW],
            "manual": counts[ConversionConfidence.MANUAL],
            "high_pct": pct(counts[ConversionConfidence.HIGH]),
            "medium_pct": pct(counts[ConversionConfidence.MEDIUM]),
            "low_pct": pct(counts[ConversionConfidence.LOW]),
            "manual_pct": pct(counts[ConversionConfidence.MANUAL]),
            "auto_convertible_pct": pct(auto),
            "avg_score": round(total_score / total, 1),
            "estimated_effort_hours": round(effort, 1),
        }

    # ------------------------------------------------------------------
    # Report generation
    # ------------------------------------------------------------------

    def generate_confidence_report(
        self,
        all_scores: list[ConversionScore],
        output_path: str,
    ) -> str:
        """Generate a detailed confidence report in markdown. Returns the file path."""
        summary = self.get_mapping_summary(all_scores)

        lines = [
            "# Conversion Confidence Report",
            "",
            f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
            "",
            "## Overall Summary",
            "",
            f"| Metric | Value |",
            f"|--------|------:|",
            f"| Total transformations | {summary['total']} |",
            f"| Auto-convertible (HIGH + MEDIUM) | {summary['high'] + summary['medium']} ({summary['auto_convertible_pct']}%) |",
            f"| HIGH confidence | {summary['high']} ({summary['high_pct']}%) |",
            f"| MEDIUM confidence | {summary['medium']} ({summary['medium_pct']}%) |",
            f"| LOW confidence (needs review) | {summary['low']} ({summary['low_pct']}%) |",
            f"| MANUAL (requires developer) | {summary['manual']} ({summary['manual_pct']}%) |",
            f"| Average score | {summary['avg_score']} / 100 |",
            f"| Estimated manual effort | {summary['estimated_effort_hours']} hours |",
            "",
            "## Per-Transformation Breakdown",
            "",
            "| Transformation | Type | Confidence | Score | Method | Issues |",
            "|---------------|------|-----------|------:|--------|--------|",
        ]

        # Sort: MANUAL first, then LOW, MEDIUM, HIGH
        priority = {
            ConversionConfidence.MANUAL: 0,
            ConversionConfidence.LOW: 1,
            ConversionConfidence.MEDIUM: 2,
            ConversionConfidence.HIGH: 3,
        }
        sorted_scores = sorted(all_scores, key=lambda s: priority.get(s.confidence, 4))

        for s in sorted_scores:
            issues_str = "; ".join(s.issues) if s.issues else "-"
            lines.append(
                f"| {s.transformation_name} | {s.transformation_type} "
                f"| {s.confidence.value} | {s.score} "
                f"| {s.conversion_method} | {issues_str} |"
            )

        # MANUAL items detail section
        manual_items = [s for s in all_scores if s.confidence == ConversionConfidence.MANUAL]
        if manual_items:
            lines.extend([
                "",
                "## Items Requiring Manual Conversion",
                "",
                "These transformations cannot be auto-converted and need developer attention.",
                "",
            ])
            for s in manual_items:
                lines.append(f"### {s.transformation_name} ({s.transformation_type})")
                lines.append("")
                if s.issues:
                    for issue in s.issues:
                        lines.append(f"- {issue}")
                if s.notes:
                    for note in s.notes:
                        lines.append(f"- {note}")
                effort = _EFFORT_HOURS.get(s.confidence, 0)
                lines.append(f"- Estimated effort: {effort} hours")
                lines.append("")

        # LOW items with side-by-side code
        low_items = [s for s in all_scores if s.confidence == ConversionConfidence.LOW]
        if low_items:
            lines.extend([
                "",
                "## LOW Confidence Items (Review Required)",
                "",
            ])
            for s in low_items:
                lines.append(f"### {s.transformation_name} ({s.transformation_type})")
                lines.append("")
                if s.issues:
                    for issue in s.issues:
                        lines.append(f"- {issue}")
                lines.append("")
                if s.original_code:
                    lines.append("**Original:**")
                    lines.append("```")
                    lines.append(s.original_code)
                    lines.append("```")
                    lines.append("")
                if s.converted_code:
                    lines.append("**Converted:**")
                    lines.append("```python")
                    lines.append(s.converted_code)
                    lines.append("```")
                    lines.append("")

        # Effort summary
        lines.extend([
            "",
            "## Effort Estimate",
            "",
            "| Confidence | Count | Hours/Each | Total Hours |",
            "|-----------|------:|-----------:|------------:|",
        ])
        for conf in ConversionConfidence:
            count = sum(1 for s in all_scores if s.confidence == conf)
            if count > 0:
                per = _EFFORT_HOURS[conf]
                total_h = count * per
                lines.append(f"| {conf.value} | {count} | {per} | {total_h} |")

        lines.append(
            f"\n**Total estimated manual effort: {summary['estimated_effort_hours']} hours**"
        )
        lines.append("")

        report_text = "\n".join(lines)
        os.makedirs(output_path, exist_ok=True)
        file_path = os.path.join(output_path, "confidence_report.md")
        with open(file_path, "w", encoding="utf-8") as f:
            f.write(report_text)
        return file_path

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _base_confidence(self, tx_type: TransformationType) -> ConversionConfidence:
        if tx_type in _HIGH_CONFIDENCE_TYPES:
            return ConversionConfidence.HIGH
        if tx_type in _MEDIUM_CONFIDENCE_TYPES:
            return ConversionConfidence.MEDIUM
        if tx_type in _LOW_CONFIDENCE_TYPES:
            return ConversionConfidence.LOW
        if tx_type in _MANUAL_REQUIRED_TYPES:
            return ConversionConfidence.MANUAL
        # Unknown types default to LOW
        return ConversionConfidence.LOW

    @staticmethod
    def _confidence_to_score(conf: ConversionConfidence) -> int:
        return {
            ConversionConfidence.HIGH: 95,
            ConversionConfidence.MEDIUM: 70,
            ConversionConfidence.LOW: 35,
            ConversionConfidence.MANUAL: 10,
        }[conf]

    @staticmethod
    def _downgrade(cs: ConversionScore, to: ConversionConfidence, max_score: int):
        """Downgrade confidence only (never upgrade)."""
        order = [ConversionConfidence.HIGH, ConversionConfidence.MEDIUM,
                 ConversionConfidence.LOW, ConversionConfidence.MANUAL]
        if order.index(to) > order.index(cs.confidence):
            cs.confidence = to
        cs.score = min(cs.score, max_score)

    @staticmethod
    def _collect_expressions(tx: Transformation) -> list[str]:
        """Collect all non-trivial expressions from transformation fields."""
        exprs = []
        for fld in tx.fields:
            expr = getattr(fld, "expression", "")
            if expr and expr.strip():
                # Skip pure field references (just a column name)
                if not re.match(r"^[A-Za-z_][A-Za-z0-9_.]*$", expr.strip()):
                    exprs.append(expr)
        return exprs

    @staticmethod
    def _max_nesting_depth(expr: str) -> int:
        """Count maximum parenthesis nesting depth."""
        depth = 0
        max_d = 0
        for ch in expr:
            if ch == "(":
                depth += 1
                max_d = max(max_d, depth)
            elif ch == ")":
                depth -= 1
        return max_d

    @staticmethod
    def _has_oracle_specific_sql(sql: str) -> bool:
        """Quick check for Oracle-specific SQL patterns."""
        upper = sql.upper()
        patterns = [
            r"\bCONNECT\s+BY\b", r"\bSTART\s+WITH\b", r"\bROWNUM\b",
            r"\bDBMS_", r"\bUTL_", r"\bSYS_CONTEXT\b", r"\(\+\)",
            r"\bDECODE\s*\(", r"\bNVL2\s*\(",
        ]
        return any(re.search(p, upper) for p in patterns)
