"""Hallucination Detector — detect and suppress LLM hallucinations in generated PySpark code.

When using Claude to convert Informatica transformations to PySpark,
the LLM may generate code that:
  - Looks syntactically valid but doesn't match the original Informatica logic
  - Invents PySpark functions that don't exist
  - References columns not present in the source XML
  - Adds business logic not in the original expression
  - Makes up table names, schema names, or connection details

This detector compares the generated code against the canonical ConversionSpec
(the structured JSON representation of the original Informatica transformation)
to catch these issues before they reach production.

Detection Layers:
  1. Column Grounding     — every column in code must exist in the spec
  2. Function Grounding   — every F.xxx() must be a real PySpark function
  3. Logic Fidelity       — output fields in spec must appear in code
  4. Fabrication Detection — detect made-up table names, schemas, constants
  5. Cross-Check          — compare LLM output vs rule-based output for drift
  6. Confidence Penalty   — score how likely the code contains hallucinations
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Optional

import infa_compat

from .models import ConversionAttempt, ConversionSpec

# infa_compat is a CLOSED API (see
# engine/infa_compat/SUPPORTED_OPERATIONS.md) -- every name a generated
# notebook may call as `infa_compat.X(...)` is listed in the package's own
# `__all__`. Read live from the installed package rather than retyped here,
# so this set can never drift out of sync with the real library.
INFA_COMPAT_VALID_NAMES = frozenset(infa_compat.__all__)


@dataclass
class HallucinationIssue:
    """A single detected hallucination or suspicious pattern."""
    category: str  # COLUMN, FUNCTION, LOGIC, FABRICATION, DRIFT
    severity: str  # HIGH, MEDIUM, LOW
    description: str
    line_hint: str = ""  # The suspicious line of code
    suggestion: str = ""  # How to fix it


@dataclass
class HallucinationReport:
    """Full hallucination analysis for a code generation attempt."""
    issues: list = field(default_factory=list)  # List[HallucinationIssue]
    hallucination_score: float = 0.0  # 0.0 = clean, 1.0 = definitely hallucinated
    is_clean: bool = True
    summary: str = ""

    def add(self, issue: HallucinationIssue):
        self.issues.append(issue)
        self.is_clean = False


class HallucinationDetector:
    """Detect and flag LLM hallucinations in generated PySpark code.

    Usage:
        detector = HallucinationDetector()
        report = detector.check(attempt)
        if not report.is_clean:
            for issue in report.issues:
                print(f"[{issue.severity}] {issue.category}: {issue.description}")
    """

    # Known PySpark functions (must match validator_agent.py)
    VALID_FUNCTIONS = frozenset({
        "col", "lit", "when", "otherwise", "isnull", "isNull", "coalesce",
        "concat", "concat_ws", "substring", "length", "trim", "ltrim", "rtrim",
        "upper", "lower", "lpad", "rpad", "regexp_replace", "regexp_extract",
        "locate", "instr", "split", "initcap", "translate", "repeat",
        "to_date", "to_timestamp", "date_format", "date_add", "date_sub",
        "add_months", "months_between", "datediff",
        "current_timestamp", "current_date", "trunc", "date_trunc",
        "year", "month", "dayofmonth", "dayofweek", "dayofyear",
        "hour", "minute", "second",
        "round", "floor", "ceil", "abs", "pow", "sqrt", "log", "exp",
        "sum", "count", "avg", "min", "max", "first", "last",
        "countDistinct", "sumDistinct", "approx_count_distinct",
        "collect_list", "collect_set", "array_contains",
        "row_number", "rank", "dense_rank", "lead", "lag",
        "ntile", "percent_rank", "cume_dist",
        "monotonically_increasing_id", "broadcast",
        "explode", "posexplode", "array", "struct", "create_map",
        "greatest", "least", "md5", "sha1", "sha2", "hash", "xxhash64",
        "from_json", "to_json", "from_csv",
        "desc", "asc", "expr", "udf",
        "isnull", "isnan", "nanvl",
        "eqNullSafe",
    })

    # Common hallucinated functions (LLMs make these up frequently)
    # NOTE: F.nvl, F.nvl2, F.ifnull, F.nullif, F.decode and F.substr are
    # REAL pyspark.sql.functions since Spark 3.5 (and validator_agent.py
    # lists them as valid). They were listed here as hallucinations, so a
    # notebook the validator PASSED was then suppressed to a placeholder.
    HALLUCINATED_FUNCTIONS = {
        "iif": "Use F.when(...).otherwise() instead",
        "to_char": "Use F.date_format() instead",
        "to_number": "Use F.col(...).cast() instead",
        "sysdate": "Use F.current_timestamp() instead",
        "rownum": "Use F.row_number().over(Window) instead",
        "str_to_date": "Use F.to_date() instead",
        "date_parse": "Use F.to_date() instead",
        "string_agg": "Use F.collect_list() + F.concat_ws() instead",
        "listagg": "Use F.collect_list() + F.concat_ws() instead",
        "apply": "PySpark DataFrames don't have .apply() — use .withColumn()",
        "iterrows": "PySpark DataFrames don't have .iterrows()",
        "to_csv": "Use .write.csv() instead of .to_csv()",
        "to_pandas": "Avoid — use PySpark native operations",
    }

    def check(self, attempt: ConversionAttempt) -> HallucinationReport:
        """Run all hallucination detection checks on a code generation attempt.

        Args:
            attempt: The ConversionAttempt with generated_code and spec.

        Returns:
            HallucinationReport with all detected issues and a hallucination score.
        """
        report = HallucinationReport()
        code = attempt.generated_code
        spec = attempt.spec

        if not code or not code.strip():
            return report

        # Layer 1: Column grounding
        self._check_column_grounding(code, spec, report)

        # Layer 2: Function grounding
        self._check_function_grounding(code, report)

        # Layer 3: Logic fidelity
        self._check_logic_fidelity(code, spec, report)

        # Layer 4: Fabrication detection
        self._check_fabrication(code, spec, report)

        # Layer 5: Cross-check with rule-based output
        if hasattr(attempt, 'rule_based_code') and attempt.rule_based_code:
            self._check_drift(code, attempt.rule_based_code, spec, report)

        # Compute hallucination score
        report.hallucination_score = self._compute_score(report)
        report.summary = self._summarize(report)

        return report

    # ── Layer 1: Column Grounding ───────────────────────────────────────

    def _check_column_grounding(
        self, code: str, spec: Optional[ConversionSpec], report: HallucinationReport
    ):
        """Verify every column reference in code exists in the spec."""
        if not spec:
            return

        # Collect known columns from spec
        known_columns = set()
        for f in spec.inputs:
            known_columns.add(f.get("name", "").upper())
        for f in spec.outputs:
            known_columns.add(f.get("name", "").upper())

        # Also add columns from the logic dict
        if isinstance(spec.logic, dict):
            for key in ("group_by_fields", "sort_keys"):
                for col in spec.logic.get(key, []):
                    if isinstance(col, dict):
                        known_columns.add(col.get("field", col.get("name", "")).upper())
                    elif isinstance(col, str):
                        known_columns.add(col.upper())
            for expr_item in spec.logic.get("expressions", []):
                if isinstance(expr_item, dict):
                    known_columns.add(expr_item.get("field", "").upper())

        if not known_columns:
            return

        # Extract column references from code: F.col("NAME") or col("NAME")
        col_refs = set()
        for match in re.finditer(r'(?:F\.)?col\(["\'](\w+)["\']\)', code):
            col_refs.add(match.group(1).upper())

        # Also catch df["NAME"] patterns
        for match in re.finditer(r'\w+\[["\'](\w+)["\']\]', code):
            col_refs.add(match.group(1).upper())

        # Check each referenced column against known columns
        for col in col_refs:
            if col not in known_columns:
                # Could be a derived column from a previous transformation — only flag
                # if it's clearly not a standard column name
                report.add(HallucinationIssue(
                    category="COLUMN",
                    severity="MEDIUM",
                    description=f"Column '{col}' referenced in code but not found in XML spec inputs/outputs",
                    line_hint=f"F.col('{col}')",
                    suggestion=f"Verify '{col}' exists in the source data or is created by a prior transformation",
                ))

    # ── Layer 2: Function Grounding ─────────────────────────────────────

    def _check_function_grounding(self, code: str, report: HallucinationReport):
        """Detect hallucinated PySpark functions and hallucinated infa_compat calls."""
        # infa_compat is a closed API (SUPPORTED_OPERATIONS.md)
        # -- the whole point of closing it is that this check can now be exact
        # instead of the best-effort VALID_FUNCTIONS/HALLUCINATED_FUNCTIONS
        # heuristics below. Any `infa_compat.<name>(...)` call whose <name>
        # isn't in the live package's own `__all__` is a fabrication, full stop.
        for match in re.finditer(r'infa_compat\.(\w+)\s*\(', code):
            name = match.group(1)
            if name not in INFA_COMPAT_VALID_NAMES:
                report.add(HallucinationIssue(
                    category="FUNCTION",
                    severity="HIGH",
                    description=(
                        f"Fabricated call infa_compat.{name}() — not part of the "
                        f"closed infa_compat API (see "
                        f"engine/infa_compat/SUPPORTED_OPERATIONS.md)"
                    ),
                    line_hint=f"infa_compat.{name}(",
                    suggestion=(
                        "Use one of the real infa_compat functions "
                        f"({', '.join(sorted(INFA_COMPAT_VALID_NAMES))}) or, if the "
                        "mapping genuinely needs a Class C semantic not listed "
                        "there, flag it in a comment instead of inventing a call."
                    ),
                ))

        # Extract all F.xxx() calls
        func_calls = set(re.findall(r'F\.(\w+)\s*\(', code))

        # Check for known hallucinated functions
        for func in func_calls:
            func_lower = func.lower()
            if func_lower in self.HALLUCINATED_FUNCTIONS:
                report.add(HallucinationIssue(
                    category="FUNCTION",
                    severity="HIGH",
                    description=f"Hallucinated function F.{func}() — this does not exist in PySpark",
                    line_hint=f"F.{func}(",
                    suggestion=self.HALLUCINATED_FUNCTIONS[func_lower],
                ))
            elif func not in self.VALID_FUNCTIONS and func_lower not in self.VALID_FUNCTIONS:
                # Unknown function — may or may not exist
                report.add(HallucinationIssue(
                    category="FUNCTION",
                    severity="MEDIUM",
                    description=f"Unknown function F.{func}() — not in the known PySpark function list",
                    line_hint=f"F.{func}(",
                    suggestion="Verify this function exists in pyspark.sql.functions",
                ))

        # Check for pandas-style methods (common LLM hallucination)
        pandas_patterns = [
            (r'\.apply\(', "FUNCTION", "HIGH", ".apply() does not exist on PySpark DataFrames"),
            (r'\.iterrows\(', "FUNCTION", "HIGH", ".iterrows() does not exist on PySpark DataFrames"),
            (r'\.to_csv\(', "FUNCTION", "HIGH", "Use .write.csv() instead of .to_csv()"),
            (r'\.to_dict\(', "FUNCTION", "MEDIUM", ".to_dict() is a pandas method, not PySpark"),
            (r'import pandas', "FUNCTION", "HIGH", "Using pandas instead of PySpark"),
            (r'pd\.DataFrame', "FUNCTION", "HIGH", "Using pandas DataFrame instead of PySpark"),
        ]
        for pattern, cat, sev, desc in pandas_patterns:
            if re.search(pattern, code):
                report.add(HallucinationIssue(
                    category=cat, severity=sev, description=desc,
                    line_hint=re.findall(pattern, code)[0] if re.findall(pattern, code) else "",
                ))

    # ── Layer 3: Logic Fidelity ─────────────────────────────────────────

    def _check_logic_fidelity(
        self, code: str, spec: Optional[ConversionSpec], report: HallucinationReport
    ):
        """Verify the generated code implements the spec's logic."""
        if not spec or not spec.logic:
            return

        logic = spec.logic

        # Check: all output fields from spec should appear in the code
        for f in spec.outputs:
            field_name = f.get("name", "")
            if field_name and field_name not in code:
                report.add(HallucinationIssue(
                    category="LOGIC",
                    severity="MEDIUM",
                    description=f"Output field '{field_name}' from spec is not referenced in generated code",
                    suggestion=f"The code should produce a column named '{field_name}'",
                ))

        # Check: expressions from spec should be reflected in code
        for expr_item in logic.get("expressions", []):
            if isinstance(expr_item, dict):
                expr = expr_item.get("expression", "")
                field = expr_item.get("field", "")
                # If expression is not a pass-through and field doesn't appear in code
                if expr and expr != field and field and field not in code:
                    report.add(HallucinationIssue(
                        category="LOGIC",
                        severity="HIGH",
                        description=f"Expression '{field} = {expr}' from spec is missing in generated code",
                        suggestion=f"Code should include: df.withColumn('{field}', <converted expression>)",
                    ))

        # Check: filter condition from spec
        condition = logic.get("condition", "")
        if condition and "filter" not in code.lower() and "where" not in code.lower():
            report.add(HallucinationIssue(
                category="LOGIC",
                severity="HIGH",
                description=f"Filter condition '{condition}' from spec but no .filter() or .where() in code",
                suggestion="The code should filter the DataFrame based on the spec condition",
            ))

        # Check: join condition from spec
        join_cond = logic.get("join_condition", "")
        if join_cond and "join" not in code.lower():
            report.add(HallucinationIssue(
                category="LOGIC",
                severity="HIGH",
                description=f"Join condition '{join_cond}' from spec but no .join() in code",
                suggestion="The code should join DataFrames based on the spec condition",
            ))

        # Check: group by from spec
        group_by = logic.get("group_by_fields", [])
        if group_by and "groupBy" not in code and "groupby" not in code.lower():
            report.add(HallucinationIssue(
                category="LOGIC",
                severity="HIGH",
                description=f"Group by fields {group_by} from spec but no .groupBy() in code",
                suggestion="The code should group the DataFrame on the specified fields",
            ))

    # ── Layer 4: Fabrication Detection ──────────────────────────────────

    def _check_fabrication(
        self, code: str, spec: Optional[ConversionSpec], report: HallucinationReport
    ):
        """Detect fabricated table names, schemas, connection details."""
        # Detect hardcoded connection strings (LLMs often make these up)
        suspicious_patterns = [
            (r'jdbc:oracle:thin:@\w+:\d+/\w+', "FABRICATION", "HIGH",
             "Hardcoded JDBC URL — likely hallucinated. Use spark.conf.get() instead"),
            (r'password\s*=\s*["\'][^"\']+["\']', "FABRICATION", "HIGH",
             "Hardcoded password in generated code — security risk and likely fabricated"),
            (r'username\s*=\s*["\'][^"\']+["\']', "FABRICATION", "MEDIUM",
             "Hardcoded username in generated code — should use spark.conf or secrets"),
            (r'host\s*=\s*["\'][^"\']+["\']', "FABRICATION", "MEDIUM",
             "Hardcoded host — likely hallucinated. Use spark.conf.get() instead"),
        ]

        for pattern, cat, sev, desc in suspicious_patterns:
            matches = re.findall(pattern, code)
            if matches:
                report.add(HallucinationIssue(
                    category=cat, severity=sev, description=desc,
                    line_hint=matches[0][:50],
                ))

        # Detect suspiciously specific numbers (LLMs hallucinate these)
        # E.g., "0.08" tax rate that wasn't in the spec
        if spec and spec.logic:
            spec_text = str(spec.logic)
            # Find numeric literals in code that aren't in the spec
            code_numbers = set(re.findall(r'\b\d+\.\d+\b', code))
            spec_numbers = set(re.findall(r'\b\d+\.\d+\b', spec_text))
            fabricated_numbers = code_numbers - spec_numbers - {"0.0", "1.0", "0.01", "100.0"}
            for num in fabricated_numbers:
                report.add(HallucinationIssue(
                    category="FABRICATION",
                    severity="LOW",
                    description=f"Numeric literal {num} in code but not in spec — verify it's correct",
                    line_hint=num,
                    suggestion="Check if this value comes from the original Informatica mapping",
                ))

    # ── Layer 5: Cross-Check (rule-based vs LLM) ───────────────────────

    def _check_drift(
        self, llm_code: str, rule_code: str,
        spec: Optional[ConversionSpec], report: HallucinationReport
    ):
        """Compare LLM output against rule-based output for significant drift."""
        if not rule_code or not rule_code.strip():
            return

        # Extract column operations from both
        llm_columns = set(re.findall(r'withColumn\(["\'](\w+)["\']', llm_code))
        rule_columns = set(re.findall(r'withColumn\(["\'](\w+)["\']', rule_code))

        # Columns in LLM output but not in rule-based
        extra_columns = llm_columns - rule_columns
        for col in extra_columns:
            report.add(HallucinationIssue(
                category="DRIFT",
                severity="MEDIUM",
                description=f"LLM added column '{col}' that rule-based converter did not produce",
                suggestion="Verify this column is needed — it may be hallucinated",
            ))

        # Columns in rule-based but missing from LLM output
        missing_columns = rule_columns - llm_columns
        for col in missing_columns:
            report.add(HallucinationIssue(
                category="DRIFT",
                severity="HIGH",
                description=f"LLM is missing column '{col}' that rule-based converter produced",
                suggestion="This column should be in the output — the LLM may have dropped it",
            ))

    # ── Scoring ─────────────────────────────────────────────────────────

    def _compute_score(self, report: HallucinationReport) -> float:
        """Compute a 0.0-1.0 hallucination risk score."""
        if not report.issues:
            return 0.0

        score = 0.0
        for issue in report.issues:
            if issue.severity == "HIGH":
                score += 0.25
            elif issue.severity == "MEDIUM":
                score += 0.1
            elif issue.severity == "LOW":
                score += 0.03

        return min(1.0, score)

    def _summarize(self, report: HallucinationReport) -> str:
        """Generate a human-readable summary."""
        if report.is_clean:
            return "No hallucinations detected — code is grounded in the spec"

        high = sum(1 for i in report.issues if i.severity == "HIGH")
        med = sum(1 for i in report.issues if i.severity == "MEDIUM")
        low = sum(1 for i in report.issues if i.severity == "LOW")

        score_label = "LOW RISK"
        if report.hallucination_score > 0.5:
            score_label = "HIGH RISK"
        elif report.hallucination_score > 0.2:
            score_label = "MEDIUM RISK"

        return (
            f"Hallucination check: {score_label} "
            f"(score: {report.hallucination_score:.2f}) — "
            f"{high} HIGH, {med} MEDIUM, {low} LOW issues"
        )

    # ── Convenience method for pipeline integration ─────────────────────

    def check_and_suppress(
        self, attempt: ConversionAttempt, suppress_threshold: float = 0.5
    ) -> tuple[ConversionAttempt, HallucinationReport]:
        """Check for hallucinations and suppress code if score exceeds threshold.

        If the hallucination score is above the threshold, the generated code
        is replaced with a TODO comment explaining what went wrong.

        Args:
            attempt: The code generation attempt to check.
            suppress_threshold: Score above which to suppress (default: 0.5).

        Returns:
            Tuple of (modified attempt, report).
        """
        report = self.check(attempt)

        if report.hallucination_score >= suppress_threshold:
            # Suppress the hallucinated code
            original_code = attempt.generated_code
            spec_desc = ""
            if attempt.spec:
                spec_desc = f"Type: {attempt.spec.transformation_type}, Name: {attempt.spec.transformation_name}"

            suppressed_lines = [
                f"# HALLUCINATION DETECTED — LLM output suppressed (score: {report.hallucination_score:.2f})",
                f"# {spec_desc}",
                f"# Issues found:",
            ]
            for issue in report.issues:
                suppressed_lines.append(f"#   [{issue.severity}] {issue.category}: {issue.description}")
            suppressed_lines.append(f"#")
            suppressed_lines.append(f"# Original LLM output (for reference):")
            for line in original_code.split("\n")[:10]:
                suppressed_lines.append(f"#   {line}")
            if len(original_code.split("\n")) > 10:
                suppressed_lines.append(f"#   ... ({len(original_code.split(chr(10))) - 10} more lines)")
            suppressed_lines.append(f"#")
            suppressed_lines.append(f"# TODO: Manual conversion required for this transformation")
            suppressed_lines.append(f"df_out = df  # Placeholder — replace with correct PySpark")

            attempt.generated_code = "\n".join(suppressed_lines)
            attempt.confidence = max(0.0, attempt.confidence - 0.5)

        return attempt, report
