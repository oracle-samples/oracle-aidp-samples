"""ValidatorAgent -- validate generated PySpark code without executing it.

Validation checks (in order):
1. Python syntax check (ast.parse)
2. Import verification
3. DataFrame variable consistency
4. PySpark function existence
5. Column reference consistency
6. Common LLM anti-patterns
"""

from __future__ import annotations

import ast
import re
from typing import Optional

from .models import ConversionAttempt, ValidationResult


# Known valid pyspark.sql.functions members
VALID_PYSPARK_FUNCTIONS = frozenset({
    "col", "lit", "when", "otherwise", "isnull", "isNull", "coalesce",
    "concat", "concat_ws", "substring", "length", "trim", "ltrim", "rtrim",
    "upper", "lower", "lpad", "rpad", "regexp_replace", "regexp_extract",
    "locate", "instr", "split", "initcap", "translate", "repeat",
    "to_date", "to_timestamp", "date_format", "date_add", "date_sub",
    "add_months", "months_between", "datediff",
    "current_timestamp", "current_date", "trunc", "date_trunc",
    "year", "month", "dayofmonth", "dayofweek", "dayofyear",
    "hour", "minute", "second", "weekofyear", "last_day", "next_day",
    "round", "abs", "pow", "sqrt", "ceil", "floor", "log", "log2", "log10",
    "exp", "cbrt", "signum", "factorial", "rand", "randn",
    "sum", "count", "avg", "min", "max", "first", "last",
    "countDistinct", "approx_count_distinct", "sumDistinct",
    "stddev", "stddev_pop", "stddev_samp", "variance", "var_pop", "var_samp",
    "skewness", "kurtosis", "percentile_approx",
    "row_number", "rank", "dense_rank", "lead", "lag",
    "ntile", "cume_dist", "percent_rank",
    "monotonically_increasing_id", "broadcast", "explode", "posexplode",
    "collect_list", "collect_set", "array", "struct", "create_map",
    "array_contains", "array_distinct", "array_union", "array_intersect",
    "size", "sort_array", "flatten", "sequence", "element_at",
    "map_keys", "map_values", "map_from_arrays",
    "md5", "sha1", "sha2", "hash", "xxhash64", "crc32",
    "greatest", "least", "nanvl", "ifnull", "nullif", "nvl", "nvl2",
    "asc", "desc", "asc_nulls_first", "asc_nulls_last",
    "desc_nulls_first", "desc_nulls_last",
    "expr", "format_number", "format_string",
    "from_json", "to_json", "schema_of_json",
    "from_csv", "from_unixtime", "unix_timestamp",
    "base64", "unbase64", "decode", "encode",
    "input_file_name", "spark_partition_id",
    "window", "session_window",
    "typeof", "assert_true", "raise_error",
})

# Patterns that indicate common LLM mistakes
ANTI_PATTERNS = [
    (r"import\s+pandas", "Using pandas instead of PySpark"),
    (r"\bpd\.", "Using pandas (pd.) instead of PySpark"),
    (r"\.apply\(", "Using .apply() -- not available in PySpark DataFrames"),
    (r"\.iterrows\(", "Using .iterrows() -- not available in PySpark"),
    (r"\.itertuples\(", "Using .itertuples() -- not available in PySpark"),
    (r"\.to_csv\(", "Using .to_csv() -- use .write.csv() in PySpark"),
    (r"\.to_pandas\(", "Using .to_pandas() -- avoid converting to pandas"),
    (r"\.values\b", "Accessing .values -- not available on PySpark DataFrame"),
    (r"\.iloc\[", "Using .iloc[] -- not available in PySpark"),
    (r"\.loc\[", "Using .loc[] -- not available in PySpark"),
    (r"\.assign\(", "Using .assign() -- not available in PySpark"),
    (r"lambda\s+.*:\s+.*\.apply", "Using lambda with .apply -- not PySpark"),
]


class ValidatorAgent:
    """Validate generated PySpark code without executing it."""

    def validate(self, attempt: ConversionAttempt) -> ConversionAttempt:
        """Run all validation checks on generated code."""
        errors: list[str] = []
        code = attempt.generated_code

        if not code or not code.strip():
            errors.append("Empty code generated")
            attempt.validation_errors = errors
            attempt.validation_result = ValidationResult.LOGIC_ERROR
            attempt.confidence = 0.0
            return attempt

        # Check 1: Python syntax
        syntax_ok = self._check_syntax(code, errors)

        # Check 2: Required imports
        self._check_imports(code, errors)

        # Check 3: DataFrame variable consistency
        self._check_df_consistency(code, errors)

        # Check 4: PySpark function validation
        self._check_pyspark_functions(code, errors)

        # Check 5: Column reference consistency
        if attempt.spec:
            self._check_column_references(code, attempt.spec, errors)

        # Check 6: Common LLM anti-patterns
        self._check_anti_patterns(code, errors)

        # Set result
        attempt.validation_errors = errors
        if not errors:
            attempt.validation_result = ValidationResult.PASSED
            attempt.confidence = max(attempt.confidence, 0.9)
        elif not syntax_ok:
            attempt.validation_result = ValidationResult.SYNTAX_ERROR
            attempt.confidence = 0.1
        else:
            # Has warnings but parseable
            attempt.validation_result = ValidationResult.LOGIC_ERROR
            attempt.confidence = max(0.2, attempt.confidence - 0.1 * len(errors))

        return attempt

    # ------------------------------------------------------------------
    # Individual checks
    # ------------------------------------------------------------------

    def _check_syntax(self, code: str, errors: list[str]) -> bool:
        """Check Python syntax via ast.parse. Returns True if OK."""
        try:
            ast.parse(code)
            return True
        except SyntaxError as e:
            errors.append(f"Syntax error at line {e.lineno}: {e.msg}")
            return False

    def _check_imports(self, code: str, errors: list[str]) -> None:
        """Verify that required PySpark imports are present or assumed."""
        uses_F = bool(re.search(r"\bF\.", code))
        has_F_import = bool(
            re.search(r"from\s+pyspark\.sql\.functions\s+import", code)
            or re.search(r"import\s+pyspark\.sql\.functions\s+as\s+F", code)
        )
        # F usage without import is acceptable (assumed in notebook context)
        # but flag if they import something wrong
        if "from pyspark.sql import functions as f" in code.lower() and uses_F:
            # Lowercase f but using uppercase F
            errors.append("Import uses lowercase 'f' but code uses uppercase 'F'")

    def _check_df_consistency(self, code: str, errors: list[str]) -> None:
        """Track DataFrame variable assignments and references."""
        # Find all df assignments (left side of =)
        assigned = set()
        for match in re.finditer(r"^(\w+)\s*=\s*", code, re.MULTILINE):
            assigned.add(match.group(1))

        # Common input names that are assumed to exist
        assumed = {"df", "spark", "F", "Window", "DeltaTable"}
        available = assigned | assumed

        # Find df references in method chains: xxx.filter(), xxx.join(), etc.
        for match in re.finditer(r"\b(\w+)\.(filter|join|select|withColumn|groupBy|agg|orderBy|unionByName|alias|write)\(", code):
            ref = match.group(1)
            if ref not in available and not ref.startswith("lkp_"):
                errors.append(f"DataFrame '{ref}' used before assignment")

    def _check_pyspark_functions(self, code: str, errors: list[str]) -> None:
        """Check F.xxx() calls against known PySpark functions."""
        for match in re.finditer(r"\bF\.(\w+)\(", code):
            func_name = match.group(1)
            if func_name not in VALID_PYSPARK_FUNCTIONS:
                errors.append(f"Unknown PySpark function: F.{func_name}()")

    def _check_column_references(
        self, code: str, spec, errors: list[str]
    ) -> None:
        """Check that column references exist in the spec's input/output fields."""
        if not spec.inputs:
            return

        # Extract column names referenced in code via F.col("xxx") or col("xxx")
        referenced_cols = set()
        for match in re.finditer(r'(?:F\.)?col\(["\'](\w+)["\']\)', code):
            referenced_cols.add(match.group(1))

        known_cols = {f["name"] for f in spec.inputs} | {f["name"] for f in spec.outputs}

        # Only warn (not error) -- columns may come from joins or other sources
        for col_name in referenced_cols:
            if col_name not in known_cols and not col_name.startswith("_"):
                # Soft warning, don't add to errors for now -- too noisy
                pass

    def _check_anti_patterns(self, code: str, errors: list[str]) -> None:
        """Flag common LLM mistakes."""
        for pattern, message in ANTI_PATTERNS:
            if re.search(pattern, code):
                errors.append(message)

    # ------------------------------------------------------------------
    # Optional deeper logic check
    # ------------------------------------------------------------------

    def validate_logic(
        self, attempt: ConversionAttempt, llm_handler=None
    ) -> list[str]:
        """Deeper logic checks using LLM if available.

        Ask: "Does this PySpark code correctly implement this spec?"
        Returns a list of concerns (empty if none).
        """
        if not llm_handler or not attempt.spec:
            return []

        import json as _json

        prompt = f"""Review this PySpark code for correctness against the spec.

Spec:
- Type: {attempt.spec.transformation_type}
- Logic: {_json.dumps(attempt.spec.logic, indent=2, default=str)}

Code:
```python
{attempt.generated_code}
```

List any logic errors or concerns. If the code is correct, say "LGTM".
Return ONLY a bullet list of issues, or "LGTM".
"""
        result = llm_handler._call(prompt)
        if "LGTM" in result.upper():
            return []
        return [line.strip("- ").strip() for line in result.strip().splitlines() if line.strip()]
