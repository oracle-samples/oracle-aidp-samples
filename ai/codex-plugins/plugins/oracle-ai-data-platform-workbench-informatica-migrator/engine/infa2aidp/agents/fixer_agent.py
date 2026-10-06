"""FixerAgent -- fix errors in generated PySpark code.

Strategy:
1. Syntax errors -> try auto-fix (common patterns)
2. Runtime errors -> use LLM with error context
3. Logic errors -> use LLM with spec + error description
"""

from __future__ import annotations

import json
import re
from typing import Optional

from .models import ConversionAttempt, ConversionSpec, ValidationResult


class FixerAgent:
    """Fix errors in generated PySpark code."""

    def __init__(self, llm_handler=None):
        self.llm = llm_handler

    def fix(self, attempt: ConversionAttempt) -> ConversionAttempt:
        """Attempt to fix errors in the code. Returns a new attempt."""
        new_attempt = ConversionAttempt(
            attempt_number=attempt.attempt_number + 1,
            spec=attempt.spec,
            agent_used="fixer",
        )

        code = attempt.generated_code
        errors = list(attempt.validation_errors)

        # Step 1: Try auto-fixes (no LLM needed)
        code, auto_fixed = self._auto_fix(code, errors)

        # Step 2: If auto-fix didn't resolve everything -> LLM
        if errors and self.llm and not auto_fixed:
            code = self._llm_fix(code, errors, attempt.spec)
            new_attempt.fix_applied = "llm-fix"
        elif auto_fixed:
            new_attempt.fix_applied = "auto-fix"
        else:
            new_attempt.fix_applied = "no-fix-available"

        new_attempt.generated_code = code
        return new_attempt

    # ------------------------------------------------------------------
    # Auto-fix patterns
    # ------------------------------------------------------------------

    def _auto_fix(self, code: str, errors: list[str]) -> tuple[str, bool]:
        """Try common auto-fixes. Returns (fixed_code, was_fixed)."""
        fixed = False

        for error in list(errors):
            lower = error.lower()

            if "syntax error" in lower:
                new_code = self._fix_syntax(code)
                if new_code != code:
                    code = new_code
                    fixed = True

            elif "using pandas" in lower or "using .apply()" in lower:
                new_code = self._fix_pandas_usage(code)
                if new_code != code:
                    code = new_code
                    fixed = True

            elif "unknown pyspark function" in lower:
                func_match = re.search(r"F\.(\w+)\(\)", error)
                if func_match:
                    new_code = self._fix_unknown_function(code, func_match.group(1))
                    if new_code != code:
                        code = new_code
                        fixed = True

            elif "before assignment" in lower:
                new_code = self._fix_undefined_var(code, error)
                if new_code != code:
                    code = new_code
                    fixed = True

            elif "import" in lower:
                new_code = self._fix_import(code, error)
                if new_code != code:
                    code = new_code
                    fixed = True

        return code, fixed

    def _fix_syntax(self, code: str) -> str:
        """Fix common syntax issues: unbalanced parens, missing colons."""
        # Fix unbalanced parentheses
        open_count = code.count("(")
        close_count = code.count(")")
        if open_count > close_count:
            code = code.rstrip() + ")" * (open_count - close_count)
        elif close_count > open_count:
            # Remove trailing extra parens
            lines = code.split("\n")
            for i in range(len(lines) - 1, -1, -1):
                while lines[i].rstrip().endswith(")") and close_count > open_count:
                    lines[i] = lines[i].rstrip()[:-1]
                    close_count -= 1
                if close_count == open_count:
                    break
            code = "\n".join(lines)

        # Fix unbalanced brackets
        open_b = code.count("[")
        close_b = code.count("]")
        if open_b > close_b:
            code = code.rstrip() + "]" * (open_b - close_b)

        # Fix unbalanced quotes in strings (triple quotes excluded)
        # Only fix simple cases
        lines = code.split("\n")
        for i, line in enumerate(lines):
            stripped = line.lstrip("#").strip()
            if stripped.count('"') % 2 != 0 and '"""' not in stripped:
                lines[i] = line + '"'
        code = "\n".join(lines)

        return code

    def _fix_pandas_usage(self, code: str) -> str:
        """Replace common pandas patterns with PySpark equivalents."""
        # import pandas -> remove
        code = re.sub(r"import\s+pandas\s+as\s+pd\n?", "", code)
        code = re.sub(r"import\s+pandas\n?", "", code)

        # .apply(lambda ...) -> withColumn pattern (best effort)
        # This is a rough heuristic; LLM should handle complex cases
        code = re.sub(
            r"\.apply\(lambda\s+\w+:\s*(.+?)\)",
            r"  # TODO: replace .apply() with .withColumn() + PySpark UDF",
            code,
        )

        # .iterrows() -> flag
        code = re.sub(
            r"for\s+\w+,\s*\w+\s+in\s+\w+\.iterrows\(\):",
            "# TODO: replace iterrows loop with DataFrame operations",
            code,
        )

        # .to_csv -> .write.csv
        code = re.sub(r"\.to_csv\(", ".write.csv(", code)

        # pd.DataFrame -> spark.createDataFrame
        code = re.sub(r"pd\.DataFrame\(", "spark.createDataFrame(", code)

        return code

    def _fix_unknown_function(self, code: str, func_name: str) -> str:
        """Replace unknown F.xxx() with a known equivalent or TODO."""
        # Common misnamed functions
        replacements = {
            "isNull": "isnull",
            "is_null": "isnull",
            "ifnull": "coalesce",
            "if_null": "coalesce",
            "string": "lit",
            "to_string": "col",  # usually F.col(...).cast("string")
            "to_int": "col",     # usually F.col(...).cast("int")
            "to_integer": "col",
            "substr": "substring",
            "len": "length",
            "now": "current_timestamp",
            "today": "current_date",
            "str": "lit",
            "int": "lit",
            "float": "lit",
            "power": "pow",
            "ceiling": "ceil",
            "truncate": "trunc",
            "string_concat": "concat",
            "date_diff": "datediff",
        }

        if func_name in replacements:
            replacement = replacements[func_name]
            code = code.replace(f"F.{func_name}(", f"F.{replacement}(")
        else:
            # Wrap in expr() as fallback
            code = code.replace(
                f"F.{func_name}(",
                f"F.expr(\"{func_name}(\"  # TODO: verify function name\n# F.{func_name}(",
            )

        return code

    def _fix_undefined_var(self, code: str, error: str) -> str:
        """Add missing variable definitions."""
        match = re.search(r"DataFrame '(\w+)' used before assignment", error)
        if not match:
            return code
        var_name = match.group(1)

        # If it looks like a lookup df, add a placeholder
        if var_name.startswith("lkp_"):
            table_hint = var_name.replace("lkp_", "")
            code = (
                f'{var_name} = spark.read.table("{table_hint}")  # TODO: set correct table\n'
                + code
            )
        elif var_name.endswith("_master"):
            code = f"{var_name} = df  # TODO: set master DataFrame source\n" + code
        else:
            code = f"{var_name} = df  # TODO: set source for {var_name}\n" + code

        return code

    def _fix_import(self, code: str, error: str) -> str:
        """Add missing imports."""
        if "F." in code and "import" not in code:
            code = "from pyspark.sql import functions as F\n" + code
        if "Window" in code and "from pyspark.sql.window" not in code:
            code = "from pyspark.sql.window import Window\n" + code
        if "DeltaTable" in code and "from delta.tables" not in code:
            code = "from delta.tables import DeltaTable\n" + code
        return code

    # ------------------------------------------------------------------
    # LLM-based fix
    # ------------------------------------------------------------------

    def _llm_fix(
        self, code: str, errors: list[str], spec: Optional[ConversionSpec]
    ) -> str:
        """Use LLM to fix errors with full error context."""
        error_text = "\n".join(f"- {e}" for e in errors)

        spec_context = ""
        if spec:
            spec_context = f"""
Original spec: {spec.transformation_type} -- {spec.transformation_name}
Expected behavior: {json.dumps(spec.logic, indent=2, default=str)}
"""

        prompt = f"""Fix the following PySpark code that has validation errors.
{spec_context}
Code with errors:
```python
{code}
```

Errors found:
{error_text}

Fix ALL errors and return ONLY the corrected PySpark code.
- Use pyspark.sql.functions as F
- Use proper DataFrame API (no pandas)
- Variable names: input is `df`, output is `df_out`
"""
        return self.llm._call(prompt)
