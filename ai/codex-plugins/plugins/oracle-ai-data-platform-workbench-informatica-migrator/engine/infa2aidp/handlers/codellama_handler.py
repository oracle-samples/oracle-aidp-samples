"""Claude (Anthropic API) integration for complex transformation conversion.

Converts Informatica expressions, stored procedures, and SQL to PySpark via
the Anthropic Claude API. Requires ANTHROPIC_API_KEY. The default model is
Opus 5 (overridable via CLAUDE_MODEL).
"""

import hashlib
import json
import os
import re
import time

try:
    import anthropic as _anthropic_mod
except ImportError:
    _anthropic_mod = None

from .. import config as _cfg

DEFAULT_CLAUDE_MODEL = _cfg.CLAUDE_MODEL

_CLAUDE_SYSTEM_PROMPT = (
    "You are an expert Informatica PowerCenter to PySpark migration specialist. "
    "Convert the given Informatica expression/code to equivalent PySpark code. "
    "Return ONLY the PySpark code, no explanations."
)


class LLMHandler:
    """Converts Informatica expressions to PySpark using the Claude API."""

    def __init__(
        self,
        timeout: int = 30,
        max_retries: int = 3,
        claude_model: str = DEFAULT_CLAUDE_MODEL,
    ):
        self.timeout = timeout
        self.max_retries = max_retries
        self.claude_model = claude_model
        self._cache: dict[str, str] = {}
        self._claude_client = None
        self._init_claude_client()

    # ------------------------------------------------------------------
    # Claude setup
    # ------------------------------------------------------------------

    def _init_claude_client(self):
        """Create the anthropic client using ANTHROPIC_API_KEY."""
        if _anthropic_mod is None:
            return
        api_key = os.environ.get("ANTHROPIC_API_KEY")
        if api_key:
            self._claude_client = _anthropic_mod.Anthropic(api_key=api_key)

    def is_available(self) -> bool:
        return self._claude_client is not None

    def generate(
        self,
        prompt: str,
        system: str | None = None,
        max_tokens: int | None = None,
        timeout: int | None = None,
    ) -> str:
        """Long-form generation, matching ``OpenAIHandler.generate``.

        The notebook generator and the validator both fall back to
        ``llm.generate(prompt)`` when they cannot find a ``_claude_client``
        to stream through. That fallback used to be dead code -- this class
        had no ``generate`` at all -- so it would have raised AttributeError
        had it ever been reached. It is implemented here so the two handlers
        present the same surface and the fallback means what it says.
        """
        if not self.is_available():
            raise RuntimeError(
                "Anthropic is not configured: set ANTHROPIC_API_KEY, or run "
                "without --use-llm for rule-based conversion only."
            )
        from ..generators.llm_notebook_generator import _extract_text

        client = self._claude_client
        if timeout is not None:
            client = client.with_options(timeout=timeout)
        return _extract_text(client.messages.create(
            model=self.claude_model,
            max_tokens=max_tokens or 4096,
            system=system or _CLAUDE_SYSTEM_PROMPT,
            messages=[{"role": "user", "content": prompt}],
        ))

    def _call_claude(self, prompt: str) -> str:
        # _extract_text (shared with llm_notebook_generator / llm_validator):
        # Opus 5 runs adaptive thinking by default, so content[0] is a
        # `thinking` block, not text -- content[0].text raises
        # AttributeError. 4096 max_tokens is well under the non-streaming
        # ceiling, so this stays on messages.create().
        from ..generators.llm_notebook_generator import _extract_text

        message = self._claude_client.messages.create(
            model=self.claude_model,
            max_tokens=4096,
            system=_CLAUDE_SYSTEM_PROMPT,
            messages=[{"role": "user", "content": prompt}],
        )
        raw = _extract_text(message)
        return self._extract_code(raw)

    # ------------------------------------------------------------------
    # Public conversion methods
    # ------------------------------------------------------------------

    def convert_expression(self, infa_expression: str, context: str = "") -> str:
        prompt = (
            "Convert this Informatica PowerCenter expression to PySpark code.\n\n"
            f"Informatica expression:\n{infa_expression}\n\n"
        )
        if context:
            prompt += f"Context: {context}\n\n"
        prompt += (
            "Requirements:\n"
            "- Use pyspark.sql.functions as F\n"
            "- Return only the PySpark expression, no explanation\n"
            "- Handle NULL values properly\n"
            "- Use F.col() for column references\n"
        )
        return self._call(prompt, fallback_original=infa_expression)

    def convert_stored_procedure(self, sp_name: str, sp_body: str, params: list) -> str:
        params_str = ", ".join(params) if params else "(none)"
        prompt = (
            "Convert this Oracle stored procedure to PySpark code.\n\n"
            f"Procedure name: {sp_name}\n"
            f"Parameters: {params_str}\n\n"
            f"Procedure body:\n{sp_body}\n\n"
            "Requirements:\n"
            "- Use pyspark.sql.functions as F\n"
            "- Implement as a Python function that accepts a SparkSession\n"
            "- Handle NULL values properly\n"
            "- Return only the PySpark code, no explanation\n"
        )
        return self._call(prompt, fallback_original=sp_body)

    def convert_complex_sql(self, sql: str, source_dialect: str = "oracle") -> str:
        prompt = (
            f"Convert this {source_dialect.upper()} SQL to Spark SQL.\n\n"
            f"Original SQL:\n{sql}\n\n"
            "Requirements:\n"
            "- Use Spark SQL syntax\n"
            "- Replace vendor-specific functions with Spark equivalents\n"
            "- Return only the Spark SQL, no explanation\n"
        )
        return self._call(prompt, fallback_original=sql)

    def generate_merge_statement(
        self,
        target_table: str,
        source_df: str,
        key_columns: list,
        update_columns: list,
    ) -> str:
        keys = ", ".join(key_columns)
        updates = ", ".join(update_columns)
        prompt = (
            "Generate a Delta Lake MERGE INTO statement in PySpark.\n\n"
            f"Target table: {target_table}\n"
            f"Source DataFrame: {source_df}\n"
            f"Key (join) columns: {keys}\n"
            f"Columns to update on match: {updates}\n\n"
            "Requirements:\n"
            "- Use delta.tables.DeltaTable API\n"
            "- Include WHEN MATCHED UPDATE and WHEN NOT MATCHED INSERT\n"
            "- Return only the PySpark code, no explanation\n"
        )
        return self._call(
            prompt,
            fallback_original=f"MERGE INTO {target_table} USING {source_df}",
        )

    def explain_transformation(self, transformation: dict) -> str:
        prompt = (
            "Explain this Informatica PowerCenter transformation in plain English "
            "for documentation purposes. Be concise (2-4 sentences).\n\n"
            f"Transformation details:\n{json.dumps(transformation, indent=2, default=str)}\n"
        )
        return self._call(prompt, fallback_original=str(transformation))

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _cache_key(self, prompt: str) -> str:
        return hashlib.sha256(prompt.encode()).hexdigest()

    def _call(self, prompt: str, fallback_original: str = "") -> str:
        key = self._cache_key(prompt)
        if key in self._cache:
            return self._cache[key]

        if not self.is_available():
            return self._fallback(fallback_original)

        for attempt in range(1, self.max_retries + 1):
            try:
                code = self._call_claude(prompt)
                self._cache[key] = code
                return code
            except Exception:
                if attempt < self.max_retries:
                    time.sleep(2 ** attempt)
                    continue
                return self._fallback(fallback_original)

        return self._fallback(fallback_original)

    @staticmethod
    def _extract_code(text: str) -> str:
        """Strip markdown fences and explanatory text, return only code."""
        match = re.search(r"```(?:\w*)\n(.*?)```", text, re.DOTALL)
        if match:
            return match.group(1).strip()
        return text.strip()

    @staticmethod
    def _fallback(original: str) -> str:
        """Return a TODO comment with the original expression."""
        lines = original.strip().splitlines()
        commented = "\n".join(f"# {line}" for line in lines)
        return (
            "# TODO: Manual conversion required — Claude API unavailable\n"
            f"{commented}\n"
        )


# Backward-compatible alias.
CodeLlamaHandler = LLMHandler
