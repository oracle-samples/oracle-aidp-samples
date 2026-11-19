"""LLM-based Migration Validator.

Sends the mapping spec (JSON, normalized from either a PowerCenter XML or
an IICS/IDMC JSON export -- see llm_notebook_generator._mapping_to_spec)
plus the generated notebook to the LLM for validation, exactly like
uploading both to Claude.ai manually. Returns a structured score and list
of issues.

Architecture:
  Mapping Spec (JSON) + Generated Notebook → LLM Validator → Score + Issues

IMPORTANT — this score is CIRCULAR, not an independent check:
the spec above was built from the SAME parse that fed the generator, so if
the parser silently dropped a construct, this validator never sees it
either -- both are blind to the gap identically, and a notebook missing
that construct can still score 100/100. As of, a low score routes
the mapping to human review; it no longer drives a feed-back-and-regenerate
loop, because regenerating against the same spec cannot repair a gap the
spec never described. ``source_fidelity.py`` is the independent check that
compares the notebook against the RAW export instead of this spec.
"""

import json
import logging
import re
from dataclasses import dataclass, field
from typing import Optional

logger = logging.getLogger(__name__)

_VALIDATOR_SYSTEM_PROMPT = """You are an expert Informatica PowerCenter to PySpark migration validator.

You receive:
1. An Informatica mapping specification (JSON) describing the original ETL logic
2. A generated PySpark notebook that claims to implement this mapping

Your job: validate that the notebook correctly implements EVERY transformation in the XML spec.

CHECK EACH OF THESE:
1. DATA FLOW: Does the notebook follow the CONNECTOR graph? Are transforms in correct order?
2. JOIN DIRECTION: For "Master Outer" joins, the DETAIL source must be on the LEFT.
   Master Source is the SMALLER/cached table. Detail is preserved (all rows kept).
   df_detail.join(df_master, ..., "left") — NOT df_master.join(df_detail, ..., "left")
3. ROUTER LINEAGE: After Router splits, do downstream transforms operate on the CORRECT
   split DataFrame? (e.g., EXP_ALERT_REASON on df_suspicious, not df_all)
4. DD_DELETE: Does DD_DELETE only delete from the connected TARGET table?
   No inserts, no source table modifications.
5. DD_INSERT: Is it using append mode (not merge)?
6. AGGREGATOR: If agg feeds a lookup, is it a side-branch (separate df joined back)?
   Or does it overwrite the main pipeline df?
7. COLUMN NAMES: Do downstream transforms reference columns that actually exist?
   Check LKP_ aliases, connector renames, expression outputs.
8. SEQUENCE GENERATOR: Does it use the Current Value offset? Is it a side-input?
9. SOURCE FILTERS: Are WHERE clauses from Source Qualifier SQL applied?
10. TARGET WRITES: Does each target get ONLY its defined columns?
11. NATURAL KEYS: Are merge conditions using natural keys (not surrogate _SK)?
12. EXPRESSION LOGIC: Do IIF/DECODE/DATE_DIFF translations match the XML?
13. JAVA TRANSFORMATION: If ported, is the logic additive (score += points)?

OUTPUT FORMAT — Return ONLY this JSON, nothing else:
{
  "score": <0-100>,
  "critical_issues": [
    {"step": "transform_name", "issue": "description", "fix": "what to change"}
  ],
  "warnings": [
    {"step": "transform_name", "issue": "description", "fix": "what to change"}
  ],
  "info": [
    {"step": "transform_name", "issue": "description"}
  ],
  "correct_steps": ["step1", "step2", ...]
}

SCORING:
- Start at 100
- Each critical issue: -15 points
- Each warning: -5 points
- Each info: -1 point
- Minimum score: 0

Be thorough but fair. Only flag real issues, not style preferences."""


@dataclass
class ValidationResult:
    """Result of validating a migration."""
    score: int = 0
    critical_issues: list = field(default_factory=list)
    warnings: list = field(default_factory=list)
    info: list = field(default_factory=list)
    correct_steps: list = field(default_factory=list)
    raw_response: str = ""
    attempt: int = 0


class LLMMigrationValidator:
    """Validates generated notebooks against XML specs using LLM."""

    def __init__(self, llm_handler):
        self.llm = llm_handler

    def validate(
        self,
        spec_json: str,
        notebook_json: str,
        mapping_name: str,
    ) -> ValidationResult:
        """Validate a generated notebook against the XML spec.

        Args:
            spec_json: The canonical mapping spec (JSON string)
            notebook_json: The generated notebook content (ipynb JSON string)
            mapping_name: Name for logging

        Returns:
            ValidationResult with score and issues
        """
        # Extract code cells from notebook
        notebook_code = self._extract_code(notebook_json)

        prompt = f"""Validate this PySpark notebook against the Informatica mapping specification.

## Informatica Mapping Specification
```json
{spec_json}
```

## Generated PySpark Notebook Code
```python
{notebook_code}
```

Validate every transformation step. Return ONLY the JSON result."""

        logger.info("Validating '%s' with LLM (%d chars)", mapping_name, len(prompt))

        try:
            raw = self._call_llm(prompt)
            result = self._parse_result(raw)
            result.raw_response = raw
            logger.info("Validation score for '%s': %d/100 (%d critical, %d warnings)",
                        mapping_name, result.score,
                        len(result.critical_issues), len(result.warnings))
            return result
        except Exception as exc:
            logger.error("Validation failed: %s", exc)
            return ValidationResult(score=0, raw_response=str(exc))

    def _call_llm(self, prompt: str) -> str:
        """Call LLM with retry.

        Uses ``_extract_text`` (shared with ``llm_notebook_generator``, the
        only other place that calls Claude directly) rather than
        ``message.content[0].text``: Opus 5 runs adaptive thinking by
        default, so content[0] is a ``thinking`` block, not text --
        indexing [0].text raises AttributeError. Confirmed live against
        the real API during development: this exact call broke with
        ``'ThinkingBlock' object has no attribute 'text'`` under
        claude-opus-5 before this fix, silently collapsing every
        validation score to 0 (caught by the broad except in
        ``validate()``). 4096 max_tokens is well under the non-streaming
        ceiling, so this call stays on ``messages.create()`` -- only
        ``llm_notebook_generator``'s much larger max_tokens needs
        streaming.
        """
        import time
        from .llm_notebook_generator import _extract_text

        max_retries = 3
        for attempt in range(max_retries):
            try:
                if hasattr(self.llm, '_claude_client') and self.llm._claude_client:
                    message = self.llm._claude_client.messages.create(
                        model=self.llm.claude_model,
                        max_tokens=4096,
                        system=_VALIDATOR_SYSTEM_PROMPT,
                        messages=[{"role": "user", "content": prompt}],
                    )
                    return _extract_text(message)
                # Non-Anthropic provider: carry the VALIDATOR system
                # prompt, not the handler's default expression prompt.
                return self.llm.generate(
                    prompt, system=_VALIDATOR_SYSTEM_PROMPT, max_tokens=4096,
                ) or ""
            except Exception as exc:
                if "429" in str(exc) or "rate_limit" in str(exc).lower():
                    wait = (attempt + 1) * 30
                    logger.warning("Rate limited — waiting %ds (%d/%d)", wait, attempt + 1, max_retries)
                    time.sleep(wait)
                    continue
                raise
        raise RuntimeError("Validation LLM call failed after retries")

    def _parse_result(self, raw: str) -> ValidationResult:
        """Parse LLM validation response into ValidationResult."""
        raw = raw.strip()
        # Remove markdown fences
        if raw.startswith("```"):
            first_nl = raw.index("\n")
            last_fence = raw.rfind("```")
            if last_fence > first_nl:
                raw = raw[first_nl + 1:last_fence].strip()

        try:
            data = json.loads(raw)
        except json.JSONDecodeError:
            # Try to find JSON in response
            match = re.search(r'\{[^{}]*"score"[^{}]*\}', raw, re.DOTALL)
            if match:
                try:
                    data = json.loads(match.group())
                except json.JSONDecodeError:
                    return ValidationResult(score=50, raw_response=raw)
            else:
                return ValidationResult(score=50, raw_response=raw)

        return ValidationResult(
            score=data.get("score", 50),
            critical_issues=data.get("critical_issues", []),
            warnings=data.get("warnings", []),
            info=data.get("info", []),
            correct_steps=data.get("correct_steps", []),
        )

    @staticmethod
    def _extract_code(notebook_json: str) -> str:
        """Extract all code cells from a notebook JSON string."""
        try:
            nb = json.loads(notebook_json)
            cells = []
            for i, cell in enumerate(nb.get("cells", [])):
                if cell.get("cell_type") == "code":
                    source = cell.get("source", [])
                    if isinstance(source, list):
                        code = "".join(source)
                    else:
                        code = source
                    cells.append(f"# === Cell {i} ===\n{code}")
            return "\n\n".join(cells)
        except (json.JSONDecodeError, KeyError):
            return notebook_json

    def format_issues_for_regeneration(self, result: ValidationResult) -> str:
        """Format validation issues into human-readable review text.

        As of, ``llm_notebook_generator.generate`` no longer
        feeds this back into another LLM call automatically -- a low score
        routes to human review instead of an automatic regenerate loop
        (see this module's docstring). Kept as a formatting utility for
        that review step, or for a future explicit "regenerate" action.
        """
        lines = [
            f"The previous notebook scored {result.score}/100. Fix these issues:",
            ""
        ]

        if result.critical_issues:
            lines.append("CRITICAL ISSUES (must fix):")
            for issue in result.critical_issues:
                step = issue.get("step", "unknown")
                desc = issue.get("issue", "")
                fix = issue.get("fix", "")
                lines.append(f"  - [{step}] {desc}")
                if fix:
                    lines.append(f"    FIX: {fix}")

        if result.warnings:
            lines.append("\nWARNINGS (should fix):")
            for w in result.warnings:
                step = w.get("step", "unknown")
                desc = w.get("issue", "")
                fix = w.get("fix", "")
                lines.append(f"  - [{step}] {desc}")
                if fix:
                    lines.append(f"    FIX: {fix}")

        return "\n".join(lines)
