"""ConversionPipeline -- orchestrate the full agentic conversion loop.

Flow:
1. SpecAgent: Transformation -> ConversionSpec (deterministic)
2. CodeGenAgent: ConversionSpec -> PySpark code (rule-based + LLM)
3. ValidatorAgent: Validate generated code
4. If validation fails and attempts < max:
   a. FixerAgent: Fix errors
   b. ValidatorAgent: Re-validate
   c. Loop back to (a) if still failing
5. Return final ConversionRecord
"""

from __future__ import annotations

import json
from typing import Optional

from ..models import Mapping, Transformation
from .models import ConversionAttempt, ConversionRecord, ConversionSpec, ValidationResult
from .spec_agent import SpecAgent
from .codegen_agent import CodeGenAgent
from .validator_agent import ValidatorAgent
from .fixer_agent import FixerAgent


class ConversionPipeline:
    """Orchestrate the full agentic conversion pipeline."""

    def __init__(
        self,
        llm_handler=None,
        custom_rules: Optional[dict] = None,
        max_attempts: int = 3,
        rag_store=None,
    ):
        self.spec_agent = SpecAgent()
        self.codegen_agent = CodeGenAgent(llm_handler, custom_rules)
        self.validator_agent = ValidatorAgent()
        self.fixer_agent = FixerAgent(llm_handler)
        self.rag_store = rag_store
        self.max_attempts = max_attempts

    def convert_transformation(
        self,
        transformation: Transformation,
        mapping_context: Optional[dict] = None,
    ) -> ConversionRecord:
        """Convert a single transformation through the full pipeline."""
        record = ConversionRecord(
            transformation_name=transformation.name,
            transformation_type=(
                transformation.type.value
                if hasattr(transformation.type, "value")
                else str(transformation.type)
            ),
            max_attempts=self.max_attempts,
        )

        # Step 1: Generate canonical spec (deterministic, no LLM)
        spec = self.spec_agent.generate_spec(transformation, mapping_context)

        # Step 1.5: Check RAG for similar past conversions
        if self.rag_store:
            past_code = self._check_rag(spec)
            if past_code:
                record.final_code = past_code
                record.final_status = "success"
                record.total_attempts = 0
                record.attempts.append(ConversionAttempt(
                    attempt_number=0,
                    spec=spec,
                    generated_code=past_code,
                    validation_result=ValidationResult.PASSED,
                    confidence=0.95,
                    agent_used="rag-cache",
                ))
                return record

        # Step 2: Initial code generation
        attempt = self.codegen_agent.generate(spec)

        # Step 3: Validation loop
        for i in range(self.max_attempts):
            attempt = self.validator_agent.validate(attempt)
            record.attempts.append(attempt)
            record.total_attempts = i + 1

            if attempt.validation_result == ValidationResult.PASSED:
                record.final_code = attempt.generated_code
                record.final_status = "success"
                break

            if i < self.max_attempts - 1:
                # Fix and retry
                attempt = self.fixer_agent.fix(attempt)

        # If no attempt passed, use the best one
        final_attempt = record.attempts[-1] if record.attempts else None
        if record.final_status != "success" and record.attempts:
            best = max(record.attempts, key=lambda a: a.confidence)
            record.final_code = best.generated_code
            record.final_status = "partial" if best.generated_code.strip() else "failed"
            final_attempt = best

        # Step 4: Hallucination detection — check the attempt that was
        # actually chosen above (the best one), not unconditionally the last
        # attempt, which used to overwrite final_code with the fixer's last
        # (often worse) output and made the "best attempt" choice dead code.
        if record.final_code and final_attempt is not None:
            from .hallucination_detector import HallucinationDetector
            detector = HallucinationDetector()
            final_attempt, h_report = detector.check_and_suppress(
                final_attempt, suppress_threshold=0.5
            )
            record.final_code = final_attempt.generated_code
            # Downgrade status if hallucinations were suppressed
            if h_report.hallucination_score >= 0.5:
                record.final_status = "partial"

        # Store successful conversion in RAG (only if clean)
        if self.rag_store and record.final_status == "success":
            self._store_rag(spec, record.final_code)

        return record

    def convert_mapping(
        self,
        mapping: Mapping,
        session=None,
    ) -> list[ConversionRecord]:
        """Convert all transformations in a mapping."""
        context = {
            "mapping_name": mapping.name,
            "folder": mapping.folder or "Migrated",
            "sources": [s.name for s in mapping.sources],
            "targets": [t.name for t in mapping.targets],
            "parameters": mapping.parameters,
        }

        if session:
            context["pre_sql"] = getattr(session, "pre_sql", "")
            context["post_sql"] = getattr(session, "post_sql", "")
            context["session_params"] = getattr(session, "parameters", {})

        records = []
        for tx in mapping.transformations:
            record = self.convert_transformation(tx, context)
            records.append(record)

        return records

    # ------------------------------------------------------------------
    # RAG helpers
    # ------------------------------------------------------------------

    def _check_rag(self, spec: ConversionSpec) -> Optional[str]:
        """Return a past conversion's PySpark only for an *identical* spec.

        The caller assigns this straight to ``final_code``, marks the record
        a success and skips validation, so whatever comes back is emitted
        verbatim. Nothing re-templates column names: stored code for
        ``ROUND(AMT * RATE, 2)`` says ``F.col('AMT')``, which is wrong for a
        mapping whose ports are ``price`` and ``fx_rate``.

        So this keys on ``find_exact`` (content hash -- expressions verbatim
        plus port names), never ``find_similar``. A merely similar entry is a
        pattern to adapt, not a conversion to copy; using it here emitted
        another transformation's columns as a 0.95-confidence success.
        """
        if not self.rag_store:
            return None
        try:
            entry = self.rag_store.find_exact(spec)
            if entry and hasattr(entry, 'pyspark_code'):
                return entry.pyspark_code
            return None
        except Exception:
            return None

    def _store_rag(self, spec: ConversionSpec, code: str) -> None:
        """Store a successful conversion in RAG for future reuse."""
        if not self.rag_store:
            return
        try:
            self.rag_store.store(spec, code)
        except Exception:
            pass

    # ------------------------------------------------------------------
    # Reporting
    # ------------------------------------------------------------------

    def generate_pipeline_report(
        self,
        records: list[ConversionRecord],
        output_path: str,
    ) -> dict:
        """Generate a report of the agentic conversion process.

        Returns a summary dict and writes detailed JSON to output_path.
        """
        total = len(records)
        success = sum(1 for r in records if r.final_status == "success")
        partial = sum(1 for r in records if r.final_status == "partial")
        failed = sum(1 for r in records if r.final_status == "failed")
        pending = sum(1 for r in records if r.final_status == "pending")

        total_attempts = sum(r.total_attempts for r in records)
        avg_attempts = total_attempts / total if total else 0

        # Collect common error patterns
        error_counts: dict[str, int] = {}
        for r in records:
            for a in r.attempts:
                for err in a.validation_errors:
                    # Normalise error for grouping
                    key = err.split(":")[0].strip() if ":" in err else err[:60]
                    error_counts[key] = error_counts.get(key, 0) + 1

        top_errors = sorted(error_counts.items(), key=lambda x: -x[1])[:10]

        # Per-transformation detail
        details = []
        for r in records:
            detail = {
                "transformation": r.transformation_name,
                "type": r.transformation_type,
                "status": r.final_status,
                "attempts": r.total_attempts,
                "agent_used": r.attempts[-1].agent_used if r.attempts else "",
                "fix_applied": r.attempts[-1].fix_applied if r.attempts else "",
                "confidence": r.attempts[-1].confidence if r.attempts else 0,
                "errors": (
                    r.attempts[-1].validation_errors if r.attempts else []
                ),
            }
            details.append(detail)

        summary = {
            "total_transformations": total,
            "success": success,
            "partial": partial,
            "failed": failed,
            "pending": pending,
            "success_rate": f"{success / total * 100:.1f}%" if total else "N/A",
            "average_attempts": round(avg_attempts, 2),
            "top_error_patterns": top_errors,
            "details": details,
        }

        with open(output_path, "w", encoding="utf-8") as fh:
            json.dump(summary, fh, indent=2, default=str)

        return summary
