"""Human-in-the-loop review system for migration conversions.

Two modes:
1. Interactive CLI review -- prompts the user to approve/reject/edit each
   LOW/MANUAL conversion in the terminal.
2. Review file generation -- creates a YAML review file that humans can edit
   offline, then import back to approve/fix conversions.
"""

import json
import os
import sys
import yaml
from dataclasses import dataclass, field
from typing import Optional

from .models import ConversionRecord, ConversionSpec, ValidationResult
from .rag_store import RAGStore


@dataclass
class ReviewItem:
    """A single item for human review."""

    id: str = ""
    mapping_name: str = ""
    transformation_name: str = ""
    transformation_type: str = ""
    original_logic: str = ""
    generated_pyspark: str = ""
    confidence: str = ""              # HIGH, MEDIUM, LOW, MANUAL
    validation_errors: list = field(default_factory=list)
    attempts: int = 0
    # Review fields (filled by human)
    decision: str = "pending"         # pending, approved, rejected, edited
    edited_code: str = ""
    reviewer_notes: str = ""


class HumanReviewer:
    """Human-in-the-loop review for migration conversions."""

    def __init__(self, rag_store: RAGStore = None):
        self.rag_store = rag_store or RAGStore()

    # ── helpers ──────────────────────────────────────────────────────────

    def _get_confidence(self, record: ConversionRecord) -> str:
        """Determine confidence level from a conversion record."""
        if record.final_status == "failed":
            return "MANUAL"
        if record.final_status == "partial":
            return "LOW"
        if record.attempts:
            last = record.attempts[-1]
            if last.confidence >= 0.8:
                return "HIGH"
            elif last.confidence >= 0.5:
                return "MEDIUM"
            else:
                return "LOW"
        return "MEDIUM"

    def _store_approved(self, item: ReviewItem, code: str):
        """Store an approved conversion in the RAG store."""
        spec = ConversionSpec(
            transformation_name=item.transformation_name,
            transformation_type=item.transformation_type,
            logic={"original": item.original_logic} if item.original_logic else {},
        )
        self.rag_store.store(
            spec,
            code,
            approved=True,
            confidence=1.0,
            tags=[item.transformation_type, item.confidence, "human-reviewed"],
        )

    # ── Mode 1: Generate / import YAML review file ──────────────────────

    def generate_review_file(
        self,
        records: list,
        output_path: str,
        include_confidence: list = None,
    ) -> int:
        """Generate a YAML review file from conversion records.

        Args:
            records: List of ConversionRecord from the agentic pipeline.
            output_path: Path to write the YAML review file.
            include_confidence: Which confidence levels to include.
                Defaults to ``["LOW", "MANUAL"]``.

        Returns:
            Number of items written to the review file.
        """
        if include_confidence is None:
            include_confidence = ["LOW", "MANUAL"]

        review_items = []
        for record in records:
            confidence = self._get_confidence(record)
            if confidence not in include_confidence:
                continue

            original_logic = ""
            if record.attempts:
                spec = record.attempts[0].spec
                if spec and spec.logic:
                    original_logic = json.dumps(spec.logic, indent=2)

            item = {
                "id": record.transformation_name,
                "mapping_name": (
                    record.transformation_name.rsplit(".", 1)[0]
                    if "." in record.transformation_name
                    else ""
                ),
                "transformation_name": record.transformation_name,
                "transformation_type": record.transformation_type,
                "confidence": confidence,
                "attempts": record.total_attempts,
                "status": record.final_status,
                "original_informatica_logic": original_logic,
                "generated_pyspark": record.final_code,
                "validation_errors": (
                    record.attempts[-1].validation_errors if record.attempts else []
                ),
                # Human fills these:
                "decision": "pending",
                "edited_code": "",
                "reviewer_notes": "",
            }
            review_items.append(item)

        review_doc = {
            "review_metadata": {
                "generated_by": "infa2aidp",
                "total_items": len(review_items),
                "instructions": (
                    "Review each item below. For each:\n"
                    "  - Set 'decision' to 'approved' if the PySpark code is correct\n"
                    "  - Set 'decision' to 'edited' and fill 'edited_code' if you fixed it\n"
                    "  - Set 'decision' to 'rejected' if it cannot be auto-converted\n"
                    "  - Add 'reviewer_notes' for context\n"
                    "Then run: infa2aidp review --import review_file.yaml"
                ),
            },
            "items": review_items,
        }

        os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)
        with open(output_path, "w", encoding="utf-8") as f:
            yaml.dump(
                review_doc,
                f,
                default_flow_style=False,
                sort_keys=False,
                allow_unicode=True,
                width=120,
            )

        return len(review_items)

    def import_review_file(self, review_path: str) -> list:
        """Import a completed review file and apply decisions.

        - approved items -> stored in RAG as approved
        - edited items  -> stored in RAG with corrected code
        - rejected items -> logged but not stored

        Returns:
            List of ReviewItem with decisions applied.
        """
        with open(review_path, encoding="utf-8") as f:
            review_doc = yaml.safe_load(f)

        results = []
        for item_dict in review_doc.get("items", []):
            item = ReviewItem(
                id=item_dict.get("id", ""),
                mapping_name=item_dict.get("mapping_name", ""),
                transformation_name=item_dict.get("transformation_name", ""),
                transformation_type=item_dict.get("transformation_type", ""),
                original_logic=item_dict.get("original_informatica_logic", ""),
                generated_pyspark=item_dict.get("generated_pyspark", ""),
                confidence=item_dict.get("confidence", ""),
                validation_errors=item_dict.get("validation_errors", []),
                attempts=item_dict.get("attempts", 0),
                decision=item_dict.get("decision", "pending"),
                edited_code=item_dict.get("edited_code", ""),
                reviewer_notes=item_dict.get("reviewer_notes", ""),
            )

            if item.decision == "approved":
                self._store_approved(item, item.generated_pyspark)
            elif item.decision == "edited":
                code = item.edited_code or item.generated_pyspark
                self._store_approved(item, code)

            results.append(item)

        return results

    # ── Mode 2: Interactive CLI review ──────────────────────────────────

    def interactive_review(
        self,
        records: list,
        include_confidence: list = None,
    ) -> list:
        """Interactive CLI review -- prompts user for each item.

        Only works when running in a terminal (``sys.stdin.isatty()``).
        """
        if not sys.stdin.isatty():
            print(
                "Interactive review requires a terminal. Use --review-file instead.",
                file=sys.stderr,
            )
            return []

        if include_confidence is None:
            include_confidence = ["LOW", "MANUAL"]

        items_to_review = []
        for record in records:
            confidence = self._get_confidence(record)
            if confidence in include_confidence:
                items_to_review.append((record, confidence))

        if not items_to_review:
            print("No items need review -- all conversions are HIGH/MEDIUM confidence.")
            return []

        print(f"\n{'=' * 60}")
        print(f"  Human Review -- {len(items_to_review)} item(s) need attention")
        print(f"{'=' * 60}\n")

        reviewed = []
        for i, (record, confidence) in enumerate(items_to_review, 1):
            print(
                f"--- [{i}/{len(items_to_review)}] "
                f"{record.transformation_name} ({record.transformation_type}) ---"
            )
            print(
                f"Confidence: {confidence} | Status: {record.final_status} "
                f"| Attempts: {record.total_attempts}"
            )

            # Show original logic
            if record.attempts and record.attempts[0].spec:
                logic = record.attempts[0].spec.logic
                if logic:
                    print("\nOriginal Informatica logic:")
                    print(f"  {json.dumps(logic, indent=2)[:500]}")

            # Show generated code (first 20 lines)
            print("\nGenerated PySpark:")
            code_lines = record.final_code.split("\n")
            for line in code_lines[:20]:
                print(f"  {line}")
            if len(code_lines) > 20:
                print(f"  ... ({len(code_lines) - 20} more lines)")

            # Show validation errors
            if record.attempts and record.attempts[-1].validation_errors:
                print("\nValidation errors:")
                for err in record.attempts[-1].validation_errors:
                    print(f"  - {err}")

            # Prompt
            print("\nOptions: [a]pprove  [r]eject  [s]kip  [q]uit")
            choice = input("Decision: ").strip().lower()

            item = ReviewItem(
                transformation_name=record.transformation_name,
                transformation_type=record.transformation_type,
                generated_pyspark=record.final_code,
                confidence=confidence,
            )

            if choice in ("a", "approve"):
                item.decision = "approved"
                self._store_approved(item, record.final_code)
                print("  -> Approved and stored in RAG")
            elif choice in ("r", "reject"):
                item.decision = "rejected"
                notes = input("Rejection reason (optional): ").strip()
                item.reviewer_notes = notes
                print("  -> Rejected")
            elif choice in ("q", "quit"):
                print("  -> Review stopped")
                reviewed.append(item)
                break
            else:
                item.decision = "pending"
                print("  -> Skipped")

            reviewed.append(item)
            print()

        # Summary
        approved = sum(1 for r in reviewed if r.decision == "approved")
        rejected = sum(1 for r in reviewed if r.decision == "rejected")
        skipped = sum(1 for r in reviewed if r.decision in ("pending", ""))
        print(
            f"\nReview Summary: {approved} approved, {rejected} rejected, "
            f"{skipped} skipped"
        )
        stats = self.rag_store.stats()
        print(
            f"RAG store: {stats['total_entries']} entries "
            f"({stats['approved_entries']} approved)"
        )

        return reviewed

    # ── Reporting ───────────────────────────────────────────────────────

    def generate_review_report(self, reviewed_items: list, output_path: str):
        """Generate a markdown report of review decisions."""
        lines = ["# Human Review Report", ""]

        approved = [r for r in reviewed_items if r.decision == "approved"]
        rejected = [r for r in reviewed_items if r.decision == "rejected"]
        edited = [r for r in reviewed_items if r.decision == "edited"]
        pending = [r for r in reviewed_items if r.decision == "pending"]

        lines.append("## Summary")
        lines.append(f"- Approved: {len(approved)}")
        lines.append(f"- Edited: {len(edited)}")
        lines.append(f"- Rejected: {len(rejected)}")
        lines.append(f"- Pending: {len(pending)}")
        lines.append("")

        if rejected:
            lines.append("## Rejected Items (Need Manual Rewrite)")
            lines.append("")
            lines.append("| Transformation | Type | Confidence | Reason |")
            lines.append("|---------------|------|------------|--------|")
            for r in rejected:
                lines.append(
                    f"| {r.transformation_name} | {r.transformation_type} "
                    f"| {r.confidence} | {r.reviewer_notes or 'N/A'} |"
                )
            lines.append("")

        if edited:
            lines.append("## Edited Items (Human-Corrected)")
            lines.append("")
            for r in edited:
                lines.append(f"### {r.transformation_name}")
                lines.append(f"```python\n{r.edited_code}\n```")
                lines.append("")

        os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)
        with open(output_path, "w", encoding="utf-8") as f:
            f.write("\n".join(lines))
