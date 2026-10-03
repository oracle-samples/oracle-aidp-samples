"""Local file-based RAG store for Informatica-to-PySpark conversion patterns.

Stores successful (and optionally human-approved) spec-to-code mappings so
future similar transformations can reuse proven conversions.  No external
dependencies -- matching is fingerprint-based (transformation type + expression
structure), stored as plain JSON.
"""

import hashlib
import json
import os
import re
from dataclasses import asdict, dataclass, field
from datetime import datetime
from typing import Optional


@dataclass
class RAGEntry:
    """A stored successful conversion."""

    id: str = ""
    transformation_type: str = ""
    # The SHAPE of the transformation, deliberately free of field names so
    # it can generalise. Used for fuzzy suggestion only.
    spec_fingerprint: str = ""
    # A hash over everything that affects the generated code -- expressions
    # verbatim, port names, conditions. Used for exact reuse. Empty on an
    # entry written before this field existed, which makes that entry
    # fuzzy-only rather than wrongly exact.
    content_hash: str = ""
    spec_json: dict = field(default_factory=dict)
    pyspark_code: str = ""
    confidence: float = 0.0
    approved: bool = False
    approved_by: str = ""
    created_at: str = ""
    used_count: int = 0
    tags: list = field(default_factory=list)


class RAGStore:
    """Local file-based RAG store for Informatica conversion patterns.

    Storage: JSON file at ``~/.infa2aidp/rag_store.json`` (configurable).

    Two fingerprints, for two different jobs:

    ``_content_fingerprint`` covers everything that reaches the generated
    code -- every expression verbatim, the port names, conditions, joins.
    Only an identical one may be reused as-is, which is what
    :meth:`find_exact` now keys on.

    ``_fingerprint`` covers the *shape*: type, port counts, which function
    names appear, presence flags. It is deliberately free of field names so
    it can generalise, which makes it useful for suggesting a pattern and
    unsafe for reuse.

    Conflating the two was a defect. ``find_exact`` keyed on the shape, so
    ``IIF(AMT > 100, 'BIG', 'SMALL')`` and ``IIF(QTY < 5, 'LOW', 'HIGH')``
    hashed identically -- same type, one input, one output, one function
    named IIF -- and the first one's code was returned for the second as an
    exact match. Different column, different operator, different literals,
    delivered as a confident hit.

    Matching strategy (highest to lowest priority):
    1. **Exact** -- identical content hash. Safe to reuse verbatim.
    2. **Fuzzy** -- same type + Jaccard similarity on shape tokens. A
       suggestion to adapt, never a conversion to copy.
    """

    DEFAULT_PATH = os.path.expanduser("~/.infa2aidp/rag_store.json")

    def __init__(self, store_path: str = None):
        self.store_path = store_path or self.DEFAULT_PATH
        self.entries: dict[str, RAGEntry] = {}
        self._load()

    # ------------------------------------------------------------------
    # Persistence
    # ------------------------------------------------------------------

    def _load(self):
        if os.path.exists(self.store_path):
            with open(self.store_path, encoding="utf-8") as f:
                data = json.load(f)
            for entry_dict in data.get("entries", []):
                entry = RAGEntry(**entry_dict)
                self.entries[entry.id] = entry

    def _save(self):
        os.makedirs(os.path.dirname(self.store_path), exist_ok=True)
        data = {
            "version": "1.0",
            "updated_at": datetime.utcnow().isoformat(),
            "entry_count": len(self.entries),
            "entries": [asdict(e) for e in self.entries.values()],
        }
        with open(self.store_path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, default=str)

    # ------------------------------------------------------------------
    # Fingerprinting
    # ------------------------------------------------------------------

    def _fingerprint(self, spec) -> str:
        """Build a normalised fingerprint that captures the *shape* of
        a transformation without binding to specific field names.

        Captures: transformation type, input/output counts, function names
        used in expressions, presence of conditions / SQL overrides / joins /
        group-bys.
        """
        parts = [spec.transformation_type]

        parts.append(f"in:{len(spec.inputs)}")
        parts.append(f"out:{len(spec.outputs)}")

        if "expressions" in spec.logic:
            for expr in spec.logic["expressions"]:
                text = expr.get("expression", "") or ""
                funcs = sorted(set(re.findall(r"\b([A-Z_]+)\s*\(", text)))
                parts.append(f"funcs:{'|'.join(funcs)}")
                # The expression's SHAPE with identifiers blanked, so two
                # expressions that share function names but differ in
                # operators or literals (ROUND(A * B, 2) vs ROUND(A / B, 0))
                # are not the same entry. Matching on function names alone
                # returned another transformation's code as a 0.95-confidence
                # "success".
                parts.append(f"shape:{self._expression_shape(text)}")
        for out in spec.outputs or []:
            if isinstance(out, dict) and out.get("expression"):
                parts.append(f"shape:{self._expression_shape(str(out['expression']))}")

        if "condition" in spec.logic:
            parts.append("has_condition")
        if spec.logic.get("sql_override"):
            parts.append("has_sql_override")
        if "join_type" in spec.logic:
            parts.append(f"join:{spec.logic['join_type']}")
        if "group_by_fields" in spec.logic:
            parts.append(f"group_by:{len(spec.logic['group_by_fields'])}")

        return "|".join(parts)

    def _content_fingerprint(self, spec) -> str:
        """Everything that affects the generated code, verbatim.

        Field names are included on purpose. They appear in the output as
        ``F.col('AMT')``, so a stored conversion that used a different
        column is not the same conversion -- which is exactly the collision
        the shape fingerprint could not see.
        """
        parts = [f"type:{spec.transformation_type}"]
        parts.append("in:" + ",".join(str(i) for i in spec.inputs))
        parts.append("out:" + ",".join(str(o) for o in spec.outputs))
        for key in sorted(spec.logic):
            value = spec.logic[key]
            if key == "expressions" and isinstance(value, list):
                for expr in value:
                    if isinstance(expr, dict):
                        rendered = "=>".join(
                            f"{k}:{expr[k]}" for k in sorted(expr)
                        )
                    else:
                        rendered = str(expr)
                    parts.append(f"expr[{rendered}]")
            else:
                parts.append(f"{key}:{value}")
        return "|".join(parts)

    @staticmethod
    def _expression_shape(text: str) -> str:
        """An expression with every identifier replaced by ``_`` and
        whitespace collapsed -- function names, operators and literals are
        kept, so the skeleton distinguishes ``ROUND(_ * _, 2)`` from
        ``ROUND(_ / _, 0)`` while still matching a renamed port."""
        skeleton = re.sub(r"'[^']*'", "'~'", text)                       # string literals
        skeleton = re.sub(r"\b([A-Za-z_]\w*)\s*\(", lambda m: m.group(1).upper() + "(", skeleton)
        skeleton = re.sub(r"\b(?![A-Z_]+\()(?!\d)[A-Za-z_$][\w$]*\b(?!\s*\()", "_", skeleton)
        return re.sub(r"\s+", "", skeleton)

    @staticmethod
    def _hash(fingerprint: str) -> str:
        return hashlib.sha256(fingerprint.encode()).hexdigest()[:16]

    # ------------------------------------------------------------------
    # Store / retrieve
    # ------------------------------------------------------------------

    def store(
        self,
        spec,
        pyspark_code: str,
        approved: bool = False,
        confidence: float = 0.0,
        tags: list = None,
    ) -> str:
        """Store a successful conversion.  Returns the entry id."""
        fingerprint = self._fingerprint(spec)
        content = self._content_fingerprint(spec)
        # Keyed on content, not shape: two different transformations that
        # merely look alike must not overwrite each other's entry either.
        entry_id = self._hash(content)

        self.entries[entry_id] = RAGEntry(
            id=entry_id,
            transformation_type=spec.transformation_type,
            spec_fingerprint=fingerprint,
            content_hash=self._hash(content),
            spec_json=spec.logic if isinstance(spec.logic, dict) else {},
            pyspark_code=pyspark_code,
            confidence=confidence,
            approved=approved,
            created_at=datetime.utcnow().isoformat(),
            tags=tags or [],
        )
        self._save()
        return entry_id

    def find_exact(self, spec) -> Optional[RAGEntry]:
        """Find an entry whose CONTENT is identical, so it can be reused.

        Keyed on the content hash rather than the shape. Keying it on the
        shape returned another transformation's code as an exact match --
        see the class docstring.

        An entry stored before ``content_hash`` existed has an empty one and
        is deliberately unreachable here; it can still be found by
        :meth:`find_similar`, where an inexact match is what the caller
        expects.
        """
        entry_id = self._hash(self._content_fingerprint(spec))
        entry = self.entries.get(entry_id)
        if entry is not None and not entry.content_hash:
            return None
        return entry

    def find_similar(
        self, spec, min_similarity: float = 0.6
    ) -> Optional[RAGEntry]:
        """Find the best fuzzy match above *min_similarity*.

        Uses Jaccard similarity on fingerprint tokens.  Approved entries
        receive a +0.1 bonus so they are preferred over unapproved ones.
        """
        if not self.entries:
            return None

        query_parts = set(self._fingerprint(spec).split("|"))

        best_match = None
        best_score = 0.0

        for entry in self.entries.values():
            if entry.transformation_type != spec.transformation_type:
                continue

            entry_parts = set(entry.spec_fingerprint.split("|"))
            # Expression shape is a gate, not a vote: two Expression
            # transformations whose skeletons share nothing produce
            # different code, however many structural tokens they share.
            q_shapes = {x for x in query_parts if x.startswith("shape:")}
            e_shapes = {x for x in entry_parts if x.startswith("shape:")}
            if q_shapes and e_shapes and not (q_shapes & e_shapes):
                continue
            union = query_parts | entry_parts
            score = len(query_parts & entry_parts) / len(union) if union else 0.0

            if entry.approved:
                score += 0.1

            if score > best_score and score >= min_similarity:
                best_score = score
                best_match = entry

        if best_match:
            best_match.used_count += 1
            self._save()

        return best_match

    # ------------------------------------------------------------------
    # Management helpers
    # ------------------------------------------------------------------

    def approve(self, entry_id: str, approved_by: str = "user"):
        if entry_id in self.entries:
            self.entries[entry_id].approved = True
            self.entries[entry_id].approved_by = approved_by
            self._save()

    def remove(self, entry_id: str):
        if entry_id in self.entries:
            del self.entries[entry_id]
            self._save()

    def list_entries(
        self,
        transformation_type: str = None,
        approved_only: bool = False,
    ) -> list[RAGEntry]:
        entries = list(self.entries.values())
        if transformation_type:
            entries = [e for e in entries if e.transformation_type == transformation_type]
        if approved_only:
            entries = [e for e in entries if e.approved]
        return sorted(entries, key=lambda e: e.used_count, reverse=True)

    def stats(self) -> dict:
        entries = list(self.entries.values())
        types = {e.transformation_type for e in entries}
        return {
            "total_entries": len(entries),
            "approved_entries": sum(1 for e in entries if e.approved),
            "total_retrievals": sum(e.used_count for e in entries),
            "by_type": {
                t: sum(1 for e in entries if e.transformation_type == t)
                for t in types
            },
        }

    # ------------------------------------------------------------------
    # Import / export (for sharing across teams)
    # ------------------------------------------------------------------

    def export(self, output_path: str):
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(
                {
                    "stats": self.stats(),
                    "entries": [
                        asdict(e)
                        for e in sorted(
                            self.entries.values(), key=lambda e: e.transformation_type
                        )
                    ],
                },
                f,
                indent=2,
                default=str,
            )

    def import_entries(self, input_path: str, approve_all: bool = False):
        with open(input_path, encoding="utf-8") as f:
            data = json.load(f)
        for entry_dict in data.get("entries", []):
            entry = RAGEntry(**entry_dict)
            if approve_all:
                entry.approved = True
            self.entries[entry.id] = entry
        self._save()
