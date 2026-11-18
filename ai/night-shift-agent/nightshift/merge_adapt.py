"""Turn a real, human-written Spark SQL MERGE into a replay-harness template.

The harness (replay.spark_sql_plan) applies ``repair_sql_template`` with two placeholders: ``{target}`` is
the sandbox clone of the production table and ``{batch}`` is one delivery of rows. A production MERGE
names its real target and source instead; this module swaps those two names for the placeholders and
keeps everything else (aliases, ON predicate, WHEN clauses) exactly as written, so what is graded is
the engineer's own logic, not a paraphrase of it.
"""
from __future__ import annotations

import re
from dataclasses import dataclass

_IDENT = r"[A-Za-z_][A-Za-z0-9_$]*|`[^`]+`"
_QUALIFIED = rf"(?:{_IDENT})(?:\s*\.\s*(?:{_IDENT}))*"
_MERGE_RE = re.compile(
    rf"(?P<head>\bMERGE\s+INTO\s+)(?P<target>{_QUALIFIED})(?P<talias>\s+(?:AS\s+)?(?!USING\b)(?:{_IDENT}))?"
    rf"(?P<using>\s+USING\s+)(?P<source>\((?:[^()]|\((?:[^()]|\([^()]*\))*\))*\)|{_QUALIFIED})"
    rf"(?P<salias>\s+(?:AS\s+)?(?!ON\b)(?:{_IDENT}))?",
    re.IGNORECASE | re.DOTALL,
)
_ON_RE = re.compile(r"\bON\s+(?P<on>.+?)\s+WHEN\b", re.IGNORECASE | re.DOTALL)
_EQ_RE = re.compile(rf"(?P<l>(?:{_IDENT})\s*\.\s*(?:{_IDENT}))\s*=\s*(?P<r>(?:{_IDENT})\s*\.\s*(?:{_IDENT}))", re.IGNORECASE)
_MATCHED_COND_RE = re.compile(r"WHEN\s+MATCHED\s+AND\s+(?P<cond>.+?)\s+THEN", re.IGNORECASE | re.DOTALL)


@dataclass(frozen=True)
class AdaptedMerge:
    template: str
    target: str
    source: str
    target_alias: str | None
    source_alias: str | None
    key_cols: tuple[str, ...]
    ts_guard: str | None  # the WHEN MATCHED AND ... condition, if any (ordering guard)

    @property
    def has_ordering_guard(self) -> bool:
        return self.ts_guard is not None


def _strip(ident: str) -> str:
    return ident.strip().strip("`")


def adapt_merge(sql: str) -> AdaptedMerge:
    """Replace the MERGE target and source with ``{target}`` / ``{batch}``; infer key columns from the ON
    equality predicates. Raises ValueError when the statement is not a MERGE or the source is a subquery
    (the harness supplies rows as a view, so a subquery source cannot be swapped faithfully)."""
    text = sql.strip().rstrip(";")
    m = _MERGE_RE.search(text)
    if not m:
        raise ValueError("not a recognisable MERGE INTO ... USING ... statement")
    source = m.group("source").strip()
    if source.startswith("("):
        raise ValueError("MERGE source is a subquery; materialise it as a table or view first")
    target = m.group("target").strip()
    talias = (m.group("talias") or "").strip() or None
    salias = (m.group("salias") or "").strip() or None
    if talias and talias.upper().startswith("AS "):
        talias = talias[3:].strip()
    if salias and salias.upper().startswith("AS "):
        salias = salias[3:].strip()
    template = text[: m.start("target")] + "{target}" + text[m.end("target"): m.start("source")] + "{batch}" + text[m.end("source"):]

    keys: list[str] = []
    on = _ON_RE.search(text)
    if on:
        t_names = {_strip(target.split(".")[-1]).lower()}
        if talias:
            t_names.add(_strip(talias).lower())
        for eq in _EQ_RE.finditer(on.group("on")):
            for side in (eq.group("l"), eq.group("r")):
                qual, col = [_strip(x) for x in side.split(".", 1)]
                if qual.lower() in t_names:
                    if col not in keys:
                        keys.append(col)
    guard = _MATCHED_COND_RE.search(text)
    return AdaptedMerge(template=template, target=target, source=source, target_alias=talias, source_alias=salias,
                        key_cols=tuple(keys), ts_guard=guard.group("cond").strip() if guard else None)
