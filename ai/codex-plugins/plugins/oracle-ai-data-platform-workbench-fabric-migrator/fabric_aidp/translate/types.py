"""Shared result shapes for every translator.

Deliberately identical to `aws_aidp.translate.athena_to_spark_sql.Finding` /
`TranslationResult` so the two tools' migrate reports render the same way.
"""
from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class Finding:
    rule: str           # stable rule id, e.g. "NB01_ONELAKE_PATH"
    detail: str         # human-readable, one line
    # Three values, not the two this said. `info` was undocumented, and it
    # is the one that costs something: `changes` below counts `rewrite`
    # alone, so an `info` finding is worth nothing to every count
    # downstream. Correct for a finding that changed nothing -- NB16, NB25,
    # PL-disabled-activity -- and wrong for NB14, which rewrote the name and
    # reported `info`, so verify printed no change count for an edited file.
    severity: str       # "rewrite" (applied) | "flag" (needs a human)
                        # | "info" (noted; nothing was rewritten)

    def __str__(self) -> str:
        return f"[{self.severity}] {self.rule}: {self.detail}"


@dataclass
class TranslationResult:
    source_sql: str
    translated_sql: str
    findings: list = field(default_factory=list)

    @property
    def changes(self) -> int:
        return sum(1 for f in self.findings if f.severity == "rewrite")

    @property
    def flags(self) -> int:
        return sum(1 for f in self.findings if f.severity == "flag")

    @property
    def needs_manual_review(self) -> bool:
        return self.flags > 0
