"""Knowledge-base loader for the medallion-author skill.

Loads ``known-deltas.yaml`` (sibling file in this package) into typed
records the :mod:`reasoner` consults when scoring candidates.

The KB is hand-seeded at v0.1.0 with three documented variants
(cancelled-status alternates, currency-code casing, COA segment depth).
Each new entry bumps :data:`medallion_author.SKILL_VERSION`.
"""

from __future__ import annotations

import fnmatch
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Literal

import yaml


KNOWN_DELTAS_PATH = Path(__file__).parent / "known_deltas.yaml"


@dataclass(frozen=True)
class KnownDelta:
    """One row in the KB. Matched against observed bronze columns via
    fnmatch-style globs (alternation via ``|``)."""

    id: str
    variation_point: str
    """Pack VP name OR ``coa_*_segment``-style glob for families of VPs."""

    kind: Literal["columnAliases", "semanticVariants"]
    pattern: str
    """fnmatch glob, may include ``|`` alternation
    (e.g. ``*CurrencyCode|*CcyCode``). Compiled into a list of globs at
    load time."""

    priority: Literal["high", "medium", "low"] = "medium"
    notes: str = ""


@dataclass(frozen=True)
class KnowledgeBase:
    deltas: tuple[KnownDelta, ...]

    def for_variation_point(
        self,
        *,
        name: str,
        kind: str,
    ) -> list[KnownDelta]:
        """Filter to deltas matching this VP (name + kind).

        Supports VP-name globs in the delta (e.g. ``coa_*_segment``).
        """
        return [
            d
            for d in self.deltas
            if d.kind == kind and _vp_matches(d.variation_point, name)
        ]


def load_known_deltas(path: Path | None = None) -> KnowledgeBase:
    """Load the KB from disk. Defaults to the package-shipped file.

    Args:
        path: optional override path (used by tests + future
            operator-supplied KB extensions).

    Returns:
        :class:`KnowledgeBase` with one entry per row in the YAML.
    """
    target = path or KNOWN_DELTAS_PATH
    raw = yaml.safe_load(target.read_text(encoding="utf-8")) or {}
    entries: list[KnownDelta] = []
    for entry in raw.get("deltas", []) or []:
        entries.append(
            KnownDelta(
                id=entry["id"],
                variation_point=entry["variationPoint"],
                kind=entry["kind"],
                pattern=entry["pattern"],
                priority=entry.get("priority", "medium"),
                notes=entry.get("notes", ""),
            )
        )
    return KnowledgeBase(deltas=tuple(entries))


def column_matches_pattern(column_name: str, pattern: str) -> bool:
    """fnmatch glob with ``|`` alternation.

    Used by :mod:`reasoner` to rank observed columns against a known
    delta's pattern.
    """
    for alt in pattern.split("|"):
        if fnmatch.fnmatchcase(column_name, alt.strip()):
            return True
    return False


def _vp_matches(declared: str, observed: str) -> bool:
    """Match a VP name (or glob) against an observed VP name."""
    if "*" in declared or "?" in declared:
        return fnmatch.fnmatchcase(observed, declared)
    return declared == observed


__all__ = [
    "KNOWN_DELTAS_PATH",
    "KnowledgeBase",
    "KnownDelta",
    "column_matches_pattern",
    "load_known_deltas",
]
