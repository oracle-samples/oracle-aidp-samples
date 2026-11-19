"""Pure-Python variation-point candidate walker.

This module holds the *algorithm* — what to do with a candidate list
and an observed-column set. It does NOT touch Spark. The Spark probe
lives in :mod:`bronze_probe`; bootstrap composes the two by probing once into
a dict and feeding the dict's per-dataset column set to the walker.

Separating the algorithm from the probe keeps the walker mock-free
unit-testable (every case below is a pure-function input/output pair).

Three outcomes per variation point:

* :class:`AutoResolved` — exactly one candidate matched.
* :class:`MultiMatch` — two or more candidates matched; operator picks.
* :class:`NoMatch` — zero candidates matched; ``AIDPF-2010`` /
  ``AIDPF-2011`` if ``required: true``, otherwise silently skipped.

Priority order is the pack-declared ``candidates: [...]`` order. For
:class:`MultiMatch`, the returned ``matched`` list preserves the
priority order so the operator (or ``--non-interactive`` auto-pick) sees
the most-preferred candidate first.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Union

from ..schema.medallion_pack import ColumnAlias, SemanticVariant


# ---------------------------------------------------------------------------
# Outcome types
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class AutoResolved:
    """Exactly one candidate matched. Bootstrap silently pins it."""

    chosen: str
    """Logical id of the matched candidate (column name for
    :class:`ColumnAlias`; candidate id like ``cancelled_date`` for
    :class:`SemanticVariant`)."""


@dataclass(frozen=True)
class MultiMatch:
    """Two or more candidates matched. Operator prompt (or scripted
    resolution via ``--resolutions``) picks one."""

    matched: list[str]
    """Logical ids of every matched candidate, in priority order."""


@dataclass(frozen=True)
class CandidateAttempt:
    """Per-candidate walker outcome for :class:`NoMatch` reporting."""

    candidate: str
    outcome: str  # "column_not_found" | "detect_clause_failed"
    detail: str | None = None


@dataclass(frozen=True)
class NoMatch:
    """Zero candidates matched. Bootstrap writes a diagnostic artifact
    when the variation point is ``required: true``."""

    candidates_tried: list[CandidateAttempt]


CandidateWalkResult = Union[AutoResolved, MultiMatch, NoMatch]


# ---------------------------------------------------------------------------
# Walkers
# ---------------------------------------------------------------------------


def walk_column_alias(
    spec: ColumnAlias,
    observed_columns: set[str],
) -> CandidateWalkResult:
    """Walk a ``columnAliases.<name>`` against the observed bronze columns.

    Args:
        spec: the parsed :class:`ColumnAlias` from the resolved pack.
        observed_columns: case-insensitive set of column names present in
            the appliesTo bronze table. Callers normalise to lowercase
            before passing; the walker is case-insensitive on the
            candidate side too.

    Returns:
        One of :class:`AutoResolved` / :class:`MultiMatch` /
        :class:`NoMatch`. The walker NEVER raises — bootstrap maps the
        outcome to AIDPF codes / prompts / writes itself.
    """
    observed_lower = {col.lower() for col in observed_columns}

    matched: list[str] = []
    attempts: list[CandidateAttempt] = []
    for candidate in spec.candidates:
        if candidate.lower() in observed_lower:
            matched.append(candidate)
        else:
            attempts.append(
                CandidateAttempt(candidate=candidate, outcome="column_not_found")
            )

    return _outcome(matched, attempts)


def walk_semantic_variant(
    spec: SemanticVariant,
    observed_columns: set[str],
) -> CandidateWalkResult:
    """Walk a ``semanticVariants.<name>`` against the observed bronze columns.

    Each candidate carries a ``detect.columnExists`` predicate naming
    the column whose presence indicates this semantic shape. The walker
    returns the candidate's ``id`` (e.g. ``cancelled_date``), NOT the
    detect column name — bootstrap pins the id into
    ``resolved.semantic.<name>``, which the renderer maps back to the
    candidate's ``fragment``.
    """
    observed_lower = {col.lower() for col in observed_columns}

    matched: list[str] = []
    attempts: list[CandidateAttempt] = []
    for candidate in spec.candidates:
        detect_col = candidate.detect.column_exists
        if detect_col.lower() in observed_lower:
            matched.append(candidate.id)
        else:
            attempts.append(
                CandidateAttempt(
                    candidate=candidate.id,
                    outcome="detect_clause_failed",
                    detail=f"detect.columnExists={detect_col}",
                )
            )

    return _outcome(matched, attempts)


def _outcome(
    matched: list[str], attempts: list[CandidateAttempt]
) -> CandidateWalkResult:
    if len(matched) == 1:
        return AutoResolved(chosen=matched[0])
    if len(matched) >= 2:
        return MultiMatch(matched=matched)
    return NoMatch(candidates_tried=attempts)


__all__ = [
    "AutoResolved",
    "CandidateAttempt",
    "CandidateWalkResult",
    "MultiMatch",
    "NoMatch",
    "walk_column_alias",
    "walk_semantic_variant",
]
