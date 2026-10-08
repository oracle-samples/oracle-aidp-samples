"""Snowflake identifier case policy.

Unquoted Snowflake identifiers fold to UPPER; quoted ones preserve case and are
case-SENSITIVE. So `customers` is really CUSTOMERS, while "customers" is a
DIFFERENT object. Spark/Delta folds to lower by default, so naive lowercasing
merges them and loses data with no error.

The form is captured at extraction time -- SHOW output tells us which was used --
and a collision HALTS rather than being resolved by guessing.
"""
from __future__ import annotations

import collections

__all__ = ["UnsafeIdentifier", "assert_safe_identifier", "case_form",
           "detect_collisions"]

_FORBIDDEN = set('`"\'') | {c for c in map(chr, range(32))}


class UnsafeIdentifier(ValueError):
    """Identifier is empty, quoted, or contains whitespace/control characters."""


def case_form(name: str) -> str:
    return "UPPER_UNQUOTED" if name == name.upper() else "MIXED_QUOTED_CASE_SENSITIVE"


def assert_safe_identifier(name: str) -> str:
    if not isinstance(name, str) or not name.strip():
        raise UnsafeIdentifier(f"empty or non-string identifier: {name!r}")
    if any(c in _FORBIDDEN or c.isspace() for c in name):
        raise UnsafeIdentifier(f"identifier contains unsafe characters: {name!r}")
    return name


def detect_collisions(identifiers: list[str]) -> dict[str, list[str]]:
    """Group identifiers that differ only by case. Empty result means safe."""
    buckets: dict[str, set[str]] = collections.defaultdict(set)
    for raw in identifiers:
        buckets[raw.strip().upper()].add(raw)
    return {k: sorted(v) for k, v in buckets.items() if len(v) > 1}
