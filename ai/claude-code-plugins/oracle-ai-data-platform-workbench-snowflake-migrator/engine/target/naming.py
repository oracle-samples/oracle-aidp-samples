"""AIDP display-name translation. Pure, zero I/O.

Provisioning creates real named resources — workspaces, clusters, catalogs —
from names that originate in Snowflake (account names, warehouse names,
database names) or from a human. Those names carry spaces, dots, hyphens,
accents and case that some AIDP surfaces accept and others quietly do not,
and a name rejected mid-provisioning is a half-created environment.

So every name passes through ONE deliberately conservative translator before
it reaches an API body. The contract:

  * lowercase ASCII letters, digits and underscore only — the intersection
    that every surface (REST displayName, Spark identifier, URL path segment)
    accepts without quoting;
  * starts with a letter;
  * short (63 by default — well under every documented limit, and safe for
    surfaces whose limit is NOT documented);
  * deterministic, and collision-safe under truncation: a truncated name
    carries a hash of the original, so two long names that share a prefix
    cannot silently fold into one — the same failure class as the
    identifier-case collision HALT, and it is prevented, not detected.

Every change is recorded in `notes`, because a resource created under a name
the user did not type must be attributable to the rule that renamed it.
An input that sanitises to nothing raises: refusing beats inventing a name.
"""
from __future__ import annotations

import hashlib
import re
import unicodedata
from dataclasses import dataclass, field

__all__ = ["NameTranslation", "UnusableName", "translate_name"]

# Conservative across every AIDP surface: workspace displayName documents
# 1-1000, catalog 1-255, cluster and job limits are not documented -- 63 is
# safe everywhere and keeps names readable in the console.
DEFAULT_MAX_LENGTH = 63

_HASH_LEN = 6


class UnusableName(ValueError):
    """Nothing usable survived sanitisation. The caller must ask, not guess."""


@dataclass
class NameTranslation:
    original: str
    name: str
    changed: bool
    notes: list[str] = field(default_factory=list)


def _ascii_fold(value: str) -> str:
    # ÁÉÇ -> AEC. Characters with no ASCII decomposition drop out here and
    # are then handled by the character filter below.
    return (unicodedata.normalize("NFKD", value)
            .encode("ascii", "ignore").decode("ascii"))


def translate_name(original: str, *, kind: str = "resource",
                   max_length: int = DEFAULT_MAX_LENGTH) -> NameTranslation:
    """One AIDP-safe display name out of whatever the source called it.

    `kind` names the resource in notes and in the digit-start prefix, so a
    renamed workspace and a renamed cluster read differently in the report.
    """
    if max_length < _HASH_LEN + 3:
        raise ValueError(f"max_length {max_length} leaves no room for a name")
    notes: list[str] = []
    working = original or ""

    folded = _ascii_fold(working)
    if folded != working:
        notes.append("non-ASCII characters folded or dropped")
    working = folded

    lowered = working.lower()
    if lowered != working:
        notes.append("lower-cased (AIDP stores identifiers in lower case)")
    working = lowered

    replaced = re.sub(r"[^a-z0-9]+", "_", working)
    if replaced != working:
        notes.append("separators and punctuation replaced with '_'")
    working = replaced.strip("_")
    working = re.sub(r"_{2,}", "_", working)

    if not working:
        raise UnusableName(
            f"the {kind} name {original!r} sanitises to nothing usable; "
            f"choose a name rather than have one invented")

    if working[0].isdigit():
        working = f"{kind[0]}_{working}"
        notes.append(f"prefixed '{kind[0]}_': names must start with a letter")

    if len(working) > max_length:
        # Truncation folds distinct long names together, so the tail carries
        # a hash of the ORIGINAL -- prevention, not detection.
        digest = hashlib.sha256(original.encode()).hexdigest()[:_HASH_LEN]
        working = f"{working[:max_length - _HASH_LEN - 1].rstrip('_')}_{digest}"
        notes.append(
            f"truncated to {max_length} with a stable suffix from the "
            f"original, so two long names cannot fold into one")

    return NameTranslation(original=original, name=working,
                           changed=working != original, notes=notes)
