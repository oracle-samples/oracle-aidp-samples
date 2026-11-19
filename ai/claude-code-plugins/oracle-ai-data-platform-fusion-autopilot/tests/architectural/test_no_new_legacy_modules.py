"""Architectural guardrail: no new `dimensions/dim_*.py` or `transforms/gold/*.py`.

v2 explicitly moves silver/gold modeling out of hardcoded Python modules and
into declarative content packs (PLAN §15 Phase 0 step 9, §5.1, ADR-0011).
This test fails if a new Python module appears under those legacy paths
without an entry in `legacy_modules_allowlist.txt`.

Why this exists:
    Without enforcement, contributors will keep adding `dim_*.py` modules
    out of habit. The v1 failure mode is "module exists but nobody calls
    it" — the test prevents that pattern from regressing v2.

How to handle a legitimate exception:
    1. Add the module path to `legacy_modules_allowlist.txt`.
    2. Include a comment line above the entry with the documented reason.
    3. Open a PR; reviewer skill (per PLAN §13.4) cites this test in review.

Allowlist file format: one path per line, POSIX-style, repo-root-relative.
Blank lines and `# comment` lines ignored.
"""

from __future__ import annotations

from pathlib import Path

# Repo layout: this test file lives at
# `claude-code-plugins/oracle-ai-data-platform-fusion-autopilot/tests/architectural/`.
# Plugin root is two levels up from this file.
PLUGIN_ROOT = Path(__file__).resolve().parent.parent.parent

# Watched directories (relative to plugin root).
WATCHED = [
    PLUGIN_ROOT / "scripts" / "oracle_ai_data_platform_fusion_autopilot" / "dimensions",
    PLUGIN_ROOT / "scripts" / "oracle_ai_data_platform_fusion_autopilot" / "transforms" / "gold",
]

# File patterns that count as "legacy module shape" (per PLAN §15 Phase 0 step 9).
LEGACY_PATTERNS = ["dim_*.py", "*.py"]
# Note: under `transforms/gold/`, every *.py module counts; under `dimensions/`,
# only `dim_*.py` files count (excludes __init__.py, helpers, etc.).

ALLOWLIST_FILE = Path(__file__).parent / "legacy_modules_allowlist.txt"


def _load_allowlist() -> set[str]:
    """Read allowlist entries; skip blank lines and comments."""
    if not ALLOWLIST_FILE.exists():
        return set()
    entries: set[str] = set()
    for raw in ALLOWLIST_FILE.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        entries.add(line)
    return entries


def _scan_dimensions() -> set[str]:
    """Return repo-root-relative paths of `dimensions/dim_*.py` files."""
    dimensions_dir = WATCHED[0]
    if not dimensions_dir.exists():
        return set()
    found: set[str] = set()
    for path in dimensions_dir.glob("dim_*.py"):
        rel = path.relative_to(PLUGIN_ROOT)
        found.add(rel.as_posix())
    return found


def _scan_gold_transforms() -> set[str]:
    """Return repo-root-relative paths of `transforms/gold/*.py` files (excluding __init__.py)."""
    gold_dir = WATCHED[1]
    if not gold_dir.exists():
        return set()
    found: set[str] = set()
    for path in gold_dir.glob("*.py"):
        if path.name == "__init__.py":
            continue
        rel = path.relative_to(PLUGIN_ROOT)
        found.add(rel.as_posix())
    return found


def test_no_unlisted_legacy_modules() -> None:
    """Fail if a legacy module exists without an allowlist entry."""
    allowlist = _load_allowlist()
    found = _scan_dimensions() | _scan_gold_transforms()
    unlisted = found - allowlist
    assert not unlisted, (
        "Found legacy-shaped modules not in the allowlist:\n"
        + "\n".join(f"  - {p}" for p in sorted(unlisted))
        + "\n\nv2 explicitly moves silver/gold modeling into content packs."
        + " If this addition is justified, add the path to"
        + f" {ALLOWLIST_FILE.relative_to(PLUGIN_ROOT)} with a comment explaining why."
        + " See PLAN §15 Phase 0 step 9 and §5.1."
    )


def test_allowlist_is_clean() -> None:
    """Allowlist entries that no longer exist on disk are stale and should be removed."""
    allowlist = _load_allowlist()
    found = _scan_dimensions() | _scan_gold_transforms()
    stale = allowlist - found
    assert not stale, (
        "Allowlist contains entries for modules that no longer exist:\n"
        + "\n".join(f"  - {p}" for p in sorted(stale))
        + f"\n\nClean up {ALLOWLIST_FILE.relative_to(PLUGIN_ROOT)}."
    )
