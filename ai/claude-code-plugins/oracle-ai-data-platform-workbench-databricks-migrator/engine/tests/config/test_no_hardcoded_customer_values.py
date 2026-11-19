"""Ban-list test: no customer-specific value may appear in source code.

Runs a static grep over `scripts/`, `dbx_export/`, `aidp_compat/`, and
`run_migration.sh`. This is the enforcement floor for Phase 2 — the goal
state is ZERO violations.

Phase 2 is not complete yet, so the pre-existing offenders are FROZEN in
`phase2_baseline.txt` (path<TAB>string<TAB>count). The test fails only when
a file's count for a forbidden string EXCEEDS its frozen baseline (or a new
file/string pair appears) — i.e. it gates NEW hardcodings while the burn-down
proceeds. Shrink the baseline as offenders are cleaned up; when it is empty,
delete it and this test becomes the strict zero-violations gate.
`test_phase2_baseline_is_not_stale` nudges the baseline downward: entries
whose offenders are fully cleaned must be removed from the file.

Excluded from search: `tests/`, `aidp_config/example.customer.yaml`, docs
(`README.md`, `CLAUDE.md`), reports/, __pycache__/, .venv/, .git/, and
customer config files (`*aidp_migrator.yaml` — those legitimately contain
these values).

If you need to legitimately reference a forbidden string (e.g., in a
docstring example that MUST show the exact format), add it to
`_ALLOWLIST_LINES` with a comment.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest


REPO_ROOT = Path(__file__).resolve().parents[2]

# Strings that MUST NOT appear in source code. Grouped by category for
# diagnostic readability.
FORBIDDEN_STRINGS = {
    # ─── the pilot deployment identity values ─────────────────────────────────────────────
    "00000000-0000-4000-8000-000000000001",              # the pilot deployment workspace UUID
    "amaaaaaaexampleexampleexampleexampleexampleexampleexample001",   # the pilot deployment lake OCID hash
    "00000000-0000-4000-8000-000000000002",              # the pilot deployment cluster UUID (aidp_compat mock)
    "arn:aws:sts::000000000000",                          # the pilot deployment entity-crypto AWS account
    "mlflow.customer.example",                         # the pilot deployment MLflow host
    # ─── the pilot deployment OCI namespaces ───────────────────────────────────────────────
    "exmplnspace1",
    "exmplnspace2",
    "exmplnspace3",
    "exmplnspace4",
    # ─── the pilot deployment paths / buckets ──────────────────────────────────────────────
    "example-user",
    "/Workspace/connection-keys/Config",
    "/Workspace/scratch/param",
    "/Workspace/dbc/config",
    "migration_sandbox",
}

# Reintroduction guard for the pilot deployment's name: enforced by the
# whole-tree scan in test_publication_scrub.py via the EXTERNAL ban list
# (AIDP_PUBLICATION_BANLIST) — the name must not live in this repository in
# ANY reversible form, including assembled-at-runtime constants.

# Directories to search under REPO_ROOT.
SEARCH_ROOTS = ("scripts", "dbx_export", "aidp_compat")

# Individual files to also search (relative to REPO_ROOT).
SEARCH_FILES = ("run_migration.sh",)

# Path substrings to skip (all matched against str(path)).
EXCLUDE_SUBSTRINGS = (
    "/tests/",
    "/aidp_config/",           # loader is generic; example.customer.yaml has none
    "/__pycache__/",
    "/.venv/",
    "/.git/",
    "/reports/",
    "/aidp_compat/build/",     # built copies
    "/dbx_export/scripts/",    # sandboxed dbx-side fixtures
    "/tests/config/fixtures/", # customer YAML fixtures may reference values
)

# Lines that are ALLOWED to contain a forbidden string. Each entry is
# (path-relative-to-repo, forbidden-string) — the exact match survives the
# ban-list check. Use sparingly and comment why.
_ALLOWLIST_LINES = frozenset({
    # (populated as we discover legitimate exceptions during Phase 2)
})


def _iter_source_files():
    for root in SEARCH_ROOTS:
        for path in (REPO_ROOT / root).rglob("*"):
            if not path.is_file():
                continue
            if not path.suffix in (".py", ".sh", ".yaml", ".yml", ".json"):
                continue
            # Normalise to forward slashes so EXCLUDE_SUBSTRINGS ("/tests/")
            # also matches on Windows, where str(path) uses backslashes.
            spath = str(path).replace("\\", "/")
            if any(x in spath for x in EXCLUDE_SUBSTRINGS):
                continue
            yield path
    for f in SEARCH_FILES:
        p = REPO_ROOT / f
        if p.is_file():
            yield p


_BASELINE_PATH = Path(__file__).with_name("phase2_baseline.txt")


def _load_baseline():
    """{(rel_path, forbidden_string): frozen_count} from phase2_baseline.txt."""
    baseline = {}
    if not _BASELINE_PATH.is_file():
        return baseline
    for raw in _BASELINE_PATH.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        rel, forbidden, count = line.split("\t")
        baseline[(rel, forbidden)] = int(count)
    return baseline


def _scan_violations():
    """[(rel, lineno, forbidden, sample)] for every non-allowlisted hit."""
    violations = []
    for path in _iter_source_files():
        try:
            text = path.read_text(encoding="utf-8", errors="ignore")
        except Exception:
            continue
        rel = path.relative_to(REPO_ROOT).as_posix()
        for lineno, line in enumerate(text.splitlines(), start=1):
            for forbidden in FORBIDDEN_STRINGS:
                if forbidden in line:
                    if (rel, forbidden) in _ALLOWLIST_LINES:
                        continue
                    violations.append((rel, lineno, forbidden, line.strip()[:120]))
    return violations


def test_no_forbidden_strings_in_source():
    """Fails if any file gains a forbidden string beyond its frozen baseline."""
    baseline = _load_baseline()
    counts = {}
    samples = {}
    for rel, lineno, forbidden, sample in _scan_violations():
        key = (rel, forbidden)
        counts[key] = counts.get(key, 0) + 1
        samples.setdefault(key, []).append(f"  {rel}:{lineno}  [{forbidden}]  {sample}")
    regressions = {
        key: n for key, n in counts.items() if n > baseline.get(key, 0)
    }
    if regressions:
        lines = ["NEW hardcoded customer values (beyond the frozen Phase-2 baseline):"]
        for key in sorted(regressions):
            over = counts[key] - baseline.get(key, 0)
            lines.append(f"* {key[0]} [{key[1]}]: {counts[key]} found, "
                         f"{baseline.get(key, 0)} frozen ({over} new)")
            lines.extend(samples[key][:5])
        lines.append("Move the value to the customer YAML (aidp_migrator.yaml) — do "
                     "not extend phase2_baseline.txt for new code.")
        pytest.fail("\n".join(lines))


def test_phase2_baseline_is_not_stale():
    """Baseline entries whose offenders are gone must be deleted (ratchet).

    This keeps the burn-down honest: once a file is cleaned, its frozen
    allowance disappears so the string cannot quietly come back.
    """
    baseline = _load_baseline()
    if not baseline:
        pytest.skip("no phase2_baseline.txt — strict mode")
    counts = {}
    for rel, _lineno, forbidden, _sample in _scan_violations():
        key = (rel, forbidden)
        counts[key] = counts.get(key, 0) + 1
    stale = [key for key in baseline if key not in counts]
    if stale:
        lines = ["phase2_baseline.txt entries fully cleaned up — remove them:"]
        lines.extend(f"  {rel}\t{forbidden}" for rel, forbidden in sorted(stale))
        pytest.fail("\n".join(lines))
