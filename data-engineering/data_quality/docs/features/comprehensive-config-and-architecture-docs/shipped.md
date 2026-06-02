---
shipped: 2026-05-10
---

# Shipped: comprehensive-config-and-architecture-docs (focused minimum)

## Summary

Focused-minimum docs refresh — README absorbs the
recently-shipped capabilities; new `docs/architecture.md`
traces an end-to-end value flow + WAP lifecycle + the
storage read contract; 5 deferred-scope items captured as
backlog idea.md files. The original "comprehensive
configuration reference" is the most labor-intensive surface
and is intentionally deferred to the
`configuration-reference-audit` backlog item the user can
prioritize when ready.

## Key Changes

- **`docs/architecture.md`** (new, ~290 lines):
  - Section 1: ASCII data-flow diagram tracing
    `Qualifire.run_config_parsed` → `_run_dataset` →
    collection phase → validation phase → snapshot prefetch
    → persistence → notifications → next-run history read,
    with each step naming concrete module paths.
  - Section 1b: Storage read pattern — two-stage logic
    (rank by recency → filter by `is_active`) with per-
    backend shape variations enumerated honestly.
  - Section 1c: Skip-* flag interaction matrix (which
    storage helper each flag's pre-pass calls).
  - Section 2: WAP lifecycle ASCII diagram (write → audit →
    publish or rollback).
  - Section 3: pointers to all 5 backlog items.
- **`README.md`** revised:
  - Key Features bullets now mention skip-* flags,
    backfill+soft-delete, drift explainer per-slice, column-
    name redaction, plus the architecture pointer.
  - **New** "Minimal example (no Spark required)" Quick Start
    using PandasBackend + in-memory SQLite. Verified by
    running the literal snippet locally:
    `overall_severity: PASS`. Output line is in the doc.
  - **Notifier precedence** corrected from the stale
    "YAML wins" claim to the actual two-layer contract
    (YAML + programmatic; programmatic always wins). Pointer
    to `docs/programmatic_api.md` as authoritative.
  - **New** "Best Practices" section (~75 lines) covering:
    when to use the runtime skip-* flags; history warm-up
    for drift/forecast/pattern; `partition_ts` consistency;
    column-redaction scope precedence; dashboard cadence;
    WAP for partial-day backfills.
- **`docs/CHANGELOG.md`**: Documentation entry under
  Unreleased.
- **5 backlog `idea.md` files**:
  - `configuration-reference-audit` — audit + extend the
    existing 665-line `docs/configuration.md` to cover the 9
    most-used surfaces.
  - `pydantic-docstring-audit` — audit + tighten Pydantic
    config-class docstrings.
  - `architecture-backfill-loop-diagram` — backfill driver
    loop ASCII diagram.
  - `docs-lint-ci` — broken-internal-link smoke test.
  - `configuration-reference-collectors-extension` — extends
    the audited reference to 4 less-common collectors.

## Files Changed

8 files; +662 / −19 lines.

## Plan PR

[#26](https://github.com/amitranjan-oracle/qualifire/pull/26).

## Review Cycles

Plan: 1 adversarial + 3 codex rounds. Codex R1 found 2 MAJORs
(data-flow ordering vs engine.py:475; missing CHANGELOG sweep)
+ 1 MEDIUM (AC3 unverifiable) + 2 LOWs. R2 found 1 MEDIUM
(`ValidationResult` vs `QualifireResult` for `overall_severity`).
R3 PASS at v4.

Implementation: 3 codex rounds.
- R1: 2 MAJORs — SQL "same shape on every backend" was false;
  notifier-precedence "3-layer" was fabricated. Plus a LOW on
  unsupported "<1s" dashboard claim.
- R2: SQL fix was still inaccurate — caught remaining
  per-backend shape mismatches. Notifier + dashboard fixes
  verified.
- R3 PASS.

## Local Test Results

- 1506 passed, 2 skipped (no production-code changes; no
  regressions).
- Quick Start example runs end-to-end against a current
  qualifire install.
