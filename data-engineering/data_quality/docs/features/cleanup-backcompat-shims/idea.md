---
id: cleanup-backcompat-shims
name: Cleanup — delete backwards-compat shims & stale review-history comments
type: Tech Debt
priority: P2
effort: Medium
impact: Medium
created: 2026-05-11
---

# Cleanup — Backcompat Shims

## Problem Statement

`qualifire` is pre-release with no external users. Yet the
codebase carries multiple migration shims, dual-shape
acceptance paths, and stale "Round-N / Codex round X /
pre-2026" review-history comments that exist only to protect
hypothetical legacy callers. They make the code harder to
read, harder to review, and signal a maturity the project
hasn't actually committed to.

## What to Delete

Surfaced by a code/docs/tests audit on 2026-05-11. Each item
is a single deletable shim; nothing depends on external
users having migrated:

1. `_REMOVED_FIELDS_GUIDANCE` + `_reject_removed_fields`
   (`qualifire/core/config.py`) — catches removed YAML keys
   (`suppress_repeat_alerts`) with a custom migration error.
   Wired into 6 validation config classes.
2. `_reject_legacy_backend_field`
   (`qualifire/core/config.py`) — catches the old top-level
   `backend:` YAML key.
3. `CustomQueryCollectionConfig._reject_nonempty_filter` +
   its 19-line "Evolution across review rounds" docstring —
   rejects deprecated `filter:` on custom_query.
4. `expected_value_compact: bool = True` flag — controls
   compact vs verbose dict shape; verbose path is
   "pre-2026-Q2 behaviour" with no live caller.
5. Bare-number threshold bounds dual-shape — the drift /
   forecast validators accept BOTH `{deviation_pct: 30}`
   (legacy bare = symmetric) and `{min, max}` dict (signed).
   Drop the bare-number branch.
6. Storage-side null-tolerance shims — pre-release, no
   production data: `COALESCE(is_active, 'true')` defensive
   path in 4 backends; `record_type` null-tolerance in
   sqlite reads; the two `test_legacy_*` tests that exercise
   these paths. Verify first that new writes always set the
   columns; if yes, delete the COALESCE + tests.
7. Stale review-history comments — `pre-2026-05-09`,
   `Round-N`, `Codex round X` references in
   `qualifire/{api,core/config,core/engine,core/secrets,
   notification/_redact,reporting/*}.py`. Replace WHY-this-
   code-looks-like-this notes that aren't load-bearing
   anymore with either nothing (when obvious from
   tests/CHANGELOG) or with a concise WHY stripped of
   review-round phrasing.

## Why This Belongs in a Single Feature

The deletions touch a common set of files (`config.py`,
`engine.py`, storage backends, the same test packages) and
share one acceptance criterion: "no aliases, no dual paths,
no review-round narrative." Bundling them lets one codex
impl review verify the whole sweep against the user's
"no-backcompat" policy at once rather than 7 micro-PRs.

## Affected Areas

- `qualifire/core/config.py` (heaviest hit)
- `qualifire/core/engine.py`, `qualifire/api.py`,
  `qualifire/core/secrets.py`
- `qualifire/storage/{sqlite,delta,jdbc,external_catalog}_storage.py`
- `qualifire/notification/_redact.py`
- `qualifire/reporting/{health,html_report,_snapshot_details}.py`
- `tests/test_skip_renotification.py`,
  `tests/test_config.py`, `tests/test_api_from_config.py`,
  `tests/test_validation/test_threshold.py`,
  `tests/test_signed_drift.py`,
  `tests/test_storage/{test_sqlite,test_soft_delete_sqlite}.py`
- `README.md`
- `docs/configuration.md` (CustomQuery filter pitfall)
