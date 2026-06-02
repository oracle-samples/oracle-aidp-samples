---
id: backfill-severity-before-broken-readback
name: Fix _read_severity_before in backfill (always returns None today)
type: Bug Fix
priority: P3
effort: Small
impact: Low
created: 2026-05-10
---

# Fix `_read_severity_before` in backfill

## Problem Statement

`qualifire/core/backfill.py:_read_severity_before` calls
`storage.read_validation_history(validation_name="")`. The
SQLite backend's exact-match `validation_name = ?` filter never
matches a real validation row (validation_name is always
populated), so `severity_before` is always `None`. Surfaced by
codex impl-review R4 HIGH during the
`backfill-followups-and-polish` plan iterations.

Effect: `BackfillReport.partitions[*].severity_before` is always
`None` — operators can't tell if a backfill flipped a row's
severity, only that the value changed. The diff-classification
also over-reports `refreshed` when only severity differs (the
value is unchanged but the validator evaluated to a different
severity in the new run, but we don't see the prior severity to
compare).

## Why This Was Deferred

Surfaced during the `backfill-followups-and-polish` plan
review (R4). Fixing it would expand scope into the
`_classify_diff` semantics (`refreshed` vs `unchanged` when only
severity differs) — orthogonal to the parallelism / multi-row /
auto-detect work that feature was scoped to.

The pre-existing behavior is preserved by the refactor (Pass 1a
still calls `_read_severity_before` exactly as before). This
backlog item picks up the fix in its own iteration.

## Proposed Approach

The simplest fix: pass the actual validation name(s) into
`_read_severity_before`. The scope's `validations` is in scope
at the call site — pick the validation that produced the metric
(metric → validation lookup via `validation.expected_metrics()`).

If multiple validations share one metric, define a deterministic
aggregation policy (e.g., max severity wins).

## Affected Areas

- `qualifire/core/backfill.py:_read_severity_before`
- `qualifire/core/backfill.py:_process_anchor` (caller, needs
  metric→validation name resolution)
- `tests/test_backfill.py` (regression test: unchanged value +
  unchanged severity reports `unchanged`, not `refreshed`)

## References

- `backfill-followups-and-polish` plan §HS8 (out-of-scope marker).
- Codex impl-review R4 HIGH.
