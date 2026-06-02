---
id: findings-sweep
name: Findings Sweep
type: Tech Debt
priority: P0
effort: Large
impact: High
created: 2026-05-04
---

# Findings Sweep

## Problem Statement

Two independent reviews of the codebase (`.tmp/findings_codex.md` and
`.tmp/findings_claude.md`, consolidated in `.tmp/findings.md`) surfaced
five Blocking and eight Should-fix issues that span correctness,
public-API ergonomics, repository hygiene, and architecture. Several
items are silently wrong — they look like the library is working
when in fact validation is operating on the wrong segment, persistence
is dropping rows on the floor, or the public API parameter the user
specified is being ignored. The current state erodes trust on three
levels: end users see contradictory promises in docs vs. checked-in
source; operators lose alert fidelity through coarse routing and
swallowed persistence errors; and stakeholders read a generated
dashboard that lists work as shipped while the implementation lives on
an unmerged branch. The volume and shape of the findings argue for a
single coordinated sweep rather than incremental drip-fixes — most of
the Blocking items share a common architectural root (identity
propagation), and fixing them piecewise will make the eventual
identity-keys refactor more painful, not less.

## Why now

- Several items are *silently* wrong (no exception, no test failure)
  — every day they ship is a day a downstream consumer might be
  trusting a wrong answer.
- The boring-layer correctness gap undermines the case for the more
  sophisticated ML validators sitting on top of it.
- The reviews triangulated independently on the same neighborhoods,
  which means the issues are real, not stylistic.
- Top Priority #4 in the findings doc (typed identity keys) is the
  structural root of #1 and #3 — sequencing the architectural fix
  *with* the correctness fixes is cheaper than retrofitting later.

## In Scope

The findings document at `.tmp/findings.md` is the canonical input.
At minimum, this feature covers:

**Blocking**
- Dimension-sliced collection collapsing in threshold validator.
- Custom `validation_name` ignored for non-pattern validators.
- `validate(notify=)` and `write_audit_publish(notify=)` dead
  parameters.
- `Qualifire.write_audit_publish(df=)` monkey-patches engine
  internals.
- `spark-primary-industry-packs` shipped status inconsistent with
  checked-in source.

**Should-fix**
- Per-validation notification routing + dedupe (currently
  dataset-granular).
- WAP cleanup not exception-safe.
- `shape` validator's past-column union (mirror pattern's
  intersection fix).
- `engine._persist_results` swallows write failures.
- Inconsistent missing-PySpark behavior.
- N synchronous round-trips in `should_suppress`.
- `JDBCStorage` advertised as backend-agnostic but is Spark-coupled.
- `Backend` Protocol leaks materialization through `hasattr`
  branches.
- `requirements.txt` vs. tiered-install docs.

**Architectural**
- Promote identity into typed keys carried by both
  `CollectionResult` and `ValidationResult` — this is the structural
  fix underneath several Blocking items.

**Auto-gen hook hardening**
- Make the dashboard generator count only committed feature
  artifacts (so the next half-staged `shipped.md` doesn't reproduce
  the same inconsistency).

## Out of Scope

Polish items in the findings doc that don't relate to correctness or
the identity-key fix (cosmetic refactors, unrelated CLI scaffolding,
new validators) belong in their own backlog items. The split between
"in scope here" vs. "deferred" should be settled in the planning
phase, not pre-decided here.

## Affected Areas

- core (engine, config, models, exceptions)
- api
- collection (aggregation, sampler)
- validation (threshold, isolation_forest, pattern_check, base)
- notification (base, routing, dedupe)
- wap (lifecycle, exception safety)
- storage (jdbc, persistence error surfacing)
- backends (Backend Protocol)
- docs (README install story, industry pack coverage matrix)
- tests (regression coverage for each fix)
- feature-workflow tooling (dashboard auto-gen hook)

## Success Criteria

- Every Blocking and Should-fix finding either has a regression test
  pinning the fix or a documented decision to defer.
- A user calling `validate(notify=...)` produces a
  `NotificationResult` (or the parameter is removed and docstrings
  match).
- Multi-dimension threshold validation produces one
  `ValidationResult` per `(metric, dimension_value)` pair, persisted
  with the dimension intact.
- `shipped.md` and `DASHBOARD.md` on `main` agree with the manifest
  and example READMEs on `main`.
- Identity propagation: the same typed key flows from
  `CollectionResult` through `ValidationResult` into persistence and
  notification dedupe — verified by a test that writes through and
  reads back using the same key.

## References

- `.tmp/findings.md` — canonical combined findings.
- `.tmp/findings_codex.md` — Codex's original review.
- `.tmp/findings_claude.md` — Claude's original review.
