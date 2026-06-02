---
id: codex-review-fixes
name: Address Codex Review Findings
type: Bug Fix
priority: P0
effort: Large
impact: High
created: 2026-04-18
---

# Address Codex Review Findings

## Problem Statement

An external Codex review of the core library surfaced six correctness
and contract issues in the public API, plus one documentation drift.
The critical findings are inlined below; the corresponding plan
captures evidence lines, proposed changes, tests, and decisions per
item.

The critical issues cause real incorrectness, not just rough edges:

1. **`Qualifire.validate(df=...)` is a no-op.** Callers pass a DataFrame
   expecting it to be validated; the engine silently validates the named
   table instead. Can produce metrics on stale or unrelated data.
2. **`custom_query.filter` is accepted by config but silently ignored.**
   Dataset-level filters are dropped on custom-query validations, so
   metrics compute over the wrong slice. Collector's contract also makes
   the obvious "wrap with WHERE" fix semantically wrong.
3. **JDBC system-table backend is documented and has a storage impl,
   but `Qualifire.__init__` can't initialize it.** Anyone following the
   docs hits a hard runtime error.
4. **The documented `step` window for drift and forecast is dead
   config.** Users configuring week-over-week or daily cadence get the
   latest N rows instead. Biases drift detection and forecast accuracy
   under irregular schedules.
5. **Pandas is advertised as first-class but several collectors still
   call `.collect()` unconditionally.** Tests hide this with an
   `E2EPandasBackend` shim.
6. **`qualifire report` crashes on non-SQLite backends when Spark is
   missing.** The CLI fallback only works for SQLite, yet advertises
   itself as the no-Spark path.
7. **README points to a nonexistent `requirements-all.txt`.** Fresh
   installs following the README fail.

## Why It Matters

- Items 1, 2, and 4 produce **wrong results silently** — the worst
  failure mode for a data-quality tool. A drift or forecast validator
  that quietly uses the wrong data is indistinguishable from one that
  works, until someone notices metrics don't match reality.
- Items 3 and 6 produce **hard failures on documented paths** — users
  following the README or config reference hit errors immediately.
- Item 5 is **overstated compatibility** — the test suite itself
  papers over a production gap.
- The review will be repeated on the fix PRs, so the response needs to
  survive careful re-review.

## Affected Areas

- `qualifire/api.py` — `validate()`, `_init_storage()`
- `qualifire/collection/custom_query.py` — filter contract
- `qualifire/collection/recency.py` — Pandas compatibility
- `qualifire/core/engine.py` — temp-view lifecycle for `df=...`
- `qualifire/validation/historical.py`, `qualifire/validation/forecast.py` — `step` plumbing
- `qualifire/storage/*.py` — `step` implementation (if implementing)
- `qualifire/cli.py` — report fallback gating
- `README.md`, `docs/configuration.md`, `docs/programmatic_api.md`,
  `docs/trend_check.md`, `docs/validators.md` — doc alignment
- `tests/` — regression coverage for every fixed item
