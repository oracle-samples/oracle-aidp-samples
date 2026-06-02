---
shipped: 2026-05-10
---

# Shipped: webhook-payload-redaction — Column-Name Redaction

## Summary

Compute-time column-name redaction across SHAP top-features,
value-drift explainer entries, and pattern_check / isolation_forest
schema-change emission (including the message-string interpolation).
Operators in compliance-sensitive industries (HIPAA / GDPR / PCI)
list sensitive source columns; the redacted name never enters
`details_json`, so every downstream egress (notifications, system
table, dashboard, programmatic API, third-party webhook)
automatically inherits the protection.

## Key Changes

- **New** `qualifire/core/_redaction.py`: `RedactionPolicy.build()`
  + `RedactionPolicy.redacted()`. Denylist union, allowlist one-
  sided semantics (None at one scope = other scope governs; both
  set = strictest-wins intersection), denylist precedence.
- **`qualifire/core/config.py:DatasetConfig`**: new
  `redacted_columns: list[str] = []` and
  `allowlist_columns: list[str] | None = None` fields.
- **`qualifire/api.py:Qualifire.__init__`**: new
  `redacted_columns=` and `allowlist_columns=` kwargs.
  `Qualifire.from_config` accepts both via overrides (codex R1
  MAJOR — initial commit forgot to wire through).
- **`qualifire/core/context.py`**: stash on context as
  `instance_redacted_columns` / `instance_allowlist_columns`.
  All 4 `ctx = QualifireContext(...)` sites in api.py + the
  per-anchor backfill ctx in backfill.py set both.
- **`qualifire/core/backfill.py`**: threaded
  `instance_redacted_columns` / `instance_allowlist_columns`
  through `run_backfill` → `_process_anchor` →
  `_run_anchor_once` (BOTH serial and parallel call sites —
  same indent-bug pattern as skip-renotification's R1).
- **`qualifire/core/engine.py:_validate`**: builds
  `RedactionPolicy` per dataset from instance + dataset lists;
  passes to `PatternCheckValidator` and `IsolationForestValidator`.
  Three `DatasetConfig` rebuilds in engine.py (query, df,
  staging WAP) and one in backfill.py copy
  `redacted_columns` / `allowlist_columns` (codex R1 MAJOR —
  initial commit dropped them on rebuild).
- **`qualifire/validation/{pattern_check,isolation_forest}.py`**:
  added `redaction_policy` param defaulting to no-op
  `RedactionPolicy()`. Three helper methods on each:
  `_redact_columns`, `_redact_top_features`,
  `_redact_drift_entries`. Redaction sites:
  - `top_contributing_features` (post-explainer; explainer gets
    REAL names so its `encoding_map.get(feat_name)` lookup works)
  - `value_drift_explainer` entries (identifying fields →
    `<redacted>`; numeric blocks intact)
  - schema-change emission (`new_columns` /
    `dropped_columns` / `inconsistent_past_columns` lists +
    `message` interpolation)
- **`qualifire/validation/_drift_explainer.py`**: untouched.
  Validators redact AFTER the explainer returns.

- **Tests**: 16 new tests across
  `tests/test_validation/test_pattern_check_redaction.py` and
  `tests/test_validation/test_isolation_forest_redaction.py`:
  T1 (denylist), T2 (no-op for non-listed), T3 (allowlist
  redacts outside-the-set), T4 (denylist precedence over
  allowlist), T5 (drift-entry identifying-field redaction with
  numeric blocks intact), T6 (instance + dataset denylist
  union), T7 (allowlist one-sided semantics), T8 (schema-
  change list + message redaction).

- **Documentation**: CHANGELOG Enhancement entry under
  Unreleased.

## Files Changed

12 files; +624 / −41 lines.

## Plan PR

[#22](https://github.com/amitranjan-oracle/qualifire/pull/22).

## Review Cycles

Plan: 2 adversarial + 3 codex rounds. Codex R1 found 2 BLOCKERs
(pre-explainer redaction language contradicting v3's post-
explainer fix) + 2 MAJORs (isolation_forest schema-change leak
+ message-string interpolation) + 1 MEDIUM (allowlist one-sided
semantics); R2 found contradiction between Locked Decisions and
body text on POST-explainer ordering + AC6 / T7+T8 mismatch;
R3 PASS at v5.

Implementation: 2 adversarial + 2 codex rounds. Codex R1 found
2 MAJORs:
1. `DatasetConfig` rebuilds (engine.py:663, 704, 2477;
   backfill.py:873) silently dropped `redacted_columns` /
   `allowlist_columns` — dataset-level redaction lost on query
   / df / WAP staging / backfill paths.
2. `Qualifire.from_config(...)` accepted_overrides set didn't
   include the new kwargs — programmatic from_config callers
   couldn't set them.
Both fixed; codex R2 PASS.

## Local Test Results

- 1492 passed, 2 skipped (+16 new tests; up from 1476).
- No regressions; full suite green.

## Known Limitations (deferred)

- **MEDIUM (out of scope)**: engine schema-error / validator-
  exception messages still interpolate raw column names via
  `validation_message`. These are infrastructure failure paths;
  the column names there are user-declared in YAML, not SHAP-
  emitted. Deferred to a separate `engine-warning-redaction`
  follow-up if any operator hits the leak.
- **LOW**: `_unmapped_entry` defensive path in `_drift_explainer.py`
  sets `source_column` to the post-encoded feature name.
  A denylisted source whose encoded variant lands in an unmapped
  entry would slip through. Acceptable as a defensive-fallback
  edge case; the normal mapping path handles all production
  shapes.
