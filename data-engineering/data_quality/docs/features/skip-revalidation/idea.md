---
id: skip-revalidation
name: Skip-Revalidation — Skip Validation When Verdict Already in System Table
type: Enhancement
priority: P1
effort: Medium
impact: Medium
created: 2026-05-09
---

# Skip-Revalidation — Skip Validation When Verdict Already in System Table

## Problem Statement

The validation stage of the qualifire pipeline runs each
configured validator against the collected metric values and
writes one row per
`(dataset_name, validation_name, metric_name, dimension_value,
partition_ts)` natural key into the system table with a
severity verdict. Re-running the same validator for the same key
against the same input produces an identical verdict —
deterministic functions of metric value × thresholds × YAML
config.

This feature adds a `skip_revalidation: bool = False` runtime
flag that says: **for each validator the engine would invoke,
look up the natural key in the system table; if an active
validation row already exists for that key, replay the
persisted `validation_status` instead of running the validator.**

The eligibility check is purely **data-presence**: the question
is "is there a validation row at this key?", not "what kind of
validator is it?", "what kind of collector produced the input?",
or "what filter scope was applied?" The validation row either
exists in the system table or it doesn't.

This is independent of any collection-stage or notification-
stage skip mechanism. The validation stage stands on its own:
it consumes whatever the collection stage produced (cached or
freshly-collected), looks up its own persisted output, and
short-circuits when present.

## Why It Matters

- **Faster retries** after transient failures downstream of
  validation (notifier outages, persistence hiccups). The
  verdicts are already in the system table; re-running the
  validator just rebuilds them from scratch.
- **Cheaper backfills.** Validation is usually cheap relative
  to collection, but for validators with non-trivial
  computation (forecast's Prophet fit, pattern's RandomForest
  cross-validation) it's meaningful — the persisted severity
  is already the right answer.
- **Operator UX consistency** with the sibling
  `skip_recollection` and `skip_renotification` flags. Three
  symmetric flags, three pipeline stages, default-False each.
- **CI / smoke harness reproducibility.** Snapshot a known-
  good run, replay it, observe identical pipeline behaviour.

## Default

`skip_revalidation = False`. Re-validate on every run unless the
operator opts in. Matches the safe default of the sibling
skip-* flags.

## Who Benefits

- **Operators retrying after transient failures** in stages
  downstream of validation (notifier failure, system-table
  notification-row write hiccup).
- **Backfill drivers replaying historical partitions** where
  the validation rows for those partitions are already
  persisted and stable.
- **CI / smoke harnesses** asserting reproducibility.

## Affected Areas

### Public API — new `skip_revalidation` flag

- `Qualifire.run_config(..., skip_revalidation=False)`
- `Qualifire.run_config_parsed(..., skip_revalidation=False)`
- `Qualifire.backfill(..., skip_revalidation=False)`
- `Qualifire.write_audit_publish(..., skip_revalidation=False)`
- CLI: `--skip-revalidation` on `qualifire run` and
  `qualifire backfill`
- `QualifireContext.skip_revalidation` attribute

### Engine — new validation-stage pre-pass

- `qualifire/core/engine.py` — add
  `_try_skip_revalidation(ds_config, val_config, collected,
  table)` that runs after collection and before the validator
  dispatch.
- The pre-pass checks the system table for an active validation
  row at the natural key. When present, builds a synthetic
  `ValidationResult` from the persisted row's
  `validation_status`, `validation_message`, `metric_value`,
  `actual_value_text`, etc., marks `details['from_cache'] = True`,
  and returns it.
- When no row is present (or any expected validation row is
  missing), the pre-pass returns `None` and the validator
  dispatch runs normally.
- Eligibility is pure data-presence — no validator-type
  whitelist. Sample-based validators (pattern, anomaly) emit
  one validation row per (sample-validator, metric=auc/score)
  just like every other type; their persisted row is replayable
  the same way.

### Storage — read path

- New helper
  `read_validation_at_partition(table_name, validation_name,
  metric_name, partition_ts, dimension_value, ...)` returning
  the most-recent active validation row, or `None`.
  Implemented across all four backends (`SQLiteStorage`,
  `DeltaStorage`, `JDBCStorage`, `ExternalCatalogStorage`).
- Filter-scope safety: if validation rows can carry different
  filter scopes for the same `(dataset, validation, metric,
  dimension, partition)` natural key, the lookup must
  distinguish them. The existing `read_validation_history`
  read paths show how each backend filters by scope today;
  `read_validation_at_partition` mirrors that contract.
  Plan-phase question: do we need a new identity column for
  validation-stage rows analogous to whatever the collection
  stage uses, or is the existing natural key sufficient?

### Tests

- Per-collector regression matrix: each collector type
  (aggregation, profiling, metrics, sample, custom_query,
  recency) produces validation rows that replay correctly
  when `skip_revalidation=True`.
- Per-validator-type assertion (one regression each for SLO,
  threshold, drift, forecast, pattern, anomaly, profiling
  threshold) confirming the persisted row's severity replays
  without touching the validator's compute path.
- Stale row handling: tombstoned (`is_active='false'`) rows
  do NOT short-circuit; the engine falls through to re-validate.
- Cross-flag matrix: `skip_recollection` ×
  `skip_revalidation` × `skip_renotification` covered
  combinatorially (8 cells) — but each combination is verified
  for INDEPENDENT correctness, not for cross-flag dependencies.

### Documentation

- `docs/programmatic_api.md` — flag added with examples.
- `docs/CHANGELOG.md` — Enhancement entry with retry / replay
  use cases.
- Validator pages — each validator's page notes "this
  validator's verdicts can be replayed from the system table
  via `skip_revalidation=True`".

## Relationship to Other Features

- **Independent of `skip-recollection` and
  `skip-renotification`.** All three are sibling flags, each
  governing one pipeline stage, each gated on data presence in
  the system table for that stage's natural key. They compose
  freely — any subset of the three can be enabled in any
  combination — but they don't depend on each other for
  eligibility, schema, or correctness. Plan phase ships them
  as independent features.
- **Does not depend on
  `and-combine-collector-filter`'s runtime cache bypass.**
  That bypass operates at the collection stage; this feature
  operates at the validation stage. Validation rows already
  carry enough natural-key identity for safe replay.

## Open Questions for Planning

- **Validator output round-trip fidelity.** The persisted
  validation row carries `validation_status`,
  `validation_message`, `actual_value`, `actual_value_text`,
  `expected_value`, `details_json`, `dimension_value`. A
  synthetic `ValidationResult` rebuilt from these should be
  byte-equal to the freshly-computed one for the same input.
  Plan phase verifies for each validator type that no
  in-memory-only field (an unpersisted detail key, a
  derived-on-the-fly attribute) breaks replay.
- **Filter-scope identity in validation rows.** Validation
  rows currently identify by
  `(dataset, validation_name, metric, dimension, partition_ts)`
  with no filter dimension. If two runs of the same validator
  with different filter scopes can land in the system table
  for the same natural key, the lookup is ambiguous.
  Plan-phase audit needed.
- **Multi-row validation history under soft-delete.**
  Existing `read_validation_history_bulk(limit > 1)` is a
  known gap (`backfill-followups-and-polish`). This feature
  needs `limit=1`, so it doesn't block on that.
- **Engine warnings.** Engine-level warnings (persistence
  failures, suppression-read failures) write validation-style
  rows. Are those replayable via `skip_revalidation`? Probably
  not — they're emitted from infrastructure paths, not from a
  configured validator. Plan phase pins the boundary.

## Migration

- No migration needed for the flag itself; default `False`
  matches today's "always re-validate" behaviour.
- Storage helper additions are additive — existing readers
  continue to work.
