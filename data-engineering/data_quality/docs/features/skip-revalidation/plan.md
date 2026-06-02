# Plan: skip-revalidation

## Goal

Add a runtime `skip_revalidation: bool = False` flag that
short-circuits the validator dispatch when an active validation
row already exists at the natural key for the current partition.
Faster retries / cheaper backfill replays / reproducible CI
snapshots without re-running validators that produce
deterministic verdicts on the same input.

## Locked Decisions (planner's recommendation)

1. **Default `False`.** Re-validate by default; opt-in for
   replays and retries. Mirrors `skip_recollection` /
   `skip_renotification`.
2. **Plumbing pattern**: mirror `skip_if_cached` exactly —
   present on `Qualifire.run_config`, `run_config_parsed`,
   `backfill`, `validate`. **NOT** on `validate_query` or
   `write_audit_publish` (codex R1 MEDIUM — `skip_if_cached`
   is also absent there; this feature mirrors the actual
   pattern, not an idealized "all entry points" version).
   Plus CLI flag and `QualifireContext.skip_revalidation`.
   No per-validation YAML knob.
3. **Eligibility = data presence**, not validator type. Sample-
   based validators (Pattern / IsolationForest), SLO, threshold,
   drift, forecast all replay identically.
4. **Engine warnings are NOT replayable.** Persistence /
   suppression-read warnings come from infrastructure paths,
   not configured validators. The pre-pass is scoped to user-
   configured validations only.
5. **Filter-scope lookup**: keyed on
   `(dataset, validation_name, metric_name, dimension_value,
   partition_ts)` only. The `cache-key-filter-identity`
   discussion settled on "same shape as collection-stage skip"
   per user direction during planning sweep — different filter
   would also skip if a row exists at the same natural key.
   Documented in the plan and CHANGELOG so the operator's mental
   model matches.
6. **Storage**: new per-backend helper
   `read_validations_at_partition(dataset_name,
   validation_name_prefix, partition_ts) -> list[dict]` —
   plural; returns ALL active validation rows whose
   `validation_name` matches the prefix exactly OR starts
   with `prefix + "."` (catches Pattern's `.schema` sub-
   emission). Latest dedup per natural key
   `(metric_name, dimension_value)`. All 4 backends.
7. **Synthetic ValidationResult rebuild** (codex R1 BLOCKER —
   explicit field mapping; persisted row lacks
   `validation_base_name`):
   - `validation_name` → `validation_name` (verbatim)
   - `validation_type` → `validation_type` (verbatim)
   - `validation_status` (string `"PASS"`/`"WARNING"`/`"ERROR"`)
     → `severity` via `Severity(row["validation_status"])`
   - `validation_message` → `message`
   - `metric_name` → `metric_name` (verbatim, can be `None`)
   - `metric_value` → `actual_value` (the in-memory shape
     stores the numeric value here)
   - `actual_value_text` → `actual_value_text`
   - `dimension_value` → `dimension_value`
   - `expected_value` → JSON-decode if non-None
     (`json.loads(row["expected_value"])`); persisted as a
     JSON string per the storage write path
   - `details_json` → JSON-decode into `details` dict; then
     `details["from_cache"] = True` marker added
   - `validation_base_name` → resolved from the **current
     val_config** via `_resolve_validation_base_name(val_config,
     ds_config)` (NOT in the persisted row; the storage schema
     has no `validation_base_name` column — codex R1 BLOCKER)
   - `collected_at` / `validated_at` → `None` (these are wall-
     clock timestamps; replaying with stale ones would
     misleadingly suggest the work happened just now). The
     persisted row's run_timestamp is preserved separately on
     the row that gets re-persisted, so audit trail stays.
8. **Pre-pass timing**: AFTER collection produces
   `CollectionResults`, BEFORE validator dispatch. The
   collection step still runs (or is itself short-circuited
   by `skip_recollection`); only the validator's compute is
   bypassed.
8a. **Anchor resolution** (codex R1 MAJOR): the pre-pass
    derives `anchor_partition_ts` via the existing
    `_resolve_run_level_partition_ts(self._resolve_partition_ts_expr(ds_config),
    self.context)` helper at `engine.py:725` area. Same
    resolved-anchor the persistence path uses post-validation
    (per `_partition_ts_for_validation`); ensures the
    pre-pass lookup matches the partition the freshly-
    computed run would have written. Sample-based validators
    (Pattern / IsolationForest) get the same run-level
    fallback they already use during persistence; their
    persisted `auc` / `anomaly_ratio` rows DO carry that
    partition_ts via `partition_ts` column, so the lookup
    works (codex R1 MAJOR — sample-validator partition trail
    exists end-to-end through persistence). Returns `None`
    when the resolver can't compute an anchor (no
    `partition_ts` set anywhere) — pre-pass falls through to
    validator.
9. **Persisted side**: the synthetic ValidationResult goes
   through the same persistence path as a fresh one — appears
   in `details_json`, system table, notifications. Operators
   see identical downstream behavior. (`from_cache=True` is the
   only marker.)

## What Changes

### Public API — flag plumbing pattern (mirror skip_if_cached)

- **`qualifire/core/context.py`** — add
  `self.skip_revalidation: bool = False` next to
  `skip_renotification`.
- **`qualifire/api.py`** — add `skip_revalidation: bool = False`
  to:
  - `run_config(...)`
  - `run_config_parsed(...)` — sets
    `ctx.skip_revalidation = skip_revalidation`
  - `backfill(...)` — pass-through to `run_backfill`
  - `validate(...)` — sets ctx
  - **NOT** added to `validate_query` or `write_audit_publish`
    (mirrors actual `skip_if_cached` plumbing — Locked
    Decision 2).
- **`qualifire/core/backfill.py`** — thread through
  `run_backfill` → `_process_anchor` → `_run_anchor_once` →
  `ctx.skip_revalidation`. **BOTH** serial and parallel
  call sites (this trap caught skip-renotification's R1).
- **`qualifire/cli.py`** — `--skip-revalidation` argparse flag
  on `run` and `backfill` subcommands.

### Storage — new per-backend helper

`read_validations_at_partition(self, *, dataset_name: str,
validation_name_prefix: str, partition_ts) -> list[dict]`

(R1 MAJOR rework: from "predict shape, lookup by natural key"
to "any rows found = replay all rows". Avoids hardcoding
validator-emission knowledge in the engine.)

- Returns all active validation rows (record_type =
  'validation', is_active='true') at the exact partition_ts
  whose `validation_name` column matches the prefix exactly
  OR starts with `prefix + "."` (catches Pattern's `.schema`
  sub-emission). The system table has no `validation_base_name`
  column; the engine derives the base name from the current
  `val_config` post-fetch when rebuilding the synthetic
  ValidationResult (Locked Decision 7).
- Mirrors the existing `read_collection_metric_at_partition`
  read shape; same SQL idioms apply.
- Implementations:
  - **SQLite**: `SELECT ... WHERE record_type='validation' AND
    is_active='true' AND dataset_name=? AND
    (validation_name=? OR validation_name LIKE ? || '.%') AND
    partition_ts=? ORDER BY run_timestamp DESC` (latest
    deduped per natural key).
  - **Delta / ExternalCatalog**: same SQL through
    `self._spark.sql(...)`.
  - **JDBC**: same SQL through `_read_jdbc()`.
- Latest-row dedup per natural key: identical to the existing
  `read_validation_history_bulk` ROW_NUMBER over `(metric_name,
  dimension_value)` partition.

### Engine — new pre-pass (R1 MAJOR rework)

New method
`_try_skip_revalidation(self, ds_config, val_config,
logical_table, anchor_partition_ts) -> list[ValidationResult]
| None` invoked from `_validate(...)` BEFORE the validator
branch.

- Returns `None` when:
  - `self.context.skip_revalidation` is False (the gate)
  - `self.storage` is None
  - `anchor_partition_ts is None` (no resolvable anchor —
    can't do a per-partition lookup; fall through to validator)
  - `read_validations_at_partition(...)` returns an empty list
    (no persisted rows found at this anchor)
- Returns `list[ValidationResult]` when AT LEAST ONE active
  validation row is found at the anchor for this validator's
  base name. Every returned row is rebuilt into a
  `ValidationResult` with `details["from_cache"] = True`. The
  validator's compute is bypassed entirely.

**Why "any row = replay all rows" instead of "predict shape +
all-or-nothing"** (codex-likely-MEDIUM): different validators
emit different shapes (Pattern's `.schema` sub-emission;
cold_start markers; error states like `non-finite-scores`;
per-dimension rows). Predicting the full set requires the
engine to mirror every validator's emission logic — a
maintenance trap that would silently skip valid rows when a
new validator type or new emission shape is added. The
data-presence approach replays whatever was persisted in
whatever shape, exactly matching today's persisted snapshot.
Trade-off: if the validator emitted nothing on a prior run
(pure no-op), the engine re-validates this run. Acceptable —
"no row found" is the right signal that the validator should
re-run.

- Engine warning rows (record_type=validation,
  validation_name="qualifire.persistence" /
  "qualifire.suppression_read") are excluded by the
  `dataset_name` filter (engine warnings carry
  `dataset_name="qualifire.engine"`, not the user's dataset
  name) — Locked Decision 4 holds without special-casing.

### Tests

- **New** `tests/test_skip_revalidation.py`:
  - T1: flag=True, validation row exists at exact partition →
    notifier sees a validation result whose `details.from_cache`
    is True; the validator's `validate(...)` method is NOT
    called (assertion via `MagicMock` patch on the validator).
  - T2: flag=False (default) → validator runs as today
    even when a persisted row exists.
  - T3: flag=True, NO persisted row at this partition →
    validator runs (fall-through correctness).
  - T4: flag=True, row exists but `is_active='false'`
    (tombstone) → validator runs (tombstone honored).
  - T5: per-validator-type matrix — one regression each for
    SLO, threshold, drift, forecast, pattern, anomaly. Pin
    that the rebuilt ValidationResult byte-equals the freshly-
    computed one (severity, message, metric_name, metric_value,
    actual_value_text, dimension_value).
  - T6: parallel backfill threads the flag into every worker
    (the same indent-bug guard the previous two features hit).
  - T7: cross-flag — `skip_recollection=True,
    skip_revalidation=True` simultaneously: both stages
    short-circuit, persistence still records the run, no
    validator OR collector compute happens.
  - T8: filter-scope documented limitation — running with
    `filter` set, a prior run without `filter` still
    short-circuits (per Locked Decision 5; pin the behavior
    for review-time visibility).

### Documentation

- `docs/programmatic_api.md` — flag added with example.
- `docs/CHANGELOG.md` — Enhancement entry under Unreleased.
- `docs/configuration.md` — add a "Runtime skip flags" section
  listing the trio (skip_recollection / skip_revalidation /
  skip_renotification) with their per-stage gates.
- Validator pages — note "verdicts are replayable via
  `skip_revalidation=True`".

## Acceptance Criteria

- AC1: `skip_revalidation: bool = False` on
  `Qualifire.run_config`, `run_config_parsed`, `backfill`,
  `validate`, plus `--skip-revalidation` on CLI run +
  backfill, plus `QualifireContext.skip_revalidation`. Mirrors
  `skip_if_cached` (NOT on `validate_query` /
  `write_audit_publish`).
- AC2: New `read_validations_at_partition` helper on all 4
  backends (SQLite, Delta, ExternalCatalog, JDBC) returning a
  list of dicts (zero or more rows). Plural — to catch
  multi-row emissions like Pattern's `.schema` sub-emission
  alongside the main `auc` row, plus per-dimension rows.
- AC3: `_try_skip_revalidation` returns whatever
  `read_validations_at_partition(...)` finds at the anchor
  partition, with each row rebuilt as a ValidationResult
  carrying `details.from_cache=True`. Empty result → validator
  runs normally. (Data-presence semantics; not "predict-shape
  all-or-nothing".)
- AC4: The synthetic ValidationResult carries
  `details["from_cache"] = True` and has identical
  severity / message / metric / value / dim fields to the
  persisted row.
- AC5: Tombstones (`is_active='false'`) do NOT short-circuit;
  the validator runs.
- AC6: Default-False produces today's behavior verbatim
  (regression check via the existing test_engine.py
  validation-flow tests still passing without modification).
- AC7: Tests T1–T8 pass. Parallel backfill threads the flag
  to every worker (T6 — covers the recurring indent bug).
- AC8: Cross-flag with `skip_recollection` (T7) — both stages
  short-circuit independently; persistence still records run.

## Risks and Mitigations

| Risk | Mitigation |
|------|------------|
| Filter-scope ambiguity (Locked Decision 5) — same natural key under different filter scopes replays the prior verdict | Document explicitly in CHANGELOG + flag's docstring; same trade-off the operator agreed to for skip_recollection. |
| Synthetic ValidationResult drifts from real one over time as new fields are added | AC4 + T5 per-validator regressions pin the round-trip; future additions to ValidationResult must update the synthetic-builder helper or the test fails. |
| `read_validations_at_partition` performance on Spark (extra round-trip per validator) | Defer optimization to a `read_validations_at_partition_bulk` follow-up if real workloads show contention. Per-validator point lookups are cheap on Spark dialect already. |
| Parallel backfill flag-drop bug (recurring) | T6 explicit assertion. |
| Engine warnings replay accidentally (Locked Decision 4) | Pre-pass is invoked from `_validate`, which is only called for user-configured validators — engine warnings are emitted from a different code path. Pin via test. |

## Out-of-Band Reviews

- 2 adversarial plan reviews.
- 2 codex plan reviews → iterate to PASS.
- Then implement; 2 adversarial impl reviews + 2 codex impl reviews.

## Effort

Medium. Estimated:
- ~120 LOC across 4 storage backends (new helper).
- ~80 LOC engine pre-pass.
- ~50 LOC API/CLI/context plumbing.
- ~250 LOC tests.
- Docs.

## Plan Iteration Log

- v1: initial draft.
- v5: addressed codex plan-review round 3 — Risks table still
  used singular `read_validation_at_partition` /
  `_bulk` names; updated to plural matching LD6/AC2/Storage.
- v4: addressed codex plan-review round 2 —
  (a) Locked Decision 6 contradicted AC2 / Storage section by
  describing a singular helper; rewrote to plural matching the
  v3 design;
  (b) Storage helper parameter `validation_base_name_prefix`
  contradicted the SQL filter (which queries `validation_name`
  column); renamed to `validation_name_prefix` so the parameter
  name and the column name match unambiguously.
- v3: addressed codex plan-review round 1 —
  (a) BLOCKER: AC2 corrected to `read_validations_at_partition`
  (plural) returning a list of dicts;
  (b) BLOCKER: synthetic ValidationResult rebuild now
  explicitly maps every persisted column, parses
  `Severity(...)` enum, JSON-decodes `expected_value` /
  `details_json`, derives `validation_base_name` from current
  val_config (NOT in persisted row);
  (c) MAJOR: pinned anchor resolution via existing
  `_resolve_run_level_partition_ts` helper at engine.py:725;
  (d) MAJOR: confirmed sample-validator partition trail
  exists end-to-end (Pattern's `auc` and IsolationForest's
  `anomaly_ratio` carry partition_ts via run-level fallback
  during persistence);
  (e) MEDIUM: narrowed plumbing scope to match `skip_if_cached`
  exactly — NOT on `validate_query` or `write_audit_publish`;
  AC1 + Locked Decision 2 updated.
- v2: addressed adversarial round 1 — major design rework
  from "predict per-validator shape, all-or-nothing lookup" to
  "read all persisted rows at the partition, replay whatever's
  there." Catches `IsolationForestValidator`'s actual
  `metric_name="anomaly_ratio"` (was wrongly listed as
  `anomaly_score`), `PatternValidator`'s `.schema` sub-emission
  with no metric, error-state rows (cold_start, non-finite
  scores, CV-failed), per-dimension row enumeration, and any
  future validator emission shape — without the engine having
  to mirror validator logic.
