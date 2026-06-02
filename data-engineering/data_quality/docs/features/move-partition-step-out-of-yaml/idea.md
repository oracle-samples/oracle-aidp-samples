---
id: move-partition-step-out-of-yaml
name: Move partition_step out of YAML — backfill()-only kwarg
type: Tech Debt
priority: P2
effort: Small
impact: Low
created: 2026-05-11
---

# Move `partition_step` out of YAML — backfill()-only kwarg

## Problem Statement

`partition_step` lives on **both** `QualifireConfig` (run-level)
and `DatasetConfig` (dataset-level), plus it's accepted as a
kwarg on `qf.backfill(...)` and the `qualifire backfill` CLI.
But by the codebase's own admission it is **only load-bearing
for backfill**:

- `docs/configuration.md:49` — *"Used by **`qualifire backfill`**
  range expansion (`--partition START..END`); history-backed
  validators read their cadence from their OWN rule
  (`compare.step` / `model.step` / `history.step`), not from
  this field."*
- `qualifire/core/config.py:1528` (invariant docstring) —
  *"`partition_step` is only load-bearing for backfill /
  metric-by-metric deactivation paths."*
- `qualifire/api.py:1067` — `backfill()` already accepts
  `partition_step=` as a kwarg with documented precedence
  over the YAML value.

The result: `partition_step` is a YAML field that does nothing
unless the user later runs `qf.backfill(...)`, and even then
the `backfill()` kwarg can override it. For the normal
`qf.run()` / `qf.validate()` / `qf.run_config()` paths it is
dead config — present, validated by Pydantic, threaded through
`effective_partition_step`, but never consumed.

## Why It Matters

- **Cognitive friction.** New users reading the YAML schema
  encounter `partition_step` and reasonably assume it affects
  every run. The documented "only for backfill" caveat is
  buried in `configuration.md:49`.
- **Invariant complexity.** The cross-field validator
  `_validate_partition_step_invariant` at
  `core/config.py:1523` carries two negative rules
  (`partition_step` without `partition_ts`, run-level
  `partition_step` with no dataset using `partition_ts`) that
  only matter to backfill — but they fire at every
  `load_config()`, including for users who will never call
  `backfill()`.
- **Asymmetric API.** Other backfill-only knobs (e.g.,
  `max_partitions`, the `--partition START..END` range) are
  `backfill()` kwargs, not YAML fields. `partition_step` is
  the odd one out.

## Who Benefits

- **New users** reading `docs/configuration.md`: one fewer
  field on the dataset-level / run-level schema; one fewer
  cross-field invariant to keep in mind.
- **CLI users**: `qualifire backfill --partition-step P1D
  --partition 2026-01-01..2026-01-31` reads as a single
  command-line contract instead of "set this in YAML, then
  invoke the CLI."
- **Maintainers**: fewer schema fields to keep documented and
  invariant-tested across the codebase.

## Scope

### A — Remove `partition_step` from both config types

- `QualifireConfig.partition_step` (run-level): delete the
  field + its `@field_validator`.
- `DatasetConfig.partition_step` (dataset-level): delete the
  field + its `@field_validator`.
- Delete `effective_partition_step(dataset, run_config)` helper
  and `_validate_partition_step_invariant` cross-field check.

### B — Make `partition_step` a required-when-applicable
`backfill()` kwarg

- `qf.backfill(...)` already accepts `partition_step=` —
  promote it to the canonical source of truth.
- Decide the missing-value contract: today the YAML provides
  a fallback; without YAML the kwarg must be supplied when
  range expansion (`--partition START..END`) is in use. Single-
  partition `--partition T` backfill does not need a step.
- `qualifire backfill` CLI: keep `--partition-step` flag (or
  add it if missing).

### C — Update docs

- `docs/configuration.md` — delete the `partition_step` table
  rows and the invariant explanation.
- `docs/backfill_and_soft_delete.md` — promote `partition_step`
  to a required-when-range-backfilling kwarg.
- `docs/architecture.md` — if the backfill ASCII diagram
  references the field, update.
- `docs/CHANGELOG.md` — Breaking change entry under Tech Debt.

### D — Migrate tests

- `tests/test_config_partition_step.py` — survey: which tests
  validate the load-time invariant (delete) vs. test the
  effective-resolution helper (delete) vs. test
  `backfill(partition_step=...)` pass-through (keep)? Likely
  most of this file is deleted.
- `tests/test_backfill*.py` — convert any test that today
  reads `partition_step` from YAML to passing it as a
  `backfill()` kwarg.

## Non-goals

- **No change** to `partition_ts` (the partition anchor) — it
  remains a YAML field for the normal validation path.
  History-backed validators read it on every `qf.run()` to
  compute `anchor − k·step`.
- **No change** to per-rule `step` fields
  (`compare.step` / `model.step` / `history.step`) — those are
  per-validator cadences and live on the rule, not the dataset
  or run config.
- **No backwards-compat alias.** Qualifire is pre-release; we
  break the YAML schema cleanly.

## Affected Areas

- `qualifire/core/config.py` (delete fields, validators, helper)
- `qualifire/core/engine.py` (remove `partition_step` plumbing)
- `qualifire/core/backfill.py` (treat the kwarg as canonical;
  no YAML fallback)
- `qualifire/api.py` (`backfill()` signature stays the same;
  `validate()` signature loses `partition_step=`)
- `qualifire/cli.py` (CLI accepts `--partition-step`)
- `tests/test_config_partition_step.py` (probably mostly deleted)
- `tests/test_backfill*.py` (rewire YAML→kwarg)
- `docs/configuration.md`, `docs/backfill_and_soft_delete.md`,
  `docs/architecture.md`, `docs/CHANGELOG.md`

## Open Questions for Planning

1. **`validate(partition_step=...)` kwarg** — `qualifire/api.py:956`
   currently shows `validate()` taking `partition_step=`. Is
   this for backfill-during-validate? Plan phase confirms
   whether to drop it from `validate()` entirely.
2. **Engine-level `engine.py:2575` plumbing** — what consumes
   `ds_config.partition_step` inside the engine path? Plan
   phase reads this site to confirm it is backfill-only and
   can be removed.
3. **Per-rule `step` fields** — should this feature also audit
   whether `compare.step` / `model.step` / `history.step` could
   default from a single field? Probably not — they are
   per-rule by design. Plan phase confirms.
4. **CLI surface** — does `--partition-step` already exist on
   `qualifire backfill` or does this feature add it? Plan
   phase checks.
5. **Breaking-change risk on AIDP** — are there in-house
   qualifire YAMLs already shipping with `partition_step`?
   Plan phase confirms with the operator before merging.
