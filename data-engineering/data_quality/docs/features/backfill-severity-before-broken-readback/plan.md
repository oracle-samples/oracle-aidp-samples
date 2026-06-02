# Plan: backfill-severity-before-broken-readback

## Goal

Fix `qualifire/core/backfill.py:_read_severity_before` so it
actually returns the prior severity for a metric. Today it
calls `storage.read_validation_history(validation_name="")`
which never matches a real row (validation_name is always
populated), so every backfill run reports
`severity_before=None`. Knock-on effect: `_classify_diff`
over-reports `refreshed` when only severity flipped between
runs.

## Locked Decisions

1. **Resolve `metric → validation_name(s)` at the call site.**
   The scope's `validations` is in scope; build a map
   `metric_to_validation_names: dict[str, list[str]]` from
   each val_config's `expected_metrics()` × resolved base_name
   and the per-validator suffixing rule (threshold /
   historical / forecast use `f"{base}.{rule.metric}"`; SLO,
   Pattern, AnomalyDetection emit one row at the base_name
   itself, but those don't have `expected_metrics()` so they
   never reach `_read_severity_before` anyway).
2. **Aggregation policy when multiple validations share a
   metric**: max severity wins (ERROR > WARNING > PASS).
   Deterministic + matches operator intuition ("if any
   validator on this metric was alerting, treat that as the
   severity to compare against").
3. **NULL-safe fallback**: when the lookup returns no rows,
   keep returning `None` (today's behavior on the contract
   level — operators see `severity_before=None` only for
   genuinely-cold partitions, not as a bug).
4. **Test coverage** for the bug and the fix:
   - Regression test: a backfill of an unchanged value
     against an unchanged-severity history row reports
     `unchanged`, not `refreshed`.
   - Severity-flip test: a backfill of an unchanged value
     against a DIFFERENT-severity history row reports
     `refreshed`.
   - Multi-validation aggregation test: two validations on
     the same metric (one ERROR, one PASS in history) →
     `severity_before == ERROR`.

## What Changes

### `qualifire/core/backfill.py`

- New helper
  `_resolve_metric_to_validation_names(scope: ScopedDataset)
  -> dict[str, list[str]]` that walks `scope.validations` and
  for each validation with `expected_metrics()` builds the
  `(metric_name → list of full validation_names)` map.
- `_read_severity_before` signature change (codex R1 MAJOR
  finding 6 — partition-anchor):
  `_read_severity_before(*, storage, dataset_name, metric_name,
  validation_names: list[str], partition_ts: datetime) -> Severity | None`.
  Reuses the just-shipped `storage.read_validations_at_partition`
  (skip-revalidation feature) — accepts a
  `validation_name_prefix` and returns ALL active validation
  rows at that exact partition. We pass the validation's
  base_name as the prefix and filter the returned rows
  client-side by exact validation_name match (since the
  helper's prefix matches both `prefix` and `prefix.<sub>`).
  Then filter by `metric_name` and pick max severity.
- The scope-of-data fix: querying at `unit.anchor` means we
  get the severity from the SAME partition we're backfilling,
  not the most-recent-overall row. Matches the rest of
  `_process_anchor`'s anchor-scoped reads
  (`_read_original_value` already takes `anchor`).
- `_process_anchor` builds the map once at the start of the
  per-anchor loop, passes `validation_names` + `partition_ts`
  per metric.

### Empty-data rows note (codex R1 MAJOR finding 5)

`_empty_data_result` (validation/base.py:41) emits a
`ValidationResult` with `metric_name=None` when the dataset
has no data. The persisted row's `metric_name IS NULL`. Our
metric-filtered lookup correctly skips these — that's the
intended contract: "severity_before for THIS metric" means
"severity of a prior run that emitted THIS metric." A prior
empty-data run did NOT emit the metric; its severity isn't
applicable to the metric-level diff. Falling through to
`severity_before=None` for an after-empty-data backfill is
the right behavior, not a bug. Document inline so future
reviewers don't try to "fix" it.

### `tests/test_backfill.py` or `tests/test_backfill_parallelism.py`

- New `TestSeverityBeforeReadback` class with the 3 tests
  above.

## Acceptance Criteria

- AC1: `_read_severity_before` returns the actual persisted
  severity (max-aggregated when multiple) for the metric on
  the dataset. Verified by the regression tests.
- AC2: `_classify_diff` reports `unchanged` when value AND
  severity are unchanged; `refreshed` when either differs.
- AC3: Test added for the multi-validation aggregation rule
  (max severity wins).
- AC4: Existing backfill tests pass without modification.
- AC5: No external API change (`BackfillReport.severity_before`
  was already a `Severity | None` field — operators just
  start seeing real values instead of always-None).

## Risks and Mitigations

| Risk | Mitigation |
|------|------------|
| Validation-name suffix convention varies more than the planner thinks | Use the existing `_resolve_validation_base_name` helper from `qualifire/core/engine.py` — same source of truth the engine uses on the write path. |
| Multi-validation aggregation across heterogeneous types (e.g. threshold + drift on the same metric) | Plan picks max severity. Documented; test pins. |
| Sample-based validators (Pattern / Anomaly) don't have `expected_metrics()` | Already excluded from `metric_names` via `_all_metric_names`'s `hasattr` guard; `_read_severity_before` is never called for them. No special case needed. |

## Out-of-Band Reviews

- 2 adversarial plan reviews.
- 2 codex plan reviews → iterate to PASS.
- Then implement; 2 adversarial impl reviews + 2 codex impl reviews.

## Effort

Small. ~50 LOC in backfill.py + ~80 LOC of tests.

## Plan Iteration Log

- v1: initial draft.
- v2: addressed codex plan-review round 1 — MAJOR #6:
  thread `partition_ts` into `_read_severity_before` so the
  read is anchor-scoped (matches `_read_original_value`'s
  shape; reuses the just-shipped
  `read_validations_at_partition` from skip-revalidation).
  MAJOR #5: empty-data validation rows have
  `metric_name=None` and are correctly filtered out by the
  metric_name match — that's the intended contract, not a
  bug. Documented inline.
