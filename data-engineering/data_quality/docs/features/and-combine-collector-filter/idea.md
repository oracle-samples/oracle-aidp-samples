---
id: and-combine-collector-filter
name: AND-combine collector filter with dataset filter
type: Feature
priority: P1
effort: Small
impact: High
created: 2026-05-08
breaking: true
---

# AND-combine collector filter with dataset filter

## Problem Statement

Three collectors (`aggregation`, `profiling`, `metrics`) accept a
`filter` field on the collector config in addition to the
`DatasetConfig.filter` that already scopes the dataset. Today the
engine composes these two filters with an **override**:

```python
# qualifire/core/engine.py:1231, 1247, 1255 (current main as of 2026-05-09)
filter_expr=collection.filter or filter_expr
```

If both are set, the dataset filter is silently dropped. The
collector filter becomes the only scope.

This is wrong on two fronts:

1. **It silently drops safety guards.** Operators routinely put
   defensive predicates on `DatasetConfig.filter` — soft-delete
   exclusions, PII redaction (`is_test = false`), tenant
   isolation. The moment a single validator on that dataset
   introduces its own collector-level filter (to look at a
   subset), all those guards quietly disappear from that
   validator's row scope. Nothing in the run output flags this;
   the operator sees a number and trusts it.

2. **It contradicts the `sample` collector.** The same operator,
   on the same dataset, gets AND-combination for `shape` /
   `pattern` validators (sampler explicitly preserves dataset
   filter alongside the per-slice predicate via
   `qualifire/collection/sampler.py:_and_combine` at line ~278;
   `SampleCollectionConfig` docstring at
   `qualifire/core/config.py:267-269` documents the contract) and
   override behaviour for `threshold` / `drift` / `forecast`
   validators backed by `aggregation` / `profiling` / `metrics`.
   The asymmetry isn't documented anywhere; nothing surfaces it.
   Operators discover it the hard way — usually when a validator
   that "should" be filtered isn't.

## Why It Matters

- **Correctness.** The current behaviour silently produces the
  wrong row population when both layers are set. The validator
  result is computed against a different scope than the operator
  asked for.
- **Auditability.** Compliance reviewers reading the YAML cannot
  tell from the file alone whether `DatasetConfig.filter` is
  actually applied to a given check. The answer depends on the
  collector below it.
- **Convergence with `sample`.** The codebase already has the
  AND-combine semantic in one place. Spreading it to the rest
  removes a footgun and makes one rule operators can carry across
  every validator type.

## Who Benefits

- **Operators authoring multi-validator configs** — the common
  case where one dataset block carries 3–5 different validators,
  each with its own narrow scope on top of a shared dataset
  guard.
- **Compliance / audit reviewers** — predicate composition becomes
  predictable from the YAML alone, no engine-side asymmetry.
- **New users** — the AND-combine model matches naive expectation
  ("all the filters apply"). The override model surprises
  everyone.

## Affected Areas

- `qualifire/collection/sampler.py` — `_and_combine` already
  implements the desired AND-combine semantic (parenthesised, None-
  tolerant). Promote to a shared module so engine call sites and
  sampler call sites use one implementation.
- `qualifire/core/engine.py` — three call sites at lines
  1231 (`AggregationCollector`), 1247 (`ProfilingCollector`),
  1255 (`MetricsCollector`). Replace `collection.filter or
  filter_expr` with the shared helper.
- `qualifire/core/config.py` — Pydantic field validators on
  `AggregationCollectionConfig.filter`,
  `ProfilingCollectionConfig.filter`,
  `MetricsCollectionConfig.filter` to coerce empty / whitespace-
  only strings to `None` (mirrors the empty-tolerant strip in
  `CustomQueryCollectionConfig._reject_nonempty_filter`); plus
  docstring updates to spell out the composition rule.
- `docs/validators/validator_collector_matrix.md` — new "Filter
  precedence" section, documented examples of dataset+collector
  combos.
- `docs/validators/README.md` — one-paragraph cross-reference.
- `tests/test_filter_combination.py` *(new)* — unit tests for the
  shared helper plus end-to-end pandas integration tests covering
  all three collector paths.

## Backwards-compatibility risk

This is a **behaviour change**. Any operator config that today
sets both `DatasetConfig.filter` and a collector-level `filter`
will see a narrower row scope post-change.

Concrete shape of the migration:

```yaml
# Pre-change (override): scoped to "all returned orders globally"
filter: "region = 'US'"
validations:
  - type: threshold
    collection:
      type: aggregation
      filter: "status = 'returned'"

# Post-change (AND): scoped to "US returned orders only"
# Same YAML, narrower scope. Counts, sums, ratios may all drop.
```

Operators relying on the override behaviour have one of two
migration paths:

1. **Embrace the AND-combine** — usually what they actually
   wanted; thresholds may need re-tuning against the narrower
   scope.
2. **Move the broader filter into the collector** — explicit:
   `filter: "status = 'returned'"` (drop dataset-level), or
   `filter: "status = 'returned' AND region IN ('US','EU','APAC')"`
   if they really want a wider scope than the dataset's.

Either path is a one-line YAML edit. The risk surface is high
priority alerts that flap on first run because the row count is
genuinely smaller.

## Open Questions for Planning

(For `plan.md` to resolve.)

- **Migration messaging.** Should the engine emit a one-time
  WARNING log on first run when both filters are present, naming
  the dataset and the resulting combined filter? Helps operators
  notice the change without forcing a config edit. Or keep it
  silent and rely on release notes alone?
- **Empty-string handling.** Today `collection.filter or
  filter_expr` treats `""` and `None` identically (both falsy).
  AND-combine via `f"({a}) AND ({b})"` would emit `"() AND
  (...)"` for empty strings — invalid SQL. Should we coerce empty
  to None at config-load (Pydantic validator) or in the helper?
- **Logging the combined filter.** Helpful for debugging or noisy
  for big configs? If logging, what level?
- **Should we deprecate, then change, vs. change directly?**
  Deprecation cycle: emit WARNING for one minor version when both
  are set, change behaviour the next minor. Or change
  immediately and rely on the release-note + AND-combine being
  what most operators expected anyway. The case for direct
  change: today's behaviour is silently wrong; a deprecation
  cycle prolongs the wrongness.
