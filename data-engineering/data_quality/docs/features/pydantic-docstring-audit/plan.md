# Plan: pydantic-docstring-audit

## Goal

Audit + tighten class-level docstrings on the 10 highest-priority
Pydantic config classes in `qualifire/core/config.py` so they
read as the operator-facing authoritative answer to "what is this
config and what does it do?" The audited docstrings will be the
primary source for the deferred `configuration-reference-audit`
backlog (the configuration reference can quote them rather than
re-explain).

## Locked Decisions

1. **Scope: 10 classes, not all 36.** Cover the 9 most-used
   surfaces from `configuration-reference-audit`'s scope plus
   `QualifireConfig` (top-level container).
   - `QualifireConfig`
   - `DatasetConfig`
   - `ThresholdValidationConfig`
   - `HistoricalValidationConfig` (drift)
   - `ForecastValidationConfig`
   - `PatternValidationConfig`
   - `AnomalyDetectionValidationConfig`
   - `SLOValidationConfig`
   - `AggregationCollectionConfig`
   - `SampleCollectionConfig`

   **Already-covered, no work needed** (codex R1 — both
   already carry substantive class docstrings tuned for
   operators):
   - `WAPConfig` (line 970 area; embedded on `DatasetConfig.wap`)
   - `JDBCConfig` (line 167 area; embedded on `QualifireConfig.jdbc`)

   The remaining 24 classes get docstring updates as part of
   future iterations (Profiling/Metrics/CustomQuery/Recency
   collectors are scoped to the
   `configuration-reference-collectors-extension` backlog;
   model / threshold / rule sub-classes are tightly coupled to
   their parent validation configs and don't need standalone
   class docstrings to support the configuration reference).

2. **Per-class shape**: each docstring has 3 elements:
   - One-paragraph "what this is" intro (operator's mental
     model — what does this config do, when do they reach for
     it, what's the smallest legal config?).
   - Field-level inline notes (existing inline comments stay;
     audit pass tightens any that read as engineering-review
     noise rather than operator-facing).
   - One-line cross-link to the most-relevant docs reference
     (e.g. `See docs/CHANGELOG.md` for default-flips, or
     `See docs/notifications.md` for routing).

3. **Don't touch field-level types or defaults.** This is
   prose work only. Any "this default looks wrong" judgment
   call surfaces as an idea / issue, not silently changed.

4. **Verify field accuracy** at audit time by spot-grepping
   each class's fields against where they're consumed (so a
   docstring claim about a field's behavior matches the actual
   code).

## What Changes

### `qualifire/core/config.py` — 10 docstring updates

Each class gets a `"""..."""` immediately after the `class X(BaseModel):`
line. **Existing substantive docstrings are merged + preserved**
(codex R1 finding — `AggregationCollectionConfig` and
`SampleCollectionConfig` already carry useful class
docstrings; the audit pass adds the operator-mental-model
intro WITHOUT overwriting the existing factual content). For
classes with no existing docstring (e.g.
`ThresholdValidationConfig`), add a fresh one matching the
3-element template below.

The pattern (using `ThresholdValidationConfig` as the template):

```python
class ThresholdValidationConfig(BaseModel):
    """A threshold validation: assert that aggregated metric
    values stay within configured bounds.

    Use this when the question is "is `row_count >= 100`?" or
    "is `avg(amount) between 50 and 200`?". Threshold rules are
    static — the bound is the same on every run; for a
    moving / partition-anchored bound see `HistoricalValidationConfig`
    (drift) or `ForecastValidationConfig` (trend).

    The smallest legal config:

    ```yaml
    type: threshold
    collection:
      type: aggregation
      expressions: {row_count: 'COUNT(*)'}
    rules:
      - metric: row_count
        thresholds:
          error: {min: 1}
    ```

    See `docs/configuration.md` (once the
    `configuration-reference-audit` backlog item ships) for the
    full field reference + worked examples.
    """
    type: Literal["threshold"] = "threshold"
    ...
```

### Tests

No new tests required — docstrings are not behaviorally tested.
But the `docs-lint-ci` smoke test (just shipped) will catch any
bad cross-link added in the docstrings to non-existent docs.

## Acceptance Criteria

- AC1: All 10 classes named in Locked Decision 1 have a
  class-level docstring with the 3 elements (intro paragraph,
  field-level notes preserved or tightened, cross-link to a
  real doc).
- AC2: No production-code behavior change — only prose
  changes inside `qualifire/core/config.py` docstrings (and
  inline comments where existing ones are tightened).
- AC3: Full test suite still passes (no regression — but
  docstrings are not tested directly).
- AC4: A spot-grep verification: for each class, the docstring
  doesn't make a claim contradicted by the actual code (e.g.
  "default is X" matches the field's actual default).

## Risks and Mitigations

| Risk | Mitigation |
|------|------------|
| Docstring drift from code over time | The configuration reference (deferred to backlog) will quote these docstrings — operators landing on the wrong claim file an issue. Future field changes should update both code + docstring in the same PR. |
| Cross-links to `docs/configuration.md` (currently 665-line partial) imply a level of completeness the file doesn't yet have | Phrase cross-links as "see `docs/configuration.md` (once `configuration-reference-audit` ships)" — honest about state. |
| Mismatch between docstring claims and field behavior | AC4 + spot-grep at audit time. |

## Out-of-Band Reviews

- 1 codex plan review.
- Then implement; 1 codex impl review (focus: factual
  accuracy of the new docstrings, not prose-style nits).

## Effort

Small. ~10 LOC of Python per class × 10 classes = ~100-150 LOC of
docstring text + minor inline tightening.

## Plan Iteration Log

- v1: initial draft.
- v2: addressed codex plan-review round 1 — Locked Decision 1
  enumerates `WAPConfig` + `JDBCConfig` as already-covered
  (substantive docstrings exist; no work needed). The
  "replace any existing class docstring" wording softened to
  "merge + preserve existing factual content" so audited
  classes that already carry useful docstrings (Aggregation,
  Sample) don't lose information.
