# Plan: drift-explainer-per-slice-breakdown

## Goal

Add an opt-in per-slice breakdown to value-drift explainer
entries so operators can answer "which past partition shifted?"
during triage. Today's union-only delta hides whether a drift was
gradual (slow burn) or sudden (break), and the sampler already
retains the per-slice DataFrames — adding the breakdown is mostly
plumbing.

## Locked Decisions (planner's recommendation)

1. **Payload boundedness**: when `drift_breakdown_by_slice=True`,
   the per-slice block is limited to **top-3 features** instead of
   the standard top-5. Keeps the existing 16 KB
   `_PAYLOAD_MAX_BYTES` cap intact. Documented in the model-config
   field doc.
2. **Numeric features**: per-slice `current_vs_slice` carries
   `mean_pct`, `p99_pct`, `null_pct_abs` (no `summary` string —
   slice-level summaries inflate the payload without adding
   signal beyond the numbers).
3. **One-hot features**: per-slice `current_vs_slice` carries
   ONLY `rate_pp` (the absolute rate diff for the one-hot
   category being explained). No full top-N category mix per
   slice — that's the verbosity trap the idea.md flagged.
4. **Boolean features** (codex-likely-MEDIUM-find): per-slice
   `current_vs_slice` carries `true_rate_pp` only.
5. **Datetime features**: per-slice block is **omitted** — the
   union-level entry already shows the min/max range; per-slice
   range diffs are noisy and rarely actionable for triage.
   Document inline.
5a. **Label-encoded features** (codex R1 MAJOR): per-slice block
    is **omitted**. The label encoding maps high-cardinality
    categoricals to ordinal integers; rate-pp / mean-pct
    statistics on those integers are misleading (the integer
    values are arbitrary identifiers, not magnitudes). Operators
    needing per-slice cardinality drift consult the union-level
    `kind="label_encoded"` entry's existing `unique_count_delta`.
5b. **Unknown / fallback kinds** (codex R1 MAJOR): per-slice
    block is **omitted**. The `kind="unknown"` branch in
    `_entry_for_kind` is a defensive catch-all; per-slice would
    have no defined shape.
6. **Unmapped / failed entries**: per-slice block is **omitted**
   — these are defensive-fallback paths with no real source
   data; per-slice would be meaningless.
7. **Output shape**:
   ```python
   entry = {
     "feature": "amount", "source_column": "amount", "kind": "numeric",
     "current": {...}, "past": {...}, "delta": {...},   # union, unchanged
     "summary": "amount; p50 100 vs 70; mean +51%; p99 +67%",
     "per_slice": [
       {"label": "past_1", "current_vs_slice": {"mean_pct": 0.10}},
       {"label": "past_2", "current_vs_slice": {"mean_pct": 0.30}},
       {"label": "past_3", "current_vs_slice": {"mean_pct": 0.50}},
       {"label": "past_4", "current_vs_slice": {"mean_pct": 0.67}},
     ],
   }
   ```
8. **Top-3 attachment, top-5 union preserved** (R1 MEDIUM):
   the union-level top_features count stays at 5 regardless of
   the flag — only the first 3 carry `per_slice`. Default-False
   thus produces today's shape verbatim, and flag=True doesn't
   shrink the union. AC2 stays meaningful.
9. **Default `False`**. Operator opt-in.
10. **Notification body unchanged**. The per-slice block lands in
    `details_json` only; notifications still render the union
    summary line.
11. **Dashboard sparkline rendering**: OUT of scope. The detail
    panel (`dashboard-rich-detail-panel`) can render whatever
    shape it wants in a follow-up; this feature only persists
    the data.
12. **Truncation ordering** (R1 MEDIUM): if the per_slice block
    on an entry would push the entry past a per-entry budget
    (~3 KB defensive cap; documented), drop `per_slice` from
    that entry first and set `entry["per_slice_truncated"] =
    True`. Then run the existing `_enforce_payload_budget` over
    the entry list as today. The new per-entry truncation
    happens BEFORE the existing total-budget enforcement —
    avoids `_enforce_payload_budget` having to know about
    `per_slice` internals.

## What Changes

### Public API

- `qualifire/core/config.py:AnomalyModelConfig`: add
  `drift_breakdown_by_slice: bool = False`.
- `qualifire/core/config.py:PatternModelConfig`: same field.
- `qualifire/validation/pattern_check.py:PatternCheckValidator
  __init__`: new `drift_breakdown_by_slice: bool = False` param;
  validator stores it as `self.drift_breakdown_by_slice`.
- `qualifire/validation/isolation_forest.py
  IsolationForestValidator __init__`: same.
- `qualifire/core/engine.py:_validate`: thread the flag through
  the existing validator instantiation sites at lines 1630
  (IsolationForest) and 1651 (Pattern), reading from
  `model.drift_breakdown_by_slice`.

### Explainer changes

- `qualifire/validation/_drift_explainer.py:explain_value_drift`:
  - new param `breakdown_by_slice: bool = False`
  - when False: today's behavior, no per_slice field.
  - when True:
    - Process all `top_features` for the union shape unchanged.
      Then attach `per_slice` only to entries that satisfy ALL
      of these conditions (codex R1 MEDIUM — explicit gate):
        a. The entry is among the first 3 in the list (Locked
           Decision 8 — top-5 union count preserved).
        b. The entry's `kind` is one of `{numeric, onehot,
           boolean}` (Locked Decisions 2, 3, 4). All other
           kinds (`datetime`, `label_encoded`, `unknown`,
           and the defensive paths) are omitted.
        c. The entry is mapped (i.e., NOT produced by
           `_unmapped_entry` and NOT produced by the
           per-feature exception fallback at
           `_drift_explainer.py:91`).
    - For matched entries, walk `past_pdfs` as separate slices
      (don't concat) and compute per-slice stats per the kind:
      - `numeric`: `{"mean_pct", "p99_pct", "null_pct_abs"}`
      - `onehot`: `{"rate_pp"}`
      - `boolean`: `{"true_rate_pp"}`
    - Append `entry["per_slice"] = [
        {"label": label, "current_vs_slice": {...}}
        for label, _ in past_pdfs
      ]`
  - Per-entry truncation (Locked Decision 12): if an entry's
    serialized form (including per_slice) exceeds the per-entry
    budget `_PER_ENTRY_PER_SLICE_MAX_BYTES = 3072` (3 KB; pinned
    constant so impl-review can verify), drop `per_slice` from
    that entry and set `per_slice_truncated=True`. Run BEFORE
    the existing `_enforce_payload_budget` to keep that helper
    unchanged.
  - For `_unmapped_entry` and the per-feature exception path,
    skip per_slice entirely (Locked Decision 6).

### Validators

- `pattern_check.py` / `isolation_forest.py` at the
  `explain_value_drift(...)` call site: pass
  `breakdown_by_slice=self.drift_breakdown_by_slice`.
- No other validator changes — the flag is plumbing-only.

### Tests

- New `tests/test_validation/test_drift_explainer_per_slice.py`:
  - T1: flag=False → today's behavior; no `per_slice` key on
    any entry.
  - T2: flag=True with 4 past slices → top-3 entries each carry
    `per_slice` with 4 entries (one per past slice in input
    order). Entries 4–5 do NOT carry `per_slice` (top-5 union
    preserved per Locked Decision 8).
  - T3: flag=True, numeric feature → per_slice entries have
    `current_vs_slice = {mean_pct, p99_pct, null_pct_abs}`.
  - T4: flag=True, one-hot feature → per_slice entries have
    `current_vs_slice = {rate_pp}` only.
  - T5: flag=True, boolean feature → per_slice entries have
    `current_vs_slice = {true_rate_pp}` only.
  - T6: flag=True, datetime feature → entry has NO `per_slice`
    key (Locked Decision 5).
  - T7: flag=True, top-N limit — when top_features has 5
    entries, the union still has 5 entries; only the first 3
    carry `per_slice`.
  - T8: per-entry truncation — synthetic case where one entry's
    serialized size exceeds `_PER_ENTRY_PER_SLICE_MAX_BYTES`;
    that entry has no `per_slice` key but has
    `per_slice_truncated=True`. Sibling entries unaffected.
  - T9: unmapped / failed entry → no `per_slice` key (Locked
    Decision 6).
- `tests/test_validation/test_pattern_check.py` regression:
  flag=False default doesn't change today's output (existing
  AC#6-style snapshot test should already cover this; add an
  explicit assertion that `per_slice` isn't present).

### Documentation

- `docs/features/feature-value-drift-explainer/shipped.md` —
  cross-link the follow-up.
- `docs/CHANGELOG.md` — Enhancement entry under Unreleased.

## Acceptance Criteria

- AC1: `drift_breakdown_by_slice: bool = False` field on
  AnomalyModelConfig and PatternModelConfig.
- AC2: Default-False produces today's `value_drift_explainer`
  shape verbatim (no `per_slice` key).
- AC3: Default-True produces `per_slice: [{"label": ...,
  "current_vs_slice": {...}}, ...]` in input order on the top-3
  features only.
- AC4: Per-slice `current_vs_slice` shape per kind:
  - `numeric` → `{mean_pct, p99_pct, null_pct_abs}`
  - `onehot` → `{rate_pp}`
  - `boolean` → `{true_rate_pp}`
  - `datetime` → entry has no `per_slice` key (omitted)
  - `label_encoded` → entry has no `per_slice` key (omitted)
  - `unknown` → entry has no `per_slice` key (omitted)
  - unmapped / failed → entry has no `per_slice` key (omitted)
- AC5: Per-entry overflow against
  `_PER_ENTRY_PER_SLICE_MAX_BYTES` drops `per_slice` from the
  offending entry and sets `per_slice_truncated=True`; the
  union fields stay intact and sibling entries unaffected.
- AC6: T1-T9 pass for both pattern_check and isolation_forest
  (the flag is identical-shape on both validators; the explainer
  is shared).
- AC7: Notification rendering is unchanged (no per_slice in
  message bodies).

## Risks and Mitigations

| Risk | Mitigation |
|------|------------|
| Per-slice block exceeds 16 KB cap on long history | Locked Decision 1 (top-3 features) + AC5 (per-entry truncation) |
| Categorical full top-N per slice is verbose | Locked Decision 3 (rate_pp only) |
| Dashboard panel doesn't render the new field yet | Out of scope. Detail panel is permissive — extra keys pass through unchanged. |
| Plan-time top-3 limit hides interesting low-importance shifts | Acceptable — the union-level entry still names the feature; per-slice trend is a triage aid, not the primary signal. |

## Out-of-Band Reviews

- 2 adversarial plan reviews.
- 2 codex plan reviews → iterate to PASS.
- Then implement; 2 adversarial impl reviews + 2 codex impl reviews.

## Effort

Small. ~150 LOC explainer additions + ~50 LOC config plumbing +
~120 LOC tests.

## Plan Iteration Log

- v1: initial draft.
- v4: addressed codex plan-review round 1 — added explicit
  per-slice OMIT for `label_encoded` and `unknown` kinds
  (codex MAJOR; missing kinds in `EncodedFeatureSpec`); pinned
  the per_slice gate explicitly (a/b/c conditions on first-3
  + supported-kind + mapped); expanded AC4 to enumerate all
  6 kinds + 1 fallback path. Codex round 2 PASS.
- v3: addressed adversarial round 2 — pinned
  `_PER_ENTRY_PER_SLICE_MAX_BYTES = 3072` constant; expanded
  test list (T1-T9) to cover boolean, datetime omit, unmapped
  omit, and per-entry truncation; updated AC4 and AC6 to match.
- v2: addressed adversarial round 1 — added per_slice shapes
  for `boolean` (true_rate_pp) and explicit OMIT for `datetime`
  (R1 MEDIUM); per_slice attaches only to first 3 entries while
  union preserves top-5 count (R1 MEDIUM); pinned per-entry
  truncation ordering (run BEFORE `_enforce_payload_budget`,
  keeps that helper unchanged); skip per_slice for unmapped /
  failed entries (R1 LOW).
