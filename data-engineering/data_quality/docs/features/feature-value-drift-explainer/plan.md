---
id: feature-value-drift-explainer
name: Per-Feature Value Drift Explainer
status: planned
created: 2026-05-09
---

# Per-Feature Value Drift Explainer — Plan

## Goal & Hard Scope

Augment the `shape` and `pattern` validators so each entry in
`details.top_contributing_features` carries a **value-shift summary** —
how the top SHAP features actually changed between the current sample
and the historical sample.

In scope:

- New helper module `qualifire/validation/_drift_explainer.py`.
- Extend `qualifire/validation/_encoding.py` to expose an
  encoder-name → (original column, encoding kind, category) mapping
  alongside the existing `(X, feature_names)` return.
- Wire the explainer into `pattern_check.py` and `isolation_forest.py`
  after SHAP, behind an opt-out `explain_value_drift` constructor flag
  (default `True`).
- Render the new block in `notification/base.py:format_validation_details`
  for `shape` and `pattern` types, capped at the top-3 SHAP features
  for body length.
- Extend `qualifire/reporting/plots.py` with a `plot_value_drift`
  matplotlib helper for notebook consumption (HTML report wiring
  deferred — see "HTML report integration — deferred").
- Tests: per-kind regression for the explainer (numeric, boolean,
  datetime, one-hot bin, label-encoded), encoder mapping round-trip,
  notifier rendering, validator integration, system-table round-trip.
- Docs: `docs/validators/shape.md`, `docs/validators/pattern.md`, CHANGELOG.

Out of scope (hard-stop — defer to a follow-up):

- Any change to the SHAP attribution pipeline itself.
- Any change to the `top_contributing_features` schema beyond
  additive nesting.
- Any change to the sampler / persistence / engine cache paths.
- Plotly / interactive dashboard rendering of the explainer (the
  static `plot_value_drift` is enough; dashboard integration is its
  own iteration once the schema settles).
- A separate runtime flag in `Qualifire.run_config(...)`. The
  per-validator `explain_value_drift=True` constructor parameter is
  the only knob; no engine-level override.

## Design

### Output schema (additive on `details`)

```jsonc
{
  "top_contributing_features": [               // unchanged today
    {"feature": "amount", "importance": 0.42},
    {"feature": "channel_mobile", "importance": 0.18},
    ...
  ],
  "value_drift_explainer": [                   // NEW, list parallel to top_contributing_features
    {
      "feature": "amount",                     // verbatim from feature_names
      "source_column": "amount",               // pre-encoding column
      "kind": "numeric",                       // one of: numeric|boolean|datetime|onehot|label_encoded|unknown
      "current": {"count": 1000, "null_pct": 0.02, "mean": 120.5, "std": 45.3,
                  "p25": 80.0, "p50": 100.0, "p75": 150.0, "p95": 280.0, "p99": 500.0,
                  "min": 0.0, "max": 999.0},
      "past":    {"count": 3000, "null_pct": 0.01, "mean": 80.0,  "std": 22.1,
                  "p25": 60.0, "p50": 70.0, "p75": 95.0, "p95": 180.0, "p99": 300.0,
                  "min": 0.0, "max": 700.0},
      "delta":   {"mean_pct": 0.506, "p99_pct": 0.667, "null_pct_abs": 0.01},
      "summary": "current p50=100.0 vs past p50=70.0 (+43%); mean +51%; p99 +67%"
    },
    {
      "feature": "channel_mobile",
      "source_column": "channel",
      "kind": "onehot",
      "category": "mobile",
      "current": {"count": 1000, "rate": 0.60},
      "past":    {"count": 3000, "rate": 0.30},
      "delta":   {"rate_pp": 0.30},
      "summary": "channel='mobile' rate 60% vs 30% (+30pp)"
    },
    ...
  ],
  "value_drift_explainer_error": "ImportError: ..."  // present only on failure (additive, optional)
}
```

Schema invariants (frozen):

- `value_drift_explainer` is the same length and order as
  `top_contributing_features`. A reader can zip the two.
- Every entry has `feature`, `source_column`, `kind`, `current`,
  `past`, `summary`. `delta` and `category` are kind-dependent.
- `current.count` and `past.count` are always present so a reader
  can detect pathological zero-row slices.
- Numeric `delta_pct` keys are `None` when the past denominator is
  zero (avoid `inf` / `NaN` polluting the JSON round-trip).
- All numeric values are plain floats / ints. No numpy scalars,
  no pandas extension types — they don't survive `details_json`
  round-trip across all four storage backends.

### Per-kind stats

| kind | current/past stats |
|------|---------------------|
| numeric | count, null_pct, mean, std, p25, p50, p75, p95, p99, min, max |
| boolean | count, null_pct, true_rate |
| datetime | count, null_pct, min (ISO), max (ISO) |
| onehot | count, rate |
| label_encoded | count, null_pct, top-5 category mix `[{value, rate}]` |
| unknown | count, null_pct (only) |

### Encoder mapping

`encode_columns` today returns `(X, feature_names)`. Extend to return
`(X, feature_names, encoding_map)` where `encoding_map` is
`dict[str, EncodedFeatureSpec]`:

```python
@dataclass(frozen=True)
class EncodedFeatureSpec:
    source_column: str          # original df column
    kind: str                   # numeric|boolean|datetime|onehot|label_encoded|unknown
    category: str | None = None # only for kind="onehot"; raw category value
    is_null_bin: bool = False   # only true for the get_dummies(dummy_na=True)
                                # bin; disambiguates from a real category whose
                                # value happens to be the string "__NULL__"
```

Migration: clean break (all call sites updated in this PR — there are
exactly two: `pattern_check.py:233`, `isolation_forest.py:233`). The
encoder is internal (`qualifire/validation/_encoding.py`), no
external callers.

### Past comparison

Compare current vs **the union of all past slices**. SHAP itself is
fitted on the union (label-0 = all past, label-1 = current); the
explainer mirrors that contract so the per-feature delta and the
SHAP attribution describe the same comparison. Most-recent-only would
disagree with what SHAP saw and confuse operators.

Plan-pinned alternative considered and rejected: per-slice
breakdown (`past_1`, `past_2`, …) — interesting but blows up the
notification body and the `details_json` payload. Defer to a
follow-up if operators ask for it.

### Validator wiring

Both validators have inline `validate()` methods (no `_explain` helper
exists today). The SHAP block lives at `pattern_check.py:366–417`
inside `validate()` (the `if self.explain and severity != Severity.PASS:`
branch) and at `isolation_forest.py:275–288` inside `validate()`
(the `if self.explain and anomaly_count > 0:` branch). The wiring
follows the same pattern in both:

1. After computing `top_features` (the existing list of dicts),
   call `explain_value_drift(top_features, current_pdf, past_pdfs,
   encoding_map)` from `_drift_explainer.py`.
2. The helper returns the parallel `value_drift_explainer` list.
3. Stash on `details["value_drift_explainer"]`.
4. Wrap the call in a single `try/except Exception`. On failure: log
   `logger.warning("Value drift explainer failed: %s", exc)` and set
   `details["value_drift_explainer_error"] = f"{type(exc).__name__}: {exc}"`.
   Never propagate — explainer failure must not turn a successful
   validator run into ERROR.
5. New constructor flag `explain_value_drift: bool = True`. Skipped
   when `False` OR when `explain=False` (no SHAP → no top features → no
   explainer input).

### Notifier rendering

`format_validation_details` (notification/base.py:251+) already has a
`shape | pattern` branch rendering `top contributing features:`. Extend
that branch:

- For each of the **first 3** top features, append the explainer's
  `summary` line below the existing `• {feature} ({importance})`
  bullet, indented further: `      → {summary}` (U+2192, matching
  the existing U+2022 bullet glyph used in the same renderer).
- If `value_drift_explainer_error` is set, append a single
  `- value drift unavailable: {error}` line under the feature list.

The 3-line cap keeps the Slack body readable. `details_json` keeps
the full top-5 entries (subject to the 16 KB payload cap below); a
future HTML/dashboard iteration may consume the same persisted shape.

### Size budgets (storage and notification)

Hard caps applied at the explainer:

- Per-feature `summary` string: max 200 chars. Overflow truncated
  with the literal suffix `... [truncated]`.
- Per-category value (label_encoded top-N): max 64 chars per
  string; overflow truncated as above.
- Top-N count for label_encoded mix: 5 (was 5; pinned).
- Total `value_drift_explainer` payload: max 16 KB serialized JSON.
  When the payload exceeds that (rare; only on label_encoded with
  long category values), entries past the budget are **replaced**
  with a placeholder entry of shape:
  `{"feature": <name>, "source_column": <name-or-mapped>, "kind": "truncated", "summary": "payload truncated"}`.
  This preserves the parallel-length / parallel-order invariant
  with `top_contributing_features` (Schema invariant in this same
  document is load-bearing — readers can `zip()` the two lists).
  The validator sets `details["value_drift_explainer_truncated"] = true`
  when ANY entry has `kind == "truncated"`. The original
  `top_contributing_features` list is unaffected. Entries are
  truncated in reverse-importance order (the least-important
  entries are placeholdered first, so the operator-actionable head
  of the list is preserved).

Notification body caps (in `format_validation_details`):

- Body shows top-3 explainer summaries, each on one line.
- Per-line cap at render time: 240 chars. Notifier-side truncation
  applies on top of the 200-char per-summary cap so future schema
  changes that loosen the explainer cap don't blow the body.

### Observability

When an entry's feature_name is missing from `encoding_map` (defensive
path that should never fire given R1.1's uniqueness assertion), the
explainer:

- emits a `kind="unknown"` entry with summary
  `"unmapped feature: {feature_name}"`,
- counts the occurrence in
  `details["value_drift_explainer_mapping_errors"]` (an int, present
  only when > 0),
- logs `logger.warning("value drift explainer: unmapped feature %r", ...)`.

Operators reading `details_json` see the count without having to
diff the two parallel lists.

### HTML report integration — deferred

The static HTML report (`qualifire/reporting/html_report.py`) does not
today render validator details — it shows summary table rows only.
Adding `plot_value_drift` integration into the HTML report is OUT
OF SCOPE for this PR; it is a separate iteration once the schema has
landed and the demo notebooks consume the new data shape. The
`plot_value_drift` helper is for notebook callers (and any future
HTML wiring) only — the plan no longer claims HTML-report consumption.

### Static plot helper

`plot_value_drift(explainer: list[dict], title: str = ...)` —
horizontal grouped bar chart, one row per feature, current vs past
on a single scalar per kind:

- numeric → `mean`
- boolean → `true_rate`
- onehot → `rate`
- label_encoded → top-1 category's `rate`
- datetime → omitted (a bar chart over min/max isoformat strings is
  not meaningful; rendered as a text annotation row at the end of
  the figure with the per-side ISO range)
- unknown → omitted

Used in notebooks. NOT wired into `qualifire/reporting/html_report.py`
in this PR (see "HTML report integration — deferred").

## Files Touched

- `qualifire/validation/_encoding.py` — extend return tuple, add
  `EncodedFeatureSpec`, populate per branch.
- `qualifire/core/config.py` — declare
  `AnomalyModelConfig.explain_value_drift` and
  `PatternModelConfig.explain_value_drift` (both `bool = True`).
- `qualifire/core/engine.py` — pass `explain_value_drift=model.explain_value_drift`
  through both `IsolationForestValidator(...)` (lines ~1630) and
  `PatternCheckValidator(...)` (lines ~1650) construction sites.
- `qualifire/validation/_drift_explainer.py` (NEW) — `explain_value_drift`
  + per-kind stat helpers.
- `qualifire/validation/pattern_check.py` — wire explainer into the
  `if self.explain and severity != Severity.PASS:` block, add
  `explain_value_drift` constructor param.
- `qualifire/validation/isolation_forest.py` — same wiring.
- `qualifire/notification/base.py` — extend `format_validation_details`
  shape/pattern branch.
- `qualifire/reporting/plots.py` — add `plot_value_drift`.
- `tests/test_validation/test_drift_explainer.py` (NEW) — per-kind
  regression suite + missing-values edge cases.
- `tests/test_validation/test_encoding.py` — extend for the new
  return shape and `EncodedFeatureSpec` invariants.
- `tests/test_validation/test_pattern_check.py` — assert
  `value_drift_explainer` shape on a happy-path run.
- `tests/test_validation/test_isolation_forest.py` — same.
- `tests/test_notification/` (find or add file) — assert the
  rendered Slack/email body carries the `→ {summary}` lines (U+2192).
- `tests/test_reporting_charts.py` — extend with `plot_value_drift`
  smoke.
- `tests/test_e2e.py` — extend an existing pattern/shape e2e to
  assert the explainer round-trips through the system-table
  `details_json`.
- `docs/validators/shape.md`, `docs/validators/pattern.md` — new "Value Drift
  Explainer" subsection.
- `docs/CHANGELOG.md` — Enhancement entry.

## Acceptance Criteria (verifiable from a diff)

1. `encode_columns` return is `(X, feature_names, encoding_map)`;
   both existing callers updated; one new test asserts every entry
   in `feature_names` has a matching `encoding_map[name]` with a
   non-None `source_column` and a `kind` from the enumerated set.
2. `explain_value_drift([…], current_pdf, past_pdfs, encoding_map)`
   returns a list of the same length and order as the input top
   features. Each dict has the keys from the schema above (per kind)
   and `summary` is a non-empty string.
3. Pattern + shape integration tests assert
   `details["value_drift_explainer"]` is present and parallel to
   `top_contributing_features` on a non-PASS verdict.
4. Notifier rendering test asserts the body carries
   `→ <summary>` for the first 3 top features and never for the 4th
   or 5th. The same test asserts the line is preceded by the existing
   `• {feature}` bullet so the indent contract holds.
5. Failure injection test (mock `explain_value_drift` to raise)
   asserts the validator still returns the original verdict and
   `details["value_drift_explainer_error"]` is populated.
6. E2E test asserts a non-PASS pattern/shape result round-trips
   through SQLite (`SQLiteStorage`) and the explainer block survives
   `details_json` decode. Round-trip on Delta / JDBC / external
   catalog is **not** asserted by this PR — those backends use the
   same `json.dumps(details)` serializer surface, and existing
   `tests/test_storage/` tests already cover the JSON-string
   serialization invariant. Plan-pinned: a follow-up adds
   per-backend round-trip if any operator deployment shows
   detail-truncation under the production size limits.
7. `plot_value_drift` returns a matplotlib `Figure` for a payload
   covering all five non-unknown kinds (smoke).
8. `pattern_check.py` and `isolation_forest.py` accept
   `explain_value_drift=False` and skip the explainer cleanly
   (top features still present, explainer key absent).
9. `docs/validators/shape.md` and `docs/validators/pattern.md` each contain a
   "Value Drift Explainer" heading with the schema example.
10. `docs/CHANGELOG.md` Enhancement entry mentions the schema key
    and the per-validator flag.

## Hard Stops

- Encoder return shape change must NOT introduce a backwards-
  compatible `*` overload — clean tuple return, both call sites
  updated atomically. (Layered shims have bitten this codebase
  before; no half-step.)
- `details_json` round-trip MUST be lossless — verified by the
  e2e test reading the persisted row back. If any storage backend
  loses precision on a numeric stat, the schema is converted to
  plain floats before persisting (no numpy scalars, no extension
  dtypes).
- The explainer MUST be wrapped in `try/except Exception` at the
  validator call site. A defective explainer cannot turn a SHAP
  computation into a validator-level ERROR.
- Notification body length cap: top-3 explainer summaries only in
  the body. The same notifier render must NOT inline more than 3.
- No change to today's `top_contributing_features` shape, no
  change to `actual_value` / `expected_value` / `metric_name`.
- No new dependencies. The explainer uses pandas + numpy already
  imported by the validators.

## Open Questions Resolved

| Question (from idea.md) | Pin |
|-------------------------|-----|
| Numeric summary shape | Quantile triple p25/p50/p75 + p95/p99 + mean/std + min/max. Slack body uses p50 + mean + p99 in the summary string. |
| Categorical summary | One-hot bins: rate-per-bin. Label-encoded high-cardinality: top-5 category mix. |
| Encoded-only features | Recover original `source_column` and `category` via `encoding_map`. Surface raw column name in the body, not the encoded name. |
| Notification budget | Body shows top-3 explainer summaries; full top-5 in `details_json` (HTML/dashboard rendering deferred). |
| Partition fairness | Compare against the **union of past slices** (matches SHAP's training contract). |

## Why this is shippable in one PR

- Two-validator surface; one helper module; one schema addition.
- Encoder change is mechanical (clean tuple return, two call sites).
- All four storage backends already serialize arbitrary dict trees
  through `details_json`; no schema migration required.
- No new public API on `Qualifire`; only per-validator constructor
  flags. Operator UX surface = two new YAML keys
  (`model.explain_value_drift`).

## Iteration Notes

### Adversarial round 1 fixes (applied below)

R1.1 One-hot prefix collision: `pd.get_dummies(prefix=col)` produces
`col_<value>` which is ambiguous if another column already exists with
that exact name. `encoding_map` is populated **inline inside
`encode_columns`** at the exact branch where `prefix=col` and the
category value are still in scope — never via post-hoc string
splitting. `encode_columns` additionally asserts `len(set(feature_names))
== len(feature_names)` and raises a descriptive `ValueError` on
duplicates so collisions surface at encoder time, not in the
explainer.

R1.2 Datetime suffix lookup: SHAP returns `created_at_timestamp`;
explainer **must** look that string up in `encoding_map[name]`,
never strip `_timestamp`. The encoder populates the map keyed by the
full `f"{col}_timestamp"` name with `source_column=col`, `kind="datetime"`.

R1.3 Pre-encoding pandas capture point: in `pattern_check.py`
the explainer reads from the **post-keep_cols, pre-`_qf_label`** form
— captured into `current_for_explain` and `past_for_explain` after
the `keep_cols` filter (line ~218) and before `current_pdf["_qf_label"] = 1`
(line ~222). In `isolation_forest.py` same — captured after the
`common_cols` reduce (line ~202) and before `current_pdf["_qf_period"] = "current"`
(line ~210). Both validators pass these explicit references through
to the explainer.

R1.4 Label-encoded categorical reads raw string values from the
pre-encoding pandas Series (`.value_counts()`), never from the
encoded integer matrix.

R1.5 JSON-jsonable cast: helper `_to_jsonable(value)` in
`_drift_explainer.py` casts numpy scalars / pandas extension scalars
to plain `int` / `float` / `bool` / `str`. Every leaf value in the
returned dict passes through it. Non-finite floats (`NaN`, `+inf`,
`-inf`) → `None`.

R1.6 Non-finite handling: any computed float that is NaN or inf →
`None`. `delta_*_pct` with past-mean / past-rate of zero → `None`.
Single-row series → `std` is `None`. Empty series → all stats are
`None` except `count=0`.

R1.7 Encoder uniqueness assertion (covered by R1.1).

R1.8 Body arrow: use `→` (U+2192) — the existing notifier already
emits `•` (U+2022) for SHAP feature bullets, so adding U+2192 keeps
the rendering consistent. (R1's earlier ASCII-only stance is
reversed here in R2 after auditing existing renderer output.)

R1.9 Label-encoded top-N category mix shape pinned: union of
current-top-5 and past-top-5 by raw value, sorted by current rate
descending; missing rate on either side → `0.0`. Same N applies to
both sides so the operator can read across without alignment math.

R1.10 YAML config wiring: `qualifire/core/config.py` is now in the
"Files Touched" list. `AnomalyModelConfig.explain_value_drift: bool = True`
and `PatternModelConfig.explain_value_drift: bool = True` are
declared so the YAML key `model.explain_value_drift` is accepted by
preflight. Engine/factory wiring (whichever maps `*_config.model.*`
→ validator constructor kwargs) requires no change because Pydantic
exposes the new field on the same model dump.

R1.11 Explicit signature pinned:
```python
def explain_value_drift(
    top_features: list[dict[str, Any]],
    current_pdf: pd.DataFrame,            # post-keep_cols, pre-label
    past_pdfs: list[tuple[str, pd.DataFrame]],   # same shape as sampler.metadata["past_dfs"]
    encoding_map: dict[str, EncodedFeatureSpec],
) -> list[dict[str, Any]]:
```

### Adversarial round 2 fixes (applied below)

R2.1 `qualifire/core/engine.py` (lines ~1630, ~1650) needs explicit
`explain_value_drift=model.explain_value_drift` pass-through —
added to "Files Touched". Plan-only oversight in R1.

R2.2 Empty / degenerate `top_features` edge cases:
- `top_features == []` → explainer returns `[]`. No error, no log.
- `top_features` entry references a feature_name not in
  `encoding_map` (defensive: should never happen given R1.1's
  uniqueness assertion + populated map) → entry's `kind="unknown"`,
  `source_column=feature_name`, summary string is
  `"unmapped feature: {feature_name}"`.

R2.3 `unknown` kind summary shape pinned:
`"{feature} — count cur={c}, past={p}"` (no distribution stats).

R2.4 One-hot category extraction MUST iterate `dummies.columns`
post-`pd.get_dummies` and strip the literal `f"{col}_"` prefix; do
not reconstruct names from `series.unique()` (pandas-version-dependent
NaN suffix). The dummy_na column's stripped suffix is mapped to
`category="__NULL__"` regardless of pandas's literal `nan`/`NaN`
spelling.

R2.5 Body line for an entry whose `kind="unknown"` is still emitted
(top-3 cap applies uniformly) so the operator sees the partial
information rather than a silent gap.

R2.6 Additional acceptance criteria:
- AC#15: empty `top_features` → empty `value_drift_explainer`
  list, no errors, no log.
- AC#16: feature_name not in `encoding_map` → entry has
  `kind="unknown"` and the documented summary string.
- AC#17: engine constructs `IsolationForestValidator` and
  `PatternCheckValidator` with `explain_value_drift=` set from the
  config; preflight test exercises both.

R1.12 Additional acceptance criteria:
- AC#11: `encode_columns` raises `ValueError` on duplicate
  feature_names (collision test).
- AC#12: `_to_jsonable(np.float64(1.0))` returns plain `float`;
  `_to_jsonable(np.int64(1))` returns plain `int`;
  `_to_jsonable(float("nan"))` returns `None`.
- AC#13: `category` is `None` for every kind except `onehot`;
  for `onehot` it is the raw category value as a string (or
  `"__NULL__"` for the `dummy_na=True` bin).
- AC#14: `AnomalyModelConfig` and `PatternModelConfig` accept
  `explain_value_drift: false` from YAML (preflight test).

### Codex round 1 fixes (applied)

C1.1 [HIGH] One-hot null bin disambiguation — `EncodedFeatureSpec`
now carries `is_null_bin: bool = False`. The `dummy_na=True` bin
sets `is_null_bin=True`; a real category whose value happens to be
the literal string `"__NULL__"` keeps `is_null_bin=False`. Schema:
`category` field still carries the raw value as a string for
display, but the read contract is "treat as null bin iff
`is_null_bin == true`." Schema example and AC#13 updated.

C1.2 [HIGH] Notification body / storage size budgets — new
"Size budgets" section above pins per-summary, per-category, and
per-payload caps with truncation markers. Notifier-side per-line
cap is independent of the explainer-side cap so future schema
loosening doesn't blow the body.

C1.3 [HIGH] HTML report wiring — deferred. The plan no longer
claims `plot_value_drift` is wired into `generate_html_report`.
The static plot helper is for notebook callers only; HTML
integration is its own iteration once the schema settles.

C1.4 [HIGH] Storage backend coverage — AC#6 narrowed to SQLite.
The plan no longer promises four-backend round-trip testing in
this PR; per-backend coverage is a follow-up. The text
acknowledges the existing `tests/test_storage/` suite already
covers the `json.dumps` serializer surface.

C1.5 [MED] Glyph contradiction — `→` (U+2192) used uniformly in
the body section, AC#4, and the rendering examples. R1.8's earlier
ASCII-only stance was reversed in R2 after auditing the existing
`•` (U+2022) bullet usage; codex confirmed the contradiction; now
fully consistent on `→`.

C1.6 [MED] Edge-case acceptance criteria — added below.

C1.7 [MED] Mapping-error observability — `details["value_drift_explainer_mapping_errors"]`
counter + WARNING log; covered in the new "Observability" section.

C1.8 [MED] Documentation paths — `docs/validators/shape.md` and
`docs/validators/pattern.md` (replaced earlier wrong `docs/shape_check.md`
/ `docs/pattern_check.md`).

C1.9 [LOW] Validator method reference — plan now correctly says
the SHAP block is inside `validate()`, not in a non-existent
`_explain` method. Line-number ranges for the existing branches
captured.

C1.10 [LOW] Datetime plotting — bar metric removed for the
datetime kind; the plot renders a text annotation with the
per-side ISO min/max range instead.

### Codex round 1 — additional acceptance criteria

- AC#18: `EncodedFeatureSpec` for the `dummy_na=True` bin has
  `is_null_bin=True`; for every other one-hot bin
  `is_null_bin=False`. Test passes a column with values
  `["a", None, "__NULL__"]` and asserts the three bins are
  distinguishable (one with `category="a"`, one with
  `category="__NULL__"` and `is_null_bin=False`, one with
  `is_null_bin=True`).
- AC#19: Numeric explainer with all-NaN `current` series →
  every numeric stat is `None`, `count=0`, `null_pct=None`.
- AC#20: Numeric explainer with single-row series → `std=None`,
  rest of the stats present.
- AC#21: One-hot explainer with mismatched current vs past
  categories (e.g. current has `mobile` only, past has `web`,
  `mobile`) → both bins appear, missing-side `rate=0.0`.
- AC#22: Label-encoded explainer payload exceeding the 16 KB
  budget → `details["value_drift_explainer_truncated"] == True`,
  the explainer list is shorter than `top_contributing_features`,
  remaining entries are in importance order.
- AC#23: Per-summary string strictly ≤ 200 chars; overflow ends
  with `... [truncated]`. Notifier per-line strictly ≤ 240 chars.
- AC#24: Mapping-error path — when an entry's feature_name is
  not in `encoding_map` (forced by mocking), the validator's
  result has `details["value_drift_explainer_mapping_errors"] >= 1`
  and the body still emits the `→ unmapped feature: …` line.

### Codex round 2 fixes (applied)

C2.1 [HIGH] Truncation contract: parallel-length invariant is
preserved. Over-budget entries are **replaced in place** with a
placeholder dict carrying `kind="truncated"` and a fixed minimal
shape; entries are placeholdered in reverse-importance order so the
top entries survive. Truncation flag set by validator from
inspecting any `kind == "truncated"` in the returned list. AC#22
updated below to assert the parallel-length invariant.

C2.2 [HIGH] HTML deferral consistency: line 187 ("full top-5 in
details_json keeps the full top-5") rewritten without the HTML
claim; line 30 (in-scope bullet) clarified to "notebook only";
"Open Questions Resolved" / Notification-budget row rewritten
without the HTML claim. Only the explicit "HTML report
integration — deferred" section now mentions HTML.

C2.3 [HIGH] Unknown vs unmapped summary split:
- `kind == "unknown"` (encoder couldn't determine kind, e.g.
  encoder branch's `else: drop_complex` path returning a label-
  encoded fallback that the encoder marks `unknown`) →
  `summary = "{feature} — count cur={c}, past={p}"`.
- `kind == "unknown"` AND feature was missing from `encoding_map`
  (defensive path) → `summary = "unmapped feature: {feature}"`.
  Distinguishable via the presence of `details["value_drift_explainer_mapping_errors"]`
  counter (only the unmapped path increments it).
- `kind == "truncated"` → `summary = "payload truncated"` (C2.1).

C2.4 [HIGH] Datetime NaT — explicit ACs added below.

C2.5 [HIGH] Object/unhashable label-encoded values — the explainer
applies the same fill / stringify rule the encoder applies before
calling `value_counts()`: `series.astype(str).fillna("__NULL__")`.
Top-N category values pass through `_to_jsonable` (string already);
unhashable cell values are stringified, never raw. AC added.

C2.6 [MED] `↳` removed from the test-files section — now uses `→`.

C2.7 [MED] AC#19 corrected: all-NaN non-empty current numeric
series → `count=0`, `null_pct=1.0`, numeric stats `None`. The
`null_pct=None` sentinel is reserved for the strictly zero-row
slice (where the denominator is 0). Updated below.

### Codex round 2 — replacements / additions

- **AC#19 (replaces R1's version):** all-NaN non-empty current
  numeric series → `count=0` for finite values, `null_pct=1.0`,
  every numeric stat (`mean`, `std`, all percentiles, `min`,
  `max`) is `None`. A strictly zero-row slice (e.g. past slice
  filtered down to 0 rows) → `count=0`, `null_pct=None`, every
  stat `None`. The two cases are distinguishable from the
  `null_pct` value alone.
- **AC#22 (replaces R1's version):** Label-encoded explainer
  payload exceeding the 16 KB budget → at least one entry has
  `kind == "truncated"`, the **list length still equals
  `len(top_contributing_features)`**, the placeholdered entries
  are at the tail (lowest importance), and
  `details["value_drift_explainer_truncated"] == True`.
- **AC#25 (NEW, datetime NaT):** Datetime current series
  with mixed NaT and valid timestamps → `null_pct` reflects the
  NaT fraction, `min`/`max` are ISO 8601 strings of the
  non-NaT extremes, `pd.NaT` does not appear anywhere in the
  returned dict. All-NaT non-empty series → `null_pct=1.0`,
  `min=None`, `max=None`, `count=0`.
- **AC#26 (NEW, label_encoded object/unhashable):** Object dtype
  column with cell values that include a dict / list (label-
  encoded by the encoder) → top-N category mix uses the same
  `astype(str).fillna("__NULL__")` representation the encoder
  applied. No raw dict / list survives in the output JSON.
- **AC#27 (NEW, unmapped vs unknown disambiguation):** Two
  scenarios in one validator integration test:
  (a) feature_name missing from `encoding_map` → entry summary
      starts with `unmapped feature:` AND
      `details["value_drift_explainer_mapping_errors"] >= 1`.
  (b) feature_name present in `encoding_map` with `kind="unknown"`
      → entry summary matches the count-only template AND
      `mapping_errors` counter is absent or 0.
