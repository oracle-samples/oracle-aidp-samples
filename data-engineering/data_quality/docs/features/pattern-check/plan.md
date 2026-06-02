---
started: 2026-04-21
---

# Implementation Plan: Pattern Check (Random Forest Two-Sample)

## Overview

Ship a new validation type `pattern` (YAML `type: "pattern"`, builder
`pattern_check()`) that answers the question **"does the overall
multivariate shape of the current run differ from past runs?"** by
training a Random Forest two-sample classifier (past rows = label 0,
current rows = label 1) and alerting when the cross-validated AUC is
meaningfully above 0.5. SHAP over the trained classifier names the
features driving the separation.

This sits **alongside** `shape` (Isolation Forest, per-row outlier)
and `drift` (single-metric historical deviation) — nothing about
existing validators changes.

### Stated outcome (from `idea.md`)

Users can write:

```yaml
- type: "pattern"
  collection:
    type: "sample"
    n_records: 10000
    history:
      past_dates: 3
      step: "P7D"
  thresholds:
    warning: { auc: 0.65 }
    error:   { auc: 0.80 }
```

…and get a batch-level, multivariate "did this run look different
from past runs?" signal with SHAP-driven feature attribution, using
the same sampling/history machinery already backing `shape`.

### What this plan is NOT

- Not a replacement or reimplementation of `shape` — the Isolation
  Forest path is untouched.
- Not a change to `drift`/`trend` — their history model is unchanged.
- Not a new collector — reuses `SamplerCollector` + `CollectionResult
  (metric_name="sample", metadata={current_df, past_dfs})`.
- Not a storage / system-table schema change — persistence shape
  matches `shape`'s today.

## Scope gates (hard-stop conditions)

Implementation MUST stop and escalate to the user if any of the
following become true during development:

1. **Scope gate — no new collector.** If a design pressure emerges to
   add a new collection type (e.g., `pattern_sample`), stop. Reusing
   `SampleCollectionConfig` is load-bearing for this plan; a new
   collector would double the documentation and E2E surface and is
   out of scope.
2. **Scope gate — no changes to `shape`.** If a refactor into a
   shared "sample-based validator" base class is tempting, stop. The
   Isolation-Forest validator is independently reasoned about; a
   shared base would couple their blast radii.
3. **Scope gate — no new optional-dependency group.** Both
   scikit-learn and SHAP are already in the `anomaly` extra (see
   `qualifire/validation/isolation_forest.py:19-41`). Reuse the same
   extra; do not create a `pattern` extra.
4. **Scope gate — no storage-layer work.** `pattern` persists via
   the same `_persist_results` path as `shape`. If a change to
   `qualifire/storage/*` is being considered, stop.
5. **Correctness gate — SHAP must be optional, never required.**
   `explain: false` must still produce a valid ValidationResult even
   if SHAP is not installed. The existing `_import_shap()` guard
   pattern applies.
6. **Hard-stop — AUC cross-validation must be real.** The plan
   explicitly forbids reporting train-set AUC. If cross-validation
   cannot be made to work under the tiny-sample edge cases in
   Testing Strategy, stop and revise the plan.

## Acceptance Criteria

All of the following are required to consider the feature done.
Each is verifiable from the diff + CI output (no "feels good"
acceptance).

1. A YAML config with `type: "pattern"` round-trips through
   `load_config()` and produces a `PatternValidationConfig` instance.
   A typo like `type: "patten"` fails at `validate-config` time with
   a clear Pydantic message, not a runtime crash.
2. Running a dataset with only a `pattern` validator against
   fixture data where `current` and all `past_*` samples are drawn
   from the same distribution (seed-controlled) produces
   `severity = PASS` and `actual_value (auc) < 0.60` with
   deterministic `random_state=42`.
3. Running the same setup where `current` is drawn from a clearly
   shifted distribution (documented fixture in the unit test)
   produces `severity in (WARNING, ERROR)` and `actual_value > 0.65`.
4. When `past_dfs == []` (cold start), the validator returns one
   `ValidationResult` whose severity follows `on_missing_history`
   (`ignore → PASS` with `details.cold_start=True`, `warn → WARNING`,
   `error → ERROR`). No unhandled exception, no model training.
5. When `current_df` is empty, the validator returns
   `self._empty_data_result(...)` — same pattern as
   `isolation_forest.py:118-123`. Severity follows `on_empty_data`.
6. When `explain=True` and SHAP is installed and
   `severity != PASS`, `details["top_contributing_features"]` is a
   list of `{feature, importance}` dicts, length ≤ 5, non-empty.
   When `explain=False`, the key is absent (not present-but-empty).
7. When `explain=True` and SHAP raises *any* exception during
   explanation, the validator logs a warning and returns the
   ValidationResult **without** a `top_contributing_features` key
   (no crash). Mirrors `isolation_forest.py:233-234`.
8. `pattern_check()` programmatic builder exists on `Qualifire` and
   produces an equivalent `PatternValidationConfig` to the YAML path
   (unit-tested by round-tripping both through the engine and
   asserting identical `model_dump()`).
9. `pattern` is listed in `_HISTORY_BACKED_VALIDATION_TYPES` in
   `qualifire/api.py` so `validate_query()` refuses to default the
   dataset name (protects history-key stability — see round-14
   finding referenced at `api.py:48-55`).
10. An E2E scenario in `tests/test_e2e.py` (or sibling file) exercises
    `pattern` alongside `drift`, `shape`, `trend`, `slo`, `threshold`
    in a single run, asserting all six types produce rows in the
    system table with the correct `validation_type`.
11. `docs/validators.md` lists `pattern` in the validator table;
    `docs/pattern_check.md` exists with a structure mirroring
    `docs/shape_check.md`; `docs/configuration.md` documents the new
    YAML schema block; `README.md` mentions `pattern` wherever
    `shape`/`drift` are enumerated.
12. `pytest -q tests/test_validation/test_pattern_check.py` and
    `pytest -q tests/test_cli.py -k pattern` both pass locally with
    the project's existing lint/type configuration (no new
    `mypy` / `ruff` ignores introduced).

13. **Leakage-control verification.** A unit test drives `pattern`
    with a fixture where `current` and `past_*` differ **only** on
    a date column (`event_date`). Without the column in
    `exclude_columns` the validator MUST alert (AUC ≈ 1.0) — this
    is the positive control proving the failure mode is real. With
    `event_date` in `exclude_columns`, same fixture MUST PASS with
    AUC near 0.5. Both assertions in the same test, same fixture.

14. **Config validation — threshold keys and ranges.**
    Typoed threshold keys (`warning: { aux: 0.65 }`), out-of-range
    values (`warning: { auc: 1.5 }`, `warning: { auc: 0.3 }`), and
    inverted thresholds (`warning.auc=0.9, error.auc=0.7`) MUST fail
    at `validate-config` time with a Pydantic error naming the
    field. Unit tests cover all three cases.

15. **End-to-end data-slice wiring proof.** An E2E scenario in
    `tests/test_e2e.py` configures `pattern` with a real
    `slice_column` + `slice_value` pair (selecting current rows) and
    an explicit `history.filters` list (selecting past rows) against a seeded
    fixture where the partitioned slices are (a) same-distribution
    — asserts PASS, and (b) shifted for the current slice —
    asserts alert. This proves the engine→collector→validator
    wiring feeds the *right* data to the classifier, not just
    that the classifier math is correct. To keep the test
    deterministic and scoped to this feature's surface (see Out of
    Scope for deterministic Spark sampling), both scenarios run
    against the **Pandas backend** — same contract as the sampler,
    no Spark cluster required, no flake risk from unseeded Spark
    sampling.

16. **`history.filters` length contract — config-load validation.**
    `SamplerCollector` ignores `past_dates` when `history_filters`
    is provided (see `qualifire/collection/sampler.py:58-62`). A
    `pattern` config that specifies both (`past_dates: 3` +
    `filters: [a, b, c, d, e]`) silently trains on 5 past slices,
    changing class balance and CV stability with no operator
    signal. `PatternValidationConfig` MUST add a
    `@model_validator(mode="after")` that raises
    `ValueError` when `collection.history` has both fields set and
    `len(filters) != past_dates`. The error message must name both
    values. (This local validator is inside
    `PatternValidationConfig` — not `SampleHistoryConfig` — to
    avoid changing `shape`'s validation semantics per scope gate
    §2.) Unit test covers the mismatch case.

17. **Persistence round-trip.** An E2E assertion verifies that a
    `pattern` row lands in the system table with `details_json`
    non-null and parseable — catches the JSON-safety failure mode
    (numpy scalars in `details`) described in Design §10.

## Design — config schema

Mirrors the existing `AnomalyDetectionValidationConfig` shape exactly
so the surface is familiar to existing operators. New symbols live in
`qualifire/core/config.py`.

```python
class PatternModelConfig(BaseModel):
    # Hyperparameters chosen to match idea.md defaults. These are
    # overridable because pattern_check is a power-user tool and
    # saturating the forest on tiny samples is a real footgun.
    n_estimators: int = 200
    max_depth: int | None = 8          # None = unlimited (sklearn default)
    class_weight: Literal["balanced", "balanced_subsample"] | None = "balanced"
    random_state: int = 42
    # Cross-validation folds. Enforced to [2, 10] at config-load time.
    cv_folds: int = 5
    # SHAP: optional, never required. Absent key in details when false.
    explain: bool = True
    # Schema-change behavior mirrors shape.
    drop_complex: bool = False
    alert_on_schema_change: bool = False
    # Cold-start behavior mirrors shape.
    on_missing_history: Literal["ignore", "warn", "error"] = "ignore"
    # Leakage control — partition / date / ingestion-timestamp / ID
    # columns that are used to *define* the current-vs-past slices
    # would let the classifier separate runs trivially even when the
    # business distribution is unchanged. Operators MUST list them
    # here; the validator drops these columns before encoding. Empty
    # list = no exclusions (caller asserts no leakage columns exist).
    exclude_columns: list[str] = Field(default_factory=list)

    @field_validator("cv_folds")
    @classmethod
    def _validate_cv_folds(cls, v: int) -> int:
        # CV folds < 2 is mathematically meaningless (single-split
        # train/test is not CV). Folds > 10 on default sample sizes
        # means each test fold has O(100) rows and the AUC variance
        # swamps the signal. Cap at [2, 10] at config-load time so
        # operators get a pre-flight error, not a mid-run crash.
        if v < 2 or v > 10:
            raise ValueError(f"cv_folds must be in [2, 10]; got {v}. Defaults to 5.")
        return v

    @field_validator("exclude_columns")
    @classmethod
    def _validate_exclude_columns(cls, v: list[str]) -> list[str]:
        # Same identifier grammar the rest of the schema uses for
        # column names. Prevents "accidentally SQL-injection-shaped"
        # strings from loading and then no-op'ing at runtime.
        for col in v:
            if not _SAFE_IDENTIFIER_RE.match(col):
                raise ValueError(
                    f"exclude_columns entry must be a valid identifier, got: {col!r}"
                )
        return v


class PatternThresholdConfig(BaseModel):
    # Per-AUC thresholds. Only ``auc`` key is accepted today — forward-
    # compat for adding ``kl`` etc. later, but typoed keys must fail
    # at config-load time so users don't ship silent fallback-to-default.
    warning: dict[str, float] | None = None   # {"auc": 0.65}
    error: dict[str, float] | None = None     # {"auc": 0.80}

    _ALLOWED_KEYS = {"auc"}

    @field_validator("warning", "error")
    @classmethod
    def _validate_threshold_block(
        cls, v: dict[str, float] | None
    ) -> dict[str, float] | None:
        if v is None:
            return v
        bad_keys = set(v.keys()) - cls._ALLOWED_KEYS
        if bad_keys:
            raise ValueError(
                f"Unknown threshold key(s) {sorted(bad_keys)}. "
                f"Allowed: {sorted(cls._ALLOWED_KEYS)}."
            )
        auc = v.get("auc")
        if auc is not None and not (0.5 <= auc <= 1.0):
            raise ValueError(
                f"auc threshold must be in [0.5, 1.0]; got {auc}. "
                "Below 0.5 means the classifier is worse than random; "
                "above 1.0 is mathematically impossible."
            )
        return v

    @model_validator(mode="after")
    def _warning_le_error(self) -> PatternThresholdConfig:
        w = (self.warning or {}).get("auc")
        e = (self.error or {}).get("auc")
        if w is not None and e is not None and w > e:
            raise ValueError(
                f"warning.auc ({w}) must be <= error.auc ({e}); "
                "an AUC that triggers ERROR should also trigger WARNING."
            )
        return self


class PatternValidationConfig(BaseModel):
    type: Literal["pattern"] = "pattern"
    name: str | None = None
    collection: SampleCollectionConfig          # REUSE — not a new type

    @field_validator("name")
    @classmethod
    def _validate_name(cls, v: str | None) -> str | None:
        if v is not None and not _SAFE_NAME_RE.match(v):
            raise ValueError(
                f"'name' must contain only alphanumeric characters, "
                f"underscores, dots, or hyphens, got: {v!r}"
            )
        return v

    model: PatternModelConfig = Field(default_factory=PatternModelConfig)
    thresholds: PatternThresholdConfig = Field(default_factory=PatternThresholdConfig)
    on_empty_data: Literal["pass", "warning", "error"] = "warning"
    notify: NotifyConfig = Field(default_factory=NotifyConfig)
    suppress_repeat_alerts: bool = True
```

`PatternValidationConfig` is added to the discriminated `Union`:

```python
ValidationConfig = Union[
    SLOValidationConfig,
    ThresholdValidationConfig,
    HistoricalValidationConfig,
    ForecastValidationConfig,
    AnomalyDetectionValidationConfig,
    PatternValidationConfig,        # new, last entry
]
```

The ordering matters only for Pydantic's discriminator resolution;
adding at the end is safe because `type` is a `Literal` discriminator
on each arm.

## Design — validator

New module: `qualifire/validation/pattern_check.py`.
Mirrors `isolation_forest.py` structurally but implements a **two-
sample classifier**, not an outlier detector.

Key steps per `CollectionResult` where `metric_name == "sample"`:

1. **Read inputs.** `current_df` and `past_dfs` (list of
   `(label, df)` tuples) from `cr.metadata`. Convert Spark
   DataFrames to pandas via `toPandas()` as `isolation_forest.py`
   does. Preserves the collector contract exactly.
2. **Empty / cold-start guards.** Reuse the same logic as
   `isolation_forest.py:118-136` — empty current → `_empty_data_result`;
   no past → `on_missing_history` branch.
3. **Column intersection + schema-change detection.** Same as
   `isolation_forest.py:139-163` — intersect column sets across
   current and all pasts; if `alert_on_schema_change`, emit an
   auxiliary `{name}.schema` WARNING ValidationResult. Use the
   intersection columns only for the classifier.
4. **Build labeled training set.** Concatenate all pasts with
   `label=0` and current with `label=1`. Drop `_qf_period`-style
   internal columns that `SamplerCollector` *does not emit* today;
   the current contract is raw row samples so there is nothing to
   strip. (Double-check during implementation: if
   `SamplerCollector` later adds bookkeeping columns, they must be
   dropped here.)
5. **Drop leakage columns, then encode features.**
   Before encoding, drop every column named in
   `config.model.exclude_columns` from both current and past. This
   is a **required** leakage-control step: because the classifier
   is trained to separate past (label 0) from current (label 1),
   any partition / date / ingestion-timestamp / surrogate-ID column
   used to *define* the current vs past slices would let the forest
   classify runs trivially at AUC ≈ 1.0 even when the business
   distribution is unchanged. This failure mode is load-bearing to
   prevent, not theoretical — any operator who slices by date is
   exposed by default. The doc must call this out prominently
   (`docs/pattern_check.md` — see Implementation Step 6) and every
   example config must include a representative `exclude_columns`.

   Then reuse the **exact** encoding function from
   `IsolationForestValidator._encode_columns` — either factor it out
   to a shared module `qualifire/validation/_encoding.py` with both
   validators importing it, OR copy it verbatim with a comment
   pointing at the shared invariant. The plan commits to **factor
   out** (see Implementation Steps §3) because encoding divergence
   between `shape` and `pattern` would be a silent footgun.
6. **Guard trivial training sets.** The ONLY feature-count gate is
   "zero encodable columns" — a single encoded feature is still a
   valid two-sample classifier input (e.g., a dataset whose only
   shift is in one column after exclusions). Do NOT reject single-
   column matrices. Additionally, if either class has fewer than
   `2 * cv_folds` rows (so `StratifiedKFold` can actually split),
   return a `_empty_data_result` with a message naming the gate
   that failed. This is the hard-stop for "AUC on 3 rows" pathology.
7. **Train + cross-validate.**
   ```python
   from sklearn.ensemble import RandomForestClassifier
   from sklearn.model_selection import StratifiedKFold, cross_val_score
   model = RandomForestClassifier(
       n_estimators=config.n_estimators,
       max_depth=config.max_depth,
       class_weight=config.class_weight,
       random_state=config.random_state,
       n_jobs=1,  # stay single-threaded; caller owns parallelism
   )
   skf = StratifiedKFold(
       n_splits=config.cv_folds, shuffle=True, random_state=config.random_state
   )
   auc_scores = cross_val_score(model, X, y, cv=skf, scoring="roc_auc")
   auc = float(auc_scores.mean())
   auc_std = float(auc_scores.std())
   ```
   Report **mean CV AUC** as `actual_value`; attach per-fold scores
   and std to `details`.
8. **Severity.** Apply `determine_severity(auc, warning_check, error_check)`
   where each `*_check` is `auc >= threshold`. Fall back to defaults
   (`warning=0.65`, `error=0.80`) exactly like `shape` falls back
   (`isolation_forest.py:713-714` pattern in `engine._validate`).
9. **SHAP explanation (optional).** When `explain=True` and
   `severity != PASS`:
   - Fit a final model on the full `(X, y)` (fresh estimator, not
     a CV fold's estimator — scikit-learn does not expose those).
   - `shap.TreeExplainer(model)` → `shap_values(X)`.
   - For binary classification `shap.TreeExplainer` returns either
     a list `[neg_class, pos_class]` or a stacked array depending
     on SHAP version; select the `current` (label=1) array
     defensively (`np.array(shap_values)[..., 1]` if 3D, else
     `shap_values[1]` if list, else `shap_values`). Wrap the whole
     block in try/except per acceptance criterion §7 so a SHAP
     API/version mismatch does not fail the validator.
   - Mean-abs SHAP → top-5 features → `details["top_contributing_features"]`.
10. **Emit ValidationResult.** Same shape as `shape`. Every value
    written into `details` MUST be a builtin JSON-safe type — the
    system-table persistence path (`engine._persist_results` →
    `storage.write_results` → `json.dumps(details)`) will silently
    log-and-drop writes if `details` contains `numpy.float64`,
    `numpy.int64`, or similar. Explicitly cast scalars with
    `float(...)`/`int(...)` and list elements with a comprehension:

    ```python
    ValidationResult(
        validation_name=self.name,
        validation_type="pattern",
        severity=severity,
        message=f"Pattern AUC: {auc:.3f} (±{auc_std:.3f})",
        metric_name="auc",
        expected_value={
            "warning_threshold": float(self.warning_threshold),
            "error_threshold": float(self.error_threshold),
        },
        actual_value=float(auc),
        details={
            "auc": float(auc),
            "auc_std": float(auc_std),
            # Cast each fold score — cross_val_score returns numpy
            # scalars and a raw list of those serializes to a
            # persistence error, not a JSON array.
            "auc_folds": [float(s) for s in auc_scores],
            "n_current": int((y == 1).sum()),
            "n_past": int((y == 0).sum()),
            "n_features": int(X.shape[1]),
            # "top_contributing_features" set iff explain succeeded;
            # its "importance" field is also cast to float(...).
        },
        collected_at=cr.collected_at,
    )
    ```

## Implementation Steps

Single PR. Sequenced so each commit leaves tests green.

- [x] **Step 1 — Config plumbing (no behavior yet).**
  - Add `PatternModelConfig`, `PatternThresholdConfig`,
    `PatternValidationConfig` in `qualifire/core/config.py`.
  - Register in `ValidationConfig = Union[..., PatternValidationConfig]`.
  - Add unit test in `tests/test_cli.py` modeled on
    `test_validate_config_rejects_invalid_step_in_historical_rule`
    asserting (a) valid `type: "pattern"` YAML round-trips, (b)
    `cv_folds=1` and `cv_folds=11` are rejected at load time, (c)
    typo `type: "patten"` is rejected at load time.
  - **Exit:** `pytest -q tests/test_cli.py -k pattern` passes.
    Engine does not yet know about the type.

- [x] **Step 2 — Factor out encoding into `_encoding.py`.**
  - Move `IsolationForestValidator._encode_columns` (and its
    `CARDINALITY_THRESHOLD` constant) into
    `qualifire/validation/_encoding.py` as a free function
    `encode_columns(df, *, drop_complex, cardinality_threshold=20)`.
  - Update `IsolationForestValidator` to import from the new module.
  - **Exit:** existing `pytest -q tests/test_validation/test_isolation_forest.py`
    passes without modification.

- [x] **Step 3 — PatternCheckValidator.**
  - New `qualifire/validation/pattern_check.py` with
    `PatternCheckValidator` implementing the design above. Imports
    `encode_columns` from Step 2.
  - Wire into `qualifire/core/engine.py::_validate` with a new
    `isinstance(val_config, PatternValidationConfig)` branch
    mirroring the `AnomalyDetectionValidationConfig` branch at
    `engine.py:710-726`.
  - Add `PatternValidationConfig` to `_HISTORY_BACKED_VALIDATION_TYPES`
    in `qualifire/api.py` (per acceptance §9).
  - Add builder `Qualifire.pattern_check()` in `qualifire/api.py`
    with the same shape as `shape_check` (line `api.py:715`). The
    builder MUST accept an optional `history_filters: list[str] |
    None = None` argument that is forwarded into
    `SampleHistoryConfig(filters=...)` so programmatic callers can
    match the YAML path; the parity test (Step 4) exercises both
    the default (`past_dates`/`step`) and the explicit-filters
    path.
  - **Exit:** `pytest -q tests/test_validation/test_pattern_check.py`
    passes (tests written in Step 4).

- [x] **Step 4 — Unit tests for the validator.**
  New file `tests/test_validation/test_pattern_check.py`.
  Structured identically to `test_isolation_forest.py` (class-based,
  seeded numpy fixtures). Cover every acceptance criterion §2-§9
  and §13:
  - PASS on same-distribution current+past.
  - WARNING / ERROR on clearly shifted current.
  - Cold start: empty `past_dfs` with each `on_missing_history` value.
  - Empty current: each `on_empty_data` value.
  - Explain on / off branches (mock SHAP absent with
    `monkeypatch` on `_import_shap`).
  - Tiny-sample guard: verify the "not enough rows per class for
    CV" branch returns an `_empty_data_result`, not a sklearn
    traceback.
  - Schema-change detection: `alert_on_schema_change=True` with a
    column that appears only in current.
  - **Leakage-control positive + negative control** (§13).
  - Programmatic builder vs YAML parity: `Qualifire.pattern_check(...)`
    and `load_config(...)` yield equal `model_dump()`.
  - Config validation of threshold keys / ranges / inversion
    (§14), in `tests/test_cli.py`.
  - **Exit:** all of the above green.

- [x] **Step 5 — E2E coverage.**
  - Extend `tests/test_e2e.py` with a scenario that runs `pattern`
    alongside the other five validator types in a single
    `Qualifire.run()` invocation against a seeded fixture,
    asserting:
      - No `QualifireValidationError` is raised when current is
        same-distribution as past.
      - A `pattern` row lands in the captured system-table output
        with `validation_type == "pattern"` and a numeric
        `metric_value` ∈ [0, 1].
  - Add a second E2E scenario (acceptance §15) using real
    `slice_column` + `slice_value` + `history.filters` over a
    partitioned fixture to prove engine→collector→validator wiring feeds the
    intended slices: same-distribution config PASSES, shifted
    current config alerts.
  - **Exit:** `pytest -q tests/test_e2e.py -k pattern` passes.

- [x] **Step 6 — Docs.**
  - New `docs/pattern_check.md` structured like `docs/shape_check.md`.
  - `docs/validators.md`: add `pattern` row to the validator table.
  - `docs/configuration.md`: add `type: "pattern"` block to the
    validation examples list.
  - `docs/programmatic_api.md` (if it enumerates builders): add
    `pattern_check`.
  - `README.md`: add `pattern` wherever `shape`/`drift`/`trend` are
    listed.
  - **Exit:** grep for every new symbol shows ≥1 match in `docs/`.

## Testing Strategy

| Layer | File | What it proves |
|-------|------|----------------|
| Config unit | `tests/test_cli.py` | `type: "pattern"` loads; bad `cv_folds` / typo fails at load time (acceptance §1) |
| Validator unit | `tests/test_validation/test_pattern_check.py` | All per-run branches (acceptance §2–§9) |
| Encoding unit | `tests/test_validation/test_isolation_forest.py` (unchanged) | Factored encoding still works for `shape` |
| Builder unit | same test file | Programmatic / YAML parity (acceptance §8) |
| E2E | `tests/test_e2e.py` | `pattern` coexists with other validators (§10) |

Hard-stop: any failure in `test_isolation_forest.py` after Step 2 is
a regression and blocks the PR. The factor-out must be behavior-
preserving.

## Risks & Mitigations

1. **Risk — tiny samples produce wildly unstable AUC.**
   A `current_df` with 30 rows and `past_dfs` with 30 rows each,
   5-fold StratifiedKFold leaves ~6 test rows per class per fold.
   AUC variance is O(0.1), so a false positive is plausible.
   **Mitigation:** Step 3 guard — refuse to train if
   `min(n_class_0, n_class_1) < 2 * cv_folds`; return
   `_empty_data_result("insufficient samples for N-fold CV")` with
   the exact counts in `details`. This surfaces as the operator's
   `on_empty_data` severity rather than as a false-positive drift.

2. **Risk — encoding divergence vs `shape`.**
   If `shape` and `pattern` encode the same DataFrame differently,
   a user who alerts on both would see inconsistent "top feature"
   attributions for the same run — a debugging nightmare.
   **Mitigation:** Step 2 factor-out into `_encoding.py`, with a
   unit test in `test_isolation_forest.py` that asserts the public
   function signature does not change. Both validators import the
   same symbol.

3. **Risk — SHAP version drift.**
   `TreeExplainer.shap_values` return shape for binary classifiers
   has changed across SHAP major versions. A pinned working
   version today may break on upgrade.
   **Mitigation:** (a) defensive selection pattern documented in
   Design §9; (b) the outer try/except per acceptance §7 means
   SHAP failure degrades gracefully to "no top features" rather
   than breaking the validator; (c) unit test that simulates a
   SHAP `ValueError` via `monkeypatch` and asserts the
   ValidationResult still has `severity` / `actual_value` set.

4. **Risk — class imbalance with `history_filters`.**
   An operator providing `past_dfs` of wildly different row counts
   than `current` skews the classifier.
   **Mitigation:** default `class_weight="balanced"` (idea.md
   default) rescales at training time; the AUC threshold is
   class-balance invariant so the actual_value is stable even
   under imbalance; documented in `docs/pattern_check.md`.

5. **Risk — silent bump to scikit-learn / SHAP versions.**
   Neither is listed in this plan's dependencies, because both are
   already in the `anomaly` extra (`isolation_forest.py:19-41`).
   **Mitigation:** reuse the existing extra, no change to
   `pyproject.toml`. If a pinned version bump is ever needed, that
   is a follow-up feature.

6. **Risk — confusion between `pattern` and `shape`.**
   Operators may toggle on both and get correlated alerts.
   **Mitigation:** `docs/pattern_check.md` includes a "When to use
   `pattern` vs `shape`" section drawn from `idea.md` vocabulary
   (drift/shape/pattern). Linked from `docs/validators.md`.

7. **Risk — history-key contamination.**
   `pattern` is history-backed (it reads samples keyed on the
   dataset's logical identity). If `validate_query()` defaults
   the dataset name (round-14 finding, `api.py:48-55`), two
   unrelated queries would share a history key.
   **Mitigation:** acceptance §9 requires adding
   `PatternValidationConfig` to `_HISTORY_BACKED_VALIDATION_TYPES`.
   An assertion test in `test_cli.py` verifies the constant is
   updated (greps for the type name in the tuple).

## Out of Scope (for explicit reviewer check)

- **Rendering `top_contributing_features` in notification channels
  (Slack/email/webhook) or the HTML report.** `pattern` publishes
  the same `details["top_contributing_features"]` surface that
  `shape` already publishes today; extending notifiers/reports to
  render that field applies equally to both validators and must be
  scoped as a separate feature. Doing it under `pattern` would
  silently change `shape`'s alert text in production and is a
  scope expansion this plan explicitly rejects. Idea.md asks that
  the *validation result* surface the features — that is covered
  by acceptance §6. The phrase "so downstream notifications and
  reports can show why" is a forward-looking capability, not a
  requirement that ships with v1.
- Multi-class classifier (N > 2 past periods treated as distinct
  classes). Past is one class, current is the other — this is a
  two-sample test, not a multi-period ANOVA.
- Drift decomposition per feature (PSI-style). SHAP gives feature
  attribution on the classifier; that is the designed surface.
- Streaming / incremental training. `cross_val_score` refits each
  fold; anything resembling online learning is a separate feature.
- Custom scoring functions. AUC is the only scoring metric; other
  metrics (F1, accuracy) would change the severity semantics and
  are out of scope for v1.
- Changes to `SamplerCollector`. The collector contract is stable;
  any change there is a separate feature.
- Deterministic Spark sampling. `backend.sample_records()` uses
  Spark's default non-deterministic sampling today; that's a
  repo-wide property and any change belongs in a separate feature
  that also covers `shape`. Unit tests use pandas fixtures with
  fixed seeds, so validator behavior remains deterministic where
  it is asserted.
