---
started: 2026-05-04
---

# Implementation Plan: Findings Sweep

## Overview

Coordinated single-PR sweep of the 5 Blocking + 9 Should-fix items
(R3 fix: counted explicitly against `.tmp/findings.md` §Should-fix —
nine bullets) raised in `.tmp/findings.md` (the consolidated review
combining `findings_codex.md` and `findings_claude.md`), plus the
typed identity-keys architectural refactor that sits underneath
several of the correctness bugs, plus the dashboard auto-gen hook
hardening.

The plan is organized so the architectural change (Phase 1) lands
before the correctness fixes that depend on it (Phase 2). Phases 3-5
group remaining items by surface area.

**Single PR.** All phases ship together against `main` via
`feature/findings-sweep`. The phase boundaries exist for review
clarity, not for staged delivery.

## Non-Goals

- Splitting `engine.py` / `api.py` along their seams. (Polish item from
  the findings doc, deliberately deferred — too large to bundle here.)
- Adding new validators, new collectors, or new notification channels.
- Changing the CLI surface (no new subcommands).
- Adding lint/format/type-check tooling. (Polish item, deferred.)
- Renaming the `jdbc` storage backend identifier — instead, document
  Spark coupling in-place (cheaper, equally clear). The rename path
  is named in §R3 as a deliberate non-goal.
- Implementing a real pandas-only JDBC path via `pyodbc`/`SQLAlchemy`.
- Migrating system table schema in a backwards-incompatible way.
  (See §R5 — schema additions are append-only, existing rows remain
  readable.)

## Inputs

- `.tmp/findings.md` — canonical combined findings (gitignored under
  `.tmp/`; reviewers must read the local copy, not a committed
  artifact).
- `docs/features/findings-sweep/idea.md` — backlog entry capturing
  scope.

## Phase 1 — Typed Identity Keys (foundation)

### Why first

Multiple Blocking bugs share a root cause: identity (dataset,
validation, metric, dimension) is propagated as loose strings or
collapsed into one of them. Threading typed keys end-to-end in Phase 1
makes Phase 2's correctness fixes one-line lookups instead of
re-engineering exercises.

### Steps

- [ ] **P1.1** — Add two fields to `ValidationResult` at
      `qualifire/core/models.py:44-56`:
      ```python
      # In dataclass body:
      dimension_value: str = "_default"
      validation_base_name: str = field(kw_only=True)
      # Python 3.10+ kw_only=True: required, but keyword-only —
      # avoids breaking positional-arg callers; forces every emit
      # site to name the field explicitly. R4 fix.
      ```
      `pyproject.toml:11` already requires Python ≥ 3.9, but
      `kw_only` needs 3.10+. **Decision pin (R4+R5):** bump
      `requires-python` to `>=3.10` as part of this PR — Python
      3.9 hits EOL in October 2025, so the bump is overdue
      anyway. Update README/getting_started prerequisites
      AND drop `"Programming Language :: Python :: 3.9"` from
      the classifiers at `pyproject.toml:21` so package metadata
      matches the runtime floor.
      `dimension_value` default `"_default"` matches
      `CollectionResult.dimension_value` (R2 fix, pinned canonical).
      `validation_base_name` carries the base name resolved by
      `_resolve_validation_base_name` (P1.3) — used by routing /
      payload filtering / suppression so identity is **not** parsed
      out of `validation_name` substring (R3 fix: prevents
      prefix-collision, e.g. base `"a"` and base `"a.foo"`
      ambiguity).
  - [ ] Test: `tests/test_models.py` —
        `ValidationResult(...).dimension_value == "_default"`;
        `ValidationResult(..., dimension_value='{"region":"EU"}').dimension_value == '{"region":"EU"}'`;
        `ValidationResult(validation_base_name="a", validation_name="a.row_count").validation_base_name == "a"`.
  - [ ] **Wire format note.** Dimensioned collectors write
        `dimension_value` as a JSON-encoded dict like
        `'{"region":"EU"}'` (`collection/aggregation.py:91-94`,
        `metrics.py:~95`, `custom_query.py:~88`). Tests and any
        validator-side construction MUST use the JSON-encoded form,
        not bare `"EU"`. Validator code copies `cr.dimension_value`
        verbatim from collection through to validation, never
        reformats.
  - [ ] **Validator wiring.** Each validator constructor stays
        unchanged in surface, but the `name` arg passed in by the
        engine becomes the `validation_base_name`. Inside the
        validator, every emitted `ValidationResult` sets
        `validation_base_name=self.name` and `validation_name=
        f"{self.name}.{metric}"` (or just `self.name` for
        single-metric validators like SLO). The base name is the
        identity unit; the suffixed `validation_name` is for
        reporting only.
  - [ ] **Enumerated emit-site checklist (R4 finding).** Every
        site that constructs a `ValidationResult` must set
        `validation_base_name`. Audit list:
        - `qualifire/validation/slo.py` — single emit; base = self.name
        - `qualifire/validation/threshold.py:75-83,106-117` — per-rule emits; base = self.name
        - `qualifire/validation/historical.py` — per-rule emits
        - `qualifire/validation/forecast.py` — per-rule emits
        - `qualifire/validation/isolation_forest.py` — per-row emits + schema-change row
        - `qualifire/validation/pattern_check.py` — per-run emit + schema-change row + error rows
        - `qualifire/validation/base.py:_empty_data_result` —
          generic empty-data emit; base = self.name (passed in)
        - `qualifire/core/engine.py:182-193` — dataset-error
          synthetic emit; base = `"qualifire.dataset_error"`
          (new identity reserved for engine).
        - `qualifire/core/engine.py:264-269` — query_execution
          synthetic emit; base = `"qualifire.query"`.
        - `qualifire/core/engine.py:303-308` — df_registration
          synthetic emit; base = `"qualifire.df"`.
        - `qualifire/core/engine.py:543-563` — query_schema emits;
          base = `"qualifire.query_schema"`.
        - `qualifire/core/engine.py:601-625` — `_run_single_validation`
          fallback / error emits; base = val_config-derived.
        - **Engine warning sites:** persistence warning emit
          (P4.1); base = `"qualifire.persistence"`. Distinct
          base per engine error type so multiple engine
          warnings don't suppress each other (R4 finding:
          `validation_base_name=""` default would collapse
          collisions on suppression).
  - [ ] **Default `""` rejected.** The field has no default —
        constructors must pass it explicitly. R4 finding: the
        default-`""` proposed in the original P1.1 would let
        every untouched ValidationResult collide on suppression.
        Force explicit identity at construction.

- [ ] **P1.2** — Persist `dimension_value` on validation rows. The
      system table already has the column for collection rows
      (`qualifire/core/engine.py:1103`), but the validation branch
      (`engine.py:1069`) hardcodes `"dimension_value": None`. Replace
      with `vr.dimension_value`.
  - [ ] Verify against every storage backend's column list at
        `qualifire/storage/base.py:SYSTEM_TABLE_COLUMNS`. No schema
        change required (column already declared).
  - [ ] **Backward compat.** Existing validation rows in the system
        table have `dimension_value IS NULL`. New validation rows
        write `"_default"` for non-dimensioned datasets and
        JSON-encoded dicts for dimensioned ones. **All read paths**
        that filter on `dimension_value` (P4.3) treat NULL and
        `"_default"` as equivalent via `COALESCE(dimension_value,
        '_default') = ?`. Document this as the storage-side read
        invariant in `qualifire/storage/base.py` module docstring.
  - [ ] Test: `tests/test_storage/` — write a `ValidationResult` with
        `dimension_value='{"region":"EU"}'`, read back, assert the
        row carries the JSON string verbatim. Also test the
        `_default` and legacy-NULL equivalence on read.

- [ ] **P1.3** — Honor `val_config.name` for every validator type, not
      just pattern. Currently `engine.py:856-928` hardcodes
      `name=f"<type>.{ds_config.name}"` for SLO/threshold/drift/
      forecast/anomaly; only `PatternValidationConfig` reads
      `val_config.name` (`engine.py:910`).
  - **Verified existing state (R1 review):** every `*ValidationConfig`
    already declares `name: str | None = None` plus a `_SAFE_NAME_RE`
    validator (`config.py:367-455`). The config-side work is **not**
    needed; the gap is engine-side and builder-side.
  - [ ] Replace the per-branch hardcoded names in `engine.py:856-928`
        with a helper `_resolve_validation_base_name(val_config,
        ds_config)`. **R2 fix: explicit fallback mapping.** Today's
        defaults are `slo`/`threshold`/`historical`/`forecast`/
        `anomaly`/`pattern`. Config types are `slo`/`threshold`/
        `drift`/`trend`/`anomaly_detection`/`pattern`. The helper
        uses an explicit dict not `val_config.type`:
        ```python
        _FALLBACK_PREFIX = {
            SLOValidationConfig: "slo",
            ThresholdValidationConfig: "threshold",
            HistoricalValidationConfig: "historical",  # not "drift"
            ForecastValidationConfig: "forecast",      # not "trend"
            AnomalyDetectionValidationConfig: "anomaly",
            PatternValidationConfig: "pattern",
        }
        ```
        Returns `val_config.name or f"{_FALLBACK_PREFIX[type(vc)]}.{ds_config.name}"`.
        Pre-existing history rows under `historical.X` and
        `forecast.X` continue to match.
  - [ ] **Remove dead pattern fallback** (R3 finding). Pattern's
        existing `base_name = val_config.name or f"pattern.
        {ds_config.name}"` at `engine.py:910` becomes the helper
        call too — no special-case branch left for pattern.
        Single source of truth.
  - [ ] Test (default-name regression): each validator type's
        default name is unchanged from before the refactor —
        explicit assertion against the historical strings.
  - [ ] **Preserve metric-suffix invariant.** Validators like
        `ThresholdValidator` emit `validation_name=f"{self.name}.{metric}"`
        (`threshold.py:108`); historical (`historical.py:173`),
        forecast (`forecast.py:191`) do similar. The custom
        `val_config.name` becomes the **base** prefix, not a
        replacement for the metric-suffix segment.
        `ValidationKey.validation_name` (P1.4) carries the
        **full persisted suffixed** name (e.g.
        `"my_check.row_count"`) — NOT the base. The
        `validation_base_name` field (separate, on
        `ValidationResult`) carries the base for routing /
        payload filtering. R6 fix: original prose called this
        "the full base" which read ambiguously; the
        authoritative phrasing is "full persisted suffixed
        validation_name" — same as written to storage.
  - [ ] Add `name: str | None = None` arg to public builders
        `slo_check`, `threshold_check`, `drift_check`, `trend_check`,
        `shape_check` in `qualifire/api.py` (currently only
        `pattern_check` accepts it at `api.py:747`). Pass through to
        the constructed `*ValidationConfig`.
  - [ ] Test (config): YAML with `name: my_check` on a threshold,
        SLO, drift, forecast, anomaly each → every emitted
        `ValidationResult.validation_name` *starts with* `my_check`
        (the base may have a metric-suffix appended for multi-rule
        checks; assert `.startswith("my_check")`, not `==`).
  - [ ] Test (collision): two threshold checks on the same dataset
        with `name: a` and `name: b` produce two distinct
        `validation_name` *base prefixes* in `ValidationResult` and
        persistence — previously both collapsed to `f"threshold.{ds.name}"`.
  - [ ] Test (programmatic): `qf.threshold_check(name="my_check",
        aggregations=..., rules=...)` builds a config whose
        emitted result names start with `my_check`.

- [ ] **P1.4** — Two typed identity helpers (R2 split: collection
      and validation have different identity shapes — collection
      has no `validation_name`):
  ```python
  # qualifire/core/models.py
  @dataclass(frozen=True)
  class MetricKey:
      """Identity for a CollectionResult: what was measured, where, on
      which segment. Does NOT include validation_name (a single metric
      can be consumed by multiple validations)."""
      dataset_name: str
      metric_name: str
      dimension_value: str = "_default"

  @dataclass(frozen=True)
  class ValidationKey:
      """Identity for a ValidationResult: which check, on which dataset,
      against which metric/segment. Used by suppression — keyed on the
      *persisted full suffixed* validation_name (e.g. 'my_check.row_count'),
      NOT the validation_base_name. Routing and payload filtering use
      validation_base_name (a separate field) — this is the §R6 split."""
      dataset_name: str
      validation_name: str  # full suffixed name as persisted; NOT base
      metric_name: str | None = None
      dimension_value: str = "_default"
  ```
  - [ ] **Decision pin (R6, revised after R2 review):** the two key
        types are distinct because `CollectionResult` lacks
        `validation_name`. A single collected metric can feed
        multiple validations on the same dataset (threshold +
        drift on `row_count`); they share `MetricKey` but produce
        distinct `ValidationKey`s.
  - [ ] Add free functions `metric_key_for(dataset_name, cr) ->
        MetricKey` and `validation_key_for(dataset_name, vr) ->
        ValidationKey` in `qualifire/core/models.py`. Engine,
        dedupe, and bulk pre-fetch (P4.3) call them instead of
        ad-hoc tuples.
  - [ ] **Suppression key.** `should_suppress` (P2.3 / P4.3) uses
        full `ValidationKey` so per-metric and per-dimension
        suppression decisions are independent. Severity is a
        separate parameter; the key does NOT include severity.
  - [ ] **`dimension_value` defaults to `"_default"` everywhere.**
        Both keys treat `"_default"` and the literal JSON-encoded
        dict form as separate values; only at storage read time
        does the COALESCE-NULL-or-`_default` equivalence kick in
        (see P4.3).
  - [ ] Test: both keys are hashable; equal instances are dict-key
        compatible.
  - [ ] Test: two `ValidationResult`s with same dataset+validation
        but different `dimension_value` produce distinct
        `ValidationKey`s.
  - [ ] Test: a single `CollectionResult` consumed by two
        validators yields one `MetricKey` and two `ValidationKey`s
        sharing that `(metric, dim)` pair.

### Phase 1 exit criteria

- All existing tests still pass (no behavior change for callers that
  don't set custom names or use dimensions).
- New regression tests above pass.
- `git grep "f\"slo\\."` and similar return only the helper call
  site — no lingering hardcoded names.

## Phase 2 — Correctness Fixes (depend on Phase 1)

### Steps

- [ ] **P2.1 (Blocking)** — Threshold, historical, and forecast
      validators no longer collapse multi-dimension
      `CollectionResult` rows. Read path also filters by dimension.
  - Evidence: `qualifire/validation/threshold.py:64`,
    `qualifire/validation/historical.py:50`,
    `qualifire/validation/forecast.py:69` all build
    `metrics = {cr.metric_name: cr for cr in collected}`. For a
    metric collected per `(US, EU, APAC)` the dict only retains the
    last entry. **R1 review correctly expanded scope** beyond
    threshold-only.
  - **Write path:**
    - [ ] Replace the `dict[str, CollectionResult]` with
          `dict[tuple[str, str | None], CollectionResult]` keyed by
          `(metric_name, dimension_value)` in *all three* validators.
    - [ ] For each rule, iterate every `dimension_value` matching
          the metric and emit one `ValidationResult` per dimension.
          Each result carries the `dimension_value` field (P1.1).
    - [ ] Aggregation collector at
          `qualifire/collection/aggregation.py:98` already emits
          per-dimension rows — no collector change needed.
  - **Read path (drift / forecast — new in R1):**
    - [ ] Extend `read_metric_history(table_name: str, metric_name:
          str, limit: int = 90, step: str | None = None)` on
          `qualifire/storage/base.py:103` and every backend (sqlite,
          delta, external_catalog, jdbc) with a `dimension_value:
          str = "_default"` parameter. **R3 fix:** the actual
          signature uses `table_name, metric_name, limit, step` —
          earlier plan prose had wrong order/names. Filter
          historical reads by `(table_name, metric_name,
          dimension_value)` (with NULL/`_default` equivalence per
          P1.2 backwards-compat invariant) so per-dimension drift
          compares against the same dimension's history, not all
          dimensions averaged together.
    - [ ] Update call sites at `historical.py:84` and
          `forecast.py:108` to pass the current
          `cr.dimension_value` to the read.
    - [ ] Test: seed history under `(metric=row_count,
          dimension_value="US")` with a stable mean; current run for
          `US` close to mean and current run for `EU` far from
          mean (no EU history) → US passes drift; EU triggers
          `on_missing_history` (whatever its policy is).
          **Currently** the read averages across all dimensions
          and the per-dim verdict is meaningless.
  - **Tests (write path):**
    - [ ] Test (threshold, single-dim): rule on `row_count` against
          a dataset with `dimensions=["region"]` and seeded values
          `{US: 500, EU: 1500, APAC: 9}` and rule
          `error: { min: 100 }` → exactly 3 `ValidationResult`s,
          APAC at ERROR, US/EU at PASS. **Each result's
          `dimension_value` is the JSON-encoded form**:
          `'{"region": "APAC"}'`, `'{"region": "EU"}'`,
          `'{"region": "US"}'` (R2 fix: bare `"EU"` is wrong).
    - [ ] Test (historical/drift, single-dim): same shape, asserting
          per-dim severity tracks per-dim history. Storage history
          row's `dimension_value` matches the JSON form.
    - [ ] Test (forecast/trend, single-dim): same shape, asserting
          per-dim band check uses per-dim history.
    - [ ] Test (no-dim regression): existing single-row
          aggregations produce identical output (one
          `ValidationResult` with `dimension_value="_default"` —
          R2 canonicalization).
    - [ ] Test (notification): grouped notification message renders
          per-dimension rows clearly with the JSON-encoded
          `dimension_value` formatted into a human-readable form
          (decode to `region=APAC` for display purposes).

- [ ] **P2.2 (Should-fix)** — `shape` validator intersects past
      columns instead of unioning.
  - Evidence: `qualifire/validation/isolation_forest.py:142` builds
    the past-column union; `:168` reindexes every past slice with
    that union → `KeyError` on slices missing a column.
    `qualifire/validation/pattern_check.py:165` already carries the
    documented round-3 fix.
  - [ ] Mirror the intersection-then-reindex pattern from
        `pattern_check.py:165-180` in `isolation_forest.py:140-170`.
        Preserve schema-change reporting (new/dropped columns) using
        the union, like pattern does.
  - [ ] Test: shape validator with 3 past slices where slice 2 is
        missing column `currency` → no `KeyError`; metric output
        matches what the intersection-only feature space would
        produce; schema-change report names `currency` as
        inconsistent across slices when `alert_on_schema_change=True`.

- [ ] **P2.3 (Should-fix)** — Per-validation notification routing,
      deduplication, **and payload filtering** (currently
      dataset-granular on all three axes).
  - Evidence: `qualifire/core/engine.py:958` merges all validation
    channels by severity at dataset level; `engine.py:982` uses
    `any()` across all validations for suppression;
    `engine.py:1017` builds the notifier payload from the
    *whole* dataset's results, then `notification/base.py:125-128`
    renders rows for any vr where `vr.severity >= severity`. A
    warning from check A routes to check B's channels;
    `suppress_repeat_alerts=False` on A can be overridden by
    another's default; channel-specific groups render results from
    *unrelated* checks that happened to fail.
  - [ ] **Routing.** Refactor `_send_grouped_notifications` to
        group by `(channel, severity, validation_name)`, not just
        `(channel, severity)` per dataset.
  - [ ] **Suppression.** Use `ValidationKey` (P1.4) — keyed on
        `(dataset_name, validation_name, metric_name,
        dimension_value)` — for per-validation, per-metric, per-dim
        dedup. Suppress flag is read per-validation, not via
        `any()`.
  - [ ] **Payload filtering (R1+R2 finding).** When sending a group
        for `(channel, severity, validation_name=X)`, both the
        synthetic summary AND the `datasets=[...]` argument passed
        to `notifier.send` must carry filtered copies of the
        underlying `DatasetResult`s — only validation results
        belonging to validation `X` survive. **R2 fix:** today
        `notification/base.py:56-60` ignores the summary when
        `datasets=` is non-None and renders rows directly from the
        original (unfiltered) datasets via `:151-157`. So filtering
        only the summary is insufficient; the engine must build
        *new* `DatasetResult` instances with `validation_results`
        already filtered, and pass those in `datasets=`.
  - [ ] **Prefix-collision-safe filter (R2+R3 finding).** Don't
        parse identity from `validation_name`. Use the new
        `validation_base_name` field on `ValidationResult` (P1.1)
        and filter by **exact equality**: `vr.validation_base_name
        == base_X`. This survives all dotted-name corner cases
        (base `"a"` vs. `"a.foo"`, base `"ab"`, etc.) because the
        base name is a separate, non-overlapping field.
  - [ ] **Routing vs. suppression scope (R4 finding).** The
        `validation_base_name` field is used **only** for routing
        and payload filtering (in-memory engine-side decisions).
        It is **not** used for suppression / `ValidationKey`. The
        suppression key uses the **persisted** `validation_name`
        (the full suffixed reporting name like
        `"my_check.row_count"`) plus `metric_name` and
        `dimension_value` (per-rule, per-dim suppression
        granularity). This avoids the §R5 "no DDL changes"
        constraint — storage has no base-name column. Validators
        like `ThresholdValidator` already produce one
        `ValidationResult` per `(metric, dim)` triplet; the
        per-validation-row identity is its full suffixed
        `validation_name` + `metric_name` + `dimension_value`.
  - [ ] **Cross-dataset grouping is preserved**: one Slack message
        for two datasets on the same `(channel, severity,
        validation_name)`. Cross-validation grouping is what we're
        deliberately removing.
  - [ ] Test (cross-routing): two threshold checks on the same
        dataset, A routes errors to `slack_a`, B routes errors to
        `slack_b`. A produces ERROR, B produces PASS. → exactly one
        notification on `slack_a`; nothing on `slack_b`. Currently
        the merged routing sends to both.
  - [ ] Test (payload filter): two threshold checks A and B on the
        same dataset, both ERROR, A routes to `slack_a` and B
        routes to `slack_b`. → `slack_a` message contains only A's
        ERROR rows; `slack_b` message contains only B's. Currently
        both messages render both checks' ERRORs.
  - [ ] Test (per-check suppress): A=`suppress_repeat_alerts=False`,
        B=`suppress_repeat_alerts=True`, both have a prior matching
        history row. → A re-fires; B is suppressed. Currently `any()`
        bug suppresses both or neither depending on order.
  - [ ] Test (per-dim suppress, R3-canonical): single threshold
        over `dimensions=["region"]`. Seed prior history with
        `dimension_value='{"region": "US"}'` only; EU has no
        prior row. → US suppressed, EU notification fires.
        Currently the per-validation read collapses dimensions.
  - [ ] Test (cross-dataset payload-filter, R3 finding): two
        datasets each emit ERROR for validation A and B; A
        routes to `slack_a`, B to `slack_b`. Both datasets
        share both channels. → `slack_a` receives ONE grouped
        message rendering A's rows from both datasets only;
        `slack_b` similarly for B. Verifies that the filtered
        `datasets=` argument flows through grouped formatting,
        not just the synthetic summary.

- [ ] **P2.4 (Should-fix)** — WAP `publish` failure path leaks
      staging (audit failure path is already covered).
  - **R1 review correction:** the original P2.4 statement was
    factually wrong on two counts:
    1. `_run_dataset` already swallows per-dataset exceptions
       (`engine.py:178-193`), so a *naive* audit-raises-leaks-staging
       scenario doesn't exist for the in-band path. The real gap is
       elsewhere.
    2. `wap/pattern.py:140-152` already uses two independent try
       blocks for unpersist + drop. The "ordering" claim was wrong.
  - **Real gap:** `WAPExecutor.publish` (`wap/pattern.py:121-133`)
    runs `INSERT INTO target SELECT * FROM staging` before
    `_cleanup`. If that INSERT raises (target table missing,
    permission denied, partition mismatch), `_cleanup` never runs
    and staging leaks.
  - **Real gap (audit, narrower):** `_run_wap` at
    `engine.py:1164-1171` calls `_run_dataset(staging_ds_config)`,
    which catches *validation*-level exceptions but not
    materialization-level failures inside the WAP staging dataset
    (e.g. `_cache_table` raise, `_drop_temp_view` failure that
    propagates). If `_run_dataset` itself raises, control returns
    to `_run_wap` without the publish/rollback decision and
    staging leaks.
  - [ ] Wrap `_run_wap` body in `try / except / finally`:
        ```python
        try:
            ds_result = _run_dataset(...)
            if ds_result.has_errors:
                executor.rollback()
            else:
                executor.publish()
        except Exception:
            try:
                executor.rollback()
            except Exception as cleanup_err:
                logger.error("WAP cleanup failed: %s", cleanup_err)
                # Do NOT re-raise — primary exception must propagate.
            raise
        ```
        **R2 fix.** Cleanup failure in the `except` block must NOT
        replace the primary exception. Catch and log, then `raise`
        the original.
  - [ ] In `WAPExecutor.publish`, wrap the INSERT in a try and call
        `_cleanup()` from a `finally` so a publish failure still
        drops staging. Same primary-exception preservation rule:
        if `_cleanup` raises during the finally, log and continue
        (Python's exception machinery will preserve the original
        if the `finally` finishes without re-raising).
  - [ ] **Cleanup ownership pin (R4 finding).** `_run_wap`'s outer
        `except Exception: rollback(); raise` and `publish()`'s
        new `finally: _cleanup()` overlap. Pin: `publish()` /
        `rollback()` are the **only** code paths that call
        `_cleanup`. `_run_wap`'s outer except calls
        `executor.rollback()` (which itself calls `_cleanup`)
        once. Idempotency: `_cleanup` checks `self._cached_df is
        None` before unpersist (`wap/pattern.py:142`) — making it
        callable twice without error. Add an explicit
        `_already_cleaned: bool` flag on `WAPExecutor` to make
        double-call a no-op for the staging-table drop too.
  - [ ] **Cleanup-only-failure surfacing (R5+R6 finding).** When
        the publish INSERT *succeeds* but `_cleanup` raises in
        the `finally`, silently swallowing the cleanup error
        leaks staging without telling anyone. Pin:
        - If a primary exception is in flight (INSERT failed),
          log the cleanup error and let the primary propagate
          (Python's exception chain preserves both via
          `__context__`).
        - If no primary exception (INSERT succeeded), the
          cleanup failure is the only event. **R6+R7 fix.**
          `WAPExecutor.publish()` cannot directly append to
          `QualifireResult.engine_warnings` — it has no
          reference to the result. Instead, `publish()` and
          `rollback()` accumulate cleanup warnings on a
          per-executor `self.cleanup_warnings:
          list[ValidationResult]` field.
        - **Transport pin (R7).** Change
          `engine._run_wap(ds_config) -> DatasetResult` to
          `engine._run_wap(ds_config) -> tuple[DatasetResult,
          list[ValidationResult]]` (the second element is
          drained from `executor.cleanup_warnings`). Both
          dispatch sites in `engine.run` — sequential
          (`engine.py:163`) and parallel via futures
          (`engine.py:171`) — destructure the tuple, append
          the `DatasetResult` to `result.datasets`, and
          extend `result.engine_warnings` with the warnings
          list. The `_run_dataset` path stays as
          `_run_dataset(ds_config) -> DatasetResult` (no
          tuple), so non-WAP datasets are unaffected.
        - The merge happens BEFORE the suppression pre-fetch
          (P4.1 step 2) so the engine-warning keys are
          included in the snapshot collection.
  - [ ] Test (cleanup-only-failure): publish INSERT succeeds,
        monkeypatch `backend.drop_table` (called from
        `_cleanup`) to raise → result returns successfully but
        `result.engine_warnings` contains a
        `validation_base_name="qualifire.wap_cleanup"` row;
        `executor.cleanup_warnings` is non-empty before
        `_run_wap` drains it.
  - [ ] Test (cleanup-masks-original — rollback path):
        monkeypatch `executor.rollback` to raise on a failed
        publish → assert the publish exception (not the rollback
        exception) is what propagates to the caller. The rollback
        failure is logged.
  - [ ] Test (cleanup-masks-original — publish path, R3 fix):
        the `_cleanup()` in `WAPExecutor.publish`'s `finally`
        block must not mask the INSERT failure. Monkeypatch
        `backend.execute_sql` to raise on the publish INSERT
        AND `backend.drop_table` (cleanup) to also raise →
        assert the INSERT exception propagates; the cleanup
        failure is logged.
  - [ ] Test (publish-mid-flight): monkeypatch
        `backend.execute_sql` so the published INSERT raises →
        assert staging table is dropped (or, in cached mode, temp
        view dropped + cache unpersisted) AND the
        `QualifireValidationError`/raised exception still
        propagates.
  - [ ] Test (run_dataset raise): monkeypatch
        `engine._run_dataset` to raise on a WAP-driven dataset →
        staging gone, original `target_table` untouched, the
        exception bubbles out of `engine.run`.

### Phase 2 exit criteria

- All four correctness fixes have at least one failing-before /
  passing-after regression test.
- `pytest tests/test_validation tests/test_engine tests/test_wap`
  green.

## Phase 3 — Public-API Consistency

### Steps

- [ ] **P3.1 (Blocking)** — Fix or remove `validate(notify=)` and
      `write_audit_publish(notify=)`.
  - Evidence: `qualifire/api.py:353,521` accept the arg and the
    docstring at `api.py:378` advertises it; the constructed
    `DatasetConfig` (`api.py:414-422`, `:555-563`) never threads
    it; `qualifire/core/config.py:625-639` has no `notify` field
    on `DatasetConfig`.
  - **Decision pin:** *thread the parameter through* rather than
    delete it. The user-facing semantic ("set default routing for
    every validation in this call unless the validation has its
    own `notify`") is a real ergonomics win, and the docstring
    already documents it. Removing would be a breaking change
    relative to the *documented* behavior.
  - [ ] On `validate(notify=...)`: when `notify` is provided, fan it
        out onto every entry in `validations=[...]` whose own
        `notify` is the empty default. Per-validation `notify` wins
        over the dataset-level default — never silently overwrite.
  - [ ] **Non-mutating fanout (R2 fix).** Use
        `vc.model_copy(update={"notify": ...})` to produce a new
        config; never mutate the caller-owned `validations` list
        elements in place. Reusing the same builder config in a
        later call without `notify=` must NOT carry over the
        previous routing.
  - [ ] **Explicit-empty notify (R3+R4 fix).** A caller may want
        to pass `notify={}` (empty dict) to mean "this validation
        explicitly opts out of dataset-level fanout." Distinguish
        unset from explicit-empty.
  - [ ] **R4 finding.** The original `model_fields_set` approach
        fails because every builder passes `notify=NotifyConfig(
        **(notify or {}))` unconditionally — `model_fields_set`
        always contains `notify`. Replace with a builder-level
        change: builders take `notify: dict[str, list[str]] |
        None = None`. When `notify is None`, do **not** pass the
        field to the config constructor at all (use kwargs
        spread). The config then defaults to `NotifyConfig()` and
        `model_fields_set` legitimately excludes `notify`. When
        the caller passes `notify={}`, the builder sets
        `notify=NotifyConfig()` explicitly — `model_fields_set`
        contains `notify`, and the engine treats this as
        explicit-empty (inheritance-blocking).
  - [ ] Test (model_fields_set explicitness): build with
        `notify=None` → resulting config's
        `model_fields_set` does NOT contain `notify`. Build with
        `notify={}` → `model_fields_set` contains `notify`, and
        the inherited dataset-level fanout does NOT apply.
  - [ ] Same for `write_audit_publish(notify=...)`.
  - [ ] Test (reuse, non-mutation): build one
        `qf.threshold_check(...)` config, call
        `qf.validate(..., notify={"error": ["a"]}, validations=[cfg])`,
        then call `qf.validate(..., validations=[cfg])` *without*
        `notify`. Second call must NOT route to channel `a`.
        Currently this test would expose mutation if the fanout
        modified the original.
  - [ ] Test (positive): `qf.validate(table=..., notify={"error":
        ["slack"]})` with one builder that doesn't set `notify` →
        a `NotificationResult` lands on `slack`. **Currently**
        nothing fires — this is the regression we're closing.
  - [ ] Test (precedence): builder-level `notify` wins; the
        dataset-level value does NOT overwrite it.
  - [ ] Test (None): `validate(...)` without `notify=` is unchanged
        (no fanout, no surprise).

- [ ] **P3.2 (Blocking)** — Replace WAP-DataFrame monkey-patch with
      proper config plumbing.
  - Evidence: `qualifire/api.py:574-605` defines `patched_run_wap`
    and assigns it to `engine._run_wap`. Inverts layering.
  - [ ] Add `df: Any = Field(default=None, exclude=True, repr=False)`
        to `WAPConfig` at `qualifire/core/config.py:616-619` AND
        set `model_config = {"arbitrary_types_allowed": True,
        "validate_assignment": True}` on `WAPConfig` (R3+R4 fix:
        `Any` field with a Spark / pandas DataFrame value can
        fail Pydantic schema-build without `arbitrary_types_allowed`;
        `validate_assignment=True` ensures the §R9 mutual-exclusion
        validator fires when callers attach `df` post-construction
        — `DatasetConfig:626` already has `arbitrary_types_allowed`
        for the same reason).
  - [ ] Test (R4 mutual-exclusion via assignment): construct
        `WAPConfig(write_sql="SELECT 1")`, then assign
        `cfg.df = some_df` → `pydantic.ValidationError` raised
        because both fields would be set. With
        `validate_assignment=False`, the assignment would
        silently violate §R9.
  - [ ] In `Qualifire.write_audit_publish(df=...)` (`api.py:574-605`),
        attach `df` to `WAPConfig.df` and remove the patched
        function entirely.
  - [ ] In `engine._run_wap` (`engine.py:1141-1173`), if
        `wap_config.df is not None`, instantiate `WAPExecutor` with
        `df=wap_config.df` AND **pass `write_sql=None`** explicitly
        (R2 fix: `WAPExecutor.write` at `wap/pattern.py:78` checks
        `if self.write_sql:` first and silently drops the df if
        both are set). The mutual-exclusion validator on `WAPConfig`
        (§R9) prevents both being set in valid configs, but the
        engine call site must still avoid passing both — defensive
        coding that doesn't rely on the validator alone.
  - [ ] Test: `qf.write_audit_publish(df=my_df, target_table=...,
        validations=[...])` end-to-end: staging populated from
        `df`, audit runs, publish writes to target, no monkey-patch
        on `engine._run_wap`. Assert
        `"_run_wap" not in engine.__dict__` (R1 review fix:
        bound-method `is` comparison is false even for an
        un-patched engine, so the original test assertion would
        have failed even after the correct fix).
  - [ ] Test (rollback path): same scenario but with a failing
        validation → staging dropped, target untouched.

### Phase 3 exit criteria

- `git grep "patched_run_wap\|engine._run_wap = "` returns nothing.
- `validate(notify=...)` test fails on `main` and passes on the branch.

## Phase 4 — Operational and Hygiene Fixes

### Steps

- [ ] **P4.1 (Should-fix)** — `engine._persist_results` surfaces
      write failures.
  - Evidence: `qualifire/core/engine.py:1135-1139` swallows all
    `storage.write_results(rows)` exceptions with `logger.error`.
    Drift / forecast / pattern silently lose their baseline rows;
    next runs cold-start.
  - **R1 + R2 sequencing fix.** Today `engine.run` runs
    `_send_grouped_notifications` (line 198) BEFORE
    `_persist_results` (line 201). The naive resequence
    (persist-then-notify) introduces a new bug: notifications
    `should_suppress` reads see the row just written for the
    current run and treat every alert as a duplicate (R2 finding,
    confirmed by inspecting `sqlite_storage.py:189-206` — no
    `run_id` exclusion).
  - [ ] **Resequence `engine.run` with pre-fetched suppression
        snapshot:**
        1. Run datasets (collect → validate).
        2. **Pre-fetch suppression snapshot.** Collect all
           `ValidationKey`s (P1.4) from three sources:
           (i) `result.datasets[*].validation_results`,
           (ii) `result.engine_warnings` already accumulated
           (R8 fix — `qualifire.wap_cleanup` rows merged from
           `_run_wap`'s tuple transport land here BEFORE
           pre-fetch), and
           (iii) the well-known `engine_warning_keys` — a
           fixed set covering warnings that haven't been
           emitted yet but might be: currently
           `ValidationKey(dataset_name="qualifire.engine",
           validation_name="qualifire.persistence",
           metric_name=None, dimension_value="_default")` plus
           `qualifire.suppression_read` (R5).
           Call
           `storage.read_validation_history_bulk(all_keys,
           limit=1)` ONCE. Store as `_suppression_snapshot` on
           the engine. Snapshot represents *pre-run* state;
           subsequent writes can't poison it.
        3. Try `_persist_data_rows(result)` — persist validation
           and collection rows. On failure, append a synthetic
           `ValidationResult(validation_name="qualifire.persistence",
           validation_type="engine", severity=Severity.WARNING)`
           to `result.engine_warnings`.
        4. `_send_grouped_notifications(result, snapshot=
           _suppression_snapshot)` — uses the pre-fetched
           snapshot, NOT a fresh read. The persistence-warning
           is routed via `engine_notify` (§R10).
        5. `_persist_notification_rows(result.notifications)` —
           persist the notification audit trail. If THIS step
           fails, log + emit to stderr; do not re-attempt
           notifications (would loop).
  - [ ] **Engine warning shape pinned (R2+R3 fix).** Add a new
        field `engine_warnings: list[ValidationResult] = field(
        default_factory=list)` to `QualifireResult` at
        `qualifire/core/models.py:99-124`. Extend
        `QualifireResult.has_warnings` to `any(ds.has_warnings for
        ds in self.datasets) or bool(self.engine_warnings)`. NOT
        a member of any `DatasetResult` — engine-level events
        don't belong to a dataset.
  - [ ] **Engine warning routing — pinned.** R3 finding: the
        `engine_notify` field by itself doesn't tell
        `_send_grouped_notifications` how to format the warning,
        because that function iterates over
        `self.config.datasets`. Pin: when `engine_warnings` is
        non-empty AND `config.engine_notify` is set, the engine
        builds a *synthetic* `DatasetResult(
        dataset_name="qualifire.engine", table=None,
        validation_results=engine_warnings)` and routes it through
        the same notifier path used for regular datasets.
  - [ ] **Engine warnings ARE persisted (R9+R10 fix).** Engine
        warnings persist as `record_type="validation"` under
        `dataset_name="qualifire.engine"`, so the suppression
        contract works on subsequent runs.
  - [ ] **Two-pass persistence (R10 fix — chicken/egg).** A
        `qualifire.persistence` warning is BORN of a failed
        `_persist_data_rows`, so it cannot ride the same write.
        Pin the sequencing:
        - Pass A — `_persist_data_rows(result.datasets +
          result.engine_warnings_pre_persist)`. The
          pre-persist set covers `qualifire.wap_cleanup`,
          `qualifire.suppression_read` (R5), and any future
          warnings born BEFORE the data write. On failure,
          append `qualifire.persistence` to
          `result.engine_warnings`.
        - Pass B — best-effort warning-only retry. If Pass A
          failed, attempt
          `storage.write_results([persistence_warning_row])`
          in its own `try/except`. If Pass B also fails, log
          and proceed; the in-memory `result.engine_warnings`
          still routes via notifications, just without
          history-row persistence for THIS run. Future runs
          may also lack the row and re-notify; that's
          accepted graceful-degrade.
        - Pass A's failure does not abort the run.
          Notifications still fire (snapshot was pre-fetched
          before persistence per P4.1 step 2). The
          observability gap is signaled via the
          `qualifire.persistence` warning notification, not
          buried.
  - [ ] **R10 test refinement.** The two-run suppression test
        must work in two flavors:
        - Wap-cleanup or suppression-read warnings (born
          before persistence): Pass A persists them; second
          run reads history and suppresses if the warning
          repeats. Test passes.
        - Persistence-failure warnings: Pass A failed by
          definition; Pass B is best-effort. Test seeds the
          prior `qualifire.persistence` row directly into
          the system table (using `storage.write_results` on
          a healthy storage) and asserts the next run's
          (failing) Pass A still routes the notification but
          suppression-snapshot finds the seeded row when
          requested — proving the read-side contract works.
          The B-pass write itself is exercised with a
          separate test that checks Pass B happens-or-fails
          gracefully.
  - [ ] Suppression for engine warnings is keyed on
        `ValidationKey(dataset_name="qualifire.engine",
        validation_name=<warning_kind>, metric_name=None,
        dimension_value="_default")`.
  - [ ] Test (two-run engine-warning suppression): run with
        forced persistence failure → first run notifies on
        `qualifire.persistence`. Second run (storage now
        healthy) reads history, sees the prior
        `qualifire.persistence` row, suppresses the duplicate
        if it again fails. Without R9 persistence, the
        second run cold-starts and re-notifies even on
        repeat-failure runs.
  - [ ] Test: persistence failure with `engine_notify=
        {"warning": ["slack"]}` configured AND a prior matching
        `qualifire.persistence` row in history → suppression hit;
        no notification fires. Without the prior row → one
        notification fires.
  - [ ] Decision: WARNING (not ERROR). Validation work itself
        succeeded — this is observability degradation, not a
        validation failure. Operators can route the warning if
        they want immediate signal.
  - [ ] **Routing of the persistence warning.** It needs a routing
        contract since it isn't tied to a user-defined `notify=`
        block. Pin: route to a new top-level `engine_notify`
        config field on `QualifireConfig` (default empty → no
        route). Operators opt in.
  - [ ] Test: monkeypatch `storage.write_results` to raise →
        `result.has_warnings is True`; the synthetic
        `ValidationResult` with `validation_name="qualifire.persistence"`
        is present in the returned result; the validation results
        that ran *before* the persistence attempt are unchanged.
  - [ ] Test: with `engine_notify={"warning": ["slack"]}`
        configured, the persistence-failure scenario produces
        exactly one notification on `slack` whose payload names
        the engine-level warning.
  - [ ] Test: notification persistence failure (step 4) does NOT
        raise back to the caller; the validation result is
        returned unchanged.

- [ ] **P4.2 (Should-fix)** — Standardize missing-PySpark behavior.
  - Evidence: three behaviors today —
    `qualifire/validation/pattern_check.py:34-56` raises with install
    text; `qualifire/core/engine.py:628-642` (`_cache_table`)
    silently returns `None` (catches **broad `Exception`**, not
    just `ImportError` — R1 finding); `qualifire/wap/pattern.py:116-119`
    silently falls back with a `logger.warning`.
  - **R1 distinction.** Two failure modes are conflated today:
    1. **PySpark not installed** — `ImportError` on the import.
       This is a configuration error; the caller asked for
       Spark caching but doesn't have Spark. Raise.
    2. **Spark caching itself failed** — `Exception` (memory
       pressure, executor lost, lineage too long). This is a
       runtime degradation; current swallow-with-warn is
       acceptable since the validation can still run uncached.
       Keep the swallow.
  - [ ] In `_cache_table` (`engine.py:628-642`): split the
        `try / except` so `ImportError` paths raise an explicit
        `RuntimeError("PySpark is required for cache=True; install
        with: pip install 'qualifire[spark]'")` **only when the
        backend is Spark-flavored** — i.e., when the engine
        called `_cache_table` because `ds_config.cache=True` AND
        the backend is Spark-capable. **R4 reconciliation with
        P4.5.** PandasBackend also supports `cache=True`
        semantically (it's a no-op since pandas is already
        in-memory), so passing `cache=True` against
        PandasBackend should not raise — `_cache_table` returns
        `None` and the validation continues uncached. Only the
        Spark-needs-PySpark branch raises.
  - [ ] **Discriminator pin (R5 finding).** "Backend is
        Spark-capable" is decided by
        `getattr(self.backend, "spark", None) is not None` —
        the same probe used in
        `qualifire/api.py:176-184` to gate JDBC backend
        selection. Avoid `isinstance(self.backend, SparkBackend)`
        — third-party backends that follow the Protocol and
        expose `.spark` should also be treated as
        Spark-capable. PandasBackend has no `.spark` attr,
        so this check correctly excludes it.
  - [ ] In `_write_df_cached` (`wap/pattern.py:100-119`): same
        ImportError-vs-Exception split. The non-cached fallback
        on a Spark error is still a reasonable degradation; the
        ImportError fallback is misleading and is replaced with
        an explicit raise.
  - [ ] **Add a `[spark]` extra to `pyproject.toml`** so the
        install-text recommendation actually works. (Originally
        listed in §R3 as deferred Polish — promoted to P4.2 because
        the error message references it.)
  - [ ] Test (R5 split — Spark + no PySpark): backend is
        `SparkBackend` (or any backend with a `.spark` attr),
        `cache=True`, PySpark not importable → explicit
        `RuntimeError` with the install hint.
  - [ ] Test (R5 split — Pandas + no PySpark): backend is
        `PandasBackend`, `cache=True`, PySpark not importable →
        no raise; `_cache_table` returns `None`; validation
        continues uncached. Pandas data is already in memory;
        cache is semantically a no-op.
  - [ ] Test: `cache=True` with PySpark installed but `df.persist()`
        raising a Spark error → swallow + warn + None return,
        validation continues uncached.

- [ ] **P4.3 (Should-fix)** — `should_suppress` bulk pre-fetch +
      full `ValidationKey` (per-metric, per-dim) dedup.
  - Evidence: `qualifire/notification/base.py:82-106` issues one
    `read_validation_history(limit=1)` per
    `(dataset, validation, severity)` tuple from
    `engine._send_grouped_notifications` (`engine.py:986`). At
    `dataset_parallelism × validation_parallelism × N`, this is
    N synchronous round-trips. Today's read also keys only on
    `(dataset_name, validation_name)` (sqlite_storage.py:189
    and equivalents), so per-metric and per-dim are squashed
    onto the same history row.
  - [ ] **Bulk fetch.** Add `read_validation_history_bulk(
        keys: list[ValidationKey], limit: int = 1)` to
        `qualifire/storage/base.py:SystemTableStorage` returning
        `dict[ValidationKey, list[dict]]`. Wire on every backend
        (sqlite, delta, external_catalog, jdbc).
  - [ ] **Per-backend SQL pinning (R1+R2 findings).** Each
        backend has a different dialect; NULL-safe matching and
        per-key `LIMIT` semantics differ. Pin one approach per
        backend:
        - **sqlite** (`storage/sqlite_storage.py`): use the
          `temp-view-of-keys` JOIN approach uniformly. Build a
          temp table from the input keys, then JOIN on:
          ```sql
          (t.metric_name = k.metric_name
              OR (t.metric_name IS NULL AND k.metric_name IS NULL))
            AND COALESCE(t.dimension_value, '_default') =
                COALESCE(k.dimension_value, '_default')
          ```
          **R3 fix.** Empty-string-as-NULL-sentinel for
          `metric_name` is wrong — empty string is a *legal*
          (if odd) metric name. Use explicit `IS NULL` for
          `metric_name`. The `dimension_value` COALESCE form is
          fine because `'_default'` is a reserved sentinel.
          Per-key `LIMIT 1` via `ROW_NUMBER() OVER (PARTITION BY
          dataset_name, validation_name, metric_name,
          COALESCE(dimension_value, '_default') ORDER BY
          run_timestamp DESC) <= limit`.
        - **delta** (`storage/delta_storage.py`): same approach,
          Spark SQL syntax. Spark supports `<=>` (NULL-safe
          equality); use that instead of COALESCE if simpler.
        - **external_catalog** (`storage/external_catalog.py`):
          same JOIN approach. AIDP catalog runs Spark SQL
          underneath, so `<=>` is available.
        - **jdbc** (`storage/jdbc_storage.py`): the Spark JDBC
          reader has predicate-pushdown limits; the temp-view
          JOIN runs server-side via the Spark query planner.
          Pin the SQL form in the implementation; do not rely
          on `<=>` (Oracle/Postgres don't support it) — use
          `COALESCE` form.
        Each backend's bulk method has its own targeted unit
        test asserting both NULL-key and non-NULL-key match.
  - [ ] **Key shape after R3+R4.** `ValidationKey` (P1.4) has
        `dimension_value: str = "_default"` (no `None`).
        `metric_name: str | None = None` (still optional). The
        SQL predicate uses **explicit IS NULL for metric_name**
        (R4 fix; empty-string sentinel is wrong because empty
        strings are legal values) and **COALESCE for
        dimension_value** (sentinel `'_default'` is reserved):
        ```sql
        (metric_name = ? OR (metric_name IS NULL AND ? IS NULL))
          AND COALESCE(dimension_value, '_default') = ?
        ```
        Same shape across all four backends.
  - [ ] **Backwards compat for legacy NULL.** Existing
        validation rows in the system table have
        `dimension_value IS NULL`. The COALESCE form treats them
        as `"_default"` — equivalent to new non-dimensioned
        rows. `metric_name IS NULL` legacy rows match callers
        passing `metric_name=None`.
  - [ ] **External callers of `read_validation_history`** see
        signature additions, not behavior changes. **R4 fix.**
        Existing two-arg calls `(dataset_name, validation_name)`
        keep the original wildcard semantics — they match every
        row sharing those two fields, regardless of
        `metric_name` / `dimension_value`. The new strict
        per-key matching is only used by the bulk path
        (`read_validation_history_bulk`), which is engine-internal
        and always passes a full `ValidationKey`. The single-key
        API also gains optional kwargs `metric_name=None`,
        `dimension_value=None` that activate the strict
        predicate when explicitly set; default behavior
        unchanged. This preserves §R5's "external callers see
        signature additions, not behavior changes" guarantee.
        Documented in the storage backend module docstrings.
  - [ ] **Pre-fetch ordering (R3 fix).** The pre-fetch lives in
        `engine.run` BEFORE `_persist_data_rows`, NOT inside
        `_send_grouped_notifications`. (P4.1 already pinned this
        sequencing.) The bulk dict is threaded into
        `_send_grouped_notifications(result, snapshot=...)` as a
        parameter, and `should_suppress(snapshot, key, severity)`
        consumes the dict directly — no fresh storage reads
        during notification routing.
  - [ ] **Pre-fetch failure handling (R5+R11 fix).**
        Two cases, each with distinct semantics:
        - **`self.storage is None`** (storage optional; the
          `Qualifire(...)` caller did not configure
          `system_table=`): use an empty snapshot and emit
          NO `qualifire.suppression_read` warning. Storage
          being absent is a configuration choice, not an
          observability failure. This mirrors the existing
          `should_suppress` no-storage path
          (`qualifire/notification/base.py:93-94` returns
          `False` silently when storage is None). R11 fix.
        - **Storage configured but read fails**: wrap the
          bulk-read in `try/except Exception` at the
          engine.run level. Log + emit an
          `engine_warning(validation_base_name=
          "qualifire.suppression_read", severity=WARNING,
          message="Suppression history unavailable; alerts
          may re-fire")` and proceed with an *empty*
          snapshot. Empty snapshot means `should_suppress`
          returns False for every key — better to
          over-notify than to silently abort.
  - [ ] Test (pre-fetch failure): monkeypatch
        `storage.read_validation_history_bulk` to raise →
        `result.engine_warnings` contains the
        `qualifire.suppression_read` row; alerts that would have
        been suppressed by prior history all fire (not
        suppressed); the run completes successfully overall.
  - [ ] Test (no-system-table run, R11): instantiate
        `Qualifire(backend=..., system_table=None)`, run a
        validation that produces a WARNING →
        `result.engine_warnings` is empty (no
        `qualifire.suppression_read` row), the WARNING
        notification fires (no suppression possible),
        run completes successfully.
  - [ ] Test (correctness, single-validation): bulk pre-fetch +
        per-key dedup produces identical suppression decisions
        vs. the old per-call version when there is no per-metric
        / per-dim differentiation.
  - [ ] Test (correctness, per-metric): two threshold rules
        emit `(metric=row_count, dim="_default")` and
        `(metric=null_pct, dim="_default")`. Seed history
        matching only `row_count`. → row_count suppressed;
        null_pct notifies. Today both are suppressed under the
        coarse key.
  - [ ] Test (correctness, per-dim, R3-canonical): one
        threshold rule across `dimensions=["region"]` emits
        per-dim results, each with
        `dimension_value='{"region": "US"}'`,
        `'{"region": "EU"}'`, `'{"region": "APAC"}'`. Seed
        history matching only the US JSON-encoded form. →
        US suppressed; EU/APAC notify.
  - [ ] Test (performance, deterministic): mock storage
        records every read; assert the engine performs at most
        one bulk read per run, regardless of validation count.

- [ ] **P4.4 (Should-fix)** — `JDBCStorage` is Spark-coupled by
      design. Document, do not rename.
  - Evidence: `qualifire/storage/jdbc_storage.py` is 772 LOC,
    requires a `SparkSession` (`api.py:176-184`), and the
    README presents it as backend-agnostic at `README.md:441-447`.
  - **Decision pin (R3):** *do not rename* the backend identifier.
    The rename (`jdbc` → `spark_jdbc`) is a breaking change for
    every YAML file in the field. Alternative: edit the docs to
    name the Spark dependency explicitly.
  - [ ] Add a paragraph to `qualifire/storage/jdbc_storage.py`
        module docstring naming the Spark dependency.
  - [ ] Edit `README.md:441-447` JDBC row Notes to: "Requires
        SparkBackend (uses Spark JDBC reader/writer); see
        [docs/configuration.md](docs/configuration.md#jdbc-system-table)."
  - [ ] Edit `docs/configuration.md` JDBC section to lead with the
        Spark requirement.
  - [ ] No code changes; no test changes. (Existing
        `Qualifire._init_storage:176-184` already raises a clear
        `ValueError` on Pandas backend — that's the runtime
        guard; this step makes the docs match.)

- [ ] **P4.5 (Should-fix)** — Lift `register_table` /
      `drop_temp_view` into the `Backend` Protocol; remove
      `hasattr` branches; **distinguish temp-view drop from
      permanent-table drop** (R2 fix).
  - Evidence: `qualifire/core/engine.py:513,535,569` switches on
    `hasattr(self.backend, "register_table")`. The `Backend`
    Protocol at `qualifire/backends/base.py:8-55` does not declare
    `register_table`. `SparkBackend.drop_table` runs `DROP TABLE
    IF EXISTS` (`spark_backend.py:102-103`) — wrong for temp
    views. The current engine `_drop_temp_view` at `engine.py:566-577`
    branches: Pandas calls `drop_table` (works because Pandas
    conflates the two), Spark runs raw `DROP VIEW`.
  - [ ] Add to `Backend` Protocol (`backends/base.py`):
        ```python
        def register_table(self, name: str, df: Any) -> None: ...
        def drop_temp_view(self, name: str) -> None: ...
        ```
        (`drop_table` for permanent tables stays on the Protocol;
        WAP cleanup uses it.)
  - [ ] `SparkBackend.register_table(name, df)`:
        `df.createOrReplaceTempView(name)`.
  - [ ] `SparkBackend.drop_temp_view(name)`:
        `self.spark.sql(f"DROP VIEW IF EXISTS \`{name}\`")`. Note
        the backtick quoting — view names from the engine have
        underscores only, but Spark catalog rules require quoted
        identifiers for some characters.
  - [ ] `PandasBackend.drop_temp_view(name)`: same as
        `drop_table` (removes from internal `_tables` dict). For
        PandasBackend, temp views and tables are the same dict
        entry — symmetry preserved.
  - [ ] In `engine._drop_temp_view` (`engine.py:566-577`),
        replace the `hasattr` branch with
        `self.backend.drop_temp_view(view_name)` unconditionally.
        Engine no longer issues raw SQL.
  - [ ] WAP staging-table cleanup (`wap/pattern.py:154`)
        continues to call `backend.drop_table` (permanent table
        in non-cached mode) — unchanged.
  - [ ] **WAP cached-mode wiring (R3 finding).** Today
        `WAPExecutor._write_df_cached` (`wap/pattern.py:100-119`)
        calls `df.createOrReplaceTempView(view_name)` directly —
        Spark-specific. With register_table on the Backend
        Protocol, replace with
        `self.backend.register_table(view_name, df)`. Cleanup at
        `wap/pattern.py:148-152` runs raw `DROP VIEW IF EXISTS`
        SQL — replace with `self.backend.drop_temp_view(
        self.staging_table)` after the cached check. Net result:
        WAP no longer carries Spark-specific SQL, and a
        Pandas-backed WAP run actually works through the cached
        path (today it falls into the ImportError fallback at
        `wap/pattern.py:116`).
  - [ ] Test: WAP-cached path on PandasBackend → staging
        registered as a temp view (in `_tables` dict),
        validations run, publish moves to target, drop_temp_view
        clears the entry. Currently this path doesn't work on
        Pandas at all.
  - [ ] Test: existing materialization tests still pass on both
        backends.
  - [ ] Test: a third hypothetical backend that implements only
        the Protocol (mock) materializes correctly without any
        Spark/Pandas-specific check.
  - [ ] Test: WAP non-cached path drops the staging *table*
        (DROP TABLE on Spark); WAP cached path drops the staging
        *view* (DROP VIEW on Spark) — separate paths, separate
        backend methods.

- [ ] **P4.6 (Should-fix)** — Reconcile `requirements.txt` with
      tiered-install docs. Resolve the
      "core-includes-threshold/historical" promise.
  - Evidence: `README.md:28` says `requirements.txt` is "Core
    only," but `requirements.txt:5` includes pandasql, Prophet,
    sklearn, SHAP. `Makefile:12` references missing
    `requirements-all.txt`.
  - **R1 finding.** Naïvely making `requirements.txt` mirror
    `requirements-core.txt` removes pandasql; threshold and
    historical validators on PandasBackend depend on
    `pandasql.sqldf` (`backends/pandas_backend.py:28-36`). The
    README markets threshold/historical as core. Removing
    pandasql breaks the documented core surface on Pandas.
  - **Decision pin (revised after R1):** declare the
    *Spark* backend as core. PandasBackend stays in the package
    but its SQL path requires the `[pandas]` extra. The README
    "core" paragraph names PySpark + the basic deps; pandas
    support is documented as an optional extra. This matches
    the existing `pandasql` entry under
    `[project.optional-dependencies]` `pandas` in
    `pyproject.toml:36`.
  - [ ] Make `requirements.txt` mirror `requirements-core.txt`
        (drops pandasql + Prophet + sklearn + SHAP). Keep
        `requirements-{forecast,anomaly,dev}.txt` as-is.
  - [ ] Add `requirements-all.txt` unioning `core + forecast +
        anomaly + pandas` for parity with `qualifire[all]`.
  - [ ] Update `Makefile:12` reference to point at the now-real
        `requirements-all.txt`.
  - [ ] **Update README install section** to name the four
        validator types that work on core-only Spark (SLO,
        threshold, historical/drift, custom-query) and the
        three that require extras
        (trend/forecast → `[forecast]`,
        shape → `[anomaly]`, pattern → `[anomaly]`).
        Pandas backend gets a one-line note: requires
        `[pandas]` extra for the SQL path; `validate(df=...)`
        with explicit `dimensions`/`measures` works without it.
  - [ ] Test (Spark, core-only): in a fresh environment with
        `pip install -r requirements.txt` (Spark assumed
        pre-installed per AIDP convention), threshold + drift
        + SLO run; trend/shape/pattern raise the documented
        `ImportError`.
  - [ ] Test (Pandas, core-only): pandasql NOT installed,
        `qf.validate(table=..., validations=[threshold_check(...)])`
        on a `PandasBackend` raises a clear `ImportError`
        message naming `pip install 'qualifire[pandas]'`.
        Currently this raises an unhelpful pandasql-internal
        error.
  - **CI / Makefile (R2 finding).** `make test` after just
    `make install` would fail on tests that exercise pandas /
    forecast / anomaly paths, because core-only is now truly
    core-only. Tests must run against `[all,dev]` extras.
  - [ ] Update `Makefile`: `make test` depends on `install-all`
        (Pre-test `pip install` is too heavy for incremental
        re-runs; alternative: document `make install-all`
        prerequisite in `make test` help / CONTRIBUTING).
        **Decision:** add a `test-precheck` target that errors
        with a clear message if `pandasql`/`prophet`/`shap` are
        missing, recommending `make install-all`. `make test`
        depends on `test-precheck`.
  - [ ] `tests.yml` workflow at `.github/workflows/tests.yml:30-38`
        already installs `[all,dev]` + pyspark out-of-band — the
        current comment ("`make install-all` references a
        nonexistent `requirements-all.txt` on main") becomes
        stale once P4.6 lands `requirements-all.txt`. Update
        the comment + flip the install step to `make install-all
        + pip install pyspark>=3.5` for parity with the
        documented contributor flow.
  - [ ] CONTRIBUTING.md update: describe the
        `core` / `[pandas]` / `[forecast]` / `[anomaly]` /
        `[all]` matrix; recommend `make install-all` for any
        contributor who runs the test suite.

### Phase 4 exit criteria

- All Should-fix items have a corresponding test or doc change.
- `make test` green.

## Phase 5 — Reconcile `spark-primary-industry-packs` shipped status + dashboard hook

### Steps

- [ ] **P5.1 (Blocking)** — Resolve the local
      `docs/features/spark-primary-industry-packs/shipped.md` vs.
      checked-in source inconsistency.
  - Evidence: dashboard at
    `docs/features/DASHBOARD.md:29` (auto-generated, gitignored)
    lists feature as "Completed 2026-04-21" because of the
    untracked working-tree `shipped.md`. The implementation
    commits live on unmerged
    `origin/spark-primary-industry-packs`.
  - **Decision pin (R7):** *delete the local `shipped.md`*. The
    branch is unmerged and not the target of this PR; the right
    action is to remove the local artifact so the dashboard
    stops claiming the feature is shipped. The branch can be
    landed in a follow-up feature.
  - [ ] `git rm` the file in this branch's tree if it was
        committed; otherwise just delete it locally and verify
        `git status` no longer shows it untracked.
  - [ ] When this feature ships, the resulting
        `docs/features/findings-sweep/shipped.md` (written by
        `/feature-ship` after merge) names the spark-primary
        branch as still-pending and recommends a follow-up
        feature to land it. **R2 fix:** there is no Phase 6 in
        this plan; the prior reference was wrong.

- [ ] **P5.2 (Should-fix)** — Harden the dashboard auto-gen hook.
  - The hook script lives at
    `/Users/amitranjan/.claude/plugins/cache/schuettc-claude-code-plugins/feature-workflow/9.4.4/skills/shared/lib/run_dashboard.py`
    — it is **not** in this repo. R3 finding: leaving it as
    documentation only does not actually fix the problem; the
    next half-staged `shipped.md` reproduces the dashboard lie.
  - [ ] **Read the hook script** to confirm whether it filters by
        `git ls-files`. R3 spot-check found it does not (it
        scans the directory tree directly).
  - [ ] **Two-pronged fix:**
        1. **Repo-side guard:** add a tiny pre-commit / pytest
           guard at `tests/test_feature_workflow_hygiene.py` that
           reads `docs/features/*/shipped.md` paths from the
           filesystem AND from `git ls-files`, and asserts every
           on-disk `shipped.md` is also tracked by git. Fails
           the test suite if a contributor leaves an untracked
           `shipped.md` lying around. This is what we *can*
           control from inside the project PR.
        2. **Upstream issue (out-of-scope but tracked):** open
           an issue against `schuettc-claude-code-plugins`
           proposing the hook filter `shipped.md` candidates
           through `git ls-files`. Note the issue link in
           `docs/features/findings-sweep/plan.md` progress log
           when filed; the upstream fix lands separately.
  - [ ] Test: create a fake untracked `docs/features/test-feat/
        shipped.md`, run the new hygiene test → fails with a
        clear message naming the file. Remove the fake file →
        test passes.

### Phase 5 exit criteria

- `git status` post-branch shows no untracked
  `docs/features/spark-primary-industry-packs/` artifacts.
- The hook investigation has a written conclusion in
  `docs/features/findings-sweep/plan.md` progress log
  (committed) — either "verified hook already filters" or
  "filed upstream + added contributor warning."

## Cross-cutting concerns

### §R1 — Backward compatibility

- All Pydantic config additions (`name` on validators,
  `df` on `WAPConfig`) are *additive* with safe defaults. No
  existing YAML breaks.
- No system-table schema migration. New behavior populates the
  existing `dimension_value` column on validation rows; old rows
  remain readable with `dimension_value IS NULL`.
- `validate(notify=...)` now actually does what its docstring
  claims. Callers who passed it expecting nothing (relying on
  the bug) get an *additional* notification — this is documented
  behavior, not a silent regression. Callers who passed
  `notify=` and were surprised it didn't fire are the affected
  population, and the fix matches what they intended.

### §R2 — Concurrency

- `_send_grouped_notifications` already runs once after the
  parallel validation fan-out completes (`engine.run:198`), so
  the bulk pre-fetch (P4.3) does not need its own locking.
- `WAPExecutor.publish` / `rollback` (P2.4) run sequentially
  inside `_run_wap` — the new `try/finally` is single-threaded
  per dataset.

### §R3 — Out-of-scope items deliberately deferred

- Renaming `jdbc` storage backend identifier (breaking change,
  not worth it; documented in P4.4).
- Caching `overall_severity` on `DatasetResult` (Polish item).
- Carving `engine.py` along its seams (Polish item; large; will
  destabilize this PR's review).
- Maturity / `pyproject.toml` Alpha-vs-production reconciliation
  (Polish item; orthogonal positioning question).
- (`[spark]` extra promoted into P4.2 — needed to make the
  install-text accurate.)

### §R4 — Validation strategy

Each phase has its own exit criteria gate. The phases are
ordered for dependency, not commit boundary — implementation
will proceed phase-by-phase but the commits-on-branch may not
mirror phase boundaries. The branch will be squash-merged.

### §R5 — System table additivity

- No DDL changes. The `dimension_value` column already exists
  on every backend per
  `qualifire/storage/base.py:SYSTEM_TABLE_COLUMNS`.
- New `read_validation_history_bulk` method (P4.3) is a new
  read API — pure addition.
- `read_metric_history` (used by drift / forecast) gains a
  `dimension_value: str = "_default"` parameter (P2.1). Old call
  sites passing only `(table_name, metric_name, limit, step)`
  continue to work; the predicate uses
  `COALESCE(dimension_value, '_default') = ?` so legacy NULL
  rows match the new default. **R3+R4 fix — earlier prose
  named wrong arg order/types.**

### §R6 — Identity-key minimalism

**Decided after R3 review.** Two key types only:
`MetricKey` (collection identity — dataset/metric/dim) and
`ValidationKey` (validation identity — dataset/validation/metric/dim).
Both are constructed at the dataset/result seam (engine-side), not
as methods on result objects. There is **no** `ResultKey` type —
references in earlier R1 prose were renamed in R3. See P1.4.

### §R7 — `shipped.md` deletion vs. branch landing

Decided: delete (P5.1). Landing the branch in this PR would
silently expand scope and conflate two features.

### §R8 — `notify=` parameter shape (P3.1)

The dataset-level `notify=` argument on `validate()` and
`write_audit_publish()` accepts a plain dict matching
`NotifyConfig`'s field names: `{"on_success": [...],
"warning": [...], "error": [...]}`. Internally normalized to
`NotifyConfig(**notify)` so the precedence logic operates on a
typed object. Per-validation `notify` values on builder configs
take precedence over the dataset-level default — never silently
overwritten. A validation whose `notify` is the empty default
(`NotifyConfig()`) inherits the dataset-level default.

### §R9 — WAP `df` vs. `write_sql` precedence (P3.2)

`WAPConfig.df` and `WAPConfig.write_sql` are mutually
exclusive. Setting both raises a `pydantic.ValidationError` at
config-load time via a `@model_validator(mode="after")` on
`WAPConfig`. The validator message names both fields and
recommends one or the other. This guard mirrors the engine
contract today: `WAPExecutor.write` already errors if neither
is set (`wap/pattern.py:92`); we extend the symmetry to "both
is also wrong."

### §R10 — Engine-level notification routing (P4.1)

A new optional top-level `engine_notify: NotifyConfig | None =
None` field on `QualifireConfig` carries the routing for
engine-level events (currently: persistence-failure warning).
Distinct from per-validation `notify` because the event isn't
tied to a user-defined check. When unset, persistence failures
are returned in the result but not routed.

## Open Decisions (must be resolved before code)

None at this time. Decisions §R3–§R10 are pinned. If reviewers
push back on any pin, address before starting code.

## Implementation Steps Summary

```
Phase 1: P1.1, P1.2, P1.3, P1.4
Phase 2: P2.1, P2.2, P2.3, P2.4
Phase 3: P3.1, P3.2
Phase 4: P4.1, P4.2, P4.3, P4.4, P4.5, P4.6
Phase 5: P5.1, P5.2
```

## Test Strategy

- Each step lists at least one regression test (failing-before /
  passing-after) where applicable.
- Existing test count: 732 (per `pytest --collect-only`).
- Net new tests: roughly 20-25 tests across the seven test
  modules implicated below:
  - `tests/test_models.py`
  - `tests/test_storage/`
  - `tests/test_validation/test_threshold.py`
  - `tests/test_validation/test_isolation_forest.py`
  - `tests/test_engine.py`
  - `tests/test_engine_routing.py`
  - `tests/test_api.py` / `tests/test_api_extra.py`
  - `tests/test_wap.py`
  - `tests/test_notification/`
- E2E suites (`tests/test_e2e_industries`, `tests/test_e2e.py`)
  must remain green; no new E2E coverage required (the unit
  tests above are sufficient, and the industry packs already
  exercise the relevant code paths).

## Progress Log

(Filled during implementation.)

## References

- `.tmp/findings.md` — combined review.
- `docs/features/findings-sweep/idea.md` — backlog entry.
- `docs/features/codex-review-fixes/plan.md` — precedent for
  multi-phase plan structure.
