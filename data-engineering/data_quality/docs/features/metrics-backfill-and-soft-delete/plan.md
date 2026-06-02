---
id: metrics-backfill-and-soft-delete
name: Metrics Backfill and Soft-Delete
status: planning
base_branch: main
branch: feature/metrics-backfill-and-soft-delete
created: 2026-05-08
---

# Metrics Backfill and Soft-Delete — Implementation Plan

This plan implements the six workflows captured in `idea.md`:

1. Cold-start onboarding (build history before going live).
2. New validation on existing dataset (seed missing baseline).
3. Source backfilled / fixed (recompute stale metrics).
4. Bad-metric suppression (`is_active=false`).
5. `skip_if_cached` for repeat partitions on normal runs.
6. WAP backfill from `target_table` (no staging / publish).

The decision pins in `idea.md` are inputs, not open questions.

## Hard invariants (load-bearing — locked, do not relax)

These are stated up front so individual sections cannot drift:

H1. **Tombstone identity = persisted-row identity, including
    `table_name`.** Soft-delete tombstones are written at *the same*
    `record_type` and *the same natural key* as the persisted rows
    they hide, with `table_name` = the dataset's logical table
    (`dataset.table` or, for WAP, `wap.target_table`; matching what
    history reads carry). There is no `record_type='soft_delete'`
    species. A deactivation writes:
    - one **`record_type='validation'`** row per
      `(dataset_name, validation_name, metric_name, partition_ts,
        dimension_value)` for every validation that emits the metric
      on the dataset (resolved from the loaded config), AND
    - one **`record_type='collection'`** row per
      `(table_name, metric_name, partition_ts, dimension_value)`,
    each with `is_active='false'`, `metric_value=NULL`,
    `validation_message=<reason>`,
    `details_json={"soft_delete": true, "reason": "..."}`. The NULL
    `metric_value` is fine because H2 moves the
    `metric_value IS NOT NULL` filter to AFTER the soft-delete
    filter, so the tombstone wins `ROW_NUMBER`, fails the
    `is_active` filter, the key disappears from the result set
    (consumer sees no row for the deactivated key, regardless of
    metric_value).

    **Ordering invariant**: tombstone `run_timestamp` MUST be
    strictly greater than the latest active row's `run_timestamp`
    for the natural key. The `core/deactivate` helper reads back
    the maximum `run_timestamp` for the natural key set being
    tombstoned and writes
    `tombstone_run_timestamp = max(datetime.now(), latest + 1us)`.
    This handles the clock-skew case (a future-stamped row from
    an upstream system) AND the clock-collision case (writes
    landing in the same microsecond as the latest active row).
    Concurrent active writes that land *after* the tombstone
    (with even later `run_timestamp`) intentionally re-activate
    — that is the documented "next collection wins" contract.

H2. **Read predicate ordering.** Every read that respects
    soft-delete uses the order:
    `ROW_NUMBER() OVER (PARTITION BY <natural key>
     ORDER BY run_timestamp DESC, collected_at DESC) → rn = 1
     → COALESCE(is_active, 'true') = 'true'
     → metric_value IS NOT NULL (where the read requires a value)
     → record_type filter (where the read scopes by record_type)`.
    Filtering `is_active` or `metric_value IS NOT NULL` *before* the
    row-number step is forbidden — those filters would let a stale
    active row surface underneath a newer NULL-valued tombstone.
    The test in S3 fails any reordering. **Concretely**: every
    existing `read_*` query that today carries
    `WHERE metric_value IS NOT NULL` inside the FROM/CTE relocates
    that predicate to the outer `WHERE rn = 1 AND ...` clause as
    part of S3.

H2'. **Per-partition source selection during backfill.** Backfill
    is not a `partition_ts`-stamping pass — it must read *only* the
    rows that belong to the partition being backfilled. The driver
    does all of:

    1. **Context overlay.** A per-partition `QualifireContext` sets
       `ds=<anchor>` (string `YYYY-MM-DD` for day cadence,
       `YYYY-MM-DDTHH:MM:SS` for sub-day) on top of the operator's
       base context. Existing `{{ ds }}`-templated `filter:` /
       `query:` / `partition_ts:` / `collection.filter:`
       expressions re-render against the per-partition value.
       Operators with `{{ ds }}`-templated SQL get correct
       behaviour with no further intervention.

    2. **Generated partition predicate** — applied at the layer
       the engine actually consults for source filtering, not just
       at `DatasetConfig.filter`. The mapping is explicit:

       - **Table datasets (no query, no WAP):**
         predicate is *AND*-merged into `DatasetConfig.filter`
         AND into every in-scope validation's
         `collection.filter` if the validation set its own
         `collection.filter`. (The engine prefers
         `collection.filter or filter_expr`, so injecting at
         either layer alone leaks at the other.)
       - **WAP datasets:** the transient `DatasetConfig` has
         `wap=None, table=wap.target_table` per H4; the
         predicate path then mirrors the table-dataset case.
       - **Query datasets (`DatasetConfig.query`):** the
         transient overlay wraps the operator's query as
         `SELECT * FROM (<original_query>) AS _qf_inner WHERE
         <generated_predicate>` ONLY when the partition_ts
         expression names a column visible in the inner SELECT
         (validated by the dataset's existing `dimensions` /
         `measures` declarations or by a SQL parse that fails
         loudly). Otherwise the overlay is a no-op and the
         operator's query is responsible (the `{{ ds }}`-context
         overlay path stays valid).
       - **Custom-query collectors (`CustomQueryCollector`):**
         the collector has no filter hook in current code.
         `qualifire backfill` does not modify the operator's
         query for these. Operators with a `custom_query`
         validation MUST template their query against
         `{{ ds }}` to be backfill-safe; the plan documents
         this in S11 and the backfill API logs a
         `qualifire.engine` warning row when a scoped
         validation uses `CustomQueryCollector` AND its query
         text contains no `{{ ds }}` literal substring (cheap
         heuristic; false positive on operator-defined
         alternative variable names is acceptable — the warning
         names the validation and tells the operator to verify
         partition isolation manually).

    3. **Predicate literal shape.** The generated predicate uses a
       string equality, NOT a `CAST` to TIMESTAMP:
       `(<partition_ts_expr>) = '<anchor>'` for day cadence and
       `(<partition_ts_expr>) = '<anchor_iso>'` for sub-day. The
       SQLite-driven pandas backend evaluates
       `CAST('2026-04-02' AS TIMESTAMP)` as integer `2026`
       (verified), which would not match string/date columns.
       String equality works on both sides: Spark accepts
       `event_dt = '2026-04-02'` against a DATE column via
       implicit cast; SQLite/pandas accepts the same against a
       TEXT column with no cast. Operators whose `partition_ts`
       expression resolves to a non-string non-date column
       (rare) can pre-format on their side via a Jinja
       expression and the H2' generated path is bypassed (the
       `{{ ds }}`-presence heuristic catches it).

    4. **Predicate-skip cases:**
       - `partition_ts` contains `{{` (Jinja-driven) → skip.
       - dataset uses `CustomQueryCollector` → skip per #2 above.
       - dataset has `query:` and partition_ts expression isn't
         a column visible in the inner SELECT → skip with a
         `qualifire.engine` warning naming the dataset.

    5. **Tested in S5 with**: two-partition source data,
       table dataset, query dataset, validation-level
       `collection.filter` overriding dataset filter, and
       `custom_query` collector — each asserts the persisted
       metric VALUE comes from the anchor partition only (not
       just that `partition_ts` is stamped correctly), via a
       pre-seeded source where the two partitions have
       distinguishable counts (e.g., 3 rows in partition_a, 5 in
       partition_b → backfill of partition_b expects
       metric_value=5).

H3. **`partition_step` config-level invariant** (matches `idea.md`
    decision pin 3): for every dataset with an effective
    `partition_ts`, an effective `partition_step` must resolve at
    `load_config()`. The inheritance order is `dataset.partition_step
    → run.partition_step → ValueError at config-load time`.
    `partition_step` without `partition_ts` is rejected.
    Run-level `partition_step` is rejected at load time when no
    dataset has an effective `partition_ts` (otherwise the field is
    decorative and silently inconsistent). There is no
    auto-detection. **Explicit override scope:** `Qualifire.backfill`
    and `qualifire backfill` accept `partition_step=...` /
    `--partition-step ...` only as a cadence *override* for already-
    loaded configs that pass H3. The override path does NOT relax
    the load-time check — operators with legacy YAML must edit
    YAML first. The override exists for the case where YAML carries
    cadence A but the operator wants to backfill at cadence B for a
    one-off run (e.g., dataset normally daily, operator wants a
    one-time hourly backfill). Documented in S6 acceptance test #6.

H4. **WAP validations live on `DatasetConfig.validations`.** Verified
    in `qualifire/core/config.py`: `WAPConfig` has only `write_sql`,
    `target_table`, `write_options`, `df`. The backfill scope
    resolver consumes `dataset.validations` for both regular and
    WAP datasets. The transient backfill `DatasetConfig` is built
    via `model_copy(update={"wap": None, "table":
    wap.target_table, "filter": <merged with partition predicate
    per H2'>})` and inherits everything else from the original.

H5. **`skip_if_cached` and `skip_if_present` are scoped to
    *non-dimensional* metric-value validators.** Three families
    are eligible: `threshold`, `historical` (drift), and
    `forecast`. SLO, `anomaly_detection` (shape), and `pattern`
    always fall through to the live collect path (SLO because its
    persisted `metric_value` is a numeric timestamp/epoch and the
    type round-trip isn't worth it for one family; sample-based
    because their `CollectionResult` payloads are not persisted).
    **Dimensions exclusion**: a validator with a non-empty
    `collection.dimensions` (e.g., `threshold_check(
    dimensions=["region"])` running a per-segment GROUP BY) is
    additionally excluded because the cache pre-pass cannot know
    *which* segments the current partition will produce without
    reading the source. The engine emits one
    `qualifire.engine` info row per dataset where excluded
    validators caused a partial cache miss. **Tested**: S8 has
    explicit assertions that a `threshold_check(dimensions=
    ["region"])` always re-collects under `skip_if_cached=True`
    even when an active collection row exists for the partition;
    a non-dimensional sibling validator on the same dataset
    short-circuits.

H6. **`read_collection_metric_at_partition` contract.** New storage
    method, identical signature on all four backends:
    ```python
    def read_collection_metric_at_partition(
        self,
        table_name: str,                  # logical table identity
        metric_name: str,
        anchor_ts: datetime,              # exact partition_ts
        dimension_value: str | None,      # NULL-safe equality
    ) -> dict[str, Any] | None:
    ```
    Returns the latest active collection row for the natural key
    `(table_name, metric_name, partition_ts=anchor_ts,
    dimension_value)`, or `None`. Filter chain (per H2):
    `record_type = 'collection' AND table_name = ? AND metric_name
    = ? AND partition_ts = ? AND dimension_value <NULL-safe-eq> ?`,
    `ROW_NUMBER() OVER (... ORDER BY run_timestamp DESC,
    collected_at DESC)`, `WHERE rn = 1 AND COALESCE(is_active,
    'true') = 'true' AND metric_value IS NOT NULL`. Returned dict
    fields (sufficient to rebuild a `CollectionResult`):
    `metric_name`, `metric_value`, `partition_ts`, `dimension_value`,
    `collector_name`, `collected_at`, `expected_value`,
    `actual_value_text`. `read_validation_history_bulk` is
    untouched — it stays the notification-suppression API.

H7. **JDBC migration is a deploy-blocking pre-step.** The plan does
    not silently fall back to legacy reads. Operators run the
    documented `ALTER TABLE` before the upgrade lands; after the
    upgrade `JDBCStorage.initialize()` validates the column is
    present and raises `RuntimeError("system table missing column
    is_active — apply ...")` otherwise. The CLI converts this to
    an exit-1 with a copy-paste fix (consistent with how
    `JDBCStorage` handles other DDL/permission errors today). Until
    the DBA runs the migration, JDBC users cannot run *any*
    qualifire command — fail-loud, fail-fast, fail-once.

H8. **`read_latest_run` consumer contract.** Today's contract
    (`storage/base.py:165`) is "most recent run row per
    `dataset_name`." With H1, a deactivated key produces a tombstone
    that becomes the latest row, then is filtered out — the function
    can return None for a dataset whose latest validation was
    deactivated. The plan accepts this; the only consumer of
    `read_latest_run` today is the e2e/test suite (no production
    dashboard use). Acceptance test S3 #9 covers the surfaced None.

## Scope (in)

- `core`
  - `DatasetConfig.partition_step: str | None`
  - `QualifireConfig.partition_step: str | None`
  - Validators enforce H3.
  - Helper `effective_partition_step(dataset, run_config) ->
    str | None` lives in `qualifire/core/config.py` next to
    `effective_partition_ts`-style helpers (matches existing
    inheritance pattern).
  - New module `qualifire/core/selectors.py` (selector parser —
    see S4).
  - New module `qualifire/core/backfill.py` (scope resolver +
    BackfillReport dataclasses + backfill loop driver — see S5).
  - New module `qualifire/core/deactivate.py` (deactivate row
    builder — see S7).

- `storage`
  - New TEXT column `is_active` on the system table.
  - Persists as `'true'` / `'false'` strings for portability across
    SQLite / Delta / ExternalCatalog / JDBC (mirrors how
    `expected_value` and `details_json` use TEXT for compatibility).
  - Reads honour H2.
  - The four backends migrated for the six existing read methods
    listed in `storage/base.py`.
  - **New protocol method** `read_collection_metric_at_partition(
      table_name: str, metric_name: str, anchor_ts: datetime,
      dimension_value: str | None) -> dict | None`
    — used by `skip_if_cached` and `skip_if_present`. Returns the
    latest active `record_type='collection'` row for that natural
    key, or None. Implemented in all four backends.
  - SQLite / Delta / ExternalCatalog: column auto-added via
    `_ensure_columns`. JDBC: pre-deploy DBA migration (see H7).

- `engine` (`qualifire/core/engine.py`)
  - Row builders (`_build_validation_and_collection_rows` at
    line 1605 and the notification row builder at line 1588) stamp
    `is_active='true'` on every emitted row. This is the canonical
    write site for the column; `qualifire/reporting/system_table.py`
    is *not* the row builder, it's a reader/projector.
  - New private `_run_backfill(self, partitions, scope)` driver.
    Iterates partitions and calls `_run_dataset` against transient
    overlays per H4.
  - Per-partition partition_ts injection: a transient
    `DatasetConfig` via `model_copy(update={"partition_ts": "...",
    "validations": <scoped>, "wap": None, "table": <wap.target if
    is_wap else original>})`. Caller's config is never mutated.
  - Backfill rows tagged via a new `QualifireContext.backfill: bool`
    flag (default False). The row builders read the flag and add
    `"backfill": true` to the `details_json` payload (boolean,
    *not* string). `system_table.py` row template grows a single
    "if context.backfill: details["backfill"] = True" line.
  - New `QualifireContext.cached_metrics: dict[(str, str, str, str
    | None), CollectionResult] | None` populated by the
    skip-if-cached pre-pass; collectors that find their key in this
    dict return the cached `CollectionResult` instead of executing
    the SELECT.

- `api` (`qualifire/api.py`)
  - `Qualifire.backfill(self, *,
       config: QualifireConfig | None = None,
       config_path: str | None = None,
       start: str | datetime,
       end: str | datetime,
       include: str | list[str] | None = None,
       exclude: str | list[str] | None = None,
       partition_step: str | dict[str, str] | None = None,
       validate: bool = False, notify: bool = False,
       parallelism: int = 1, skip_if_present: bool = False,
       on_failure: Literal['continue','stop'] = 'continue',
       context: dict[str, str] | None = None,
    ) -> BackfillReport`
    Exactly one of `config` / `config_path` is supplied; ValueError
    otherwise.
  - `Qualifire.deactivate_metric(self, *,
       dataset: str, metric: str,
       start: str | datetime, end: str | datetime,
       dimension: str | None = None,
       validation: str | None = None,
       reason: str | None = None,
       partition_step: str | None = None,
       config: QualifireConfig | None = None,
       config_path: str | None = None,
    ) -> int`
    Returns count of tombstone rows written
    (sum of partitions × tombstones-per-partition).
    `validation=None` deactivates every validation on the dataset
    that emits `metric`; `validation="<name>"` restricts to that
    one.
  - `Qualifire.validate(..., skip_if_cached: bool = False)` and
    `Qualifire.run_config_parsed(..., skip_if_cached: bool = False)`
    accept the new kwarg.

- `cli` (`qualifire/cli.py`)
  - Subcommand `backfill` (mirrors API kwargs).
  - Subcommand `deactivate-metric` (mirrors API kwargs).
  - Flag `--skip-if-cached` on `run`.
  - All new subcommands map `QualifireConfigError` and
    `ValueError` from selector / cadence resolution into clean
    stderr + exit-2 (consistent with existing `_cmd_run`).

- `wap`
  - Backfill rewrite uses `dataset.validations` and
    `wap.target_table` per H4. `WAPExecutor` is untouched. WAP
    backfill is collect-only against the target table filtered by
    `partition_ts = anchor_ts`.

- `reporting`
  - Dashboards (interactive + matplotlib + plotly) inherit
    soft-delete behavior through their consumption of
    `read_health_data` (which gains the H2 ordering). No dashboard
    code change required; tested in S10.

- `docs`
  - `docs/wap_pattern.md` — append "Backfill" section explaining
    target-table-sourced backfill.
  - `docs/programmatic_api.md` — `Qualifire.backfill`,
    `Qualifire.deactivate_metric`, `skip_if_cached` examples.
  - `docs/configuration.md` — `partition_step` field reference and
    H3 statement.
  - `README.md` — one bullet under "What you get" linking to the
    architecture note.
  - `docs/backfill_and_soft_delete.md` (new — architecture note +
    DBA migration SQL for JDBC).

## Scope (out)

- Auto-detection of `partition_step` from system-table intervals.
- SQL-driven WAP write-options parity.
- A YAML `backfill:` block. Backfill is invocation-time only.
- Hard-deletion of historical rows. Soft-delete only.
- skip-if-cached / skip-if-present support for sample-based
  validators (anomaly_detection, pattern). H5 commits to falling
  through to live collect.
- Detecting and re-tombstoning previously soft-deleted rows after
  a backfill. Backfill *intentionally* re-activates by appending
  newer active rows. Operators who want the tombstone to stick
  must run `deactivate-metric` *after* backfill.
- A general-purpose JDBC `_ensure_columns` migration helper. JDBC
  schema additions remain CREATE-TABLE-only this feature.

## Decision Pins (locked from idea.md, restated for traceability)

1. Two surfaces, one primitive (`Qualifire.backfill` + `qualifire
   backfill`); CLI is a thin arg-parser around the API.
2. Selector `dataset:validation` strings with `*` wildcards.
3. `partition_step` mandatory when `partition_ts` is set,
   forbidden otherwise (H3).
4. Idempotency: append by default. `--skip-if-present` opts in.
5. `validate=False, notify=False` default. With `validate=True`,
   parallelism is forced to 1.
6. `BackfillReport` per-partition + summary; CLI prints summary;
   non-zero exit on failure with `on-failure=stop`.
7. Audit tag: `details_json.backfill = true` (boolean) on every
   backfill row.
8. WAP backfill = same primitive, target-table-sourced (H4).
9. Document WAP publish-phase contract.
10. Soft-delete via `is_active` TEXT column with H1 tombstoning.
11. `skip_if_cached` opt-in for normal runs, scoped per H5.

## Implementation Steps

The order below is a hard dependency chain. Each step has a
measurable exit criterion and an acceptance test that, if absent,
would let the next step ship broken.

### S1 — `partition_step` config field (per H3)

Files: `qualifire/core/config.py`,
`tests/core/test_config_partition_step.py`.

- Add `partition_step: str | None = None` to `DatasetConfig` and
  `QualifireConfig`.
- Field validator: when set, must parse via
  `qualifire.core.duration.parse_duration` and reject months /
  years (variable widths).
- Cross-field validator at `QualifireConfig` level: for every
  dataset, evaluate
  `effective_partition_ts ↔ effective_partition_step`. Both
  resolved or neither — XOR raises with the exact dataset name and
  the suggested fix.
- Helper `qualifire.core.config.effective_partition_step(dataset:
   DatasetConfig, run: QualifireConfig) -> str | None` — single
  source of truth.

Acceptance test: `tests/core/test_config_partition_step.py`:
1. dataset-level set, no run-level → resolves to dataset.
2. run-level set, no dataset-level → resolves to run-level.
3. dataset-level set, run-level set → dataset wins.
4. neither set, no `partition_ts` anywhere → resolves to None
   (no error).
5. dataset has `partition_ts`, no `partition_step` anywhere →
   `ValueError` naming the dataset.
6. dataset has no `partition_ts`, has `partition_step` →
   `ValueError` ("partition_step requires partition_ts").
7. run-level `partition_ts` + no run-level / dataset-level
   `partition_step` → `ValueError` naming the run-level inheritance.
8. invalid value (`"P1M"`, `"foo"`) → `ValueError` from validator.
9. **run-level `partition_step` with NO dataset having
   `partition_ts`** → `ValueError("run-level partition_step is set
   but no dataset has partition_ts; partition_step is meaningless
   without partition_ts")`. Closes the H3 hole.
10. run-level `partition_step` + a dataset that does have
    `partition_ts` (and inherits run-level cadence) → resolves
    cleanly (positive case for the H3 invariant).

Exit: `pytest tests/core/test_config_partition_step.py` green.

### S2 — `is_active` storage column (schema + write)

Files: `qualifire/storage/base.py`,
`qualifire/storage/sqlite_storage.py`,
`qualifire/storage/delta_storage.py`,
`qualifire/storage/external_catalog.py`,
`qualifire/storage/jdbc_storage.py`,
`qualifire/core/engine.py`,
`tests/storage/test_is_active_migration.py`.

- Append `"is_active"` to `SYSTEM_TABLE_COLUMNS` and
  `COLUMN_DEFINITIONS` (`("STRING", "TEXT")`); add
  `is_active STRING` to `SYSTEM_TABLE_DDL`.
- `_build_validation_and_collection_rows` (engine.py:1605) and the
  notification row builder (engine.py:1588) stamp
  `is_active='true'` on every emitted row.
- `_ensure_columns` already iterates a delta against
  `COLUMN_DEFINITIONS`; adding the column to that map auto-runs
  the migration on next `initialize()` for SQLite / Delta /
  ExternalCatalog.
- JDBC: column added to the `CREATE TABLE` DDL. Plus, a new
  startup check inside `JDBCStorage.initialize()`:
  `INFORMATION_SCHEMA.COLUMNS` lookup for `is_active`. Missing →
  `RuntimeError("system table {table} is missing column
  is_active. Apply: ALTER TABLE {table} ADD COLUMN is_active
  VARCHAR(8) DEFAULT 'true'; — see
  docs/backfill_and_soft_delete.md")`. CLI maps to exit-1 (already
  does so for `JDBCStorage.initialize` errors).

Acceptance test: `tests/storage/test_is_active_migration.py`:
- Fresh SQLite table at `initialize()` has the column with
  `'true'` default-on-write.
- Existing SQLite table created via the *prior* DDL has the
  column added by `_ensure_columns()` on next `initialize()`.
- Same for Delta + ExternalCatalog under the existing `pyspark`
  marker.
- JDBC: schema-creation test asserts the DDL contains
  `is_active VARCHAR`. Mock the missing-column case to assert
  `JDBCStorage.initialize()` raises with the documented message.
- Engine row builders are unit-tested to emit `is_active='true'`
  on validation, collection, and notification rows.

Exit: green test file; existing tests unchanged.

### S3 — Read paths honour `is_active` (per H2)

Files: SQLite / Delta / ExternalCatalog / JDBC storage modules,
`tests/storage/test_soft_delete_reads.py`.

- Migrate the six existing read methods to the H2 ordering. **Crucially**,
  every existing read query that today carries
  `WHERE metric_value IS NOT NULL` *inside* the FROM-side filter
  relocates that predicate to the outer
  `WHERE rn = 1 AND COALESCE(is_active, 'true') = 'true' AND
   metric_value IS NOT NULL` clause. Three SQLite reads
  (`read_metric_history` non-bucketed mode at lines 143-156, the
  bucketed mode at 165-190, and `read_metric_history_by_partition`
  at lines 196 onwards) currently filter `metric_value IS NOT NULL`
  pre-`ROW_NUMBER`; same for Delta / ExternalCatalog / JDBC. The
  S3 acceptance test exercises a tombstone-with-NULL-value to
  prove the relocation is in the right place.

- **Non-bucketed `read_metric_history` rework**: today this returns
  up to N raw rows ordered by run_timestamp DESC. With H1 / H2 a
  raw-row pass would let a stale active row surface alongside a
  newer NULL tombstone for the same partition. Migration is to
  ROW_NUMBER over `(table_name, metric_name, partition_ts,
  dimension_value)`, take `rn = 1`, then apply the H2 chain.
  The contract changes from "up to N raw rows" to "up to N
  partition-distinct rows, soft-delete-aware". This is a **behaviour
  change**: callers reading per-partition latest see no surprise;
  callers expecting raw history (none today, verified by
  `git grep "read_metric_history\b"`) would. Documented as part of
  the S11 changelog.

- **Bucketed `read_metric_history(step=...)` rework**: today this
  uses `ROW_NUMBER OVER (PARTITION BY <run_timestamp_bucket>)` —
  a single inactive tombstone landing in today's bucket fails the
  `is_active` filter, but the stale active row in its original
  bucket still surfaces. Two-stage rework:
  1. **Inner CTE — soft-delete dedup per natural key.** ROW_NUMBER
     OVER `(table_name, metric_name, partition_ts, dimension_value)`
     ORDER BY run_timestamp DESC, collected_at DESC. Take rn=1.
     Apply H2 chain (is_active, then metric_value IS NOT NULL).
     This produces the per-partition active survivor set.
  2. **Outer CTE — bucket dedup.** ROW_NUMBER OVER
     `(<run_timestamp_bucket>)` over the survivor set, take latest
     per bucket, LIMIT N. The behavioural contract is unchanged
     for non-deactivation flows; deactivation now correctly hides
     a partition even from the bucketed read.
  All four storage backends apply the same two-stage shape.

- Natural keys per method (unchanged):
  - `read_metric_history(_by_partition)`: `(table_name, metric_name,
    partition_ts, dimension_value)` over collection-preferred,
    NULL-safe equality on metric/dimension.
  - `read_validation_history(_bulk)`: `(dataset_name,
    validation_name, metric_name, partition_ts, dimension_value)`.
  - `read_health_data`: `(dataset_name, validation_name,
    metric_name, partition_ts, dimension_value)`.
  - `read_latest_run`: `(dataset_name)`.
- The `record_type` filter inside `read_health_data` (`'validation'`
  vs `('validation','collection')`) lives at the END of the chain —
  *after* `is_active`, so a deactivation tombstone (record_type
  matching the row it tombstones, per H1) survives `rn=1`, fails
  `is_active`, and the consumer sees a clean exclusion.

Acceptance test: `tests/storage/test_soft_delete_reads.py` —
parametrized over SQLite + (Delta + ExternalCatalog under pyspark
marker):
1. Insert active validation row at T0 → `read_health_data` returns
   it.
2. Insert validation tombstone at T1 (same identity,
   `is_active='false'`, `metric_value=NULL`) →
   `read_health_data` excludes the key. Asserts that the H2
   relocation of `metric_value IS NOT NULL` lets the NULL tombstone
   still win `ROW_NUMBER` so soft-delete fires.
3. Insert fresh active validation row at T2 (same identity) →
   `read_health_data` returns it again (auto-reactivation by
   newer `run_timestamp`).
4. Two distinct dimensions; deactivate one → other still reads.
5. `read_metric_history_by_partition` honours soft-delete on a
   partition outside the deactivation range (still active).
6. NULL-`is_active` legacy row treated as active (`COALESCE`).
7. **Identity regression test**: insert one collection row + one
   `record_type='soft_delete'` row (the WRONG shape, with no
   validation_name). Assert the collection row remains visible —
   tombstones must use matching `record_type`. This locks the H1
   invariant in test form so a future "let's just write a soft_delete
   row" refactor breaks loudly.
8. **H1 multi-validation test**: a dataset with two validations on
   the same metric. Insert two active validation rows
   (validation_a, validation_b), one active collection row, and a
   `deactivate_metric(metric=...)` tombstone set per H1
   (1 collection tombstone + 2 validation tombstones, all with
   matching `table_name`). Assert:
   - `read_metric_history(table, metric)` returns NO row for that
     partition (collection tombstone wins).
   - `read_metric_history_by_partition(table, metric, anchor)`
     returns NO row.
   - `read_health_data(include_collection=True)` excludes both the
     validation rows AND the collection row.
   - A sibling partition still returns its rows.
9. **`read_latest_run` after deactivation**: deactivate the most
   recent row for a dataset; `read_latest_run` returns None
   (per H8). Insert a fresh active row → it returns again.
10. **Bucketed `read_metric_history(step='P1D')` regression
    (closes R3 finding)**: Insert active row at partition_ts=T0
    in run-bucket B0 (older). Insert NULL tombstone at
    partition_ts=T0 in run-bucket B1 (newer). Call
    `read_metric_history(table, metric, step='P1D')`. Assert
    NO row returned — soft-delete must win across run-time
    buckets, not just within a bucket. Without the two-stage
    rework, the stale active row would surface in B0.
11. JDBC backend: schema-only test (test infrastructure does
    not include a live JDBC). Verifies the `CREATE TABLE`
    includes `is_active`; the missing-column path raises the
    documented `RuntimeError`.

Exit: parametrized matrix green.

### S4 — Selector parser + scope resolver

Files: new `qualifire/core/selectors.py`,
`qualifire/core/backfill.py` (scope only here; loop in S5),
`tests/core/test_selectors.py`.

- `parse_selectors(include, exclude) -> Selector`. Selector
  exposes `matches(dataset_name, validation_name) -> bool`.
- `resolve_scope(config, selector) -> list[ScopedDataset]`.
- `ScopedDataset` carries:
  - `dataset_config: DatasetConfig` (the original)
  - `validations: list[ValidationConfig]` — the in-scope subset
    (always read from `dataset.validations`, regardless of WAP per
    H4; for WAP datasets this is the same list)
  - `is_wap: bool` — `dataset_config.wap is not None`
  - `partition_step: str` — already-resolved cadence at
    construction; raises if H3 cannot resolve.
- Datasets without `partition_ts` yield no `ScopedDataset`; a
  `qualifire.engine` warning is appended to the report (operator
  can see what was skipped and why).
- Empty intersection raises `QualifireConfigError`
  ("no validations matched selectors: ..."), so a typo doesn't
  silently produce a no-op backfill, AND the CLI (which catches
  `QualifireConfigError`) maps it to a clean exit-2.

Acceptance test: `tests/core/test_selectors.py`:
- `*:*` matches everything; `dataset:*` matches one dataset;
  `*:metric_x` matches across datasets; concrete pair matches one.
- Exclude trumps include.
- Empty intersection raises `QualifireConfigError`.
- WAP dataset surfaces `dataset.validations` (the actual list).
- A dataset without `partition_ts` is excluded with a warning
  string captured.
- A dataset with `partition_ts` and unresolvable `partition_step`
  is rejected by S1's config-load step, not by the selector.

Exit: green test file.

### S5 — Backfill loop driver

Files: `qualifire/core/backfill.py`,
`qualifire/core/engine.py` (one new method `_run_backfill`),
`tests/core/test_backfill_loop.py`.

- Build the partition list from `start` → `end` inclusive in
  `partition_step` increments (one cadence per ScopedDataset; if
  datasets disagree on cadence, each dataset iterates on its own).
- **Per-partition data selection (per H2', string-equality form,
  layer-specific predicate injection)**: for each iteration step:
  1. Construct a per-partition `QualifireContext` with
     `ds=<anchor.strftime('%Y-%m-%d')>` (or `<isoformat>` for
     sub-day cadences) merged on top of the base context.
     Existing `{{ ds }}`-templated `filter:` / `query:` /
     `partition_ts:` / `collection.filter:` re-render under this
     overlay.
  2. Generated predicate per H2'. **Always string equality**:
     `(<dataset.partition_ts>) = '<anchor>'`. Never `CAST(... AS
     TIMESTAMP)` — SQLite/pandas evaluates that to integer
     (verified). Eligibility:
     - `partition_ts` contains `{{` → SKIP predicate (Jinja path).
     - Otherwise → generate the string-equality predicate.
  3. Predicate injection layer (per H2', not just
     `DatasetConfig.filter`):
     - **Table dataset (no query, no WAP):** AND-merge the
       predicate into the transient `DatasetConfig.filter` AND
       into every in-scope validation's `collection.filter` (when
       set). The engine prefers `collection.filter or
       filter_expr`; injecting at one level alone leaks at the
       other.
     - **WAP dataset:** transient overlay sets `wap=None,
       table=wap.target_table`; predicate then injects exactly
       like a table dataset.
     - **Query dataset (`DatasetConfig.query`):** wrap as
       `SELECT * FROM (<original_query>) AS _qf_inner WHERE
       <predicate>` ONLY when `partition_ts` names a column
       visible in the inner SELECT; else emit a
       `qualifire.engine` warning row naming the dataset and
       fall back to the context-overlay path.
     - **Custom-query collectors (`CustomQueryCollector`):** no
       filter hook in current code. Do NOT modify the operator's
       query. Emit a `qualifire.engine` warning row when the
       in-scope validation uses `CustomQueryCollector` AND its
       query text contains no `{{ ds }}` literal substring;
       continue execution (operator must template their query
       to be backfill-safe).
  4. Build the transient `DatasetConfig` via `model_copy(update={
     "partition_ts": "<anchor iso>", "validations": <scoped, with
     collection.filter AND-merged>, "wap": None, "table":
     <wap.target_table if is_wap else dataset.table>, "filter":
     <merged>, "query": <inner-wrapped if query dataset>})`.
     Caller's config untouched.
  5. Set `QualifireContext.backfill = True` so row builders stamp
     `details_json.backfill=true` (boolean).
  6. **`skip_if_present` pre-check** (per the shared
     `cache_eligible(validation)` contract — see below). For each
     in-scope eligible validation, `expected_metrics()` is
     queried; ineligible in-scope validators force the partition
     to run live (skip is *not* vacuously satisfied by an empty
     eligible set). For each eligible-validator metric, the
     pre-pass calls `read_collection_metric_at_partition(table,
     metric, anchor_ts, dimension_value=None)`. If every metric
     across every eligible validator hits, the dataset×partition
     is skipped and recorded with `skipped_present=True,
     rows_persisted=0, error=None`. If any eligible metric is
     missing OR any in-scope validator is ineligible, the
     partition runs end-to-end (no partial-skip).
  7. Run `_run_dataset` (collect-only when `validate=False` —
     drives the persistence path without scoring validators).

- **`cache_eligible(validation) -> bool`** (shared between
  `skip_if_present` and `skip_if_cached`):
  - eligible: `ThresholdValidationConfig`,
    `HistoricalValidationConfig`, `ForecastValidationConfig` AND
    `validation.collection.dimensions` is None or empty.
  - ineligible: `SLOValidationConfig`,
    `AnomalyDetectionValidationConfig`,
    `PatternValidationConfig`, OR any validator with non-empty
    `collection.dimensions`.
  - The same predicate gates both skip APIs; H5 implication tests
    in S5 and S8 reference this single function so a future
    refactor can't drift the two paths apart.

- **`expected_metrics()` resolver**: a new method on each
  validation config family in `qualifire/core/config.py`:
  - `ThresholdValidationConfig`, `HistoricalValidationConfig`,
    `ForecastValidationConfig` → return the rule metrics
    (`[r.metric for r in self.rules]`).
  - `SLOValidationConfig` → return `[]` (per H5; SLO does not
    participate in cache).
  - `AnomalyDetectionValidationConfig`, `PatternValidationConfig`
    → return `[]` (per H5).
  - Tested as part of S5 acceptance.

- Capture per-partition `BackfillPartitionOutcome` regardless of
  success / failure.

- **Parallelism (worker isolation)**: with `validate=False`,
  partitions run in `ThreadPoolExecutor(max_workers=parallelism)`
  per dataset. Each worker computes its own
  `BackfillPartitionOutcome` against thread-local state. The
  parent thread merges results in deterministic order
  `(dataset_name, partition_ts ASC)` after all futures complete;
  the summary counts are computed at merge time. Engine warnings
  raised inside a worker are captured into the outcome's
  `warnings` list, not the global `BackfillReport.warnings`
  (which only holds setup-time warnings — datasets skipped
  because `partition_ts` is unset, parallelism downgrade notices,
  etc.).

- **`on_failure='stop'` under parallelism**: the first failed
  outcome triggers `executor.shutdown(cancel_futures=True)` to
  stop pending submissions. In-flight workers run to completion;
  their persisted rows (if any) stay. The report distinguishes
  the failed partition (`error=...`) from cancelled-but-not-run
  partitions (which never appear in the report) and from
  completed-after-cancel partitions (`error=None`). Documented
  in the API docstring.

- **`validate=True` forces parallelism=1** because validators
  that read prior partitions for drift / forecast depend on
  earlier persistence. If the operator passed N>1, the engine
  emits one `qualifire.engine` warning row at backfill setup
  and runs sequentially.

`BackfillReport`:
```python
@dataclass
class BackfillPartitionOutcome:
    partition_ts: datetime
    dataset_name: str
    validations: list[str]   # in-scope validation names
    rows_persisted: int
    duration_seconds: float
    error: tuple[str, str] | None  # (exc_class, message)
    skipped_present: bool

@dataclass
class BackfillSummary:
    total_partitions: int
    succeeded: int
    failed: int
    skipped: int
    rows_persisted: int
    wall_clock_seconds: float

@dataclass
class BackfillReport:
    summary: BackfillSummary
    partitions: list[BackfillPartitionOutcome]
    warnings: list[str]
```

Acceptance test: `tests/core/test_backfill_loop.py`:
- 3-partition window over a stub dataset writes 3 collection rows
  tagged `details_json.backfill=true`.
- `skip_if_present=True` skips a partition that already has an
  active collection row for every expected metric, runs a
  partition that has only a tombstone, runs a partition with a
  partial cache.
- `validate=False, parallelism=4` runs partitions concurrently
  (assert via in-test thread-id capture); merged report is
  ordered by `(dataset_name, partition_ts ASC)` regardless of
  worker completion order.
- `validate=True, parallelism=4` produces a warning row and runs
  sequentially.
- `on_failure='stop'` halts after the first failure.
- `on_failure='stop', parallelism=4`: first worker raises;
  `executor.shutdown(cancel_futures=True)` is called (assert via
  spy / mock); pending partitions don't appear in report; in-flight
  workers complete and their outcomes are present.
- A single-partition exception with `on_failure='continue'` leaves
  prior partitions persisted; the report flags the failed one and
  successors run.
- **Source-selection — table dataset (H2', critical case)**:
  in-memory pandas dataset, source rows for two partitions:
  3 rows at `event_dt='2026-04-01'`, 5 rows at
  `event_dt='2026-04-02'`. Dataset YAML has
  `partition_ts: event_dt`, threshold validator with
  `aggregations: {row_count: 'COUNT(*)'}`, no operator filter.
  Run `qf.backfill(start='2026-04-02', end='2026-04-02',
  validate=False)`. Assert exactly one persisted collection row
  with `partition_ts='2026-04-02'` AND `metric_value=5` (not
  8 — proves the predicate filtered the source, not just
  stamped the partition_ts). Without the H2' generated
  predicate, both partitions are collected and metric_value=8.
- **Source-selection — query dataset**: dataset has
  `query: 'SELECT event_dt, region, amount FROM sales'`,
  `partition_ts: event_dt`. Source has rows in two
  `event_dt` partitions. Backfill at one anchor; assert the
  inner-query wrapping merged the predicate; persisted
  metric_value reflects only the anchor partition.
- **Source-selection — validation `collection.filter`**:
  table dataset, threshold validator with
  `collection.filter: 'amount > 0'`. Source has 5 rows in
  partition_b (3 with amount>0, 2 with amount=0). Backfill at
  partition_b; assert metric_value=3 (filter AND-merged with
  the generated predicate).
- **Source-selection — Jinja-constant case**: dataset has
  `partition_ts: '{{ ds }}'`, operator filter
  `event_dt = '{{ ds }}'`. Run a 1-partition backfill at
  `2026-04-02`; assert source filter rendered correctly,
  generated predicate skipped (Jinja heuristic catches
  `{{ ds }}`), exactly one row persisted with the right value.
- **Source-selection — `custom_query` collector, missing
  template**: validation uses `CustomQueryCollector` with a
  query missing `{{ ds }}`. Backfill emits a
  `qualifire.engine` warning row naming the validation; the
  partition still runs (no partial filter applied). Verify the
  warning row's `details_json.backfill=true` is set.
- **Source-selection — `custom_query` collector, templated
  positive case**: source data has 3 rows in
  `event_dt='2026-04-01'` and 5 rows in `event_dt='2026-04-02'`.
  Validation uses `CustomQueryCollector` with
  `query: "SELECT COUNT(*) AS row_count FROM source WHERE
  event_dt = '{{ ds }}'"`. Run
  `qf.backfill(start='2026-04-02', end='2026-04-02',
  validate=False)`. Assert the persisted row has
  `partition_ts='2026-04-02'` AND `metric_value=5` (proves the
  per-partition Jinja re-render fired correctly, NOT 8 from
  scanning both partitions).
- **Source-selection — WAP case**: WAP-backed dataset with
  `target_table = sales_target`; two partitions of source data
  in `sales_target`. Backfill the WAP scope at one anchor;
  assert NO staging table created, only the anchor partition's
  rows produced the metric value.
- **`cache_eligible` propagation (per H5)**:
  - SLO-only dataset: `skip_if_present=True` runs every
    partition live (eligible set is empty → not vacuously
    skipped).
  - sample-only dataset (anomaly only): same — runs live.
  - Dimensional threshold validator (`dimensions=['region']`):
    `skip_if_present=True` runs live regardless of cache
    contents (ineligible because dimensions is non-empty).
  - Mixed dataset (one eligible threshold + one ineligible
    SLO): runs live because at least one in-scope validator is
    ineligible. Asserts no partial-skip leak.
- **`expected_metrics()` resolver**: parametrized test over
  the six validator families; threshold/historical/forecast
  return rule metrics; SLO/anomaly/pattern return `[]`.

Exit: green test file.

### S6 — `Qualifire.backfill` API surface

Files: `qualifire/api.py`,
`tests/api/test_qualifire_backfill.py`.

- Translates the kwargs into a `Selector` + cadence map +
  scope, hands off to S5.
- Cadence resolution per dataset:
  1. explicit `partition_step` arg (uniform string OR
     `{dataset_name: cadence}` mapping)
  2. `dataset.partition_step` from config
  3. `run.partition_step` from config
  4. `QualifireConfigError` naming the dataset.
- When `notify=True`, the `BackfillReport` is funneled through
  the existing grouped-notification path (one summary message
  per recipient covering the affected partitions). The
  notification row uses `record_type='notification'` (existing
  shape) with `details_json.backfill=true`.

Acceptance test: `tests/api/test_qualifire_backfill.py`:
- Selector round-trip (string and list forms).
- `validate=True, notify=False` runs validators, no notifier
  invoked.
- `validate=True, notify=True` invokes notifier exactly once with
  a grouped payload covering affected partitions.
- `on_failure='continue'` produces a report with `error`
  populated on the failing partition and `error=None` on
  successors.
- `partition_step={"sales_fact": "PT1H"}` overrides config for
  that one dataset (override-only path: dataset YAML cadence is
  P1D, override switches the loop to PT1H).
- **H3 override path**: a config that loaded successfully (every
  dataset has `partition_step` per H3) accepts an explicit
  `partition_step="PT1H"` override; a *legacy* config without
  `partition_step` would have failed `load_config()` already, so
  this path is never reached. Test: load a valid config, pass
  `partition_step="PT1H"`, assert override wins. Separately:
  attempt `load_config()` on a YAML with `partition_ts` but no
  `partition_step` — fails at load time before any override would
  apply. The two tests together prove the override does NOT
  bypass H3.
- `start > end` rejected with ValueError.

Exit: green test file.

### S7 — `Qualifire.deactivate_metric` API (per H1)

Files: new `qualifire/core/deactivate.py`, `qualifire/api.py`,
`tests/api/test_deactivate_metric.py`.

- Resolves affected validations by walking the loaded config's
  dataset (`config.datasets[name]`) and finding every validation
  whose collector emits `metric` (i.e., one of its
  `aggregations`/`expressions` is named `metric`, or its
  `RecencyCollectionConfig.column` matches, or its rules name
  the metric — the resolver lives in `core/deactivate.py` and
  unit-tests every validator family).
- For each partition in `[start, end]` at the resolved cadence,
  emits per H1:
  - 1× `record_type='collection'` tombstone per
    `(table_name, metric, partition_ts, dimension)` (with
    `validation_name=NULL`, `table_name` set to the dataset's
    logical table per H1).
  - N× `record_type='validation'` tombstones, one per validation
    name in the resolved set, each with full natural key.
  - Each row carries
    `is_active='false'`,
    `metric_value=NULL`,
    `validation_status=NULL`,
    `validation_message=<reason or "manually deactivated">`,
    `details_json={"soft_delete": true, "reason": "..."}`.
  - **`run_timestamp` per the H1 read-back-and-bump helper**:
    a single pre-write read computes
    `latest_existing = MAX(run_timestamp)` across the natural-key
    set being tombstoned (one query per dataset, parameterized
    by the natural keys), then every tombstone in the batch
    uses
    `run_timestamp = max(datetime.now(), latest_existing + 1us)`.
    This guarantees the tombstone wins
    `ORDER BY run_timestamp DESC` even when an upstream system
    has already written a future-stamped active row.
- Returns total rows written.
- **`validation` parameter scope**:
  `validation="<name>"` restricts the *validation* tombstones to
  that one name (only one `record_type='validation'` tombstone is
  written per partition × dimension). The single
  `record_type='collection'` tombstone is **always written
  globally** because the metric history is shared across every
  validation that emits the metric — there is no per-validation
  collection identity. This is documented in the API docstring
  and re-stated in the deactivate helper's module docstring.
  Operators who want to deactivate one validation's interpretation
  of a metric without hiding the metric history should NOT use
  `deactivate_metric` — they should mute the validation via
  notify routing instead. S7 acceptance test #6 covers the
  documented behaviour explicitly: `validation="drift_check"`
  hides the drift_check validation row AND the metric history
  cell (because the collection tombstone is global), but leaves
  threshold_check's validation row visible.
  Missing-name → `ValueError`.

Acceptance test: `tests/api/test_deactivate_metric.py`:
- 5-day deactivation over a 1-validation, 1-dimension dataset
  writes 5×(1+1)=10 tombstone rows (one collection + one
  validation per partition).
- `read_metric_history` returns no rows for the deactivated key
  range; sibling partitions unaffected.
- `read_health_data` excludes the validation rows for the
  deactivated range.
- A subsequent `Qualifire.backfill(start=range, end=range)`
  resurfaces the data (auto-reactivation).
- Per-dimension deactivation leaves sibling dimensions active.
- `validation="drift_check"` writes only the drift_check
  validation tombstones; threshold_check stays visible.
- Resolver test: a config with two validations on the same
  metric produces two validation tombstones per partition; a
  config where no validation emits the metric raises
  `ValueError` ("metric X not produced by any validation on
  dataset Y").
- **Future-stamped tombstone race (closes R3 finding)**: insert
  an active collection row whose `run_timestamp` is 1 hour in
  the future relative to wall clock (simulating an upstream
  system with skewed clock or a backfill scheduled ahead).
  Call `qf.deactivate_metric(...)`. Assert the persisted
  tombstone has `run_timestamp >= future_ts + 1us` per the H1
  read-back-and-bump helper, and that subsequent
  `read_metric_history_by_partition` returns no row for the
  deactivated key (the tombstone wins ROW_NUMBER).

Exit: green test file.

### S8 — `skip_if_cached` for normal runs (per H5 + H6)

Files: `qualifire/core/engine.py`, `qualifire/api.py`,
`qualifire/cli.py`, `tests/core/test_skip_if_cached.py`.

- Pre-pass before `_run_dataset`'s collect phase, gated by the
  shared `cache_eligible(validation)` contract from S5/H5:
  - Resolve dataset's `partition_ts`.
  - For each `cache_eligible` in-scope validator (threshold,
    drift, forecast — *non-dimensional only*), call
    `read_collection_metric_at_partition(table_name, metric,
    partition_ts, dimension_value=None)` for each rule metric.
    Compose `dict[(table, metric, partition_ts, None),
    CollectionResult]` of cache hits.
  - Ineligible validators (SLO, anomaly_detection, pattern, OR
    any validator with non-empty `collection.dimensions`) are
    not consulted; their collectors run live as today.
  - Hand the dict to the engine via
    `QualifireContext.cached_metrics`.
  - Eligible-validator collectors check the cache before
    executing SQL: hit → return cached `CollectionResult`;
    miss → execute as today.
  - Inactive (`is_active='false'`) row is treated as cache miss
    by `read_collection_metric_at_partition` already (per H6's
    filter chain).
- **Failure modes & emitted info rows:**
  - Any missing expected metric for an eligible validator →
    that validator falls through to live collect, no cache
    reuse for that validator's metrics.
  - At least one ineligible validator in the dataset's in-scope
    set → engine emits ONE `qualifire.engine` info row naming
    the dataset and listing the ineligible validators that
    re-collected. Tag rule: `details_json.backfill=true` is set
    ONLY when `QualifireContext.backfill is True` (the row builder
    already conditions on that flag — no special case for the
    info row); when `skip_if_cached` is invoked from a normal
    `qf.validate` run, the info row is *not* tagged backfill.

Acceptance test: `tests/core/test_skip_if_cached.py`:
- threshold_check (non-dimensional) on a partition with a cached
  active collection row + `skip_if_cached=True` →
  `Backend.execute_sql` not invoked for that metric; validator
  emits a result row.
- drift_check + cached row + `skip_if_cached=True` → the
  *current* partition's collector reads cache; the *historical*
  lookback still goes through `read_metric_history_by_partition`
  (no caching for history reads).
- forecast + cached → same as drift.
- **Dimensional threshold_check (`dimensions=['region']`) +
  `skip_if_cached=True`** → collector executes live regardless of
  cache contents (ineligible per H5 / `cache_eligible`).
  Per-region values are produced. Asserts dimensional exclusion.
- **Mixed dataset (eligible threshold + dimensional threshold +
  SLO)** + `skip_if_cached=True` → eligible threshold uses
  cache; dimensional + SLO collect live. The engine emits
  exactly one `qualifire.engine` info row naming the ineligible
  validators with `details_json.backfill=true` (when invoked
  through backfill) or with the run's normal record_type
  (when invoked through `qf.validate`).
- shape_check (anomaly) on a partition + `skip_if_cached=True`
  → collector executes (sample-based, H5); emits the engine
  info row.
- SLO + `skip_if_cached=True` → collector executes (H5).
- Missing-metric for an eligible validator → falls back to
  collection path for that validator only.
- `is_active='false'` row is treated as cache miss
  (re-collection fires).
- Default `skip_if_cached=False` → no behaviour change vs main.

Exit: green test file.

### S9 — CLI: `backfill`, `deactivate-metric`, `--skip-if-cached`

Files: `qualifire/cli.py`,
`tests/cli/test_cli_backfill.py`.

- Argparse subcommands wire the API. Match `_cmd_run`'s style:
  `QualifireConfigError` → exit-2 with stderr.
- `backfill`: 0 success, 1 if `on-failure=stop` triggered,
  2 if config / selector preflight fails.
- `deactivate-metric`: 0 success, 1 on storage error, 2 on
  config / cadence preflight.
- `--skip-if-cached` on `run` plumbs through
  `Qualifire.run_config_parsed(skip_if_cached=...)`.
- CLI prints summary on stdout: one line per partition + a
  roll-up. With `--validate --parallelism N` (N>1), the CLI
  warns on stderr but does not error (engine forces N=1; H5).

Acceptance test: `tests/cli/test_cli_backfill.py`:
- Mocked `Qualifire.backfill` invocation captures the parsed
  args.
- `qualifire deactivate-metric --reason "X"` plumbs the reason.
- `qualifire run --skip-if-cached` plumbs the flag.
- `--validate --parallelism 4` warns to stderr and runs (does
  not error).
- `qualifire backfill --validations sales:nope_check
  --start D --end D` exits 2 with the
  `no validations matched selectors` message.

Exit: green test file.

### S10 — End-to-end test

Files: `tests/e2e/test_backfill_and_soft_delete_e2e.py`.

Single test using SQLite system table + an in-memory dataset
(pandas backend; no Spark dependency). Sequence:

1. `qf.validate(table=..., validations=[threshold + drift],
   partition_ts="2026-04-29")` (cold-start: drift cold-starts).
2. `qf.backfill(start="2026-04-22", end="2026-04-28",
   validate=False, parallelism=2)` → assert 7 collection rows
   per metric tagged `details_json.backfill=true`.
3. `qf.validate(... partition_ts="2026-04-30")` → drift now has
   history.
4. `qf.deactivate_metric(dataset=..., metric="row_count",
   start="2026-04-25", end="2026-04-25", reason="load test")` →
   assert that key vanishes from `qf.health_report(days=10)`,
   sibling partitions unaffected,
   `read_metric_history_by_partition` excludes that anchor.
5. `qf.backfill(start="2026-04-25", end="2026-04-25")` → the
   deactivated key reappears, AND assert (a) the new active
   row's `metric_value` matches the source partition's actual
   row count (proves H2' source-selection actually filtered),
   (b) the new row's `run_timestamp` is strictly greater than
   the tombstone's `run_timestamp` (proves rebackfill wins
   ROW_NUMBER), (c) `details_json.backfill=true` on the new
   row.
6. `qf.validate(..., skip_if_cached=True,
   partition_ts="2026-04-30")` → `Backend.execute_sql` spy is
   not called for the threshold metric on that partition;
   validation row is persisted.
7. WAP coverage: define a WAP-backed dataset, run a normal
   `qf.write_audit_publish(...)` against `partition_ts="2026-05-01"`,
   then `qf.backfill(start="2026-04-30", end="2026-04-30",
   validate=False)` over the WAP scope → assert NO staging
   table is created (capture via Spark catalog spy / SQLite
   doesn't have staging tables; so this assertion is via the
   transient `DatasetConfig` having `wap=None` and `table=
   wap.target_table` — checked at the engine level, not at the
   backend).

This test is the single source of truth that the four primitives
compose end-to-end.

Exit: green.

### S11 — Documentation + examples

Files:
- `docs/backfill_and_soft_delete.md` (new)
  - Architecture + recipes.
  - JDBC pre-deploy `ALTER TABLE` SQL with verification queries.
  - Migration story for the `partition_step` config-load break.
- `docs/wap_pattern.md` — append "Backfill" section.
- `docs/configuration.md` — `partition_step` row in the field
  table; H3 statement.
- `docs/programmatic_api.md` — three new examples:
  cold-start, deactivate-metric, skip-if-cached.
- `examples/backfill_quickstart.py` (new) — mirrors S10 against
  SQLite, hand-holds operators.
- `README.md` — one bullet under "What you get" linking to
  `backfill_and_soft_delete.md`.

Exit: visual-grep for broken links; example script runs cleanly
under `python examples/backfill_quickstart.py` against an
in-memory SQLite system table.

## Migration / Rollout

- **`is_active` column.** SQLite / Delta / ExternalCatalog auto-migrate
  via `_ensure_columns` on first `initialize()`. Existing rows are
  NULL; `COALESCE(is_active, 'true') = 'true'` treats them as
  active. Zero behaviour change until an operator runs a
  deactivation.
- **JDBC.** Per H7: pre-deploy DBA `ALTER TABLE`. The exact SQL
  lives in `docs/backfill_and_soft_delete.md` and is reprinted
  verbatim by `JDBCStorage.initialize()` when the column is
  missing. Operators MUST run this before deploying the upgrade
  on JDBC; otherwise `JDBCStorage.initialize()` raises and the
  CLI exits 1 with the migration SQL.
- **`partition_step` config-load.** Configs with `partition_ts`
  and no resolvable `partition_step` will newly fail to load.
  Migration note in `docs/configuration.md` carries the exact
  one-line YAML diff. Release notes call this out as a deliberate
  breaking change for H3.
- **`skip_if_cached`.** Default `False`. Opt-in per call only;
  no YAML knob. Existing configs unaffected.
- **Rollback.** All work is additive (new column, new methods,
  new CLI subcommands). To roll back:
  - Revert the branch.
  - Existing `is_active='true'` and pre-migration NULL rows behave
    identically once the COALESCE-based read is gone (every row
    is treated as active).
  - Tombstones persist as text; harmless once the soft-delete
    read filter is gone.

## Test Strategy (cross-cutting summary)

- **Unit:** S1, S2 (schema + write), S4, S6, S7, S8.
- **Storage matrix:** S2, S3 — parametrized over SQLite, Delta,
  ExternalCatalog. JDBC schema-only assertion + missing-column
  RuntimeError test.
- **Integration:** S5 — collector → storage → reader round-trip
  on SQLite.
- **CLI:** S9 — argparse plumbing only; trust API tests for
  semantics.
- **E2E:** S10 — composes all four primitives.
- **Regression:** the existing test suite must pass. Configs
  whose tests carry `partition_ts` without `partition_step` need a
  one-line YAML update (`partition_step: P1D`). Audit via
  `grep -rn "partition_ts" tests/ docs/ examples/` and patch.

## Risks and Mitigations

1. **Latest-row inversion on read.** Mitigation: H2; S3 acceptance
   test #2 fails any reordering.
2. **Soft-delete tombstone identity drift.** Mitigation: H1; S3
   acceptance test #7 explicitly proves the wrong shape
   (record_type='soft_delete') does NOT tombstone validation rows,
   so the implementation can't drift back to the broken model.
3. **JDBC migration friction.** Mitigation: H7 — fail-fast at
   `initialize()` with copy-paste SQL.
4. **WAP backfill writes to staging.** Mitigation: H4 — backfill
   loop calls `_run_dataset` exclusively against a transient
   config with `wap=None`; S10 asserts no staging.
5. **`skip_if_cached` correctness on stale upstream.** Mitigation:
   H5 limits scope; opt-in only; documented as "use after a clean
   upstream"; tests assert missing-metric → fallback,
   inactive-row → fallback.
6. **`partition_step` config-load break.** Mitigation: H3 +
   migration note + clear error.
7. **Backfill notification storm.** Mitigation: existing grouped
   notification path absorbs to one summary per recipient; CLI
   default is `notify=False`.
8. **Parallel `validate=True` ordering.** Mitigation: forced N=1
   when `validate=True`; warning if operator asked for N>1.
9. **Auto-reactivation surprise.** Mitigation: docs spell out the
   contract — backfill *appends* an active row that becomes
   latest. Use `deactivate-metric` *after* any planned backfill.
10. **`deactivate_metric` resolution gap.** A validator topology
    where the metric isn't named the way the resolver expects
    (e.g., `recency` validators name a column, not an
    aggregation alias) would silently produce zero tombstones.
    Mitigation: S7 acceptance test asserts ValueError when the
    resolver finds zero validations.

## Acceptance Criteria (feature-level)

A reviewer can verify each from a clean checkout:

A1. `Qualifire.backfill(start=T-7, end=T-1, validate=False)`
    over a 1-metric, 1-dimension dataset persists 7 active
    collection rows, all carrying `partition_ts` equal to the
    iteration value and `details_json.backfill=true` (boolean).
A2. `Qualifire.deactivate_metric(...)` writes one collection
    tombstone + N validation tombstones per partition. After
    deactivation, `Qualifire.health_report(days=10)` excludes
    that key set.
A3. A subsequent `Qualifire.backfill(...)` resurfaces the key
    (auto-reactivation by newer `run_timestamp`).
A4. `qualifire backfill --validate --parallelism 4` emits a
    `qualifire.engine` warning row noting parallelism was forced
    to 1.
A5. `qualifire run --skip-if-cached` against a partition with a
    cached active collection row does not call the
    `Backend.execute_sql` collector for the metric; the
    validation result is persisted.
A6. SQLite, Delta, ExternalCatalog all carry the `is_active`
    column after `initialize()` on a pre-existing table created
    with the prior DDL.
A7. A config with `partition_ts` and no resolvable
    `partition_step` fails `qualifire validate-config` with an
    error message naming the dataset (H3).
A8. `read_metric_history_by_partition` returns no row for a
    deactivated partition; the same call against a sibling
    partition still returns the active row.
A9. WAP-backed dataset backfill calls `_run_dataset` against a
    transient config whose `wap is None` and whose
    `table = wap.target_table`; no staging table is created.
A10. `examples/backfill_quickstart.py` runs end-to-end against a
     fresh SQLite store and prints a `BackfillReport` summary.

## Progress log

(Filled during implementation; one bullet per session.)

- 2026-05-08 — plan drafted; R1 adversarial + R1 codex review
  completed; revised to fix tombstone identity (H1), WAPConfig
  shape (H4), cache-cell key (H6), skip_if_cached scope (H5),
  partition_step invariant (H3), JDBC migration timing (H7),
  row-builder write site.
- 2026-05-08 — R2 adversarial + R2 codex review completed;
  revised to add per-partition source selection (H2'),
  metric_value-NOT-NULL filter relocation under H2, cache method
  contract precision (H6 method signature pinned), partition_step
  override semantics (H3 explicit override scope, S6 acceptance
  test), parallel-report ordering and `on_failure='stop'` cancel
  semantics (S5), `read_latest_run` post-deactivation contract
  (H8), `expected_metrics()` resolver pin, additional S1 tests
  for run-level partition_step.
- 2026-05-08 — R3 adversarial + R3 codex review completed;
  revised H2' to handle predicate injection at every collector
  layer (table / WAP / query / custom_query), drop CAST and use
  string equality for cross-backend portability, add bucketed
  `read_metric_history` two-stage rework so soft-delete wins
  across run-time buckets, scope `skip_if_cached` /
  `skip_if_present` to non-dimensional validators only,
  strengthen the H1 ordering invariant to read-back-and-bump
  tombstone `run_timestamp`, document `validation=`-scoped
  deactivate semantics (validation row scoped, collection row
  global).
- 2026-05-08 — R4 codex review completed; propagated H-section
  fixes into S5/S7/S8 step instructions (predicate injection
  layers, string equality form, `cache_eligible` shared
  contract, H1 read-back-and-bump in S7); added S3 bucketed
  regression test, S5 query/collection.filter/custom_query/H5
  propagation tests, S7 future-stamped tombstone test, S8
  dimensional-exclusion + mixed-dataset tests, S10 metric-value
  + run_timestamp ordering assertions.
- 2026-05-08 — R5 codex review (CONDITIONAL PASS direction);
  added the missing positive `CustomQueryCollector` test that
  asserts metric value isolation (not just the warning path),
  tightened the S8 backfill-tag rule to "only when
  QualifireContext.backfill is True" so backfill-context info
  rows stay distinguishable from normal-run info rows.
