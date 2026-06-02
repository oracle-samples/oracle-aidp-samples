---
id: metrics-backfill-and-soft-delete
name: Metrics Backfill and Soft-Delete
type: Feature
priority: P1
effort: Large
impact: High
created: 2026-05-06
---

# Metrics Backfill and Soft-Delete

## Problem Statement

Three real workflows are blocked today because qualifire only persists
metrics on the *current* run path:

1. **Cold-start onboarding.** A team with an existing dataset adopts
   qualifire and immediately wants drift / forecast / shape checks —
   but those checks need history. They have to wait N partitions of
   normal runs before the validators actually compare anything; until
   then every check trips the cold-start fallback. New adopters
   currently can't get *historical comparisons* working without a
   manual scripted backfill outside the framework.

2. **New validation introduced on an existing dataset.** Adding a
   drift / forecast / shape check for a metric that wasn't previously
   collected leaves it with no baseline. The check is dead weight
   until enough new partitions accrue. Operators want to populate the
   missing history from the source data without re-running every
   validation that was already passing.

3. **Source data was backfilled or fixed.** When upstream rewrites a
   week of partitions (late-arriving data, schema fix, dedup), the
   metrics qualifire previously persisted are stale. There's no way
   to recompute them. Worse, those stale rows continue to drive drift
   baselines and alert suppression — making the system actively
   misleading until enough fresh runs eventually push them out of the
   history window.

A fourth, related, problem:

4. **Bad metrics can't be suppressed.** When a partition produced a
   metric value that operators know is wrong (load-test traffic,
   incident-induced anomalies, data-corruption window before the
   fix), there's no surgical way to exclude that single row from
   downstream baselines. The append-only system table forbids
   in-place edits, and the validators have no notion of "ignore this
   partition's metric."

A fifth, related, problem (collected during R3 review):

5. **Re-running a partition wastes work.** A regular daily run that
   re-executes against a partition the system table already has —
   maybe a retry after a notification flake, maybe a backfill caught
   up to "today" — re-collects every metric even though the rows
   exist. There's no `skip-if-cached` mode for normal (non-backfill)
   runs. Operators currently work around this by adding YAML guards
   or coordinating retries manually.

A sixth (WAP-specific):

6. **WAP backfill needs to read from `target_table`, not staging.**
   The standard WAP flow writes to a staging table, audits against
   staging, and publishes via `INSERT INTO target FROM staging`.
   Backfilling a WAP-backed dataset has no staging — the data is
   already in `target_table`. The backfill loop must collect from
   `target_table` filtered by partition_ts, **not** invoke the
   write/staging legs of WAP.

## Proposed Solution

Two primitives, captured under one feature because they're part of
the same operator workflow ("fix the historical metrics"):

**Backfill** — a per-partition collect-only loop over a date range.
Same collectors and partition_ts plumbing as a regular run, but
iterating partitions explicitly rather than running once for "now".
Defaults to collect-only (no validation, no notifications) so it's
safe to run against a fresh system table without paging on-call.

**Soft-delete** — a column on the system table (`is_active`,
default `true`) and a tiny CLI/API to mark `(dataset, metric,
partition_ts, dimension)` rows inactive over a range. Read paths
honour `is_active=false` on the *latest* row per key; subsequent
backfills or normal runs re-activate naturally because their fresh
rows become the latest.

Both primitives need to know the dataset's partition cadence
(`partition_step`, e.g. `P1D`, `PT1H`). That field exists at
runtime today only as ad-hoc `compare.step` on individual
validators. Promote it to a first-class `DatasetConfig` field so the
backfill loop and any future cadence-aware tool reads it from the
single source of truth.

## Affected Areas

- core (DatasetConfig.partition_step, validation rules)
- engine (new backfill execution loop, collect-only mode)
- storage (is_active column, two-step "latest then filter" reads
  across SQLite / Delta / ExternalCatalog / JDBC)
- api (Qualifire.backfill, Qualifire.deactivate_metric)
- cli (qualifire backfill, qualifire deactivate-metric subcommands)
- wap (publish contract documentation; backfill resolves into
  WAPConfig.validations)
- reporting (dashboards filter inactive rows)
- docs (architecture note, README, getting-started)

## Decision Pins

(Captured here so they survive into `/feature-plan`. Each one
reflects an architectural call already vetted in the design
conversation; the plan should treat these as inputs, not open
questions.)

1. **Two surfaces, one primitive.** `Qualifire.backfill(...)` Python
   API and `qualifire backfill` CLI subcommand are the same code
   path; CLI is a thin arg-parser around the API. **No** YAML
   `backfill:` block — backfill is a workflow, not a property of a
   dataset. Operators pass dates and selectors at invocation time.

2. **Selector syntax: `dataset_name:validation_name` strings with
   `*` wildcards.** `sales_fact:drift_check`, `sales_fact:*`,
   `*:drift_check`, `*:*`. CLI uses `--validations` (canonical) and
   `--exclude` flags accepting comma-separated strings. API uses
   `include=` and `exclude=` kwargs accepting str | list[str]. Both
   default to `None` ⇒ everything in the loaded config.

3. **`partition_step` is mandatory when `partition_ts` is set** and
   forbidden otherwise. New `DatasetConfig.partition_step: str |
   None` field, validated by `parse_duration` (ISO-8601 + legacy
   compact). Resolution at backfill time: explicit CLI/API arg
   first (uniform value or `dataset:value` per-dataset map), then
   YAML `DatasetConfig.partition_step`. No hardcoded default. No
   auto-detection from system-table intervals (out of scope this
   feature; revisit later). Datasets without `partition_ts` aren't
   backfillable — emit a `qualifire.engine` warning and skip.

4. **Idempotency: append by default.** System table is append-only
   and `latest-row-per-key` semantics already work. Optional
   `--skip-if-present` (and `skip_if_present=True` API kwarg) opts
   into dedup-by-key for cheap re-runs.

5. **Default `validate=False`, `notify=False`.** Backfill is a
   collect-only loop unless explicitly told otherwise. When
   `validate=True`, parallelism is forced to 1 because validation
   over partition P may depend on prior partitions seeded earlier in
   the same backfill. With `validate=False`, `--parallelism N` is
   honoured.

6. **`BackfillReport` return value.** Per-partition outcomes (rows
   persisted, exception class + message, wall-clock duration) plus
   roll-up summary. When `notify=True`, report goes through the
   existing notification channels configured on the validations
   in scope. CLI prints the summary; non-zero exit on failure +
   `on-failure=stop`.

7. **Audit tag.** Every backfill-emitted row gets
   `details_json.backfill = true`. Read paths still pick the latest
   row regardless; the tag is informational for dashboards.

8. **WAP backfill = same primitive, target-table-sourced.** Selectors
   resolving into `WAPConfig.validations` collect from `target_table`
   filtered by `partition_ts = anchor` at each partition. **No**
   staging write during backfill, **no** publish step — the data is
   already in `target_table`. The backfill loop reuses the regular
   per-partition collect path with `target_table` swapped in for the
   source. Important corollary: validations defined under
   `WAPConfig.validations` must be portable between staging and
   target shapes (they typically are; both are the same logical
   table).

11. **Skip-if-cached for non-backfill runs.** A normal `qf.validate`
    that runs against a partition_ts already represented in the
    system table (any fresh row with `is_active=true` for the
    `(dataset, metric, partition_ts, dimension)` keys this run would
    write) skips the collection-only work and reuses the cached
    metric values for validation. Opt-in via
    `qf.validate(..., skip_if_cached=True)` and a
    `--skip-if-cached` CLI flag — default is current behaviour
    (always re-collect) so the change is non-breaking. Validators
    still execute against the cached rows, so threshold / drift /
    forecast still alert as expected; the savings is the
    collector pass on the source table.

9. **Document WAP publish-phase contract.** What `mode`,
   `partitionBy`, and `spark.sql.sources.partitionOverwriteMode=
   dynamic` do during the publish-INSERT step. Goes into the
   architecture note + README WAP section. SQL-driven WAP gaining
   write-options parity is **out of scope** this feature.

10. **Soft-delete via `is_active` column.**
    - New TEXT column (storing `'true'` / `'false'` for portability
      across all four storage backends; `parse-as-bool` on read).
    - Default `'true'` on every existing and new row.
    - All read paths (`read_metric_history`,
      `read_metric_history_by_partition`, `read_validation_history`,
      `read_validation_history_bulk`, `read_health_data`,
      `read_latest_run`) migrated to a two-step pattern:
      `ROW_NUMBER() OVER (...)` to pick latest per key, **then**
      filter `COALESCE(is_active, 'true') = 'true'`. Filtering
      before the row-number step would surface stale active rows
      under a deactivated marker.
    - Dashboards (interactive + matplotlib + plotly) inherit
      filtering automatically because they consume `read_health_data`.
    - Surface: `qualifire deactivate-metric --config x.yml --dataset
      X --metric M --start D1 --end D2 [--dimension D]
      [--reason "..."]` CLI; `qf.deactivate_metric(...)` API.
      Reason captured in `details_json` and the row's
      `validation_message`.
    - Re-activation is automatic — the next collection (backfill or
      normal) appends an `is_active=true` row that becomes latest.

## Additional scope (captured 2026-05-08, post-foundation)

Three config-shape concerns surfaced during the WAP backfill design
discussion on the end-to-end demo notebook. They're load-bearing for
the deferred follow-up work — the dict form especially makes
"backfill an aggregation by metric name" semantically clean — so they
land alongside the rest of the continuation, not as separate features.

### A. Dict-form `expressions` (replace list form, no compat shim)

Old shape (drop entirely):
```yaml
expressions:
  - "COUNT(*) AS row_count"
  - "AVG(amount) AS avg_amount"
```

New shape:
```yaml
expressions:
  row_count: "COUNT(*)"
  avg_amount: "AVG(amount)"
```

Rationale:
- The backfill loop needs metric names addressable as keys to map
  "the metric originally named `combined_revenue`" → "the expression
  that re-derives it from `target_table`". Today the mapping is
  embedded in a `<expr> AS <name>` string the collector lexes.
- `Qualifire.threshold_check(aggregations={'rows': 'COUNT(*)'})`
  already takes a dict; YAML is the inconsistent surface.
- Drops the `<expr> AS <name>` lexer entirely.

Applies to every collector that takes `expressions:` —
`AggregationCollectionConfig`, `CustomQueryCollectionConfig.metrics`
if the same shape is used, anywhere else the list-of-strings pattern
recurs. Every YAML in `docs/examples/`, `examples/industries/*/`,
`tests/manual/configs/`, and any test fixture migrates in the same
PR. Builder API stays as-is (already dict-shaped).

### B. `write_sql_file` on `WAPConfig`

```yaml
wap:
  target_table: "revenue.daily_summary"
  write_sql_file: "configs/d3_write.sql"
```

Mutually exclusive with `write_sql` and `df`, validated at
config-load time alongside the existing R9 mutual-exclusion check.
File is read + Jinja-rendered the same way inline `write_sql` is.
Useful for non-trivial multi-CTE WAP write queries that are awkward
to inline in YAML.

### C. WAP audit phase honours dataset-level `partition_ts`

`engine._run_wap` builds the staging `DatasetConfig` from
`(name, table=staging_view, validations)` only — the dataset's own
`partition_ts`, `partition_step`, `description`, `dimensions`, and
`measures` are dropped, forcing operators to set `partition_ts` at
the run level. Fix: propagate those fields into the staging config
so per-dataset overrides work for WAP too. A YAML with two WAP
datasets each backed by different target tables should be able to
set distinct `partition_ts` on each without needing a run-level
fallback at all.

This is small (one `DatasetConfig(...)` constructor call inside
`_run_wap` gains a few keyword arguments) but it removes a
real-world footgun — the end-to-end demo's WAP YAML had to set
`partition_ts` at the run level only because of this gap.

### D. WAP publish via DataFrame writer (auto-create target)

The current publish phase uses raw SQL:

```python
# qualifire/wap/pattern.py: WAPExecutor.publish
mode = self.write_options.get("mode", "append")
sql = (f"INSERT OVERWRITE TABLE {target} SELECT * FROM {staging}"
       if mode == "overwrite" else
       f"INSERT INTO {target} SELECT * FROM {staging}")
self.backend.execute_sql(sql)
```

Two real friction points this causes:

- **Target must pre-exist.** Operators bootstrapping a new WAP
  pipeline have to write target DDL by hand (the end-to-end demo
  notebook had to `CREATE TABLE revenue.daily_summary (...)` before
  the first WAP run could publish). For datasets where the target
  schema is wholly determined by the staging shape, that DDL is
  pure boilerplate.
- **Most `write_options` are ignored at publish time.**
  `partitionBy`, `format`, format-specific options like Delta's
  `mergeSchema` — silently dropped. Only `mode` survives, mapped
  to `INSERT INTO` vs `INSERT OVERWRITE`. Decision pin #9 already
  flagged this asymmetry as out-of-scope for the foundation slice.

Fix: route the publish through the DataFrame writer for
Spark-capable backends:

```python
# Sketch — final shape lives in the plan
def publish(self) -> None:
    spark = getattr(self.backend, "spark", None)
    if spark is None:
        return self._publish_via_sql()      # Pandas-style: SQL path

    df = spark.table(self.staging_table)
    writer = df.write
    fmt = self.write_options.get("format")
    if fmt: writer = writer.format(fmt)
    writer = writer.mode(self.write_options.get("mode", "append"))
    pb = self.write_options.get("partitionBy")
    if pb: writer = writer.partitionBy(*(pb if isinstance(pb, list) else [pb]))
    for k, v in self.write_options.items():
        if k in ("format", "mode", "partitionBy"): continue
        writer = writer.option(k, v)

    if not self.allow_create and not self.backend.table_exists(self.target_table):
        raise WAPPublishError(
            f"Target {self.target_table} doesn't exist and allow_create=False. "
            f"Pre-create it with the expected schema, or set wap.allow_create=true."
        )
    writer.saveAsTable(self.target_table)
```

Wins:
- `saveAsTable` auto-creates the target on first publish (when
  `allow_create=true`) — schema inferred from staging, format /
  partitionBy / write options all honoured.
- Subsequent publishes append/overwrite the existing table the
  same way the SQL path does today.
- DataFrame-driven WAP path stops paying the staging-table
  round-trip for publish (it already has the DataFrame in hand).

Behaviour to preserve:
- **Schema-strict by default.** `saveAsTable + mode=append` against
  an existing table fails when staging has new columns unless an
  explicit option (`mergeSchema=true` for Delta, etc.) is set.
  That matches the current `INSERT INTO` strict-schema contract.
- **Pandas-style backends keep the SQL path** — no `.spark`, no
  DataFrameWriter. They retain the current strict
  "target-must-exist" contract for now.
- **`mode=overwrite` interaction with
  `spark.sql.sources.partitionOverwriteMode=dynamic`** is unchanged
  — same partition-vs-whole-table semantics as `INSERT OVERWRITE
  TABLE`. Documented in the architecture note (decision pin #9
  already covers this).

New config field:
- `WAPConfig.allow_create: bool = True`. Default true so green-field
  WAP pipelines work without upfront DDL. Operators in catalog-/RBAC-
  controlled environments who require the target to be governed
  externally set it false to restore the strict "target must
  pre-exist" contract.

Decisions specific to D (locked in 2026-05-08, plan phase to
refine implementation):

- **D1. `WAPConfig.allow_create` defaults to `true`.** Friendlier
  for new operators; the end-to-end demo notebook would no longer
  need its upfront `CREATE TABLE`. Existing pipelines whose target
  was hand-created with a deliberate schema continue to work
  unchanged (`saveAsTable` against an existing table appends per
  the chosen `mode`). Operators who want the strict
  "fail if target missing" guard set `allow_create=false`.
- **D2. Schema-strict policy follows the per-format default,
  overridable via `write_options`.** Don't force a single
  schema-strict contract across formats — let Delta default to
  strict (rejects new columns), let Parquet/Hive default to
  permissive, etc. Operators who want stricter or looser
  behaviour pass the appropriate option in `write_options`
  (`mergeSchema: true` for Delta merge, `overwriteSchema: true`
  for full reshape, format-specific equivalents otherwise). Less
  code, more idiomatic Spark, predictable per-format.
- **D3. Always use `saveAsTable`** (Spark-capable backends only;
  Pandas-style backends keep the SQL fallback). One code path
  for first and subsequent publishes — `mode=append` /
  `mode=overwrite` handle the difference. No first-publish-CTAS-
  then-INSERT split: the operational benefit of the split
  ("strict insert after bootstrap") is already covered by D2's
  per-format defaults plus explicit `write_options` overrides.

## Context

- Builds directly on the just-shipped `partition_ts` work (every
  persisted row already carries the partition identity needed for
  partition-anchored history reads).
- Storage column-set additions ride the existing `_ensure_columns`
  migration path so SQLite / Delta / ExternalCatalog tables
  auto-upgrade on next `initialize()`. JDBC creates the column on
  initial table create only — operators on existing JDBC tables
  add the column via DBA ticket; document the contract.
- The two manual notebooks (`dashboard_html.ipynb`,
  `dashboard_charts.ipynb`) need to inherit the active-only filter
  through their use of `read_health_data`. Already satisfied by
  storage-layer migration; verify via test rather than touching
  notebook code.
