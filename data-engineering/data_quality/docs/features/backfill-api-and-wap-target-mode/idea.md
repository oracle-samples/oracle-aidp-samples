---
id: backfill-api-and-wap-target-mode
name: Backfill API completion + WAP target-mode + config ergonomics
type: Feature
priority: P1
effort: Large
impact: High
created: 2026-05-08
---

# Backfill API completion + WAP target-mode + config ergonomics

## Problem Statement

Continuation of
[`metrics-backfill-and-soft-delete`](../metrics-backfill-and-soft-delete/idea.md),
whose foundation slice shipped on 2026-05-08 (`partition_step` field,
`is_active` storage column, SQLite read paths honouring it,
`expected_metrics()` resolver, `QualifireContext.backfill` /
`cached_metrics`). The user-facing primitives the foundation plan
designed but explicitly deferred are still unshipped, and a separate
WAP-design discussion (during the end-to-end demo notebook work)
surfaced four config-shape concerns that are load-bearing for the
deferred work — the dict-form `expressions:` change in particular
makes the backfill loop's "re-derive metric `X` from target" semantics
clean. Bundling these into one feature keeps the engine changes,
config schema changes, API surface, CLI surface, and docs aligned in
one PR rather than churning each path twice.

Three problem clusters, all hitting the same code paths:

### 1. The deferred follow-up scope from the foundation feature

`docs/features/metrics-backfill-and-soft-delete/shipped.md` lists
S2-S11 as "Follow-up scope (NOT shipped here)":

- **S2/S3 — `is_active` honoured on Delta, ExternalCatalog, JDBC
  reads.** Today soft-delete works only on SQLite; the `is_active`
  column exists in the shared schema but the other three backends'
  read SQL bodies ignore it and don't implement
  `read_collection_metric_at_partition`.
- **S4 — selector parser + `ScopedDataset` resolver**
  (`qualifire/core/selectors.py`).
- **S5 — backfill loop driver + `_run_backfill` + per-partition
  source selection (H2'). `expected_metrics()` resolver is shipped;
  the loop that consumes it is not.**
- **S6 — `Qualifire.backfill` API + `BackfillReport` dataclasses.**
- **S7 — `Qualifire.deactivate_metric` API +
  `qualifire/core/deactivate.py` (with the H1
  read-back-and-bump helper).**
- **S8 — `skip_if_cached` engine pre-pass.** The API kwarg on
  `Qualifire.validate` exists and the cache-eligible predicate is
  shipped, but the engine doesn't yet consult
  `QualifireContext.cached_metrics` from collectors.
- **S9 — `qualifire backfill` / `qualifire deactivate-metric` /
  `--skip-if-cached` CLI subcommands.**
- **S10 — end-to-end test composing all four primitives.**
- **S11 — `docs/backfill_and_soft_delete.md` (architecture +
  JDBC pre-deploy `ALTER TABLE` SQL),
  `examples/backfill_quickstart.py`, and the README /
  `programmatic_api.md` / `configuration.md` cross-links.**

The foundation plan's H1-H8 invariants and the eleven step exit
criteria remain authoritative. These can ship in follow-up commits
without re-opening that design.

### 2. WAP-target-mode backfill (new — emerged from the WAP design discussion)

Backfilling a WAP-backed dataset by re-running its `write_sql` is
wasteful or impossible:

- **Cost.** `write_sql` typically scans the source tables (often
  multi-billion-row event logs) to roll up one partition. For a
  dataset whose target is a daily one-row summary, recomputing from
  source to refresh a single metric is orders of magnitude more
  expensive than reading the already-published target row.
- **Source retention.** Source tables routinely have shorter TTLs
  than analytics targets. The rolled-up summary may live for years;
  the raw events that produced it are gone after 30 days. There's
  no source-side path to recompute, even if cost weren't an issue.
- **Idempotency.** When the target has been corrected post-publish
  (manual fix, reprocessing), the metric "as currently in target" is
  the answer the dashboard should reflect — re-running `write_sql`
  ignores that correction.

The user-confirmed contract for WAP backfill: `target_table`'s
schema is identical to staging's (WAP publishes via
`INSERT INTO target SELECT * FROM staging`), so every aggregation
expression an operator wrote against staging is by definition
backfill-safe against `target_table WHERE partition_column = P`.
No "this expr isn't backfillable" carve-out is needed — every
collector that runs forward runs in backfill too.

The continuation needs to:

- Add a new entry point — `qf.backfill(...)` (S6 above) that for WAP
  datasets reads `target_table` filtered to the partition rather
  than re-running WRITE/PUBLISH.
- Resolve a `partition_column` per WAP dataset (explicit `WAPConfig`
  field) so the partition filter is unambiguous.
- Support both **metrics backfill** (default — re-derive metrics
  from current target state, no re-write) and **data backfill**
  (opt-in `data=True` — full WRITE→AUDIT→PUBLISH cycle on a past
  partition).
- Cover sample-based validators (pattern, shape) as well as
  aggregation. They read raw row samples in forward execution; in
  backfill, past slices come from `target_table` when available
  and skip with a structured "no historical samples" report entry
  when they aren't.

### 3. Config-shape ergonomics that block clean backfill semantics

Four config-shape concerns surfaced during the WAP backfill
discussion. They're load-bearing for the deferred work — the
dict form in particular makes "backfill an aggregation by metric
name" semantically clean — so they land alongside the rest of the
continuation, not as separate features.

**A. Dict-form `expressions:` (replace list form, no compat shim).**

```yaml
# Old (drop entirely):
expressions:
  - "COUNT(*) AS row_count"
  - "AVG(amount) AS avg_amount"

# New:
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
if the same shape is used, anywhere else the list-of-strings
pattern recurs. Every YAML in `docs/examples/`,
`examples/industries/*/`, `tests/manual/configs/`, and any test
fixture migrates in the same PR. Builder API stays as-is (already
dict-shaped).

**B. WAP source rename (`write_sql` → `sql`) + `sql_file:`.**

```yaml
wap:
  target_table: "revenue.daily_summary"
  sql_file: "configs/d3_write.sql"
```

Two changes bundled because they touch the same field set:

1. **Rename** `WAPConfig.write_sql` → `WAPConfig.sql`. The
   `write_` prefix is misleading (the value is a SELECT — qualifire
   does the writing) and verbose. `sql` is short and accurate. We
   reject `query` because `DatasetConfig.query` already exists for
   the alternate-source case; reusing the name on a different
   class is a footgun.
2. **Add `sql_file:`** as a third source form. Mutually exclusive
   with `df` and `sql` (three-way), validated at config-load time
   alongside the existing R9 check. File is read at config-load
   time, contents cached, then Jinja-rendered the same way inline
   `sql` is. Useful for non-trivial multi-CTE WAP write queries
   that are awkward to inline in YAML.

The rename is a clean break with no compat alias — matches
foundation decision pin #5 (no shim layers). 110 source occurrences
of `write_sql` migrate in lockstep with the field rename.

**C. WAP audit phase honours dataset-level `partition_ts`.**

`engine._run_wap` builds the staging `DatasetConfig` from
`(name, table=staging_view, validations)` only — the dataset's own
`partition_ts`, `partition_step`, `description`, `dimensions`, and
`measures` are dropped, forcing operators to set `partition_ts` at
the run level. Fix: propagate those fields into the staging
config so per-dataset overrides work for WAP too. A YAML with two
WAP datasets each backed by different target tables should be
able to set distinct `partition_ts` on each without needing a
run-level fallback at all.

This is small (one `DatasetConfig(...)` constructor call inside
`_run_wap` gains a few keyword arguments) but it removes a
real-world footgun — the end-to-end demo's WAP YAML had to set
`partition_ts` at the run level only because of this gap.

**D. WAP publish via DataFrame writer (auto-create target).**

The current publish path (`qualifire/wap/pattern.py:WAPExecutor.publish`)
uses raw SQL `INSERT INTO/OVERWRITE TABLE`. Two friction points:

- **Target must pre-exist.** Operators bootstrapping a new WAP
  pipeline have to write target DDL by hand (the end-to-end demo
  notebook had to `CREATE TABLE revenue.daily_summary (...)` before
  the first WAP run could publish). For datasets where the target
  schema is wholly determined by the staging shape, that DDL is
  pure boilerplate.
- **Most `write_options` are ignored at publish time.**
  `partitionBy`, `format`, format-specific options (Delta's
  `mergeSchema`) — silently dropped. Only `mode` survives, mapped
  to `INSERT INTO` vs `INSERT OVERWRITE`.

Fix: route the publish through the DataFrame writer for
Spark-capable backends:

```python
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
    raise WAPPublishError("Target doesn't exist and allow_create=False")
writer.saveAsTable(self.target_table)
```

Pandas-style backends (no `.spark`) keep the SQL path.

## Why this matters

- **Operators can complete the workflows the foundation was designed
  for.** Cold-start onboarding, adding a new validator on an existing
  dataset, recovering from upstream backfills are all real recurring
  requests. The foundation made them *possible*; this feature makes
  them *runnable*.
- **WAP-target backfill is the difference between "feasible" and
  "infeasible" for several real users.** Multiple internal teams
  have WAP pipelines whose source side is on a 30-day TTL — they
  have no recourse today and will get one.
- **Soft-delete only works on SQLite today.** Operators on
  AIDP-style external catalogs, Delta lakehouses, and JDBC-backed
  system tables have the column but not the read-side enforcement.
  S2/S3 closes that gap.
- **Config consistency reduces footguns** during the same operator
  session where they'd otherwise be context-switching between dict
  (programmatic) and string (YAML) shapes for the same concept.

## Sub-feature scope buckets

These are deliberately separate so the plan can phase / land them
independently. The WAP-target-mode work (E) depends on dict-form
expressions (A) being in. Storage parity (S2/S3) is independent and
can ship first.

| ID | Scope | Source |
|---|---|---|
| A | Dict-form `expressions:` everywhere; drop list form | post-foundation |
| B | `write_sql_file:` on `WAPConfig` | post-foundation |
| C | WAP audit propagates dataset-level `partition_ts` etc. | post-foundation |
| D | WAP publish via DataFrame writer + `allow_create=True` | post-foundation |
| E | `Qualifire.backfill(...)` API + `BackfillReport` + WAP target-mode | foundation S6 + new |
| F | `Qualifire.deactivate_metric(...)` API + `core/deactivate.py` | foundation S7 |
| G | `qualifire backfill` / `deactivate-metric` / `--skip-if-cached` CLI | foundation S9 |
| H | Engine `_run_backfill` loop driver + per-partition source select (H2') | foundation S5 |
| I | `core/selectors.py` selector parser + `ScopedDataset` resolver | foundation S4 |
| J | `skip_if_cached` engine pre-pass consumption | foundation S8 |
| K | `is_active` honoured on Delta / ExternalCatalog / JDBC reads | foundation S2/S3 |
| L | E2E test composing all primitives | foundation S10 |
| M | Docs (`docs/backfill_and_soft_delete.md`, `examples/backfill_quickstart.py`, cross-links) | foundation S11 |

## Captured Constraints (decision pins for the plan)

These pin decisions already vetted in the design conversation; the
plan should treat them as inputs, not open questions.

1. **Default mode of `qf.backfill(...)` is metrics-only**
   (`data=False`). For WAP datasets it reads `wap.target_table`
   filtered to the partition; non-WAP reads `dataset.table`
   filtered the same way forward execution would. WRITE / PUBLISH
   are skipped. `data=True` opts into the full WRITE→AUDIT→PUBLISH
   cycle on a past partition.
2. **Symmetric expr contract for WAP target-mode.** Target.schema
   ≡ staging.schema by WAP construction (publish is
   `INSERT INTO target SELECT * FROM staging`); every aggregation
   expression the operator wrote against staging is backfill-safe
   against `target_table WHERE partition_col = P`. No
   "non-backfillable" carve-out — if the target was updated
   post-publish, the current-target-state value is the answer the
   dashboard should reflect.
3. **`WAPConfig.partition_column` is explicit, required for
   target-mode backfill.** Auto-detection from `partition_ts`
   expression is plan-phase open question; the safe shape is
   "explicit field, plan can add an auto-detect later if there's
   demand."
4. **Sample collectors in backfill mode** read past slices from
   target where available; raw-source-only past slices yield a
   "skipped: no historical samples available" entry in the report
   rather than silently passing. Pattern / shape AUC values
   surface on the dashboard for backfilled partitions when the
   target carries enough history.
5. **Dict-form `expressions:` replaces list form outright.** No
   compat shim. Every YAML in the repo migrates in the same PR.
   Builder API already dict-shaped — only YAML changes.
6. **`WAPConfig.write_sql_file` is mutually exclusive with
   `write_sql` and `df`.** Existing R9 mutual-exclusion validator
   gains the third member. File contents are read + Jinja-rendered
   the same way `write_sql` is.
7. **`engine._run_wap` propagates dataset-level fields into staging
   config.** `partition_ts`, `partition_step`, `description`,
   `dimensions`, `measures` all flow through. Removes the
   run-level fallback footgun.
8. **`WAPConfig.allow_create: bool = True`** by default
   (decision D1 from the WAP-publish discussion). Operators in
   catalog-/RBAC-controlled environments set it false to restore
   the strict "target must pre-exist" contract.
9. **Schema-strict policy on WAP publish follows the per-format
   default** (decision D2). Don't force a single contract across
   formats; let Delta default to strict, Parquet/Hive default to
   permissive, and let `write_options` operators override
   (`mergeSchema=true` etc.). Less code, more idiomatic Spark.
10. **Always `saveAsTable`** for WAP publish on Spark-capable
    backends (decision D3). One code path for first and subsequent
    publishes — `mode=append` / `mode=overwrite` handle the
    difference. Pandas-style backends keep the SQL fallback.
11. **Backfill is target-only re-audit by default.** Operators
    expect `qf.backfill_metrics` to refresh the system table
    without re-writing target. Re-writing target is a *data*
    backfill (different operation), gated behind `data=True` and
    explicit operator confirmation in the CLI.
12. **Soft-delete coupling.** Backfill writes fresh collection
    rows with newer `run_timestamp`; the existing dedupe rule
    (latest `run_timestamp` wins, validation > collection on tie)
    handles supersession. To explicitly tombstone the original,
    the operator passes `soft_delete_prior=True` (or runs
    `qf.deactivate_metric` separately).
13. **Audit trail.** Backfilled rows persist with
    `collected_via='backfill'` (or richer breadcrumb — plan
    phase) so dashboards can distinguish "originally validated"
    from "metric refreshed from current target". Useful for
    explaining surprise verdict flips.
14. **Backfill report.** Per-partition diff (original metric →
    backfilled metric, severity flip if any), aggregate counts
    (refreshed / unchanged / skipped / errored). Returned by
    `qf.backfill(...)`; rendered by the CLI subcommand.

## Open Questions (for the plan phase)

- **Q1. Range vs explicit partitions.** Should
  `qf.backfill(partition_ts=...)` accept a single date, a list,
  or a range tuple? Plan picks one and documents the reasoning.
- **Q2. Concurrency across partitions in a range.** Sequential
  by default; explicit `--parallelism N` flag for cases where
  history-backed validators don't depend on prior partitions
  inside the same backfill window.
- **Q3. `partition_column` auto-detection.** When `partition_ts`
  is a bare column reference (`event_date` rather than
  `'{{ ds }}'`), can we auto-extract the column name? Lean
  towards "explicit only" for v1, revisit if there's demand.
- **Q4. Selector grammar.** The foundation plan picked
  `dataset_name:validation_name` with `*` wildcards. Does
  metric-name need to be addressable too (`dataset:val:metric`)
  for backfill use cases? Probably yes — backfill's natural
  unit is the metric.
- **Q5. Audit-trail field naming.** `collected_via='backfill'`
  vs richer breadcrumb (`source='wap_target_backfill'` /
  `mode='metrics-only'`). Pick one shape for the plan, document
  the reason.
- **Q6. Communications plan for the YAML break.** Dict-form
  `expressions:` is a backwards-incompatible YAML change.
  Release-notes blurb, single-line migration recipe, deprecation
  timeline (zero — no compat shim).

## Affected Areas

- `qualifire/core/config.py` —
  `AggregationCollectionConfig.expressions` shape change;
  `WAPConfig.write_sql_file`, `partition_column`, `allow_create`
  fields with mutual-exclusion validators; selector grammar
  parsing helpers consumed by the CLI / API.
- `qualifire/core/engine.py` — `_run_wap` propagates dataset-level
  fields into staging config; new `_run_backfill` loop driver
  consuming `expected_metrics()`; `skip_if_cached` engine pre-pass
  reads `QualifireContext.cached_metrics`; per-partition source
  selection (target-mode for WAP, source-mode otherwise).
- `qualifire/core/selectors.py` (new) — selector grammar +
  `ScopedDataset` resolver.
- `qualifire/core/deactivate.py` (new) — `deactivate_metric`
  helper with the H1 read-back-and-bump pattern.
- `qualifire/api.py` — new `Qualifire.backfill(...)` and
  `Qualifire.deactivate_metric(...)`; `BackfillReport` dataclass
  returned from both.
- `qualifire/cli.py` — `qualifire backfill <config>
  [--partition <ds>] [--data] [--skip-if-cached]
  [--soft-delete-prior]` and `qualifire deactivate-metric` plus
  the `--skip-if-cached` flag on existing subcommands.
- `qualifire/collection/aggregation.py` — read dict expressions
  directly (no `AS <name>` lexer); honour
  `QualifireContext.backfill` to read from `wap.target_table`
  filtered to `partition_column = P` instead of running
  `write_sql`; dimensions GROUP BY as configured.
- `qualifire/collection/sampler.py` — same target-table source
  override in backfill mode for WAP datasets.
- `qualifire/wap/pattern.py:WAPExecutor.publish` — DataFrame
  writer path with `allow_create` honoured; SQL fallback for
  Pandas backends.
- `qualifire/storage/delta_storage.py`,
  `qualifire/storage/external_catalog.py`,
  `qualifire/storage/jdbc_storage.py` — `is_active` honoured on
  every existing read site (matching the SQLite rework already
  shipped in the foundation slice).
- All YAML config files using `expressions:` —
  `docs/examples/*.yaml`, `examples/industries/*/config.yaml`,
  `tests/manual/configs/*.yaml`, any test fixture.
- `tests/test_backfill_e2e.py` (new) — composing dict-form
  expressions, WAP target-mode, soft-delete, the report.
- `examples/backfill_quickstart.py` (new) — walking through all
  four primitives.
- Documentation: `docs/backfill_and_soft_delete.md` (new),
  `docs/wap.md` (target-mode section + `write_sql_file`),
  `docs/configuration.md` (dict-form expressions; per-dataset
  `partition_ts` for WAP), `docs/programmatic_api.md`
  (`backfill`, `deactivate_metric`, `BackfillReport`),
  `docs/cli.md` (new subcommands), README cross-links, release
  notes for the dict-form YAML break.

## References

- [`metrics-backfill-and-soft-delete/idea.md`](../metrics-backfill-and-soft-delete/idea.md)
  — original problem statement + decision pins #1-11.
- [`metrics-backfill-and-soft-delete/plan.md`](../metrics-backfill-and-soft-delete/plan.md)
  — H1-H8 invariants and the eleven step exit criteria the
  follow-up scope inherits.
- [`metrics-backfill-and-soft-delete/shipped.md`](../metrics-backfill-and-soft-delete/shipped.md)
  — explicit list of S2-S11 deferred surfaces.

## Notes

- Sub-features A-D were originally appended to
  `metrics-backfill-and-soft-delete/idea.md` as
  "Additional scope (post-foundation)". They've now moved here so
  the shipped foundation feature stays a clean historical record
  of what shipped, and the continuation work has a single
  consolidated home. A small follow-up commit removes that
  appendix from the foundation idea.
- Pairs with
  [`qualifire-from-config-factory`](../qualifire-from-config-factory/idea.md)
  — both touch the YAML schema. Their PRs can land in either order
  (no shared field names / no schema collisions).
- Pairs with
  [`external-catalog-system-table-hardening`](../external-catalog-system-table-hardening/idea.md)
  — soft-delete on external_catalog (sub-feature K here) overlaps
  with that feature's "infrastructure persistence outcome" /
  "eager CREATE TABLE DDL" work in the same backend module. The
  plan phase should sequence them so the storage rework lands
  once.
