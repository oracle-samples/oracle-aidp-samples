# Plan: Backfill API + WAP target-mode + config ergonomics

> **Status: design doc for the upcoming PR. Nothing in this plan
> has been implemented yet. Every "Step N" below describes work
> this PR will perform; it is not a description of repo state.**
> Repo at HEAD ships only the foundation slice — see
> `metrics-backfill-and-soft-delete/shipped.md` and D14 below for
> the actually-shipped state and the foundation-doc-vs-code drift
> this plan reconciles.

This plan covers the **entire** scope of
`backfill-api-and-wap-target-mode`: every sub-feature in idea.md
(A through M), the WAP-config rename, and `WAPConfig.partition_column`.
The PR will be large (estimated +5000 LOC source + tests + docs).
The plan compensates for that size by:

- Tight invariants that constrain implementation choices.
- Step-by-step ordering that makes commit-by-commit review possible
  (each step targets a well-bounded surface).
- Explicit acceptance criteria per sub-feature.

## In-scope sub-features

| ID | Source | Scope |
|----|--------|-------|
| **A** | idea.md sub-feature A | Dict-form `expressions:` everywhere (drops `<expr> AS <name>` lexer). 127 list-form construction sites + ~30 YAML files. |
| **B** | idea.md sub-feature B (rescoped) | Rename `WAPConfig.write_sql` → `WAPConfig.sql`; add `WAPConfig.sql_file`. Three-way mutual exclusion. |
| **C** | idea.md sub-feature C | `engine._run_wap` propagates dataset-level fields into staging config. |
| **D** | idea.md sub-feature D | WAP publish via DataFrame writer + `WAPConfig.allow_create`. |
| **E** | foundation S6 | `Qualifire.backfill(...)` API + `BackfillReport` dataclass. |
| **F** | foundation S7 | `Qualifire.deactivate_metric(...)` API + `qualifire/core/deactivate.py`. |
| **G** | foundation S9 | CLI: `qualifire backfill`, `qualifire deactivate-metric`, `--skip-if-cached`. |
| **H** | foundation S5 | Engine `_run_backfill` loop driver + per-partition source select. |
| **I** | foundation S4 | `qualifire/core/selectors.py` — selector grammar + `ScopedDataset` resolver. |
| **J** | foundation S8 | `skip_if_cached` engine pre-pass + `qualifire/core/backfill_eligibility.py`. |
| **K** | foundation S2/S3 | `is_active` honoured on Delta / ExternalCatalog / JDBC reads; `read_collection_metric_at_partition` on those backends. |
| **L** | foundation S10 | E2E composite test (forward → deactivate → backfill → re-deactivate, plus WAP target-mode backfill). |
| **M** | foundation S11 | `docs/backfill_and_soft_delete.md`, `examples/backfill_quickstart.py`, README + cross-links. |
| **WAP-pc** | idea.md decision pin #3 | `WAPConfig.partition_column` field. Required by E for WAP target-mode backfill. |

## Out-of-scope (genuine future work)

- Backfill `--parallelism N` (idea.md Q2): sequential by default;
  parallelism is a follow-up performance optimization.
- `partition_column` auto-detection from `partition_ts` (idea.md Q3):
  always explicit in v1.
- Multi-row `read_validation_history_bulk(limit > 1)` under soft-
  delete: foundation pinned this as `ValueError(limit>1)`. K
  preserves the contract on every backend.
- New backend support beyond the four already wired (SQLite, Delta,
  ExternalCatalog, JDBC).
- Backfill backed by source tables that have been schema-evolved
  (column added/removed since the original run): documented as
  operator responsibility (run forward first to backfill the
  schema state into the system table).

---

## Goals

1. Operators can run `Qualifire.backfill(config, partition_ts=...)`
   from Python or `qualifire backfill <config> --partition <P>`
   from the CLI. Result is a `BackfillReport` with per-partition
   diff + aggregate counts.
2. WAP-backed datasets backfill from `wap.target_table` filtered to
   `wap.partition_column = P`, not by re-running the write SQL.
   `data=True` opts into the full WAP cycle on the past partition.
3. Operators can deactivate a metric programmatically with
   `Qualifire.deactivate_metric(...)` or via CLI, writing a
   tombstone INSERT honoured by every backend.
4. Soft-delete (`is_active='false'`) is honoured on all four
   backends, with `read_collection_metric_at_partition` (H6) on
   each. Pre-existing tables without the `is_active` column read
   without crashing (`COALESCE(is_active, 'true')`).
5. The engine has a `skip_if_cached` pre-pass that consults
   `QualifireContext.cached_metrics` for cache-eligible
   validators. `Qualifire.validate(skip_if_cached=...)` and
   `qualifire validate --skip-if-cached` both surface it.
6. WAP YAMLs use the cleaner `sql:` / `sql_file:` source fields
   (renamed from `write_sql:`, no compat alias).
7. WAP publish goes through the DataFrame writer for Spark-capable
   backends, with `allow_create=True` (default) so operators don't
   need pre-existing target DDL. `partitionBy`, `format`, and
   format-specific options are honoured.
8. WAP audit phase honours per-dataset `partition_ts`,
   `partition_step`, `description`, `dimensions`, `measures`.
9. YAML `expressions:` is dict-form everywhere
   (`{name: expression}`) — list form `[<expr> AS <name>]` is
   removed.
10. One end-to-end test orchestrates forward → deactivate → backfill
    → re-deactivate across both WAP and non-WAP datasets. Public
    docs and a `examples/backfill_quickstart.py` complete the
    operator-facing story.

---

## Invariants

Inherited from `metrics-backfill-and-soft-delete/plan.md`,
authoritative for this PR:

- **H1** — Soft-delete = tombstone row (`is_active='false'`,
  same natural key, newer `run_timestamp`). INSERT-only; never
  UPDATE.
- **H2** — Reads apply `is_active` filter **after** ROW_NUMBER per
  natural key. Tombstone in newer run-time bucket hides earlier
  active row.
- **H3** — `partition_step` ↔ `partition_ts` XOR rejected at
  config-load time.
- **H6** — `read_collection_metric_at_partition`: collection-only,
  exact `partition_ts`, NULL-safe `dimension_value`, ROW_NUMBER
  ORDER BY `run_timestamp DESC, collected_at DESC`, then `rn = 1`
  ∧ `is_active` ∧ `metric_value IS NOT NULL`.
- **H8** — `read_latest_run` returns `None` when latest row is a
  tombstone.

New invariants for this PR:

- **N1** (WAP rename atomicity). `WAPConfig.write_sql` no longer
  exists. Every call site (~110) migrated. No alias.
- **N2** (sql / sql_file / df three-way mutual exclusion). Setting
  any two of the three on `WAPConfig` raises `ValidationError` at
  construction AND assignment.
- **N3** (sql_file resolution). Resolved at config-load time on a
  Pydantic `PrivateAttr` (`_resolved_sql`). UTF-8 strict; failures
  → `QualifireConfigError`.
- **N4** (staging propagation). `_run_wap` audit `DatasetConfig`
  carries dataset-level fields. `wap` stripped (recursion guard).
- **N5** (dict-form atomicity). After A, `AggregationCollectionConfig.expressions`
  is `dict[str, str]`. Every list-form construction site migrated.
  Every YAML migrated. `<expr> AS <name>` lexer removed from
  collector. No alias / no shim.
- **N6** (deactivate semantics). H1 read-back-and-bump applies
  (`run_timestamp = max(now, latest.run_timestamp + 1ms)`).
  Idempotent retry returns 0; no exception.
- **N7** (selector resolution scope). Selector grammar resolves
  against parsed config only. No-match → `QualifireConfigError`.
- **N8** (skip-if-cached coupling). Engine pre-pass consults
  `cached_metrics` only when `skip_if_cached=True` AND
  `cache_eligible(validator)`. Cache stores metric VALUES, not
  validation verdicts; thresholds re-evaluate.
- **N9** (no UPDATE). All writes INSERT-only across all four
  backends, including `deactivate_metric` and backfill.
- **N10** (read parity). Every read method on Delta /
  ExternalCatalog / JDBC produces the same row set as SQLite for
  the same input data.
- **N11** (column-tolerant reads). `COALESCE(is_active, 'true')`
  everywhere.
- **N12** (backfill metric-level idempotency). Re-running
  `qf.backfill(...)` on the same partition with no upstream
  change yields the same `metric_value`. Backfilled rows carry
  `details_json.collected_via='backfill'`.
- **N13** (backfill is target-only by default for WAP). For WAP
  datasets, `qf.backfill` reads `wap.target_table WHERE
  partition_column = P`. WRITE / PUBLISH skipped. `data=True`
  opts into the full cycle.
- **N14** (deactivate-after-backfill). A backfill that re-collects
  a deactivated key intentionally **wins** (newer `run_timestamp`).
  Operators run `deactivate_metric` AFTER backfill to keep a
  tombstone sticky.
- **N15** (WAP publish via DataFrame writer). Spark-capable
  backends use `df.write.format(...).options(...).saveAsTable(target)`.
  Pandas-style backends keep the SQL fallback.
- **N16** (publish allow_create). `WAPConfig.allow_create: bool =
  True` by default. False = strict "target must pre-exist."
- **N17** (BackfillReport shape stability). Frozen dataclass; public
  attributes form a stable contract; future fields additive only.
- **N18** (partition-error isolation). One errored partition
  inside a multi-partition `qf.backfill` does NOT abort siblings.
  Report records `errored`; after all complete, if `report.has_errors`,
  `qf.backfill` raises `QualifireValidationError` with the
  report attached. CLI exit code 1.

---

## Decisions

### D1 — `AggregationCollectionConfig.expressions: dict[str, str]` (sub-feature A)

Replace:
```yaml
expressions:
  - "COUNT(*) AS row_count"
  - "AVG(amount) AS avg_amount"
```
with:
```yaml
expressions:
  row_count: "COUNT(*)"
  avg_amount: "AVG(amount)"
```

Pydantic field type: `dict[str, str]`, non-empty validator.
Collector reads `dict.items()` directly — no lexer for
`<expr> AS <name>`.

**Why no shim / no alias**:
- The list-form lexer is the brittle part (regex matching
  `\s+AS\s+(\w+)$`) — it gets in the way of writing arbitrary SQL
  expressions where the name might collide with a SQL keyword.
- Foundation feature pin #5: clean breaks over shim layers.
- Migration is mechanical:
  ```python
  # YAML migration:
  for line "{prefix}- \"{expr} AS {name}\"" → "{indent}{name}: \"{expr}\""
  # Python migration:
  expressions=["EXPR AS NAME", ...] → expressions={"NAME": "EXPR", ...}
  ```

**API builders** (`Qualifire.threshold_check` etc.) already
construct `expressions=[...]` internally; these get rewritten to
build dicts directly.

**Builder API stays as-is** for `aggregations: dict[str, str]`
(already dict-shaped) — the only public surface that changes is
the YAML schema and the constructor of
`AggregationCollectionConfig` (Python).

### D2 — `WAPConfig` rename: `write_sql` → `sql`, add `sql_file`

After this PR `WAPConfig` source-of-data fields are:
- `df` (unchanged)
- `sql` (was `write_sql`) — SELECT string
- `sql_file` (new) — path to `.sql` file, read at config-load time

Three-way pairwise mutual exclusion (N2).

**`sql` not `query`**: `query` collides with `DatasetConfig.query`.

**No compat alias**: pin #5 — clean break.

`_resolved_sql` is a Pydantic v2 `PrivateAttr` populated by a
mode='after' validator. `effective_sql` property returns
`_resolved_sql or sql`. Engine reads `effective_sql`.

File-read errors (`FileNotFoundError`, `UnicodeDecodeError`,
`OSError`) all map to `QualifireConfigError` with the original
chained as `__cause__`.

Resolution policy: `sql_file` paths resolved relative to CWD by
default; `QUALIFIRE_CONFIG_BASE_DIR` env var overrides if set.
Absolute paths used as-is.

### D3 — `_run_wap` staging-config projection (sub-feature C)

```python
# inside _run_wap:
staging_ds_config = DatasetConfig(
    name=ds_config.name,
    description=ds_config.description,
    table=executor.staging_table,
    dimensions=ds_config.dimensions,
    measures=ds_config.measures,
    partition_ts=ds_config.partition_ts,
    partition_step=ds_config.partition_step,
    validations=ds_config.validations,
    # wap=None — recursion guard (N4)
    # filter, query, df — staging is the just-written subset
    # cache, validation_parallelism — orthogonal
)
```

### D4 — WAP publish via DataFrame writer (sub-feature D)

Spark-capable backends use:
```python
df = self.backend.spark.table(self.staging_table)
writer = df.write
fmt = self.write_options.get("format")
if fmt: writer = writer.format(fmt)
writer = writer.mode(self.write_options.get("mode", "append"))
pb = self.write_options.get("partitionBy")
if pb: writer = writer.partitionBy(*(pb if isinstance(pb, list) else [pb]))
for k, v in self.write_options.items():
    if k in ("format", "mode", "partitionBy"): continue
    writer = writer.option(k, v)

target_exists = self.backend.table_exists(self.target_table)
if not target_exists and not self.allow_create:
    raise WAPPublishError(
        f"Target table {self.target_table!r} does not exist and "
        f"WAPConfig.allow_create=False; either create the target "
        f"or set allow_create=True"
    )
writer.saveAsTable(self.target_table)
```

Pandas-style backends (no `.spark` attribute) keep the existing
SQL `INSERT INTO/OVERWRITE TABLE` fallback path.

**`allow_create` field on `WAPConfig`**: `bool = True` (N16).
Operators in catalog/RBAC-controlled environments set False to
restore the strict "target must pre-exist" contract.

**Schema-strict policy** is per-format default (D2 follows
idea.md decision pin #9): Delta defaults to strict via Spark's
own behavior; Parquet/Hive default permissive; operator overrides
via `write_options` (`mergeSchema=true`, etc.). No qualifire-side
enforcement.

**`backend.table_exists` helper**: this PR adds the helper to the
`Backend` base if absent, with backend-specific implementations
(Spark: `spark.catalog.tableExists`; pandas: existing convention).

### D5 — `WAPConfig.partition_column: str | None`

New required-for-target-mode-backfill field.

- Forward execution: ignored.
- Backfill (`qf.backfill`) on a WAP dataset: required. Missing →
  `QualifireConfigError("WAPConfig.partition_column required for
  backfill on dataset '{name}': add partition_column='<col>' to
  the dataset's wap config")` at backfill entry, before any read.
- Auto-detection from `partition_ts` (idea.md Q3): NOT in this
  PR. Always explicit in v1. The plan has room to add an
  auto-detect helper later without breaking the contract.

`DatasetConfig.partition_ts` and `WAPConfig.partition_column` are
**different things** by design (idea.md sub-feature 2):
- `partition_ts` is a SQL expression rendered through Jinja for
  forward collection.
- `partition_column` is a bare column name on `target_table` for
  backfill filtering.

### D6 — Selector grammar (sub-feature I)

Three-part addressable: `<dataset>:<validation>[:<metric>]`. Each
part is a literal name or `*` wildcard.

| Selector | Matches |
|----------|---------|
| `sales:*` | All validations on dataset `sales` |
| `*:row_count_check` | The `row_count_check` validation across every dataset |
| `sales:row_count_check` | Single validation |
| `sales:*:row_count` | All validations on `sales` producing metric `row_count` |
| `*:*:revenue` | Every validation in the run producing metric `revenue` |

```python
@dataclass(frozen=True)
class SelectorPart:
    dataset: str   # literal or '*'
    validation: str  # literal or '*'
    metric: str | None  # literal, '*', or None (all metrics)

@dataclass(frozen=True)
class ScopedDataset:
    dataset: DatasetConfig
    validations: list[ValidationConfig]
    metric_filter: set[str] | None  # None = all metrics
```

**Edge cases**:
- Empty (`""`) → `QualifireConfigError`.
- One segment (`sales`) → error: "validation segment required".
- Trailing colon (`sales:`) → error: "validation may not be empty".
- Empty middle (`sales::row_count`) → error.
- Whitespace stripped before parsing.
- Multiple selectors → comma-separated; results concatenated and
  deduped by `(dataset.name, validation.id)`.
- No-match → `QualifireConfigError` ("selector matched no scope").

**Name-character contract**: `_SAFE_NAME_RE = ^[A-Za-z0-9._-]+$`.
Colons not legal — selector `:` separator unambiguously parses.

### D7 — `Qualifire.backfill` API (sub-feature E)

```python
def backfill(
    self,
    config: QualifireConfig | str | Path,
    *,
    partition_ts: str | datetime | list[str] | tuple[str, str],
    selector: str | None = None,
    data: bool = False,
    skip_if_cached: bool = False,
    soft_delete_prior: bool = False,
) -> BackfillReport: ...
```

**`partition_ts` shapes**:
- Single string / datetime → one partition.
- List → explicit partition list.
- Tuple `(start, end)` → inclusive range; expanded using each
  dataset's `effective_partition_step` (H3).

**`selector`**: optional. Default = "all validations in the
config." Restricts scope.

**`data`**: see N13. `data=True` + non-WAP dataset →
`QualifireConfigError("data=True is only supported for WAP
datasets; dataset '{name}' is non-WAP")`.

**`skip_if_cached`**: when True, the cache pre-pass (J) shorts
existing collection rows for the partition.

**`soft_delete_prior`**: when True, write a tombstone INSERT for
every metric the backfill touches BEFORE the new collection
INSERTs. Net effect: the new row is the only active row; the
tombstone serves as audit trail. Default False (cheaper path).

**Range expansion**: `expand_range(start, end, step) -> list[datetime]`
is a pure helper. Inclusive on both ends. Tested independently.

Sequential by default. `--parallelism N` deferred (out-of-scope).

### D8 — `BackfillReport` shape (sub-feature E)

```python
@dataclass(frozen=True)
class PartitionDiff:
    dataset_name: str
    metric_name: str
    dimension_value: str | None
    partition_ts: datetime
    original_value: float | None
    backfilled_value: float | None
    severity_before: Severity | None
    severity_after: Severity | None
    status: Literal["refreshed", "unchanged", "skipped", "errored"]
    error: str | None
    skip_reason: str | None

@dataclass(frozen=True)
class BackfillReport:
    partitions: list[PartitionDiff]  # one per (dataset, metric, dim, partition)
    refreshed: int
    unchanged: int
    skipped: int
    errored: int
    @property
    def total(self) -> int: ...
    @property
    def has_errors(self) -> bool: ...
```

**`original_value` / `severity_before` provenance**: read once per
key BEFORE the backfill INSERTs:
- `original_value` ← `read_collection_metric_at_partition(...)`
  (`metric_value` field; None when no prior active row).
- `severity_before` ← latest validation severity via
  `read_validation_history_bulk(limit=1)` (foundation contract).

**Status assignment**:
- `refreshed` — `original_value != backfilled_value` OR
  `severity_before != severity_after` (float comparison via
  `math.isclose(rel_tol=1e-9)`).
- `unchanged` — both equal within tolerance.
- `skipped` — explicit skip (sample collector + no historical
  data; per-partition source-select rejection; etc.); `skip_reason`
  set.
- `errored` — collection / validation exception; `error` set to
  `repr(exc)`; sibling partitions continue (N18).

Frozen dataclass exported from `qualifire/__init__.py`.

### D9 — Per-partition source select for backfill (sub-feature H)

When `context.backfill=True`:

| Dataset has `wap`? | `data=True`? | Source |
|--------------------|--------------|--------|
| Yes | False (default) | `wap.target_table WHERE partition_column = P` |
| Yes | True | Full WAP cycle on the partition (write→audit→publish) |
| No | False | `dataset.table` filtered to partition |
| No | True | rejected → `QualifireConfigError` (D7) |

For non-WAP datasets, partition filter is built from
`partition_ts` rendered with the partition's value substituted
into `ds`:

```
SELECT ... FROM <table>
WHERE <partition_ts_rendered_with_ds=P> = <P>
  [AND <dataset.filter>]
```

`partition_ts` must be a column reference (or a SQL expression
that evaluates to a column) for non-WAP backfill. A constant
(`'{{ ds }}'`) is rejected at backfill entry with a clear
diagnostic — backfill of `partition_ts="'{{ ds }}'"` is
ambiguous because the render would produce `WHERE '2026-04-01' =
'2026-04-01'` (true for every row). WAP datasets bypass via
`wap.partition_column`.

### D10 — Sample collectors in backfill mode

Pattern / shape validators (sample-based):
- WAP target-mode: samples come from `wap.target_table WHERE
  partition_column = P`. Same expressions, target.schema ≡
  staging.schema (idea.md decision pin #2).
- Non-WAP, source has data: samples from `dataset.table`
  filtered.
- Non-WAP, raw events expired: empty result → validator emits
  `ValidationResult(severity=Severity.INFO, status='skipped',
  details_json={'skip_reason': 'no_historical_samples'})`. Counted
  as `skipped` in BackfillReport.

We do not inspect upstream TTL metadata; we query and treat the
empty result as the "no historical samples" signal.

### D11 — Cache eligibility (sub-feature J)

`cache_eligible(validator) -> bool`:

| Validator type | cache_eligible | Reasoning |
|----------------|----------------|-----------|
| ThresholdValidation | True | Cache replaces collection SELECT; threshold logic re-evaluates. |
| HistoricalValidation | True | Same — cache covers the current-partition value; history reads run regardless. |
| ForecastValidation | True | Same shape — model evaluation runs regardless. |
| AnomalyDetectionValidation | True | Same. |
| PatternValidation | False | Collection produces row samples, not a single metric_value. |
| SLOValidation | False | Collection IS the freshness query; caching it defeats SLO. |

Cache stores VALUES, not verdicts (N8).

### D12 — Tombstone audit trail (idea.md Q5)

`details_json.collected_via='backfill'` (string) on every
backfilled collection row. Forward rows omit the field (absence
== forward). Future variants (`details_json.backfill_mode`) can
be added additively without breaking existing consumers.

### D13 — CLI surface (sub-feature G)

The qualifire CLI today exposes three subcommands: `run`,
`validate-config`, `report` (verified via grep). This PR adds
`backfill` and `deactivate-metric`, and adds `--skip-if-cached`
to the existing `run` subcommand.

```
qualifire run --config <path> [options]
  --skip-if-cached       # NEW — short-circuit cache-eligible collectors
  ...                    # existing flags preserved

qualifire backfill --config <path> [options]
  --partition <value>    # required: single, comma-list, or start..end range
  --selector <selector>
  --data
  --skip-if-cached
  --soft-delete-prior
  --json                 # emit BackfillReport as JSON

qualifire deactivate-metric --config <path>
  --dataset <name>
  --metric <name>
  --dimension <encoded>
  --partition <ts>
  --note <text>
```

`--partition` grammar: single ISO-8601 date / datetime; comma-list
(no spaces); `start..end` literal `..` separator; mutually
exclusive (`,` and `..` together is a parse error).

Each value parsed by the existing `partition_ts` parser used by
forward execution.

Exit codes: 0 = success; 1 = `BackfillReport.has_errors`; 2 =
argparse usage error.

### D14 — Foundation scaffold gaps backfilled in this PR

The foundation `shipped.md` claims certain scaffolds shipped that
are NOT in `main`:

| Claimed | Actual | Plan response |
|---------|--------|---------------|
| `qualifire/core/backfill_eligibility.py` | Absent | Step 6 creates. |
| `_build_skip_if_cached_pre_pass` API scaffold | Absent | Step 6 implements pre-pass directly in `_run_dataset`. |
| `Qualifire.validate(skip_if_cached=...)` kwarg + plumbing through `run_config[_parsed]` and CLI `qualifire run` | Absent | Step 6 adds the kwarg on `validate`, `run_config`, `run_config_parsed`, and Step 11 wires the `--skip-if-cached` CLI flag onto the existing `run` subcommand (NOT a new `validate` subcommand — qualifire CLI surface today is `run` / `validate-config` / `report`). |
| `_add_backfill_tag` engine helper | Absent | Step 10 (H + E) adds equivalent inline. |
| `expected_metrics()` resolver on threshold / historical / forecast / anomaly validators | Absent | Step 6 adds the method as part of the cache pre-pass scaffold (returns `list[tuple[str, str | None]]` of `(metric_name, dimension_value_or_none)` pairs the validator will consume — backfill_eligibility's predicate plus this resolver are the two halves of the cache-key generator). |

Foundation actually shipped: `partition_step` field + H3,
`is_active` shared-schema column + write-stamping, SQLite read
paths H1/H2/H6/H8, SQLite `read_collection_metric_at_partition`,
`QualifireContext.backfill` and `cached_metrics` fields (wired,
unconsumed).

A small follow-up commit on the foundation's `shipped.md` to fix
the doc-vs-code drift is left as a follow-up — out of scope.

### D15 — Pre-existing tables without `is_active` column

`COALESCE(is_active, 'true')` makes reads tolerant. Writes
(deactivate, soft_delete_prior, backfill) on legacy tables
without the column raise loud backend errors (column not found),
not silent no-ops. Release notes carry per-backend ALTER TABLE
one-liners; `docs/configuration.md` "Schema migration" subsection
captures them.

### D16 — Backend SQL bodies stay backend-native (sub-feature K)

Each backend rewrites its own SQL — no shared SQL builder. Pattern
(D6 from the previous slim plan, restated here):

```sql
WITH ranked AS (
  SELECT <projection>,
    ROW_NUMBER() OVER (
      PARTITION BY <natural_key>
      ORDER BY run_timestamp DESC, collected_at DESC
    ) AS rn
  FROM <results_table>
  WHERE <consumer_predicates>
)
SELECT <projection>
FROM ranked
WHERE rn = 1
  AND COALESCE(is_active, 'true') = 'true'
  AND metric_value IS NOT NULL  -- omit on validation-row reads
  AND <record_type clause>
ORDER BY <consumer ordering>
LIMIT <consumer limit>;
```

JDBC `_dialect` shim handles Oracle FETCH FIRST vs LIMIT N. The
TEXT comparison on `is_active` avoids dialect BOOL quirks.

---

## Implementation steps

Steps are ordered to minimize backtracking. Each step is a
separate commit.

### Step 1 — Sub-feature A: dict-form `expressions:`

**Files**:
- `qualifire/core/config.py`:
  - `AggregationCollectionConfig.expressions: dict[str, str]`
    (was `list[str]`).
  - Validator: non-empty.
- `qualifire/collection/aggregation.py`:
  - Drop the `<expr> AS <name>` lexer.
  - `AggregationCollector.__init__` takes `expressions: dict[str, str]`.
  - SELECT clause built from `[(f"({expr}) AS {nm}") for nm,
    expr in expressions.items()]`.
- `qualifire/api.py` builder methods (`threshold_check`,
  `drift_check`, `trend_check`, `anomaly_check`, etc.):
  - Build dicts directly: `expressions = {nm: f"({expr})" for ...}`.
- `qualifire/core/engine.py`:
  - `_construct_collector` constructs `AggregationCollector(expressions=collection.expressions)`.
- All YAMLs: list-form → dict-form. Find via `grep -rln
  "expressions:" docs/examples examples tests`.
- All test fixtures with `expressions=[...]`: rewrite to
  `expressions={...}`.

**Tests**:
- New: `tests/test_aggregation_dict_form.py` (~10 cases) — load
  YAML with dict expressions, run, verify.
- Existing tests with list-form → migrate to dict-form.
- Negative: list-form YAML now raises `ValidationError` with
  helpful message ("expressions must be a dict; pass `name:
  expression` pairs instead of `expression AS name` strings").

**Exit criteria**:
- All test fixtures migrated.
- All YAMLs migrated.
- `grep -rn 'expressions: \?\[' docs/examples examples tests`
  returns 0 hits.
- `grep -rn 'expressions=\[' qualifire tests` returns 0 hits.
- Lexer `_parse_aliased_expr` (or equivalent) removed; grep
  confirms.

### Step 2 — Sub-feature B: WAP rename + `sql_file`

**Files**:
- `qualifire/core/config.py`:
  - `WAPConfig.write_sql` → `WAPConfig.sql`.
  - Add `WAPConfig.sql_file: str | None = None`.
  - Add `_resolved_sql: PrivateAttr[str | None]`.
  - `effective_sql` property.
  - Three-way mutual-exclusion validator.
  - `_resolve_sql_file` validator (mode='after'); errors →
    `QualifireConfigError`.
- `qualifire/wap/pattern.py`: `WAPExecutor.write_sql` →
  `self.sql`; param rename.
- `qualifire/api.py`: `Qualifire.validate(write_sql=...)` →
  `sql=...`. Docstrings.
- `qualifire/core/engine.py`: `_run_wap` reads
  `wap_config.effective_sql`.
- All YAMLs: `write_sql:` → `sql:`.
- All Python files: `write_sql=` keyword → `sql=`.
- `tests/test_findings_sweep.py::test_wapconfig_df_and_write_sql_*`
  → `..._df_and_sql_*`.

**Tests** (`tests/test_wap_config_sql.py`, ~20 cases):
- Round-trip; mutual exclusion (every pair of {df, sql,
  sql_file}); `validate_assignment`; missing/non-UTF-8/empty
  file; `QUALIFIRE_CONFIG_BASE_DIR`; `model_dump()` PrivateAttr
  semantics; pin two same-`sql_file:` configs equal at schema
  level; existing WAP tests pass under rename.

**Exit criteria**:
- `grep -rn 'write_sql' qualifire/ tests/ docs/ examples/`
  returns 0 production hits.
- All existing WAP tests green under rename.

### Step 3 — Sub-feature C: `_run_wap` propagation

**Files**:
- `qualifire/core/engine.py` `_run_wap`: D3 projection.

**Tests**:
- WAP dataset with `partition_ts="event_date"` +
  `partition_step="P1D"` → audit emits rows with right
  `partition_ts`.
- WAP dataset with `dimensions=["region"]` → per-region rows.
- Description, dimensions, partition_step all forwarded.
- Run-level fallback regression.
- Two WAP datasets with different per-dataset `partition_ts` →
  isolation pinned.
- Recursion guard: staging config has `wap is None`.

**Exit criteria**: tests green; remove redundant run-level
`partition_ts` from `tests/manual/configs/end_to_end_wap.yaml`
where it was a footgun-workaround.

### Step 4 — Sub-feature K: storage parity

**Files**:
- `qualifire/storage/delta_storage.py`: 6 read methods rewritten
  + `read_latest_run` H8 + `read_collection_metric_at_partition`
  (new).
- `qualifire/storage/external_catalog.py`: same 8 methods.
- `qualifire/storage/jdbc_storage.py`: same 8 methods, dialect-
  tolerant.
- SQL pattern: D16.

**Tests**:
- `tests/test_storage/test_soft_delete_delta.py` (new, ~15
  cases) — mirror SQLite suite, gated on pyspark.
- `tests/test_storage/test_soft_delete_external_catalog.py`
  (new, ~15) — local-Spark warehouse fixture.
- `tests/test_storage/test_soft_delete_jdbc.py` (new, ~15) —
  real-Spark JDBC fixture.
- `tests/test_storage/test_h6_parity.py` (new) — parametrized
  over four backends.

**Exit criteria**:
- 15-case parity suite green per backend.
- `read_collection_metric_at_partition` cross-backend parity
  green.
- Pre-existing-table-without-column → no crash.
- `read_validation_history_bulk(limit > 1)` raises on every
  backend.
- Schema-migration ALTER statements documented.

### Step 5 — Sub-feature I: selectors

**Files**:
- `qualifire/core/selectors.py` (new): `SelectorPart`,
  `ScopedDataset`, `parse_selector`, `resolve_selector`. Edge
  cases per D6.

**Tests** (`tests/test_selectors.py`, ~25 cases): two-segment,
three-segment, wildcards, multi-selector dedup, whitespace,
empty, no-match, trailing/empty-middle colon, name-character
edge.

**Exit criteria**: tests green.

### Step 6 — Sub-feature J: `skip_if_cached` engine pre-pass + `expected_metrics()` resolver

**Files**:
- `qualifire/core/backfill_eligibility.py` (new):
  `cache_eligible(validator) -> bool` per D11.
- `qualifire/core/config.py`:
  - Add `expected_metrics(self, dataset: DatasetConfig) ->
    list[tuple[str, str | None]]` method to `ThresholdValidationConfig`,
    `HistoricalValidationConfig`, `ForecastValidationConfig`,
    `AnomalyDetectionValidationConfig`. Returns the list of
    `(metric_name, dimension_value_or_None)` pairs the validator
    expects to consume. For non-dimensional validators →
    `[(metric_name, None)]`. For dimensional validators (when
    `collection.dimensions` is set) → cartesian-product across
    declared dimensions if known statically, else returns just
    the metric names with `None` (engine pre-pass falls back to
    a "best-effort" cache lookup keyed by the actual dim values
    discovered at run-time). `PatternValidationConfig` and
    `SLOValidationConfig` either omit the method or return `[]`
    — `cache_eligible` returns False for them, so the engine
    pre-pass never calls `expected_metrics` on those types.
  - Helper covered by tests: each validator config's
    `expected_metrics(dataset)` round-trips through the engine
    pre-pass and produces correctly-keyed `cached_metrics`.
- `qualifire/core/context.py`: `skip_if_cached: bool = False`
  field on `QualifireContext`.
- `qualifire/core/engine.py`: `_run_dataset` pre-pass when
  `context.skip_if_cached` AND `cache_eligible(validator)`.
  Calls `validator.expected_metrics(dataset)` to get the keys,
  performs `read_collection_metric_at_partition` for each, and
  populates `context.cached_metrics`. Validators emit
  `details_json.from_cache=True` on cache hits.
- `qualifire/api.py`:
  - `Qualifire.validate(skip_if_cached=False, ...)` kwarg;
    plumbs to `context.skip_if_cached`.
  - `Qualifire.run_config(yaml_path, skip_if_cached=False, ...)`
    kwarg.
  - `Qualifire.run_config_parsed(config, skip_if_cached=False, ...)`
    kwarg. Both plumb through to `context.skip_if_cached` so the
    YAML / CLI driven paths surface the same knob the
    programmatic `validate` does.

**Tests** (`tests/test_skip_if_cached.py`, ~15 cases):
- Threshold + cache hit → collector skipped, validator runs from
  cache.
- Threshold + no cached row → normal run.
- Pattern + cache → ignored (not eligible).
- SLO + cache → ignored.
- Multi-validator dataset; mixed eligibility.
- Cache hit with `metric_value=None` → miss.
- Cache row tombstone → miss (H8).
- Threshold rule changed between original and re-run → cache
  serves value, validator re-evaluates against new rule.

**Exit criteria**: tests green; no regression in default
(`skip_if_cached=False`) paths.

### Step 7 — Sub-feature F: `Qualifire.deactivate_metric`

**Files**:
- `qualifire/core/deactivate.py` (new): `deactivate_metric(storage,
  dataset_name, metric_name, dimension_value=None,
  partition_ts=None, note=None) -> int`. H1 read-back-and-bump.
- `qualifire/api.py`: `Qualifire.deactivate_metric(...)` thin
  wrapper.

**Tests** (`tests/test_deactivate_metric.py`, ~15 cases): single
row, multiple partitions, dimensional filter, idempotent retry,
no-match returns 0, `note` propagates, H1 invariant pinned, JDBC
loud-fail on legacy table missing column.

**Exit criteria**: tests green; docstring documents non-WAP-aware
contract and `partition_ts=None` semantics.

### Step 8 — Sub-feature WAP-pc: `WAPConfig.partition_column`

**Files**:
- `qualifire/core/config.py`: `WAPConfig.partition_column: str | None
  = None` field. Validator: `_SAFE_NAME_RE` pattern (column-name
  characters).

**Tests**: field accepts/rejects per pattern.

**Exit criteria**: backfill consumers (Step 9) use it.

### Step 9 — Sub-feature D: WAP DataFrame writer publish + allow_create

**Files**:
- `qualifire/core/config.py`: `WAPConfig.allow_create: bool =
  True` field.
- `qualifire/wap/pattern.py:WAPExecutor.publish`:
  - Spark backends: D4 DataFrame writer path.
  - Pandas backends: existing SQL fallback retained.
- `qualifire/backends/base.py` and Spark/pandas implementations:
  add `table_exists(name) -> bool` if absent.

**Tests** (`tests/test_wap_publish.py`, ~12 cases):
- Spark backend, allow_create=True, target absent → CREATE.
- Spark backend, allow_create=False, target absent → raise
  `WAPPublishError`.
- Spark backend, allow_create=False, target present → publish.
- partitionBy honoured (single string and list).
- format / mergeSchema honoured (Delta path).
- mode=append vs mode=overwrite.
- Pandas backend: SQL path preserved (regression).

**Exit criteria**: all WAP examples / e2e configs continue to
publish; no operator-visible behavior change for existing
target-table-pre-exists deployments.

### Step 10 — Sub-features H + E: backfill engine loop + API + report

**Files**:
- `qualifire/core/backfill_report.py` (new): `PartitionDiff`,
  `BackfillReport` per D8.
- `qualifire/core/engine.py`:
  - `_run_backfill(config, scoped, partitions, *, data,
    skip_if_cached, soft_delete_prior) -> BackfillReport`.
  - Set `context.backfill=True` for the duration; tag rows with
    `details_json.collected_via='backfill'`.
  - Per-partition source select per D9.
  - Sample collectors per D10.
  - Read original_value / severity_before before INSERT (D8).
  - Partition error isolation (N18).
- `qualifire/api.py`: `Qualifire.backfill(config, *, partition_ts,
  selector, data, skip_if_cached, soft_delete_prior)`.
  - Range-expansion: `expand_range(start, end, step)`.
  - Selector resolution.
  - Aggregates per-partition diffs into `BackfillReport`.
  - `report.has_errors` → raises `QualifireValidationError` with
    report attached.
- `qualifire/__init__.py`: export `BackfillReport`,
  `PartitionDiff`.

**Tests** (`tests/test_backfill.py`, ~30 cases):
- Single-partition non-WAP refresh.
- Single-partition WAP target-mode reads from target_table (spy
  pinned).
- Range expansion `(start, end)` + step.
- List form `[d1, d2, d3]`.
- Selector restricts; selector no-match → error.
- WAP missing `partition_column` + backfill → `QualifireConfigError`.
- `data=True` non-WAP → rejected.
- `data=True` WAP → full WAP cycle on past partition.
- Sample collector + WAP target → samples from target.
- Sample collector + source-only no-history → skipped in report.
- `soft_delete_prior=True` → tombstone INSERTs visible.
- `details_json.collected_via='backfill'` on every backfilled row.
- N12 idempotency, N14 deactivate-after-backfill.
- Multi-partition with one errored → report records errored,
  others complete; final raise with report attached (N18).
- `BackfillReport.has_errors` / `.total` correct.
- `partition_ts="'{{ ds }}'"` (constant) non-WAP → rejected.
- N7 selector-no-match propagates.

**Exit criteria**: tests green; `Qualifire.backfill` is
covered by docstrings; `BackfillReport` exported from
`qualifire/__init__.py`.

### Step 11 — Sub-feature G: CLI

**Files**:
- `qualifire/cli.py`:
  - Register two new subparsers: `backfill`, `deactivate-metric`.
  - `_cmd_backfill(args)`: invokes `Qualifire.backfill`; print
    summary or `--json`; exit 1 on `report.has_errors`.
  - `_cmd_deactivate_metric(args)`: invokes
    `Qualifire.deactivate_metric`; print count.
  - Add `--skip-if-cached` to the existing `run` subparser
    (NOT `validate` — that subcommand does not exist; the YAML/
    CLI-driven validation entrypoint is `qualifire run`).
  - `--partition` parser (D13 grammar).

**Tests** (`tests/test_cli_backfill.py`, ~15 cases):
- `qualifire backfill --config <yaml> --partition 2026-04-01`
  runs.
- Comma-list, range syntax.
- `--selector sales:*`.
- `--data`.
- `--json` round-trips through json.loads.
- Exit code 1 on errors.
- `qualifire deactivate-metric --config <yaml> --dataset sales
  --metric row_count`.
- `--note "ops-12345"` propagates.
- `qualifire run --config <yaml> --skip-if-cached` short-
  circuits cache-eligible collectors (mock-storage assertion).

**Exit criteria**: tests green.

### Step 12 — Sub-feature L: e2e composite test

**Files**:
- `tests/test_backfill_e2e.py` (new):
  1. Forward run on partition `2026-04-01` (collects, validates,
     persists) on a non-WAP dataset.
  2. `Qualifire.deactivate_metric(metric=row_count)` →
     subsequent reads return None (H8).
  3. `Qualifire.backfill(partition_ts="2026-04-01")` →
     row reactivated.
  4. Second `Qualifire.deactivate_metric` → tombstone wins.
  5. Setup a WAP dataset; forward run.
  6. `Qualifire.backfill(partition_ts=("2026-04-01",
     "2026-04-03"))` on the WAP dataset → reads from
     target_table; report shape verified.
  7. Run on SQLite (always) and JDBC (existing in-memory
     fixture); Delta + ExternalCatalog gated by their existing
     CI markers.

**Exit criteria**: e2e green; no regression in existing tests.

### Step 13 — Sub-feature M: docs + examples

**Files**:
- `docs/backfill_and_soft_delete.md` (new): architecture,
  invariants H1-H8 + N1-N18, BackfillReport shape, sample-
  collector semantics, per-backend ALTER statements, deactivate-
  after-backfill, failure modes.
- `examples/backfill_quickstart.py` (new): walk through forward
  → soft-delete → backfill on a SQLite-backed dataset.
- `docs/wap.md`: rename note, `sql_file:` example, per-dataset
  partition_ts, DataFrame writer publish path,
  `partition_column` / `allow_create` fields.
- `docs/configuration.md`: dict-form `expressions:` example;
  "Schema migration" subsection (per-backend ALTER for
  `is_active`); per-dataset `partition_ts` for WAP.
- `docs/programmatic_api.md`: `Qualifire.backfill`,
  `Qualifire.deactivate_metric`, `BackfillReport`,
  `skip_if_cached`.
- `docs/cli.md`: `qualifire backfill`,
  `qualifire deactivate-metric`, `--skip-if-cached`.
- `README.md`: cross-link to backfill quickstart.
- `CHANGELOG.md` (or `docs/release_notes/...`): release-notes
  blurb covering both rename (B) and dict-form expressions (A);
  list `Qualifire.backfill` / `Qualifire.deactivate_metric` as
  new public APIs.
- `idea.md` Notes: append "All sub-features A-M shipped here in
  one PR. The deferred sub-features mentioned mid-conversation
  (auto-detection of partition_column; backfill --parallelism)
  remain genuine future work."

**Exit criteria**:
- `examples/backfill_quickstart.py` runs end-to-end on SQLite.
- All documentation links resolve.

### Step 14 — Cross-cutting regression matrix

**Files**:
- N/A (verification step).

**Exit criteria**:
- `pytest -x -q` green.
- `pytest -x -q tests/test_storage/` green for all backends
  (Spark gated as appropriate).
- No new lint / mypy errors.
- PR description maps each commit to a step in this plan.

---

## Risks

| Risk | Severity | Mitigation |
|------|----------|------------|
| PR is too large to review in one pass | High | 14 steps, each a separate commit; PR description lists step-to-files. Reviewer can take each commit independently. |
| Backend SQL dialect quirks (Oracle FETCH FIRST etc.) | Medium | D16 absorbs in `_dialect`. Tests gated on real-Spark fixtures already exist. |
| Pre-existing tables without `is_active` column | High | D15 + N11. Loud-fail on writes; tolerant reads. |
| WAP rename misses a call site | High | Step 2 exit criterion: `grep -rn 'write_sql'` returns 0. |
| Dict-form expressions migration misses a YAML | High | Step 1 exit criterion: `grep -rn 'expressions: \?\['` returns 0. |
| Three-way mutual exclusion misses a corner | Low | Validator counts non-None; tests cover all 7 non-empty subsets. |
| Recursion: staging config retains `wap` | Low | D3 strips. Pinned via test. |
| Selector grammar ambiguities | Low | D6 enumerates edges; tests cover. |
| `_resolved_sql` PrivateAttr leak | Low | Pydantic semantics; pinned via test. |
| Round-trip `WAPConfig` validation requires file on host | Medium | Documented; PrivateAttr equality semantics keep schema equality stable. |
| Cache eligibility wrong per validator type | Medium | D11 reasoned per-type; tests pin per-type behavior. |
| `deactivate_metric` race with concurrent forward run | Low | H1 read-back-and-bump; INSERT-only (N9). |
| BackfillReport memory blow-up for huge runs | Low | Operator-controlled; ~5MB for typical 20k entries. |
| Range expansion edge cases (start > end, leap dates) | Medium | `expand_range` is a pure helper with explicit tests for these cases. start > end → empty range; leap dates handled by step arithmetic. |
| WAP DataFrame writer breaks existing publish on Pandas backend | High | Pandas branch keeps existing SQL fallback; pinned via test. |
| `allow_create=True` default could surprise operators in catalog/RBAC environments | Medium | Documented in `docs/wap.md` and release notes; `False` is one keyword away. |
| Dict-form `expressions` YAML schema break causes silent confusion when operators run old YAMLs | High | Validator emits an explicit "expected dict, got list" error referencing the migration doc; release notes carry sed recipe. |
| Partition-error isolation (N18) hides real bugs by reporting them as `errored` and continuing | Medium | After all partitions complete, `qf.backfill` raises with the report attached — operator sees the error count and can introspect per-partition. CLI exit code 1. |
| Existing `Qualifire.validate(write_sql=...)` calls in operator notebooks break post-rename | Medium | Documented in release notes. The notebook in `tests/manual/notebook_end_to_end.ipynb` is updated as part of Step 2. |
| Sample collector "no historical samples" detection mistakes "all-null partition" for "no rows" | Low | Detection is on row count (count == 0); a non-empty partition with all-null target column produces a different skip path (validator's existing all-null handling). |

---

## Acceptance Checklist

### Sub-feature A (dict-form expressions)
- [ ] `AggregationCollectionConfig.expressions: dict[str, str]`.
- [ ] `<expr> AS <name>` lexer removed from collector.
- [ ] All YAMLs migrated.
- [ ] All Python construction sites migrated.
- [ ] Builders construct dicts directly.
- [ ] List-form YAML raises with diagnostic.

### Sub-feature B (rename + sql_file)
- [ ] `WAPConfig.write_sql` renamed to `WAPConfig.sql`.
- [ ] `sql_file` field; load-time `_resolved_sql` PrivateAttr.
- [ ] Three-way mutual exclusion.
- [ ] `effective_sql` property; engine reads it.
- [ ] All 110-ish source occurrences migrated.
- [ ] Round-trip preserves `sql_file`, drops `_resolved_sql`.

### Sub-feature C (WAP audit propagation)
- [ ] `_run_wap` forwards dataset-level fields per D3.
- [ ] Recursion guard pinned (`wap is None` in staging config).

### Sub-feature D (DataFrame writer publish + allow_create)
- [ ] WAPExecutor.publish uses DataFrame writer on Spark.
- [ ] `allow_create: bool = True` on `WAPConfig`.
- [ ] partitionBy / format / format-options honoured.
- [ ] Pandas SQL fallback preserved.
- [ ] `backend.table_exists` helper exists per backend.

### Sub-feature E (backfill API + report)
- [ ] `Qualifire.backfill(...)` API.
- [ ] `BackfillReport` + `PartitionDiff` exported.
- [ ] partition_ts shapes (single, list, range).
- [ ] selector restricts.
- [ ] N18 partition-error isolation.

### Sub-feature F (deactivate)
- [ ] `qualifire/core/deactivate.py`.
- [ ] `Qualifire.deactivate_metric(...)`.
- [ ] H1 read-back-and-bump.
- [ ] Idempotent retry.
- [ ] `note` propagates.
- [ ] Loud-fail on legacy table missing column.

### Sub-feature G (CLI)
- [ ] `qualifire backfill`, `qualifire deactivate-metric`,
      `--skip-if-cached`.
- [ ] `--partition` grammar (single/list/range).
- [ ] `--json` output.
- [ ] Exit codes per D13.

### Sub-feature H (engine backfill loop)
- [ ] `_run_backfill` driver in `engine.py`.
- [ ] Per-partition source select (D9).
- [ ] Sample collector handling (D10).
- [ ] `details_json.collected_via='backfill'` on every row.

### Sub-feature I (selectors)
- [ ] `qualifire/core/selectors.py`.
- [ ] Three-segment grammar; wildcards.
- [ ] No-match raises.
- [ ] Multi-selector dedup.
- [ ] Edge cases (trailing colon, empty middle).

### Sub-feature J (skip_if_cached)
- [ ] `qualifire/core/backfill_eligibility.py`.
- [ ] Engine pre-pass.
- [ ] Validators emit `from_cache=True`.
- [ ] Tombstone cache rows treated as miss.
- [ ] Per-validator-type eligibility per D11.

### Sub-feature K (storage parity)
- [ ] Delta + EC + JDBC: 8 read methods.
- [ ] H6 method on all three.
- [ ] 15-case parity per backend.
- [ ] Cross-backend H6 parity.
- [ ] `COALESCE(is_active, 'true')` everywhere.
- [ ] `read_validation_history_bulk(limit > 1)` raises on each.

### Sub-feature L (e2e)
- [ ] `tests/test_backfill_e2e.py` covers forward → deactivate
      → backfill → re-deactivate.
- [ ] WAP target-mode backfill exercise.
- [ ] Runs on SQLite and JDBC; Delta + EC gated.

### Sub-feature M (docs)
- [ ] `docs/backfill_and_soft_delete.md`.
- [ ] `examples/backfill_quickstart.py` runs.
- [ ] `docs/wap.md` updated (rename, sql_file, DataFrame writer,
      partition_column, allow_create, per-dataset partition_ts).
- [ ] `docs/configuration.md` (dict expressions, schema
      migration).
- [ ] `docs/programmatic_api.md` (backfill, deactivate, report).
- [ ] `docs/cli.md`.
- [ ] README cross-links.
- [ ] CHANGELOG / release notes (covers A + B breaking changes).

### Sub-feature WAP-pc (`partition_column`)
- [ ] Field on `WAPConfig`.
- [ ] Validator pattern.
- [ ] Required-when-target-mode-backfill enforcement at
      `Qualifire.backfill` entry.

### Cross-cutting
- [ ] `pytest -x -q` green.
- [ ] No new lint / mypy errors.
- [ ] PR description maps each commit to a step.
- [ ] `idea.md` Notes appended with "all sub-features shipped."

---

## Out-of-band review notes

- Plan went through 2 adversarial self-passes + codex
  `codex-reviewer:feature-review-plan` rounds. Codex round 1
  verdicted FAIL with a slicing recommendation; round 2 was on
  a slimmer slice that the operator overrode in favor of "ship
  everything in one PR." This plan is the resulting comprehensive
  plan after that override. Codex rounds against this version
  may push back on PR size; the response is "operator made the
  call; the PR is structured commit-by-commit so reviewers can
  take it in pieces."
