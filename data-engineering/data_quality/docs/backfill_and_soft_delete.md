# Backfill and Soft-Delete

This document covers the operator-facing surfaces shipped in
`backfill-api-and-wap-target-mode`:

- **Soft-delete** (`is_active='false'` tombstones) on every backend.
- **`Qualifire.deactivate_metric(...)`** — programmatic write of a
  tombstone INSERT.
- **`Qualifire.backfill(...)`** — refresh past-partition metrics from
  WAP target tables (or non-WAP sources filtered to the partition).
- **`skip_recollection`** — short-circuit cache-eligible collectors.
- The matching CLI subcommands (`qualifire backfill`,
  `qualifire deactivate-metric`, `--skip-recollection`).

## What soft-delete means

Every collection / validation row qualifire writes carries an
`is_active` column (TEXT: `'true'` / `'false'`). Read paths apply the
filter **after** the per-natural-key dedup ROW_NUMBER, so a
tombstone (newer `run_timestamp`) wins over an older active row and
the result set excludes the (table, metric, partition, dim) tuple
entirely. See plan invariants H1, H2, H6, H8 in
`docs/features/metrics-backfill-and-soft-delete/plan.md`.

The shared schema declares `is_active`. SQLite auto-adds the column
on `initialize()` for legacy tables; **Delta, ExternalCatalog, and
JDBC do NOT auto-migrate**. Operators upgrading those backends from
a pre-foundation install must run the per-backend ALTER once before
their first `deactivate_metric` or `backfill(soft_delete_prior=True)`
call.

### Schema migration (Delta / ExternalCatalog / JDBC)

```sql
-- Delta / Spark catalogs
ALTER TABLE catalog.schema.qualifire_history ADD COLUMNS (is_active STRING);

-- JDBC (Postgres / MySQL / Oracle / SQL Server)
ALTER TABLE qualifire_history ADD COLUMN is_active VARCHAR(8);
-- (Oracle: VARCHAR2(8); SQL Server: NVARCHAR(8) — adjust per dialect)
```

Reads tolerate the missing column via `COALESCE(is_active, 'true')`,
so existing read traffic on legacy tables continues working without
the migration. Writes that emit tombstones (`deactivate_metric`,
`backfill --soft-delete-prior`) require the column.

## `Qualifire.deactivate_metric(...)`

Tombstones every active row matching the filter.

```python
from qualifire import Qualifire

qf = Qualifire(
    backend=backend,
    system_table="cat.schema.qualifire_history",
    system_table_backend="sqlite",
)
n = qf.deactivate_metric(
    config,                       # YAML path or QualifireConfig
    dataset_name="sales_daily",
    metric_name="row_count",
    dimension_value=None,         # NULL = non-dimensioned
    partition_ts="2026-04-01",    # None = all partitions
    note="OPS-12345",             # captured in details_json
)
print(f"Wrote {n} tombstone(s)")
```

H1 read-back-and-bump applies: the tombstone's `run_timestamp` is
`max(now, latest.run_timestamp + 1ms)` so it always sorts newer
than the active row(s) it supersedes. Idempotent retry returns 0
with a debug log; no exception.

CLI:

```bash
qualifire deactivate-metric \
    --config qualifire.yaml \
    --dataset sales_daily \
    --metric row_count \
    --partition 2026-04-01 \
    --note "OPS-12345"
```

## `Qualifire.backfill(...)`

Refresh past-partition metrics without re-running the source-side
write SQL.

```python
report = qf.backfill(
    config,
    partition_ts="2026-04-01",   # single, list, or (start, end) tuple
    selector="sales:*",          # optional: <dataset>:<validation>[:<metric>]
    data=False,                  # WAP-only: True = full WAP cycle
    skip_recollection=False,        # short-circuit cache-eligible collectors
    soft_delete_prior=False,     # tombstone prior rows before backfill INSERTs
    parallelism=1,               # 1 (serial) - 64 thread-pool workers
    max_partitions=10000,        # cap on (start, end) range expansion
)

print(f"refreshed={report.refreshed} unchanged={report.unchanged} "
      f"skipped={report.skipped} errored={report.errored}")
for diff in report.partitions:
    print(f"{diff.dataset_name}.{diff.metric_name} @ "
          f"{diff.partition_ts}: {diff.original_value} → "
          f"{diff.backfilled_value} [{diff.status}]")
```

### Per-partition source select

| Dataset has WAP? | `data=` | Source |
|------------------|---------|--------|
| Yes | False (default) | `wap.target_table WHERE wap.partition_column = P` |
| Yes | True | Full WAP cycle on the past partition (write→audit→publish) |
| No | False | `dataset.table` filtered by `partition_ts = P` |
| No | True | Rejected — `data=True` is WAP-only. |

The metrics-only path (default) is the cheap way to refresh
metrics: it reads from the already-published target table, doesn't
re-run multi-billion-row source aggregations, and is idempotent
across reruns. Plan decision pin #1 / N6 / N13.

### Range expansion

`partition_ts=("2026-04-01", "2026-04-07")` expands to one anchor
per `effective_partition_step` step (inclusive on both ends). The
range is capped at 10000 partitions to prevent runaway expansion;
beyond that, pass an explicit list.

### Selector grammar

```
qualifire backfill --config q.yaml --selector "sales:*"
qualifire backfill --config q.yaml --selector "sales:row_count_check"
qualifire backfill --config q.yaml --selector "*:*:revenue"
```

Three-part addressable: `<dataset>:<validation>[:<metric>]`. Each
part may be `*` for wildcard. Comma-separated for multiple
selectors. Selector matching no scope raises `QualifireConfigError`
("selector matched no scope").

### CLI

```bash
qualifire backfill --config q.yaml \
    --partition 2026-04-01 \
    --selector "sales:*" \
    --skip-recollection \
    --json
```

`--partition` accepts:
- Single ISO-8601 date / datetime.
- Comma-list: `2026-04-01,2026-04-02,2026-04-03`.
- Range: `2026-04-01..2026-04-07` (inclusive).

Exit codes:
- `0` — all partitions completed successfully.
- `1` — at least one partition errored.
- `2` — argparse usage error.

### Backfill report

`BackfillReport` is exported from the top-level package:

```python
from qualifire import BackfillReport, PartitionDiff
```

Frozen dataclass; public attributes (`partitions`, `refreshed`,
`unchanged`, `skipped`, `errored`) are a stable contract — future
versions add fields but never remove. `to_dict()` produces a
JSON-serializable shape suitable for `--json` CLI output.

### Deactivate-after-backfill stickiness

A backfill that re-collects a deactivated key intentionally **wins**
(newer `run_timestamp`). Operators who want a deactivation to stay
sticky run `deactivate_metric` AFTER any backfill that touches the
range. To express the same intent in a single command, use
`backfill(..., soft_delete_prior=True)` — every metric touched gets
a tombstone INSERT BEFORE the new collection INSERTs, so the row
history shows the operator's deactivate intent even though the
backfill row is the one currently active.

## `parallelism`

`Qualifire.backfill(parallelism=N)` (default `1`, capped at `64`)
fans out distinct `(scope, anchor)` units across a thread pool.
Useful for backfilling a year of daily partitions: with
`parallelism=8` an 8x speedup is typical on Spark-backed datasets
(I/O-bound on storage reads + Spark calls).

**Notification suppression.** When `parallelism > 1`, the backfill
driver forces `notifiers={}` on the inner engine call to avoid
suppression-read races between workers reading and writing
`read_validation_history_bulk` concurrently. The returned
`BackfillReport.notifications_suppressed` field flags this; the
CLI prints a stderr advisory on entry. To restore notifications,
re-run with `parallelism=1`.

**SQLite caveat.** The dev/test SQLite backend serializes all
storage I/O behind a `RLock` (parallelism doesn't speed up SQLite
backfills). Production parallelism wins live in Spark-backed
backends.

**Cancellation contract.** Ctrl-C / `KeyboardInterrupt` propagates;
queued futures are cancelled, in-flight workers complete, no
partial `BackfillReport` is constructed. Re-run with a narrowed
range to recover.

## `max_partitions`

`Qualifire.backfill(max_partitions=N)` (default `10000`) caps the
number of partitions produced by a `(start, end)` range
expansion. Operators backfilling 5 years of hourly partitions
(~43800) can lift this; smaller values surface runaway
expansions earlier. CLI: `--max-partitions N`.

## `skip_recollection`

Short-circuits the **collection step** for cache-eligible validators
when a matching cached value already exists in the system table at
the resolved partition anchor.

```python
qf.run_config(yaml_path, skip_recollection=True)
```

Or via CLI:

```bash
qualifire run --config q.yaml --skip-recollection
```

The cache stores **values, not validation verdicts**. Threshold
rules and other validator parameters re-evaluate against the cached
value — a threshold change between the original run and the
`skip_recollection` re-run produces a different verdict against the
same cached metric. This is intentional.

Cache-eligible validator types (sub-feature J / D11):

- `ThresholdValidation` — True
- `HistoricalValidation` — True (collection saved; history reads
  still run)
- `ForecastValidation` — True (collection saved; model runs)
- `AnomalyDetectionValidation` — False (samples, not metric_value)
- `PatternValidation` — False (samples, not metric_value)
- `SLOValidation` — False (collection IS the freshness clock)

## `WAPConfig` field rename: `write_sql` → `sql` + `sql_file`

Sub-feature B (clean break — no compat alias). Three source forms,
pairwise mutually exclusive:

```yaml
wap:
  target_table: "revenue.daily_summary"
  sql: "SELECT * FROM raw.events WHERE event_date = '{{ ds }}'"
```

```yaml
wap:
  target_table: "revenue.daily_summary"
  sql_file: "configs/d3_write.sql"   # read at config-load time
```

```python
WAPConfig(
    target_table="revenue.daily_summary",
    df=my_dataframe,                  # DataFrame-driven
)
```

Migration:

```bash
sed -i 's/write_sql:/sql:/g' your-config.yaml
```

```python
# Python
WAPConfig(write_sql="...")  →  WAPConfig(sql="...")
Qualifire().write_audit_publish(write_sql="...")  →  ...sql="..."
```

`sql_file` paths resolve relative to `QUALIFIRE_CONFIG_BASE_DIR`
env var if set, else CWD. Absolute paths used as-is. UTF-8 strict;
file-read errors map to `QualifireConfigError`.

## WAP DataFrame-writer publish (sub-feature D)

Spark-capable backends route WAP publish through the DataFrame
writer (`df.write.format(...).options(...).saveAsTable(target)`),
honouring `write_options['format']`, `partitionBy`, and
format-specific options (Delta `mergeSchema`, etc.) — none of
which the prior `INSERT INTO/OVERWRITE TABLE` SQL path consumed.

```yaml
wap:
  target_table: "revenue.daily_summary"
  sql: "..."
  allow_create: true        # default; gates target auto-creation
  partition_column: "event_date"  # required for backfill
  write_options:
    format: "delta"
    partitionBy: ["region", "month"]
    mergeSchema: "true"
    mode: "overwrite"
```

`allow_create=False` restores the strict "target must pre-exist"
contract — useful in catalog/RBAC-controlled environments.

Pandas-style backends (no `.spark` attribute) keep the existing SQL
fallback (`INSERT INTO` / `INSERT OVERWRITE`) — the only mode and
options preserved by that path.

## Dict-form `expressions:` (sub-feature A)

Sub-feature A renames `AggregationCollectionConfig.expressions`
from `list[str]` (with a fragile `<expr> AS <name>` lexer) to a
`dict[str, str]` of `{metric_name: sql_expression}`. The dict form
makes metric names addressable for the backfill loop's per-metric
source-select, and YAML / Python share one shape.

Migration:

```yaml
# OLD
expressions:
  - "COUNT(*) AS row_count"
  - "AVG(amount) AS avg_amount"

# NEW
expressions:
  row_count: "COUNT(*)"
  avg_amount: "AVG(amount)"
```

```python
# OLD
AggregationCollectionConfig(expressions=["COUNT(*) AS cnt"])

# NEW
AggregationCollectionConfig(expressions={"cnt": "COUNT(*)"})
```

Builder methods (`Qualifire.threshold_check`,
`Qualifire.drift_check`, `Qualifire.trend_check`) construct dicts
directly from operator-supplied `aggregations: dict[str, str]` —
no list manipulation needed.

The list form raises a precise migration hint at
`AggregationCollectionConfig` construction:

> AggregationCollectionConfig.expressions is now a dict[str, str]
> (sub-feature A). Convert ["<expr> AS <name>", ...] to
> {"<name>": "<expr>", ...}. See docs/configuration.md for the
> migration recipe.

## Foundation-vs-claimed-shipped audit

The foundation feature `metrics-backfill-and-soft-delete/shipped.md`
claimed certain scaffolds shipped that were not actually present in
`main`. This PR creates them:

- `qualifire/core/backfill_eligibility.py` (Step 6 / sub-feature J).
- `Qualifire.validate(skip_recollection=...)` kwarg + same on
  `run_config` and `run_config_parsed` (Step 6).
- `expected_metrics()` resolver on Threshold / Historical / Forecast
  validator configs (Step 6).
- `_run_backfill` engine driver — replaced by
  `qualifire/core/backfill.py:run_backfill` (Steps 7 + 10).

A small follow-up commit on the foundation's `shipped.md` to fix
the doc-vs-code drift is left for later.
