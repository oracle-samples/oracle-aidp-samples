# Table maintenance and data layout — Snowflake vs AIDP

A customer who asks for "the same `OPTIMIZE`/`VACUUM` maintenance on AIDP" is
asking a question with a useful answer: **Snowflake has no such commands, and
AIDP does.** The difference is not a missing feature on either side — it is
*who runs maintenance*.

- **Snowflake** maintains layout and reclaims storage **automatically, in the
  background.** There is no `VACUUM` and no `OPTIMIZE` statement.
- **AIDP** (Spark 3.5 / Delta Lake 3.2.0 OSS) has `OPTIMIZE`, `VACUUM`,
  `ZORDER BY` and liquid clustering as **explicit statements** that the table
  owner schedules.

So the migration keeps the capability and moves the responsibility. Tell the
customer early: tables that nobody maintains accumulate small files, and
storage that nobody vacuums keeps growing.

## What each side offers

### Snowflake

| Concern | Mechanism | Who runs it |
|---|---|---|
| Data layout / clustering | `CLUSTER BY (cols)` + **Automatic Clustering** | Snowflake, background, serverless credits |
| Manual recluster | `ALTER TABLE … RECLUSTER` (deprecated) | — |
| Storage reclamation | Automatic once Time Travel + Fail-safe expire | Snowflake |
| Time travel | `DATA_RETENTION_TIME_IN_DAYS` (0–1 Standard, 0–90 Enterprise+) | Snowflake |
| Retention extension | `MAX_DATA_EXTENSION_TIME_IN_DAYS` | Snowflake |
| Disaster safety net | **Fail-safe: 7 days, not user-controllable** | Snowflake |
| Point lookups on non-cluster keys | Search Optimization Service | Snowflake |
| Change capture | `CHANGE_TRACKING` + streams | Snowflake |

Micro-partition maintenance is not exposed as a user command.

### AIDP — Delta Lake 3.2.0 (OSS) on Spark 3.5

| Concern | Statement / setting |
|---|---|
| Compaction | `OPTIMIZE c.s.t [WHERE <partition pred>]` — bin-packing, idempotent |
| Ordered layout | `OPTIMIZE c.s.t ZORDER BY (cols)` — **opt-in**; bin-packing alone does not order |
| Liquid clustering | `CREATE TABLE … CLUSTER BY (col)` / `ALTER TABLE … CLUSTER BY` |
| Storage reclamation | `VACUUM c.s.t RETAIN 168 HOURS` — **destructive, explicit** |
| Prevent small files | `optimizeWrite` (pre-write shuffle), AQE coalesce / `REBALANCE` hint |
| Compact after write | `autoCompact` — bin-packing only, **never z-orders** |
| Time travel | `DESCRIBE HISTORY`, `VERSION AS OF`, `TIMESTAMP AS OF`, `RESTORE` |
| Time-travel bound | `delta.deletedFileRetentionDuration`, `delta.logRetentionDuration` |
| Change capture | Change Data Feed (`delta.enableChangeDataFeed`) |

## The mapping

| Snowflake | AIDP equivalent | Same behaviour? |
|---|---|---|
| `CLUSTER BY` + Automatic Clustering | `CLUSTER BY` (liquid) or `OPTIMIZE … ZORDER BY` | **Not automatic.** Needs a scheduled job |
| (automatic storage reclamation) | `VACUUM … RETAIN n HOURS` | **Explicit and destructive** |
| `DATA_RETENTION_TIME_IN_DAYS` | `delta.deletedFileRetentionDuration` + `delta.logRetentionDuration` | Close, but coupled to `VACUUM` — see below |
| `MAX_DATA_EXTENSION_TIME_IN_DAYS` | — | No equivalent; Delta retention is one duration |
| Fail-safe (7 days) | — | **No equivalent**; plan backups and recovery accordingly |
| Search Optimization Service | Data skipping + ZORDER (partial) | **No direct equivalent** for arbitrary point lookups |
| `CHANGE_TRACKING` / streams | Change Data Feed | Broadly equivalent |
| (no `OPTIMIZE`) | `OPTIMIZE` | A capability Snowflake does not expose |

## Four points to settle before anyone commits

**1. On Delta, `VACUUM` is what bounds time travel.** In Snowflake, retention
and storage reclamation are separate, automatic concerns. On Delta they are
the same setting: vacuuming to a short retention removes the ability to query
`VERSION AS OF` beyond it. Choose the retention from the recovery
requirement, not from the storage bill. `RETAIN` below 168 hours is blocked
unless `spark.databricks.delta.retentionDurationCheck.enabled=false`; that
guard exists for exactly this reason and should stay on.

**2. `OPTIMIZE` increases storage until `VACUUM` runs.** It writes new
compacted files and keeps the old ones until retention expires, so the
physical file count falls only after `VACUUM`. Schedule `OPTIMIZE` together
with `VACUUM`.

**3. Maintenance is a scheduled job.** Every `OPTIMIZE`/`VACUUM` runs as an
AIDP job that the customer schedules and monitors — an operational task that
Snowflake handled in the background.

**4. Prefer preventing small files over compacting them.** Object storage
applies request rate limits (HTTP 429) under bursts of small-file writes, and
`OPTIMIZE`/`autoCompact` write additional files. `optimizeWrite` and AQE
coalescing reduce the number of files written in the first place.
`delta.targetFileSize` is not part of OSS Delta 3.2.0; use
`spark.databricks.delta.optimize.maxFileSize` (default 1 GiB) and
`spark.databricks.delta.optimizeWrite.binSize` (default 512 MiB). Both are OSS
Delta Spark settings that keep this historical prefix.

## What this plugin does

**Measures the source and reports the gap.** `snowmig
maintenance` writes `maintenance.json` + `MAINTENANCE.md`: clustering keys,
`automatic_clustering`, Search Optimization, `change_tracking`, the retention
cascade with per-table effective values, and reclustering credits plus DML
churn from `ACCOUNT_USAGE` — reporting *"not measured"* rather than zero when
the grant is absent.

Each data-movement option also declares who takes on the maintenance work and
which of the four points above apply to it, so the choice is made with that
cost visible.

Source settings with a Delta equivalent are **carried into the CREATE TABLE**
by `ddl` and listed per object in the DDL plan under *"Carried into the CREATE
TABLE"*:

- a plain-column clustering key as liquid `CLUSTER BY` (at most four keys,
  each on a column Delta keeps statistics for);
- `retention_time` (`DATA_RETENTION_TIME_IN_DAYS`) as
  `delta.deletedFileRetentionDuration` / `delta.logRetentionDuration`, only
  where it is above Delta's 7 / 30 day defaults (nothing is lowered);
- `change_tracking`, or a stream on the table, as
  `delta.enableChangeDataFeed = true`.

The structure job applies them and reads the properties back. `snowmig deploy
--execute` cannot carry them (the catalog API's table body has no field for
them); the plan says so per table (`R13`) and the deploy result lists each
clause under `properties_not_applied`. What cannot be carried — an expression
key, a key on a type Delta cannot cluster on — stays under *"Maintenance and
layout — decisions, NOT applied"* with the reason, and raises the object's
risk to MEDIUM.

**No maintenance job is generated or executed** — no `OPTIMIZE`, no `VACUUM`,
no `ZORDER BY`. The settings above are carried because they are the source's
own values; choosing a cadence needs the customer's recovery requirements and
query patterns, so it stays with the team that owns the target tables and is
outside this plugin's scope.
