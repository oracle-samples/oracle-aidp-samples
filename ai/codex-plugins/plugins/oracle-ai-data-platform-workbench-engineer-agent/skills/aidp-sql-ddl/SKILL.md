---
name: aidp-sql-ddl
description: Write-side Spark SQL on the AIDP lakehouse — DDL (CREATE/ALTER/DROP table·view·schema), DML (INSERT/UPDATE/DELETE/MERGE upsert), and Delta maintenance (OPTIMIZE, VACUUM, time travel, RESTORE, schema evolution, liquid clustering). Use when the user wants to create/alter/drop a table or view, insert/update/delete/merge rows, upsert, compact/optimize/vacuum a Delta table, query an old version (time travel), restore, evolve a schema, or run any non-SELECT SQL. For read-only analytics use aidp-analyzing-data; for control-plane (CLI/REST) table lifecycle use aidp-table-management.
---

# `aidp-sql-ddl` — lakehouse write SQL (DDL / DML / Delta maintenance)

AIDP runs **Spark 3.5 + Delta Lake 3.2** (platform-ref §39), so the full write grammar works through the
bundled `$HOME/.aidp/aidp_sql.py` helper — the same engine `aidp-analyzing-data` uses for SELECT. This skill
covers everything that *changes* data or schema. **Live-verified on AIDP 2026-06-10** (USER cluster, via
`aidp_sql.py`, all un-wrapped → `status: ok`): `CREATE TABLE`, `INSERT`, `UPDATE`, `DELETE`, `MERGE`,
`OPTIMIZE`, `VACUUM`, `DESCRIBE HISTORY`, time travel (`VERSION AS OF`), `DROP` — the full Spark 3.5 / Delta
3.2 write grammar (platform-ref §39) executes end-to-end. (A tester once saw the first bare `CREATE TABLE` on a
brand-new instance throw a transient catalog-registration error; it was **not reproducible** on a fresh
instance — see Caveat below.)

## When to use
- Create/alter/drop a table, view, or schema · insert/update/delete/**merge (upsert)** rows ·
  **OPTIMIZE / VACUUM / time-travel / RESTORE** a Delta table · evolve a schema · liquid clustering.
- NOT for SELECT/analytics → `aidp-analyzing-data`. NOT for control-plane catalog/connection registration
  → `aidp-table-management`.

## Mutation gate (required)
Every statement here **changes state**. Before running any DML/DDL:
1. Confirm the cluster is RUNNING (`aidp-cluster-ops`) and qualify objects fully (`catalog.schema.table`).
2. **Show the exact SQL and get explicit confirmation** before executing — especially `DROP`, `DELETE`,
   `TRUNCATE`, `INSERT OVERWRITE`, `VACUUM` (VACUUM permanently removes old files → breaks time travel).
3. Persist the statement(s) to `.aidp/payloads/` per [references/payloads.md]($HOME/.aidp/references/payloads.md).
4. Prefer a dry-run first: `SELECT count(*) … WHERE <predicate>` before the matching `UPDATE/DELETE`.

## Execute (same helper as analyzing-data)
```bash
python "$HOME/.aidp/aidp_sql.py" \
  --region <region> --datalake <DATALAKE_OCID> --workspace <ws> --cluster <cluster-key> \
  --code "spark.sql('''<SQL>''')"
```
Returns JSON `{status, execution_count, outputs, spark_job_ids, error}`. No MCP / no `AIDP_SESSION` needed.

## Quick reference (Spark 3.5 / Delta 3.2)
| Need | SQL |
|---|---|
| Create managed Delta table | `CREATE TABLE c.s.t (id INT, v STRING) USING DELTA` |
| CTAS | `CREATE TABLE c.s.t USING DELTA AS SELECT …` |
| Liquid clustering | `CREATE TABLE … CLUSTER BY (col)` (or `ALTER TABLE … CLUSTER BY`) |
| Insert | `INSERT INTO c.s.t VALUES (…)` · full reload `INSERT OVERWRITE c.s.t SELECT …` |
| Update / Delete | `UPDATE c.s.t SET v='x' WHERE …` · `DELETE FROM c.s.t WHERE …` |
| **Upsert (MERGE)** | `MERGE INTO c.s.t t USING src s ON t.id=s.id WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *` |
| Schema evolution | `ALTER TABLE c.s.t ADD COLUMNS (x INT)` · `… RENAME COLUMN` · `… DROP COLUMN`; or write with `mergeSchema` |
| Compact | `OPTIMIZE c.s.t [ZORDER BY (col)]` |
| Reclaim files | `VACUUM c.s.t RETAIN 168 HOURS` *(destructive — confirm; <168h needs `spark.databricks.delta.retentionDurationCheck.enabled=false`)* |
| History | `DESCRIBE HISTORY c.s.t` |
| Time travel | `SELECT * FROM c.s.t VERSION AS OF 3` · `… TIMESTAMP AS OF '2026-06-01'` |
| Restore | `RESTORE TABLE c.s.t TO VERSION AS OF 3` |
| View | `CREATE VIEW c.s.v AS SELECT …` · `CREATE OR REPLACE VIEW …` · `DROP VIEW …` |
| Schema (namespace) | `CREATE SCHEMA IF NOT EXISTS c.s` · `DROP SCHEMA c.s [CASCADE]` |

## Caveat — a transient first-`CREATE TABLE` report (NOT reproduced)
A tester once reported that the first bare `CREATE TABLE … USING DELTA` on a just-provisioned DataLake threw
`ArrayIndexOutOfBoundsException: Index 0 out of bounds for length 0` (a suspected catalog-registration race,
not a SQL error). **This could NOT be reproduced.** On a genuinely freshly-provisioned DataLake + cluster, the
*very first* bare `CREATE TABLE` **succeeded** (live-checked 2026-06-12 on a brand-new instance — table created,
confirmed via `SHOW TABLES`; also fine on established instances). So treat it as a possible **transient** in a
narrow post-provision window, not a deterministic behavior. If you ever do hit it, don't hammer the DDL —
create the first table via the writer (`spark.createDataFrame(rows).write.saveAsTable('c.s.t')`) or
ingest-then-CTAS (`aidp-ingest-file-to-table`) to warm the catalog, then bare `CREATE TABLE` works.

## Reliability
- Never fabricate table/column names — verify against `.aidp/catalog.md` or a `DESCRIBE`/`SHOW COLUMNS` cell.
- On error, read the helper's `error` field, fix grounded in the catalog, retry once — don't guess repeatedly.
- MERGE is the idempotent upsert pattern for re-runnable pipelines (pair with `aidp-pipelines`).
- External/Iceberg/Delta-on-`oci://` table reads/writes from other sources → delegate to the
  `…-spark-connectors` plugin (`aidp-iceberg`, `aidp-object-storage`); this skill is lakehouse-native SQL.

## References
- [$HOME/.aidp/aidp_sql.py]($HOME/.aidp/aidp_sql.py) — the Spark-SQL executor
- [aidp-analyzing-data](../aidp-analyzing-data/SKILL.md) (SELECT) · [aidp-table-management](../aidp-table-management/SKILL.md) (control-plane CRUD) · [aidp-cluster-ops](../aidp-cluster-ops/SKILL.md) (cluster must be RUNNING)
- [references/payloads.md]($HOME/.aidp/references/payloads.md) — persist + confirm before mutating
