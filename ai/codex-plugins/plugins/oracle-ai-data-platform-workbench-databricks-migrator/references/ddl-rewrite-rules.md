# DDL Rewrite Rules — Databricks → AIDP

The migrator's `~/.aidp-migrator/engine/scripts/catalog_ddl_rewriter.py` implements **10 rules** when porting Unity Catalog / HMS DDL to AIDP — a subset of an 18-rule design spec; the un-implemented spec rules surface as warnings. The implemented rule ids in code are R01, R02, R03, R09, R10, R11, R13, R14, R16, R17 (the numbering BELOW is this document's own reference numbering, not the code's rule ids). The actual implementation is `rewrite_table_ddl()` + `schema_create_sql()` in `catalog_ddl_rewriter.py`.

Each rule below shows the **input shape** (what Databricks emits) and the **output shape** (what AIDP accepts).

---

## Naming + qualification

### Rule 1: 3-part → 2-part name flatten

```
-- IN  (Databricks UC):
CREATE TABLE <src_cat>.<schema>.<table> (...)

-- OUT (AIDP, single catalog = default):
CREATE TABLE <schema>.<table> (...)
```

With the default `--flatten-strategy catalog-remap`, EVERY 3-part name is flattened: a source catalog listed in `--catalog-map` maps to its declared target, and any unmapped source catalog maps to `--default-catalog` (default `default`). Alternative strategies: `schema-prefix` (`<cat>_<schema>.<table>` — preserves the origin catalog in the schema name) and `schema-only`.

### Rule 2: Backticked identifiers preserved through flatten

```
-- IN:
CREATE TABLE `<src_cat>`.`<schema with space>`.`<table>` (...)
-- OUT:
CREATE TABLE `<schema with space>`.`<table>` (...)
```

Backticks survive every rule below; the rewriter is identifier-quote-aware.

---

## External-table locations

### Rule 3: `s3://` → `oci://` via bucket-map

```
-- IN:
CREATE TABLE <schema>.<table> ... LOCATION 's3://<bucket>/<path>'
-- OUT:
CREATE TABLE <schema>.<table> ... LOCATION 'oci://<bucket>@<namespace>/<path>'
```

Bucket → namespace mapping comes from your `bucket_mapping.json` (see [`aidp-bucket-mapping`](../skills/aidp-bucket-mapping/SKILL.md)).

### Rule 4: Warn (do NOT reject) when bucket not in mapping

```
-- IN with unknown bucket:
CREATE TABLE ... LOCATION 's3://<unknown-bucket>/...'
-- OUT:
CREATE TABLE ... LOCATION 's3://<unknown-bucket>/...'   -- unchanged
-- plus audit entry: R02_BUCKET_UNMAPPED (severity: warn)
```

The statement is NOT skipped: the original `s3://` LOCATION passes through unchanged and the rewriter records an `R02_BUCKET_UNMAPPED` warning in its rule-application audit. Review every such warning in `--out-report` / `--dry-run` output — a replayed table that still points at `s3://` fails at first read on AIDP. Fix by adding the bucket to the mapping CSV and re-running.

### Rule 5: `dbfs:/` paths — NOT rewritten (passed through; the caller warns and the path needs a separate dbfs mapping)

```
-- IN:
LOCATION 'dbfs:/FileStore/<path>'
-- OUT (unchanged; reported for a separate dbfs mapping):
LOCATION 'dbfs:/FileStore/<path>'
```

The migrator's `translate_path()` helper handles this.

---

## Storage format

### Rule 6: Source storage format **preserved** (Delta stays Delta)

```
-- IN  (Databricks UC, Delta-backed table):
CREATE TABLE ... USING DELTA
TBLPROPERTIES ('delta.minReaderVersion'='2', 'delta.minWriterVersion'='5', 'description'='…')

-- OUT (AIDP, Delta preserved):
CREATE TABLE ... USING DELTA
TBLPROPERTIES ('description'='…')
```

The catalog rewriter **does not downgrade Delta tables**. The CLI default is `--target-using None`, which means *preserve the source storage format*: Delta stays Delta, Parquet stays Parquet, Iceberg stays Iceberg. AIDP supports Delta natively. To deliberately convert (e.g. Delta → Parquet for a cluster without the Delta library), pass `--target-using parquet` explicitly.

All `delta.*` table properties ARE still stripped (see Rule 9 below) — Delta-runtime metadata (`delta.minReaderVersion`, `delta.feature.*`, `delta.lastCommitTimestamp`, …) is Delta-managed and cannot be set as a CREATE-time `TBLPROPERTIES`. The new Delta table inherits its own runtime defaults at create time. So Rule 6 = preserve source format; Rule 9 = scrub Delta-managed properties so the CREATE doesn't reject them.

### Rule 7: `USING ICEBERG` → kept (Iceberg is supported)

Pass-through.

### Rule 8: `USING HIVE` / file-format synonyms (`ORC`, `JSON`, `CSV`, `AVRO`) → preserved

Pass-through.

---

## Property scrub (`DROPPED_PROP_PREFIXES` catch-all)

### Rule 9: `delta.*` catch-all

```
-- IN:
TBLPROPERTIES (
  'delta.minReaderVersion' = '2',
  'delta.minWriterVersion' = '5',
  'delta.enableDeletionVectors' = 'true',
  ...
)
-- OUT:
(all delta.* keys dropped; if no other props remain, TBLPROPERTIES clause is removed entirely)
```

Implementation: `DROPPED_PROP_PREFIXES = ("spark.sql.", "delta.", "view.query.", "unity.", "pipelines.")`. Any key matching one of these is silently stripped during the rewrite.

### Rule 10: `spark.sql.*` catch-all (e.g. `spark.sql.statistics.numRows`)

Dropped. These are session-level configs that can't be set in DDL.

### Rule 11: `unity.*` catch-all (UC-internal flags)

Dropped.

### Rule 12: `pipelines.*` catch-all (Databricks DLT properties)

Dropped.

### Rule 13: `view.query.*` catch-all (UC view-specific properties)

Dropped.

---

## Schema-level rules

### Rule 14: `CREATE SCHEMA … COMMENT '...'` colon strip

```
-- IN:
CREATE SCHEMA <schema> COMMENT 'team:<name>'
-- OUT:
CREATE SCHEMA <schema>
```

**Important data-correctness rule.** AIDP silently corrupts the schema when COMMENT contains a colon. The rewriter ALWAYS strips the COMMENT clause to be safe. To re-add comments post-migration, do so via `ALTER SCHEMA <schema> SET DBPROPERTIES ('description'='...')`.

### Rule 15: `MANAGED LOCATION` clause stripped

```
-- IN:
CREATE SCHEMA <schema> MANAGED LOCATION 's3://...'
-- OUT:
CREATE SCHEMA <schema>
```

AIDP's managed schema location is determined by the DataLake — not by the schema DDL.

---

## Reject (unsupported) types

### Rule 16: MATERIALIZED VIEW rejection

```
-- IN:
CREATE MATERIALIZED VIEW <schema>.<mv> AS SELECT ...
-- OUT:
[SKIP] materialized view <schema>.<mv>: AIDP does not support MATERIALIZED VIEW. Re-implement as a scheduled CTAS pipeline.
```

### Rule 17: STREAMING TABLE rejection

```
-- IN:
CREATE STREAMING TABLE <schema>.<st> AS SELECT ... FROM STREAM ...
-- OUT:
[SKIP] streaming table <schema>.<st>: AIDP does not support STREAMING TABLE (DLT-only). Re-implement as structured streaming + scheduled writer.
```

### Rule 18 (spec only, not implemented): VIEW handling

Views are not replayed: the rewriter records `R15_VIEW_DEFERRED` and passes the
statement through for manual migration (the source-workspace samples contain no
views, so this path is deliberately deferred).
```
-- IN:
CREATE VIEW <schema>.<v> AS SELECT * FROM <other_cat>.<other_schema>.<other_table>
-- OUT: unchanged; verdict row R15_VIEW_DEFERRED
```

Statement is NOT replayed (recorded as `R15_VIEW_DEFERRED` and passed through) (views are cheap; let it fail loudly if the catalog truly doesn't resolve).

---

## End-to-end example

Input (Databricks UC `DESCRIBE TABLE EXTENDED ... ; SHOW CREATE TABLE` style):

```sql
CREATE TABLE <src_cat>.<schema>.<events>
(
  event_id BIGINT NOT NULL,
  customer_id STRING,
  amount DECIMAL(18,2),
  event_date DATE
)
USING DELTA
PARTITIONED BY (event_date)
LOCATION 's3://<src_bucket>/<schema>/<events>'
TBLPROPERTIES (
  'delta.minReaderVersion' = '2',
  'delta.minWriterVersion' = '5',
  'spark.sql.statistics.numRows' = '1234567',
  'description' = 'event log'
)
COMMENT 'team:platform owner:alice';
```

After rewriter (assuming `<src_cat>` is in the manifest and `<src_bucket>` maps to `<oci_bucket>@<oci_ns>`, default `--target-using None` preserves the source `DELTA` format):

```sql
CREATE TABLE <schema>.<events>
(
  event_id BIGINT NOT NULL,
  customer_id STRING,
  amount DECIMAL(18,2),
  event_date DATE
)
USING DELTA
PARTITIONED BY (event_date)
LOCATION 'oci://<oci_bucket>@<oci_ns>/<schema>/<events>'
TBLPROPERTIES (
  'description' = 'event log'
);
```

(Pass `--target-using parquet` to deliberately convert to `USING parquet` instead.)

Note: the COMMENT clause is preserved on `CREATE TABLE` (only `CREATE SCHEMA` strips COMMENTs with colons).

---

## How to inspect what the rewriter did

`migrate_catalog.py --dry-run` prints each statement's BEFORE → AFTER. Pipe to a file and review before the live replay:

```bash
python3 ~/.aidp-migrator/engine/scripts/migrate_catalog.py \
  --pack reports/catalog_pack.json \
  --aidp-cluster <CLUSTER_ID> --aidp-lake <LAKE_OCID> --aidp-workspace <WORKSPACE_UUID> \
  --out-manifest reports/catalog_manifest.json --out-report reports/catalog_report.md \
  --dry-run > /tmp/catalog_rewrite_preview.txt
less /tmp/catalog_rewrite_preview.txt
```

(`--aidp-cluster`, `--aidp-lake`, `--aidp-workspace`, `--out-manifest`, `--out-report` are required by argparse even for `--dry-run`.)

---

## What this rewriter does NOT do

- **Stored procedures / SQL UDFs** — not in scope. Re-author in Spark SQL or as Python UDFs.
- **GRANT / REVOKE / OWNERSHIP** — AIDP's permission model is different. Re-apply manually.
- **Sequences** — preserve manually if needed.
- **Identity columns** (`GENERATED ALWAYS AS IDENTITY`) — AIDP support varies by Spark version; review case-by-case.
- **Generated columns** (`GENERATED ALWAYS AS (expr)`) — preserved as-is.
