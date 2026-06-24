# DDL Rewrite Rules — Databricks → AIDP

The migrator's `${CLAUDE_PLUGIN_ROOT}/engine/scripts/catalog_ddl_rewriter.py` applies **18 rules** when porting Unity Catalog / HMS DDL to AIDP. This document is the reference; the actual implementation is `rewrite_table_ddl()` + `schema_create_sql()` in the migrator repo.

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

Only triggered when the source catalog appears in the catalog manifest. Without a manifest, 3-part names pass through unchanged (the migrator does not assume `<src_cat>` should map to `default`).

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

Bucket → namespace mapping comes from the customer's `bucket_mapping.json` (see [`aidp-bucket-mapping`](../skills/aidp-bucket-mapping/SKILL.md)).

### Rule 4: Reject when bucket not in mapping

```
-- IN with unknown bucket:
CREATE TABLE ... LOCATION 's3://<unknown-bucket>/...'
-- OUT:
[SKIP] table <schema>.<table>: S3 bucket "<unknown-bucket>" not found in OCI bucket mapping
```

Statement is omitted from the replay batch; the bad table is listed in the rewriter's `UnsupportedDDL` output.

### Rule 5: `dbfs:/` paths translated to `/Volumes/<vol>/`

```
-- IN:
LOCATION 'dbfs:/FileStore/<path>'
-- OUT:
LOCATION '/Volumes/default/default/dbfs/FileStore/<path>'
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

### Rule 18: VIEW with cross-catalog FROM clause flagged

```
-- IN:
CREATE VIEW <schema>.<v> AS SELECT * FROM <other_cat>.<other_schema>.<other_table>
-- OUT (if <other_cat> not in manifest):
[FLAG] view <schema>.<v>: references cross-catalog <other_cat>.<other_schema>.<other_table>;
       review whether the source catalog should be added to --catalog-manifest before replay.
```

Statement IS replayed (views are cheap; let it fail loudly if the catalog truly doesn't resolve).

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
python3 ${CLAUDE_PLUGIN_ROOT}/engine/scripts/migrate_catalog.py \
  --pack reports/catalog_pack.json \
  --dry-run > /tmp/catalog_rewrite_preview.txt
less /tmp/catalog_rewrite_preview.txt
```

---

## What this rewriter does NOT do

- **Stored procedures / SQL UDFs** — not in scope. Re-author in Spark SQL or as Python UDFs.
- **GRANT / REVOKE / OWNERSHIP** — AIDP's permission model is different. Re-apply manually.
- **Sequences** — preserve manually if needed.
- **Identity columns** (`GENERATED ALWAYS AS IDENTITY`) — AIDP support varies by Spark version; review case-by-case.
- **Generated columns** (`GENERATED ALWAYS AS (expr)`) — preserved as-is.
