---
name: aidp-federate
description: Federate across multiple data sources in one AIDP Spark session — read from several connectors (Oracle ADB/ExaCS, Fusion, Snowflake, S3, lakehouse tables, …) and join them in a single notebook. Use when the user wants to combine/join data from more than one source, blend an external system with the lakehouse, or do cross-source analysis. Composes the spark-connectors plugin; does not duplicate connectors.
---

# `aidp-federate` — cross-source federation in one Spark session

Blend multiple sources by reading each into one Spark session and joining them — a signature differentiator
(competitor agents degrade on foreign tables or need an external orchestrator + manual key-joins). Execution
is via the bundled `scripts/aidp_sql.py` helper (no MCP, no AIDP_SESSION required).

## When to use
- "Join data from <source A> and <source B>", "combine Fusion + the lakehouse", "blend Snowflake with
  store_sales", any cross-source analysis.
- **Also the entry point for a SINGLE external source** ("a notebook that connects to Fusion / EPM / ADB / …"):
  the connection recipe still comes from the spark-connectors plugin's `aidp-<source>` skill (step 1) — this
  skill just adds the join. **Never hand-roll the source connection.** (Check the spark-connectors plugin is
  installed via `claude plugin list`; run its **`aidp-connectors-bootstrap`** skill once to push the
  `oracle_ai_data_platform_connectors` helper package to `/Workspace/Shared` — via the AIDP MCP, or manually
  if the MCP can't reach the instance.)

## How it works (composition, not new surface)
1. **Source reads → spark-connectors plugin.** For each external source, use the matching connector skill
   from `oracle-ai-data-platform-workbench-spark-connectors` (e.g. `aidp-alh`/`aidp-oracle-db`/`aidp-exacs`,
   `aidp-fusion-bicc`/`aidp-fusion-rest`, `aidp-snowflake`, `aidp-aws-s3`, `aidp-object-storage`, …) to get
   the `spark.read.format(...).option(...).load()` recipe for each source. This skill does **not** re-implement
   connectors.
2. **Join in one session.** Run a single cell that reads each source into a DataFrame, registers each as a
   temp view, and `spark.sql(...)` the join — all in the **same** Spark session created by the helper. Use
   join keys from `.aidp/semantic.md` / `.aidp/catalog.md` (don't guess). The helper mints a UPST from the
   api_key DEFAULT profile and auto-creates a scratch notebook on the target cluster:
   ```bash
   python "$PLUGIN_DIR/scripts/aidp_sql.py" \
     --region <region> --datalake <DATALAKE_OCID> --workspace <ws> --cluster <cluster-key> \
     --code "
   df_lake = spark.table('default.default.customer')
   df_ext  = spark.read.format('...').option('...', '...').load()   # recipe from the connector skill
   df_lake.createOrReplaceTempView('lake')
   df_ext.createOrReplaceTempView('ext')
   spark.sql('''SELECT ... FROM lake l JOIN ext e ON l.key = e.key ...''').show(50, truncate=False)
   "
   ```
   Returns JSON `{status, execution_count, outputs, spark_job_ids, error}`. Read the SQL/Spark error from the
   `error` field and fix grounded in the catalog — don't guess repeatedly. Smoke-test connectivity first with
   `--code "spark.sql('SELECT 1').show()"`.
3. **Present** the blended result; optionally persist as a table (`aidp-ingest-file-to-table` /
   `manage-tables`) or save the join via `aidp-verified-queries`.

## Honesty / framing (no-fabrication)
- Describe this as **"federate in one Spark session"** (read each source into Spark, then join) — that is
  literally what the helper does: every source read and the join run in the single Spark session of one
  `aidp_sql.py` invocation. Do **not** claim *single-query pushdown federation across heterogeneous sources*
  unless verified live on the target environment — that capability is an open question, so don't overstate it.
- Keep all reads in **one** invocation/cell so they share the session; a second `aidp_sql.py` call is a fresh
  session and the earlier temp views are gone.
- Mind volume: large external reads should be filtered/pushed down at the source where the connector supports
  it; otherwise sample. Ensure the cluster is RUNNING before executing (see `aidp-cluster-ops`).

## References
- spark-connectors plugin (source reads) · [scripts/aidp_sql.py](../../scripts/aidp_sql.py) — bundled Spark-SQL executor
- [references/semantic-model.md](../../references/semantic-model.md) · [references/no-mcp-rest-map.md](../../references/no-mcp-rest-map.md) · pairs with `aidp-analyzing-data`
