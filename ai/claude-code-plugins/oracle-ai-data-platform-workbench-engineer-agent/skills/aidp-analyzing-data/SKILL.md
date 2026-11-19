---
name: aidp-analyzing-data
description: Answer business questions over the AIDP lakehouse with Spark SQL. Use when the user asks a data question ("how many…", "top N…", "show me…", "trend of…", "revenue by…") or wants to run ad-hoc Spark SQL. Grounds in .aidp/catalog.md + .aidp/semantic.md and reuses validated verified queries before generating SQL, then executes via the bundled aidp_sql.py helper.
---

# `aidp-analyzing-data` — natural language → Spark SQL

Answer business questions by grounding in the catalog/semantic model, reusing verified queries when
possible, then executing Spark SQL via the bundled `scripts/aidp_sql.py` helper.

## When to use
- Any data question, or "run this SQL on AIDP".

> **Source is an external / non-lakehouse system** (Fusion, EPM, Oracle ADB/ExaCS, Snowflake, S3, …)? This
> skill is **lakehouse-native Spark SQL**. To pull from an external source, use the
> **`oracle-ai-data-platform-workbench-spark-connectors`** plugin's `aidp-<source>` skill (install it if
> absent; run its `aidp-connectors-bootstrap` skill once to push the helper package to the cluster), or
> `aidp-federate` to join across sources.

## Workflow (grounding-first — this is the accuracy lever)
1. **Verified-query match.** Read `.aidp/verified-queries.md`; if a `verified: true` entry closely matches
   the question (similar text + table overlap), **reuse its SQL** (adapt only dates/bind values) and say so.
2. **Ground.** Otherwise read `.aidp/catalog.md` + `.aidp/semantic.md`: map concepts→tables via Quick
   Reference/synonyms, use recorded **join keys** (don't guess joins), use **value dictionaries** for WHERE
   literals, prefer metric SQL expressions from the semantic model. If the catalog cache is missing, run
   `aidp-catalog-init` first.
3. **Scope small.** Use the few tables the question needs; for big fact tables add date filters; consider a
   pre-joined view for repeated complex asks.
4. **Execute.** Run the SQL via the bundled helper — it mints a UPST from the api_key DEFAULT profile and
   auto-creates a scratch notebook on the target cluster (no MCP, no AIDP_SESSION required):
   ```bash
   python "$PLUGIN_DIR/scripts/aidp_sql.py" \
     --region <region> --datalake <DATALAKE_OCID> --workspace <ws> --cluster <cluster-key> \
     --code "spark.sql('''<SQL>''').show(50, truncate=False)"
   ```
   Returns JSON `{status, execution_count, outputs, spark_job_ids, error}`. Each invocation runs the cell;
   keep the same `<SQL>` shape across follow-ups. Smoke-test connectivity with `--code "spark.sql('SELECT 1').show()"`.
5. **Present** the result clearly (table + a one-line read of what it shows). Show the SQL you ran.
6. **Cache the learning.** Offer to save a new concept→table mapping to `.aidp/catalog.md` and/or register
   the working query via `aidp-verified-queries` (which validates before marking it verified).

## Reliability rules
- Real-world NL-to-SQL is unreliable without grounding — never fabricate column/table names; if unsure,
  confirm against the catalog cache (or a `SHOW COLUMNS` / `DESCRIBE` cell) or ask.
- Qualify tables fully (`catalog.schema.table`). Default catalog/schema only when the user implies them.
  This includes metadata commands: use `SHOW TABLES IN <catalog>.<schema>` (e.g. `SHOW TABLES IN default.default`),
  **not** the unqualified `SHOW TABLES IN default` — the bare form raises `AnalysisException: [SCHEMA_NOT_FOUND]`
  because `default` resolves as a catalog, not a schema.
- If a query errors, read the Spark error from the helper's `error` field, fix grounded in the catalog, and
  retry — don't guess repeatedly.
- Ensure the cluster is RUNNING before executing (see `aidp-cluster-ops`); the helper attaches to the
  cluster you pass via `--cluster`.
- For LLM-in-SQL (`ai_generate`) see `aidp-ai-sql`; for cross-source joins see `aidp-federate`.

## References
- [scripts/aidp_sql.py](../../scripts/aidp_sql.py) — bundled Spark-SQL executor (the plugin's only code)
- [references/verified-queries.md](../../references/verified-queries.md) · [references/semantic-model.md](../../references/semantic-model.md) · [references/no-mcp-rest-map.md](../../references/no-mcp-rest-map.md)
