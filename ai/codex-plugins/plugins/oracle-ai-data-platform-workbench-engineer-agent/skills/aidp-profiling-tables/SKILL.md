---
name: aidp-profiling-tables
description: Profile an AIDP table — row count, per-column null %, distinct count, min/max/mean, and top-K values. Use when the user asks to profile a table, wants column statistics or a data-quality snapshot, or needs to understand a dataset's shape before using it. Runs bounded Spark SQL via the bundled aidp_sql.py helper.
---

# `aidp-profiling-tables` — single-table profile

Produce a column-level profile of an AIDP table via Spark SQL. Self-contained: control-plane lookups
use `oci raw-request`; profiling SQL runs through the bundled `$HOME/.aidp/aidp_sql.py` helper. No aidp MCP
server is required.

## When to use
- "Profile <table>", "what does <table> look like", "column stats / data quality snapshot".

## Workflow
1. Resolve the table (`aidp-catalog-explore` / `.aidp/catalog.md`) → fully-qualified `catalog.schema.table`
   and its columns/types. Without a cache, list via `oci raw-request`:
   `GET /tables?catalogKey=<cat>&schemaKey=<cat.schema>` and filter for the table client-side (see
   [references/no-mcp-rest-map.md]($HOME/.aidp/references/no-mcp-rest-map.md)). Use the column types to pick the
   right per-column profiling SQL.
2. Run bounded profiling SQL via the helper (one cell per call; the scratch notebook + kernel are managed
   for you):
   ```bash
   python "$HOME/.aidp/aidp_sql.py" --region <r> --datalake <ocid> --workspace <ws> --cluster <key> \
     --code "spark.sql('''<profiling SQL>''').show(50, truncate=False)"
   ```
   - **Overview:** `SELECT COUNT(*) FROM t` (flag if LARGE; sample for the rest).
   - **Numeric cols:** `MIN`, `MAX`, `AVG`, `COUNT`, null %, approx distinct (`approx_count_distinct`).
   - **String/categorical:** null %, `approx_count_distinct`, top-K via `GROUP BY … ORDER BY count DESC LIMIT k`.
   - **Date/timestamp:** `MIN`/`MAX` range, null %.
   Use `TABLESAMPLE`/`LIMIT` on large tables to stay cheap; say when you sampled. The helper returns JSON
   (`status`, `outputs`, `spark_job_ids`) — parse `outputs` for the result rows.
3. Present a per-column table: type, null %, distinct, min/max/mean (numeric), top values (categorical).
4. Offer to feed findings into `.aidp/catalog.md` value dictionaries (`aidp-catalog-init`) and to add
   data-quality rules (`aidp-data-quality`).

## Reliability rules
- Profile from real query output, not assumptions; note sampling.
- For very large tables, profile a sample and label it clearly.
- The helper mints a UPST from the api_key DEFAULT profile and auto-creates a scratch notebook; pass
  `--session-profile AIDP_SESSION` only if your tenancy is session-token-only. On a kernel/auth error,
  refresh (`oci session refresh --profile AIDP_SESSION`) and retry.

## References
- [references/oci-raw-request.md]($HOME/.aidp/references/oci-raw-request.md) · [references/no-mcp-rest-map.md]($HOME/.aidp/references/no-mcp-rest-map.md) · pairs with `aidp-data-quality`, `aidp-catalog-init`
