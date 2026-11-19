---
name: aidp-ai-sql
description: Run LLM functions inside Spark SQL on AIDP via ai_generate(). Use when the user wants to summarize/classify/extract/enrich rows with an LLM directly in SQL, generate narratives over aggregated results, or do grounded RAG-style analysis in the lakehouse. Signature is model-first; available models must be confirmed live before relying on it.
---

# `aidp-ai-sql` — LLM-in-SQL with `ai_generate()`

Call an LLM directly inside Spark SQL on AIDP — summarize, classify, extract, or narrate over lakehouse
data without leaving SQL. A signature differentiator: most competitor agents can't do this inline.

This is a **SQL-helper** skill. Interactive Spark SQL runs through the bundled helper
`$HOME/.aidp/aidp_sql.py` (it mints a UPST from the api_key DEFAULT profile, auto-creates a scratch notebook,
and returns JSON). No aidp MCP and no AIDP_SESSION required.

## When to use
- "Summarize / classify / extract / enrich these rows with AI in SQL."
- Generate a grounded narrative over an aggregate (e.g. a finance summary over a spend rollup).

## Signature (model FIRST)
```sql
ai_generate('<model>', '<prompt>')
```
e.g. `ai_generate('openai.gpt-5.4', 'Summarize this supplier spend: ...')`.

**LIVE-VERIFIED** model-first `(model, prompt)` signature with `openai.gpt-5.4`, `openai.gpt-4o`, and
`xai.grok-4`.

> **Verify before relying on it (no-fabrication):** confirm the exact signature and the **available model
> names** live on the target cluster before treating this as guaranteed — run a trivial
> `SELECT ai_generate('<model>', 'hello')` cell first (see smoke test below). Model availability varies
> by environment. If a model name fails, list/ask for the correct one rather than guessing.
>
> **Don't gate on the `/models` REST catalog.** `ai_generate` resolves the model at the Spark engine level,
> so it can work even when `aidp-models-catalog`'s `GET /models?modelType=GENERATIVE_AI` returns an empty
> list. The smoke test (not the catalog endpoint) is the source of truth for whether `ai_generate` works.

## How to run a cell
```bash
python "$HOME/.aidp/aidp_sql.py" \
  --region <region> --datalake <DATALAKE_OCID> --workspace <ws> --cluster <cluster-key> \
  --code "<python/spark code>"
```
Returns JSON: `{"status":"ok|error","outputs":[...],"spark_job_ids":[...]}`. Exit 0 on success, 1 on
cell error. See [$HOME/.aidp/aidp_sql.py]($HOME/.aidp/aidp_sql.py) for full flags (`--profile`,
`--session-profile`, `--notebook`, `--timeout`).

### Smoke test (do this first)
```bash
python "$HOME/.aidp/aidp_sql.py" --region <region> --datalake <ocid> --workspace <ws> --cluster <key> \
  --code "spark.sql(\"SELECT ai_generate('openai.gpt-5.4', 'hello')\").show(truncate=False)"
```

## Workflow (grounded RAG pattern)
1. Ensure cluster RUNNING (cluster-ops via `oci raw-request`; see `references/no-mcp-rest-map.md`).
2. **Ground first:** aggregate/select the rows you want the LLM to reason over (small, relevant set).
3. Embed that grounded context into the prompt and call `ai_generate('<model>', '<grounded prompt>')`.
   For per-row enrichment, call it as a column expression over a bounded set.
4. Present the generated text alongside the underlying data so the user can verify it.

Pass the cell to `--code`:
```python
ctx = spark.sql("SELECT ... FROM gold.supplier_spend ...").toPandas().to_string()
res = spark.sql(f"SELECT ai_generate('openai.gpt-5.4', 'As a finance analyst, summarize: {ctx}') AS summary")
res.show(truncate=False)
```

## Reliability rules
- Always ground the prompt in real query output — don't ask the model to recall data it can't see.
- Bound row counts for per-row `ai_generate` (cost + latency).
- Show the data behind any AI narrative; never present generated numbers as ground truth without the SQL.

## References
- [$HOME/.aidp/aidp_sql.py]($HOME/.aidp/aidp_sql.py) — the SQL/notebook-cell executor (bundled helper)
- [references/oci-raw-request.md]($HOME/.aidp/references/oci-raw-request.md) — REST control-plane (clusters, auth)
- [references/no-mcp-rest-map.md]($HOME/.aidp/references/no-mcp-rest-map.md) · pairs with `aidp-analyzing-data`
