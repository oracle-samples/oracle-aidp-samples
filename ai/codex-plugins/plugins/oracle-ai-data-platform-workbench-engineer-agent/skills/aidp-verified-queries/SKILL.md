---
name: aidp-verified-queries
description: Register and validate reusable question→Spark-SQL pairs in .aidp/verified-queries.md so the agent reuses trusted SQL before generating new SQL. Use when the user wants to save a working query as canonical, build a verified-query repository, or improve answer reliability for recurring questions. Validates each pair on the cluster before marking it verified.
---

# `aidp-verified-queries` — the verified-query repository (VQR)

Maintain `.aidp/verified-queries.md`: validated `question → Spark SQL` pairs that `aidp-analyzing-data`
reuses before generating SQL from scratch — the highest-reliability NL-to-SQL mechanism.

## When to use
- Save a working query as the canonical answer to a recurring question.
- Curate/clean the verified-query repository.

## Quality gate (critical — do not skip)
A *wrong* verified query makes accuracy **worse**. Before setting `verified: true`, the pair MUST:
1. be **syntactically valid** Spark SQL,
2. **execute** on the cluster (run it via the bundled `$HOME/.aidp/aidp_sql.py` helper),
3. **actually answer** the stated question (sanity-check the result shape/values).
If any check fails, keep `verified: false` (DRAFT) and explain why — never auto-promote a failing pair.

## Workflow
1. Read the candidate question + SQL (or take the last query run in `aidp-analyzing-data`).
2. Prefer logical names from `.aidp/semantic.md`; record the physical tables touched.
3. **Validate** by running the SQL on the cluster with the bundled helper (no MCP required):
   ```bash
   python "$HOME/.aidp/aidp_sql.py" \
     --region <region> --datalake <DATALAKE_OCID> --workspace <ws> --cluster <cluster-key> \
     --code "spark.sql('''<your SELECT … LIMIT 50>''').show(50, truncate=False)"
   ```
   It mints a UPST from the api_key DEFAULT profile, auto-creates a scratch notebook, and returns JSON
   `{status, outputs, spark_job_ids}`. Require `status == "ok"` and a result that answers the question.
   Run on a **bounded sample** (add `LIMIT`) to keep validation cheap.
4. Append the entry to `.aidp/verified-queries.md` in the documented format; set `verified: true` only on
   a recorded successful run (note cluster + date).
5. On reuse, `aidp-analyzing-data` matches by question similarity + table overlap and adapts only
   dates/bind values.

## Notes
- `.aidp/verified-queries.md` is user-editable and git-ignored (per-project).
- Keep entries small, single-purpose; complex asks get a complete worked example.
- This skill is self-contained: validation runs through `$HOME/.aidp/aidp_sql.py`, not any MCP server. If an
  `aidp` MCP happens to be configured you *may* use its `nb_execute_code` as an accelerator, but it is not
  required.

## References
- [references/verified-queries.md]($HOME/.aidp/references/verified-queries.md) · [references/semantic-model.md]($HOME/.aidp/references/semantic-model.md)
- SQL execution helper: [references/no-mcp-rest-map.md]($HOME/.aidp/references/no-mcp-rest-map.md) (No-MCP SQL via `$HOME/.aidp/aidp_sql.py`)
