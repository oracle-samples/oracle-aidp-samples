---
name: aidp-data-quality
description: Run data-quality rule checks on AIDP tables — not-null, uniqueness, allowed ranges/sets, referential integrity, and freshness. Use when the user wants to validate data, check for nulls/duplicates/orphans, assert a column's domain, or gate a pipeline on quality. Expresses each rule as bounded Spark SQL and reports pass/fail with offending counts.
---

# `aidp-data-quality` — rule checks via Spark SQL

Validate AIDP tables against explicit data-quality rules, each compiled to bounded Spark SQL and executed
with the bundled helper — no MCP and no `ai-data-engineer-agent` repo required.

## When to use
- "Check <table> for nulls/duplicates", "validate <column>", "are there orphan rows", "is the data fresh",
  or gating a pipeline on quality.

## Rule types (each → a counting SQL that should return 0 violations)
| Rule | Check (violations) |
|---|---|
| not-null | `COUNT(*) WHERE col IS NULL` |
| unique | `COUNT(*) - COUNT(DISTINCT key)` (or `GROUP BY key HAVING COUNT(*)>1`) |
| range / set | `COUNT(*) WHERE col NOT BETWEEN lo AND hi` / `col NOT IN (...)` |
| referential | `COUNT(*) child LEFT JOIN parent ... WHERE parent.key IS NULL` |
| freshness | `MAX(ts)` vs SLA (e.g. `datediff(current_date, MAX(ts)) <= N`) |

## Workflow
1. Resolve table(s)/columns; use join keys from `.aidp/catalog.md` for referential checks (don't guess).
   Pull rule definitions from `.aidp/semantic.md` value dictionaries where available.
2. Ensure the cluster is RUNNING (`aidp-cluster-ops` / `oci raw-request`), then for each rule run the
   violation-count SQL with the bundled helper (PASS if 0, else FAIL):
   ```bash
   python "$HOME/.aidp/aidp_sql.py" --region <region> --datalake <DATALAKE_OCID> --workspace <ws> \
     --cluster <cluster-key> \
     --code "spark.sql('''SELECT COUNT(*) AS v FROM cat.sch.t WHERE col IS NULL''').show()"
   ```
   It mints a UPST from the api_key DEFAULT profile, auto-creates a scratch notebook, and returns JSON with
   `status` / `outputs` / `spark_job_ids`. No `AIDP_SESSION` required (`--session-profile` optional).
3. On a non-zero count, FAIL and pull a few example offending rows with a separate bounded `LIMIT` query.
4. Report a summary table: rule · target · result · violation count.
5. Offer to (a) persist the rule set for re-runs (see below), and (b) wire checks into a Job
   (`aidp-pipelines`) as a gating task.

## Persisting a re-runnable rule set
Register validated rules in **`.aidp/dq-rules.md`** so they can be re-run later (the quality analogue of
`.aidp/verified-queries.md`). One entry per rule records the target table/column, rule-type (the five types
above), the **violation-SQL** (counts violations → PASS when 0), and `last-result` / `last-checked`. To
re-run, execute each entry's stored violation-SQL via `$HOME/.aidp/aidp_sql.py`, set the result to `PASS (0)` or
`FAIL (<count>)`, and record the cluster + date — never mark PASS without a `status: ok` run returning 0.
Format and re-run rules: [references/dq-rules.md]($HOME/.aidp/references/dq-rules.md).

## Reliability rules
- Run real SQL via `$HOME/.aidp/aidp_sql.py`; never assert a rule passed without a `status: ok` result.
- Keep checks bounded; sample example offenders rather than dumping full result sets.
- If a cell returns `status: error`, read the error, fix the SQL grounded in the catalog, and retry.

## References
- [references/dq-rules.md]($HOME/.aidp/references/dq-rules.md) (`.aidp/dq-rules.md` rule-set format + re-run)
- [$HOME/.aidp/aidp_sql.py]($HOME/.aidp/aidp_sql.py) · [references/no-mcp-rest-map.md]($HOME/.aidp/references/no-mcp-rest-map.md) · [references/oci-raw-request.md]($HOME/.aidp/references/oci-raw-request.md) · [references/semantic-model.md]($HOME/.aidp/references/semantic-model.md)
