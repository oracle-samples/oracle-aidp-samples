# `.aidp/verified-queries.md` — the verified-query repository (VQR)

A per-project store of **validated** `question → Spark SQL` pairs. `aidp-analyzing-data` matches an incoming
question against this repository and **reuses the validated SQL** before generating from scratch — the
single highest-reliability NL-to-SQL mechanism (Snowflake VQR / Databricks Genie trusted-assets pattern).
Maintained by `aidp-verified-queries`.

> **Quality gate (critical):** a *wrong* verified query makes accuracy WORSE. Every pair MUST pass
> validation — (1) syntactically valid Spark SQL, (2) executes on the cluster, (3) actually answers the
> stated question — before it is marked `verified: true`. `aidp-verified-queries` runs the SQL via the
> bundled `$HOME/.aidp/aidp_sql.py` helper (no MCP required) and refuses to mark a failing/irrelevant pair as verified.

## File format

```markdown
# Verified queries — <project / domain>

### Q: top 10 items by net sales last year
- tables: store_sales, item, date_dim
- verified: true        # validated on cluster <name> at <date>
- sql: |
    SELECT i.i_item_id, SUM(s.ss_net_paid) AS net_sales
    FROM default.default.store_sales s
    JOIN default.default.item i      ON s.ss_item_sk = i.i_item_sk
    JOIN default.default.date_dim d  ON s.ss_sold_date_sk = d.d_date_sk
    WHERE d.d_year = year(current_date) - 1
    GROUP BY i.i_item_id
    ORDER BY net_sales DESC
    LIMIT 10

### Q: monthly revenue trend
- tables: store_sales, date_dim
- verified: false       # DRAFT — not yet validated; do NOT auto-reuse
- sql: | …
```

## Matching rules (used by `aidp-analyzing-data`)
1. Find the closest `verified: true` entry by question similarity + table overlap.
2. If a strong match exists, **reuse its SQL** (adapt only bind values/date ranges), and say so.
3. If no match, ground from `semantic.md` + `catalog.md`, generate, run, present — then offer to add the
   new pair via `aidp-verified-queries` (which validates before setting `verified: true`).

## Rules
- Prefer logical names from `semantic.md`; expand to physical names at execution.
- Never set `verified: true` without a recorded successful execution that answers the question.
- Keep entries small and single-purpose; complex asks get a complete worked example.
