# `.aidp/dq-rules.md` — the data-quality rule set (DQR)

A per-project store of **persisted, re-runnable** data-quality rules. Each rule pairs a target
table/column with a rule type and a **violation-SQL** that returns the count of bad rows (PASS when 0).
`aidp-data-quality` registers rules here, re-runs the violation-SQL via the bundled `scripts/aidp_sql.py`
helper (no MCP required), and records the last pass/fail result — the same "trusted asset" pattern as
`.aidp/verified-queries.md`, applied to quality checks instead of NL-to-SQL.

> **Convention (critical):** the violation-SQL is the source of truth — it MUST count **violations**, not
> conforming rows, so a passing rule returns `0`. Re-running a rule re-runs its stored SQL and updates
> `last-result` / `last-checked`; it never edits the rule definition itself. Rule types match the ones in
> `aidp-data-quality` (not-null, unique, range, set, RI, freshness).

## Rule types

| rule-type | Counts (violations) |
|---|---|
| `not-null` | rows where the column IS NULL |
| `unique` | duplicate key rows (`COUNT(*) - COUNT(DISTINCT key)`, or `GROUP BY key HAVING COUNT(*)>1`) |
| `range` | rows where the column is outside `[lo, hi]` |
| `set` | rows where the column is not in the allowed value set |
| `RI` (referential) | child rows whose join key has no matching parent row |
| `freshness` | `1` if the table is staler than the SLA (`datediff(current_date, MAX(ts)) > N`), else `0` |

## File format

```markdown
# Data-quality rules — <project / domain>

### R: orders_id_not_null
- target: default.default.orders (o_order_id)
- rule-type: not-null
- last-result: PASS (0)     # validated on cluster <name> at <date>
- violation-sql: |
    SELECT COUNT(*) AS v
    FROM default.default.orders
    WHERE o_order_id IS NULL

### R: orders_id_unique
- target: default.default.orders (o_order_id)
- rule-type: unique
- last-result: FAIL (37)    # 37 duplicate keys on cluster <name> at <date>
- violation-sql: |
    SELECT COUNT(*) AS v
    FROM (SELECT o_order_id FROM default.default.orders
          GROUP BY o_order_id HAVING COUNT(*) > 1) d

### R: order_status_in_set
- target: default.default.orders (o_status)
- rule-type: set
- last-result: PASS (0)
- violation-sql: |
    SELECT COUNT(*) AS v
    FROM default.default.orders
    WHERE o_status NOT IN ('NEW','SHIPPED','CANCELLED')

### R: order_amount_range
- target: default.default.orders (o_amount)
- rule-type: range
- last-result: PASS (0)
- violation-sql: |
    SELECT COUNT(*) AS v
    FROM default.default.orders
    WHERE o_amount NOT BETWEEN 0 AND 1000000

### R: orders_customer_ri
- target: default.default.orders (o_customer_sk → customer.c_customer_sk)
- rule-type: RI
- last-result: PASS (0)
- violation-sql: |
    SELECT COUNT(*) AS v
    FROM default.default.orders c
    LEFT JOIN default.default.customer p ON c.o_customer_sk = p.c_customer_sk
    WHERE p.c_customer_sk IS NULL

### R: orders_freshness_1d
- target: default.default.orders (o_load_ts)
- rule-type: freshness
- last-result: not-run      # DRAFT — never executed; do NOT trust last-result
- violation-sql: |
    SELECT CASE WHEN datediff(current_date, MAX(o_load_ts)) > 1 THEN 1 ELSE 0 END AS v
    FROM default.default.orders
```

## Re-run rules (used by `aidp-data-quality`)
1. For each rule, run its `violation-sql` via the bundled helper:
   ```bash
   python "$PLUGIN_DIR/scripts/aidp_sql.py" --region <region> --datalake <DATALAKE_OCID> --workspace <ws> \
     --cluster <cluster-key> \
     --code "spark.sql('''<violation-sql>''').show()"
   ```
   Require `status == "ok"`; read the single count `v`. PASS if `v == 0`, else FAIL.
2. Update `last-result` to `PASS (0)` / `FAIL (<count>)` and `last-checked` to the cluster + date.
3. On FAIL, pull a few offending rows with a separate bounded `LIMIT` query (don't dump full sets).
4. Report a summary table: rule · target · rule-type · result · violation count.

## Rules
- Prefer logical names from `.aidp/semantic.md`; expand to physical `catalog.schema.table` at execution.
  Use join keys from `.aidp/catalog.md` for RI rules — don't guess them.
- A rule's `violation-sql` MUST count violations (PASS ⇒ `0`); never store a "good-row" count here.
- Never set `last-result: PASS` without a recorded successful execution (`status: ok`) returning `0`.
- Keep entries small and single-purpose; one rule = one check on one target.
- `.aidp/dq-rules.md` is user-editable and git-ignored (per-project, may reference real table names).
