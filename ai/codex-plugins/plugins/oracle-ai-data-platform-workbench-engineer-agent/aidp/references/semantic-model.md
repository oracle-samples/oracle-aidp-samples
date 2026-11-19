# `.aidp/semantic.md` — the semantic grounding layer

A per-project, user-editable file that grounds NL-to-SQL in business meaning. Maintained by
`aidp-semantic-model`, read by `aidp-analyzing-data`, `aidp-verified-queries`, `aidp-data-quality`,
`aidp-profiling-tables`. This is the lever curated NL-to-SQL systems (Snowflake semantic model,
Databricks Genie metrics/joins) rely on; without it, raw NL-to-SQL accuracy on real schemas is low.

## Instruction hierarchy (most → least reliable)
1. **SQL expressions** for metrics/filters (preferred).
2. **Example SQL** for ambiguous prompts (see `verified-queries.md`).
3. **Free text** only as a last resort.

## File format

```markdown
# Semantic model — <project / domain>

## Logical entities
| Logical name | Physical table | Grain | Key | Notes |
|---|---|---|---|---|
| customer | default.default.customer | one row per customer | c_customer_sk | |
| sale | default.default.store_sales | one row per line item | ss_item_sk + ss_ticket_number | LARGE (~28.8M) |

## Metrics (SQL expressions — preferred over prose)
| Metric | SQL expression | Grain |
|---|---|---|
| net_sales | SUM(ss_net_paid) | sale |
| order_count | COUNT(DISTINCT ss_ticket_number) | sale |

## Joins (with cardinality — prevents wrong/ambiguous joins)
| From | To | On | Cardinality |
|---|---|---|---|
| store_sales | item | ss_item_sk = i_item_sk | many-to-one |
| store_sales | date_dim | ss_sold_date_sk = d_date_sk | many-to-one |

## Synonyms / jargon (maps user words → logical names)
| User says | Means |
|---|---|
| revenue, sales | net_sales |
| customers | customer |

## Value dictionaries (prevents wrong WHERE literals, e.g. "California" vs "CA")
| Column | Canonical values / pattern |
|---|---|
| c_birth_country | UNITED STATES, CANADA, … (UPPERCASE) |
```

## Rules
- Logical/metric/synonym entries should reference names that exist in `.aidp/catalog.md`.
- Keep the working set focused per question (a handful of tables) — broad, unfocused context reduces accuracy.
- `aidp-semantic-model` never invents columns/values — it reads them from the catalog or asks the user.
