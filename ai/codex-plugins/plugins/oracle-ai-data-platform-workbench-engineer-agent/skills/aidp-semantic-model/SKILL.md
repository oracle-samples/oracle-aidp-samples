---
name: aidp-semantic-model
description: Maintain a semantic grounding layer (.aidp/semantic.md) for AIDP — logical entity names, SQL-defined metrics, joins with cardinality, synonyms, and value dictionaries. Use when the user wants to define metrics/business terms, improve NL-to-SQL accuracy, standardize "revenue/customers/etc.", or set up a semantic model. Read by analyzing-data, verified-queries, profiling, and data-quality.
---

# `aidp-semantic-model` — the semantic grounding layer

Create and maintain `.aidp/semantic.md`: the business-meaning layer that grounds NL-to-SQL. This is the
lever curated systems (Snowflake semantic model, Databricks Genie metrics/joins) rely on — without it,
real-world NL-to-SQL accuracy is low.

## When to use
- Define metrics (revenue, active_customers, gross_margin…), logical names, joins, synonyms, or value sets.
- The user wants consistent, reusable business semantics across questions.

## Instruction hierarchy (most → least reliable)
1. **SQL expressions** for metrics/filters (preferred).
2. **Example SQL** for ambiguous prompts (store these via `aidp-verified-queries`).
3. **Free text** only as a last resort.

## Workflow
1. Ensure `.aidp/catalog.md` exists (`aidp-catalog-init`) — the semantic model references real tables/columns.
2. Edit `.aidp/semantic.md` per the format in `references/semantic-model.md`: logical entities, metrics (as
   SQL expressions), joins (with cardinality), synonyms, value dictionaries.
3. **Never invent** columns/values — read them from the catalog or confirm with the user.
4. Optionally validate a metric by running its SQL on a small sample — hand off to `aidp-analyzing-data`,
   which executes Spark SQL via `python "$HOME/.aidp/aidp_sql.py"` (no MCP required).
5. Keep the per-domain working set small and focused.

## AIDP native Ontologies (related feature — UI-driven)
AIDP ships a native **Ontologies** feature (RDF/OWL business glossary: terms, synonyms, definitions, a graph
view, and ontology-driven governance like `av:isSensitive` / `av:requiresRole`). It overlaps this semantic
layer but is **UI-driven** — **no programmatic REST API was found** (`GET …/ontologies` and
`…/workspaces/<ws>/ontologies` both returned **404**, probed 2026-06-10). So:
- For an **agent-usable, programmatic** semantic/glossary layer today, use `.aidp/semantic.md` (this skill) —
  it's the API-free analog the agent can read/write and ground SQL with.
- If the user specifically needs the **native Ontologies** (graph view, TTL/R2RML export, sensitivity
  governance), that is authored in the AIDP console UI; don't claim a REST endpoint for it. Sensitivity tags
  there feed masking governance (`aidp-roles-access` → masking section).

## Notes
- `.aidp/semantic.md` is user-editable and git-ignored (per-project).
- Pairs with `aidp-verified-queries` (example/verified SQL) and `aidp-analyzing-data` (consumes both).

## References
- [references/semantic-model.md]($HOME/.aidp/references/semantic-model.md) · [references/verified-queries.md]($HOME/.aidp/references/verified-queries.md)
