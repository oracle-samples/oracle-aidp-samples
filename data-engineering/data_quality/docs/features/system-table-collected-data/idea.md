---
id: system-table-collected-data
name: System Table for Collected Data
type: Feature
priority: P0
effort: Medium
impact: High
created: 2026-04-01
---

# System Table for Collected Data

## Problem Statement
There is currently no persistent storage for the data collected during profiling and validation runs. Without a system table, there is no way to look up what was collected, when it was collected, or what dimension values were observed — making historical comparison, auditing, and debugging difficult or impossible.

Each collection and validation needs a name, and collections should reference an optional dimension field (defaulting to a standard value when no dimension is specified). The system table should store:

- **Dataset name** — which dataset the collection belongs to
- **Collector name** — a descriptive identifier for the collection (e.g., `count_last_7d`)
- **Run date** — the date on which the data-quality run executed
- **Dimension values** — all unique values of the dimension returned by the query

For example, if a query checks the count of a dataset for the last 7 days based on a `created_at` field, the system table would store the collector name `count_last_7d`, the dataset name, the run date, and all 7 dates returned by the query as dimension values.

This table is foundational — other features like historical comparison and trend checks depend on having this data persisted.

## Affected Areas
- checks/validations (naming collectors and dimensions)
- pipeline/orchestration (writing results to system table after each run)
- configuration/YAML (defining collector names and dimension fields)
- storage/persistence (new system table schema)
