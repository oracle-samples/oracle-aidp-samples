---
id: custom-sql-query-check
name: Custom SQL Query Check
type: Feature
priority: P1
effort: Medium
impact: High
created: 2026-04-01
---

# Custom SQL Query Check

## Problem Statement
Currently, checks operate on individual tables with predefined logic. There is no way to run a custom SQL query that joins multiple tables or applies complex business logic, and then validate the resulting metrics. Users who need to validate cross-table relationships or computed metrics across joined datasets have no supported path to do so.

Allowing users to define a custom SQL query — specifying which columns serve as dimensions and which as measures — would unlock validation of multi-table metrics, complex aggregations, and business-specific calculations that can't be expressed with single-table checks.

## Proposed Solution
Introduce a new check type that accepts a user-defined SQL query along with explicit dimension and measure column designations. The query results would then flow through the existing validation pipeline (shape check, trend check, etc.) just like any other dataset.

## Affected Areas
- checks/validations (new custom SQL check runner)
- configuration/YAML (new schema for query, dimension, measure definitions)
- pipeline/orchestration (integration of custom query results into validation flow)
