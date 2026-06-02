---
id: historical-data-missing-check
name: Historical Data Missing Check
type: Bug Fix
priority: P1
effort: Small
impact: High
created: 2026-04-01
---

# Historical Data Missing Check

## Problem Statement
When historical values are not present for comparison, the system lacks proper checks to handle this gracefully. Checks that rely on historical data (e.g., trend checks, shape checks) may produce incorrect results, fail silently, or throw unhelpful errors when prior data is unavailable. This undermines trust in validation outcomes and makes debugging difficult.

## Proposed Solution
Add guard checks across the pipeline to detect when historical data is missing before running comparisons. Surface clear warnings or skip comparisons gracefully with informative messages, rather than failing silently or producing misleading results.

## Affected Areas
- checks/validations (shape_check, trend_check, etc.)
- pipeline/orchestration
- configuration/YAML
