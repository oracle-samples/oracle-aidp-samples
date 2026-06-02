---
id: handle-empty-check-output
name: Handle Empty Check Output
type: Bug Fix
priority: P1
effort: Small
impact: High
created: 2026-04-01
---

# Handle Empty Check Output

## Problem Statement
When a check runs but produces no output (e.g., the query returns no rows, or the check logic yields an empty result), this case is not handled properly. The absence of output can be silently ignored, leading to false confidence that validations passed when in reality no validation occurred. There needs to be explicit detection and handling when a check returns empty or no results.

## Proposed Solution
Add validation after each check execution to detect empty or missing output. When detected, surface a clear warning or error so users know the check did not produce meaningful results, rather than treating silence as success.

## Affected Areas
- checks/validations (shape_check, trend_check, etc.)
