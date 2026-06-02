---
id: collection-persistence-test-gaps
name: Close Collection Persistence Test Gaps
type: Tech Debt
priority: P2
effort: Small
impact: Medium
created: 2026-04-01
---

# Close Collection Persistence Test Gaps

## Problem Statement
The QA review of the `system-table-collected-data` feature identified three test coverage gaps that reduce confidence in the collection persistence layer:

1. **No unit tests for `_collect_with_dimension` GROUP BY path**: The dimension-aware collection logic in `AggregationCollector`, `MetricsCollector`, and `CustomQueryCollector` is only exercised indirectly through engine integration tests. A failure in the GROUP BY branch (e.g., wrong SQL construction, incorrect dimension value extraction, mixed column handling) would not be caught by the existing collector-level test classes.

2. **No test for `metric_value=None` skip in `_persist_results`**: The engine correctly guards against persisting collection results with `None` metric values (line 538), but no test verifies this behavior. If the guard were accidentally removed, non-numeric collection results (e.g., DataFrame samples from `SamplerCollector`) could be written as corrupt rows in the system table.

3. **`ProfilingCollectionConfig.dimension` field is silently unused**: The `dimension` field exists in the profiling config for consistency, but `ProfilingCollector` never receives it. If a user sets `dimension` on a profiling collection, it is silently dropped with no warning. This should either be documented as intentional or produce a validation warning.

These gaps were identified during the QA phase of shipping `system-table-collected-data` and flagged as non-blocking follow-ups.

## Affected Areas
- tests/test_collection/ (new unit tests for dimension-aware collectors)
- tests/test_engine.py (test for None metric_value skip)
- qualifire/core/config.py or docs (document profiling dimension no-op)
