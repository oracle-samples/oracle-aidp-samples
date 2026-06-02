---
shipped: 2026-04-01
---

# Shipped: Close Collection Persistence Test Gaps

## Summary
Closed all three test coverage gaps identified during QA review of the system-table-collected-data feature. Added 8 new targeted tests covering dimension-aware collectors, None/non-numeric metric_value skip, and profiling dimension no-op.

## Key Changes
- 5 tests for `_collect_with_dimension` GROUP BY path across AggregationCollector (3), MetricsCollector (1), CustomQueryCollector (1)
- 2 tests for `metric_value=None` and non-numeric skip guard in `_persist_results`
- 1 test confirming `ProfilingCollectionConfig.dimension` is accepted but unused by ProfilingCollector

## Files Changed
- `tests/test_collection/test_collectors.py` — 6 new tests
- `tests/test_engine.py` — 2 new tests

## Testing
- 401 tests pass (all previous + 8 new targeting the specific gaps)
- Each new test directly validates a gap flagged during QA review

## Notes
- `ProfilingCollectionConfig.dimension` remains a no-op — documented by test as intentional behavior
- No code changes, purely additive test coverage
