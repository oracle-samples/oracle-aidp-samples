---
shipped: 2026-04-01
---

# Shipped: Persist Collection Results for Historical Comparison

## Summary
Fixed two gaps in collection result persistence: RecencyCollector datetime values (previously silently dropped) are now converted to epoch seconds and persisted, and ProfilingCollector now sets `collection_type: "profiling"` metadata. Added end-to-end tests verifying the write-then-readback cycle works correctly.

## Key Changes
- Engine converts `datetime` metric values to epoch seconds (`float(ts.timestamp())`) before the numeric type check in `_persist_results()`, enabling RecencyCollector values to be stored
- ProfilingCollector metadata now includes `"collection_type": "profiling"`, filling the previously-NULL field in system table rows
- Added end-to-end test: writes collection data via engine, reads back via `read_metric_history()`, verifies values match
- Added test verifying RecencyCollector datetime values are stored as epoch seconds
- Added test verifying ProfilingCollector `collection_type` is persisted correctly

## Files Changed
- `qualifire/core/engine.py`
- `qualifire/collection/profiler.py`
- `tests/test_engine.py`

## Testing
- 393 tests pass (3 new tests added in `TestCollectionPersistenceReadback`)
- End-to-end readback test: AggregationCollector values written and read back correctly
- RecencyCollector datetime persistence: epoch seconds stored and verifiable
- ProfilingCollector metadata: `collection_type = "profiling"` confirmed in persisted rows
- Security review passed with no blocking issues

## Notes
- SamplerCollector (`metric_value=None`) intentionally not persisted — IsolationForestValidator uses DataFrames directly, not system table history
- Numeric collectors (Aggregation, Metrics, CustomQuery) were already working — this fix primarily addresses RecencyCollector and ProfilingCollector metadata
- Security review noted naive datetime timezone ambiguity (Low) — consider normalizing to UTC in a follow-up if cross-timezone pipelines are a concern
