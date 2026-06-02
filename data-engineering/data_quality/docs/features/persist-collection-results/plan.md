---
started: 2026-04-01
---

# Implementation Plan: Persist Collection Results for Historical Comparison

## Overview
The system table already persists numeric collection results (from AggregationCollector, MetricsCollector, CustomQueryCollector, ProfilingCollector), and `read_metric_history()` correctly reads them back. However, RecencyCollector outputs (datetime values) are silently dropped because the engine only persists int/float. ProfilingCollector also omits `collection_type` metadata. This plan fixes both gaps and adds end-to-end test coverage.

Note: The `timestamp-tracking` feature recently shipped, adding `collected_at`, `validated_at`, `notified_at` fields to models and persistence. These fields are already integrated into engine.py's `_persist_results()`. Tests must account for these fields.

## Implementation Steps
- [x] Step 1: Convert datetime metric values to epoch seconds in `engine.py._persist_results()` so RecencyCollector values are persisted
- [x] Step 2: Add `collection_type: "profiling"` to ProfilingCollector metadata
- [x] Step 3: Add end-to-end test: write collection rows → read back via `read_metric_history()` → verify values match
- [x] Step 4: Add test for RecencyCollector datetime persistence specifically
- [x] Step 5: Verify existing tests still pass (393 passed)

## Technical Decisions

### RecencyCollector persistence
- Convert `datetime` metric_value to epoch seconds (`float(ts.timestamp())`) in the engine persistence loop, before the `isinstance(cr.metric_value, (int, float))` check.
- This keeps the CollectionResult model unchanged — the conversion happens only at the persistence boundary.
- Epoch seconds is the natural numeric representation for timestamps and integrates with existing `read_metric_history()` which returns `metric_value` as float.

### ProfilingCollector metadata
- Add `"collection_type": "profiling"` to the metadata dict in `profiler.py`.
- This fills the NULL `collection_type` field in persisted profiling rows.

### What's NOT changed
- SamplerCollector intentionally returns `metric_value=None` (stores DataFrames in metadata). These are used by IsolationForestValidator which doesn't need system table history. No change needed.
- The `read_metric_history()` SQL query already correctly prioritizes `record_type = 'collection'` over `'validation'`. No change needed.

## Files to Modify

| File | Change |
|------|--------|
| `qualifire/core/engine.py` | Convert datetime to epoch seconds in `_persist_results()` |
| `qualifire/collection/profiler.py` | Add `collection_type: "profiling"` to metadata |
| `tests/test_engine.py` | Add end-to-end collection persistence + readback test |
| `tests/test_engine.py` | Add RecencyCollector datetime persistence test |

## Testing Strategy
- End-to-end test: AggregationCollector → persist → read_metric_history → verify metric_value matches
- RecencyCollector test: verify datetime values are persisted as epoch seconds
- ProfilingCollector test: verify collection_type is set in persisted rows
- Run full test suite to ensure no regressions

## Risks & Mitigations

| Risk | Mitigation |
|------|------------|
| Epoch seconds format confuses downstream consumers | RecencyCollector is used by SLOValidator which computes delta, not reads raw history — no impact |
| Breaking existing persistence behavior | Only adding conversion for datetime, not changing int/float handling |
| ProfilingCollector metadata change breaks something | Additive only — adds a key that was previously absent |
