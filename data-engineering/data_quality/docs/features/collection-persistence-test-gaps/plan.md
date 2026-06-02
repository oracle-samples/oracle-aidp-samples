---
started: 2026-04-01
---

# Implementation Plan: Close Collection Persistence Test Gaps

## Overview
Add targeted tests for three gaps identified during QA review: dimension-aware collector paths, None metric_value skip, and profiling dimension no-op.

## Implementation Steps
- [ ] Step 1: Add `_collect_with_dimension` tests for AggregationCollector, MetricsCollector, CustomQueryCollector
- [ ] Step 2: Add test for `metric_value=None` skip in engine `_persist_results`
- [ ] Step 3: Add test that ProfilingCollectionConfig.dimension is accepted but unused
- [ ] Step 4: Run full test suite to verify

## Testing Strategy
All changes are tests themselves — verification is that the full suite passes.
