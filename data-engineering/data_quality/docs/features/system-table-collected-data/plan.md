---
started: 2026-04-01
---

# Implementation Plan: System Table for Collected Data

## Overview
Extended the system table to persist collection results (raw metrics from profiling/aggregation) alongside validation outcomes. Added collector naming, dimension tracking, and preference for collection rows in historical reads.

## Implementation Steps
- [x] Step 1: Add `record_type`, `collector_name`, `dimension_value` columns to system table schema
- [x] Step 2: Add `name` to validation configs and `dimension` to collection configs
- [x] Step 3: Add `dimension_value` to CollectionResult and `collection_results` to DatasetResult
- [x] Step 4: Update engine to persist collection results and propagate collector names
- [x] Step 5: Add dimension-aware GROUP BY support to aggregation, metrics, and custom_query collectors
- [x] Step 6: Update DDL and `read_metric_history()` across all 4 storage backends
- [x] Step 7: Add 17 new tests covering config, models, storage, and engine changes

## Technical Decisions
- Single table with `record_type` discriminator (not a separate table) for simplicity across all backends
- `read_metric_history()` prefers collection rows over validation rows via ORDER BY CASE
- Backward compatible: legacy rows without `record_type` still work
- Collector name auto-generated as `"{type}.{dataset}"` when not user-specified

## Testing Strategy
- All 338 tests pass (321 original + 17 new)
- Tests cover: config validation, model defaults, SQLite collection persistence, collection row priority, legacy backward compatibility, engine collection propagation, custom collector names

## Risks & Mitigations
- Existing tables need ALTER TABLE for new columns (or recreate) — mitigated by nullable columns
- Dimension-aware collections increase row volume — mitigated by only persisting numeric values
