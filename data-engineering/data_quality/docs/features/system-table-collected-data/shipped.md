---
shipped: 2026-04-01
---

# Shipped: System Table for Collected Data

## Summary
Extended the system table to persist collection results (raw metrics from profiling/aggregation) alongside validation outcomes. Added collector naming, dimension tracking, and preference for collection rows in historical reads. Includes input validation against SQL injection for all new config fields.

## Key Changes
- Added 3 new columns to system table schema: `record_type`, `collector_name`, `dimension_value`
- Added optional `name` field to all 5 validation config types (with safe-character validation)
- Added optional `dimension` field to 4 collection config types (with SQL identifier validation + backtick quoting)
- Engine now persists CollectionResult rows with `record_type="collection"` alongside ValidationResult rows
- `read_metric_history()` prefers collection rows over validation rows for historical comparison
- Dimension-aware GROUP BY support in aggregation, metrics, and custom_query collectors

## Files Changed
- `qualifire/storage/base.py` — schema constants
- `qualifire/core/config.py` — config models with input validation
- `qualifire/core/models.py` — CollectionResult + DatasetResult
- `qualifire/core/engine.py` — PipelineResult, collection persistence
- `qualifire/collection/aggregation.py` — dimension GROUP BY
- `qualifire/collection/metrics.py` — dimension GROUP BY
- `qualifire/collection/custom_query.py` — dimension support
- `qualifire/storage/sqlite_storage.py` — DDL + query update
- `qualifire/storage/external_catalog.py` — query update
- `qualifire/storage/delta_storage.py` — DDL + query update
- `qualifire/storage/jdbc_storage.py` — query update

## Testing
- 342 tests pass (321 original + 21 new)
- New tests cover: config validation, model defaults, SQLite collection persistence, collection row priority, legacy backward compatibility, engine collection propagation, custom collector names, SQL injection rejection, safe identifier acceptance

## Notes
- Security review identified pre-existing SQL injection risks in storage backends (not introduced by this feature) — tracked separately
- Existing tables need ALTER TABLE for new columns or table recreation
- Backward compatible: legacy rows without `record_type` still work in all queries
