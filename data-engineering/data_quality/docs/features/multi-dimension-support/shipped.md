---
shipped: 2026-04-01
---

# Shipped: Multi-Dimension Support for Collection and Validation

## Summary
Replaced the single `dimension` field with `dimensions: list[str]` on collection configs, enabling GROUP BY on multiple columns simultaneously. Compound dimension values are encoded as JSON with alphabetically sorted keys (e.g., `{"category": "electronics", "region": "US"}`).

## Key Changes
- Replaced `dimension: str | None` with `dimensions: list[str] | None` on AggregationCollectionConfig, MetricsCollectionConfig, CustomQueryCollectionConfig, and ProfilingCollectionConfig
- Updated AggregationCollector, MetricsCollector, and CustomQueryCollector to GROUP BY multiple columns and produce JSON-encoded compound dimension_value
- Updated engine to read `dimensions` list from config and pass to collectors
- Removed backtick from `_SAFE_IDENTIFIER_RE` (security fix: backtick is the quoting mechanism and must not appear in identifiers)
- No storage schema changes — `dimension_value` column is already a STRING

## Files Changed
- `qualifire/core/config.py` — `dimensions` field, `_validate_dimensions`, removed `_SAFE_IDENTIFIER_RE` backtick
- `qualifire/collection/aggregation.py` — Multi-dimension GROUP BY, JSON encoding
- `qualifire/collection/metrics.py` — Same
- `qualifire/collection/custom_query.py` — Same
- `qualifire/core/engine.py` — Passes `dimensions` list to collectors
- `tests/test_collection/test_collectors.py` — Updated + 8 new multi-dimension tests
- `tests/test_config.py` — Updated dimension config tests
- `docs/configuration.md` — Documented `dimensions` field

## Testing
- 409 total tests pass (8 new for multi-dimension)
- Verified JSON encoding with sorted keys is deterministic
- Verified multi-dimension GROUP BY produces correct compound values
- Verified config validation rejects invalid identifiers
- Security review: blocked and fixed backtick-in-identifier regex

## Notes
- Breaking change: `dimension` (singular) field removed from collection configs. Existing YAML configs must be updated to use `dimensions: ["column_name"]`.
- Dotted identifiers (e.g., `struct.field`) are still supported via the regex, but each segment is not individually quoted. If segment-level quoting is needed, a future enhancement can add `_quote_identifier()`.
