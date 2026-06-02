---
started: 2026-04-01
---

# Implementation Plan: Multi-Dimension Support for Collection and Validation

## Overview

Extend the collection pipeline to accept a list of dimensions instead of a single string. When multiple dimensions are specified, collectors GROUP BY all of them and produce one CollectionResult per unique combination, with a pipe-separated compound `dimension_value` (e.g., `"US|electronics"`).

Backward compatible: the existing `dimension: "region"` syntax keeps working. The new `dimensions: ["region", "category"]` syntax enables multi-dimension slicing.

## Implementation Steps

- [x] Step 1: Add `dimensions` field to collection configs in `config.py`
  - Add `dimensions: list[str] | None = None` to `AggregationCollectionConfig`, `MetricsCollectionConfig`, `CustomQueryCollectionConfig`, and `ProfilingCollectionConfig`
  - Add a `model_validator` that merges `dimension` (single) into `dimensions` (list), errors if both are set
  - Validate each item in `dimensions` with `_SAFE_IDENTIFIER_RE`
  - Keep `dimension` field for backward compatibility

- [x] Step 2: Update `AggregationCollector` for multi-dimension
  - Change `__init__` to accept `dimensions: list[str] | None = None` (replace `dimension`)
  - Update `_collect_with_dimension()` to GROUP BY all dimension columns
  - Encode compound dimension value as JSON with sorted keys: `json.dumps({d: str(row[d]) for d in sorted(dimensions)}, sort_keys=True)`
  - Single dimension: `{"region": "US"}`. Multiple: `{"category": "electronics", "region": "US"}`
  - Store all dimension names in metadata: `"dimensions": self.dimensions`

- [x] Step 3: Update `MetricsCollector` for multi-dimension
  - Same pattern as AggregationCollector

- [x] Step 4: Update `CustomQueryCollector` for multi-dimension
  - Same pattern; filter out all dimension columns from metric output (not just one)

- [x] Step 5: Update engine `_collect()` to resolve dimensions
  - Read `dimensions` (list) from collection config, falling back to `[dimension]` if only single is set
  - Pass `dimensions=resolved_list` to collector constructors

- [x] Step 6: Write tests
  - Multi-dimension GROUP BY produces correct compound dimension_value
  - Single dimension still works (backward compat)
  - Both `dimension` and `dimensions` set in config → error
  - Compound dimension_value is persisted to system table
  - Dimension values with special characters are encoded correctly

- [x] Step 7: Update docs and examples
  - Update `docs/configuration.md` collection type docs with `dimensions` field
  - Add multi-dimension example

## Technical Decisions

1. **JSON encoding with sorted keys** for compound dimension values: `{"category": "electronics", "region": "US"}`. Alphabetical key ordering ensures deterministic, comparable strings. Easy to parse in any language.

2. **Backward compatibility**: `dimension: "region"` → internally becomes `dimensions: ["region"]`. Existing configs work unchanged. New configs can use `dimensions: [...]`.

3. **Mutually exclusive**: `dimension` and `dimensions` cannot both be set — config validation error.

4. **Collectors receive `dimensions: list[str]`**: The singular `dimension` field is a config-level convenience. Collectors always work with the list.

5. **No storage schema changes**: `dimension_value` column is already a STRING. Compound values fit naturally.

## Files to Modify

| File | Change |
|------|--------|
| `qualifire/core/config.py` | Add `dimensions` field + merge validator on 4 collection configs |
| `qualifire/collection/aggregation.py` | Accept `dimensions`, GROUP BY multiple columns |
| `qualifire/collection/metrics.py` | Same |
| `qualifire/collection/custom_query.py` | Same |
| `qualifire/core/engine.py` | Resolve dimensions from config, pass to collectors |
| `tests/test_config.py` | Config validation tests |
| `tests/test_collection/test_collectors.py` | Multi-dimension collection tests |
| `docs/configuration.md` | Document `dimensions` field |

## Files That Should NOT Change

| File | Reason |
|------|--------|
| `qualifire/core/models.py` | `dimension_value` stays as `str` — compound values are encoded |
| `qualifire/storage/*.py` | No schema change needed |
| `qualifire/validation/*.py` | Validators consume CollectionResult — decoupled from dimensions |

## Testing Strategy

- **Unit tests**: Config merge/validation for dimension → dimensions
- **Unit tests**: Each collector with 2 dimensions produces compound dimension_value
- **Integration test**: Full engine pipeline with multi-dimension, verify system table has compound values
- **Backward compat**: All existing single-dimension tests still pass

## Risks & Mitigations

| Risk | Mitigation |
|------|------------|
| Special characters in dimension values | JSON encoding handles all characters natively — no escaping concerns |
| Historical reads with compound dimension_value | Existing `read_metric_history` filters by `table_name + metric_name`, not by dimension — no change needed |
| Large cardinality from multi-dimension GROUP BY | User responsibility; same as single dimension. No new risk. |
