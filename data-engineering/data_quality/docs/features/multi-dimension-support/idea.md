---
id: multi-dimension-support
name: Multi-Dimension Support for Collection and Validation
type: Enhancement
priority: P1
effort: Medium
impact: High
created: 2026-04-01
---

# Multi-Dimension Support for Collection and Validation

## Problem Statement
Currently, collection and validation configs support at most a single `dimension` field for GROUP BY slicing. In practice, data quality checks often need to be broken down by multiple dimensions simultaneously (e.g., by region AND product category, or by date AND source system). Without multi-dimension support, users must create separate validation configs for each dimension combination, leading to config duplication, incomplete coverage, and difficulty tracking quality across compound segments.

## Proposed Solution
Extend the collection and validation pipeline to accept a list of dimensions instead of a single string. When multiple dimensions are specified, collectors should GROUP BY all of them and produce one CollectionResult per unique combination. Validators and the persistence layer should propagate the compound dimension value through the pipeline.

## Affected Areas
- collection configs (AggregationCollectionConfig, MetricsCollectionConfig, CustomQueryCollectionConfig — `dimension` field)
- collectors (AggregationCollector, MetricsCollector, CustomQueryCollector — GROUP BY logic)
- core models (CollectionResult `dimension_value` — may need to encode multiple values)
- engine/persistence (system table `dimension_value` column — compound key encoding)
- storage/read (read_metric_history — filtering by compound dimension)
- YAML config schema
