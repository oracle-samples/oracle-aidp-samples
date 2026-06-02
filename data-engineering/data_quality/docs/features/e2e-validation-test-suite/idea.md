---
id: e2e-validation-test-suite
name: End-to-End Validation Test Suite
type: Feature
priority: P0
effort: Large
impact: High
created: 2026-04-01
blocked_by: auto-create-system-table, collection-persistence-test-gaps, consolidate-requirements-files, custom-sql-query-check, data-quality-health-reports, handle-empty-check-output, historical-data-missing-check, persist-collection-results, timestamp-tracking
---

# End-to-End Validation Test Suite

## Problem Statement
There is no comprehensive end-to-end test that proves the entire Qualifire framework works against realistic data. Existing tests use mocks and isolated unit checks, which don't verify the full pipeline (data generation → Spark loading → collection → validation → result with explanation) under real-world conditions.

A proper E2E test suite needs:

- **Realistic data generation**: At least 30 daily partitions with weekly seasonality patterns, mimicking real-world dataset behavior (e.g., weekday peaks, weekend dips, gradual trends).
- **All collection and validation types exercised**: Every collector (aggregation, metrics, custom_query) and every validation type (SLO, threshold, drift/historical, trend/forecast, anomaly detection/shape) — each tested with scenarios that legitimately pass and scenarios that legitimately fail. **WAP (Write-Audit-Publish)** patterns must also be covered.
- **Multiple datasets for custom SQL**: Custom SQL queries that reference multiple tables must be tested — e.g., fact tables joined with dimension tables, foreign-key constraint checks, cross-table aggregations. This requires generating and loading multiple related datasets (fact + dimension) to validate multi-table query scenarios.
- **No gaming**: Pass and fail outcomes must be driven by the data itself, not by artificially tuned thresholds or cherry-picked metrics. The data should have genuine patterns, and the validations should catch (or pass) based on those patterns naturally.
- **Local Spark execution**: Data loaded into a local Spark table, queried via Spark SQL, and validated through the full engine pipeline.
- **Explainable results**: Each validation result should include a clear explanation of why it passed or failed, verifiable against the generated data.
- **Generated artifacts**: The E2E process must produce a **runnable script** and a full set of **markdown documentation**:
  - **README** — overview of what the E2E suite covers, how it maps to framework capabilities, and how to interpret results
  - **Test setup guide** — prerequisites, environment setup, dataset generation, and Spark configuration
  - **Test execution guide** — how to run the suite locally, expected durations, and troubleshooting common failures
  - **Test report** — scenario-by-scenario results with expected vs actual outcomes, pass/fail status, and explanations
  These artifacts serve as **living documentation of framework correctness** and as **presentation material** to demonstrate the capability and stability of the library to stakeholders.
- **CI/CD integration**: The E2E suite must be runnable as part of a CI/CD pipeline to catch regressions automatically. It should act as a **confidence gate** — a failing E2E run blocks releases and signals a real problem. The pipeline should produce the markdown report as a build artifact so that every run has auditable proof of what passed and what failed.

Without this, there's no confidence that the framework works correctly end-to-end, regressions could go unnoticed, and there's no repeatable way to demonstrate library stability.

## Affected Areas
- tests (new E2E test module)
- data generation (synthetic data with realistic patterns, including related fact/dimension datasets)
- all collection types (aggregation, metrics, custom_query)
- all validation types (SLO, threshold, drift, trend, shape, WAP)
- custom SQL (multi-table joins, foreign-key checks)
- engine/pipeline (full collect → validate → persist flow)
- storage (system table persistence of results)
- artifacts (runnable script, README, setup guide, execution guide, test report)
- CI/CD (pipeline integration, regression gate, report as build artifact)
