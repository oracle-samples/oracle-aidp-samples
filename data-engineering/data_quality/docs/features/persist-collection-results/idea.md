---
id: persist-collection-results
name: Persist Collection Results for Historical Comparison
type: Enhancement
priority: P1
effort: Small
impact: High
created: 2026-04-01
---

# Persist Collection Results for Historical Comparison

## Problem Statement
The system table (shipped in `system-table-collected-data`) stores collection metadata — dataset name, collector name, run date, and dimension values — but does not persist the actual collected metric values. Without stored results, checks that need to compare the current data point against historical values have no baseline to reference.

This is specifically needed for **SLO checks**, **trend detection**, and **drift detection**, which all require access to prior collected values to evaluate whether current data points are within expected bounds or following expected patterns. Anomaly detection checks do not require this, as they use a different statistical approach.

Without persisted results, these checks cannot function as designed, and users lose the ability to generate reports that show how data quality metrics have changed over time.

## Proposed Solution
Store the actual collected metric values alongside the existing system table metadata so that subsequent runs can retrieve and compare against historical data points.

## Affected Areas
- storage/persistence (extend system table or add results table)
- collection pipeline (write metric values after collection)
- checks/validations (SLO, trend, drift — read historical values for comparison)
- reporting (historical metric trends)
