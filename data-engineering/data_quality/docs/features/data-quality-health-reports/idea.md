---
id: data-quality-health-reports
name: Data Quality Health Reports
type: Feature
priority: P1
effort: Medium
impact: High
created: 2026-04-01
blocked_by: system-table-collected-data
---

# Data Quality Health Reports

## Problem Statement
There is no built-in way to visualize or report on the overall health of datasets being monitored by the data quality system. Users lack answers to key questions:

- **Coverage**: What percentage of datasets have checks configured? How does coverage break down by standard catalog vs. external catalog?
- **Check category distribution**: What percentage of datasets are checked against each category of test (SLO, rule-based, trend, anomaly, etc.)?
- **Performance**: How are data quality checks performing over time? Are pass rates improving or degrading?
- **Problem datasets**: Which datasets are failing the most checks, and which checks are they failing?

Without these reports, teams have no aggregated view of their data quality posture and cannot prioritize remediation efforts effectively. They must manually inspect individual check results to piece together the big picture.

## Proposed Solution
Provide built-in report generation that queries the system table to produce health dashboards covering coverage metrics, check category breakdowns, performance trends, and a "worst offenders" view of datasets with the most failures.

## Affected Areas
- reporting/visualization (new report generation layer)
- system table queries (read from persisted collection and validation data)
- configuration (which reports to generate, filters, groupings)
