---
id: timestamp-tracking
name: Timestamp Tracking for Collection, Validation, and Notification
type: Enhancement
priority: P2
effort: Small
impact: Medium
created: 2026-04-01
---

# Timestamp Tracking for Collection, Validation, and Notification

## Problem Statement
The system table currently stores only a `run_date` (date granularity) for each collection and validation row. This assumes datasets are loaded once per day, but in practice many datasets are loaded **hourly or every N hours**. When multiple runs occur on the same date, there is no way to distinguish between them or determine which run produced which results.

Additionally, there is no visibility into when each pipeline phase (collection, validation, notification) actually executed. This makes it difficult to debug pipeline latency, audit execution order, or correlate results with specific data loads.

## Proposed Solution
Replace or supplement the date-only `run_date` with separate timestamps for each pipeline phase:

- **collected_at** — when data collection completed
- **validated_at** — when validation checks completed
- **notified_at** — when notifications were sent

This enables distinguishing intra-day runs and provides observability into pipeline phase timing.

## Affected Areas
- storage/persistence (system table schema — add timestamp columns across all backends)
- collection pipeline (record collected_at after collection)
- validation pipeline (record validated_at after validation)
- notification pipeline (record notified_at after notification)
- historical reads (query by timestamp instead of date for sub-daily granularity)
