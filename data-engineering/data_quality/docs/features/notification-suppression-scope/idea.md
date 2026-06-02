---
id: notification-suppression-scope
name: Notification Suppression Scope — Partition-Aware Dedup
type: Enhancement
priority: P2
effort: Medium
impact: Medium
created: 2026-05-09
---

# Notification Suppression Scope — Partition-Aware Dedup

## Problem Statement

The current notification suppression key is `(dataset_name, validation_name,
metric_name, dimension_value)` — `partition_ts` is intentionally excluded
(`qualifire/core/models.py:248-261`, `engine.py:1600-1613`). Once an alert
fires for a given (dataset, check, metric, dimension) at some severity, the
**next run that produces the same severity for the same key is suppressed
regardless of which partition produced it**.

This is the right contract for nightly cron pipelines that process one
partition per execution and don't want to re-page operators about an
unchanged condition. It is **surprising** for any execution model that
treats alerts as per-partition events:

- Backfilling 14 partitions in a single notebook run silently produces one
  alert + thirteen `notification_status='suppressed'` rows when the data
  finding is consistent across days.
- Re-running an *older* partition (e.g. `current_date - 2`) compares the
  current severity against the most-recent-by-`run_timestamp` prior row,
  not against the row for the same partition. So the re-run is also
  suppressed when severities match — even when the prior row described a
  different partition.
- Operators tracking per-day SLO breaches expect each day's breach to fire
  its own alert.

This was surfaced concretely while building the `dataquality` demo on AIDP:
the seeder ramps `mean_amount` and `mean_sessions` on days 11–14
(2026-05-05..2026-05-08), each of which produces an ERROR for
`sales_amount_drift.avg_amount`. After the first day's alert was persisted,
the remaining three days collapsed into `suppressed` synthetic
notification rows — which the operator never sees on Slack, never sees on
console, and which carry NULL `dataset_name` / `validation_name` /
`validation_message` columns in the system table (see "Related: persistence
bug" below).

## Affected Areas

- notification (suppression contract)
- core/models (`ValidationKey`)
- core/engine (`_dispatch_notifications`, `_prefetch_suppression_snapshot`,
  `_is_suppressed`)
- core/config (per-validation `suppress_repeat_alerts` flag — extend or
  add `suppress_scope`)
- docs (notification-design page documenting the contract)
- tests (engine + integration regressions)

## Proposed Solution (high-level — refine in `/feature-plan`)

Three options, **not yet decided**. The plan phase will pick:

**A) Include `partition_ts` in `ValidationKey`** — partition-scoped dedup
becomes the default. Each (key × partition) is its own suppression cell.
Backwards-incompatible behaviour change for existing pipelines that depend
on the per-issue dedup model. Low code volume (~30 lines + tests).

**B) Per-validation `suppress_scope` switch** — keep current key as the
default but let YAML opt in:
```yaml
- type: drift
  name: sales_amount_drift
  suppress_scope: "partition"   # default: "issue" (current behaviour)
```
Each validation chooses. Doesn't disrupt existing setups; gives operators
the per-partition behaviour where they explicitly need it. Slightly larger
change (config schema + key dispatch).

**C) Documentation-only** — keep current behaviour, document it
explicitly with worked examples in `docs/notification-design.md` and the
YAML schema. Cheapest. Doesn't fix the operational surprise; just
acknowledges it.

The plan-phase decision should weigh: how often operators run multi-
partition backfills, whether per-validation flexibility matters, and the
migration cost for in-flight users.

## Related: persistence bug for suppressed alert rows

The synthetic `NotificationResult` emitted by `engine.py:1788` when an
alert is suppressed is constructed with a meaningful `message` field
(`f"Duplicate alert suppressed for {dataset}/{base}"`), but the
persistence layer writes `dataset_name`, `validation_name`, and
`validation_message` as SQL NULL on the resulting system-table row.
Verified on the live ADW system table: 16 most-recent suppression rows all
have NULL in those columns.

This loses forensic context — operators can't reconstruct which alerts
were suppressed in which run. Fix should populate at least
`dataset_name`, `validation_name` (the suppressed validation's base or
full name), and `validation_message` (the suppression reason) on the
persisted row. Smallest-blast-radius approach: extend the
`NotificationResult` shape with explicit `dataset_name` / `validation_name`
fields, populate them in the engine's suppression branch, and have the
persistence path write them.

This bug should ship in the same feature — it surfaced in the same
investigation and fixing only the scope question would leave the
forensic gap.
