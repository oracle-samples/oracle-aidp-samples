---
id: skip-renotification
name: Skip-Renotification — Skip Notification When Already Sent
type: Enhancement
priority: P1
effort: Small
impact: Medium
created: 2026-05-09
---

# Skip-Renotification — Skip Notification When Already Sent

## Problem Statement

The notification stage of the qualifire pipeline routes each
validation result through its configured channels and writes one
row per `(channel, dataset_name, validation_name, metric_name,
dimension_value, severity)` notification key into the system
table with a `notification_status` of `sent`, `failed`, or
`suppressed`. Re-running the same notification dispatch for a
key that already has a `sent` row is wasted work — and worse,
it spams the channel with duplicate alerts.

This feature adds a `skip_renotification: bool = False` runtime
flag that says: **for each notification the engine would
dispatch, look up the natural key in the system table; if a
`sent` notification row already exists for that key, skip the
dispatch and don't re-fire.**

The eligibility check is purely **data-presence**: the question
is "is there a `sent` notification row at this key?", not "what
kind of validator triggered it?" or "what filter scope was
applied?" The notification row either exists in the system
table or it doesn't.

This is independent of any collection-stage or validation-stage
mechanism. The notification stage stands on its own: the engine
takes whatever validation results came out of the validation
stage (cached or freshly-computed), looks up its own persisted
`sent` rows, and short-circuits per key when present.

It is also distinct from today's per-validation
`suppress_repeat_alerts` YAML flag (default `True`,
`qualifire/core/engine.py:_dispatch_notifications`). That flag
suppresses based on prior validation history per the
notification-suppression-scope contract — a fundamentally
different mechanism keyed on validation-row severity, not
notification-row presence. `skip_renotification` keys directly
on the notification row's existence and is unconditional once
opted in.

## Why It Matters

- **No alert spam on retries.** Today, retrying a failed run
  re-fires every notification because the dispatch path runs
  again with fresh validation results. A runtime kill switch
  bypasses dispatch entirely for keys already delivered.
- **Backfill replays don't page.** Backfill currently routes
  notifications through the same dispatcher
  (`qualifire/core/backfill.py:173, 371, 577`). Operators
  replaying 90 days don't want 90 days of pages.
- **Operator UX consistency** with sibling skip-* flags
  governing other pipeline stages — three runtime flags,
  three stages, default-False each, each gated on data
  presence in the system table for that stage's natural key.
- **CI / smoke runs** that exercise the notification path but
  shouldn't broadcast to production channels — flip
  `skip_renotification=True` for the run.

## Default

`skip_renotification = False`. Notify normally on every run
unless the operator opts in. Matches the safe default of the
sibling skip-* flags.

## Who Benefits

- **On-call rotations** — replay scenarios that today fire
  duplicate pages no longer do.
- **Backfill drivers** — replays of 90-day windows no longer
  spam channels with already-fired alerts.
- **CI / smoke harnesses** — rehearse the dispatch path without
  broadcasting.
- **AIDP demo / iteration loops** rehearsing the validator
  pipeline multiple times in a session.

## Affected Areas

### Public API — new `skip_renotification` flag

- `Qualifire.run_config(..., skip_renotification=False)`
- `Qualifire.run_config_parsed(..., skip_renotification=False)`
- `Qualifire.backfill(..., skip_renotification=False)`
- `Qualifire.write_audit_publish(..., skip_renotification=False)`
- CLI: `--skip-renotification` on `qualifire run` and
  `qualifire backfill`
- `QualifireContext.skip_renotification` attribute

### Engine — notification-stage pre-pass

- `qualifire/core/engine.py:_dispatch_notifications` — for each
  candidate `(channel, dataset, validation_name, metric,
  dimension, severity)` key the dispatcher would route, check
  the system table for an active notification row with
  `notification_status='sent'`. If present, skip the dispatch
  for that key and (optionally) emit a synthetic
  `notification_status='run_level_skip'` row for forensic
  visibility. Plan phase decides whether the synthetic row is
  worth writing.
- The check is purely data-presence. No validator-type
  whitelist; no special-case for sample-based or filtered
  validators; no special-case for engine-warning notifications.
  Plan phase decides whether engine-warning notifications
  should be skippable too (probably yes — operators can always
  fix the underlying issue without paging) or unconditional
  (probably no — operators need to know storage is broken).

### Storage — read path

- New helper
  `read_notification_at_key(channel, dataset_name,
  validation_name, metric_name, dimension_value, severity,
  partition_ts, ...)` returning the most-recent active
  notification row with `notification_status='sent'`, or
  `None`. Implemented across all four backends
  (`SQLiteStorage`, `DeltaStorage`, `JDBCStorage`,
  `ExternalCatalogStorage`).
- The natural key may need to include `partition_ts` —
  notification rows currently carry it but the read path
  doesn't always filter on it. Plan phase audits.

### Tests

- Replay regression: flag set, run a partition that produces
  ERROR validations, assert zero notifier `send` calls when
  the prior run already delivered for those keys.
- Backfill replay regression: flag set, replay 14 partitions,
  assert dispatch counts match expectations.
- Stale row handling: tombstoned notification rows do NOT
  short-circuit; the engine falls through to dispatch.
- Failure-row handling: a `notification_status='failed'` row
  does NOT short-circuit; only `sent` rows do (failure means
  the alert never landed, so it must retry).
- Per-channel independence: skipping `slack` for a key when
  `slack` has a sent row but `console` doesn't — `console`
  still dispatches.

### Documentation

- `docs/notifications.md` — note the flag and the per-key
  data-presence rule that drives it.
- `docs/CHANGELOG.md` — Enhancement entry with the retry /
  replay use cases.
- `docs/programmatic_api.md` — flag added with examples.

## Relationship to Other Features

- **Independent of `skip-recollection` and
  `skip-revalidation`.** All three are sibling runtime flags,
  each governing one pipeline stage, each gated on data
  presence in the system table for that stage's natural key.
  They compose freely — any subset of the three can be
  enabled in any combination — but they don't depend on each
  other for eligibility, schema, or correctness. Plan phase
  ships them as independent features.
- **Distinct from `notification-suppression-scope`**. That
  backlog item proposes broadening the per-validation
  `suppress_repeat_alerts` key to include `partition_ts`. This
  feature adds an orthogonal *runtime* flag that bypasses
  dispatch entirely on a per-(channel, key) basis when a
  `sent` row exists. The two solve different problems (per-
  validation YAML config tweak vs. runtime kill switch on
  duplicate dispatch) and can ship independently in either
  order.

## Open Questions for Planning

- **Synthetic forensic row** — should the engine write a
  `notification_status='run_level_skip'` row when the flag
  short-circuits a key, mirroring the existing
  `'suppressed'` rows emitted by `suppress_repeat_alerts`?
  Helps operators audit what *would* have fired.
- **Engine-warning notifications** — engine-level warnings
  (persistence failures, suppression-read failures) route
  through the same dispatcher. Should `skip_renotification`
  apply to those, or are they unconditional? Probably
  unconditional (the operator needs to know infrastructure
  is broken). Plan phase pins.
- **Failure-row replay** — should a key with only a
  `notification_status='failed'` row still attempt a fresh
  dispatch under `skip_renotification=True`? Yes (it never
  delivered) but worth pinning explicitly so the test matrix
  covers it.
- **Per-channel replay matrix** — when one channel succeeded
  and another failed in a prior run, the next run with
  `skip_renotification=True` should retry only the failed
  channel for that key. Plan phase verifies the lookup
  granularity supports this.
- **Cross-partition matching** — should the lookup match
  across partitions or be partition-anchored? Plan phase
  pins (probably partition-anchored to match the natural
  key).

## Migration

- No migration needed for the flag itself; default `False`
  matches today's "always dispatch" behaviour.
- Storage helper additions are additive — existing readers
  continue to work.
- Existing per-validation `suppress_repeat_alerts: true` (the
  default) continues to apply when `skip_renotification=False`.
  When `skip_renotification=True`, both gates fire (the
  per-validation suppression first, then the runtime kill
  switch); plan phase pins precedence to avoid surprise.
