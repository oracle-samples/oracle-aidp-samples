---
id: webhook-payload-redaction
name: Webhook Payload Redaction Policy
type: Enhancement
priority: P2
effort: Small
impact: Medium
created: 2026-05-07
---

# Webhook Payload Redaction Policy

## Problem Statement

The R3 notification enrichment pushes `vr.details` into Slack /
email / webhook bodies in full. For shape / pattern alerts, that
includes `top_contributing_features` — the SHAP-named columns the
classifier weighted highest.

Today there's no way for an operator to say "don't ship the names
of these columns over an unencrypted webhook" or "redact this
column entirely from Slack." A pattern-check running on a customer
table will leak feature names like `email_domain`, `home_zipcode`,
`account_routing_number` straight into the configured channels —
and a SHAP top-feature list naming `home_zipcode` is enough to
flag a HIPAA / GDPR / PCI compliance review even though the values
themselves never leave the data plane.

It's not a bug — the field never promised privacy. It's a missing
policy knob.

## Why It Matters

- Operators in regulated industries (financial services, life
  sciences, healthcare) have to disable the notification
  enrichment wholesale today, which throws away the operational
  benefit. The R3 enrichment is the most-cited improvement of
  the partition-tracking-and-dashboards release; we shouldn't
  force operators to choose between operational signal and
  compliance posture.
- Webhook receivers may be third-party services (PagerDuty,
  Opsgenie, Teams) — log retention there is outside the operator's
  control. A column name leaked into a third-party retention
  bucket is harder to retract than a field in their own system
  table.
- The fix is small (allowlist / denylist + redaction marker) but
  the policy choice (allowlist vs denylist, dataset-level vs
  validation-level vs notification-level) deserves explicit
  capture before someone ad-hocs it.

## Who Benefits

- Operators in compliance-sensitive industries who currently
  can't enable webhook notifications at all.
- Security teams who want a single audit point for
  "what column names can leave our perimeter."
- Anyone doing PII scans on outbound notification channels.

## Affected Areas

- `qualifire/notification/webhook_notifier.py` (payload
  construction, where the redaction would land)
- `qualifire/notification/base.py` (Slack / email body —
  the same redaction must apply to those bodies, not just
  webhook)
- `qualifire/core/config.py` (where the redaction config
  attaches — see open questions)
- `qualifire/validation/{pattern_check,isolation_forest}.py`
  (so SHAP-naming itself can skip redacted columns at compute
  time, not just at notification time, for defense in depth)

## Open Questions for Planning

These belong in `plan.md`, listing here for context only:

- **Allowlist or denylist?** Allowlist is the safer default
  (explicit opt-in to ship a column name); denylist is the
  ergonomic default for operators who only need to redact a
  handful. Likely both, with the allowlist taking precedence.
- **Where does the redaction config attach?** Notification-level
  (apply to all alerts on a channel), validation-level (apply
  to one specific check), or dataset-level (apply to every
  validation on a sensitive table)? Probably dataset + a
  global Qualifire-instance-level allowlist.
- **What does the redacted output look like?** Full omission
  (drop the feature entirely from the body), placeholder
  (`<redacted>`), or hashed reference (`<column_a3f0…>` so
  alert correlation across runs still works)? Placeholder is
  cheapest; hashed reference preserves alert grouping at the
  cost of reversibility.
- **Should this also apply to the system-table `details_json`
  write?** The system table is operator-controlled and usually
  on the same compliance perimeter as the data, but if it
  isn't (Snowflake target, JDBC to a less-trusted tier), the
  same redaction may apply.
- **Retroactive cleanup?** When an operator adds a column to
  their denylist, do already-persisted system-table rows need
  scrubbing? Probably out of scope — this feature is about
  egress, not stored-data retention.
