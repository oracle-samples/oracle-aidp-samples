---
id: grouped-notification-by-recipient
name: Grouped Notifications by Recipient and Severity
type: Enhancement
priority: P1
effort: Small
impact: Medium
created: 2026-04-01
---

# Grouped Notifications by Recipient and Severity

## Problem Statement
When multiple datasets or validations produce alerts, recipients can be overwhelmed by individual notifications. Notifications of the same severity going to the same recipient should be collected into a single message with an appropriate, descriptive title rather than sent as separate alerts. This ensures recipients get one clear summary per severity level rather than a flood of individual messages, making it easier to triage and act on data quality issues.

## Proposed Solution
Ensure that all notifications targeting the same recipient at the same severity level are grouped into a single message. Each grouped notification should have a meaningful title that summarizes the scope (e.g., number of datasets affected, severity level). This builds on the existing `_send_grouped_notifications` logic in the engine, which already groups by (channel, severity), but should be reviewed to ensure the message titles and content are clear and actionable.

## Affected Areas
- engine/notification grouping (`_send_grouped_notifications`)
- notification formatting (message title and body construction)
- notifiers (Slack, email, webhook — ensure they handle grouped messages well)
