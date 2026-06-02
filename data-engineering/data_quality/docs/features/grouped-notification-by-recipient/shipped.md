---
shipped: 2026-04-01
---

# Shipped: Grouped Notifications by Recipient and Severity

## Summary
Wired up the existing but unused `format_grouped_notification_message()` function to all three notifiers (Slack, Email, Webhook). Grouping logic is centralized in the `Notifier` base class via a new `_format_message()` helper — individual notifiers call it and only handle channel-specific concerns (emoji, subject line, JSON structure). When multiple datasets are grouped into one notification, recipients now get a single well-structured message with a meaningful title showing the dataset count, rather than a flat summary with `"{N} dataset(s)"` as the name.

## Key Changes
- Added `_format_message()` helper to `Notifier` base class that centralizes the grouped vs. single formatting decision
- Added optional `datasets: list[DatasetResult] | None` parameter to `Notifier.send()` ABC (backward compatible)
- Engine passes original datasets list to `notifier.send()` for multi-dataset groups
- SlackNotifier: grouped title shows dataset count (e.g., `":x: *Qualifire ERROR — 3 datasets*"`)
- EmailNotifier: grouped subject includes dataset count and owner/bu (e.g., `"[Qualifire ERROR] 3 datasets — team/bu"`)
- WebhookNotifier: adds `"datasets"` array to JSON payload with per-dataset breakdown when grouped
- All notifiers use `self._format_message()` instead of directly calling `format_notification_message()`

## Files Changed
- `qualifire/notification/base.py`
- `qualifire/core/engine.py`
- `qualifire/notification/slack_notifier.py`
- `qualifire/notification/email_notifier.py`
- `qualifire/notification/webhook_notifier.py`
- `tests/test_notification/test_notifiers.py`

## Testing
- 431 tests pass (10 new tests added)
- Tests for `format_grouped_notification_message()` — single and multi-dataset
- Tests for each notifier with grouped datasets — verify correct title/subject/JSON
- Tests for each notifier with single dataset — verify backward compatibility
- Security review passed with no blocking issues

## Notes
- The `format_grouped_notification_message()` function already existed in base.py but was never called — this feature wires it up
- The `datasets` parameter on `send()` is optional with default `None` — existing custom notifier implementations continue to work unchanged
- Webhook grouped payload adds a `datasets` array alongside existing fields (additive, not breaking)
