---
started: 2026-04-01
---

# Implementation Plan: Grouped Notifications by Recipient and Severity

## Overview
The engine already groups notifications by (channel, severity) and sends one notification per group. However, notifiers always call `format_notification_message()` (single-dataset format), ignoring `format_grouped_notification_message()` which exists but is never called. The grouping decision should be centralized in the `Notifier` base class so individual implementations don't need to handle it.

## Implementation Steps
- [x] Step 1: Add `datasets` parameter to `Notifier.send()` and a `_format_message()` helper on the base class that centralizes the grouped vs. single formatting decision
- [x] Step 2: Update engine `_send_grouped_notifications()` to pass the original datasets list to `notifier.send()`
- [x] Step 3: Update `SlackNotifier.send()` to use `self._format_message()` with grouped title
- [x] Step 4: Update `EmailNotifier.send()` to use `self._format_message()` with grouped subject line
- [x] Step 5: Update `WebhookNotifier.send()` to include per-dataset breakdown in JSON payload when grouped
- [x] Step 6: Add tests for grouped message formatting across all three notifiers
- [x] Step 7: Verify full test suite passes (431 passed)

## Technical Decisions

### Base class handles grouping decision
- Add `datasets: list[DatasetResult] | None = None` to `Notifier.send()`
- Add `_format_message(dataset_result, severity, owner, bu, datasets=None)` to the base `Notifier` class
- `_format_message()` checks: if `datasets` has >1 entry, calls `format_grouped_notification_message()`; otherwise calls `format_notification_message()`
- All notifiers call `self._format_message(...)` — no grouping logic in implementations

### Engine change
- Pass the original datasets list to `notifier.send(..., datasets=datasets)` for multi-dataset groups
- Single-dataset groups continue to pass just the one DatasetResult (no `datasets` param)

### Per-notifier changes (minimal)
- **Slack**: Use `self._format_message()` for text; adjust title to show dataset count when grouped
- **Email**: Use `self._format_message()` for body; adjust subject to show dataset count when grouped
- **Webhook**: Add `datasets` array to JSON when grouped (structural, not text formatting)

## Files to Modify

| File | Change |
|------|--------|
| `qualifire/notification/base.py` | Add `datasets` param to `send()`, add `_format_message()` helper |
| `qualifire/core/engine.py` | Pass datasets list to `notifier.send()` |
| `qualifire/notification/slack_notifier.py` | Use `self._format_message()`, grouped title |
| `qualifire/notification/email_notifier.py` | Use `self._format_message()`, grouped subject |
| `qualifire/notification/webhook_notifier.py` | Add per-dataset JSON breakdown |
| `tests/test_notification/test_notifiers.py` | Add grouped notification tests |

## Testing Strategy
- Test `_format_message()` base class method directly for grouped and single cases
- Test each notifier with grouped datasets — verify correct formatting
- Test each notifier with single dataset — verify existing behavior unchanged
- Run full test suite to confirm no regressions

## Risks & Mitigations

| Risk | Mitigation |
|------|------------|
| Breaking existing notifier implementations | `datasets` parameter is optional with default `None` |
| Grouped message too long for Slack | Slack limits are 40K chars — validation messages are short |
| Webhook consumers expect current JSON structure | `datasets` field is additive |
