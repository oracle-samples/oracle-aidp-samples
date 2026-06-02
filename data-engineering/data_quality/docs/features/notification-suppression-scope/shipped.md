---
shipped: 2026-05-10
status: superseded
---

# Superseded: notification-suppression-scope

This idea proposed broadening the per-validation
`suppress_repeat_alerts` flag to include `partition_ts` in
the dedup natural key so a re-run on a NEW partition (after
a prior fire on a DIFFERENT partition) would still page.

## Why superseded

`skip-renotification` (PR #21, merged 2026-05-10) shipped a
broader replacement: the per-validation
`suppress_repeat_alerts: bool = True` YAML field was REMOVED
entirely (clean break) and replaced with a runtime
`skip_renotification: bool = False` flag. Default flipped:
retries / replays now re-page by default; operator opts in to
skip duplicate alerts via `--skip-renotification` /
`Qualifire.run_config(skip_renotification=True)`.

This redesign sidesteps the "per-validation YAML config tweak"
this idea proposed, in favor of a global runtime kill switch
gated on data presence. The semantics are different
(notification-row data presence vs. validation-row severity
match), but the operator-visible outcome is the same: avoid
duplicate paging on retries / replays.

## What if an operator wants partition-aware dedup specifically?

The runtime flag's default-False means today's behavior is
"always dispatch on every run" — the partition-aware case
falls out naturally. If a user later asks for a finer-grained
gate (e.g., "skip ONLY when both severity AND partition
match"), it would land as a follow-up on the runtime flag's
contract, not on the now-removed per-validation YAML field.

## No action required

No code changes; this file marks the idea as superseded so the
backlog dashboard reflects accurate state.
