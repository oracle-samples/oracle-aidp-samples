# Plan: skip-renotification

## Goal

Replace the per-validation `suppress_repeat_alerts: bool = True`
config flag with a runtime `skip_renotification: bool = False`
flag plumbed through `Qualifire.run_config`, `run_config_parsed`,
`backfill`, `write_audit_publish`, the CLI, and
`QualifireContext`. Same snapshot-based duplicate-alert mechanism;
the trigger moves from per-validation YAML to a single
opt-in runtime kill switch. Default flips from "always
suppress per-validation" to "always dispatch unless opted in."

## Locked Decisions (from planning sweep)

1. **Remove `suppress_repeat_alerts` entirely.** Clean break.
   No backwards-compat alias. Per durable repo rule "no shims —
   qualifire is pre-release."
2. **`notification_status='skipped'`** for the synthetic
   forensic row when the runtime flag fires (was
   `'suppressed'`).
3. **Default `False`.** Re-runs and replays *will* re-page by
   default; the operator must opt in via `--skip-renotification`
   on retries / backfills.
4. **All four storage backends** see no change. The mechanism
   reuses the existing `read_validation_history_bulk` snapshot —
   no new read path needed.

## Scope Realignment from idea.md

The original idea.md proposed reading **notification rows** via a
new `read_notification_at_key` storage helper across four
backends. Investigation shows today's notification rows persist
at one row per `(channel, severity, base_name)` group, with
`dataset_name=None`, `validation_name=None`, `metric_name=None`,
`dimension_value=None`, `partition_ts=None` (see
`_persist_notification_rows` in `qualifire/core/engine.py:2117`).
A natural-key lookup keyed on those columns is therefore not
implementable without a much larger refactor (per-VR notification
rows + new storage shapes). The minimal mechanism that satisfies
the user's stated goal — "retries / backfills don't re-page" —
is to repurpose the existing snapshot-based suppression filter
that already runs against `read_validation_history_bulk`. That
snapshot keys on `(dataset, validation, metric, dim)` already and
was used by `suppress_repeat_alerts` today. The flag rename + a
default flip is the minimal correctness-preserving change.

The deeper "per-channel notification-row data-presence check"
design from idea.md is captured as a follow-up backlog item
(`skip-renotification-per-channel-row`) that would shift the
notification-persistence shape to per-VR rows. Not in scope here.

## What Changes

### Public API — flag plumbing pattern

**Pattern: mirror `skip_if_cached` exactly** (already shipped,
proven on this codebase).

Site-by-site changes (verified by grep at plan time):

- **`qualifire/core/context.py:63`** — add
  `self.skip_renotification: bool = False` next to the existing
  `self.skip_if_cached`.
- **`qualifire/api.py:522`** `Qualifire.run_config(...,
  skip_renotification: bool = False)` — mirrors line 522.
- **`qualifire/api.py:569`** `run_config_parsed(...,
  skip_renotification: bool = False)` — sets
  `ctx.skip_renotification = skip_renotification` next to the
  existing line 645 setter.
- **`qualifire/api.py:671`** `Qualifire.backfill(...,
  skip_renotification: bool = False)` — pass-through alongside
  the line 779 forwarding.
- **`qualifire/api.py:915-925`** `Qualifire.validate(...,
  skip_renotification: bool = False)` — codex round 1 MAJOR;
  this entry point also builds a `QualifireContext` (line 976),
  so `skip_renotification` MUST be plumbed here too. Same for
  `Qualifire.validate_query()` (api.py:1017 area — verify and
  update).
- **`qualifire/api.py` write_audit_publish** — same pattern.
- **`qualifire/core/backfill.py`** — codex round 1 BLOCKER:
  `run_backfill(..., skip_if_cached, ...)` at line 197 also takes
  `skip_renotification: bool`. Threads through `_process_anchor`
  (line 338) and `_run_anchor_once` (line 558); then sets
  `ctx.skip_renotification` at line 588 alongside the existing
  `ctx.skip_if_cached`.
- **`qualifire/cli.py:_cmd_run`, `_cmd_backfill`** — add
  `--skip-renotification` argparse flag at the same sites that
  carry `--skip-if-cached` today (lines 245 and 426); forward to
  the API.

**Test stub adjustment** (codex round 1 MEDIUM): the existing
`tests/test_cli.py:748` defines a fake `run_config_parsed`
signature that hardcodes `skip_if_cached=False`. The same stub
must accept `skip_renotification=False` so the CLI plumbing test
still runs after the new flag is added to the real signature.

Engine read site: `qualifire/core/engine.py:1887` —
replace `getattr(val_config, "suppress_repeat_alerts", True)`
with `self.context.skip_renotification`. Today's per-validation
gate becomes a runtime gate; the rest of the dispatch loop is
unchanged.

### Loud-fail on stale YAML key (codex R2 MAJOR)

Pydantic v2's default `extra="ignore"` behavior would silently
drop `suppress_repeat_alerts: true` from a YAML config —
operators upgrading would lose their setting without any warning.
A clean break requires a loud failure. Add a single shared
helper that all six affected validation config classes invoke:

```python
# qualifire/core/config.py — near the top
_REMOVED_FIELDS_GUIDANCE = {
    "suppress_repeat_alerts": (
        "removed in skip-renotification feature; pass "
        "skip_renotification=True at the runtime entry point "
        "(Qualifire.run_config / qualifire run / qualifire backfill) "
        "instead. Each pipeline stage now has a single runtime "
        "kill switch instead of a per-validation YAML flag."
    ),
}

def _reject_removed_fields(cls, values):
    if not isinstance(values, dict):
        return values
    for field, guidance in _REMOVED_FIELDS_GUIDANCE.items():
        if field in values:
            raise ValueError(
                f"{cls.__name__}: field '{field}' has been removed — "
                f"{guidance}"
            )
    return values
```

Each of the 6 affected validation config classes adds:

```python
_check_removed_fields = model_validator(mode="before")(
    _reject_removed_fields,
)
```

Cost: ~30 lines including the helper; defended against silent
key drop forever; adds zero state and zero ongoing maintenance
burden.

### Removals (clean break)

- **REMOVE** `suppress_repeat_alerts` from 6 config locations:
  - `qualifire/core/config.py:630, 648, 688, 716, 745, 873`
- **REMOVE** `suppress_repeat_alerts: bool = True` from the two
  `_SyntheticVC` dataclasses inside `qualifire/core/engine.py`
  (lines 1860, 2012). Replace with the new runtime gate.
- **REMOVE** the `getattr(val_config, "suppress_repeat_alerts", True)`
  read at `engine.py:1887`. The runtime gate replaces it.

### Engine — gate consolidation

Today the engine pre-fetches the suppression snapshot
unconditionally and reads the per-validation
`suppress_repeat_alerts` flag inside the per-group dispatch loop
to decide whether to filter against the snapshot. Post-change:

- **Gate the pre-fetch on the runtime flag** (codex round 2
  optimization): when `self.context.skip_renotification` is
  False (the default), `_prefetch_suppression_snapshot` returns
  an empty dict immediately without hitting storage. This avoids
  a wasted bulk-read on every run — the snapshot is only ever
  consumed inside the suppression branch.
- The snapshot filter only fires when
  `self.context.skip_renotification` is True. When False, **all**
  matching VRs are dispatched regardless of prior history —
  restoring "always re-page on retry" as the new default.

Rename the synthetic `'suppressed'` row to `'skipped'` to match
the runtime flag's name; both the channel column (`*`) and the
message body update accordingly.

### Engine warnings — same gate

`_SyntheticVC` for engine warnings (engine.py:2020) loses
`suppress_repeat_alerts=True`. Engine warnings are now governed
by the same runtime gate: when `skip_renotification=True`, a
prior `sent` engine warning is suppressed; default-False means
infrastructure failures continue to page on every retry, which
is the safer behavior anyway.

### Storage — no changes

The existing `read_validation_history_bulk(keys, limit=1)` path
is reused. No new helpers, no schema migration, no per-backend
work.

### Tests

**Plan-time audit result**: `grep -rn "suppress_repeat_alerts"
tests/` returns ZERO matches. The duplicate-alert suppression
mechanism today is **untested** — this is itself a gap the
present feature closes. Nothing to remove; only additions
needed.

- **NEW** `tests/test_skip_renotification.py`:
  - `Qualifire.run_config(skip_renotification=True)` against a
    snapshot containing a prior ERROR for the same key →
    notifier `send` not called; synthetic `'skipped'` row
    emitted.
  - `Qualifire.run_config(skip_renotification=False)` (default)
    against the same snapshot → notifier `send` called even
    though the prior ERROR exists. **This pins the default-flip**
    that this feature introduces.
  - `Qualifire.backfill(skip_renotification=True)` replay
    regression: 14 partitions replayed, dispatch count stays at
    zero when the prior history matches.
  - Per-key partial: 2 keys, 1 has prior ERROR, 1 doesn't —
    new key still dispatches, suppressed key emits the
    `'skipped'` synthetic row only when *all* keys are
    suppressed (preserves the existing all-or-nothing-vs-partial
    contract from engine.py:1929-1943).
  - Engine-warning flow: `qualifire.persistence` warning fires
    on second retry too when default-False (was suppressed
    before; this is the documented behavior change).
- **NEW** CLI plumbing tests (in existing
  `tests/test_cli_run.py` / `tests/test_cli_backfill.py`):
  `--skip-renotification` flag plumbs through to
  `run_config_parsed` / `backfill` namespace.

### Models

- **`qualifire/core/models.py:181`** (codex round 1 LOW):
  `NotificationResult.status` is `str`-typed; the docstring
  enumerates `"sent" | "failed" | "suppressed"` today. Update
  to `"sent" | "failed" | "skipped"` (clean break — no alias).

### Documentation

- `docs/notifications.md` — section retitled to runtime flag;
  remove the per-validation YAML example; add the `--skip-
  renotification` CLI form + retry/replay use case.
- `docs/configuration.md` — drop the `suppress_repeat_alerts`
  YAML field documentation.
- `docs/programmatic_api.md` — flag added with example.
- `docs/CHANGELOG.md` — Breaking entry: per-validation flag
  removed; new runtime flag default-False (regret-free for
  one-off runs, opt-in for replays).

## Acceptance Criteria

- AC1: `suppress_repeat_alerts` does not appear anywhere in
  `qualifire/`, `tests/`, or `docs/` **as a live config field
  or runtime read**. The only allowed mentions are: (i) the
  `_REMOVED_FIELDS_GUIDANCE` dict in `qualifire/core/config.py`
  that emits the loud-fail message (codex R2 MAJOR /
  resolved-AC8); (ii) the rejection-test YAML in
  `tests/test_skip_renotification.py`; (iii) historical mentions
  in shipped feature plans under `docs/features/`. Note: AC1 and
  AC8 are mutually consistent under this narrowing — codex R3
  MAJOR.
- AC2: `skip_renotification` flag plumbed through all five
  entry points (`run_config`, `run_config_parsed`, `backfill`,
  `write_audit_publish`, CLI).
- AC3: `QualifireContext.skip_renotification` attribute exists
  and is read by the engine.
- AC4: Synthetic forensic row uses `notification_status='skipped'`
  when the gate fires; channel column stays `*`.
- AC5: Default behavior (`skip_renotification=False`) is "always
  dispatch matching VRs"; the snapshot is **NOT** pre-fetched in
  this case (codex round 2 MEDIUM — single source of truth: the
  bulk-read short-circuits to `{}` when the flag is False; tests
  must assert `read_validation_history_bulk` is not called in
  the default path).
- AC6: Tests cover (a) gate fires when prior matching severity
  exists, (b) gate doesn't fire when default-False, (c)
  partial-key suppression still dispatches the un-seen rows,
  (d) backfill replay with the flag set produces zero alerts.
- AC7: Full test suite green. The old field has zero test
  references (audit confirmed) so no removals are needed; only
  the new tests above are added.
- AC8: Loud-fail on stale YAML — a YAML carrying
  `suppress_repeat_alerts: true` raises `ValueError` at config
  load time with guidance pointing at the new runtime flag
  (codex R2 MAJOR). Test: `test_yaml_with_removed_field_raises`
  in `tests/test_skip_renotification.py`.

## Risks and Mitigations

| Risk | Mitigation |
|------|------------|
| Default flip surprises operators on retries | Explicit Breaking entry in CHANGELOG; the new flag name is self-documenting. Pre-release (no external users) makes the flip safe. |
| Tests with hardcoded `suppress_repeat_alerts=False` (config-level opt-out) lose intent on rename | Plan-time grep audit; rewrite each call site against the new runtime flag where the test still has meaning, delete where the flag's absence makes the test redundant. |
| `_prefetch_suppression_snapshot` becomes dead code when default is False | Gate the pre-fetch on the runtime flag (codex R2 MEDIUM): when `skip_renotification=False`, the helper short-circuits to `{}` without hitting storage. AC5 + the engine section + the test matrix all agree the read is skipped. |
| Engine-warning suppression default flips too | This is intentional — re-paging on infra failures is the safer default. Documented in CHANGELOG. |

## Out-of-Band Reviews

- 2 adversarial plan reviews (self-critique).
- 2 codex plan reviews via `codex:codex-rescue` → iterate to PASS.
- Then implement; 2 adversarial impl reviews + 2 codex impl reviews.

## Effort

Small. Estimated diffs: ~150 net LOC across `core/config.py`
(field removals), `core/engine.py` (gate consolidation +
`'skipped'` rename + flag plumbing), `core/api.py`,
`core/context.py`, `cli.py`, and ~5 test files. Plus docs.

## Plan Iteration Log

- v1: initial draft.
- v2: addressed adversarial round 1 — fixed AC7 (no old tests
  exist; only additions needed); pinned `skip_if_cached`-mirror
  plumbing pattern with file-by-file change list; specified the
  exact test sites to add.
- v3: addressed adversarial round 2 — gate
  `_prefetch_suppression_snapshot` on the runtime flag (avoid a
  wasted bulk-read on every default-False run; verified via
  engine.py:475 that the snapshot is only consumed inside
  `_send_grouped_notifications`).
- v6: addressed codex plan-review round 3 — narrowed AC1 to
  exempt the `_REMOVED_FIELDS_GUIDANCE` dict in config.py and
  the rejection-test YAML (the AC1-vs-AC8 contradiction codex
  flagged). Codex round 4 PASS.
- v5: addressed codex plan-review round 2 —
  (a) MAJOR: added shared `_reject_removed_fields` model_validator
  on the 6 affected config classes so a stale YAML
  `suppress_repeat_alerts: true` raises ValueError instead of
  silently dropping; new AC8 + test;
  (b) MEDIUM: AC5 now agrees with the engine section and the
  risk table — `_prefetch_suppression_snapshot` short-circuits
  to `{}` when default-False, with an explicit test asserting
  `read_validation_history_bulk` is not called.
- v4: addressed codex plan-review round 1 —
  (a) BLOCKER: `qualifire/core/backfill.py` `run_backfill` /
  `_process_anchor` / `_run_anchor_once` need `skip_renotification`
  threaded through their internal signatures (the API-level
  forwarding stops short of the per-anchor engine setup at line
  588);
  (b) MAJOR: `Qualifire.validate()` (api.py:915) and
  `Qualifire.validate_query()` (api.py:1017) are also public
  entry points that build a `QualifireContext` and must accept
  the new flag;
  (c) MEDIUM: existing CLI plumbing test at
  `tests/test_cli.py:748` defines a fake `run_config_parsed`
  with hardcoded `skip_if_cached` only — needs
  `skip_renotification` added or the new flag will trigger a
  TypeError there;
  (d) LOW: `NotificationResult.status` enum comment at
  `qualifire/core/models.py:181` enumerates `"suppressed"` —
  rename to `"skipped"`.
