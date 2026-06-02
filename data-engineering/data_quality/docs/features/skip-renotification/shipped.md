---
shipped: 2026-05-10
---

# Shipped: skip-renotification — Runtime Kill Switch for Duplicate Alerts

## Summary

Replaces the per-validation `suppress_repeat_alerts: bool = True`
config flag with a runtime `skip_renotification: bool = False` flag
plumbed through every entry point: `Qualifire.run_config`,
`run_config_parsed`, `backfill`, `validate`, `validate_query`,
`write_audit_publish`, plus `qualifire run --skip-renotification` /
`qualifire backfill --skip-renotification` on the CLI. Same
snapshot-based duplicate-alert mechanism; the trigger moves from
per-validation YAML to a single opt-in runtime kill switch.
**Default flips** from "always suppress per-validation" to "always
dispatch unless opted in" — retries / replays re-page by default,
operators set the flag for backfill replays / CI rehearsals.

## Key Changes

- **Config field removal** —
  `qualifire/core/config.py`: dropped
  `suppress_repeat_alerts: bool = True` from
  `SLOValidationConfig`, `ThresholdValidationConfig`,
  `HistoricalValidationConfig`, `ForecastValidationConfig`,
  `AnomalyDetectionValidationConfig`,
  `PatternValidationConfig`. Added shared
  `_REMOVED_FIELDS_GUIDANCE` dict + `_reject_removed_fields`
  helper + `model_validator(mode="before")` wiring on each of
  the 6 affected classes — a stale YAML key raises `ValueError`
  with guidance pointing at the new runtime flag.

- **Engine gate consolidation** —
  `qualifire/core/engine.py`: dispatch loop reads
  `self.context.skip_renotification` instead of per-validation
  `getattr(val_config, "suppress_repeat_alerts", True)`.
  Removed `suppress_repeat_alerts` from `_SyntheticEngineVC`
  and `_SyntheticVC`. Synthetic forensic row renamed
  `'suppressed'` → `'skipped'`.
  `_prefetch_suppression_snapshot` short-circuits to `{}` when
  the flag is False — saves a per-run bulk-read against the
  system table on every default invocation.

- **Public API plumbing** —
  `qualifire/api.py`: `skip_renotification: bool = False`
  added to all 6 entry points; `ctx.skip_renotification`
  setter mirrors the existing `ctx.skip_if_cached` pattern.
  `qualifire/core/backfill.py`: threaded through `run_backfill`
  → `_process_anchor` → `_run_anchor_once`. Both the serial
  call site **and** the parallel `ThreadPoolExecutor.submit`
  call site (codex impl-review R1 MAJOR — initial commit
  silently dropped the flag for parallel workers).
  `qualifire/cli.py`: `--skip-renotification` argparse flag
  added to `run` and `backfill` subcommands.

- **Models** —
  `qualifire/core/models.py:181`: `NotificationResult.status`
  doc enumerates `"sent" | "failed" | "skipped"` (was
  `"suppressed"`).

- **Tests** — new
  `tests/test_skip_renotification.py` (7 tests):
  T1 (gate fires when prior matches), T2 (default-False
  dispatches despite prior history — pins the new default),
  T4×2 (prefetch skipped/runs based on flag), T5×2 (stale
  YAML key rejected via kwargs and dict paths), T6 (parallel
  backfill threads the flag to every worker).
  `tests/test_cli.py`: existing fake `run_config_parsed`
  stub at line 748 updated.

- **Documentation** —
  `docs/notifications.md`: rewritten to document the runtime
  flag instead of the per-validation YAML field.
  `docs/configuration.md`: per-validation example replaced
  with a pointer to the runtime flag.
  `docs/CHANGELOG.md`: Breaking entry under Unreleased.

## Files Changed

12 files; +427 / −106 lines.

## Plan PR

[#21](https://github.com/amitranjan-oracle/qualifire/pull/21).

## Review Cycles

Plan: 2 adversarial + 4 codex rounds. Codex R1 found a BLOCKER
(backfill internal plumbing) + a MAJOR (validate / validate_query
entry points missed) + a MEDIUM + a LOW; R2 found a MAJOR (silent
YAML key drop) + a MEDIUM (snapshot prefetch contradiction); R3
found a MAJOR (AC1-vs-AC8 contradiction); R4 PASS at v6.

Implementation: 2 adversarial + 2 codex rounds. Codex R1 found a
MAJOR — `qualifire/core/backfill.py:300` parallel
`ThreadPoolExecutor.submit` call dropped the flag silently;
serial path correctly threaded it through but the parallel path
didn't get the same fix from `replace_all` because of indent
differences. Fixed + new T6 test pinning per-worker context value.
Codex R2 PASS.

## Local Test Results

- 1476 passed, 2 skipped (unchanged from baseline + 7 new tests).
- No regressions; full suite green.
