# Plan: cleanup-backcompat-shims

## Goal

Delete every backwards-compat shim surfaced by the
2026-05-11 audit. Pre-release with no external users —
no aliases, no dual-shape acceptance, no review-round
narrative comments. Code stays readable + every dual path
collapses to one shape.

## Locked Decisions

### Shims to delete

1. **`_REMOVED_FIELDS_GUIDANCE` + `_reject_removed_fields`**
   (config.py:20-46 + 6 `_check_removed_fields` attach sites
   at lines 744, 804, 885, 941, 1008, 1181).
   - Delete the dict, the validator, all 6 attach lines.
   - Delete `tests/test_skip_renotification.py:249-289`
     (`test_loud_fail_on_stale_yaml_key` test cluster T5).
   - Pydantic v2 default `extra="ignore"` will silently
     drop unknown keys. That's acceptable pre-release.

2. **`_reject_legacy_backend_field`**
   (config.py:1624-1651).
   - Delete the validator + its attach line.
   - Delete `tests/test_config.py:104-135`
     (`test_rejects_legacy_backend_field` +
     `test_legacy_backend_via_load_config`).
   - Delete `tests/test_api_from_config.py:612-650`
     (`TestSchemaMigration` class).

3. **`CustomQueryCollectionConfig._reject_nonempty_filter`**
   (config.py:483-527).
   - Delete the entire validator including the 19-line
     "Evolution across review rounds" docstring.
   - Pydantic's `extra="ignore"` (default) drops the stray
     `filter:` key silently. Update
     `docs/configuration.md` `### CustomQueryCollectionConfig
     field reference` pitfall to say "`filter:` is silently
     ignored (no GROUP BY / WHERE wrapper for
     custom_query); put predicates inside `sql`'s WHERE
     clause" instead of quoting the now-removed migration
     error.
   - Delete any test that exercises the rejection
     specifically (`tests/test_custom_query_filter_removed.py`
     hard-fail assertions).

4. **`expected_value_compact: bool = True`**
   (config.py:792-802) + engine branch
   (engine.py:1711).
   - Delete the field; always emit compact form.
   - Delete the dual-branch in engine.py — always call
     `model_dump(exclude_none=True)`.
   - Delete `tests/test_validation/test_threshold.py:126-160`
     (`test_expected_value_compact_can_be_disabled`).
   - Keep `test_expected_value_compact_via_engine`
     (line 76) but rename to drop the suffix.

5. **Bare-number threshold bounds in drift / forecast**
   (interpretation branch in historical validator).
   - Drop the bare-number → symmetric interpretation;
     require `{min, max}` dict form always.
   - Update `HistoricalThresholds` model
     (config.py:590-612): change
     `dict[str, float | dict[str, float]]` to
     `dict[str, dict[str, float]]`.
   - Update the docstring (config.py:593-609) to drop
     "Two accepted value forms" → "One accepted form (signed
     min/max dict)".
   - Delete `tests/test_signed_drift.py:94-109`
     (`test_legacy_bare_number_symmetric`).
   - Update `README.md:18, 181` to remove "legacy
     bare-number (symmetric absolute)" wording.

6. **Storage `COALESCE(is_active, 'true')` defensive path
   + `record_type` NULL tolerance**.
   - VERIFIED before locking: every current write path
     in `qualifire/core/engine.py:2262, 2420, 2462,
     2502, 2273, 2435, 2473, 2513` and
     `qualifire/core/deactivate.py:341, 352` sets BOTH
     `record_type` AND `is_active` explicitly. No live
     writer omits either column.
   - Delete the COALESCE wrapper on every
     `is_active` read in
     `qualifire/storage/{sqlite_storage,delta_storage,jdbc_storage,external_catalog}.py` (note the irregular naming: 3 backends carry the `_storage` suffix, `external_catalog.py` does not).
     Use plain `is_active = 'true'` instead.
   - Delete the `record_type` NULL-tolerance code paths
     in sqlite reads if present (verify during impl).
   - Delete `tests/test_storage/test_sqlite.py:363-387`
     (`test_legacy_rows_without_record_type`).
   - Delete `tests/test_storage/test_soft_delete_sqlite.py:113-119`
     (`test_legacy_null_is_active_treated_active`).
   - Update `docs/architecture.md` §1 "Storage read pattern":
     drop "`COALESCE(is_active, 'true') = 'true'`"
     references → just `is_active = 'true'`.

7. **Bare-number narrative in `validation/historical.py:215-217`**.
   - Comment block explicitly cites "Bare-number bounds...
     keep the symmetric (absolute) interpretation for
     back-compat." Once item 5 deletes the dual-shape
     support, this comment becomes incorrect.
   - Delete the comment + the symmetric-interpretation
     code path it documents.

8. **"backwards compat" phrasing in `core/backfill.py:25-30`**.
   - Comment narrates that `_SAFE_NAME_RE` accepts leading
     digits / dots / hyphens "for backwards compat with
     explicitly-set `WAPConfig.partition_column` values".
   - Strip the "backwards compat" phrasing only; KEEP the
     regex itself (broad blast radius — used across many
     config validators at config.py:737). Rewrite as
     "Stricter than `_SAFE_NAME_RE` in core/config.py
     (which permits leading digits, dots, and hyphens for
     identifiers like partition columns)."

9. **`api.py:412` `warnings.warn(QualifireReinitWarning)`**.
   - Verified during plan review: NOT a backcompat shim —
   it's an operator-facing runtime signal for intentional
   storage swaps on `Qualifire(...)` reinit. KEEP as-is.
   Listed here so the AC4 grep audit explicitly excludes
   it.

10. **Stale review-history comments** in production code
   (NOT in `docs/features/*` plan/shipped files — those
   stay verbatim as PR records).
   - Strip `Round-N`, `Codex round X`, `pre-2026-05-09`,
     `Round-6 softened... round-8 restores` narrative from
     ALL of these (codex plan-review R1 BLOCKER 5 expanded
     this list):
     - `qualifire/api.py:58, 502, 1097, 1609`
     - `qualifire/core/config.py:261, 326, 352, 495-511,
       801, 1053, 1760-1761, 1826`
     - `qualifire/core/engine.py:749-752, 893,
       1129-1134, 1296`
     - `qualifire/core/secrets.py:222, 389, 421`
     - `qualifire/notification/_redact.py:33, 76`
     - `qualifire/reporting/health.py:23`
     - `qualifire/reporting/html_report.py:972`
     - `qualifire/reporting/_snapshot_details.py:42, 54`
     - `qualifire/validation/_encoding.py:93, 112, 141`
     - `qualifire/validation/pattern_check.py:342, 370, 462`
     - `qualifire/storage/external_catalog.py:33`
   - Rule for each: if the comment explains a NON-OBVIOUS
     invariant (concurrency contract, race-condition fix,
     locking order) → KEEP the substantive WHY, drop the
     "Round X / Codex flagged" phrasing only. If the
     comment is just "this was added by Codex round N to
     fix bug Y" with the fix obvious from the code →
     DELETE entirely. Judgment call per site.

### Out of scope

- The CHANGELOG `Unreleased` entries describing
  past-feature migrations (e.g. skip-recollection's
  "rename from skip_if_cached") stay verbatim. CHANGELOG
  is intentionally a historical record.
- Shipped/plan markdown files under `docs/features/*`
  stay verbatim (PR records, not live narrative).

## What Changes

Per the section above, in summary:
- `qualifire/core/config.py` — heaviest hit: ~80 LOC
  deleted across 7 shim removals + comment cleanup.
- `qualifire/core/engine.py` — 1 branch deletion +
  comment cleanup.
- `qualifire/storage/{sqlite_storage,delta_storage,jdbc_storage,external_catalog}.py` (note the irregular naming: 3 backends carry the `_storage` suffix, `external_catalog.py` does not)
  — `COALESCE(is_active, 'true')` → `is_active = 'true'`
  in 4 files; sqlite `record_type` NULL handling drops.
- 6 test files lose specific shim-exercising tests but
  retain the substantive coverage.
- `README.md`, `docs/configuration.md`,
  `docs/architecture.md` — small wording fixes (no longer
  teach the dual shape / migration recipe).
- `qualifire/{api,core/secrets,notification/_redact,
  reporting/*}.py` — comment cleanup only.

## Acceptance Criteria

### Diff-verifiable

- AC1: `qualifire/core/config.py` no longer contains
  `_REMOVED_FIELDS_GUIDANCE`, `_reject_removed_fields`,
  `_reject_legacy_backend_field`,
  `_reject_nonempty_filter`, or `expected_value_compact`.
  No `_check_removed_fields = ...` attach lines remain.
- AC2: `grep -rn "COALESCE(is_active" qualifire/` returns
  zero hits. `grep -rn "is_active = 'true'" qualifire/storage/`
  returns the expected hits (one per backend per read
  query that previously had the COALESCE).
- AC3: `HistoricalThresholds.{warning,error}` field type
  is `dict[str, dict[str, float]] | None` (no `float |
  dict[str, float]` union). README and configuration.md
  no longer use the word "legacy" in the threshold
  bounds context.
- AC4: Across the 11 files listed in item 10 (and only
  those files — substantive WHY comments elsewhere may
  still legitimately reference rounds in passing if a
  reviewer judges the narrative load-bearing), the
  following greps return ZERO hits each:
  - `grep -rEn "Codex round|codex round" <files>`
  - `grep -rEn "[Rr]ound[- ][0-9]+" <files>`
  - `grep -rEn "pre-2026" <files>`
  Stripped variants ("this avoids the race that occurs
  when...") are allowed because they no longer contain
  the banned phrasing.
- AC5: Deleted test names: `test_loud_fail_on_stale_yaml_key`
  (T5 in skip_renotification), `test_rejects_legacy_backend_field`,
  `test_legacy_backend_via_load_config`,
  `TestSchemaMigration::test_legacy_backend_field_errors`,
  `test_expected_value_compact_can_be_disabled`,
  `test_legacy_bare_number_symmetric`,
  `test_legacy_rows_without_record_type`,
  `test_legacy_null_is_active_treated_active`. Confirmed
  by `grep` on the relevant test files returning zero.

### Runtime-gate

- AC-Run-1: `pytest` full suite passes. Expected count
  drops by ~8 tests (the deletions); no NEW failures.
- AC-Run-2: `pytest tests/test_docs_links.py` passes
  (the doc edits don't introduce broken links).
- AC-Run-3: Codex impl review approves the sweep against
  the user's explicit "no-backcompat" policy.

## Risks and Mitigations

| Risk | Mitigation |
|------|------------|
| Deleting `COALESCE(is_active, 'true')` exposes a latent bug where SOME writer forgets to set the column | Pre-impl verification (DONE) confirmed every writer sets it. If impl finds a writer missing the column, add the column to that writer rather than re-introducing COALESCE. |
| Pydantic `extra="ignore"` silently drops typo'd keys (no longer caught by `_REMOVED_FIELDS_GUIDANCE`) | Accepted trade-off per user's "pre-release, keep it simple" policy. If typo-detection becomes a problem post-release, revisit with `model_config = ConfigDict(extra="forbid")` site-wide. Not this feature's scope. |
| Stripping review-history comments removes context a future maintainer might want | Substantive WHY-this-is-non-obvious comments stay; only the "Round X / Codex flagged" phrasing goes. CHANGELOG + git log remain authoritative for "why was this change made." |
| Bare-number threshold deletion breaks an internal example config | Grep all `tests/`, `examples/`, `tests/manual/`, `tests/integration/` for `deviation_pct: \d+$` shape before deletion; convert any matches to dict form. |

## Out-of-Band Reviews

- 1 codex plan review (this feature is policy-driven so
  the plan-review focus is "did we miss a shim or
  mis-classify something as not-shim?").
- 1-2 codex impl reviews (focus on "did the deletion miss
  a call site or break a non-obvious invariant?").

## Effort

Medium. Estimated ~250 LOC removed, ~30 LOC modified
(error-message text, doc text), 6-8 test deletions, and
~10-15 comment cleanups.

## Plan Iteration Log

- v1: initial draft.
- v2: codex plan-review R1 raised 3 BLOCKERs.
  - Wrong storage filename `external_catalog_storage.py` →
    `external_catalog.py` (no `_storage` suffix on this one
    backend). Fixed in items 6 + What Changes.
  - 3 missed shim hits: `validation/historical.py:215-217`
    bare-number back-compat comment (added as item 7);
    `core/backfill.py:25-30` "backwards compat" phrasing
    on the `_SAFE_NAME_RE` narrative (added as item 8);
    `api.py:412` `QualifireReinitWarning` is NOT a shim
    but explicitly excluded from AC4 (added as item 9).
  - AC4 grep was too broad — would fail its own check
    after deletions. Tightened scope to the 11 named
    files and split into 3 specific phrase greps so
    stripped variants pass.
  - Item 10 file list expanded to add
    `validation/_encoding.py`,
    `validation/pattern_check.py`,
    `storage/external_catalog.py`,
    and `config.py:1053` per codex grep results.
