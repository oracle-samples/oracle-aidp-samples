---
shipped: 2026-05-11
---

# Shipped: cleanup-backcompat-shims

## Summary

Deletes every migration shim, dual-shape acceptance path, and
review-history narrative comment surfaced by the 2026-05-11
audit. qualifire is pre-release with no external users; no
backcompat plumbing should exist anywhere.

## Key Changes

- **`qualifire/core/config.py`** — 7 shims gone:
  - `_REMOVED_FIELDS_GUIDANCE` + `_reject_removed_fields` + 6
    `_check_removed_fields = ...` attach sites (caught stale
    `suppress_repeat_alerts:` YAML key with a custom error;
    Pydantic's default `extra="ignore"` now drops the key
    silently).
  - `_reject_legacy_backend_field` (caught the old top-level
    `backend:` YAML key).
  - `CustomQueryCollectionConfig._reject_nonempty_filter`
    plus its 19-line "Evolution across review rounds"
    docstring narrative.
  - `expected_value_compact: bool = True` flag — engine now
    always emits the compact `expected_value` shape.
  - `HistoricalThresholds.{warning,error}` type tightened
    from `dict[str, float | dict[str, float]] | None` to
    `dict[str, dict[str, float]] | None`. Bare-number
    symmetric-interpretation branch in `HistoricalValidator`
    deleted.
  - "Migration recipe (operators upgrading)" YAML examples
    + `_validate_expressions`'s "(sub-feature A)" migration
    error text.

- **`qualifire/validation/historical.py`** — bare-number
  back-compat comment + interpretation branch removed.
  `_first_violation` now requires the `{min, max}` dict shape.

- **`qualifire/storage/{sqlite,delta,jdbc,external_catalog}.py`**:
  - All `COALESCE(is_active, 'true') = 'true'` SQL reads
    replaced with plain `is_active = 'true'`.
  - Six `F.coalesce(F.col("is_active"), F.lit("true"))`
    Spark-side expressions in `jdbc_storage.py` collapsed
    to `F.col("is_active") == F.lit("true")`.
  - `write_results` on all 4 backends defaults
    `is_active="true"` when the writer omits it, so newly-
    written rows never carry NULL in that column.
  - Three legacy re-export aliases removed (`_spark_dim_predicate`
    / `_jdbc_dim_predicate` import-as shims); all call sites
    use the canonical names from `qualifire/storage/_predicates`.

- **`qualifire/core/backfill.py`** — "backwards compat"
  phrasing on the `_SAFE_NAME_RE` narrative removed; regex
  itself unchanged.

- **`qualifire/core/deactivate.py`** — module docstring
  updated to drop the COALESCE reference + the "loud-fail
  on legacy tables" paragraph.

- **Review-history narrative** — `Round-N` / `Codex round X`
  / `pre-2026` phrasing stripped from 11 production files
  (`api.py`, `core/{config,engine,secrets}.py`,
  `notification/_redact.py`, `reporting/{health,html_report,
  _snapshot_details}.py`, `validation/{_encoding,pattern_check}.py`,
  `storage/external_catalog.py`). Substantive WHY comments
  preserved; only the round-labelling phrasing went.

- **Tests** — 8 named legacy-shim tests deleted
  (`test_legacy_bare_number_symmetric`,
  `test_legacy_rows_without_record_type`,
  `test_legacy_null_is_active_treated_active`,
  `test_legacy_backend_field_errors`,
  `TestRemovedFieldRejection`,
  `test_expected_value_compact_can_be_disabled`,
  `test_run_exits_cleanly_on_legacy_custom_query_filter`,
  `test_report_exits_cleanly_on_legacy_custom_query_filter`).
  The dedicated `tests/test_custom_query_filter_removed.py`
  file deleted. `HistoricalThresholds` bare-number usages
  across `tests/` + `examples/industries/*/config.yaml`
  converted to `{min, max}` dict form.

- **`docs/configuration.md`** — `HistoricalThresholds`
  type signature + worked examples updated; CustomQuery
  `filter:` pitfall now describes silent-drop semantics
  (Pydantic `extra="ignore"`) instead of the deleted
  rejection error.

- **`docs/architecture.md`** — §1 storage-read narrative
  no longer references `COALESCE(is_active, 'true')`.

- **`docs/CHANGELOG.md`** — Unreleased / Breaking entry
  documenting the cleanup.

- **`docs/programmatic_api.md`** — pre-existing broken link
  to `tests/manual/local/dashboard_charts.ipynb` retargeted
  to the canonical `tests/manual/dashboard_charts.ipynb`
  path (drive-by fix unblocking docs-lint).

- **`README.md`** — "legacy bare-number (symmetric
  absolute)" wording removed; threshold form now described
  as `{min, max}` only.

## Files Changed

~55 files; +320 / -780 lines (excluding plan.md / shipped.md).

## Plan PR

[#33](https://github.com/amitranjan-oracle/qualifire/pull/33).

## Review Cycles

Plan: 3 codex rounds.
- R1: 3 BLOCKERs — wrong storage filename
  (`external_catalog_storage.py` doesn't exist;
  `external_catalog.py` is the actual file); 3 missed shim
  hits (`validation/historical.py` bare-number comment,
  `core/backfill.py` "backwards compat" phrasing, and
  `api.py` reinit warning verified NOT a shim); AC4 grep
  was too broad and would fail its own check after
  deletions.
- R2: 1 BLOCKER — 12-vs-11 file-count discrepancy between
  AC4 and item 10's enumeration.
- R3 PASS at v3.

Implementation: 2 codex rounds, **3 factual issues caught
before merge**.
- R1: 3 BLOCKERs —
  - Two stale doc references to the deleted
    `_reject_nonempty_filter` in `config.py:52` and `:1684`.
  - `docs/configuration.md:553-565` still teaching the
    deleted bare-number `HistoricalThresholds` shape.
  - 6 Spark-side `F.coalesce(F.col("is_active"), F.lit("true"))`
    survivors in `jdbc_storage.py` + 3 legacy re-export
    aliases (`_spark_dim_predicate` / `_jdbc_dim_predicate`)
    in `delta_storage.py`, `jdbc_storage.py`,
    `external_catalog.py`.
- R2 PASS.

## Local Test Results

- Full pytest: **1497 passed, 2 skipped, 0 failures**.
- `tests/test_docs_links.py` passes (after fixing one
  pre-existing broken link in `docs/programmatic_api.md`).
- Net test delta: −8 deletable tests removed; surviving
  tests still cover the same correctness invariants under
  the new shapes.
