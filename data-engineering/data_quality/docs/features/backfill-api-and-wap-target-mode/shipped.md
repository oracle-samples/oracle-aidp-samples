---
shipped: 2026-05-09
---

# Shipped: Backfill API + WAP target-mode + config ergonomics

## Summary

Delivers the **full** scope of the feature in one PR, addressing
S2-S11 follow-ups deferred from `metrics-backfill-and-soft-delete`
plus the four WAP-design ergonomics items (sub-features A-D in
`idea.md`) and the `WAPConfig.partition_column` field.

The PR went through **2 adversarial self-passes + 2 codex
plan-review rounds** for the plan, then 5 codex impl-review rounds
for the implementation (R1 FAIL → R2 FAIL → R3 FAIL → R4 FAIL → R5
PASS). Each round identified a single concrete MUST-FIX which was
addressed before the next round. Final test count: 1205 tests
green on the branch.

## Key Changes (sub-feature → commit)

| ID | Sub-feature | Commits |
|----|-------------|---------|
| **A** | Dict-form `expressions:` everywhere; drops `<expr> AS <name>` lexer | `40ad9a2` |
| **B** | `WAPConfig.write_sql` → `sql`; add `sql_file` PrivateAttr-resolved field; three-way mutual exclusion | `effc6bf` |
| **C** | `_run_wap` propagates dataset-level fields into staging audit config | `baf37c0` |
| **D** | WAP publish via DataFrame writer + `allow_create=True` field; SQL fallback for Pandas backends | `2e086d8` |
| **E** | `Qualifire.backfill(...)` API + `BackfillReport` / `PartitionDiff` dataclasses (exported) | `6149d6e` |
| **F** | `Qualifire.deactivate_metric(...)` API + `qualifire/core/deactivate.py` (H1 read-back-and-bump) | `fcc7869` |
| **G** | CLI: `qualifire backfill`, `qualifire deactivate-metric`, `--skip-if-cached` flag on `run` | `12ce2c4` |
| **H** | Engine `_run_backfill` driver in `qualifire/core/backfill.py` + per-partition source select | `6149d6e` |
| **I** | `qualifire/core/selectors.py` — selector grammar parser + `ScopedDataset` resolver | `4e556b4` |
| **J** | `skip_if_cached` engine pre-pass + `qualifire/core/backfill_eligibility.py` + `expected_metrics()` | `9f05dc2` |
| **K** | `is_active` honoured on Delta / ExternalCatalog / JDBC reads + H6 `read_collection_metric_at_partition` on those backends | `dbb7e75` |
| **L** | `tests/test_backfill_e2e.py` — composite forward → deactivate → backfill flow | `fb4446a` |
| **M** | `docs/backfill_and_soft_delete.md` + `examples/backfill_quickstart.py` + cross-link updates | `fb4446a` |
| **WAP-pc** | `WAPConfig.partition_column` field with `_SAFE_NAME_RE` validator | `b88639c` |

Codex impl-review iteration commits: `da4ed4f`, `76c53c1`,
`a9d653b`, `20cd190`, `8f4bc4d`.

## Files Changed

68 files, +14,000 / -540. Major files:

- `qualifire/api.py` — `Qualifire.backfill`, `Qualifire.deactivate_metric`, `skip_if_cached` plumbed through `validate` / `run_config` / `run_config_parsed`.
- `qualifire/cli.py` — new `backfill` / `deactivate-metric` subcommands; `--skip-if-cached` on `run`.
- `qualifire/core/backfill.py` (new) — engine loop driver + per-partition source select.
- `qualifire/core/backfill_eligibility.py` (new) — `cache_eligible` predicate.
- `qualifire/core/backfill_report.py` (new) — `BackfillReport` + `PartitionDiff`.
- `qualifire/core/deactivate.py` (new) — `deactivate_metric` H1 helper.
- `qualifire/core/selectors.py` (new) — selector grammar.
- `qualifire/core/config.py` — `expressions: dict[str,str]` rename, `WAPConfig.sql/sql_file/partition_column/allow_create`, `expected_metrics()` on validators.
- `qualifire/core/context.py` — `skip_if_cached` field.
- `qualifire/core/engine.py` — `_run_wap` propagation, `_try_skip_if_cached` pre-pass, `_tag_with_backfill` row stamper, `_json_safe_or_str` coercion.
- `qualifire/storage/{delta,external_catalog,jdbc}_storage.py` — H1/H2/H6/H8 invariant rewrites; new H6 method on each backend.
- `qualifire/wap/pattern.py` — DataFrame-writer publish + `allow_create` + `WAPPublishError`.
- All YAMLs in `docs/examples/`, `examples/industries/*/`, `tests/manual/configs/` — `expressions:` list-form → dict-form; `write_sql:` → `sql:`.
- `tests/` — 60+ test files updated; 9 new test modules:
  - `test_aggregation_dict_form.py`
  - `test_backfill.py`
  - `test_backfill_e2e.py`
  - `test_cli_backfill.py`
  - `test_deactivate_metric.py`
  - `test_selectors.py`
  - `test_skip_if_cached.py`
  - `test_storage/test_*` (storage parity tests embedded in existing)
  - `test_wap_config_sql.py`
- `docs/backfill_and_soft_delete.md` (new) — operator-facing reference.
- `examples/backfill_quickstart.py` (new) — runnable on SQLite (no Spark needed).

## Testing

- **1205 unit tests** passing locally (`pytest -x -q`) before merge.
- `examples/backfill_quickstart.py` runs end-to-end on SQLite +
  PandasBackend with no external dependencies.
- E2E composite test in `tests/test_backfill_e2e.py` exercises the
  full forward → deactivate → backfill → re-deactivate flow,
  including `soft_delete_prior=True` and the `collected_via='backfill'`
  marker contract.
- WAP changes (D, B, C) covered by `tests/test_wap.py` + new
  `test_wap_config_sql.py` (sql_file PrivateAttr semantics, 3-way
  mutual exclusion, env-var resolution, model-dump round-trip).
- Storage parity (K) tested on the existing real-Spark fixtures
  (`test_external_catalog_real.py`, `test_jdbc_bucketing_real.py`).

## Notes

### Plan-vs-implementation deltas (codex iteration history)

Five impl-review rounds. Each round closed one concrete contract gap:

- **R1 FAIL** → 3 MUST-FIX: skip_reason canonicalize to
  `"no_historical_samples"`, selector with metric segment must
  narrow the engine's validation set (not just the report diffs),
  `Qualifire.backfill` must raise `QualifireValidationError` with
  `.report` attached on `report.has_errors`.
- **R2 FAIL** → 1 MUST-FIX: `details_json.collected_via='backfill'`
  was promised by plan N12/D12 but never actually emitted on rows.
  Fixed via `_tag_with_backfill` helper called from row builders
  when `context.backfill=True`.
- **R3 FAIL** → 1 MUST-FIX: `_tag_with_backfill` only tagged dict
  payloads; string / JSON-string / list / scalar payloads slipped
  through untagged. Fixed by parsing JSON strings, wrapping
  non-decodable payloads under `raw_details`.
- **R4 FAIL** → 1 regression introduced by R3: wrapping a non-JSON-
  serializable payload (e.g., `datetime`) under `raw_details` broke
  `json.dumps` in the persistence layer. Fixed via `_json_safe_or_str`
  coercion before wrapping.
- **R5 PASS** — all rounds' fixes confirmed; no remaining MUST-FIX.

### Foundation-doc-vs-code drift

`metrics-backfill-and-soft-delete/shipped.md` claimed certain
scaffolds shipped that were not actually present in `main`:
`backfill_eligibility.py`, `expected_metrics()` resolver,
`skip_if_cached` API kwarg, `_add_backfill_tag` helper. This PR
created them as part of Step 6 / Step 10. A small follow-up commit
to fix the foundation's `shipped.md` drift is left as a future
docs-only PR.

### Breaking changes (operator-visible)

Two clean-break renames bundled into this PR (foundation
decision pin #5: no shim layers):

- **`WAPConfig.write_sql` → `WAPConfig.sql`** (sub-feature B).
  ~110 source occurrences migrated. New `sql_file` field reads
  the SQL from a file at config-load time. Migration:
  ```bash
  sed -i 's/write_sql:/sql:/g' your-config.yaml
  ```
- **`AggregationCollectionConfig.expressions: list[str]` → `dict[str, str]`**
  (sub-feature A). 127 list-form construction sites + ~30 YAMLs
  migrated. List-form raises a precise migration hint.

Both breaks are operator-visible. Release notes / migration recipe
captured in `docs/backfill_and_soft_delete.md` and the WAP
documentation in `docs/wap_pattern.md`.

### Out-of-scope (genuine future work)

Documented in the plan; not blockers for this ship:

- Backfill `--parallelism N` flag (sequential by default; future
  performance optimization).
- `WAPConfig.partition_column` auto-detection from `partition_ts`
  expression (always explicit in v1).
- Multi-row `read_validation_history_bulk(limit > 1)` under
  soft-delete (foundation pinned this as `ValueError(limit>1)`;
  the multi-row tombstone-aware walk is out of scope).

## PR

[#13](https://github.com/amitranjan-oracle/qualifire/pull/13) —
`feat(backfill-api-and-wap-target-mode): plan + implementation (full A-M scope)`
