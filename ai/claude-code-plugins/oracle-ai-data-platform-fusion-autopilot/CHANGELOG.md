# Changelog

All notable changes to this project are documented here.

## [Unreleased]

### Security
- The generated dispatch notebook no longer prints the length of the BICC password.
- The manual-install fallback installs from the plugin checkout instead of an
  unregistered PyPI name.

### Fixed — cluster-side runtime dependency gate (REST dispatch)
- **REST dispatch now validates its runtime dependency closure on the cluster
  before importing the orchestrator.** The generated dispatch notebook installs
  the plugin wheel with `pip install --no-deps` (deterministic offline path), so
  declared runtime deps (`pydantic`, `pyyaml`) were neither bundled nor checked;
  on a clean AIDP runtime the creds cell's `import orchestrator` would die with a
  raw `ImportError` **before** any pipeline or safety gate ran. The install cell
  now gates on the import-critical closure, best-effort installs any missing deps
  into the staged target (self-heals on clusters with a reachable index), and
  otherwise fails closed with a `DISPATCH_RUNTIME_DEPS_MISSING` diagnostic +
  remediation. New tests in `tests/unit/dispatch/test_notebook_builder.py`
  (`TestRuntimeDepPreflight`) cover clean-runtime fail-closed, self-heal, and
  present-runtime no-op paths.

### Added — COA semantic-role resolution (feature `coa-role-segment-resolution`)
- **COA role segments now resolve from explicit config, not column existence.**
  `coa_balancing_segment` / `coa_cost_center_segment` / `coa_natural_account_segment`
  are tagged `resolution: semanticRole` and resolved from
  `profile.chartOfAccounts` (physical column **names**) via a fail-closed ladder
  (config → `--refresh` carry-forward → legacy back-derive → accepted convention →
  fail closed `AIDPF-2013`), with honest **per-role** provenance — never
  `auto_resolve`. This fixes a silent correctness bug where every tenant was pinned
  to `Segment1/2/3` regardless of its actual chart-of-accounts layout.
- **`{{ coa.<role> }}` renderer token + `$coa.<role>` required-column refs.**
  Single-COA renders a bare column; multi-COA renders a per-row, **parameterized**
  `CASE` over `CodeCombinationChartOfAccountsId` (chart-id WHEN values are bound
  params, never inlined). `$coa.<role>` expands to the union of arm columns.
- **Preflight COA gate (validate-only, per `chart_of_accounts_id`):** Tier A
  union-existence + per-arm distinctness; Tier B natural-account↔account_type
  contradiction (sample-floor guarded); **multi-COA fail-closed gate** (`AIDPF-2018`)
  cleared by `--accept-singleton-coa` (persisted via bootstrap) or a `byChart` map.
- **`content-pack validate`** rejects a COA role modeled as a bare existence alias
  (`AIDPF-2014`) or bound outside the `gl_coa` bronze contract (`AIDPF-2015`); the
  `gl_coa` contract was extended to `Segment1–6`.
- **BEHAVIOR CHANGE:** a **non-interactive** bootstrap of a tenant with no explicit
  `profile.chartOfAccounts` now **fails closed** (`AIDPF-2013`) instead of silently
  defaulting — pass `--accept-coa-convention` to accept the pack default, or declare
  `chartOfAccounts`. A **multi-COA** tenant fails closed at `dim_account` preflight
  (cascade-skips only `gl_balance`; supplier marts still seed) until
  `--accept-singleton-coa` or `byChart` is provided.
- **Remediation for existing pinned profiles:** add/edit
  `profile.chartOfAccounts` (or accept the convention), run `bootstrap --refresh`
  (legacy pins back-derive as `legacy_unverified` with a warning until verified),
  then reseed affected silver/gold. See **LIMITS.md §Resolved-CoaSemanticRole**.
- New tests: `test_coa_resolution`, `test_coa_gate`, `test_coa_validators`,
  `test_coa_renderer`, `test_coa_preflight_gate` (+ updated bootstrap/render suites).

### Added — COA depth via tenant overlay (Segment7–30)
- **Tenants with deep COA layouts (role at `CodeCombinationSegment7–30`) are now
  supported by a tenant overlay — no starter-pack fork.** An overlay extends the
  COA role candidate domain (`inherit` + deep segments) **and** the `gl_coa` bronze
  `outputSchema` (`extendColumns`) together; the tenant pins meaning in
  `profile.chartOfAccounts`; `bootstrap --refresh` derives `resolved.column.coa_*`.
  Ships `examples/coa-deep-overlay/` as a product sample (+ a profile fragment).
- **Validator domain cap:** COA role candidates must be `CodeCombinationSegment(1–30)`
  (Fusion GL flexfield max) — `Segment31`+/non-segment rejected (**AIDPF-2019**);
  binding a deep segment still requires the overlay to extend `gl_coa.outputSchema`
  (**AIDPF-2015**), so depth is gated by the contract, not a hardcoded 6.
- **medallion-author COA-depth mode** (`/medallion-author coa-depth --tenant <t>
  --segments 7-10`): drafts the coordinated overlay (candidates + `gl_coa.outputSchema`)
  + a `profile.chartOfAccounts` runbook fragment, from **operator input with no runtime
  diagnostic** (the AIDPF-2015 trigger writes none). Honest provenance: `trigger:
  operator_input` + `operatorInputId` (never a faked `diagnosticRunId`);
  `validate_overlay` now requires `diagnosticRunId` **XOR** `operatorInputId`.
- **Gate ordering:** a deep segment declared in the extended `outputSchema` but absent
  from the live PVO fails closed at the **`gl_coa` bronze** source gate (AIDPF-4071),
  before `dim_account` preflight; the `$coa.*` union existence check (AIDPF-2042) is the
  silver-side backstop. `gl_coa.requiredColumns` extension is deferred to
  `bronze-required-columns-overlay` (not load-bearing — the full PVO lands regardless).
- New tests: `test_coa_depth_overlay`, `test_coa_depth_drafter`.

### Security & correctness — maintainer review (2026-06-17, PR #4)
- **SQL-identifier allowlisting at pack-load (AIDPF-2082).** `RefreshIncremental.naturalKey` / `partitionColumns` / `trackedColumns` and `watermark.column` are now validated against `^[A-Za-z_][A-Za-z0-9_]*$` when a pack loads. These names interpolate unquoted into `MERGE ON` / partition / watermark SQL; validation rejects both injection and cryptic Spark parse errors from typos. Defense-in-depth checks were added at the MERGE-helper layer (`_natural_key_join_sql`, `_payload_diff_predicate_sql`), the bronze-extract adapter (live PVO column names), and the state schema-reconcile `ADD COLUMNS` path.
- **Calendar-date validation (AIDPF-2083).** `CalendarProfile.startDate`/`endDate` must be real ISO-8601 (`YYYY-MM-DD`) dates; `dim_calendar` builder re-validates at the SQL-builder layer so the tenant-override path (which bypasses Pydantic) can't interpolate raw strings into `sequence(DATE'...')`.
- **Resume identity-drift gate now fails closed.** `check_identity_drift` raises `ResumeRunNotResumableError` on a corrupt/unparseable plan snapshot instead of silently returning — the gate runs before any credential unwrap, so a non-resumable snapshot must block, not skip the check.
- **Supplier marts no longer over-count.** `gold.supplier_spend` and `gold.ap_aging` (and the `overlay-pack` example) now dedupe `dim_supplier` to one row per `vendor_id` inside the join, so a `vendor_id` shared across supplier numbers can't fan out invoices and inflate aggregates. No output change on clean (1:1) data.
- **`dim_account.code_combination` is now positionally stable.** Segments are `COALESCE(..., '')`-wrapped before `CONCAT_WS` so a NULL segment keeps its dotted position instead of shifting the key.
- **Retry helper docstring corrected.** `orchestrator/retry.py` no longer claims to be always-on; it documents that `run_with_retry` is available but not yet wired into the content-pack node loop (tracked follow-up).
- **Cross-platform fixes.** CLI forces UTF-8 stdout/stderr so rich status glyphs (`✓`, `→`) don't crash on a Windows `cp1252` console; the v1-registry transcription normalizes CRLF before blob-hashing and emits UTF-8; a few tests no longer depend on POSIX-only paths or the host `~/.oci` session token.
- **Removed an internal reference from `CONTRIBUTING.md`.** A branch-policy note referencing unrelated internal repos was dropped from this public doc.
- 18 regression tests added (`tests/unit/test_pr4_review_hardening.py`).

### Added — Phase 2 dataflow (towards v0.2.0)
- **2026-05-07 (P1.1) — `dimensions/dim_supplier.py`** — first conformed dimension shipped as a Python module. Reads `bronze.erp_suppliers`, dedupes on `SEGMENT1` (supplier_number), handles NULL/0 ID columns with `NULLIF(CAST(... AS BIGINT), 0)`, and writes `silver.dim_supplier` with bronze→silver audit lineage. Ships an `id_populated_pct(spark, column="vendor_id")` helper that downstream marts consult to pick between canonical join and spend-only fallback paths. 14 unit tests pass. **Live-validated** end-to-end on `fusion_autopilot_dev` AIDP cluster — see `tests/live/TC8b_dim_supplier_module_results.md`. Productizes (and corrects) TC8's prototype: TC8 misdiagnosed column case (`Segment1` → actually `SEGMENT1`) and overgeneralized "demo pod has masked IDs" — <fusion-pod-2> does, <fusion-pod> doesn't, and the new helper handles both.
- **2026-05-07 (P1.2) — `transforms/gold/supplier_spend.py`** — first gold mart shipped as a Python module. **Single LEFT-JOIN form** (`bronze.ap_invoices LEFT JOIN silver.dim_supplier`) groups on `CAST(inv.ApInvoicesVendorId AS BIGINT)` so every invoice dollar is preserved in the output regardless of whether its vendor is in the dim. Dim attributes (`supplier_number`, `supplier_name`, `business_relationship`) populate from the dim where the join matches; NULL otherwise. The single form handles both demo-shape (<fusion-pod-2>, dim `vendor_id` all NULL → all dim attrs NULL) and production-shape (<fusion-pod> or customer pod, dim `vendor_id` populated → dim attrs populated where matched) without a code-path branch. Includes regression tests (`TestNoSilentInvoiceDropContract`) that explicitly forbid INNER JOIN re-introduction or picker re-addition. 17 unit tests pass. **Live-validated**: TC8c reproduces TC8's $3.21B aggregate within 2% on <fusion-pod-2> (different demo pod, different supplier counts) — see `tests/live/TC8c_supplier_spend_module_results.md`. Top-5 vendor IDs match TC8 exactly. Production-shape live verification (with populated `vendor_id` in dim) remains pending — blocked on a pod with rotated/working creds.
- **2026-05-07 (P1.4) — `dimensions/dim_calendar.py`** — system-generated calendar dimension; no bronze source. Generates Gregorian + Fiscal calendars for a configurable date range (default 2020-2030, 4,018 days) via `sequence(DATE, DATE, INTERVAL 1 DAY) + EXPLODE`. Surrogate `calendar_key = YYYYMMDD as BIGINT` is deterministic from the date (stable across rebuilds). Configurable `fiscal_start_month` parameter handles calendar-year (default), Jul-Jun, Oct-Sep, etc. 16 unit tests pass. **Live-validated** at 100% — see `tests/live/TC21_dim_calendar_results.md`: 4018 rows, 0 gaps, 0 surrogate-key mismatches, leap day 2024-02-29 present, Saturday/Sunday correctly flagged. Required by `gold.gl_balance` (P1.8) and `gold.po_backlog` (P1.11).
- **2026-05-07 (P1.3) — `dimensions/dim_account.py`** — Chart of Accounts conformed dimension. Reads `bronze.gl_coa` (BICC `CodeCombinationExtractPVO`), dedupes on the natural CCID key (`CodeCombinationCodeCombinationId`), keeps the most recent extract per account, and writes `silver.dim_account` (20 columns) with bronze→silver audit lineage. Bronze convention is **PascalCase with `CodeCombination` prefix** (matches `ap_invoices`'s `ApInvoices` prefix; differs from `erp_suppliers`'s bare UPPERCASE) — third distinct convention encountered, all handled by per-PVO column-aware projections. Ships **6 named segment columns** (`company`, `cost_center`, `account`, `subaccount`, `product`, `intercompany`) by default; tenants needing segments 7–30 extend the SQL builder non-breakingly (Fusion's CoA supports up to 30). Surfaces `account_type`, `enabled_flag`, `summary_flag`, `detail_posting_allowed_flag`, `financial_category` so consumers (gold marts, GenAI prompts) decide what to filter — the dim doesn't impose a policy. 20 unit tests pass (covering dedupe, NULL CCID filter, BIGINT casts, CONCAT_WS for code_combination, all 6 segment aliases, all 5 flag/classification aliases, native date pass-through, custom table-name parameterization, no-implicit-filter invariant, and the BACKLOG-required empty-CoA edge case). **Live-validated** end-to-end on `fusion_autopilot_dev` against production-shape demo CoA — see `tests/live/TC22_dim_account_results.md`: 63,464 rows in / 63,464 out, distinct `account_id` == row count, **100% populated** on every measured column, account-type distribution sums exactly to row count (E:40,624 / R:15,934 / A:3,774 / L:2,681 / O:451), real Fusion 6-segment flexfield (`101.10.41000.430.235.000`), idempotent rebuild. Strongest live verification of any module shipped so far. **Unblocks P1.8 `gold.gl_balance`** dim-side (its other required dim — `dim_calendar` — already exists).
- **AIDP Credential Store integration discovered** — `aidputils.secrets.get(name=..., key=...)` is the documented Oracle pattern for resolving secrets from AIDP notebooks (Resource Principals are not exposed to the notebook kernel context, so the bundle's standard OCI-Vault flow can't run there directly; the AIDP credential store wraps Vault references and brokers access via `aidputils`). Used by the `fusion_autopilot_dev` cluster bootstrap path. Documented in `tests/live/TC8b_dim_supplier_module_results.md` §"Live bootstrap on dedicated cluster".
- **Test count**: 139 → **207** (68 net new tests, zero regressions in pre-existing suite).

### Changed (Phase 2 in progress)
- **2026-05-07 — `run` command now exits non-zero when no real work is performed.** Both `--inline` (when `orchestrator.run` is missing) and the default dispatch path (which currently only prints a plan) now return exit code `2` instead of `0`. The plan / status messages still print, but a CI script doing `aidp-fusion-autopilot run && next-step` will no longer mistake a no-op for a successful pipeline execution. Exit code returns to `0` once **P1.5** wires the orchestrator entry point + dispatch submission.
- **2026-05-07 (P1.2 follow-up) — `gold.supplier_spend` switched from a two-form picker to a single LEFT-JOIN form for financial correctness.** Pre-PR review caught that the original INNER-JOIN form silently drops invoices for vendors missing from the dim, which would understate spend. The "two forms picked by `id_populated_pct(vendor_id) >= 0.5`" abstraction has been removed; the unified LEFT JOIN handles both demo-shape (dim has all-NULL vendor_id → all dim attrs NULL on output) and production-shape (dim has populated vendor_id → dim attrs populated where the invoice's vendor matches) without a code-path branch. The grain is now `CAST(inv.ApInvoicesVendorId AS BIGINT)` (the invoice's claim of vendor) — never `ds.vendor_id` — so the financial number is grounded in the invoice ledger, not the dim's internal completeness. The `id_populated_pct` helper in `dim_supplier` stays as a diagnostic but is no longer load-bearing for path selection. The earlier removed exports (`build_join_form_sql`, `build_spend_only_form_sql`, `DEFAULT_JOIN_THRESHOLD`, `use_join_form` parameter) are gone; new regression tests in `test_supplier_spend.py` (`TestNoSilentInvoiceDropContract`) lock in the LEFT-JOIN invariant so the bug can't silently come back.

### Known limitations (Phase 2 in progress)
- **`run` CLI is a stub until P1.5 lands.** The Phase 2 silver/gold modules shipped above (`dim_supplier`, `dim_calendar`, `supplier_spend`) are importable as a Python package — a customer can call `dim_supplier.build(spark)` etc. directly from inside an AIDP notebook session — but the `aidp-fusion-autopilot run` CLI command does **not** yet invoke them. The CLI surfaces this with a clear error (exit 2) and points at the importable module names. Full CLI wiring (orchestrator + notebook entry point + state-table watermarking) lands in P1.5.

### Changed
- **2026-05-03 (TC10h-4 — `dashboard install` end-to-end SUCCESS on disposable OAC1)** — first clean end-to-end run of the install command with all four documented OAC REST calls green. The `find_connection` precheck (already in install.py since TC10h-2) lit up after the `search=*` fix below, allowing the realistic deployment flow to run cleanly: customer creates the AIDP connection via OAC UI once, then re-runs `dashboard install` REST-only thereafter. Evidence: snapshot `bd820501-9a3f-426e-8354-2d8c279b35b2` REGISTERed; workRequest `lfc-cc:13347-c9:3962654` RESTORE_SNAPSHOT SUCCEEDED.
- **2026-05-03 (TC10h-3 — snapshot register/restore round-trip live-validated + 2 helper bug fixes)**:
  - **BAR URI shape**: live-validated correct shape is `file:///<folder>/<name>.bar` (NOT `oci://...`, NOT bare object name, NOT the OCI Object Storage HTTPS URL). Documented in `tests/live/TC10_oac_integration_results.md` § TC10h-3 and `docs/oac_rest_api_setup.md`.
  - **Fixed**: `register_snapshot` now handles the async response shape (`202 + {"workRequestId": "..."}`). Previously assumed synchronous `{"id": "..."}`. Helper polls the work request to terminal status, then resolves the snapshot record by name (or via `workRequest.resources[].identifier`). Added `wait=False` opt-out for callers that want raw async.
  - **Fixed**: `list_connections` now defaults `search="*"`. Without it, OAC's `/catalog?type=connections` returns a single-element TypeInfo header (`[{"type":"connections"}]`) instead of items, causing `find_connection` to always return `None`. TypeInfo header rows are filtered out of the result.
  - **Fixed**: `find_connection` now passes `search=<name>` server-side for narrowing and enforces exact-match client-side (OAC search is substring; `aidp_fusion_jdbc` would otherwise false-match `aidp_fusion_jdbc_v2`).
  - Tests: 132 → 139 passing (+3 async REGISTER paths, +4 list/find connection paths).
- **2026-05-01 (TC10h-2 refactor)** — OAC integration refactored to use only Oracle-documented public REST endpoints, after a full audit of the OAC docs portal + canonical openapi.json:
  - **Removed** (endpoints not in openapi.json): `OacRestClient.import_workbook` (POST /catalog/workbooks/imports is UI-only), `OacRestClient.export_workbook` (would have exported PDF/PNG, not .dva), `OacRestClient.delete_workbook` (no public DELETE), the `dashboard export` CLI subcommand.
  - **Fixed**: `list_connections` now uses the documented `/catalog?type=connections&search=` shape. `delete_connection` Base64URL-encodes the `'<owner>'.'<name>'` object ID per Oracle's contract.
  - **Added**: snapshot lifecycle helpers (`register_snapshot`, `restore_snapshot`, `poll_work_request`, `delete_snapshot`, `get_snapshot`, `list_snapshots`). New `WorkRequestStatus` enum + `encode_catalog_id` helper. Snapshot register reads from OCI Object Storage with Resource Principal auth.
  - **Reworked**: `dashboard install` now performs four documented REST calls (POST /catalog/connections, POST /snapshots, POST /system/actions/restoreSnapshot, GET /workRequests/{id} polling). Workbook content delivery is via a single `bundle-vN.bar` snapshot the customer uploads to their own OCI Object Storage bucket (instead of per-workbook .dva files in the repo).
  - **Added** CLI flags: `--bar-bucket`, `--bar-uri`, `--bar-password`, `--snapshot-name`, `--overwrite-connection`, `--prompt-login`. **Removed**: `--workbooks-dir`.
  - **Bundle.yaml schema**: replaced `oac.workbooks: [...]` with `oac.snapshot: {bucket, uri, password, snapshotName}`.
- **2026-04-30 (TC10h)** — OAC auth model corrected to Authorization Code + PKCE + Refresh Token (after Oracle's docs explicitly forbade client_credentials grant). Audience now auto-discovered via OAC `/ui/` redirect probe. AIDP `idljdbc` connectionType captured from UI network traffic.

### Added
- Initial repo skeleton (2026-04-30): plugin metadata, skill, Python package skeleton, schema models, BICC + REST extractors mirroring the official Oracle AIDP sample, examples, unit tests.

### Tests
- 139/139 passing (was 132 after TC10h-2; net +7 for TC10h-3 fixes — 3 async REGISTER paths, 4 list/find connection paths). End-to-end `dashboard install` validated live on disposable OAC1.

## [0.1.0-alpha] — 2026-05-05

Phase 1 deliverable per the implementation plan: core BICC path + Supplier Extract.

### Achieved
- BICC extractor for `FscmTopModelAM.SupplierExtractPVO` mirroring [`oracle-aidp-samples/data-engineering/ingestion/Read_Only_Ingestion_Connectors.ipynb`](../../../data-engineering/ingestion/Read_Only_Ingestion_Connectors.ipynb)
- GL trio (Journal Lines, Period Balances, Chart of Accounts)
- `dim_account` + `dim_calendar` + `dim_supplier`
- Bootstrap probe (BICC role, External Storage profile, IAM policy)
- Live-test TC1-TC8 against demo Fusion pod (`demo-pod`)

## [0.1.0] — TBD (after live tests pass)

End-of-Phase-3 release. Tier-1 gate per the plan.
