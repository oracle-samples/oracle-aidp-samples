# Changelog

All notable changes to this project are documented here.

## [Unreleased]

### Changed
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
- 132/132 passing (was 122 pre-refactor; net +10 for snapshot helpers, -4 for removed export tests).

## [0.1.0-alpha] — TBD (Phase 1 gate, week 1)

Phase 1 deliverable per [PLAN](../../../.claude/plans/oracle-ai-data-platform-fusion-bundle.md): core BICC path + Supplier Extract.

### Planned
- BICC extractor for `FscmTopModelAM.SupplierExtractPVO` mirroring [`oracle-aidp-samples/data-engineering/ingestion/Read_Only_Ingestion_Connectors.ipynb`](../../../data-engineering/ingestion/Read_Only_Ingestion_Connectors.ipynb)
- GL trio (Journal Lines, Period Balances, Chart of Accounts)
- `dim_account` + `dim_calendar` + `dim_supplier`
- Bootstrap probe (BICC role, External Storage profile, IAM policy)
- Live-test TC1-TC8 against demo Fusion pod (`saasfademo1`)

## [0.1.0] — TBD (after live tests pass)

End-of-Phase-3 release. Tier-1 gate per the plan.
