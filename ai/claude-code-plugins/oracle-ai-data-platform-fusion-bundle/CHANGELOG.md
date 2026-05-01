# Changelog

All notable changes to this project are documented here.

## [Unreleased]

### Added
- Initial repo skeleton (2026-04-30): plugin metadata, skill, Python package skeleton, schema models, BICC + REST extractors mirroring the official Oracle AIDP sample, examples, unit tests.

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
