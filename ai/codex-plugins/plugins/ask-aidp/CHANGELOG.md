# Changelog

All notable changes to this plugin are documented here.

## [0.8.0] - 2026-07-17

### Added

- Generated CLI catalog refreshed from the current AIDP SDK CLI reference: 17 command groups and 242 commands, including the Agent group and bundle action aliases.
- Typed Agent tools for create, deploy, list, and agent-session trace retrieval.
- REST API reference tool and catalog for the current `/20260430` API surface, including Agent and Git categories.

### Changed

- REST fallback guidance now cites the current Oracle REST catalog and What's New page. The June 2026 update added SDK and CLI resource links; no later REST endpoint version is listed.

## [0.7.2] - 2026-07-01

Initial Oracle Samples release of the Ask AIDP Codex plugin.

### Added

- Generic `aidp_cli` tool and generated reference for documented AIDP CLI commands.
- Workspace and cluster connection checks.
- N-notebook workflow creation, execution, run tracking, output export, and log collection.
- Workspace code upload and Git-backed workspace folder creation.
- Native TypeScript SDK Git tools for push, pull, status, diff, branch, merge, rebase, and reset workflows.
- Catalog, schema, Delta table, external catalog, CSV table SQL, and initial table data helpers.
- Medallion architecture, bundle creation, bundle deployment, and workflow auto-healing helpers.
- Binary release artifacts under `dist/` for offline distribution.
