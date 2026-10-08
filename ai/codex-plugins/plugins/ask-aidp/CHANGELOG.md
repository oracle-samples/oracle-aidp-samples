# Changelog

All notable changes to this plugin are documented here.

## [Unreleased]

### Security
- The MCP server resolves `oci-common` and `aidp-typescript-client` only from the plugin's
  own `vendor/node_modules` / `node_modules` (or `AIDP_VENDOR_NODE_MODULES`), no longer from
  the process working directory or sibling sample trees, so a planted module cannot be
  loaded in the process that holds the user's OCI credentials.

## [0.9.1] - 2026-08-27

### Fixed

- Stage inline-row CSV data without a header and use positional `selectedColumns`
  so `schema create-data-table` does not ingest the header as a data row.
- Upload workspace files through `WorkspaceObjectClient#createWorkspaceObject`,
  with safe folder-conflict handling and dry-run placeholders that do not require
  an instance OCID or read file contents.
- Fail offline packaging when the vendor source is missing `aidp-cli`,
  `aidp-typescript-client`, or `oci-common` instead of producing an incomplete
  archive.

### Changed

- Strengthen static QA with exact version/tool-count checks, portable MCP and
  symlink validation, marketplace resolution, JSON-only stdout enforcement, and
  regression coverage for workspace upload and inline-row table loading.
- Make prompt-based installation the recommended first option and document Codex
  app restarts, post-install verification, environment inheritance, and guarded
  offline packaging on macOS, Linux, and Windows.

## [0.9.0] - 2026-07-20

### Added

- Typed AI Compute helpers: `aidp_create_ai_compute`, `aidp_list_ai_computes`, and `aidp_update_ai_compute`. AI Compute is managed as a cluster with `type: AI_COMPUTE`, typed driver shape/configuration, and replica settings.
- Generic `aidp_rest` tool for any endpoint in the generated Oracle REST catalog. It applies OCI request signing, expands configured AIDP/workspace/cluster identifiers, supports request bodies and query parameters, and rejects paths or methods outside the documented catalog.
- Generated REST endpoint catalog refreshed from Oracle documentation: 18 categories and 257 operations for API version `/20260430`.

### Changed

- `aidp_rest_api_reference` can now summarize coverage, filter a category, find an exact operation, or search the complete REST catalog.

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
