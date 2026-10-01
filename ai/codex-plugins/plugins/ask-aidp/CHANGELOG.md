# Changelog

All notable changes to this plugin are documented here.

## [0.10.0] - 2026-09-21

### Fixed

- Select CLI root request models using the CLI operation manifest instead of the
  first (potentially nested) model in the Markdown. Regenerate the catalog,
  including `ExportComputeConfigurationDetails` and its four root fields.
- Default Compute configuration export REST requests to `Accept: application/x-yaml`
  and JSON request bodies to `Content-Type: application/json`. Merge headers once
  so OCI request composition does not append conflicting Accept values; expose
  the same effective headers in dry runs.
- Add regressions for model ordering, root fields, empty models, MLflow field
  spelling, header overrides, composed HTTP headers, and mocked YAML responses.
  Live export and workspace YAML readback still require an enabled instance.

### Changed

- Regenerate the CLI catalog from Oracle's current reference: 18 groups and 256
  commands, including Data Lineage, bundle publishing, Compute configuration
  import/export and cloning, Maven search, ZIP operations, and task-run retries.
- Regenerate the REST catalog from Oracle's full endpoint index: 19 categories
  and 271 operations for API version `/20260430`. Preserve documented deprecated
  endpoints for compatibility and use POST for bundle publish status, as specified
  in the operation reference.
- Document the newer CLI requirement and route new bundle publishing requests
  through the generic CLI/REST tools while retaining legacy deployment helpers.
- Test CLI lookup, REST lookup, and REST request planning for all 14 additions,
  including rejection of the incorrect GET method for bundle publish status.
- Retain the upload, CSV ingestion, marketplace, and packaging fixes from 0.9.1.
- Preserve relative npm executable symlinks when packaging so archives do not
  depend on paths on the build machine.

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
