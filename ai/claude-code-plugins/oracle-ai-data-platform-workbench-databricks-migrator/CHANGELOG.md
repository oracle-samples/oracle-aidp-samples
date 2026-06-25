# Changelog

All notable changes to this plugin are documented here. Format loosely follows [Keep a Changelog](https://keepachangelog.com/).

## [0.2.0] — 2026-06-24

**Self-contained engine bundled.** The plugin no longer requires a separate clone of the migrator toolkit. The full Python engine ships under `engine/` and is invoked from skills via `${CLAUDE_PLUGIN_ROOT}/engine/scripts/...`.

### Added

- **`engine/` directory bundled with the plugin:**
  - `engine/scripts/` — 38 Python files (`job_migrate.py`, `agent_migrate.py`, `cluster_session.py`, `aidp_executor.py`, `build_dag.py`, `check_data_availability.py`, `migrate_catalog.py`, `extract_catalog_databricks.py`, `acceptance_contract.py`, `fixup_cell` helpers, etc.)
  - `engine/aidp_compat/` — 21 Python files (drop-in `dbutils` compatibility shim)
  - `engine/schemas/` — JSON schemas (acceptance contract)
  - `engine/setup.py`, `engine/requirements.txt` — Python package metadata + deps
  - `engine/run_migration.sh` — generic convenience script
- LICENSE at validator repo root (MIT).

### Changed

- All 10 SKILL.md script-path examples updated from `python3 scripts/...` to `python3 ${CLAUDE_PLUGIN_ROOT}/engine/scripts/...`.
- `references/cli-map.md` (19 entries) updated to canonical bundled-engine paths.
- `README.md` Prerequisites section: "Clone the migrator repo" → "the engine ships bundled — just `pip install -r ${CLAUDE_PLUGIN_ROOT}/engine/requirements.txt`".
- `PRIVACY.md`: "knowledge-only" framing → "self-contained, bundled engine, no telemetry".
- All hardcoded customer identifiers (OCIDs, UUIDs, region, namespaces, customer name, workspace paths, internal hostnames, personal usernames) in the bundled engine replaced with `<PLACEHOLDER>` style values.

### Security / governance

- Deep adversarial scan across the whole engine + plugin returns 0 hits on all 21 leakage patterns.
- Source customer output artifacts (`reports/`, `dbx_export/`) removed from the source repo.

## [0.1.0] — 2026-06-20 (initial release)

First public release of the Claude Code plugin for the Oracle AIDP Databricks Migration Toolkit.

### Skills (10)

- `aidp-migrator-overview` — router / lay of the toolkit
- `aidp-migrator-bootstrap` — environment readiness check (Python deps, OCI auth, cluster state, env-coords)
- `aidp-build-dag` — build migration manifest from a Databricks workspace path
- `aidp-check-data` — pre-migration data-availability scan
- `aidp-migrate-job` — Pass-1 deps + Pass-2 cell-by-cell execute/verify/fix on a live AIDP cluster
- `aidp-fixup-cell` — targeted rewind: re-execute cells from a history index
- `aidp-resume-migration` — resume an interrupted run
- `aidp-migrate-catalog` — Unity Catalog / HMS DDL → 18-rule rewriter → batched replay
- `aidp-bucket-mapping` — `s3://` → `oci://` bucket/namespace mapping config
- `aidp-acceptance-contract` — consecutive-zero-window convergence for batch / streaming

### Slash commands (4)

- `/migrate-job` — guided full-job migration flow
- `/migrate-catalog` — guided catalog migration flow
- `/check-data` — data-availability scan
- `/migration-status` — parse + summarize a `JOB_REPORT.md`

### Agents (2)

- `databricks-notebook-analyzer` — pre-migration notebook analysis
- `migration-reviewer` — post-Pass-2 migrated-notebook correctness review

### References (5)

- DDL rewrite rules (18 rules with examples)
- 15 Databricks → AIDP gotchas + fix recipes
- Env-coords scaffold
- `JOB_REPORT.md` parsing format
- CLI map (every migrator entrypoint → purpose + canonical invocation)
