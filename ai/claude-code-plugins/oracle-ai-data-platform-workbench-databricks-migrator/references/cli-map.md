# Migrator CLI Map

Every entrypoint of the migrator toolkit, what it does, and the canonical invocation. Use this as the source-of-truth when a skill needs to construct a command line.

> The migrator engine ships **bundled with this plugin** under `engine/`. All commands below invoke it via `${CLAUDE_PLUGIN_ROOT}/engine/scripts/...` which Claude Code resolves to the install path.

## Config file (read this first)

Every entrypoint accepts `--config <path>` pointing at a customer YAML (template: `${CLAUDE_PLUGIN_ROOT}/engine/default_aidp_migrator.yaml`). Discovery order when `--config` is omitted: `$AIDP_CONFIG_PATH` → `./aidp_migrator.yaml` → `~/aidp_migrator.yaml`. The YAML supplies the AIDP coordinates (`aidp.lake_ocid`, `aidp.workspace_id`, `aidp.region`, `aidp.oci_profile`), the output paths (`paths.migrate_output`, `paths.export_output`), and `bucket_mapping.path`. The per-script `--lake-ocid` / `--workspace-id` / `--oci-profile` flags are per-run overrides of the YAML, not the primary mechanism. See [`env-coords.template.md`](./env-coords.template.md).

---

## Export a Databricks job into AIDP

### `python3 -m dbx_export` — canonical Databricks ingestion

Run from `${CLAUDE_PLUGIN_ROOT}/engine/` (it is a package, not a script). Three phases: discover+download → upload → clusters+workflow. See the [`aidp-dbx-export`](../skills/aidp-dbx-export/SKILL.md) skill.

```bash
cd ${CLAUDE_PLUGIN_ROOT}/engine
python3 -m dbx_export --config <customer>.yaml --job-id <dbx-job-id>
```

| Flag | Purpose |
|---|---|
| `--job-id` | Databricks job_id to export (required unless `--probe`) |
| `--probe` | Connectivity check only (hits DBX `/api/2.2/jobs/list?limit=1`) |
| `--config` | Customer YAML (discovery order above) |
| `--export-output` | AIDP base path for all exported artifacts (default: `cfg.paths.export_output`; `--target-base` is a back-compat alias) |
| `--prefix` | Prefix for created AIDP workflow + cluster names (default `ai_tool`) |
| `--dbx-host` / `--dbx-token` | Direct DBX access (env: `DATABRICKS_HOST` / `DATABRICKS_TOKEN`) |
| `--dbx-proxy` (+ `--proxy-basic-user/-pass`) | DBX proxy for cluster-side runs (env: `DBX_PROXY`, ...) |
| `--lake-ocid` / `--workspace-id` / `--oci-profile` | Per-run overrides of the YAML |
| `--overwrite` | Force re-upload / re-create (default is no-overwrite) |
| `--strict` / `--strict-deps` / `--strict-jars` / `--strict-unresolved-jars` | Fail-loud variants (default: JAR fetch failures are non-blocking) |
| `--param KEY=VALUE` | Repeatable; resolves widget-based `notebook.run` paths during discovery |
| `--dry-run` | Phase 1 + pre-flight only; no AIDP writes |
| `--notebook-only` | Upload the notebook tree only; skip files/jars and Phase 3 |

---

## Build the execution DAG

### `${CLAUDE_PLUGIN_ROOT}/engine/scripts/build_dag_from_workflow.py` — canonical (workflow-based)

Builds the manifest from an **AIDP workflow** (typically one created by `dbx_export`) via the **AIDP Job API** — it does NOT talk to Databricks and needs no `DATABRICKS_HOST` / `DATABRICKS_TOKEN`. Preserves `depends_on` task edges.

```bash
python3 ${CLAUDE_PLUGIN_ROOT}/engine/scripts/build_dag_from_workflow.py \
  --config <customer>.yaml \
  --job-key <workflow-key> \
  --cluster <CLUSTER_ID> \
  --output reports/<MyJob>_manifest.json
```

| Flag | Purpose |
|---|---|
| `--job-key` | **Required.** AIDP job key (UUID) of the workflow |
| `--cluster` | **Required.** AIDP cluster ID used for scanning notebooks |
| `--job-name` | Manifest name (default: from the workflow) |
| `--output` | Manifest write path (default: stdout) |
| `--validate-only` | Only validate notebooks exist + print the task graph |
| `--export-output` | AIDP export base the notebooks live under (auto-derived from the first task's notebook path if omitted; fails loud if derivation fails) |
| `--source-base` | Source (dbx_export) base for `%run` prefix stripping (auto-derived if omitted) |
| `--params-from-run <RUN_KEY>` | Populate manifest parameters from a prior successful AIDP job run (`GET /jobRuns/{RUN_KEY}`) |
| `--param KEY=VALUE` | Repeatable; override a workflow parameter during dependency discovery |
| `--config` / `--lake-ocid` / `--workspace-id` / `--oci-profile` | Customer YAML + per-run overrides |

### `${CLAUDE_PLUGIN_ROOT}/engine/scripts/build_dag.py` — DEPRECATED (directory scan)

**Do not invoke as a CLI.** Superseded by `build_dag_from_workflow.py`; kept only as an importable library for the workflow variant. For the record, its argparse surface is `--root` (workspace folder), `--entry`, `--job-name`, `--cluster`, `--graph-only`, `--output`, `--param KEY=VALUE`, `--lake-ocid`, `--workspace-id`, `--oci-profile`. There is no `--include-pattern` flag.

---

## Pre-migration data check

### `${CLAUDE_PLUGIN_ROOT}/engine/scripts/check_data_availability.py` — path-rooted

```bash
python3 ${CLAUDE_PLUGIN_ROOT}/engine/scripts/check_data_availability.py \
  --root "<workspace-root-path>" \
  --cluster <CLUSTER_ID> \
  [--bucket-mapping <path>] \
  [--lake-ocid <LAKE_OCID> --workspace-id <WORKSPACE_UUID> --oci-profile <profile>]
```

Flags: `--root` (required), `--cluster` (required), `--bucket-mapping` (override of `cfg.bucket_mapping.path`), `--lake-ocid`, `--workspace-id`, `--oci-profile`.

### `${CLAUDE_PLUGIN_ROOT}/engine/scripts/check_data_availability_for_workflow.py` — workflow-based

```bash
python3 ${CLAUDE_PLUGIN_ROOT}/engine/scripts/check_data_availability_for_workflow.py \
  --config <customer>.yaml \
  --job-key <workflow-key> \
  --cluster <CLUSTER_ID> \
  [--manifest reports/<MyJob>_manifest.json] \
  [--only-tasks <substrings>] \
  [--output reports/<MyJob>_data_check.json] \
  [--bucket-mapping <path>]
```

| Flag | Purpose |
|---|---|
| `--job-key` | AIDP job key (UUID) of the workflow |
| `--manifest` | Pre-built manifest JSON (skips the API fetch) |
| `--cluster` | **Required.** AIDP control/data-probe cluster ID |
| `--only-tasks` | Comma-separated task_key substrings to check |
| `--output` | Save the JSON report to a file |
| `--databricks-job-id` | Source DBX job ID for the embedded data_migration contract |
| `--param KEY=VALUE` | Repeatable; resolves f-strings and date filters |
| `--bucket-mapping` / `--config` / `--lake-ocid` / `--workspace-id` / `--oci-profile` / `--oci-config-path` | Mapping + environment overrides |

Report classes: tables → `EXISTS (<n> rows)` / `ERROR` / `MISSING`; paths → `EXISTS (<n> items)` / `ERROR` / `NOT FOUND`. All clear prints `RESULT: ALL DATA AVAILABLE - ready to migrate`; otherwise `RESULT: <n> data sources missing`. (There is no `EMPTY` class.)

---

## Catalog migration (Unity Catalog / HMS → AIDP)

### `${CLAUDE_PLUGIN_ROOT}/engine/scripts/extract_catalog_databricks.py` — stage 1 (extract)

```bash
python3 ${CLAUDE_PLUGIN_ROOT}/engine/scripts/extract_catalog_databricks.py \
  --catalogs "<catalog_a>,<catalog_b>" \
  --schemas-only "<catalog_a>:<schema_1>" \
  --out reports/catalog_pack.json
```

Requires `DATABRICKS_HOST` and a PAT from `DATABRICKS_TOKEN` or `--token-file <path>` (a file
only its owner can read, `chmod 600`). `--token <value>` is refused with exit code 2 -- never put
the token on the command line.

### `${CLAUDE_PLUGIN_ROOT}/engine/scripts/migrate_catalog.py` — stage 2 (rewrite + replay)

```bash
python3 ${CLAUDE_PLUGIN_ROOT}/engine/scripts/migrate_catalog.py \
  --pack reports/catalog_pack.json \
  --aidp-cluster <CLUSTER_ID> \
  --aidp-lake <LAKE_OCID> \
  --aidp-workspace <WORKSPACE_UUID> \
  --aidp-profile <profile> \
  --out-manifest reports/catalog_manifest.json \
  --out-report reports/catalog_report.md \
  [--dry-run]
```

| Flag | Purpose |
|---|---|
| `--pack` | **Required.** Stage-1 catalog pack JSON |
| `--aidp-cluster` / `--aidp-lake` / `--aidp-workspace` | **Required.** Target AIDP coordinates |
| `--aidp-profile` | OCI profile (default `AIDP_API`) |
| `--out-manifest` / `--out-report` | **Required.** Output manifest (feed to `job_migrate_from_workflow --catalog-manifest`) + human report |
| `--catalog-map` | Optional explicit source→target catalog map |
| `--default-catalog` | Target catalog for flattened names (default `default`) |
| `--bucket-map` | S3→OCI 3-column CSV (`s3_bucket,oci_bucket,oci_namespace`) for external-table LOCATION rewrites |
| `--flatten-strategy` | How 3-part names flatten (default `catalog-remap`) |
| `--location-strategy` | `auto` (default: EXTERNAL keeps rewritten LOCATION, MANAGED drops it) / `preserve` / `drop` |
| `--target-using` | Force a storage format (default: preserve source format — Delta stays Delta) |
| `--dry-run` | Print rewritten DDL, don't execute |

There is no `--chunk-size`, `--catalogs`, `--schemas`, or `--aidp-base` flag on this script.

---

## The big run: notebook migration

### `${CLAUDE_PLUGIN_ROOT}/engine/scripts/job_migrate_from_workflow.py` — CANONICAL runner

Drives the core migrator against an AIDP workflow (per-task clusters come from the workflow/manifest).

```bash
export ANTHROPIC_API_KEY=sk-ant-...
python3 ${CLAUDE_PLUGIN_ROOT}/engine/scripts/job_migrate_from_workflow.py \
  --config <customer>.yaml \
  --job-key <workflow-key>
```

| Flag | Purpose |
|---|---|
| `--job-key` | AIDP workflow key (or `--manifest <path>` for a pre-built manifest) |
| `--config` | Customer YAML — supplies AIDP coordinates and `paths.migrate_output` |
| `--migrate-output <path>` | Per-run override of `cfg.paths.migrate_output` (`--output-base` is a deprecated back-compat alias) |
| `--cluster` | **DEPRECATED no-op** — ignored with a warning; per-task clusters come from the workflow/manifest. Never pass it. |
| `--param KEY=VALUE` | Repeatable parameter override |
| `--save-manifest <path>` | Save the generated manifest |
| `--catalog-manifest <path>` | Catalog-migration manifest (from `migrate_catalog.py --out-manifest`) for deterministic source-catalog literal remap. **Only on this runner**, not on `job_migrate.py`. |
| `--validate-only` | Validate notebooks exist + print the task graph |
| `--start-task <substring>` / `--only-tasks <names>` | Resume from / restrict to tasks |
| `--skip-migrated` / `--no-skip-migrated` | Reuse already-migrated notebooks. **Default OFF** (fresh migration guarantees nested `%run` deps are converted); prefer `--start-task` to scope resumes. |
| `--refresh-mirror` | Refresh the local mirror |
| `--direct-execute` / `--code-only` | Execution-mode switches |
| `--strict-cell-gating` | Fail the run on any unexecuted/skipped/unverified cell |
| `--bucket-mapping <path>` | Override `cfg.bucket_mapping.path` |
| `--aidp-base` / `--lake-ocid` / `--workspace-id` / `--oci-profile` / `--oci-config-path` | Per-run environment overrides |
| `--readiness-report` / `--readiness-max-age-seconds` | Phase-1 readiness report overrides |

There is **no `--acceptance-contract` flag**: acceptance contracts live in the manifest as an `acceptance_contract` key on a task (or job) entry — see [`aidp-acceptance-contract`](../skills/aidp-acceptance-contract/SKILL.md).

### `${CLAUDE_PLUGIN_ROOT}/engine/scripts/job_migrate.py` — core library (direct invocation DEPRECATED)

`job_migrate_from_workflow.py` drives this module; invoke the wrapper, not this. Its own argparse (for completeness): `--manifest`, `--parallel` (default 20), `--cluster`, `--jobs`, `--bucket-mapping`, `--config`, `--aidp-base`, `--datalake-ocid`, `--workspace-id`, `--oci-profile`, `--migrate-output` (alias `--output-base`), `--start-task`, `--only-tasks`, `--skip-migrated`/`--no-skip-migrated` (default OFF), `--refresh-mirror`, `--direct-execute`/`--code-only`, `--strict-cell-gating`, `--wave-size`. It has **no `--catalog-manifest`** and **no `--acceptance-contract`**.

---

## Clone an AIDP workflow (post-migration)

### `${CLAUDE_PLUGIN_ROOT}/engine/scripts/clone_workflow.py`

> **WARNING — this script CREATES a real AIDP workflow on EVERY invocation** (except `--dry-run`). Never run it "to verify" or on your own initiative; workflow creation is a human-approved action. Use `--dry-run` to preview.

```bash
python3 ${CLAUDE_PLUGIN_ROOT}/engine/scripts/clone_workflow.py \
  --job-key <SOURCE_AIDP_JOB_KEY> \
  --registry <registry-path> \
  --dry-run \
  --config <customer>.yaml
```

| Flag | Purpose |
|---|---|
| `--job-key` | **Required.** Source AIDP job key to clone |
| `--registry` | **Required.** Clone-registry path (idempotency record) |
| `--dry-run` | Preview only — the ONLY mode that creates nothing |
| `--param KEY=VALUE` | Repeatable; override a matching job/task parameter in the clone |
| `--config` / `--lake-ocid` / `--workspace-id` / `--oci-profile` | Customer YAML + overrides |

---

## Cluster lifecycle

### `${CLAUDE_PLUGIN_ROOT}/engine/scripts/cluster_lifecycle.py` — library only (NO CLI)

This module has no argparse / `__main__` — it is imported by the runners (cluster start/stop, library install, idle timeout). Callers that accept `--config` must call `cluster_lifecycle.configure(config_path)` to re-resolve the dependency repository and derived constants. Do not try to invoke it as a script.

---

## How skills compose these

The skills don't run these CLIs directly — they generate the right command line based on the user's request, surface it for the user's confirmation, then invoke it. Each `<placeholder>` above maps to a coordinate in [`env-coords.template.md`](./env-coords.template.md) — preferably via the customer YAML passed as `--config`.
