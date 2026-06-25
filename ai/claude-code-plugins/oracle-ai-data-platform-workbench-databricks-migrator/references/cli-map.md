# Migrator CLI Map

Every entrypoint of the migrator toolkit, what it does, and the canonical invocation. Use this as the source-of-truth when a skill needs to construct a command line.

> The migrator engine ships **bundled with this plugin** under `engine/`. All commands below invoke it via `${CLAUDE_PLUGIN_ROOT}/engine/scripts/...` which Claude Code resolves to the install path.

---

## Build the execution DAG

### `${CLAUDE_PLUGIN_ROOT}/engine/scripts/build_dag.py` — path-based

Walks a Databricks workspace folder, resolves `%run` / `dbutils.notebook.run` chains, emits a topo-ordered manifest.

```bash
python3 ${CLAUDE_PLUGIN_ROOT}/engine/scripts/build_dag.py \
  --root "<databricks-workspace-path>" \
  --job-name "<MyJob>" \
  --output reports/<MyJob>_manifest.json
```

| Flag | Purpose |
|---|---|
| `--root` | Databricks workspace folder (e.g. `Users/<user>/<job-folder>`) |
| `--job-name` | Manifest name + output-base subdirectory |
| `--output` | Where to write the manifest JSON |
| `--include-pattern` | (optional) glob filter for which notebooks to include |

### `${CLAUDE_PLUGIN_ROOT}/engine/scripts/build_dag_from_workflow.py` — workflow-based

Pulls task definitions from a Databricks Job ID via the Workflows REST API, preserving `depends_on` edges.

```bash
python3 ${CLAUDE_PLUGIN_ROOT}/engine/scripts/build_dag_from_workflow.py \
  --job-id <databricks-job-id> \
  --output reports/<MyJob>_manifest.json
```

Requires `DATABRICKS_HOST` + `DATABRICKS_TOKEN` env.

---

## Pre-migration data check

### `${CLAUDE_PLUGIN_ROOT}/engine/scripts/check_data_availability.py` — manifest-based

```bash
python3 ${CLAUDE_PLUGIN_ROOT}/engine/scripts/check_data_availability.py \
  --root "<databricks-workspace-path>" \
  --cluster <CLUSTER_ID> \
  --aidp-base <AIDP_BASE> \
  --datalake-ocid <DATALAKE_OCID> \
  --workspace-id <WORKSPACE_UUID> \
  --oci-profile <profile>
```

### `${CLAUDE_PLUGIN_ROOT}/engine/scripts/check_data_availability_for_workflow.py` — workflow-based

```bash
python3 ${CLAUDE_PLUGIN_ROOT}/engine/scripts/check_data_availability_for_workflow.py \
  --job-id <databricks-job-id> \
  --cluster <CLUSTER_ID> \
  --aidp-base <AIDP_BASE> \
  --datalake-ocid <DATALAKE_OCID> \
  --workspace-id <WORKSPACE_UUID> \
  --oci-profile <profile>
```

Both output an OK / MISSING / EMPTY report by table + path.

---

## Catalog migration (Unity Catalog / HMS → AIDP)

### `${CLAUDE_PLUGIN_ROOT}/engine/scripts/extract_catalog_databricks.py` — stage 1 (extract)

```bash
python3 ${CLAUDE_PLUGIN_ROOT}/engine/scripts/extract_catalog_databricks.py \
  --catalogs "<catalog_a>,<catalog_b>" \
  --schemas-only "<catalog_a>:<schema_1>" \
  --out reports/catalog_pack.json
```

Requires `DATABRICKS_HOST` + `DATABRICKS_TOKEN`.

### `${CLAUDE_PLUGIN_ROOT}/engine/scripts/migrate_catalog.py` — stage 2 (rewrite + replay)

```bash
python3 ${CLAUDE_PLUGIN_ROOT}/engine/scripts/migrate_catalog.py \
  --pack reports/catalog_pack.json \
  --cluster <CLUSTER_ID> \
  --aidp-base <AIDP_BASE> \
  --datalake-ocid <DATALAKE_OCID> \
  --workspace-id <WORKSPACE_UUID> \
  --output-base <output-workspace-path> \
  --oci-profile <profile>
```

Useful flags:
- `--dry-run` — print rewritten DDL, don't execute.
- `--chunk-size 25` — statements per WS execute batch.
- `--catalogs <list>` / `--schemas <list>` — filter what to replay.
- `--bucket-mapping <path>` — used for `s3://` → `oci://` rewrites in external-table LOCATIONs.

---

## The big run: notebook migration

### `${CLAUDE_PLUGIN_ROOT}/engine/scripts/job_migrate.py` — manifest-based

Pass-1 deps + Pass-2 cell-by-cell on a live AIDP cluster.

```bash
export ANTHROPIC_API_KEY=sk-ant-...
python3 ${CLAUDE_PLUGIN_ROOT}/engine/scripts/job_migrate.py \
  --manifest reports/<MyJob>_manifest.json \
  --cluster <CLUSTER_ID> \
  --aidp-base <AIDP_BASE> \
  --datalake-ocid <DATALAKE_OCID> \
  --workspace-id <WORKSPACE_UUID> \
  --output-base <output-workspace-path> \
  --oci-profile <profile>
```

Common additional flags:
| Flag | Purpose |
|---|---|
| `--jobs <name>,<name>` | Migrate only specific jobs from the manifest. |
| `--start-task <substring>` | Resume from this task. |
| `--only-tasks <names>` | Run ONLY these task names. |
| `--skip-migrated` / `--no-skip-migrated` | Skip already-migrated notebooks (default ON). |
| `--parallel <N>` | Concurrent task workers (default 20). |
| `--catalog-manifest <path>` | Source-catalog → `default` literal remap. |
| `--bucket-mapping <path>` | `s3://` → `oci://` mapping for path rewrites. |
| `--acceptance-contract <path>` | YAML contract for convergence verification. |

### `${CLAUDE_PLUGIN_ROOT}/engine/scripts/job_migrate_from_workflow.py` — workflow-shape manifest

Same flags as above, plus reads the Databricks Workflows task DAG verbatim (vs inferring deps from `%run`).

---

## Clone an AIDP workflow (post-migration)

### `${CLAUDE_PLUGIN_ROOT}/engine/scripts/clone_workflow.py`

After migrating one workflow, you may want to clone it (sandbox copy, regional fan-out) without re-running the migrator.

```bash
python3 ${CLAUDE_PLUGIN_ROOT}/engine/scripts/clone_workflow.py \
  --source-job-key <SOURCE_AIDP_JOB_KEY> \
  --target-name "<NewName>" \
  --aidp-base <AIDP_BASE> \
  --datalake-ocid <DATALAKE_OCID> \
  --workspace-id <WORKSPACE_UUID> \
  --oci-profile <profile>
```

---

## Cluster lifecycle

### `${CLAUDE_PLUGIN_ROOT}/engine/scripts/cluster_lifecycle.py`

Read-only inspection + non-destructive nudges (start a stopped cluster). NOT used for create / destroy.

```bash
python3 ${CLAUDE_PLUGIN_ROOT}/engine/scripts/cluster_lifecycle.py \
  --cluster <CLUSTER_ID> \
  --aidp-base <AIDP_BASE> \
  --datalake-ocid <DATALAKE_OCID> \
  --workspace-id <WORKSPACE_UUID> \
  --oci-profile <profile> \
  --action [status | start | ensure_active]
```

---

## How skills compose these

The skills don't run these CLIs directly — they generate the right command line based on the user's request, surface it for the user's confirmation, then invoke it. Each `<placeholder>` above maps to a coordinate in [`env-coords.template.md`](./env-coords.template.md).
