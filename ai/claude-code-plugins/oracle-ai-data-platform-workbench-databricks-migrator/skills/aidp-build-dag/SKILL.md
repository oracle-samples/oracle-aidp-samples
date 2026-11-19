---
name: aidp-build-dag
description: Build a migration manifest (the execution DAG) from an AIDP workflow (exported from Databricks by dbx_export). Walks %run dependency chains, captures dbutils.notebook.run invocations, and emits reports/<job>_manifest.json — the input every other execute-skill consumes. Use when the user wants to see what would migrate, or before invoking aidp-migrate-job for the first time on a new workload.
---

# `aidp-build-dag` — build the migration manifest

The execution DAG is what the migrator reads to know which notebooks to migrate, in what order, with what dependencies. Build it once per workload.

## When to use

- User asks "what would migrate", "show me the dependency tree", "build the manifest".
- Before any [`aidp-migrate-job`](../aidp-migrate-job/SKILL.md) invocation against a new workload.
- After changing the exported workflow or its notebooks.

## One canonical entry point

The canonical builder is **workflow-based**: `${CLAUDE_PLUGIN_ROOT}/engine/scripts/build_dag_from_workflow.py`. It reads an **AIDP workflow** (normally created by [`aidp-dbx-export`](../aidp-dbx-export/SKILL.md)) via the **AIDP Job API** — it does NOT call the Databricks REST API and needs no `DATABRICKS_HOST` / `DATABRICKS_TOKEN`.

> `${CLAUDE_PLUGIN_ROOT}/engine/scripts/build_dag.py` (path-based directory scan) is **DEPRECATED** — do not invoke it as a CLI; it survives only as an importable library used by the workflow variant.

## Workflow-based invocation (canonical)

```bash
python3 ${CLAUDE_PLUGIN_ROOT}/engine/scripts/build_dag_from_workflow.py \
  --config <customer>.yaml \
  --job-key <workflow-key> \
  --cluster <CLUSTER_ID> \
  --output reports/<MyJob>_manifest.json
```

- `--job-key` — **required**; the AIDP job key (UUID) of the workflow.
- `--cluster` — **required**; the AIDP cluster ID used for scanning notebooks.
- `--config` — customer YAML (falls back to `$AIDP_CONFIG_PATH`, `./aidp_migrator.yaml`, `~/aidp_migrator.yaml`); `--lake-ocid` / `--workspace-id` / `--oci-profile` are per-run overrides.
- `--output` — manifest write path (default: stdout).
- Useful extras: `--validate-only` (validate notebooks exist + print the task graph), `--job-name`, `--export-output` / `--source-base` (base-path overrides when auto-derivation guesses wrong), `--params-from-run <RUN_KEY>` (populate parameters from a prior AIDP job run), repeatable `--param KEY=VALUE`.

This pulls the workflow's task definitions from the AIDP Job API and converts `depends_on` task edges into the manifest's DAG. The builder also follows `%run` chains AND `dbutils.notebook.run(...)` calls in the task notebooks to build the topo-ordered DAG and flag transitive deps, so Pass-1 knows which notebooks to migrate code-only first.

## Manifest shape

The output is JSON with this top-level structure:

```json
{
  "job_name": "<MyJob>",
  "tasks": [
    {
      "task_key": "extract",
      "notebook_path": "Users/.../extract.ipynb",
      "depends_on": []
    },
    {
      "task_key": "transform",
      "notebook_path": "Users/.../transform.ipynb",
      "depends_on": ["extract"]
    }
  ],
  "deps": [
    {
      "notebook_path": "Users/.../helpers/io_utils.ipynb",
      "referenced_by": ["extract", "transform"]
    }
  ]
}
```

`tasks` are the named entry points; `deps` are `%run` / `notebook.run` targets discovered transitively. Pass-1 migrates the `deps` first (code-only), then Pass-2 executes the `tasks` in topo order.

## Sanity-check the manifest before running

After the builder finishes, do these three reads — small, fast, catch most config issues:

```bash
# 1. count
jq '.tasks | length, .deps | length' reports/<MyJob>_manifest.json

# 2. topo correctness — no cycles, deps come before users
jq '.tasks[] | select(.depends_on | length > 0) | {task: .task_key, deps: .depends_on}' reports/<MyJob>_manifest.json

# 3. notebook paths resolve (every path is reachable from the Databricks workspace)
jq -r '.tasks[].notebook_path' reports/<MyJob>_manifest.json
```

If any `notebook_path` doesn't exist in Databricks, the builder logs a warning but does NOT fail. Catch it here.

## Known caveats

- **`dbutils.notebook.run` with dynamic paths.** If the source notebook builds the target path at runtime (`dbutils.notebook.run(some_var, ...)`), the builder cannot resolve it. The dep won't appear in the manifest and the runtime call will fail post-migration. Tell the user to either (a) make the path literal, or (b) add the target to a `dep_hints` section manually.
- **Workflow tasks with `for_each_task`.** Not modeled. Convert to a regular `notebook_task` first.
- **Sub-workflows (Workflow-runs-Workflow).** The DAG builder follows the OUTER workflow only. Nested workflows need a separate manifest each.

## After this

Once the manifest looks right:
1. Run [`aidp-check-data`](../aidp-check-data/SKILL.md) to verify source tables exist on the cluster.
2. Run [`aidp-migrate-job`](../aidp-migrate-job/SKILL.md) with `--manifest reports/<MyJob>_manifest.json`.
