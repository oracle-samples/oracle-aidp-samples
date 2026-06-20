---
name: aidp-build-dag
description: Build a migration manifest (the execution DAG) from a Databricks workspace path or workflow ID. Walks %run dependency chains, captures dbutils.notebook.run invocations, and emits reports/<job>_manifest.json — the input every other execute-skill consumes. Use when the user wants to see what would migrate, or before invoking aidp-migrate-job for the first time on a new workload.
---

# `aidp-build-dag` — build the migration manifest

The execution DAG is what the migrator reads to know which notebooks to migrate, in what order, with what dependencies. Build it once per workload.

## When to use

- User asks "what would migrate", "show me the dependency tree", "build the manifest".
- Before any [`aidp-migrate-job`](../aidp-migrate-job/SKILL.md) invocation against a new workload.
- After changing the Databricks workspace path or workflow ID.

## Two entry points

The migrator ships two DAG builders. Pick based on input shape:

| Input | Entrypoint |
|---|---|
| **Path-based**: a folder of `.ipynb` / `.py` notebooks the user wants migrated | `scripts/build_dag.py` |
| **Workflow-based**: a Databricks Job ID whose tasks the user wants migrated, preserving the task DAG | `scripts/build_dag_from_workflow.py` |

## Path-based invocation

```bash
python3 scripts/build_dag.py \
  --root "<databricks-workspace-path>" \
  --job-name "<MyJob>" \
  --output reports/<MyJob>_manifest.json
```

- `--root` — Databricks workspace folder containing the entry notebooks. The script walks every `*.ipynb` / `*.py` under this prefix.
- `--job-name` — a name for the manifest (used as the output-base subdirectory).
- `--output` — manifest write path.

The builder follows `%run` chains AND `dbutils.notebook.run(...)` calls to build a topo-ordered DAG. It also flags transitive deps so Pass-1 knows which notebooks to migrate code-only first.

## Workflow-based invocation

```bash
python3 scripts/build_dag_from_workflow.py \
  --job-id <databricks-job-id> \
  --output reports/<MyJob>_manifest.json
```

This pulls the Job's task definitions via the Databricks Jobs REST API and converts `depends_on` task edges into the manifest's DAG. Use when the user wants the AIDP migration to mirror the Databricks Workflow shape (vs. just inferring dependencies from `%run`).

Required env / args:
- `DATABRICKS_HOST` — `https://<workspace>.cloud.databricks.com`
- `DATABRICKS_TOKEN` — a PAT with workspace-read permission

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
