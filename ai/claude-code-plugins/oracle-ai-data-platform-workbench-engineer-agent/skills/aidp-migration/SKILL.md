---
name: aidp-migration
description: Guide a migration of notebooks/jobs from another platform (e.g. Databricks) into AIDP. Use when the user wants to port Databricks notebooks/jobs to AIDP, move workloads onto the AIDP lakehouse, or plan a migration. Orchestration-only — it composes the other self-contained aidp-* skills; it adds no new API surface.
---

# `aidp-migration` — guided migration into AIDP

Plan and execute a migration of notebooks/jobs onto AIDP by composing the other skills. Adds no new API
surface — it sequences ingestion, notebooks, pipelines, and validation. Like every skill in this plugin
it is self-contained: control-plane ops run via `oci raw-request` and interactive Spark-SQL/cell
execution runs via the bundled `scripts/aidp_sql.py`. No MCP server or `ai-data-engineer-agent` repo
is required.

## When to use
- "Migrate these Databricks notebooks/jobs to AIDP", "move this workload onto AIDP", "plan a migration".

## Workflow
1. **Inventory** the source assets (notebooks, jobs/schedules, tables, libraries) — list what must move.
2. **Land data**: ingest source tables/files (`aidp-ingest-file-to-table`; external sources via the
   spark-connectors plugin + `aidp-federate`).
3. **Port notebooks**: recreate notebooks in the workspace (`aidp-notebooks` / `aidp-workspace-files`),
   adapting platform-specifics (paths, `compute:///` defaultFS caveats, cluster/session APIs, Delta vs
   other formats). Validate cells run with the bundled helper (`python "$PLUGIN_DIR/scripts/aidp_sql.py" … --code …`).
4. **Recreate jobs**: build the task DAG + schedule (`aidp-pipelines`), heeding the `clusterName`-UUID
   pitfall and `NOTEBOOK_TASK`/`dependsOn` shape.
5. **Validate**: profile + quality-check migrated tables (`aidp-profiling-tables`, `aidp-data-quality`);
   compare row counts/aggregates against the source; dry-run the job and inspect output.
6. **Cut over**: only after validation; keep the source as fallback until confirmed.

## Engines (inherited from the composed skills)
- **Control-plane** (workspaces, catalogs, tables, clusters, jobs, files) → `oci raw-request` against the
  AIDP REST API — see [references/oci-raw-request.md](../../references/oci-raw-request.md) and
  [references/no-mcp-rest-map.md](../../references/no-mcp-rest-map.md).
- **Interactive Spark-SQL / cell execution** (validate ported cells, compare counts/aggregates) →
  `python "$PLUGIN_DIR/scripts/aidp_sql.py" --region <r> --datalake <OCID> --workspace <ws> --cluster <key> --code <…>`.

## Notes
- Common AIDP gotchas to apply during porting: `compute:///` defaultFS (executors can't write the driver
  FS; size APIs return 0 — measure via `oci://`), manifest commit semantics for external tables, and the
  `clusterName`-UUID pitfall when wiring jobs.
- Keep scope to AIDP-native migration. OAC and OCI networking are out of scope.
- This is a guided, human-confirmed process — no bulk automated conversion claims.

## References
- composes `aidp-ingest-file-to-table`, `aidp-notebooks`, `aidp-workspace-files`, `aidp-pipelines`,
  `aidp-profiling-tables`, `aidp-data-quality`, `aidp-federate`
- [references/oci-raw-request.md](../../references/oci-raw-request.md) · [references/no-mcp-rest-map.md](../../references/no-mcp-rest-map.md) · `scripts/aidp_sql.py`
