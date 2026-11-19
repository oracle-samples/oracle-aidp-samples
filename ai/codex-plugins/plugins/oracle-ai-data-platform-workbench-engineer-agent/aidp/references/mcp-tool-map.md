# `aidp` MCP tool map (OPTIONAL accelerator — NOT required)

> **The plugin does NOT depend on the `aidp` MCP server.** It is fully self-contained:
> - **Control-plane** ops (catalogs, schemas, tables, clusters, jobs, workspaces, roles, volumes, files,
>   credentials, sharing, git, bundle, mlops, models, agent-flows) run via `oci raw-request` against the
>   AIDP REST API — see `oci-raw-request.md` and `no-mcp-rest-map.md`.
> - **Interactive Spark-SQL / notebook cells** run via the bundled helper `$HOME/.aidp/aidp_sql.py`
>   (mints a UPST from the api_key DEFAULT profile, auto-creates a scratch notebook, returns JSON with
>   status/outputs/spark_job_ids). No `AIDP_SESSION` and no MCP required.
>
> If an `aidp` MCP server happens to be configured, you **MAY** use its tools as a convenience — they
> mirror the REST control-plane endpoints and the `aidp_sql.py` SQL path one-to-one. But this is purely
> an accelerator: nothing in the plugin assumes the MCP is present, and no skill should be framed as
> needing it. When in doubt, use `oci raw-request` (control-plane) or `$HOME/.aidp/aidp_sql.py` (SQL).

The tool list below is kept **for reference only** — it maps each MCP tool to the REST/helper path the
plugin actually relies on. (Tool names are verbatim from the MCP server's `docs/mcp_server.md`; the
server exposes 82 tools, 90 with `AIDP_MCP_ENABLE_ADMIN_TOOLS=true`. MCP tools are workspace-scoped —
pass `workspace_id` / `workspace_name` when the default workspace isn't the target.)

## Workspace / instance
`list_workspaces` · `get_workspace` · `create_workspace` · `create_aidp_instance` · `delete_aidp_instance`

## Files (workspace FS + Jupyter)
`list_files` · `upload_file` · `download_file` · `delete_file` · `create_directory` · `move_file` ·
`rename_file` · `create_notebook` · `nb_create_file` · `nb_read_file` · `nb_save_file` ·
`nb_save_notebook` · `nb_rename`

## Notebook kernel (Spark SQL / Python execution over WebSocket)
`nb_list_sessions` · `nb_create_session` · `nb_patch_session` · `nb_delete_session` · `nb_execute_code`

> **Spark SQL does NOT require these.** The plugin's own path is the bundled helper:
> `python "$HOME/.aidp/aidp_sql.py" --region <r> --datalake <ocid> --workspace <ws> --cluster <key>
> --code "<python/spark code>"` (mints a UPST from the api_key DEFAULT profile, auto-creates a scratch
> notebook, returns JSON with status/outputs/spark_job_ids — no MCP, no `AIDP_SESSION`). Smoke test:
> a `SELECT 1` cell.
> If the MCP is configured, `nb_create_session(notebook, cluster)` + `nb_execute_code("df = spark.sql('…'); df.show()")`
> is an equivalent accelerator (`nb_execute_code` keeps kernel state across calls), but it is optional.

## Clusters
`create_cluster` · `list_clusters` · `get_cluster_status` · `start_cluster` · `stop_cluster` ·
`restart_cluster` · `list_cluster_libraries` · `get_default_cluster` · `search_cluster_logs` ·
`download_cluster_logs` · `get_cluster_metrics` · `summarize_metrics_data`

## Spark UI (REST proxy — read-only diagnostics, NOT a SQL executor)
`spark_list_jobs` · `spark_get_job` · `spark_list_stages` · `spark_get_stage` · `spark_get_task_summary` ·
`spark_get_task_list` · `spark_list_executors` · `spark_list_storage_rdds` · `spark_get_storage_rdd` ·
`spark_list_sql_queries` · `spark_get_sql_query` · `spark_query_via_kernel`

> `spark_query_via_kernel` queries the Spark **REST API** from inside the kernel — it is NOT for running
> business SQL. Use `nb_execute_code` for SQL.

## Tables from file (3-step or 1-step)
`create_table_from_file` (1-step) · `upload_data_file` → `infer_schema` → `create_table` (3-step)

## Jobs / workflow
`get_job` · `create_job` · `update_job` · `delete_job` · `list_jobs` · `list_job_runs` · `run_job` ·
`get_job_run` · `cancel_job_run` · `list_task_runs` · `get_task_run` · `get_task_run_output`

> `create_job` gotchas: tasks use `type` (`NOTEBOOK_TASK`/`PYTHON_TASK`), absolute notebook paths,
> each task needs `taskKey`/`type`/`notebookPath`/`dependsOn`. **Never set `clusterName` to a UUID** —
> use the cluster name or omit it (server resolves from `clusterKey`).

## Catalog
`list_catalogs` · `get_catalog` · `list_schemas` · `list_tables` · `get_table`

## Volumes
`list_volumes` · `get_volume` · `list_volume_files` · `upload_file_with_par` · `download_file_with_par` ·
`make_volume_dir`

## Roles & observability
`list_roles` · `list_agent_flows` (read-only) · `list_async_operations` · `get_async_operation_status` ·
`wait_for_async_operation_completion` · `list_recent_activities`

## Admin (gated behind `AIDP_MCP_ENABLE_ADMIN_TOOLS=true`; MCP restart required)
`list_workspace_permissions` · `manage_workspace_permission` · `list_create_workspace_permissions` ·
`manage_create_workspace_permission` · `list_cluster_permissions` · `manage_cluster_permission` ·
`list_volume_permissions` · `manage_volume_permission`

## The REST control plane (what the plugin actually uses — `oci raw-request`)
Every control-plane MCP tool above mirrors a REST endpoint the plugin calls directly via `oci raw-request`
(pattern in `oci-raw-request.md`; tool→endpoint map in `no-mcp-rest-map.md`). Some control-plane areas have
**no MCP tool at all** and are REST-only regardless of whether the MCP is configured:
credentials · data sharing (shares/recipients) · git · agent-flow **authoring**/deploy/run ·
bundles · role **writes** (CRUD/members) · MLOps/MLflow · models catalog.
