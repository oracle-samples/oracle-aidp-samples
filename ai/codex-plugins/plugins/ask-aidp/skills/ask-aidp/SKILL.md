---
name: ask-aidp
description: Use when the user asks Codex to connect to Oracle AI Data Platform Workbench, run aidp-cli commands, manage AIDP agents/notebooks/workflows/clusters/catalogs/schemas/volumes/MLOps/bundles/audit/roles/credentials, execute a notebook workflow, export task outputs, or collect AIDP logs.
---

# Ask AIDP

Use this skill for Oracle AI Data Platform Workbench tasks that should be performed with `aidp-cli`.

## Preferred Tooling

When the Ask AIDP MCP tools are available, prefer them over shelling out manually:

- `aidp_check_connection`: verify workspace and optional cluster access.
- `aidp_notebook_workflow`: create any N notebooks, create a sequential workflow, run it, export task outputs, and collect cluster logs.
- `aidp_three_notebook_workflow`: compatibility alias for a three-notebook workflow.
- `aidp_upload_workspace_code`: upload local code files or a directory tree to an AIDP workspace path.
- `aidp_create_git_folder`: connect to Git and fetch code into a Git-backed AIDP workspace folder.
- `aidp_git_commit_push`: use the native TypeScript SDK `GitClient` to stage files, commit, and push a Git-backed workspace folder.
- `aidp_git_pull`: use the native TypeScript SDK `GitClient` to pull remote changes into a Git-backed workspace folder.
- `aidp_git_get_repository`: use the native TypeScript SDK `GitClient` to fetch connected repository metadata.
- `aidp_git_operation_state`: use the native TypeScript SDK `GitClient` to inspect Git worktree or operation state.
- `aidp_git_list_branches`, `aidp_git_create_branch`, and `aidp_git_checkout_branch`: use the native TypeScript SDK `GitClient` for branch workflows.
- `aidp_git_list_diffs` and `aidp_git_diff_detail`: use the native TypeScript SDK `GitClient` for workspace Git diff review.
- `aidp_git_merge`, `aidp_git_rebase`, and `aidp_git_reset`: use the native TypeScript SDK `GitClient` for advanced Git operations.
- `aidp_create_agent`: create an AIDP agent with common typed inputs and optional agent-card, diagram, guardrail, and session configuration objects.
- `aidp_deploy_agent`: deploy an AIDP agent with typed compute, OAuth, and session-retention inputs.
- `aidp_list_agents`: list agents with typed display-name, compute, pagination, and sort filters.
- `aidp_get_agent_session_trace`: retrieve a trace for an agent session message.
- `aidp_collect_logs`: collect logs for an existing workflow run.
- `aidp_track_runs`: track workflow runs, task runs, notebook sessions, task outputs, and logs.
- `aidp_create_schema`: create a schema from catalog name and display name.
- `aidp_create_delta_table`: create a managed or external Delta table from catalog/schema keys and typed column inputs.
- `aidp_create_table_with_data`: create a managed table and load initial data from inline rows, a local data file, or an existing object storage path.
- `aidp_generate_csv_table_sql`: generate `%sql CREATE TABLE ... USING CSV OPTIONS (... header 'true')` for CSV-backed tables whose first row contains headers.
- `aidp_list_catalogs`: list catalogs with typed filters for display name, state, type, pagination, and sort order.
- `aidp_get_catalog`: get details for one catalog by key or GUID.
- `aidp_create_catalog`: create an internal or external catalog with simplified typed inputs.
- `aidp_create_external_catalog`: create an external catalog with source type and connection properties.
- `aidp_auto_heal_workflow`: inspect failed workflow task runs and repair/rerun selected tasks with `workflow repair-job-run`.
- `aidp_create_medallion_architecture`: create bronze/silver/gold medallion schemas from scratch.
- `aidp_create_bundle`: create bundles for jobs or agent flows.
- `aidp_deploy_bundle`: deploy bundles and optionally fetch deployment status.
- `aidp_cli`: run any `aidp-cli` command group or command not covered by the higher-level tools.
- `aidp_command_help`: inspect `aidp-cli` command groups and command help.
- `aidp_cli_reference`: inspect the generated command reference for all documented CLI command groups and commands from the aidataplatform-sdk CLI README.
- `aidp_rest_api_reference`: inspect the current REST API version, category coverage, and official REST reference links.

If tools are not available, use `aidp-cli` directly with argument arrays or shell commands. For workspace Git repository push, pull, status, diff, branch, merge, rebase, and reset operations, prefer the native SDK-backed Git tools because those operations are not covered by `aidp-cli`.

## Full CLI Coverage

The plugin supports all documented `aidp-cli` command groups through `aidp_cli`, and the generated command catalog in `CLI_COMMANDS.md` lists every documented command. Use `aidp_cli_reference` before a raw `aidp_cli` call when you need usage, request-body fields, command summaries, or to search the full command catalog.

Documented command groups covered by the plugin:

- `agent`
- `async-operations`
- `audit`
- `bundle`
- `catalog`
- `cluster`
- `credentials`
- `delta-share`
- `mlops`
- `notebook`
- `role`
- `schema`
- `user-setting`
- `volume`
- `workflow`
- `workspace`
- `workspace-object`

## Configuration

Prefer environment variables so commands are reproducible:

- `AIDP_ENDPOINT`
- `AIDP_OCID` or `AIDP_INSTANCE_ID`
- `AIDP_WORKSPACE_KEY`
- `AIDP_CLUSTER_KEY`
- `OCI_PROFILE`
- `OCI_CONFIG_FILE`
- `AIDP_AUTH` such as `api_key`, `security_token`, `instance_principal`, or `resource_principal`
- `AIDP_CLI_BIN` when `aidp` is not on `PATH`

If `oci login` or security-token authentication does not work for the user, guide them to configure OCI API key authentication with Oracle's API signing key process: <https://docs.oracle.com/en-us/iaas/Content/API/Concepts/apisigningkey.htm#two>. The required setup is an IAM user with the needed AIDP permissions, a PEM RSA API signing key pair, the public key uploaded in OCI Console under User settings > API Keys, the generated config snippet copied into `~/.oci/config` or `%USERPROFILE%\.oci\config`, `key_file` updated to the private key path, and private-key permissions restricted to the user. Then set `AIDP_AUTH=api_key`, `OCI_PROFILE` to that config profile, and `OCI_CONFIG_FILE` when the config file is not in the default location.

For live operations that create or update resources, write evidence to a local run folder and report:

- notebook paths
- job key
- job run key
- final job run status
- task run statuses
- transcript path
- log/output artifact paths

When generating notebooks, organize code into focused cells with comments. Keep setup/common values together, keep related work together, and keep validation/output markers together. For SQL notebooks, preserve `%sql` as the first line of the SQL cell and use SQL comments (`-- ...`) inside that cell.

For catalog/schema/table creation and lookup, prefer the dedicated convenience tools when their input shape fits. Use `aidp_list_catalogs` for "show/list/get catalogs", `aidp_get_catalog` for one catalog, `aidp_create_catalog` for general catalog creation, and `aidp_create_external_catalog` when the user specifically wants an external catalog. Use `dryRun: true` first when the user is deciding on names, keys, table columns, source type, connection properties, or sample data. Use `aidp_create_table_with_data` when the user asks to create a table and insert or load initial data into it. Be clear that this wraps `schema create-data-table` for a new managed table with an initial load; for appending to an existing table, use `aidp_cli` or a notebook/workflow if the environment exposes that operation.

For CSV-backed tables where the first row should be treated as column headers, prefer `aidp_generate_csv_table_sql` and run the generated SQL in an AIDP notebook or workflow. The generated SQL should use `USING CSV` and `OPTIONS (path '<path>', header 'true')`, matching the documented notebook SQL pattern.

For workflow healing, prefer `aidp_auto_heal_workflow`. Use `dryRun: true` first unless the user clearly asks to perform the repair. If task keys are omitted, the tool selects task runs with failed/error statuses from the target job run.

For workspace code, use `aidp_upload_workspace_code` for local files/directories and `aidp_create_git_folder` for Git-backed workspace folders. For Git push/pull/status/diff/branch workflows inside a Git-backed workspace folder, use the `aidp_git_*` tools; these route through the TypeScript SDK `GitClient` while the rest of the plugin remains CLI-backed. Prefer passing `gitFolderPath`; when `gitRepositoryKey` is omitted, the plugin resolves it with `WorkspaceObjectClient#listWorkspaceObjects` and the Git folder metadata `repoKey`. Use dry-run before uploading many files or performing mutating Git operations.

For file paths in notebooks, follow AIDP Workbench file access patterns:

- Volumes: `/Volumes/<catalog>/<schema>/<volume>/<file>` for POSIX-style volume access.
- Workspace files: `/Workspace/<folder>/<file>` for POSIX-style workspace access.
- URI-style local paths: `file:///Volumes/...` or `file:///Workspace/...`.
- OCI Object Storage: `oci://<bucket>@<namespace>/<folder-or-file>`.

Use Spark reads such as `spark.read.csv(path, header=True, inferSchema=True, sep=",")` or pandas reads such as `pd.read_csv(path, header=0, sep=",")` when generating notebook examples.

For medallion architecture, use `aidp_create_medallion_architecture` to create bronze, silver, and gold schemas, then use `aidp_create_delta_table` to add empty Delta tables or `aidp_create_table_with_data` to seed managed tables with initial data in each layer.

For bundle workflows, use `aidp_create_bundle`, `aidp_deploy_bundle`, and `aidp_cli` or `aidp_command_help` for advanced bundle sync/purge operations.

If an operation fails through both `aidp-cli` and the TypeScript SDK, REST API fallback is acceptable when the operation is documented in the Oracle AI Data Platform Workbench REST API catalog. The current REST API version is `/20260430`; Oracle's June 2026 update added SDK and CLI resource links, while the May 2026 release introduced the REST API documentation and no later REST endpoint version is listed. Use `aidp_rest_api_reference` before a fallback, OCI request signing, and the same endpoint/config values already used for SDK calls. Report that REST was used as a fallback and include the endpoint, method, request body path, and response evidence.

## Safety

`aidp_cli` accepts an argument array, not a shell string. Do not embed shell operators. Use the `body` argument for JSON request bodies instead of writing one-off shell heredocs.
