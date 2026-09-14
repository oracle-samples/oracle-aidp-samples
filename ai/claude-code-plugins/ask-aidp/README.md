# Ask AIDP Claude Code Plugin

This plugin connects Claude Code to Oracle AI Data Platform Workbench through every documented `aidp-cli` command, OCI-signed REST endpoints, and native SDK workspace and Git tools.

It provides:

- A generic `aidp_cli` MCP tool for every `aidp-cli` command group and command.
- `aidp_cli_reference` for the generated reference of all 242 documented CLI commands across 17 command groups.
- `aidp_check_connection` for workspace and cluster smoke checks.
- `aidp_notebook_workflow` for N-notebook workflows: create commented notebooks with grouped setup/work/validation cells, create a sequential workflow, run it, export task outputs, and collect cluster logs.
- `aidp_three_notebook_workflow` as a compatibility alias for a three-notebook workflow, including dry-run planning.
- `aidp_upload_workspace_code` for SDK-backed local file or directory upload into workspace paths. Use `localBaseDir` when passing a relative `localPath` from outside the plugin directory.
- `aidp_create_git_folder` for connecting Git repos to workspace folders.
- `aidp_git_commit_push` and `aidp_git_pull` for SDK-backed Git push/pull from Git-backed workspace folders. These tools can resolve `gitRepositoryKey` from `gitFolderPath` with `WorkspaceObjectClient#listWorkspaceObjects`.
- `aidp_git_get_repository`, `aidp_git_operation_state`, `aidp_git_list_branches`, `aidp_git_create_branch`, `aidp_git_checkout_branch`, `aidp_git_list_diffs`, `aidp_git_diff_detail`, `aidp_git_merge`, `aidp_git_rebase`, and `aidp_git_reset` for native SDK Git repository workflows.
- `aidp_create_agent`, `aidp_deploy_agent`, `aidp_list_agents`, and `aidp_get_agent_session_trace` for typed Agent workflows.
- `aidp_create_ai_compute`, `aidp_list_ai_computes`, and `aidp_update_ai_compute` for typed AI Compute cluster workflows.
- `aidp_collect_logs` for existing job-run evidence collection. Use `continueOnError` for smoke tests that should preserve partial evidence when one log stream is unavailable.
- `aidp_track_runs` for notebook sessions, workflow runs, task runs, task outputs, and log downloads. Use `continueOnLogError` with `downloadLogs` for partial log evidence.
- `aidp_create_schema` for schema creation.
- `aidp_create_delta_table` for managed or external Delta table creation.
- `aidp_create_table_with_data` for creating a managed table and loading data from inline rows, a local file, or an existing object storage path.
- `aidp_generate_csv_table_sql` for `%sql CREATE TABLE ... USING CSV OPTIONS (... header 'true')` statements when CSV first rows are headers.
- `aidp_list_catalogs`, `aidp_get_catalog`, and `aidp_create_catalog` for catalog lookup and creation.
- `aidp_create_external_catalog` for external catalog creation.
- `aidp_auto_heal_workflow` for failed workflow repair/rerun using `workflow repair-job-run`.
- `aidp_create_medallion_architecture` for bronze/silver/gold schemas.
- `aidp_create_bundle` and `aidp_deploy_bundle` for bundle promotion workflows.
- `aidp_command_help` for command discovery.
- `aidp_rest_api_reference` for the generated catalog of 257 REST operations across 18 categories.
- `aidp_rest` for OCI-signed calls restricted to documented method/path pairs.

`tools/list` returns `43` tools.

## Requirements

- Claude Code with the `plugin` subcommand, verified on `2.1.250`.
- Node.js available to Claude Code (verify with `node --version`).
- OCI config and credentials that can access the target AIDP instance.
- AIDP CLI (`aidp`), required for full plugin functionality. Install the latest `aidp-cli` from
  [`oracle-samples/aidataplatform-sdk`](https://github.com/oracle-samples/aidataplatform-sdk),
  either on `PATH` or selected with `AIDP_CLI_BIN`.
- OCI CLI (`oci`), required to create the initial token when using the documented
  session-token authentication flow (`oci session authenticate`). It can be
  skipped when configuring API-key credentials directly.
- The latest `aidp-typescript-client` and `oci-common` packages when using the
  native SDK workspace upload and Git tools; `oci-common` is also required for
  signed REST calls.

AIDP CLI (`aidp`) and OCI CLI (`oci`) are different tools. Installing OCI CLI
does not install AIDP CLI. AIDP CLI defaults to `security_token` unless overridden.
For API-key authentication, explicitly set `AIDP_AUTH=api_key` so Ask-AIDP passes
the correct authentication mode to CLI calls. The environment samples below use
this API-key alternative.

This GitHub directory is the plugin's source distribution. It does **not**
contain generated `dist/` archives or a vendored `node_modules` tree. The build
script can create offline archives for a separate release process, but those
artifacts are not published here.

### Set up and verify AIDP CLI

Before connecting the plugin, follow the
[AIDP CLI installation instructions](https://github.com/oracle-samples/aidataplatform-sdk#cli)
to install the CLI and its matching SDK package. Installing this plugin from
GitHub does not install those dependencies. Then verify in the shell that will
launch Claude Code:

```sh
node --version
aidp --help
```

If `aidp` is not on `PATH`, set `AIDP_CLI_BIN` to its absolute executable path.
Verify that executable with `"$AIDP_CLI_BIN" --help` on macOS/Linux or
`& $env:AIDP_CLI_BIN --help` in PowerShell. Restart Claude Code after changing
its environment.

Without AIDP CLI, CLI-backed tools such as connection checks, notebook workflows,
and catalog operations fail. Reference tools may still respond, and direct
SDK/REST tools can work with their own dependencies and credentials, but that
does not verify full plugin functionality.

For file work in notebooks, use AIDP Workbench path patterns such as `/Volumes/<catalog>/<schema>/<volume>/<file>`, `/Workspace/<folder>/<file>`, `file:///Volumes/...`, `file:///Workspace/...`, and `oci://<bucket>@<namespace>/<folder-or-file>`.

For REST-only or advanced operations, use `aidp_rest_api_reference` to find the documented operation, then call `aidp_rest` with `dryRun: true` before a live request. The plugin signs the request with the configured OCI identity and only permits endpoint/method pairs from the generated Oracle REST catalog. The current documented endpoint version is `/20260430`.

## Install Or Update With Claude Code — Recommended

The easiest way to install or update the plugin is to prompt Claude Code with:

```text
Install or update plugin from https://github.com/oracle-samples/oracle-aidp-samples/tree/main/ai/claude-code-plugins/ask-aidp
```

After installation or update, restart Claude Code so it reloads the plugin, skill, and MCP
server.

## Install From A Repository Checkout

The plugin directory is also a single-plugin Claude Code marketplace. Clone the
repository, register that directory, and install Ask AIDP:

```sh
git clone https://github.com/oracle-samples/oracle-aidp-samples.git
claude plugin marketplace add ./oracle-aidp-samples/ai/claude-code-plugins/ask-aidp
claude plugin install ask-aidp@oracle-aidp-ask-aidp --scope user --yes
claude plugin list
```

`claude plugin list` should show `ask-aidp` installed and enabled. To inspect the installed components:

```sh
claude plugin details ask-aidp
```

After installation or update, restart Claude Code and start a new session so it
reloads the plugin, skill, and MCP server.

To update an existing installation:

```sh
claude plugin marketplace update oracle-aidp-ask-aidp
claude plugin update ask-aidp@oracle-aidp-ask-aidp
```

If you extracted a packaged marketplace on macOS and Claude Code refuses to
load it, clear quarantine metadata first (adjust the path to your extract
location):

```sh
xattr -dr com.apple.quarantine <marketplace-root>
```

### Validate before install (optional)

```sh
claude plugin validate --strict ./oracle-aidp-samples/ai/claude-code-plugins/ask-aidp
claude plugin validate ./oracle-aidp-samples/ai/claude-code-plugins/ask-aidp/.claude-plugin/marketplace.json
```

## Configure

### Configure OCI credentials

For session-token authentication, install
[OCI CLI](https://docs.oracle.com/en-us/iaas/Content/API/SDKDocs/cliinstall.htm)
and create the initial token with `oci session authenticate`. Then set
`AIDP_AUTH=security_token` and `OCI_PROFILE` to the generated profile name before
launching the assistant. AIDP CLI consumes the existing session credentials;
setting `AIDP_AUTH` does not create a token or perform the initial login. Follow
[Oracle's session-token instructions](https://docs.oracle.com/en-us/iaas/Content/API/SDKDocs/clitoken.htm)
to validate, refresh, or reauthenticate the session when needed.

For API-key authentication, OCI CLI is not required. Reuse a valid OCI profile or
follow [Oracle's API signing key instructions](https://docs.oracle.com/en-us/iaas/Content/API/Concepts/apisigningkey.htm#two):

1. Use an IAM user with permission to access the target AIDP resources and upload
   the public API signing key under that user's API Keys in OCI Console.
2. Save the generated profile in `~/.oci/config` on macOS/Linux or
   `%USERPROFILE%\.oci\config` on Windows. Include `user`, `tenancy`,
   `fingerprint`, `region`, and `key_file`, pointing to the private PEM key.
3. Restrict private-key access to your user. Set `AIDP_AUTH=api_key` and
   `OCI_PROFILE` to the profile name; set `OCI_CONFIG_FILE` for a custom location.

If OCI CLI is already installed, `oci setup config` can also help create an
API-key profile.

### Set the plugin environment

Provide AIDP/OCI settings through environment variables in the shell you launch Claude Code
from. The plugin's `.mcp.json` does not inject environment variables, so the MCP server sees
only what it inherits from the Claude Code process -- load them before launching `claude`, and
restart an already-running session after changing them. Copy the sample env file and fill in
your own values:

```sh
cp examples/aidp.env.sample ./aidp.env
chmod 600 ./aidp.env
# edit ./aidp.env and replace every placeholder
source ./aidp.env
claude
```

The manual equivalent is:

```sh
export AIDP_ENDPOINT="https://aidp.<region>.oci.oraclecloud.com"
export AIDP_OCID="ocid1.aidataplatform..."
export AIDP_WORKSPACE_KEY="<workspace-key>"
export AIDP_CLUSTER_KEY="<cluster-key>"
export OCI_PROFILE="DEFAULT"
export AIDP_AUTH="api_key"
```

Optional variables:

```sh
export OCI_CONFIG_FILE="$HOME/.oci/config"
export OCI_REGION="us-ashburn-1"
export AIDP_CLI_BIN="/absolute/path/to/aidp"
export AIDP_CLUSTER_NAME="<cluster-display-name>"
export AIDP_TIMEOUT_SECONDS="60"
```

On Windows PowerShell, set the same variables with `$env:NAME = "value"` before launching `claude`, and use `[Environment]::SetEnvironmentVariable("NAME", "VALUE", "User")` to persist them.

Never commit a completed `aidp.env` or any OCI private key.

## Verify

Inside Claude Code:

```text
/plugin
```

Then prompt:

```text
Use Ask AIDP to verify my workspace and cluster connection.
```

```text
Use Ask AIDP to look up AIDP CLI command reference.
```

The workspace check should succeed, the cluster check should succeed when `AIDP_CLUSTER_KEY` is present, and the CLI and REST reference tools should respond.

Maintainers and users working from a repository checkout can also run the
static QA suite. It does not contact AIDP or OCI:

```sh
cd ai/claude-code-plugins/ask-aidp
node scripts/qa-claude.mjs
```

Expected: `"ok": true`, 43 MCP tools, 242 CLI commands, and 257 REST
operations.

## Build Offline Archives — Maintainers

The package build fails if its vendor source does not contain `aidp-cli`,
`aidp-typescript-client`, and `oci-common`; this prevents publishing an archive
that cannot run offline. On macOS or Linux:

```sh
AIDP_VENDOR_NODE_MODULES=/absolute/path/to/node_modules \
  node scripts/package-claude-plugin.mjs
```

On Windows PowerShell:

```powershell
$env:AIDP_VENDOR_NODE_MODULES = "C:\absolute\path\to\node_modules"
node scripts/package-claude-plugin.mjs
```

The archives, environment sample, install guide, and SHA-256 checksum file are
written to `dist/`. The archive extracts to a self-contained marketplace root;
register it with `claude plugin marketplace add <extracted-dir>`.

## Use

Example prompts:

```text
Use Ask AIDP to check my workspace connection.
```

```text
Use Ask AIDP to create five sample notebooks, run them in sequence, and download logs.
```

```text
Use Ask AIDP to run: workflow list-jobs <workspace-key> --limit 10
```

```text
Use Ask AIDP to look up the docs for schema create-table.
```

```text
Use Ask AIDP to search the AIDP CLI reference for registered model commands.
```

```text
Use Ask AIDP to create a 7-notebook workflow and run it.
```

```text
Use Ask AIDP to upload this local src directory into /Workspace/project/src.
```

```text
Use Ask AIDP to connect my workspace to this Git repository on branch main.
```

```text
Use Ask AIDP to create bronze, silver, and gold schemas for my lakehouse catalog.
```

```text
Use Ask AIDP to create a bundle for this job and deploy the bundle.
```

```text
Use Ask AIDP to create and deploy an agent in my workspace.
```

```text
Use Ask AIDP to list my AI Compute clusters.
```

```text
Use Ask AIDP to search the REST API reference for agent operations.
```

For generic commands, the plugin passes an argument array to `aidp-cli` and appends common endpoint, instance, profile, auth, and timeout flags from the environment.

The generated CLI reference covers all 242 current documented commands in these groups: `agent`, `async-operations`, `audit`, `bundle`, `catalog`, `cluster`, `credentials`, `delta-share`, `mlops`, `notebook`, `role`, `schema`, `user-setting`, `volume`, `workflow`, `workspace`, and `workspace-object`. Use `aidp_cli_reference` to list groups, list commands in a group, fetch one command reference, or search all documented commands. The generated REST reference covers all 257 current documented `/20260430` operations across 18 categories.

Convenience tool prompts:

```text
Use Ask AIDP to create a schema named finance_sandbox in my catalog.
```

```text
Use Ask AIDP to list all active external catalogs sorted by display name.
```

```text
Use Ask AIDP to get catalog details for <catalog-key>.
```

```text
Use Ask AIDP to dry-run creating an internal catalog named finance_lakehouse.
```

```text
Use Ask AIDP to create a managed Delta table with id and amount columns.
```

```text
Use Ask AIDP to create a managed table named sample_orders and insert these rows into it.
```

```text
Use Ask AIDP to dry-run creating an ADW external catalog with these connection properties.
```

The create-schema, create-Delta-table, create-table-with-data, create-catalog, and create-external-catalog tools support `dryRun` so users can inspect the generated `aidp-cli` command and JSON body before creating resources. Catalog list/get tools also support `dryRun` for command inspection. Prefer `dryRun: true` before any mutating operation.

`aidp_create_table_with_data` wraps `aidp schema create-data-table`. It creates a new managed table and loads initial data from one of:

- `rows`, which the tool stages as CSV using `schema generate-temp-file-upload-target`;
- `localDataFile`, which the tool stages through the same temporary upload target;
- `objectStorageLocationPath`, when the data file is already in object storage.

For inline `rows`, the staged CSV is headerless because `create-data-table` reads the source positionally. The tool uses `_c0`, `_c1`, and so on in `selectedColumns`; `tableFields` retains the real column names and types.

This is an initial-load workflow for a newly-created managed table. For appending rows to an existing table, use `aidp_cli` or a notebook/workflow if that operation is exposed by your AIDP environment.

Auto-heal workflow prompt:

```text
Use Ask AIDP to dry-run auto-healing job run <job-run-key>.
```

`aidp_auto_heal_workflow` inspects the job run, selects failed task keys by default, and wraps `aidp workflow repair-job-run`. It can also accept explicit `taskKeys`, rerun parameters, and `pollToCompletion`.

## Evidence

Workflow runs create a local evidence directory with:

- request JSON files;
- raw CLI responses;
- `commands-and-responses.md`;
- task output exports;
- cluster log responses;
- `summary.json`.

## Limitations

- The plugin wraps `aidp-cli`; it does not bypass CLI behavior, validation, throttling, or service-side permissions.
- Live operations require network access to OCI and valid OCI credentials.
- The plugin does not manage OCI key generation or IAM policy setup.
- `aidp_cli` is intentionally argument-array based and does not execute shell pipelines, redirects, or scripts.
- Long-running workflows depend on cluster availability and can time out if the service run exceeds the configured polling timeout.
- Task output export depends on AIDP returning an output key for the task run.
- `aidp_create_table_with_data` uses `schema create-data-table`, which creates a managed table with an initial data load. It is not a general-purpose append/merge command for existing tables.
- Packaged archives include `aidp-cli`, `aidp-typescript-client`, and `oci-common`; packaging fails instead of creating an incomplete offline archive when any required dependency is missing.
