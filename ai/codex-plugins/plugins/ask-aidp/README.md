# Ask AIDP Codex Plugin

This plugin connects Codex to Oracle AI Data Platform Workbench through CLI commands with a REST API as a backup. Use this for general work such as to create workflows, clusters and even upload notebooks.

## Requirements

- Codex CLI with plugin support and Node.js available to Codex.
- OCI config and credentials that can access the target AIDP instance.
- The latest `aidp-cli` from
  [`oracle-samples/aidataplatform-sdk`](https://github.com/oracle-samples/aidataplatform-sdk),
  either on `PATH` or selected with `AIDP_CLI_BIN`.
- The latest `aidp-typescript-client` and `oci-common` packages when using the
  native SDK workspace Git tools.

This GitHub directory is the plugin's source distribution. It does **not** contain
generated `dist/` tarballs, zip files, or a vendored `node_modules` tree. Install the
plugin from the GitHub marketplace as described below. The build script can create
offline archives for a separate release process, but those archives are not published
here.

For file work in notebooks, use AIDP Workbench path patterns such as `/Volumes/<catalog>/<schema>/<volume>/<file>`, `/Workspace/<folder>/<file>`, `file:///Volumes/...`, `file:///Workspace/...`, and `oci://<bucket>@<namespace>/<folder-or-file>`.

If both `aidp-cli` and the TypeScript SDK fail for an operation, use `aidp_rest_api_reference` to verify the documented Oracle AI Data Platform Workbench REST API coverage before a REST fallback. The current documented endpoint version is `/20260430`; Oracle's June 2026 update added SDK and CLI resource links, and no later REST endpoint version is listed.

## Install From GitHub

The same marketplace commands work on macOS, Linux, and Windows. Register the
Oracle AIDP marketplace once:

```bash
codex plugin marketplace add oracle-samples/oracle-aidp-samples \
    --ref main \
    --sparse ai/codex-plugins
```

Install Ask AIDP:

```bash
codex plugin add ask-aidp@oracle-aidp-codex
```

Verify the installation:

```bash
codex plugin list
```

Start a new Codex thread after installation so the Ask AIDP skill and MCP tools
are loaded. You do not need to download an archive, extract a plugin directory,
or create `~/.agents/plugins/marketplace.json` manually.

To update an existing installation:

```bash
codex plugin marketplace upgrade oracle-aidp-codex
codex plugin add ask-aidp@oracle-aidp-codex
```

Then start another new Codex thread.

## Configure AIDP

Use [`examples/aidp.env.sample`](./examples/aidp.env.sample) as the template. Do
not distribute a completed `aidp.env`, because it identifies a user's OCI profile,
AIDP instance, workspace, and cluster.

On macOS or Linux, download and load the sample from the shell that launches
Codex:

```sh
curl -L \
  https://raw.githubusercontent.com/oracle-samples/oracle-aidp-samples/main/ai/codex-plugins/plugins/ask-aidp/examples/aidp.env.sample \
  -o aidp.env.sample
cp aidp.env.sample aidp.env
chmod 600 aidp.env
# Replace every placeholder before loading the file.
source ./aidp.env
```

On Windows PowerShell, download the sample as a reference and set the equivalent
environment variables in the PowerShell session that launches Codex:

```powershell
Invoke-WebRequest `
  -Uri "https://raw.githubusercontent.com/oracle-samples/oracle-aidp-samples/main/ai/codex-plugins/plugins/ask-aidp/examples/aidp.env.sample" `
  -OutFile ".\aidp.env.sample"

$env:AIDP_ENDPOINT = "https://aidp.<region>.oci.oraclecloud.com"
$env:AIDP_OCID = "ocid1.aidataplatform..."
$env:AIDP_WORKSPACE_KEY = "<workspace-key>"
$env:AIDP_CLUSTER_KEY = "<cluster-key>"
$env:OCI_PROFILE = "DEFAULT"
$env:AIDP_AUTH = "api_key"
```

Set `AIDP_CLI_BIN` when `aidp` is not already on `PATH`:

```sh
export AIDP_CLI_BIN="/absolute/path/to/aidp"
```

```powershell
$env:AIDP_CLI_BIN = "C:\path\to\aidp.cmd"
```

If `oci login` does not work, configure OCI API key authentication using
[Oracle's API signing key instructions](https://docs.oracle.com/en-us/iaas/Content/API/Concepts/apisigningkey.htm#two).
Set `AIDP_AUTH=api_key`, select the profile with `OCI_PROFILE`, and set
`OCI_CONFIG_FILE` when the OCI config is not in its default location.

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
Use Ask AIDP to create an agent from /Workspace/agents/support_agent and deploy it on my agent compute.
```

```text
Use Ask AIDP to list my agents and retrieve the trace for this agent session message.
```

For generic commands, the plugin passes an argument array to `aidp-cli` and appends common endpoint, instance, profile, auth, and timeout flags from the environment. The plugin also has typed `aidp_create_agent`, `aidp_deploy_agent`, `aidp_list_agents`, and `aidp_get_agent_session_trace` helpers for common Agent work.

The generated CLI reference covers all 242 current documented commands in these groups: `agent`, `async-operations`, `audit`, `bundle`, `catalog`, `cluster`, `credentials`, `delta-share`, `mlops`, `notebook`, `role`, `schema`, `user-setting`, `volume`, `workflow`, `workspace`, and `workspace-object`. Use `aidp_cli_reference` to list groups, list commands in a group, fetch one command reference, or search all documented commands. Use `aidp_rest_api_reference` to verify REST version `/20260430`, category coverage, and the official REST catalog before using a REST fallback.

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

The create-schema, create-Delta-table, create-table-with-data, create-catalog, and create-external-catalog tools support `dryRun` so users can inspect the generated `aidp-cli` command and JSON body before creating resources. Catalog list/get tools also support `dryRun` for command inspection.

`aidp_create_table_with_data` wraps `aidp schema create-data-table`. It creates a new managed table and loads initial data from one of:

- `rows`, which the tool stages as CSV using `schema generate-temp-file-upload-target`;
- `localDataFile`, which the tool stages through the same temporary upload target;
- `objectStorageLocationPath`, when the data file is already in object storage.

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
- The GitHub marketplace source does not include generated archives or vendored
  Node dependencies. Install `aidp-cli` separately and set `AIDP_CLI_BIN` or
  `PATH`; native workspace Git tools additionally require
  `aidp-typescript-client` and `oci-common` to be resolvable by the MCP server.
