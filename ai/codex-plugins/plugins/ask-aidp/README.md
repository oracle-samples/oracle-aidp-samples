# Ask AIDP Codex Plugin

This plugin connects Codex to Oracle AI Data Platform Workbench through every documented `aidp-cli` command, OCI-signed REST endpoints, and native SDK Git tools. Use it to create workflows, AI Compute clusters, agents, catalogs, schemas, tables, bundles, and notebooks, as well as upload workspace code and collect run logs.

## Requirements

- Codex CLI with the `plugin` subcommand and Node.js available to Codex. The
  installation flow is verified on `codex-cli 0.147.0`.
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

This GitHub directory is the plugin's source distribution. It does **not** contain
generated `dist/` tarballs, zip files, or a vendored `node_modules` tree. Install the
plugin from the GitHub marketplace as described below. The build script can create
offline archives for a separate release process, but those archives are not published
here.

### Set up and verify AIDP CLI

Before connecting the plugin, follow the
[AIDP CLI installation instructions](https://github.com/oracle-samples/aidataplatform-sdk#cli)
to install the CLI and its matching SDK package. Installing this plugin from
GitHub does not install those dependencies. Then verify in the shell that will
launch Codex:

```sh
node --version
aidp --help
```

If `aidp` is not on `PATH`, set `AIDP_CLI_BIN` to its absolute executable path.
Verify that executable with `"$AIDP_CLI_BIN" --help` on macOS/Linux or
`& $env:AIDP_CLI_BIN --help` in PowerShell. Restart Codex after changing its
environment.

Without AIDP CLI, CLI-backed tools such as connection checks, notebook workflows,
and catalog operations fail. Reference tools may still respond, and direct
SDK/REST tools can work with their own dependencies and credentials, but that
does not verify full plugin functionality.

For file work in notebooks, use AIDP Workbench path patterns such as `/Volumes/<catalog>/<schema>/<volume>/<file>`, `/Workspace/<folder>/<file>`, `file:///Volumes/...`, `file:///Workspace/...`, and `oci://<bucket>@<namespace>/<folder-or-file>`.

For REST-only or advanced operations, use `aidp_rest_api_reference` to find the documented operation, then call `aidp_rest` with `dryRun: true` before a live request. The plugin signs the request with the configured OCI identity and only permits endpoint/method pairs from the generated Oracle REST catalog. The current documented endpoint version is `/20260430`.

## Install Or Update With Codex — Recommended

The easiest way to install or update the plugin is to prompt Codex with:

```text
Install or update plugin from https://github.com/oracle-samples/oracle-aidp-samples/tree/main/ai/codex-plugins/plugins/ask-aidp
```

After installation or update, restart the Codex app so it reloads the plugin,
skill, and MCP server.

## Install From GitHub Manually

The same marketplace commands work on macOS, Linux, and Windows. Register the
Oracle AIDP marketplace once:

```bash
codex plugin marketplace add oracle-samples/oracle-aidp-samples \
    --ref main \
    --sparse .agents \
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

Restart the Codex app after installation, then start a new thread so the Ask AIDP
skill and MCP tools are loaded. You do not need to download an archive, extract a
plugin directory, or create `~/.agents/plugins/marketplace.json` manually.

To update an existing installation:

```bash
codex plugin marketplace upgrade oracle-aidp-codex
codex plugin add ask-aidp@oracle-aidp-codex
```

Restart the Codex app, then start another new thread.

## Verify The Install

Confirm that Codex installed and enabled version `0.9.1` and registered the MCP
server:

```bash
codex plugin list
codex mcp list
```

Maintainers and users working from a repository checkout can run the static QA
suite. It does not contact AIDP or OCI, but it requires `aidp` on `PATH` or
`AIDP_CLI_BIN` to point to the CLI executable:

```bash
cd ai/codex-plugins/plugins/ask-aidp
node scripts/qa.mjs
```

Expected: `"ok": true`, 43 MCP tools, 242 CLI commands, and 257 REST operations.

## Configure AIDP

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

Use [`examples/aidp.env.sample`](./examples/aidp.env.sample) as the template. Do
not distribute a completed `aidp.env`, because it identifies a user's OCI profile,
AIDP instance, workspace, and cluster.

The MCP configuration does not inject environment variables. Configure them in
the shell that launches Codex and restart an already-running Codex app after
changing them.

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

## Build Offline Archives — Maintainers

The package build fails if its vendor source does not contain `aidp-cli`,
`aidp-typescript-client`, and `oci-common`; this prevents publishing an archive
that cannot run offline. On macOS or Linux, run from the repository root:

```sh
AIDP_VENDOR_NODE_MODULES=/absolute/path/to/node_modules \
  node ai/codex-plugins/plugins/ask-aidp/scripts/package-plugin.mjs
```

On Windows PowerShell:

```powershell
$env:AIDP_VENDOR_NODE_MODULES = "C:\absolute\path\to\node_modules"
node ai/codex-plugins/plugins/ask-aidp/scripts/package-plugin.mjs
```

The archives, environment sample, and SHA-256 checksum file are written to
`ai/codex-plugins/plugins/ask-aidp/dist/`.

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
Use Ask AIDP to create an AI Compute cluster named support-agent-compute with shape VM.GPU.A10.1, 8 OCPUs, 64 GB memory, and one to two replicas.
```

```text
Use Ask AIDP to list my agents and retrieve the trace for this agent session message.
```

For generic commands, the plugin passes an argument array to `aidp-cli` and appends common endpoint, instance, profile, auth, and timeout flags from the environment. The plugin also has typed `aidp_create_agent`, `aidp_deploy_agent`, `aidp_list_agents`, and `aidp_get_agent_session_trace` helpers for common Agent work.

The generated CLI reference covers all 242 current documented commands in these groups: `agent`, `async-operations`, `audit`, `bundle`, `catalog`, `cluster`, `credentials`, `delta-share`, `mlops`, `notebook`, `role`, `schema`, `user-setting`, `volume`, `workflow`, `workspace`, and `workspace-object`. Use `aidp_cli_reference` to list groups, list commands in a group, fetch one command reference, or search all documented commands. The generated REST reference covers all 257 current documented `/20260430` operations across 18 categories. Use `aidp_rest_api_reference` to search or inspect an endpoint, then use `aidp_rest` for the signed call.

AI Compute convenience tool prompts:

```text
Use Ask AIDP to dry-run creating an AI Compute cluster named inference-compute with one to three replicas.
```

```text
Use Ask AIDP to list all my AI Compute clusters.
```

```text
Use Ask AIDP to update AI Compute cluster <cluster-key> to use two replicas.
```

For any REST operation without a dedicated helper:

```text
Use Ask AIDP to search the REST reference for "create credential", dry-run the matching request, then run it.
```

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

For inline `rows`, the staged CSV is headerless because `create-data-table` reads
the source positionally. The tool uses `_c0`, `_c1`, and so on in
`selectedColumns`; `tableFields` retains the real column names and types.

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
  `PATH`; native workspace upload and Git tools additionally require
  `aidp-typescript-client` and `oci-common` to be resolvable by the MCP server.
