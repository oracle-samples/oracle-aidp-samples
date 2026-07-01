# Ask AIDP Codex Plugin

This plugin connects Codex to Oracle AI Data Platform Workbench through `aidp-cli`. Workspace Git repository actions that are not available in `aidp-cli` use the native AIDP TypeScript SDK `GitClient`.

It provides:

- A generic `aidp_cli` MCP tool for every `aidp-cli` command group and command.
- `aidp_cli_reference` for the generated reference of all 215 documented CLI commands across 16 command groups.
- `aidp_check_connection` for workspace and cluster smoke checks.
- `aidp_notebook_workflow` for N-notebook workflows: create commented notebooks with grouped setup/work/validation cells, create a sequential workflow, run it, export task outputs, and collect cluster logs.
- `aidp_three_notebook_workflow` as a compatibility alias for a three-notebook workflow.
- `aidp_upload_workspace_code` for local file or directory upload into workspace paths.
- `aidp_create_git_folder` for connecting Git repos to workspace folders.
- `aidp_git_commit_push` and `aidp_git_pull` for SDK-backed Git push/pull from Git-backed workspace folders. These tools can resolve `gitRepositoryKey` from `gitFolderPath` with `WorkspaceObjectClient#listWorkspaceObjects`.
- `aidp_git_get_repository`, `aidp_git_operation_state`, `aidp_git_list_branches`, `aidp_git_create_branch`, `aidp_git_checkout_branch`, `aidp_git_list_diffs`, `aidp_git_diff_detail`, `aidp_git_merge`, `aidp_git_rebase`, and `aidp_git_reset` for native SDK Git repository workflows.
- `aidp_collect_logs` for existing job-run evidence collection.
- `aidp_track_runs` for notebook sessions, workflow runs, task runs, task outputs, and log downloads.
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

## Requirements

- Codex with plugin support.
- Node.js available to Codex.
- OCI config and credentials that can access the target AIDP instance.
- `aidp-cli` available through one of:
  - the packaged plugin archive, which vendors `aidp-cli`;
  - `AIDP_CLI_BIN=/absolute/path/to/aidp`;
  - `aidp` on `PATH`.
- `aidp-typescript-client` and `oci-common` are vendored in the packaged plugin archive for native SDK Git tools. When running from source, install or vendor the same Node dependencies.

The distribution folder includes `aidp.env.sample`. Send this sample file with the install guide, but do not send a completed `aidp.env`.

This repository copy also includes release binaries under `dist/` for offline distribution: the macOS/Linux tarball, Windows zip, checksum file, install guide, and `aidp.env.sample`.

For file work in notebooks, use AIDP Workbench path patterns such as `/Volumes/<catalog>/<schema>/<volume>/<file>`, `/Workspace/<folder>/<file>`, `file:///Volumes/...`, `file:///Workspace/...`, and `oci://<bucket>@<namespace>/<folder-or-file>`.

If both `aidp-cli` and the TypeScript SDK fail for an operation, fall back to the documented Oracle AI Data Platform Workbench REST API only when the needed endpoint is present in the REST catalog.

## Simplest Install With Codex

One of the easiest ways to install the plugin is to download the release binaries, then ask Codex to install them for you.

Download these files into the same local folder, such as `Downloads/ask-aidp`:

- `ask-aidp-codex-plugin-0.7.2.tar.gz` on macOS or Linux, or `ask-aidp-codex-plugin-0.7.2.zip` on Windows.
- `ask-aidp-codex-plugin-0.7.2.sha256`.
- `aidp.env.sample`.
- This install guide.

Then start a Codex session and prompt it with the folder path:

```text
Install the Ask AIDP plugin from the downloaded binaries in ~/Downloads/ask-aidp.
Use the archive for my operating system, verify the checksum if possible, install it into my personal Codex plugin marketplace, and tell me when to start a new Codex thread.
```

On Windows, use a Windows path:

```text
Install the Ask AIDP plugin from the downloaded binaries in C:\Users\<user>\Downloads\ask-aidp.
Use the zip archive, verify the checksum if possible, install it into my personal Codex plugin marketplace, and tell me when to start a new Codex thread.
```

After installation, ask Codex to help configure AIDP access:

```text
Read aidp.env.sample from the same folder, help me create aidp.env with my AIDP endpoint, instance OCID, workspace key, cluster key, OCI profile, and API key authentication settings.
```

## Install On macOS

Use the tarball on macOS:

```sh
mkdir -p "$HOME/plugins"
tar -xzf ask-aidp-codex-plugin-0.7.2.tar.gz -C "$HOME/plugins"
```

The extracted plugin directory is:

```text
$HOME/plugins/ask-aidp
```

If `~/.agents/plugins/marketplace.json` does not exist, create it:

```sh
mkdir -p "$HOME/.agents/plugins"
cat > "$HOME/.agents/plugins/marketplace.json" <<'JSON'
{
  "name": "personal",
  "interface": {
    "displayName": "Personal"
  },
  "plugins": [
    {
      "name": "ask-aidp",
      "source": {
        "source": "local",
        "path": "./plugins/ask-aidp"
      },
      "policy": {
        "installation": "AVAILABLE",
        "authentication": "ON_INSTALL"
      },
      "category": "Productivity"
    }
  ]
}
JSON
```

If the file already exists, add this entry to its `plugins` array:

```json
{
  "name": "ask-aidp",
  "source": {
    "source": "local",
    "path": "./plugins/ask-aidp"
  },
  "policy": {
    "installation": "AVAILABLE",
    "authentication": "ON_INSTALL"
  },
  "category": "Productivity"
}
```

Then install from the personal marketplace:

```sh
codex plugin add ask-aidp@personal
```

Configure AIDP/OCI for the current shell:

You can either export the variables manually or copy the sample file:

```sh
cp aidp.env.sample ./aidp.env
chmod 600 ./aidp.env
```

Edit `./aidp.env`, replace every placeholder, then load it:

```sh
source ./aidp.env
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

Optional macOS variables:

```sh
export OCI_CONFIG_FILE="$HOME/.oci/config"
export OCI_REGION="us-ashburn-1"
export AIDP_CLI_BIN="/absolute/path/to/aidp"
export AIDP_CLUSTER_NAME="<cluster-display-name>"
export AIDP_TIMEOUT_SECONDS="60"
```

If `oci login` does not work in your environment, use OCI API key authentication instead. Follow Oracle's API signing key setup at <https://docs.oracle.com/en-us/iaas/Content/API/Concepts/apisigningkey.htm#two>: create or use an IAM user with the required AIDP permissions, generate a PEM RSA API signing key pair, upload the public key in OCI Console under User settings > API Keys, copy the generated config snippet into `~/.oci/config`, update `key_file` to the private key path, and restrict the private key with `chmod go-rwx ~/.oci/oci_api_key.pem`. Then set `AIDP_AUTH=api_key`, `OCI_PROFILE` to the profile in `~/.oci/config`, and `OCI_CONFIG_FILE` if the config is not in the default location.

## Install On Windows

Use the zip archive on Windows PowerShell:

```powershell
New-Item -ItemType Directory -Force "$HOME\plugins" | Out-Null
Expand-Archive -Path ".\ask-aidp-codex-plugin-0.7.2.zip" -DestinationPath "$HOME\plugins" -Force
```

The extracted plugin directory is:

```text
%USERPROFILE%\plugins\ask-aidp
```

If `%USERPROFILE%\.agents\plugins\marketplace.json` does not exist, create it:

```powershell
New-Item -ItemType Directory -Force "$HOME\.agents\plugins" | Out-Null
@'
{
  "name": "personal",
  "interface": {
    "displayName": "Personal"
  },
  "plugins": [
    {
      "name": "ask-aidp",
      "source": {
        "source": "local",
        "path": "./plugins/ask-aidp"
      },
      "policy": {
        "installation": "AVAILABLE",
        "authentication": "ON_INSTALL"
      },
      "category": "Productivity"
    }
  ]
}
'@ | Set-Content -Encoding UTF8 "$HOME\.agents\plugins\marketplace.json"
```

If the file already exists, add this entry to its `plugins` array:

```json
{
  "name": "ask-aidp",
  "source": {
    "source": "local",
    "path": "./plugins/ask-aidp"
  },
  "policy": {
    "installation": "AVAILABLE",
    "authentication": "ON_INSTALL"
  },
  "category": "Productivity"
}
```

Then install from the personal marketplace:

```powershell
codex plugin add ask-aidp@personal
```

Configure AIDP/OCI for the current PowerShell session:

You can either set the variables manually or copy the sample file:

```powershell
Copy-Item ".\aidp.env.sample" ".\aidp.env"
```

Edit `.\aidp.env`, replace every placeholder, then set the equivalent PowerShell environment variables below.

```powershell
$env:AIDP_ENDPOINT = "https://aidp.<region>.oci.oraclecloud.com"
$env:AIDP_OCID = "ocid1.aidataplatform..."
$env:AIDP_WORKSPACE_KEY = "<workspace-key>"
$env:AIDP_CLUSTER_KEY = "<cluster-key>"
$env:OCI_PROFILE = "DEFAULT"
$env:AIDP_AUTH = "api_key"
```

Optional Windows variables:

```powershell
$env:OCI_CONFIG_FILE = "$HOME\.oci\config"
$env:OCI_REGION = "us-ashburn-1"
$env:AIDP_CLI_BIN = "C:\path\to\aidp.cmd"
$env:AIDP_CLUSTER_NAME = "<cluster-display-name>"
$env:AIDP_TIMEOUT_SECONDS = "60"
```

To persist variables for future PowerShell sessions, use `[Environment]::SetEnvironmentVariable("NAME", "VALUE", "User")`.

If `oci login` does not work in your environment, use OCI API key authentication instead. Follow Oracle's API signing key setup at <https://docs.oracle.com/en-us/iaas/Content/API/Concepts/apisigningkey.htm#two>: create or use an IAM user with the required AIDP permissions, generate a PEM RSA API signing key pair, upload the public key in OCI Console under User settings > API Keys, copy the generated config snippet into `%USERPROFILE%\.oci\config`, update `key_file` to the private key path, and keep the private key readable only by your user. On Windows, Oracle documents using Git Bash/OpenSSL to generate the key pair. Then set `$env:AIDP_AUTH = "api_key"`, `$env:OCI_PROFILE` to the profile in `%USERPROFILE%\.oci\config`, and `$env:OCI_CONFIG_FILE` if the config is not in the default location.

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

For generic commands, the plugin passes an argument array to `aidp-cli` and appends common endpoint, instance, profile, auth, and timeout flags from the environment.

The generated CLI reference covers these command groups: `async-operations`, `audit`, `bundle`, `catalog`, `cluster`, `credentials`, `delta-share`, `mlops`, `notebook`, `role`, `schema`, `user-setting`, `volume`, `workflow`, `workspace`, and `workspace-object`. Use `aidp_cli_reference` to list groups, list commands in a group, fetch one command reference, or search all documented commands.

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
- Packaged archives include the local `aidp-cli` dependency tree when available; otherwise users must install `aidp-cli` separately and set `AIDP_CLI_BIN` or `PATH`.
