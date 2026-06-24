# Test Oracle AIDP Codex Plugins

This folder is a Codex plugin marketplace hosted inside `oracle-samples/oracle-aidp-samples`.

## Add Marketplace

```bash
codex plugin marketplace add oracle-samples/oracle-aidp-samples \
    --ref main \
    --sparse ai/codex-plugins
codex plugin marketplace list
```

Expected marketplace name:

```text
oracle-aidp-codex
```

> **Path resolution (verified, codex-cli 0.141.0):** Codex resolves each plugin's
> `source.path` in `.agents/plugins/marketplace.json` relative to the **marketplace
> root** (the `ai/codex-plugins` dir / sparse subdir you add), *not* relative to the
> manifest file's own directory. So `"path": "./plugins/<name>"` correctly resolves to
> `ai/codex-plugins/plugins/<name>`. Confirm with `codex plugin list` — the PATH column
> should point at `…/ai/codex-plugins/plugins/<name>`.

## Install Engineer Agent

```bash
codex plugin add oracle-ai-data-platform-workbench-engineer-agent@oracle-aidp-codex
```

Start a new Codex thread after install.

## Engineer Agent Prerequisites

- Codex CLI installed and signed in.
- Python 3.10 or newer on `PATH`.
- OCI CLI configured with a working `DEFAULT` API key profile.
- An AIDP DataLake, workspace, and reachable Spark cluster.

Set the runtime environment:

```bash
export AIDP_REGION=<your-region>
export AIDP_DATALAKE=<your-datalake-ocid>
export AIDP_WORKSPACE=<your-workspace-id>
export AIDP_CLUSTER=<your-cluster-key>
```

PowerShell:

```powershell
$env:AIDP_REGION = "<your-region>"
$env:AIDP_DATALAKE = "<your-datalake-ocid>"
$env:AIDP_WORKSPACE = "<your-workspace-id>"
$env:AIDP_CLUSTER = "<your-cluster-key>"
```

## Engineer Agent Smoke Flow

Ask Codex these in a new thread:

```text
Set up and verify my AIDP connection.
```

```text
Map my catalog - what data do I have?
```

```text
What were my total net store sales, transactions and customers, and how are sales spread across stores?
```

```text
Using AI right inside the SQL, give me a business read on how sales are distributed across stores.
```

```text
Show the version history of a Delta table and optimize it.
```

## Install Databricks Migrator

```bash
codex plugin add oracle-ai-data-platform-workbench-databricks-migrator@oracle-aidp-codex
```

See [`plugins/oracle-ai-data-platform-workbench-databricks-migrator/README.md`](./plugins/oracle-ai-data-platform-workbench-databricks-migrator/README.md) for migrator-specific prerequisites and smoke tests.

## Update

```bash
codex plugin marketplace upgrade oracle-aidp-codex
```