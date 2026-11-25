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

## Install Fusion Autopilot

```bash
codex plugin add oracle-ai-data-platform-fusion-autopilot@oracle-aidp-codex
```

See [`plugins/oracle-ai-data-platform-fusion-autopilot/README.md`](./plugins/oracle-ai-data-platform-fusion-autopilot/README.md) for Fusion/AIDP/OAC prerequisites, and its `tests/` for the unit suite (`make test` inside the plugin directory).

## Install Snowflake Migrator

```bash
codex plugin add oracle-ai-data-platform-workbench-snowflake-migrator@oracle-aidp-codex
```

See [`plugins/oracle-ai-data-platform-workbench-snowflake-migrator/README.md`](./plugins/oracle-ai-data-platform-workbench-snowflake-migrator/README.md) for prerequisites and the runbook. The offline suite runs with `bin/snowmig-test` inside the plugin directory, and `bin/snowmig demo` runs the whole pipeline against an emulated estate with no credentials.

## Install Agent Builder (experimental)

```bash
codex plugin add oracle-ai-data-platform-agent-builder@oracle-aidp-codex
```

Start a new Codex chat. The [plugin README](./plugins/oracle-ai-data-platform-agent-builder/README.md)
explains the component-contract authoring flow, optional public AIDP operations,
shared UI/CLI and native runtime prerequisites.

From the repository root, run the offline suites:

```bash
cd ai/codex-plugins/plugins/oracle-ai-data-platform-agent-builder
python3 -m unittest discover -s tests -v
python3 -m unittest discover -s runtime/tests -v
node --test runtime/tests/test_ui.mjs
```

Python tests use temporary synthetic files and fake cloud transports; the HTTP
boundary test binds a loopback socket. Node is required only for UI tests.
No cloud credentials, source data, GenAI calls or deployments are required.

Authoring smoke prompt:

```text
Use AIDP Agent Builder. Create an agent that displays monthly deliveries
and reviews late deliveries using documented rules. Use synthetic records
for an offline authoring rehearsal; validate, package and show a deployment
plan without publishing or claiming native runtime verification.
```

Check saved contracts, stable actions and requirement mapping. Run the bundled
source validator and packager on the generated project. Verify that a plan's
operations remain dry runs with an unresolved created-agent key, and that no
private target configuration enters the ZIP. Offline success does not qualify
native CODE hooks, A2A Parts, real data/model tools, persistence or interrupts.
Those require separate probes on the selected target before a native readiness
claim or UI/CLI end-to-end demonstration.

## Update

```bash
codex plugin marketplace upgrade oracle-aidp-codex
```