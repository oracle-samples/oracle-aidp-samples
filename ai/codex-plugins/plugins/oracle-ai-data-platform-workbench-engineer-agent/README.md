# Oracle AI Data Platform - Engineer Agent (Codex Plugin)

Run the full Oracle AI Data Platform (AIDP) data-engineering surface from Codex in natural language.

This plugin includes 37 AIDP-focused skills plus a bundled Spark SQL helper. It is self-contained for the common path: control-plane work uses the official `aidp` CLI or `oci raw-request`, and interactive Spark SQL/notebook cells use the staged helper at `~/.aidp/aidp_sql.py`. The AIDP MCP server is optional.

## Install

Register the Oracle AIDP Codex plugin marketplace:

```bash
codex plugin marketplace add oracle-samples/oracle-aidp-samples \
    --ref main \
    --sparse ai/codex-plugins
```

Install this plugin:

```bash
codex plugin add oracle-ai-data-platform-workbench-engineer-agent@oracle-aidp-codex
```

Start a new Codex thread after install so the plugin skills and SessionStart hook are loaded.

## Runtime Prerequisites

- Codex CLI installed and signed in.
- Python 3.10 or newer on `PATH`.
- OCI CLI configured with a working `DEFAULT` API key profile.
- An AIDP DataLake, workspace, and reachable Spark cluster.

Set these values in the Codex shell or in `~/.codex/AGENTS.md`:

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

## First Run

The SessionStart hook stages the bundled helper to `~/.aidp/` and best-effort installs its Python dependencies. You should see:

```text
[aidp] helper staged to ~/.aidp - ready
```

On Windows, if `python3` is not on `PATH`, stage the helper manually once from the marketplace checkout:

```powershell
python "./plugins/oracle-ai-data-platform-workbench-engineer-agent/hooks/session_start.py"
```

## Suggested Smoke Test

Ask Codex:

```text
Set up and verify my AIDP connection.
```

Then:

```text
Map my catalog - what data do I have?
```

Then:

```text
What were my total net store sales, and how are sales spread across stores?
```

## What You Get

- Catalog initialization and exploration.
- Plain-English SQL analysis.
- AI-in-SQL workflows.
- Delta table management, optimize, and time travel.
- Data quality and profiling.
- Pipelines, notebooks, clusters, and Spark tuning.
- Governance, roles, credentials, audit, and data sharing.
- Agent flows, tools, knowledge bases, MLOps, bundles, git, migration, semantic model, verified queries, federation, ingestion, volumes, and workspace administration.

## Layout

```text
oracle-ai-data-platform-workbench-engineer-agent/
|-- .codex-plugin/plugin.json
|-- .mcp.json
|-- AGENTS.md
|-- aidp/
|   |-- aidp_sql.py
|   |-- check_env.py
|   |-- requirements.txt
|   `-- references/
|-- hooks/
|   |-- hooks.json
|   `-- session_start.py
`-- skills/
    `-- aidp-*/
        `-- SKILL.md
```

## Privacy

This plugin does not collect or transmit telemetry. It runs locally in Codex and uses the user's configured OCI credentials and AIDP environment. See [`PRIVACY.md`](./PRIVACY.md).

## License

MIT - see [`LICENSE`](./LICENSE).